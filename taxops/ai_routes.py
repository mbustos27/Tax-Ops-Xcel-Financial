from __future__ import annotations

import functools
import io
import json
import logging
import os
import re

import requests
from datetime import date

from flask import (
    Blueprint,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

import db_tools

from config import (
    CHAT_ROUTER_CONFIDENCE_MIN,
    CHAT_ROUTER_TRANSPORT_FAIL_AGGREGATES,
    CHAT_ANSWER_ON_ROUTER_TIMEOUT_SKIP_LLM,
    CHAT_TRAINING_LOG_CACHE_HITS,
    CHAT_TRAINING_LOG_ENABLE,
    CHAT_TRAINING_LOG_PATH,
    OLLAMA_BASE_URL,
    OLLAMA_CHAT_ANSWER_TIMEOUT_SEC,
    OLLAMA_CHAT_MODEL,
    OLLAMA_CHAT_ROUTER_TIMEOUT_SEC,
    OLLAMA_EXTRACT_MODEL_TEXT,
    OLLAMA_EXTRACT_MODEL_VISION,
    OLLAMA_MODEL,
    OLLAMA_ROUTER_MODEL,
)
from db import get_connection
from db_tools import lookup_rejection_code
from form_schema import FORM_INTEGER_COLUMNS, FORM_TABLE_INSERT_COLUMNS
from llm import chat, extract_json
from utils import now, scrub_ssn_from_dict
from chat_cache import (
    CHAT_ALLOWED_STATUSES,
    apply_row_tool_grounding_guard,
    chronological_superlative_chat_payload,
    classify_intent,
    dataplane_digest_freshness_banner,
    ensure_chat_cache_for_year,
    format_season_rejection_breakdown_for_chat,
    get_cached_answer,
    normalize_chat_router_payload,
    normalize_question,
    prefers_narrative_list_answer,
    set_cached_answer,
    snapshot_office_brief_digest,
    try_deterministic_response,
    use_narrative_return_rows,
    wants_qualitative_return_answer,
    wants_rejection_reason_breakdown,
    wants_tool_row_aggregate,
    _extract_processor_guess,
    _extract_status_from_question,
    _resolve_processor_match,
)
from chat_scope_classifier import should_block_tool_router_llm
from chat_training_log import append_staff_ai_chat_question

ai = Blueprint("ai", __name__, url_prefix="/ai")

CHAT_TOOLS = [
    {
        "name": "get_returns_by_status",
        "description": (
            "Get returns by **workflow queue/status** only: PROCESSING, HOLD, FINALIZE, PICKUP, "
            "EFILE READY, LOG OUT, REJECTED, CANCELLED (exact labels stored on the return). "
            "Never use invented statuses such as UNPAID, **EFILE**, or EFILING—those are not stored labels. "
            "For unpaid fees / who owes money / ranking balances use get_balance_due_returns instead. "
            "For \"e-file finished and logged out\" / clerical logout volume, use status **LOG OUT** (completed), "
            "not **EFILE READY** (transmit queue). "
        ),
        "args": {
            "status": "one of: PROCESSING, HOLD, FINALIZE, PICKUP, EFILE READY, LOG OUT, REJECTED, CANCELLED",
            "year": "4-digit tax year integer e.g. 2025",
        },
    },
    {
        "name": "get_returns_by_processor",
        "description": "Get returns assigned to a specific preparer/processor for a given tax year. Use for questions like 'what returns does Maria have?' or 'show me John's workload'.",
        "args": {
            "processor": "preparer name string",
            "year": "4-digit tax year integer",
        },
    },
    {
        "name": "get_client_returns",
        "description": "Get all returns for a specific client across all years. Use for questions like 'what returns does the Martinez family have?' or 'show me all years for client ID 123'.",
        "args": {"client_id": "integer client ID"},
    },
    {
        "name": "get_balance_due_returns",
        "description": (
            "Returns where recorded payments are less than the billed fee (still owe money). "
            "Use for any fee/balance question: outstanding balances, totals, WHO owes the most, "
            "highest or lowest balance client, ranking by amount owed—same data regardless of "
            "PROCESSING/HOLD/etc."
        ),
        "args": {"year": "4-digit tax year integer"},
    },
    {
        "name": "get_missing_docs",
        "description": "Get unresolved missing documents for a specific return. Use for questions like 'what documents are missing for return 456?' or 'what is the Martinez file waiting on?'.",
        "args": {"return_id": "integer return ID"},
    },
    {
        "name": "search_clients",
        "description": "Search for clients by name. Use when the staff member mentions a client name and you need their ID or return information. Use for questions like 'find the Ramirez family' or 'search for John Smith'.",
        "args": {"query": "name search string"},
    },
]

CHAT_TOOL_ALLOWLIST = {t["name"] for t in CHAT_TOOLS}

# Embed the same live roll-up in router + answer prompts: all status counts + preparer tallies
# (staff names from `returns.processor`, not client taxpayer PII—see get_system_context docstring).
_AI_CHAT_PREP_ROLLUP_CAP = 240

# JSON shard length caps — office dataplane (counts only); keep router prompt slim.
_AI_ROUTER_DATAPLANE_JSON_CAP = 880
_AI_CHAT_DATAPLANE_JSON_COMPACT_CAP = 1100
# Markdown office dataplane injected on **every** LLM routed turn (router + aggregate answers).
_AI_ROUTER_UNIVERSAL_DIGEST_CAP = 5200
_AI_CHAT_NULL_TOOL_BRIEF_CAP = 11800
# When router Ollama fails and we skip the answer LLM, cap digest pasted into the reply (UI / payload size).
_AI_CHAT_DEGRADED_SKIP_LLM_DIGEST_CAP = 6200


def _shard_dataplane_json(dc: object, max_chars: int) -> str:
    if not isinstance(dc, dict) or not dc:
        return ""
    try:
        s = json.dumps(dc, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        return ""
    if len(s) <= max_chars:
        return s
    return s[: max(0, max_chars - 20)] + "…(truncate)"


_OFFICE_SUMMARY_SENTENCE = (
    "TaxOps is internal workflow software for tax offices—it tracks returns by status, assigned "
    "preparer workload, unpaid balances (fee vs payments), and missing documents for the active "
    "filing season; aggregates below omit client-identifying fields."
)


def _status_count_lookup(status_counts: dict[str, int], status_guess: str) -> int | None:
    g = status_guess.strip()
    if not g:
        return None
    if g in status_counts:
        return int(status_counts[g])
    gu = g.upper()
    for k, val in status_counts.items():
        if str(k).strip().upper() == gu:
            return int(val)
    return None


def _format_ai_chat_system_compact(ctx: dict) -> str:
    """Always includes every status count + preparer tallies (truncation note if over cap)."""
    td = str(ctx.get("today_local_iso") or "")
    yr = ctx.get("season_year")
    sc = dict(ctx.get("status_counts") or {})
    pmap = dict(ctx.get("processor_return_counts") or {})
    bal = int(ctx.get("balance_due_season_total") or 0)
    total_returns = sum(int(v) for v in sc.values())
    st_bits = "; ".join(f"{k}: {sc[k]}" for k in sorted(sc.keys(), key=lambda s: str(s).casefold()))
    prep_pairs = sorted(pmap.items(), key=lambda kv: str(kv[0]).casefold())
    cap = _AI_CHAT_PREP_ROLLUP_CAP
    shown = prep_pairs[:cap]
    prep_bits = "; ".join(f"{name}: {int(pv)}" for name, pv in shown)
    omit = ""
    if len(prep_pairs) > cap:
        omit = f" (+{len(prep_pairs) - cap} more preparers truncated in this line for length.)"
    core = (
        "### SYSTEM CONTEXT — compact (authoritative counts; no client names, IDs, or log numbers)\n"
        "\n"
        f"{_OFFICE_SUMMARY_SENTENCE}\n\n"
        f"- Server date **{td}**; season year **{yr}**.\n"
        f"- **Every workflow status → count:** {st_bits or '(none)'}\n"
        f"- **Preparer → return count:** {prep_bits or '(none)'}{omit}\n"
        f"- Returns with unpaid fee balance (season tally): **{bal}**; sum(status counts) ≈ **{total_returns}**."
    )
    dp_shard = _shard_dataplane_json(dict(ctx.get("dataplane_compact") or {}), _AI_CHAT_DATAPLANE_JSON_COMPACT_CAP)
    if dp_shard:
        core += (
            "\n\n### Office dataplane (counts / staged money KPIs; JSON)\n"
            + dp_shard
        )
    return core


def _format_ai_chat_system_context(ctx: dict) -> str:
    """Plain-text office snapshot for LLM prompts (< ~300 words; no taxpayer PII)."""
    td = str(ctx.get("today_local_iso") or "")
    yr = ctx.get("season_year")
    sc = dict(ctx.get("status_counts") or {})
    pmap = dict(ctx.get("processor_return_counts") or {})
    bal = int(ctx.get("balance_due_season_total") or 0)
    total_returns = sum(int(v) for v in sc.values())

    lines: list[str] = [
        "### SYSTEM CONTEXT — authoritative aggregates (no taxpayer names, IDs, log numbers)",
        "",
        _OFFICE_SUMMARY_SENTENCE,
        "",
        f"- Today's date (TaxOps server local calendar): **{td}**",
        f"- **Season year selector** for this POST /ai/chat: **{yr}**.",
        f"- Returns in season **by workflow status**:",
    ]
    for label in sorted(sc.keys(), key=lambda s: s.casefold()):
        lines.append(f"  • `{label}`: **{sc[label]}**")
    lines.append("")
    lines.append("- **Preparer workload** (distinct processor assignments this season, as stored internally):")
    prep_pairs = sorted(pmap.items(), key=lambda kv: kv[0].casefold())
    MAX_PREP_ROWS = _AI_CHAT_PREP_ROLLUP_CAP
    for name, pv in prep_pairs[:MAX_PREP_ROWS]:
        lines.append(f"  • **{name}**: {int(pv)} return(s)")
    if len(prep_pairs) > MAX_PREP_ROWS:
        lines.append(
            f"  • … _(+{len(prep_pairs) - MAX_PREP_ROWS} more preparers; use DB tools when exact rows are needed)_."
        )
    elif not prep_pairs:
        lines.append("  • _(none with a labeled processor)_")
    lines.extend(
        (
            "",
            f"- Returns flagged with unpaid balance (season): **{bal}**.",
            f"- Combined count across statuses above (**sanity**: sum of statuses): **{total_returns}**.",
        )
    )
    text_core = "\n".join(lines)
    dp_shard = _shard_dataplane_json(dict(ctx.get("dataplane_compact") or {}), _AI_CHAT_DATAPLANE_JSON_COMPACT_CAP)
    if len(text_core.split()) > 320:
        return _format_ai_chat_system_compact(ctx)
    text = text_core
    if dp_shard:
        text += (
            "\n\n### Office dataplane (counts / staged money KPIs; JSON)\n"
            + dp_shard
        )
    return text


def _prepend_ai_chat_system_context(
    body: str, ctx: dict, *, office_brief_addon: str | None = None
) -> str:
    head = _format_ai_chat_system_context(ctx).rstrip()
    if office_brief_addon:
        head += (
            "\n\n### Expanded office dataplane composite (trusted aggregates)\n\n"
            + office_brief_addon.strip()
            + "\n"
        )
    return head + "\n\n---\n\n" + body


def _format_router_compact_system_context(ctx: dict) -> str:
    """Compact aggregates for router LLM (minimal tokens → reliable JSON tool selection)."""
    td = str(ctx.get("today_local_iso") or "")
    yr = ctx.get("season_year", "")
    sc = dict(ctx.get("status_counts") or {})
    pmap = dict(ctx.get("processor_return_counts") or {})
    bal = int(ctx.get("balance_due_season_total") or 0)
    total = sum(int(v) for v in sc.values())
    st_bits = "; ".join(f"{k}={sc[k]}" for k in sorted(sc.keys(), key=lambda s: str(s).casefold()))
    cap = _AI_CHAT_PREP_ROLLUP_CAP
    prep_ordered = sorted(pmap.items(), key=lambda kv: str(kv[0]).casefold())
    prep_items = prep_ordered[:cap]
    prep_bits = "; ".join(f"{name}:{int(c)}" for name, c in prep_items)
    more_prep = ""
    if len(prep_ordered) > cap:
        more_prep = f" (+{len(prep_ordered) - cap} preparers truncated in ROUTER CONTEXT for length)."
    head = (
        "### ROUTER CONTEXT — trusted aggregates (no taxpayer PII)\n"
        f"{_OFFICE_SUMMARY_SENTENCE}\n"
        f"Server date **{td}**; season_year selector **{yr}**.\n"
        f"Statuses → count: {st_bits or '(none)'}.\n"
        f"Preparer → return_count: {prep_bits or '(none)'}{more_prep}\n"
        f"Unpaid_balance_season_tally=**{bal}**; sum_of_status_badges≈**{total}**."
    )
    dp_shard = _shard_dataplane_json(dict(ctx.get("dataplane_compact") or {}), _AI_ROUTER_DATAPLANE_JSON_CAP)
    if dp_shard:
        head += f"\nDataplane KPIs_JSON= {dp_shard}"
    return head


def _prepend_router_system_context(body: str, ctx: dict) -> str:
    return _format_router_compact_system_context(ctx).rstrip() + "\n\n---\n\n" + body


def _chat_arg_int(raw, fallback: int) -> int:
    try:
        if raw is None:
            return fallback
        return int(float(str(raw).strip()))
    except (TypeError, ValueError):
        return fallback


def _normalized_chat_tool_args(tool_name: str, raw_args: object, fallback_year: int) -> dict:
    """Normalize LLM JSON args so db_tools call signatures match."""
    args = raw_args if isinstance(raw_args, dict) else {}
    low = {(str(k).strip().lower() if k is not None else ""): v for k, v in args.items()}
    low = {k: v for k, v in low.items() if k}

    if tool_name == "get_returns_by_status":
        return {
            "status": str(low.get("status") or "").strip(),
            "year": _chat_arg_int(low.get("year"), fallback_year),
        }
    if tool_name == "get_returns_by_processor":
        return {
            "processor": str(low.get("processor") or "").strip(),
            "year": _chat_arg_int(low.get("year"), fallback_year),
        }
    if tool_name == "get_client_returns":
        return {"client_id": _chat_arg_int(low.get("client_id"), 0)}
    if tool_name == "get_balance_due_returns":
        return {"year": _chat_arg_int(low.get("year"), fallback_year)}
    if tool_name == "get_missing_docs":
        return {"return_id": _chat_arg_int(low.get("return_id"), 0)}
    if tool_name == "search_clients":
        return {"query": str(low.get("query") or "").strip()}
    return {}


_ALLOWED_FORM_TABLES = frozenset(
    {
        "w2_records",
        "f1099_nec_records",
        "f1099_misc_records",
        "f1099_int_records",
        "f1099_div_records",
    }
)


def _detect_form_type(doc_type: str | None, fields: dict) -> str | None:
    """
    Determine which form table to save extracted data to.
    Returns table name string or None if form type unknown.
    """

    dt = (doc_type or "").strip()

    tagged = frozenset({"prior_return", "government_id", "misc"})
    if dt in tagged:
        return None

    form_type_raw = str(fields.get("form_type", "") or "").upper().strip()
    form_type_map = {
        "W-2": "w2_records",
        "W2": "w2_records",
        "1099-NEC": "f1099_nec_records",
        "1099NEC": "f1099_nec_records",
        "1099-MISC": "f1099_misc_records",
        "1099MISC": "f1099_misc_records",
        "1099-INT": "f1099_int_records",
        "1099INT": "f1099_int_records",
        "1099-DIV": "f1099_div_records",
        "1099DIV": "f1099_div_records",
    }
    if form_type_raw in form_type_map:
        return form_type_map[form_type_raw]

    doc_type_map = {
        "W-2": "w2_records",
        "1099": "f1099_nec_records",
    }
    if dt in doc_type_map:
        return doc_type_map[dt]

    if fields.get("box1_wages_tips_other") or fields.get("box3_social_security_wages"):
        return "w2_records"
    if fields.get("box1_nonemployee_compensation"):
        return "f1099_nec_records"
    if fields.get("box1_interest_income"):
        return "f1099_int_records"
    if fields.get("box1a_total_ordinary_dividends"):
        return "f1099_div_records"
    if (
        fields.get("box1_rents")
        or fields.get("box2_royalties")
        or fields.get("box3_other_income")
    ):
        return "f1099_misc_records"

    employer = str(fields.get("employer_name", "") or "").strip().lower()
    wages_raw = fields.get("box1_wages_tips_other")
    wages = str(wages_raw or "").strip()
    if wages and employer:
        return "w2_records"

    payer = str(fields.get("payer_name", "") or "").strip()
    nonemployee = fields.get("box1_nonemployee_compensation", "")
    if payer and str(nonemployee or "").strip():
        return "f1099_nec_records"

    return None


_ALLOWED_CLASSIFY_DOC_TYPES = frozenset(
    {"W-2", "1099", "prior_return", "government_id", "misc", "unknown"}
)

_CLASSIFY_SUPPORTED_EXTS = frozenset({".pdf", ".jpg", ".jpeg", ".png"})


def _form_table_to_doc_type(table_name: str | None) -> str:
    if not table_name:
        return "unknown"
    return {
        "w2_records": "W-2",
        "f1099_nec_records": "1099",
        "f1099_misc_records": "1099",
        "f1099_int_records": "1099",
        "f1099_div_records": "1099",
    }.get(table_name, "unknown")


def _classify_document_using_row(
    conn,
    row,
    *,
    only_if_still_unknown: bool,
) -> dict:
    """
    Run _extract_fields + _detect_form_type and optionally UPDATE return_documents.doc_type.
    Never raises. Does not commit (caller commits).
    """
    doc_id = row["id"]
    out: dict = {"doc_type": "unknown", "doc_id": doc_id}
    file_path = row["file_path"]
    filename = (row["filename"] or os.path.basename(file_path or "") or "document").strip()

    if not file_path or not isinstance(file_path, str):
        return out
    if not os.path.isfile(file_path):
        return out

    ext = os.path.splitext(file_path)[1].lower()
    if ext not in _CLASSIFY_SUPPORTED_EXTS:
        return out

    fields = None
    try:
        from extractor import _extract_fields

        tup = _extract_fields(file_path, filename)
        if tup and tup[0]:
            fields = tup[0]
    except Exception as e:
        logging.getLogger(__name__).info("Classify extract skipped for doc %s: %s", doc_id, e)

    if not fields:
        return out

    doc_type_db = (row["doc_type"] or "unknown").strip()
    table = _detect_form_type(doc_type_db, fields)
    tag = _form_table_to_doc_type(table)
    if tag not in _ALLOWED_CLASSIFY_DOC_TYPES:
        tag = "unknown"
    out["doc_type"] = tag

    if tag == "unknown":
        return out

    if only_if_still_unknown:
        conn.execute(
            """
            UPDATE return_documents
            SET doc_type = ?
            WHERE id = ? AND is_deleted = 0
              AND (
                doc_type IS NULL
                OR TRIM(doc_type) = ''
                OR LOWER(TRIM(doc_type)) = 'unknown'
              )
            """,
            (tag, doc_id),
        )
    else:
        conn.execute(
            """
            UPDATE return_documents
            SET doc_type = ?
            WHERE id = ? AND is_deleted = 0
            """,
            (tag, doc_id),
        )
    return out


def _classify_document(doc_id: int, *, only_if_still_unknown: bool = True) -> dict:
    """
    DOC-5 — auto doc_type from LLM fields. Safe for background threads (own connection).
    Does not raise; returns {doc_type, doc_id}. Staff-tagged types preserved when
    only_if_still_unknown is True.
    """
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT id, return_id, doc_type, file_path, filename
            FROM return_documents
            WHERE id = ? AND is_deleted = 0
            """,
            (doc_id,),
        ).fetchone()
        if not row:
            return {"doc_type": "unknown", "doc_id": doc_id}
        out = _classify_document_using_row(
            conn, row, only_if_still_unknown=only_if_still_unknown
        )
        conn.commit()
        return out
    except Exception as e:
        logging.getLogger(__name__).error("Classify failed for doc %s: %s", doc_id, e)
        try:
            conn.rollback()
        except Exception:
            pass
        return {"doc_type": "unknown", "doc_id": doc_id}
    finally:
        conn.close()


def _coerce_sql_integer_field(val) -> int:
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, (int, float)):
        return 1 if int(val) != 0 else 0
    s = str(val or "").strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return 1
    return 0


def _save_form_data(conn, table_name: str, return_id: int, doc_id: int, fields: dict) -> bool:
    """
    Save extracted form data to the correct typed table.
    Never stores SSN or identification-number fields.
    Text amounts stay TEXT; checkbox-style fields are INTEGER 0/1.
    Never raises — logs errors and returns False on failure.
    """
    if table_name not in _ALLOWED_FORM_TABLES:
        return False

    ridok = conn.execute("SELECT 1 FROM returns WHERE id = ?", (return_id,)).fetchone()
    if not ridok:
        return False

    doc_row = conn.execute(
        """
        SELECT 1 FROM return_documents
        WHERE id = ? AND return_id = ? AND is_deleted = 0
        """,
        (doc_id, return_id),
    ).fetchone()
    if not doc_row:
        return False

    allowed_cols = FORM_TABLE_INSERT_COLUMNS.get(table_name, [])
    if not allowed_cols:
        return False

    fd = dict(fields)

    cols = ["return_id", "doc_id", "source", "created_at"]
    vals: list = [return_id, doc_id, "extracted", now()]

    for col in allowed_cols:
        if col not in fd:
            continue
        val = fd[col]
        if val is None:
            continue
        if col in FORM_INTEGER_COLUMNS:
            cols.append(col)
            vals.append(_coerce_sql_integer_field(val))
            continue
        sval = str(val).strip()
        if sval:
            cols.append(col)
            vals.append(sval)

    if len(cols) <= 4:
        return False

    placeholders = ", ".join(["?" for _ in vals])
    col_names = ", ".join(cols)

    try:
        conn.execute(
            f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})",
            vals,
        )
        return True
    except Exception as e:
        current_app.logger.error("Form data save failed %s: %s", table_name, e)
        return False


# ---------------------------------------------------------------------------
# DOC-4 document extraction — PDF text vs vision pipeline
# ---------------------------------------------------------------------------

def _extract_pdf_text(file_path: str) -> str | None:
    """
    Extract text from a generated PDF using pdfplumber.
    Returns extracted text string or None if PDF has no text layer.
    Never raises — returns None on any failure.
    """
    try:
        import pdfplumber

        with pdfplumber.open(file_path) as pdf:
            text_parts = []
            for page in pdf.pages[:3]:
                text = page.extract_text()
                if text:
                    text_parts.append(text.strip())
            full_text = "\n".join(text_parts)
            if len(full_text.strip()) < 50:
                return None
            return full_text[:3000]
    except Exception as e:
        try:
            current_app.logger.info(f"PDF text extraction failed: {e}")
        except RuntimeError:
            logging.getLogger(__name__).info("PDF text extraction failed: %s", e)
        return None


def _pdf_to_image_b64(file_path: str) -> str:
    """
    Convert first page of PDF to base64 JPEG for vision model.
    Uses PyMuPDF (no Poppler/Ghostscript — works on Windows without extra PATH setup).
    Raises ValueError if conversion fails.
    """
    import base64

    try:
        import fitz  # PyMuPDF
    except ImportError:
        raise ValueError(
            "PDF rasterizer not installed — run: pip install pymupdf"
        ) from None

    try:
        from PIL import Image
    except ImportError:
        raise ValueError("Pillow required — pip install Pillow") from None

    try:
        with fitz.open(file_path) as doc:
            if doc.page_count < 1:
                raise ValueError("PDF has no pages")
            page = doc.load_page(0)
            zoom = 200 / 72
            mat = fitz.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat, alpha=False)
            mode = "RGB" if pix.n == 3 else "RGBA"
            img = Image.frombytes(mode, [pix.width, pix.height], pix.samples)
            buffer = io.BytesIO()
            if img.mode != "RGB":
                img = img.convert("RGB")
            img.save(buffer, format="JPEG", quality=85)
            buffer.seek(0)
            return base64.b64encode(buffer.read()).decode("utf-8")
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"PDF conversion failed: {e}") from e


def _image_to_b64(file_path: str) -> str:
    import base64

    with open(file_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_USELESS_REASONS = frozenset({
    "", "test", "n/a", "na", "none", "tbd", "unknown",
    "?", "-", ".", "no reason", "no reason provided",
})

def _reason_is_useful(reason: str) -> bool:
    """Return True if rejection_reason contains meaningful staff-entered text.

    Filters out empty strings, placeholder values, and entries too short to
    convey any real information (< 10 chars after stripping).
    """
    stripped = (reason or "").strip()
    if len(stripped) < 10:
        return False
    return stripped.lower() not in _USELESS_REASONS


# ---------------------------------------------------------------------------
# Auth — same logic as login_required in app.py; duplicated here to avoid
# a circular import (app.py imports this module).
# ---------------------------------------------------------------------------

def _login_required(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            p = request.path or ""
            if p.startswith("/api/") or p.startswith("/ai/"):
                return jsonify({"error": "login_required"}), 401
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return wrapper


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@ai.get("/status")
@_login_required
def ai_status():
    """Ping Ollama and report whether it is reachable.
    Also reports fastText classifier status.
    Always returns HTTP 200 — callers check the 'ok' field.
    """
    # ── Ollama status ──────────────────────────────────────────────────────────
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        resp.raise_for_status()
        data        = resp.json()
        model_count = len(data.get("models") or [])
        ollama_ok   = True
        ollama_err  = None
    except Exception as exc:
        ollama_ok   = False
        model_count = 0
        ollama_err  = str(exc)

    # ── fastText classifier status ─────────────────────────────────────────────
    from classifier import MODEL_PATH, MIN_TRAINING_EXAMPLES
    import os as _os

    model_exists  = _os.path.exists(MODEL_PATH)
    model_size_kb = int(_os.path.getsize(MODEL_PATH) / 1024) if model_exists else 0

    try:
        conn = get_connection()
        training_count = conn.execute(
            "SELECT COUNT(*) FROM email_classifications WHERE confirmed_by IS NOT NULL"
        ).fetchone()[0]
        conn.close()
    except Exception:
        training_count = 0

    fasttext_status = {
        "model_trained":    model_exists,
        "model_size_kb":    model_size_kb,
        "training_examples": training_count,
        "min_required":     MIN_TRAINING_EXAMPLES,
        "ready":            model_exists and training_count >= MIN_TRAINING_EXAMPLES,
    }

    response = {
        "ok":          ollama_ok,
        "model_count": model_count,
        "fasttext":    fasttext_status,
        "chat_ready": ollama_ok,
        "chat_tools_available": len(CHAT_TOOL_ALLOWLIST),
        "chat_tools": list(CHAT_TOOL_ALLOWLIST),
        "ollama_chat": {
            "default_model": OLLAMA_MODEL,
            "router_model": OLLAMA_ROUTER_MODEL,
            "answer_model": OLLAMA_CHAT_MODEL,
            "router_timeout_sec": OLLAMA_CHAT_ROUTER_TIMEOUT_SEC,
            "answer_timeout_sec": OLLAMA_CHAT_ANSWER_TIMEOUT_SEC,
        },
        "ollama_extraction": {
            "text_model": OLLAMA_EXTRACT_MODEL_TEXT,
            "vision_model": OLLAMA_EXTRACT_MODEL_VISION,
        },
    }
    try:
        from extractor import extraction_queue_status_for_api

        response["extraction_queue"] = extraction_queue_status_for_api()
    except Exception:
        response["extraction_queue"] = {
            "pending": 0,
            "processing": 0,
            "completed_today": 0,
            "needs_review": 0,
            "failed": 0,
        }
    if ollama_err:
        response["error"] = ollama_err
    return jsonify(response)


@ai.get("/chat")
@_login_required
def ai_chat_page():
    """Staff UI: plain-English questions routed to allowlisted DB tools via the LLM."""
    try:
        year = int(request.args.get("year", date.today().year))
    except ValueError:
        year = date.today().year

    from app import base_ctx

    ctx = base_ctx(year)
    ctx["active_page"] = "ai_chat"
    return render_template("ai_chat.html", **ctx)


@ai.post("/chat")
@_login_required
def ai_chat():
    """LLM-6 — tool router: LLM picks an allowlisted db_tools function; results are scrubbed."""
    try:
        return _ai_chat_submit()
    except Exception:
        current_app.logger.exception("POST /ai/chat failed")
        return jsonify(
            {
                "answer": "",
                "error": "Something went wrong while handling your question. Check server logs.",
                "tool_used": None,
                "args_used": None,
            }
        ), 500


def _ai_chat_submit():
    data = request.get_json(silent=True) or {}
    question = (data.get("question") or "").strip()
    year = data.get("year", 2025)

    if not question:
        return jsonify({"error": "No question provided"}), 400

    try:
        year = int(year)
    except (TypeError, ValueError):
        year = 2025

    nq = normalize_question(question)
    hit = get_cached_answer(nq, year)
    intent_log, _ = classify_intent(question)
    if hit:
        append_staff_ai_chat_question(
            question=question,
            normalized=nq,
            year=year,
            response_payload=dict(hit),
            classified_intent=intent_log,
            scope_classifier_blocked=False,
            from_answer_cache_hit=True,
            log_enable=CHAT_TRAINING_LOG_ENABLE,
            log_include_cache_hits=CHAT_TRAINING_LOG_CACHE_HITS,
            log_path=CHAT_TRAINING_LOG_PATH,
            telemetry={},
        )
        return jsonify(hit), 200

    ensure_chat_cache_for_year(year)
    intent, ents = classify_intent(question)

    router_office_digest = snapshot_office_brief_digest(year, max_chars=_AI_ROUTER_UNIVERSAL_DIGEST_CAP)
    null_answer_office_digest = snapshot_office_brief_digest(
        year, max_chars=_AI_CHAT_NULL_TOOL_BRIEF_CAP
    )

    conn_fc = get_connection()
    try:
        live_ctx = db_tools.get_system_context(conn_fc, year)
        fast_resp = try_deterministic_response(
            conn_fc,
            intent,
            ents,
            question,
            year,
            office_ctx_live=live_ctx,
        )
    finally:
        conn_fc.close()

    log_chat_ctx: dict = {"scope_blocked": False, "telemetry": {}}

    def _chat_finalize(payload: dict):
        body = dict(payload)
        if "cached" not in body and body.get("answer") is not None:
            set_cached_answer(nq, year, body)
        telem = {
            str(k): v
            for k, v in (log_chat_ctx.get("telemetry") or {}).items()
            if v is not None and str(k)
        }
        append_staff_ai_chat_question(
            question=question,
            normalized=nq,
            year=year,
            response_payload=body,
            classified_intent=intent,
            scope_classifier_blocked=log_chat_ctx["scope_blocked"],
            from_answer_cache_hit=body.get("cached") is True,
            log_enable=CHAT_TRAINING_LOG_ENABLE,
            log_include_cache_hits=CHAT_TRAINING_LOG_CACHE_HITS,
            log_path=CHAT_TRAINING_LOG_PATH,
            telemetry=telem,
        )
        return jsonify(body), 200

    if fast_resp is not None:
        return _chat_finalize(fast_resp)

    if should_block_tool_router_llm(question):
        current_app.logger.info("Chat scope classifier blocked LLM tool router")
        log_chat_ctx["scope_blocked"] = True
        return _chat_finalize(
            {
                "answer": (
                    "I can only answer questions about returns, clients, balances, "
                    "and missing documents in TaxOps. Try asking about a specific status, preparer, or client."
                ),
                "tool_used": None,
                "args_used": None,
            }
        )

    tools_description = "\n".join(
        [f"- {t['name']}: {t['description']} Args: {t['args']}" for t in CHAT_TOOLS]
    )

    llm_office_ctx_cache: dict | None = None

    def live_office_llm_context() -> dict:
        nonlocal llm_office_ctx_cache
        if llm_office_ctx_cache is None:
            cx = get_connection()
            try:
                llm_office_ctx_cache = db_tools.get_system_context(cx, year)
            finally:
                cx.close()
        return llm_office_ctx_cache

    selection_prompt = (
        "You are a planner + tool router for TaxOps tax-office workflow software.\n"
        f'Staff question: "{question}"\n'
        f"Current tax year: {year}\n"
        f"Deterministic classifier tag: `{intent}` (regex + heuristics—when **`fallback`** the question "
        "was not classified; aggregates-first is safer).\n\n"
        "**Modes:** `aggregates` = answer from KPI roll-up / dataplane markdown only (**tool** must be **`null`**). "
        "`need_rows` = one structured tool with concrete parameters is unmistakably required. "
        "`clarify` = wording is ambiguous; set **tool** **`null`** and put a short clarification in "
        "**`clarify_prompt`**.\n\n"
        f"Available tools (only valid when **`mode`**=`need_rows`):\n{tools_description}\n\n"
        f"Confidence: set **`confidence`** 0–1 for how certain you are; below **{CHAT_ROUTER_CONFIDENCE_MIN:g}** "
        "alongside **`need_rows`** the server will drop the tool and answer from aggregates instead.\n\n"
        'Respond with JSON only — no markdown, no explanation:\n'
        "{\n"
        '  "mode": "aggregates" | "need_rows" | "clarify",\n'
        '  "confidence": 0.0,\n'
        '  "tool": null,\n'
        '  "args": {},\n'
        '  "clarify_prompt": null\n'
        "}\n\n"
        "Rules:\n"
        "- **`mode`**=`aggregates` whenever office-wide counts / histograms / composite markdown can answer.\n"
        '- Use **`mode`**=`aggregates` when ROUTER CONTEXT already lists the exact status totals needed '
        "(read Statuses → count).\n"
        "- Use **`mode`**=`need_rows` only for explicit per-return / per-client row pulls (IDs, named search, "
        "missing-doc for a specific return id, balance rank lists, preparer roster with a real name in the text).\n"
        "- **Never** invent workflow status strings; never chain multiple statuses into one `status` arg.\n"
        "- If the question asks about **kinds / types** of returns (form mix—not PROCESSING/HOLD workflow), "
        "**`mode`**=`aggregates` unless per-return rows are clearly required.\n"
        f"- For year args use {year} unless another year is explicit in the question.\n"
        "- Never include ssn or ssn_last4 in args.\n"
        "- Tool names must come from the list above when **`mode`**=`need_rows`."
    )

    try:
        router_ctx = live_office_llm_context()
    except Exception as exc_ctx:
        current_app.logger.exception("Chat office context query failed")
        return jsonify(
            {
                "error": "Could not load office statistics for chat.",
                "detail": str(exc_ctx)[:240],
            }
        ), 500

    freshness_banner_md = dataplane_digest_freshness_banner(year, router_ctx)

    selection_for_router = selection_prompt + "\n\n" + freshness_banner_md
    digest_md = router_office_digest.strip() if router_office_digest.strip() else ""
    if digest_md:
        selection_for_router += (
            "### Office dataplane composite (trusted season-wide KPIs—not taxpayer identifiers)\n\n"
            + digest_md
            + "\n"
        )
    if intent == "fallback":
        selection_for_router += (
            "\n### Fallback-intent stress\n"
            "Classifier `fallback`: set **`mode`**=`aggregates` unless the text clearly names workflow "
            "statuses, a preparer, a return/client id, or unmistakable balance-rank language.\n"
        )

    degraded_router_transport = False
    try:
        tool_selection = extract_json(
            _prepend_router_system_context(selection_for_router, router_ctx),
            model=OLLAMA_ROUTER_MODEL,
            timeout=OLLAMA_CHAT_ROUTER_TIMEOUT_SEC,
        )
    except json.JSONDecodeError as je:
        current_app.logger.warning("Chat tool-router model returned invalid JSON: %s", je)
        tool_selection = normalize_chat_router_payload({"tool": None, "args": {}, "confidence": None})
    except requests.HTTPError as he:
        ollama_body = ""
        if he.response is not None:
            ollama_body = (he.response.text or "").strip()[:480]
        current_app.logger.error(
            "Ollama HTTP error during tool routing status=%s: %s",
            he.response.status_code if he.response else "?",
            ollama_body or he,
        )
        payload = {
            "error": "Ollama HTTP error during tool routing—check router model name on the inference host.",
            "ollama_base_url": OLLAMA_BASE_URL,
            "router_model": OLLAMA_ROUTER_MODEL,
            "hint": (
                "On the machine running TaxOps verify OLLAMA_BASE_URL points at the GPU host; "
                "on that host run `ollama pull " + str(OLLAMA_ROUTER_MODEL) + "` "
                "if the tag is missing."
            ),
        }
        if ollama_body:
            payload["ollama_response"] = ollama_body
        return jsonify(payload), 503
    except requests.RequestException as rexc:
        detail = str(rexc)[:320]
        low = detail.lower()
        url_s = str(OLLAMA_BASE_URL or "")
        ul = url_s.lower()
        is_loopback = (
            "localhost" in ul
            or "127.0.0.1" in ul
            or ul.startswith("http://[::1]")
        )
        ts = int(OLLAMA_CHAT_ROUTER_TIMEOUT_SEC)
        rm = str(OLLAMA_ROUTER_MODEL or "")

        if "read timed out" in low or "read time out" in low:
            if is_loopback:
                hint = (
                    "Ollama hit a read timeout on loopback. If `.env` points at a GPU host, "
                    "fix `OLLAMA_BASE_URL` in NSSM (process env overrides `.env`) and restart the service."
                )
            else:
                hint = (
                    f"Ollama at {url_s} connected but the tool-router request did not finish within {ts}s. "
                    "Typical on LAN: model cold-start, GPU busy, or a heavy router tag. "
                    f"Add to NSSM AppEnvironmentExtra (and restart TaxOps): +OLLAMA_CHAT_ROUTER_TIMEOUT=120 "
                    f"(or 180), or use a faster/smaller OLLAMA_ROUTER_MODEL. On {url_s} run once: "
                    f"`ollama run {rm}` to warm the model. Quick check from this machine: "
                    f"`curl {url_s}/api/tags` or `Invoke-WebRequest {url_s}/api/tags -TimeoutSec 10`."
                )
        elif any(
            x in low
            for x in (
                "connection refused",
                "failed to establish",
                "name or service not known",
                "getaddrinfo failed",
                "network is unreachable",
                "no route to host",
            )
        ):
            hint = (
                f"Cannot open a TCP connection to {url_s}. On the Ollama host ensure the service is running, "
                "port 11434 is allowed by firewall, and Ollama listens on the LAN (not only 127.0.0.1). "
                "From this TaxOps machine test: `Test-NetConnection 192.168.1.141 -Port 11434` (adjust IP)."
            )
        elif is_loopback:
            hint = (
                "If TaxOps should use a remote Ollama, set `OLLAMA_BASE_URL` in NSSM to that host "
                "(process env overrides `.env`) and restart TaxOpsService."
            )
        else:
            hint = (
                f"Check Ollama on {url_s}, firewall paths, and VPN. "
                f"Current tool-router read timeout is {ts}s (OLLAMA_CHAT_ROUTER_TIMEOUT)."
            )

        if CHAT_ROUTER_TRANSPORT_FAIL_AGGREGATES:
            degraded_router_transport = True
            current_app.logger.warning(
                "Chat router Ollama transport failed — continuing aggregate-first without JSON router (%s)",
                detail,
            )
            log_chat_ctx["telemetry"]["router_transport_degraded"] = True
            log_chat_ctx["telemetry"]["router_transport_hint"] = hint[:500]
            log_chat_ctx["telemetry"]["router_transport_detail"] = detail[:400]
            tool_selection = normalize_chat_router_payload(
                {"mode": "aggregates", "confidence": 1.0, "tool": None, "args": {}}
            )
        else:
            current_app.logger.error(
                "Ollama unreachable during tool routing (OLLAMA_BASE_URL=%s): %s",
                OLLAMA_BASE_URL,
                rexc,
            )
            return jsonify(
                {
                    "error": "Cannot reach Ollama for tool routing (network/connect timeout).",
                    "ollama_base_url": OLLAMA_BASE_URL,
                    "router_model": OLLAMA_ROUTER_MODEL,
                    "router_timeout_sec": ts,
                    "hint": hint,
                    "detail": detail,
                }
            ), 503
    except (KeyError, TypeError, ValueError) as oresp:
        current_app.logger.error("Unexpected Ollama response shape during routing: %s", oresp)
        return jsonify(
            {
                "error": "Ollama response could not be read for tool routing.",
                "ollama_base_url": OLLAMA_BASE_URL,
                "router_model": OLLAMA_ROUTER_MODEL,
                "detail": str(oresp)[:240],
            }
        ), 503

    if not isinstance(tool_selection, dict):
        current_app.logger.warning("LLM tool selection was not a JSON object")
        return _chat_finalize(
            {
                "answer": (
                    "I can only answer questions about returns, clients, balances, "
                    "and missing documents in TaxOps. Try asking about a specific status, preparer, or client."
                ),
                "tool_used": None,
                "args_used": None,
            }
        )


    tool_selection = normalize_chat_router_payload(tool_selection)
    rm = str(tool_selection.get("router_mode") or "aggregates")
    rc = float(tool_selection.get("router_confidence") or 0.0)
    log_chat_ctx["telemetry"]["router_mode"] = rm
    log_chat_ctx["telemetry"]["router_confidence"] = round(rc, 4)

    clarify_txt = tool_selection.get("clarify_prompt")
    cq = clarify_txt.strip() if isinstance(clarify_txt, str) else ""

    if rm == "clarify":
        answer_txt = cq or (
            "I need something more concrete—mention a workflow status name, client/search text, numeric "
            "return id, client id, preparer first name for workload, or say if you mean unpaid balances."
        )
        log_chat_ctx["telemetry"]["router_clarify_fallback"] = not bool(cq)
        return _chat_finalize({"answer": answer_txt, "tool_used": None, "args_used": None})

    if rm == "need_rows" and rc < CHAT_ROUTER_CONFIDENCE_MIN:
        log_chat_ctx["telemetry"]["confidence_abstain"] = True
        tool_selection["tool"] = None
        tool_selection["args"] = {}
        tool_selection = normalize_chat_router_payload(tool_selection)

    tool_name_raw = tool_selection.get("tool")
    tool_args_raw = tool_selection.get("args", {})
    if not isinstance(tool_args_raw, dict):
        tool_args_raw = {}

    prior_pick = ""
    if isinstance(tool_name_raw, str):
        prior_pick = tool_name_raw.strip()
    elif tool_name_raw is not None:
        prior_pick = str(tool_name_raw).strip()

    guarded_tool, tool_args_raw = apply_row_tool_grounding_guard(
        question, intent, ents, tool_name_raw, tool_args_raw
    )
    if (
        prior_pick
        and prior_pick.lower() not in ("null", "none")
        and guarded_tool is None
    ):
        log_chat_ctx["telemetry"]["row_tool_guard_dropped"] = prior_pick
        current_app.logger.info(
            "Chat: row grounding guard dropped tool=%s intent=%s",
            prior_pick,
            intent,
        )

    tool_name_eff = guarded_tool
    tn = ""
    if isinstance(tool_name_eff, str):
        tn = tool_name_eff.strip()
    elif tool_name_eff is not None:
        tn = str(tool_name_eff).strip()

    tn_lower = tn.strip().lower()
    if not tn or tn_lower in ("null", "none"):
        ctxx = router_ctx
        cq_vol = wants_tool_row_aggregate(question) and not (
            prefers_narrative_list_answer(question) or wants_qualitative_return_answer(question)
        )
        if cq_vol and intent == "count_by_status":
            sta = (ents.get("status") or _extract_status_from_question(question) or "").strip()
            nc = (
                _status_count_lookup(dict(ctxx.get("status_counts") or {}), sta)
                if sta
                else None
            )
            if sta and nc is not None:
                return _chat_finalize(
                    {
                        "answer": f"There are {nc} returns in {sta} status for {year}.",
                        "tool_used": None,
                        "args_used": None,
                        "result_count": nc,
                        "fast": True,
                    }
                )
        if cq_vol and intent == "count_by_processor":
            hint_p = (
                str(ents.get("processor") or "").strip()
                or str(_extract_processor_guess(question) or "").strip()
            )
            pmap = dict(ctxx.get("processor_return_counts") or {})
            pname, ptot = (
                _resolve_processor_match(hint_p, pmap) if hint_p and pmap else ("", 0)
            )
            if pname:
                return _chat_finalize(
                    {
                        "answer": f"{pname} has {ptot} return(s) for {year}.",
                        "tool_used": None,
                        "args_used": None,
                        "result_count": ptot,
                        "fast": True,
                    }
                )
        if cq_vol and intent == "balance_due":
            bd = int(ctxx.get("balance_due_season_total") or 0)
            return _chat_finalize(
                {
                    "answer": (
                        f"There are {bd} return(s) with an unpaid balance tracked for season "
                        f"{year} (fee billed exceeds recorded payments)."
                    ),
                    "tool_used": None,
                    "args_used": None,
                    "result_count": bd,
                    "fast": True,
                }
            )

        nl_inner = (
            "Tool router emitted null (or aggregates-only path).\n"
            f'Staff question: "{question}"\n\n'
            "Answer succinctly quoting ONLY aggregates from SYSTEM CONTEXT "
            "(status totals, preparer workload, unpaid balance tally) "
            "and especially the Expanded office dataplane composite block when present "
            "(form mix, missing-doc rollups, e-file import pipeline, extraction queue, payments, …). "
            "Never invent statuses; never cite taxpayer identifiers."
        )
        if CHAT_ANSWER_ON_ROUTER_TIMEOUT_SKIP_LLM and degraded_router_transport:
            log_chat_ctx["telemetry"]["answer_llm_skipped_router_transport"] = True
            dig_deg = null_answer_office_digest.strip()
            cap_d = _AI_CHAT_DEGRADED_SKIP_LLM_DIGEST_CAP
            if len(dig_deg) > cap_d:
                dig_deg = (
                    dig_deg[:cap_d].rstrip()
                    + "\n\n_(Office dataplane digest truncated—router timed out or was unreachable.)_"
                )
            degraded_note = (
                "**Note:** The tool-router LLM timed out or was unreachable; TaxOps skipped a second LLM "
                "pass and returned KPI aggregates below only.\n"
            )
            parts_deg = [
                degraded_note.rstrip(),
                freshness_banner_md.rstrip(),
                _format_ai_chat_system_compact(ctxx),
            ]
            if dig_deg:
                parts_deg.append(
                    "### Expanded office dataplane composite (trusted aggregates)\n\n" + dig_deg
                )
            ans_nl = "\n\n".join(p for p in parts_deg if p)
        else:
            try:
                addon_parts = [freshness_banner_md.rstrip()]
                nd = null_answer_office_digest.strip()
                if nd:
                    addon_parts.append(nd)
                brief_addon = "\n\n".join(p for p in addon_parts if p).strip()
                brief_addon = brief_addon or None
                ans_nl = chat(
                    _prepend_ai_chat_system_context(
                        nl_inner, ctxx, office_brief_addon=brief_addon
                    ),
                    model=OLLAMA_CHAT_MODEL,
                    timeout=OLLAMA_CHAT_ANSWER_TIMEOUT_SEC,
                )
            except Exception as exc_nl:
                current_app.logger.error(f"LLM answer (null-tool contextual) failed: {exc_nl}")
                ans_nl = (
                    "I only see workflow-wide totals in context—narrow the question "
                    "or rely on structured tools."
                )
        return _chat_finalize(
            {
                "answer": ans_nl.strip(),
                "tool_used": None,
                "args_used": None,
            }
        )

    tool_name = tn

    if tool_name not in CHAT_TOOL_ALLOWLIST:
        current_app.logger.warning(f"LLM requested invalid tool: {tool_name}")
        return _chat_finalize(
            {
                "answer": "I wasn't able to find the right tool to answer that question.",
                "tool_used": None,
                "args_used": None,
            }
        )

    tool_call_kw = _normalized_chat_tool_args(tool_name, tool_args_raw, year)

    if tool_name == "get_returns_by_status":
        raw_original = str(tool_call_kw.get("status") or "").strip()
        raw_st = raw_original.lower()

        canon: str | None = None
        for lab in CHAT_ALLOWED_STATUSES:
            if lab.upper() == raw_original.upper():
                canon = lab
                break

        if raw_st in ("", "none", "null", "unknown", "n/a"):
            conn_rescue = get_connection()
            try:
                rescue = try_deterministic_response(
                    conn_rescue,
                    "dataplane_slice",
                    {"slice": "form_leader"},
                    question,
                    year,
                    office_ctx_live=live_office_llm_context(),
                )
            finally:
                conn_rescue.close()
            if rescue is not None:
                current_app.logger.info(
                    "Chat: routed get_returns_by_status(empty status) -> form_leader dataplane"
                )
                return _chat_finalize(rescue)

        if canon is not None:
            tool_call_kw["status"] = canon
        elif canon is None and (
            any(ch in raw_original for ch in (",", ";", "|", "/", "&"))
            or len(raw_original) > 80
        ):
            conn_rescue = get_connection()
            try:
                rescue = try_deterministic_response(
                    conn_rescue,
                    "season_totals",
                    {},
                    question,
                    year,
                    office_ctx_live=live_office_llm_context(),
                )
            finally:
                conn_rescue.close()
            if rescue is not None:
                current_app.logger.info(
                    "Chat: get_returns_by_status invalid/multi status -> season_totals "
                    "(raw=%s)",
                    raw_original[:140],
                )
                return _chat_finalize(rescue)

    conn = get_connection()
    try:
        try:
            tool_func = getattr(db_tools, tool_name)
            result = tool_func(conn, **tool_call_kw)
        except TypeError as e:
            current_app.logger.error(f"Tool call failed {tool_name} {tool_call_kw}: {e}")
            return _chat_finalize(
                {
                    "answer": "I found the right tool but couldn't run it with those parameters.",
                    "tool_used": tool_name,
                    "args_used": tool_call_kw,
                }
            )
        except Exception as e:
            current_app.logger.error(f"Tool execution error {tool_name}: {e}")
            return _chat_finalize(
                {
                    "answer": "I ran into an error retrieving that data.",
                    "tool_used": tool_name,
                    "args_used": tool_call_kw,
                }
            )
    finally:
        conn.close()

    if isinstance(result, list):
        safe_result = [
            scrub_ssn_from_dict(r) if isinstance(r, dict) else r for r in result
        ]
    elif isinstance(result, dict):
        safe_result = scrub_ssn_from_dict(result)
    else:
        safe_result = result

    actual_count = (
        len(safe_result)
        if isinstance(safe_result, list)
        else (1 if safe_result else 0)
    )

    if isinstance(safe_result, list) and safe_result:
        year_arg_early = tool_call_kw.get("year", year)
        chrono_resp = chronological_superlative_chat_payload(
            tool_name,
            tool_call_kw,
            safe_result,
            question,
            actual_count,
            year_arg_early,
        )
        if chrono_resp is not None:
            return _chat_finalize(chrono_resp)

    if isinstance(safe_result, list) and len(safe_result) > 20:
        safe_result = safe_result[:20]
        truncated = True
    else:
        truncated = False

    result_text = str(safe_result) if safe_result else "No results found."
    truncation_note = (
        f" (showing first 20 of {actual_count} total results)"
        if truncated
        else f" ({actual_count} total results)"
    )

    year_arg = tool_call_kw.get("year", year)
    narrative_rows = use_narrative_return_rows(question)

    if isinstance(safe_result, list):
        if tool_name == "get_returns_by_status":
            sta = str(tool_call_kw.get("status") or "").strip()
            if sta and not narrative_rows:
                if sta.upper() == "REJECTED" and wants_rejection_reason_breakdown(question):
                    cx = get_connection()
                    try:
                        summ = db_tools.summarize_season_rejections_for_chat(cx, year_arg)
                        ttl = int(summ.get("total") or actual_count)
                        verb = "is" if ttl == 1 else "are"
                        subj = "return" if ttl == 1 else "returns"
                        body = format_season_rejection_breakdown_for_chat(summ, year_arg)
                        head = f"There {verb} **{ttl}** {subj} in {sta} status for {year_arg}."
                        answ = head if not (body or "").strip() else f"{head}\n\n{body}"
                        return _chat_finalize(
                            {
                                "answer": answ,
                                "tool_used": tool_name,
                                "args_used": tool_call_kw,
                                "result_count": ttl,
                                "fast": True,
                            }
                        )
                    finally:
                        cx.close()
                return _chat_finalize(
                    {
                        "answer": (
                            f"There are {actual_count} returns in {sta} status for {year_arg}."
                        ),
                        "tool_used": tool_name,
                        "args_used": tool_call_kw,
                        "result_count": actual_count,
                        "fast": True,
                    }
                )
        if tool_name == "get_returns_by_processor":
            proc = str(tool_call_kw.get("processor") or "").strip()
            if proc and not narrative_rows:
                return _chat_finalize(
                    {
                        "answer": (
                            f"{proc} has {actual_count} returns for {year_arg}."
                        ),
                        "tool_used": tool_name,
                        "args_used": tool_call_kw,
                        "result_count": actual_count,
                        "fast": True,
                    }
                )

    wants_aggregate = wants_tool_row_aggregate(question) and not use_narrative_return_rows(
        question
    )

    if wants_aggregate and isinstance(safe_result, list):
        status_arg = tool_call_kw.get("status", "")
        processor_arg = tool_call_kw.get("processor", "")

        if status_arg:
            direct_answer = (
                f"There are {actual_count} returns in {status_arg} status for {year_arg}."
            )
        elif processor_arg:
            direct_answer = (
                f"{processor_arg} has {actual_count} returns for {year_arg}."
            )
        else:
            direct_answer = f"Found {actual_count} results for {year_arg}."

        return _chat_finalize(
            {
                "answer": direct_answer,
                "tool_used": tool_name,
                "args_used": tool_call_kw,
                "result_count": actual_count,
                "fast": True,
            }
        )

    answer_prompt = (
        "You are a helpful assistant for a tax preparation office called Xcel Financial.\n"
        f'A staff member asked: "{question}"\n\n'
        f"The system returned {actual_count} result(s){truncation_note}.\n"
        f"Here is the data:\n{result_text}\n\n"
        "Answer the staff member's question in plain English. "
        f"When reporting totals always use the exact number {actual_count} — do not count rows yourself. "
        "Ground every date and client name ONLY in fields shown above "
        "(e.g. display_name, log_number, intake_date, pickup_date, logout_date, ack_date). "
        "If a date field is empty or missing, say it is not recorded — do NOT invent calendar dates. "
        "Be concise. Do not mention function names or internal tool names."
    )

    try:
        answer_text = chat(
            _prepend_ai_chat_system_context(answer_prompt, live_office_llm_context()),
            model=OLLAMA_CHAT_MODEL,
            timeout=OLLAMA_CHAT_ANSWER_TIMEOUT_SEC,
        )
    except Exception as e:
        current_app.logger.error(f"LLM answer generation failed: {e}")
        answer_text = f"Found {actual_count} result(s)."

    return _chat_finalize(
        {
            "answer": answer_text.strip(),
            "tool_used": tool_name,
            "args_used": tool_call_kw,
            "result_count": actual_count,
        }
    )


@ai.post("/return/<int:return_id>/draft-email")
@_login_required
def ai_draft_email(return_id: int):
    """Draft a plain-English rejection follow-up email for a REJECTED return.

    Reads only — no DB writes, no ssn_last4 in prompt or response.
    """
    conn = get_connection()

    # Fetch return with client join — replicates the _SELECT join pattern from app.py
    row = conn.execute(
        """
        SELECT
            r.id, r.client_status, r.tax_year,
            c.last_name, c.first_name, c.display_name
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        WHERE r.id = ?
        """,
        (return_id,),
    ).fetchone()

    if not row:
        conn.close()
        return jsonify({"error": "Return not found"}), 404

    if row["client_status"] != "REJECTED":
        conn.close()
        return jsonify({"error": "Return is not in REJECTED status"}), 400

    # Build display name the same way _enrich() does in app.py
    display_name = (
        row["display_name"]
        or (
            f"{row['last_name']}, {row['first_name']}"
            if row["first_name"]
            else row["last_name"] or ""
        )
    )
    tax_year = row["tax_year"] or ""

    # Fetch most recent rejection details from efile_batch_items
    batch_row = conn.execute(
        """
        SELECT rejection_code, rejection_reason
        FROM efile_batch_items
        WHERE return_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (return_id,),
    ).fetchone()
    conn.close()

    rejection_code   = (batch_row["rejection_code"]   or "") if batch_row else ""
    rejection_reason = (batch_row["rejection_reason"] or "") if batch_row else ""

    reason_useful = _reason_is_useful(rejection_reason)
    static        = lookup_rejection_code(rejection_code)

    if static:
        # Known code — ground the LLM in the exact explanation from the reference table.
        # rejection_reason is never passed here regardless of its value.
        prompt = (
            f"You are helping a tax office staff member draft a follow-up email to a client "
            f"whose federal tax return was rejected by the IRS.\n\n"
            f"Client name: {display_name}\n"
            f"Tax year: {tax_year}\n"
            f"Rejection code: {rejection_code}\n"
            f"What this code means: {static['explanation']}\n"
            f"What the preparer must do: {static['action']}\n\n"
            f"Write a short, professional, plain-English email to the client based on the above. "
            f"Be specific — use the actual explanation above, not vague language. "
            f"Ask the client to contact the office as soon as possible. "
            f"Do not include a subject line. Do not sign the email. "
            f"Do not mention specific dollar amounts. "
            f"3 sentences maximum."
        )
    elif reason_useful:
        # Path A — unknown code, but staff recorded a meaningful reason.
        prompt = (
            f"You are helping a tax office staff member draft a follow-up email to a client "
            f"whose federal tax return was rejected by the IRS.\n\n"
            f"Client name: {display_name}\n"
            f"Tax year: {tax_year}\n"
            f"Rejection code: {rejection_code}\n"
            f"Rejection reason: {rejection_reason}\n\n"
            f"Write a short, professional, plain-English email explaining what caused the rejection "
            f"using the reason provided above. "
            f"Ask the client to contact the office as soon as possible. "
            f"Do not include a subject line. Do not sign the email. "
            f"Do not mention specific dollar amounts. "
            f"3 sentences maximum."
        )
    else:
        # Path B — unknown code, no useful reason recorded.
        # rejection_reason is completely excluded from this prompt.
        prompt = (
            f"You are helping a tax office staff member draft a follow-up email to a client "
            f"whose federal tax return was rejected by the IRS.\n\n"
            f"Client name: {display_name}\n"
            f"Tax year: {tax_year}\n"
            f"Rejection code: {rejection_code}\n\n"
            f"Use your knowledge of IRS e-file rejection code {rejection_code} to write a short, "
            f"professional, plain-English email explaining exactly what this specific rejection code means "
            f"and what the client needs to do to fix it. "
            f"Do not say a reason was not provided. "
            f"Do not use vague language like 'an issue was found' or 'discrepancies were detected'. "
            f"Be specific about what this code actually means. "
            f"Ask the client to contact the office as soon as possible. "
            f"Do not include a subject line. Do not sign the email. "
            f"Do not mention specific dollar amounts. "
            f"3 sentences maximum."
        )

    try:
        draft = chat(prompt)
        return jsonify({"draft": draft})
    except Exception:
        return jsonify({"error": "LLM unavailable — is Ollama running?"}), 503


@ai.post("/rejection-code/lookup")
@_login_required
def ai_rejection_code_lookup():
    """Normalize a raw IRS rejection code and return a plain-English explanation.

    Checks the static reference table first — instant, no LLM needed for known codes.
    Falls back to LLM only for unrecognized codes.
    No DB reads or writes. No ssn_last4 anywhere.
    """
    data = request.get_json(force=True) or {}
    raw  = (data.get("code") or "").strip()
    if not raw:
        return jsonify({"error": "No code provided"}), 400

    # Normalize: strip spaces, uppercase, reinsert canonical hyphens by code family
    cleaned   = re.sub(r'\s+', '', raw).upper()
    bare      = cleaned.replace('-', '')
    ind_match = re.match(r'^([A-Z]{2,4})(\d+)$', bare)
    r_match   = re.match(r'^(R)(\d{4})(\d+)$', bare)
    s_match   = re.match(r'^(S)(\d{3})(\d{3})(\d*)$', bare)

    if r_match:
        normalized = f"{r_match.group(1)}{r_match.group(2)}-{r_match.group(3)}"
    elif s_match and s_match.group(4):
        normalized = f"{s_match.group(1)}{s_match.group(2)}-{s_match.group(3)}-{s_match.group(4)}"
    elif s_match:
        normalized = f"{s_match.group(1)}{s_match.group(2)}-{s_match.group(3)}"
    elif ind_match:
        normalized = f"{ind_match.group(1)}-{ind_match.group(2)}"
    else:
        normalized = bare

    # Try static reference table first — instant, no LLM needed
    static_result = lookup_rejection_code(normalized)

    if static_result:
        return jsonify({
            "normalized_code": normalized,
            "recognized":      True,
            "explanation":     static_result["explanation"],
            "action":          static_result["action"],
            "irs_reference":   static_result["irs_reference"],
            "source":          "reference",
        }), 200

    # LLM fallback for unrecognized codes
    prompt = (
        "You are an IRS e-file rejection code reference assistant for a tax preparation office.\n\n"
        f"Rejection code: {normalized}\n\n"
        "Respond in this JSON format only — no markdown, no extra text, no explanation outside the JSON:\n"
        "{\n"
        '  "recognized": true or false,\n'
        '  "explanation": "One sentence in plain English describing what caused this rejection. Staff-facing, no jargon.",\n'
        '  "action": "One sentence describing the single most important thing the preparer must do to fix it.",\n'
        '  "irs_reference": "The official IRS publication or help article most relevant to this code, or empty string if unknown."\n'
        "}\n\n"
        "Rules:\n"
        "- explanation must be one sentence only\n"
        "- action must be one sentence only\n"
        "- If you do not recognize the code, set recognized to false\n"
        "- Do not invent IRS references — use empty string if unsure\n"
        "- Do not include SSNs, EINs, or any taxpayer identification numbers"
    )

    try:
        result = extract_json(prompt)
    except json.JSONDecodeError:
        return jsonify({"error": "LLM returned malformed response — try again"}), 502
    except Exception:
        return jsonify({"error": "LLM unavailable — is Ollama running?"}), 503

    if not all(k in result for k in ("recognized", "explanation", "action")):
        return jsonify({"error": "LLM returned malformed response — try again"}), 502

    allowed = {"recognized", "explanation", "action", "irs_reference"}
    clean   = {k: v for k, v in result.items() if k in allowed}

    return jsonify({
        "normalized_code": normalized,
        "recognized":      bool(clean.get("recognized", False)),
        "explanation":     str(clean.get("explanation", "")),
        "action":          str(clean.get("action", "")),
        "irs_reference":   str(clean.get("irs_reference") or ""),
        "source":          "llm",
    }), 200


@ai.post("/documents/<int:doc_id>/extract")
@_login_required
def ai_document_extract(doc_id: int):
    """DOC-4 — Extract intake-safe fields: PDF text → llama3.2, scanned PDF / images → vision.

    Reads file_path server-side only; never returned. scrub_ssn_from_dict on all LLM output.
    """
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT id, return_id, COALESCE(doc_type, 'unknown') AS doc_type,
                   file_path, filename
            FROM return_documents
            WHERE id = ? AND is_deleted = 0
            """,
            (doc_id,),
        ).fetchone()

        if not row:
            return jsonify({"error": "Document not found"}), 400

        file_path = row["file_path"]
        return_id_doc = row["return_id"]
        doc_type_val = row["doc_type"]
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found on disk"}), 400

        doc_name = (row["filename"] or os.path.basename(file_path) or "document").strip()

        ext = os.path.splitext(file_path)[1].lower()
        if ext not in (".pdf", ".jpg", ".jpeg", ".png"):
            return jsonify({"error": f"Unsupported file type: {ext}"}), 400

        from extractor import _extract_fields

        tup = _extract_fields(file_path, doc_name)
        if not tup or tup[0] is None:
            return jsonify(
                {"error": "LLM returned malformed response — try again"}
            ), 502
        clean, extraction_method = tup[0], tup[1] or "vision"
    
        saved_to_table = None
        table_name = _detect_form_type(doc_type_val, clean)
        if table_name:
            if _save_form_data(conn, table_name, return_id_doc, doc_id, clean):
                saved_to_table = table_name
                doc_tag = _form_table_to_doc_type(table_name)
                if doc_tag != "unknown":
                    conn.execute(
                        """
                        UPDATE return_documents
                        SET doc_type = ?
                        WHERE id = ? AND return_id = ? AND is_deleted = 0
                        """,
                        (doc_tag, doc_id, return_id_doc),
                    )
                current_app.logger.info(
                    "DOC-7: saved extracted data to %s for return %s",
                    table_name,
                    return_id_doc,
                )
        conn.commit()
        return jsonify(
            {
                "fields": clean,
                "method": extraction_method or "vision",
                "saved_to": saved_to_table,
            }
        ), 200
    finally:
        conn.close()


@ai.post("/documents/<int:doc_id>/classify")
@_login_required
def ai_document_classify(doc_id: int):
    """DOC-5 — classify doc_type via _extract_fields + _detect_form_type (no extra LLM path)."""
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT id, COALESCE(doc_type, 'unknown') AS doc_type, file_path, filename
            FROM return_documents
            WHERE id = ? AND is_deleted = 0
            """,
            (doc_id,),
        ).fetchone()
        if not row:
            return jsonify({"error": "Document not found"}), 400

        fp = row["file_path"]
        if not fp or not os.path.isfile(fp):
            return jsonify({"error": "File not found on disk"}), 400

        out = _classify_document_using_row(
            conn, row, only_if_still_unknown=False
        )
        conn.commit()
        return jsonify({"doc_type": out["doc_type"], "doc_id": doc_id}), 200
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        current_app.logger.error("Classify route error for doc %s: %s", doc_id, e)
        return jsonify({"doc_type": "unknown", "doc_id": doc_id}), 200
    finally:
        conn.close()

