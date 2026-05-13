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
    CHAT_TRAINING_LOG_CACHE_HITS,
    CHAT_TRAINING_LOG_ENABLE,
    CHAT_TRAINING_LOG_PATH,
    OLLAMA_BASE_URL,
    OLLAMA_CHAT_ANSWER_TIMEOUT_SEC,
    OLLAMA_CHAT_MODEL,
    OLLAMA_EXTRACT_MODEL_TEXT,
    OLLAMA_EXTRACT_MODEL_VISION,
    OLLAMA_MODEL,
)
from db import get_connection
from db_tools import lookup_rejection_code
from form_schema import FORM_INTEGER_COLUMNS, FORM_TABLE_INSERT_COLUMNS
from llm import chat, extract_json
from utils import normalize_staff_question_key, now, scrub_ssn_from_dict
from chat_cache import (
    CHAT_ALLOWED_STATUSES,
    ensure_chat_cache_for_year,
    ensure_structured_cache_for_year,
    get_all_processor_counts,
    get_all_status_counts,
    get_cached_answer,
    get_cache_age_seconds,
    get_cache_year,
    get_data_plane_text,
    get_financial_stats,
    get_returns_for_status,
    get_status_count,
    is_cache_warm,
    set_cached_answer,
    snapshot_office_brief_digest,
)
from chat_training_log import append_staff_ai_chat_question

ai = Blueprint("ai", __name__, url_prefix="/ai")

# Substring phrases (lowercase) → canonical workflow label for structured-chat cache lookups.
# Longest phrases first via sort — see _STRUCTURED_TRY_CACHE_STATUS_PHRASES.
_STRUCTURED_TRY_CACHE_STATUS_PHRASES_RAW: tuple[tuple[str, str], ...] = (
    ("e-file ready", "EFILE READY"),
    ("efile ready", "EFILE READY"),
    ("picked up", "LOG OUT"),
    ("checked out", "LOG OUT"),
    ("logged out", "LOG OUT"),
    ("logging out", "LOG OUT"),
    ("logout", "LOG OUT"),
    ("log out", "LOG OUT"),
    ("finalize", "FINALIZE"),
    ("finaliz", "FINALIZE"),
    ("pick up", "PICKUP"),
    ("processing", "PROCESSING"),
    ("efile", "EFILE READY"),
    ("e-file", "EFILE READY"),
    ("pickup", "PICKUP"),
    ("cancelled", "CANCELLED"),
    ("canceled", "CANCELLED"),
    ("rejection", "REJECTED"),
    ("rejected", "REJECTED"),
    ("hold", "HOLD"),
)
_STRUCTURED_TRY_CACHE_STATUS_PHRASES: tuple[tuple[str, str], ...] = tuple(
    sorted(set(_STRUCTURED_TRY_CACHE_STATUS_PHRASES_RAW), key=lambda x: (-len(x[0]), x[0]))
)


def _extract_chat_status_phrase(question: str) -> str | None:
    """Match workflow status only as a whole phrase (prevents hidden substring matches)."""
    for st in sorted(CHAT_ALLOWED_STATUSES, key=len, reverse=True):
        parts = st.split()
        if len(parts) == 1:
            pat = r"\b" + re.escape(parts[0]) + r"\b"
        else:
            pat = r"\b" + r"\s+".join(re.escape(p) for p in parts) + r"\b"
        if re.search(pat, question, flags=re.I):
            return st
    return None


def _canonical_status_from_structured_try_cache_question(ql: str, question: str) -> str | None:
    """Map natural wording to workflow status label for structured cache fast paths."""
    for phrase, canonical in _STRUCTURED_TRY_CACHE_STATUS_PHRASES:
        if phrase in ql:
            return canonical
    return _extract_chat_status_phrase(question)


CHAT_TOOLS = [
    {
        "name": "get_returns_by_status",
        "description": (
            "Get returns with a specific workflow status. "
            "Use for PROCESSING, HOLD, FINALIZE, PICKUP, EFILE READY, LOG OUT, REJECTED, or CANCELLED. "
            "LOG OUT means the client/folder has picked up or been checked out (completed in-office logout). "
            "Examples: 'how many in PROCESSING', 'show me HOLD returns', 'who was logged out last', "
            "'last person to pick up their return'. "
            "Never use invented statuses such as UNPAID or EFILER for workflow queue. "
            "For unpaid fee balances use get_balance_due_returns instead. "
            "For clerical logout / finished-and-picked-up use **LOG OUT**, not **EFILE READY** (transmit queue)."
        ),
        "args": {
            "status": (
                "one of: PROCESSING, HOLD, FINALIZE, PICKUP, "
                "EFILE READY, LOG OUT, REJECTED, CANCELLED"
            ),
            "year": "4-digit tax year integer e.g. 2026",
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

# KPI / dataplane prompt caps (staff names come from `returns.processor` aggregates only).
_AI_CHAT_PREP_ROLLUP_CAP = 240
_AI_CHAT_DATAPLANE_JSON_COMPACT_CAP = 1100
_AI_CHAT_DATAPLANE_MARKDOWN_CAP = 11800


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
    body: str,
    ctx: dict,
    *,
    office_brief_addon: str | None = None,
) -> str:
    head = _format_ai_chat_system_context(ctx).rstrip()
    if office_brief_addon:
        head += (
            "\n\n### Expanded office dataplane composite (trusted aggregates)\n\n"
            + office_brief_addon.strip()
            + "\n"
        )
    return head + "\n\n---\n\n" + body


_SIMPLE_OFF_TOPIC_RE = re.compile(
    r"\b(?:weather|forecast|temperature|recipe|recipes|cook(?:ing)?|sport|NBA|NFL|MLB|scores?|NASDAQ|NYSE|"
    r"bitcoin|ethereum|crypto(?:currency)?|\bjoke\b|\bmovies?\b)\b",
    re.I,
)

_NEEDS_LOOKUP_LINE = re.compile(
    r"(?im)^\s*NEEDS_LOOKUP:\s*([a-zA-Z0-9_]+)\s*:\s*(.*?)\s*$",
)

_LOOKUP_TOOL_NAMES = frozenset(
    {
        "search_clients",
        "get_missing_docs",
        "get_returns_by_status",
        "get_returns_by_processor",
        "get_client_returns",
        "get_balance_due_returns",
    }
)


def _simple_off_topic_gate(question: str) -> bool:
    q = question.strip()
    return bool(q and _SIMPLE_OFF_TOPIC_RE.search(q))


def _is_chat_countish_question(question: str) -> bool:
    ql = question.strip().lower()
    if any(kw in ql for kw in ("how many", "total number", "number of")):
        return True
    return bool(re.search(r"\bcount\b", ql))


def _chat_wants_volume_aggregate(question: str) -> bool:
    if _is_chat_countish_question(question):
        return True
    ql = question.strip().lower()
    return bool(
        re.search(
            r"\breturns?\b.{1,48}\b(do\s+we\s+have|have\s+we|we\s+have|we'?ve\s+got|got)\b"
            r"|\b(do\s+we\s+have|have\s+we|we\s+have|how\s+much\b).{1,52}\breturns?\b",
            ql,
        )
    )


def _first_needs_lookup(answer: str) -> tuple[str, str] | None:
    m = _NEEDS_LOOKUP_LINE.search(answer or "")
    if not m:
        return None
    return m.group(1).strip(), m.group(2).strip()


def _run_needs_lookup_tool(conn, tool: str, arg: str, year: int) -> object:
    tn = tool.strip().lower()
    if tn == "search_clients":
        query = arg.strip().strip("\"'")
        if not query:
            return []
        return db_tools.search_clients(conn, query)
    if tn == "get_missing_docs":
        rid = int(str(arg).strip())
        return db_tools.get_missing_docs(conn, rid)
    if tn == "get_returns_by_status":
        raw_u = arg.strip().upper().replace("-", " ")
        status = None
        for lab in CHAT_ALLOWED_STATUSES:
            if lab.upper().replace(" ", "") == raw_u.replace(" ", ""):
                status = lab
                break
            if lab.upper() == raw_u:
                status = lab
                break
        if not status:
            return {"error": f"Unknown workflow status {arg!r}"}
        return db_tools.get_returns_by_status(conn, status, year)
    if tn == "get_returns_by_processor":
        proc = arg.strip().strip("\"'")
        if not proc:
            return []
        return db_tools.get_returns_by_processor(conn, proc, year)
    if tn == "get_client_returns":
        cid = int(str(arg).strip())
        return db_tools.get_client_returns(conn, cid)
    if tn == "get_balance_due_returns":
        return db_tools.get_balance_due_returns(conn, year)
    return {"error": f"Unsupported NEEDS_LOOKUP tool {tool!r}"}


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


def _apply_extraction_doc_tag(
    conn,
    *,
    doc_id: int,
    return_id: int,
    doc_tag: str,
) -> int:
    """After a successful typed form save — set doc_type only when unset or unknown. Returns rows updated."""
    if doc_tag == "unknown":
        return 0
    cur = conn.execute(
        """
        UPDATE return_documents
        SET doc_type = ?
        WHERE id = ? AND return_id = ? AND is_deleted = 0
          AND (
               doc_type IS NULL
               OR TRIM(doc_type) = ''
               OR LOWER(TRIM(doc_type)) = 'unknown'
          )
        """,
        (doc_tag, doc_id, return_id),
    )
    return int(cur.rowcount or 0)


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

    FORMS-3: INSERT uses only FORM_TABLE_INSERT_COLUMNS (canonical IRS box_* names).
    Legacy mirrored DDL columns on DOC-7 form tables remain NULL for newly extracted rows.
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
    Also reports trained email classifier (sklearn) disk + training counters.
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
            "answer_model": OLLAMA_CHAT_MODEL,
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

    try:
        from chat_cache import (
            get_all_processor_counts,
            get_all_status_counts,
            get_cache_age_seconds,
            get_cache_year,
            get_financial_stats,
            is_cache_warm,
        )

        response["cache"] = {
            "warm": is_cache_warm(),
            "age_seconds": (
                round(get_cache_age_seconds()) if is_cache_warm() else None
            ),
            "year": get_cache_year(),
            "status_counts": get_all_status_counts(),
            "processor_counts": get_all_processor_counts(),
            "financial_stats": get_financial_stats(),
        }
    except Exception:
        response["cache"] = {"warm": False}

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
    """Staff chat: structured cache fast paths, KPI + dataplane LLM, optional NEEDS_LOOKUP second pass."""
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


def _fin_struct_cache_payload(answer: str, stat_key: str | None, year: int, age: float) -> dict:
    au: dict = {"year": year}
    if stat_key:
        au["stat"] = stat_key
    return {
        "answer": answer,
        "tool_used": "cache",
        "args_used": au,
        "result_count": 1,
        "source": "cache",
        "cache_age_seconds": int(age),
        "fast": True,
    }


def _try_cache_response(question: str, year: int) -> dict | None:
    """
    Fast-path answers from CHAT-1 structured indexes (no LLM, no sqlite round-trip beyond cache build).
    Never includes client identification beyond display names elsewhere in chat layers.
    """
    from chat_cache import (
        get_cache_age_seconds,
        get_cache_year,
        get_financial_stats,
        get_returns_for_status,
        get_status_count,
        is_cache_warm,
    )

    if not is_cache_warm() or get_cache_year() != int(year):
        return None

    age = round(get_cache_age_seconds())
    ql = question.strip().lower()
    stats = get_financial_stats()

    st = _canonical_status_from_structured_try_cache_question(ql, question)
    chrono_q = ("last" in ql) or ("most recent" in ql) or ("latest" in ql)
    list_triggers = (
        "who",
        "show me",
        "list",
        "which returns",
        "what returns",
    )
    is_list_q = any(t in ql for t in list_triggers)

    def _struct_cache_payload(answer: str, args_used: dict, result_count: int) -> dict:
        return {
            "answer": answer,
            "tool_used": "cache",
            "args_used": args_used,
            "result_count": result_count,
            "source": "cache",
            "cache_age_seconds": age,
            "fast": True,
        }

    def _recency_sort_returns(retlist: list[dict]) -> list[dict]:
        def _rk(r: dict) -> tuple:
            primary = str(r.get("logout_date") or "").strip()
            primary = primary or str(r.get("intake_date") or "").strip()
            try:
                ln = int(float(str(r.get("log_number") or "").strip() or "0"))
            except (ValueError, TypeError):
                ln = 0
            rid = int(r.get("id") or 0)
            return (primary, ln, rid)

        return sorted(retlist, key=_rk, reverse=True)

    if chrono_q and st:
        returns = get_returns_for_status(st)
        if not returns:
            return _struct_cache_payload(
                f"No returns in {st} status for {year}.",
                {"status": st, "year": year},
                0,
            )
        sorted_r = _recency_sort_returns(returns)
        latest = sorted_r[0]
        name = str(latest.get("display_name") or "Unknown").strip() or "Unknown"
        logout_d = str(latest.get("logout_date") or "").strip()
        intake_d = str(latest.get("intake_date") or "").strip()
        date_disp = logout_d if logout_d else (intake_d if intake_d else "unknown date")
        processor = str(latest.get("processor") or "unknown preparer").strip() or "unknown preparer"
        return _struct_cache_payload(
            f"The most recent return in {st} status is {name}, processed by "
            f"{processor} on {date_disp}.",
            {"status": st, "year": year},
            1,
        )

    if is_list_q and st:
        returns = get_returns_for_status(st)
        if not returns:
            return _struct_cache_payload(
                f"There are no returns in {st} status for {year}.",
                {"status": st, "year": year},
                0,
            )
        top = _recency_sort_returns(returns)[:10]
        names = [str(r["display_name"]).strip() for r in top if r.get("display_name")]
        count = len(returns)
        name_list = ", ".join(names[:5])
        more = f" and {count - 5} more" if count > 5 else ""
        suffix = f" Clients include: {name_list}{more}." if name_list else ""
        return _struct_cache_payload(
            f"There are {count} returns in {st} status for {year}.{suffix}",
            {"status": st, "year": year},
            count,
        )

    countish = _is_chat_countish_question(question) or _chat_wants_volume_aggregate(question)

    if countish:
        stc = st
        if not stc:
            if "efile ready" in ql.replace("-", " ") or "e file ready" in ql:
                stc = "EFILE READY"
            elif "how many" in ql and "log out" in ql.replace("-", ""):
                stc = "LOG OUT"
        if stc:
            n = get_status_count(stc)
            return _struct_cache_payload(
                f"There are {n} returns in {stc} status for {year}.",
                {"status": stc, "year": year},
                n,
            )

    if stats:
        if (
            any(
                w in ql
                for w in (
                    "balance due",
                    "with a balance due",
                    "with balance due",
                    "outstanding balance",
                )
            )
            and ("how many" in ql or "number" in ql or "count" in ql or "total" in ql)
        ):
            c = int(stats.get("balance_due_count", 0))
            return {
                "answer": f"There are {c} returns with an outstanding balance for {year}.",
                "tool_used": "cache",
                "args_used": {"year": year},
                "result_count": c,
                "source": "cache",
                "cache_age_seconds": age,
                "fast": True,
            }

        if ("how much have we collected" in ql or "total collected" in ql) or (
            "collected" in ql
            and (
                "how much" in ql
                or "have we" in ql
                or "total" in ql
            )
        ):
            v = float(stats.get("total_collected", 0))
            return _fin_struct_cache_payload(
                f"The total collected for {year} is ${v:,.2f}.",
                "total_collected",
                year,
                age,
            )

        if "total billed" in ql or (
            "billed" in ql and ("how much" in ql or "what" in ql and "total" in ql)
        ):
            v = float(stats.get("total_billed", 0))
            return _fin_struct_cache_payload(
                f"The total billed for {year} is ${v:,.2f}.",
                "total_billed",
                year,
                age,
            )

        if "total outstanding" in ql or ("outstanding" in ql and "total" in ql):
            v = float(stats.get("total_outstanding", 0))
            return _fin_struct_cache_payload(
                f"The total outstanding for {year} is ${v:,.2f}.",
                "total_outstanding",
                year,
                age,
            )

    return None


def _ai_chat_submit():
    data = request.get_json(silent=True) or {}
    question = (data.get("question") or "").strip()
    year_raw = data.get("year", 2025)

    if not question:
        return jsonify({"error": "No question provided"}), 400

    try:
        year = int(year_raw)
    except (TypeError, ValueError):
        year = 2025

    nq = normalize_staff_question_key(question)
    hit = get_cached_answer(nq, year)
    if hit:
        append_staff_ai_chat_question(
            question=question,
            normalized=nq,
            year=year,
            response_payload=dict(hit),
            classified_intent="answer_cache_ttl",
            scope_classifier_blocked=False,
            from_answer_cache_hit=True,
            log_enable=CHAT_TRAINING_LOG_ENABLE,
            log_include_cache_hits=CHAT_TRAINING_LOG_CACHE_HITS,
            log_path=CHAT_TRAINING_LOG_PATH,
            telemetry={},
        )
        return jsonify(hit), 200

    ensure_chat_cache_for_year(year)
    ensure_structured_cache_for_year(year)

    log_ctx: dict = {}

    def _finalize(intent_tag: str, payload: dict) -> tuple:
        body = dict(payload)
        scope_excl = body.get("source") == "scope_gate"
        if (
            not scope_excl
            and body.get("answer") is not None
            and not body.get("error")
            and isinstance(body.get("answer"), str)
            and body["answer"].strip()
        ):
            set_cached_answer(nq, year, body)
        append_staff_ai_chat_question(
            question=question,
            normalized=nq,
            year=year,
            response_payload=body,
            classified_intent=intent_tag,
            scope_classifier_blocked=False,
            from_answer_cache_hit=bool(body.get("cached")),
            log_enable=CHAT_TRAINING_LOG_ENABLE,
            log_include_cache_hits=CHAT_TRAINING_LOG_CACHE_HITS,
            log_path=CHAT_TRAINING_LOG_PATH,
            telemetry=dict(log_ctx),
        )
        return jsonify(body), 200

    struct_early = _try_cache_response(question, year)
    if struct_early is not None:
        return _finalize("structured_cache_hit", dict(struct_early))

    if _simple_off_topic_gate(question):
        log_ctx.clear()
        return _finalize(
            "scope_gate_regex",
            {
                "answer": (
                    "I can only answer questions about returns, clients, balances, "
                    "and preparers in TaxOps."
                ),
                "tool_used": None,
                "args_used": None,
                "result_count": 0,
                "source": "scope_gate",
            },
        )

    conn_ctx = get_connection()
    try:
        live_ctx = db_tools.get_system_context(conn_ctx, year)
    except Exception as exc_ctx:
        current_app.logger.exception("Chat office context query failed")
        return jsonify(
            {
                "error": "Could not load office statistics for chat.",
                "detail": str(exc_ctx)[:240],
            }
        ), 500
    finally:
        conn_ctx.close()

    digest = snapshot_office_brief_digest(year, max_chars=_AI_CHAT_DATAPLANE_MARKDOWN_CAP).strip()
    dp_plain = (get_data_plane_text() or "").strip()
    dataplane_md = dp_plain if dp_plain else digest
    td_iso = str(live_ctx.get("today_local_iso") or "").strip()
    freshness = (
        f"_Dataplane snapshot for season **{year}** "
        f"(server local date **{td_iso or '?'}**) — KPIs omit taxpayer identifiers unless "
        "a NEEDS_LOOKUP tool fetched explicit rows._"
    )

    nl_allowed = "\n".join(f"- NEEDS_LOOKUP:{n}:<argument>" for n in sorted(_LOOKUP_TOOL_NAMES))

    instruct = (
        "You help Xcel TaxOps staff interpret office workflow data.\n"
        f'- Staff question (verbatim): "{question}"\n'
        f"- Season year selector: **{year}**\n\n"
        "Ground every factual claim ONLY in SYSTEM CONTEXT KPI block and dataplane markdown below. "
        "Do NOT invent statuses, balances, counts, dates, preparer totals, client names, or return IDs.\n\n"
        "Prefer office-wide KPI aggregates from the KPI block whenever they fully answer.\n\n"
        "When KPIs/dataplane are insufficient and structured DB rows are required, emit **exactly one** "
        "`NEEDS_LOOKUP:tool_name:argument` line somewhere in your reply (whole-line match):\n"
        "- tool_name must be one of: "
        + ", ".join(sorted(_LOOKUP_TOOL_NAMES))
        + ".\n"
        "- Leave argument empty **only** for `get_balance_due_returns`; otherwise supply a concise "
        "argument (client name substring, numeric return id, workflow status literal, processor name substring).\n"
        "Examples:\n"
        + nl_allowed
        + "\n\n"
        "Otherwise reply in Markdown. Do NOT paste this instructions block verbatim."
    )

    first_body = freshness + "\n\n" + instruct
    dataplane_blob = dataplane_md.strip() + ("\n\n" if dataplane_md.strip() else "")
    sys_ctx = (
        first_body
        + "\n\n### Dataplane snapshot (trusted aggregates)\n\n"
        + dataplane_blob
    )

    try:
        ans1 = chat(
            _prepend_ai_chat_system_context(sys_ctx.strip(), live_ctx),
            model=OLLAMA_CHAT_MODEL,
            timeout=OLLAMA_CHAT_ANSWER_TIMEOUT_SEC,
        ).strip()
    except Exception as exc1:
        current_app.logger.warning("Primary chat LLM failed: %s", exc1)
        log_ctx.clear()
        log_ctx["llm_primary_error"] = True
        return _finalize(
            "llm_primary_error",
            {
                "answer": (
                    "I could not reach the chat model — check OLLAMA_BASE_URL "
                    "and that OLLAMA_CHAT_MODEL is pulled on that host."
                ),
                "tool_used": None,
                "args_used": None,
                "detail": str(exc1)[:240],
            },
        )

    lk = _first_needs_lookup(ans1)
    if lk is None:
        log_ctx.clear()
        out = {"answer": ans1, "tool_used": None, "args_used": None, "result_count": 0}
        return _finalize("llm_primary", out)

    tool_key_raw, raw_arg = lk
    tn_key = tool_key_raw.strip().lower()
    log_ctx.clear()
    log_ctx["needs_lookup_followup"] = True
    log_ctx["needs_lookup_tool"] = tn_key

    conn_tool = get_connection()
    try:
        try:
            raw_result = _run_needs_lookup_tool(conn_tool, tn_key, raw_arg, year)
        except Exception:
            current_app.logger.exception("NEEDS_LOOKUP tool exec failed (%s)", tn_key)
            raw_result = {"error": "Tool execution failed — see server logs."}

        if isinstance(raw_result, list):
            safe_result = [
                scrub_ssn_from_dict(r) if isinstance(r, dict) else r for r in raw_result
            ]
        elif isinstance(raw_result, dict):
            safe_result = scrub_ssn_from_dict(raw_result)
        else:
            safe_result = raw_result

        preview = json.loads(json.dumps(safe_result, default=str))

        if isinstance(safe_result, list):
            cnt = len(safe_result)
        elif isinstance(safe_result, dict) and safe_result.get("error"):
            cnt = 0
        elif isinstance(safe_result, dict):
            cnt = 1
        else:
            cnt = 0 if safe_result is None else 1

        second_body = (
            "First-pass reply (verbatim):\n"
            + ans1
            + "\n\n### Tool result (privacy-scrubbed JSON)\n```json\n"
            + json.dumps(preview, ensure_ascii=False, default=str)[:12000]
            + "\n```\n\n"
            + "Produce the final Markdown reply for staff. Quote facts only from KPI context and this JSON "
            "(no hallucinated identifiers). Omit internal tool jargon unless briefly helpful."
        )

        ans2 = chat(
            _prepend_ai_chat_system_context(second_body.strip(), live_ctx),
            model=OLLAMA_CHAT_MODEL,
            timeout=OLLAMA_CHAT_ANSWER_TIMEOUT_SEC,
        ).strip()
    finally:
        conn_tool.close()

    return _finalize(
        f"lookup_{tn_key}",
        {
            "answer": ans2,
            "tool_used": tn_key,
            "args_used": raw_arg.strip() if isinstance(raw_arg, str) else raw_arg,
            "result_count": cnt,
            "needs_lookup_followup": True,
        },
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
                    _apply_extraction_doc_tag(
                        conn, doc_id=doc_id, return_id=return_id_doc, doc_tag=doc_tag
                    )
                current_app.logger.info(
                    "DOC-7: saved extracted data to %s for return %s",
                    table_name,
                    return_id_doc,
                )
        conn.commit()
        return jsonify(
            scrub_ssn_from_dict(
                {
                    "fields": clean,
                    "method": extraction_method or "vision",
                    "saved_to": saved_to_table,
                }
            )
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

