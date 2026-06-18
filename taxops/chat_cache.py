"""
Structured in-memory indexes + dataplane markdown snapshot for POST /ai/chat.

Rebuild on a 5‑minute cadence (`start_cache_worker`). Optional TTL + SQLite persistence
for repeat answers (`get_cached_answer` / `set_cached_answer`).

Privacy: cached dict rows pass through scrub_ssn_from_dict(); never cache SSN/TIN fields.
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from config import AI_CHAT_DISK_CACHE_ENABLE, AI_CHAT_DISK_CACHE_HOURS
from db import get_connection
from db_tools import gather_season_dataplane
from utils import now, parse_iso_datetime, scrub_ssn_from_dict

# Duplicated from db_tools (avoid importing private _SEASON_CLAUSE).
_SEASON_CLAUSE = (
    "(strftime('%Y', r.intake_date) = ? OR "
    "(r.intake_date IS NULL AND r.tax_year = ?))"
)

logger = logging.getLogger(__name__)

DISPLAY_NAME_SQL = (
    "COALESCE(c.display_name, c.last_name || CASE WHEN c.first_name IS NOT NULL AND "
    "c.first_name != '' THEN ', ' || c.first_name ELSE '' END) AS display_name"
)

# Must match CHAT_TOOLS / staff workflow literals where referenced elsewhere.
CHAT_ALLOWED_STATUSES: tuple[str, ...] = (
    "EFILE READY",
    "PROCESSING",
    "PICKUP",
    "FINALIZE",
    "HOLD",
    "LOG OUT",
    "REJECTED",
    "CANCELLED",
)

# ---------------------------------------------------------------------------
# Legacy snapshot keyed by intake season (`refresh_chat_cache`) — feeds dataplane text
# ---------------------------------------------------------------------------
_snapshot: dict = {}
_cache_lock = threading.Lock()
_lazy_init_year: int | None = None

_data_plane_text: str = ""

# ---------------------------------------------------------------------------
# CHAT‑1 structured in-memory indexes
# ---------------------------------------------------------------------------
_STATUS_COUNTS: dict[str, int] = {}
_RETURNS_BY_STATUS: dict[str, list[dict]] = {}
_PROCESSOR_COUNTS: dict[str, int] = {}
_RETURNS_BY_PROCESSOR: dict[str, list[dict]] = {}
_FINANCIAL_STATS: dict[str, float | int] = {}
_BALANCE_DUE_IDS: set[int] = set()
_cache_built_at: float = 0.0
_cache_year: int = 0
_structured_index_lock = threading.Lock()
_structured_lazy_year: int | None = None

# ---------------------------------------------------------------------------
# In-process + optional SQLite answer cache
# ---------------------------------------------------------------------------
ANSWER_CACHE: dict[str, tuple[dict, float]] = {}
ANSWER_CACHE_TTL_SEC = 300
_CHAT_ANSWER_ROUTE_VERSION = "v17"


def answer_cache_payload_key(norm_q: str, year: int) -> str:
    return hashlib.md5(f"{_CHAT_ANSWER_ROUTE_VERSION}:{norm_q}|{year}".encode()).hexdigest()


# pytest can register shared :memory: connections here so disk helpers skip close().
_DISK_CACHE_CONN_SKIP_CLOSE_IDS: set[int] = set()


def _disk_close_conn(conn: sqlite3.Connection | None) -> None:
    if conn is None:
        return
    if id(conn) in _DISK_CACHE_CONN_SKIP_CLOSE_IDS:
        return
    conn.close()


def _disk_cache_active() -> bool:
    return AI_CHAT_DISK_CACHE_ENABLE and AI_CHAT_DISK_CACHE_HOURS > 0


def _scrub_persistent_payload(payload: dict) -> dict:
    return scrub_ssn_from_dict(
        {k: v for k, v in payload.items() if k not in ("cached", "persistent_hit")}
    )


def _persistable_disk_payload(payload: dict) -> bool:
    if not isinstance(payload, dict) or payload.get("error"):
        return False
    ans = payload.get("answer")
    if not isinstance(ans, str) or not ans.strip():
        return False
    t = ans.strip()
    if t.startswith("I can only answer questions about returns"):
        return False
    if t.startswith("I wasn't able to find the right tool"):
        return False
    return True


def _disk_expires_at_iso() -> str:
    hours = float(AI_CHAT_DISK_CACHE_HOURS)
    ts = datetime.now(timezone.utc) + timedelta(hours=hours)
    return ts.replace(microsecond=0).isoformat(timespec="seconds")


def _get_disk_cached_answer(cache_key: str) -> dict | None:
    if not _disk_cache_active():
        return None
    try:
        conn = get_connection()
    except Exception:
        logger.warning("Disk chat cache: could not open DB", exc_info=True)
        return None
    try:
        row = conn.execute(
            "SELECT payload_json, expires_at FROM ai_chat_common_answers "
            "WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if not row:
            return None
        exp = parse_iso_datetime(row["expires_at"])
        if exp is None:
            conn.execute(
                "DELETE FROM ai_chat_common_answers WHERE cache_key = ?",
                (cache_key,),
            )
            conn.commit()
            return None
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        if exp < datetime.now(timezone.utc):
            conn.execute(
                "DELETE FROM ai_chat_common_answers WHERE cache_key = ?",
                (cache_key,),
            )
            conn.commit()
            return None
        conn.execute(
            "UPDATE ai_chat_common_answers SET hit_count = hit_count + 1 "
            "WHERE cache_key = ?",
            (cache_key,),
        )
        conn.commit()
        try:
            row_payload = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError):
            return None
        if not isinstance(row_payload, dict):
            return None
        sto = _scrub_persistent_payload(row_payload)
        out = dict(sto)
        out["cached"] = True
        out["persistent_hit"] = True
        return out
    except Exception:
        logger.warning("Disk chat cache read failed", exc_info=True)
        return None
    finally:
        _disk_close_conn(conn)


def _save_disk_cached_answer(norm_q: str, year: int, payload: dict, cache_key: str) -> None:
    if not _disk_cache_active() or not _persistable_disk_payload(payload):
        return
    sto = _scrub_persistent_payload(payload)
    answer_text = sto.get("answer")
    answer_str = answer_text.strip() if isinstance(answer_text, str) else ""
    if not answer_str:
        return
    tool_used = sto.get("tool_used")
    tool_str = tool_used if isinstance(tool_used, str) else (
        None if tool_used is None else str(tool_used)
    )
    try:
        payload_json = json.dumps(sto, ensure_ascii=True)
    except (TypeError, ValueError):
        return
    created = now()
    expires = _disk_expires_at_iso()
    conn = None
    try:
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO ai_chat_common_answers (
              cache_key, normalized_question, season_year, answer, tool_used,
              payload_json, created_at, expires_at, hit_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(cache_key) DO UPDATE SET
              answer = excluded.answer,
              tool_used = excluded.tool_used,
              payload_json = excluded.payload_json,
              expires_at = excluded.expires_at
            """,
            (
                cache_key,
                norm_q,
                int(year),
                answer_str,
                tool_str,
                payload_json,
                created,
                expires,
            ),
        )
        conn.commit()
    except Exception:
        logger.warning("Disk chat cache write failed", exc_info=True)
    finally:
        if conn is not None:
            _disk_close_conn(conn)


def get_cached_answer(norm_q: str, year: int) -> dict | None:
    key = answer_cache_payload_key(norm_q, year)
    tnow = time.time()
    entry = ANSWER_CACHE.get(key)
    if entry:
        payload, ts = entry
        if tnow - ts < ANSWER_CACHE_TTL_SEC:
            out = dict(payload)
            out["cached"] = True
            return out
        del ANSWER_CACHE[key]

    disk = _get_disk_cached_answer(key)
    if disk is not None:
        sto = {k: v for k, v in disk.items() if k not in ("cached", "persistent_hit")}
        ANSWER_CACHE[key] = (_scrub_persistent_payload(sto), time.time())
        return disk
    return None


def set_cached_answer(norm_q: str, year: int, payload: dict) -> None:
    key = answer_cache_payload_key(norm_q, year)
    sto = {k: v for k, v in payload.items() if k not in ("cached", "persistent_hit")}
    sto = scrub_ssn_from_dict(sto)
    ANSWER_CACHE[key] = (sto, time.time())
    _save_disk_cached_answer(norm_q, year, sto, key)


# ---------------------------------------------------------------------------
# Dataplane markdown (12‑section composite)
# ---------------------------------------------------------------------------
def _fmt_form_leader_section(dp: dict, year: int) -> str:
    fm = (dp.get("forms") or {}) if isinstance(dp, dict) else {}
    total = int(fm.get("returns_in_season") or 0)
    if total <= 0:
        return f"_No intake-season `{year}` returns found for form profiling._"

    nop = int(fm.get("no_form_profile_row") or 0)
    mapping = (
        ("Form 1040 / package indicator (`return_forms.form_1040`)", int(fm.get("ct_1040") or 0)),
        ("Schedules A/D", int(fm.get("ct_sched_ad") or 0)),
        ("Schedule C", int(fm.get("ct_sched_c") or 0)),
        ("Schedule E", int(fm.get("ct_sched_e") or 0)),
        ("Form 1120 corporate", int(fm.get("ct_1120") or 0)),
        ("Form 1120-S", int(fm.get("ct_1120s") or 0)),
        ("Form 1065 / LLC partnerships", int(fm.get("ct_1065") or 0)),
        ("Forms 990/1041 flagged", int(fm.get("ct_9901041") or 0)),
        ("Corp officer workflow", int(fm.get("ct_corp_officer") or 0)),
        ("Business owner workflow", int(fm.get("ct_business_owner") or 0)),
    )
    non_zero = [(lbl, n) for lbl, n in mapping if n > 0]
    non_zero.sort(key=lambda x: -x[1])
    lead_n = non_zero[0][1] if non_zero else 0
    leaders = [lbl for lbl, n in non_zero if n == lead_n]

    breakdown = (
        "\n".join(f"- **{lbl}**: {cnt}" for lbl, cnt in non_zero[:12])
        or "_No `return_forms` booleans flagged yet — every row saved as zero/absent._"
    )
    tops = ", ".join(f"**{x}**" for x in leaders) if leaders else "_(none flagged)_"

    tie_note = ""
    if len(leaders) > 1:
        tie_note = f" (**{lead_n}** returns each)"

    hdr = (
        f"Season **{year}** has **{total}** return(s) in-scope; **{nop}** still lack "
        "`return_forms` linkage."
        f"\nLargest flagged **return_forms** bucket: {tops} with **{lead_n}** return(s)."
        f"{tie_note}"
    )

    detail = "**Per-flag counts (overlap allowed):**\n" + breakdown

    caveat = (
        "\n_Use `return_forms` booleans—not final IRS filings; multiple flags can coexist on "
        "one return._"
    )
    return hdr + "\n" + detail + caveat


def _fmt_histogram_section(title: str, rows: list, label_k: str, val_k: str, limit: int = 14) -> str:
    out = []
    if not isinstance(rows, list):
        rows = []
    trimmed = rows[:limit]
    for row in trimmed:
        if not isinstance(row, dict):
            continue
        lbl = str(row.get(label_k) or "(blank)")[:240]
        n = int(row.get(val_k) or row.get("n") or 0)
        out.append(f"- **{lbl}**: {n}")
    body = "\n".join(out) if out else "_No rows aggregated._"
    return f"### {title}\n{body}"


def format_dataplane_slice_answer(slice_key: str, dp: dict, year: int) -> str | None:
    if not isinstance(dp, dict) or not slice_key:
        return None

    def _compact_flags() -> str:
        fg = dp.get("flags") or {}
        return (
            f"### Return complexity counters (season {year})\n"
            f"- **Extensions flagged (`is_extension`)**: {int(fg.get('ext') or 0)}\n"
            f"- **Returns with Form W‑7 linkage (`has_w7`)**: {int(fg.get('w7') or 0)}\n"
            f"- **Amended returns flagged (`is_amended`)**: {int(fg.get('amd') or 0)}"
        )

    def _compact_missing() -> str:
        m = dp.get("missing_docs") or {}
        return (
            "### Office-wide missing documents\n"
            f"- **Open missing-doc line items**: {int(m.get('n_items') or 0)}\n"
            f"- **Distinct returns with any open missing item**: {int(m.get('n_returns') or 0)}"
        )

    def _compact_fees() -> str:
        froll = dp.get("fee_rollups") or {}
        return (
            "### Rolled fee & payment aggregates (payments joined to season returns)\n"
            f"- **Sum billed `total_fee`**: ${float(froll.get('sum_total_fee') or 0):,.2f}\n"
            f"- **Sum `fee_paid`**: ${float(froll.get('sum_fee_paid') or 0):,.2f}\n"
            f"- **Sum `refund_amount` recorded**: ${float(froll.get('sum_refund') or 0):,.2f}\n"
            f"- **Sum deposits (`bank_deposit`)**: ${float(froll.get('sum_deposit') or 0):,.2f}\n"
            f"- **Sum specialty `accounting_fee` lines**: ${float(froll.get('sum_acct_fee') or 0):,.2f}\n"
            f"- **Sum down payments captured**: ${float(froll.get('sum_down') or 0):,.2f}\n"
            f"- **Combined discount-ish fields**: ${float(froll.get('sum_discount_like') or 0):,.2f}"
        )

    sections: dict[str, str] = {
        "form_leader": "## Return-form / product-profile mix\n" + _fmt_form_leader_section(dp, year),
        "drake_status_histogram": "## Drake / prep import labels (`returns.drake_status_raw`)"
        "\n"
        + _fmt_histogram_section(
            f"Season {year} histogram (counts can share labels across returns)",
            list(dp.get("drake_histogram") or []),
            "label",
            "n",
        ),
        "return_complexity_flags": _compact_flags(),
        "missing_docs_office_wide": _compact_missing(),
        "extraction_pipeline": "### Document extraction backlog\n"
        + _fmt_histogram_section(
            f"Statuses for queued PDFs scoped to season {year}",
            list(dp.get("extraction_by_status") or []),
            "status_label",
            "n",
        ),
        "payment_mix": "### Payment-method distribution\n"
        + _fmt_histogram_section(
            "`payments.payment_method` (one row may exist per payment record)",
            list(dp.get("payment_methods") or []),
            "payment_method_label",
            "n",
        ),
        "fee_rollups": _compact_fees(),
        "workflow_transitions": "### Workflow/status transition edges\n"
        f"_Total annotated events this season_: **{int(dp.get('status_event_count') or 0)}**.\n\n"
        + _fmt_histogram_section(
            "`status_events.old_status → status_events.new_status`",
            list(dp.get("status_transitions") or []),
            "edge",
            "n",
            limit=14,
        ),
        "efile_pipeline_summary": "## E-file telemetry\n### Batch items by `ack_status` (-season returns)"
        "\n"
        + _fmt_histogram_section(
            "Linkage via `returns` ↔ `efile_batch_items`",
            list(dp.get("efile_items_by_ack_status") or []),
            "ack_status_label",
            "n",
        )
        + "\n\n### E-file batches created this calendar year (by stored batch `status`)\n"
        + _fmt_histogram_section(
            "`efile_batches` rows where strftime('%Y', created_at) matches the selector year)",
            list(dp.get("efile_batches_by_status") or []),
            "batch_status_label",
            "n",
            limit=10,
        ),
        "import_pipeline": "### Drake / CSV imports (calendar "
        f"`{year}` on `import_batches.imported_at`)\n"
        f"- **Batches**: {int((dp.get('import_batches_calendar_year') or {}).get('batches_y') or 0)}\n"
        f"- **Row slots tallied**: {int((dp.get('import_batches_calendar_year') or {}).get('rows_y') or 0)}\n"
        f"- **Success rows summed**: "
        f"{int((dp.get('import_batches_calendar_year') or {}).get('ok_y') or 0)}\n"
        f"- **Error rows summed**: "
        f"{int((dp.get('import_batches_calendar_year') or {}).get('errs_y') or 0)}\n"
        f"- **Review-queue rows summed**: "
        f"{int((dp.get('import_batches_calendar_year') or {}).get('reviews_y') or 0)}",
        "email_classifier_stats": "## Email-domain classifier backlog\n"
        + _fmt_histogram_section(
            f"Rows with `strftime('%Y', created_at)` = `{year}`",
            list(dp.get("email_classifications_year") or []),
            "classification",
            "n",
            limit=20,
        ),
    }

    if slice_key == "office_brief":
        keys_order = (
            "form_leader",
            "drake_status_histogram",
            "return_complexity_flags",
            "missing_docs_office_wide",
            "extraction_pipeline",
            "payment_mix",
            "fee_rollups",
            "workflow_transitions",
            "efile_pipeline_summary",
            "import_pipeline",
            "email_classifier_stats",
        )
        blobs = []
        for k in keys_order:
            part = sections.get(k)
            if part:
                blobs.append(part.strip())
        text = ("\n\n---\n\n").join(blobs)
        if len(text) > 12000:
            text = text[:11900].rstrip() + "\n\n_(Truncated snapshot for chat length limits.)_"
        return "## Office dataplane composite\n" + text

    return sections.get(slice_key)


def _read_snapshot() -> dict:
    with _cache_lock:
        return dict(_snapshot) if isinstance(_snapshot, dict) else {}


def snapshot_office_brief_digest(year: int, *, max_chars: int = 6000) -> str:
    """Markdown composite built from ``refresh_chat_cache`` dataplane in memory."""
    snap = _read_snapshot()
    if int(snap.get("season_year") or -1) != int(year):
        return ""
    dp = snap.get("dataplane")
    if not isinstance(dp, dict):
        return ""
    body = format_dataplane_slice_answer("office_brief", dp, year) or ""
    if not body:
        return ""
    if len(body) > max_chars:
        body = body[: max_chars - 40].rstrip() + "\n\n…(truncated)"
    return body


def get_data_plane_text() -> str:
    return _data_plane_text


def build_data_plane(app, year: int, *, max_chars: int = 12000) -> None:  # noqa: ARG001
    """Rebuild cached markdown dataplane snapshot for LLM grounding."""
    global _data_plane_text
    refresh_chat_cache(year=int(year))
    _data_plane_text = snapshot_office_brief_digest(int(year), max_chars=max_chars) or ""


def refresh_chat_cache(
    conn: sqlite3.Connection | None = None, year: int | None = None
) -> None:
    """Rebuild in-memory aggregates + dataplane for intake season year."""
    global _snapshot
    if year is None:
        from datetime import date

        year = date.today().year

    owns = conn is None
    if owns:
        conn = get_connection()
    assert conn is not None

    try:
        rows_sql = f"""
            SELECT
                r.id,
                r.client_status,
                r.log_number,
                r.processor,
                r.intake_date,
                {DISPLAY_NAME_SQL}
            FROM returns r
            JOIN clients c ON c.id = r.client_id
            WHERE {_SEASON_CLAUSE}
            ORDER BY CAST(r.log_number AS INTEGER), r.id
        """
        raw_rows = [dict(row) for row in conn.execute(rows_sql, (str(year), year - 1))]

        rb_status: dict[str, list] = defaultdict(list)
        rb_proc: dict[str, list] = defaultdict(list)
        proc_counts: dict[str, int] = defaultdict(int)

        for row in raw_rows:
            clean = scrub_ssn_from_dict(dict(row))
            st = clean.get("client_status") or ""
            rb_status[st].append(clean)

            pname = (clean.get("processor") or "").strip() or "(unassigned)"
            rb_proc[pname].append(clean)
            proc_counts[pname] += 1

        balance_sql = f"""
            SELECT
                r.id,
                r.log_number,
                {DISPLAY_NAME_SQL},
                p.total_fee,
                p.fee_paid
            FROM returns r
            JOIN clients c ON c.id = r.client_id
            LEFT JOIN payments p ON p.return_id = r.id
            WHERE (p.total_fee IS NOT NULL AND COALESCE(p.fee_paid, 0) < p.total_fee)
              AND {_SEASON_CLAUSE}
            ORDER BY CAST(r.log_number AS INTEGER), r.id
        """
        bal_raw = [
            scrub_ssn_from_dict(dict(row))
            for row in conn.execute(balance_sql, (str(year), year - 1))
        ]
        bal_rows = bal_raw[:500]
        bal_set = frozenset(row["id"] for row in bal_rows if row.get("id") is not None)

        processor_counts_plain = dict(proc_counts)

        stats = {
            "status_counts": {s: len(rb_status[s]) for s in sorted(rb_status)},
            "processor_counts": processor_counts_plain,
            "balance_due_total": len(bal_raw),
        }

        dataplane = gather_season_dataplane(conn, year)

        new_snap = {
            "season_year": year,
            "returns_by_status": {k: v for k, v in rb_status.items()},
            "returns_by_processor": {k: v for k, v in rb_proc.items()},
            "balance_due_rows": bal_rows,
            "balance_due_set": bal_set,
            "stats": stats,
            "dataplane": dataplane,
        }
        with _cache_lock:
            _snapshot = new_snap
    finally:
        if owns:
            conn.close()


def ensure_chat_cache_for_year(year: int) -> None:
    """Lazy load / refresh when tax season selector changes."""
    global _lazy_init_year
    with _cache_lock:
        cached_y = _snapshot.get("season_year") if isinstance(_snapshot, dict) else None
        pending = cached_y != year or _lazy_init_year is None
    if pending:
        refresh_chat_cache(year=year)
        with _cache_lock:
            _lazy_init_year = year


def _not_cancelled_returns_predicate() -> str:
    return (
        "(r.client_status IS NULL OR TRIM(r.client_status) = '' OR "
        "UPPER(TRIM(r.client_status)) != 'CANCELLED')"
    )


def build_cache(app, year: int) -> None:  # noqa: ARG001
    """
    Build structured in-memory indexes for the intake season aligned with *year*.
    Thread-safe; skips if a build is already running.
    """
    global _STATUS_COUNTS, _RETURNS_BY_STATUS, _PROCESSOR_COUNTS
    global _RETURNS_BY_PROCESSOR, _FINANCIAL_STATS, _BALANCE_DUE_IDS
    global _cache_built_at, _cache_year

    if not _structured_index_lock.acquire(blocking=False):
        logger.info("Structured chat cache build already in progress — skipping")
        return

    y = int(year)
    params = (str(y), y - 1)
    nc = _not_cancelled_returns_predicate()

    try:
        logger.info("Building structured chat cache for season year %s…", y)
        t0 = time.time()
        conn = get_connection()
        try:
            rows_sql = f"""
                SELECT
                    r.id,
                    r.client_status,
                    r.log_number,
                    r.processor,
                    r.intake_date,
                    r.logout_date,
                    {DISPLAY_NAME_SQL}
                FROM returns r
                JOIN clients c ON c.id = r.client_id
                WHERE {_SEASON_CLAUSE}
                  AND {nc}
                ORDER BY CAST(r.log_number AS INTEGER), r.id
            """
            raw_rows = conn.execute(rows_sql, params).fetchall()

            new_status_counts: dict[str, int] = {}
            new_returns_by_status: dict[str, list[dict]] = {}
            new_processor_counts: dict[str, int] = {}
            new_returns_by_processor: dict[str, list[dict]] = {}

            for row in raw_rows:
                rd = dict(row)
                clean = scrub_ssn_from_dict(rd)
                st = str(clean.get("client_status") or "").strip() or "UNKNOWN"
                proc_raw = (clean.get("processor") or "").strip()
                processor = proc_raw if proc_raw else "Unassigned"

                r_dict = {
                    "id": int(clean["id"]),
                    "display_name": str(clean.get("display_name") or "").strip(),
                    "processor": processor,
                    "intake_date": str(clean.get("intake_date") or "").strip(),
                    "logout_date": str(clean.get("logout_date") or "").strip(),
                    "log_number": str(clean.get("log_number") or "").strip(),
                    "status": st,
                }

                new_status_counts[st] = new_status_counts.get(st, 0) + 1
                new_returns_by_status.setdefault(st, []).append(r_dict)

                new_processor_counts[processor] = new_processor_counts.get(processor, 0) + 1
                new_returns_by_processor.setdefault(processor, []).append(r_dict)

            fee_sql = f"""
                SELECT
                  COALESCE(SUM(p.total_fee), 0.0) AS sum_total_fee,
                  COALESCE(SUM(p.fee_paid), 0.0) AS sum_fee_paid,
                  COALESCE(
                    SUM(
                      CASE
                        WHEN p.total_fee IS NOT NULL
                         AND (p.total_fee - COALESCE(p.fee_paid, 0)) > 0
                        THEN (p.total_fee - COALESCE(p.fee_paid, 0))
                        ELSE 0.0
                      END
                    ),
                    0.0
                  ) AS sum_outstanding
                FROM returns r
                LEFT JOIN payments p ON p.return_id = r.id
                WHERE {_SEASON_CLAUSE}
                  AND {nc}
            """
            fee_row = conn.execute(fee_sql, params).fetchone()
            fee_d = dict(fee_row) if fee_row else {}

            cnt_ret_row = conn.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM returns r
                WHERE {_SEASON_CLAUSE}
                  AND {nc}
                """,
                params,
            ).fetchone()
            return_count = int(cnt_ret_row["n"] if cnt_ret_row else 0)

            bal_sql = f"""
                SELECT COUNT(DISTINCT r.id) AS n
                FROM returns r
                LEFT JOIN payments p ON p.return_id = r.id
                WHERE (p.total_fee IS NOT NULL AND COALESCE(p.fee_paid, 0) < p.total_fee)
                  AND {_SEASON_CLAUSE}
                  AND {nc}
            """
            bal_row = conn.execute(bal_sql, params).fetchone()
            balance_due_count = int(bal_row["n"] if bal_row else 0)

            bal_ids_sql = f"""
                SELECT DISTINCT r.id AS id
                FROM returns r
                LEFT JOIN payments p ON p.return_id = r.id
                WHERE (p.total_fee IS NOT NULL AND COALESCE(p.fee_paid, 0) < p.total_fee)
                  AND {_SEASON_CLAUSE}
                  AND {nc}
            """
            id_rows = conn.execute(bal_ids_sql, params).fetchall()
            new_balance_ids = {
                int(r["id"]) for r in id_rows if r["id"] is not None
            }

            new_financial = {
                "return_count": return_count,
                "total_billed": float(fee_d.get("sum_total_fee") or 0),
                "total_collected": float(fee_d.get("sum_fee_paid") or 0),
                "total_outstanding": float(fee_d.get("sum_outstanding") or 0),
                "balance_due_count": balance_due_count,
            }

            _STATUS_COUNTS = new_status_counts
            _RETURNS_BY_STATUS = new_returns_by_status
            _PROCESSOR_COUNTS = new_processor_counts
            _RETURNS_BY_PROCESSOR = new_returns_by_processor
            _FINANCIAL_STATS = new_financial
            _BALANCE_DUE_IDS = new_balance_ids
            _cache_built_at = time.time()
            _cache_year = y

            logger.info(
                "Structured chat cache built in %.2fs — %s returns, %s processors, season %s",
                time.time() - t0,
                return_count,
                len(new_processor_counts),
                y,
            )
        finally:
            conn.close()
    except Exception as e:
        logger.error("Structured chat cache build failed: %s", e, exc_info=True)
    finally:
        _structured_index_lock.release()


def refresh_cache(app, year: int) -> None:
    """On-demand structured index rebuild."""
    build_cache(app, year)


def ensure_structured_cache_for_year(year: int) -> None:
    """Lazy-load structured indexes when the chat season selector changes."""
    global _structured_lazy_year
    y = int(year)
    if is_cache_warm() and get_cache_year() == y:
        _structured_lazy_year = y
        return
    build_cache(None, y)
    _structured_lazy_year = y


def get_status_count(status: str) -> int:
    su = str(status or "").strip().upper()
    for k, v in _STATUS_COUNTS.items():
        if str(k).strip().upper() == su:
            return int(v)
    return 0


def get_all_status_counts() -> dict[str, int]:
    return dict(_STATUS_COUNTS)


def get_returns_for_status(status: str) -> list[dict]:
    su = str(status or "").strip().upper()
    for k, lst in _RETURNS_BY_STATUS.items():
        if str(k).strip().upper() == su:
            return [dict(x) for x in lst]
    return []


def get_processor_count(processor: str) -> int:
    needle = str(processor or "").strip().lower()
    if not needle:
        return 0
    for name, count in _PROCESSOR_COUNTS.items():
        if str(name).strip().lower() == needle:
            return int(count)
    return 0


def get_all_processor_counts() -> dict[str, int]:
    return dict(_PROCESSOR_COUNTS)


def get_financial_stats() -> dict[str, float | int]:
    return dict(_FINANCIAL_STATS)


def is_balance_due(return_id: int) -> bool:
    return int(return_id) in _BALANCE_DUE_IDS


def get_cache_age_seconds() -> float:
    if _cache_built_at <= 0:
        return float("inf")
    return time.time() - _cache_built_at


def get_cache_year() -> int:
    return int(_cache_year)


def is_cache_warm() -> bool:
    return _cache_built_at > 0


def start_cache_worker(app) -> None:
    """Background rebuild every 5 minutes — structured indexes + dataplane markdown."""
    from datetime import date as _date

    def _worker() -> None:
        yr = _date.today().year
        try:
            with app.app_context():
                build_cache(app, yr)
                build_data_plane(app, yr)
        except Exception as exc:
            logger.exception("chat cache worker startup pass failed: %s", exc)

        while True:
            time.sleep(300)
            yr = _date.today().year
            try:
                with app.app_context():
                    build_cache(app, yr)
                    build_data_plane(app, yr)
            except Exception as exc:
                logger.error("Structured chat cache worker error: %s", exc)

    threading.Thread(target=_worker, daemon=True, name="chat-cache-worker").start()
    logger.info("Chat cache worker started (structured indexes + data plane text)")
