"""
In-memory chat fast paths + TTL answer cache for POST /ai/chat.

Rebuild on demand per season year; answer cache TTL (default 5 min). Optionally persist
successful answers to SQLite (`ai_chat_common_answers`) for repeat lookup across restarts.
Privacy: all cached dict rows pass through scrub_ssn_from_dict().
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Literal

from config import AI_CHAT_DISK_CACHE_ENABLE, AI_CHAT_DISK_CACHE_HOURS
from db import get_connection
from db_tools import (
    count_returns_with_positive_refund_in_season,
    gather_season_dataplane,
    get_balance_due_returns,
    get_missing_docs,
    search_clients,
    summarize_season_rejections_for_chat,
)
from utils import now, parse_iso_datetime, scrub_ssn_from_dict

# Duplicated from db_tools (do not import private _SEASON_CLAUSE).
_SEASON_CLAUSE = (
    "(strftime('%Y', r.intake_date) = ? OR "
    "(r.intake_date IS NULL AND r.tax_year = ?))"
)

logger = logging.getLogger(__name__)

DISPLAY_NAME_SQL = (
    "COALESCE(c.display_name, c.last_name || CASE WHEN c.first_name IS NOT NULL AND "
    "c.first_name != '' THEN ', ' || c.first_name ELSE '' END) AS display_name"
)

# Must match CHAT_TOOLS / staff workflow literals.
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


def _aggregate_status_lookup(status_counts: dict, status_guess: str) -> int | None:
    """Resolved count from live or snapshot aggregates; same matching as ai_routes."""
    g = str(status_guess or "").strip()
    if not g:
        return None
    raw = dict(status_counts or {})
    if not raw:
        return None
    if g in raw:
        return int(raw[g])
    gu = g.upper()
    for k, val in raw.items():
        if str(k).strip().upper() == gu:
            return int(val)
    return None


def is_count_question(question: str) -> bool:
    q = question.strip().lower()
    if any(
        kw in q
        for kw in (
            "how many",
            "total number",
            "number of",
        )
    ):
        return True
    if re.search(r"\bcount\b", q):
        return True
    return False


def _resolved_status_from_natural_language_volume(question: str) -> str | None:
    """Map staff wording (e-file done + folder logged out) to canonical `client_status` for volume counts."""
    q = question.strip().lower()
    if not q:
        return None
    if _extract_status_from_question(question):
        return None
    if re.search(r"\bnot\s+log(?:ged)?\s+out\b", q):
        return None
    logged_out = bool(
        re.search(r"\blog(?:ged)?\s+out\b", q) or re.search(r"\blogout\b", q)
    )
    efile_done = bool(
        re.search(r"\b(?:e[-\s]?filed|fully\s+e[-\s]?file(?:d)?)\b", q)
    )
    if efile_done and logged_out:
        return "LOG OUT"
    if logged_out and re.search(r"\b(?:returns?|clients?|files)\b", q):
        return "LOG OUT"
    return None


def wants_tool_row_aggregate(question: str) -> bool:
    """Staff asks for totals/volume — use DB row counts, not LLM-invented numbers."""
    if is_count_question(question):
        return True
    q = question.strip().lower()
    return bool(
        re.search(
            r"\breturns?\b.{1,48}\b(do\s+we\s+have|have\s+we|we\s+have|we'?ve\s+got|got)\b"
            r"|\b(do\s+we\s+have|have\s+we|we\s+have|how\s+much\b).{1,52}\breturns?\b",
            q,
        )
        or bool(
            re.search(
                r"\b(do\s+we\s+have|have\s+we|we\s+have)\b.{1,54}\bin\b.+?\b(stage|stat(?:e|us)|queue|pile|backlog)\b",
                q,
            )
        )
    )


def prefers_narrative_list_answer(question: str) -> bool:
    """Obvious listing requests skip aggregate short-circuit so the LLM can summarize rows."""
    if is_count_question(question):
        return False
    q = question.lower()
    return bool(
        re.search(
            r"\b(show\s+me|please\s+(?:show|list)|\blist\b|enumerate|every\s+(?:single\s+)?"
            r"return|each\s+(?:individual\s+)?return|(?:pull|bring)\s+up|display\b)",
            q,
        )
    )


def _canonicalize_balance_misspellings(text: str) -> str:
    """Map common typos for *balance* (e.g. valance, balence) so fee-balance intents still match."""
    if not (text and text.strip()):
        return text
    s = text
    s = re.sub(r"\bvalances?\b", "balance", s, flags=re.I)
    s = re.sub(r"\bbalences?\b", "balance", s, flags=re.I)
    return s


_SUPERLATIVE_HI_BALANCE_RE = re.compile(
    r"\b(?:highest|largest|biggest|maximum)\s+(?:[^\n?.]{0,48}?)\bbalance\b"
    r"|^\s*\bbalance\b(?:[^\n?.]{0,40}?)\b(?:highest|largest|biggest|maximum)\b"
    r"|\bbalance\b[^\n?.]{0,24}\btop\b"
    r"|\btop\b[^\n?.]{0,40}\bbalance\b"
    r"|\bwho\s+(?:owe|owes)\b[^\n?.]{0,48}\b(?:the\s+)?most\b"
    r"|\b(?:most|maximum)\s+owed\b"
    r"|\bobligor\b[^\n?.]{0,30}\bhighest\b",  # office jargon fallback
    re.I | re.UNICODE,
)


_SUPERLATIVE_LO_BALANCE_RE = re.compile(
    r"\b(?:lowest|smallest|minimum)\s+(?:[^\n?.]{0,48}?)\bbalance\b"
    r"|^\s*\bbalance\b(?:[^\n?.]{0,40}?)\b(?:lowest|smallest|minimum)\b",
    re.I | re.UNICODE,
)


def balance_superlative_sort_mode(question: str) -> Literal["high", "low"] | None:
    """
    Highest/lowest unpaid *fee balance* question — not workflow status PROCESSING/HOLD/etc.
    Workflow tool get_returns_by_status does not model payment balances; callers must use
    get_balance_due_returns."""
    ql = _canonicalize_balance_misspellings(question).strip().lower()
    lm = _SUPERLATIVE_LO_BALANCE_RE.search(ql)
    hm = _SUPERLATIVE_HI_BALANCE_RE.search(ql)
    if hm and lm:
        return "high" if hm.start() <= lm.start() else "low"
    if hm:
        return "high"
    if lm:
        return "low"
    return None


def _balance_still_due_numeric(row: dict) -> float:
    try:
        tf = float(row.get("total_fee") or 0)
        fp = float(row.get("fee_paid") or 0)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, tf - fp)


def _money_display(v) -> str:
    if v is None:
        return "—"
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return "—"


def wants_qualitative_return_answer(question: str) -> bool:
    """
    Staff wants identities, ordering, or specifics — not only a total count.

    These questions should see row-level tool output (LLM summarizes) instead of the
    deterministic "There are N returns in STATUS" short-circuit.
    """
    if is_count_question(question):
        return False
    q = question.strip().lower()
    if re.search(
        r"\b(who|whose|whom|which\s+(?:return|client|file|one|taxpayer|person))\b",
        q,
    ):
        return True
    if re.search(
        r"\b(earliest|latest|soonest|first|last|oldest|newest|next|previous)\b",
        q,
    ):
        return True
    if re.search(r"\bsort(?:ed)?\s+by\b|\border\s+by\b|\brank(?:ing|ed)?\b", q):
        return True
    return False


def use_narrative_return_rows(question: str) -> bool:
    """True when we should not collapse get_returns_* tools to a bare count."""
    if wants_tool_row_aggregate(question):
        return False
    return prefers_narrative_list_answer(question) or wants_qualitative_return_answer(question)


_CHRONO_EARLY_RE = re.compile(
    r"\b(earliest|first|soonest|oldest)\b",
    re.I,
)
_CHRONO_LATE_RE = re.compile(
    r"\b(latest|last|newest|most\s+recent)\b",
    re.I,
)


def chronological_superlative_chat_payload(
    tool_name: str,
    tool_call_kw: dict,
    rows: list[dict],
    question: str,
    actual_count: int,
    year_arg: int,
) -> dict | None:
    """
    Deterministic earliest/latest-by-date for LOG OUT / PICKUP status lists — avoids LLM inventing dates.
    Uses full row list before truncation (all matching returns for the season query).
    """
    if (
        tool_name != "get_returns_by_status"
        or not isinstance(rows, list)
        or not rows
        or actual_count <= 0
        or not wants_qualitative_return_answer(question)
    ):
        return None

    q = question.lower()
    early = bool(_CHRONO_EARLY_RE.search(question))
    late = bool(_CHRONO_LATE_RE.search(question))
    if not early and not late:
        return None

    sta = str(tool_call_kw.get("status") or "").strip().upper()
    field_mode: str | None = None
    if sta == "LOG OUT":
        field_mode = "completion"
    elif sta == "PICKUP":
        field_mode = "pickup"
    else:
        return None

    def log_num(row: dict) -> int:
        try:
            return int(float(str(row.get("log_number") or 0)))
        except (TypeError, ValueError):
            return 0

    scored: list[tuple] = []

    if field_mode == "completion":
        for row in rows:
            if not isinstance(row, dict):
                continue
            lo = parse_iso_datetime(row.get("logout_date"))
            ak = parse_iso_datetime(row.get("ack_date"))
            if lo:
                scored.append((row, lo, "recorded office logout"))
            elif ak:
                scored.append((row, ak, "IRS ack"))
    else:
        for row in rows:
            if not isinstance(row, dict):
                continue
            pu = parse_iso_datetime(row.get("pickup_date"))
            if pu:
                scored.append((row, pu, "pickup date"))

    if not scored:
        kind = "logout/ack" if field_mode == "completion" else "pickup"
        return {
            "answer": (
                f"No {kind} dates are recorded on these {sta} returns for {year_arg}, "
                f"so earliest/latest cannot be determined from the system. "
                f"({actual_count} returns in {sta}.)"
            ),
            "tool_used": tool_name,
            "args_used": tool_call_kw,
            "result_count": actual_count,
            "fast": True,
        }

    if early:
        row, dt, label = min(scored, key=lambda x: (x[1], log_num(x[0])))
        sup = "Earliest"
    else:
        row, dt, label = max(scored, key=lambda x: (x[1], log_num(x[0])))
        sup = "Latest"

    nm = (row.get("display_name") or "Client").strip()
    lg = row.get("log_number")
    dstr = dt.date().isoformat()
    ctx = f"{sup} {sta} by {label}: LOG {lg} — {nm}, date {dstr}."
    tail = f" ({actual_count} returns in {sta} for {year_arg}.)"
    return {
        "answer": ctx + tail,
        "tool_used": tool_name,
        "args_used": tool_call_kw,
        "result_count": actual_count,
        "fast": True,
    }


ANSWER_CACHE: dict[str, tuple[dict, float]] = {}
ANSWER_CACHE_TTL_SEC = 300

# Include in MD5(cache_key): bump when deterministic routing / tool choice changes so we do not serve
# stale answers that were keyed under old logic (TTL + SQLite rows become misses automatically).
_CHAT_ANSWER_ROUTE_VERSION = "v15"

_cache_lock = threading.Lock()
_lazy_init_year: int | None = None

# Replaced atomically via pointer swap — readers take ref under lock briefly.
_snapshot: dict = {
    "season_year": None,
    "returns_by_status": {},
    "returns_by_processor": {},
    "balance_due_rows": [],
    "balance_due_set": frozenset(),
    "stats": {
        "status_counts": {},
        "processor_counts": {},
        "balance_due_total": 0,
    },
    "dataplane": {},
}


def normalize_question(question: str) -> str:
    return question.strip().lower()


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
    """
    Skip empty, error-shaped, or generic scope / tool-router fallback answers so the
    SQLite cache stays useful as a living FAQ.
    """
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


def _extract_tax_year(question: str, default: int) -> int:
    m = re.search(r"\b(20[2-9][0-9])\b", question)
    if not m:
        return default
    try:
        return int(m.group(1))
    except ValueError:
        return default


def _extract_return_id(question: str) -> int | None:
    mi = re.search(
        r"(?:return|file)\s*(?:id|#)?\s*(\d{2,})", question, re.I
    )
    if mi:
        try:
            return int(mi.group(1))
        except ValueError:
            return None
    return None


def _extract_client_search_query(question: str) -> str | None:
    m = re.search(
        r"(?:find|search\s+(?:for|clients?\s+for)|look\s+up)\s+(?:the\s+)?(.+?)(?:\?|$)",
        question,
        re.I,
    )
    if not m:
        return None
    q = (m.group(1) or "").strip()
    q = re.sub(r"\s+family$", "", q, flags=re.I).strip()
    return q[:200] if q else None


def _extract_status_from_question(question: str) -> str | None:
    """Match a workflow label only as a whole phrase (avoids matches hidden inside unrelated words)."""
    for st in sorted(CHAT_ALLOWED_STATUSES, key=len, reverse=True):
        parts = st.split()
        if len(parts) == 1:
            pat = r"\b" + re.escape(parts[0]) + r"\b"
        else:
            pat = r"\b" + r"\s+".join(re.escape(p) for p in parts) + r"\b"
        if re.search(pat, question, flags=re.I):
            return st
    return None


def _extract_processor_guess(question: str) -> str | None:
    for pat in (
        r"(?:assigned to|for)\s+(?:prep(?:arer)?\.?|processor)\s+([^?.;\n]{2,120})",
        r"how\s+many\s+returns?\s+does\s+([^?.;\n]{2,80})\s+(?:have|prep)",
        r"(?:prep(?:arer)?\.?|processor)\s+(?:called\s+)?['\"]?([^'\"?.;\n]{2,80})['\"]?",
    ):
        m = re.search(pat, question, re.I)
        if m:
            cand = (m.group(1) or "").strip()
            if len(cand) >= 2:
                return cand
    return None


def wants_rejection_reason_breakdown(question: str) -> bool:
    """True when staff want IRS rejection causes, not only a REJECTED status count."""
    q = question.strip().lower()
    if not re.search(
        r"\b(?:why|reason|reasons|because|cause|causes|explains?|explain|what\s+went\s+wrong|how\s+come)\b",
        q,
    ):
        return False
    return bool(
        re.search(r"\breject(?:ed|ions?|s)?\b", q)
        or _extract_status_from_question(question) == "REJECTED"
    )


def format_season_rejection_breakdown_for_chat(summ: dict, year: int) -> str:
    """Plain-language roll-up lines (office-wide codes only; never client-identifying data)."""
    total = int(summ.get("total") or 0)
    _ = year
    if total <= 0:
        return ""
    bullets: list[str] = []
    for row in summ.get("by_code") or []:
        code = row.get("code") or "(unknown)"
        cnt = int(row.get("count") or 0)
        expl = row.get("explanation")
        ntok = "return" if cnt == 1 else "returns"
        if expl:
            bullets.append(f"- **{code}** — {cnt} {ntok}: {expl}")
        else:
            bullets.append(
                f"- **{code}** — {cnt} {ntok}: (no local IRS reference entry for this code; "
                "open the return or IRS ack detail in TaxOps for the verbatim message)."
            )
    nh = int(summ.get("no_batch_history_count") or 0)
    if nh > 0:
        nhtok = "return" if nh == 1 else "returns"
        bullets.append(
            f"- **No e-file batch history** — {nh} {nhtok} marked REJECTED with no matching "
            "`efile_batch_items` row (rejection detail may live only outside that table)."
        )
    hdr = "**Why (latest IRS rejection code per return, office-wide)**"
    return hdr + "\n" + ("\n".join(bullets) if bullets else "_(No coded rejections grouped yet.)_")


_SEASON_TOTAL_EXACT_PHRASES: frozenset[str] = frozenset(
    {
        "total returns",
        "total return",
        "returns total",
        "return total",
        "returns count",
        "return count",
        "how many returns",
        "how many return",
        "number of returns",
        "number of return",
        "count of returns",
        "count of return",
    }
)


def _wants_season_totals_aggregate_only(question: str) -> bool:
    """
    Whole-season return volume (sum of workflow status buckets)—no router / no tool.

    Exclude status-specific counts, preparer-qualified counts, and fee/balance phrasing.
    """
    if prefers_narrative_list_answer(question) or wants_qualitative_return_answer(question):
        return False
    if _extract_status_from_question(question) or _extract_processor_guess(question):
        return False
    qc = re.sub(r"\s+", " ", normalize_question(question).rstrip("?.! ").strip())
    negatives = (
        "balance",
        "unpaid",
        "owing",
        "owe ",
        " owed",
        "missing",
        "document",
        "reject",
        "who ",
        "which ",
        "show me",
        "list ",
        "list all",
        "every return",
        "enumerate",
        "highest",
        "lowest",
        "top ",
    )
    if any(n in qc for n in negatives):
        return False
    if qc in _SEASON_TOTAL_EXACT_PHRASES:
        return True
    if re.match(
        r"^((what(\s+is|\s*'s)\s+)(the\s+)?)?(combined\s+|total\s+)?(number\s+of\s+)?returns?(\s+total)?(\s+this\s+(year|season))?(\s+in\s+taxops)?$",
        qc,
    ):
        return True
    # Whole-season footprint: returns "in TaxOps/in the system/overall DB" — not workflow status tally.
    if re.search(
        r"\bhow\s+many\b|\btotal\b|\bnumber\s+of\b|\bcount\s+of\b",
        qc,
    ) and re.search(r"\breturns?\b", qc):
        scope = (
            r"\bin\s+taxops\b|\bacross\s+taxops\b|\bin\s+(the\s+)?(system|office|software|database|db)\b"
            r"|\b(?:in|across)\s+(?:our\s+|the\s+)?office\b"
        )
        if re.search(scope, qc) and not re.search(
            r"\b(types?|kinds?)\s+of\s+returns?\b|\breturns?\s+types?\b",
            qc,
        ):
            return True
    return False


def _wants_season_refund_return_count(question: str) -> bool:
    """
    Season-wide COUNT tied to ``payments.refund_amount`` (> 0)—no router / SQL only.

    Excludes ambiguous “what returns…” questions unless phrased as total/how many/etc.
    """
    if prefers_narrative_list_answer(question) or wants_qualitative_return_answer(question):
        return False
    if _extract_status_from_question(question) or _extract_processor_guess(question):
        return False
    qc = re.sub(r"\s+", " ", normalize_question(question).rstrip("?.! ").strip())
    if "refund" not in qc:
        return False
    if re.match(r"^what\s+returns?\b", qc) and not re.match(r"^what(\s+is|\s*'s)\b", qc):
        return False
    negatives = (
        "balance due",
        "outstanding balance",
        "still owe",
        "unpaid",
        "owe money",
        "balance owed",
        "owe a balance",
        "missing doc",
        "who ",
        "which ",
        "show me",
        "list ",
        "list all",
    )
    if any(n in qc for n in negatives):
        return False
    if re.search(r"\breturns?\s+with\s+refunds?\b", qc):
        return True
    if re.search(r"\btotal\s+returns?\s+with\s+refunds?\b", qc):
        return True
    if re.search(
        r"\b(how\s+many|number\s+of|count\b|combined|total)\b.{1,80}\breturns?\b.{0,72}\brefund",
        qc,
    ):
        return True
    if re.search(
        r"\b(how\s+many|number\s+of|count\b|combined|total)\b.{1,80}\brefunds?\b.{0,72}\breturns?\b",
        qc,
    ):
        return True
    return False


def _wants_dominant_workflow_status(question: str) -> bool:
    """
    Staff asks for the mode workflow bucket (PROCESSING/HOLD/etc.)—not IRS reject reasons
    nor business-vs-1040 return product mix unless they name a form explicitly.
    """
    q = question.strip().lower()
    if prefers_narrative_list_answer(question) or wants_qualitative_return_answer(question):
        return False
    if re.search(
        r"\b(1040|1040[-\s]?sr|1120[-\s]?s?|1065|w-?2\b|1099\b|schedule\s+[a-ce]|sch\.?\s*[a-ce])\b",
        q,
    ):
        return False
    if re.search(
        r"\b(preparer|processor|assigned\s+to|reject|rejected|missing\s+doc|balance|owe)\b",
        q,
    ):
        return False
    if re.search(
        r"\btypes?\s+of\s+returns?\b|\b(kind|kinds)\s+of\s+returns?\b",
        q,
    ):
        return False
    if "drake" in q:
        return False
    theme = bool(
        re.search(r"\b(return|returns|workflow|status|queue|pipeline|office|season)\b", q)
        or ("system" in q and "return" in q)
    )
    if not theme:
        return False
    if re.search(r"\bmost\s+(?:common|frequent)\b", q) or re.search(r"\bcommonest\b", q):
        return True
    return bool(re.search(r"\bwhich\s+.+\s+has\s+the\s+most\b", q) or re.search(r"\bmode\b", q))


def classify_dataplane_slice(question: str) -> str | None:
    """Map natural language to synthetic aggregate answers (office-wide KPIs).

    Narrow patterns—skipped for obviously per-return lookups unless they say “season/office”.
    """
    if prefers_narrative_list_answer(question) or wants_qualitative_return_answer(question):
        return None
    q = question.strip().lower()
    if _extract_return_id(question) is not None and not re.search(
        r"\b(?:whole\s+office|season(?:-wide)?|overall|every\s+return|bulk|rollup)\b", q,
    ):
        return None

    if re.search(
        r"\b(?:operational\s+snapshot|op\s+brief|kitchen\s+sink|everything\s+slices?)\b",
        q,
    ) or ("all dataplane" in q or "office snapshot all" in q):
        return "office_brief"

    form_mix_general = (
        re.search(r"\b(types?\s+of\s+returns?|(kind|kinds)\s+of\s+returns?)\b", q)
        and (
            re.search(r"\b(how\s+many|different|distinct|variation|vary|profiles?)\b", q)
            or re.search(
                r"\b(in\s+the\s+system|overall|whole\s+|office\s+wide|office|season)\b",
                q,
            )
            or re.search(r"\b(what|which)\s+(kind|kinds|types?)\s+of\s+returns?\b", q)
        )
    )

    formish = (
        re.search(r"\bmost\s+(?:common|frequent)|commonest|modal|peak\s+volume\b", q)
        and re.search(
            r"\btypes?\s+of\s+returns?|\b(kind|kinds)\s+of\s+returns?|"
            r"\b1065\b|\bsch\.?\s*c\b|schedule\s+c\b|corp|1120\b|business\s+(?:packages?|returns?)|"
            r"\bmix\b.*\bform",
            q,
        )
    ) or re.search(
        r"\b(form\s+mix|return\s+(?:profiles?|products?)|entity\s+(?:volume|counts?))"
        r"|\bhow\s+many\b.*\b(schedule\s+c|sch\.?\s*c|1040s?|corp|1065\b|1120\b)\b",
        q,
    )
    if form_mix_general:
        formish = True
    if formish:
        return "form_leader"

    if "drake" in q:
        return "drake_status_histogram"

    if re.search(
        r"\b(extension|extensions?)\b|\bw[- ]?7\b|\b(amended|1040x)\b", q,
    ) and re.search(r"\b(how\s+many|count\b|percentage|pct|share|volume|ratio)", q):
        return "return_complexity_flags"

    if re.search(
        r"\b(how\s+many|count\b|\brollup\b).*"
        r"\b(extension|extensions?|amended\b|1040\s*x\b|w[- ]?7)\b|\b(extension|extensions?)\s+volume\b",
        q,
    ):
        return "return_complexity_flags"

    if re.search(r"\bmissing\b", q) and re.search(
        r"\bdocuments?|documentation|dock\b|\bopen\s+tickets\b", q,
    ):
        return "missing_docs_office_wide"

    if re.search(
        r"\bextraction\b|\bpdf\s*pipeline\b|\bocr\b|\bscan\s+labor\b|\bdocuments?\s+extract",
        q,
    ):
        return "extraction_pipeline"

    if re.search(
        r"\bpayment\s+methods?\b|\bhow\s+.+\bpaid\b|\bzelle\b|\bcash\b\s+pickup\b",
        q,
    ):
        return "payment_mix"

    if re.search(
        r"\bfee\b.*\b(rollup|combined|summar|snapshot|pulse)\b|\brolled-?up\s+fees\b|"
        r"\baccounting_fee\b|\b(sum|total).*fee\b\s*(year|office|season)",
        q,
    ):
        return "fee_rollups"

    if re.search(r"\bimport\b.*\bbatch(es)?\b|\bcsv\b.*\b(sync|bring|bring-?in|load)\b", q):
        return "import_pipeline"

    if re.search(
        r"\bemail\b.{0,40}\b(class|classification|routing|filter)\b|\bpromotional\s+mail\b",
        q,
    ):
        return "email_classifier_stats"

    if re.search(
        r"\bstatus\b.*\b(transition|change|history)|\bworkflow\s+funnel\b", q,
    ):
        return "workflow_transitions"

    if re.search(r"\befile\b", q) and not re.search(r"\breject|why\b", q):
        return "efile_pipeline_summary"

    return None


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


def snapshot_office_brief_digest(year: int, *, max_chars: int = 6000) -> str:
    """
    Markdown composite from in-memory refresh_chat_cache dataplane — no extra DB round-trip.
    Used to ground router + aggregate answers office-wide KPIs before row-level tools spin.
    """
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


def normalize_chat_router_payload(sel: dict) -> dict:
    """
    Canonical fields from tool-router JSON: router_mode, router_confidence, tool, args, clarify_prompt.

    Fills sensible defaults when the model omits planner fields (backward compatible).
    """
    if not isinstance(sel, dict):
        return {
            "router_mode": "aggregates",
            "router_confidence": 1.0,
            "tool": None,
            "args": {},
            "clarify_prompt": "",
        }

    raw_tool = sel.get("tool")
    tn = ""
    if isinstance(raw_tool, str):
        tn = raw_tool.strip()
    elif raw_tool is not None and str(raw_tool).strip():
        tn = str(raw_tool).strip()
    if tn.lower() in ("null", "none"):
        tn = ""

    raw_args = sel.get("args", {})
    args = raw_args if isinstance(raw_args, dict) else {}

    mode_raw = str(sel.get("mode") or sel.get("router_mode") or "").strip().lower()
    clarify = sel.get("clarify_prompt") or sel.get("clarify") or sel.get(
        "clarifying_question"
    )
    clarify_str = clarify.strip() if isinstance(clarify, str) else ""

    rf = sel.get("router_confidence")
    if rf is None:
        rf = sel.get("confidence")
    conf: float | None
    try:
        conf = float(rf) if rf is not None else None
    except (TypeError, ValueError):
        conf = None
    if conf is not None:
        conf = max(0.0, min(1.0, conf))

    allowed_modes = frozenset({"aggregates", "need_rows", "clarify"})

    if mode_raw == "clarify" or (not tn and clarify_str and mode_raw != "need_rows"):
        mode = "clarify"
        if conf is None:
            conf = 1.0
    elif mode_raw in allowed_modes:
        mode = mode_raw
        if conf is None:
            conf = 1.0 if mode == "aggregates" else (0.72 if mode == "need_rows" else 1.0)
    elif tn:
        mode = "need_rows"
        if conf is None:
            conf = 0.72
    else:
        mode = "aggregates"
        if conf is None:
            conf = 1.0

    if mode == "need_rows" and not tn:
        mode = "aggregates"

    if mode == "aggregates":
        tn = ""
        args = {}
        clarify_str = ""
    elif mode == "need_rows":
        clarify_str = ""
    elif mode == "clarify":
        tn = ""
        args = {}

    assert conf is not None
    out = dict(sel)
    out["router_mode"] = mode
    out["router_confidence"] = float(conf)
    out["tool"] = tn if tn else None
    out["args"] = dict(args)
    out["clarify_prompt"] = clarify_str
    out["mode"] = mode
    return out


def dataplane_digest_freshness_banner(year: int, ctx_live: dict | None) -> str:
    """Short note distinguishing live KPI roll-up vs snapshot dataplane markdown."""
    from datetime import datetime, timezone

    utc_now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    sc = dict((ctx_live or {}).get("status_counts") or {})
    try:
        n_status = sum(int(v) for v in sc.values()) if sc else 0
    except (TypeError, ValueError):
        n_status = 0
    return (
        f"_Live **ROUTER CONTEXT** status roll-up covers **{n_status}** return(s) "
        f"bucketed across workflow labels for season **{year}** (`get_system_context` at **`{utc_now_iso}`**) — instant._\n\n"
        f"_Markdown KPI sections below use the TaxOps **`refresh_chat_cache`** dataplane snapshot for **`{year}`** "
        f"(composite rebuilds when the intake year rotates or is explicitly refreshed)._\n\n"
        "---\n\n"
    )


def apply_row_tool_grounding_guard(
    question: str,
    classified_intent: str,
    intent_entities: dict,
    tool_name_raw: object,
    raw_args: object,
) -> tuple[str | None, dict]:
    """
    Narrow row-queue tools unless question + classifier hints justify them (all intents—not only fallback).
    """
    args_in = raw_args if isinstance(raw_args, dict) else {}
    ents = intent_entities if isinstance(intent_entities, dict) else {}

    tn = ""
    if isinstance(tool_name_raw, str):
        tn = tool_name_raw.strip()
    elif tool_name_raw is not None:
        tn = str(tool_name_raw).strip()
    if not tn or tn.lower() in ("null", "none"):
        return None, args_in

    tn_lower = tn.lower()
    qh = question.lower()

    def _guess_processor_from_args(proc_arg: str) -> bool:
        if not proc_arg.strip():
            return False
        g = (_extract_processor_guess(question) or "").strip().lower()
        if not g:
            return False
        pa = proc_arg.strip().lower()
        return pa in g or g in pa

    def _guess_status_from_question() -> str | None:
        return _extract_status_from_question(question) or _resolved_status_from_natural_language_volume(
            question
        )

    def _chosen_status_lab(st_raw: str) -> str | None:
        raw = str(st_raw or "").strip()
        return next((lab for lab in CHAT_ALLOWED_STATUSES if lab.upper() == raw.upper()), None)

    if tn_lower == "get_returns_by_status":
        st_arg = str(args_in.get("status") or args_in.get("Status") or "").strip()
        if not st_arg or any(ch in st_arg for ch in (",", ";", "|", "/", "&")):
            return None, args_in
        chosen = _chosen_status_lab(st_arg)
        if chosen is None:
            return None, args_in

        ents_st_u = str(ents.get("status") or "").strip().upper()
        if classified_intent == "count_by_status" and ents_st_u and ents_st_u == chosen.upper():
            return tn, args_in

        hinted = _guess_status_from_question()
        if hinted:
            if hinted.upper() != chosen.upper():
                return None, args_in
            return tn, args_in
        pat = chosen.lower().replace(" ", r"\s+")
        try:
            mentioned = bool(re.search(rf"\b{pat}\b", qh))
        except re.error:
            mentioned = chosen.lower() in qh
        if not mentioned:
            return None, args_in
        return tn, args_in

    if tn_lower == "get_returns_by_processor":
        proc_arg = str(args_in.get("processor") or args_in.get("Processor") or "").strip()
        if not proc_arg:
            return None, args_in
        ip = str(ents.get("processor") or "").strip()
        if classified_intent == "count_by_processor" and ip:
            ipa, pba = ip.lower(), proc_arg.lower()
            if ipa in pba or pba in ipa:
                return tn, args_in
            return None, args_in
        pa_l = proc_arg.lower()
        if len(pa_l) >= 3 and pa_l in qh:
            return tn, args_in
        if _guess_processor_from_args(proc_arg):
            return tn, args_in
        if (
            "preparer" in qh
            or "processor" in qh
            or re.search(r"\bassigned\s+(?:to|for)\s+\w+", qh)
        ):
            return tn, args_in
        return None, args_in

    if tn_lower == "get_balance_due_returns":
        if classified_intent == "balance_due" or balance_superlative_sort_mode(question) is not None:
            return tn, args_in
        if any(
            w in qh
            for w in (
                "balance due",
                "outstanding balance",
                "still owe",
                "owe money",
                "unpaid",
                "owe a balance",
                "balance owed",
                "owe balance",
                "who owes",
            )
        ):
            return tn, args_in
        return None, args_in

    if tn_lower == "search_clients":
        qarg = str(args_in.get("query") or args_in.get("Query") or "").strip()
        if not qarg:
            return None, args_in
        qa = qarg.lower()
        cq = (_extract_client_search_query(question) or "").strip().lower()
        if classified_intent == "search_client" and cq:
            if qa in cq or cq in qa:
                return tn, args_in
            return None, args_in
        if len(qa) >= 3 and qa in qh:
            return tn, args_in
        return None, args_in

    if tn_lower == "get_missing_docs":
        try:
            rid_arg = int(str(args_in.get("return_id") or args_in.get("Return_id") or 0))
        except (TypeError, ValueError):
            return None, args_in
        if rid_arg <= 0:
            return None, args_in
        erid = ents.get("return_id")
        if erid is not None and int(erid) == rid_arg:
            return tn, args_in
        ex = _extract_return_id(question)
        if ex == rid_arg:
            return tn, args_in
        if re.search(rf"\b(?:return|missing|documents?)?\s*#?\s*{rid_arg}\b", question, re.I):
            return tn, args_in
        return None, args_in

    if tn_lower == "get_client_returns":
        try:
            cid_arg = int(str(args_in.get("client_id") or args_in.get("Client_id") or 0))
        except (TypeError, ValueError):
            return None, args_in
        if cid_arg <= 0:
            return None, args_in
        m = re.search(
            r"\bclient\s*#?\s*(\d{2,})\b|\bclient\s+id\s*(\d{2,})\b",
            question,
            re.I,
        )
        if m:
            try:
                n = int((m.group(1) or m.group(2) or "").strip())
            except (TypeError, ValueError):
                n = None
            if n is not None and n == cid_arg:
                return tn, args_in
        if classified_intent == "search_client" and str(cid_arg) in question:
            return tn, args_in
        return None, args_in

    return tn, args_in


def classify_intent(question: str) -> tuple[str, dict]:
    """
    Regex intent router. Returns (intent_name, entities dict).
    Intents: returns_with_refund_count, season_totals, dominant_status, dataplane_slice, count_by_status, count_by_processor, balance_due, missing_docs,
    search_client, fallback.
    """
    ents: dict = {}

    mi = re.search(
        r"(?:missing\s+documents?|documents?\s+missing|what\s+documents?\s+are\s+missing"
        r"|what\s+(?:documents?|is)\s+missing"
        r"|waiting\s+(?:for|on)\s+documents?)\s+(?:for\s+)?(?:return\s*#?\s*)?(\d{2,})"
        r"|(?:for\s+)?return\s*#?\s*(\d{2,}).{0,80}(?:missing|documents?|waiting)",
        question,
        re.I | re.DOTALL,
    )
    if mi:
        rid_s = mi.group(1) or mi.group(2)
        try:
            return "missing_docs", {"return_id": int(rid_s)}
        except (TypeError, ValueError):
            pass

    if balance_superlative_sort_mode(question) is not None:
        return "balance_due", {}

    if _wants_season_refund_return_count(question):
        return "returns_with_refund_count", {}

    if _wants_season_totals_aggregate_only(question):
        return "season_totals", {}

    if _wants_dominant_workflow_status(question):
        return "dominant_status", {}

    dp_slice = classify_dataplane_slice(question)
    if dp_slice:
        return "dataplane_slice", {"slice": dp_slice}

    vol_count_gate = wants_tool_row_aggregate(question) and not (
        prefers_narrative_list_answer(question) or wants_qualitative_return_answer(question)
    )
    if vol_count_gate:
        nl_status = _resolved_status_from_natural_language_volume(question)
        if nl_status:
            return "count_by_status", {"status": nl_status}

    qh = question.lower()
    aggregate_focus = wants_tool_row_aggregate(
        question
    ) and not prefers_narrative_list_answer(question)
    st = _extract_status_from_question(question)
    proc = _extract_processor_guess(question)
    proc_context = (
        "preparer" in qh
        or "processor" in qh
        or "assigned" in qh
        or re.search(r"how\s+many\s+returns?\s+does\s+", qh) is not None
    )

    if aggregate_focus:
        if proc and proc_context:
            return "count_by_processor", {"processor": proc}
        if st:
            return "count_by_status", {"status": st}

    if any(
        w in question.lower()
        for w in (
            "balance due",
            "outstanding balance",
            "still owe",
            "unpaid",
            "owe money",
            "balance owed",
            "owe a balance",
        )
    ):
        return "balance_due", {}

    if re.search(r"\bmissing\b", question.lower()) and not mi:
        rid = _extract_return_id(question)
        if rid is not None:
            return "missing_docs", {"return_id": rid}

    cq = _extract_client_search_query(question)
    if cq:
        return "search_client", {"query": cq}

    return "fallback", {}


def _read_snapshot() -> dict:
    with _cache_lock:
        return _snapshot


def ensure_chat_cache_for_year(year: int) -> None:
    """Lazy load / refresh when tax season year Changes."""
    global _lazy_init_year
    with _cache_lock:
        cached_y = _snapshot.get("season_year")
        pending = cached_y != year or _lazy_init_year is None
    if pending:
        refresh_chat_cache(year=year)
        with _cache_lock:
            _lazy_init_year = year


def refresh_chat_cache(
    conn: sqlite3.Connection | None = None, year: int | None = None
) -> None:
    """Rebuild in-memory aggregates for intake season year (calendar-based, db_tools-aligned)."""
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


def _resolve_processor_match(name: str, processor_counts: dict[str, int]) -> tuple[str, int]:
    name_l = name.strip().lower()
    if not name_l:
        return "", 0
    keys = sorted(processor_counts.keys(), key=lambda k: (-len(k or "")))
    exact = [(k, processor_counts[k]) for k in keys if k and k.strip().lower() == name_l]
    if exact:
        return exact[0]
    for k in keys:
        if not k or k == "(unassigned)":
            continue
        kl = k.lower()
        if name_l in kl or kl in name_l:
            return k, processor_counts[k]
    return "", 0


def try_deterministic_response(
    conn: sqlite3.Connection,
    intent: str,
    entities: dict,
    question: str,
    year: int,
    office_ctx_live: dict | None = None,
) -> dict | None:
    snap = _read_snapshot()
    if snap.get("season_year") != year:
        return None

    stats = snap["stats"]
    count_q = wants_tool_row_aggregate(question) and not (
        prefers_narrative_list_answer(question) or wants_qualitative_return_answer(question)
    )

    if intent == "returns_with_refund_count":
        n = count_returns_with_positive_refund_in_season(conn, year)
        return {
            "answer": (
                f"For season {year}, **{n}** distinct return(s) have a recorded refund amount "
                f"greater than zero on their payment row (import/Drake data)."
            ),
            "tool_used": None,
            "args_used": None,
            "result_count": n,
            "fast": True,
        }

    if intent == "season_totals":
        sc = (
            dict(office_ctx_live.get("status_counts") or {})
            if office_ctx_live
            else dict(stats["status_counts"])
        )
        total = sum(int(v) for v in sc.values())
        return {
            "answer": (
                f"For season {year}, there are **{total}** returns in TaxOps "
                f"(total across all workflow status buckets for this season)."
            ),
            "tool_used": None,
            "args_used": None,
            "result_count": total,
            "fast": True,
        }

    if intent == "dominant_status":
        sc_raw = (
            dict(office_ctx_live.get("status_counts") or {})
            if office_ctx_live
            else dict(stats["status_counts"])
        )
        pairs: list[tuple[str, int]] = []
        for k, raw in sc_raw.items():
            try:
                pairs.append((str(k).strip(), int(raw)))
            except (TypeError, ValueError):
                continue
        if not pairs:
            return {
                "answer": (
                    f"No workflow status counts are loaded for season {year}—nothing to rank yet."
                ),
                "tool_used": None,
                "args_used": None,
                "result_count": 0,
                "fast": True,
            }
        max_v = max(v for _, v in pairs)
        leaders = sorted([k for k, v in pairs if v == max_v], key=lambda s: s.casefold())
        total = sum(v for _, v in pairs)
        pct = round(100.0 * max_v / total, 1) if total else 0.0
        note = (
            " This is the **workflow queue status** (TaxOps bucket), not business form type "
            "(1040 vs entity) unless you ask about specific forms."
        )
        if len(leaders) == 1:
            ans = (
                f"For season {year}, the most common workflow status is **{leaders[0]}** "
                f"with **{max_v}** return(s) ({pct}% of **{total}** return(s) in the season roll-up)."
                f"{note}"
            )
        else:
            joined = " and ".join(f"**{x}**" for x in leaders)
            ans = (
                f"For season {year}, {joined} tie for the most common workflow status "
                f"at **{max_v}** return(s) each ({pct}% of **{total}** when tied)."
                f"{note}"
            )
        return {
            "answer": ans,
            "tool_used": None,
            "args_used": None,
            "result_count": max_v,
            "fast": True,
        }

    if intent == "dataplane_slice":
        sl = str(entities.get("slice") or "").strip()
        if not sl:
            return None
        dp = snap.get("dataplane")
        if not isinstance(dp, dict) or int(dp.get("season_year") or -1) != year:
            dp = gather_season_dataplane(conn, year)
        body = format_dataplane_slice_answer(sl, dp if isinstance(dp, dict) else {}, year)
        if not isinstance(body, str) or not body.strip():
            return None
        return {
            "answer": body.strip(),
            "tool_used": None,
            "args_used": {"slice": sl},
            "result_count": int((dp.get("forms") or {}).get("returns_in_season") or 0)
            if isinstance(dp, dict)
            else 0,
            "fast": True,
        }

    if intent == "count_by_status" and count_q:
        status = entities.get("status") or _extract_status_from_question(question)
        if not status:
            return None
        scope = (
            dict(office_ctx_live.get("status_counts") or {})
            if office_ctx_live
            else dict(stats["status_counts"])
        )
        n = _aggregate_status_lookup(scope, status)
        count = int(n) if n is not None else 0

        if status == "REJECTED" and wants_rejection_reason_breakdown(question):
            summ = summarize_season_rejections_for_chat(conn, year)
            total_rb = int(summ.get("total") or count)
            subj = "return" if total_rb == 1 else "returns"
            verb = "is" if total_rb == 1 else "are"
            head = f"There {verb} **{total_rb}** {subj} in REJECTED status for {year}."
            body = format_season_rejection_breakdown_for_chat(summ, year)
            combined = head if not body.strip() else f"{head}\n\n{body}"
            return {
                "answer": combined,
                "tool_used": None,
                "args_used": None,
                "result_count": total_rb,
                "fast": True,
            }

        ql = question.lower()
        clarify = ""
        if status == "LOG OUT" and re.search(
            r"\b(?:e[-\s]?filed|fully\s+e[-\s]?file(?:d)?)\b",
            ql,
        ):
            clarify = (
                " In TaxOps, **LOG OUT** is the completed workflow bucket (not `EFILE` or "
                "`EFILE READY`)."
            )
        return {
            "answer": f"There are {count} returns in {status} status for {year}.{clarify}",
            "tool_used": None,
            "args_used": None,
            "result_count": count,
            "fast": True,
        }

    if intent == "count_by_processor" and count_q:
        proc_hint = entities.get("processor") or _extract_processor_guess(question)
        if not proc_hint:
            return None
        pmap_live = (
            dict(office_ctx_live.get("processor_return_counts") or {})
            if office_ctx_live
            else dict(stats["processor_counts"])
        )
        pname, count = _resolve_processor_match(proc_hint, pmap_live)
        if count == 0:
            return None
        return {
            "answer": f"{pname} has {count} returns for {year}.",
            "tool_used": None,
            "args_used": None,
            "result_count": count,
            "fast": True,
        }

    if intent == "balance_due":
        total = (
            int(office_ctx_live.get("balance_due_season_total") or 0)
            if office_ctx_live
            else int(stats["balance_due_total"])
        )
        rank_mode = balance_superlative_sort_mode(question)
        if rank_mode is not None:
            owed_rows = [
                scrub_ssn_from_dict(dict(r))
                for r in get_balance_due_returns(conn, year)
            ]
            if not owed_rows:
                return {
                    "answer": f"No returns with an outstanding balance were found for {year}.",
                    "tool_used": "get_balance_due_returns",
                    "args_used": {"year": year},
                    "result_count": 0,
                    "fast": True,
                }
            owed_rows.sort(
                key=_balance_still_due_numeric,
                reverse=(rank_mode == "high"),
            )
            apex = _balance_still_due_numeric(owed_rows[0])
            tier: list[dict] = []
            for rr in owed_rows:
                if abs(_balance_still_due_numeric(rr) - apex) < 0.005:
                    tier.append(rr)
                else:
                    break
            super_head = (
                "Highest unpaid fee balance"
                if rank_mode == "high"
                else "Lowest unpaid fee (among returns that still owe)"
            )
            parts: list[str] = []
            for rr in tier[:12]:
                nm = rr.get("display_name") or "—"
                lg = rr.get("log_number") if rr.get("log_number") is not None else "—"
                amt = _balance_still_due_numeric(rr)
                tf = rr.get("total_fee")
                fp = rr.get("fee_paid")
                parts.append(
                    f"{nm} (log {lg}): ${amt:,.2f} still due"
                    + (
                        f" (fee {_money_display(tf)}, paid {_money_display(fp)})"
                        if tf is not None or fp is not None
                        else ""
                    )
                )
            joint = "; ".join(parts)
            n_tied = len(tier)
            tie_note = ""
            if n_tied > 1:
                tie_note = f" ({n_tied} returns tied at ${apex:,.2f})."
            elif len(owed_rows) > 1 and rank_mode == "high":
                tie_note = " (among returns with unpaid fees)."
            return {
                "answer": f"{super_head} for season {year} (${apex:,.2f}): {joint}.{tie_note}",
                "tool_used": "get_balance_due_returns",
                "args_used": {"year": year},
                "result_count": total,
                "fast": True,
            }

        if count_q:
            return {
                "answer": f"There are {total} returns with a balance still due for {year}.",
                "tool_used": "get_balance_due_returns",
                "args_used": {"year": year},
                "result_count": total,
                "fast": True,
            }
        rows = snap["balance_due_rows"][:25]
        if not rows:
            return {
                "answer": f"No returns with an outstanding balance were found for {year}.",
                "tool_used": "get_balance_due_returns",
                "args_used": {"year": year},
                "result_count": 0,
                "fast": True,
            }
        lines = []
        for r in rows:
            nm = r.get("display_name") or "—"
            lg = r.get("log_number") or "—"
            lines.append(f"Log {lg}: {nm}")
        body = "; ".join(lines)
        return {
            "answer": f"Returns with a balance due for {year} (first {len(rows)}): {body}.",
            "tool_used": "get_balance_due_returns",
            "args_used": {"year": year},
            "result_count": total,
            "fast": True,
        }

    if intent == "missing_docs":
        rid = entities.get("return_id")
        if rid is None:
            rid = _extract_return_id(question)
        if rid is None:
            return None
        rows = get_missing_docs(conn, int(rid))
        safe = [scrub_ssn_from_dict(dict(r)) for r in rows]
        n = len(safe)
        if not safe:
            return {
                "answer": f"No unresolved missing documents are listed for return {rid}.",
                "tool_used": "get_missing_docs",
                "args_used": {"return_id": int(rid)},
                "result_count": 0,
                "fast": True,
            }
        items = ", ".join(str(x.get("item_text") or "") for x in safe[:20])
        return {
            "answer": f"Return {rid} is missing {n} item(s): {items}.",
            "tool_used": "get_missing_docs",
            "args_used": {"return_id": int(rid)},
            "result_count": n,
            "fast": True,
        }

    if intent == "search_client":
        q = entities.get("query") or _extract_client_search_query(question)
        if not q:
            return None
        rows = search_clients(conn, q)
        safe = [scrub_ssn_from_dict(dict(r)) for r in rows[:20]]
        n = len(safe)
        if not safe:
            return {
                "answer": f"No clients matched “{q}”.",
                "tool_used": "search_clients",
                "args_used": {"query": q},
                "result_count": 0,
                "fast": True,
            }
        parts = [f"{r.get('display_name') or '—'} (id {r.get('id')})" for r in safe]
        return {
            "answer": f"Found {n} client(s): " + "; ".join(parts),
            "tool_used": "search_clients",
            "args_used": {"query": q},
            "result_count": n,
            "fast": True,
        }

    return None
