"""AUDIT-2…8 — Request audit trail, masking, append-only storage, retention purge.

AUDIT-8: Application code must never UPDATE or DELETE individual audit_log rows.
The only DELETE from audit_log is purge_audit_logs_older_than() below (retention).
"""

from __future__ import annotations

import csv
import io
import json
import logging
import queue
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from difflib import unified_diff
from typing import Any

from flask import g, has_request_context, request

from config import AUDIT_LOGGING_ENABLED, AUDIT_RETENTION_YEARS_DEFAULT
from db import get_connection
from utils import _scrub_ssn_from_string, now

log = logging.getLogger(__name__)

SETTINGS_KEY_AUDIT_RETENTION = "audit_retention_years"

# Cap JSON stored per row (bytes, UTF-8) after masking
_AUDIT_JSON_MAX_BYTES = 450_000

# REL-2: single long-lived writer thread drains this queue.
# Bounded at 4096 so a burst never grows without limit; overflow drops with a warning.
_AUDIT_QUEUE_MAXSIZE = 4096
_audit_queue: queue.Queue = queue.Queue(maxsize=_AUDIT_QUEUE_MAXSIZE)
_audit_writer_started = False
_audit_writer_lock = threading.Lock()


def _audit_writer_loop() -> None:
    """REL-2: drain _audit_queue in a single long-lived daemon thread.

    Batches up to 64 rows per commit to amortise SQLite transaction overhead
    while keeping individual write latency low under normal traffic.
    """
    BATCH = 64
    while True:
        batch: list[tuple] = []
        try:
            # Block until at least one item arrives.
            item = _audit_queue.get(timeout=5)
            batch.append(item)
            # Drain additional items without blocking.
            while len(batch) < BATCH:
                try:
                    batch.append(_audit_queue.get_nowait())
                except queue.Empty:
                    break
        except queue.Empty:
            continue

        try:
            conn = get_connection()
            try:
                conn.executemany(
                    """
                    INSERT INTO audit_log
                      (user_id, action, entity_type, entity_id,
                       before_json, after_json, ip_address, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch,
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            log.exception("audit_log batch insert failed (%d rows dropped)", len(batch))
        finally:
            for _ in batch:
                _audit_queue.task_done()


def start_audit_writer() -> None:
    """REL-2: start the single audit writer thread (idempotent)."""
    global _audit_writer_started
    with _audit_writer_lock:
        if _audit_writer_started:
            return
        _audit_writer_started = True
    t = threading.Thread(target=_audit_writer_loop, name="audit-log-writer", daemon=True)
    t.start()
    log.info("REL-2: audit writer thread started")


def audit_queue_depth() -> int:
    """REL-2: current number of pending audit writes (used by /health)."""
    return _audit_queue.qsize()

# Bulk snapshot limits (before/after payloads)
_BULK_ID_CAP = 120

_FINANCIAL_SUBSTRINGS = (
    "fee", "balance", "refund", "deposit", "receipt", "zelle", "routing", "account",
    "ssn", "paid", "collect", "discount", "estimate", "cash", "credit", "invoice",
    "price", "amt", "amount", "payment", "tin", "ein", "routing", "bank_", "down_",
)

_EXACT_SENSITIVE_KEYS = frozenset(
    x.lower()
    for x in (
        "ssn_last4", "social_security_number", "password", "pass", "creditcard",
        "credit_card", "card_number", "cvv", "bank_account", "bank_routing",
        "routing_number", "account_number", "zelle_or_check_ref", "cash_or_qpay_ref",
    )
)


def get_audit_retention_years(conn) -> int:
    row = conn.execute(
        "SELECT value FROM app_settings WHERE key = ?",
        (SETTINGS_KEY_AUDIT_RETENTION,),
    ).fetchone()
    if row and str(row["value"]).strip():
        try:
            y = int(str(row["value"]).strip())
            return max(1, min(50, y))
        except ValueError:
            pass
    return AUDIT_RETENTION_YEARS_DEFAULT


def set_audit_retention_years(conn, years: int) -> None:
    y = max(1, min(50, int(years)))
    ts = now()
    conn.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (SETTINGS_KEY_AUDIT_RETENTION, str(y), ts),
    )


def mask_audit_payload(obj: Any) -> Any:
    """AUDIT-5 — redact SSN/financial patterns in nested JSON-like structures."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            kl = str(k).lower()
            if kl in _EXACT_SENSITIVE_KEYS or any(s in kl for s in _FINANCIAL_SUBSTRINGS):
                out[k] = "[REDACTED]"
            else:
                out[k] = mask_audit_payload(v)
        return out
    if isinstance(obj, list):
        return [mask_audit_payload(x) for x in obj]
    if isinstance(obj, str):
        return _scrub_ssn_from_string(obj)
    return obj


def _json_bytes_trimmed(obj: Any) -> str:
    raw = json.dumps(obj, default=str, ensure_ascii=False)
    if len(raw.encode("utf-8")) <= _AUDIT_JSON_MAX_BYTES:
        return raw
    return raw[:_AUDIT_JSON_MAX_BYTES] + "\n…[truncated]…"


def _row_to_dict(row) -> dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def fetch_return_bundle(conn, return_id: int) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM returns WHERE id = ?", (return_id,)).fetchone()
    if not r:
        return None
    rd = _row_to_dict(r)
    cid = rd.get("client_id")
    cd: dict[str, Any] | None = None
    if cid is not None:
        cr = conn.execute("SELECT * FROM clients WHERE id = ?", (int(cid),)).fetchone()
        if cr:
            cd = _row_to_dict(cr)
    return {"return": rd, "client": cd}


def fetch_multi_return_bundles(conn, ids: list[int]) -> dict[str, Any]:
    cap = _BULK_ID_CAP
    sliced = ids[:cap]
    out: dict[str, Any] = {"returns": {}}
    for rid in sliced:
        b = fetch_return_bundle(conn, rid)
        if b:
            out["returns"][str(rid)] = b
    if len(ids) > cap:
        out["_truncated"] = True
        out["_total_ids"] = len(ids)
    return out


def fetch_client_bundle(conn, client_id: int) -> dict[str, Any] | None:
    cr = conn.execute("SELECT * FROM clients WHERE id = ?", (client_id,)).fetchone()
    if not cr:
        return None
    return {"client": _row_to_dict(cr)}


def _request_ip() -> str | None:
    if not has_request_context():
        return None
    return (request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip() or None


def _should_skip_audit() -> bool:
    if not AUDIT_LOGGING_ENABLED:
        return True
    if not has_request_context():
        return True
    m = request.method.upper()
    if m not in ("POST", "PUT", "PATCH", "DELETE"):
        return True
    p = request.path or ""
    if p.startswith("/static/"):
        return True
    if p in ("/health",) or p.startswith("/favicon"):
        return True
    if p == "/login" and m == "POST":
        return True
    if p == "/api/client-error":
        return True

    try:
        from flask import session
    except Exception:
        return True
    if not session.get("logged_in"):
        return True

    return False


def _build_action() -> str:
    ep = request.endpoint or "?"
    return f"{request.method.upper()} {ep}"


def _capture_before_for_request(conn) -> dict[str, Any]:
    ep = request.endpoint or ""
    va = dict(request.view_args or {})
    meta: dict[str, Any] = {
        "action": _build_action(),
        "path": request.path,
        "endpoint": ep,
    }

    if "return_id" in va:
        rid = int(va["return_id"])
        meta["entity_type"] = "return"
        meta["entity_id"] = str(rid)
        meta["before"] = fetch_return_bundle(conn, rid)
        return meta

    if "client_id" in va:
        cid = int(va["client_id"])
        meta["entity_type"] = "client"
        meta["entity_id"] = str(cid)
        meta["before"] = fetch_client_bundle(conn, cid)
        return meta

    # Bulk return endpoints
    if ep in ("api_returns_bulk_status", "api_returns_bulk_processor"):
        body = request.get_json(silent=True) or {}
        raw_ids = body.get("return_ids")
        ids: list[int] = []
        if isinstance(raw_ids, list):
            for x in raw_ids:
                try:
                    ids.append(int(x))
                except (TypeError, ValueError):
                    pass
        meta["entity_type"] = "return_bulk"
        meta["entity_id"] = ",".join(str(i) for i in ids[:20]) + (
            f",…(+{len(ids) - 20})" if len(ids) > 20 else ""
        )
        meta["before"] = fetch_multi_return_bundles(conn, ids) if ids else {"returns": {}}
        return meta

    # Merge clients API
    if ep in ("api_merge_clients", "api_merge_clients_bulk", "api_audit_merge_client"):
        body = request.get_json(silent=True) or {}
        meta["entity_type"] = "merge_clients"
        meta["entity_id"] = json.dumps(body, default=str)[:500]
        meta["before"] = {"payload_preview": mask_audit_payload(body)}
        return meta

    # Phase 3.4: dead-letter requeue — capture doc_id/return_id/status/attempts
    # explicitly so "acting user, doc_id, return_id, timestamp" is answerable
    # straight from audit_log without cross-referencing extraction_queue.
    if ep == "email_health.api_email_health_requeue" and "eq_id" in va:
        eqid = int(va["eq_id"])
        meta["entity_type"] = "extraction_queue"
        meta["entity_id"] = str(eqid)
        row = conn.execute(
            "SELECT id, doc_id, return_id, status, attempts, error_message "
            "FROM extraction_queue WHERE id = ?",
            (eqid,),
        ).fetchone()
        meta["before"] = dict(row) if row else None
        return meta

    # Phase 3.5: soft-delete restore — scoped by endpoint name (not the bare
    # "item_id" view-arg, which the assign/delete/file email-inbox routes
    # also use) so this does not change auditing behavior for those routes.
    if ep == "api_email_inbox_restore" and "item_id" in va:
        iid = int(va["item_id"])
        meta["entity_type"] = "email_inbox"
        meta["entity_id"] = str(iid)
        row = conn.execute(
            "SELECT id, is_deleted, filename, received_at FROM email_inbox WHERE id = ?",
            (iid,),
        ).fetchone()
        meta["before"] = dict(row) if row else None
        return meta

    meta["entity_type"] = "http_request"
    meta["entity_id"] = ep
    meta["before"] = {
        "path": request.path,
        "query_string": (request.query_string or b"").decode("utf-8", "replace")[:2000],
    }
    return meta


def _capture_after_for_meta(conn, meta: dict[str, Any]) -> Any:
    et = meta.get("entity_type")
    if et == "return" and meta.get("entity_id"):
        return fetch_return_bundle(conn, int(meta["entity_id"]))
    if et == "client" and meta.get("entity_id"):
        return fetch_client_bundle(conn, int(meta["entity_id"]))
    if et == "return_bulk" and isinstance(meta.get("before"), dict):
        before = meta["before"]
        ids: list[int] = []
        if "returns" in before:
            ids = [int(k) for k in before["returns"].keys()]
        # Re-parse body ids for completeness
        body = request.get_json(silent=True) or {}
        raw_ids = body.get("return_ids")
        if isinstance(raw_ids, list):
            for x in raw_ids:
                try:
                    i = int(x)
                    if i not in ids:
                        ids.append(i)
                except (TypeError, ValueError):
                    pass
        return fetch_multi_return_bundles(conn, ids)
    if et == "extraction_queue" and meta.get("entity_id"):
        row = conn.execute(
            "SELECT id, doc_id, return_id, status, attempts, error_message "
            "FROM extraction_queue WHERE id = ?",
            (int(meta["entity_id"]),),
        ).fetchone()
        return dict(row) if row else None
    if et == "email_inbox" and meta.get("entity_id"):
        row = conn.execute(
            "SELECT id, is_deleted, filename, received_at FROM email_inbox WHERE id = ?",
            (int(meta["entity_id"]),),
        ).fetchone()
        return dict(row) if row else None
    return {"note": "after snapshot not captured for this entity_type"}


def _enqueue_write(
    *,
    user_id: str | None,
    action: str,
    entity_type: str,
    entity_id: str | None,
    before: Any,
    after: Any,
    ip_address: str | None,
    http_status: int,
) -> None:
    """REL-2: build the row tuple and push it onto the shared audit queue.

    The single _audit_writer_loop thread does all SQLite I/O, eliminating
    per-request thread spawning.  Masking/serialisation still happens here
    (on the request thread) so the writer stays lightweight.
    """
    try:
        b = mask_audit_payload(before)
        a = mask_audit_payload(after)
        payload_meta = {"http_status": http_status}
        if isinstance(a, dict):
            a = {**a, "_response": payload_meta}
        else:
            a = {"_value": a, "_response": payload_meta}
        before_s = _json_bytes_trimmed(b) if b is not None else None
        after_s = _json_bytes_trimmed(a) if a is not None else None
        row = (user_id, action, entity_type, entity_id, before_s, after_s, ip_address, now())
        try:
            _audit_queue.put_nowait(row)
        except queue.Full:
            log.warning(
                "audit_queue full (%d); dropping write for action=%s entity=%s/%s",
                _AUDIT_QUEUE_MAXSIZE, action, entity_type, entity_id,
            )
    except Exception:
        log.exception("audit_log _enqueue_write serialisation failed")


def audit_before_request() -> None:
    if _should_skip_audit():
        return
    try:
        conn = get_connection()
        try:
            meta = _capture_before_for_request(conn)
            g._audit_ctx = meta
        finally:
            conn.close()
    except Exception:
        log.exception("audit_before_request failed")
        g._audit_ctx = None


def audit_after_request(response):
    ctx = getattr(g, "_audit_ctx", None)
    if not ctx:
        return response
    try:
        user_id = None
        try:
            from flask import session

            user_id = (session.get("username") or "").strip() or None
        except Exception:
            pass
        action = str(ctx.get("action") or _build_action())
        entity_type = str(ctx.get("entity_type") or "http_request")
        entity_id = ctx.get("entity_id")
        eid_str = None if entity_id is None else str(entity_id)[:2000]

        status = getattr(response, "status_code", 0) or 0
        after: Any = None
        if status < 400:
            try:
                conn = get_connection()
                try:
                    after = _capture_after_for_meta(conn, ctx)
                finally:
                    conn.close()
            except Exception:
                log.exception("audit after snapshot failed")
                after = {"_error": "after_snapshot_failed"}
        else:
            after = {"http_status": status, "skipped_full_after": True}

        _enqueue_write(
            user_id=user_id,
            action=action,
            entity_type=entity_type,
            entity_id=eid_str,
            before=ctx.get("before"),
            after=after,
            ip_address=_request_ip(),
            http_status=int(status),
        )
    except Exception:
        log.exception("audit_after_request orchestration failed")
    return response


def register_audit_hooks(app) -> None:
    app.before_request(audit_before_request)
    app.after_request(audit_after_request)
    _start_retention_daemon()


_retention_daemon_started = False
_retention_lock = threading.Lock()


def _start_retention_daemon() -> None:
    global _retention_daemon_started
    with _retention_lock:
        if _retention_daemon_started:
            return
        _retention_daemon_started = True

    def loop() -> None:
        time.sleep(30)
        while True:
            try:
                conn = get_connection()
                try:
                    yrs = get_audit_retention_years(conn)
                    n = purge_audit_logs_older_than(conn, yrs)
                    if n:
                        conn.commit()
                        log.info("audit retention purge removed %s rows (>%s yr)", n, yrs)
                finally:
                    conn.close()
            except Exception:
                log.exception("audit retention loop error")
            time.sleep(24 * 3600)

    threading.Thread(target=loop, name="audit-retention", daemon=True).start()


def purge_audit_logs_older_than(conn, years: int) -> int:
    """AUDIT-7 — delete rows older than policy. Only DELETE allowed on audit_log."""
    yrs = max(1, min(50, int(years)))
    cutoff = datetime.now(timezone.utc) - timedelta(days=365 * yrs)
    cutoff_s = cutoff.isoformat(timespec="seconds")
    cur = conn.execute("DELETE FROM audit_log WHERE created_at < ?", (cutoff_s,))
    return cur.rowcount if cur.rowcount is not None else 0


def query_audit_logs(
    conn,
    *,
    user_id: str | None = None,
    action_contains: str | None = None,
    entity_type: str | None = None,
    entity_id: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 75,
    offset: int = 0,
):
    clauses: list[str] = ["1=1"]
    params: list[Any] = []

    if user_id:
        clauses.append("user_id = ?")
        params.append(user_id)
    if action_contains:
        clauses.append("action LIKE ?")
        params.append(f"%{action_contains}%")
    if entity_type:
        clauses.append("entity_type = ?")
        params.append(entity_type)
    if entity_id:
        clauses.append("entity_id LIKE ?")
        params.append(f"%{entity_id}%")
    if date_from:
        clauses.append("created_at >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("created_at < ?")
        params.append(date_to)

    where = " AND ".join(clauses)

    cnt = conn.execute(f"SELECT COUNT(*) n FROM audit_log WHERE {where}", params).fetchone()["n"]

    rows = conn.execute(
        f"""
        SELECT id, user_id, action, entity_type, entity_id, created_at
        FROM audit_log
        WHERE {where}
        ORDER BY id DESC
        LIMIT ? OFFSET ?
        """,
        (*params, limit, offset),
    ).fetchall()
    return cnt, rows


def fetch_audit_entry(conn, entry_id: int):
    return conn.execute("SELECT * FROM audit_log WHERE id = ?", (entry_id,)).fetchone()


def format_json_diff(before_raw: str | None, after_raw: str | None) -> list[str]:
    def _prep(raw: str | None) -> list[str]:
        if not raw or not raw.strip():
            return ["(empty)\n"]
        try:
            obj = json.loads(raw)
            txt = json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False)
            return txt.splitlines(keepends=True)
        except json.JSONDecodeError:
            return [raw if raw.endswith("\n") else raw + "\n"]

    a = _prep(before_raw)
    b = _prep(after_raw)
    return list(unified_diff(a, b, fromfile="before", tofile="after", lineterm=""))


def format_json_diff_styled_chunks(
    before_raw: str | None, after_raw: str | None
) -> list[tuple[str, str]]:
    chunks: list[tuple[str, str]] = []
    for line in format_json_diff(before_raw, after_raw):
        if line.startswith("+++ ") or line.startswith("--- ") or line.startswith("@@"):
            cls = "text-slate-400"
        elif line.startswith("+") and not line.startswith("+++"):
            cls = "text-green-400"
        elif line.startswith("-") and not line.startswith("---"):
            cls = "text-red-400"
        else:
            cls = "text-slate-200"
        chunks.append((cls, line))
    return chunks


def write_audit_export_csv(conn, *, filters: dict[str, Any]) -> io.StringIO:
    """AUDIT-6 — CSV for filtered audit rows (bounded)."""
    max_rows = 20_000
    _, rows = query_audit_logs(
        conn,
        user_id=filters.get("user_id"),
        action_contains=filters.get("action_contains"),
        entity_type=filters.get("entity_type"),
        entity_id=filters.get("entity_id"),
        date_from=filters.get("date_from"),
        date_to=filters.get("date_to"),
        limit=max_rows,
        offset=0,
    )

    ids = [r["id"] for r in rows]
    by_id: dict[int, dict] = {}
    if ids:
        qmarks = ",".join("?" for _ in ids)
        full = conn.execute(
            f"SELECT * FROM audit_log WHERE id IN ({qmarks})",
            ids,
        ).fetchall()
        by_id = {r["id"]: _row_to_dict(r) for r in full}

    buf = io.StringIO()
    w = csv.writer(buf, quoting=csv.QUOTE_ALL)
    w.writerow(
        [
            "id",
            "created_at",
            "user_id",
            "action",
            "entity_type",
            "entity_id",
            "ip_address",
            "before_json",
            "after_json",
        ]
    )
    for i in ids:
        d = by_id.get(i, {})
        w.writerow(
            [
                d.get("id"),
                d.get("created_at"),
                d.get("user_id"),
                d.get("action"),
                d.get("entity_type"),
                d.get("entity_id"),
                d.get("ip_address"),
                d.get("before_json"),
                d.get("after_json"),
            ]
        )
    buf.seek(0)
    return buf


def sanitize_filename_audit(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", name)[:120] or "audit"
