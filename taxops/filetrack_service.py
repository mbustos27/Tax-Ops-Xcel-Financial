"""filetrack_service — TaxOps-side integration for the M3 filetrack feature.

This module is the ONE seam that imports both TaxOps internals (db.py) and
filetrack.config. filetrack.labels and filetrack.listener must never import
this module or anything else TaxOps-specific (db, app, config.py at the repo
root) — that boundary is what keeps them standalone-testable.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from db import get_connection
from filetrack.config import ALLOWED_STATUSES, format_log_number

logger = logging.getLogger("filetrack")


class FiletrackStatusError(Exception):
    """Raised when apply_filetrack_status() is asked to apply a status name
    that isn't in filetrack.config.ALLOWED_STATUSES. The caller (the
    /filetrack/status endpoint) turns this into an HTTP 400 — an unknown
    status is a caller/config bug, never silently coerced or ignored."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def apply_filetrack_status(
    log_number,
    new_status: str,
    *,
    scanned_at: Optional[str] = None,
    source: str = "scanner",
    conn=None,
) -> dict:
    """Apply one scanner-confirmed status to the return identified by
    `log_number`.

    Behavior (mirrors the status_events pattern used for returns.client_status):
      - Validates `new_status` against filetrack.config.ALLOWED_STATUSES.
        Raises FiletrackStatusError on an unknown name — never coerced,
        never silently dropped.
      - Resolves the return by log_number, trying the canonical zero-padded
        form first, then the raw/un-padded form (older rows may predate the
        5-digit convention).
      - If a return IS found: updates returns.filetrack_status /
        filetrack_status_updated_at, and records old_status from the
        pre-update value.
      - If NO return matches (a physical folder scanned before being logged
        into TaxOps, a mis-scanned barcode, etc.): still writes exactly one
        filetrack_status_history row with return_id=NULL, so the event is
        visible for triage rather than silently discarded. This function
        does NOT raise for an unmatched log_number — a scan-station operator
        has no way to "fix" a 400 from the scanner itself.
      - Every call writes exactly one filetrack_status_history row.
      - If `conn` is supplied, the caller owns the transaction (no commit/
        rollback/close here) — used by tests and any future caller that
        wants to batch this with other writes. Otherwise this function opens,
        commits/rolls back, and closes its own connection.

    Returns:
      {"matched": bool, "return_id": int|None, "old_status": str|None,
       "new_status": str}
    """
    if not isinstance(new_status, str) or new_status.strip().upper() not in ALLOWED_STATUSES:
        raise FiletrackStatusError(f"Unknown filetrack status: {new_status!r}")
    status = new_status.strip().upper()

    formatted_log = format_log_number(log_number)
    raw_log = str(log_number).strip()
    now = _now_iso()
    scanned_at_value = scanned_at or now

    owns_conn = conn is None
    active_conn = conn or get_connection()
    try:
        row = active_conn.execute(
            "SELECT id, filetrack_status FROM returns WHERE log_number = ?",
            (formatted_log,),
        ).fetchone()
        if row is None and raw_log != formatted_log:
            row = active_conn.execute(
                "SELECT id, filetrack_status FROM returns WHERE log_number = ?",
                (raw_log,),
            ).fetchone()

        return_id = row["id"] if row is not None else None
        old_status = row["filetrack_status"] if row is not None else None

        if return_id is not None:
            active_conn.execute(
                "UPDATE returns SET filetrack_status = ?, filetrack_status_updated_at = ? WHERE id = ?",
                (status, now, return_id),
            )
        else:
            logger.warning(
                "filetrack: no return found for log_number=%s (status=%s) — "
                "recorded in filetrack_status_history only",
                formatted_log, status,
            )

        active_conn.execute(
            """
            INSERT INTO filetrack_status_history
              (return_id, log_number, old_status, new_status, source, scanned_at, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (return_id, formatted_log, old_status, status, source, scanned_at_value, now),
        )

        if owns_conn:
            active_conn.commit()

        return {
            "matched": return_id is not None,
            "return_id": return_id,
            "old_status": old_status,
            "new_status": status,
        }
    except Exception:
        if owns_conn:
            active_conn.rollback()
        raise
    finally:
        if owns_conn:
            active_conn.close()


def get_filetrack_history(log_number, *, limit: int = 50, conn=None) -> list[dict]:
    """Read-only helper: the filetrack_status_history timeline for one
    log_number, most recent first. Used by future UI/debug tooling — not
    required by M3's endpoint itself, but kept here so callers never need to
    hand-write this query."""
    formatted_log = format_log_number(log_number)
    owns_conn = conn is None
    active_conn = conn or get_connection()
    try:
        rows = active_conn.execute(
            """
            SELECT id, return_id, log_number, old_status, new_status, source, scanned_at, recorded_at
            FROM filetrack_status_history
            WHERE log_number = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (formatted_log, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        if owns_conn:
            active_conn.close()
