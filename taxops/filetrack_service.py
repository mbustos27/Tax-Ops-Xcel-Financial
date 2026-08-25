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


# Mirrors app.py's STATUS_DATE_STAMP exactly, for the same reason
# filetrack.config.ALLOWED_STATUSES is a deliberate copy of app.py's
# STATUS_FLOW rather than an import: filetrack.labels/filetrack.listener must
# stay importable with zero TaxOps imports, and this module is the one
# permitted seam into app-side behavior. If app.py's STATUS_DATE_STAMP
# changes, update this to match (see also the 2026-07-22 client_status-sync
# change below, and consider adding a consistency test between the two).
_STATUS_DATE_STAMP = {
    "PICKUP": "pickup_date",
    "LOG OUT": "logout_date",
}


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

    Behavior:
      - Validates `new_status` against filetrack.config.ALLOWED_STATUSES.
        Raises FiletrackStatusError on an unknown name — never coerced,
        never silently dropped.
      - Resolves the return by log_number, trying in order: (1) the
        canonical zero-padded form, (2) the raw/as-received form (helps only
        when the CALLER passed something unpadded, e.g. a manual API test —
        a real scan's payload is always already zero-padded, since the
        barcode itself was printed via format_log_number(), so this branch
        is effectively dead for live scanner traffic), (3) the fully
        zero-stripped form (str(int(...))) — added 2026-07-22 after a real
        return (log_number stored unpadded as "1282") failed to match a scan
        of its own zero-padded label ("01282"): the scanned payload was
        already zero-padded so (2) never even queried "1282", and (1)'s
        padded query didn't match the unpadded DB value either. (3) closes
        that gap regardless of which side (scan input vs. DB row) has the
        padding.
      - If a return IS found:
          * updates returns.filetrack_status / filetrack_status_updated_at
            (the scan-specific shadow field — old_status/new_status in the
            return value and in filetrack_status_history always refer to
            THIS field, unchanged from before this function also drove the
            real workflow status — see below).
          * ALSO updates returns.client_status — the same field
            app.py's POST /api/return/<id>/status (api_status) drives, i.e.
            the status every screen in the app actually displays. Added
            2026-07-22: before this, a scan reached the server and was
            recorded, but nothing a staff member looks at ever moved,
            because filetrack_status was a write-only shadow column with no
            UI. Mirrors api_status's side effects exactly, MINUS its
            receptionist role-gate transition restriction, which does not
            apply here — a scan has no user/role attached at all (see
            routes/filetrack.py's module docstring: this endpoint is
            deliberately NOT session/RBAC), it is a machine reporting what
            just physically happened to a folder:
              - sets the STATUS_DATE_STAMP-mirrored date field
                (pickup_date/logout_date) the first time that status is
                reached, exactly like api_status.
              - writes one status_events row (event_type='STATUS_CHANGED',
                source_file='FILETRACK' so it's distinguishable from a
                staff-driven 'APP' row in the return's timeline), deduped
                by (return_id, event_type, event_timestamp) the same way
                api_status dedupes.
              - clears/sets contact_status + last_contacted_date on
                entering/leaving REJECTED, identically to api_status.
      - If NO return matches (a physical folder scanned before being logged
        into TaxOps, a mis-scanned barcode, etc.): still writes exactly one
        filetrack_status_history row with return_id=NULL, so the event is
        visible for triage rather than silently discarded. This function
        does NOT raise for an unmatched log_number — a scan-station operator
        has no way to "fix" a 400 from the scanner itself. Nothing
        client_status-related happens for an unmatched scan (there is no
        return row to update).
      - Every call writes exactly one filetrack_status_history row.
      - If `conn` is supplied, the caller owns the transaction (no commit/
        rollback/close here) — used by tests and any future caller that
        wants to batch this with other writes. Otherwise this function opens,
        commits/rolls back, and closes its own connection.

    Returns:
      {"matched": bool, "return_id": int|None, "old_status": str|None,
       "new_status": str, "client_status_synced": bool}
      old_status/new_status describe filetrack_status (unchanged meaning);
      client_status_synced is True iff a return was matched and its
      client_status was updated to `new_status` alongside it.
    """
    if not isinstance(new_status, str) or new_status.strip().upper() not in ALLOWED_STATUSES:
        raise FiletrackStatusError(f"Unknown filetrack status: {new_status!r}")
    status = new_status.strip().upper()

    formatted_log = format_log_number(log_number)
    raw_log = str(log_number).strip()
    now = _now_iso()
    today_iso = now[:10]  # now is always "YYYY-MM-DDTHH:MM:SS+00:00"
    scanned_at_value = scanned_at or now

    owns_conn = conn is None
    active_conn = conn or get_connection()
    try:
        def _lookup(value: str):
            return active_conn.execute(
                "SELECT id, filetrack_status, client_status, pickup_date, logout_date "
                "FROM returns WHERE log_number = ?",
                (value,),
            ).fetchone()

        row = _lookup(formatted_log)
        tried = {formatted_log}
        if row is None and raw_log not in tried:
            row = _lookup(raw_log)
            tried.add(raw_log)
        if row is None and formatted_log.isdigit():
            stripped = str(int(formatted_log))  # "01282" -> "1282", "00007" -> "7"
            if stripped not in tried:
                row = _lookup(stripped)
                tried.add(stripped)

        return_id = row["id"] if row is not None else None
        old_status = row["filetrack_status"] if row is not None else None
        old_client_status = row["client_status"] if row is not None else None

        if return_id is not None:
            date_field = _STATUS_DATE_STAMP.get(status)
            if date_field and not row[date_field]:
                active_conn.execute(
                    f"UPDATE returns SET filetrack_status = ?, filetrack_status_updated_at = ?, "
                    f"client_status = ?, {date_field} = ?, updated_at = ? WHERE id = ?",
                    (status, now, status, today_iso, now, return_id),
                )
            else:
                active_conn.execute(
                    "UPDATE returns SET filetrack_status = ?, filetrack_status_updated_at = ?, "
                    "client_status = ?, updated_at = ? WHERE id = ?",
                    (status, now, status, now, return_id),
                )

            # Mirror api_status's REJECTED contact-workflow side effect exactly.
            if status == "REJECTED":
                active_conn.execute(
                    "UPDATE returns SET contact_status='not_contacted', last_contacted_date=NULL, "
                    "updated_at=? WHERE id=?",
                    (now, return_id),
                )
            elif old_client_status == "REJECTED" and status != "REJECTED":
                active_conn.execute(
                    "UPDATE returns SET contact_status=NULL, last_contacted_date=NULL, updated_at=? WHERE id=?",
                    (now, return_id),
                )

            # Same status_events audit trail a staff-driven change gets from
            # api_status, so this scan shows up in the return's normal
            # activity timeline — not just in filetrack_status_history.
            exists = active_conn.execute(
                "SELECT id FROM status_events WHERE return_id=? AND event_type='STATUS_CHANGED' "
                "AND event_timestamp=?",
                (return_id, now),
            ).fetchone()
            if not exists:
                active_conn.execute(
                    """
                    INSERT INTO status_events
                      (return_id, event_type, old_status, new_status, event_timestamp, source_file, note)
                    VALUES (?, 'STATUS_CHANGED', ?, ?, ?, 'FILETRACK', ?)
                    """,
                    (return_id, old_client_status, status, now, f"Updated via filetrack scan (source={source})"),
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
            "client_status_synced": return_id is not None,
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
