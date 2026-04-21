from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional


def create_status_events(
    conn: sqlite3.Connection,
    return_id: int,
    before: Dict[str, Any],
    after: Dict[str, Any],
    import_time: str,
    source_file: str,
) -> int:
    created = 0

    created += _insert_if_needed(
        conn,
        return_id=return_id,
        event_type="INTAKE_RECORDED",
        old_status=None,
        new_status=None,
        event_timestamp=after.get("intake_date"),
        source_file=source_file,
        should_create=before.get("intake_date") != after.get("intake_date") and after.get("intake_date") is not None,
        note="intake_date changed",
    )

    created += _insert_if_needed(
        conn,
        return_id=return_id,
        event_type="STATUS_CHANGED",
        old_status=before.get("client_status"),
        new_status=after.get("client_status"),
        event_timestamp=import_time,
        source_file=source_file,
        should_create=before.get("client_status") != after.get("client_status"),
        note="client_status changed",
    )

    verified_before = bool(before.get("verified")) if before.get("verified") is not None else False
    verified_after = bool(after.get("verified")) if after.get("verified") is not None else False
    created += _insert_if_needed(
        conn,
        return_id=return_id,
        event_type="VERIFIED_MARKED",
        old_status=None,
        new_status=None,
        event_timestamp=import_time,
        source_file=source_file,
        should_create=(not verified_before) and verified_after,
        note="verified changed to true",
    )

    created += _date_change_event(
        conn, return_id, "date_emailed", "EMAILED_TO_CLIENT", before, after, source_file
    )
    created += _date_change_event(
        conn, return_id, "pickup_date", "READY_FOR_PICKUP", before, after, source_file
    )
    created += _date_change_event(
        conn, return_id, "logout_date", "LOGGED_OUT", before, after, source_file
    )
    created += _date_change_event(
        conn, return_id, "updated_date", "RECORD_UPDATED", before, after, source_file
    )

    return created


def _date_change_event(
    conn: sqlite3.Connection,
    return_id: int,
    field_name: str,
    event_type: str,
    before: Dict[str, Any],
    after: Dict[str, Any],
    source_file: str,
) -> int:
    old_val = before.get(field_name)
    new_val = after.get(field_name)
    return _insert_if_needed(
        conn,
        return_id=return_id,
        event_type=event_type,
        old_status=None,
        new_status=None,
        event_timestamp=new_val,
        source_file=source_file,
        should_create=old_val != new_val and new_val is not None,
        note=f"{field_name} changed",
    )


def _insert_if_needed(
    conn: sqlite3.Connection,
    *,
    return_id: int,
    event_type: str,
    old_status: Optional[str],
    new_status: Optional[str],
    event_timestamp: Optional[str],
    source_file: str,
    should_create: bool,
    note: str,
) -> int:
    if not should_create or event_timestamp is None:
        return 0

    existing = conn.execute(
        """
        SELECT id
        FROM status_events
        WHERE return_id = ? AND event_type = ? AND event_timestamp = ?
        LIMIT 1
        """,
        (return_id, event_type, event_timestamp),
    ).fetchone()
    if existing:
        return 0

    conn.execute(
        """
        INSERT INTO status_events
        (return_id, event_type, old_status, new_status, event_timestamp, source_file, note)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (return_id, event_type, old_status, new_status, event_timestamp, source_file, note),
    )
    return 1
