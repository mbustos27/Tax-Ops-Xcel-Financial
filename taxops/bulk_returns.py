"""Bulk dashboard updates — shared transaction helpers (BULK-3/#107, BULK-4/#108)."""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from typing import Any

from normalizer import is_locked_status
from preparer import normalize_preparer


def iso_timestamp_micro() -> str:
    return datetime.now(timezone.utc).isoformat()


def bulk_apply_status_changes(
    conn: sqlite3.Connection,
    *,
    return_ids: list[int],
    new_status: str,
    status_flow: tuple[str, ...],
    status_date_stamp: dict[str, str],
    actor_username: str,
) -> list[dict[str, Any]]:
    """
    Validate and apply bulk status transitions in the current SQLite transaction.

    Rolls back externally if desired; callers should BEGIN + commit or rollback.
    Returns a list of per-return error dicts; empty list means all requested IDs succeed.
    """
    if new_status.upper().strip() not in status_flow:
        return [{"return_id": None, "error": f"invalid status {new_status!r}"}]
    canon = new_status.upper().strip()

    ids = sorted({int(x) for x in return_ids if x is not None})
    if not ids:
        return [{"return_id": None, "error": "no return_ids"}]

    qmarks = ",".join("?" for _ in ids)
    rows = conn.execute(f"SELECT * FROM returns WHERE id IN ({qmarks})", ids).fetchall()
    by_id = {r["id"]: dict(r) for r in rows}
    errs: list[dict[str, Any]] = []

    for rid in ids:
        row = by_id.get(rid)
        if row is None:
            errs.append({"return_id": rid, "error": "not_found"})
            continue
        old = row["client_status"]
        if is_locked_status(old):
            errs.append({"return_id": rid, "error": f"status locked ({old})"})
            continue

    if errs:
        return errs

    note = f"Bulk dashboard · user={(actor_username or '').strip() or '?'}"

    today_iso = date.today().isoformat()

    for rid in ids:
        row = by_id[rid]
        old_status = row["client_status"]
        ts = iso_timestamp_micro()
        date_field = status_date_stamp.get(canon)

        if date_field and not row.get(date_field):
            conn.execute(
                f"UPDATE returns SET client_status=?, {date_field}=?, updated_at=? WHERE id=?",
                (canon, today_iso, ts, rid),
            )
        else:
            conn.execute(
                "UPDATE returns SET client_status=?, updated_at=? WHERE id=?",
                (canon, ts, rid),
            )

        exists = conn.execute(
            "SELECT 1 FROM status_events WHERE return_id=? AND event_type='STATUS_CHANGED' "
            "AND event_timestamp=? LIMIT 1",
            (rid, ts),
        ).fetchone()
        if not exists:
            conn.execute(
                """
                INSERT INTO status_events
                  (return_id, event_type, old_status, new_status, event_timestamp, source_file, note)
                VALUES (?, 'STATUS_CHANGED', ?, ?, ?, 'APP_BULK', ?)
                """,
                (rid, old_status, canon, ts, note),
            )

        if canon == "REJECTED":
            conn.execute(
                """
                UPDATE returns SET contact_status='not_contacted', last_contacted_date=NULL, updated_at=?
                WHERE id=?
                """,
                (ts, rid),
            )
        elif old_status == "REJECTED" and canon != "REJECTED":
            conn.execute(
                "UPDATE returns SET contact_status=NULL, last_contacted_date=NULL, updated_at=? WHERE id=?",
                (ts, rid),
            )

    return []


def bulk_apply_processor_changes(
    conn: sqlite3.Connection,
    *,
    return_ids: list[int],
    new_processor_raw: Any,
    actor_username: str,
) -> tuple[list[dict[str, Any]], int]:
    """
    Assign normalized preparer/processor across many returns inside one transaction.
    Audit rows use event_type PROCESSOR_CHANGED with old/new in old_status/new_status TEXT columns.

    Returns (errors, rows_updated); rows_updated counts non–no-op updates only.
    """
    normalized = normalize_preparer(new_processor_raw)
    ids = sorted({int(x) for x in return_ids if x is not None})
    if not ids:
        return [{"return_id": None, "error": "no return_ids"}], 0

    note = f"Bulk preparer · user={(actor_username or '').strip() or '?'}"

    qmarks = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"SELECT id, processor FROM returns WHERE id IN ({qmarks})",
        ids,
    ).fetchall()
    by_id = {r["id"]: r for r in rows}
    errs: list[dict[str, Any]] = []

    for rid in ids:
        if rid not in by_id:
            errs.append({"return_id": rid, "error": "not_found"})

    if errs:
        return errs, 0

    updated = 0
    for rid in ids:
        prev = by_id[rid]["processor"] or ""
        if (prev or None) == (normalized or None):
            continue  # skip no-op rows but treat as success
        updated += 1
        ts = iso_timestamp_micro()
        conn.execute(
            "UPDATE returns SET processor=?, updated_at=? WHERE id=?",
            (normalized, ts, rid),
        )
        exists = conn.execute(
            "SELECT 1 FROM status_events WHERE return_id=? AND event_type='PROCESSOR_CHANGED' "
            "AND event_timestamp=? LIMIT 1",
            (rid, ts),
        ).fetchone()
        if not exists:
            conn.execute(
                """
                INSERT INTO status_events
                  (return_id, event_type, old_status, new_status, event_timestamp, source_file, note)
                VALUES (?, 'PROCESSOR_CHANGED', ?, ?, ?, 'APP_BULK', ?)
                """,
                (rid, prev or "", normalized or "", ts, note),
            )
    return [], updated
