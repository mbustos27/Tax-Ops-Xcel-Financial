"""Auto-advance returns to LOG OUT when Drake shows full e-file acceptance."""

from __future__ import annotations

import sqlite3
from datetime import date
from typing import Any

from config import DRAKE_STATUS_MAP
from engagement_status_rules import drake_indicates_efile_complete
from normalizer import is_locked_status
from utils import now

_LOGOUT_STATUS = "LOG OUT"
_SYNC_SOURCE = "EFILE_SYNC"


def _logout_proven_by_drake(drake_status_raw: str | None) -> bool:
    """Full return e-filed — client export Status is EF Accepted / E-Filed: YES."""
    return drake_indicates_efile_complete(drake_status_raw)


def return_should_auto_logout(row: sqlite3.Row | dict[str, Any]) -> bool:
    """True when office rules say this return must be LOG OUT."""
    get = row.get if isinstance(row, dict) else lambda k, d=None: row[k] if k in row.keys() else d
    status = (get("client_status") or "").strip().upper()
    if status == _LOGOUT_STATUS:
        return False
    if is_locked_status(status):
        return False
    if _logout_proven_by_drake(get("drake_status_raw")):
        return True
    # Tax Ops export has no Status column — ack_date is the only e-file signal.
    raw = (get("drake_status_raw") or "").strip()
    if not raw and (get("ack_date") or "").strip():
        return True
    return False


def maybe_sync_efile_logout(
    conn: sqlite3.Connection,
    return_id: int,
    *,
    source: str = _SYNC_SOURCE,
) -> bool:
    """Set LOG OUT (+ logout_date) when drake/ack proves e-file accepted. Returns True if updated."""
    row = conn.execute(
        """
        SELECT id, client_status, drake_status_raw, ack_date, logout_date
        FROM returns WHERE id = ?
        """,
        (int(return_id),),
    ).fetchone()
    if not row or not return_should_auto_logout(row):
        return False

    old_status = row["client_status"]
    ts = now()
    today_iso = date.today().isoformat()
    if row["logout_date"]:
        conn.execute(
            "UPDATE returns SET client_status = ?, updated_at = ? WHERE id = ?",
            (_LOGOUT_STATUS, ts, int(return_id)),
        )
    else:
        conn.execute(
            """
            UPDATE returns SET client_status = ?, logout_date = ?, updated_at = ?
            WHERE id = ?
            """,
            (_LOGOUT_STATUS, today_iso, ts, int(return_id)),
        )

    conn.execute(
        """
        INSERT INTO status_events
          (return_id, event_type, old_status, new_status, event_timestamp, source_file, note)
        VALUES (?, 'STATUS_CHANGED', ?, ?, ?, ?, ?)
        """,
        (
            int(return_id),
            old_status,
            _LOGOUT_STATUS,
            ts,
            source,
            "Auto LOG OUT — Drake e-file accepted",
        ),
    )
    return True


def sync_efile_accepted_logouts(
    conn: sqlite3.Connection,
    *,
    tax_year: int | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Batch-sync all returns that should be LOG OUT per Drake client-export status."""
    clauses = [
        "UPPER(COALESCE(r.client_status, '')) != 'LOG OUT'",
        "UPPER(COALESCE(r.client_status, '')) != 'CANCELLED'",
        "("
        "TRIM(COALESCE(r.drake_status_raw, '')) IN ('EF Accepted', 'E-Filed: YES')"
        " OR ("
        "TRIM(COALESCE(r.drake_status_raw, '')) = ''"
        " AND r.ack_date IS NOT NULL AND TRIM(r.ack_date) != ''"
        ")"
        ")",
    ]
    params: list[Any] = []
    if tax_year is not None:
        clauses.append("r.tax_year = ?")
        params.append(int(tax_year))

    sql = f"""
        SELECT r.id, r.log_number, r.tax_year, r.client_status, r.drake_status_raw
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        WHERE COALESCE(c.is_test, 0) = 0
          AND {" AND ".join(clauses)}
        ORDER BY r.tax_year, r.log_number
    """
    rows = conn.execute(sql, params).fetchall()
    updated = 0
    samples: list[dict[str, Any]] = []

    for row in rows:
        if dry_run:
            if len(samples) < 20:
                samples.append(
                    {
                        "return_id": int(row["id"]),
                        "log_number": row["log_number"],
                        "tax_year": row["tax_year"],
                        "from_status": row["client_status"],
                        "drake_status_raw": row["drake_status_raw"],
                    }
                )
            updated += 1
            continue
        if maybe_sync_efile_logout(conn, int(row["id"])):
            updated += 1

    return {
        "dry_run": dry_run,
        "tax_year": tax_year,
        "candidate_count": len(rows),
        "updated": updated,
        "samples": samples,
    }


def _status_from_drake_raw(drake_status_raw: str | None) -> str:
    raw = (drake_status_raw or "").strip()
    if not raw:
        return "PROCESSING"
    mapped = DRAKE_STATUS_MAP.get(raw.upper(), "PROCESSING")
    if mapped == _LOGOUT_STATUS and not drake_indicates_efile_complete(raw):
        return "PROCESSING"
    return mapped


def revert_premature_logouts(
    conn: sqlite3.Connection,
    *,
    tax_year: int | None = None,
    entity_only: bool = False,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Revert LOG OUT when Drake client export does not show full e-file acceptance."""
    clauses = [
        "UPPER(COALESCE(r.client_status, '')) = 'LOG OUT'",
        "NOT TRIM(COALESCE(r.drake_status_raw, '')) IN ('EF Accepted', 'E-Filed: YES')",
        "NOT ("
        "TRIM(COALESCE(r.drake_status_raw, '')) = ''"
        " AND r.ack_date IS NOT NULL AND TRIM(r.ack_date) != ''"
        ")",
    ]
    params: list[Any] = []
    if tax_year is not None:
        clauses.append("r.tax_year = ?")
        params.append(int(tax_year))
    if entity_only:
        clauses.append(
            "("
            "COALESCE(rf.form_1120, 0) = 1 OR COALESCE(rf.form_1120s, 0) = 1"
            " OR COALESCE(rf.form_1065_llc, 0) = 1 OR COALESCE(rf.form_990_1041, 0) = 1"
            ")"
        )

    join_forms = "LEFT JOIN return_forms rf ON rf.return_id = r.id" if entity_only else ""
    sql = f"""
        SELECT r.id, r.log_number, r.tax_year, r.client_status, r.drake_status_raw
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        {join_forms}
        WHERE COALESCE(c.is_test, 0) = 0
          AND {" AND ".join(clauses)}
        ORDER BY r.tax_year, r.log_number
    """
    rows = conn.execute(sql, params).fetchall()
    updated = 0
    samples: list[dict[str, Any]] = []

    for row in rows:
        target = _status_from_drake_raw(row["drake_status_raw"])
        if dry_run:
            if len(samples) < 20:
                samples.append(
                    {
                        "return_id": int(row["id"]),
                        "log_number": row["log_number"],
                        "tax_year": row["tax_year"],
                        "from_status": row["client_status"],
                        "to_status": target,
                        "drake_status_raw": row["drake_status_raw"],
                    }
                )
            updated += 1
            continue

        ts = now()
        conn.execute(
            "UPDATE returns SET client_status = ?, updated_at = ? WHERE id = ?",
            (target, ts, int(row["id"])),
        )
        conn.execute(
            """
            INSERT INTO status_events
              (return_id, event_type, old_status, new_status, event_timestamp, source_file, note)
            VALUES (?, 'STATUS_CHANGED', ?, ?, ?, ?, ?)
            """,
            (
                int(row["id"]),
                _LOGOUT_STATUS,
                target,
                ts,
                _SYNC_SOURCE,
                "Reverted LOG OUT — Drake has not fully e-file accepted",
            ),
        )
        updated += 1

    return {
        "dry_run": dry_run,
        "tax_year": tax_year,
        "entity_only": entity_only,
        "candidate_count": len(rows),
        "updated": updated,
        "samples": samples,
    }
