"""Restart-safety checks for TaxOps (office LAN).

Cookie sessions alone cannot show who is online. After deploy, authenticated
requests upsert ``staff_presence``. This module combines that with recent DB
write signals and SQLite lock state so admins can see if a restart would
interrupt active work.
"""
from __future__ import annotations

import os
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from utils import now, parse_iso_datetime

# How long after last activity we still treat someone as "at the desk".
DEFAULT_IDLE_MINUTES = int(os.environ.get("TAXOPS_RESTART_IDLE_MINUTES", "5"))
# Writes inside this window → WAIT (pickup/payment/status edits in flight).
DEFAULT_WRITE_MINUTES = int(os.environ.get("TAXOPS_RESTART_WRITE_MINUTES", "3"))
# Throttle presence upserts so browsing does not hammer SQLite.
PRESENCE_TOUCH_SECONDS = int(os.environ.get("TAXOPS_PRESENCE_TOUCH_SECONDS", "30"))

_MUTATING = frozenset({"POST", "PUT", "PATCH", "DELETE"})


@dataclass
class RestartAssessment:
    verdict: str  # SAFE | CAUTION | WAIT
    safe_to_restart: bool
    idle_minutes: int
    write_minutes: int
    checked_at: str
    summary: str
    active_staff: list[dict[str, Any]] = field(default_factory=list)
    recent_writes: list[dict[str, Any]] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _cutoff(minutes: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat(
        timespec="seconds"
    )


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def touch_presence(
    conn: sqlite3.Connection,
    *,
    username: str,
    path: str,
    method: str,
    ip: str | None = None,
    force: bool = False,
) -> bool:
    """Upsert staff_presence. Returns True if a row was written."""
    user = (username or "").strip()
    if not user or not _table_exists(conn, "staff_presence"):
        return False
    method_u = (method or "GET").upper()
    mutating = method_u in _MUTATING
    ts = now()
    row = conn.execute(
        "SELECT last_seen, last_write_at FROM staff_presence WHERE username = ?",
        (user,),
    ).fetchone()
    if row and not force and not mutating:
        last = parse_iso_datetime(row["last_seen"])
        if last is not None:
            age = (
                datetime.now(timezone.utc) - last.astimezone(timezone.utc)
            ).total_seconds()
            if age < PRESENCE_TOUCH_SECONDS:
                return False
    last_write = ts if mutating else (row["last_write_at"] if row else None)
    conn.execute(
        """
        INSERT INTO staff_presence (username, last_seen, last_path, last_method, last_ip, last_write_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(username) DO UPDATE SET
          last_seen = excluded.last_seen,
          last_path = excluded.last_path,
          last_method = excluded.last_method,
          last_ip = excluded.last_ip,
          last_write_at = COALESCE(excluded.last_write_at, staff_presence.last_write_at)
        """,
        (user, ts, (path or "")[:240], method_u, (ip or "")[:64] or None, last_write),
    )
    conn.commit()
    return True


def _count_since(conn: sqlite3.Connection, sql: str, cutoff: str) -> int:
    try:
        row = conn.execute(sql, (cutoff,)).fetchone()
    except sqlite3.Error:
        return 0
    if not row:
        return 0
    return int(row[0] or 0)


def _active_staff(conn: sqlite3.Connection, idle_minutes: int) -> list[dict[str, Any]]:
    if not _table_exists(conn, "staff_presence"):
        return []
    cutoff = _cutoff(idle_minutes)
    rows = conn.execute(
        """
        SELECT username, last_seen, last_path, last_method, last_ip, last_write_at
        FROM staff_presence
        WHERE last_seen >= ?
        ORDER BY last_seen DESC
        """,
        (cutoff,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    write_cut = _cutoff(DEFAULT_WRITE_MINUTES)
    for r in rows:
        out.append(
            {
                "username": r["username"],
                "last_seen": r["last_seen"],
                "last_path": r["last_path"],
                "last_method": r["last_method"],
                "last_ip": r["last_ip"],
                "recent_write": bool(
                    r["last_write_at"] and str(r["last_write_at"]) >= write_cut
                ),
            }
        )
    return out


def _recent_write_signals(
    conn: sqlite3.Connection, write_minutes: int
) -> list[dict[str, Any]]:
    cutoff = _cutoff(write_minutes)
    signals: list[dict[str, Any]] = []
    checks = [
        (
            "status_changes",
            "SELECT COUNT(*) FROM status_events WHERE event_timestamp >= ?",
            "Status / workflow changes",
        ),
        (
            "notes",
            "SELECT COUNT(*) FROM notes WHERE created_at >= ?",
            "Notes added",
        ),
        (
            "documents",
            "SELECT COUNT(*) FROM return_documents WHERE uploaded_at >= ?",
            "Documents uploaded",
        ),
        (
            "returns_updated",
            "SELECT COUNT(*) FROM returns WHERE updated_at >= ?",
            "Returns updated",
        ),
        (
            "clients_updated",
            "SELECT COUNT(*) FROM clients WHERE updated_at >= ?",
            "Clients updated",
        ),
        (
            "imports",
            "SELECT COUNT(*) FROM import_batches WHERE imported_at >= ?",
            "CSV imports",
        ),
        (
            "extraction_created",
            "SELECT COUNT(*) FROM extraction_queue WHERE created_at >= ?",
            "Extraction jobs created",
        ),
    ]
    for key, sql, label in checks:
        n = _count_since(conn, sql, cutoff)
        if n:
            signals.append({"key": key, "label": label, "count": n, "window_minutes": write_minutes})
    return signals


def _pending_extraction(conn: sqlite3.Connection) -> dict[str, int]:
    if not _table_exists(conn, "extraction_queue"):
        return {"pending": 0, "processing": 0}
    try:
        rows = conn.execute(
            """
            SELECT status, COUNT(*) AS n
            FROM extraction_queue
            WHERE status IN ('pending', 'processing')
            GROUP BY status
            """
        ).fetchall()
    except sqlite3.Error:
        return {"pending": 0, "processing": 0}
    out = {"pending": 0, "processing": 0}
    for r in rows:
        out[str(r["status"])] = int(r["n"] or 0)
    return out


def _sqlite_writable(db_path: str) -> tuple[bool, str]:
    """Try BEGIN IMMEDIATE; failure usually means another writer holds the lock."""
    try:
        conn = sqlite3.connect(db_path, timeout=0.5)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.rollback()
            return True, "Database lock free"
        finally:
            conn.close()
    except sqlite3.OperationalError as exc:
        return False, f"Database busy/locked: {exc}"
    except sqlite3.Error as exc:
        return False, f"Database check failed: {exc}"


def assess_restart(
    conn: sqlite3.Connection,
    *,
    db_path: Optional[str] = None,
    idle_minutes: int = DEFAULT_IDLE_MINUTES,
    write_minutes: int = DEFAULT_WRITE_MINUTES,
) -> RestartAssessment:
    idle_minutes = max(1, int(idle_minutes))
    write_minutes = max(1, int(write_minutes))
    checked = now()
    staff = _active_staff(conn, idle_minutes)
    writes = _recent_write_signals(conn, write_minutes)
    extraction = _pending_extraction(conn)
    blockers: list[str] = []
    warnings: list[str] = []
    notes: list[str] = [
        "Cookie logins alone are invisible until presence tracking is live "
        "(after this build is running and staff click around).",
        f"Idle window: {idle_minutes} min · write window: {write_minutes} min.",
    ]

    path = db_path
    if not path:
        try:
            from config import DB_PATH as _DB

            path = _DB
        except Exception:
            path = None
    lock_ok, lock_msg = (True, "Skipped lock check") if not path else _sqlite_writable(path)
    if not lock_ok:
        blockers.append(lock_msg)

    if extraction.get("processing"):
        blockers.append(
            f"{extraction['processing']} document extraction job(s) currently processing"
        )
    elif extraction.get("pending"):
        warnings.append(
            f"{extraction['pending']} extraction job(s) queued "
            "(usually safe; worker will resume after restart)"
        )

    if writes:
        blockers.append(
            "Recent database writes in the last "
            f"{write_minutes} min — someone may be mid-save"
        )

    writers = [s for s in staff if s.get("recent_write")]
    browsers = [s for s in staff if not s.get("recent_write")]
    if writers:
        names = ", ".join(s["username"] for s in writers)
        blockers.append(f"Staff with recent saves online: {names}")
    if browsers:
        names = ", ".join(s["username"] for s in browsers)
        warnings.append(f"Staff browsing (no recent save): {names}")

    if not _table_exists(conn, "staff_presence"):
        warnings.append("staff_presence table missing — run app once to migrate")

    if blockers:
        verdict = "WAIT"
        safe = False
        summary = "Do not restart yet — active saves or a busy database."
    elif warnings and browsers:
        verdict = "CAUTION"
        safe = True
        summary = (
            "No recent saves detected, but people still have the app open. "
            "Warn the desk, then restart."
        )
    elif warnings:
        verdict = "CAUTION"
        safe = True
        summary = "Mostly clear — review warnings, then restart if OK."
    else:
        verdict = "SAFE"
        safe = True
        summary = "No active staff or recent writes detected — safe to restart."

    return RestartAssessment(
        verdict=verdict,
        safe_to_restart=safe,
        idle_minutes=idle_minutes,
        write_minutes=write_minutes,
        checked_at=checked,
        summary=summary,
        active_staff=staff,
        recent_writes=writes,
        blockers=blockers,
        warnings=warnings,
        notes=notes,
    )


def format_assessment_text(a: RestartAssessment) -> str:
    lines = [
        f"TaxOps restart check — {a.verdict}",
        f"Checked: {a.checked_at}",
        a.summary,
        "",
    ]
    if a.blockers:
        lines.append("Blockers:")
        lines.extend(f"  • {b}" for b in a.blockers)
        lines.append("")
    if a.warnings:
        lines.append("Warnings:")
        lines.extend(f"  • {w}" for w in a.warnings)
        lines.append("")
    if a.active_staff:
        lines.append("Active staff:")
        for s in a.active_staff:
            write = "write" if s.get("recent_write") else "browse"
            lines.append(
                f"  • {s['username']}  {s.get('last_method')} {s.get('last_path')}  "
                f"({write}, last_seen {s.get('last_seen')})"
            )
        lines.append("")
    if a.recent_writes:
        lines.append("Recent writes:")
        for w in a.recent_writes:
            lines.append(f"  • {w['label']}: {w['count']} (last {w['window_minutes']} min)")
        lines.append("")
    lines.append(
        "SAFE = restart OK · CAUTION = warn desk first · WAIT = finish active work first"
    )
    return "\n".join(lines)
