"""Shared LOG number allocation — same race-safe pattern as intake.

Always call inside (or starting) a write transaction with BEGIN IMMEDIATE.
Never bare MAX+1 outside a write lock.
"""
from __future__ import annotations

import sqlite3


def next_log_number_for_year(conn: sqlite3.Connection, tax_year: int) -> str:
    """Return the next unpadded log_number string for tax_year.

    Caller must already hold a write lock (BEGIN IMMEDIATE).
    """
    row = conn.execute(
        "SELECT MAX(CAST(log_number AS INTEGER)) AS mx FROM returns WHERE tax_year = ?",
        (tax_year,),
    ).fetchone()
    mx = 0
    if row is not None:
        try:
            mx = row["mx"]
        except (TypeError, KeyError, IndexError):
            mx = row[0]
    return str((mx or 0) + 1)


def ensure_return_log_number(
    conn: sqlite3.Connection,
    return_id: int,
    *,
    begin_immediate: bool = True,
) -> tuple[str, bool]:
    """Ensure the return has a log_number.

    Returns (log_number, newly_allocated).
    When newly_allocated is True, the caller may print a label once.
    """
    if begin_immediate:
        conn.execute("BEGIN IMMEDIATE")

    row = conn.execute(
        "SELECT id, log_number, tax_year FROM returns WHERE id = ?",
        (return_id,),
    ).fetchone()
    if row is None:
        if begin_immediate:
            conn.rollback()
        raise ValueError(f"return {return_id} not found")

    existing = (row["log_number"] or "").strip() if row["log_number"] is not None else ""
    if existing:
        if begin_immediate:
            # Release the write lock without changing anything.
            conn.commit()
        return existing, False

    tax_year = int(row["tax_year"])
    log_number = next_log_number_for_year(conn, tax_year)
    conn.execute(
        "UPDATE returns SET log_number = ? WHERE id = ? AND (log_number IS NULL OR TRIM(log_number) = '')",
        (log_number, return_id),
    )
    if begin_immediate:
        conn.commit()
    return log_number, True
