"""
Shared fixtures for TaxOps test suite.
Uses sqlite3 in-memory DB only — no file I/O, no Flask test client setup here.
"""
from __future__ import annotations

import sqlite3
import sys
import os
import pytest

# Make taxops package importable from tests/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "taxops"))


@pytest.fixture()
def mem_db() -> sqlite3.Connection:
    """Fresh in-memory DB with full schema initialised."""
    from db import init_db

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    init_db(conn)
    return conn


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Return the set of column names for *table*."""
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


def insert_client(conn: sqlite3.Connection, last: str = "Smith", first: str = "John") -> int:
    cur = conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) VALUES (?,?,datetime('now'),datetime('now'))",
        (last, first),
    )
    conn.commit()
    return cur.lastrowid


def insert_return(
    conn: sqlite3.Connection,
    client_id: int,
    tax_year: int = 2025,
    log_number: str = "1001",
    status: str = "PROCESSING",
    ack_date: str | None = None,
) -> int:
    cur = conn.execute(
        """INSERT INTO returns
           (client_id, tax_year, log_number, client_status, ack_date, created_at, updated_at)
           VALUES (?,?,?,?,?,datetime('now'),datetime('now'))""",
        (client_id, tax_year, log_number, status, ack_date),
    )
    conn.commit()
    return cur.lastrowid


def insert_batch(
    conn: sqlite3.Connection,
    status: str = "open",
    transmitted_at: str | None = None,
) -> int:
    cur = conn.execute(
        """INSERT INTO efile_batches
           (transmission_date, status, transmitted_at, created_at)
           VALUES (date('now'), ?, ?, datetime('now'))""",
        (status, transmitted_at),
    )
    conn.commit()
    return cur.lastrowid


def insert_batch_item(
    conn: sqlite3.Connection,
    batch_id: int,
    return_id: int,
    ack_status: str = "pending",
    log_number: str = "1001",
    client_name: str = "Smith, John",
) -> int:
    cur = conn.execute(
        """INSERT INTO efile_batch_items
           (batch_id, return_id, log_number, client_name, ack_status, created_at)
           VALUES (?,?,?,?,?,datetime('now'))""",
        (batch_id, return_id, log_number, client_name, ack_status),
    )
    conn.commit()
    return cur.lastrowid
