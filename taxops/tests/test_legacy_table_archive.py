"""Phase 2.2 (email system revamp): archive the legacy 6-layer-classifier tables.

email_classifications and domain_classifications were confirmed dead in the
Phase 1 audit — no live code path reads or writes either table. This migration
renames (never drops) them so historical rows stay reachable, and fresh
installs no longer create them at all.
"""
from __future__ import annotations

import sqlite3

import pytest


def test_fresh_install_does_not_create_legacy_tables(taxops_db_path):
    from db import get_connection
    conn = get_connection(taxops_db_path)
    names = {
        r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    conn.close()
    assert "email_classifications" not in names
    assert "domain_classifications" not in names


def test_migration_renames_preexisting_legacy_tables_preserving_rows(tmp_path, monkeypatch):
    """Simulate an old production DB that still has the pre-archive schema and
    real rows, then run init_db() and confirm the rename preserves the data."""
    import config as cfg
    import db as db_mod

    path = str(tmp_path / "legacy.db")
    monkeypatch.setattr(cfg, "DB_PATH", path)
    monkeypatch.setattr(db_mod, "DB_PATH", path)

    # Build a minimal pre-migration schema by hand (old shape).
    raw = sqlite3.connect(path)
    raw.execute(
        """
        CREATE TABLE email_classifications (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          sender_email TEXT, sender_domain TEXT, subject_snippet TEXT,
          classification TEXT NOT NULL, confirmed_by TEXT, confirmed_at TEXT,
          created_at TEXT NOT NULL, source TEXT NOT NULL DEFAULT 'auto'
        )
        """
    )
    raw.execute(
        "INSERT INTO email_classifications (sender_email, sender_domain, classification, created_at) "
        "VALUES ('a@example.com', 'example.com', 'promotional', '2026-01-01T00:00:00Z')"
    )
    raw.execute(
        """
        CREATE TABLE domain_classifications (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          domain TEXT NOT NULL UNIQUE, classification TEXT NOT NULL,
          confidence_count INTEGER NOT NULL DEFAULT 1, last_seen TEXT NOT NULL,
          last_confirmed_by TEXT, last_confirmed_at TEXT, graduated INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    raw.execute(
        "INSERT INTO domain_classifications (domain, classification, last_seen) "
        "VALUES ('example.com', 'promotional', '2026-01-01T00:00:00Z')"
    )
    raw.commit()
    raw.close()

    from db import get_connection, init_db
    conn = get_connection(path)
    init_db(conn)
    conn.close()

    conn = get_connection(path)
    names = {
        r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "email_classifications" not in names, "legacy name must no longer exist after archiving"
    assert "domain_classifications" not in names
    assert "archive_email_classifications" in names
    assert "archive_domain_classifications" in names

    ec_row = conn.execute("SELECT * FROM archive_email_classifications").fetchone()
    assert ec_row["sender_email"] == "a@example.com"
    dc_row = conn.execute("SELECT * FROM archive_domain_classifications").fetchone()
    assert dc_row["domain"] == "example.com"
    conn.close()


def test_migration_is_idempotent_across_repeated_init_db_calls(tmp_path, monkeypatch):
    """Running init_db() (and therefore the archive migration) multiple times
    in a row — as happens on every app restart — must not error or re-create
    the legacy tables."""
    import config as cfg
    import db as db_mod

    path = str(tmp_path / "legacy2.db")
    monkeypatch.setattr(cfg, "DB_PATH", path)
    monkeypatch.setattr(db_mod, "DB_PATH", path)

    raw = sqlite3.connect(path)
    raw.execute(
        "CREATE TABLE email_classifications (id INTEGER PRIMARY KEY, created_at TEXT NOT NULL, "
        "classification TEXT NOT NULL DEFAULT 'x')"
    )
    raw.commit()
    raw.close()

    from db import get_connection, init_db
    for _ in range(3):
        conn = get_connection(path)
        init_db(conn)
        conn.close()

    conn = get_connection(path)
    names = {
        r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    conn.close()
    assert "email_classifications" not in names
    assert "archive_email_classifications" in names


def test_current_schema_version_reflects_phase_2_2(taxops_db_path):
    from db import get_connection, get_schema_version, CURRENT_SCHEMA_VERSION
    assert CURRENT_SCHEMA_VERSION >= 11
    conn = get_connection(taxops_db_path)
    assert get_schema_version(conn) == CURRENT_SCHEMA_VERSION
    conn.close()
