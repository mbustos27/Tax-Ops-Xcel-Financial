"""Prompt J: mass-email schema, resolve_client_email, and migration discipline."""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

_MASS_EMAIL_TABLES = (
    "email_templates",
    "email_campaigns",
    "client_email_sends",
    "tax_deadlines",
)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _integrity_ok(path: str) -> bool:
    conn = sqlite3.connect(path)
    try:
        return conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        conn.close()


def test_schema_v29_tables_exist_after_init(taxops_db_path):
    from db import CURRENT_SCHEMA_VERSION, get_connection, get_schema_version

    conn = get_connection(taxops_db_path)
    try:
        assert get_schema_version(conn) == CURRENT_SCHEMA_VERSION
        assert CURRENT_SCHEMA_VERSION >= 29
        names = {
            r["name"]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        for table in _MASS_EMAIL_TABLES:
            assert table in names, f"missing table {table}"
        assert "do_not_email" in _table_columns(conn, "clients")
    finally:
        conn.close()


def test_resolve_client_email_prefers_taxpayer(taxops_db_path):
    from client_email import resolve_client_email
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        conn.execute(
            """
            INSERT INTO clients (
              id, last_name, first_name, taxpayer_email, spouse_email, do_not_email
            ) VALUES (9001, 'A', 'B', 'tp@example.com', 'sp@example.com', 0)
            """
        )
        conn.commit()
        assert resolve_client_email(conn, 9001) == "tp@example.com"
    finally:
        conn.close()


def test_resolve_client_email_falls_back_to_spouse(taxops_db_path):
    from client_email import resolve_client_email
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        conn.execute(
            """
            INSERT INTO clients (
              id, last_name, first_name, taxpayer_email, spouse_email, do_not_email
            ) VALUES (9002, 'A', 'B', NULL, 'sp@example.com', 0)
            """
        )
        conn.commit()
        assert resolve_client_email(conn, 9002) == "sp@example.com"
    finally:
        conn.close()


def test_resolve_client_email_none_when_opted_out(taxops_db_path):
    from client_email import resolve_client_email
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        conn.execute(
            """
            INSERT INTO clients (
              id, last_name, first_name, taxpayer_email, spouse_email, do_not_email
            ) VALUES (9003, 'A', 'B', 'tp@example.com', 'sp@example.com', 1)
            """
        )
        conn.commit()
        assert resolve_client_email(conn, 9003) is None
    finally:
        conn.close()


def test_resolve_client_email_none_when_both_empty(taxops_db_path):
    from client_email import resolve_client_email
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        conn.execute(
            """
            INSERT INTO clients (
              id, last_name, first_name, taxpayer_email, spouse_email, do_not_email
            ) VALUES (9004, 'A', 'B', NULL, NULL, 0)
            """
        )
        conn.commit()
        assert resolve_client_email(conn, 9004) is None
    finally:
        conn.close()


def test_client_email_sends_unique_campaign_client(taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        now = "2026-08-26T12:00:00Z"
        conn.execute(
            """
            INSERT INTO email_templates (
              key, subject, body_html, created_at, updated_at
            ) VALUES ('t1', 'Subj', '<p>hi</p>', ?, ?)
            """,
            (now, now),
        )
        template_id = conn.execute("SELECT id FROM email_templates").fetchone()["id"]
        conn.execute(
            """
            INSERT INTO email_campaigns (
              template_id, audience_definition, status, created_at
            ) VALUES (?, '{}', 'draft', ?)
            """,
            (template_id, now),
        )
        campaign_id = conn.execute("SELECT id FROM email_campaigns").fetchone()["id"]
        conn.execute(
            """
            INSERT INTO clients (id, last_name, first_name)
            VALUES (9100, 'X', 'Y')
            """
        )
        conn.execute(
            """
            INSERT INTO client_email_sends (
              campaign_id, client_id, recipient_email, status, dedupe_key
            ) VALUES (?, 9100, 'x@example.com', 'sent', 'k1')
            """,
            (campaign_id,),
        )
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO client_email_sends (
                  campaign_id, client_id, recipient_email, status, dedupe_key
                ) VALUES (?, 9100, 'x@example.com', 'sent', 'k2')
                """,
                (campaign_id,),
            )
            conn.commit()
    finally:
        conn.close()


def test_migration_idempotent_for_mass_email_tables(taxops_db_path):
    from db import get_connection, init_db

    for _ in range(3):
        conn = get_connection(taxops_db_path)
        init_db(conn)
        conn.close()
    assert _integrity_ok(taxops_db_path)


@pytest.mark.skipif(
    not Path(r"T:\taxops\taxops.db").is_file(),
    reason="production DB copy not available at T:\\taxops\\taxops.db",
)
def test_migration_runs_clean_on_production_db_copy(tmp_path):
    """Dry-run migration against a copy of the live-sized DB — never the live file."""
    src = Path(r"T:\taxops\taxops.db")
    dest = tmp_path / "taxops_migration_copy.db"
    shutil.copy2(src, dest)
    for suffix in ("-wal", "-shm"):
        wal = Path(str(src) + suffix)
        if wal.is_file():
            shutil.copy2(wal, Path(str(dest) + suffix))

    from db import get_connection, init_db

    assert _integrity_ok(str(dest))
    conn = get_connection(str(dest))
    init_db(conn)
    conn.close()
    assert _integrity_ok(str(dest))

    conn = get_connection(str(dest))
    try:
        names = {
            r["name"]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        for table in _MASS_EMAIL_TABLES:
            assert table in names
        assert "do_not_email" in _table_columns(conn, "clients")
    finally:
        conn.close()
