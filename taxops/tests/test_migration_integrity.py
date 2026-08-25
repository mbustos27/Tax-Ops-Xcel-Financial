"""Phase 2.4: migration discipline — every Phase 2 schema change (2.1's
email_sender_rules.rule_scope/action + email_processing_log.suppression_reason
columns, 2.2's legacy-table archive rename) is exercised against a synthetic
copy of a pre-Phase-2 production database and verified with
PRAGMA integrity_check, per the addendum's migration discipline requirement.
"""
from __future__ import annotations

import sqlite3

import pytest


def _rewind_to_pre_phase2_shape(path: str) -> None:
    """Take a fully-current (post-init_db) DB copy and roll back just the
    Phase 2.1/2.2 pieces to their pre-migration shape, with real rows —
    everything else (clients, returns, and every other table's current
    columns) is left exactly as init_db built it, so this exercises the
    migration in as realistic a "copy of the DB" scenario as a synthetic
    fixture can, without hand-maintaining a full legacy schema."""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        DROP TABLE email_sender_rules;
        CREATE TABLE email_sender_rules (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          domain TEXT NOT NULL UNIQUE,
          rule_type TEXT NOT NULL DEFAULT 'always_promotional',
          note TEXT, created_by TEXT, created_at TEXT NOT NULL
        );

        DROP TABLE email_processing_log;
        CREATE TABLE email_processing_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          message_uid TEXT NOT NULL, imap_folder TEXT NOT NULL,
          sender_domain TEXT, subject_snippet TEXT, outcome TEXT NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 1, last_attempt_at TEXT,
          error_message TEXT, doc_id INTEGER, return_id INTEGER,
          UNIQUE(message_uid, imap_folder)
        );

        CREATE TABLE email_classifications (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          sender_email TEXT, sender_domain TEXT, subject_snippet TEXT,
          classification TEXT NOT NULL, confirmed_by TEXT, confirmed_at TEXT,
          created_at TEXT NOT NULL, source TEXT NOT NULL DEFAULT 'auto'
        );
        CREATE TABLE domain_classifications (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          domain TEXT NOT NULL UNIQUE, classification TEXT NOT NULL,
          confidence_count INTEGER NOT NULL DEFAULT 1, last_seen TEXT NOT NULL,
          last_confirmed_by TEXT, last_confirmed_at TEXT, graduated INTEGER NOT NULL DEFAULT 0
        );

        INSERT INTO clients (id, last_name, first_name, display_name)
          VALUES (1, 'Test', 'Client', 'Test Client');
        INSERT INTO returns (id, client_id, tax_year, client_status)
          VALUES (1, 1, 2025, 'PROCESSING');

        INSERT INTO email_sender_rules (domain, rule_type, created_by, created_at)
          VALUES ('spamvendor.com', 'always_promotional', 'staff', '2026-01-01T00:00:00Z');

        INSERT INTO email_processing_log
          (message_uid, imap_folder, sender_domain, subject_snippet, outcome, last_attempt_at)
          VALUES ('100', 'INBOX', 'spamvendor.com', 'deals', 'skip', '2026-01-01T00:00:00Z');

        INSERT INTO email_classifications (sender_email, sender_domain, classification, created_at)
          VALUES ('a@example.com', 'example.com', 'promotional', '2026-01-01T00:00:00Z');

        INSERT INTO domain_classifications (domain, classification, last_seen)
          VALUES ('example.com', 'promotional', '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()


@pytest.fixture()
def pre_phase2_copy(taxops_db_path):
    """A copy of a fully-current DB with the Phase 2.1/2.2 pieces rolled back
    to their pre-migration shape (see _rewind_to_pre_phase2_shape), wired up
    as the active DB_PATH via the shared taxops_db_path fixture so init_db()'s
    migration path runs against it exactly as it would in production."""
    _rewind_to_pre_phase2_shape(taxops_db_path)
    return taxops_db_path


def _integrity_ok(path: str) -> bool:
    conn = sqlite3.connect(path)
    try:
        result = conn.execute("PRAGMA integrity_check").fetchone()[0]
        return result == "ok"
    finally:
        conn.close()


def test_migrations_run_clean_on_pre_phase2_copy_with_integrity_check(pre_phase2_copy):
    """Run the full init_db migration path against a copy of a pre-Phase-2 DB
    and verify PRAGMA integrity_check passes afterward — the explicit 2.4
    migration-discipline gate."""
    from db import get_connection, init_db

    assert _integrity_ok(pre_phase2_copy), "fixture DB must be valid before migrating"

    conn = get_connection(pre_phase2_copy)
    init_db(conn)
    conn.close()

    assert _integrity_ok(pre_phase2_copy), "PRAGMA integrity_check must pass after migration"


def test_migration_adds_2_1_columns_and_preserves_rows(pre_phase2_copy):
    from db import get_connection, init_db

    conn = get_connection(pre_phase2_copy)
    init_db(conn)

    rule = conn.execute(
        "SELECT rule_scope, action FROM email_sender_rules WHERE domain='spamvendor.com'"
    ).fetchone()
    assert rule["rule_scope"] == "domain"
    assert rule["action"] == "block", "pre-existing always_promotional rows must map to action='block'"

    log_row = conn.execute(
        "SELECT suppression_reason FROM email_processing_log WHERE message_uid='100'"
    ).fetchone()
    assert "suppression_reason" in log_row.keys()
    conn.close()


def test_migration_archives_legacy_classifier_tables_preserving_data(pre_phase2_copy):
    from db import get_connection, init_db

    conn = get_connection(pre_phase2_copy)
    init_db(conn)

    names = {
        r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "email_classifications" not in names
    assert "domain_classifications" not in names
    assert "archive_email_classifications" in names
    assert "archive_domain_classifications" in names

    ec = conn.execute("SELECT * FROM archive_email_classifications").fetchone()
    assert ec["sender_email"] == "a@example.com"
    dc = conn.execute("SELECT * FROM archive_domain_classifications").fetchone()
    assert dc["domain"] == "example.com"
    conn.close()


def test_migration_preserves_unrelated_business_data(pre_phase2_copy):
    """The migration must not touch tables/rows outside its scope."""
    from db import get_connection, init_db

    conn = get_connection(pre_phase2_copy)
    init_db(conn)

    row = conn.execute("SELECT last_name FROM clients WHERE id=1").fetchone()
    assert row["last_name"] == "Test"
    ret = conn.execute("SELECT tax_year, client_status FROM returns WHERE id=1").fetchone()
    assert ret["tax_year"] == 2025
    assert ret["client_status"] == "PROCESSING"
    conn.close()


def test_migration_idempotent_and_stays_valid_across_repeated_runs(pre_phase2_copy):
    """Simulates repeated app restarts against the same migrated DB — must
    not error and must remain structurally valid every time."""
    from db import get_connection, init_db

    for _ in range(3):
        conn = get_connection(pre_phase2_copy)
        init_db(conn)
        conn.close()
        assert _integrity_ok(pre_phase2_copy)


# ── Phase 3.1: email_inbox.sender_name/suggested_return_id/suggestion_* ─────

def _rewind_email_inbox_to_pre_3_1_shape(path: str) -> None:
    """Roll email_inbox back to its pre-3.1 shape (no sender_name/suggestion
    columns) with a real row, leaving every other table at current schema."""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        DROP TABLE email_inbox;
        CREATE TABLE email_inbox (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          sender_email        TEXT,
          sender_domain       TEXT,
          subject_snippet     TEXT,
          filename            TEXT NOT NULL,
          original_filename   TEXT,
          file_path           TEXT NOT NULL,
          file_size_bytes     INTEGER,
          received_at         TEXT NOT NULL,
          assigned_return_id  INTEGER REFERENCES returns(id),
          assigned_by         TEXT,
          assigned_at         TEXT,
          is_assigned         INTEGER NOT NULL DEFAULT 0,
          is_deleted          INTEGER NOT NULL DEFAULT 0
        );

        INSERT INTO email_inbox
          (sender_email, sender_domain, subject_snippet, filename, original_filename,
           file_path, file_size_bytes, received_at, is_assigned, is_deleted)
          VALUES ('a@example.com', 'example.com', 'W-2', 'w2.pdf', 'w2.pdf',
                  '/data/email_inbox/w2.pdf', 1024, '2026-01-01T00:00:00Z', 0, 0);
        """
    )
    conn.commit()
    conn.close()


@pytest.fixture()
def pre_3_1_email_inbox_copy(taxops_db_path):
    _rewind_email_inbox_to_pre_3_1_shape(taxops_db_path)
    return taxops_db_path


def test_migration_adds_3_1_email_inbox_columns_and_preserves_rows(pre_3_1_email_inbox_copy):
    from db import get_connection, init_db

    assert _integrity_ok(pre_3_1_email_inbox_copy), "fixture DB must be valid before migrating"

    conn = get_connection(pre_3_1_email_inbox_copy)
    init_db(conn)

    cols = {r["name"] for r in conn.execute("PRAGMA table_info(email_inbox)").fetchall()}
    assert {"sender_name", "suggested_return_id", "suggestion_method", "suggestion_score"} <= cols

    row = conn.execute(
        "SELECT sender_email, filename, suggested_return_id, suggestion_method, sender_name "
        "FROM email_inbox WHERE sender_email='a@example.com'"
    ).fetchone()
    assert row["filename"] == "w2.pdf", "pre-existing row must survive the migration"
    assert row["suggested_return_id"] is None
    assert row["suggestion_method"] is None
    assert row["sender_name"] is None
    conn.close()

    assert _integrity_ok(pre_3_1_email_inbox_copy)


def test_migration_3_1_idempotent_across_repeated_runs(pre_3_1_email_inbox_copy):
    from db import get_connection, init_db

    for _ in range(3):
        conn = get_connection(pre_3_1_email_inbox_copy)
        init_db(conn)
        conn.close()
        assert _integrity_ok(pre_3_1_email_inbox_copy)
