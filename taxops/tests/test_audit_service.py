"""AUDIT-2 … AUDIT-8 — masking, inserts, purge, retention settings."""

from __future__ import annotations

import time

from audit_service import (
    mask_audit_payload,
    purge_audit_logs_older_than,
    set_audit_retention_years,
)

from db import get_connection


def test_app_settings_table_exists_after_init(taxops_db_path: str):
    conn = get_connection(taxops_db_path)
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='app_settings'",
    ).fetchone()
    assert row is not None
    conn.close()


def test_mask_audit_financial_ssn_patterns():
    m = mask_audit_payload(
        {"fee_paid": 420, "name": "ok", "ssn_last4": "1234", "nested": {"bank_routing": "abc"}}
    )
    assert m["fee_paid"] == "[REDACTED]"
    assert m["name"] == "ok"
    assert m["ssn_last4"] == "[REDACTED]"
    assert m["nested"]["bank_routing"] == "[REDACTED]"


def test_purge_audit_deletes_old_rows_only(taxops_db_path: str):
    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT INTO audit_log
          (user_id, action, entity_type, entity_id, before_json, after_json, ip_address, created_at)
        VALUES ('t', 'POST x', 'test', '1', '{}', '{}', '127.0.0.1', '2000-01-01T00:00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO audit_log
          (user_id, action, entity_type, entity_id, before_json, after_json, ip_address, created_at)
        VALUES ('t', 'POST y', 'test', '2', '{}', '{}', '127.0.0.1', '2099-01-01T00:00:00')
        """
    )
    conn.commit()
    n = purge_audit_logs_older_than(conn, 10)
    conn.commit()
    assert n >= 1
    left = conn.execute("SELECT COUNT(*) n FROM audit_log WHERE entity_id='1'").fetchone()["n"]
    assert left == 0
    conn.close()


def test_retention_settings_roundtrip(taxops_db_path: str):
    conn = get_connection(taxops_db_path)
    set_audit_retention_years(conn, 12)
    conn.commit()
    from audit_service import get_audit_retention_years

    assert get_audit_retention_years(conn) == 12
    conn.close()


def test_audit_middleware_inserts_on_api_status(client_logged_in, taxops_db_path: str):
    """Requires logged-in session + async writer thread."""
    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (id, last_name, created_at, updated_at) "
        "VALUES (8801, 'AuditHook', datetime('now'), datetime('now'))",
    )
    conn.execute(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status, created_at, updated_at)
        VALUES (9901, 8801, 'AH9901', 2025, 'PROCESSING', datetime('now'), datetime('now'))
        """,
    )
    conn.commit()
    conn.close()

    rv = client_logged_in.post("/api/return/9901/status", json={"status": "FINALIZE"})
    assert rv.status_code == 200

    n = 0
    deadline = time.time() + 6.0
    while time.time() < deadline:
        c = get_connection(taxops_db_path)
        try:
            n = c.execute(
                "SELECT COUNT(*) n FROM audit_log WHERE entity_type='return' "
                "AND entity_id='9901' AND action LIKE '%api_status%'",
            ).fetchone()["n"]
        finally:
            c.close()
        if n:
            break
        time.sleep(0.08)

    assert n >= 1
