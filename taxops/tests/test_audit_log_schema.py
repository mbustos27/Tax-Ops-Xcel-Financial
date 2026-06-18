"""AUDIT-1: audit_log table shape after init_db."""

from __future__ import annotations

from db import get_connection


def test_audit_log_columns_after_init(taxops_db_path: str):
    conn = get_connection(taxops_db_path)
    rows = conn.execute("PRAGMA table_info(audit_log)").fetchall()
    names = [r["name"] for r in rows]
    assert names == [
        "id",
        "user_id",
        "action",
        "entity_type",
        "entity_id",
        "before_json",
        "after_json",
        "ip_address",
        "created_at",
    ]
    pk = [r["name"] for r in rows if r["pk"]]
    assert pk == ["id"]
    conn.close()
