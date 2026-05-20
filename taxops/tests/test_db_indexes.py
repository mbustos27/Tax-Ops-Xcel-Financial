"""SEC-6: database index coverage tests — verify hot query paths use indexes."""
from __future__ import annotations


_EXPECTED_INDEXES = {
    "idx_return_docs_return",
    "idx_return_docs_type",
    "idx_payments_return",
    "idx_notes_return",
    "idx_missing_docs_return",
    "idx_missing_docs_open",
    "idx_extraction_status",
    "idx_extraction_return",
    "idx_email_class_email",
    "idx_email_class_domain",
    "idx_audit_log_user",
    "idx_returns_status_year",
    "idx_returns_proc_year",
    "idx_returns_updated_at",
    "idx_auth_users_username",
}


def test_sec6_indexes_exist_after_init(taxops_db_path):
    """SEC-6: all hot-path indexes are present after init_db runs."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    existing = {
        r["name"]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
    }
    conn.close()

    missing = _EXPECTED_INDEXES - existing
    assert not missing, f"SEC-6 indexes missing from init_db: {sorted(missing)}"


def _explain_uses_index(conn, sql: str) -> bool:
    """Return True if EXPLAIN QUERY PLAN for the SQL uses an index (not a bare SCAN)."""
    plan = conn.execute(f"EXPLAIN QUERY PLAN {sql}").fetchall()
    for row in plan:
        detail = row["detail"] if "detail" in row.keys() else str(dict(row))
        upper = detail.upper()
        # A raw SCAN without USING is a full table scan
        if "SCAN" in upper and "USING" not in upper and "TEMP" not in upper:
            return False
    return True


def test_return_documents_return_id_uses_index(taxops_db_path):
    """SEC-6: return_documents lookup by return_id uses an index."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    assert _explain_uses_index(
        conn, "SELECT * FROM return_documents WHERE return_id = 1"
    ), "return_documents(return_id) is doing a full table scan"
    conn.close()


def test_payments_return_id_uses_index(taxops_db_path):
    """SEC-6: payments lookup by return_id uses an index."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    assert _explain_uses_index(conn, "SELECT * FROM payments WHERE return_id = 1")
    conn.close()


def test_notes_return_id_uses_index(taxops_db_path):
    """SEC-6: notes lookup by return_id uses an index."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    assert _explain_uses_index(conn, "SELECT * FROM notes WHERE return_id = 1")
    conn.close()


def test_missing_docs_return_id_uses_index(taxops_db_path):
    """SEC-6: missing_docs lookup by return_id and is_resolved uses an index."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    assert _explain_uses_index(
        conn, "SELECT * FROM missing_docs WHERE return_id = 1 AND is_resolved = 0"
    )
    conn.close()


def test_extraction_queue_status_uses_index(taxops_db_path):
    """SEC-6: extraction_queue pending lookup uses an index."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    assert _explain_uses_index(
        conn,
        "SELECT * FROM extraction_queue WHERE status = 'pending' ORDER BY created_at",
    )
    conn.close()


def test_returns_status_year_uses_index(taxops_db_path):
    """SEC-6: dashboard returns filter by client_status + tax_year uses an index."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    assert _explain_uses_index(
        conn,
        "SELECT * FROM returns WHERE client_status = 'PROCESSING' AND tax_year = 2025",
    )
    conn.close()


def test_returns_updated_at_uses_index(taxops_db_path):
    """SEC-6: ORDER BY updated_at on returns uses an index instead of a filesort."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    assert _explain_uses_index(
        conn, "SELECT * FROM returns ORDER BY updated_at DESC LIMIT 50"
    )
    conn.close()
