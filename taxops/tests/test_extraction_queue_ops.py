"""Extraction queue ops: stuck processing requeue + status CLI helpers."""
from __future__ import annotations

from extractor import MAX_ATTEMPTS, _requeue_stuck_processing
from utils import now


def test_requeue_stuck_processing_null_stamp(taxops_db_path):
    from db import get_connection, init_db

    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.execute(
        """
        INSERT INTO clients (id, last_name, first_name, created_at)
        VALUES (1, 'A', 'B', ?)
        """,
        (now(),),
    )
    conn.execute(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status, created_at)
        VALUES (1, 1, '1', 2025, 'PROCESSING', ?)
        """,
        (now(),),
    )
    conn.execute(
        """
        INSERT INTO return_documents (id, return_id, filename, file_path, uploaded_at, is_deleted)
        VALUES (1, 1, 'x.pdf', '/tmp/nope.pdf', ?, 0)
        """,
        (now(),),
    )
    conn.execute(
        """
        INSERT INTO extraction_queue
          (id, doc_id, return_id, status, attempts, created_at, processed_at)
        VALUES (1, 1, 1, 'processing', 1, ?, NULL)
        """,
        (now(),),
    )
    conn.commit()
    n = _requeue_stuck_processing(conn, stale_minutes=15)
    assert n == 1
    row = conn.execute("SELECT status, error_message FROM extraction_queue WHERE id = 1").fetchone()
    assert row["status"] == "pending"
    assert "stuck" in (row["error_message"] or "").lower()
    conn.close()


def test_max_attempts_constant():
    assert MAX_ATTEMPTS >= 1
