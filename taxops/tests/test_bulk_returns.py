"""Bulk dashboard API: transactional status / preparer (BULK-3…5)."""

from __future__ import annotations

from db import get_connection


def _seed_return(conn, rid: int, client_id: int, status: str = "PROCESSING", processor=None):
    conn.execute(
        """
        INSERT INTO clients (id, last_name, created_at, updated_at)
        VALUES (?, 'TestBulk', datetime('now'), datetime('now'))
        """,
        (client_id,),
    )
    conn.execute(
        """
        INSERT INTO returns (
          id, client_id, log_number, tax_year, processor, client_status,
          created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        """,
        (rid, client_id, str(8000 + rid), 2024, processor, status),
    )
    conn.commit()


def test_bulk_status_happy_path_audit(client_logged_in, taxops_db_path):
    conn = get_connection(taxops_db_path)
    _seed_return(conn, 801, 701, "PROCESSING")
    _seed_return(conn, 802, 702, "PROCESSING")

    rv = client_logged_in.post(
        "/api/returns/bulk-status",
        json={"return_ids": [801, 802], "status": "FINALIZE"},
    )
    assert rv.status_code == 200
    body = rv.get_json()
    assert body.get("success") is True
    assert body.get("changed") == 2
    conn = get_connection(taxops_db_path)
    for rid in (801, 802):
        row = conn.execute("SELECT client_status FROM returns WHERE id=?", (rid,)).fetchone()
        assert row["client_status"] == "FINALIZE"

    evt = conn.execute(
        """SELECT COUNT(*) FROM status_events
           WHERE return_id IN (801,802) AND event_type='STATUS_CHANGED'
           AND source_file='APP_BULK' AND new_status='FINALIZE'"""
    ).fetchone()[0]
    assert evt >= 2
    conn.close()


def test_bulk_status_rollback_partial_invalid_id(client_logged_in, taxops_db_path):
    conn = get_connection(taxops_db_path)
    _seed_return(conn, 803, 703, "PROCESSING")

    rv = client_logged_in.post(
        "/api/returns/bulk-status",
        json={"return_ids": [803, 999991], "status": "PICKUP"},
    )
    assert rv.status_code == 409
    body = rv.get_json()
    assert body.get("success") is False
    assert any(e.get("return_id") == 999991 for e in body.get("errors") or [])
    conn = get_connection(taxops_db_path)
    row = conn.execute(
        "SELECT client_status FROM returns WHERE id=?",
        (803,),
    ).fetchone()
    assert row["client_status"] == "PROCESSING"
    n_bulk = conn.execute(
        "SELECT COUNT(*) FROM status_events WHERE source_file='APP_BULK'",
    ).fetchone()[0]
    assert n_bulk == 0
    conn.close()


def test_bulk_status_locked_cancelled_returns_409(client_logged_in, taxops_db_path):
    conn = get_connection(taxops_db_path)
    _seed_return(conn, 804, 704, "CANCELLED")

    rv = client_logged_in.post(
        "/api/returns/bulk-status",
        json={"return_ids": [804], "status": "PICKUP"},
    )
    assert rv.status_code == 409
    err = next((e for e in (rv.get_json().get("errors") or []) if e.get("return_id") == 804), None)
    assert err is not None
    conn.close()


def test_bulk_processor_skips_noop(client_logged_in, taxops_db_path):
    conn = get_connection(taxops_db_path)
    _seed_return(conn, 805, 705, processor="Lucila Yanez")

    rv = client_logged_in.post(
        "/api/returns/bulk-processor",
        json={"return_ids": [805], "processor": "ly"},
    )
    assert rv.status_code == 200
    body = rv.get_json()
    assert body.get("success") is True
    assert body.get("changed") == 0


def test_bulk_processor_updates_and_audits(client_logged_in, taxops_db_path):
    conn = get_connection(taxops_db_path)
    _seed_return(conn, 806, 706, processor=None)

    rv = client_logged_in.post(
        "/api/returns/bulk-processor",
        json={"return_ids": [806], "processor": "mb"},
    )
    assert rv.status_code == 200
    assert rv.get_json().get("changed") == 1
    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT processor FROM returns WHERE id=?", (806,)).fetchone()
    proc = row["processor"] if row else None
    assert proc and "Bustos" in proc
    evt = conn.execute(
        """SELECT 1 FROM status_events WHERE return_id=806 AND event_type='PROCESSOR_CHANGED'
           AND source_file='APP_BULK' LIMIT 1"""
    ).fetchone()
    assert evt is not None
    conn.close()
