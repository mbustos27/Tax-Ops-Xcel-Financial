"""Tests for post-ai_routes reference helpers (rejection lookup + extract requeue)."""
from __future__ import annotations

from routes.reference import _normalize_rejection_code


def test_normalize_rejection_code_families():
    assert _normalize_rejection_code("ind 507") == "IND-507"
    assert _normalize_rejection_code("R000050001") == "R0000-50001"
    assert _normalize_rejection_code("IND-507") == "IND-507"


def test_rejection_code_lookup_known(client_logged_in):
    resp = client_logged_in.post(
        "/api/rejection-code/lookup",
        json={"code": "IND-507"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["recognized"] is True
    assert data["source"] == "reference"
    assert data["explanation"]
    assert data["normalized_code"] == "IND-507"


def test_rejection_code_lookup_unknown(client_logged_in):
    resp = client_logged_in.post(
        "/api/rejection-code/lookup",
        json={"code": "ZZZZ-99999"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["recognized"] is False
    assert data["source"] == "unknown"


def test_rejection_code_lookup_requires_code(client_logged_in):
    resp = client_logged_in.post("/api/rejection-code/lookup", json={})
    assert resp.status_code == 400


def test_requeue_extraction(client_logged_in, app):
    from db import get_connection
    from utils import now

    with app.app_context():
        conn = get_connection()
        conn.execute(
            "INSERT INTO clients (id, last_name, first_name, created_at) "
            "VALUES (9101, 'Test', 'Requeue', ?)",
            (now(),),
        )
        conn.execute(
            "INSERT INTO returns (id, client_id, tax_year, created_at) "
            "VALUES (9101, 9101, 2025, ?)",
            (now(),),
        )
        conn.execute(
            "INSERT INTO return_documents "
            "(id, return_id, filename, doc_type, source, file_path, uploaded_at, is_deleted) "
            "VALUES (9101, 9101, 'test.pdf', 'W-2', 'walk_in', '/tmp/x.pdf', ?, 0)",
            (now(),),
        )
        conn.commit()
        conn.close()

    resp = client_logged_in.post("/return/9101/documents/9101/requeue-extraction")
    assert resp.status_code == 200, resp.get_data(as_text=True)
    data = resp.get_json()
    assert data["success"] is True
    assert data["status"] == "pending"

    with app.app_context():
        conn = get_connection()
        row = conn.execute(
            "SELECT status, attempts FROM extraction_queue WHERE doc_id = 9101"
        ).fetchone()
        conn.close()
    assert row is not None
    assert row["status"] == "pending"
    assert row["attempts"] == 0


def test_requeue_extraction_already_pending(client_logged_in, app):
    from db import get_connection
    from utils import now

    with app.app_context():
        conn = get_connection()
        conn.execute(
            "INSERT OR IGNORE INTO clients (id, last_name, first_name, created_at) "
            "VALUES (9102, 'Test', 'Pending', ?)",
            (now(),),
        )
        conn.execute(
            "INSERT OR IGNORE INTO returns (id, client_id, tax_year, created_at) "
            "VALUES (9102, 9102, 2025, ?)",
            (now(),),
        )
        conn.execute(
            "INSERT OR IGNORE INTO return_documents "
            "(id, return_id, filename, doc_type, source, file_path, uploaded_at, is_deleted) "
            "VALUES (9102, 9102, 'test.pdf', 'W-2', 'walk_in', '/tmp/y.pdf', ?, 0)",
            (now(),),
        )
        conn.execute(
            "INSERT INTO extraction_queue "
            "(doc_id, return_id, status, attempts, created_at) "
            "VALUES (9102, 9102, 'pending', 0, ?)",
            (now(),),
        )
        conn.commit()
        conn.close()

    resp = client_logged_in.post("/return/9102/documents/9102/requeue-extraction")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True
    assert data["message"] == "Already queued"


def test_reference_routes_registered(app):
    rules = {r.rule for r in app.url_map.iter_rules()}
    assert "/api/rejection-code/lookup" in rules
    assert (
        "/return/<int:return_id>/documents/<int:doc_id>/requeue-extraction" in rules
    )
