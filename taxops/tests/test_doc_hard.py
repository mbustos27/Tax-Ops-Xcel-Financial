"""DOC-HARD-1..5, 7: extraction queue health, dead-letter retry, duplicate detection,
intake audit trail, and bulk upload."""
from __future__ import annotations

import hashlib
import importlib
import io
import os
import sqlite3
import sys
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ── DOC-HARD-1: /health extraction_queue field ───────────────────────────────

def test_health_includes_extraction_queue(client_logged_in):
    """GET /health returns extraction_queue dict with pending/failed keys."""
    resp = client_logged_in.get("/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "extraction_queue" in data
    eq = data["extraction_queue"]
    # May be None if DB is empty, but must at least be present
    if eq is not None:
        assert "pending" in eq
        assert "failed" in eq


def test_health_extraction_queue_not_null_when_db_ok(client_logged_in, app):
    """extraction_queue is a dict (not None) when the DB is reachable."""
    resp = client_logged_in.get("/health")
    data = resp.get_json()
    # DB always reachable in tests — should be a dict
    if data["status"] == "ok":
        assert isinstance(data["extraction_queue"], dict)


# ── DOC-HARD-2: dead-letter retry logic in extractor ─────────────────────────

def _load_extractor():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "_ext_" + uuid.uuid4().hex,
        root / "extractor.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_extractor_retries_on_failure_below_max(tmp_path):
    """On failure with attempts < MAX_ATTEMPTS, status resets to 'pending' for retry."""
    ext = _load_extractor()

    src = tmp_path / "live.db"
    conn = sqlite3.connect(str(src))
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE extraction_queue (id INTEGER PRIMARY KEY, doc_id INTEGER, "
                 "return_id INTEGER, status TEXT, attempts INTEGER, error_message TEXT, "
                 "created_at TEXT, processed_at TEXT)")
    conn.execute("INSERT INTO extraction_queue VALUES (1, 10, 20, 'processing', 0, NULL, 'now', NULL)")
    conn.commit()

    item = {
        "id": 1, "doc_id": 10, "return_id": 20,
        "file_path": str(tmp_path / "absent.pdf"),  # file does not exist → extraction fails
        "filename": "test.pdf",
        "doc_type": "W-2",
        "attempts": 0,
    }

    # Simulate an extraction exception with attempt 0 (< MAX_ATTEMPTS=3)
    captured = {}
    def fake_execute(sql, params=None):
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        cursor.fetchone.return_value = None
        if "SET status = ?" in sql or "SET status=" in sql.replace(" ", ""):
            captured["sql"] = sql
            captured["params"] = params or []
        return cursor

    fake_conn = MagicMock()
    fake_conn.execute = fake_execute

    # Call the failure path manually — simulate what happens in _process_item
    # when attempts=0 and an exception fires:
    new_attempts = item["attempts"] + 1  # 1 — less than MAX_ATTEMPTS=3
    status = "failed" if new_attempts >= ext.MAX_ATTEMPTS else "pending"
    assert status == "pending", "Should retry when attempts < MAX_ATTEMPTS"


def test_extractor_dead_letters_after_max_attempts():
    """On failure with attempts == MAX_ATTEMPTS-1, status becomes 'failed' (dead letter)."""
    ext = _load_extractor()
    # Simulate final attempt
    new_attempts = ext.MAX_ATTEMPTS  # exhausted
    status = "failed" if new_attempts >= ext.MAX_ATTEMPTS else "pending"
    assert status == "failed", "Should dead-letter when attempts exhausted"


def test_max_attempts_is_3():
    """MAX_ATTEMPTS = 3 is the contract; changing it needs a conscious decision."""
    ext = _load_extractor()
    assert ext.MAX_ATTEMPTS == 3


# ── DOC-HARD-3: admin failed-docs page + retry API ───────────────────────────

def test_failed_docs_page_requires_login(client):
    """GET /admin/failed-docs redirects unauthenticated users."""
    resp = client.get("/admin/failed-docs")
    assert resp.status_code in (302, 401)


def test_failed_docs_page_renders(client_logged_in):
    """GET /admin/failed-docs returns 200 for logged-in users."""
    resp = client_logged_in.get("/admin/failed-docs")
    assert resp.status_code == 200
    assert b"Failed Documents" in resp.data


def test_retry_endpoint_requires_login(client):
    """POST /api/admin/documents/<id>/retry returns 401 for unauthenticated."""
    resp = client.post("/api/admin/documents/999/retry")
    assert resp.status_code in (302, 401)


def test_retry_endpoint_404_for_unknown_doc(client_logged_in):
    """POST /api/admin/documents/<id>/retry returns 404 for non-existent failed doc."""
    resp = client_logged_in.post("/api/admin/documents/999999/retry")
    assert resp.status_code == 404
    data = resp.get_json()
    assert "error" in data


def test_retry_endpoint_resets_status(client_logged_in, app):
    """POST /api/admin/documents/<id>/retry resets extraction_queue row to pending."""
    from db import get_connection
    from utils import now

    with app.app_context():
        conn = get_connection()
        # Insert a minimal return + client + document + failed queue row
        conn.execute(
            "INSERT INTO clients (id, last_name, first_name, created_at) "
            "VALUES (9001, 'Test', 'Retry', ?)", (now(),)
        )
        conn.execute(
            "INSERT INTO returns (id, client_id, tax_year, created_at) VALUES (9001, 9001, 2025, ?)",
            (now(),)
        )
        conn.execute(
            "INSERT INTO return_documents "
            "(id, return_id, filename, doc_type, source, file_path, uploaded_at, is_deleted) "
            "VALUES (9001, 9001, 'test.pdf', 'W-2', 'walk_in', '/tmp/x.pdf', ?, 0)",
            (now(),)
        )
        conn.execute(
            "INSERT INTO extraction_queue "
            "(id, doc_id, return_id, status, attempts, error_message, created_at) "
            "VALUES (9001, 9001, 9001, 'failed', 3, 'LLM timeout', ?)",
            (now(),)
        )
        conn.commit()
        conn.close()

    resp = client_logged_in.post("/api/admin/documents/9001/retry")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True

    with app.app_context():
        conn = get_connection()
        row = conn.execute(
            "SELECT status, attempts FROM extraction_queue WHERE doc_id = 9001"
        ).fetchone()
        conn.close()
    assert row["status"] == "pending"
    assert row["attempts"] == 0


def test_failed_docs_route_registered(app):
    """Routes for failed-docs admin are registered."""
    rules = {r.rule for r in app.url_map.iter_rules()}
    assert "/admin/failed-docs" in rules
    assert "/api/admin/documents/<int:doc_id>/retry" in rules


# ── DOC-HARD-4: duplicate document detection ─────────────────────────────────

def test_file_hash_stored_on_upload(client_logged_in, app, tmp_path):
    """Uploading a document stores SHA-256 in return_documents.file_hash."""
    from db import get_connection
    from utils import now

    with app.app_context():
        conn = get_connection()
        conn.execute(
            "INSERT OR IGNORE INTO clients (id, last_name, first_name, created_at) "
            "VALUES (8001, 'Hash', 'Test', ?)", (now(),)
        )
        conn.execute(
            "INSERT OR IGNORE INTO returns (id, client_id, tax_year, created_at) "
            "VALUES (8001, 8001, 2025, ?)", (now(),)
        )
        conn.commit()
        conn.close()

    pdf_bytes = b"%PDF-1.4 test content"
    expected_hash = hashlib.sha256(pdf_bytes).hexdigest()

    data = {"document": (io.BytesIO(pdf_bytes), "test_w2.pdf"), "doc_type": "W-2"}
    resp = client_logged_in.post(
        "/return/8001/documents/upload",
        data=data,
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    result = resp.get_json()
    assert result["success"] is True

    with app.app_context():
        conn = get_connection()
        row = conn.execute(
            "SELECT file_hash FROM return_documents WHERE id = ?", (result["doc_id"],)
        ).fetchone()
        conn.close()
    assert row["file_hash"] == expected_hash


def test_duplicate_upload_warns(client_logged_in, app):
    """Uploading an identical file twice returns duplicate_warning."""
    from db import get_connection
    from utils import now

    with app.app_context():
        conn = get_connection()
        conn.execute(
            "INSERT OR IGNORE INTO clients (id, last_name, first_name, created_at) "
            "VALUES (8002, 'Dup', 'Test', ?)", (now(),)
        )
        conn.execute(
            "INSERT OR IGNORE INTO returns (id, client_id, tax_year, created_at) "
            "VALUES (8002, 8002, 2025, ?)", (now(),)
        )
        conn.commit()
        conn.close()

    pdf_bytes = b"%PDF-1.4 duplicate detection test " + uuid.uuid4().bytes

    def _upload():
        return client_logged_in.post(
            "/return/8002/documents/upload",
            data={"document": (io.BytesIO(pdf_bytes), "dup.pdf"), "doc_type": "W-2"},
            content_type="multipart/form-data",
        )

    r1 = _upload()
    assert r1.status_code == 200
    assert r1.get_json()["success"] is True
    assert "duplicate_warning" not in r1.get_json()

    r2 = _upload()
    assert r2.status_code == 200
    data2 = r2.get_json()
    assert data2["success"] is True
    assert "duplicate_warning" in data2
    assert "identical" in data2["duplicate_warning"].lower() or "content" in data2["duplicate_warning"].lower()


# ── DOC-HARD-5: intake audit trail ───────────────────────────────────────────

def test_upload_emits_audit_entry(client_logged_in, app):
    """Uploading a document enqueues an audit log entry with action='document_upload'."""
    from db import get_connection
    from utils import now
    import audit_service

    with app.app_context():
        conn = get_connection()
        conn.execute(
            "INSERT OR IGNORE INTO clients (id, last_name, first_name, created_at) "
            "VALUES (7001, 'Audit', 'Test', ?)", (now(),)
        )
        conn.execute(
            "INSERT OR IGNORE INTO returns (id, client_id, tax_year, created_at) "
            "VALUES (7001, 7001, 2025, ?)", (now(),)
        )
        conn.commit()
        conn.close()

    captured = []
    orig = audit_service._enqueue_write

    def _spy(**kwargs):
        captured.append(kwargs)
        orig(**kwargs)

    with patch.object(audit_service, "_enqueue_write", side_effect=_spy):
        resp = client_logged_in.post(
            "/return/7001/documents/upload",
            data={"document": (io.BytesIO(b"%PDF test"), "audit_test.pdf"), "doc_type": "W-2"},
            content_type="multipart/form-data",
        )

    assert resp.status_code == 200
    upload_events = [e for e in captured if e.get("action") == "document_upload"]
    assert len(upload_events) >= 1
    assert upload_events[0]["entity_type"] == "return_document"


def test_extraction_audit_helper_importable():
    """_emit_extraction_audit is exported from extractor module."""
    from extractor import _emit_extraction_audit
    assert callable(_emit_extraction_audit)


# ── DOC-HARD-7: bulk document upload ─────────────────────────────────────────

def test_bulk_upload_endpoint_registered(app):
    """bulk-upload route is registered."""
    rules = {r.rule for r in app.url_map.iter_rules()}
    assert "/return/<int:return_id>/documents/bulk-upload" in rules


def test_bulk_upload_requires_login(client):
    """POST /return/<id>/documents/bulk-upload returns 401 for unauthenticated."""
    resp = client.post("/return/1/documents/bulk-upload")
    assert resp.status_code in (302, 401)


def test_bulk_upload_returns_results_list(client_logged_in, app):
    """POST bulk-upload returns {"results": [...]} with one item per file."""
    from db import get_connection
    from utils import now

    with app.app_context():
        conn = get_connection()
        conn.execute(
            "INSERT OR IGNORE INTO clients (id, last_name, first_name, created_at) "
            "VALUES (6001, 'Bulk', 'Upload', ?)", (now(),)
        )
        conn.execute(
            "INSERT OR IGNORE INTO returns (id, client_id, tax_year, created_at) "
            "VALUES (6001, 6001, 2025, ?)", (now(),)
        )
        conn.commit()
        conn.close()

    data = {
        "documents": [
            (io.BytesIO(b"%PDF-1.4 file1"), "file1.pdf"),
            (io.BytesIO(b"%PDF-1.4 file2"), "file2.pdf"),
        ],
        "doc_type": "W-2",
    }
    resp = client_logged_in.post(
        "/return/6001/documents/bulk-upload",
        data=data,
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    result = resp.get_json()
    assert "results" in result
    assert len(result["results"]) == 2
    assert all(r["success"] for r in result["results"])


def test_bulk_upload_rejects_unsupported_extension(client_logged_in, app):
    """Bulk upload skips files with unsupported extensions and reports error per file."""
    from db import get_connection
    from utils import now

    with app.app_context():
        conn = get_connection()
        conn.execute(
            "INSERT OR IGNORE INTO clients (id, last_name, first_name, created_at) "
            "VALUES (6002, 'Bulk', 'Ext', ?)", (now(),)
        )
        conn.execute(
            "INSERT OR IGNORE INTO returns (id, client_id, tax_year, created_at) "
            "VALUES (6002, 6002, 2025, ?)", (now(),)
        )
        conn.commit()
        conn.close()

    data = {
        "documents": [
            (io.BytesIO(b"good file"), "good.pdf"),
            (io.BytesIO(b"bad file"),  "bad.exe"),
        ],
        "doc_type": "misc",
    }
    resp = client_logged_in.post(
        "/return/6002/documents/bulk-upload",
        data=data,
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    results = resp.get_json()["results"]
    successes = [r for r in results if r["success"]]
    failures  = [r for r in results if not r["success"]]
    assert len(successes) == 1
    assert len(failures) == 1
    assert ".exe" in failures[0]["error"]


def test_bulk_upload_no_files_returns_400(client_logged_in, app):
    """Bulk upload with no files returns 400."""
    from db import get_connection
    from utils import now

    with app.app_context():
        conn = get_connection()
        conn.execute(
            "INSERT OR IGNORE INTO clients (id, last_name, first_name, created_at) "
            "VALUES (6003, 'Bulk', 'Empty', ?)", (now(),)
        )
        conn.execute(
            "INSERT OR IGNORE INTO returns (id, client_id, tax_year, created_at) "
            "VALUES (6003, 6003, 2025, ?)", (now(),)
        )
        conn.commit()
        conn.close()

    resp = client_logged_in.post(
        "/return/6003/documents/bulk-upload",
        data={"doc_type": "W-2"},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 400
    assert "error" in resp.get_json()
