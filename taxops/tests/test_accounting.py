"""ACCOUNTING-12: Tests for the Receipt OCR → QuickBooks accounting pipeline.

Coverage:
  - Schema: receipt_queue table + indices exist in db.py
  - Config: ACCOUNTING-2 env vars loaded correctly
  - receipt_ocr: JSON parse, partial failure, _error propagation
  - coa_matcher: CSV load, rapidfuzz fallback, confidence bands
  - qb_export: CSV format, IIF format, file writing
  - Routes: upload, approve, reject, export, settings, coa_rebuild (Flask test client)
  - Intake hook: doc_type='receipt' enqueues receipt_queue
  - Audit: approvals/rejections/exports emit audit entries
  - Worker: _process_one happy path, retry, dead-letter
"""
from __future__ import annotations

import contextlib
import csv
import io
import json
import os
import sqlite3
import tempfile
import textwrap
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# Uses conftest.py fixtures: taxops_db_path, app, client, client_logged_in


# ─────────────────────────────────────────────────────────────────────────────
# ACCOUNTING-1: Schema
# ─────────────────────────────────────────────────────────────────────────────

def test_receipt_queue_table_exists():
    """receipt_queue table and key columns exist after init_db."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    from db import init_db
    init_db(conn)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(receipt_queue)")}
    for expected in ("id", "status", "image_path", "vendor", "total_amount",
                     "suggested_category", "approved_category", "confidence",
                     "line_items", "category_candidates", "ocr_raw", "attempts"):
        assert expected in cols, f"Missing column: {expected}"
    conn.close()


def test_receipt_queue_indices_exist():
    """receipt_queue status+date and doc link indices exist."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    from db import init_db
    init_db(conn)
    idx = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert "idx_receipt_queue_status" in idx
    assert "idx_receipt_queue_doc" in idx
    conn.close()


def test_schema_version_incremented():
    """Schema version is >= 3 after ACCOUNTING-1 migration."""
    from db import CURRENT_SCHEMA_VERSION
    assert CURRENT_SCHEMA_VERSION >= 3


# ─────────────────────────────────────────────────────────────────────────────
# ACCOUNTING-2: Config
# ─────────────────────────────────────────────────────────────────────────────

def test_accounting_config_defaults():
    import config as _cfg
    assert isinstance(_cfg.ACCOUNTING_VISION_MODEL, str) and _cfg.ACCOUNTING_VISION_MODEL
    assert isinstance(_cfg.ACCOUNTING_OCR_TIMEOUT, int) and _cfg.ACCOUNTING_OCR_TIMEOUT > 0
    assert _cfg.QB_EXPORT_MODE in ("csv", "iif")
    assert 0.0 < _cfg.ACCOUNTING_CONFIDENCE_HIGH <= 1.0
    assert 0.0 < _cfg.ACCOUNTING_CONFIDENCE_MEDIUM < _cfg.ACCOUNTING_CONFIDENCE_HIGH
    assert _cfg.ACCOUNTING_MAX_ATTEMPTS >= 1


def test_accounting_config_env_override(monkeypatch):
    monkeypatch.setenv("QB_EXPORT_MODE", "iif")
    monkeypatch.setenv("ACCOUNTING_CONFIDENCE_HIGH", "0.95")
    import importlib, config as _cfg
    importlib.reload(_cfg)
    assert _cfg.QB_EXPORT_MODE == "iif"
    assert _cfg.ACCOUNTING_CONFIDENCE_HIGH == pytest.approx(0.95)
    importlib.reload(_cfg)  # restore


# ─────────────────────────────────────────────────────────────────────────────
# ACCOUNTING-3: receipt_ocr
# ─────────────────────────────────────────────────────────────────────────────

def test_receipt_ocr_parse_response():
    """_parse_response handles clean JSON and strips markdown fences."""
    from services.receipt_ocr import _parse_response
    raw = '```json\n{"vendor": "Staples", "date": "2026-05-19", "total_amount": 42.50, "payment_method": "credit", "line_items": []}\n```'
    result = _parse_response(raw)
    assert result["vendor"] == "Staples"
    assert result["total_amount"] == pytest.approx(42.50)


def test_receipt_ocr_image_load_failure(tmp_path):
    """extract_receipt_data sets _error when image file is missing."""
    from services.receipt_ocr import extract_receipt_data
    result = extract_receipt_data(str(tmp_path / "nonexistent.jpg"))
    assert result["_error"] is not None
    assert "image_load_failed" in result["_error"]


def test_receipt_ocr_ollama_failure(tmp_path):
    """extract_receipt_data sets _error when Ollama call fails."""
    img = tmp_path / "test.jpg"
    img.write_bytes(b"\xff\xd8\xff" + b"\x00" * 100)

    with patch("llm.chat", side_effect=Exception("connection refused")):
        from services.receipt_ocr import extract_receipt_data
        result = extract_receipt_data(str(img))
    assert result["_error"] is not None
    assert "ollama_call_failed" in result["_error"]


def test_receipt_ocr_json_parse_failure(tmp_path):
    """extract_receipt_data sets _error when LLM returns non-JSON."""
    img = tmp_path / "test.jpg"
    img.write_bytes(b"\xff\xd8\xff" + b"\x00" * 100)

    with patch("llm.chat", return_value="sorry, I cannot help with that"):
        from services.receipt_ocr import extract_receipt_data
        result = extract_receipt_data(str(img))
    assert result["_error"] is not None
    assert "json_parse_failed" in result["_error"]


def test_receipt_ocr_happy_path(tmp_path):
    """extract_receipt_data returns structured dict on success."""
    img = tmp_path / "receipt.jpg"
    img.write_bytes(b"\xff\xd8\xff" + b"\x00" * 100)
    mock_response = json.dumps({
        "vendor": "Home Depot",
        "date": "2026-05-10",
        "total_amount": 87.99,
        "payment_method": "credit",
        "line_items": [{"description": "Paint brushes", "amount": 12.99}],
    })
    with patch("llm.chat", return_value=mock_response):
        from services.receipt_ocr import extract_receipt_data
        result = extract_receipt_data(str(img))
    assert result["vendor"] == "Home Depot"
    assert result["total_amount"] == pytest.approx(87.99)
    assert len(result["line_items"]) == 1
    assert result["_error"] is None


# ─────────────────────────────────────────────────────────────────────────────
# ACCOUNTING-4: coa_matcher
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def coa_csv(tmp_path):
    """Minimal COA CSV fixture."""
    path = tmp_path / "coa.csv"
    rows = [
        {"account_code": "6100", "account_name": "Office Supplies", "account_type": "Expense", "description": "Paper pens stationery"},
        {"account_code": "6200", "account_name": "Meals & Entertainment", "account_type": "Expense", "description": "Restaurant food dining"},
        {"account_code": "6300", "account_name": "Travel", "account_type": "Expense", "description": "Airfare hotel transport"},
        {"account_code": "5000", "account_name": "Advertising", "account_type": "Expense", "description": "Marketing promotions ads"},
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["account_code", "account_name", "account_type", "description"])
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


def test_coa_matcher_load(coa_csv, monkeypatch):
    """CoaMatcher loads entries from CSV."""
    monkeypatch.setenv("COA_CSV_PATH", coa_csv)
    import config as _cfg
    monkeypatch.setattr(_cfg, "COA_CSV_PATH", coa_csv)
    from services.coa_matcher import CoaMatcher
    m = CoaMatcher()
    ok = m.load(coa_csv)
    assert ok
    assert len(m._entries) == 4


def test_coa_matcher_rapidfuzz_match(coa_csv, monkeypatch):
    """categorize returns best match using rapidfuzz fallback (no sentence-transformers needed)."""
    monkeypatch.setenv("COA_CSV_PATH", coa_csv)
    import config as _cfg
    monkeypatch.setattr(_cfg, "COA_CSV_PATH", coa_csv)
    monkeypatch.setattr(_cfg, "ACCOUNTING_CONFIDENCE_HIGH", 0.80)
    monkeypatch.setattr(_cfg, "ACCOUNTING_CONFIDENCE_MEDIUM", 0.50)

    from services.coa_matcher import CoaMatcher
    m = CoaMatcher()
    m.load(coa_csv)
    m._use_embeddings = False  # force rapidfuzz path

    result = m.categorize("office paper and pens from Staples")
    assert result["suggested_category"] == "Office Supplies"
    assert result["suggested_account"] == "6100"
    assert result["confidence"] in ("high", "medium", "low")
    assert len(result["candidates"]) >= 1


def test_coa_matcher_missing_csv(monkeypatch):
    """categorize returns error result when COA CSV is not configured."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "COA_CSV_PATH", "")
    from services.coa_matcher import CoaMatcher
    m = CoaMatcher()
    result = m.categorize("office supplies")
    assert result["_error"] == "coa_not_loaded"
    assert result["suggested_category"] is None


def test_coa_matcher_confidence_bands(monkeypatch):
    """_confidence_band returns correct band based on score."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "ACCOUNTING_CONFIDENCE_HIGH", 0.80)
    monkeypatch.setattr(_cfg, "ACCOUNTING_CONFIDENCE_MEDIUM", 0.50)
    from services.coa_matcher import _confidence_band
    assert _confidence_band(0.95) == "high"
    assert _confidence_band(0.65) == "medium"
    assert _confidence_band(0.30) == "low"


# ─────────────────────────────────────────────────────────────────────────────
# ACCOUNTING-5: qb_export
# ─────────────────────────────────────────────────────────────────────────────

SAMPLE_ROWS = [
    {
        "id": 1,
        "receipt_date": "2026-05-10",
        "vendor": "Staples",
        "total_amount": 45.67,
        "payment_method": "credit",
        "approved_category": "Office Supplies",
        "approved_account": "6100",
        "review_notes": "Q2 supplies",
    },
    {
        "id": 2,
        "receipt_date": "2026-05-12",
        "vendor": "Delta Airlines",
        "total_amount": 312.00,
        "payment_method": "credit",
        "approved_category": "Travel",
        "approved_account": "6300",
        "review_notes": "",
    },
]


def test_export_to_csv_format():
    """export_to_csv produces valid QB Online CSV."""
    from services.qb_export import export_to_csv
    content = export_to_csv(SAMPLE_ROWS)
    lines = content.strip().splitlines()
    assert lines[0] == "Date,Description,Amount,Memo,Account,Name"
    assert len(lines) == 3  # header + 2 data rows
    data_row = list(csv.reader([lines[1]]))[0]
    assert data_row[1] == "Staples"
    assert "-45.67" in data_row[2]
    assert "Office Supplies" in data_row[4]


def test_export_to_iif_format():
    """export_to_iif produces valid QB Desktop IIF blocks."""
    from services.qb_export import export_to_iif
    content = export_to_iif(SAMPLE_ROWS)
    assert "!TRNS" in content
    assert "TRNS" in content
    assert "SPL" in content
    assert "ENDTRNS" in content
    assert "Staples" in content
    assert "Office Supplies" in content
    assert "45.67" in content


def test_export_receipts_writes_file(tmp_path, monkeypatch):
    """export_receipts writes a file and returns correct path + format."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "QB_EXPORT_MODE", "csv")
    from services.qb_export import export_receipts
    path, fmt = export_receipts(SAMPLE_ROWS, tmp_path)
    assert fmt == "csv"
    assert os.path.isfile(path)
    content = Path(path).read_text(encoding="utf-8")
    assert "Staples" in content


def test_export_receipts_iif(tmp_path, monkeypatch):
    import config as _cfg
    monkeypatch.setattr(_cfg, "QB_EXPORT_MODE", "iif")
    from services.qb_export import export_receipts
    path, fmt = export_receipts(SAMPLE_ROWS, tmp_path)
    assert fmt == "iif"
    assert path.endswith(".iif")


def test_export_date_normalisation():
    """_fmt_date_csv handles various input formats."""
    from services.qb_export import _fmt_date_csv
    assert _fmt_date_csv("2026-05-10") == "05/10/2026"
    assert _fmt_date_csv("05/10/2026") == "05/10/2026"
    assert _fmt_date_csv(None) != ""  # returns today's date string


# ─────────────────────────────────────────────────────────────────────────────
# ACCOUNTING-6: Routes (Flask test client)
# ─────────────────────────────────────────────────────────────────────────────

def _seed_receipt(taxops_db_path, image_path, status="review"):
    conn = sqlite3.connect(taxops_db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """INSERT INTO receipt_queue
           (image_path, original_filename, status, vendor, total_amount,
            receipt_date, suggested_category, suggested_account, confidence,
            line_items, category_candidates, attempts, created_at)
           VALUES (?, 'test.jpg', ?, 'Staples', 45.67,
                   '2026-05-10', 'Office Supplies', '6100', 'high',
                   '[]', '[]', 0, '2026-05-19T00:00:00Z')""",
        (image_path, status),
    )
    conn.commit()
    rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return rid


def test_receipt_queue_list_requires_login(client):
    r = client.get("/accounting/receipts")
    assert r.status_code in (302, 401)


def test_receipt_queue_list_authenticated(client_logged_in):
    r = client_logged_in.get("/accounting/receipts?status=review")
    assert r.status_code == 200
    assert b"Receipt Queue" in r.data


def test_receipt_upload_success(client_logged_in, tmp_path):
    with patch("accounting_worker._notify_receipt_worker"):
        img_bytes = b"\xff\xd8\xff\xe0" + b"\x00" * 20
        data = {"receipt": (io.BytesIO(img_bytes), "test_receipt.jpg")}
        r = client_logged_in.post("/accounting/receipts/upload",
                                   data=data, content_type="multipart/form-data")
    assert r.status_code == 201
    j = r.get_json()
    assert j["success"] is True
    assert "receipt_id" in j


def test_receipt_upload_bad_extension(client_logged_in):
    data = {"receipt": (io.BytesIO(b"hello"), "test.exe")}
    r = client_logged_in.post("/accounting/receipts/upload",
                               data=data, content_type="multipart/form-data")
    assert r.status_code == 400


def test_receipt_approve(client_logged_in, taxops_db_path, tmp_path):
    img = tmp_path / "r.jpg"
    img.write_bytes(b"\xff\xd8\xff" + b"\x00" * 20)
    rid = _seed_receipt(taxops_db_path, str(img), status="review")
    r = client_logged_in.post(
        f"/api/accounting/receipts/{rid}/approve",
        json={"category": "Office Supplies", "account": "6100", "notes": "test"},
        content_type="application/json",
    )
    assert r.status_code == 200
    assert r.get_json()["status"] == "approved"

    conn = sqlite3.connect(taxops_db_path)
    row = conn.execute("SELECT status, approved_category FROM receipt_queue WHERE id=?", (rid,)).fetchone()
    conn.close()
    assert row[0] == "approved"
    assert row[1] == "Office Supplies"


def test_receipt_approve_requires_category(client_logged_in, taxops_db_path, tmp_path):
    img = tmp_path / "r.jpg"
    img.write_bytes(b"\xff\xd8\xff" + b"\x00" * 20)
    rid = _seed_receipt(taxops_db_path, str(img), status="review")
    r = client_logged_in.post(
        f"/api/accounting/receipts/{rid}/approve",
        json={"category": "", "account": ""},
        content_type="application/json",
    )
    assert r.status_code == 400


def test_receipt_reject(client_logged_in, taxops_db_path, tmp_path):
    img = tmp_path / "r.jpg"
    img.write_bytes(b"\xff\xd8\xff" + b"\x00" * 20)
    rid = _seed_receipt(taxops_db_path, str(img), status="review")
    r = client_logged_in.post(
        f"/api/accounting/receipts/{rid}/reject",
        json={"reason": "blurry image"},
        content_type="application/json",
    )
    assert r.status_code == 200
    assert r.get_json()["status"] == "rejected"

    conn = sqlite3.connect(taxops_db_path)
    row = conn.execute("SELECT status, review_notes FROM receipt_queue WHERE id=?", (rid,)).fetchone()
    conn.close()
    assert row[0] == "rejected"
    assert row[1] == "blurry image"


def test_receipt_export_no_approved(client_logged_in):
    r = client_logged_in.post("/api/accounting/receipts/export",
                               json={}, content_type="application/json")
    assert r.status_code == 400


def test_receipt_export_approved(client_logged_in, taxops_db_path, tmp_path, monkeypatch):
    img = tmp_path / "r.jpg"
    img.write_bytes(b"\xff\xd8\xff" + b"\x00" * 20)
    rid = _seed_receipt(taxops_db_path, str(img), status="approved")
    conn = sqlite3.connect(taxops_db_path)
    conn.execute("UPDATE receipt_queue SET approved_category='Office Supplies' WHERE id=?", (rid,))
    conn.commit()
    conn.close()

    import config as _cfg
    monkeypatch.setattr(_cfg, "QB_EXPORT_MODE", "csv")
    r = client_logged_in.post("/api/accounting/receipts/export",
                               json={}, content_type="application/json")
    assert r.status_code == 200
    assert len(r.data) > 0

    conn = sqlite3.connect(taxops_db_path)
    row = conn.execute("SELECT status FROM receipt_queue WHERE id=?", (rid,)).fetchone()
    conn.close()
    assert row[0] == "exported"


def test_accounting_settings_save(client_logged_in, taxops_db_path):
    r = client_logged_in.post(
        "/api/accounting/settings",
        json={"coa_csv_path": "/tmp/coa.csv", "qb_export_mode": "iif"},
        content_type="application/json",
    )
    assert r.status_code == 200
    assert r.get_json()["success"] is True

    conn = sqlite3.connect(taxops_db_path)
    row = conn.execute("SELECT value FROM app_settings WHERE key='qb_export_mode'").fetchone()
    conn.close()
    assert row[0] == "iif"


def test_coa_rebuild_no_csv(client_logged_in, monkeypatch):
    import config as _cfg
    monkeypatch.setattr(_cfg, "COA_CSV_PATH", "")
    r = client_logged_in.post("/api/accounting/coa/rebuild")
    assert r.status_code == 400


# ─────────────────────────────────────────────────────────────────────────────
# ACCOUNTING-10: Intake hook
# ─────────────────────────────────────────────────────────────────────────────

def test_receipt_doc_type_allowed():
    """'receipt' must be in _ALLOWED_RETURN_DOC_TYPES."""
    from routes.documents import _ALLOWED_RETURN_DOC_TYPES
    assert "receipt" in _ALLOWED_RETURN_DOC_TYPES


def test_enqueue_receipt_creates_row(taxops_db_path, tmp_path, monkeypatch):
    """_enqueue_receipt inserts a receipt_queue row for a given doc_id."""
    import db as _db
    monkeypatch.setattr(_db, "DB_PATH", taxops_db_path)

    img = tmp_path / "receipt.jpg"
    img.write_bytes(b"\xff\xd8\xff" + b"\x00" * 20)

    conn = sqlite3.connect(taxops_db_path)
    conn.execute("INSERT INTO clients (last_name, first_name, created_at) VALUES ('Test','User','2026-01-01')")
    conn.execute("INSERT INTO returns (client_id, log_number, created_at) VALUES (1,'T001','2026-01-01')")
    conn.execute(
        """INSERT INTO return_documents
           (return_id, filename, original_filename, doc_type, file_path, uploaded_at, is_deleted)
           VALUES (1, 'receipt.jpg', 'My Receipt.jpg', 'receipt', ?, '2026-01-01', 0)""",
        (str(img),),
    )
    conn.commit()
    doc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()

    from routes.documents import _enqueue_receipt
    with patch("accounting_worker._notify_receipt_worker"):
        _enqueue_receipt(doc_id, str(img))

    conn = sqlite3.connect(taxops_db_path)
    row = conn.execute("SELECT * FROM receipt_queue WHERE return_document_id=?", (doc_id,)).fetchone()
    conn.close()
    assert row is not None
    assert row[2] == str(img)  # image_path column (3rd column after id, return_document_id)
    assert row[4] == "pending"  # status column


# ─────────────────────────────────────────────────────────────────────────────
# ACCOUNTING-7: Worker retry + dead-letter
# ─────────────────────────────────────────────────────────────────────────────

def test_worker_retries_on_ocr_failure(taxops_db_path, app, monkeypatch):
    """Worker resets status to 'pending' and increments attempts on transient failure."""
    import db as _db
    import config as _cfg
    monkeypatch.setattr(_cfg, "ACCOUNTING_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(_db, "DB_PATH", taxops_db_path)

    img_path = str(Path(taxops_db_path).parent / "r.jpg")
    Path(img_path).write_bytes(b"\xff\xd8\xff" + b"\x00" * 20)

    conn = sqlite3.connect(taxops_db_path)
    conn.execute(
        "INSERT INTO receipt_queue (image_path, status, attempts, created_at) VALUES (?, 'pending', 0, '2026-01-01T00:00:00Z')",
        (img_path,),
    )
    conn.commit()
    conn.close()

    with patch("services.receipt_ocr.extract_receipt_data", return_value={"_error": "ocr_fail", "vendor": None, "line_items": []}):
        with patch("accounting_worker._emit_audit"):
            from accounting_worker import _process_one
            processed = _process_one(app)

    assert processed is True
    conn = sqlite3.connect(taxops_db_path)
    row = conn.execute("SELECT status, attempts FROM receipt_queue ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    assert row[0] == "pending"
    assert row[1] == 1


def test_worker_dead_letters_after_max_attempts(taxops_db_path, app, monkeypatch):
    """Worker sets status='failed' after max_attempts exhausted."""
    import db as _db
    import config as _cfg
    monkeypatch.setattr(_cfg, "ACCOUNTING_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(_db, "DB_PATH", taxops_db_path)

    img_path = str(Path(taxops_db_path).parent / "r2.jpg")
    Path(img_path).write_bytes(b"\xff\xd8\xff" + b"\x00" * 20)

    conn = sqlite3.connect(taxops_db_path)
    # Start at attempts=2 so next failure (attempt=3) triggers dead-letter
    conn.execute(
        "INSERT INTO receipt_queue (image_path, status, attempts, created_at) VALUES (?, 'pending', 2, '2026-01-01T00:00:00Z')",
        (img_path,),
    )
    conn.commit()
    conn.close()

    with patch("services.receipt_ocr.extract_receipt_data", return_value={"_error": "ocr_fail", "vendor": None, "line_items": []}):
        with patch("accounting_worker._emit_audit"):
            from accounting_worker import _process_one
            processed = _process_one(app)

    assert processed is True
    conn = sqlite3.connect(taxops_db_path)
    row = conn.execute("SELECT status FROM receipt_queue ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    assert row[0] == "failed"


# ─────────────────────────────────────────────────────────────────────────────
# ACCOUNTING-11: Audit entries
# ─────────────────────────────────────────────────────────────────────────────

def test_approve_emits_audit(client_logged_in, taxops_db_path, tmp_path):
    """Approving a receipt enqueues an audit write."""
    img = tmp_path / "r.jpg"
    img.write_bytes(b"\xff\xd8\xff" + b"\x00" * 20)
    rid = _seed_receipt(taxops_db_path, str(img), status="review")

    with patch("routes.accounting._enqueue_audit") as mock_audit:
        client_logged_in.post(
            f"/api/accounting/receipts/{rid}/approve",
            json={"category": "Travel", "account": "6300"},
            content_type="application/json",
        )
        mock_audit.assert_called_once()
        kwargs = mock_audit.call_args[1]
        assert kwargs["action"] == "receipt_approved"
        assert kwargs["entity_id"] == str(rid)


def test_reject_emits_audit(client_logged_in, taxops_db_path, tmp_path):
    """Rejecting a receipt enqueues an audit write."""
    img = tmp_path / "r.jpg"
    img.write_bytes(b"\xff\xd8\xff" + b"\x00" * 20)
    rid = _seed_receipt(taxops_db_path, str(img), status="review")

    with patch("routes.accounting._enqueue_audit") as mock_audit:
        client_logged_in.post(
            f"/api/accounting/receipts/{rid}/reject",
            json={"reason": "duplicate"},
            content_type="application/json",
        )
        mock_audit.assert_called_once()
        assert mock_audit.call_args[1]["action"] == "receipt_rejected"
