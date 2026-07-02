"""Fix 6 — tests for image-skip extraction + unconfirmed match workflow.

Covers:
  1. Image files skip vision extraction (status='skipped', no Ollama call)
  2. EXTRACTOR_VISION_ENABLED=true restores vision path for images
  3. Email attachment save sets match_confirmed=0 + match_score
  4. Walk-in upload columns default (match_confirmed=1, match_method='manual')
  5. POST /api/email-review/unconfirmed-matches/<id>/confirm sets confirmed=1
  6. POST /api/email-review/unconfirmed-matches/<id>/reassign moves file + updates DB
  7. POST bulk-confirm only affects match_score >= 0.90 rows
"""
from __future__ import annotations

import os
import sqlite3
import threading
from email.message import EmailMessage
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from db import get_connection, init_db


# ── Shared helpers ─────────────────────────────────────────────────────────────

class _QuietThread(threading.Thread):
    def start(self) -> None:
        return None


def _seed_return(db_path: str, client_id: int = 1, return_id: int = 1) -> None:
    conn = get_connection(db_path)
    conn.execute(
        "INSERT OR IGNORE INTO clients (id, last_name, first_name, display_name) "
        "VALUES (?, 'Test', 'Client', 'Test Client')",
        (client_id,),
    )
    conn.execute(
        "INSERT OR IGNORE INTO returns (id, client_id, tax_year, client_status) "
        "VALUES (?, ?, 2025, 'PROCESSING')",
        (return_id, client_id),
    )
    conn.commit()
    conn.close()


def _make_email_with_pdf(filename: str = "doc.pdf", payload: bytes = b"%PDF-test") -> object:
    msg = EmailMessage()
    msg["From"] = "client@example.com"
    msg["Subject"] = "My W2"
    msg.add_attachment(payload, maintype="application", subtype="pdf", filename=filename)
    return msg


# ── Fix 1: image files skip vision extraction ────────────────────────────────

def test_image_files_skip_vision_extraction(tmp_path, monkeypatch):
    """PNG in extraction_queue → status='skipped', no Ollama call."""
    import config as cfg
    import db as db_mod
    import extractor

    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(cfg, "DB_PATH", db_path)
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)
    monkeypatch.setattr(cfg, "EXTRACTOR_VISION_ENABLED", False)

    conn = get_connection(db_path)
    init_db(conn)
    _seed_return(db_path)

    png_path = tmp_path / "photo.png"
    png_path.write_bytes(b"\x89PNG\r\n\x1a\n")

    conn.execute(
        "INSERT INTO return_documents (id, return_id, filename, doc_type, source, file_path, is_deleted) "
        "VALUES (1, 1, 'photo.png', 'unknown', 'email', ?, 0)",
        (str(png_path),),
    )
    conn.execute(
        "INSERT INTO extraction_queue (id, doc_id, return_id, status, attempts, created_at) "
        "VALUES (1, 1, 1, 'pending', 0, '2025-01-01T00:00:00Z')"
    )
    conn.commit()

    ollama_called = []

    with patch.object(extractor, "_extract_json_retry_on_timeout",
                      side_effect=lambda *a, **k: ollama_called.append(1) or None):
        item = conn.execute(
            "SELECT eq.*, rd.file_path, rd.filename, rd.doc_type "
            "FROM extraction_queue eq JOIN return_documents rd ON rd.id = eq.doc_id "
            "WHERE eq.id = 1"
        ).fetchone()
        extractor._process_item(conn, dict(item))

    row = conn.execute("SELECT status, extraction_method FROM extraction_queue WHERE id = 1").fetchone()
    conn.close()

    assert row["status"] == "skipped", f"Expected 'skipped', got {row['status']!r}"
    assert row["extraction_method"] == "image_skipped"
    assert not ollama_called, "Ollama must not be called for image files when vision is disabled"


def test_image_skip_respects_vision_enabled_flag(tmp_path, monkeypatch):
    """EXTRACTOR_VISION_ENABLED=true → image goes to vision path, not skipped."""
    import config as cfg
    import extractor

    monkeypatch.setattr(cfg, "EXTRACTOR_VISION_ENABLED", True)

    png_path = tmp_path / "scan.png"
    png_path.write_bytes(b"\x89PNG\r\n\x1a\n")

    vision_called = []

    def _fake_ollama(*args, **kwargs):
        vision_called.append(kwargs.get("model") or args)
        return None  # return None → no fields, but the call was made

    import form_store
    with patch.object(extractor, "_extract_json_retry_on_timeout", side_effect=_fake_ollama), \
         patch.object(form_store, "_image_to_b64", return_value="base64data"):
        fields, method = extractor._extract_fields(str(png_path), "scan.png")

    assert vision_called, "Ollama vision should be called when EXTRACTOR_VISION_ENABLED=true"
    assert method != "image_skipped"


# ── Fix 2: email match sets match_confirmed=0 ────────────────────────────────

def test_email_match_sets_match_confirmed_zero(tmp_path, monkeypatch):
    """_save_attachments with match_score → return_documents.match_confirmed=0."""
    import config as cfg
    import db as db_mod
    import mail_watcher as mw
    import utils as utils_mod

    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(cfg, "DB_PATH", db_path)
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)
    monkeypatch.setattr(mw.threading, "Thread", _QuietThread)
    monkeypatch.setattr(utils_mod, "_enqueue_extraction", lambda *a, **k: None)

    conn = get_connection(db_path)
    init_db(conn)
    _seed_return(db_path)
    conn.close()

    docs_root = tmp_path / "docs"
    monkeypatch.setattr(
        utils_mod, "get_return_documents_path",
        lambda rid: str(docs_root / str(rid)),
    )
    (docs_root / "1").mkdir(parents=True, exist_ok=True)

    msg = _make_email_with_pdf()
    fake_app = MagicMock()

    count = mw._save_attachments(
        fake_app, msg, 1,
        match_score=0.87,
        match_method="fuzzy",
    )

    assert count == 1
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT match_confirmed, match_score, match_method FROM return_documents WHERE id = 1"
    ).fetchone()
    conn.close()

    assert row["match_confirmed"] == 0, "Email-sourced doc must start unconfirmed"
    assert abs(row["match_score"] - 0.87) < 0.001
    assert row["match_method"] == "fuzzy"


def test_walk_in_upload_columns_default(tmp_path, monkeypatch):
    """Direct DB insert with match_confirmed=1, match_method='manual' (walk-in default)."""
    import config as cfg
    import db as db_mod

    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(cfg, "DB_PATH", db_path)
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)

    conn = get_connection(db_path)
    init_db(conn)
    _seed_return(db_path)

    conn.execute(
        """
        INSERT INTO return_documents (
          return_id, filename, original_filename, doc_type, source,
          file_path, file_size_bytes, file_hash, uploaded_by, uploaded_at, notes, is_deleted,
          match_confirmed, match_score, match_method
        ) VALUES (1, 'w2.pdf', 'w2.pdf', 'W-2', 'walk_in',
                  '/tmp/w2.pdf', 100, NULL, 'staff', '2025-01-01T00:00:00Z', NULL, 0,
                  1, NULL, 'manual')
        """
    )
    conn.commit()

    row = conn.execute(
        "SELECT match_confirmed, match_score, match_method FROM return_documents WHERE id = 1"
    ).fetchone()
    conn.close()

    assert row["match_confirmed"] == 1
    assert row["match_score"] is None
    assert row["match_method"] == "manual"


# ── Fix 3/5: API routes ────────────────────────────────────────────────────────

def _seed_unconfirmed_doc(db_path: str, doc_id: int, match_score: float,
                           return_id: int = 1, file_path: str = "/tmp/test.pdf") -> None:
    conn = get_connection(db_path)
    conn.execute(
        """
        INSERT OR IGNORE INTO return_documents
          (id, return_id, filename, doc_type, source, file_path,
           uploaded_by, uploaded_at, is_deleted, match_confirmed, match_score, match_method)
        VALUES (?, ?, 'test.pdf', 'unknown', 'email', ?,
                'mail_watcher', '2025-01-01T00:00:00Z', 0, 0, ?, 'fuzzy')
        """,
        (doc_id, return_id, file_path, match_score),
    )
    conn.commit()
    conn.close()


def test_confirm_match_sets_confirmed(client_logged_in, taxops_db_path):
    """POST /confirm → match_confirmed=1 for that doc_id only."""
    _seed_return(taxops_db_path, client_id=50, return_id=50)
    _seed_unconfirmed_doc(taxops_db_path, doc_id=10, match_score=0.87, return_id=50)

    rv = client_logged_in.post("/api/email-review/unconfirmed-matches/10/confirm")
    assert rv.status_code == 200, rv.data
    assert rv.get_json()["success"] is True

    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT match_confirmed FROM return_documents WHERE id = 10").fetchone()
    conn.close()
    assert row["match_confirmed"] == 1


def test_reassign_moves_document(client_logged_in, taxops_db_path, tmp_path):
    """POST /reassign → return_id updated, match_confirmed=1, match_method='manual'."""
    import utils as utils_mod

    _seed_return(taxops_db_path, client_id=60, return_id=60)
    _seed_return(taxops_db_path, client_id=61, return_id=61)

    old_dir = tmp_path / "docs" / "60"
    old_dir.mkdir(parents=True)
    new_dir = tmp_path / "docs" / "61"
    new_dir.mkdir(parents=True)

    old_file = old_dir / "test.pdf"
    old_file.write_bytes(b"PDF content")

    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT INTO return_documents
          (id, return_id, filename, doc_type, source, file_path,
           uploaded_by, uploaded_at, is_deleted, match_confirmed, match_score, match_method)
        VALUES (20, 60, 'test.pdf', 'unknown', 'email', ?,
                'mail_watcher', '2025-01-01T00:00:00Z', 0, 0, 0.87, 'fuzzy')
        """,
        (str(old_file),),
    )
    conn.commit()
    conn.close()

    # Patch get_return_documents_path where reassign imports it from
    with patch("utils.get_return_documents_path",
               side_effect=lambda rid: str(tmp_path / "docs" / str(rid))):
        rv = client_logged_in.post(
            "/api/email-review/unconfirmed-matches/20/reassign",
            json={"return_id": 61},
        )

    assert rv.status_code == 200, rv.data
    assert rv.get_json()["success"] is True

    conn = get_connection(taxops_db_path)
    row = conn.execute(
        "SELECT return_id, match_confirmed, match_method FROM return_documents WHERE id = 20"
    ).fetchone()
    conn.close()

    assert row["return_id"] == 61
    assert row["match_confirmed"] == 1
    assert row["match_method"] == "manual"


def test_bulk_confirm_only_high_confidence(client_logged_in, taxops_db_path):
    """Bulk confirm only affects docs with match_score >= 0.90."""
    _seed_return(taxops_db_path, client_id=70, return_id=70)
    _seed_return(taxops_db_path, client_id=71, return_id=71)
    _seed_unconfirmed_doc(taxops_db_path, doc_id=30, match_score=0.95, return_id=70)
    _seed_unconfirmed_doc(taxops_db_path, doc_id=31, match_score=0.70, return_id=71)

    rv = client_logged_in.post("/api/email-review/unconfirmed-matches/bulk-confirm")
    assert rv.status_code == 200, rv.data
    data = rv.get_json()
    assert data["success"] is True
    assert data["confirmed_count"] == 1

    conn = get_connection(taxops_db_path)
    high = conn.execute("SELECT match_confirmed FROM return_documents WHERE id = 30").fetchone()
    low  = conn.execute("SELECT match_confirmed FROM return_documents WHERE id = 31").fetchone()
    conn.close()

    assert high["match_confirmed"] == 1, "0.95 score should be confirmed"
    assert low["match_confirmed"]  == 0, "0.70 score should remain unconfirmed"
