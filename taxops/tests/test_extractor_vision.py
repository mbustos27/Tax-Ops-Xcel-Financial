"""Vision-extraction gating (taxops-invariants.mdc: EXTRACTOR_VISION_ENABLED).

Split out of the deleted test_unconfirmed_matches.py (Phase 0.4) — this
coverage is independent of the email-classifier architecture change and is
preserved as-is: it exercises extractor.py's image-handling path, not the
email pipeline.
"""
from __future__ import annotations

from unittest.mock import patch

from db import get_connection, init_db


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


def test_image_files_skip_vision_extraction(tmp_path, monkeypatch):
    """PNG in extraction_queue → status='skipped', no Ollama call, when
    EXTRACTOR_VISION_ENABLED=false (the default)."""
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
        return None  # return None -> no fields, but the call was made

    import form_store
    with patch.object(extractor, "_extract_json_retry_on_timeout", side_effect=_fake_ollama), \
         patch.object(form_store, "_image_to_b64", return_value="base64data"):
        fields, method = extractor._extract_fields(str(png_path), "scan.png")

    assert vision_called, "Ollama vision should be called when EXTRACTOR_VISION_ENABLED=true"
    assert method != "image_skipped"
