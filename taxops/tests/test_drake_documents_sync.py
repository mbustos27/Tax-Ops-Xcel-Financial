"""DOC-6 — Drake Documents staging copy (no Drake API)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import config as cfg


@pytest.fixture
def _patch_db_and_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, taxops_db_path: str):
    """tmp drake root + document source file + one return row."""
    from db import get_connection, init_db

    monkeypatch.setattr(cfg, "DRAKE_DOCUMENTS_PATH", str(tmp_path / "drake_root"))

    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.executescript(
        """
        INSERT INTO clients (id, last_name, first_name, display_name)
        VALUES (501, 'Ace', 'Amy', 'Ace, Amy');
        INSERT INTO returns (
            id, client_id, log_number, tax_year, processor, verified, client_status,
            intake_date
        ) VALUES (9001, 501, '42', 2025, '', 0, 'PROCESSING', '2026-01-15');
        """
    )
    src_dir = tmp_path / "taxdocs" / "9001"
    src_dir.mkdir(parents=True)
    src_file = src_dir / "sample_w2.pdf"
    src_file.write_bytes(b"%PDF-1.4 test content")

    conn.execute(
        """
        INSERT INTO return_documents (
            return_id, filename, original_filename, doc_type, source,
            file_path, file_size_bytes, uploaded_at, is_deleted
        ) VALUES (9001, 'sample_w2.pdf', 'sample_w2.pdf', NULL, 'test',
                  ?, 12, '2026-01-01', 0)
        """,
        (str(src_file),),
    )
    conn.commit()
    conn.close()
    yield


def test_sync_to_drake_copies_and_manifest(tmp_path, taxops_db_path, _patch_db_and_paths):
    import drake_documents_sync as dds

    out = dds.sync_to_drake(9001)
    assert out["success"] is True
    staging = Path(out["staging_directory"])
    assert staging.is_dir()
    assert (staging / "sample_w2.pdf").is_file()
    meta = staging / "taxops_drake_sync.json"
    assert meta.is_file()
    data = json.loads(meta.read_text(encoding="utf-8"))
    assert data["taxops_return_id"] == 9001
    assert data["documents_copied"] == 1
    assert data["staging_relative"].replace("\\", "/") == "2025/ACE_Amy_c501/log42_rid9001"


def test_sync_to_drake_requires_path(monkeypatch, tmp_path, taxops_db_path, _patch_db_and_paths):
    import drake_documents_sync as dds

    monkeypatch.setattr(cfg, "DRAKE_DOCUMENTS_PATH", "")
    bad = dds.sync_to_drake(9001)
    assert bad["success"] is False
    assert bad["error"] == "drake_documents_path_unset"


def test_sync_to_drake_missing_return(taxops_db_path, monkeypatch, tmp_path):
    import drake_documents_sync as dds

    root = tmp_path / "dr"
    root.mkdir()
    monkeypatch.setattr(cfg, "DRAKE_DOCUMENTS_PATH", str(root))
    from db import get_connection, init_db

    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.close()
    miss = dds.sync_to_drake(999999)
    assert miss["success"] is False
    assert miss["error"] == "return_not_found"


def test_api_sync_to_drake_route(client_logged_in, monkeypatch, tmp_path, taxops_db_path, _patch_db_and_paths):
    rv = client_logged_in.post("/api/return/9001/sync-to-drake")
    assert rv.status_code == 200
    body = rv.get_json()
    assert body["success"] is True


def test_api_sync_to_drake_unconfigured_returns_400(client_logged_in, monkeypatch, taxops_db_path, _patch_db_and_paths):
    monkeypatch.setattr(cfg, "DRAKE_DOCUMENTS_PATH", "")
    rv = client_logged_in.post("/api/return/9001/sync-to-drake")
    assert rv.status_code == 400
