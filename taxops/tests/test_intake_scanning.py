"""Tests for intake scanning helpers: FTS, log allocator, scan-agent auth, upload source."""
from __future__ import annotations

import io
import os

import pytest


def test_schema_v23_has_fts_ocr_cache_and_flags(taxops_db_path):
    from db import CURRENT_SCHEMA_VERSION, get_connection, get_schema_version

    assert CURRENT_SCHEMA_VERSION == 23
    conn = get_connection(taxops_db_path)
    assert get_schema_version(conn) == 23
    cols = {r[1] for r in conn.execute("PRAGMA table_info(return_documents)")}
    assert "ocr_text_indexed" in cols
    ret_cols = {r[1] for r in conn.execute("PRAGMA table_info(returns)")}
    assert "scan_deferred" in ret_cols
    fts = conn.execute(
        "SELECT name FROM sqlite_master WHERE name='return_documents_fts'"
    ).fetchone()
    assert fts is not None
    cache = conn.execute(
        "SELECT name FROM sqlite_master WHERE name='ocr_extraction_cache'"
    ).fetchone()
    assert cache is not None
    conn.close()


def test_document_fts_index_and_search(taxops_db_path):
    from document_fts import flatten_extracted_fields, index_document_text, search_documents_fts
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) "
        "VALUES ('DOE','JANE',datetime('now'),datetime('now'))"
    )
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, log_number, created_at) "
        "VALUES (?,?, '100', datetime('now'))",
        (cid, 2026),
    )
    rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """
        INSERT INTO return_documents
          (return_id, filename, doc_type, source, file_path, is_deleted, ocr_text_indexed)
        VALUES (?, 'w2.pdf', 'W-2', 'scan_agent', 'x', 0, 0)
        """,
        (rid,),
    )
    doc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()

    text = flatten_extracted_fields({"employer_name": "ACME CORP", "box1": "55000"})
    assert "ACME" in text
    assert index_document_text(conn, doc_id, {"employer_name": "ACME CORP", "box1": "55000"})
    conn.commit()
    ids = search_documents_fts(conn, "ACME", return_id=rid)
    assert doc_id in ids
    row = conn.execute(
        "SELECT ocr_text_indexed FROM return_documents WHERE id=?", (doc_id,)
    ).fetchone()
    assert int(row["ocr_text_indexed"]) == 1
    conn.close()


def test_ensure_return_log_number_reuses(taxops_db_path):
    from db import get_connection
    from log_numbers import ensure_return_log_number

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) "
        "VALUES ('DOE','JANE',datetime('now'),datetime('now'))"
    )
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, log_number, created_at) "
        "VALUES (?,?, '42', datetime('now'))",
        (cid, 2026),
    )
    rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    log_n, newly = ensure_return_log_number(conn, rid)
    assert log_n == "42"
    assert newly is False
    conn.close()


def test_ensure_return_log_number_allocates(taxops_db_path):
    from db import get_connection
    from log_numbers import ensure_return_log_number

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) "
        "VALUES ('DOE','JANE',datetime('now'),datetime('now'))"
    )
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, log_number, created_at) "
        "VALUES (?,?, NULL, datetime('now'))",
        (cid, 2026),
    )
    rid1 = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, log_number, created_at) "
        "VALUES (?,?, '7', datetime('now'))",
        (cid, 2026),
    )
    conn.commit()
    log_n, newly = ensure_return_log_number(conn, rid1)
    assert newly is True
    assert log_n == "8"
    conn.close()


def test_scan_agent_requires_token():
    from scan_agent.server import handle_scan_request

    status, body, _ = handle_scan_request(token_header="", expected_token="")
    assert status == 401
    assert "SCAN_AGENT_TOKEN" in body["error"]

    status, body, _ = handle_scan_request(token_header="wrong", expected_token="secret")
    assert status == 401


def test_resolve_upload_provenance_scan_agent():
    from flask import Flask
    from routes import documents as docs_mod

    app = Flask(__name__)
    with app.test_request_context(
        "/", method="POST", data={"source": "scan_agent", "doc_type": "W-2"}
    ):
        assert docs_mod._resolve_upload_provenance() == ("scan_agent", "scan_agent")
    with app.test_request_context("/", method="POST", data={}):
        assert docs_mod._resolve_upload_provenance() == ("walk_in", "manual")


def test_document_delete_requires_preparer():
    import inspect
    from routes import documents as docs_mod

    src = inspect.getsource(docs_mod.return_document_delete)
    # Decorators aren't in getsource of the function body — check module source around delete
    mod_src = inspect.getsource(docs_mod)
    assert '@role_required("preparer")' in mod_src
    assert "def return_document_delete" in mod_src


def test_can_scan_intake_docs_permission():
    from config import ROLE_PERMISSIONS

    assert "can_scan_intake_docs" in ROLE_PERMISSIONS
    assert "receptionist" in ROLE_PERMISSIONS["can_scan_intake_docs"]


def test_pages_to_pdf_roundtrip():
    from PIL import Image
    from scan_agent.server import pages_to_pdf

    buf = io.BytesIO()
    Image.new("RGB", (32, 32), color=(200, 200, 200)).save(buf, format="JPEG")
    pdf = pages_to_pdf([buf.getvalue()])
    assert pdf[:4] == b"%PDF"


def test_query_returns_scan_deferred_filter(app, taxops_db_path):
    from db import get_connection
    import app as app_mod

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) "
        "VALUES ('DOE','JANE',datetime('now'),datetime('now'))"
    )
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    year = 2026
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, intake_date, log_number, "
        "scan_deferred, created_at) VALUES (?,?, '2026-01-15', '10', 1, datetime('now'))",
        (cid, year - 1),
    )
    deferred_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, intake_date, log_number, "
        "scan_deferred, created_at) VALUES (?,?, '2026-01-16', '11', 0, datetime('now'))",
        (cid, year - 1),
    )
    conn.commit()
    conn.close()

    with app.test_request_context("/"):
        rows = app_mod.query_returns({"year": year, "scan_deferred": "1"})
    ids = {r["id"] for r in rows}
    assert deferred_id in ids
    assert len(ids) == 1


def test_bulk_upload_scan_agent_requires_permission():
    """source=scan_agent must check can_scan_intake_docs (source inspection)."""
    import inspect
    from routes import documents as docs_mod

    src = inspect.getsource(docs_mod.return_documents_bulk_upload)
    assert "can_scan_intake_docs" in src
    src2 = inspect.getsource(docs_mod.return_documents_upload)
    assert "can_scan_intake_docs" in src2


def _seed_return(taxops_db_path: str) -> int:
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) "
        "VALUES ('DOE','JANE',datetime('now'),datetime('now'))"
    )
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, log_number, created_at) "
        "VALUES (?,?, '77', datetime('now'))",
        (cid, 2026),
    )
    rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()
    return rid


def test_scan_agent_status_proxies_health(client_logged_in, monkeypatch, taxops_db_path):
    from routes import documents as docs_mod

    monkeypatch.setattr(
        docs_mod,
        "_call_scan_agent_health",
        lambda timeout=8: {
            "ok": True,
            "scanner_found": True,
            "scanner_names": "EPSON ES-500W II",
            "tips": [],
            "agent_url": "http://192.168.1.9:8766",
        },
    )
    resp = client_logged_in.get("/api/scan-agent/status")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert "EPSON" in data["scanner_names"]


def test_scan_intake_saves_pdf_and_queues(client_logged_in, monkeypatch, taxops_db_path, tmp_path):
    from PIL import Image
    from routes import documents as docs_mod
    from scan_agent.server import pages_to_pdf
    from db import get_connection

    rid = _seed_return(taxops_db_path)
    buf = io.BytesIO()
    Image.new("RGB", (48, 48), color=(240, 240, 240)).save(buf, format="JPEG")
    pdf = pages_to_pdf([buf.getvalue()])

    monkeypatch.setattr(docs_mod, "_call_scan_agent", lambda handwriting=False: (pdf, 1))
    monkeypatch.setattr(
        docs_mod,
        "_after_scan_agent_upload",
        lambda conn, return_id: {
            "log_number": "77",
            "label_printed": False,
            "log_newly_allocated": False,
        },
    )
    enqueued: list[int] = []
    monkeypatch.setattr(
        docs_mod,
        "_enqueue_extraction",
        lambda doc_id, return_id: enqueued.append(doc_id),
    )
    # Avoid spawning classify threads against live Claude/Ollama in unit tests.
    monkeypatch.setattr(
        "form_store._classify_document",
        lambda doc_id, only_if_still_unknown=True: {"doc_type": "unknown", "doc_id": doc_id},
    )
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    monkeypatch.setattr(docs_mod, "get_return_documents_path", lambda _rid: str(docs_dir))

    resp = client_logged_in.post(
        f"/api/return/{rid}/scan-intake",
        json={"doc_type": "unknown", "handwriting": False},
        content_type="application/json",
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    data = resp.get_json()
    assert data["success"] is True
    assert data["page_count"] == 1
    assert data["auto_sort"] is True
    assert data["doc_id"]
    assert enqueued == [data["doc_id"]]

    conn = get_connection(taxops_db_path)
    row = conn.execute(
        "SELECT source, match_method, doc_type, file_path FROM return_documents WHERE id=?",
        (data["doc_id"],),
    ).fetchone()
    conn.close()
    assert row["source"] == "scan_agent"
    assert row["match_method"] == "scan_agent"
    assert row["doc_type"] == "unknown"
    assert os.path.isfile(row["file_path"])
    with open(row["file_path"], "rb") as fh:
        assert fh.read(4) == b"%PDF"


def test_scan_intake_surfaces_agent_tips_on_failure(client_logged_in, monkeypatch, taxops_db_path):
    from routes import documents as docs_mod

    rid = _seed_return(taxops_db_path)

    def _boom(handwriting=False):
        raise RuntimeError("No WIA scanner found")

    monkeypatch.setattr(docs_mod, "_call_scan_agent", _boom)
    monkeypatch.setattr(
        docs_mod,
        "_call_scan_agent_health",
        lambda timeout=5: {
            "ok": False,
            "tips": ["Epson 'ES-500' shows Unknown (CM_PROB_PHANTOM) — fix USB."],
            "agent_url": "http://192.168.1.9:8766",
        },
    )
    resp = client_logged_in.post(
        f"/api/return/{rid}/scan-intake",
        json={"doc_type": "unknown"},
        content_type="application/json",
    )
    assert resp.status_code == 502
    data = resp.get_json()
    assert "No WIA" in data["error"]
    assert any("PHANTOM" in t for t in data["tips"])


def test_scan_agent_wia_paths_use_com_sta():
    """Flask worker threads need CoInitialize — both WIA entrypoints must wrap _com_sta."""
    import inspect
    from scan_agent import server as sa

    assert "_wia_call" in inspect.getsource(sa.list_wia_scanners)
    assert "_wia_call" in inspect.getsource(sa._scan_pages_wia)
    assert "_com_sta" in inspect.getsource(sa)
    src = inspect.getsource(sa)
    assert "com_sta_v4" in src
    assert "CoInitializeEx" in src
    assert "_StaWiaPump" in src
    assert "sys.coinit_flags" in src
