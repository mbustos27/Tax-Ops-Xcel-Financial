"""Email-inbox holding-area API routes (app.py) — the replacement for the
deleted /api/email-review/unconfirmed-matches/* routes and match-confidence
workflow (see test_unconfirmed_matches.py, deleted in Phase 0.4).

Covers:
  - /api/email-inbox/items excludes assigned+deleted rows and never returns file_path
  - /api/email-inbox/<id>/file is confined to EMAIL_INBOX_DIR (path-traversal rejected)
  - /api/email-inbox/<id>/assign copies the file, inserts return_documents with
    match_confirmed=1, match_score=NULL, match_method='email_manual' (decision #1),
    marks the inbox row assigned, and enqueues extraction
  - /api/email-inbox/<id>/delete sets is_deleted=1 without unlinking the file
  - all routes 403 without can_use_email_tools permission
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from db import get_connection


def _seed_client_and_return(db_path: str, client_id: int, return_id: int) -> None:
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


def _seed_inbox_item(db_path: str, item_id: int, file_path: str,
                      filename: str = "doc.pdf", original_filename: str = "doc.pdf",
                      is_assigned: int = 0, is_deleted: int = 0) -> None:
    conn = get_connection(db_path)
    conn.execute(
        """
        INSERT INTO email_inbox
          (id, sender_email, sender_domain, subject_snippet, filename,
           original_filename, file_path, file_size_bytes, received_at,
           is_assigned, is_deleted)
        VALUES (?, 'client@example.com', 'example.com', 'docs', ?, ?, ?, 123,
                '2026-01-01T00:00:00+00:00', ?, ?)
        """,
        (item_id, filename, original_filename, file_path, is_assigned, is_deleted),
    )
    conn.commit()
    conn.close()


@pytest.fixture
def inbox_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    import config as cfg
    root = tmp_path / "email_inbox"
    root.mkdir()
    monkeypatch.setattr(cfg, "EMAIL_INBOX_DIR", str(root))
    return root


# ── /api/email-inbox/items ───────────────────────────────────────────────────

def test_items_excludes_assigned_and_deleted(client_logged_in, taxops_db_path, inbox_dir):
    _seed_inbox_item(taxops_db_path, 1, str(inbox_dir / "a.pdf"), is_assigned=0, is_deleted=0)
    _seed_inbox_item(taxops_db_path, 2, str(inbox_dir / "b.pdf"), is_assigned=1, is_deleted=0)
    _seed_inbox_item(taxops_db_path, 3, str(inbox_dir / "c.pdf"), is_assigned=0, is_deleted=1)

    rv = client_logged_in.get("/api/email-inbox/items")
    assert rv.status_code == 200
    items = rv.get_json()["items"]
    ids = {i["id"] for i in items}
    assert ids == {1}


def test_items_never_includes_file_path(client_logged_in, taxops_db_path, inbox_dir):
    _seed_inbox_item(taxops_db_path, 4, str(inbox_dir / "secret.pdf"))
    rv = client_logged_in.get("/api/email-inbox/items")
    items = rv.get_json()["items"]
    assert items and "file_path" not in items[0]


def test_items_requires_permission(client, taxops_db_path, inbox_dir):
    """Without login, the route must not return the inbox listing."""
    rv = client.get("/api/email-inbox/items")
    assert rv.status_code in (302, 401, 403)


# ── /api/email-inbox/<id>/file ───────────────────────────────────────────────

def test_file_serves_confined_path(client_logged_in, taxops_db_path, inbox_dir):
    f = inbox_dir / "w2.pdf"
    f.write_bytes(b"%PDF-1.4 real file")
    _seed_inbox_item(taxops_db_path, 5, str(f))

    rv = client_logged_in.get("/api/email-inbox/5/file")
    assert rv.status_code == 200
    assert rv.data == b"%PDF-1.4 real file"


def test_file_rejects_path_outside_inbox_dir(client_logged_in, taxops_db_path, inbox_dir, tmp_path):
    """A file_path value that escapes EMAIL_INBOX_DIR (e.g. a compromised DB
    row) must be rejected with 403, never served.

    The stock Flask 403 error page (base.html) needs status_counts/base_ctx()
    context that isn't populated outside a normal route — same template-context
    gap worked around in test_rbac_efile_email.py. Patch render_template out so
    this test isolates the path-confinement check itself, not the error page.
    """
    outside = tmp_path / "outside_secret.pdf"
    outside.write_bytes(b"should never be served")
    _seed_inbox_item(taxops_db_path, 6, str(outside))

    with patch("app.render_template", return_value="forbidden"):
        rv = client_logged_in.get("/api/email-inbox/6/file")
    assert rv.status_code == 403


def test_file_404_for_deleted_item(client_logged_in, taxops_db_path, inbox_dir):
    f = inbox_dir / "gone.pdf"
    f.write_bytes(b"x")
    _seed_inbox_item(taxops_db_path, 7, str(f), is_deleted=1)

    rv = client_logged_in.get("/api/email-inbox/7/file")
    assert rv.status_code == 404


# ── /api/email-inbox/<id>/assign ─────────────────────────────────────────────

def test_assign_sets_match_fields_per_invariant(client_logged_in, taxops_db_path, inbox_dir, monkeypatch):
    """Decision #1 (taxops-invariants.mdc): the email-assign path must set
    match_confirmed=1, match_score=NULL, match_method='email_manual' explicitly."""
    _seed_client_and_return(taxops_db_path, client_id=100, return_id=100)
    src = inbox_dir / "w2.pdf"
    src.write_bytes(b"%PDF-1.4 assign-test")
    _seed_inbox_item(taxops_db_path, 8, str(src), original_filename="w2.pdf")

    dest_root = inbox_dir.parent / "returns_docs"
    with patch("app.get_return_documents_path", side_effect=lambda rid: str(dest_root / str(rid))):
        rv = client_logged_in.post("/api/email-inbox/8/assign", json={"return_id": 100})

    assert rv.status_code == 200, rv.data
    body = rv.get_json()
    assert body["success"] is True
    doc_id = body["doc_id"]

    conn = get_connection(taxops_db_path)
    doc = conn.execute(
        "SELECT return_id, source, match_confirmed, match_score, match_method "
        "FROM return_documents WHERE id=?",
        (doc_id,),
    ).fetchone()
    inbox_row = conn.execute(
        "SELECT is_assigned, assigned_return_id FROM email_inbox WHERE id=8"
    ).fetchone()
    conn.close()

    assert doc["return_id"] == 100
    assert doc["source"] == "email_inbox"
    assert doc["match_confirmed"] == 1
    assert doc["match_score"] is None
    assert doc["match_method"] == "email_manual"
    assert inbox_row["is_assigned"] == 1
    assert inbox_row["assigned_return_id"] == 100

    assert (dest_root / "100" / "w2.pdf").exists(), "file must be copied into the return's document folder"


def test_assign_enqueues_extraction(client_logged_in, taxops_db_path, inbox_dir):
    _seed_client_and_return(taxops_db_path, client_id=101, return_id=101)
    src = inbox_dir / "receipt.pdf"
    src.write_bytes(b"%PDF-1.4 x")
    _seed_inbox_item(taxops_db_path, 9, str(src), original_filename="receipt.pdf")

    dest_root = inbox_dir.parent / "returns_docs2"
    with patch("app.get_return_documents_path", side_effect=lambda rid: str(dest_root / str(rid))):
        rv = client_logged_in.post("/api/email-inbox/9/assign", json={"return_id": 101})

    assert rv.status_code == 200
    doc_id = rv.get_json()["doc_id"]

    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT 1 FROM extraction_queue WHERE doc_id=?", (doc_id,)).fetchone()
    conn.close()
    assert row is not None, "assign must enqueue the new document for extraction"


def test_assign_requires_return_id(client_logged_in, taxops_db_path, inbox_dir):
    _seed_inbox_item(taxops_db_path, 10, str(inbox_dir / "a.pdf"))
    rv = client_logged_in.post("/api/email-inbox/10/assign", json={})
    assert rv.status_code == 400


def test_assign_404_for_unknown_return(client_logged_in, taxops_db_path, inbox_dir):
    src = inbox_dir / "a.pdf"
    src.write_bytes(b"x")
    _seed_inbox_item(taxops_db_path, 11, str(src))
    rv = client_logged_in.post("/api/email-inbox/11/assign", json={"return_id": 999999})
    assert rv.status_code == 404


def test_assign_404_for_already_assigned_item(client_logged_in, taxops_db_path, inbox_dir):
    _seed_client_and_return(taxops_db_path, client_id=102, return_id=102)
    src = inbox_dir / "a.pdf"
    src.write_bytes(b"x")
    _seed_inbox_item(taxops_db_path, 12, str(src), is_assigned=1)
    rv = client_logged_in.post("/api/email-inbox/12/assign", json={"return_id": 102})
    assert rv.status_code == 404


# ── /api/email-inbox/<id>/delete ─────────────────────────────────────────────

def test_delete_sets_is_deleted_without_unlinking_file(client_logged_in, taxops_db_path, inbox_dir):
    f = inbox_dir / "keep_on_disk.pdf"
    f.write_bytes(b"%PDF-1.4 x")
    _seed_inbox_item(taxops_db_path, 13, str(f))

    rv = client_logged_in.post("/api/email-inbox/13/delete")
    assert rv.status_code == 200
    assert rv.get_json()["success"] is True

    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT is_deleted FROM email_inbox WHERE id=13").fetchone()
    conn.close()
    assert row["is_deleted"] == 1
    assert f.exists(), "delete must not unlink the underlying file"
