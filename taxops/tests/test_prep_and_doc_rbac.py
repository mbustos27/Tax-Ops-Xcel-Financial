"""Reception document RBAC + Prep workspace smoke tests."""
from __future__ import annotations


def test_can_manage_return_documents_includes_receptionist():
    from config import ROLE_PERMISSIONS

    roles = ROLE_PERMISSIONS["can_manage_return_documents"]
    assert "receptionist" in roles
    assert "preparer" in roles
    assert "admin" in roles


def test_receptionist_can_delete_document(client_logged_in, taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) "
        "VALUES ('DOC','RECV',datetime('now'),datetime('now'))"
    )
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, log_number, created_at) "
        "VALUES (?,?, '501', datetime('now'))",
        (cid, 2025),
    )
    rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO return_documents "
        "(return_id, filename, original_filename, doc_type, source, file_path, "
        " uploaded_at, is_deleted) "
        "VALUES (?, 'a.pdf', 'a.pdf', 'misc', 'upload', 'C:/tmp/a.pdf', datetime('now'), 0)",
        (rid,),
    )
    doc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    with client_logged_in.session_transaction() as sess:
        sess["role"] = "receptionist"
        sess.pop("preview_role", None)

    resp = client_logged_in.post(f"/return/{rid}/documents/{doc_id}/delete")
    assert resp.status_code == 200, resp.get_data(as_text=True)[:400]
    assert resp.get_json().get("success") is True


def test_prep_mode_toggle_and_workspace(client_logged_in, taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) "
        "VALUES ('PREP','VIEW',datetime('now'),datetime('now'))"
    )
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, log_number, client_status, created_at) "
        "VALUES (?,?, '777', 'PROCESSING', datetime('now'))",
        (cid, 2025),
    )
    rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO return_documents "
        "(return_id, filename, original_filename, doc_type, source, file_path, "
        " uploaded_at, is_deleted) "
        "VALUES (?, 'prep.pdf', 'prep.pdf', 'misc', 'upload', 'C:/tmp/prep.pdf', datetime('now'), 0)",
        (rid,),
    )
    conn.commit()
    conn.close()

    with client_logged_in.session_transaction() as sess:
        sess["role"] = "preparer"
        sess.pop("preview_role", None)
        sess["prep_mode"] = False

    toggle = client_logged_in.post(
        "/api/prep-mode",
        json={"enabled": True},
        content_type="application/json",
    )
    assert toggle.status_code == 200
    assert toggle.get_json()["prep_mode"] is True

    page = client_logged_in.get(f"/prep/{rid}")
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    assert "prep-doc-list" in html
    assert "prep-viewer-modal" in html
    assert "/documents/" in html and "/view" in html
    assert "prep-viewer-modal hidden" not in html
    assert "Purple sheet" in html or "purple" in html.lower()


def test_return_open_path_respects_prep_mode(app):
    import app as app_mod

    with app.test_request_context("/"):
        from flask import session

        session["logged_in"] = True
        session["role"] = "preparer"
        session["prep_mode"] = True
        assert app_mod.return_open_path(42) == "/prep/42"
        session["prep_mode"] = False
        assert app_mod.return_open_path(42) == "/return/42"
        session["role"] = "receptionist"
        session["prep_mode"] = True
        assert app_mod.return_open_path(42) == "/return/42"
