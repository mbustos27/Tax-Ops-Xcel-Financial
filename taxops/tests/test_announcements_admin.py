"""Admin staff announcements — in-app bell notifications."""
from __future__ import annotations

from werkzeug.security import generate_password_hash

from db import get_connection


def _seed_user(taxops_db_path: str, username: str, role: str) -> int:
    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT OR IGNORE INTO auth_users (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, ?, 1, '2025-01-01T00:00:00Z')
        """,
        (username, generate_password_hash("pw12345"), username.title(), role),
    )
    conn.commit()
    uid = conn.execute("SELECT id FROM auth_users WHERE username=?", (username,)).fetchone()["id"]
    conn.close()
    return int(uid)


def test_announcements_admin_page_renders(client_logged_in):
    rv = client_logged_in.get("/admin/announcements")
    assert rv.status_code == 200, rv.data[:400]
    html = rv.data.decode("utf-8")
    assert "Staff Announcements" in html
    assert "Send announcement" in html


def test_announcements_admin_forbidden_for_preparer(app, taxops_db_path):
    _seed_user(taxops_db_path, "__ann_prep__", "preparer")
    with app.test_client() as client:
        client.post("/login", data={"username": "__ann_prep__", "password": "pw12345"}, follow_redirects=True)
        assert client.get("/admin/announcements").status_code == 302
        assert client.post(
            "/api/admin/announcements/send",
            json={"title": "Hi", "recipient_scope": "all"},
        ).status_code == 403


def test_send_announcement_to_all(client_logged_in, taxops_db_path):
    target_id = _seed_user(taxops_db_path, "__ann_target__", "receptionist")
    rv = client_logged_in.post(
        "/api/admin/announcements/send",
        json={
            "title": "Office note",
            "body": "Please read the runbook.",
            "link_url": "/admin/runbook",
            "recipient_scope": "all",
        },
    )
    assert rv.status_code == 200, rv.get_json()
    data = rv.get_json()
    assert data["success"] is True
    assert data["sent_count"] >= 1

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            "SELECT title, sent_count, recipient_scope FROM announcements WHERE id=?",
            (data["announcement_id"],),
        ).fetchone()
        assert row["title"] == "Office note"
        assert row["recipient_scope"] == "all"
        assert row["sent_count"] >= 1

        note = conn.execute(
            """
            SELECT title, body, link_url, entity_type, entity_id, is_read
            FROM notifications WHERE user_id=? ORDER BY id DESC LIMIT 1
            """,
            (target_id,),
        ).fetchone()
        assert note is not None
        assert note["title"] == "Office note"
        assert "runbook" in (note["body"] or "").lower()
        assert note["link_url"] == "/admin/runbook"
        assert note["entity_type"] == "announcement"
        assert note["entity_id"] == data["announcement_id"]
        assert note["is_read"] == 0
    finally:
        conn.close()


def test_send_announcement_to_selected(client_logged_in, taxops_db_path):
    a_id = _seed_user(taxops_db_path, "__ann_sel_a__", "preparer")
    b_id = _seed_user(taxops_db_path, "__ann_sel_b__", "receptionist")

    rv = client_logged_in.post(
        "/api/admin/announcements/send",
        json={
            "title": "Selected only",
            "recipient_scope": "selected",
            "user_ids": [a_id],
        },
    )
    assert rv.status_code == 200, rv.get_json()

    conn = get_connection(taxops_db_path)
    try:
        a_count = conn.execute(
            "SELECT COUNT(*) n FROM notifications WHERE user_id=? AND title='Selected only'",
            (a_id,),
        ).fetchone()["n"]
        b_count = conn.execute(
            "SELECT COUNT(*) n FROM notifications WHERE user_id=? AND title='Selected only'",
            (b_id,),
        ).fetchone()["n"]
        assert a_count == 1
        assert b_count == 0
    finally:
        conn.close()


def test_send_announcement_rejects_bad_link(client_logged_in):
    rv = client_logged_in.post(
        "/api/admin/announcements/send",
        json={
            "title": "Bad link",
            "link_url": "https://evil.example",
            "recipient_scope": "all",
        },
    )
    assert rv.status_code == 200
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT link_url FROM announcements ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row["link_url"] is None
    finally:
        conn.close()


def test_send_announcement_requires_title(client_logged_in):
    rv = client_logged_in.post(
        "/api/admin/announcements/send",
        json={"title": "  ", "recipient_scope": "all"},
    )
    assert rv.status_code == 400
