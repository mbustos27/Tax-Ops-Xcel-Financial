"""Peer staff messages — compose, reply threads, floating panel API."""
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


def test_staff_messages_requires_login(app):
    with app.test_client() as client:
        assert client.get("/staff-messages").status_code == 302


def test_staff_messages_page_renders(client_logged_in):
    rv = client_logged_in.get("/staff-messages")
    assert rv.status_code == 200, rv.data[:400]
    html = rv.data.decode("utf-8")
    assert "Staff Messages" in html
    assert "Send message" in html
    assert "Related client" in html
    assert "Related return" not in html
    assert "msg-panel" in html


def test_send_staff_message_with_client(client_logged_in, taxops_db_path):
    from db import get_connection, set_active_intake_tax_year

    recipient_id = _seed_user(taxops_db_path, "__msg_recipient__", "preparer")
    conn = get_connection(taxops_db_path)
    set_active_intake_tax_year(conn, 2025)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name) VALUES (903, 'GARCIA', 'MARIA')"
    )
    conn.execute(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status)
        VALUES (9103, 903, '7003', 2025, 'HOLD')
        """
    )
    conn.commit()
    conn.close()

    rv = client_logged_in.post(
        "/api/staff-messages/send",
        json={
            "subject": "Client callback",
            "body": "Maria Garcia called — please call 555-0100.",
            "client_id": 903,
            "user_ids": [recipient_id],
        },
    )
    assert rv.status_code == 200, rv.get_json()
    data = rv.get_json()
    assert data["success"] is True
    assert data["sent_count"] == 1
    assert data["thread_ids"][0] == data["message_ids"][0]

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            """
            SELECT subject, body, client_id, link_url, is_read, notification_id,
                   recipient_user_id, thread_id
            FROM staff_messages WHERE id=?
            """,
            (data["message_ids"][0],),
        ).fetchone()
        assert row["subject"] == "Client callback"
        assert "555-0100" in row["body"]
        assert row["client_id"] == 903
        assert row["link_url"] == "/return/9103"
        assert row["is_read"] == 0
        assert row["recipient_user_id"] == recipient_id
        assert row["notification_id"] is not None
        assert row["thread_id"] == data["message_ids"][0]

        note = conn.execute(
            "SELECT title, body, link_url FROM notifications WHERE id=?",
            (row["notification_id"],),
        ).fetchone()
        assert "Message from" in note["title"]
        assert "Client callback" in note["body"]
        assert note["link_url"] == "/return/9103"
    finally:
        conn.close()


def test_reply_creates_thread_continuation(client_logged_in, taxops_db_path, app):
    recipient_id = _seed_user(taxops_db_path, "__msg_reply_to__", "preparer")
    send = client_logged_in.post(
        "/api/staff-messages/send",
        json={
            "subject": "Quiet consult",
            "body": "Can you glance at this W-2?",
            "user_ids": [recipient_id],
        },
    )
    assert send.status_code == 200, send.get_json()
    thread_id = send.get_json()["thread_ids"][0]

    with app.test_client() as other:
        other.post(
            "/login",
            data={"username": "__msg_reply_to__", "password": "pw12345"},
            follow_redirects=True,
        )
        reply = other.post(
            "/api/staff-messages/reply",
            json={"thread_id": thread_id, "body": "Yes — looks fine."},
        )
    assert reply.status_code == 200, reply.get_json()
    assert reply.get_json()["thread_id"] == thread_id

    thread = client_logged_in.get(f"/api/staff-messages/thread/{thread_id}")
    assert thread.status_code == 200
    payload = thread.get_json()
    assert len(payload["messages"]) == 2
    assert payload["messages"][1]["body"] == "Yes — looks fine."
    assert payload["messages"][1]["subject"].lower().startswith("re:")


def test_messages_panel_payload(client_logged_in, taxops_db_path):
    recipient_id = _seed_user(taxops_db_path, "__msg_panel_peer__", "receptionist")
    client_logged_in.post(
        "/api/staff-messages/send",
        json={
            "subject": "Panel test",
            "body": "Hello from panel.",
            "user_ids": [recipient_id],
        },
    )
    rv = client_logged_in.get("/api/staff-messages/panel")
    assert rv.status_code == 200
    data = rv.get_json()
    assert "notifications" in data
    assert "conversations" in data
    assert "staff" in data
    assert any(c["subject"] == "Panel test" for c in data["conversations"])
    assert any(u["id"] == recipient_id for u in data["staff"])


def test_cannot_message_self(client_logged_in, taxops_db_path):
    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            "SELECT id FROM auth_users WHERE username='__test_user__'"
        ).fetchone()
        self_id = int(row["id"])
    finally:
        conn.close()
    rv = client_logged_in.post(
        "/api/staff-messages/send",
        json={
            "subject": "Note",
            "body": "Hi",
            "user_ids": [self_id],
        },
    )
    assert rv.status_code == 400


def test_receptionist_can_send(app, taxops_db_path):
    _seed_user(taxops_db_path, "__msg_rx__", "receptionist")
    target_id = _seed_user(taxops_db_path, "__msg_prep__", "preparer")
    with app.test_client() as client:
        client.post("/login", data={"username": "__msg_rx__", "password": "pw12345"}, follow_redirects=True)
        rv = client.post(
            "/api/staff-messages/send",
            json={
                "subject": "Front desk note",
                "body": "Client waiting.",
                "user_ids": [target_id],
            },
        )
    assert rv.status_code == 200, rv.get_json()
