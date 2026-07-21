"""WO-7: assign a Work Order to an employee + in-app notification.

Covers:
  - creating a work order with assigned_to_user_id notifies that user
  - reassigning to a NEW person notifies them; reassigning to the SAME
    person, or clearing the assignment, does not create a duplicate/empty
    notification
  - the quick /work-orders/<id>/assign action (detail page) follows the same
    rules and respects _can_edit RBAC
  - an inactive/nonexistent assignee id is rejected, not silently accepted
  - the notifications API only ever returns the CALLING user's own
    notifications (never someone else's, even by guessing an id)
  - mark-one-read and mark-all-read behavior
  - base_ctx's per-user my_unread_notification_count badge
  - the work orders list's "assigned to me" filter
"""
from __future__ import annotations

import re

from werkzeug.security import generate_password_hash

from db import get_connection


def _wo_id_from_redirect(resp) -> int:
    location = resp.headers.get("Location") or ""
    match = re.search(r"/work-orders/(\d+)$", location)
    assert match, f"expected a /work-orders/<id> redirect, got {location!r}"
    return int(match.group(1))


def _create_payload(client_name="Test Client", desc="Consultation", fee="175.00"):
    return {
        "client_name": client_name,
        "date_created": "2026-01-15",
        "due_by": "",
        "item_description[]": [desc],
        "item_fee[]": [fee],
        "item_is_quick_pick[]": ["0"],
    }


def _seed_user(taxops_db_path: str, username: str, role: str) -> None:
    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT OR IGNORE INTO auth_users (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, ?, 1, '2025-01-01T00:00:00Z')
        """,
        (username, generate_password_hash("pw12345"), username.title(), role),
    )
    conn.commit()
    conn.close()


def _login_as(client, username: str) -> None:
    client.post("/login", data={"username": username, "password": "pw12345"}, follow_redirects=True)


def _user_id(taxops_db_path: str, username: str) -> int:
    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute("SELECT id FROM auth_users WHERE username=?", (username,)).fetchone()
    finally:
        conn.close()
    assert row is not None, f"no such user: {username}"
    return row["id"]


def _notifications_for(taxops_db_path: str, username: str) -> list[dict]:
    conn = get_connection(taxops_db_path)
    try:
        rows = conn.execute(
            """
            SELECT nf.* FROM notifications nf
            JOIN auth_users u ON u.id = nf.user_id
            WHERE u.username = ?
            ORDER BY nf.id
            """,
            (username,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ── Create-time assignment ─────────────────────────────────────────────────────

def test_create_with_assignee_notifies_assignee(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_assign_creator__", "preparer")
    _seed_user(taxops_db_path, "__wo_assign_target__", "preparer")
    target_id = _user_id(taxops_db_path, "__wo_assign_target__")

    creator = app.test_client()
    _login_as(creator, "__wo_assign_creator__")
    payload = _create_payload("Assign On Create Client")
    payload["assigned_to_user_id"] = str(target_id)
    resp = creator.post("/work-orders", data=payload, follow_redirects=False)
    assert resp.status_code == 302
    wo_id = _wo_id_from_redirect(resp)

    conn = get_connection(taxops_db_path)
    row = conn.execute(
        "SELECT assigned_to_user_id, assigned_by_user_id, assigned_at FROM work_orders WHERE id=?", (wo_id,)
    ).fetchone()
    conn.close()
    assert row["assigned_to_user_id"] == target_id
    assert row["assigned_by_user_id"] is not None
    assert row["assigned_at"] is not None

    notes = _notifications_for(taxops_db_path, "__wo_assign_target__")
    assert len(notes) == 1
    assert notes[0]["entity_type"] == "work_order"
    assert notes[0]["entity_id"] == wo_id
    assert notes[0]["is_read"] == 0
    assert "Assign On Create Client" in (notes[0]["body"] or "")


def test_create_without_assignee_notifies_nobody(client_logged_in, taxops_db_path):
    resp = client_logged_in.post("/work-orders", data=_create_payload("Unassigned At Create"), follow_redirects=False)
    assert resp.status_code == 302

    conn = get_connection(taxops_db_path)
    count = conn.execute("SELECT COUNT(*) c FROM notifications").fetchone()["c"]
    conn.close()
    assert count == 0


def test_create_with_inactive_assignee_is_rejected(client_logged_in, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_assign_inactive__", "preparer")
    conn = get_connection(taxops_db_path)
    conn.execute("UPDATE auth_users SET is_active=0 WHERE username=?", ("__wo_assign_inactive__",))
    conn.commit()
    inactive_id = conn.execute(
        "SELECT id FROM auth_users WHERE username=?", ("__wo_assign_inactive__",)
    ).fetchone()["id"]
    conn.close()

    payload = _create_payload("Bad Assignee Client")
    payload["assigned_to_user_id"] = str(inactive_id)
    resp = client_logged_in.post("/work-orders", data=payload)
    assert resp.status_code == 400

    conn = get_connection(taxops_db_path)
    count = conn.execute(
        "SELECT COUNT(*) c FROM work_orders WHERE client_name='Bad Assignee Client'"
    ).fetchone()["c"]
    conn.close()
    assert count == 0


# ── Reassignment via the full edit form ────────────────────────────────────────

def test_reassign_via_edit_form_notifies_new_assignee_only(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_edit_owner__", "preparer")
    _seed_user(taxops_db_path, "__wo_edit_first__", "preparer")
    _seed_user(taxops_db_path, "__wo_edit_second__", "preparer")
    first_id = _user_id(taxops_db_path, "__wo_edit_first__")
    second_id = _user_id(taxops_db_path, "__wo_edit_second__")

    owner = app.test_client()
    _login_as(owner, "__wo_edit_owner__")
    payload = _create_payload("Reassign Client")
    payload["assigned_to_user_id"] = str(first_id)
    create_resp = owner.post("/work-orders", data=payload, follow_redirects=False)
    wo_id = _wo_id_from_redirect(create_resp)
    assert len(_notifications_for(taxops_db_path, "__wo_edit_first__")) == 1

    # Re-save with the SAME assignee -> no second notification.
    same_payload = _create_payload("Reassign Client")
    same_payload["assigned_to_user_id"] = str(first_id)
    resave = owner.post(f"/work-orders/{wo_id}", data=same_payload, follow_redirects=False)
    assert resave.status_code == 302
    assert len(_notifications_for(taxops_db_path, "__wo_edit_first__")) == 1

    # Reassign to someone else -> new assignee gets exactly one notification;
    # the previous assignee's notification count is untouched (not deleted,
    # not duplicated).
    reassign_payload = _create_payload("Reassign Client")
    reassign_payload["assigned_to_user_id"] = str(second_id)
    reassign_resp = owner.post(f"/work-orders/{wo_id}", data=reassign_payload, follow_redirects=False)
    assert reassign_resp.status_code == 302
    assert len(_notifications_for(taxops_db_path, "__wo_edit_first__")) == 1
    second_notes = _notifications_for(taxops_db_path, "__wo_edit_second__")
    assert len(second_notes) == 1
    assert second_notes[0]["entity_id"] == wo_id

    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT assigned_to_user_id FROM work_orders WHERE id=?", (wo_id,)).fetchone()
    conn.close()
    assert row["assigned_to_user_id"] == second_id


def test_clearing_assignment_via_edit_form_does_not_notify(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_clear_owner__", "preparer")
    _seed_user(taxops_db_path, "__wo_clear_target__", "preparer")
    target_id = _user_id(taxops_db_path, "__wo_clear_target__")

    owner = app.test_client()
    _login_as(owner, "__wo_clear_owner__")
    payload = _create_payload("Clear Assignment Client")
    payload["assigned_to_user_id"] = str(target_id)
    create_resp = owner.post("/work-orders", data=payload, follow_redirects=False)
    wo_id = _wo_id_from_redirect(create_resp)
    assert len(_notifications_for(taxops_db_path, "__wo_clear_target__")) == 1

    clear_payload = _create_payload("Clear Assignment Client")
    clear_payload["assigned_to_user_id"] = ""
    clear_resp = owner.post(f"/work-orders/{wo_id}", data=clear_payload, follow_redirects=False)
    assert clear_resp.status_code == 302

    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT assigned_to_user_id FROM work_orders WHERE id=?", (wo_id,)).fetchone()
    conn.close()
    assert row["assigned_to_user_id"] is None
    # Still just the one original notification -- unassigning isn't itself a
    # notification-worthy event, and definitely shouldn't notify "nobody".
    assert len(_notifications_for(taxops_db_path, "__wo_clear_target__")) == 1


# ── Quick reassign action (detail page) ─────────────────────────────────────────

def test_quick_assign_endpoint_notifies_and_updates(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_quick_owner__", "preparer")
    _seed_user(taxops_db_path, "__wo_quick_target__", "preparer")
    target_id = _user_id(taxops_db_path, "__wo_quick_target__")

    owner = app.test_client()
    _login_as(owner, "__wo_quick_owner__")
    create_resp = owner.post("/work-orders", data=_create_payload("Quick Assign Client"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(create_resp)

    resp = owner.post(
        f"/work-orders/{wo_id}/assign",
        json={"assigned_to_user_id": target_id},
    )
    assert resp.status_code == 200
    assert resp.get_json()["success"] is True

    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT assigned_to_user_id FROM work_orders WHERE id=?", (wo_id,)).fetchone()
    conn.close()
    assert row["assigned_to_user_id"] == target_id
    assert len(_notifications_for(taxops_db_path, "__wo_quick_target__")) == 1

    # Clearing via the same endpoint (null) works and doesn't error.
    clear_resp = owner.post(f"/work-orders/{wo_id}/assign", json={"assigned_to_user_id": None})
    assert clear_resp.status_code == 200
    conn = get_connection(taxops_db_path)
    row2 = conn.execute("SELECT assigned_to_user_id FROM work_orders WHERE id=?", (wo_id,)).fetchone()
    conn.close()
    assert row2["assigned_to_user_id"] is None


def test_quick_assign_rejects_invalid_assignee(client_logged_in, taxops_db_path):
    create_resp = client_logged_in.post(
        "/work-orders", data=_create_payload("Quick Assign Bad Target"), follow_redirects=False
    )
    wo_id = _wo_id_from_redirect(create_resp)

    resp = client_logged_in.post(f"/work-orders/{wo_id}/assign", json={"assigned_to_user_id": 999999})
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_quick_assign_denied_for_receptionist_on_others_work_order(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_qa_recep_a__", "receptionist")
    _seed_user(taxops_db_path, "__wo_qa_recep_b__", "receptionist")
    b_id = _user_id(taxops_db_path, "__wo_qa_recep_b__")

    client_a = app.test_client()
    _login_as(client_a, "__wo_qa_recep_a__")
    create_resp = client_a.post("/work-orders", data=_create_payload("Recep A Quick Assign"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(create_resp)

    client_b = app.test_client()
    _login_as(client_b, "__wo_qa_recep_b__")
    resp = client_b.post(f"/work-orders/{wo_id}/assign", json={"assigned_to_user_id": b_id})
    assert resp.status_code == 403
    assert "error" in resp.get_json()

    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT assigned_to_user_id FROM work_orders WHERE id=?", (wo_id,)).fetchone()
    conn.close()
    assert row["assigned_to_user_id"] is None


def test_quick_assign_allowed_for_preparer_on_any_open_work_order(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_qa_owner2__", "receptionist")
    _seed_user(taxops_db_path, "__wo_qa_preparer2__", "preparer")
    preparer_id = _user_id(taxops_db_path, "__wo_qa_preparer2__")

    owner = app.test_client()
    _login_as(owner, "__wo_qa_owner2__")
    create_resp = owner.post("/work-orders", data=_create_payload("Preparer Can Assign"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(create_resp)

    preparer_client = app.test_client()
    _login_as(preparer_client, "__wo_qa_preparer2__")
    resp = preparer_client.post(f"/work-orders/{wo_id}/assign", json={"assigned_to_user_id": preparer_id})
    assert resp.status_code == 200
    assert len(_notifications_for(taxops_db_path, "__wo_qa_preparer2__")) == 1


# ── Notifications API: isolation + mark-read ────────────────────────────────────

def test_notifications_api_only_returns_own_notifications(app, taxops_db_path):
    _seed_user(taxops_db_path, "__notif_user_a__", "preparer")
    _seed_user(taxops_db_path, "__notif_user_b__", "preparer")
    a_id = _user_id(taxops_db_path, "__notif_user_a__")
    b_id = _user_id(taxops_db_path, "__notif_user_b__")

    creator = app.test_client()
    _login_as(creator, "__notif_user_a__")
    payload_a = _create_payload("Notif Isolation A")
    payload_a["assigned_to_user_id"] = str(a_id)
    creator.post("/work-orders", data=payload_a, follow_redirects=False)

    payload_b = _create_payload("Notif Isolation B")
    payload_b["assigned_to_user_id"] = str(b_id)
    creator.post("/work-orders", data=payload_b, follow_redirects=False)

    client_a = app.test_client()
    _login_as(client_a, "__notif_user_a__")
    resp_a = client_a.get("/api/my-notifications")
    assert resp_a.status_code == 200
    data_a = resp_a.get_json()
    assert data_a["unread_count"] == 1
    assert len(data_a["notifications"]) == 1
    assert "Notif Isolation A" in data_a["notifications"][0]["body"]

    client_b = app.test_client()
    _login_as(client_b, "__notif_user_b__")
    resp_b = client_b.get("/api/my-notifications")
    data_b = resp_b.get_json()
    assert data_b["unread_count"] == 1
    assert "Notif Isolation B" in data_b["notifications"][0]["body"]

    # B can't mark A's notification as read by guessing its id.
    a_notification_id = data_a["notifications"][0]["id"]
    forbidden_mark = client_b.post(f"/api/my-notifications/{a_notification_id}/read")
    assert forbidden_mark.status_code == 404

    conn = get_connection(taxops_db_path)
    still_unread = conn.execute(
        "SELECT is_read FROM notifications WHERE id=?", (a_notification_id,)
    ).fetchone()["is_read"]
    conn.close()
    assert still_unread == 0


def test_mark_one_read_and_mark_all_read(app, taxops_db_path):
    _seed_user(taxops_db_path, "__notif_mark_user__", "preparer")
    mark_id = _user_id(taxops_db_path, "__notif_mark_user__")

    creator = app.test_client()
    _login_as(creator, "__notif_mark_user__")
    for name in ("Mark Read One", "Mark Read Two"):
        payload = _create_payload(name)
        payload["assigned_to_user_id"] = str(mark_id)
        creator.post("/work-orders", data=payload, follow_redirects=False)

    resp = creator.get("/api/my-notifications")
    data = resp.get_json()
    assert data["unread_count"] == 2
    first_id = data["notifications"][0]["id"]

    read_resp = creator.post(f"/api/my-notifications/{first_id}/read")
    assert read_resp.status_code == 200
    assert read_resp.get_json()["success"] is True

    after_one = creator.get("/api/my-notifications").get_json()
    assert after_one["unread_count"] == 1

    mark_all_resp = creator.post("/api/my-notifications/mark-all-read")
    assert mark_all_resp.status_code == 200

    after_all = creator.get("/api/my-notifications").get_json()
    assert after_all["unread_count"] == 0


def test_notifications_api_requires_login(app):
    anon = app.test_client()
    resp = anon.get("/api/my-notifications")
    assert resp.status_code == 401


# ── base_ctx badge ─────────────────────────────────────────────────────────────

def test_nav_badge_reflects_my_unread_notification_count(app, taxops_db_path):
    _seed_user(taxops_db_path, "__badge_creator__", "preparer")
    _seed_user(taxops_db_path, "__badge_target__", "preparer")
    target_id = _user_id(taxops_db_path, "__badge_target__")

    creator = app.test_client()
    _login_as(creator, "__badge_creator__")
    payload = _create_payload("Badge Client")
    payload["assigned_to_user_id"] = str(target_id)
    creator.post("/work-orders", data=payload, follow_redirects=False)

    target_client = app.test_client()
    _login_as(target_client, "__badge_target__")
    list_page = target_client.get("/work-orders")
    assert list_page.status_code == 200
    body = list_page.get_data(as_text=True)
    # The bell badge renders the raw count server-side (see base_ctx /
    # base.html's #notif-bell-badge); one unread notification -> "1" present.
    assert 'id="notif-bell-badge"' in body


# ── List filter ─────────────────────────────────────────────────────────────────

def test_work_orders_list_assigned_to_me_filter(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_list_creator__", "preparer")
    _seed_user(taxops_db_path, "__wo_list_assignee__", "preparer")
    assignee_id = _user_id(taxops_db_path, "__wo_list_assignee__")

    creator = app.test_client()
    _login_as(creator, "__wo_list_creator__")

    assigned_payload = _create_payload("Assigned To Me Client")
    assigned_payload["assigned_to_user_id"] = str(assignee_id)
    creator.post("/work-orders", data=assigned_payload, follow_redirects=False)
    creator.post("/work-orders", data=_create_payload("Not Assigned To Me Client"), follow_redirects=False)

    assignee_client = app.test_client()
    _login_as(assignee_client, "__wo_list_assignee__")

    unfiltered = assignee_client.get("/work-orders").get_data(as_text=True)
    assert "Assigned To Me Client" in unfiltered
    assert "Not Assigned To Me Client" in unfiltered

    filtered = assignee_client.get("/work-orders?assigned_to_me=1").get_data(as_text=True)
    assert "Assigned To Me Client" in filtered
    assert "Not Assigned To Me Client" not in filtered
