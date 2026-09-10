"""WO-7: generic per-user in-app notifications (bell/badge in base.html).

The `notifications` table (db.py) is intentionally generic — not tied to
work orders specifically — so any future "notify this one user" feature can
reuse it. The first (and currently only) writer is
routes/work_orders.py's _notify_work_order_assigned.

TaxOps has no outbound email today (mail_watcher.py is inbound IMAP only),
so this is purely an in-app mechanism: a nav bell showing unread count, a
dropdown listing recent notifications, and mark-read actions.

Routes:
  GET  /api/my-notifications                — list current user's notifications
  POST /api/my-notifications/<id>/read       — mark one as read
  POST /api/my-notifications/mark-all-read   — mark all of the user's as read
"""
from __future__ import annotations

from flask import Blueprint, jsonify, session

from auth import login_required
from db import get_connection

notifications_bp = Blueprint("notifications", __name__)

_LIST_LIMIT = 30


def notify_user(
    conn,
    *,
    user_id: int,
    title: str,
    body: str | None,
    link_url: str | None,
    entity_type: str | None,
    entity_id: int | None,
    ts: str,
) -> int:
    """Insert one in-app notification row for a single auth_users.id."""
    conn.execute(
        "INSERT INTO notifications (user_id, title, body, link_url, entity_type, entity_id, is_read, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 0, ?)",
        (user_id, title, body, link_url, entity_type, entity_id, ts),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _current_user_id(conn) -> int | None:
    username = session.get("username")
    if not username:
        return None
    row = conn.execute("SELECT id FROM auth_users WHERE username=?", (username,)).fetchone()
    return row["id"] if row else None


@notifications_bp.get("/api/my-notifications")
@login_required
def api_list_my_notifications():
    conn = get_connection()
    try:
        uid = _current_user_id(conn)
        if uid is None:
            return jsonify({"notifications": [], "unread_count": 0})
        rows = conn.execute(
            "SELECT id, title, body, link_url, entity_type, entity_id, is_read, created_at "
            "FROM notifications WHERE user_id=? ORDER BY created_at DESC, id DESC LIMIT ?",
            (uid, _LIST_LIMIT),
        ).fetchall()
        unread_count = conn.execute(
            "SELECT COUNT(*) n FROM notifications WHERE user_id=? AND is_read=0", (uid,)
        ).fetchone()["n"]
        return jsonify({
            "notifications": [dict(r) for r in rows],
            "unread_count": unread_count,
        })
    finally:
        conn.close()


@notifications_bp.post("/api/my-notifications/<int:notification_id>/read")
@login_required
def api_mark_notification_read(notification_id: int):
    conn = get_connection()
    try:
        uid = _current_user_id(conn)
        row = conn.execute(
            "SELECT id FROM notifications WHERE id=? AND user_id=?", (notification_id, uid)
        ).fetchone()
        if not row:
            # Ownership check doubles as existence check — never let a user
            # discover another user's notification ids via a 404-vs-200 diff.
            return jsonify({"error": "Notification not found"}), 404
        conn.execute("UPDATE notifications SET is_read=1 WHERE id=?", (notification_id,))
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


@notifications_bp.post("/api/my-notifications/mark-all-read")
@login_required
def api_mark_all_notifications_read():
    conn = get_connection()
    try:
        uid = _current_user_id(conn)
        if uid is not None:
            conn.execute("UPDATE notifications SET is_read=1 WHERE user_id=? AND is_read=0", (uid,))
            conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()
