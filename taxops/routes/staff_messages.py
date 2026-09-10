"""Staff-to-staff messages — leave a note or mini-chat with a colleague (in-app).

Use cases:
  - Client called for someone who is out; front desk leaves them a message.
  - Quiet / emergency consult without speaking aloud (floating Messages panel).

Routes:
  GET  /staff-messages                      — compose + inbox + sent (full page)
  POST /api/staff-messages/send             — start a thread (1+ colleagues)
  POST /api/staff-messages/reply            — continue a thread (mini-chat)
  GET  /api/staff-messages/conversations    — thread summaries for panel
  GET  /api/staff-messages/thread/<id>      — messages in one thread
  GET  /api/staff-messages/panel            — notifications + conversations (+ staff)
  POST /api/staff-messages/<id>/read        — mark one inbound message read
"""
from __future__ import annotations

import logging

from flask import Blueprint, jsonify, render_template, request, session

from auth import login_required
from db import get_active_intake_tax_year, get_connection
from routes.notifications import notify_user
from utils import now

logger = logging.getLogger(__name__)

staff_messages_bp = Blueprint("staff_messages", __name__)

_MAX_SUBJECT = 200
_MAX_BODY = 4000
_HISTORY_LIMIT = 40
_THREAD_LIMIT = 80
_CONV_LIMIT = 40
_NOTIF_LIMIT = 30


def _client_display(row) -> str | None:
    if not row.get("client_id"):
        return None
    disp = (row.get("client_display_name") or "").strip()
    if disp:
        return disp
    ln = (row.get("client_last_name") or "").strip()
    fn = (row.get("client_first_name") or "").strip()
    if ln and fn:
        return f"{ln}, {fn}"
    return ln or fn or f"Client #{row['client_id']}"


def _return_link_for_client(conn, client_id: int) -> str | None:
    """Best return link for a staff callback — active season first, else latest."""
    active_year = get_active_intake_tax_year(conn)
    row = conn.execute(
        """
        SELECT id FROM returns
        WHERE client_id = ?
          AND tax_year = ?
          AND COALESCE(client_status, '') != 'CANCELLED'
        ORDER BY id DESC
        LIMIT 1
        """,
        (client_id, active_year),
    ).fetchone()
    if row:
        return f"/return/{int(row['id'])}"
    row = conn.execute(
        """
        SELECT id FROM returns
        WHERE client_id = ?
          AND COALESCE(client_status, '') != 'CANCELLED'
        ORDER BY tax_year DESC, id DESC
        LIMIT 1
        """,
        (client_id,),
    ).fetchone()
    return f"/return/{int(row['id'])}" if row else None


def _validate_client_id(conn, client_id: int) -> bool:
    return conn.execute("SELECT id FROM clients WHERE id=?", (client_id,)).fetchone() is not None


def _enrich_message_row(row: dict, *, privacy_mode: bool) -> dict:
    out = dict(row)
    label = _client_display(out)
    if label and privacy_mode:
        label = f"XXXXX #{out['client_id']}"
    out["client_name"] = label
    return out


def _current_user(conn) -> tuple[int | None, str]:
    username = session.get("username") or ""
    row = conn.execute(
        "SELECT id, display_name FROM auth_users WHERE username=? AND is_active=1",
        (username,),
    ).fetchone()
    if not row:
        return None, username or "Staff"
    disp = (row["display_name"] or "").strip() or username
    return int(row["id"]), disp


def _active_staff(conn, *, exclude_user_id: int | None = None) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, username, display_name, role
        FROM auth_users
        WHERE is_active = 1
        ORDER BY COALESCE(display_name, username), username
        """
    ).fetchall()
    out = [dict(r) for r in rows]
    if exclude_user_id is not None:
        out = [u for u in out if int(u["id"]) != exclude_user_id]
    return out


def _user_in_thread(conn, thread_id: int, user_id: int) -> bool:
    row = conn.execute(
        """
        SELECT 1 AS ok FROM staff_messages
        WHERE COALESCE(thread_id, id) = ?
          AND (sender_user_id = ? OR recipient_user_id = ?)
        LIMIT 1
        """,
        (thread_id, user_id, user_id),
    ).fetchone()
    return row is not None


def _thread_other_party(conn, thread_id: int, user_id: int) -> dict | None:
    """Other staffer in a 1:1 thread (first distinct counterpart found)."""
    row = conn.execute(
        """
        SELECT
          CASE WHEN m.sender_user_id = ? THEN m.recipient_user_id ELSE m.sender_user_id END AS other_id
        FROM staff_messages m
        WHERE COALESCE(m.thread_id, m.id) = ?
          AND (m.sender_user_id = ? OR m.recipient_user_id = ?)
        ORDER BY m.id ASC
        LIMIT 1
        """,
        (user_id, thread_id, user_id, user_id),
    ).fetchone()
    if not row:
        return None
    other = conn.execute(
        "SELECT id, username, display_name, role FROM auth_users WHERE id=?",
        (int(row["other_id"]),),
    ).fetchone()
    return dict(other) if other else None


def _list_conversations(conn, user_id: int) -> list[dict]:
    roots = conn.execute(
        """
        SELECT DISTINCT COALESCE(thread_id, id) AS tid
        FROM staff_messages
        WHERE sender_user_id = ? OR recipient_user_id = ?
        ORDER BY tid DESC
        LIMIT ?
        """,
        (user_id, user_id, _CONV_LIMIT * 3),
    ).fetchall()
    out: list[dict] = []
    for r in roots:
        tid = int(r["tid"])
        latest = conn.execute(
            """
            SELECT m.id, m.subject, m.body, m.created_at, m.sender_user_id, m.recipient_user_id,
                   m.is_read, m.client_id, m.link_url,
                   s.display_name AS sender_display, s.username AS sender_username,
                   rec.display_name AS recipient_display, rec.username AS recipient_username
            FROM staff_messages m
            JOIN auth_users s ON s.id = m.sender_user_id
            JOIN auth_users rec ON rec.id = m.recipient_user_id
            WHERE COALESCE(m.thread_id, m.id) = ?
              AND (m.sender_user_id = ? OR m.recipient_user_id = ?)
            ORDER BY m.created_at DESC, m.id DESC
            LIMIT 1
            """,
            (tid, user_id, user_id),
        ).fetchone()
        if not latest:
            continue
        unread = conn.execute(
            """
            SELECT COUNT(*) n FROM staff_messages
            WHERE COALESCE(thread_id, id) = ?
              AND recipient_user_id = ?
              AND is_read = 0
            """,
            (tid, user_id),
        ).fetchone()["n"]
        other = _thread_other_party(conn, tid, user_id)
        other_name = None
        if other:
            other_name = (other.get("display_name") or "").strip() or other.get("username")
        root = conn.execute(
            "SELECT subject, client_id, link_url FROM staff_messages WHERE id=?",
            (tid,),
        ).fetchone()
        out.append({
            "thread_id": tid,
            "subject": (root["subject"] if root else None) or latest["subject"],
            "preview": (latest["body"] or "")[:160],
            "created_at": latest["created_at"],
            "unread_count": int(unread),
            "other_user_id": int(other["id"]) if other else None,
            "other_name": other_name,
            "client_id": root["client_id"] if root else latest["client_id"],
            "link_url": (root["link_url"] if root else None) or latest["link_url"],
        })
    out.sort(key=lambda c: c["created_at"] or "", reverse=True)
    return out[:_CONV_LIMIT]


def _thread_messages(conn, thread_id: int, user_id: int) -> list[dict]:
    rows = conn.execute(
        """
        SELECT m.id, m.thread_id, m.parent_id, m.subject, m.body, m.created_at,
               m.sender_user_id, m.recipient_user_id, m.is_read, m.client_id, m.link_url,
               s.display_name AS sender_display, s.username AS sender_username
        FROM staff_messages m
        JOIN auth_users s ON s.id = m.sender_user_id
        WHERE COALESCE(m.thread_id, m.id) = ?
          AND (m.sender_user_id = ? OR m.recipient_user_id = ?)
        ORDER BY m.created_at ASC, m.id ASC
        LIMIT ?
        """,
        (thread_id, user_id, user_id, _THREAD_LIMIT),
    ).fetchall()
    return [dict(r) for r in rows]


def _mark_thread_read(conn, thread_id: int, user_id: int) -> None:
    rows = conn.execute(
        """
        SELECT id, notification_id FROM staff_messages
        WHERE COALESCE(thread_id, id) = ?
          AND recipient_user_id = ?
          AND is_read = 0
        """,
        (thread_id, user_id),
    ).fetchall()
    for row in rows:
        conn.execute("UPDATE staff_messages SET is_read=1 WHERE id=?", (row["id"],))
        if row["notification_id"]:
            conn.execute(
                "UPDATE notifications SET is_read=1 WHERE id=? AND user_id=?",
                (row["notification_id"], user_id),
            )


def _insert_message(
    conn,
    *,
    sender_id: int,
    recipient_id: int,
    subject: str,
    body: str,
    client_id: int | None,
    link_url: str | None,
    thread_id: int | None,
    parent_id: int | None,
    sender_display: str,
    ts: str,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO staff_messages (
          sender_user_id, recipient_user_id, subject, body, client_id, link_url,
          is_read, created_at, thread_id, parent_id
        ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
        """,
        (sender_id, recipient_id, subject, body, client_id, link_url, ts, thread_id, parent_id),
    )
    msg_id = int(cur.lastrowid)
    if thread_id is None:
        conn.execute("UPDATE staff_messages SET thread_id=? WHERE id=?", (msg_id, msg_id))
        thread_id = msg_id
    notif_title = f"Message from {sender_display}"
    notif_body = f"{subject}\n\n{body}"
    notif_id = notify_user(
        conn,
        user_id=recipient_id,
        title=notif_title,
        body=notif_body,
        link_url=link_url or f"/staff-messages?thread={thread_id}",
        entity_type="staff_message",
        entity_id=msg_id,
        ts=ts,
    )
    conn.execute(
        "UPDATE staff_messages SET notification_id=? WHERE id=?",
        (notif_id, msg_id),
    )
    return msg_id


@staff_messages_bp.route("/staff-messages")
@login_required
def staff_messages_page():
    from app import base_ctx, privacy_mode_enabled

    ctx = base_ctx()
    ctx["active_page"] = "staff_messages"
    privacy = privacy_mode_enabled()
    conn = get_connection()
    try:
        uid, _ = _current_user(conn)
        ctx["staff"] = _active_staff(conn, exclude_user_id=uid)
        ctx["inbox"] = []
        ctx["sent"] = []
        if uid is not None:
            inbox_rows = conn.execute(
                """
                SELECT m.id, m.subject, m.body, m.link_url, m.created_at, m.is_read,
                       m.client_id, m.thread_id, m.parent_id,
                       c.last_name AS client_last_name,
                       c.first_name AS client_first_name,
                       c.display_name AS client_display_name,
                       s.display_name AS sender_display, s.username AS sender_username
                FROM staff_messages m
                JOIN auth_users s ON s.id = m.sender_user_id
                LEFT JOIN clients c ON c.id = m.client_id
                WHERE m.recipient_user_id = ?
                ORDER BY m.created_at DESC, m.id DESC
                LIMIT ?
                """,
                (uid, _HISTORY_LIMIT),
            ).fetchall()
            ctx["inbox"] = [_enrich_message_row(dict(r), privacy_mode=privacy) for r in inbox_rows]

            sent_rows = conn.execute(
                """
                SELECT m.id, m.subject, m.body, m.link_url, m.created_at,
                       m.client_id, m.thread_id, m.parent_id,
                       c.last_name AS client_last_name,
                       c.first_name AS client_first_name,
                       c.display_name AS client_display_name,
                       r.display_name AS recipient_display, r.username AS recipient_username
                FROM staff_messages m
                JOIN auth_users r ON r.id = m.recipient_user_id
                LEFT JOIN clients c ON c.id = m.client_id
                WHERE m.sender_user_id = ?
                ORDER BY m.created_at DESC, m.id DESC
                LIMIT ?
                """,
                (uid, _HISTORY_LIMIT),
            ).fetchall()
            ctx["sent"] = [_enrich_message_row(dict(r), privacy_mode=privacy) for r in sent_rows]
    finally:
        conn.close()
    return render_template("staff_messages.html", **ctx)


@staff_messages_bp.post("/api/staff-messages/send")
@login_required
def api_send_staff_message():
    data = request.get_json(silent=True) or {}
    subject = (data.get("subject") or "").strip() or "Quick message"
    body = (data.get("body") or "").strip()
    raw_client_id = data.get("client_id")
    raw_ids = data.get("user_ids")

    if not body:
        return jsonify({"error": "message is required"}), 400
    if len(subject) > _MAX_SUBJECT:
        return jsonify({"error": f"subject must be at most {_MAX_SUBJECT} characters"}), 400
    if len(body) > _MAX_BODY:
        return jsonify({"error": f"message must be at most {_MAX_BODY} characters"}), 400
    if not isinstance(raw_ids, list) or not raw_ids:
        return jsonify({"error": "Select at least one colleague"}), 400

    conn = get_connection()
    try:
        sender_id, sender_display = _current_user(conn)
        if sender_id is None:
            return jsonify({"error": "Account not found"}), 401

        active_ids = {int(u["id"]) for u in _active_staff(conn)}
        try:
            recipient_ids = sorted({int(x) for x in raw_ids})
        except (TypeError, ValueError):
            return jsonify({"error": "user_ids must be a list of integers"}), 400

        if sender_id in recipient_ids:
            return jsonify({"error": "You cannot send a message to yourself"}), 400
        bad = [uid for uid in recipient_ids if uid not in active_ids]
        if bad:
            return jsonify({"error": "one or more selected colleagues are inactive or unknown"}), 400

        client_id: int | None = None
        if raw_client_id not in (None, ""):
            try:
                client_id = int(raw_client_id)
            except (TypeError, ValueError):
                return jsonify({"error": "client_id must be an integer"}), 400
            if not _validate_client_id(conn, client_id):
                return jsonify({"error": "client not found"}), 400

        link_url = _return_link_for_client(conn, client_id) if client_id else None

        ts = now()
        conn.execute("BEGIN IMMEDIATE")
        message_ids: list[int] = []
        thread_ids: list[int] = []
        for rid in recipient_ids:
            msg_id = _insert_message(
                conn,
                sender_id=sender_id,
                recipient_id=rid,
                subject=subject,
                body=body,
                client_id=client_id,
                link_url=link_url,
                thread_id=None,
                parent_id=None,
                sender_display=sender_display,
                ts=ts,
            )
            message_ids.append(msg_id)
            thread_ids.append(msg_id)

        conn.commit()
        logger.info(
            "Staff message from user_id=%s to %s recipient(s)",
            sender_id,
            len(recipient_ids),
        )
        return jsonify({
            "success": True,
            "sent_count": len(recipient_ids),
            "message_ids": message_ids,
            "thread_ids": thread_ids,
        })
    except Exception as exc:
        conn.rollback()
        logger.exception("api_send_staff_message failed: %s", exc)
        return jsonify({"error": "Could not send message"}), 500
    finally:
        conn.close()


@staff_messages_bp.post("/api/staff-messages/reply")
@login_required
def api_reply_staff_message():
    data = request.get_json(silent=True) or {}
    body = (data.get("body") or "").strip()
    if not body:
        return jsonify({"error": "message is required"}), 400
    if len(body) > _MAX_BODY:
        return jsonify({"error": f"message must be at most {_MAX_BODY} characters"}), 400

    raw_thread = data.get("thread_id")
    raw_parent = data.get("parent_id")
    try:
        thread_id = int(raw_thread) if raw_thread not in (None, "") else None
        parent_id = int(raw_parent) if raw_parent not in (None, "") else None
    except (TypeError, ValueError):
        return jsonify({"error": "thread_id / parent_id must be integers"}), 400

    if thread_id is None and parent_id is None:
        return jsonify({"error": "thread_id or parent_id is required"}), 400

    conn = get_connection()
    try:
        sender_id, sender_display = _current_user(conn)
        if sender_id is None:
            return jsonify({"error": "Account not found"}), 401

        if thread_id is None:
            parent = conn.execute(
                "SELECT id, COALESCE(thread_id, id) AS tid FROM staff_messages WHERE id=?",
                (parent_id,),
            ).fetchone()
            if not parent:
                return jsonify({"error": "Message not found"}), 404
            thread_id = int(parent["tid"])
            parent_id = int(parent["id"])

        if not _user_in_thread(conn, thread_id, sender_id):
            return jsonify({"error": "Thread not found"}), 404

        other = _thread_other_party(conn, thread_id, sender_id)
        if not other:
            return jsonify({"error": "No recipient for this thread"}), 400
        recipient_id = int(other["id"])

        root = conn.execute(
            "SELECT subject, client_id, link_url FROM staff_messages WHERE id=?",
            (thread_id,),
        ).fetchone()
        subject = (root["subject"] if root else None) or "Quick message"
        if not subject.lower().startswith("re:"):
            subject = f"Re: {subject}"
        client_id = root["client_id"] if root else None
        link_url = root["link_url"] if root else None

        ts = now()
        conn.execute("BEGIN IMMEDIATE")
        msg_id = _insert_message(
            conn,
            sender_id=sender_id,
            recipient_id=recipient_id,
            subject=subject[:_MAX_SUBJECT],
            body=body,
            client_id=client_id,
            link_url=link_url,
            thread_id=thread_id,
            parent_id=parent_id,
            sender_display=sender_display,
            ts=ts,
        )
        conn.commit()
        return jsonify({
            "success": True,
            "message_id": msg_id,
            "thread_id": thread_id,
        })
    except Exception as exc:
        conn.rollback()
        logger.exception("api_reply_staff_message failed: %s", exc)
        return jsonify({"error": "Could not send reply"}), 500
    finally:
        conn.close()


@staff_messages_bp.get("/api/staff-messages/conversations")
@login_required
def api_staff_conversations():
    conn = get_connection()
    try:
        uid, _ = _current_user(conn)
        if uid is None:
            return jsonify({"conversations": []})
        return jsonify({"conversations": _list_conversations(conn, uid)})
    finally:
        conn.close()


@staff_messages_bp.get("/api/staff-messages/thread/<int:thread_id>")
@login_required
def api_staff_thread(thread_id: int):
    conn = get_connection()
    try:
        uid, _ = _current_user(conn)
        if uid is None:
            return jsonify({"error": "Account not found"}), 401
        if not _user_in_thread(conn, thread_id, uid):
            return jsonify({"error": "Thread not found"}), 404
        _mark_thread_read(conn, thread_id, uid)
        conn.commit()
        messages = _thread_messages(conn, thread_id, uid)
        other = _thread_other_party(conn, thread_id, uid)
        other_name = None
        if other:
            other_name = (other.get("display_name") or "").strip() or other.get("username")
        subject = messages[0]["subject"] if messages else ""
        root = conn.execute(
            "SELECT subject FROM staff_messages WHERE id=?", (thread_id,)
        ).fetchone()
        if root:
            subject = root["subject"] or subject
        return jsonify({
            "thread_id": thread_id,
            "subject": subject,
            "other_user_id": int(other["id"]) if other else None,
            "other_name": other_name,
            "messages": messages,
            "current_user_id": uid,
        })
    finally:
        conn.close()


@staff_messages_bp.get("/api/staff-messages/panel")
@login_required
def api_messages_panel():
    """Combined payload for the floating Messages panel (notifications + chats)."""
    conn = get_connection()
    try:
        uid, _ = _current_user(conn)
        if uid is None:
            return jsonify({
                "notifications": [],
                "unread_notifications": 0,
                "conversations": [],
                "unread_messages": 0,
                "staff": [],
            })
        notif_rows = conn.execute(
            """
            SELECT id, title, body, link_url, entity_type, entity_id, is_read, created_at
            FROM notifications WHERE user_id=?
            ORDER BY created_at DESC, id DESC LIMIT ?
            """,
            (uid, _NOTIF_LIMIT),
        ).fetchall()
        unread_notifications = conn.execute(
            "SELECT COUNT(*) n FROM notifications WHERE user_id=? AND is_read=0",
            (uid,),
        ).fetchone()["n"]
        unread_messages = conn.execute(
            "SELECT COUNT(*) n FROM staff_messages WHERE recipient_user_id=? AND is_read=0",
            (uid,),
        ).fetchone()["n"]
        return jsonify({
            "notifications": [dict(r) for r in notif_rows],
            "unread_notifications": int(unread_notifications),
            "conversations": _list_conversations(conn, uid),
            "unread_messages": int(unread_messages),
            "staff": _active_staff(conn, exclude_user_id=uid),
            "current_user_id": uid,
        })
    finally:
        conn.close()


@staff_messages_bp.post("/api/staff-messages/<int:message_id>/read")
@login_required
def api_mark_staff_message_read(message_id: int):
    conn = get_connection()
    try:
        uid, _ = _current_user(conn)
        if uid is None:
            return jsonify({"error": "Account not found"}), 401
        row = conn.execute(
            "SELECT id, notification_id FROM staff_messages WHERE id=? AND recipient_user_id=?",
            (message_id, uid),
        ).fetchone()
        if not row:
            return jsonify({"error": "Message not found"}), 404
        conn.execute("UPDATE staff_messages SET is_read=1 WHERE id=?", (message_id,))
        if row["notification_id"]:
            conn.execute(
                "UPDATE notifications SET is_read=1 WHERE id=? AND user_id=?",
                (row["notification_id"], uid),
            )
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()
