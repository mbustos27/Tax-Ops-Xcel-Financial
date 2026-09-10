"""Admin announcements — broadcast in-app notifications to staff.

Uses the generic ``notifications`` table (nav bell in base.html). TaxOps has
no outbound staff email today; this is in-app only.

Routes:
  GET  /admin/announcements              — compose + history page
  GET  /api/admin/announcements          — recent announcement log (JSON)
  POST /api/admin/announcements/send     — deliver to all or selected staff
"""
from __future__ import annotations

import json
import logging
from flask import Blueprint, jsonify, redirect, render_template, request, session, url_for

from auth import login_required
from db import get_connection
from routes.notifications import notify_user
from utils import now

logger = logging.getLogger(__name__)

announcements_bp = Blueprint("announcements", __name__)

_MAX_TITLE = 200
_MAX_BODY = 4000
_HISTORY_LIMIT = 50


def _admin_required(f):
    import functools

    @functools.wraps(f)
    @login_required
    def wrapper(*args, **kwargs):
        if session.get("role") != "admin":
            p = request.path or ""
            if p.startswith("/api/"):
                return jsonify({"error": "admin_required"}), 403
            return redirect(url_for("dashboard"))
        return f(*args, **kwargs)

    return wrapper


def _sanitize_link(raw: str | None) -> str | None:
    link = (raw or "").strip()
    if not link:
        return None
    if not link.startswith("/") or link.startswith("//"):
        return None
    return link


def _active_staff(conn) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, username, display_name, role
        FROM auth_users
        WHERE is_active = 1
        ORDER BY COALESCE(display_name, username), username
        """
    ).fetchall()
    return [dict(r) for r in rows]


def _current_admin(conn) -> tuple[int | None, str]:
    username = session.get("username") or ""
    row = conn.execute(
        "SELECT id, display_name FROM auth_users WHERE username=? AND is_active=1",
        (username,),
    ).fetchone()
    if not row:
        return None, username or "Admin"
    disp = (row["display_name"] or "").strip() or username
    return int(row["id"]), disp


@announcements_bp.route("/admin/announcements")
@_admin_required
def announcements_admin():
    from app import base_ctx

    ctx = base_ctx()
    ctx["active_page"] = "announcements"
    conn = get_connection()
    try:
        ctx["staff"] = _active_staff(conn)
        rows = conn.execute(
            """
            SELECT id, title, body, link_url, recipient_scope, recipient_user_ids,
                   sent_count, created_by_display, created_at
            FROM announcements
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (_HISTORY_LIMIT,),
        ).fetchall()
        history = []
        for r in rows:
            item = dict(r)
            raw_ids = item.pop("recipient_user_ids", None)
            if raw_ids:
                try:
                    item["recipient_user_ids"] = json.loads(raw_ids)
                except json.JSONDecodeError:
                    item["recipient_user_ids"] = []
            else:
                item["recipient_user_ids"] = []
            history.append(item)
        ctx["announcements"] = history
    finally:
        conn.close()
    return render_template("announcements_admin.html", **ctx)


@announcements_bp.get("/api/admin/announcements")
@_admin_required
def api_list_announcements():
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, title, body, link_url, recipient_scope, recipient_user_ids,
                   sent_count, created_by_display, created_at
            FROM announcements
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (_HISTORY_LIMIT,),
        ).fetchall()
        out = []
        for r in rows:
            item = dict(r)
            raw_ids = item.get("recipient_user_ids")
            item["recipient_user_ids"] = json.loads(raw_ids) if raw_ids else []
            out.append(item)
        return jsonify({"announcements": out})
    finally:
        conn.close()


@announcements_bp.post("/api/admin/announcements/send")
@_admin_required
def api_send_announcement():
    data = request.get_json(silent=True) or {}
    title = (data.get("title") or "").strip()
    body = (data.get("body") or "").strip() or None
    link_url = _sanitize_link(data.get("link_url"))
    scope = (data.get("recipient_scope") or "all").strip().lower()
    raw_ids = data.get("user_ids")

    if not title:
        return jsonify({"error": "title is required"}), 400
    if len(title) > _MAX_TITLE:
        return jsonify({"error": f"title must be at most {_MAX_TITLE} characters"}), 400
    if body and len(body) > _MAX_BODY:
        return jsonify({"error": f"body must be at most {_MAX_BODY} characters"}), 400
    if scope not in ("all", "selected"):
        return jsonify({"error": "recipient_scope must be 'all' or 'selected'"}), 400

    conn = get_connection()
    try:
        active = _active_staff(conn)
        active_ids = {int(u["id"]) for u in active}

        if scope == "all":
            recipient_ids = sorted(active_ids)
        else:
            if not isinstance(raw_ids, list) or not raw_ids:
                return jsonify({"error": "user_ids required when recipient_scope is 'selected'"}), 400
            try:
                recipient_ids = sorted({int(x) for x in raw_ids})
            except (TypeError, ValueError):
                return jsonify({"error": "user_ids must be a list of integers"}), 400
            bad = [uid for uid in recipient_ids if uid not in active_ids]
            if bad:
                return jsonify({"error": "one or more selected users are inactive or unknown"}), 400

        if not recipient_ids:
            return jsonify({"error": "no active recipients to notify"}), 400

        admin_id, admin_display = _current_admin(conn)
        ts = now()
        ids_json = json.dumps(recipient_ids) if scope == "selected" else None

        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            """
            INSERT INTO announcements (
              title, body, link_url, recipient_scope, recipient_user_ids,
              sent_count, created_by_user_id, created_by_display, created_at
            ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
            """,
            (title, body, link_url, scope, ids_json, admin_id, admin_display, ts),
        )
        announcement_id = int(cur.lastrowid)

        notif_body = body
        if admin_display:
            prefix = f"From {admin_display}"
            notif_body = f"{prefix}\n\n{body}" if body else prefix

        for uid in recipient_ids:
            notify_user(
                conn,
                user_id=uid,
                title=title,
                body=notif_body,
                link_url=link_url,
                entity_type="announcement",
                entity_id=announcement_id,
                ts=ts,
            )

        conn.execute(
            "UPDATE announcements SET sent_count=? WHERE id=?",
            (len(recipient_ids), announcement_id),
        )
        conn.commit()
        logger.info(
            "Admin announcement id=%s sent to %s user(s) by=%s",
            announcement_id,
            len(recipient_ids),
            session.get("username"),
        )
        return jsonify({
            "success": True,
            "announcement_id": announcement_id,
            "sent_count": len(recipient_ids),
        })
    except Exception as exc:
        conn.rollback()
        logger.exception("api_send_announcement failed: %s", exc)
        return jsonify({"error": "Could not send announcement"}), 500
    finally:
        conn.close()
