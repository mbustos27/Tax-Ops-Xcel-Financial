"""ONBOARD-1: Admin user management Blueprint.

Covers:
  GET  /admin/users                            — admin panel page
  POST /api/admin/users                        — create user
  POST /api/admin/users/<id>/deactivate        — deactivate account
  POST /api/admin/users/<id>/reactivate        — reactivate account
  POST /api/admin/users/<id>/reset-password    — set new temporary password
"""
from __future__ import annotations

import logging
from flask import Blueprint, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import generate_password_hash

from auth import login_required
from db import get_connection
from utils import now

logger = logging.getLogger(__name__)

users_bp = Blueprint("users", __name__)

_MIN_PASSWORD_LEN = 8
_ALLOWED_ROLES = frozenset({"admin", "preparer", "receptionist"})


def _admin_required(f):
    """Extend login_required with admin role check."""
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


# ── Page ─────────────────────────────────────────────────────────────────────

@users_bp.route("/admin/users")
@_admin_required
def admin_users():
    from app import base_ctx  # lazy to avoid circular import
    ctx = base_ctx()
    ctx["active_page"] = "admin_users"
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, username, display_name, role, is_active,
                   created_at, last_login_at, must_change_password
            FROM auth_users
            ORDER BY created_at DESC
            """
        ).fetchall()
        ctx["users"] = [dict(r) for r in rows]
    finally:
        conn.close()
    return render_template("users_admin.html", **ctx)


# ── APIs ──────────────────────────────────────────────────────────────────────

@users_bp.post("/api/admin/users")
@_admin_required
def api_admin_create_user():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip().lower()
    display_name = (data.get("display_name") or "").strip()
    role = (data.get("role") or "staff").strip().lower()
    temp_password = data.get("temporary_password") or ""

    if not username:
        return jsonify({"error": "username is required"}), 400
    if role not in _ALLOWED_ROLES:
        return jsonify({"error": f"role must be one of {sorted(_ALLOWED_ROLES)}"}), 400
    if len(temp_password) < _MIN_PASSWORD_LEN:
        return jsonify({"error": f"temporary_password must be at least {_MIN_PASSWORD_LEN} characters"}), 400

    password_hash = generate_password_hash(temp_password)
    conn = get_connection()
    try:
        existing = conn.execute(
            "SELECT id FROM auth_users WHERE username = ?", (username,)
        ).fetchone()
        if existing:
            return jsonify({"error": "username already exists"}), 409

        cur = conn.execute(
            """
            INSERT INTO auth_users
                (username, password_hash, display_name, role, is_active,
                 created_at, must_change_password)
            VALUES (?, ?, ?, ?, 1, ?, 1)
            """,
            (username, password_hash, display_name or username, role, now()),
        )
        conn.commit()
        logger.info("Admin created user username=%s role=%s by=%s", username, role, session.get("username"))
        return jsonify({"success": True, "user_id": cur.lastrowid})
    except Exception as exc:
        conn.rollback()
        logger.error("api_admin_create_user failed: %s", exc)
        return jsonify({"error": "Could not create user"}), 500
    finally:
        conn.close()


@users_bp.post("/api/admin/users/<int:user_id>/deactivate")
@_admin_required
def api_admin_deactivate_user(user_id: int):
    current = session.get("username")
    conn = get_connection()
    try:
        row = conn.execute("SELECT username FROM auth_users WHERE id = ?", (user_id,)).fetchone()
        if not row:
            return jsonify({"error": "User not found"}), 404
        if row["username"] == current:
            return jsonify({"error": "You cannot deactivate your own account"}), 400
        conn.execute("UPDATE auth_users SET is_active = 0 WHERE id = ?", (user_id,))
        conn.commit()
        logger.info("Admin deactivated user id=%s by=%s", user_id, current)
        return jsonify({"success": True})
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@users_bp.post("/api/admin/users/<int:user_id>/reactivate")
@_admin_required
def api_admin_reactivate_user(user_id: int):
    conn = get_connection()
    try:
        row = conn.execute("SELECT id FROM auth_users WHERE id = ?", (user_id,)).fetchone()
        if not row:
            return jsonify({"error": "User not found"}), 404
        conn.execute("UPDATE auth_users SET is_active = 1 WHERE id = ?", (user_id,))
        conn.commit()
        logger.info("Admin reactivated user id=%s by=%s", user_id, session.get("username"))
        return jsonify({"success": True})
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@users_bp.post("/api/admin/users/<int:user_id>/change-role")
@_admin_required
def api_admin_change_role(user_id: int):
    current_username = session.get("username")
    data = request.get_json(silent=True) or {}
    new_role = (data.get("role") or "").strip().lower()
    if new_role not in _ALLOWED_ROLES:
        return jsonify({"error": f"role must be one of {sorted(_ALLOWED_ROLES)}"}), 400

    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT username FROM auth_users WHERE id = ?", (user_id,)
        ).fetchone()
        if not row:
            return jsonify({"error": "User not found"}), 404
        if row["username"] == current_username:
            return jsonify({"error": "You cannot change your own role"}), 400
        conn.execute("UPDATE auth_users SET role = ? WHERE id = ?", (new_role, user_id))
        conn.commit()
        logger.info(
            "Admin changed role for user id=%s to %s by=%s", user_id, new_role, current_username
        )
        return jsonify({"success": True, "role": new_role})
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@users_bp.post("/api/admin/users/<int:user_id>/reset-tour")
@_admin_required
def api_admin_reset_tour(user_id: int):
    """TOUR-4: delete tour_completed_<user_id> from app_settings so the user sees the tour again."""
    conn = get_connection()
    try:
        row = conn.execute("SELECT id FROM auth_users WHERE id = ?", (user_id,)).fetchone()
        if not row:
            return jsonify({"error": "User not found"}), 404
        key = f"tour_completed_{user_id}"
        conn.execute("DELETE FROM app_settings WHERE key = ?", (key,))
        conn.commit()
        logger.info("Admin reset tour for user id=%s by=%s", user_id, session.get("username"))
        return jsonify({"success": True})
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@users_bp.post("/api/admin/users/<int:user_id>/reset-password")
@_admin_required
def api_admin_reset_password(user_id: int):
    data = request.get_json(silent=True) or {}
    temp_password = data.get("temporary_password") or ""
    if len(temp_password) < _MIN_PASSWORD_LEN:
        return jsonify({"error": f"temporary_password must be at least {_MIN_PASSWORD_LEN} characters"}), 400

    password_hash = generate_password_hash(temp_password)
    conn = get_connection()
    try:
        row = conn.execute("SELECT id FROM auth_users WHERE id = ?", (user_id,)).fetchone()
        if not row:
            return jsonify({"error": "User not found"}), 404
        conn.execute(
            "UPDATE auth_users SET password_hash = ?, must_change_password = 1, failed_attempts = 0, locked_until = NULL WHERE id = ?",
            (password_hash, user_id),
        )
        conn.commit()
        logger.info("Admin reset password for user id=%s by=%s", user_id, session.get("username"))
        return jsonify({"success": True})
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()
