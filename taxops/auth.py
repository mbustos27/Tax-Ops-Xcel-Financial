"""DEBT-1: shared authentication helpers used by app.py and route blueprints.

Keeping login_required here (rather than in app.py) avoids circular imports
when blueprints in routes/ need the decorator.
"""
from __future__ import annotations

import functools

from flask import abort, g, jsonify, redirect, request, session, url_for


def get_effective_role() -> str:
    """Return the role currently governing UI access and route guards.

    When an admin is using the preview-as-role feature, ``preview_role`` is set
    and takes precedence so the entire app behaves as if that role is active.
    All role_required / view_only_for decorators and template guards call this
    function so preview works automatically everywhere.
    """
    return session.get("preview_role") or session.get("role", "receptionist")


def login_required(f):
    """Decorator: redirect unauthenticated users to /login; return 401 JSON for API paths.

    ONBOARD-2: if session['must_change_password'] is True the user is redirected to
    /change-password on every request except /change-password and /logout themselves.
    API callers receive a 403 with a clear error rather than a silent redirect.
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            p = request.path or ""
            if p.startswith("/api/") or p.startswith("/ai/"):
                return jsonify({"error": "login_required"}), 401
            return redirect(url_for("login", next=request.path))
        # ONBOARD-2: force password change before any other action
        if session.get("must_change_password"):
            p = request.path or ""
            if p not in ("/change-password", "/logout"):
                if p.startswith("/api/") or p.startswith("/ai/"):
                    return jsonify({"error": "password_change_required"}), 403
                return redirect(url_for("change_password"))
        return f(*args, **kwargs)
    return wrapper


def role_required(min_role: str):
    """Decorator factory: enforce a minimum role, implicitly wrapping login_required.

    Aborts with 403 when the authenticated user's role rank is below min_role.
    API paths (/api/*, /ai/*) receive a 403 JSON response; HTML routes get abort(403).
    """
    def decorator(f):
        @functools.wraps(f)
        @login_required
        def wrapper(*args, **kwargs):
            from config import ROLE_HIERARCHY
            user_role = get_effective_role()
            if ROLE_HIERARCHY.get(user_role, 0) < ROLE_HIERARCHY.get(min_role, 0):
                p = request.path or ""
                if p.startswith("/api/") or p.startswith("/ai/"):
                    return jsonify({"error": "forbidden", "required_role": min_role}), 403
                abort(403)
            return f(*args, **kwargs)
        return wrapper
    return decorator


def view_only_for(max_role: str):
    """Decorator factory: set ``g.view_only = True`` when user's role rank <= max_role's rank.

    Does not block access — the route renders normally. Templates check ``view_only``
    to suppress edit controls for lower-privileged users.
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            from config import ROLE_HIERARCHY
            user_role = get_effective_role()
            g.view_only = ROLE_HIERARCHY.get(user_role, 0) <= ROLE_HIERARCHY.get(max_role, 0)
            return f(*args, **kwargs)
        return wrapper
    return decorator


def has_permission(permission: str) -> bool:
    """Return True if the current effective role has the named permission.

    Consults ``ROLE_PERMISSIONS`` in config; unknown permissions always return False.
    Safe to call from templates via the ``has_permission`` Jinja global.
    """
    from config import ROLE_PERMISSIONS
    return get_effective_role() in ROLE_PERMISSIONS.get(permission, frozenset())


def permission_required(permission: str):
    """Decorator factory: allow access iff current role has the named permission.

    Uses ``ROLE_PERMISSIONS`` from config as the single source of truth.
    API paths (/api/*, /ai/*) receive 403 JSON; HTML routes get abort(403).
    Implicitly wraps login_required.
    """
    def decorator(f):
        @functools.wraps(f)
        @login_required
        def wrapper(*args, **kwargs):
            if not has_permission(permission):
                p = request.path or ""
                if p.startswith("/api/") or p.startswith("/ai/"):
                    return jsonify({"error": "forbidden", "required_permission": permission}), 403
                abort(403)
            return f(*args, **kwargs)
        return wrapper
    return decorator
