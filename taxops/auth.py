"""DEBT-1: shared authentication helpers used by app.py and route blueprints.

Keeping login_required here (rather than in app.py) avoids circular imports
when blueprints in routes/ need the decorator.
"""
from __future__ import annotations

import functools

from flask import jsonify, redirect, request, session, url_for


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
