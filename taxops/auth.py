"""Office-LAN authentication: login, CSRF, roles, safe redirects.

This app is internal (office LAN). Do not set SESSION_COOKIE_SECURE unless the
office is actually serving HTTPS (TAXOPS_COOKIE_SECURE=true).
"""
from __future__ import annotations

import functools
import hmac
import secrets
import time
from typing import Any
from urllib.parse import urlparse

from flask import abort, current_app, g, jsonify, redirect, request, session, url_for

from config import ROLE_HIERARCHY, parse_taxops_users


# Prefix checks run longest-first so /efile-queue/export is preparer, not /export admin.
_ADMIN_PREFIXES = (
    "/upload",
    "/export",
    "/source-compare",
    "/merge-clients",
    "/import-audit",
    "/api/source-compare",
    "/api/merge-clients",
    "/api/audit/",
)
_PREPARER_PREFIXES = (
    "/efile-queue",
    "/efile-batch",
    "/email-review",
    "/ai/",
    "/api/email-",
    "/api/efile",
    "/api/rule-suggestions",
    "/api/email-sender",
)

# Simple per-IP login throttle (LAN walk-up / shared workstation).
_LOGIN_WINDOW_SEC = 300
_LOGIN_MAX_ATTEMPTS = 8
_login_attempts: dict[str, list[float]] = {}


def _secure_eq(left: str, right: str) -> bool:
    a = left.encode("utf-8")
    b = right.encode("utf-8")
    if len(a) != len(b):
        hmac.compare_digest(a, a)
        return False
    return hmac.compare_digest(a, b)


def parse_role(raw: str | None) -> str:
    role = (raw or "staff").strip().lower()
    return role if role in ROLE_HIERARCHY else "staff"


def role_rank(role: str | None) -> int:
    return ROLE_HIERARCHY.get(parse_role(role), 0)


def role_at_least(user_role: str | None, min_role: str) -> bool:
    return role_rank(user_role) >= role_rank(min_role)


def authenticate_user(username: str, password: str) -> dict[str, str] | None:
    """Return ``{username, role}`` on success. Never uses a hardcoded password."""
    name = (username or "").strip()
    if not name or password is None:
        return None
    users = parse_taxops_users()
    rec = users.get(name)
    if rec is None:
        return None
    stored = rec.get("password") or ""
    if not stored or not _secure_eq(stored, password):
        return None
    return {"username": name, "role": parse_role(rec.get("role"))}


def login_configured() -> bool:
    return bool(parse_taxops_users())


def login_allowed(ip: str) -> bool:
    now = time.time()
    hits = [t for t in _login_attempts.get(ip, []) if now - t < _LOGIN_WINDOW_SEC]
    _login_attempts[ip] = hits
    return len(hits) < _LOGIN_MAX_ATTEMPTS


def record_login_failure(ip: str) -> None:
    _login_attempts.setdefault(ip, []).append(time.time())


def clear_login_failures(ip: str) -> None:
    _login_attempts.pop(ip, None)


def safe_next_url(candidate: str | None, fallback: str | None = None) -> str:
    """Only allow same-origin relative paths (block open redirects)."""
    dest = fallback
    if not dest:
        try:
            dest = url_for("dashboard")
        except RuntimeError:
            dest = "/"
    raw = (candidate or "").strip()
    if not raw:
        return dest
    parsed = urlparse(raw)
    if parsed.scheme or parsed.netloc:
        return dest
    if not raw.startswith("/") or raw.startswith("//"):
        return dest
    if "\\" in raw or raw.startswith("/\\"):
        return dest
    return raw


def ensure_csrf_token() -> str:
    token = session.get("_csrf")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf"] = token
    return token


def _extract_csrf_token() -> str:
    header = request.headers.get("X-CSRFToken") or request.headers.get("X-CSRF-Token")
    if header:
        return header.strip()
    form_token = request.form.get("csrf_token")
    if form_token:
        return str(form_token).strip()
    if request.is_json:
        data = request.get_json(silent=True) or {}
        if isinstance(data, dict) and data.get("csrf_token"):
            return str(data.get("csrf_token")).strip()
    return ""


def csrf_protect():
    """before_request hook. Skipped in TESTING and for safe methods."""
    if current_app.config.get("TESTING"):
        return None
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return None
    ep = request.endpoint or ""
    if ep == "static":
        return None
    expected = session.get("_csrf")
    provided = _extract_csrf_token()
    if not expected or not provided or not _secure_eq(expected, provided):
        p = request.path or ""
        if p.startswith("/api/") or p.startswith("/ai/") or request.is_json:
            return jsonify({"error": "csrf_failed"}), 403
        abort(403)
    return None


def required_role_for_path(path: str) -> str | None:
    p = (path or "").split("?", 1)[0]
    for prefix in _ADMIN_PREFIXES:
        if p == prefix or p.startswith(prefix.rstrip("/") + "/"):
            return "admin"
        if prefix.endswith("/") and p.startswith(prefix):
            return "admin"
    for prefix in _PREPARER_PREFIXES:
        if p == prefix or p.startswith(prefix):
            return "preparer"
    return None


def forbidden_response():
    p = request.path or ""
    if p.startswith("/api/") or p.startswith("/ai/") or request.is_json:
        return jsonify({"error": "forbidden"}), 403
    abort(403)


def login_required(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            p = request.path or ""
            if p.startswith("/api/") or p.startswith("/ai/"):
                return jsonify({"error": "login_required"}), 401
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return wrapper


def role_required(min_role: str):
    def decorator(f):
        @functools.wraps(f)
        @login_required
        def wrapper(*args, **kwargs):
            if not role_at_least(session.get("role"), min_role):
                return forbidden_response()
            return f(*args, **kwargs)
        return wrapper
    return decorator


def view_only_for(max_role: str):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            g.view_only = role_rank(session.get("role")) <= role_rank(max_role)
            return f(*args, **kwargs)
        return wrapper
    return decorator


def new_upload_token(tmp_path: str) -> str:
    token = secrets.token_urlsafe(24)
    tokens = dict(session.get("upload_tokens") or {})
    tokens[token] = tmp_path
    session["upload_tokens"] = tokens
    return token


def pop_upload_path(token: str | None) -> str | None:
    if not token:
        return None
    tokens = dict(session.get("upload_tokens") or {})
    path = tokens.pop(token, None)
    session["upload_tokens"] = tokens
    return path if isinstance(path, str) and path else None
