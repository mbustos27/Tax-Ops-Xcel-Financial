"""SEC-3: session cookie security flags and response header tests."""
from __future__ import annotations

import pytest


# ── app config assertions ─────────────────────────────────────────────────────

def test_session_cookie_httponly_is_true(app):
    """SEC-3: SESSION_COOKIE_HTTPONLY is True so JS cannot access the session cookie."""
    assert app.config.get("SESSION_COOKIE_HTTPONLY") is True


def test_session_cookie_samesite_is_lax(app):
    """SEC-3: SESSION_COOKIE_SAMESITE is 'Lax' to block cross-site POST cookie leakage."""
    assert app.config.get("SESSION_COOKIE_SAMESITE") == "Lax"


def test_permanent_session_lifetime_is_12_hours(app):
    """SEC-3: PERMANENT_SESSION_LIFETIME is 12 hours (43200 seconds)."""
    from datetime import timedelta

    lifetime = app.config.get("PERMANENT_SESSION_LIFETIME")
    assert lifetime == timedelta(hours=12), (
        f"Expected timedelta(hours=12) but got {lifetime!r}"
    )


def test_templates_auto_reload_matches_debug(app):
    """SEC-3: TEMPLATES_AUTO_RELOAD is False in non-debug mode (no disk I/O overhead in production)."""
    assert app.config.get("TEMPLATES_AUTO_RELOAD") == app.debug


# ── response security headers ─────────────────────────────────────────────────

def test_security_headers_present_on_health(client):
    """SEC-3: /health response includes all expected security headers."""
    rv = client.get("/health")
    h = rv.headers
    assert h.get("X-Frame-Options") == "DENY"
    assert h.get("X-Content-Type-Options") == "nosniff"
    assert h.get("Referrer-Policy") == "same-origin"
    assert "no-store" in (h.get("Cache-Control") or "")


def test_csp_header_present_on_health(client):
    """SEC-3: Content-Security-Policy header is set on all responses."""
    rv = client.get("/health")
    csp = rv.headers.get("Content-Security-Policy") or ""
    assert csp, "Content-Security-Policy header is missing"
    assert "default-src 'self'" in csp
    assert "object-src 'none'" in csp
    assert "base-uri 'self'" in csp
    assert "form-action 'self'" in csp
    assert "frame-ancestors 'none'" in csp


def test_csp_header_present_on_login_page(client):
    """SEC-3: CSP header is also present on the unauthenticated login page."""
    rv = client.get("/login")
    csp = rv.headers.get("Content-Security-Policy") or ""
    assert csp, "Content-Security-Policy missing on /login"
    assert "frame-ancestors 'none'" in csp


# ── session permanence ────────────────────────────────────────────────────────

def test_session_is_permanent_after_login(client, taxops_db_path):
    """SEC-3: session.permanent is set to True on successful login so the 12-h lifetime is applied."""
    from db import get_connection
    from werkzeug.security import generate_password_hash

    conn = get_connection(taxops_db_path)
    conn.execute(
        """INSERT OR IGNORE INTO auth_users
           (username, password_hash, display_name, role, is_active, created_at)
           VALUES (?, ?, ?, 'staff', 1, '2025-01-01T00:00:00Z')""",
        ("sec3_user", generate_password_hash("sec3_pass"), "SEC3"),
    )
    conn.commit()
    conn.close()

    rv = client.post(
        "/login",
        data={"username": "sec3_user", "password": "sec3_pass"},
        follow_redirects=False,
    )
    assert rv.status_code in (301, 302), f"Expected redirect after login, got {rv.status_code}"

    # Inspect the Set-Cookie header for the session cookie.
    # Flask sets Max-Age / Expires only when session.permanent=True.
    set_cookie = rv.headers.get("Set-Cookie") or ""
    # Either Max-Age or Expires will be present for a permanent session.
    has_expiry = "Max-Age" in set_cookie or "Expires" in set_cookie
    assert has_expiry, (
        "Session cookie has no Max-Age/Expires — session.permanent was not set to True on login. "
        f"Set-Cookie: {set_cookie!r}"
    )
