"""SEC-1: CSRF protection tests.

Verifies that state-changing endpoints reject requests that lack a valid CSRF token
when enforcement is enabled, and pass through when a token is supplied.
"""
from __future__ import annotations

import pytest


import re

_CSRF_META_RE = re.compile(rb'<meta name="csrf-token" content="([^"]+)"')


@pytest.fixture
def csrf_client(app, monkeypatch):
    """Test client with CSRF enforcement re-enabled (overrides the conftest default).

    Logs in via the login page so the test client's session is the same session
    that Flask-WTF uses when validating tokens — required for correct token binding.
    """
    import app as mod

    app.config["WTF_CSRF_ENABLED"] = True
    app.config.setdefault("SECRET_KEY", "test-secret-csrf")
    monkeypatch.setattr(mod, "_LOGIN_USER", "__test_user__")
    monkeypatch.setattr(mod, "_LOGIN_PASS", "__test_pass__")
    client = app.test_client()

    # Fetch login page to seed a CSRF token into the client's session cookie.
    rv_get = client.get("/login")
    m = _CSRF_META_RE.search(rv_get.data or b"")
    assert m, "CSRF meta tag not found in login page — was it rendered correctly?"
    login_token = m.group(1).decode()

    # POST login with that token to authenticate.
    client.post(
        "/login",
        data={
            "username": "__test_user__",
            "password": "__test_pass__",
            "csrf_token": login_token,
        },
        follow_redirects=True,
    )
    yield client
    app.config["WTF_CSRF_ENABLED"] = False


def _extract_csrf_from_page(client, url: str = "/") -> str:
    """GET a page that extends base.html and return the CSRF token from its meta tag."""
    rv = client.get(url, follow_redirects=True)
    m = _CSRF_META_RE.search(rv.data or b"")
    assert m, f"No csrf-token meta tag found on {url}"
    return m.group(1).decode()


def test_post_without_csrf_token_is_rejected(csrf_client):
    """SEC-1: POST to a protected JSON endpoint without X-CSRFToken must return 400."""
    rv = csrf_client.post(
        "/api/return/1/status",
        json={"status": "PROCESSING"},
        content_type="application/json",
    )
    assert rv.status_code == 400, (
        f"Expected 400 (CSRF rejection) but got {rv.status_code}. "
        "Ensure CSRFProtect is wired and WTF_CSRF_ENABLED=True for this test."
    )


def test_post_with_valid_csrf_token_passes_middleware(csrf_client):
    """SEC-1: POST WITH a valid X-CSRFToken (extracted from a rendered page) must not
    be rejected by CSRF middleware.  Uses a non-existent return ID so the CSRF layer
    is exercised first; any non-CSRF-400 response means the middleware accepted the token.
    """
    token = _extract_csrf_from_page(csrf_client)

    rv = csrf_client.post(
        "/api/return/99999/status",
        json={"status": "FINALIZE"},
        content_type="application/json",
        headers={"X-CSRFToken": token},
    )
    csrf_rejected = rv.status_code == 400 and b"CSRF" in (rv.data or b"")
    assert not csrf_rejected, (
        f"Request with a valid CSRF token was still CSRF-rejected (HTTP {rv.status_code})."
    )
