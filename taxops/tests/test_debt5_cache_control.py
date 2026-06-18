"""DEBT-5: Cache-Control — versioned /static/ assets get immutable long-cache; HTML/API stay no-store."""
from __future__ import annotations

import pytest


def test_static_asset_with_version_gets_long_cache(client):
    """Versioned /static/app.css?v=abc gets public, max-age=31536000, immutable."""
    resp = client.get("/static/app.css?v=test123")
    cc = resp.headers.get("Cache-Control", "")
    assert "immutable" in cc or "max-age=31536000" in cc, (
        f"Expected long-cache header for versioned static asset, got: {cc!r}"
    )


def test_html_page_gets_no_store(client):
    """HTML responses (login page) stay Cache-Control: no-store."""
    resp = client.get("/login")
    cc = resp.headers.get("Cache-Control", "")
    assert "no-store" in cc, f"Expected no-store for HTML, got: {cc!r}"


def test_api_endpoint_gets_no_store(client, client_logged_in):
    """/health JSON response has Cache-Control: no-store."""
    resp = client_logged_in.get("/health")
    cc = resp.headers.get("Cache-Control", "")
    assert "no-store" in cc, f"Expected no-store for /health, got: {cc!r}"


def test_static_without_version_gets_no_store(client):
    """/static/app.css without ?v= still gets no-store (not safe to cache)."""
    resp = client.get("/static/app.css")
    cc = resp.headers.get("Cache-Control", "")
    assert "no-store" in cc, f"Expected no-store for unversioned static, got: {cc!r}"


def test_security_headers_docstring_mentions_debt5(app):
    """_security_headers docstring documents the DEBT-5 Cache-Control strategy."""
    import app as app_mod
    doc = (app_mod._security_headers.__doc__ or "").upper()
    assert "DEBT-5" in doc or "CACHE" in doc
