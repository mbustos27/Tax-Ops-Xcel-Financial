"""Automated checks aligned with TaxOps GitHub project cards.

Each test is tagged with @pytest.mark.ghNN so you can scope runs to board work:

    cd taxops
    pip install -r requirements.txt -r requirements-dev.txt
    pytest tests/ -v -m gh28
    pytest tests/ -v -m \"gh26 or gh28\"

Issues: epic #21, DoDs #22–#28."""

from __future__ import annotations

import pytest


# ── Smoke: real workflow usability (#28 / epic #21) ─────────────────────────


@pytest.mark.gh21
@pytest.mark.gh28
def test_dashboard_reachable_logged_in_lists_returns_header(client_logged_in):
    """Imported/empty DB still exposes the same core dashboard shell staff use."""
    r = client_logged_in.get("/")
    assert r.status_code == 200, "logged-in user must reach dashboard without redirect"
    body = r.get_data(as_text=True).lower()
    assert "taxops" in body, "dashboard must render TaxOps branding/title"
    assert (
        "data-table" in body or "quick filter" in body
    ), "dashboard must show the returns grid and/or quick filter so staff can work"


@pytest.mark.gh21
@pytest.mark.gh28
def test_return_missing_id_returns_not_found_when_logged_in(client_logged_in):
    """Authenticated users hitting a bogus return id get a proper missing response."""
    r = client_logged_in.get("/return/999999")
    assert r.status_code == 404, "missing return id must 404, not 500 or OK"


@pytest.mark.gh21
@pytest.mark.gh28
def test_anonymous_dashboard_redirects_to_login(client):
    """Guests cannot load the dashboard HTML."""
    r = client.get("/", follow_redirects=False)
    assert r.status_code == 302, "dashboard must redirect when not logged in"
    loc = (r.headers.get("Location") or "").lower()
    assert "login" in loc, "redirect must send user to login"


@pytest.mark.gh21
@pytest.mark.gh28
def test_anonymous_return_detail_redirects_to_login(client):
    """Guests cannot open return detail."""
    r = client.get("/return/1", follow_redirects=False)
    assert r.status_code == 302, "return detail must redirect when not logged in"
    loc = (r.headers.get("Location") or "").lower()
    assert "login" in loc, "redirect must send user to login"


# ── Next checks to add (same markers as in pytest.ini) ──────────────────────
# gh22: Drake CSV → DB round-trip   gh23: Manual Excel log
# gh24–25: Matcher thresholds + review queue    gh26: no duplicate client+year
# gh27: Log numbers visible on dashboard/detail
# Existing scripts (test_matching.py, main.py, etc.) can be folded in as pytest cases.
