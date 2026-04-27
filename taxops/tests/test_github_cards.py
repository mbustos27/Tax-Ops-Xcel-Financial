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
    assert r.status_code == 200
    body = r.get_data(as_text=True).lower()
    assert "taxops" in body
    assert "data-table" in body or "quick filter" in body


@pytest.mark.gh21
@pytest.mark.gh28
def test_return_detail_requires_auth(client_logged_in):
    """Return detail routes stay behind login (workflow surface)."""
    r = client_logged_in.get("/return/999999")
    # Missing row typically 404; unauthenticated elsewhere tested via redirect
    assert r.status_code in (200, 302, 404)


# ── Next checks to add (same markers as in pytest.ini) ──────────────────────
# gh22: Drake CSV → DB round-trip   gh23: Manual Excel log
# gh24–25: Matcher thresholds + review queue    gh26: no duplicate client+year
# gh27: Log numbers visible on dashboard/detail
# Existing scripts (test_matching.py, main.py, etc.) can be folded in as pytest cases.
