"""PROD-6: POST /api/client-error (GitHub #94)."""

from __future__ import annotations

import logging

import pytest


def test_client_error_anonymous_returns_401_json(client):
    rv = client.post(
        "/api/client-error",
        json={"kind": "error", "message": "boom", "page_url": "/"},
    )
    assert rv.status_code == 401
    assert rv.get_json().get("error") == "login_required"


def test_client_error_invalid_json_logged_in(client_logged_in):
    rv = client_logged_in.post(
        "/api/client-error",
        data="not-json",
        content_type="text/plain",
    )
    assert rv.status_code == 400
    assert rv.get_json().get("error") == "expected_json_object"


def test_client_error_logs_warning(client_logged_in, caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.WARNING, logger="taxops.frontend"):
        rv = client_logged_in.post(
            "/api/client-error",
            json={
                "kind": "unhandledrejection",
                "message": "unit-test client boundary",
                "page_url": "http://test/return/1",
                "filename": "app.js",
                "lineno": 12,
                "stack": "Error: unit-test\n    at synthetic",
            },
        )
    assert rv.status_code == 200
    assert rv.get_json() == {"ok": True}

    msgs = "\n".join(r.message for r in caplog.records if r.name == "taxops.frontend")
    assert "CLIENT_JS" in msgs or "CLIENT_JS[" in "".join(caplog.messages)
    assert "unit-test client boundary" in msgs
