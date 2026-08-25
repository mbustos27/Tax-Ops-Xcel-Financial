"""M3 — POST /filetrack/status: the internal, non-session, token-authed
endpoint the scan listener's HTTP sink calls. Uses the same taxops_db_path/
client fixtures as the rest of the suite, but deliberately never uses
client_logged_in — this endpoint must work with NO browser session at all."""
from __future__ import annotations

import pytest


def _seed_return(taxops_db_path, log_number="00123"):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) "
        "VALUES ('Test', 'Client', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')"
    )
    client_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, log_number, tax_year, client_status, created_at, updated_at) "
        "VALUES (?, ?, 2026, 'PROCESSING', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        (client_id, log_number),
    )
    conn.commit()
    return_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return return_id


def test_disabled_by_default_returns_404(client, taxops_db_path, monkeypatch):
    import routes.filetrack as ft

    monkeypatch.setattr(ft, "FILETRACK_ENABLED", False)
    resp = client.post("/filetrack/status", json={"log_number": "123", "status": "FINALIZE"})
    assert resp.status_code == 404


def test_enabled_without_token_configured_allows_localhost(client, taxops_db_path, monkeypatch):
    import routes.filetrack as ft

    monkeypatch.setattr(ft, "FILETRACK_ENABLED", True)
    monkeypatch.setattr(ft, "FILETRACK_TOKEN", "")
    return_id = _seed_return(taxops_db_path, "00123")

    resp = client.post(
        "/filetrack/status",
        json={"log_number": "123", "status": "FINALIZE"},
        environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["success"] is True
    assert body["matched"] is True
    assert body["return_id"] == return_id
    assert body["client_status_synced"] is True

    # 2026-07-22: the whole point of syncing — the real client_status the
    # app displays actually moved, not just the filetrack_status shadow field.
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute("SELECT client_status FROM returns WHERE id=?", (return_id,)).fetchone()
        assert row["client_status"] == "FINALIZE"
    finally:
        conn.close()


def test_enabled_without_token_configured_rejects_non_localhost(client, taxops_db_path, monkeypatch):
    import routes.filetrack as ft

    monkeypatch.setattr(ft, "FILETRACK_ENABLED", True)
    monkeypatch.setattr(ft, "FILETRACK_TOKEN", "")

    resp = client.post(
        "/filetrack/status",
        json={"log_number": "123", "status": "FINALIZE"},
        environ_overrides={"REMOTE_ADDR": "10.0.0.55"},
    )
    assert resp.status_code == 401


def test_enabled_with_token_requires_matching_header(client, taxops_db_path, monkeypatch):
    import routes.filetrack as ft

    monkeypatch.setattr(ft, "FILETRACK_ENABLED", True)
    monkeypatch.setattr(ft, "FILETRACK_TOKEN", "s3cr3t")
    _seed_return(taxops_db_path, "00123")

    resp_no_token = client.post("/filetrack/status", json={"log_number": "123", "status": "FINALIZE"})
    assert resp_no_token.status_code == 401

    resp_wrong_token = client.post(
        "/filetrack/status",
        json={"log_number": "123", "status": "FINALIZE"},
        headers={"X-Filetrack-Token": "wrong"},
    )
    assert resp_wrong_token.status_code == 401

    resp_ok = client.post(
        "/filetrack/status",
        json={"log_number": "123", "status": "FINALIZE"},
        headers={"X-Filetrack-Token": "s3cr3t"},
    )
    assert resp_ok.status_code == 200
    assert resp_ok.get_json()["success"] is True


def test_unknown_status_returns_400(client, taxops_db_path, monkeypatch):
    import routes.filetrack as ft

    monkeypatch.setattr(ft, "FILETRACK_ENABLED", True)
    monkeypatch.setattr(ft, "FILETRACK_TOKEN", "")

    resp = client.post(
        "/filetrack/status",
        json={"log_number": "123", "status": "NOT_A_REAL_STATUS"},
        environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
    )
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_missing_fields_returns_400(client, taxops_db_path, monkeypatch):
    import routes.filetrack as ft

    monkeypatch.setattr(ft, "FILETRACK_ENABLED", True)
    monkeypatch.setattr(ft, "FILETRACK_TOKEN", "")

    resp = client.post(
        "/filetrack/status",
        json={"log_number": "123"},
        environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
    )
    assert resp.status_code == 400


def test_unmatched_log_number_still_succeeds(client, taxops_db_path, monkeypatch):
    import routes.filetrack as ft

    monkeypatch.setattr(ft, "FILETRACK_ENABLED", True)
    monkeypatch.setattr(ft, "FILETRACK_TOKEN", "")

    resp = client.post(
        "/filetrack/status",
        json={"log_number": "00999", "status": "PICKUP"},
        environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["matched"] is False
    assert body["return_id"] is None


def test_endpoint_is_csrf_exempt(app, taxops_db_path, monkeypatch):
    """Regression test for the 2026-07-22 production incident: the first real
    listener run got "CSRF token missing or invalid" (HTTP 400) from every
    scan, because Flask-WTF's global CSRFProtect(app) in app.py protects all
    POST routes by default and nothing had exempted this one yet. conftest.py
    disables WTF_CSRF_ENABLED for the whole suite (see its SEC-1 comment),
    which is exactly why the other tests in this file never caught this — so
    this test deliberately re-enables enforcement, the same pattern
    test_csrf.py's csrf_client fixture uses, but WITHOUT logging in (the
    entire point of this endpoint is that a headless listener has no session
    or CSRF token to send at all — see test_endpoint_requires_no_session_login_at_all
    below)."""
    import routes.filetrack as ft

    monkeypatch.setattr(ft, "FILETRACK_ENABLED", True)
    monkeypatch.setattr(ft, "FILETRACK_TOKEN", "sekret")
    app.config.setdefault("SECRET_KEY", "test-secret-csrf")
    app.config["WTF_CSRF_ENABLED"] = True
    try:
        client = app.test_client()
        resp = client.post(
            "/filetrack/status",
            json={"log_number": "123", "status": "FINALIZE"},
            headers={"X-Filetrack-Token": "sekret"},
        )
        csrf_rejected = resp.status_code == 400 and b"CSRF" in (resp.data or b"")
        assert not csrf_rejected, (
            f"Got CSRF-rejected (HTTP {resp.status_code}, body={resp.data!r}) — "
            "the _csrf.exempt(api_filetrack_status) call in app.py is missing "
            "or was undone."
        )
        assert resp.status_code == 200
    finally:
        app.config["WTF_CSRF_ENABLED"] = False


def test_endpoint_requires_no_session_login_at_all(client, taxops_db_path, monkeypatch):
    """This is the whole point of the endpoint — a headless listener process
    has no browser session, so /login must never be required."""
    import routes.filetrack as ft

    monkeypatch.setattr(ft, "FILETRACK_ENABLED", True)
    monkeypatch.setattr(ft, "FILETRACK_TOKEN", "sekret")

    with client.session_transaction() as sess:
        assert "logged_in" not in sess  # never logged in

    resp = client.post(
        "/filetrack/status",
        json={"log_number": "123", "status": "FINALIZE"},
        headers={"X-Filetrack-Token": "sekret"},
    )
    assert resp.status_code == 200
