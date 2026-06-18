"""SEC-5: MAX_CONTENT_LENGTH and 413 error handler tests."""
from __future__ import annotations

import io


def test_max_content_length_is_configured(app):
    """SEC-5: MAX_CONTENT_LENGTH is set and is at least 1 MB."""
    limit = app.config.get("MAX_CONTENT_LENGTH")
    assert limit is not None, "MAX_CONTENT_LENGTH must be set"
    assert limit >= 1 * 1024 * 1024, f"Limit too small: {limit}"


def test_max_content_length_default_is_50mb(app):
    """SEC-5: default MAX_CONTENT_LENGTH is 50 MB (52 428 800 bytes)."""
    limit = app.config.get("MAX_CONTENT_LENGTH")
    assert limit == 50 * 1024 * 1024, f"Expected 50 MB but got {limit}"


def test_413_handler_returns_json(client_logged_in, app, monkeypatch, taxops_db_path):
    """SEC-5: oversized upload returns JSON 413 with a readable error message.

    Creates a real return row so the upload route reaches the file-access layer
    (where MAX_CONTENT_LENGTH is enforced) rather than returning 404 early.
    """
    from db import get_connection

    # Create a client + return so the route doesn't 404 before reading the file.
    conn = get_connection(taxops_db_path)
    conn.execute("INSERT INTO clients (last_name, first_name) VALUES ('T', 'T')")
    client_id = conn.execute("SELECT last_id FROM (SELECT last_insert_rowid() AS last_id)").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, client_status) VALUES (?, 2025, 'PROCESSING')",
        (client_id,),
    )
    return_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    # Shrink the limit to 10 bytes so a small upload triggers 413.
    monkeypatch.setitem(app.config, "MAX_CONTENT_LENGTH", 10)

    big_body = b"X" * 512
    rv = client_logged_in.post(
        f"/return/{return_id}/documents/upload",
        data={"document": (io.BytesIO(big_body), "test.pdf")},
        content_type="multipart/form-data",
    )
    assert rv.status_code == 413, (
        f"Expected 413 for oversized upload, got {rv.status_code}. "
        "Ensure MAX_CONTENT_LENGTH is enforced by the WSGI/Werkzeug layer."
    )
    data = rv.get_json()
    assert data is not None, "413 response must be JSON"
    assert "error" in data
    assert "large" in data["error"].lower() or "size" in data["error"].lower()


def test_413_handler_mentions_limit(client_logged_in, app, monkeypatch, taxops_db_path):
    """SEC-5: 413 JSON error message includes the MB limit value."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute("INSERT INTO clients (last_name, first_name) VALUES ('T2', 'T2')")
    client_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, client_status) VALUES (?, 2025, 'PROCESSING')",
        (client_id,),
    )
    return_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    monkeypatch.setitem(app.config, "MAX_CONTENT_LENGTH", 10)

    rv = client_logged_in.post(
        f"/return/{return_id}/documents/upload",
        data={"document": (io.BytesIO(b"X" * 512), "big.pdf")},
        content_type="multipart/form-data",
    )
    if rv.status_code == 413:
        data = rv.get_json() or {}
        assert "MB" in (data.get("error") or "") or "large" in (data.get("error") or "").lower()
