"""Return detail cancel / uncancel / status / field / note APIs."""

from __future__ import annotations


def _seed_return(taxops_db_path: str, *, status: str = "PROCESSING") -> int:
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name) VALUES (901, 'Cancel', 'Test')"
    )
    conn.execute(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status)
        VALUES (9101, 901, '9101', 2025, ?)
        """,
        (status,),
    )
    conn.execute(
        "INSERT INTO payments (return_id, total_fee, fee_paid) VALUES (9101, 250.0, 50.0)"
    )
    conn.commit()
    conn.close()
    return 9101


def test_cancel_and_uncancel_return(client_logged_in, taxops_db_path):
    rid = _seed_return(taxops_db_path)

    r = client_logged_in.post(
        f"/api/return/{rid}/cancel",
        json={"reason": "Client withdrew"},
    )
    assert r.status_code == 200, r.get_json()
    assert r.get_json()["success"] is True
    assert r.get_json()["original_fee"] == 250.0

    from db import get_connection

    conn = get_connection(taxops_db_path)
    row = conn.execute(
        "SELECT client_status, cancelled_reason, cancelled_fee FROM returns WHERE id=?",
        (rid,),
    ).fetchone()
    assert row["client_status"] == "CANCELLED"
    assert row["cancelled_reason"] == "Client withdrew"
    assert float(row["cancelled_fee"] or 0) == 250.0
    pmt = conn.execute(
        "SELECT total_fee, fee_paid, cancelled_fee FROM payments WHERE return_id=?",
        (rid,),
    ).fetchone()
    assert float(pmt["total_fee"] or 0) == 0.0
    assert float(pmt["cancelled_fee"] or 0) == 250.0
    conn.close()

    r2 = client_logged_in.post(f"/api/return/{rid}/uncancel", json={})
    assert r2.status_code == 200, r2.get_json()
    conn = get_connection(taxops_db_path)
    row = conn.execute(
        "SELECT client_status, cancelled_reason FROM returns WHERE id=?", (rid,)
    ).fetchone()
    assert row["client_status"] == "PROCESSING"
    assert row["cancelled_reason"] is None
    pmt = conn.execute(
        "SELECT total_fee, cancelled_fee FROM payments WHERE return_id=?", (rid,)
    ).fetchone()
    assert float(pmt["total_fee"] or 0) == 250.0
    conn.close()


def test_cancel_requires_reason(client_logged_in, taxops_db_path):
    rid = _seed_return(taxops_db_path)
    r = client_logged_in.post(f"/api/return/{rid}/cancel", json={"reason": "  "})
    assert r.status_code == 400


def test_status_and_field_and_note(client_logged_in, taxops_db_path):
    rid = _seed_return(taxops_db_path)

    r = client_logged_in.post(
        f"/api/return/{rid}/status",
        json={"status": "HOLD"},
    )
    assert r.status_code == 200, r.get_json()
    assert r.get_json().get("success") is True

    r = client_logged_in.post(
        f"/api/return/{rid}/field",
        json={"field": "extension_requested", "value": 1},
    )
    assert r.status_code == 200, r.get_json()

    r = client_logged_in.post(
        f"/api/return/{rid}/note",
        json={"text": "staff note"},
    )
    assert r.status_code == 200, r.get_json()

    from db import get_connection

    conn = get_connection(taxops_db_path)
    assert conn.execute(
        "SELECT client_status FROM returns WHERE id=?", (rid,)
    ).fetchone()["client_status"] == "HOLD"
    assert conn.execute(
        "SELECT COUNT(*) c FROM notes WHERE return_id=? AND note_text LIKE '%staff note%'",
        (rid,),
    ).fetchone()["c"] >= 1
    conn.close()


def test_return_detail_exports_cancel_handlers():
    """onclick cancel buttons need window.* exports (script is an IIFE)."""
    from pathlib import Path

    html = Path(__file__).resolve().parents[1].joinpath(
        "templates", "return_detail.html"
    ).read_text(encoding="utf-8")
    for name in (
        "showCancelForm",
        "hideCancelForm",
        "confirmCancel",
        "uncancelReturn",
        "deleteReturn",
        "removeDependent",
        "setStatusDetail",
        "submitNote",
        "quickToggle",
    ):
        assert f"window.{name}" in html, f"missing window.{name} export"
