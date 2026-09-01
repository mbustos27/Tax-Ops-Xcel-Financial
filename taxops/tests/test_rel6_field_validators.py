"""REL-6: api_field type validation — bad payloads rejected, good payloads pass."""
from __future__ import annotations

import json

import pytest


# ── unit tests for _validate_field ──────────────────────────────────────────

def test_bool_field_accepts_zero_and_one():
    from app import _validate_field

    ok, v = _validate_field("verified", 0)
    assert ok and v == 0
    ok, v = _validate_field("verified", 1)
    assert ok and v == 1
    ok, v = _validate_field("form_1120s", 0)
    assert ok and v == 0
    ok, v = _validate_field("form_1120s", 1)
    assert ok and v == 1


def test_bool_field_rejects_banana():
    from app import _validate_field

    ok, msg = _validate_field("verified", "banana")
    assert not ok
    assert "0 or 1" in msg


def test_bool_field_accepts_none():
    from app import _validate_field

    ok, v = _validate_field("verified", None)
    assert ok and v is None


def test_tax_year_accepts_valid_year():
    from app import _validate_field

    ok, v = _validate_field("tax_year", "2025")
    assert ok and v == 2025


def test_tax_year_rejects_bad_string():
    from app import _validate_field

    ok, msg = _validate_field("tax_year", "twenty-twenty")
    assert not ok


def test_tax_year_rejects_out_of_range():
    from app import _validate_field

    ok, msg = _validate_field("tax_year", 1800)
    assert not ok
    assert "out of range" in msg


def test_date_field_accepts_iso_date():
    from app import _validate_field

    ok, v = _validate_field("intake_date", "2026-01-15")
    assert ok and v == "2026-01-15"


def test_date_field_rejects_bad_format():
    from app import _validate_field

    ok, msg = _validate_field("intake_date", "15/01/2026")
    assert not ok


def test_money_field_accepts_positive():
    from app import _validate_field

    ok, v = _validate_field("total_fee", "250.00")
    assert ok and v == 250.0


def test_money_field_rejects_negative():
    from app import _validate_field

    ok, msg = _validate_field("total_fee", -10)
    assert not ok
    assert "negative" in msg


def test_money_field_rejects_string():
    from app import _validate_field

    ok, msg = _validate_field("total_fee", "free")
    assert not ok


def test_text_field_passthrough():
    """Text fields like processor pass through stripped."""
    from app import _validate_field

    ok, v = _validate_field("filing_status", "  MFJ  ")
    assert ok and v == "MFJ"


# ── integration tests via HTTP ───────────────────────────────────────────────

@pytest.fixture
def seeded_return(taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (id, display_name, created_at) VALUES (1, 'Test', '2026-01-01')"
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, tax_year, client_status, created_at) "
        "VALUES (1, 1, 2025, 'IN PROGRESS', '2026-01-01')"
    )
    conn.commit()
    conn.close()
    return 1


def test_api_field_rejects_invalid_tax_year(client_logged_in, seeded_return):
    resp = client_logged_in.post(
        f"/api/return/{seeded_return}/field",
        data=json.dumps({"field": "tax_year", "value": "not-a-year"}),
        content_type="application/json",
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data


def test_api_field_accepts_valid_tax_year(client_logged_in, seeded_return):
    resp = client_logged_in.post(
        f"/api/return/{seeded_return}/field",
        data=json.dumps({"field": "tax_year", "value": 2025}),
        content_type="application/json",
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data.get("success") is True


def test_api_field_rejects_invalid_verified(client_logged_in, seeded_return):
    resp = client_logged_in.post(
        f"/api/return/{seeded_return}/field",
        data=json.dumps({"field": "verified", "value": "banana"}),
        content_type="application/json",
    )
    assert resp.status_code == 400


def test_api_field_accepts_valid_verified(client_logged_in, seeded_return):
    resp = client_logged_in.post(
        f"/api/return/{seeded_return}/field",
        data=json.dumps({"field": "verified", "value": 1}),
        content_type="application/json",
    )
    assert resp.status_code == 200


def test_api_field_toggle_return_form(client_logged_in, seeded_return, taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO return_forms (return_id, form_1120s, form_1040) VALUES (?, 1, 0)",
        (seeded_return,),
    )
    conn.commit()
    conn.close()

    resp = client_logged_in.post(
        f"/api/return/{seeded_return}/field",
        data=json.dumps({"field": "form_1120s", "value": 0}),
        content_type="application/json",
    )
    assert resp.status_code == 200
    assert resp.get_json().get("success") is True

    conn = get_connection(taxops_db_path)
    val = conn.execute(
        "SELECT form_1120s FROM return_forms WHERE return_id=?",
        (seeded_return,),
    ).fetchone()[0]
    conn.close()
    assert int(val) == 0


def test_api_field_set_return_form_creates_row(client_logged_in, seeded_return, taxops_db_path):
    from db import get_connection

    resp = client_logged_in.post(
        f"/api/return/{seeded_return}/field",
        data=json.dumps({"field": "form_1065_llc", "value": 1}),
        content_type="application/json",
    )
    assert resp.status_code == 200

    conn = get_connection(taxops_db_path)
    row = conn.execute(
        "SELECT form_1065_llc FROM return_forms WHERE return_id=?",
        (seeded_return,),
    ).fetchone()
    conn.close()
    assert row is not None
    assert int(row[0]) == 1


def test_api_field_receptionist_may_save_client_email(client, app, taxops_db_path, seeded_return):
    """Front desk must save taxpayer_email on client profile without preparer role."""
    from werkzeug.security import generate_password_hash
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT OR IGNORE INTO auth_users (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, 'receptionist', 1, '2026-01-01T00:00:00Z')
        """,
        ("__recep_field__", generate_password_hash("pass"), "Recep"),
    )
    conn.commit()
    conn.close()

    client.post("/login", data={"username": "__recep_field__", "password": "pass"}, follow_redirects=True)
    resp = client.post(
        f"/api/return/{seeded_return}/field",
        data=json.dumps({"field": "taxpayer_email", "value": "client@example.com"}),
        content_type="application/json",
    )
    assert resp.status_code == 200, resp.get_json()
    assert resp.get_json().get("success") is True

    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT taxpayer_email FROM clients WHERE id=1").fetchone()
    conn.close()
    assert row["taxpayer_email"] == "client@example.com"


def test_api_field_receptionist_blocked_from_preparer_fields(client, app, taxops_db_path, seeded_return):
    from werkzeug.security import generate_password_hash
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT OR IGNORE INTO auth_users (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, 'receptionist', 1, '2026-01-01T00:00:00Z')
        """,
        ("__recep_block__", generate_password_hash("pass"), "Recep"),
    )
    conn.commit()
    conn.close()

    client.post("/login", data={"username": "__recep_block__", "password": "pass"}, follow_redirects=True)
    resp = client.post(
        f"/api/return/{seeded_return}/field",
        data=json.dumps({"field": "verified", "value": 1}),
        content_type="application/json",
    )
    assert resp.status_code == 403
    assert resp.get_json().get("required_role") == "preparer"
