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
