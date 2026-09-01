"""Upcoming deadline flag and resolution."""

from __future__ import annotations

from datetime import date, timedelta

import pytest


def test_upcoming_deadline_within_30_days(taxops_db_path):
    from db import get_connection
    from return_deadlines import (
        UPCOMING_DEADLINE_FLAG,
        enrich_deadline_fields,
        upcoming_deadline_flag,
    )

    conn = get_connection(taxops_db_path)
    try:
        due = (date.today() + timedelta(days=14)).isoformat()
        row = {
            "client_status": "PROCESSING",
            "tax_year": 2025,
            "extension_due_date": due,
            "is_extension": 0,
            "extension_requested": 0,
        }
        assert upcoming_deadline_flag(row, {}) is True
        enrich_deadline_fields(row, {})
        assert row["upcoming_deadline_flag"] is True
        assert row["days_until_deadline"] == 14
    finally:
        conn.close()


def test_upcoming_deadline_false_when_logged_out():
    from return_deadlines import upcoming_deadline_flag

    row = {
        "client_status": "LOG OUT",
        "tax_year": 2025,
        "extension_due_date": (date.today() + timedelta(days=10)).isoformat(),
    }
    assert upcoming_deadline_flag(row, {}) is False


def test_upcoming_deadline_false_beyond_window():
    from return_deadlines import upcoming_deadline_flag

    row = {
        "client_status": "PROCESSING",
        "tax_year": 2025,
        "extension_due_date": (date.today() + timedelta(days=45)).isoformat(),
    }
    assert upcoming_deadline_flag(row, {}) is False


def test_effective_deadline_extension_fallback():
    from return_deadlines import effective_deadline_for_return

    due, label = effective_deadline_for_return(
        {
            "client_status": "PROCESSING",
            "tax_year": 2025,
            "is_extension": 1,
            "extension_requested": 0,
        },
        {},
    )
    assert due == "2026-10-15"
    assert "extended" in label.lower()


def test_effective_deadline_s_corp_regular():
    from datetime import date

    from return_deadlines import effective_deadline_for_return

    due, label = effective_deadline_for_return(
        {
            "client_status": "PROCESSING",
            "tax_year": 2025,
            "form_1120s": 1,
            "is_extension": 0,
        },
        {},
        today=date(2026, 3, 1),
    )
    assert due == "2026-03-15"
    assert "S corporation" in label


def test_effective_deadline_partnership_extension():
    from return_deadlines import effective_deadline_for_return

    due, label = effective_deadline_for_return(
        {
            "client_status": "PROCESSING",
            "tax_year": 2025,
            "form_1065_llc": 1,
            "drake_status_raw": "EF Ext Accepted",
        },
        {},
    )
    assert due == "2026-09-15"
    assert "Partnership" in label


def test_effective_deadline_open_past_regular_uses_extended():
    from datetime import date

    from return_deadlines import effective_deadline_for_return

    due, label = effective_deadline_for_return(
        {
            "client_status": "PROCESSING",
            "tax_year": 2025,
            "form_1120s": 1,
            "drake_status_raw": "In Progress",
        },
        {},
        today=date(2026, 8, 26),
    )
    assert due == "2026-09-15"
    assert "extended" in label.lower()


def test_upcoming_deadline_open_s_corp_past_march(taxops_db_path):
    from datetime import date

    from return_deadlines import upcoming_deadline_flag

    row = {
        "client_status": "PROCESSING",
        "tax_year": 2025,
        "form_1120s": 1,
        "drake_status_raw": "In Progress",
    }
    assert upcoming_deadline_flag(row, {}, today=date(2026, 8, 26)) is True


def test_effective_deadline_c_corp_regular():
    from datetime import date

    from return_deadlines import effective_deadline_for_return

    due, label = effective_deadline_for_return(
        {
            "client_status": "PROCESSING",
            "tax_year": 2025,
            "form_1120": 1,
        },
        {},
        today=date(2026, 4, 1),
    )
    assert due == "2026-04-15"
    assert "C corporation" in label


def test_enrich_adds_upcoming_to_risk_flags(app):
    from app import _enrich

    row = {
        "client_status": "HOLD",
        "tax_year": 2025,
        "extension_due_date": (date.today() + timedelta(days=7)).isoformat(),
        "total_fee": None,
        "fee_paid": None,
    }
    with app.test_request_context("/"):
        out = _enrich(row, deadline_lookup={})
    assert "UPCOMING DEADLINE" in out["risk_flags"]
