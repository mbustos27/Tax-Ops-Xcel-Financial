"""Deadline windows, stale processing, and expanded mass-email templates."""

from __future__ import annotations

from datetime import date, timedelta

import pytest


def _seed_client_return(
    conn,
    *,
    client_id: int,
    last_name: str,
    tax_year: int,
    log_number: str,
    client_status: str = "PROCESSING",
    taxpayer_email: str | None = None,
    do_not_email: int = 0,
    intake_date: str | None = None,
    logout_date: str | None = None,
    ack_date: str | None = None,
    drake_status_raw: str | None = None,
) -> int:
    conn.execute(
        """
        INSERT INTO clients (
          id, last_name, first_name, taxpayer_email, do_not_email
        ) VALUES (?, ?, 'Test', ?, ?)
        """,
        (client_id, last_name, taxpayer_email, do_not_email),
    )
    conn.execute(
        """
        INSERT INTO returns (
          id, client_id, tax_year, log_number, client_status,
          intake_date, logout_date, ack_date, drake_status_raw, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (
            client_id * 10,
            client_id,
            tax_year,
            log_number,
            client_status,
            intake_date,
            logout_date,
            ack_date,
            drake_status_raw,
        ),
    )
    return client_id * 10


def test_sanitize_stale_and_deadline_window_keys():
    from email_campaigns import sanitize_audience_definition

    aud = sanitize_audience_definition(
        {
            "stale_processing": True,
            "in_progress_only": True,
            "tax_deadline_id": 3,
            "deadline_due_within_days": 30,
            "deadline_due_min_days": 8,
        }
    )
    assert aud["stale_processing"] == "1"
    assert aud["in_progress_only"] == "1"
    assert aud["tax_deadline_id"] == 3
    assert aud["deadline_due_within_days"] == 30
    assert aud["deadline_due_min_days"] == 8


def test_resolve_stale_processing_matches_dashboard_logic(taxops_db_path):
    from email_campaigns import resolve_audience
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        old_intake = (date.today() - timedelta(days=61)).isoformat()
        recent_intake = (date.today() - timedelta(days=10)).isoformat()

        r_stale = _seed_client_return(
            conn,
            client_id=9001,
            last_name="Stale",
            tax_year=2026,
            log_number="901",
            taxpayer_email="stale@example.com",
            intake_date=old_intake,
            drake_status_raw="Pending",
        )
        conn.execute(
            "UPDATE clients SET display_name = 'STALE CLIENT' WHERE id = 9001"
        )
        _seed_client_return(
            conn,
            client_id=9002,
            last_name="Fresh",
            tax_year=2026,
            log_number="902",
            taxpayer_email="fresh@example.com",
            intake_date=recent_intake,
        )
        _seed_client_return(
            conn,
            client_id=9003,
            last_name="Done",
            tax_year=2026,
            log_number="903",
            taxpayer_email="done@example.com",
            intake_date=old_intake,
            drake_status_raw="Printed",
        )
        conn.commit()

        recipients, stats = resolve_audience(
            conn,
            {"tax_year": 2026, "stale_processing": True},
        )
        assert stats.audience_return_rows == 1
        assert stats.resolved_recipients == 1
        assert recipients[0].client_id == 9001
        assert r_stale == recipients[0].return_id
    finally:
        conn.close()


def test_deadline_window_blocks_outside_range(taxops_db_path):
    from email_campaigns import resolve_audience
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        far_future = (date.today() + timedelta(days=90)).isoformat()
        conn.execute(
            """
            INSERT INTO tax_deadlines (label, applies_to, due_date, tax_year, is_active)
            VALUES ('Test deadline', 'test', ?, 2026, 1)
            """,
            (far_future,),
        )
        td_id = conn.execute("SELECT id FROM tax_deadlines").fetchone()["id"]
        _seed_client_return(
            conn,
            client_id=9010,
            last_name="Win",
            tax_year=2026,
            log_number="910",
            taxpayer_email="win@example.com",
        )
        conn.commit()

        _, stats = resolve_audience(
            conn,
            {
                "tax_year": 2026,
                "tax_deadline_id": td_id,
                "deadline_due_within_days": 7,
                "deadline_due_min_days": 0,
            },
        )
        assert stats.deadline_window_blocked is True
        assert stats.resolved_recipients == 0
        assert stats.deadline_window_reason == "outside_deadline_window"
    finally:
        conn.close()


def test_deadline_window_allows_and_sets_variables(taxops_db_path):
    from email_campaigns import resolve_audience
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        due = (date.today() + timedelta(days=5)).isoformat()
        conn.execute(
            """
            INSERT INTO tax_deadlines (label, applies_to, due_date, tax_year, is_active)
            VALUES ('Federal individual return due', 'federal/1040', ?, 2026, 1)
            """,
            (due,),
        )
        td_id = conn.execute("SELECT id FROM tax_deadlines").fetchone()["id"]
        _seed_client_return(
            conn,
            client_id=9020,
            last_name="Due",
            tax_year=2026,
            log_number="920",
            taxpayer_email="due@example.com",
            client_status="HOLD",
        )
        conn.commit()

        recipients, stats = resolve_audience(
            conn,
            {
                "tax_year": 2026,
                "tax_deadline_id": td_id,
                "deadline_due_within_days": 7,
                "deadline_due_min_days": 0,
                "in_progress_only": True,
            },
        )
        assert stats.deadline_window_blocked is False
        assert stats.resolved_recipients == 1
        vars_ = recipients[0].template_variables
        assert vars_["deadline_date"] == due
        assert vars_["deadline_label"] == "Federal individual return due"
        assert vars_["days_until_deadline"] == 5
    finally:
        conn.close()


def test_all_mass_email_templates_render_without_syntax_leaks():
    from email_campaigns import render_email_template
    from mass_email_templates import ALL_TEMPLATES

    variables = {
        "client_name": "EXAMPLE, CLIENT",
        "name": "EXAMPLE, CLIENT",
        "display_name": "EXAMPLE, CLIENT",
        "log_number": "1234",
        "tax_year": 2026,
        "missing_items": ["W-2", "1099-INT"],
        "missing_items_text": "• W-2\n• 1099-INT",
        "deadline_date": "2027-04-15",
        "deadline_label": "Federal individual return due",
        "days_until_deadline": 14,
        "intake_date": "2026-02-01",
        "cycle_days": 28,
        "office_phone": "(555) 555-0100",
        "reply_to": "office@example.com",
    }
    for spec in ALL_TEMPLATES:
        rendered = render_email_template(spec, variables)
        assert "{{" not in rendered.subject, spec["key"]
        assert "{{" not in rendered.body_html, spec["key"]
        assert rendered.body_text and "{{" not in rendered.body_text, spec["key"]


def test_resolve_slow_cycle_open_returns(taxops_db_path):
    """Slow-cycle campaigns target open returns, not completed logout/ack cycles."""
    from email_campaigns import resolve_audience
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        old_intake = (date.today() - timedelta(days=25)).isoformat()
        recent_intake = (date.today() - timedelta(days=5)).isoformat()

        _seed_client_return(
            conn,
            client_id=9030,
            last_name="SlowOpen",
            tax_year=2026,
            log_number="930",
            taxpayer_email="slowopen@example.com",
            client_status="PROCESSING",
            intake_date=old_intake,
        )
        _seed_client_return(
            conn,
            client_id=9031,
            last_name="FastOpen",
            tax_year=2026,
            log_number="931",
            taxpayer_email="fast@example.com",
            client_status="PROCESSING",
            intake_date=recent_intake,
        )
        _seed_client_return(
            conn,
            client_id=9032,
            last_name="SlowDone",
            tax_year=2026,
            log_number="932",
            taxpayer_email="slowdone@example.com",
            client_status="LOG OUT",
            intake_date=old_intake,
            logout_date=(date.today() - timedelta(days=1)).isoformat(),
        )
        conn.commit()

        recipients, stats = resolve_audience(
            conn,
            {
                "tax_year": 2026,
                "slow_cycle": True,
                "status": ["PROCESSING", "HOLD"],
            },
        )
        assert stats.resolved_recipients == 1
        assert recipients[0].client_id == 9030
        assert recipients[0].template_variables["cycle_days"] >= 25
    finally:
        conn.close()


def test_drake_efile_complete_excluded_from_slow_cycle(taxops_db_path):
    from email_campaigns import resolve_audience
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        old_intake = (date.today() - timedelta(days=25)).isoformat()
        _seed_client_return(
            conn,
            client_id=9040,
            last_name="ShouldSkip",
            tax_year=2026,
            log_number="940",
            taxpayer_email="skip@example.com",
            client_status="PROCESSING",
            intake_date=old_intake,
            drake_status_raw="EF Accepted",
        )
        _seed_client_return(
            conn,
            client_id=9041,
            last_name="ShouldKeep",
            tax_year=2026,
            log_number="941",
            taxpayer_email="keep@example.com",
            client_status="PROCESSING",
            intake_date=old_intake,
            drake_status_raw="EF Ext Accepted",
        )
        conn.commit()

        recipients, stats = resolve_audience(
            conn,
            {"tax_year": 2026, "slow_cycle": True, "status": ["PROCESSING"]},
        )
        assert stats.resolved_recipients == 1
        assert recipients[0].client_id == 9041
    finally:
        conn.close()


def test_engagement_status_rules():
    from engagement_status_rules import (
        ENGAGEMENT_EFILE_ACCEPTED,
        ENGAGEMENT_EXTENSION_EFILE_ACCEPTED,
        engagement_status_violation,
    )

    assert engagement_status_violation(ENGAGEMENT_EFILE_ACCEPTED, "LOG OUT") is None
    assert (
        engagement_status_violation(ENGAGEMENT_EFILE_ACCEPTED, "PROCESSING")
        == "efile_accepted_not_logout"
    )
    assert (
        engagement_status_violation(ENGAGEMENT_EXTENSION_EFILE_ACCEPTED, "PROCESSING")
        is None
    )
    assert (
        engagement_status_violation(
            ENGAGEMENT_EXTENSION_EFILE_ACCEPTED, "PENDING INTAKE"
        )
        == "extension_accepted_before_processing"
    )


def test_tax_deadline_seeds_count():
    from mass_email_templates import TAX_DEADLINE_SEEDS

    assert len(TAX_DEADLINE_SEEDS) >= 23
    labels = {row["label"] for row in TAX_DEADLINE_SEEDS}
    assert "Federal individual return due" in labels
    assert "C corporation return due" in labels
    assert "Partnership / LLC return due" in labels
    assert "Q4 estimated tax payment" in labels
    assert any(row["tax_year"] == 2025 for row in TAX_DEADLINE_SEEDS)
