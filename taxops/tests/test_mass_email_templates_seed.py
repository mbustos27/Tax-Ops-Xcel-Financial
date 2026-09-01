"""Prompt M: seeded templates render cleanly; seed script is idempotent."""

from __future__ import annotations

import json

import pytest

FIXTURE_FULL = {
    "client_name": "EXAMPLE, CLIENT",
    "name": "EXAMPLE, CLIENT",
    "display_name": "EXAMPLE, CLIENT",
    "log_number": "1234",
    "tax_year": 2026,
    "missing_items": ["W-2", "1099-INT"],
    "missing_items_text": "• W-2\n• 1099-INT",
    "deadline_date": "2027-10-15",
    "office_phone": "(555) 555-0100",
    "reply_to": "office@example.com",
    "client_status": "PROCESSING",
    "contact_status": "",
}

FIXTURE_EMPTY_OPTIONAL = {
    "client_name": "SAMPLE, TAXPAYER",
    "name": "SAMPLE, TAXPAYER",
    "display_name": "SAMPLE, TAXPAYER",
    "log_number": "5678",
    "tax_year": 2026,
    "missing_items": [],
    "missing_items_text": "",
    "deadline_date": "",
    "office_phone": "",
    "reply_to": "office@example.com",
    "client_status": "PROCESSING",
    "contact_status": "",
}


@pytest.mark.parametrize(
    "template_key",
    ["missing_docs_reminder", "extension_deadline_reminder"],
)
def test_deliverable_templates_render_without_leaked_syntax(template_key):
    from email_campaigns import render_email_template
    from mass_email_templates import ALL_TEMPLATES

    spec = next(t for t in ALL_TEMPLATES if t["key"] == template_key)
    rendered = render_email_template(spec, FIXTURE_FULL)
    assert "{{" not in rendered.subject
    assert "{{" not in rendered.body_html
    assert rendered.body_text and "{{" not in rendered.body_text
    assert "Xcel Financial Services" in rendered.body_html
    assert "1234" in rendered.subject or "1234" in rendered.body_html


def test_missing_docs_empty_optional_fields():
    from email_campaigns import render_email_template
    from mass_email_templates import TEMPLATE_MISSING_DOCS_REMINDER

    rendered = render_email_template(TEMPLATE_MISSING_DOCS_REMINDER, FIXTURE_EMPTY_OPTIONAL)
    assert "Still needed" not in rendered.body_html
    assert "call us at" not in rendered.body_html.lower()


def test_extension_deadline_empty_deadline_branch():
    from email_campaigns import render_email_template
    from mass_email_templates import TEMPLATE_EXTENSION_DEADLINE_REMINDER

    rendered = render_email_template(
        TEMPLATE_EXTENSION_DEADLINE_REMINDER, FIXTURE_EMPTY_OPTIONAL
    )
    assert "due on" not in rendered.body_html.lower()
    assert "Reminder" in rendered.subject or "5678" in rendered.subject


def test_seed_templates_idempotent(taxops_db_path):
    from db import get_connection
    from mass_email_seed import seed_mass_email_platform

    conn = get_connection(taxops_db_path)
    r1 = seed_mass_email_platform(conn, tax_year=2026)
    r2 = seed_mass_email_platform(conn, tax_year=2026)
    conn.close()

    assert r1["template_ids"] == r2["template_ids"]
    assert r1["campaign_ids"] == r2["campaign_ids"]

    conn = get_connection(taxops_db_path)
    count = conn.execute("SELECT COUNT(*) AS n FROM email_templates").fetchone()["n"]
    conn.close()
    assert count == 2


def test_seed_then_preview_campaign(taxops_db_path):
    from db import get_connection
    from email_campaigns import preview_campaign
    from mass_email_seed import seed_mass_email_platform

    conn = get_connection(taxops_db_path)
    result = seed_mass_email_platform(conn, tax_year=2026)
    tid = result["template_ids"]["missing_docs_reminder"]
    payload = preview_campaign(
        conn,
        {"tax_year": 2026, "open_missing_docs": True},
        template_id=tid,
        sample_limit=1,
    )
    conn.close()
    assert "stats" in payload
    assert payload["template_found"] is True


def test_greeting_uses_display_name_in_rendered_html():
    from email_campaigns import render_email_template
    from mass_email_templates import TEMPLATE_MISSING_DOCS_REMINDER

    vars_friendly = dict(FIXTURE_FULL)
    vars_friendly["display_name"] = "Friendly Name"
    rendered = render_email_template(TEMPLATE_MISSING_DOCS_REMINDER, vars_friendly)
    assert "Hello Friendly Name" in rendered.body_html
