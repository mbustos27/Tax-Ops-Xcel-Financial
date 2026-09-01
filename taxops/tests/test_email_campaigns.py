"""Prompt K: audience resolution and template rendering (preview only)."""

from __future__ import annotations

import json

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
    spouse_email: str | None = None,
    do_not_email: int = 0,
    extension_due_date: str | None = None,
    is_extension: int = 0,
    extension_requested: int = 0,
    contact_status: str | None = None,
) -> int:
    conn.execute(
        """
        INSERT INTO clients (
          id, last_name, first_name, taxpayer_email, spouse_email, do_not_email
        ) VALUES (?, ?, 'Test', ?, ?, ?)
        """,
        (client_id, last_name, taxpayer_email, spouse_email, do_not_email),
    )
    conn.execute(
        """
        INSERT INTO returns (
          id, client_id, tax_year, log_number, client_status,
          extension_due_date, is_extension, extension_requested, contact_status,
          created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (
            client_id * 10,
            client_id,
            tax_year,
            log_number,
            client_status,
            extension_due_date,
            is_extension,
            extension_requested,
            contact_status,
        ),
    )
    return client_id * 10


def test_sanitize_audience_dashboard_shape():
    from email_campaigns import sanitize_audience_definition

    aud = sanitize_audience_definition(
        {
            "tax_year": 2026,
            "status": ["PROCESSING", "bogus"],
            "open_missing_docs": True,
            "extension_due_within_days": 14,
        }
    )
    assert aud["tax_year"] == 2026
    assert aud["status"] == ["PROCESSING"]
    assert aud["open_missing_docs"] is True
    assert aud["extension_due_within_days"] == 14


def test_resolve_audience_missing_docs_dedupes_client(taxops_db_path):
    from email_campaigns import resolve_audience
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        rid = _seed_client_return(
            conn,
            client_id=8001,
            last_name="Dedup",
            tax_year=2026,
            log_number="100",
            taxpayer_email="dedup@example.com",
        )
        conn.execute(
            "INSERT INTO missing_docs (return_id, item_text, is_resolved, created_at) "
            "VALUES (?, 'W-2', 0, datetime('now'))",
            (rid,),
        )
        conn.execute(
            "INSERT INTO missing_docs (return_id, item_text, is_resolved, created_at) "
            "VALUES (?, '1099-INT', 0, datetime('now'))",
            (rid,),
        )
        conn.commit()

        recipients, stats = resolve_audience(
            conn,
            {
                "tax_year": 2026,
                "status": ["PROCESSING"],
                "open_missing_docs": True,
            },
        )
        assert stats.audience_return_rows == 1
        assert stats.unique_clients_matched == 1
        assert stats.resolved_recipients == 1
        assert len(recipients) == 1
        assert recipients[0].client_id == 8001
        assert set(recipients[0].template_variables["missing_items"]) == {"W-2", "1099-INT"}
    finally:
        conn.close()


def test_resolve_audience_hand_checked_sql_count(taxops_db_path):
    """PROCESSING + open missing_docs + tax_year=2026 — match direct SQL count."""
    from email_campaigns import resolve_audience
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        r1 = _seed_client_return(
            conn, client_id=8010, last_name="A", tax_year=2026, log_number="10",
            taxpayer_email="a@example.com",
        )
        r2 = _seed_client_return(
            conn, client_id=8011, last_name="B", tax_year=2026, log_number="11",
            taxpayer_email="b@example.com",
        )
        _seed_client_return(
            conn, client_id=8012, last_name="C", tax_year=2026, log_number="12",
            taxpayer_email="c@example.com",
            client_status="LOG OUT",
        )
        conn.execute(
            "INSERT INTO missing_docs (return_id, item_text, is_resolved, created_at) "
            "VALUES (?, 'W-2', 0, datetime('now'))",
            (r1,),
        )
        conn.execute(
            "INSERT INTO missing_docs (return_id, item_text, is_resolved, created_at) "
            "VALUES (?, 'W-2', 0, datetime('now'))",
            (r2,),
        )
        conn.commit()

        sql_n = conn.execute(
            """
            SELECT COUNT(DISTINCT c.id)
            FROM returns r
            JOIN clients c ON c.id = r.client_id
            WHERE r.tax_year = 2026
              AND r.client_status = 'PROCESSING'
              AND UPPER(COALESCE(r.client_status, '')) != 'CANCELLED'
              AND COALESCE(c.is_test, 0) = 0
              AND EXISTS (
                SELECT 1 FROM missing_docs md
                WHERE md.return_id = r.id AND md.is_resolved = 0
              )
              AND c.taxpayer_email IS NOT NULL AND TRIM(c.taxpayer_email) != ''
              AND COALESCE(c.do_not_email, 0) = 0
            """
        ).fetchone()[0]

        recipients, stats = resolve_audience(
            conn,
            {"tax_year": 2026, "status": ["PROCESSING"], "open_missing_docs": True},
        )
        assert stats.unique_clients_matched == 2
        assert stats.resolved_recipients == sql_n
        assert len(recipients) == sql_n
    finally:
        conn.close()


def test_resolve_audience_drops_opt_out_and_no_email(taxops_db_path):
    from email_campaigns import resolve_audience
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        r_opt = _seed_client_return(
            conn, client_id=8020, last_name="Opt", tax_year=2026, log_number="20",
            taxpayer_email="opt@example.com", do_not_email=1,
        )
        r_no = _seed_client_return(
            conn, client_id=8021, last_name="No", tax_year=2026, log_number="21",
            taxpayer_email=None,
        )
        for rid in (r_opt, r_no):
            conn.execute(
                "INSERT INTO missing_docs (return_id, item_text, is_resolved, created_at) "
                "VALUES (?, 'W-2', 0, datetime('now'))",
                (rid,),
            )
        conn.commit()

        _, stats = resolve_audience(
            conn,
            {"tax_year": 2026, "open_missing_docs": True},
        )
        assert stats.unique_clients_matched == 2
        assert stats.dropped_do_not_email == 1
        assert stats.dropped_no_email == 1
        assert stats.resolved_recipients == 0
    finally:
        conn.close()


def test_render_template_no_leaked_syntax(taxops_db_path):
    from email_campaigns import render_email_template

    template = {
        "subject": "Log {{ log_number }} — {{ client_name }}",
        "body_html": "<p>Hi {{ client_name }}, missing: {{ missing_items_text }}</p>",
        "body_text": "Deadline {{ deadline_date }} — call {{ office_phone }}",
    }
    variables = {
        "log_number": "42",
        "client_name": "SMITH, JOHN",
        "missing_items_text": "• W-2",
        "deadline_date": "2026-10-15",
        "office_phone": "(555) 555-0100",
    }
    rendered = render_email_template(template, variables)
    assert "{{" not in rendered.subject
    assert "{{" not in rendered.body_html
    assert rendered.body_text and "{{" not in rendered.body_text
    assert "42" in rendered.subject
    assert "SMITH" in rendered.body_html


def test_preview_campaign_with_template_row(taxops_db_path):
    from email_campaigns import preview_campaign
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        rid = _seed_client_return(
            conn, client_id=8030, last_name="Prev", tax_year=2026, log_number="30",
            taxpayer_email="prev@example.com",
        )
        conn.execute(
            "INSERT INTO missing_docs (return_id, item_text, is_resolved, created_at) "
            "VALUES (?, 'W-2', 0, datetime('now'))",
            (rid,),
        )
        now = "2026-08-26T12:00:00Z"
        conn.execute(
            """
            INSERT INTO email_templates (
              key, subject, body_html, body_text, created_at, updated_at
            ) VALUES (
              'missing_docs', 'Docs for {{ log_number }}',
              '<p>{{ client_name }}</p>', 'plain', ?, ?
            )
            """,
            (now, now),
        )
        template_id = conn.execute("SELECT id FROM email_templates").fetchone()["id"]
        conn.commit()

        payload = preview_campaign(
            conn,
            {"tax_year": 2026, "open_missing_docs": True},
            template_id=template_id,
            sample_limit=1,
        )
        assert payload["stats"]["resolved_recipients"] == 1
        sample = payload["samples"][0]
        assert sample["subject"] == "Docs for 30"
        assert "Prev" in sample["body_html"]
    finally:
        conn.close()


def test_api_preview_route_counts(client_logged_in, taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    rid = _seed_client_return(
        conn, client_id=8040, last_name="Api", tax_year=2026, log_number="40",
        taxpayer_email="api@example.com",
    )
    conn.execute(
        "INSERT INTO missing_docs (return_id, item_text, is_resolved, created_at) "
        "VALUES (?, 'W-2', 0, datetime('now'))",
        (rid,),
    )
    conn.commit()
    conn.close()

    resp = client_logged_in.post(
        "/api/email-campaigns/preview",
        json={
            "audience_definition": {
                "tax_year": 2026,
                "open_missing_docs": True,
            },
            "sample_limit": 1,
        },
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["stats"]["resolved_recipients"] == 1
    assert len(data["samples"]) == 1
    assert data["samples"][0]["resolved_email"] == "api@example.com"
