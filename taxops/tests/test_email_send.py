"""Prompt L: send worker safety gates, RBAC, and duplicate-send guard."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from werkzeug.security import generate_password_hash


def _seed_template_and_campaign(conn, client_id: int = 9001) -> tuple[int, int, int]:
    """Template + campaign + one PROCESSING return with email and open missing doc."""
    conn.execute(
        """
        INSERT INTO clients (
          id, last_name, first_name, taxpayer_email, do_not_email
        ) VALUES (?, 'Send', 'Test', 'sendtest@example.com', 0)
        """,
        (client_id,),
    )
    rid = client_id * 10
    conn.execute(
        """
        INSERT INTO returns (
          id, client_id, tax_year, log_number, client_status, created_at
        ) VALUES (?, ?, 2026, ?, 'PROCESSING', datetime('now'))
        """,
        (rid, client_id, str(client_id)),
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
        ) VALUES ('t', 'Subj {{ log_number }}', '<p>{{ client_name }}</p>', 'plain', ?, ?)
        """,
        (now, now),
    )
    tid = conn.execute("SELECT id FROM email_templates").fetchone()["id"]
    audience = '{"tax_year":2026,"open_missing_docs":true}'
    conn.execute(
        """
        INSERT INTO email_campaigns (
          template_id, audience_definition, status, created_at
        ) VALUES (?, ?, 'draft', ?)
        """,
        (tid, audience, now),
    )
    cid = conn.execute("SELECT id FROM email_campaigns").fetchone()["id"]
    conn.commit()
    return int(tid), int(cid), rid


def test_smtp_will_connect_false_by_default(monkeypatch):
    import config as cfg
    from email_send import smtp_will_connect

    monkeypatch.setattr(cfg, "EMAIL_CAMPAIGN_ALLOW_SMTP", False)
    monkeypatch.setattr(cfg, "SMTP_DRY_RUN", True)
    assert smtp_will_connect() is False

    monkeypatch.setattr(cfg, "EMAIL_CAMPAIGN_ALLOW_SMTP", True)
    monkeypatch.setattr(cfg, "SMTP_DRY_RUN", True)
    assert smtp_will_connect() is False


def test_deliver_email_never_opens_smtp_when_gated(monkeypatch):
    import config as cfg
    from email_send import deliver_email

    monkeypatch.setattr(cfg, "EMAIL_CAMPAIGN_ALLOW_SMTP", False)
    monkeypatch.setattr(cfg, "SMTP_DRY_RUN", True)

    with patch("smtplib.SMTP") as mock_smtp:
        result = deliver_email(
            "staff@example.com",
            "Hi",
            "<p>Hi</p>",
            "Hi",
        )
        mock_smtp.assert_not_called()
    assert result.ok and result.dry_run


def test_test_send_dry_run_writes_row_no_smtp(taxops_db_path, monkeypatch):
    import config as cfg
    from db import get_connection
    from email_send import run_campaign_send

    monkeypatch.setattr(cfg, "EMAIL_CAMPAIGN_ALLOW_SMTP", False)
    monkeypatch.setattr(cfg, "SMTP_DRY_RUN", True)

    conn = get_connection(taxops_db_path)
    _, campaign_id, _ = _seed_template_and_campaign(conn, 9101)

    with patch("smtplib.SMTP") as mock_smtp:
        stats = run_campaign_send(
            conn,
            campaign_id,
            "test",
            test_address="staff@example.com",
        )
        mock_smtp.assert_not_called()

    assert stats.dry_run is True
    assert stats.sent == 1
    row = conn.execute(
        "SELECT status FROM client_email_sends WHERE campaign_id = ?",
        (campaign_id,),
    ).fetchone()
    assert row["status"] == "test_dry_run"
    camp = conn.execute(
        "SELECT status FROM email_campaigns WHERE id = ?",
        (campaign_id,),
    ).fetchone()
    assert camp["status"] == "test_sent"
    conn.close()


def test_live_send_requires_approve(taxops_db_path, monkeypatch):
    import config as cfg
    from db import get_connection
    from email_send import run_campaign_send

    monkeypatch.setattr(cfg, "EMAIL_CAMPAIGN_ALLOW_SMTP", False)
    monkeypatch.setattr(cfg, "SMTP_DRY_RUN", True)

    conn = get_connection(taxops_db_path)
    _, campaign_id, _ = _seed_template_and_campaign(conn, 9102)
    conn.close()

    conn = get_connection(taxops_db_path)
    with pytest.raises(ValueError, match="approved"):
        run_campaign_send(conn, campaign_id, "live", approved=True)
    conn.close()


def test_live_send_dry_run_and_duplicate_guard(taxops_db_path, monkeypatch):
    import config as cfg
    from db import get_connection
    from email_send import approve_campaign, run_campaign_send

    monkeypatch.setattr(cfg, "EMAIL_CAMPAIGN_ALLOW_SMTP", False)
    monkeypatch.setattr(cfg, "SMTP_DRY_RUN", True)
    monkeypatch.setattr(cfg, "EMAIL_CAMPAIGN_SEND_DELAY_SEC", 0)

    conn = get_connection(taxops_db_path)
    _, campaign_id, _ = _seed_template_and_campaign(conn, 9103)

    run_campaign_send(conn, campaign_id, "test", test_address="staff@example.com")
    approve_campaign(conn, campaign_id)

    with patch("smtplib.SMTP") as mock_smtp:
        stats1 = run_campaign_send(conn, campaign_id, "live", approved=True)
        stats2 = run_campaign_send(conn, campaign_id, "live", approved=True)
        mock_smtp.assert_not_called()

    assert stats1.dry_run is True
    assert stats1.sent == 1
    assert stats2.skipped_existing == 1
    assert stats2.sent == 0
    conn.close()


def test_api_live_send_blocked_without_approve_confirmed(client_logged_in, taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    _, campaign_id, _ = _seed_template_and_campaign(conn, 9104)
    conn.close()

    resp = client_logged_in.post(
        f"/api/email-campaigns/{campaign_id}/send",
        json={},
    )
    assert resp.status_code == 400


def test_preparer_blocked_from_campaign_admin(client, taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT OR IGNORE INTO auth_users
          (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, 'preparer', 1, '2025-01-01T00:00:00Z')
        """,
        ("prep_send", generate_password_hash("testpass"), "Prep"),
    )
    conn.commit()
    conn.close()

    client.post(
        "/login",
        data={"username": "prep_send", "password": "testpass"},
        follow_redirects=True,
    )
    assert client.get("/admin/email-campaigns").status_code == 403


def test_smtp_connects_only_when_both_gates_open(monkeypatch):
    import config as cfg
    from email_send import deliver_email

    monkeypatch.setattr(cfg, "EMAIL_CAMPAIGN_ALLOW_SMTP", True)
    monkeypatch.setattr(cfg, "SMTP_DRY_RUN", False)
    monkeypatch.setattr(cfg, "SMTP_HOST", "smtp.gmail.com")
    monkeypatch.setattr(cfg, "SMTP_PORT", 587)
    monkeypatch.setattr(cfg, "SMTP_USER", "u@example.com")
    monkeypatch.setattr(cfg, "SMTP_PASS", "secret")
    monkeypatch.setattr(cfg, "SMTP_FROM", "u@example.com")
    monkeypatch.setattr(cfg, "SMTP_USE_TLS", True)

    mock_instance = MagicMock()
    with patch("smtplib.SMTP") as mock_smtp:
        mock_smtp.return_value.__enter__.return_value = mock_instance
        result = deliver_email("dest@example.com", "S", "<p>x</p>", "x")
        mock_smtp.assert_called_once()
        mock_instance.starttls.assert_called()
        mock_instance.login.assert_called()
        mock_instance.sendmail.assert_called()
    assert result.ok and not result.dry_run
