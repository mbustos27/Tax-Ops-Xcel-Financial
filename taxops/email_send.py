"""Outbound SMTP delivery for mass-email campaigns (Prompt L).

Safety layers (all must pass for a real socket connection):
  1. ``EMAIL_CAMPAIGN_ALLOW_SMTP`` — default false; ops must opt in.
  2. ``SMTP_DRY_RUN`` — default true; logs/records only.
  3. Campaign workflow — test-send before approve; live requires approved status.
"""

from __future__ import annotations

import logging
import re
import smtplib
import time
from dataclasses import dataclass, field
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Optional

import config

_log = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@dataclass
class SmtpDeliveryResult:
    ok: bool
    dry_run: bool = False
    error: Optional[str] = None


@dataclass
class CampaignSendStats:
    mode: str
    attempted: int = 0
    sent: int = 0
    skipped_existing: int = 0
    failed: int = 0
    dry_run: bool = True
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "attempted": self.attempted,
            "sent": self.sent,
            "skipped_existing": self.skipped_existing,
            "failed": self.failed,
            "dry_run": self.dry_run,
            "error_count": len(self.errors),
        }


def smtp_will_connect() -> bool:
    """True only when both safety gates allow a real SMTP connection."""
    return bool(config.EMAIL_CAMPAIGN_ALLOW_SMTP and not config.SMTP_DRY_RUN)


def smtp_is_configured() -> bool:
    host = (config.SMTP_HOST or "").strip()
    user = (config.SMTP_USER or config.IMAP_USER or "").strip()
    password = (config.SMTP_PASS or config.IMAP_PASS or "").strip()
    return bool(host and user and password)


def _smtp_from_address() -> str:
    return (config.SMTP_FROM or config.SMTP_USER or config.IMAP_USER or "").strip()


def _validate_email(addr: str) -> str:
    a = (addr or "").strip()
    if not a or not _EMAIL_RE.match(a):
        raise ValueError("invalid email address")
    return a


def deliver_email(
    to_address: str,
    subject: str,
    body_html: str,
    body_text: Optional[str] = None,
    reply_to: Optional[str] = None,
) -> SmtpDeliveryResult:
    """Send one message. Returns dry_run result without opening SMTP when gated."""
    to_addr = _validate_email(to_address)
    if not smtp_will_connect():
        _log.info(
            "email_send dry_run to=%s subject_len=%d allow_smtp=%s smtp_dry_run=%s",
            "***",
            len(subject or ""),
            config.EMAIL_CAMPAIGN_ALLOW_SMTP,
            config.SMTP_DRY_RUN,
        )
        return SmtpDeliveryResult(ok=True, dry_run=True)

    if not smtp_is_configured():
        return SmtpDeliveryResult(ok=False, dry_run=False, error="smtp_not_configured")

    host = (config.SMTP_HOST or "").strip()
    port = int(config.SMTP_PORT or 587)
    user = (config.SMTP_USER or config.IMAP_USER or "").strip()
    password = (config.SMTP_PASS or config.IMAP_PASS or "").strip()
    from_addr = _smtp_from_address()
    if not from_addr:
        return SmtpDeliveryResult(ok=False, dry_run=False, error="smtp_from_missing")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    if reply_to:
        msg["Reply-To"] = reply_to.strip()

    if body_text:
        msg.attach(MIMEText(body_text, "plain", "utf-8"))
    msg.attach(MIMEText(body_html or "", "html", "utf-8"))

    try:
        if config.SMTP_USE_TLS:
            with smtplib.SMTP(host, port, timeout=60) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.ehlo()
                smtp.login(user, password)
                smtp.sendmail(from_addr, [to_addr], msg.as_string())
        else:
            with smtplib.SMTP_SSL(host, port, timeout=60) as smtp:
                smtp.login(user, password)
                smtp.sendmail(from_addr, [to_addr], msg.as_string())
    except Exception as exc:
        _log.warning("email_send failed: %s", type(exc).__name__)
        return SmtpDeliveryResult(ok=False, dry_run=False, error=type(exc).__name__)

    return SmtpDeliveryResult(ok=True, dry_run=False)


def run_campaign_send(
    conn,
    campaign_id: int,
    mode: str,
    *,
    test_address: Optional[str] = None,
    approved: bool = False,
    actor: Optional[str] = None,
) -> CampaignSendStats:
    """Execute test or live campaign send. Never bypasses dry-run gates."""
    from email_campaigns import (
        load_email_template,
        render_email_template,
        resolve_audience,
    )
    from utils import now

    mode_norm = (mode or "").strip().lower()
    if mode_norm not in ("test", "live"):
        raise ValueError("mode must be 'test' or 'live'")

    campaign = conn.execute(
        "SELECT id, template_id, audience_definition, status FROM email_campaigns WHERE id = ?",
        (int(campaign_id),),
    ).fetchone()
    if not campaign:
        raise ValueError("campaign not found")

    status = (campaign["status"] or "draft").strip().lower()
    if mode_norm == "live":
        if not approved:
            raise ValueError("live send requires explicit approve step")
        if status not in ("approved", "sending", "sent", "sent_with_errors"):
            raise ValueError("campaign must be approved before live send")

    if mode_norm == "test":
        test_address = _validate_email(test_address or "")
        if status not in ("draft", "test_sent", "approved"):
            pass  # allow re-test from draft

    template = load_email_template(conn, int(campaign["template_id"]))
    if not template:
        raise ValueError("template not found or inactive")

    try:
        audience = __import__("json").loads(campaign["audience_definition"])
    except (TypeError, ValueError, __import__("json").JSONDecodeError):
        audience = {}

    recipients, _ = resolve_audience(conn, audience)
    stats = CampaignSendStats(mode=mode_norm, dry_run=not smtp_will_connect())
    ts = now()

    if mode_norm == "test":
        if not recipients:
            raise ValueError("no recipients resolved for test preview")
        rec = recipients[0]
        rendered = render_email_template(template, rec.template_variables)
        test_subject = f"[TaxOps TEST] {rendered.subject}"
        test_html = (
            "<p><strong>Test send — not delivered to clients.</strong></p>"
            f"<p>Log # {rec.template_variables.get('log_number', '')} — "
            f"client_id {rec.client_id}</p>"
            f"{rendered.body_html}"
        )
        test_text = (
            "Test send — not delivered to clients.\n\n"
            + (rendered.body_text or "")
        )
        stats.attempted = 1
        result = deliver_email(
            test_address,
            test_subject,
            test_html,
            test_text,
            reply_to=rec.template_variables.get("reply_to"),
        )
        if result.ok:
            stats.sent = 1
            existing_test = conn.execute(
                "SELECT id FROM client_email_sends "
                "WHERE campaign_id = ? AND client_id = ?",
                (campaign_id, rec.client_id),
            ).fetchone()
            row_status = "test_sent" if not result.dry_run else "test_dry_run"
            if existing_test:
                conn.execute(
                    """
                    UPDATE client_email_sends SET
                      return_id = ?, recipient_email = ?, sent_at = ?,
                      status = ?, error_detail = ?, dedupe_key = ?
                    WHERE id = ?
                    """,
                    (
                        rec.return_id,
                        test_address,
                        ts,
                        row_status,
                        None if result.ok else result.error,
                        f"test:{campaign_id}",
                        existing_test["id"],
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO client_email_sends (
                      campaign_id, client_id, return_id, recipient_email,
                      sent_at, status, error_detail, dedupe_key
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        campaign_id,
                        rec.client_id,
                        rec.return_id,
                        test_address,
                        ts,
                        row_status,
                        None if result.ok else result.error,
                        f"test:{campaign_id}",
                    ),
                )
            conn.execute(
                "UPDATE email_campaigns SET status = ? WHERE id = ?",
                ("test_sent", campaign_id),
            )
        else:
            stats.failed = 1
            if result.error:
                stats.errors.append(result.error)
        conn.commit()
        return stats

    # Live send
    conn.execute(
        "UPDATE email_campaigns SET status = 'sending' WHERE id = ?",
        (campaign_id,),
    )
    conn.commit()

    delay = max(0.0, float(config.EMAIL_CAMPAIGN_SEND_DELAY_SEC or 1.0))

    for rec in recipients:
        existing = conn.execute(
            "SELECT id, status FROM client_email_sends "
            "WHERE campaign_id = ? AND client_id = ?",
            (campaign_id, rec.client_id),
        ).fetchone()
        if existing and existing["status"] in ("sent", "dry_run"):
            stats.skipped_existing += 1
            continue

        stats.attempted += 1
        rendered = render_email_template(template, rec.template_variables)
        result = deliver_email(
            rec.resolved_email,
            rendered.subject,
            rendered.body_html,
            rendered.body_text,
            reply_to=rec.template_variables.get("reply_to"),
        )
        row_status = "dry_run" if result.dry_run else ("sent" if result.ok else "failed")
        err = result.error if not result.ok else None
        dedupe = f"live:{campaign_id}:{rec.client_id}"

        if existing:
            conn.execute(
                """
                UPDATE client_email_sends SET
                  return_id = ?, recipient_email = ?, sent_at = ?,
                  status = ?, error_detail = ?, dedupe_key = ?
                WHERE id = ?
                """,
                (
                    rec.return_id,
                    rec.resolved_email,
                    ts,
                    row_status,
                    err,
                    dedupe,
                    existing["id"],
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO client_email_sends (
                  campaign_id, client_id, return_id, recipient_email,
                  sent_at, status, error_detail, dedupe_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    campaign_id,
                    rec.client_id,
                    rec.return_id,
                    rec.resolved_email,
                    ts,
                    row_status,
                    err,
                    dedupe,
                ),
            )

        if result.ok:
            stats.sent += 1
        else:
            stats.failed += 1
            if err:
                stats.errors.append(err)

        conn.commit()
        if delay and stats.attempted < len(recipients):
            time.sleep(delay)

    final_status = "sent" if stats.failed == 0 else "sent_with_errors"
    conn.execute(
        "UPDATE email_campaigns SET status = ?, sent_at = ? WHERE id = ?",
        (final_status, ts, campaign_id),
    )
    conn.commit()
    return stats


def approve_campaign(conn, campaign_id: int, actor: Optional[str] = None) -> None:
    """Mark campaign approved after staff reviewed test send."""
    row = conn.execute(
        "SELECT status FROM email_campaigns WHERE id = ?",
        (int(campaign_id),),
    ).fetchone()
    if not row:
        raise ValueError("campaign not found")
    status = (row["status"] or "").strip().lower()
    if status not in ("test_sent", "test_dry_run"):
        raise ValueError("complete a test send before approving")
    conn.execute(
        "UPDATE email_campaigns SET status = 'approved' WHERE id = ?",
        (campaign_id,),
    )
    conn.commit()


def create_campaign(
    conn,
    template_id: int,
    audience_definition: dict,
    created_by: Optional[str] = None,
) -> int:
    from email_campaigns import sanitize_audience_definition
    from utils import now

    audience = sanitize_audience_definition(audience_definition)
    if not load_template_exists(conn, template_id):
        raise ValueError("template not found or inactive")

    category = (audience.get("campaign_category") or "all").strip() or "all"
    ts = now()
    conn.execute(
        """
        INSERT INTO email_campaigns (
          template_id, audience_definition, status, category, created_by, created_at
        ) VALUES (?, ?, 'draft', ?, ?, ?)
        """,
        (
            int(template_id),
            __import__("json").dumps(audience),
            category,
            created_by,
            ts,
        ),
    )
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    return int(cid)


def load_template_exists(conn, template_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM email_templates WHERE id = ? AND is_active = 1",
        (int(template_id),),
    ).fetchone()
    return row is not None


def get_send_safety_status() -> dict[str, Any]:
    """Expose gate state to admin UI (no secrets)."""
    return {
        "smtp_dry_run": config.SMTP_DRY_RUN,
        "allow_smtp": config.EMAIL_CAMPAIGN_ALLOW_SMTP,
        "will_connect": smtp_will_connect(),
        "smtp_configured": smtp_is_configured(),
        "send_delay_sec": config.EMAIL_CAMPAIGN_SEND_DELAY_SEC,
    }
