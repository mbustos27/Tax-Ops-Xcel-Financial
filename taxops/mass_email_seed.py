"""Seed mass-email templates, tax_deadlines, and draft campaigns (Prompt M)."""

from __future__ import annotations

import copy
import json

from db import get_active_intake_tax_year
from mass_email_templates import (
    ALL_TEMPLATES,
    CAMPAIGN_AUDIENCE_DEADLINE_EARLY,
    CAMPAIGN_AUDIENCE_DEADLINE_FINAL,
    CAMPAIGN_AUDIENCE_EXTENSION_DEADLINE,
    CAMPAIGN_AUDIENCE_MISSING_DOCS,
    CAMPAIGN_AUDIENCE_SLOW_CYCLE,
    CAMPAIGN_AUDIENCE_STALE_PROCESSING,
    DEADLINE_CAMPAIGN_LABELS,
    TAX_DEADLINE_SEEDS,
)
from utils import now


def _upsert_template(conn, spec: dict) -> int:
    ts = now()
    schema_json = json.dumps(spec["variables_schema"])
    row = conn.execute(
        "SELECT id FROM email_templates WHERE key = ?",
        (spec["key"],),
    ).fetchone()
    category = (spec.get("category") or "all").strip() or "all"
    if row:
        conn.execute(
            """
            UPDATE email_templates SET
              subject = ?, body_html = ?, body_text = ?,
              variables_schema = ?, category = ?, is_active = 1, updated_at = ?
            WHERE key = ?
            """,
            (
                spec["subject"],
                spec["body_html"],
                spec["body_text"],
                schema_json,
                category,
                ts,
                spec["key"],
            ),
        )
        return int(row["id"])
    conn.execute(
        """
        INSERT INTO email_templates (
          key, subject, body_html, body_text, variables_schema, category,
          is_active, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (
            spec["key"],
            spec["subject"],
            spec["body_html"],
            spec["body_text"],
            schema_json,
            category,
            ts,
            ts,
        ),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_tax_deadlines(conn) -> int:
    added = 0
    for row in TAX_DEADLINE_SEEDS:
        exists = conn.execute(
            """
            SELECT id FROM tax_deadlines
            WHERE label = ? AND tax_year = ? AND due_date = ?
            """,
            (row["label"], row["tax_year"], row["due_date"]),
        ).fetchone()
        if exists:
            continue
        conn.execute(
            """
            INSERT INTO tax_deadlines (label, applies_to, due_date, tax_year, is_active)
            VALUES (?, ?, ?, ?, 1)
            """,
            (
                row["label"],
                row["applies_to"],
                row["due_date"],
                row["tax_year"],
            ),
        )
        added += 1
    return added


def _deadline_id(conn, label: str, tax_year: int) -> int | None:
    row = conn.execute(
        """
        SELECT id FROM tax_deadlines
        WHERE label = ? AND tax_year = ? AND is_active = 1
        ORDER BY id DESC LIMIT 1
        """,
        (label, tax_year),
    ).fetchone()
    return int(row["id"]) if row else None


def _ensure_draft_campaign(conn, template_id: int, audience: dict) -> int:
    aud_json = json.dumps(audience, sort_keys=True)
    category = (audience.get("campaign_category") or "all").strip() or "all"
    row = conn.execute(
        """
        SELECT id FROM email_campaigns
        WHERE template_id = ? AND audience_definition = ? AND status = 'draft'
        ORDER BY id DESC LIMIT 1
        """,
        (template_id, aud_json),
    ).fetchone()
    if row:
        return int(row["id"])
    ts = now()
    conn.execute(
        """
        INSERT INTO email_campaigns (
          template_id, audience_definition, status, category, created_by, created_at
        ) VALUES (?, ?, 'draft', ?, ?, ?)
        """,
        (template_id, aud_json, category, "mass_email_seed", ts),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_deadline_campaigns(
    conn,
    template_ids: dict[str, int],
    tax_year: int,
) -> dict[str, int]:
    """Draft early + final campaigns for each seeded federal deadline."""
    out: dict[str, int] = {}
    early_tpl = template_ids.get("deadline_reminder_early")
    final_tpl = template_ids.get("deadline_reminder_final")
    if not early_tpl or not final_tpl:
        return out

    for label in DEADLINE_CAMPAIGN_LABELS:
        td_id = _deadline_id(conn, label, tax_year)
        if not td_id:
            continue
        slug = label.lower().replace(" ", "_")
        early_aud = copy.deepcopy(CAMPAIGN_AUDIENCE_DEADLINE_EARLY)
        early_aud["tax_year"] = tax_year
        early_aud["tax_deadline_id"] = td_id
        out[f"deadline_early_{slug}"] = _ensure_draft_campaign(
            conn, early_tpl, early_aud
        )

        final_aud = copy.deepcopy(CAMPAIGN_AUDIENCE_DEADLINE_FINAL)
        final_aud["tax_year"] = tax_year
        final_aud["tax_deadline_id"] = td_id
        out[f"deadline_final_{slug}"] = _ensure_draft_campaign(
            conn, final_tpl, final_aud
        )
    return out


def seed_mass_email_platform(conn, tax_year: int | None = None) -> dict:
    """Insert/update templates, tax_deadlines, draft campaigns. No sends."""
    ty = int(tax_year or get_active_intake_tax_year(conn))
    template_ids: dict[str, int] = {}
    for spec in ALL_TEMPLATES:
        template_ids[spec["key"]] = _upsert_template(conn, spec)

    deadlines_added = _seed_tax_deadlines(conn)

    missing_aud = dict(CAMPAIGN_AUDIENCE_MISSING_DOCS)
    missing_aud["tax_year"] = ty
    ext_aud = dict(CAMPAIGN_AUDIENCE_EXTENSION_DEADLINE)
    ext_aud["tax_year"] = ty
    stale_aud = dict(CAMPAIGN_AUDIENCE_STALE_PROCESSING)
    stale_aud["tax_year"] = ty
    slow_aud = dict(CAMPAIGN_AUDIENCE_SLOW_CYCLE)
    slow_aud["tax_year"] = ty

    campaign_ids: dict[str, int] = {
        "missing_docs": _ensure_draft_campaign(
            conn, template_ids["missing_docs_reminder"], missing_aud
        ),
        "extension_deadline": _ensure_draft_campaign(
            conn, template_ids["extension_deadline_reminder"], ext_aud
        ),
        "stale_processing": _ensure_draft_campaign(
            conn, template_ids["stale_processing_reminder"], stale_aud
        ),
        "slow_cycle": _ensure_draft_campaign(
            conn, template_ids["slow_cycle_reminder"], slow_aud
        ),
    }
    campaign_ids.update(_seed_deadline_campaigns(conn, template_ids, ty))

    conn.commit()
    return {
        "tax_year": ty,
        "template_ids": template_ids,
        "campaign_ids": campaign_ids,
        "tax_deadlines_added": deadlines_added,
        "template_count": len(template_ids),
        "campaign_count": len(campaign_ids),
    }
