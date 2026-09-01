"""Mass-email campaign preview, test-send, approve, and live send (Prompt L)."""

from __future__ import annotations

import json
import logging

from flask import Blueprint, jsonify, render_template, request, session

from auth import login_required, permission_required
from db import get_connection
from email_campaigns import preview_campaign, campaign_categories_for_ui, list_tax_deadlines
from email_send import (
    approve_campaign,
    create_campaign,
    get_send_safety_status,
    run_campaign_send,
)

logger = logging.getLogger(__name__)

email_campaigns_bp = Blueprint("email_campaigns", __name__)


def _actor() -> str | None:
    return session.get("username") or session.get("user")


@email_campaigns_bp.route("/admin/email-campaigns")
@login_required
@permission_required("can_send_client_email")
def email_campaigns_admin():
    from app import base_ctx
    from db import get_active_intake_tax_year

    conn = get_connection()
    try:
        active_intake_tax_year = get_active_intake_tax_year(conn)
        templates = conn.execute(
            """
            SELECT id, key, subject, category, is_active
            FROM email_templates
            WHERE is_active = 1
            ORDER BY category, key
            """
        ).fetchall()
        campaigns = conn.execute(
            """
            SELECT c.id, c.template_id, c.status, c.category, c.created_at, c.sent_at,
                   t.key AS template_key
            FROM email_campaigns c
            JOIN email_templates t ON t.id = c.template_id
            ORDER BY c.id DESC
            LIMIT 50
            """
        ).fetchall()
    finally:
        conn.close()

    ctx = base_ctx()
    ctx.update(
        active_page="email_campaigns",
        email_templates=[dict(r) for r in templates],
        recent_campaigns=[dict(r) for r in campaigns],
        campaign_categories=campaign_categories_for_ui(),
        send_safety=get_send_safety_status(),
        campaign_default_tax_year=active_intake_tax_year,
    )
    return render_template("email_campaigns_admin.html", **ctx)


@email_campaigns_bp.route("/api/email-campaigns/categories")
@login_required
@permission_required("can_send_client_email")
def api_email_campaign_categories():
    return jsonify({"categories": campaign_categories_for_ui()})


@email_campaigns_bp.route("/api/email-campaigns/deadlines")
@login_required
@permission_required("can_send_client_email")
def api_email_campaign_deadlines():
    tax_year = request.args.get("tax_year")
    conn = get_connection()
    try:
        ty = int(tax_year) if tax_year else None
        deadlines = list_tax_deadlines(conn, ty)
    finally:
        conn.close()
    return jsonify({"deadlines": deadlines})


@email_campaigns_bp.route("/api/email-campaigns/safety")
@login_required
@permission_required("can_send_client_email")
def api_email_campaign_safety():
    return jsonify(get_send_safety_status())


@email_campaigns_bp.route("/api/email-campaigns/preview", methods=["POST"])
@login_required
@permission_required("can_send_client_email")
def api_email_campaign_preview():
    data = request.get_json(silent=True) or {}
    audience = data.get("audience_definition") or data.get("audience") or {}
    template_id = data.get("template_id")
    try:
        sample_limit = int(data.get("sample_limit", 5))
    except (TypeError, ValueError):
        sample_limit = 5

    tid: int | None = None
    if template_id is not None:
        try:
            tid = int(template_id)
        except (TypeError, ValueError):
            return jsonify({"error": "template_id must be an integer"}), 400

    conn = get_connection()
    try:
        payload = preview_campaign(
            conn,
            audience,
            template_id=tid,
            sample_limit=sample_limit,
        )
        payload["send_safety"] = get_send_safety_status()
    finally:
        conn.close()

    logger.info(
        "email_campaign_preview stats=%s template_id=%s",
        payload.get("stats"),
        template_id,
    )
    return jsonify(payload)


@email_campaigns_bp.route("/api/email-campaigns/create", methods=["POST"])
@login_required
@permission_required("can_send_client_email")
def api_email_campaign_create():
    data = request.get_json(silent=True) or {}
    try:
        template_id = int(data.get("template_id"))
    except (TypeError, ValueError):
        return jsonify({"error": "template_id required"}), 400
    audience = data.get("audience_definition") or data.get("audience") or {}

    conn = get_connection()
    try:
        campaign_id = create_campaign(
            conn,
            template_id,
            audience,
            created_by=_actor(),
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    finally:
        conn.close()

    return jsonify({"campaign_id": campaign_id, "status": "draft"})


@email_campaigns_bp.route("/api/email-campaigns/<int:campaign_id>/test-send", methods=["POST"])
@login_required
@permission_required("can_send_client_email")
def api_email_campaign_test_send(campaign_id: int):
    data = request.get_json(silent=True) or {}
    test_address = (data.get("test_address") or "").strip()
    if not test_address:
        return jsonify({"error": "test_address required"}), 400

    conn = get_connection()
    try:
        stats = run_campaign_send(
            conn,
            campaign_id,
            "test",
            test_address=test_address,
            actor=_actor(),
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    finally:
        conn.close()

    logger.info(
        "email_campaign_test_send campaign_id=%s stats=%s",
        campaign_id,
        stats.as_dict(),
    )
    return jsonify({"stats": stats.as_dict(), "send_safety": get_send_safety_status()})


@email_campaigns_bp.route("/api/email-campaigns/<int:campaign_id>/approve", methods=["POST"])
@login_required
@permission_required("can_send_client_email")
def api_email_campaign_approve(campaign_id: int):
    conn = get_connection()
    try:
        approve_campaign(conn, campaign_id, actor=_actor())
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    finally:
        conn.close()
    return jsonify({"campaign_id": campaign_id, "status": "approved"})


@email_campaigns_bp.route("/api/email-campaigns/<int:campaign_id>/send", methods=["POST"])
@login_required
@permission_required("can_send_client_email")
def api_email_campaign_live_send(campaign_id: int):
    """Live send — requires prior approve; still dry-run when SMTP gates are off."""
    data = request.get_json(silent=True) or {}
    if not data.get("approve_confirmed"):
        return jsonify({"error": "approve_confirmed required for live send"}), 400

    conn = get_connection()
    try:
        stats = run_campaign_send(
            conn,
            campaign_id,
            "live",
            approved=True,
            actor=_actor(),
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    finally:
        conn.close()

    logger.info(
        "email_campaign_live_send campaign_id=%s stats=%s",
        campaign_id,
        stats.as_dict(),
    )
    return jsonify({"stats": stats.as_dict(), "send_safety": get_send_safety_status()})


@email_campaigns_bp.route("/api/email-campaigns/<int:campaign_id>")
@login_required
@permission_required("can_send_client_email")
def api_email_campaign_detail(campaign_id: int):
    conn = get_connection()
    try:
        camp = conn.execute(
            """
            SELECT c.*, t.key AS template_key, t.subject AS template_subject
            FROM email_campaigns c
            JOIN email_templates t ON t.id = c.template_id
            WHERE c.id = ?
            """,
            (campaign_id,),
        ).fetchone()
        if not camp:
            return jsonify({"error": "not found"}), 404
        sends = conn.execute(
            """
            SELECT client_id, return_id, status, sent_at, error_detail
            FROM client_email_sends
            WHERE campaign_id = ?
            ORDER BY id
            """,
            (campaign_id,),
        ).fetchall()
        out = dict(camp)
        try:
            out["audience_definition"] = json.loads(camp["audience_definition"])
        except (json.JSONDecodeError, TypeError, ValueError):
            out["audience_definition"] = {}
        out["sends"] = [dict(r) for r in sends]
        out["send_safety"] = get_send_safety_status()
    finally:
        conn.close()
    return jsonify(out)
