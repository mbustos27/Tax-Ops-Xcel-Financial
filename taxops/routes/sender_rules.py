"""Phase 2.1 (email system revamp): minimal Admin-gated sender rules UI.

email_sender_rules holds staff allow/block decisions that mail_watcher's
suppression logic consults every poll cycle (see _load_sender_rules /
_suppression_decision in mail_watcher.py). Before this blueprint existed the
table could be read but never written — the old rules UI (routes/email_review.py)
was deleted along with the 6-layer classifier, leaving staff no way to change
suppression behavior. This is a deliberately minimal list/add/remove page,
Admin only.

Routes:
  GET  /admin/sender-rules                    — page
  POST /api/admin/sender-rules                — create a rule
  POST /api/admin/sender-rules/<id>/delete    — remove a rule
"""
from __future__ import annotations

import logging

from flask import Blueprint, jsonify, render_template, request, session

from auth import login_required, role_required
from db import get_connection
from utils import now

logger = logging.getLogger(__name__)

sender_rules_bp = Blueprint("sender_rules", __name__)

_ALLOWED_SCOPES = frozenset({"domain", "address"})
_ALLOWED_ACTIONS = frozenset({"allow", "block"})


def _base_domain(value: str) -> str:
    """Return the last two dot-separated labels of a domain string."""
    parts = value.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else value


@sender_rules_bp.route("/admin/sender-rules")
@login_required
@role_required("admin")
def sender_rules_admin():
    from app import base_ctx  # lazy import to avoid circular import
    ctx = base_ctx()
    ctx["active_page"] = "sender_rules"
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, domain, rule_scope, action, note, created_by, created_at
            FROM email_sender_rules
            ORDER BY created_at DESC
            """
        ).fetchall()
        ctx["sender_rules"] = [dict(r) for r in rows]
    finally:
        conn.close()
    return render_template("sender_rules_admin.html", **ctx)


@sender_rules_bp.post("/api/admin/sender-rules")
@login_required
@role_required("admin")
def api_create_sender_rule():
    from config import PERSONAL_EMAIL_DOMAINS

    data = request.get_json(silent=True) or {}
    value = (data.get("domain") or "").strip().lower()
    rule_scope = (data.get("rule_scope") or "domain").strip().lower()
    action = (data.get("action") or "block").strip().lower()
    note = (data.get("note") or "").strip() or None

    if not value:
        return jsonify({"error": "domain (or address) is required"}), 400
    if rule_scope not in _ALLOWED_SCOPES:
        return jsonify({"error": f"rule_scope must be one of {sorted(_ALLOWED_SCOPES)}"}), 400
    if action not in _ALLOWED_ACTIONS:
        return jsonify({"error": f"action must be one of {sorted(_ALLOWED_ACTIONS)}"}), 400

    if rule_scope == "address" and "@" not in value:
        return jsonify({"error": "address-scoped rules require a full email address"}), 400
    if rule_scope == "domain" and "@" in value:
        return jsonify({"error": "domain-scoped rules must not contain '@' — use an address-scoped rule instead"}), 400

    # Hard invariant (Phase 2.1 spec): personal email domains (gmail.com, etc.)
    # can never be blocked at the domain level — only a specific address on
    # that domain may be blocked. This mirrors the defensive check mail_watcher
    # also applies at suppression time.
    if rule_scope == "domain" and action == "block" and _base_domain(value) in PERSONAL_EMAIL_DOMAINS:
        return jsonify({
            "error": "Personal email domains can only be blocked at the address level, not the domain level."
        }), 400

    conn = get_connection()
    try:
        existing = conn.execute(
            "SELECT id FROM email_sender_rules WHERE domain = ?", (value,)
        ).fetchone()
        if existing:
            return jsonify({"error": "A rule for this domain/address already exists"}), 409

        cur = conn.execute(
            """
            INSERT INTO email_sender_rules
                (domain, rule_scope, action, note, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (value, rule_scope, action, note, session.get("username"), now()),
        )
        conn.commit()
        logger.info(
            "Admin created sender rule id=%s value=%s scope=%s action=%s by=%s",
            cur.lastrowid, value, rule_scope, action, session.get("username"),
        )
        return jsonify({
            "success": True,
            "rule": {
                "id": cur.lastrowid,
                "domain": value,
                "rule_scope": rule_scope,
                "action": action,
                "note": note,
            },
        })
    except Exception as exc:
        conn.rollback()
        logger.error("api_create_sender_rule failed: %s", exc)
        return jsonify({"error": "Could not create rule"}), 500
    finally:
        conn.close()


@sender_rules_bp.post("/api/admin/sender-rules/<int:rule_id>/delete")
@login_required
@role_required("admin")
def api_delete_sender_rule(rule_id: int):
    conn = get_connection()
    try:
        row = conn.execute("SELECT id FROM email_sender_rules WHERE id = ?", (rule_id,)).fetchone()
        if not row:
            return jsonify({"error": "Rule not found"}), 404
        conn.execute("DELETE FROM email_sender_rules WHERE id = ?", (rule_id,))
        conn.commit()
        logger.info("Admin deleted sender rule id=%s by=%s", rule_id, session.get("username"))
        return jsonify({"success": True})
    except Exception as exc:
        conn.rollback()
        logger.error("api_delete_sender_rule failed: %s", exc)
        return jsonify({"error": "Could not delete rule"}), 500
    finally:
        conn.close()
