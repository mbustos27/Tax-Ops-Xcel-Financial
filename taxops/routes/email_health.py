"""Phase 3.3/3.4 — Email System Health panel, Admin only.

RBAC (hard invariant, Phase 3 amended prompt #5): everything in this
blueprint is gated to the admin role, not can_use_email_tools — Preparers
and Reception get 403. This is distinct from the can_use_email_tools-gated
triage view in app.py (/email-inbox, Phase 3.1/3.2).

This module reads only DB tables (watcher_heartbeat, email_processing_log,
extraction_queue) — it never imports mail_watcher internals, keeping the
UI decoupled from watcher implementation details.

Routes:
  GET  /admin/email-health                                   — page
  GET  /api/admin/email-health/status                        — JSON snapshot
  POST /api/admin/email-health/dead-letters/<eq_id>/requeue  — Phase 3.4
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from flask import Blueprint, jsonify, render_template

from auth import login_required, role_required
from db import get_connection

logger = logging.getLogger(__name__)

email_health_bp = Blueprint("email_health", __name__)


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def _heartbeat_snapshot(conn) -> dict:
    from config import IMAP_POLL_INTERVAL

    row = conn.execute(
        "SELECT last_poll_at, last_poll_outcome_counts, last_error "
        "FROM watcher_heartbeat WHERE id = 1"
    ).fetchone()

    stale_threshold = IMAP_POLL_INTERVAL * 3
    if row is None or not row["last_poll_at"]:
        return {
            "last_poll_at": None,
            "seconds_since_last_poll": None,
            "stale": True,
            "stale_threshold_seconds": stale_threshold,
            "last_poll_outcome_counts": {},
            "last_error": None,
        }

    try:
        last_poll_dt = datetime.fromisoformat(row["last_poll_at"])
        if last_poll_dt.tzinfo is None:
            last_poll_dt = last_poll_dt.replace(tzinfo=timezone.utc)
        seconds_since = (datetime.now(timezone.utc) - last_poll_dt).total_seconds()
    except (ValueError, TypeError):
        seconds_since = None

    try:
        outcome_counts = json.loads(row["last_poll_outcome_counts"] or "{}")
    except (ValueError, TypeError):
        outcome_counts = {}

    return {
        "last_poll_at": row["last_poll_at"],
        "seconds_since_last_poll": seconds_since,
        "stale": seconds_since is None or seconds_since > stale_threshold,
        "stale_threshold_seconds": stale_threshold,
        "last_poll_outcome_counts": outcome_counts,
        "last_error": row["last_error"],
    }


def _outcome_counts_since(conn, cutoff_iso: str) -> dict:
    rows = conn.execute(
        "SELECT outcome, COUNT(*) AS n FROM email_processing_log "
        "WHERE last_attempt_at >= ? GROUP BY outcome",
        (cutoff_iso,),
    ).fetchall()
    return {r["outcome"]: r["n"] for r in rows}


def _suppression_breakdown_since(conn, cutoff_iso: str) -> dict:
    rows = conn.execute(
        "SELECT suppression_reason, COUNT(*) AS n FROM email_processing_log "
        "WHERE last_attempt_at >= ? AND outcome = 'skip' "
        "AND suppression_reason IS NOT NULL "
        "GROUP BY suppression_reason",
        (cutoff_iso,),
    ).fetchall()
    return {r["suppression_reason"]: r["n"] for r in rows}


def _dead_letters(conn) -> list[dict]:
    """Failed extraction_queue rows. Never includes file_path (privacy)."""
    rows = conn.execute(
        """
        SELECT
            eq.id AS eq_id, eq.doc_id, eq.return_id, eq.attempts,
            eq.error_message, eq.created_at, eq.processed_at,
            rd.filename, rd.original_filename,
            r.log_number, r.tax_year, c.last_name, c.first_name
        FROM extraction_queue eq
        JOIN return_documents rd ON eq.doc_id = rd.id AND rd.is_deleted = 0
        JOIN returns r ON eq.return_id = r.id
        JOIN clients c ON c.id = r.client_id
        WHERE eq.status = 'failed'
        ORDER BY eq.processed_at DESC
        LIMIT 200
        """
    ).fetchall()
    return [dict(r) for r in rows]


def _config_snapshot() -> dict:
    from config import (
        IMAP_HOST, IMAP_USER, IMAP_POLL_INTERVAL, IMAP_DRY_RUN,
        EXTRACTOR_VISION_ENABLED, USE_GMAIL_CATEGORIES,
    )
    return {
        "poll_interval_seconds": IMAP_POLL_INTERVAL,
        "dry_run": IMAP_DRY_RUN,
        "vision_enabled": EXTRACTOR_VISION_ENABLED,
        "use_gmail_categories": USE_GMAIL_CATEGORIES,
        # IMAP_USER rendered as configured/not-configured only — never the
        # address itself, and IMAP_PASS is never referenced here at all.
        "imap_configured": bool(IMAP_HOST) and bool(IMAP_USER),
    }


def _build_status(conn) -> dict:
    now = datetime.now(timezone.utc)
    cutoff_24h = _iso(now - timedelta(hours=24))
    cutoff_7d = _iso(now - timedelta(days=7))
    dead_letters = _dead_letters(conn)
    return {
        "heartbeat": _heartbeat_snapshot(conn),
        "outcome_counts": {
            "last_24h": _outcome_counts_since(conn, cutoff_24h),
            "last_7d": _outcome_counts_since(conn, cutoff_7d),
            "suppression_breakdown_24h": _suppression_breakdown_since(conn, cutoff_24h),
            "suppression_breakdown_7d": _suppression_breakdown_since(conn, cutoff_7d),
        },
        "dead_letters": {
            "count": len(dead_letters),
            "items": dead_letters,
        },
        "config": _config_snapshot(),
    }


@email_health_bp.route("/admin/email-health")
@login_required
@role_required("admin")
def email_health_admin():
    from app import base_ctx  # lazy import to avoid circular import
    ctx = base_ctx()
    ctx["active_page"] = "email_health"
    conn = get_connection()
    try:
        ctx["health"] = _build_status(conn)
    finally:
        conn.close()
    return render_template("email_health_admin.html", **ctx)


@email_health_bp.get("/api/admin/email-health/status")
@login_required
@role_required("admin")
def api_email_health_status():
    conn = get_connection()
    try:
        return jsonify(_build_status(conn))
    finally:
        conn.close()


@email_health_bp.post("/api/admin/email-health/dead-letters/<int:eq_id>/requeue")
@login_required
@role_required("admin")
def api_email_health_requeue(eq_id: int):
    """Phase 3.4: reset a dead-letter extraction row to pending and wake the
    extractor. Rejects (400) any row that isn't currently 'failed' — requeue
    is a recovery action, not a generic status editor."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id, doc_id, return_id, status FROM extraction_queue WHERE id = ?",
            (eq_id,),
        ).fetchone()
        if row is None:
            return jsonify({"error": "Dead letter not found"}), 404
        if row["status"] != "failed":
            return jsonify({"error": "Only failed items can be requeued"}), 400

        conn.execute(
            """
            UPDATE extraction_queue
            SET status = 'pending', attempts = 0, error_message = NULL, processed_at = NULL
            WHERE id = ?
            """,
            (eq_id,),
        )
        conn.commit()
        doc_id, return_id = row["doc_id"], row["return_id"]
    finally:
        conn.close()

    # Wake the extractor immediately if importable; otherwise the normal
    # 60s poll picks it up. Never create a second Event / import cycle.
    try:
        from extractor import _notify_extraction_worker
        _notify_extraction_worker()
    except Exception:
        logger.warning("Could not wake extraction worker after requeue eq_id=%s", eq_id)

    return jsonify({"success": True, "doc_id": doc_id, "return_id": return_id})
