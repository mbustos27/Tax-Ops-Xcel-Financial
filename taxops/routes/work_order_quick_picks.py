"""WO-6: Admin-gated management screen for Work Order "quick picks" — the
standardized service list (like a QuickBooks Products/Services list) that
staff can select in the Work Order form instead of hand-typing a description
and fee every time (see routes/work_orders.py's _active_quick_picks and
templates/work_order_form.html's quick-pick <select>).

A quick pick has one base label/fee plus zero-or-more *extra fee
components* (work_order_quick_pick_components) — e.g. "Statement of
Information" ($125) can carry an extra "Filing Fee" ($25) component, so
selecting it in the Work Order form adds both lines at once instead of
staff typing a second row by hand. Components are unbounded (not just one),
so an "account" can be built up out of as many fee lines as it needs.

Soft-deactivate (active=0) is used instead of hard delete, matching the
staff-accounts pattern (routes/users.py) — a quick pick that's already been
used on past work orders stays intact there (work_order_items only snapshot
description/fee, no FK), it just stops showing up as a selectable option.

Routes:
  GET  /admin/work-order-quick-picks                         — page
  POST /api/admin/work-order-quick-picks                     — create
  POST /api/admin/work-order-quick-picks/<id>                — update
  POST /api/admin/work-order-quick-picks/<id>/deactivate      — hide from form
  POST /api/admin/work-order-quick-picks/<id>/reactivate      — show again
"""
from __future__ import annotations

import logging
import sqlite3

from flask import Blueprint, jsonify, render_template, request

from auth import login_required, role_required
from db import get_connection

logger = logging.getLogger(__name__)

wo_quick_picks_bp = Blueprint("wo_quick_picks", __name__)


def _parse_components(data) -> list[dict]:
    """Components posted as a JSON list: [{"label": "...", "fee": 25.0}, ...]."""
    raw = data.get("components")
    if not isinstance(raw, list):
        return []
    out = []
    for i, comp in enumerate(raw):
        if not isinstance(comp, dict):
            continue
        label = str(comp.get("label") or "").strip()
        if not label:
            continue
        try:
            fee = float(comp.get("fee") or 0)
        except (TypeError, ValueError):
            fee = 0.0
        out.append({"label": label, "fee": max(0.0, fee), "sort_order": i})
    return out


def _fetch_all_with_components(conn: sqlite3.Connection) -> list[dict]:
    picks = conn.execute(
        "SELECT id, label, default_fee, active, sort_order FROM work_order_quick_picks "
        "ORDER BY sort_order, id"
    ).fetchall()
    components = conn.execute(
        "SELECT quick_pick_id, id, label, fee, sort_order FROM work_order_quick_pick_components "
        "ORDER BY quick_pick_id, sort_order, id"
    ).fetchall()
    by_pick: dict[int, list[dict]] = {}
    for c in components:
        by_pick.setdefault(c["quick_pick_id"], []).append(dict(c))
    return [{**dict(p), "components": by_pick.get(p["id"], [])} for p in picks]


@wo_quick_picks_bp.route("/admin/work-order-quick-picks")
@login_required
@role_required("admin")
def quick_picks_admin():
    from app import base_ctx

    conn = get_connection()
    try:
        quick_picks = _fetch_all_with_components(conn)
    finally:
        conn.close()

    ctx = base_ctx()
    ctx.update(active_page="wo_quick_picks", quick_picks=quick_picks)
    return render_template("work_order_quick_picks_admin.html", **ctx)


@wo_quick_picks_bp.post("/api/admin/work-order-quick-picks")
@login_required
@role_required("admin")
def api_create_quick_pick():
    data = request.get_json(silent=True) or {}
    label = (data.get("label") or "").strip()
    try:
        default_fee = float(data.get("default_fee") or 0)
    except (TypeError, ValueError):
        return jsonify({"error": "default_fee must be a number"}), 400

    if not label:
        return jsonify({"error": "label is required"}), 400
    if default_fee < 0:
        return jsonify({"error": "default_fee must be zero or greater"}), 400

    components = _parse_components(data)
    if any(c["fee"] < 0 for c in components):
        return jsonify({"error": "component fees must be zero or greater"}), 400

    conn = get_connection()
    try:
        next_sort = conn.execute(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 AS n FROM work_order_quick_picks"
        ).fetchone()["n"]
        cur = conn.execute(
            "INSERT INTO work_order_quick_picks (label, default_fee, active, sort_order) VALUES (?, ?, 1, ?)",
            (label, default_fee, next_sort),
        )
        pick_id = cur.lastrowid
        for comp in components:
            conn.execute(
                "INSERT INTO work_order_quick_pick_components (quick_pick_id, label, fee, sort_order) "
                "VALUES (?, ?, ?, ?)",
                (pick_id, comp["label"], comp["fee"], comp["sort_order"]),
            )
        conn.commit()
        logger.info("Admin created work order quick pick id=%s label=%s", pick_id, label)
        return jsonify({"success": True, "id": pick_id})
    except Exception as exc:
        conn.rollback()
        logger.error("api_create_quick_pick failed: %s", exc)
        return jsonify({"error": "Could not create quick pick"}), 500
    finally:
        conn.close()


@wo_quick_picks_bp.post("/api/admin/work-order-quick-picks/<int:pick_id>")
@login_required
@role_required("admin")
def api_update_quick_pick(pick_id: int):
    data = request.get_json(silent=True) or {}
    label = (data.get("label") or "").strip()
    try:
        default_fee = float(data.get("default_fee") or 0)
    except (TypeError, ValueError):
        return jsonify({"error": "default_fee must be a number"}), 400

    if not label:
        return jsonify({"error": "label is required"}), 400
    if default_fee < 0:
        return jsonify({"error": "default_fee must be zero or greater"}), 400

    components = _parse_components(data)
    if any(c["fee"] < 0 for c in components):
        return jsonify({"error": "component fees must be zero or greater"}), 400

    conn = get_connection()
    try:
        existing = conn.execute("SELECT id FROM work_order_quick_picks WHERE id=?", (pick_id,)).fetchone()
        if not existing:
            return jsonify({"error": "Quick pick not found"}), 404

        conn.execute(
            "UPDATE work_order_quick_picks SET label=?, default_fee=? WHERE id=?",
            (label, default_fee, pick_id),
        )
        # Full replace, same pattern as work_order_items on a WO edit.
        conn.execute("DELETE FROM work_order_quick_pick_components WHERE quick_pick_id=?", (pick_id,))
        for comp in components:
            conn.execute(
                "INSERT INTO work_order_quick_pick_components (quick_pick_id, label, fee, sort_order) "
                "VALUES (?, ?, ?, ?)",
                (pick_id, comp["label"], comp["fee"], comp["sort_order"]),
            )
        conn.commit()
        logger.info("Admin updated work order quick pick id=%s label=%s", pick_id, label)
        return jsonify({"success": True})
    except Exception as exc:
        conn.rollback()
        logger.error("api_update_quick_pick failed: %s", exc)
        return jsonify({"error": "Could not update quick pick"}), 500
    finally:
        conn.close()


def _set_active(pick_id: int, active: int):
    conn = get_connection()
    try:
        row = conn.execute("SELECT id FROM work_order_quick_picks WHERE id=?", (pick_id,)).fetchone()
        if not row:
            return jsonify({"error": "Quick pick not found"}), 404
        conn.execute("UPDATE work_order_quick_picks SET active=? WHERE id=?", (active, pick_id))
        conn.commit()
        return jsonify({"success": True})
    except Exception as exc:
        conn.rollback()
        logger.error("_set_active failed: %s", exc)
        return jsonify({"error": "Could not update quick pick"}), 500
    finally:
        conn.close()


@wo_quick_picks_bp.post("/api/admin/work-order-quick-picks/<int:pick_id>/deactivate")
@login_required
@role_required("admin")
def api_deactivate_quick_pick(pick_id: int):
    return _set_active(pick_id, 0)


@wo_quick_picks_bp.post("/api/admin/work-order-quick-picks/<int:pick_id>/reactivate")
@login_required
@role_required("admin")
def api_reactivate_quick_pick(pick_id: int):
    return _set_active(pick_id, 1)
