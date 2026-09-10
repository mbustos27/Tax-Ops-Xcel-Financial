"""Now Serving — take-a-number kiosk + lobby display + staff modal (reception+).

Public (no login):

  GET  /now-serving/kiosk
  POST /now-serving/kiosk/take
  GET  /now-serving/display        — TV / Raspberry Pi lobby board + EN/ES voice
  GET  /now-serving/api/snapshot
  GET  /now-serving/api/events

Staff (permission can_manage_now_serving = receptionist+):

  GET  /now-serving/board          — redirects to home with modal open
  POST /now-serving/api/call-next
  POST /now-serving/api/transfer
  POST /now-serving/api/reset      — Admin only

Each Call Next opens a fresh audit trail for that ticket
(entity_type=now_serving_ticket). Completions, transfers, and day-reset
append to the same entity so staff can reconstruct what happened.
"""
from __future__ import annotations

import json
import logging
import time

from flask import (
    Blueprint,
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    stream_with_context,
    url_for,
)
from flask_wtf.csrf import generate_csrf

from auth import permission_required, role_required
from now_serving import (
    NowServingError,
    board_snapshot,
    call_next,
    issue_ticket,
    maybe_rebalance_queues,
    reset_day,
    transfer_to_other_window,
    try_print_now_serving_ticket,
)

now_serving_bp = Blueprint("now_serving", __name__)
logger = logging.getLogger(__name__)

NOW_SERVING_PUBLIC_PREFIXES = (
    "/now-serving/kiosk",
    "/now-serving/display",
    "/now-serving/api/snapshot",
    "/now-serving/api/events",
)


def is_now_serving_public_path(path: str) -> bool:
    p = path or ""
    return any(p == pref or p.startswith(pref + "/") for pref in NOW_SERVING_PUBLIC_PREFIXES) or p == "/now-serving/kiosk/take"


def _enqueue_audit(*, action: str, entity_type: str, entity_id, before=None, after=None) -> None:
    """Best-effort audit write — never fail the staff action if audit is down."""
    try:
        from audit_service import _enqueue_write

        _enqueue_write(
            user_id=(session.get("username") or "").strip() or None,
            action=action,
            entity_type=entity_type,
            entity_id=None if entity_id is None else str(entity_id),
            before=before,
            after=after,
            ip_address=request.remote_addr,
            http_status=200,
        )
    except Exception:
        logger.exception(
            "now_serving: failed to enqueue audit action=%s entity=%s/%s",
            action,
            entity_type,
            entity_id,
        )


@now_serving_bp.get("/now-serving/kiosk")
def kiosk_page():
    snap = board_snapshot()
    return render_template(
        "now_serving_kiosk.html",
        csrf_token=generate_csrf(),
        initial_snapshot=snap,
    )


@now_serving_bp.get("/now-serving/display")
def lobby_display():
    """Fullscreen lobby / Raspberry Pi board — no login; voice announces call-next."""
    snap = board_snapshot()
    return render_template(
        "now_serving_display.html",
        initial_snapshot=snap,
    )


@now_serving_bp.post("/now-serving/kiosk/take")
def kiosk_take():
    ticket = issue_ticket()
    printed = try_print_now_serving_ticket(ticket["label"], ticket["window"])
    ticket["printed"] = printed
    _enqueue_audit(
        action="NOW_SERVING_ISSUED",
        entity_type="now_serving_ticket",
        entity_id=ticket["id"],
        after={
            "label": ticket["label"],
            "window": ticket["window"],
            "printed": printed,
        },
    )
    return jsonify({"success": True, **ticket})


@now_serving_bp.get("/now-serving/api/snapshot")
def api_snapshot():
    # Same cadence as staff modal poll — rebalance when one line is heavy/empty.
    maybe_rebalance_queues()
    return jsonify(board_snapshot())


@now_serving_bp.get("/now-serving/api/events")
def api_events():
    def _stream():
        last_rev = None
        deadline = time.monotonic() + 3600
        while time.monotonic() < deadline:
            maybe_rebalance_queues()
            snap = board_snapshot()
            rev = snap.get("revision")
            if rev != last_rev:
                payload = json.dumps(snap, separators=(",", ":"))
                yield f"data: {payload}\n\n"
                last_rev = rev
            else:
                yield ": ping\n\n"
            time.sleep(0.5)
        yield "event: reconnect\ndata: {}\n\n"

    return Response(
        stream_with_context(_stream()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@now_serving_bp.get("/now-serving/board")
@permission_required("can_manage_now_serving")
def staff_board():
    """Legacy full-page URL — send staff back to the app with the modal open."""
    return redirect(url_for("dashboard", open_now_serving=1))


@now_serving_bp.post("/now-serving/api/call-next")
@permission_required("can_manage_now_serving")
def api_call_next():
    data = request.get_json(silent=True) or {}
    try:
        window = int(data.get("window"))
    except (TypeError, ValueError):
        return jsonify({"error": "window must be 1 or 2"}), 400
    try:
        ticket = call_next(window)
    except NowServingError as exc:
        return jsonify({"success": False, "error": str(exc), "snapshot": board_snapshot()}), 409

    completed = ticket.get("completed")
    if completed:
        # Close the prior client's audit trail for this window.
        _enqueue_audit(
            action="NOW_SERVING_COMPLETED",
            entity_type="now_serving_ticket",
            entity_id=completed["id"],
            before=completed,
            after={"status": "done", "window": window, "label": completed.get("label")},
        )

    # Fresh audit log for the newly called client — all later actions on this
    # ticket (transfer, complete) share entity_type/entity_id.
    _enqueue_audit(
        action="NOW_SERVING_STARTED",
        entity_type="now_serving_ticket",
        entity_id=ticket["id"],
        after={
            "label": ticket.get("label"),
            "window": ticket.get("window"),
            "status": "serving",
            "waiting_count": ticket.get("waiting_count"),
        },
    )
    return jsonify({"success": True, "ticket": ticket, "snapshot": board_snapshot()})


@now_serving_bp.post("/now-serving/api/transfer")
@permission_required("can_manage_now_serving")
def api_transfer():
    data = request.get_json(silent=True) or {}
    try:
        ticket_id = int(data.get("ticket_id"))
    except (TypeError, ValueError):
        return jsonify({"error": "ticket_id required"}), 400
    try:
        ticket = transfer_to_other_window(ticket_id)
    except NowServingError as exc:
        return jsonify({"success": False, "error": str(exc), "snapshot": board_snapshot()}), 409

    _enqueue_audit(
        action="NOW_SERVING_TRANSFERRED",
        entity_type="now_serving_ticket",
        entity_id=ticket["id"],
        after={
            "label": ticket.get("label"),
            "window": ticket.get("window"),
            "status": ticket.get("status"),
            "queue_pos": ticket.get("queue_pos"),
            "transferred": 1,
        },
    )
    return jsonify({"success": True, "ticket": ticket, "snapshot": board_snapshot()})


@now_serving_bp.post("/now-serving/api/reset")
@role_required("admin")
def api_reset():
    data = request.get_json(silent=True) or {}
    if not data.get("confirm"):
        return jsonify({"error": "confirm required"}), 400
    before = board_snapshot()
    result = reset_day()
    _enqueue_audit(
        action="NOW_SERVING_DAY_RESET",
        entity_type="now_serving_state",
        entity_id="1",
        before={"revision": before.get("revision"), "state": before.get("state")},
        after=result,
    )
    return jsonify({"success": True, **result, "snapshot": board_snapshot()})
