"""ACCOUNTING-6: Flask Blueprint for receipt OCR → QuickBooks queue.

Routes
------
GET  /accounting/receipts                  — queue listing
GET  /accounting/receipts/<id>             — review / detail
GET  /accounting/receipts/<id>/image       — serve receipt image file
POST /accounting/receipts/upload           — standalone receipt upload
POST /api/accounting/receipts/<id>/approve — approve with final category
POST /api/accounting/receipts/<id>/reject  — reject with reason
POST /api/accounting/receipts/export       — bulk export approved receipts
GET  /accounting/settings                  — settings page
POST /api/accounting/settings              — save settings (COA path, QB mode, …)
POST /api/accounting/coa/rebuild           — force re-index COA embeddings
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from flask import (
    Blueprint,
    abort,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)

from auth import login_required
from db import get_connection

log = logging.getLogger(__name__)

accounting_bp = Blueprint("accounting", __name__)

_ALLOWED_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".webp", ".bmp", ".pdf"})
_MAX_RECEIPT_BYTES = 20 * 1024 * 1024  # 20 MB


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _receipts_dir() -> Path:
    here = Path(__file__).parent.parent
    p = here / "documents" / "receipts"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _enqueue_audit(*, user_id: str, action: str, entity_id: str, before=None, after=None) -> None:
    """ACCOUNTING-11: non-blocking audit write for approval/rejection/export actions."""
    try:
        from audit_service import _enqueue_write
        _enqueue_write(
            user_id=user_id,
            action=action,
            entity_type="receipt_queue",
            entity_id=entity_id,
            before=before,
            after=after,
            ip_address=request.remote_addr,
            http_status=200,
        )
    except Exception as exc:
        log.warning("accounting audit write failed: %s", exc)


# ── Queue listing ──────────────────────────────────────────────────────────────

@accounting_bp.route("/accounting/receipts")
@login_required
def receipt_queue_list():
    status_filter = request.args.get("status", "review")
    with contextlib.closing(get_connection()) as conn:
        rows = conn.execute(
            """
            SELECT rq.*,
                   rd.original_filename AS linked_doc_name,
                   r.log_number, c.last_name, c.first_name
            FROM receipt_queue rq
            LEFT JOIN return_documents rd ON rd.id = rq.return_document_id
            LEFT JOIN returns r ON r.id = rd.return_id
            LEFT JOIN clients c ON c.id = r.client_id
            WHERE rq.status = ?
            ORDER BY rq.created_at DESC
            LIMIT 200
            """,
            (status_filter,),
        ).fetchall()
        counts = {
            row["status"]: row["n"]
            for row in conn.execute(
                "SELECT status, COUNT(*) n FROM receipt_queue GROUP BY status"
            ).fetchall()
        }
    from app import base_ctx  # noqa: PLC0415
    ctx = base_ctx()
    ctx.update(
        active_page="receipt_queue",
        receipts=[dict(r) for r in rows],
        status_filter=status_filter,
        counts=counts,
    )
    return render_template("accounting/queue.html", **ctx)


# ── Receipt detail / review ────────────────────────────────────────────────────

@accounting_bp.route("/accounting/receipts/<int:receipt_id>")
@login_required
def receipt_detail(receipt_id: int):
    with contextlib.closing(get_connection()) as conn:
        row = conn.execute(
            """
            SELECT rq.*,
                   rd.original_filename AS linked_doc_name,
                   r.log_number, r.id AS return_id, c.last_name, c.first_name
            FROM receipt_queue rq
            LEFT JOIN return_documents rd ON rd.id = rq.return_document_id
            LEFT JOIN returns r ON r.id = rd.return_id
            LEFT JOIN clients c ON c.id = r.client_id
            WHERE rq.id = ?
            """,
            (receipt_id,),
        ).fetchone()
        # COA candidates for the dropdown
        coa_entries = conn.execute(
            """
            SELECT value FROM app_settings WHERE key = 'coa_csv_path'
            """
        ).fetchone()
    if not row:
        abort(404)

    receipt = dict(row)
    try:
        receipt["line_items_list"] = json.loads(receipt["line_items"] or "[]")
    except Exception:
        receipt["line_items_list"] = []
    try:
        receipt["candidates_list"] = json.loads(receipt["category_candidates"] or "[]")
    except Exception:
        receipt["candidates_list"] = []

    from app import base_ctx  # noqa: PLC0415
    ctx = base_ctx()
    ctx.update(
        active_page="receipt_queue",
        receipt=receipt,
        has_image=os.path.isfile(receipt.get("image_path") or ""),
    )
    return render_template("accounting/review.html", **ctx)


# ── Serve receipt image ────────────────────────────────────────────────────────

@accounting_bp.route("/accounting/receipts/<int:receipt_id>/image")
@login_required
def receipt_image(receipt_id: int):
    with contextlib.closing(get_connection()) as conn:
        row = conn.execute(
            "SELECT image_path FROM receipt_queue WHERE id=?", (receipt_id,)
        ).fetchone()
    if not row or not os.path.isfile(row["image_path"]):
        abort(404)
    return send_file(row["image_path"])


# ── Standalone upload ──────────────────────────────────────────────────────────

@accounting_bp.post("/accounting/receipts/upload")
@login_required
def receipt_upload():
    f = request.files.get("receipt")
    if not f or not f.filename:
        return jsonify({"error": "no file"}), 400
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in _ALLOWED_EXTENSIONS:
        return jsonify({"error": f"unsupported file type {ext}"}), 400
    content = f.read()
    if len(content) > _MAX_RECEIPT_BYTES:
        return jsonify({"error": "file too large (max 20 MB)"}), 413

    dest_dir = _receipts_dir()
    uid = uuid.uuid4().hex
    fname = f"{uid}{ext}"
    fpath = dest_dir / fname
    fpath.write_bytes(content)

    user = session.get("username") or "staff"
    with contextlib.closing(get_connection()) as conn:
        cur = conn.execute(
            """
            INSERT INTO receipt_queue (image_path, original_filename, status, attempts, created_at)
            VALUES (?, ?, 'pending', 0, ?)
            """,
            (str(fpath), f.filename, _now()),
        )
        receipt_id = cur.lastrowid
        conn.commit()

    from accounting_worker import _notify_receipt_worker  # noqa: PLC0415
    _notify_receipt_worker()

    _enqueue_audit(
        user_id=user,
        action="receipt_uploaded",
        entity_id=str(receipt_id),
        after={"filename": f.filename, "size_bytes": len(content)},
    )

    log.info("receipt_upload: id=%d filename=%s by %s", receipt_id, f.filename, user)
    return jsonify({"success": True, "receipt_id": receipt_id}), 201


# ── Approve ───────────────────────────────────────────────────────────────────

@accounting_bp.post("/api/accounting/receipts/<int:receipt_id>/approve")
@login_required
def receipt_approve(receipt_id: int):
    data = request.get_json(silent=True) or {}
    category = (data.get("category") or "").strip()
    account = (data.get("account") or "").strip()
    notes = (data.get("notes") or "").strip()
    user = session.get("username") or "staff"

    if not category:
        return jsonify({"error": "category required"}), 400

    with contextlib.closing(get_connection()) as conn:
        before = conn.execute(
            "SELECT status, approved_category FROM receipt_queue WHERE id=?", (receipt_id,)
        ).fetchone()
        if not before:
            return jsonify({"error": "not found"}), 404
        conn.execute(
            """
            UPDATE receipt_queue
            SET status='approved', approved_category=?, approved_account=?,
                review_notes=?, reviewed_by=?, reviewed_at=?
            WHERE id=?
            """,
            (category, account, notes, user, _now(), receipt_id),
        )
        conn.commit()

    _enqueue_audit(
        user_id=user,
        action="receipt_approved",
        entity_id=str(receipt_id),
        before={"status": before["status"], "approved_category": before["approved_category"]},
        after={"status": "approved", "approved_category": category, "approved_account": account},
    )
    return jsonify({"success": True, "receipt_id": receipt_id, "status": "approved"})


# ── Reject ────────────────────────────────────────────────────────────────────

@accounting_bp.post("/api/accounting/receipts/<int:receipt_id>/reject")
@login_required
def receipt_reject(receipt_id: int):
    data = request.get_json(silent=True) or {}
    reason = (data.get("reason") or "").strip()
    user = session.get("username") or "staff"

    with contextlib.closing(get_connection()) as conn:
        before = conn.execute(
            "SELECT status FROM receipt_queue WHERE id=?", (receipt_id,)
        ).fetchone()
        if not before:
            return jsonify({"error": "not found"}), 404
        conn.execute(
            """
            UPDATE receipt_queue
            SET status='rejected', review_notes=?, reviewed_by=?, reviewed_at=?
            WHERE id=?
            """,
            (reason, user, _now(), receipt_id),
        )
        conn.commit()

    _enqueue_audit(
        user_id=user,
        action="receipt_rejected",
        entity_id=str(receipt_id),
        before={"status": before["status"]},
        after={"status": "rejected", "reason": reason},
    )
    return jsonify({"success": True, "receipt_id": receipt_id, "status": "rejected"})


# ── Export ────────────────────────────────────────────────────────────────────

@accounting_bp.post("/api/accounting/receipts/export")
@login_required
def receipt_export():
    """Export all approved receipts not yet exported; mark them as exported."""
    data = request.get_json(silent=True) or {}
    ids_filter = data.get("ids") or []  # optional list of specific IDs

    with contextlib.closing(get_connection()) as conn:
        if ids_filter:
            placeholders = ",".join("?" * len(ids_filter))
            rows = conn.execute(
                f"SELECT * FROM receipt_queue WHERE id IN ({placeholders}) AND status='approved'",
                ids_filter,
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM receipt_queue WHERE status='approved' ORDER BY created_at"
            ).fetchall()

    if not rows:
        return jsonify({"error": "no approved receipts to export"}), 400

    rows_list = [dict(r) for r in rows]
    try:
        from services.qb_export import export_receipts  # noqa: PLC0415
        here = Path(__file__).parent.parent
        dest_dir = here / "data" / "exports"
        file_path, fmt = export_receipts(rows_list, dest_dir)
    except Exception as exc:
        current_app.logger.exception("receipt export failed")
        return jsonify({"error": str(exc)}), 500

    user = session.get("username") or "staff"
    now_ts = _now()
    with contextlib.closing(get_connection()) as conn:
        for r in rows_list:
            conn.execute(
                "UPDATE receipt_queue SET status='exported', exported_at=?, export_file=? WHERE id=?",
                (now_ts, file_path, r["id"]),
            )
        conn.commit()

    _enqueue_audit(
        user_id=user,
        action="receipt_export",
        entity_id="bulk",
        after={"count": len(rows_list), "file": file_path, "format": fmt},
    )

    return send_file(
        file_path,
        as_attachment=True,
        download_name=os.path.basename(file_path),
        mimetype="text/plain",
    )


# ── Settings page ─────────────────────────────────────────────────────────────

@accounting_bp.route("/accounting/settings")
@login_required
def accounting_settings():
    with contextlib.closing(get_connection()) as conn:
        settings_rows = conn.execute(
            "SELECT key, value FROM app_settings WHERE key LIKE 'accounting_%' OR key LIKE 'qb_%' OR key = 'coa_csv_path' OR key = 'history_csv_path'"
        ).fetchall()
    settings = {r["key"]: r["value"] for r in settings_rows}

    import config as _cfg  # noqa: PLC0415
    # Merge env defaults for first-visit display
    settings.setdefault("coa_csv_path", _cfg.COA_CSV_PATH)
    settings.setdefault("history_csv_path", _cfg.HISTORY_CSV_PATH)
    settings.setdefault("qb_export_mode", _cfg.QB_EXPORT_MODE)
    settings.setdefault("accounting_confidence_high", str(_cfg.ACCOUNTING_CONFIDENCE_HIGH))
    settings.setdefault("accounting_confidence_medium", str(_cfg.ACCOUNTING_CONFIDENCE_MEDIUM))

    from app import base_ctx  # noqa: PLC0415
    ctx = base_ctx()
    ctx.update(active_page="accounting_settings", settings=settings)
    return render_template("accounting/settings.html", **ctx)


@accounting_bp.post("/api/accounting/settings")
@login_required
def accounting_settings_save():
    data = request.get_json(silent=True) or {}
    allowed = {
        "coa_csv_path", "history_csv_path", "qb_export_mode",
        "accounting_confidence_high", "accounting_confidence_medium",
    }
    user = session.get("username") or "staff"
    now_ts = _now()

    with contextlib.closing(get_connection()) as conn:
        for key, value in data.items():
            if key not in allowed:
                continue
            conn.execute(
                """
                INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (key, str(value), now_ts),
            )
        conn.commit()

    # Reset the COA matcher singleton so it reloads with the new path.
    try:
        from services.coa_matcher import reset_matcher  # noqa: PLC0415
        reset_matcher()
    except Exception:
        pass

    _enqueue_audit(
        user_id=user, action="accounting_settings_saved", entity_id="settings", after=data
    )
    return jsonify({"success": True})


# ── COA rebuild ───────────────────────────────────────────────────────────────

@accounting_bp.post("/api/accounting/coa/rebuild")
@login_required
def coa_rebuild():
    """Force recompute COA embeddings (ACCOUNTING-11: audit this action)."""
    import config as _cfg  # noqa: PLC0415

    # Check db for overridden path first
    with contextlib.closing(get_connection()) as conn:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key='coa_csv_path'"
        ).fetchone()
    csv_path = (row["value"] if row else None) or _cfg.COA_CSV_PATH

    if not csv_path or not os.path.isfile(csv_path):
        return jsonify({"error": "COA CSV path not configured or file missing"}), 400

    try:
        from services.coa_matcher import reset_matcher, get_matcher  # noqa: PLC0415
        reset_matcher()
        matcher = get_matcher()
        ok = matcher.rebuild(csv_path)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    user = session.get("username") or "staff"
    _enqueue_audit(
        user_id=user,
        action="coa_rebuild",
        entity_id="coa",
        after={"csv_path": csv_path, "success": ok},
    )
    if ok:
        return jsonify({"success": True, "message": "COA embeddings rebuilt"})
    return jsonify({"error": "rebuild failed — check server logs"}), 500
