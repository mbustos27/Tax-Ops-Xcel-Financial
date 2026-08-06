"""Static reference helpers restored after ai_routes.py removal.

Provides:
  POST /api/rejection-code/lookup — IRS e-file code → staff explanation (table only)
  POST /return/<id>/documents/<doc_id>/requeue-extraction — staff re-queue background extract

Imported for side effect from routes.documents (decorators attach to documents_bp).
"""
from __future__ import annotations

import re

from flask import jsonify, request, session

from auth import login_required
from db import get_connection
from db_tools import lookup_rejection_code
from routes.documents import documents_bp
from utils import now


def _normalize_rejection_code(raw: str) -> str:
    """Strip spaces, uppercase, reinsert canonical hyphens by code family."""
    cleaned = re.sub(r"\s+", "", (raw or "").strip()).upper()
    bare = cleaned.replace("-", "")
    ind_match = re.match(r"^([A-Z]{2,4})(\d+)$", bare)
    r_match = re.match(r"^(R)(\d{4})(\d+)$", bare)
    s_match = re.match(r"^(S)(\d{3})(\d{3})(\d*)$", bare)

    if r_match:
        return f"{r_match.group(1)}{r_match.group(2)}-{r_match.group(3)}"
    if s_match and s_match.group(4):
        return f"{s_match.group(1)}{s_match.group(2)}-{s_match.group(3)}-{s_match.group(4)}"
    if s_match:
        return f"{s_match.group(1)}{s_match.group(2)}-{s_match.group(3)}"
    if ind_match:
        return f"{ind_match.group(1)}-{ind_match.group(2)}"
    return bare


@documents_bp.post("/api/rejection-code/lookup")
@login_required
def api_rejection_code_lookup():
    """Normalize an IRS rejection code and return the static reference entry.

    LLM fallback was removed with ai_routes; unknown codes return recognized=false.
    No DB reads/writes. No SSN/PII.
    """
    data = request.get_json(silent=True) or {}
    raw = (data.get("code") or "").strip()
    if not raw:
        return jsonify({"error": "No code provided"}), 400

    normalized = _normalize_rejection_code(raw)
    static_result = lookup_rejection_code(normalized)
    if static_result:
        return jsonify(
            {
                "normalized_code": normalized,
                "recognized": True,
                "explanation": static_result["explanation"],
                "action": static_result["action"],
                "irs_reference": static_result["irs_reference"],
                "source": "reference",
            }
        )

    return jsonify(
        {
            "normalized_code": normalized,
            "recognized": False,
            "explanation": "",
            "action": "",
            "irs_reference": "",
            "source": "unknown",
        }
    )


@documents_bp.post("/return/<int:return_id>/documents/<int:doc_id>/requeue-extraction")
@login_required
def return_document_requeue_extraction(return_id: int, doc_id: int):
    """Staff re-queue for the background extractor (replaces dead POST /ai/documents/.../extract)."""
    reviewer = session.get("username") or "staff"
    conn = get_connection()
    try:
        doc = conn.execute(
            """
            SELECT id FROM return_documents
            WHERE id = ? AND return_id = ? AND is_deleted = 0
            """,
            (doc_id, return_id),
        ).fetchone()
        if not doc:
            return jsonify({"error": "Not found"}), 404

        row = conn.execute(
            """
            SELECT id, status FROM extraction_queue
            WHERE doc_id = ?
            ORDER BY id DESC LIMIT 1
            """,
            (doc_id,),
        ).fetchone()

        if row and row["status"] in ("pending", "processing"):
            return jsonify(
                {
                    "success": True,
                    "status": row["status"],
                    "message": "Already queued",
                }
            )

        if row:
            conn.execute(
                """
                UPDATE extraction_queue
                SET status = 'pending', attempts = 0, error_message = ?,
                    processed_at = NULL
                WHERE id = ?
                """,
                (f"Re-queued by {reviewer}", row["id"]),
            )
        else:
            conn.execute(
                """
                INSERT INTO extraction_queue
                    (doc_id, return_id, status, attempts, created_at)
                VALUES (?, ?, 'pending', 0, ?)
                """,
                (doc_id, return_id, now()),
            )
        conn.commit()
    finally:
        conn.close()

    try:
        from extractor import _notify_extraction_worker

        _notify_extraction_worker()
    except Exception:
        pass

    return jsonify({"success": True, "status": "pending"})
