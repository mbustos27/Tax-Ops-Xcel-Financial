from __future__ import annotations

import functools

import requests
from flask import Blueprint, jsonify, redirect, request, session, url_for

from config import OLLAMA_BASE_URL
from db import get_connection
from llm import chat

ai = Blueprint("ai", __name__, url_prefix="/ai")


# ---------------------------------------------------------------------------
# Auth — same logic as login_required in app.py; duplicated here to avoid
# a circular import (app.py imports this module).
# ---------------------------------------------------------------------------

def _login_required(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return wrapper


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@ai.get("/status")
@_login_required
def ai_status():
    """Ping Ollama and report whether it is reachable.

    Always returns HTTP 200 — callers check the 'ok' field.
    """
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        resp.raise_for_status()
        data        = resp.json()
        model_count = len(data.get("models") or [])
        return jsonify({"ok": True, "model_count": model_count})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})


@ai.post("/return/<int:return_id>/draft-email")
@_login_required
def ai_draft_email(return_id: int):
    """Draft a plain-English rejection follow-up email for a REJECTED return.

    Reads only — no DB writes, no ssn_last4 in prompt or response.
    """
    conn = get_connection()

    # Fetch return with client join — replicates the _SELECT join pattern from app.py
    row = conn.execute(
        """
        SELECT
            r.id, r.client_status, r.tax_year,
            c.last_name, c.first_name, c.display_name
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        WHERE r.id = ?
        """,
        (return_id,),
    ).fetchone()

    if not row:
        conn.close()
        return jsonify({"error": "Return not found"}), 404

    if row["client_status"] != "REJECTED":
        conn.close()
        return jsonify({"error": "Return is not in REJECTED status"}), 400

    # Build display name the same way _enrich() does in app.py
    display_name = (
        row["display_name"]
        or (
            f"{row['last_name']}, {row['first_name']}"
            if row["first_name"]
            else row["last_name"] or ""
        )
    )
    tax_year = row["tax_year"] or ""

    # Fetch most recent rejection details from efile_batch_items
    batch_row = conn.execute(
        """
        SELECT rejection_code, rejection_reason
        FROM efile_batch_items
        WHERE return_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (return_id,),
    ).fetchone()
    conn.close()

    rejection_code   = (batch_row["rejection_code"]   or "") if batch_row else ""
    rejection_reason = (batch_row["rejection_reason"] or "") if batch_row else ""

    prompt = (
        "You are helping a tax office staff member draft a follow-up email to a client "
        "whose federal tax return was rejected by the IRS.\n\n"
        f"Client name: {display_name}\n"
        f"Tax year: {tax_year}\n"
        f"Rejection code: {rejection_code}\n"
        f"Rejection reason: {rejection_reason}\n\n"
        "Write a short, professional, plain-English email the staff member can send to the "
        "client explaining that their return was rejected and asking them to contact the office "
        "as soon as possible. Translate any IRS technical language or codes into plain English "
        "the client will understand. Do not include a subject line. Do not sign the email. "
        "Do not mention specific dollar amounts. Do not invent details not provided above."
    )

    try:
        draft = chat(prompt)
        return jsonify({"draft": draft})
    except Exception:
        return jsonify({"error": "LLM unavailable — is Ollama running?"}), 503
