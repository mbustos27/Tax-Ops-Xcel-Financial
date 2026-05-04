from __future__ import annotations

import functools
import json
import re

import requests
from flask import Blueprint, jsonify, redirect, request, session, url_for

from config import OLLAMA_BASE_URL
from db import get_connection
from db_tools import lookup_rejection_code
from llm import chat, extract_json
from utils import scrub_ssn_from_dict

ai = Blueprint("ai", __name__, url_prefix="/ai")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_USELESS_REASONS = frozenset({
    "", "test", "n/a", "na", "none", "tbd", "unknown",
    "?", "-", ".", "no reason", "no reason provided",
})

def _reason_is_useful(reason: str) -> bool:
    """Return True if rejection_reason contains meaningful staff-entered text.

    Filters out empty strings, placeholder values, and entries too short to
    convey any real information (< 10 chars after stripping).
    """
    stripped = (reason or "").strip()
    if len(stripped) < 10:
        return False
    return stripped.lower() not in _USELESS_REASONS


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

    reason_useful = _reason_is_useful(rejection_reason)
    static        = lookup_rejection_code(rejection_code)

    if static:
        # Known code — ground the LLM in the exact explanation from the reference table.
        # rejection_reason is never passed here regardless of its value.
        prompt = (
            f"You are helping a tax office staff member draft a follow-up email to a client "
            f"whose federal tax return was rejected by the IRS.\n\n"
            f"Client name: {display_name}\n"
            f"Tax year: {tax_year}\n"
            f"Rejection code: {rejection_code}\n"
            f"What this code means: {static['explanation']}\n"
            f"What the preparer must do: {static['action']}\n\n"
            f"Write a short, professional, plain-English email to the client based on the above. "
            f"Be specific — use the actual explanation above, not vague language. "
            f"Ask the client to contact the office as soon as possible. "
            f"Do not include a subject line. Do not sign the email. "
            f"Do not mention specific dollar amounts. "
            f"3 sentences maximum."
        )
    elif reason_useful:
        # Path A — unknown code, but staff recorded a meaningful reason.
        prompt = (
            f"You are helping a tax office staff member draft a follow-up email to a client "
            f"whose federal tax return was rejected by the IRS.\n\n"
            f"Client name: {display_name}\n"
            f"Tax year: {tax_year}\n"
            f"Rejection code: {rejection_code}\n"
            f"Rejection reason: {rejection_reason}\n\n"
            f"Write a short, professional, plain-English email explaining what caused the rejection "
            f"using the reason provided above. "
            f"Ask the client to contact the office as soon as possible. "
            f"Do not include a subject line. Do not sign the email. "
            f"Do not mention specific dollar amounts. "
            f"3 sentences maximum."
        )
    else:
        # Path B — unknown code, no useful reason recorded.
        # rejection_reason is completely excluded from this prompt.
        prompt = (
            f"You are helping a tax office staff member draft a follow-up email to a client "
            f"whose federal tax return was rejected by the IRS.\n\n"
            f"Client name: {display_name}\n"
            f"Tax year: {tax_year}\n"
            f"Rejection code: {rejection_code}\n\n"
            f"Use your knowledge of IRS e-file rejection code {rejection_code} to write a short, "
            f"professional, plain-English email explaining exactly what this specific rejection code means "
            f"and what the client needs to do to fix it. "
            f"Do not say a reason was not provided. "
            f"Do not use vague language like 'an issue was found' or 'discrepancies were detected'. "
            f"Be specific about what this code actually means. "
            f"Ask the client to contact the office as soon as possible. "
            f"Do not include a subject line. Do not sign the email. "
            f"Do not mention specific dollar amounts. "
            f"3 sentences maximum."
        )

    try:
        draft = chat(prompt)
        return jsonify({"draft": draft})
    except Exception:
        return jsonify({"error": "LLM unavailable — is Ollama running?"}), 503


@ai.post("/rejection-code/lookup")
@_login_required
def ai_rejection_code_lookup():
    """Normalize a raw IRS rejection code and return a plain-English explanation.

    Checks the static reference table first — instant, no LLM needed for known codes.
    Falls back to LLM only for unrecognized codes.
    No DB reads or writes. No ssn_last4 anywhere.
    """
    data = request.get_json(force=True) or {}
    raw  = (data.get("code") or "").strip()
    if not raw:
        return jsonify({"error": "No code provided"}), 400

    # Normalize: strip spaces, uppercase, reinsert canonical hyphens by code family
    cleaned   = re.sub(r'\s+', '', raw).upper()
    bare      = cleaned.replace('-', '')
    ind_match = re.match(r'^([A-Z]{2,4})(\d+)$', bare)
    r_match   = re.match(r'^(R)(\d{4})(\d+)$', bare)
    s_match   = re.match(r'^(S)(\d{3})(\d{3})(\d*)$', bare)

    if r_match:
        normalized = f"{r_match.group(1)}{r_match.group(2)}-{r_match.group(3)}"
    elif s_match and s_match.group(4):
        normalized = f"{s_match.group(1)}{s_match.group(2)}-{s_match.group(3)}-{s_match.group(4)}"
    elif s_match:
        normalized = f"{s_match.group(1)}{s_match.group(2)}-{s_match.group(3)}"
    elif ind_match:
        normalized = f"{ind_match.group(1)}-{ind_match.group(2)}"
    else:
        normalized = bare

    # Try static reference table first — instant, no LLM needed
    static_result = lookup_rejection_code(normalized)

    if static_result:
        return jsonify({
            "normalized_code": normalized,
            "recognized":      True,
            "explanation":     static_result["explanation"],
            "action":          static_result["action"],
            "irs_reference":   static_result["irs_reference"],
            "source":          "reference",
        }), 200

    # LLM fallback for unrecognized codes
    prompt = (
        "You are an IRS e-file rejection code reference assistant for a tax preparation office.\n\n"
        f"Rejection code: {normalized}\n\n"
        "Respond in this JSON format only — no markdown, no extra text, no explanation outside the JSON:\n"
        "{\n"
        '  "recognized": true or false,\n'
        '  "explanation": "One sentence in plain English describing what caused this rejection. Staff-facing, no jargon.",\n'
        '  "action": "One sentence describing the single most important thing the preparer must do to fix it.",\n'
        '  "irs_reference": "The official IRS publication or help article most relevant to this code, or empty string if unknown."\n'
        "}\n\n"
        "Rules:\n"
        "- explanation must be one sentence only\n"
        "- action must be one sentence only\n"
        "- If you do not recognize the code, set recognized to false\n"
        "- Do not invent IRS references — use empty string if unsure\n"
        "- Do not include SSNs, EINs, or any taxpayer identification numbers"
    )

    try:
        result = extract_json(prompt)
    except json.JSONDecodeError:
        return jsonify({"error": "LLM returned malformed response — try again"}), 502
    except Exception:
        return jsonify({"error": "LLM unavailable — is Ollama running?"}), 503

    if not all(k in result for k in ("recognized", "explanation", "action")):
        return jsonify({"error": "LLM returned malformed response — try again"}), 502

    allowed = {"recognized", "explanation", "action", "irs_reference"}
    clean   = {k: v for k, v in result.items() if k in allowed}

    return jsonify({
        "normalized_code": normalized,
        "recognized":      bool(clean.get("recognized", False)),
        "explanation":     str(clean.get("explanation", "")),
        "action":          str(clean.get("action", "")),
        "irs_reference":   str(clean.get("irs_reference") or ""),
        "source":          "llm",
    }), 200


