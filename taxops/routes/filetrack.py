"""M3 — the internal endpoint the scan listener's HTTP sink POSTs to.

POST /filetrack/status
  Body: {"log_number": "123", "status": "FINALIZE",
         "scanned_at": "<iso, optional>", "source": "<optional, default 'scanner'>"}

Auth model (deliberately NOT session/RBAC — @login_required/@role_required
assume a browser session, and this is hit by a headless background process
with no session at all):
  1. FILETRACK_ENABLED must be true (filetrack.config kill-switch). When
     false, this route 404s — it "doesn't exist" rather than existing-but-
     403, so an unconfigured deployment doesn't advertise the feature.
  2. X-Filetrack-Token header must match filetrack.config.FILETRACK_TOKEN
     (constant-time compare). If FILETRACK_TOKEN is unset, the request is
     allowed ONLY from 127.0.0.1/::1, with a loud warning log — a documented
     dev-only convenience, never a supported production posture.

This module imports filetrack_service (the one TaxOps<->filetrack bridge)
and filetrack.config only — never anything from filetrack.labels or
filetrack.listener directly.
"""
from __future__ import annotations

import hmac
import logging

from flask import Blueprint, jsonify, request

from filetrack.config import FILETRACK_ENABLED, FILETRACK_TOKEN
from filetrack_service import FiletrackStatusError, apply_filetrack_status

logger = logging.getLogger("filetrack")

filetrack_bp = Blueprint("filetrack", __name__)

_LOCALHOST_ADDRS = ("127.0.0.1", "::1")


def _token_valid() -> bool:
    supplied = request.headers.get("X-Filetrack-Token", "")
    if FILETRACK_TOKEN:
        return hmac.compare_digest(supplied, FILETRACK_TOKEN)
    if (request.remote_addr or "") in _LOCALHOST_ADDRS:
        logger.warning(
            "filetrack: FILETRACK_TOKEN is unset — allowing unauthenticated "
            "request from localhost. Set FILETRACK_TOKEN before deploying "
            "the listener off-box."
        )
        return True
    return False


@filetrack_bp.post("/filetrack/status")
def api_filetrack_status():
    if not FILETRACK_ENABLED:
        return jsonify({"error": "Not found"}), 404
    if not _token_valid():
        return jsonify({"error": "Unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    log_number = body.get("log_number")
    status = body.get("status")
    scanned_at = body.get("scanned_at")
    source = (body.get("source") or "scanner").strip() or "scanner"

    if not log_number or not status:
        return jsonify({"error": "log_number and status are required"}), 400

    try:
        result = apply_filetrack_status(
            log_number, status, scanned_at=scanned_at, source=source
        )
    except FiletrackStatusError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception:
        logger.exception(
            "filetrack: unexpected error applying status log_number=%r status=%r",
            log_number, status,
        )
        return jsonify({"error": "Internal error"}), 500

    return jsonify({"success": True, **result})
