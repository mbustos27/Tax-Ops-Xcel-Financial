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
import time

from flask import Blueprint, jsonify, request

from auth import role_required
from filetrack.config import FILETRACK_ENABLED, FILETRACK_TOKEN
from filetrack_service import FiletrackStatusError, apply_filetrack_status, get_filetrack_history

logger = logging.getLogger("filetrack")

filetrack_bp = Blueprint("filetrack", __name__)

# How long ago THIS process last imported filetrack.config, i.e. read
# FILETRACK_* from the environment. Combined with /api/admin/filetrack-config
# below, this answers "did my .env edit actually take effect yet?" directly,
# instead of waiting for a live print attempt and grepping server logs for
# it - see the 2026-07-21 .12 -> .9 workstation-move incident, where exactly
# that ambiguity was the actual blocker to verifying the fix.
_CONFIG_LOADED_MONOTONIC = time.monotonic()

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


@filetrack_bp.get("/api/admin/filetrack-config")
@role_required("admin")
def api_filetrack_config_status():
    """Admin-only: report the filetrack print config THIS RUNNING PROCESS
    actually has loaded right now — straight from filetrack.config's
    module-level values, never re-read from disk. NEVER includes
    FILETRACK_RELAY_TOKEN or FILETRACK_TOKEN themselves (just whether one
    is set) since this is reachable by any admin session, not just
    whoever is standing at the server console.

    Exists specifically because config changes (.env, NSSM
    AppEnvironmentExtra, etc.) only take effect after this process
    restarts and re-imports filetrack.config — there was previously no way
    to directly confirm, from outside, whether a given restart actually
    picked up a change. `config_loaded_seconds_ago` is the answer: if it's
    smaller than "how long ago I edited .env", the edit is NOT loaded yet
    (restart again); if it's larger, whatever this endpoint reports IS
    what's actually in effect right now.
    """
    # Local import (not at module top) so this always reflects the current
    # values even if something reloaded the module - matches the pattern
    # used by app.py's own print hook.
    from filetrack.config import (
        DEFAULT_PRINTER_NAME,
        FILETRACK_ENABLED,
        FILETRACK_ENDPOINT_URL,
        FILETRACK_PRINT_MODE,
        FILETRACK_RELAY_TOKEN,
        FILETRACK_RELAY_URL,
        FILETRACK_TOKEN as _status_token,
    )

    return jsonify({
        "filetrack_enabled": FILETRACK_ENABLED,
        "print_mode": FILETRACK_PRINT_MODE,
        "relay_url": FILETRACK_RELAY_URL,
        "relay_token_configured": bool(FILETRACK_RELAY_TOKEN),
        "printer_local_mode_only": DEFAULT_PRINTER_NAME,
        "status_endpoint_url": FILETRACK_ENDPOINT_URL,
        "status_token_configured": bool(_status_token),
        "config_loaded_seconds_ago": round(time.monotonic() - _CONFIG_LOADED_MONOTONIC, 1),
    })


@filetrack_bp.get("/api/admin/filetrack-history/<log_number>")
@role_required("admin")
def api_filetrack_history(log_number: str):
    """Admin-only: the filetrack_status_history timeline for one log_number,
    most recent first. Exists so a scan's "live effect" (a DB write from a
    real scanner event) can actually be verified from outside — there is no
    UI surfacing filetrack_status/filetrack_status_history anywhere yet, and
    this is the fastest way to confirm "did that scan I just did actually
    reach the server and update the DB?" without direct DB/SSH access.

    Only ever returns log_number/status/source/timestamps — the same fields
    filetrack_status_history stores, which never includes ssn, ssn_last4, or
    any other identification number (see filetrack_service.py, db.py schema)."""
    rows = get_filetrack_history(log_number)
    return jsonify({"log_number": log_number, "history": rows})
