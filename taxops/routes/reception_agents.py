"""Admin-only reception agent status (print relay + scan agent).

Polls LAN endpoints with a short timeout. Never blocks page render — the
HTML shell returns immediately; JS fetches /api/admin/reception-agents/status.
"""
from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request

from flask import Blueprint, jsonify, render_template

from auth import role_required
from config import SCAN_AGENT_TOKEN, SCAN_AGENT_URL

logger = logging.getLogger(__name__)

reception_agents_bp = Blueprint("reception_agents", __name__)

_POLL_TIMEOUT_SEC = 3


def _print_relay_base() -> str:
    """Derive http://host:8765 from FILETRACK_RELAY_URL (.../print)."""
    import os

    url = (os.environ.get("FILETRACK_RELAY_URL") or "http://127.0.0.1:8765/print").rstrip("/")
    if url.endswith("/print"):
        url = url[: -len("/print")]
    return url or "http://127.0.0.1:8765"


def _http_json(url: str, headers: dict | None = None, timeout: int = _POLL_TIMEOUT_SEC) -> dict:
    req = urllib.request.Request(url, method="GET", headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                data = {"raw": body[:200]}
            data["_http_ok"] = True
            data["_http_status"] = getattr(resp, "status", 200)
            return data
    except urllib.error.HTTPError as exc:
        return {"_http_ok": False, "_http_status": exc.code, "error": f"HTTP {exc.code}"}
    except Exception as exc:  # noqa: BLE001 — surface to admin UI
        return {"_http_ok": False, "_http_status": 0, "error": str(exc)}


def _snapshot() -> dict:
    import os

    print_base = _print_relay_base()
    print_health = _http_json(f"{print_base}/health")
    print_ok = bool(
        print_health.get("_http_ok") and print_health.get("printer_found")
    )

    scan_headers = {}
    if SCAN_AGENT_TOKEN:
        scan_headers["X-Scan-Agent-Token"] = SCAN_AGENT_TOKEN
    scan_url = (SCAN_AGENT_URL or "http://127.0.0.1:8766").rstrip("/") + "/health"
    scan_health = _http_json(scan_url, headers=scan_headers)
    scan_ok = bool(
        scan_health.get("_http_ok")
        and scan_health.get("com_sta") is True
        and (scan_health.get("code_rev") or "") == "com_sta_v4"
    )

    return {
        "print": {
            "ok": print_ok,
            "url": print_base,
            "printer_found": print_health.get("printer_found"),
            "printer_configured": print_health.get("printer_configured"),
            "error": print_health.get("error"),
        },
        "scan": {
            "ok": scan_ok,
            "url": SCAN_AGENT_URL,
            "code_rev": scan_health.get("code_rev"),
            "com_sta": scan_health.get("com_sta"),
            "error": scan_health.get("error"),
        },
        "ok": print_ok and scan_ok,
        "relay_token_configured": bool(os.environ.get("FILETRACK_RELAY_TOKEN")),
    }


@reception_agents_bp.route("/admin/reception-agents")
@role_required("admin")
def reception_agents_admin():
    # Do not poll here — template loads shell; JS hits the API (non-blocking).
    return render_template(
        "reception_agents_admin.html",
        active_page="reception_agents",
        print_url=_print_relay_base(),
        scan_url=SCAN_AGENT_URL,
    )


@reception_agents_bp.route("/api/admin/reception-agents/status")
@role_required("admin")
def reception_agents_status_api():
    return jsonify(_snapshot())
