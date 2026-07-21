"""filetrack.relay.server — print relay: run this ON the machine where the
physical label printer is actually attached (e.g. a front-desk workstation),
NOT on the TaxOps server.

Why this exists: filetrack.labels.print_label()'s direct win32print call only
works when the calling process runs on the same machine the printer is
installed on (as a real local Windows queue). A Windows *service* like
TaxOpsService runs in Session 0 and cannot see printers only visible via an
interactive RDP session on another box, and Windows printer-sharing across a
service account is fragile on a workgroup (non-domain) network. See
filetrack/DEPLOYMENT.md ("Relay mode") for the full writeup of why this
module exists.

TaxOps's intake print hook (app.py), when FILETRACK_PRINT_MODE=relay, POSTs
{"log_number": "..."} to this process's /print endpoint instead of calling
win32print itself. This module then does the real rendering + win32print
call locally, where the printer physically lives — reusing the exact same
filetrack.labels.print_label() used by "local" mode, so there is still only
one place that turns a log number into ink on a label.

Run:
    python -m filetrack.relay.server --port 8765
    python -m filetrack.relay.server --port 8765 --token SECRET

Printer name comes from FILETRACK_PRINTER (same env var as direct/local
mode) — set it on THIS machine (the one with the printer attached).
"""
from __future__ import annotations

import argparse
import hmac
import logging
import os

logger = logging.getLogger("filetrack.relay")

_LOCALHOST_ADDRS = ("127.0.0.1", "::1")


def handle_print_job(
    payload: dict,
    *,
    token_header: str,
    expected_token: str,
    remote_addr: str,
) -> tuple[int, dict]:
    """Pure request-handling logic, deliberately decoupled from Flask so it
    is unit-testable without a real HTTP server. Returns (http_status, body).

    Auth model mirrors routes/filetrack.py's /filetrack/status endpoint: if
    a token is configured, it must match (constant-time compare); if no
    token is configured, only localhost callers are allowed (dev-only
    convenience, loudly logged, never a supported production posture once
    the relay is actually on a different machine than its caller)."""
    if expected_token:
        if not hmac.compare_digest(token_header or "", expected_token):
            return 401, {"error": "Unauthorized"}
    elif remote_addr not in _LOCALHOST_ADDRS:
        logger.warning(
            "filetrack.relay: FILETRACK_RELAY_TOKEN is unset — rejecting "
            "non-localhost request from %s. Set FILETRACK_RELAY_TOKEN before "
            "printing across machines.",
            remote_addr,
        )
        return 401, {"error": "Unauthorized"}

    log_number = payload.get("log_number")
    if not log_number:
        return 400, {"error": "log_number is required"}

    from filetrack.labels.print_label import print_label
    from filetrack.labels.printer import (
        PrinterNotFoundError,
        PrinterUnavailableError,
        SpoolerError,
    )

    try:
        print_label(log_number)
    except (PrinterNotFoundError, PrinterUnavailableError, SpoolerError) as exc:
        logger.error("filetrack.relay: print failed for log_number=%s: %s", log_number, exc)
        return 500, {"error": str(exc)}
    except Exception:
        logger.exception("filetrack.relay: unexpected error printing log_number=%s", log_number)
        return 500, {"error": "internal error"}

    logger.info("filetrack.relay: printed label for log_number=%s", log_number)
    return 200, {"success": True, "log_number": log_number}


def create_app(*, token: str | None = None):
    """Build the (tiny) Flask app. Imported lazily — filetrack.relay is only
    needed on the printer's machine, which already needs pywin32; adding
    flask there too is a small ask (see filetrack/requirements.txt)."""
    from flask import Flask, jsonify, request

    from filetrack.config import FILETRACK_RELAY_TOKEN

    expected_token = token if token is not None else FILETRACK_RELAY_TOKEN

    app = Flask(__name__)

    @app.get("/health")
    def health():
        return jsonify({"status": "ok"})

    @app.post("/print")
    def do_print():
        body = request.get_json(silent=True) or {}
        status, resp_body = handle_print_job(
            body,
            token_header=request.headers.get("X-Filetrack-Token", ""),
            expected_token=expected_token,
            remote_addr=request.remote_addr or "",
        )
        return jsonify(resp_body), status

    return app


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="filetrack print relay — run on the machine the physical printer is attached to"
    )
    parser.add_argument("--host", default=os.environ.get("FILETRACK_RELAY_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("FILETRACK_RELAY_PORT", "8765")))
    parser.add_argument("--token", default=None, help="Overrides FILETRACK_RELAY_TOKEN env var")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    from filetrack.config import FILETRACK_RELAY_TOKEN

    token = args.token if args.token is not None else FILETRACK_RELAY_TOKEN
    if not token:
        logger.warning(
            "filetrack.relay: starting with NO token configured — only "
            "localhost callers will be accepted. Set FILETRACK_RELAY_TOKEN "
            "(or --token) before the TaxOps server (a different machine) "
            "needs to reach this relay."
        )

    app = create_app(token=token)
    logger.info("filetrack.relay: listening on %s:%d (printer=%s)", args.host, args.port,
                os.environ.get("FILETRACK_PRINTER") or "(unset!)")
    app.run(host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
