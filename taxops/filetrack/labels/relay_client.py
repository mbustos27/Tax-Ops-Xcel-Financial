"""filetrack.labels.relay_client — M1 print-relay mode: POST a print job to
filetrack.relay.server running on the machine where the physical printer
actually lives, instead of calling win32print directly from this process.

Used when filetrack.config.FILETRACK_PRINT_MODE == "relay" — e.g. TaxOps's
intake print hook runs inside a Windows *service* (Session 0) with no access
to a printer physically attached to a different workstation; the relay
process on that workstation does the real win32print call locally. See
filetrack/DEPLOYMENT.md ("Relay mode") for the full picture.

`requests` is an optional filetrack dependency (see filetrack/requirements.txt)
imported lazily here, same pattern as filetrack.listener.http_sink.
"""
from __future__ import annotations

from filetrack.config import (
    FILETRACK_RELAY_TIMEOUT_SEC,
    FILETRACK_RELAY_TOKEN,
    FILETRACK_RELAY_URL,
)


class RelayError(RuntimeError):
    """Raised when the relay is unreachable, times out, or rejects the job.
    Callers (e.g. app.py's print hook) decide whether to swallow this —
    it is never allowed to propagate into a DB transaction or break intake."""


def print_label_via_relay(
    log_number,
    *,
    relay_url: str | None = None,
    token: str | None = None,
    timeout: float | None = None,, log_in_date=None, **fields) -> None:
    """POST {"log_number": <str>} to the relay's /print endpoint. Raises
    RelayError on any network failure or non-200 response; returns None on
    success. Mirrors print_label()'s "one function, callers decide how to
    handle failure" contract."""
    import requests  # optional dep — see filetrack/requirements.txt

    url = relay_url or FILETRACK_RELAY_URL
    auth_token = token if token is not None else FILETRACK_RELAY_TOKEN
    timeout_seconds = timeout if timeout is not None else FILETRACK_RELAY_TIMEOUT_SEC

    headers = {"Content-Type": "application/json"}
    if auth_token:
        headers["X-Filetrack-Token"] = auth_token

    try:
        resp = requests.post(
            url,
            json={"log_number": str(log_number)},
            headers=headers,
            timeout=timeout_seconds,
        )
    except Exception as exc:
        raise RelayError(f"could not reach print relay at {url!r}: {exc}") from exc

    if resp.status_code != 200:
        raise RelayError(
            f"print relay at {url!r} rejected job (status={resp.status_code}): {resp.text[:200]}"
        )
