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

from urllib.parse import urlsplit

from filetrack.config import (
    FILETRACK_RELAY_TIMEOUT_SEC,
    FILETRACK_RELAY_TOKEN,
    FILETRACK_RELAY_URL,
)


class RelayError(RuntimeError):
    """Raised when the relay is unreachable, times out, or rejects the job.
    Callers (e.g. app.py's print hook) decide whether to swallow this —
    it is never allowed to propagate into a DB transaction or break intake."""


def _classify_network_error(exc: Exception, url: str) -> str:
    """Turn a raw `requests` exception into a message a non-Python staff
    member (or whoever is reading taxops_stderr.log at 8am) can actually
    act on, instead of just a stack trace. The distinction that matters:

      - ConnectTimeout: the TCP handshake itself never completed. Means the
        relay MACHINE is unreachable on the network (powered off, cable
        unplugged, wrong/drifted IP) OR something between us and it
        (Windows Firewall on the relay machine, in practice) is silently
        dropping the SYN instead of sending RST. Windows Firewall doing
        exactly this — drop, not reject — is the #1 real-world cause here.
      - ConnectionError (e.g. "connection refused") without it being a
        ConnectTimeout: the machine IS reachable, but nothing is listening
        on the port — the relay process/service itself is not running.
      - ReadTimeout: the connection was accepted (so the machine AND the
        relay process are both up) but it never sent a response — the
        relay is alive but hung/crashed mid-request (e.g. spooler stuck).

    Falls back to a generic message for anything else (bad URL, DNS, etc).
    """
    try:
        import requests
    except ImportError:  # pragma: no cover - requests is a hard dep of this call path
        return f"could not reach print relay at {url!r}: {exc}"

    parsed = urlsplit(url)
    host_port = f"{parsed.hostname}:{parsed.port or 80}"
    on_print_station = (
        "On the print station: confirm FiletrackRelay is running "
        "(nssm status FiletrackRelay / curl http://127.0.0.1:8765/health) — "
        "see taxops/filetrack/DEPLOYMENT.md §4."
    )

    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return (
            f"TIMED OUT connecting to the print relay at {host_port} (no response at all, "
            "not even a rejection). Most likely: the print-station PC is off/asleep/"
            "disconnected from the network, its IP address changed (check it still matches "
            f"{host_port!r} — a DHCP lease change looks exactly like this), or Windows "
            f"Firewall on that PC is silently blocking inbound port {parsed.port}. {on_print_station}"
        )
    if isinstance(exc, requests.exceptions.ConnectionError):
        return (
            f"CONNECTION REFUSED by {host_port} — the print-station PC is up and reachable "
            "on the network, but nothing is listening on that port, i.e. the print relay "
            f"process/service is not running. {on_print_station}"
        )
    if isinstance(exc, requests.exceptions.ReadTimeout):
        return (
            f"The print relay at {host_port} accepted the connection but never responded "
            "in time — the relay process is up but appears hung or stuck (e.g. the Windows "
            f"print spooler on that PC). {on_print_station}"
        )
    return f"could not reach print relay at {url!r}: {exc}"


def print_label_via_relay(
    log_number,
    *,
    relay_url: str | None = None,
    token: str | None = None,
    timeout: float | None = None,
    client_name: str = "",
    last_name: str = "",
    first_name: str = "",
    display_name: str = "",
    log_in_date=None,
) -> None:
    """POST print job to the relay's /print endpoint. Raises RelayError on
    any network failure or non-200 response; returns None on success."""
    import requests  # optional dep — see filetrack/requirements.txt

    url = relay_url or FILETRACK_RELAY_URL
    auth_token = token if token is not None else FILETRACK_RELAY_TOKEN
    timeout_seconds = timeout if timeout is not None else FILETRACK_RELAY_TIMEOUT_SEC

    headers = {"Content-Type": "application/json"}
    if auth_token:
        headers["X-Filetrack-Token"] = auth_token

    body = {"log_number": str(log_number)}
    if client_name:
        body["client_name"] = client_name
    if last_name:
        body["last_name"] = last_name
    if first_name:
        body["first_name"] = first_name
    if display_name:
        body["display_name"] = display_name
    if log_in_date is not None and str(log_in_date).strip():
        body["log_in_date"] = str(log_in_date).strip()

    try:
        resp = requests.post(
            url,
            json=body,
            headers=headers,
            timeout=timeout_seconds,
        )
    except Exception as exc:
        raise RelayError(_classify_network_error(exc, url)) from exc

    if resp.status_code != 200:
        raise RelayError(
            f"print relay at {url!r} rejected job (status={resp.status_code}): {resp.text[:200]}"
        )
