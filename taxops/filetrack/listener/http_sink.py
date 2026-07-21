"""filetrack.listener.http_sink — M3 wiring: a CallbackSink that POSTs to
TaxOps's internal /filetrack/status endpoint, with retry/backoff.

Still no TaxOps import — this module only knows about filetrack.config
(FILETRACK_ENDPOINT_URL / FILETRACK_TOKEN) and talks to TaxOps purely over
HTTP, the same boundary the M2 spec drew for filetrack.listener as a whole.
Requires `requests` (see filetrack/requirements.txt) — imported lazily so
filetrack.listener.sink/state/parser stay importable without it.
"""
from __future__ import annotations

import logging
import time

from filetrack.config import FILETRACK_ENDPOINT_URL, FILETRACK_TOKEN
from filetrack.listener.sink import CallbackSink

logger = logging.getLogger("filetrack.listener")

DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_BACKOFF_SECONDS = 1.0  # doubles each retry: 1s, 2s, 4s...
DEFAULT_TIMEOUT_SECONDS = 5.0


def _post_with_retry(
    log_number: str,
    status: str,
    ts: str,
    *,
    url: str,
    token: str,
    max_attempts: int,
    backoff_seconds: float,
    timeout_seconds: float,
) -> None:
    import requests  # optional dep — see filetrack/requirements.txt

    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Filetrack-Token"] = token
    payload = {"log_number": log_number, "status": status, "scanned_at": ts, "source": "scanner"}

    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=timeout_seconds)
            if resp.status_code >= 500:
                raise RuntimeError(f"server error {resp.status_code}: {resp.text[:200]}")
            if resp.status_code >= 400:
                # 4xx (bad status name, unauthorized, disabled) will never
                # succeed on retry — log once and give up without exhausting
                # the retry budget on a hopeless request.
                logger.error(
                    "filetrack http_sink: rejected (status=%s) log=%s status=%s body=%s — not retrying",
                    resp.status_code, log_number, status, resp.text[:200],
                )
                return
            logger.info(
                "filetrack http_sink: delivered log=%s status=%s (attempt %d/%d)",
                log_number, status, attempt, max_attempts,
            )
            return
        except Exception as exc:  # network error, timeout, 5xx raised above
            last_exc = exc
            if attempt < max_attempts:
                wait = backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "filetrack http_sink: attempt %d/%d failed (%s) — retrying in %.1fs",
                    attempt, max_attempts, exc, wait,
                )
                time.sleep(wait)

    logger.error(
        "filetrack http_sink: gave up after %d attempts for log=%s status=%s: %s",
        max_attempts, log_number, status, last_exc,
    )


def build_http_sink(
    *,
    url: str | None = None,
    token: str | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> CallbackSink:
    """Build a CallbackSink whose callback POSTs each assignment to TaxOps.
    Never raises out to run_listener's loop — a delivery failure after all
    retries is logged and the loop continues (an unreachable TaxOps server
    must never crash the scan station)."""
    endpoint = url or FILETRACK_ENDPOINT_URL
    auth_token = token if token is not None else FILETRACK_TOKEN

    def _callback(log_number: str, status: str, ts: str) -> None:
        try:
            _post_with_retry(
                log_number, status, ts,
                url=endpoint, token=auth_token,
                max_attempts=max_attempts, backoff_seconds=backoff_seconds,
                timeout_seconds=timeout_seconds,
            )
        except Exception:
            logger.exception(
                "filetrack http_sink: unexpected error delivering log=%s status=%s", log_number, status,
            )

    return CallbackSink(_callback)
