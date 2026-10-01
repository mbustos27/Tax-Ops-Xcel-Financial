"""Silent 8.5×11 printing via the Filetrack print relay (no browser dialog)."""
from __future__ import annotations

import base64
import logging

log = logging.getLogger("letter_print")


def try_print_letter_pdf(
    pdf_bytes: bytes,
    *,
    doc_name: str = "payment receipt",
) -> tuple[bool, str]:
    """Send a one-page Letter PDF to the reception print relay.

    Returns ``(ok, message)``. Never raises — pickup must still complete if
    the printer is down.
    """
    if not pdf_bytes or not pdf_bytes.startswith(b"%PDF"):
        return False, "not a PDF"
    try:
        from filetrack.config import FILETRACK_ENABLED, FILETRACK_PRINT_MODE

        if not FILETRACK_ENABLED:
            return False, "FILETRACK_ENABLED is false"

        mode = (FILETRACK_PRINT_MODE or "local").strip().lower()
        if mode == "relay":
            return _print_pdf_via_relay(pdf_bytes, doc_name=doc_name)

        from filetrack.labels.printer import send_pdf

        send_pdf(pdf_bytes, doc_name=doc_name)
        return True, "printed locally"
    except Exception as exc:
        log.warning("letter PDF print failed (%s): %s", doc_name, exc)
        return False, str(exc)


def _print_pdf_via_relay(pdf_bytes: bytes, *, doc_name: str) -> tuple[bool, str]:
    import requests
    from filetrack.config import (
        FILETRACK_RELAY_TIMEOUT_SEC,
        FILETRACK_RELAY_TOKEN,
        FILETRACK_RELAY_URL,
    )

    url = FILETRACK_RELAY_URL
    headers = {"Content-Type": "application/json"}
    if FILETRACK_RELAY_TOKEN:
        headers["X-Filetrack-Token"] = FILETRACK_RELAY_TOKEN
    # Relay may block briefly on ShellExecute + sleep — allow headroom.
    timeout = max(float(FILETRACK_RELAY_TIMEOUT_SEC), 25.0)
    try:
        resp = requests.post(
            url,
            json={
                "pdf_base64": base64.b64encode(pdf_bytes).decode("ascii"),
                "doc_name": doc_name,
            },
            headers=headers,
            timeout=timeout,
        )
    except Exception as exc:
        return False, f"relay unreachable: {exc}"
    if resp.status_code != 200:
        try:
            err = resp.json().get("error") or resp.text[:200]
        except Exception:
            err = resp.text[:200]
        return False, f"relay {resp.status_code}: {err}"
    return True, "sent to print relay"
