"""Single dispatch for printing a LOG file label (intake + UI reprint).

Callers: app.py intake create, documents scan-complete, POST print-label API.
Always uses the same render_label() ZPL → local win32print or relay.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("filetrack")


def dispatch_print_log_label(
    log_number,
    *,
    client_name: str = "",
    last_name: str = "",
    first_name: str = "",
    display_name: str = "",
    log_in_date=None,
    tax_year=None,
) -> None:
    """Print one LOG sticker for `log_number` using FILETRACK_PRINT_MODE."""
    from filetrack.config import FILETRACK_PRINT_MODE

    kwargs = dict(
        client_name=client_name,
        last_name=last_name,
        first_name=first_name,
        display_name=display_name,
        log_in_date=log_in_date,
        tax_year=tax_year,
    )
    if (FILETRACK_PRINT_MODE or "local").strip().lower() == "relay":
        from filetrack.labels.relay_client import print_label_via_relay

        print_label_via_relay(log_number, **kwargs)
    else:
        from filetrack.labels.print_label import print_label

        print_label(log_number, **kwargs)


def try_print_log_label(
    log_number,
    *,
    client_name: str = "",
    last_name: str = "",
    first_name: str = "",
    display_name: str = "",
    log_in_date=None,
    tax_year=None,
) -> bool:
    """Like dispatch_print_log_label but never raises. Returns True on success."""
    from filetrack.config import FILETRACK_ENABLED

    if not FILETRACK_ENABLED:
        return False
    try:
        dispatch_print_log_label(
            log_number,
            client_name=client_name,
            last_name=last_name,
            first_name=first_name,
            display_name=display_name,
            log_in_date=log_in_date,
            tax_year=tax_year,
        )
        return True
    except Exception as exc:
        from filetrack.labels.relay_client import RelayError

        if isinstance(exc, RelayError):
            logger.warning(
                "filetrack: label print failed for log_number=%s — %s",
                log_number,
                exc,
            )
        else:
            logger.warning(
                "filetrack: label print failed for log_number=%s",
                log_number,
                exc_info=True,
            )
        return False
