"""filetrack.labels.template — pure ZPL rendering for file (LOG) labels.

Loads LABEL_CLEAN_EDITABLE.zpl (24-dot L/R margins), injects LOG# + Code128 +
abbreviated client name + LOG-IN date, enforces 2.625\" x 1\" geometry.
"""
from __future__ import annotations

import os
import re
from datetime import date, datetime

from filetrack.config import (
    DEFAULT_TEMPLATE_PATH,
    LOG_PREFIX,
    PREFIX_DELIMITER,
    format_log_number,
)
from filetrack.labels.geometry import enforce_label_geometry

_LOGNUM_PLACEHOLDER = "{LOGNUM}"
_BARCODE_PLACEHOLDER = "{BARCODE}"
_CLIENT_PLACEHOLDER = "{CLIENT}"
_LOG_IN_DATE_PLACEHOLDER = "{LOG_IN_DATE}"
_EXT_YEAR_PLACEHOLDER = "{EXT_YEAR}"

# Fits right of bottom barcode text on 2.625\" stock (~210-dot field).
_CLIENT_MAX_LEN = 16


def load_template(template_path: str | None = None) -> str:
    path = template_path or DEFAULT_TEMPLATE_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"filetrack label template not found: {path!r}. "
            "Pass template_path= explicitly or set filetrack.config.DEFAULT_TEMPLATE_PATH."
        )
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def barcode_payload_for_log(log_number) -> str:
    """`LOG:<zero-padded log number>` barcode payload."""
    return f"{LOG_PREFIX}{PREFIX_DELIMITER}{format_log_number(log_number)}"


def format_log_in_date(value=None) -> str:
    """Format a LOG-IN date for the sticker as ``MM/DD/YYYY``.

    ``None`` / empty → today's date (server local). Accepts ``date``,
    ``datetime``, ISO ``YYYY-MM-DD`` (optionally with time), or an already
    printed ``MM/DD/YYYY`` string. Invalid values fall back to today so a
    bad override never blanks the label.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        d = date.today()
        return f"{d.month:02d}/{d.day:02d}/{d.year:04d}"

    if isinstance(value, datetime):
        d = value.date()
        return f"{d.month:02d}/{d.day:02d}/{d.year:04d}"
    if isinstance(value, date):
        return f"{value.month:02d}/{value.day:02d}/{value.year:04d}"

    s = str(value).strip()
    mdy = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if mdy:
        month, day, year = (int(mdy.group(1)), int(mdy.group(2)), int(mdy.group(3)))
        try:
            date(year, month, day)
        except ValueError:
            d = date.today()
            return f"{d.month:02d}/{d.day:02d}/{d.year:04d}"
        return f"{month:02d}/{day:02d}/{year:04d}"

    try:
        d = datetime.strptime(s[:10], "%Y-%m-%d").date()
        return f"{d.month:02d}/{d.day:02d}/{d.year:04d}"
    except ValueError:
        d = date.today()
        return f"{d.month:02d}/{d.day:02d}/{d.year:04d}"


def abbreviate_client_name(
    *,
    last_name: str = "",
    first_name: str = "",
    display_name: str = "",
    client_name: str = "",
    max_len: int = _CLIENT_MAX_LEN,
) -> str:
    """Short name for the label: ``LAST, F`` (or last / display fallback).

    Never includes SSN/TIN. Strips ZPL-unsafe ``^``. Empty if nothing usable.
    """
    if client_name and not last_name and not first_name:
        # Already-abbreviated or free-form from caller
        s = str(client_name).strip()
    else:
        last = (last_name or "").strip().upper()
        first = (first_name or "").strip()
        if last and first:
            s = f"{last}, {first[0].upper()}"
        elif last:
            s = last
        elif display_name:
            s = str(display_name).strip().upper()
        else:
            s = str(client_name or "").strip()
    # ZPL field data cannot contain caret
    s = s.replace("^", " ").replace("\n", " ").replace("\r", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > max_len:
        s = s[:max_len]
    return s


def render_label(
    log_number,
    *,
    template_path: str | None = None,
    client_name: str = "",
    last_name: str = "",
    first_name: str = "",
    display_name: str = "",
    log_in_date=None,
    **fields,
) -> str:
    """Render one LOG file label's ZPL for `log_number`.

    Optional client name fields become an abbreviated ``{CLIENT}`` on the
    bottom-right. ``log_in_date`` fills ``LOG-IN`` (defaults to today);
    EXT stays a blank ``__/__/YYYY`` with the year from that same date.
    Geometry locked to 2.625\" x 1\".
    """
    formatted = format_log_number(log_number)
    payload = barcode_payload_for_log(log_number)
    client = abbreviate_client_name(
        last_name=last_name,
        first_name=first_name,
        display_name=display_name,
        client_name=client_name,
    )
    # Allow legacy callers that stuffed the date into **fields.
    if log_in_date is None and "log_in_date" in fields:
        log_in_date = fields.get("log_in_date")
    log_in = format_log_in_date(log_in_date)
    ext_year = log_in[-4:] if len(log_in) >= 4 else str(date.today().year)

    zpl = load_template(template_path)
    zpl = zpl.replace(_LOGNUM_PLACEHOLDER, formatted)
    zpl = zpl.replace(_BARCODE_PLACEHOLDER, payload)
    zpl = zpl.replace(_CLIENT_PLACEHOLDER, client)
    zpl = zpl.replace(_LOG_IN_DATE_PLACEHOLDER, log_in)
    zpl = zpl.replace(_EXT_YEAR_PLACEHOLDER, ext_year)
    return enforce_label_geometry(zpl)
