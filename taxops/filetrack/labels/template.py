"""filetrack.labels.template — pure ZPL rendering for file labels.

Loads the base ZPL from a template file on disk (default:
LABEL_CLEAN_EDITABLE.zpl), injects the human-readable log number, a native
^BC Code128 barcode payload, and the LOG-IN date, and returns the rendered
ZPL as a string.

render_label() is a pure function: it reads the template file (the only
I/O it performs) and returns a new string. It never writes to the
template file, and never rasterizes the barcode — ^BC stays a native ZPL
field the printer itself renders.
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

_LOGNUM_PLACEHOLDER = "{LOGNUM}"
_BARCODE_PLACEHOLDER = "{BARCODE}"
_LOG_IN_DATE_PLACEHOLDER = "{LOG_IN_DATE}"
_EXT_YEAR_PLACEHOLDER = "{EXT_YEAR}"


def load_template(template_path: str | None = None) -> str:
    """Read the base ZPL template from disk. The only I/O render_label()
    performs — never writes back to this (or any) file."""
    path = template_path or DEFAULT_TEMPLATE_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"filetrack label template not found: {path!r}. "
            "Pass template_path= explicitly or set FILETRACK_TEMPLATE_PATH-equivalent "
            "default in filetrack.config.DEFAULT_TEMPLATE_PATH."
        )
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def barcode_payload_for_log(log_number) -> str:
    """The locked PREFIX:VALUE payload a file label's barcode encodes —
    `LOG:<zero-padded log number>`. Shared by render_label() and the M1
    tests/samples so there is exactly one place this string is built."""
    return f"{LOG_PREFIX}{PREFIX_DELIMITER}{format_log_number(log_number)}"


def format_log_in_date(value=None) -> str:
    """Format a LOG-IN date for the sticker as ``MM/DD/YYYY``.

    ``None`` / empty → today. Accepts ``date``, ``datetime``, ISO
    ``YYYY-MM-DD``, or ``MM/DD/YYYY``. Invalid values fall back to today.
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
        month, day, year = int(mdy.group(1)), int(mdy.group(2)), int(mdy.group(3))
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


def render_label(log_number, *, template_path: str | None = None, log_in_date=None, **fields) -> str:
    """Render one file label's ZPL for `log_number`.

    - The human-readable {LOGNUM} field and the barcode payload both use
      filetrack.config.format_log_number() — the one canonical zero-pad
      rule (5 digits) — so the printed number and the barcode value never
      drift apart.
    - ``log_in_date`` fills ``LOG-IN`` (defaults to today); EXT stays a blank
      ``__/__/YYYY`` using that date's year. Override for backdated labels.
    - Pure function — reads the template file and returns a new string.
      Never mutates the template file on disk.
    """
    if log_in_date is None and "log_in_date" in fields:
        log_in_date = fields.get("log_in_date")
    formatted = format_log_number(log_number)
    payload = barcode_payload_for_log(log_number)
    log_in = format_log_in_date(log_in_date)
    ext_year = log_in[-4:] if len(log_in) >= 4 else str(date.today().year)

    zpl = load_template(template_path)
    zpl = zpl.replace(_LOGNUM_PLACEHOLDER, formatted)
    zpl = zpl.replace(_BARCODE_PLACEHOLDER, payload)
    zpl = zpl.replace(_LOG_IN_DATE_PLACEHOLDER, log_in)
    zpl = zpl.replace(_EXT_YEAR_PLACEHOLDER, ext_year)
    return zpl
