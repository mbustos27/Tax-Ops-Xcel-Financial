"""filetrack.labels.template — pure ZPL rendering for file labels.

Loads the base ZPL from a template file on disk (default:
LABEL_CLEAN_EDITABLE.zpl, next to this module — a fixed copy of the
originally-supplied docs/LABEL_BARCODE_v1.zpl; see the module docstring
below for what was fixed and why the original is untouched), injects the
human-readable log number and a native ^BC Code128 barcode payload, and
returns the rendered ZPL as a string.

render_label() is a pure function: it reads the template file (the only
I/O it performs) and returns a new string. It never writes to the
template file, and never rasterizes the barcode — ^BC stays a native ZPL
field the printer itself renders.

Fixes applied when copying docs/LABEL_BARCODE_v1.zpl to
LABEL_CLEAN_EDITABLE.zpl (source file left untouched):
  (a) Line "^FX ^BY sets module width (2) and ratio; ^BC height 70, ..."
      embedded literal ^BY/^BC caret commands inside ^FX comment text. Per
      the ZPL spec, an ^FX comment's effect ends at the very next caret
      command — so this comment actually terminated at "^BY", which then
      received the rest of the line as garbage parameters, corrupting that
      field. Fixed by rewriting the comment without embedded carets and
      terminating it with ^FS.
  (b) Coordinate bounds (^FO/^GB vs 532x203) were verified and are already
      correct — border 5,5→528x198; barcode 70,80 h=70→bottom 150; human-
      readable barcode text 70,158 h=22→bottom 180. All inside 532x203
      with no overlaps. No change needed for this one.
"""
from __future__ import annotations

import os

from filetrack.config import (
    DEFAULT_TEMPLATE_PATH,
    LOG_PREFIX,
    PREFIX_DELIMITER,
    format_log_number,
)

_LOGNUM_PLACEHOLDER = "{LOGNUM}"
_BARCODE_PLACEHOLDER = "{BARCODE}"


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


def render_label(log_number, *, template_path: str | None = None, **fields) -> str:
    """Render one file label's ZPL for `log_number`.

    - The human-readable {LOGNUM} field and the barcode payload both use
      filetrack.config.format_log_number() — the one canonical zero-pad
      rule (5 digits) — so the printed number and the barcode value never
      drift apart.
    - The barcode is a native ^BC Code128 field already present in the
      template (^BY2,2,70 / ^BCN,70,N,N,N); this function only supplies
      the {BARCODE} value substituted into it — it never rasterizes an
      image of a barcode.
    - Pure function — reads the template file and returns a new string.
      Never mutates the template file on disk.
    - **fields is accepted but unused by this template (which only defines
      {LOGNUM}/{BARCODE}) — kept for forward-compatibility with future
      label templates that need more fields.
    """
    formatted = format_log_number(log_number)
    payload = barcode_payload_for_log(log_number)

    zpl = load_template(template_path)
    zpl = zpl.replace(_LOGNUM_PLACEHOLDER, formatted)
    zpl = zpl.replace(_BARCODE_PLACEHOLDER, payload)
    return zpl
