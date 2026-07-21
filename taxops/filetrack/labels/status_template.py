"""filetrack.labels.status_template — pure ZPL rendering for STATUS station
labels (e.g. a "FINALIZE" sticker stuck on a shelf/bin that staff scan to
set the sticky active_status before scanning a stack of LOG file labels).

This is a NEW template authored for M3 (STATUS_LABEL.zpl) — distinct from
LABEL_CLEAN_EDITABLE.zpl (the file label, which has date fill-in fields that
make no sense on a status station sticker). Same 532x203 label geometry,
same border style, no printer/rasterization here — ^BC is still a native
ZPL field the printer renders.

Barcode module width for status labels defaults to 1 (not 2, as file labels
use) because status names can be long ("STATUS:PENDING INTAKE" = 22 chars)
and would overflow the label at module width 2. This has NOT been physically
validated (no M0-equivalent scan test for status labels yet) — see
STATUS_LABEL.zpl's comments before relying on scan reliability in production.
"""
from __future__ import annotations

import os

from filetrack.config import PREFIX_DELIMITER, STATUS_PREFIX

_STATUS_NAME_PLACEHOLDER = "{STATUS_NAME}"
_BARCODE_PLACEHOLDER = "{BARCODE}"

DEFAULT_STATUS_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "STATUS_LABEL.zpl")


def load_status_template(template_path: str | None = None) -> str:
    path = template_path or DEFAULT_STATUS_TEMPLATE_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(f"filetrack status label template not found: {path!r}")
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def status_barcode_payload(status_name: str) -> str:
    """The locked PREFIX:VALUE payload a status label's barcode encodes —
    `STATUS:<NAME>`, verbatim (not zero-padded — only LOG numbers are)."""
    return f"{STATUS_PREFIX}{PREFIX_DELIMITER}{status_name}"


def render_status_label(status_name: str, *, template_path: str | None = None) -> str:
    """Render one STATUS station label's ZPL for `status_name`.

    - status_name is used verbatim for both the human-readable {STATUS_NAME}
      field and the barcode payload — callers should pass an exact entry
      from filetrack.config.ALLOWED_STATUSES (validated by status_codes.py's
      CLI, not by this pure-string function).
    - Pure function — reads the template file, does in-memory substitution,
      returns a new string. Never mutates the template file on disk.
    """
    payload = status_barcode_payload(status_name)
    zpl = load_status_template(template_path)
    zpl = zpl.replace(_STATUS_NAME_PLACEHOLDER, status_name)
    zpl = zpl.replace(_BARCODE_PLACEHOLDER, payload)
    return zpl
