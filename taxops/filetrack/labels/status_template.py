"""filetrack.labels.status_template — pure ZPL rendering for STATUS station labels.

Same 2.625\" x 1\" geometry as LOG labels (filetrack.labels.geometry).
"""
from __future__ import annotations

import os

from filetrack.config import PREFIX_DELIMITER, STATUS_PREFIX
from filetrack.labels.geometry import enforce_label_geometry

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
    """`STATUS:<NAME>` barcode payload."""
    return f"{STATUS_PREFIX}{PREFIX_DELIMITER}{status_name}"


def render_status_label(status_name: str, *, template_path: str | None = None) -> str:
    """Render one STATUS station label; geometry locked to 2.625\" x 1\"."""
    payload = status_barcode_payload(status_name)
    zpl = load_status_template(template_path)
    zpl = zpl.replace(_STATUS_NAME_PLACEHOLDER, status_name)
    zpl = zpl.replace(_BARCODE_PLACEHOLDER, payload)
    return enforce_label_geometry(zpl)
