"""M3 — pure-string tests for filetrack.labels.status_template.render_status_label()."""
from __future__ import annotations

from filetrack.config import ALLOWED_STATUSES
from filetrack.labels.status_template import render_status_label, status_barcode_payload


def test_render_status_label_contains_name_and_barcode():
    zpl = render_status_label("FINALIZE")
    assert "FINALIZE" in zpl
    assert "^FD" + status_barcode_payload("FINALIZE") + "^FS" in zpl
    assert "STATUS:FINALIZE" in zpl


def test_render_status_label_preserves_structure():
    zpl = render_status_label("HOLD")
    assert zpl.startswith("^XA")
    assert zpl.rstrip().endswith("^XZ")
    assert "^PW532" in zpl
    assert "^LL203" in zpl
    assert "^LS0" in zpl
    assert "^PQ1" in zpl
    assert "^GB523" not in zpl  # no outer border
    assert "^BY1,2,60" in zpl
    assert "^BCN,60,N,N,N" in zpl
    assert "^LH0,0" in zpl
    assert "^FO70,78^BCN,60,N,N,N" in zpl


def test_enforce_geometry_rewrites_wrong_size():
    from filetrack.labels.geometry import enforce_label_geometry

    bad = "^XA\n^PW800\n^LL400\n^LH0,0\n^FDX^FS\n^XZ\n"
    fixed = enforce_label_geometry(bad)
    assert "^PW532" in fixed
    assert "^LL203" in fixed
    assert "^LS0" in fixed
    assert "^PW800" not in fixed
    assert "^LL400" not in fixed


def test_render_status_label_no_leftover_placeholders():
    zpl = render_status_label("PICKUP")
    assert "{STATUS_NAME}" not in zpl
    assert "{BARCODE}" not in zpl


def test_render_status_label_works_for_every_allowed_status_without_error():
    for name in ALLOWED_STATUSES:
        zpl = render_status_label(name)
        assert name in zpl
        assert zpl.startswith("^XA") and zpl.rstrip().endswith("^XZ")


def test_render_status_label_does_not_mutate_template_file_on_disk():
    from filetrack.labels.status_template import DEFAULT_STATUS_TEMPLATE_PATH

    before = open(DEFAULT_STATUS_TEMPLATE_PATH, encoding="utf-8").read()
    render_status_label("REJECTED")
    after = open(DEFAULT_STATUS_TEMPLATE_PATH, encoding="utf-8").read()
    assert before == after
    assert "{STATUS_NAME}" in after


def test_longest_status_name_barcode_fits_module_width_one():
    longest = max(ALLOWED_STATUSES, key=len)
    zpl = render_status_label(longest)
    assert "^BY1,2,60" in zpl
