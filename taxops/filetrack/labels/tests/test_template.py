"""M1 — pure-string tests for filetrack.labels.template.render_label().
No printer, no win32print import, no I/O beyond reading the template file."""
from __future__ import annotations

from filetrack.labels.template import barcode_payload_for_log, render_label


def test_render_label_contains_formatted_human_number():
    zpl = render_label("123")
    assert "LOG# 00123" in zpl


def test_render_label_contains_native_bc_barcode_block():
    zpl = render_label("123")
    # Native ^BC field — never a rasterized/graphic barcode.
    assert "^BY2,2,70" in zpl
    assert "^BCN,70,N,N,N" in zpl


def test_render_label_barcode_field_data_is_log_prefixed_payload():
    zpl = render_label("123")
    assert "^FD" + barcode_payload_for_log("123") + "^FS" in zpl
    assert "LOG:00123" in zpl


def test_render_label_preserves_template_structure():
    zpl = render_label("123")
    assert zpl.startswith("^XA")
    assert zpl.rstrip().endswith("^XZ")
    assert "^PW532" in zpl
    assert "^LL203" in zpl
    # Preserved layout elements from the supplied template.
    assert "^FO5,5^GB523,193,3^FS" in zpl          # outer border
    assert "^FO18,66^GB497,2,2^FS" in zpl           # divider
    assert "LOG-IN __/__/2026" in zpl               # intake date fill-in field
    assert "EXT   __/__/2026" in zpl                # extension date fill-in field


def test_render_label_zero_pads_to_five_digits():
    zpl = render_label(7)
    assert "LOG# 00007" in zpl
    assert "LOG:00007" in zpl


def test_render_label_does_not_mutate_template_file_on_disk():
    from filetrack.config import DEFAULT_TEMPLATE_PATH
    before = open(DEFAULT_TEMPLATE_PATH, encoding="utf-8").read()
    render_label("999")
    render_label("00042")
    after = open(DEFAULT_TEMPLATE_PATH, encoding="utf-8").read()
    assert before == after
    # The unrendered placeholders must still be present in the source file.
    assert "{LOGNUM}" in after
    assert "{BARCODE}" in after


def test_render_label_no_leftover_placeholders():
    zpl = render_label("456")
    assert "{LOGNUM}" not in zpl
    assert "{BARCODE}" not in zpl


def test_template_module_never_imports_printer():
    # M1: render_label() must be usable on a machine with no pywin32 at all —
    # template.py must never import filetrack.labels.printer (or win32print).
    import filetrack.labels.template as tmpl
    src = open(tmpl.__file__, encoding="utf-8").read()
    assert "win32print" not in src
    assert "filetrack.labels.printer" not in src
    assert "import printer" not in src
