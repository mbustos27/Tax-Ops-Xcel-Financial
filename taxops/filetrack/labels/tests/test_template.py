"""M1 — pure-string tests for filetrack.labels.template.render_label()."""
from __future__ import annotations

from filetrack.labels.template import (
    abbreviate_client_name,
    barcode_payload_for_log,
    render_label,
)


def test_render_label_contains_formatted_human_number():
    zpl = render_label("123")
    assert "LOG# 00123" in zpl


def test_render_label_contains_native_bc_barcode_block():
    zpl = render_label("123")
    assert "^BY2,2,70" in zpl
    assert "^BCN,70,N,N,N" in zpl


def test_abbreviate_client_name_last_first_initial():
    assert abbreviate_client_name(last_name="Smith", first_name="Jane") == "SMITH, J"


def test_render_label_includes_abbreviated_client_bottom_right():
    zpl = render_label("123", last_name="Garcia", first_name="Maria")
    assert "GARCIA, M" in zpl
    assert "{CLIENT}" not in zpl
    assert "^FO314,154^FB194,1,0,R,0" in zpl


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
    assert "^LS0" in zpl
    assert "^PQ1" in zpl
    assert "^CI28" in zpl
    assert "^GB523" not in zpl  # no outer border
    assert "^FO24,62^GB484,2,2^FS" in zpl  # full-band divider (24..508)
    from datetime import date
    today = date.today()
    expected = f"LOG-IN {today.month:02d}/{today.day:02d}/{today.year:04d}"
    assert expected in zpl
    assert f"EXT   __/__/{today.year:04d}" in zpl
    assert "LOG-IN __/__/" not in zpl
    assert "^LH0,0" in zpl
    assert "^FO24,12^FDLOG# " in zpl
    assert "^FO248,6^FB260,1,0,R,0^FDLOG-IN " in zpl
    assert "^FO132,76^BCN,70,N,N,N" in zpl


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
    assert "{LOGNUM}" in after
    assert "{BARCODE}" in after
    assert "{CLIENT}" in after
    assert "{LOG_IN_DATE}" in after
    assert "{EXT_YEAR}" in after


def test_render_label_no_leftover_placeholders():
    zpl = render_label("456", last_name="Lee")
    assert "{LOGNUM}" not in zpl
    assert "{BARCODE}" not in zpl
    assert "{CLIENT}" not in zpl
    assert "{LOG_IN_DATE}" not in zpl
    assert "{EXT_YEAR}" not in zpl


def test_render_label_log_in_date_override():
    zpl = render_label("123", log_in_date="2024-03-05")
    assert "LOG-IN 03/05/2024" in zpl
    assert "EXT   __/__/2024" in zpl


def test_format_log_in_date_defaults_to_today():
    from datetime import date
    from filetrack.labels.template import format_log_in_date

    d = date.today()
    assert format_log_in_date(None) == f"{d.month:02d}/{d.day:02d}/{d.year:04d}"
    assert format_log_in_date("2025-12-01") == "12/01/2025"
    assert format_log_in_date("1/2/2025") == "01/02/2025"


def test_template_module_never_imports_printer():
    import filetrack.labels.template as tmpl
    src = open(tmpl.__file__, encoding="utf-8").read()
    assert "win32print" not in src
    assert "filetrack.labels.printer" not in src
    assert "import printer" not in src
