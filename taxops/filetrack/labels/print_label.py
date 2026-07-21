"""filetrack.labels.print_label — render + send a file label to the printer.

Exposes print_label() as the one function both the CLI below and TaxOps's
M3 print hook (app.py's intake route, behind FILETRACK_ENABLED) call — so
there is exactly one place that turns a log number into ink on a label.

CLI usage:
    python -m filetrack.labels.print_label --log 123 --dry-run
    python -m filetrack.labels.print_label --log 123 --printer "Arkscan 2054A"
    python -m filetrack.labels.print_label --log 123 --template PATH/to/other.zpl
"""
from __future__ import annotations

import argparse
import sys

from filetrack.labels.template import render_label


def print_label(log_number, *, printer_name: str | None = None, template_path: str | None = None) -> str:
    """Render the label for `log_number` and send it to the printer (RAW
    ZPL via filetrack.labels.printer.send_zpl). Raises PrinterUnavailableError
    / PrinterNotFoundError / SpoolerError on failure — callers decide whether
    to swallow that (e.g. app.py's print hook logs+swallows; the CLI reports
    and exits non-zero). Returns the rendered ZPL string on success."""
    from filetrack.labels.printer import send_zpl

    zpl = render_label(log_number, template_path=template_path)
    send_zpl(zpl, printer_name=printer_name)
    return zpl


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render/print a filetrack file label")
    parser.add_argument("--log", required=True, help="Log number (e.g. 123)")
    parser.add_argument("--printer", default=None, help="Windows printer name (defaults to FILETRACK_PRINTER)")
    parser.add_argument("--template", default=None, help="Path to a ZPL template (defaults to LABEL_CLEAN_EDITABLE.zpl)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the rendered ZPL to stdout instead of sending it to a printer",
    )
    args = parser.parse_args(argv)

    if args.dry_run:
        print(render_label(args.log, template_path=args.template))
        return 0

    from filetrack.labels.printer import PrinterNotFoundError, PrinterUnavailableError, SpoolerError

    try:
        print_label(args.log, printer_name=args.printer, template_path=args.template)
    except (PrinterUnavailableError, PrinterNotFoundError, SpoolerError) as exc:
        print(f"Print failed: {exc}", file=sys.stderr)
        return 1

    print(f"Printed label for log #{args.log} to {args.printer or '(default FILETRACK_PRINTER)'}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
