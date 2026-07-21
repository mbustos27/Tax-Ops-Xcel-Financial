"""filetrack.labels.status_codes — render/print STATUS station labels.

One label per status name in filetrack.config.ALLOWED_STATUSES (or a single
--status NAME). These are printed once and stuck to a shelf/bin/station —
staff scan one to set the sticky active_status, then scan a stack of LOG
file labels against it (see filetrack.listener).

Usage:
    python -m filetrack.labels.status_codes --all --dry-run
    python -m filetrack.labels.status_codes --status FINALIZE --printer "Arkscan 2054A"
"""
from __future__ import annotations

import argparse
import sys

from filetrack.config import ALLOWED_STATUSES
from filetrack.labels.status_template import render_status_label


def print_status_label(status_name: str, *, printer_name: str | None = None, template_path: str | None = None) -> str:
    """Render + send one status label. Raises the same printer exceptions as
    filetrack.labels.print_label.print_label (PrinterUnavailableError etc.)."""
    from filetrack.labels.printer import send_zpl

    zpl = render_status_label(status_name, template_path=template_path)
    send_zpl(zpl, printer_name=printer_name)
    return zpl


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render/print filetrack STATUS station labels")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--status", help="One status name (must be in filetrack.config.ALLOWED_STATUSES)")
    group.add_argument("--all", action="store_true", help="Render/print one label per allowed status")
    parser.add_argument("--printer", default=None, help="Windows printer name (defaults to FILETRACK_PRINTER)")
    parser.add_argument("--template", default=None, help="Path to a ZPL template (defaults to STATUS_LABEL.zpl)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print rendered ZPL to stdout instead of sending it to a printer",
    )
    args = parser.parse_args(argv)

    if args.status and args.status.upper() not in ALLOWED_STATUSES:
        print(f"Unknown status {args.status!r}. Allowed: {', '.join(ALLOWED_STATUSES)}", file=sys.stderr)
        return 1

    names = [args.status.upper()] if args.status else list(ALLOWED_STATUSES)

    if args.dry_run:
        for name in names:
            print(render_status_label(name, template_path=args.template))
        return 0

    from filetrack.labels.printer import PrinterNotFoundError, PrinterUnavailableError, SpoolerError

    for name in names:
        try:
            print_status_label(name, printer_name=args.printer, template_path=args.template)
        except (PrinterUnavailableError, PrinterNotFoundError, SpoolerError) as exc:
            print(f"Print failed for status {name!r}: {exc}", file=sys.stderr)
            return 1
        print(f"Printed STATUS label: {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
