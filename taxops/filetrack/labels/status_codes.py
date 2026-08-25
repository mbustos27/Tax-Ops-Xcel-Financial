"""filetrack.labels.status_codes — render/print STATUS station labels.

One label per status name in filetrack.config.ALLOWED_STATUSES (or a single
--status NAME). These are printed once and stuck to a shelf/bin/station —
staff scan one to set the sticky active_status, then scan a stack of LOG
file labels against it (see filetrack.listener).

Label geometry is locked to 2.625" x 1" (532x203 @ 203dpi).

Usage:
    python -m filetrack.labels.status_codes --all --dry-run
    python -m filetrack.labels.status_codes --status FINALIZE --printer "4BARCODE 4B-2054A"
"""
from __future__ import annotations

import argparse
import sys

from filetrack.config import ALLOWED_STATUSES, LABEL_HEIGHT_DOTS, LABEL_WIDTH_DOTS
from filetrack.labels.status_template import render_status_label


def print_status_label(status_name: str, *, printer_name: str | None = None, template_path: str | None = None) -> str:
    """Render + send one status label. Raises the same printer exceptions as
    filetrack.labels.print_label.print_label (PrinterUnavailableError etc.)."""
    from filetrack.labels.printer import send_zpl

    zpl = render_status_label(status_name, template_path=template_path)
    send_zpl(zpl, printer_name=printer_name)
    return zpl


def print_status_labels(
    status_names: list[str],
    *,
    printer_name: str | None = None,
    template_path: str | None = None,
    separate_jobs: bool = False,
) -> str:
    """Render + send one or more status labels.

    Default: one Windows print job containing N labels (each ^XA…^XZ is still
    exactly 2.625x1). Separate jobs often cause extra media advance / waste
    between labels on 4BARCODE-class printers.
    """
    from filetrack.labels.printer import send_zpl

    parts = [render_status_label(n, template_path=template_path) for n in status_names]
    if separate_jobs:
        for zpl in parts:
            send_zpl(zpl, printer_name=printer_name)
        return "\n".join(parts)

    batch = "\n".join(parts)
    send_zpl(batch, printer_name=printer_name)
    return batch


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Render/print filetrack STATUS station labels "
            f"(locked {LABEL_WIDTH_DOTS}x{LABEL_HEIGHT_DOTS} dots = 2.625x1 in @ 203dpi)"
        )
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--status", help="One status name (must be in filetrack.config.ALLOWED_STATUSES)")
    group.add_argument("--all", action="store_true", help="Render/print one label per allowed status")
    parser.add_argument("--printer", default=None, help="Windows printer name (defaults to FILETRACK_PRINTER)")
    parser.add_argument("--template", default=None, help="Path to a ZPL template (defaults to STATUS_LABEL.zpl)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print rendered ZPL to stdout instead of sending it to a printer",
    )
    parser.add_argument(
        "--separate-jobs",
        action="store_true",
        help="Send each status as its own Windows print job (default: one batch job — less media waste)",
    )
    args = parser.parse_args(argv)

    if args.status:
        normalized = args.status.replace("_", " ").strip().upper()
        if normalized not in ALLOWED_STATUSES:
            print(f"Unknown status {args.status!r}. Allowed: {', '.join(ALLOWED_STATUSES)}", file=sys.stderr)
            return 1
        names = [normalized]
    else:
        names = list(ALLOWED_STATUSES)

    if args.dry_run:
        for name in names:
            print(render_status_label(name, template_path=args.template))
        return 0

    from filetrack.labels.printer import PrinterNotFoundError, PrinterUnavailableError, SpoolerError

    try:
        print_status_labels(
            names,
            printer_name=args.printer,
            template_path=args.template,
            separate_jobs=args.separate_jobs,
        )
    except (PrinterUnavailableError, PrinterNotFoundError, SpoolerError, ValueError) as exc:
        print(f"Print failed: {exc}", file=sys.stderr)
        return 1

    for name in names:
        print(f"Printed STATUS label: {name}  (2.625x1 in / ^{LABEL_WIDTH_DOTS}x{LABEL_HEIGHT_DOTS})")
    if len(names) > 1 and not args.separate_jobs:
        print(f"Sent {len(names)} labels in one print job (no extra form-feed between).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
