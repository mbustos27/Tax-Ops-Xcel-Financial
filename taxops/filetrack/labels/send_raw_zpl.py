"""filetrack.labels.send_raw_zpl — send an already-rendered .zpl file
straight to a printer via win32print RAW. No template, no render_label() —
the file's bytes go to the printer exactly as saved.

Use this for the generated samples in filetrack/labels/samples/, or any
hand-edited .zpl file. To print a *new* label with a specific log number or
status name instead, use print_label.py / status_codes.py, which render
from the templates and then send.

Usage:
    python -m filetrack.labels.send_raw_zpl --list-printers
    python -m filetrack.labels.send_raw_zpl --file filetrack/labels/samples/sample_STATUS_FINALIZE.zpl --printer "Arkscan 2054A"
    python -m filetrack.labels.send_raw_zpl --file filetrack/labels/samples/sample_all_statuses.zpl --printer "Arkscan 2054A"
"""
from __future__ import annotations

import argparse
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Send a raw .zpl file straight to a printer, unmodified")
    parser.add_argument("--file", help="Path to a .zpl file (sent exactly as saved)")
    parser.add_argument("--printer", default=None, help="Windows printer name (defaults to FILETRACK_PRINTER)")
    parser.add_argument("--list-printers", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Print the file's contents instead of sending")
    args = parser.parse_args(argv)

    from filetrack.labels.printer import (
        PrinterNotFoundError,
        PrinterUnavailableError,
        SpoolerError,
        list_printers,
        send_zpl,
    )

    if args.list_printers:
        for name in list_printers():
            print(name)
        return 0

    if not args.file:
        print("Provide --file PATH.zpl (or --list-printers).", file=sys.stderr)
        return 1

    with open(args.file, encoding="utf-8") as fh:
        zpl = fh.read()

    if args.dry_run:
        print(zpl)
        return 0

    try:
        send_zpl(zpl, printer_name=args.printer)
    except (PrinterUnavailableError, PrinterNotFoundError, SpoolerError) as exc:
        print(f"Print failed: {exc}", file=sys.stderr)
        return 1

    print(f"Sent {args.file} to {args.printer or '(default FILETRACK_PRINTER)'}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
