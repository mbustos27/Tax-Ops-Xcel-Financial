"""M0 — standalone printer probe.

Deliberately self-contained (does NOT import filetrack.labels) so M0 can run
before M1 exists / independently verify the printer works at all, with the
least code that could plausibly go wrong. filetrack.labels.printer (M1) is
the real send path used everywhere else once M1 lands.

Usage:
    python -m filetrack.hardware_validation.print_test --list-printers
    python -m filetrack.hardware_validation.print_test --printer "Arkscan 2054A"
    python -m filetrack.hardware_validation.print_test --printer "Arkscan 2054A" --dry-run
"""
from __future__ import annotations

import argparse
import sys

try:
    import win32print
    _HAVE_WIN32PRINT = True
except ImportError:
    _HAVE_WIN32PRINT = False


# Geometry confirmed: 2.625" x 1" @ 203dpi.
_PW, _LL = 532, 203

# Three small probes:
#   1. The exact scheme M1 uses for file labels — LOG:<number>.
#   2. The exact scheme M3 uses for status labels — STATUS:<NAME>.
#   3. A payload with a hyphen alongside the colon, in case the scanner's
#      keyboard-layout mapping mangles some punctuation but not others.
_PROBE_LABELS = [
    ("LOG:00123 (file-label scheme)", "LOG:00123"),
    ("STATUS:FINALIZE (status-label scheme)", "STATUS:FINALIZE"),
    ("LOG:00123-A (punctuation stress test)", "LOG:00123-A"),
]


def _render_probe_zpl(caption: str, payload: str) -> str:
    return (
        f"^XA\n"
        f"^PW{_PW}\n"
        f"^LL{_LL}\n"
        f"^FO20,20^A0N,28,28^FD{caption}^FS\n"
        f"^FO20,70^BY2\n"
        f"^BCN,80,Y,N,N\n"
        f"^FD{payload}^FS\n"
        f"^XZ\n"
    )


def _send_raw(zpl: str, printer_name: str) -> None:
    if not _HAVE_WIN32PRINT:
        raise RuntimeError(
            "pywin32 is not installed — printing requires Windows + pywin32. "
            "Use --dry-run to inspect the ZPL without a printer."
        )
    h = win32print.OpenPrinter(printer_name)
    try:
        job = win32print.StartDocPrinter(h, 1, ("filetrack M0 probe", None, "RAW"))
        try:
            win32print.StartPagePrinter(h)
            try:
                win32print.WritePrinter(h, zpl.encode("utf-8"))
            finally:
                win32print.EndPagePrinter(h)
        finally:
            win32print.EndDocPrinter(h)
    finally:
        win32print.ClosePrinter(h)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="M0 standalone printer probe")
    parser.add_argument("--printer", help="Exact Windows printer name")
    parser.add_argument("--list-printers", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Print ZPL to stdout, don't send")
    args = parser.parse_args(argv)

    if args.list_printers:
        if not _HAVE_WIN32PRINT:
            print("pywin32 not installed — cannot list printers.", file=sys.stderr)
            return 1
        for flags, desc, name, comment in win32print.EnumPrinters(2):
            print(name)
        return 0

    if not args.dry_run and not args.printer:
        print("Provide --printer NAME (see --list-printers) or use --dry-run.", file=sys.stderr)
        return 1

    for caption, payload in _PROBE_LABELS:
        zpl = _render_probe_zpl(caption, payload)
        if args.dry_run:
            print(f"# ---- {caption} ----")
            print(zpl)
        else:
            print(f"Sending: {caption} ...")
            _send_raw(zpl, args.printer)
    if not args.dry_run:
        print(f"Sent {len(_PROBE_LABELS)} probe label(s) to {args.printer!r}.")
        print("Now scan each one with the ScanAvenger and run scan_probe.py.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
