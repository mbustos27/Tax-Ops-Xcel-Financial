"""filetrack.config — the ONE canonical place for:
  - the allowed status set (shared by listener M2 and TaxOps service M3)
  - the barcode payload scheme (locked: PREFIX:VALUE)
  - the canonical log-number format
  - hardware defaults, clearly flagged as PENDING M0 physical confirmation

Nothing in filetrack.labels or filetrack.listener may import TaxOps (app.py,
db.py, config.py at the repo root). This module is the only shared surface
between them and, later, the M3 integration layer.
"""
from __future__ import annotations

import os

# ── Allowed status set ──────────────────────────────────────────────────────
# Mirrors app.py's STATUS_FLOW (the return workflow) by design — a physical
# file's location tracks the same lifecycle stages — but is deliberately its
# own tuple here, not an import, because filetrack.labels/filetrack.listener
# must be usable/testable with zero TaxOps imports. If app.STATUS_FLOW changes,
# update this to match (M3's service layer is the natural place to add a
# consistency check/test between the two).
ALLOWED_STATUSES: tuple[str, ...] = (
    "PENDING INTAKE",
    "PROCESSING",
    "HOLD",
    "FINALIZE",
    "PICKUP",
    "EFILE READY",
    "LOG OUT",
    "REJECTED",
)

# ── Barcode payload scheme (locked architecture — do not change one side
# without the other: labels/template.py encodes it, listener/parser.py
# decodes it) ────────────────────────────────────────────────────────────────
LOG_PREFIX = "LOG"
STATUS_PREFIX = "STATUS"
PREFIX_DELIMITER = ":"

# ── Canonical log-number format ─────────────────────────────────────────────
# The one rule for turning a raw log number into its human-readable /
# barcode-payload form. Both filetrack.labels.template and
# filetrack.listener.parser must use this — never re-implement zero-padding
# elsewhere.
LOG_NUMBER_ZERO_PAD = 5


def format_log_number(log_number) -> str:
    """Zero-pad a numeric log number to LOG_NUMBER_ZERO_PAD digits.
    Non-numeric input (defensive — log numbers are normally digits-only) is
    returned stripped and unchanged rather than raising, since a barcode
    payload should never crash label rendering or the listener parser."""
    s = str(log_number).strip()
    return s.zfill(LOG_NUMBER_ZERO_PAD) if s.isdigit() else s


# ── M0 hardware defaults — ✅ CONFIRMED 2026-07-22 ──────────────────────────
# filetrack/hardware_validation/FINDINGS.md: mode=HID, suffix="\n", delimiter
# ":" all confirmed against the real Arkscan/4BARCODE printer + ScanAvenger
# scanner (print_test.py's 3 labels, a burst-vs-live comparison, and label
# geometry all checked out). Defaults below are now confirmed values, not
# just reasonable guesses. Still overridable via CLI flag or env var (see
# listener/run_listener.py --help) if a different scanner/printer is ever
# swapped in — re-run M0 and update FINDINGS.md before trusting a new unit.
HARDWARE_FINDINGS_CONFIRMED = True

DEFAULT_SCAN_MODE = os.environ.get("FILETRACK_SCAN_MODE", "hid")  # 'hid' | 'serial'
# Most 1D/2D scanners in HID keyboard-wedge mode append Enter (CR, LF, or
# CRLF) after each scan. Default to LF; --suffix / FILETRACK_SCAN_SUFFIX
# overrides once M0 confirms the ScanAvenger's actual terminator.
DEFAULT_SCAN_SUFFIX = os.environ.get("FILETRACK_SCAN_SUFFIX", "\n")
DEFAULT_SERIAL_PORT = os.environ.get("FILETRACK_SERIAL_PORT", "COM3")
DEFAULT_SERIAL_BAUD = int(os.environ.get("FILETRACK_SERIAL_BAUD", "9600"))

# ── Printer (M1) ─────────────────────────────────────────────────────────────
DEFAULT_PRINTER_NAME = os.environ.get("FILETRACK_PRINTER") or None

# ── Label geometry — confirmed: 2.625" x 1" @ 203dpi ────────────────────────
LABEL_WIDTH_DOTS = 532   # ^PW532
LABEL_HEIGHT_DOTS = 203  # ^LL203

DEFAULT_TEMPLATE_PATH = os.path.join(
    os.path.dirname(__file__), "labels", "LABEL_CLEAN_EDITABLE.zpl"
)

# ── M3 TaxOps integration (added here now so there is still only ONE place
# for filetrack config once M3 lands) ───────────────────────────────────────
FILETRACK_ENABLED = os.environ.get("FILETRACK_ENABLED", "false").lower() == "true"
FILETRACK_TOKEN = os.environ.get("FILETRACK_TOKEN", "")
FILETRACK_ENDPOINT_URL = os.environ.get(
    "FILETRACK_ENDPOINT_URL", "http://127.0.0.1:5000/filetrack/status"
)

# ── Print relay mode (post-go-live fix) ─────────────────────────────────────
# Discovered in production: the process that runs the intake print hook (a
# Windows *service*, Session 0) has no access to a printer that is only
# physically attached to a different machine (e.g. a front-desk workstation).
# win32print.OpenPrinter() only sees (a) printers truly installed as local
# queues on THIS machine, or (b) printers redirected into an interactive RDP
# session — never a plain "network share" of another box's local printer
# without extra Windows config. "local" mode (default, unchanged M1 behavior)
# calls win32print directly and requires the printer to be a real local queue
# on the machine running the caller. "relay" mode instead POSTs the print job
# to filetrack.relay.server running ON the machine with the physical printer;
# see filetrack/relay/server.py + filetrack/DEPLOYMENT.md.
FILETRACK_PRINT_MODE = os.environ.get("FILETRACK_PRINT_MODE", "local").strip().lower()  # 'local' | 'relay'
FILETRACK_RELAY_URL = os.environ.get("FILETRACK_RELAY_URL", "http://127.0.0.1:8765/print")
FILETRACK_RELAY_TOKEN = os.environ.get("FILETRACK_RELAY_TOKEN", "")
try:
    FILETRACK_RELAY_TIMEOUT_SEC = float(os.environ.get("FILETRACK_RELAY_TIMEOUT_SEC", "10"))
except ValueError:
    FILETRACK_RELAY_TIMEOUT_SEC = 10.0
