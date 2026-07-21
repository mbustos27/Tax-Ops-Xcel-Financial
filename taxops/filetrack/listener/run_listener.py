"""filetrack.listener.run_listener — the M2 scan-listener loop.

Reads scans (HID keyboard-wedge stdin lines, or a serial COM port), classifies
each record, feeds the sticky StatusState machine, and routes
Assignments/Rejections to the configured AssignmentSink. Every scan,
assignment, and rejection is logged — there is no screen UI; the log (and the
gun's own beep) is the UI, per the M2 spec.

Usage:
    python -m filetrack.listener.run_listener
    python -m filetrack.listener.run_listener --mode serial --port COM3 --suffix "\\r\\n"
    python -m filetrack.listener.run_listener --sink stdout

No TaxOps import anywhere in this module — the M3 TaxOps-calling sink is
constructed and injected from TaxOps-side code, not from here.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone

from filetrack.config import (
    ALLOWED_STATUSES,
    DEFAULT_SCAN_MODE,
    DEFAULT_SCAN_SUFFIX,
    DEFAULT_SERIAL_BAUD,
    DEFAULT_SERIAL_PORT,
    HARDWARE_FINDINGS_CONFIRMED,
)
from filetrack.listener.parser import RECORD_KIND_STATUS, classify
from filetrack.listener.sink import AssignmentSink, CallbackSink, JsonlSink
from filetrack.listener.state import Assignment, Rejection, StatusState

logger = logging.getLogger("filetrack.listener")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def process_record(
    raw: str,
    state: StatusState,
    sink: AssignmentSink,
    *,
    suffix: str = "",
    now_iso: "callable[[], str]" = _now_iso,
) -> None:
    """Classify one raw scan record, apply it to `state`, route the outcome
    to `sink`. Never raises out to the caller — a bad scan (unknown prefix,
    unknown status, log-before-status) is logged and the read loop continues,
    per the M2 spec ("never crash the loop")."""
    kind, value = classify(raw, suffix=suffix)
    logger.info("scan kind=%s value=%r raw=%r", kind, value, raw)

    try:
        outcome = state.apply(kind, value)
    except Exception:
        logger.exception("state.apply crashed on kind=%s value=%r — skipping", kind, value)
        return

    if outcome is None:
        if kind == RECORD_KIND_STATUS:
            logger.info("status changed -> active_status=%r", state.active_status)
        return

    if isinstance(outcome, Assignment):
        ts = now_iso()
        logger.info("assignment log=%s status=%s ts=%s", outcome.log_number, outcome.status, ts)
        try:
            sink.submit(outcome.log_number, outcome.status, ts)
        except Exception:
            logger.exception(
                "sink.submit failed for log=%s status=%s — assignment lost, see sink implementation for retry behavior",
                outcome.log_number, outcome.status,
            )
        return

    if isinstance(outcome, Rejection):
        logger.warning("rejected kind=%s value=%r reason=%s", outcome.kind, outcome.value, outcome.reason)
        return


def run(
    records,
    *,
    allowed_statuses: tuple[str, ...] = ALLOWED_STATUSES,
    suffix: str = "",
    sink: AssignmentSink | None = None,
    now_iso=_now_iso,
) -> StatusState:
    """Drive the loop over an iterable of raw records — a live generator in
    production, a plain list in tests. This is the seam the M2 burst-vs-live
    tests use to prove correctness depends only on record ORDER, never on
    timing between records: pass a fixed/incrementing `now_iso` callable to
    make timestamps deterministic and compare two runs byte-for-byte.
    Returns the StatusState for introspection in tests."""
    state = StatusState(allowed_statuses)
    active_sink = sink or JsonlSink()
    for raw in records:
        process_record(raw, state, active_sink, suffix=suffix, now_iso=now_iso)
    return state


def _read_hid_lines():
    """HID keyboard-wedge mode: the scanner "types" into whatever has focus.
    Reading stdin line-by-line is the standard way to capture that when this
    process itself holds focus (e.g. run in its own console window)."""
    for raw_line in sys.stdin:
        yield raw_line


def _read_serial_lines(port: str, baud: int):
    """Serial (COM port) mode via pyserial. Auto-reconnects with a 3s pause
    on any connection error/drop — the dongle-host requirement (M3 deployment)
    means this loop should survive a scanner power-cycle without needing the
    whole listener service restarted."""
    try:
        import serial  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "pyserial is not installed — `pip install -r filetrack/requirements.txt` "
            "to use --mode serial."
        ) from exc

    while True:
        try:
            with serial.Serial(port, baud, timeout=None) as ser:
                logger.info("Serial connected: %s @ %s baud", port, baud)
                buf = b""
                while True:
                    chunk = ser.read(1)
                    if not chunk:
                        continue
                    buf += chunk
                    if chunk in (b"\r", b"\n"):
                        yield buf.decode("utf-8", errors="replace")
                        buf = b""
        except Exception:
            logger.exception("Serial connection to %s lost — reconnecting in 3s", port)
            time.sleep(3)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="filetrack scan listener (M2)")
    parser.add_argument("--mode", choices=["hid", "serial"], default=DEFAULT_SCAN_MODE)
    parser.add_argument("--port", default=DEFAULT_SERIAL_PORT, help="Serial port (--mode serial only)")
    parser.add_argument("--baud", type=int, default=DEFAULT_SERIAL_BAUD, help="Serial baud (--mode serial only)")
    parser.add_argument(
        "--suffix", default=DEFAULT_SCAN_SUFFIX,
        help="Scanner's record terminator. PENDING M0 confirmation — see filetrack/hardware_validation/README.md",
    )
    parser.add_argument("--sink", choices=["jsonl", "stdout", "http"], default="jsonl")
    parser.add_argument("--jsonl-path", default="assignments.jsonl")
    parser.add_argument("--http-url", default=None, help="--sink http only; defaults to FILETRACK_ENDPOINT_URL")
    parser.add_argument("--http-token", default=None, help="--sink http only; defaults to FILETRACK_TOKEN")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if not HARDWARE_FINDINGS_CONFIRMED:
        logger.warning(
            "M0 hardware findings not confirmed — using default mode=%r suffix=%r. "
            "See filetrack/hardware_validation/README.md before relying on this in production.",
            args.mode, args.suffix,
        )

    if args.sink == "jsonl":
        sink: AssignmentSink = JsonlSink(args.jsonl_path)
    elif args.sink == "http":
        from filetrack.listener.http_sink import build_http_sink
        sink = build_http_sink(url=args.http_url, token=args.http_token)
    else:
        sink = CallbackSink(lambda log_number, status, ts: print(f"{ts} {status} {log_number}"))

    logger.info("filetrack listener starting: mode=%s suffix=%r sink=%s", args.mode, args.suffix, args.sink)

    records = _read_hid_lines() if args.mode == "hid" else _read_serial_lines(args.port, args.baud)
    try:
        run(records, suffix=args.suffix, sink=sink)
    except KeyboardInterrupt:
        logger.info("Shutting down (KeyboardInterrupt).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
