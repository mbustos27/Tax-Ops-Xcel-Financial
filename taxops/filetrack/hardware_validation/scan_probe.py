"""M0 — standalone scanner probe.

Reads whatever the ScanAvenger sends (HID keyboard-wedge → stdin lines, or
serial → pyserial) and logs the EXACT repr() of each record plus the
wall-clock gap since the previous one, so an operator can determine:
  - whether the `:` in `LOG:00123` / `STATUS:FINALIZE` survives scanning,
  - what terminator character(s) the scanner appends,
  - whether a buffered burst (storage-mode) replays in the same order as a
    slow, deliberate, one-at-a-time scan.

Deliberately self-contained — does not import filetrack.listener — because
M0 is meant to be runnable before M2 exists, and to give an unopinionated,
minimally-processed view of the raw scan stream (M2's parser.py is the
"real" interpreter used everywhere else once M2 lands).

Usage:
    python -m filetrack.hardware_validation.scan_probe --mode hid
    python -m filetrack.hardware_validation.scan_probe --mode serial --port COM3
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

_LOG_PATH = Path(__file__).with_name("scan_probe_log.jsonl")


def _log_record(raw: str, gap_seconds: float | None) -> None:
    entry = {
        "raw_repr": repr(raw),
        "gap_seconds": gap_seconds,
        "logged_at": time.time(),
    }
    print(f"scan: {entry['raw_repr']}  (gap: {gap_seconds!r}s)")
    with _LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


def _run_hid() -> None:
    print(f"HID mode: reading stdin lines. Focus this window, then scan. Log: {_LOG_PATH}")
    print("Ctrl+C to stop.")
    last_ts: float | None = None
    for raw_line in sys.stdin:
        now = time.time()
        gap = None if last_ts is None else round(now - last_ts, 4)
        last_ts = now
        _log_record(raw_line, gap)


def _run_serial(port: str, baud: int) -> None:
    try:
        import serial  # type: ignore
    except ImportError:
        print(
            "pyserial is not installed. Add it to filetrack/requirements.txt "
            "and `pip install pyserial` to use --mode serial.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    print(f"Serial mode: {port} @ {baud} baud. Log: {_LOG_PATH}")
    print("Ctrl+C to stop.")
    last_ts: float | None = None
    with serial.Serial(port, baud, timeout=None) as ser:
        buf = b""
        while True:
            chunk = ser.read(1)
            if not chunk:
                continue
            buf += chunk
            if chunk in (b"\r", b"\n"):
                now = time.time()
                gap = None if last_ts is None else round(now - last_ts, 4)
                last_ts = now
                _log_record(buf.decode("utf-8", errors="replace"), gap)
                buf = b""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="M0 standalone scanner probe")
    parser.add_argument("--mode", choices=["hid", "serial"], default="hid")
    parser.add_argument("--port", default="COM3", help="Serial port (--mode serial only)")
    parser.add_argument("--baud", type=int, default=9600, help="Serial baud (--mode serial only)")
    args = parser.parse_args(argv)

    try:
        if args.mode == "hid":
            _run_hid()
        else:
            _run_serial(args.port, args.baud)
    except KeyboardInterrupt:
        print(f"\nStopped. Full log at {_LOG_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
