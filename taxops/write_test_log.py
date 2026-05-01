#!/usr/bin/env python3
"""Run pytest in a subprocess and save full stdout/stderr to test_logs/.

Every normal `pytest` run also auto-writes:
  taxops/test_logs/pytest_latest.txt
  taxops/test_logs/pytest_<timestamp>.txt
(session summary + verification text from tests/conftest.py).

This script adds a verbatim capture (same lines as the terminal) in addition.

Usage (from taxops/):
    python write_test_log.py
    python write_test_log.py -m gh28
    python write_test_log.py tests/test_github_cards.py -k dashboard

Extra args are forwarded to pytest (default collection uses pytest.ini testpaths).
Output is UTF-8 with no ANSI color codes.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "test_logs"


def main() -> int:
    OUT_DIR.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = OUT_DIR / f"pytest_{stamp}.txt"

    # Plain text: no ANSI escape sequences in the file
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "--color=no",
        *sys.argv[1:],
    ]
    env = {**os.environ, "NO_COLOR": "1"}

    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    block = proc.stdout
    if proc.stderr:
        block += "\n--- stderr ---\n" + proc.stderr

    header = (
        f"TaxOps pytest log\n"
        f"Written: {stamp}\n"
        f"Command: {' '.join(cmd)}\n"
        f"Exit code: {proc.returncode}\n"
        f"{'=' * 72}\n\n"
    )
    log_path.write_text(header + block, encoding="utf-8")

    print(header + block, end="")
    print(f"\nSaved: {log_path}", file=sys.stderr)
    return proc.returncode


if __name__ == "__main__":
    # subprocess.os is wrong - use os.environ
    raise SystemExit(main())
