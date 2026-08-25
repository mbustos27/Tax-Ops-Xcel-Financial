"""POST a .zpl file to the print relay as raw ZPL (requires relay with zpl support).

Usage (from taxops/):
  python scripts/print_raw_zpl_via_relay.py filetrack/labels/samples/sample_LOG_MARGIN_TEST.zpl
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if not args:
        print("Usage: print_raw_zpl_via_relay.py PATH.zpl", file=sys.stderr)
        return 2
    zpl_path = Path(args[0])
    if not zpl_path.is_file():
        print(f"File not found: {zpl_path}", file=sys.stderr)
        return 1

    # Load taxops/.env the same way config does (optional).
    try:
        import config  # noqa: F401
    except Exception:
        pass

    url = os.environ.get("FILETRACK_RELAY_URL", "http://127.0.0.1:8765/print")
    token = os.environ.get("FILETRACK_RELAY_TOKEN", "")
    zpl = zpl_path.read_text(encoding="utf-8")
    body = json.dumps({"zpl": zpl}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            **({"X-Filetrack-Token": token} if token else {}),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            print(f"HTTP {resp.status}: {raw}")
            return 0 if resp.status == 200 else 1
    except urllib.error.HTTPError as exc:
        print(f"HTTP {exc.code}: {exc.read().decode('utf-8', errors='replace')}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Relay request failed: {exc}", file=sys.stderr)
        print(
            "Start the relay on the print PC first: T:\\start_print_relay.bat "
            "(or NSSM FiletrackRelay), then re-run.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
