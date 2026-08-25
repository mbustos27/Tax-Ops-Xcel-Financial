"""
check_service_health.py
~~~~~~~~~~~~~~~~~~~~~~~
Hit TaxOps /health and verify DB path (and optionally size) match expectations.

Exit 0 on success, non-zero on failure. Prints path + size only — never secrets.

Usage:

    python scripts/check_service_health.py
    python scripts/check_service_health.py --url http://192.168.1.173:5000/health
    python scripts/check_service_health.py --expect-path "C:\\TaxOps\\taxops\\taxops.db"
    python scripts/check_service_health.py --expect-size-file T:\\taxops\\taxops.db
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


DEFAULT_URL = "http://192.168.1.173:5000/health"
DEFAULT_EXPECT_PATH = r"C:\TaxOps\taxops\taxops.db"


def fetch_health(url: str, timeout: float = 10.0) -> dict:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify TaxOps /health DB path/size")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument(
        "--expect-path",
        default=DEFAULT_EXPECT_PATH,
        help="Expected db.path from /health (local server path)",
    )
    parser.add_argument(
        "--expect-size-file",
        default=None,
        help="Local file whose size should match db.size_bytes (e.g. T:\\taxops\\taxops.db)",
    )
    parser.add_argument(
        "--size-tolerance",
        type=int,
        default=0,
        help="Allowed absolute size delta in bytes (default 0)",
    )
    args = parser.parse_args()

    try:
        health = fetch_health(args.url)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"FAIL: cannot reach {args.url}: {exc}", file=sys.stderr)
        return 2

    db = health.get("db") or {}
    path = db.get("path")
    size = db.get("size_bytes")
    ok = db.get("ok")

    print(f"status:   {health.get('status')}")
    print(f"db.ok:    {ok}")
    print(f"db.path:  {path}")
    print(f"db.size:  {size}")

    rc = 0
    if not ok or health.get("status") != "ok":
        print("FAIL: health status not ok", file=sys.stderr)
        rc = 1

    if args.expect_path and path != args.expect_path:
        print(
            f"FAIL: path mismatch — expected {args.expect_path!r}, got {path!r}",
            file=sys.stderr,
        )
        rc = 1

    if args.expect_size_file:
        if not os.path.exists(args.expect_size_file):
            print(f"FAIL: size file missing: {args.expect_size_file}", file=sys.stderr)
            return 1
        local_size = os.path.getsize(args.expect_size_file)
        print(f"local:    {args.expect_size_file} size={local_size}")
        if size is None or abs(int(size) - local_size) > args.size_tolerance:
            print(
                f"FAIL: size mismatch — health={size} local={local_size}",
                file=sys.stderr,
            )
            rc = 1

    if rc == 0:
        print("OK")
    return rc


if __name__ == "__main__":
    sys.exit(main())
