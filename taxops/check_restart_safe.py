#!/usr/bin/env python3
"""CLI: is it safe to restart the TaxOps service?

Exit codes:
  0  SAFE
  2  CAUTION (warn desk, then OK)
  1  WAIT (do not restart)
  3  error
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DB_PATH  # noqa: E402
from db import get_connection, init_db  # noqa: E402
from restart_guard import (  # noqa: E402
    DEFAULT_IDLE_MINUTES,
    DEFAULT_WRITE_MINUTES,
    assess_restart,
    format_assessment_text,
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Check whether TaxOps is safe to restart")
    ap.add_argument("--db", default=os.environ.get("TAXOPS_DB") or DB_PATH)
    ap.add_argument("--idle-minutes", type=int, default=DEFAULT_IDLE_MINUTES)
    ap.add_argument("--write-minutes", type=int, default=DEFAULT_WRITE_MINUTES)
    ap.add_argument("--json", action="store_true", help="Print JSON instead of text")
    args = ap.parse_args(argv)

    conn = get_connection(args.db)
    try:
        init_db(conn)
        assessment = assess_restart(
            conn,
            db_path=args.db,
            idle_minutes=args.idle_minutes,
            write_minutes=args.write_minutes,
        )
    finally:
        conn.close()

    if args.json:
        import json

        print(json.dumps(assessment.to_dict(), indent=2))
    else:
        print(format_assessment_text(assessment))

    if assessment.verdict == "SAFE":
        return 0
    if assessment.verdict == "CAUTION":
        return 2
    if assessment.verdict == "WAIT":
        return 1
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
