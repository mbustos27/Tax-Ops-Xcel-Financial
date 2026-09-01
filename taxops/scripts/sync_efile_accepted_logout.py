"""One-shot + ongoing sync: LOG OUT returns Drake marks e-file accepted."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_TAXOPS = Path(__file__).resolve().parents[1]
if str(_TAXOPS) not in sys.path:
    sys.path.insert(0, str(_TAXOPS))

from db import get_connection
from efile_logout_sync import sync_efile_accepted_logouts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Set LOG OUT on returns with Drake e-file acceptance (dry-run by default)",
    )
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run)")
    parser.add_argument("--tax-year", type=int, default=None, help="Limit to one tax year")
    args = parser.parse_args()

    conn = get_connection()
    try:
        if args.apply:
            conn.execute("BEGIN")
        result = sync_efile_accepted_logouts(
            conn,
            tax_year=args.tax_year,
            dry_run=not args.apply,
        )
        if args.apply:
            conn.commit()
    finally:
        conn.close()

    mode = "DRY-RUN" if result["dry_run"] else "APPLIED"
    print(f"[{mode}] tax_year={result['tax_year']!r}")
    print(f"  candidates: {result['candidate_count']}")
    print(f"  updated:    {result['updated']}")
    if result.get("samples"):
        print("  sample (first rows):")
        for s in result["samples"][:10]:
            print(
                f"    log {s.get('log_number')!s:>6} TY{s['tax_year']} "
                f"{s['from_status']} drake={s.get('drake_status_raw') or '-'}"
            )
    if not args.apply and result["candidate_count"]:
        print("\nRe-run with --apply to commit.")


if __name__ == "__main__":
    main()
