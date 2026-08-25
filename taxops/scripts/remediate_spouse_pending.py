"""
remediate_spouse_pending.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Clean up bad pending spouse rows and re-import from Drake CSV using
primary-taxpayer matching (see import_spouse_info.py).

Usage:

    python scripts/remediate_spouse_pending.py --dry-run
    python scripts/remediate_spouse_pending.py --apply
    python scripts/remediate_spouse_pending.py --apply --csv CSVFILES\\TY2024Spouses.csv --combined-format
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_TAXOPS = os.path.dirname(_HERE)
sys.path.insert(0, _TAXOPS)

from config import DB_PATH  # noqa: E402
from db import get_connection  # noqa: E402
from import_spouse_info import run_import  # noqa: E402


def reject_pending(conn: sqlite3.Connection, dry_run: bool) -> int:
    rows = conn.execute(
        "SELECT id, client_id, taxpayer_name FROM spouses WHERE needs_review=1"
    ).fetchall()
    if dry_run:
        print(f"Would delete {len(rows)} pending spouse rows:")
        for r in rows:
            tp = r["taxpayer_name"] or "—"
            print(f"  id={r['id']} client={r['client_id']} drake={tp}")
        return len(rows)

    for r in rows:
        conn.execute("DELETE FROM spouses WHERE id=?", (r["id"],))
    conn.commit()
    print(f"Deleted {len(rows)} pending spouse rows.")
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Reject bad pending spouses and re-import")
    parser.add_argument("--db", default=None, help="SQLite path override")
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run)")
    parser.add_argument("--csv", default=None, help="Re-import from this CSV after cleanup")
    parser.add_argument(
        "--combined-format",
        action="store_true",
        help="TY2024-style CSV (Taxpayer Name + Spouse Name columns)",
    )
    parser.add_argument("--skip-reimport", action="store_true", help="Only delete pending rows")
    args = parser.parse_args()

    dry_run = not args.apply
    if dry_run:
        print("DRY RUN — pass --apply to write changes\n")

    conn = get_connection(args.db)
    deleted = reject_pending(conn, dry_run)
    conn.close()

    if args.skip_reimport or not args.csv:
        if not args.csv and not args.skip_reimport:
            print("\nNo --csv provided; skipped re-import.")
            print("After --apply, run import_spouse_info.py with your Drake spouse CSV.")
        return

    if dry_run:
        print(f"\nWould re-import from {args.csv}")
        run_import(
            args.csv,
            combined_format=args.combined_format,
            dry_run=True,
            db_path=args.db,
        )
    else:
        print(f"\nRe-importing from {args.csv} ...")
        run_import(
            args.csv,
            combined_format=args.combined_format,
            dry_run=False,
            db_path=args.db,
        )
        print(f"\nDone. Deleted {deleted} bad rows; check /admin/spouses-review for new pending.")


if __name__ == "__main__":
    main()
