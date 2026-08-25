"""
reject_wave4_orphans.py
~~~~~~~~~~~~~~~~~~~~~~~
Detect Wave4-fold spouse rows (source contains wave4_clients_fold) with
0% / NULL confidence and no matching Drake MFJ spouse line.

Default is dry-run (print only). Pass --apply only after human review.

Never deletes rows with confirmed_at_intake=1 (manual protection).

Usage:

    python scripts/reject_wave4_orphans.py --db T:\\taxops\\taxops.db
    python scripts/reject_wave4_orphans.py --db T:\\taxops\\taxops.db --apply
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

_HERE = os.path.dirname(os.path.abspath(__file__))
_TAXOPS = os.path.dirname(_HERE)
sys.path.insert(0, _TAXOPS)

from db import get_connection  # noqa: E402
from import_spouse_info import (  # noqa: E402
    DEFAULT_CSV,
    _load_csv_rows,
    _resolve_derived_last,
)
from name_matcher import normalize_name  # noqa: E402

BASELINE_CONFIRMED = 463
BASELINE_PENDING = 0


def _spouse_key(first: str, last: str) -> str:
    return f"{normalize_name(first)}|{normalize_name(last)}"


def _drake_spouse_keys(csv_path: str, combined_format: bool) -> set[str]:
    keys: set[str] = set()
    for row in _load_csv_rows(csv_path, combined_format):
        tp = (row.get("Taxpayer Name") or "").strip()
        if not tp or tp.upper().startswith("TOTALS"):
            continue
        sp_first = (row.get("Spouse First Name") or "").strip()
        sp_last = (row.get("Spouse Last Name") or "").strip()
        derived, _ = _resolve_derived_last(sp_last, tp)
        if not sp_first:
            continue
        keys.add(_spouse_key(sp_first, sp_last or derived))
    return keys


def _counts(conn) -> tuple[int, int]:
    confirmed = conn.execute(
        "SELECT COUNT(*) FROM spouses WHERE needs_review=0"
    ).fetchone()[0]
    pending = conn.execute(
        "SELECT COUNT(*) FROM spouses WHERE needs_review=1"
    ).fetchone()[0]
    return int(confirmed), int(pending)


def find_orphans(conn, drake_keys: set[str]) -> list:
    """
    Wave4 fold, zero/NULL confidence, not intake-confirmed, spouse name
    not present on any Drake MFJ line.
    """
    rows = conn.execute(
        """
        SELECT id, client_id, first_name, last_name, derived_last_name,
               match_confidence, source, confirmed_at_intake, taxpayer_name
        FROM spouses
        WHERE source LIKE '%wave4_clients_fold%'
          AND (match_confidence IS NULL OR match_confidence = 0)
          AND COALESCE(confirmed_at_intake, 0) = 0
        ORDER BY client_id
        """
    ).fetchall()

    orphans = []
    for r in rows:
        sp_first = (r["first_name"] or "").strip()
        sp_last = (r["last_name"] or r["derived_last_name"] or "").strip()
        if not sp_first:
            orphans.append(r)
            continue
        if _spouse_key(sp_first, sp_last) in drake_keys:
            continue  # Drake has this spouse somewhere — keep
        orphans.append(r)
    return orphans


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dry-run / reject Wave4 orphan spouse rows (0% conf, no Drake line)"
    )
    parser.add_argument("--db", default=None)
    parser.add_argument("--csv", default=DEFAULT_CSV)
    parser.add_argument("--combined-format", action="store_true")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="DELETE orphans (default: dry-run). Only after human sign-off.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        print(f"ERROR: Drake CSV not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    dry_run = not args.apply
    if dry_run:
        print("DRY RUN — pass --apply only after reviewing the list below\n")
    else:
        print("APPLY mode — deleting Wave4 orphans with no Drake spouse line\n")

    drake_keys = _drake_spouse_keys(args.csv, args.combined_format)
    conn = get_connection(args.db)
    before_c, before_p = _counts(conn)
    orphans = find_orphans(conn, drake_keys)

    print(f"Candidates: {len(orphans)}")
    print("client_ids:")
    # IDs only in the summary list (minimize PII in console); names only if few
    ids = [int(r["client_id"]) for r in orphans]
    print(" ", ", ".join(str(i) for i in ids) if ids else "(none)")

    if dry_run:
        print("\nNo writes performed.")
    else:
        for r in orphans:
            conn.execute("DELETE FROM spouses WHERE id=?", (r["id"],))
        conn.commit()
        print(f"\nDeleted {len(orphans)} Wave4 orphan rows.")

    after_c, after_p = _counts(conn)
    conn.close()

    print("\n=== Count summary ===")
    print(f"Baseline (known): confirmed~={BASELINE_CONFIRMED} pending~={BASELINE_PENDING}")
    print(f"Before:           confirmed={before_c} pending={before_p}")
    print(f"After:            confirmed={after_c} pending={after_p}")
    print(
        f"Delta:            confirmed={after_c - before_c} pending={after_p - before_p}"
    )


if __name__ == "__main__":
    main()
