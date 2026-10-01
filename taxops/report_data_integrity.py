#!/usr/bin/env python3
"""Office data-integrity report for TaxOps (C:\\TaxOps\\taxops).

Prints the remaining Medium findings from the full-system audit that are about
*data* rather than auth/RBAC (those ship in PR #275).

Usage (from repo root or taxops/):
  python taxops/report_data_integrity.py
  python taxops/report_data_integrity.py --db C:/TaxOps/taxops/taxops.db
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from config import DB_PATH  # noqa: E402

CANONICAL = frozenset(
    {
        "PROCESSING",
        "HOLD",
        "FINALIZE",
        "PICKUP",
        "EFILE READY",
        "LOG OUT",
        "REJECTED",
        "CANCELLED",
    }
)


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def report(conn: sqlite3.Connection) -> int:
    """Print findings. Return non-zero if any actionable integrity issue exists."""
    issues = 0

    print("=== TaxOps data integrity report ===")
    print(f"DB: {conn.execute('PRAGMA database_list').fetchone()['file'] or '(memory)'}")

    dups = conn.execute(
        """
        SELECT log_number, tax_year, COUNT(*) AS cnt,
               GROUP_CONCAT(id) AS return_ids
          FROM returns
         WHERE log_number IS NOT NULL AND TRIM(log_number) != ''
         GROUP BY log_number, tax_year
        HAVING cnt > 1
         ORDER BY tax_year, CAST(log_number AS INTEGER)
        """
    ).fetchall()
    print(f"\n[1] Duplicate (log_number, tax_year): {len(dups)} pairs")
    for r in dups[:25]:
        print(f"    log={r['log_number']} year={r['tax_year']} count={r['cnt']} ids={r['return_ids']}")
    if len(dups) > 25:
        print(f"    … {len(dups) - 25} more")
    issues += len(dups)

    nolog = conn.execute(
        "SELECT COUNT(*) AS n FROM returns WHERE log_number IS NULL OR TRIM(log_number)=''"
    ).fetchone()["n"]
    print(f"\n[2] Returns with no log number: {nolog}")
    issues += int(nolog > 0)

    bad = conn.execute(
        """
        SELECT COALESCE(client_status, '(null)') AS status, COUNT(*) AS cnt
          FROM returns
         WHERE client_status IS NULL
            OR client_status NOT IN (
                 'PROCESSING','HOLD','FINALIZE','PICKUP','EFILE READY',
                 'LOG OUT','REJECTED','CANCELLED'
               )
         GROUP BY client_status
         ORDER BY cnt DESC
        """
    ).fetchall()
    bad_n = sum(int(r["cnt"]) for r in bad)
    print(f"\n[3] Non-canonical client_status values: {bad_n} rows")
    for r in bad:
        print(f"    {r['status']}: {r['cnt']}")
    issues += bad_n

    cy = conn.execute(
        """
        SELECT COUNT(*) AS n FROM (
          SELECT client_id, tax_year FROM returns
           GROUP BY client_id, tax_year HAVING COUNT(*) > 1
        )
        """
    ).fetchone()["n"]
    print(f"\n[4] Duplicate (client_id, tax_year): {cy}")
    issues += int(cy)

    for table in ("payments", "notes", "status_events", "efile_batch_items", "dependents"):
        try:
            n = conn.execute(
                f"""
                SELECT COUNT(*) AS n FROM {table} t
                LEFT JOIN returns r ON r.id = t.return_id
                WHERE r.id IS NULL
                """
            ).fetchone()["n"]
        except sqlite3.OperationalError:
            continue
        print(f"\n[5] Orphan {table}: {n}")
        issues += int(n)

    neg = conn.execute(
        """
        SELECT COUNT(*) AS n
          FROM payments
         WHERE total_fee IS NOT NULL
           AND COALESCE(fee_paid, 0) > (COALESCE(total_fee, 0) + COALESCE(cc_fee, 0) + 0.009)
        """
    ).fetchone()["n"]
    print(f"\n[6] Payments with fee_paid > total_fee+cc_fee (unexpected overpay): {neg}")

    pickup = conn.execute(
        "SELECT COUNT(*) AS n FROM returns WHERE client_status = 'PICKUP'"
    ).fetchone()["n"]
    pickup_readyish = conn.execute(
        """
        SELECT COUNT(*) AS n
          FROM returns r
          LEFT JOIN payments p ON p.return_id = r.id
         WHERE r.client_status = 'PICKUP'
           AND COALESCE(r.signatures_received, 0) = 1
           AND COALESCE(p.fee_paid, 0) > 0
           AND COALESCE(p.receipt_number, '') != ''
        """
    ).fetchone()["n"]
    print(f"\n[7] PICKUP queue: {pickup} (complete sig+pay+receipt: {pickup_readyish})")

    print("\n=== done ===")
    print(f"Actionable issue groups flagged: {issues}")
    return 1 if issues else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=os.environ.get("TAXOPS_DB") or DB_PATH)
    args = ap.parse_args()
    path = str(args.db)
    if not Path(path).exists():
        print(f"DB not found: {path}", file=sys.stderr)
        return 2
    conn = _connect(path)
    try:
        return report(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
