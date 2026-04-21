"""
create_demo_db.py
-----------------
Creates taxops_demo.db from the live taxops.db, ready for a TY2026
re-intake demo.

What it does:
  1. Copies the full live DB to taxops_demo.db
  2. Removes any TY2026 returns so the re-intake flow starts completely clean
  3. Clears import_batches / import_rows / review_queue (import history
     is irrelevant in demo)
  4. Resets all TY2025 return statuses back to LOG IN so every client
     looks like they need a new intake (optional — see RESET_STATUSES flag)

Usage:
  python create_demo_db.py
  python create_demo_db.py --keep-statuses   # don't reset to LOG IN

Then run the demo app:
  $env:TAXOPS_DB  = "taxops_demo.db"
  $env:TAXOPS_ENV = "demo"
  python -m flask --app app.py run --host=0.0.0.0 --port=5001
"""

import argparse
import shutil
import sqlite3
import sys
from pathlib import Path

HERE     = Path(__file__).parent
LIVE_DB  = HERE / "taxops.db"
DEMO_DB  = HERE / "taxops_demo.db"
DEMO_YEAR = 2026   # the year being prepped for


def main(reset_statuses: bool = True) -> None:
    if not LIVE_DB.exists():
        print(f"ERROR: live DB not found at {LIVE_DB}")
        sys.exit(1)

    # ── Copy live → demo ──────────────────────────────────────────────────
    shutil.copy2(LIVE_DB, DEMO_DB)
    print(f"Copied {LIVE_DB.name} -> {DEMO_DB.name}")

    conn = sqlite3.connect(DEMO_DB)
    conn.row_factory = sqlite3.Row

    # ── Remove any existing DEMO_YEAR returns (clean slate) ───────────────
    deleted = conn.execute(
        "DELETE FROM returns WHERE tax_year = ?", (DEMO_YEAR,)
    ).rowcount
    print(f"Removed {deleted} existing TY{DEMO_YEAR} return(s)")

    # ── Cascade-clean orphaned payments / forms / notes / events for those
    # (SQLite FK cascade requires FK pragma; do it manually to be safe)
    conn.execute(
        """DELETE FROM payments WHERE return_id NOT IN (SELECT id FROM returns)"""
    )
    conn.execute(
        """DELETE FROM return_forms WHERE return_id NOT IN (SELECT id FROM returns)"""
    )
    conn.execute(
        """DELETE FROM notes WHERE return_id NOT IN (SELECT id FROM returns)"""
    )
    conn.execute(
        """DELETE FROM status_events WHERE return_id NOT IN (SELECT id FROM returns)"""
    )
    conn.execute(
        """DELETE FROM dependents WHERE return_id NOT IN (SELECT id FROM returns)"""
    )

    # ── Clear import history (not relevant in demo) ───────────────────────
    conn.execute("DELETE FROM import_batches")
    conn.execute("DELETE FROM import_rows")
    conn.execute("DELETE FROM review_queue")
    print("Cleared import history")

    # ── Optionally reset all TY2025 statuses to LOG IN ────────────────────
    if reset_statuses:
        updated = conn.execute(
            """
            UPDATE returns
            SET client_status = 'LOG IN',
                efile_date    = NULL,
                ack_date      = NULL,
                pickup_date   = NULL,
                logout_date   = NULL
            WHERE tax_year = 2025
            """
        ).rowcount
        print(f"Reset {updated} TY2025 return(s) to LOG IN for demo")
    else:
        print("Keeping existing TY2025 statuses")

    conn.commit()
    conn.close()

    # ── Summary ───────────────────────────────────────────────────────────
    conn = sqlite3.connect(DEMO_DB)
    clients = conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
    returns = conn.execute("SELECT COUNT(*) FROM returns").fetchone()[0]
    conn.close()

    print(f"\nDemo DB ready: {DEMO_DB.name}")
    print(f"  {clients} clients")
    print(f"  {returns} returns (all TY2025, status reset to LOG IN)")
    print(f"\nTo launch demo app (PowerShell):")
    print(f'  $env:TAXOPS_DB  = "taxops_demo.db"')
    print(f'  $env:TAXOPS_ENV = "demo"')
    print(f'  python -m flask --app app.py run --host=0.0.0.0 --port=5001 --debug')
    print(f"\nLive app stays on port 5000. Demo runs on port 5001.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--keep-statuses",
        action="store_true",
        help="Do not reset TY2025 statuses to LOG IN",
    )
    args = parser.parse_args()
    main(reset_statuses=not args.keep_statuses)
