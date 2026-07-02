"""Phase 0.5 backfill migration.

Sets match_method='email_manual' on any return_documents row sourced from the
email_inbox holding-area whose match_method is still NULL (i.e. inserted
before the app.py fix that stopped relying on the column default).

match_confirmed is left untouched — those rows are already 1 (via the column
default) and that is the correct value under the current invariant, since
staff assignment IS the human confirmation for this path.

Usage:
    python scripts/_migrate_backfill_email_manual.py --db path/to/copy.db   # dry run + apply on a copy
    python scripts/_migrate_backfill_email_manual.py --db taxops.db --apply # apply for real
"""
import argparse
import sqlite3
import sys


SQL = (
    "UPDATE return_documents SET match_method='email_manual' "
    "WHERE source='email_inbox' AND match_method IS NULL"
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--apply", action="store_true", help="commit the change (default: dry run + rollback)")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    before = conn.execute(
        "SELECT COUNT(*) FROM return_documents WHERE source='email_inbox' AND match_method IS NULL"
    ).fetchone()[0]
    cur = conn.execute(SQL)
    affected = cur.rowcount
    after = conn.execute(
        "SELECT COUNT(*) FROM return_documents WHERE source='email_inbox' AND match_method IS NULL"
    ).fetchone()[0]
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    print(f"db={args.db} matching_before={before} rows_updated={affected} matching_after={after} integrity={integrity}")

    if args.apply and integrity == "ok":
        conn.commit()
        print("APPLIED (committed).")
    else:
        conn.rollback()
        print("DRY RUN (rolled back). Pass --apply to commit.")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
