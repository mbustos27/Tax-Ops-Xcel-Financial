"""Remove orphan client_billing rows that break init_db FK checks after DB rebuild."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

DB = Path(sys.argv[1] if len(sys.argv) > 1 else "T:/taxops/taxops.db")


def main() -> None:
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA foreign_keys=ON")
    before = conn.execute("PRAGMA foreign_key_check").fetchall()
    print(f"FK violations before: {len(before)}")
    cur = conn.execute(
        """
        DELETE FROM client_billing
        WHERE client_id NOT IN (SELECT id FROM clients)
        """
    )
    print(f"Deleted orphan client_billing rows: {cur.rowcount}")
    conn.commit()
    after = conn.execute("PRAGMA foreign_key_check").fetchall()
    print(f"FK violations after: {len(after)}")
    if after:
        print(after[:20])
        raise SystemExit(1)
    print("OK", DB)


if __name__ == "__main__":
    main()
