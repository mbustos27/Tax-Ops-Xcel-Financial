"""Wave 4 — fold clients.spouse_* into spouses on live DB (schema v26 fragment)."""
from __future__ import annotations

import sqlite3
from pathlib import Path

DB = Path(r"T:\taxops\taxops.db")


def main() -> None:
    conn = sqlite3.connect(str(DB), timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    before = conn.execute("SELECT COUNT(*) FROM spouses").fetchone()[0]
    only = conn.execute(
        """
        SELECT COUNT(*) FROM clients c
        WHERE (
            (c.spouse_last_name IS NOT NULL AND TRIM(c.spouse_last_name) != '')
            OR (c.spouse_first_name IS NOT NULL AND TRIM(c.spouse_first_name) != '')
          )
          AND NOT EXISTS (SELECT 1 FROM spouses s WHERE s.client_id = c.id)
        """
    ).fetchone()[0]
    print("spouses_before", before, "clients_only", only)
    cur = conn.execute(
        """
        INSERT INTO spouses (
          client_id, first_name, last_name, date_of_birth, source, created_at
        )
        SELECT
          c.id,
          COALESCE(NULLIF(TRIM(c.spouse_first_name), ''), 'UNKNOWN'),
          NULLIF(TRIM(c.spouse_last_name), ''),
          NULLIF(TRIM(c.spouse_dob), ''),
          'wave4_clients_fold',
          datetime('now')
        FROM clients c
        WHERE (
            (c.spouse_last_name IS NOT NULL AND TRIM(c.spouse_last_name) != '')
            OR (c.spouse_first_name IS NOT NULL AND TRIM(c.spouse_first_name) != '')
          )
          AND NOT EXISTS (SELECT 1 FROM spouses s WHERE s.client_id = c.id)
        """
    )
    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM spouses").fetchone()[0]
    folded = conn.execute(
        "SELECT COUNT(*) FROM spouses WHERE source='wave4_clients_fold'"
    ).fetchone()[0]
    # stamp schema if possible
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(app_settings)")}
        if "updated_at" in cols:
            conn.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES ('schema_version','26', datetime('now')) "
                "ON CONFLICT(key) DO UPDATE SET value='26', updated_at=excluded.updated_at"
            )
        else:
            conn.execute(
                "INSERT INTO app_settings (key, value) VALUES ('schema_version','26') "
                "ON CONFLICT(key) DO UPDATE SET value='26'"
            )
        conn.commit()
    except sqlite3.Error as e:
        print("schema stamp skipped", e)
    print("inserted", cur.rowcount, "spouses_after", after, "wave4_fold_rows", folded)
    conn.close()


if __name__ == "__main__":
    main()
