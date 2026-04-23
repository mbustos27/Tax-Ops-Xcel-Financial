"""
fix_tax_years.py
~~~~~~~~~~~~~~~~
Corrects all bad tax_year values in the returns table:
  - TY2026 → TY2025  (all current-season returns; TY2026 does not exist yet)
  - TY22025 → TY2025 (CSV typo)
  - TY25    → TY2025 (CSV typo)

Then deduplicates any (client_id, tax_year=2025) pairs that now collide,
keeping the record with the most complete data.
"""
import sqlite3
from utils import now

conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row
ts = now()

# ── Step 1: relabel bad years ──────────────────────────────────────────────
r1 = conn.execute("UPDATE returns SET tax_year=2025, updated_at=? WHERE tax_year=2026",  (ts,))
r2 = conn.execute("UPDATE returns SET tax_year=2025, updated_at=? WHERE tax_year=22025", (ts,))
r3 = conn.execute("UPDATE returns SET tax_year=2025, updated_at=? WHERE tax_year=25",    (ts,))
print(f"Relabeled TY2026  -> TY2025: {r1.rowcount}")
print(f"Relabeled TY22025 -> TY2025: {r2.rowcount}")
print(f"Relabeled TY25    -> TY2025: {r3.rowcount}")

# ── Step 2: dedup (client_id, tax_year=2025) collisions ───────────────────
dupes = conn.execute("""
    SELECT client_id, tax_year, COUNT(*) n
    FROM returns
    WHERE tax_year = 2025
    GROUP BY client_id, tax_year
    HAVING COUNT(*) > 1
""").fetchall()
print(f"\nDuplicate (client, TY2025) groups to merge: {len(dupes)}")

deleted = 0
for d in dupes:
    cid = d["client_id"]
    records = conn.execute("""
        SELECT id, log_number, client_status, processor,
               intake_date, pickup_date, logout_date, ack_date, efile_date,
               (CASE WHEN log_number  IS NOT NULL AND log_number  != '' THEN 20 ELSE 0 END +
                CASE WHEN processor   IS NOT NULL AND processor   != '' THEN  5 ELSE 0 END +
                CASE WHEN intake_date IS NOT NULL                        THEN  3 ELSE 0 END +
                CASE WHEN pickup_date IS NOT NULL                        THEN  2 ELSE 0 END +
                CASE WHEN logout_date IS NOT NULL                        THEN  2 ELSE 0 END +
                CASE WHEN ack_date    IS NOT NULL                        THEN  2 ELSE 0 END) AS score
        FROM returns
        WHERE client_id=? AND tax_year=2025
        ORDER BY score DESC, id ASC
    """, (cid,)).fetchall()

    keeper_id = records[0]["id"]
    for loser in records[1:]:
        # Promote any non-null fields from loser onto keeper
        conn.execute("""
            UPDATE returns SET
                log_number   = COALESCE(NULLIF(log_number,''),   ?),
                processor    = COALESCE(NULLIF(processor,''),    ?),
                intake_date  = COALESCE(intake_date,  ?),
                pickup_date  = COALESCE(pickup_date,  ?),
                logout_date  = COALESCE(logout_date,  ?),
                ack_date     = COALESCE(ack_date,     ?),
                efile_date   = COALESCE(efile_date,   ?),
                updated_at   = ?
            WHERE id = ?
        """, (
            loser["log_number"], loser["processor"],
            loser["intake_date"], loser["pickup_date"], loser["logout_date"],
            loser["ack_date"], loser["efile_date"],
            ts, keeper_id
        ))
        # Clean up child records before deleting
        conn.execute("DELETE FROM return_forms WHERE return_id=?", (loser["id"],))
        conn.execute("DELETE FROM payments    WHERE return_id=?", (loser["id"],))
        conn.execute("DELETE FROM notes       WHERE return_id=?", (loser["id"],))
        conn.execute("DELETE FROM returns     WHERE id=?",        (loser["id"],))
        deleted += 1

conn.commit()

# ── Summary ────────────────────────────────────────────────────────────────
print(f"Deleted {deleted} duplicate returns after merge.")
print()
print("Final tax_year distribution:")
rows = conn.execute("""
    SELECT tax_year, COUNT(*) n,
           SUM(CASE WHEN log_number IS NOT NULL AND log_number != '' THEN 1 ELSE 0 END) has_log
    FROM returns GROUP BY tax_year ORDER BY tax_year DESC
""").fetchall()
for r in rows:
    print(f"  TY{r['tax_year']}: {r['n']} total, {r['has_log']} with log#")

conn.close()
print("\nDone.")
