"""
merge_years.py
~~~~~~~~~~~~~~
The manual CSV used tax_year=2025 while Drake used tax_year=2026 for the
same returns (same filing season, different label convention).

This script:
  1. For every client that has BOTH a TY2025 and a TY2026 return, copies
     the log_number, processor, intake/pickup/logout dates from TY2025
     onto the TY2026 record (only where TY2026 has blanks).
  2. Deletes the now-redundant TY2025 duplicate.

For clients with ONLY a TY2025 return (no TY2026 from Drake), re-labels
it as TY2026 so everything lives in one year.
"""
import sqlite3
from utils import now

conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row
ts = now()

merged   = 0
relabeled = 0
skipped  = 0

# ── Find clients with both TY2025 and TY2026 returns ──────────────────────
pairs = conn.execute("""
    SELECT
        r25.id  AS id25,
        r26.id  AS id26,
        r25.log_number      AS log25,
        r25.client_status   AS status25,
        r25.processor       AS proc25,
        r25.intake_date     AS intake25,
        r25.pickup_date     AS pickup25,
        r25.logout_date     AS logout25,
        r26.log_number      AS log26,
        r26.processor       AS proc26
    FROM returns r25
    JOIN returns r26 ON r26.client_id = r25.client_id AND r26.tax_year = 2026
    WHERE r25.tax_year = 2025
      AND (r25.log_number IS NOT NULL AND r25.log_number != '')
""").fetchall()

for p in pairs:
    # Stamp TY2026 with manual log data (never overwrite existing values)
    conn.execute("""
        UPDATE returns SET
            log_number   = COALESCE(NULLIF(log_number,''),  ?),
            client_status = ?,
            processor    = COALESCE(NULLIF(processor,''),   ?),
            intake_date  = COALESCE(intake_date,  ?),
            pickup_date  = COALESCE(pickup_date,  ?),
            logout_date  = COALESCE(logout_date,  ?),
            updated_at   = ?
        WHERE id = ?
    """, (
        p["log25"], p["status25"], p["proc25"],
        p["intake25"], p["pickup25"], p["logout25"],
        ts, p["id26"]
    ))
    # Remove the TY2025 duplicate
    conn.execute("DELETE FROM returns WHERE id = ?", (p["id25"],))
    merged += 1

# ── Clients with ONLY a TY2025 return — re-label to TY2026 ───────────────
only25 = conn.execute("""
    SELECT r.id FROM returns r
    WHERE r.tax_year = 2025
      AND (r.log_number IS NOT NULL AND r.log_number != '')
      AND NOT EXISTS (
          SELECT 1 FROM returns r2
          WHERE r2.client_id = r.client_id AND r2.tax_year = 2026
      )
""").fetchall()

for row in only25:
    conn.execute(
        "UPDATE returns SET tax_year = 2026, updated_at = ? WHERE id = ?",
        (ts, row["id"])
    )
    relabeled += 1

# ── TY2025 returns with no log number and no TY2026 pair — leave alone ─────
remaining25 = conn.execute(
    "SELECT COUNT(*) n FROM returns WHERE tax_year = 2025"
).fetchone()["n"]

conn.commit()
conn.close()

print("Done.")
print(f"  Merged TY2025 -> TY2026 (had both)  : {merged}")
print(f"  Re-labeled TY2025 -> TY2026 (only 25): {relabeled}")
print(f"  TY2025 records still remaining       : {remaining25}")
