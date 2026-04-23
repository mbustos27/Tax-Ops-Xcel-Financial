"""
dedup_returns.py
~~~~~~~~~~~~~~~~
For any (client_id, tax_year) pair that has more than one return,
keep the best record (prefers the one with a log number, then the
one with more fields filled in) and delete the rest.
"""
import sqlite3
from utils import now

conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row
ts = now()

# Find all duplicate (client_id, tax_year) groups
dupes = conn.execute("""
    SELECT client_id, tax_year, COUNT(*) n
    FROM returns
    GROUP BY client_id, tax_year
    HAVING COUNT(*) > 1
    ORDER BY tax_year DESC
""").fetchall()

print(f"Duplicate (client, year) groups: {len(dupes)}")

deleted = 0
for d in dupes:
    cid, yr = d["client_id"], d["tax_year"]
    returns = conn.execute("""
        SELECT id, log_number, client_status, processor,
               intake_date, pickup_date, logout_date, ack_date,
               (CASE WHEN log_number  IS NOT NULL AND log_number  != '' THEN 10 ELSE 0 END +
                CASE WHEN processor   IS NOT NULL AND processor   != '' THEN 5  ELSE 0 END +
                CASE WHEN intake_date IS NOT NULL THEN 3 ELSE 0 END +
                CASE WHEN pickup_date IS NOT NULL THEN 2 ELSE 0 END +
                CASE WHEN logout_date IS NOT NULL THEN 2 ELSE 0 END +
                CASE WHEN ack_date    IS NOT NULL THEN 2 ELSE 0 END) AS score
        FROM returns
        WHERE client_id=? AND tax_year=?
        ORDER BY score DESC, id ASC
    """, (cid, yr)).fetchall()

    # Keep the best-scored record; merge fields from others into it
    keeper = returns[0]
    keep_id = keeper["id"]

    for loser in returns[1:]:
        # Copy any non-null fields from loser onto keeper if keeper is blank
        conn.execute("""
            UPDATE returns SET
                log_number   = COALESCE(NULLIF(log_number,''),   ?),
                processor    = COALESCE(NULLIF(processor,''),    ?),
                intake_date  = COALESCE(intake_date,  ?),
                pickup_date  = COALESCE(pickup_date,  ?),
                logout_date  = COALESCE(logout_date,  ?),
                updated_at   = ?
            WHERE id = ?
        """, (
            loser["log_number"], loser["processor"],
            loser["intake_date"], loser["pickup_date"], loser["logout_date"],
            ts, keep_id
        ))
        # Remove related data from loser before deleting
        conn.execute("DELETE FROM return_forms WHERE return_id=?", (loser["id"],))
        conn.execute("DELETE FROM payments    WHERE return_id=?", (loser["id"],))
        conn.execute("DELETE FROM notes       WHERE return_id=?", (loser["id"],))
        conn.execute("DELETE FROM returns     WHERE id=?",        (loser["id"],))
        deleted += 1

conn.commit()
conn.close()
print(f"Deleted {deleted} duplicate returns.")

# Quick summary
conn = sqlite3.connect("taxops.db")
row = conn.execute(
    "SELECT COUNT(*) total, "
    "SUM(CASE WHEN log_number IS NOT NULL AND log_number != '' THEN 1 ELSE 0 END) has_log "
    "FROM returns WHERE tax_year=2026"
).fetchone()
conn.close()
print(f"TY2026 after dedup: {row[0]} total, {row[1]} with log numbers")
