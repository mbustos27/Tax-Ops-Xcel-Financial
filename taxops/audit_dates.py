from db import get_connection
conn = get_connection()
with_date = conn.execute(
    "SELECT COUNT(*) FROM returns WHERE intake_date IS NOT NULL AND intake_date != ''"
).fetchone()[0]
total = conn.execute("SELECT COUNT(*) FROM returns").fetchone()[0]
print("Returns with intake_date: %d / %d" % (with_date, total))

rows = conn.execute(
    "SELECT r.log_number, r.tax_year, r.intake_date, c.last_name "
    "FROM returns r JOIN clients c ON c.id=r.client_id "
    "WHERE r.log_number GLOB '[0-9]*' AND CAST(r.log_number AS INTEGER) BETWEEN 1 AND 16 "
    "ORDER BY CAST(r.log_number AS INTEGER)"
).fetchall()
for r in rows:
    print("  log=%-4s  yr=%-6s  intake=%-12s  %s" % (
        r["log_number"], r["tax_year"], r["intake_date"] or "NONE", r["last_name"]
    ))

# Check import warnings (not errors)
from db import get_connection as gc
conn2 = gc()
warn_rows = conn2.execute(
    "SELECT COUNT(*) FROM import_rows WHERE error LIKE 'Invalid date%'"
).fetchone()[0]
print("\nRemaining 'Invalid date' warnings in import_rows: %d" % warn_rows)
conn.close()
conn2.close()
