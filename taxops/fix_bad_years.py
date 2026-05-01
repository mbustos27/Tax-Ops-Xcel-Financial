"""Fix returns with malformed tax_year values (25 → 2025, 22025 → 2025)."""
from normalizer import normalize_tax_year
from db import get_connection

conn = get_connection()

# Find all returns with non-standard tax years
bad = conn.execute(
    "SELECT r.id, r.tax_year, r.log_number, c.last_name, c.first_name "
    "FROM returns r JOIN clients c ON c.id=r.client_id "
    "WHERE r.tax_year NOT BETWEEN 1990 AND 2099"
).fetchall()

if not bad:
    print("No bad tax years found.")
else:
    print("Found %d returns with bad tax_year:" % len(bad))
    for r in bad:
        fixed = normalize_tax_year(str(r["tax_year"]))
        print("  id=%-6d  tax_year=%-8s -> %-6s  log=%-6s  %s, %s" % (
            r["id"], r["tax_year"], fixed,
            r["log_number"] or "",
            r["last_name"], r["first_name"] or ""
        ))
        if fixed:
            conn.execute("UPDATE returns SET tax_year=? WHERE id=?", (fixed, r["id"]))

    conn.commit()
    print("\nAll fixed.")

conn.close()
