import sqlite3
conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row
rows = conn.execute("""
    SELECT c.last_name, c.first_name, r.log_number, r.client_status, r.tax_year
    FROM returns r JOIN clients c ON c.id = r.client_id
    WHERE r.tax_year = 2026
      AND r.log_number IS NOT NULL AND r.log_number != ''
    ORDER BY CAST(r.log_number AS INTEGER) DESC
    LIMIT 20
""").fetchall()
print("Top log numbers in TY2026:")
for r in rows:
    ln = r["last_name"] or ""
    fn = r["first_name"] or ""
    print(f"  LOG {str(r['log_number']):>5}  {ln}, {fn}  [{r['client_status']}]")

total_2026 = conn.execute("SELECT COUNT(*) FROM returns WHERE tax_year=2026").fetchone()[0]
with_log   = conn.execute("SELECT COUNT(*) FROM returns WHERE tax_year=2026 AND log_number IS NOT NULL AND log_number != ''").fetchone()[0]
print(f"\nTY2026 totals: {with_log} of {total_2026} have log numbers")
conn.close()
