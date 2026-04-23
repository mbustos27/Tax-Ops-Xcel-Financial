import sqlite3
conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row

# Count returns per year and how many have log numbers
rows = conn.execute("""
    SELECT tax_year,
           COUNT(*) total,
           SUM(CASE WHEN log_number IS NOT NULL AND log_number != '' THEN 1 ELSE 0 END) has_log,
           SUM(CASE WHEN log_number IS NULL OR log_number = '' THEN 1 ELSE 0 END) no_log
    FROM returns
    GROUP BY tax_year
    ORDER BY tax_year DESC
""").fetchall()

print(f"{'Year':<8} {'Total':<8} {'Has Log#':<12} {'No Log#'}")
print("-" * 40)
for r in rows:
    print(f"{r['tax_year']:<8} {r['total']:<8} {r['has_log']:<12} {r['no_log']}")

# Show sample TY2026 returns with no log number
print("\nSample TY2026 returns WITHOUT log numbers:")
samples = conn.execute("""
    SELECT c.last_name, c.first_name, r.log_number, r.client_status, r.intake_date
    FROM returns r JOIN clients c ON c.id = r.client_id
    WHERE r.tax_year = 2026 AND (r.log_number IS NULL OR r.log_number = '')
    ORDER BY c.last_name LIMIT 8
""").fetchall()
for r in samples:
    print(f"  {r['last_name']}, {r['first_name']}  status={r['client_status']}  intake={r['intake_date']}")

conn.close()
