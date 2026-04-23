import sqlite3
conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row

print("Current returns by tax_year:")
rows = conn.execute("""
    SELECT tax_year, COUNT(*) n,
           SUM(CASE WHEN log_number IS NOT NULL AND log_number != '' THEN 1 ELSE 0 END) has_log
    FROM returns GROUP BY tax_year ORDER BY tax_year DESC
""").fetchall()
for r in rows:
    print(f"  TY{r['tax_year']}: {r['n']} total, {r['has_log']} with log#")

print()
print("Intake year vs tax_year for log-numbered returns:")
rows2 = conn.execute("""
    SELECT strftime('%Y', intake_date) intake_yr, tax_year, COUNT(*) n
    FROM returns
    WHERE log_number IS NOT NULL AND log_number != '' AND intake_date IS NOT NULL
    GROUP BY intake_yr, tax_year
    ORDER BY intake_yr DESC, tax_year DESC
""").fetchall()
for r in rows2:
    print(f"  intake_year={r['intake_yr']}  tax_year={r['tax_year']}  count={r['n']}")

print()
print("Sample TY2026 returns (should not exist):")
samples = conn.execute("""
    SELECT c.last_name, c.first_name, r.log_number, r.intake_date, r.client_status
    FROM returns r JOIN clients c ON c.id = r.client_id
    WHERE r.tax_year = 2026
    LIMIT 10
""").fetchall()
for r in samples:
    print(f"  {r['last_name']}, {r['first_name']}  log={r['log_number']}  intake={r['intake_date']}  status={r['client_status']}")

conn.close()
