import sqlite3
conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row

print("Tax years WITH log numbers (current season work):")
rows = conn.execute("""
    SELECT tax_year, COUNT(*) n,
           MIN(CAST(log_number AS INTEGER)) min_log,
           MAX(CAST(log_number AS INTEGER)) max_log
    FROM returns
    WHERE log_number IS NOT NULL AND log_number != ''
    GROUP BY tax_year ORDER BY tax_year DESC
""").fetchall()
for r in rows:
    ty = r["tax_year"]
    print(f"  TY{ty}: {r['n']} returns  logs {r['min_log']} - {r['max_log']}")

print()
print("Intake date year distribution (returns with log numbers):")
rows2 = conn.execute("""
    SELECT strftime('%Y', intake_date) yr, COUNT(*) n
    FROM returns
    WHERE intake_date IS NOT NULL
      AND log_number IS NOT NULL AND log_number != ''
    GROUP BY yr ORDER BY yr DESC
""").fetchall()
for r in rows2:
    print(f"  Intake year {r['yr']}: {r['n']} returns")

print()
print("Sample multi-year clients (same client, multiple log numbers):")
multi = conn.execute("""
    SELECT c.last_name, c.first_name, r.tax_year, r.log_number
    FROM returns r JOIN clients c ON c.id = r.client_id
    WHERE r.log_number IS NOT NULL AND r.log_number != ''
      AND r.client_id IN (
          SELECT client_id FROM returns
          WHERE log_number IS NOT NULL AND log_number != ''
          GROUP BY client_id HAVING COUNT(DISTINCT tax_year) > 1
      )
    ORDER BY c.last_name, r.tax_year DESC
    LIMIT 20
""").fetchall()
for r in multi:
    ln = r["last_name"] or ""
    fn = r["first_name"] or ""
    print(f"  {ln}, {fn}  TY{r['tax_year']}  LOG {r['log_number']}")

conn.close()
