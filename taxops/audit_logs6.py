import json
from db import get_connection
conn = get_connection()

# Look at early rows - what YR (tax_year) do they have?
print("=== Early rows: LOG 2025 vs YR (tax_year) vs what actually stored ===")
rows = conn.execute(
    "SELECT ir.row_number, ir.action, ir.raw_json "
    "FROM import_rows ir "
    "JOIN import_batches ib ON ib.id=ir.batch_id "
    "WHERE ib.filename LIKE '%TAX LOG%' "
    "ORDER BY ir.row_number LIMIT 80"
).fetchall()

for r in rows:
    d = {}
    try: d = json.loads(r["raw_json"] or "{}")
    except: pass
    log24 = d.get("LOG 2024", "")
    log25 = d.get("LOG 2025", "")
    yr = d.get("YR", "?")
    last = d.get("TAX PAYER NAME (S) LAST", d.get("LAST", "?"))
    # Find what was actually stored in DB
    # We can match by last_name + tax_year
    print("  row=%-4s  yr=%-6s  log24=%-6s  log25=%-6s  name=%-30s  action=%s" % (
        r["row_number"], yr, log24, log25, last[:30], r["action"]
    ))

# Now check what log numbers are stored for returns where the LOG 2025 value is 1-20
# These would be rows where LOG 2025 is set but tax_year != 2025
print()
print("=== Returns with log numbers 1-20 (any tax year) ===")
rets = conn.execute(
    "SELECT r.log_number, r.tax_year, c.last_name, c.first_name "
    "FROM returns r JOIN clients c ON c.id=r.client_id "
    "WHERE CAST(r.log_number AS INTEGER) BETWEEN 1 AND 20 "
    "ORDER BY CAST(r.log_number AS INTEGER), r.tax_year"
).fetchall()
for r in rets:
    print("  log=%-6s  yr=%-6s  %s, %s" % (r["log_number"], r["tax_year"], r["last_name"], r["first_name"] or ""))

# Show the 2024 returns with their log numbers
print()
print("=== 2024 tax year returns: log number distribution ===")
rets24 = conn.execute(
    "SELECT r.log_number, r.tax_year, c.last_name "
    "FROM returns r JOIN clients c ON c.id=r.client_id "
    "WHERE r.tax_year=2024 AND r.log_number IS NOT NULL AND r.log_number != '' "
    "ORDER BY CAST(r.log_number AS INTEGER) LIMIT 20"
).fetchall()
for r in rets24:
    print("  log=%-6s  %s" % (r["log_number"], r["last_name"]))

conn.close()
