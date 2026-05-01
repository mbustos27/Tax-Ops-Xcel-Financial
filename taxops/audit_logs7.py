import json
from db import get_connection
conn = get_connection()

# Find ORMA SERVICES across all years to see what log# they have
print("=== ORMA SERVICES returns ===")
rets = conn.execute(
    "SELECT r.log_number, r.tax_year, c.last_name "
    "FROM returns r JOIN clients c ON c.id=r.client_id "
    "WHERE c.last_name LIKE '%ORMA%' ORDER BY r.tax_year"
).fetchall()
for r in rets: print("  log=%-6s yr=%-6s %s" % (r["log_number"], r["tax_year"], r["last_name"]))

# What log numbers exist for yr=2023?
print()
print("=== 2023 returns: log numbers ===")
rets23 = conn.execute(
    "SELECT r.log_number, c.last_name "
    "FROM returns r JOIN clients c ON c.id=r.client_id "
    "WHERE r.tax_year=2023 AND r.log_number IS NOT NULL AND r.log_number != '' "
    "ORDER BY CAST(r.log_number AS INTEGER) LIMIT 20"
).fetchall()
for r in rets23: print("  log=%-6s %s" % (r["log_number"], r["last_name"]))

# Key insight: for rows with yr != 2025, is LOG 2025 being used as log_number?
# Let's check rows that have yr=2023/2022/etc but log25 is populated
print()
print("=== LOG2025 for non-2025 returns: how many and what log numbers are stored? ===")
# Count returns per year
for yr in [2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015]:
    n = conn.execute(
        "SELECT COUNT(*) FROM returns WHERE tax_year=?", (yr,)
    ).fetchone()[0]
    with_log = conn.execute(
        "SELECT COUNT(*) FROM returns WHERE tax_year=? AND log_number IS NOT NULL AND log_number != ''",
        (yr,)
    ).fetchone()[0]
    if n > 0:
        print("  yr=%-6s  total=%-6d  with_log=%d" % (yr, n, with_log))

# Check if log numbers 1-140 are scattered across different tax years
print()
print("=== Log numbers 1-20 across ALL tax years ===")
for n in range(1, 21):
    rets = conn.execute(
        "SELECT r.log_number, r.tax_year, c.last_name "
        "FROM returns r JOIN clients c ON c.id=r.client_id "
        "WHERE r.log_number=?", (str(n),)
    ).fetchall()
    if rets:
        for r in rets:
            print("  log=%-4s yr=%-6s %s" % (r["log_number"], r["tax_year"], r["last_name"]))
    else:
        print("  log=%-4s  NOT IN DB" % n)

conn.close()
