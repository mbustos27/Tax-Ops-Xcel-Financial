from db import get_connection
conn = get_connection()

total = conn.execute("SELECT COUNT(*) FROM returns").fetchone()[0]
with_log = conn.execute(
    "SELECT COUNT(*) FROM returns WHERE log_number IS NOT NULL AND log_number != ''"
).fetchone()[0]
print("Total returns: %d  With log#: %d  Without: %d" % (total, with_log, total - with_log))

# Range across all years
r = conn.execute(
    "SELECT MIN(CAST(log_number AS INTEGER)), MAX(CAST(log_number AS INTEGER)) "
    "FROM returns WHERE log_number GLOB '[0-9]*'"
).fetchone()
print("All-years log# range: %s - %s" % (r[0], r[1]))

for yr in [2025, 2024, 2023, 2022]:
    rows = conn.execute(
        "SELECT CAST(log_number AS INTEGER) as n FROM returns "
        "WHERE log_number GLOB '[0-9]*' AND tax_year=? ORDER BY n",
        (yr,)
    ).fetchall()
    nums = [row["n"] for row in rows]
    if not nums:
        print("Year %d: no numeric log numbers" % yr)
        continue
    mn, mx = min(nums), max(nums)
    expected = set(range(mn, mx + 1))
    missing = sorted(expected - set(nums))
    print("Year %d: %d entries  log# %d..%d  gaps=%d %s" % (
        yr, len(nums), mn, mx, len(missing),
        ("first 10: " + str(missing[:10])) if missing else ""
    ))

# Check how many LOG 2025 values came from manual log vs Drake
print()
from_manual = conn.execute(
    "SELECT COUNT(*) FROM returns r "
    "JOIN import_rows ir ON ir.batch_id=1 "
    "WHERE r.log_number IS NOT NULL AND r.log_number != ''"
).fetchone()[0]
print("Sample log numbers (2025, first 30):")
rows = conn.execute(
    "SELECT r.log_number, c.last_name, c.first_name, r.tax_year "
    "FROM returns r JOIN clients c ON c.id=r.client_id "
    "WHERE r.log_number GLOB '[0-9]*' AND r.tax_year=2025 "
    "ORDER BY CAST(r.log_number AS INTEGER) LIMIT 30"
).fetchall()
for row in rows:
    print("  log=%-6s  year=%s  %s, %s" % (row["log_number"], row["tax_year"], row["last_name"], row["first_name"]))

conn.close()
