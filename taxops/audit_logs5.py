import json
from db import get_connection
conn = get_connection()

# Check action vs error for early rows
print("=== import_rows: action breakdown ===")
actions = conn.execute(
    "SELECT action, COUNT(*) as n FROM import_rows GROUP BY action ORDER BY n DESC"
).fetchall()
for r in actions:
    print("  action=%-20s  n=%d" % (r["action"] or "NULL", r["n"]))

print()
print("=== Early rows (1-30) — action vs error ===")
rows = conn.execute(
    "SELECT ir.row_number, ir.action, ir.error, ir.raw_json "
    "FROM import_rows ir "
    "JOIN import_batches ib ON ib.id=ir.batch_id "
    "WHERE ib.filename LIKE '%TAX LOG%' "
    "ORDER BY ir.row_number LIMIT 35"
).fetchall()
for r in rows:
    d = {}
    try: d = json.loads(r["raw_json"] or "{}")
    except: pass
    log2025 = d.get("LOG 2025", "?")
    print("  row=%-4s  action=%-12s  log2025=%-6s  err=%s" % (
        r["row_number"], r["action"] or "NULL", log2025, (r["error"] or "")[:50]
    ))

conn.close()
