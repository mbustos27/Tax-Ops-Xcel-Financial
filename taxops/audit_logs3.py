import json
from db import get_connection
conn = get_connection()

# 1. Review queue — what log numbers are stuck there?
print("=== REVIEW QUEUE: log numbers 1-140 ===")
rq = conn.execute(
    "SELECT rq.csv_log, rq.csv_last, rq.csv_first, rq.csv_year, rq.reason "
    "FROM review_queue rq "
    "WHERE CAST(rq.csv_log AS INTEGER) BETWEEN 1 AND 140 "
    "ORDER BY CAST(rq.csv_log AS INTEGER) "
).fetchall()
for r in rq:
    print("  log=%-6s  yr=%-6s  %s, %s  reason=%s" % (
        r["csv_log"], r["csv_year"], r["csv_last"], r["csv_first"], r["reason"]
    ))
print("  (total: %d)" % len(rq))

# 2. Overall review queue breakdown
print()
print("=== REVIEW QUEUE: total breakdown by reason ===")
counts = conn.execute(
    "SELECT reason, COUNT(*) as n FROM review_queue GROUP BY reason ORDER BY n DESC"
).fetchall()
for c in counts:
    print("  %-40s  %d" % (c["reason"], c["n"]))

# 3. Import rows errors  
print()
print("=== IMPORT ERRORS ===")
ir = conn.execute(
    "SELECT ir.row_number, ir.error, ir.raw_json "
    "FROM import_rows ir "
    "WHERE ir.error IS NOT NULL AND ir.error != '' "
    "LIMIT 10"
).fetchall()
for r in ir:
    d = {}
    try: d = json.loads(r["raw_json"] or "{}")
    except: pass
    log_yr = {k: v for k, v in d.items() if "log" in k.lower() or k in ("tax_year", "last_name")}
    print("  row=%-5s  err=%-60s  %s" % (r["row_number"], (r["error"] or "")[:60], log_yr))

# 4. Sample of raw import rows from manual log to see log keys
print()
print("=== SAMPLE: import_rows from manual log (first 20 ok rows) ===")
sample = conn.execute(
    "SELECT ir.row_number, ir.raw_json "
    "FROM import_rows ir "
    "JOIN import_batches ib ON ib.id=ir.batch_id "
    "WHERE ib.filename LIKE '%TAX LOG%' AND ir.action='inserted' "
    "ORDER BY ir.row_number LIMIT 20"
).fetchall()
for r in sample:
    try:
        d = json.loads(r["raw_json"] or "{}")
        keys = {k: v for k, v in d.items() if "log" in k.lower() or k in ("tax_year", "last_name", "first_name")}
        print("  row=%-4s  %s" % (r["row_number"], keys))
    except:
        print("  row=%-4s  raw=%s" % (r["row_number"], str(r["raw_json"])[:80]))

# 5. Check log numbers 1-9 specifically — where are they?
print()
print("=== WHERE ARE LOG 1-9? ===")
# In returns
for n in range(1, 10):
    ret = conn.execute(
        "SELECT r.log_number, r.tax_year, c.last_name, c.first_name "
        "FROM returns r JOIN clients c ON c.id=r.client_id "
        "WHERE r.log_number=?", (str(n),)
    ).fetchall()
    in_rq = conn.execute("SELECT count(*) FROM review_queue WHERE csv_log=?", (str(n),)).fetchone()[0]
    print("  log=%d  in_returns=%d  in_review_queue=%d" % (n, len(ret), in_rq))

conn.close()
