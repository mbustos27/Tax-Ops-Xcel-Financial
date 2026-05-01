from db import get_connection
conn = get_connection()

# 1. What happened to log numbers 1-9 and 17-140 in 2025?
# Check review_queue for those log number ranges
print("=== REVIEW QUEUE entries with log numbers 1-140 ===")
rq = conn.execute(
    "SELECT rq.suggested_log_number, rq.raw_last_name, rq.raw_first_name, rq.raw_tax_year, rq.reason "
    "FROM review_queue rq "
    "WHERE CAST(rq.suggested_log_number AS INTEGER) BETWEEN 1 AND 140 "
    "ORDER BY CAST(rq.suggested_log_number AS INTEGER) "
    "LIMIT 40"
).fetchall()
for r in rq:
    print("  log=%-6s  yr=%-6s  %s, %s  reason=%s" % (
        r["suggested_log_number"], r["raw_tax_year"], r["raw_last_name"], r["raw_first_name"], r["reason"]
    ))
print("  (total: %d)" % len(rq))

# 2. Check import_rows errors for log numbers 1-140
print()
print("=== IMPORT ERRORS with log 1-140 (import_rows) ===")
try:
    ir = conn.execute(
        "SELECT ir.row_number, ir.raw_data, ir.error_message "
        "FROM import_rows ir "
        "WHERE ir.status='error' "
        "LIMIT 20"
    ).fetchall()
    for r in ir:
        print("  row=%-5s  err=%s" % (r["row_number"], r["error_message"]))
    print("  (total: %d)" % len(ir))
except Exception as e:
    print("  ERROR:", e)

# 3. Check how many import_rows exist per batch and their statuses
print()
print("=== Import batch summary ===")
batches = conn.execute(
    "SELECT ib.id, ib.filename, ib.created_at, "
    "COUNT(CASE WHEN ir.status='ok' THEN 1 END) as ok, "
    "COUNT(CASE WHEN ir.status='review' THEN 1 END) as rev, "
    "COUNT(CASE WHEN ir.status='error' THEN 1 END) as err "
    "FROM import_batches ib "
    "LEFT JOIN import_rows ir ON ir.batch_id=ib.id "
    "GROUP BY ib.id "
    "ORDER BY ib.id"
).fetchall()
for b in batches:
    print("  batch %d: %s  ok=%d review=%d error=%d" % (b["id"], b["filename"], b["ok"], b["rev"], b["err"]))

# 4. Check how many import_rows had log numbers for 2025 and what those are
print()
print("=== Import rows for 2025 - how log numbers were stored ===")
# Look at raw data to see if those early log numbers were even present
try:
    sample = conn.execute(
        "SELECT ir.row_number, ir.raw_data "
        "FROM import_rows ir "
        "JOIN import_batches ib ON ib.id=ir.batch_id "
        "WHERE ib.filename LIKE '%TAX LOG%' AND ir.status='ok' "
        "ORDER BY ir.row_number LIMIT 20"
    ).fetchall()
    import json
    for r in sample:
        try:
            d = json.loads(r["raw_data"])
            log_keys = {k: v for k, v in d.items() if "log" in k.lower() or k == "tax_year"}
            print("  row=%-4s  %s" % (r["row_number"], log_keys))
        except:
            print("  row=%-4s  raw=%s" % (r["row_number"], str(r["raw_data"])[:80]))
except Exception as e:
    print("  ERROR:", e)

conn.close()
