import json
from db import get_connection
conn = get_connection()

# All import errors
all_errs = conn.execute(
    "SELECT ir.row_number, ir.error, ir.raw_json "
    "FROM import_rows ir "
    "WHERE ir.error IS NOT NULL AND ir.error != ''"
).fetchall()
print("Total import errors: %d" % len(all_errs))

# Group by error type
from collections import Counter
err_types = Counter()
for r in all_errs:
    # Bucket errors
    e = r["error"] or ""
    if "Invalid date" in e:
        err_types["Invalid date (bad format)"] += 1
    elif "Missing required" in e:
        err_types["Missing required fields"] += 1
    elif "float" in e.lower():
        err_types["Float/currency parse"] += 1
    else:
        err_types[e[:50]] += 1
for k, v in err_types.most_common():
    print("  %-50s %d" % (k, v))

# For Invalid date errors: show what dates look like
print()
print("=== Invalid date sample ===")
date_errs = [r for r in all_errs if "Invalid date" in (r["error"] or "")]
print("Total invalid date errors: %d" % len(date_errs))
samples = date_errs[:20]
for r in samples:
    d = {}
    try: d = json.loads(r["raw_json"] or "{}")
    except: pass
    # Show date fields
    date_fields = {k: v for k, v in d.items() if any(x in k.lower() for x in ["date", "int", "log out", "pick"])}
    print("  row=%-4s  err=%-40s  dates=%s  log=%s" % (
        r["row_number"],
        (r["error"] or "")[:40],
        date_fields,
        {k: v for k, v in d.items() if "log" in k.lower() and "2025" in k}
    ))

# Check log numbers 17-140 specifically - where are they?
print()
print("=== What happened to log numbers 17-140? ===")
rq_logs = set()
for r in conn.execute("SELECT csv_log FROM review_queue").fetchall():
    try: rq_logs.add(int(r["csv_log"]))
    except: pass

db_logs = set()
for r in conn.execute("SELECT log_number FROM returns WHERE tax_year=2025 AND log_number IS NOT NULL AND log_number != ''").fetchall():
    try: db_logs.add(int(r["log_number"]))
    except: pass

err_logs = set()
for r in all_errs:
    d = {}
    try: d = json.loads(r["raw_json"] or "{}")
    except: pass
    lg = d.get("LOG 2025") or d.get("log_number") or ""
    try: err_logs.add(int(lg))
    except: pass

missing_17_140 = set(range(17, 141)) - db_logs - rq_logs - err_logs
in_error = set(range(17, 141)) & err_logs
in_db = set(range(17, 141)) & db_logs
in_rq = set(range(17, 141)) & rq_logs
print("  17-140 in DB: %d  %s" % (len(in_db), sorted(in_db)[:10]))
print("  17-140 in review_queue: %d  %s" % (len(in_rq), sorted(in_rq)[:10]))
print("  17-140 in errors: %d  %s" % (len(in_error), sorted(in_error)[:10]))
print("  17-140 unaccounted: %d  %s" % (len(missing_17_140), sorted(missing_17_140)[:10]))

conn.close()
