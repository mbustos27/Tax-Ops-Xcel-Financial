"""
Verify that returns with no log_number genuinely have no match in the manual log.
"""
import json
from collections import Counter
from db import get_connection

conn = get_connection()

# --- How many returns have no log number? ---
total = conn.execute("SELECT COUNT(*) FROM returns").fetchone()[0]
no_log = conn.execute(
    "SELECT COUNT(*) FROM returns WHERE log_number IS NULL OR log_number = ''"
).fetchone()[0]
print("Total returns : %d" % total)
print("With log#     : %d" % (total - no_log))
print("WITHOUT log#  : %d" % no_log)

# --- Where did each no-log return come from? ---
# import_batches: batch 1 = manual log, batch 2 = TAXOPS, batch 3 = CSMDATA
batches = conn.execute("SELECT id, filename FROM import_batches ORDER BY id").fetchall()
print()
print("Import batches:")
for b in batches:
    print("  batch %d: %s" % (b["id"], b["filename"]))

# For each no-log return, find the latest import_rows action touching it
# We join via client name since import_rows stores raw_json
# Simpler: find which batch CREATED returns with no log
print()
print("=== No-log returns by tax_year ===")
yr_counts = conn.execute(
    "SELECT tax_year, COUNT(*) as n FROM returns "
    "WHERE log_number IS NULL OR log_number = '' "
    "GROUP BY tax_year ORDER BY tax_year DESC"
).fetchall()
for r in yr_counts:
    print("  yr=%-6s  no-log returns: %d" % (r["tax_year"], r["n"]))

# --- Sample no-log 2025 returns ---
print()
print("=== Sample no-log 2025 returns (first 30) ===")
sample = conn.execute(
    "SELECT r.id, c.last_name, c.first_name, r.tax_year, r.log_number "
    "FROM returns r JOIN clients c ON c.id=r.client_id "
    "WHERE (r.log_number IS NULL OR r.log_number = '') AND r.tax_year=2025 "
    "ORDER BY c.last_name LIMIT 30"
).fetchall()
for r in sample:
    print("  %-35s  yr=%s  log=%s" % (
        ("%s, %s" % (r["last_name"], r["first_name"] or ""))[:35],
        r["tax_year"], r["log_number"]
    ))

# --- Cross-check: are any of those names in the manual log import_rows? ---
print()
print("=== Checking if no-log 2025 names appear in manual log import rows ===")
manual_batch_id = batches[0]["id"] if batches else 1

# Get all names from manual log import rows
manual_rows = conn.execute(
    "SELECT raw_json FROM import_rows WHERE batch_id=?",
    (manual_batch_id,)
).fetchall()

manual_lastnames = set()
for mr in manual_rows:
    try:
        d = json.loads(mr["raw_json"] or "{}")
        ln = (d.get("TAX PAYER NAME (S) LAST") or d.get("LAST") or "").strip().upper()
        if ln:
            manual_lastnames.add(ln)
    except:
        pass

print("Unique last names in manual log: %d" % len(manual_lastnames))

# Now check how many no-log returns match manual log names
matched_in_log = 0
not_in_log = 0
matches = []
for r in conn.execute(
    "SELECT c.last_name, c.first_name, r.tax_year "
    "FROM returns r JOIN clients c ON c.id=r.client_id "
    "WHERE (r.log_number IS NULL OR r.log_number = '') AND r.tax_year=2025"
).fetchall():
    ln = (r["last_name"] or "").strip().upper()
    if ln in manual_lastnames:
        matched_in_log += 1
        matches.append("%s, %s" % (r["last_name"], r["first_name"] or ""))
    else:
        not_in_log += 1

print("No-log 2025 returns whose last_name IS in manual log : %d" % matched_in_log)
print("No-log 2025 returns whose last_name NOT in manual log: %d" % not_in_log)

if matches:
    print()
    print("Potential matches to investigate (name exists in log but no log# stored):")
    for m in matches[:30]:
        print("  %s" % m)

conn.close()
