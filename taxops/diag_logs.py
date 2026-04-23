"""
Diagnose why log numbers from the live CSV aren't landing on returns.
Checks a sample of named rows and shows DB state for each.
"""
import sqlite3
from csv_analyzer import analyze, iter_data_rows
from name_matcher import find_client, parse_name, _all_clients_cache
from normalizer import normalize_string

conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row

with open("data/incoming/TAX LOG 2025 Live.csv", "rb") as f:
    raw = f.read()

result = analyze(raw)
rows   = list(iter_data_rows(raw, result))
cache  = _all_clients_cache(conn)

missing_log = []   # client found, return found, but log# not written
no_return   = []   # client found but no return for that year
new_client  = []   # client not found

for row in rows:
    raw_last  = (normalize_string(row.get("clients.last_name","")) or "").strip()
    raw_first = (normalize_string(row.get("clients.first_name","")) or "").strip()
    log       = (row.get("returns.log_number","") or "").strip()
    yr_raw    = (row.get("returns.tax_year","") or "").strip()
    yr        = int(yr_raw) if yr_raw.isdigit() else None

    if not raw_last or not log:
        continue

    last, first = parse_name(raw_last)
    if raw_first:
        first = raw_first.upper().strip() or None
    last = last.upper().strip()

    match = find_client(conn, last, first, cache=cache)
    if not match or match["needs_review"]:
        new_client.append((log, last, first, yr))
        continue

    cid = match["client_id"]
    ret = conn.execute(
        "SELECT id, log_number, tax_year FROM returns WHERE client_id=? AND tax_year=?",
        (cid, yr)
    ).fetchone() if yr else None

    if not ret:
        no_return.append((log, last, first, yr, cid))
    elif not ret["log_number"]:
        missing_log.append((log, last, first, yr, cid, ret["id"]))
    # else: already has a log number (could be correct or different)

conn.close()

print(f"Returns found but log# missing  : {len(missing_log)}")
print(f"Client found but no return yet  : {len(no_return)}")
print(f"No client match (new/review)    : {len(new_client)}")
print()

if missing_log:
    print("Sample — client+return exists but log# is blank:")
    for log, last, first, yr, cid, rid in missing_log[:10]:
        print(f"  CSV LOG {log:>4s}  {last}, {first}  TY{yr}  client_id={cid}  return_id={rid}")

if no_return:
    print("\nSample — client exists but no TY return in DB:")
    for log, last, first, yr, cid in no_return[:10]:
        print(f"  CSV LOG {log:>4s}  {last}, {first}  TY{yr}  client_id={cid}")
