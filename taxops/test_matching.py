"""
Validate fuzzy name matching quality against TAX LOG 2025 Live.csv.
Shows exact vs fuzzy breakdown and samples of unmatched names.
"""
import sqlite3
from csv_analyzer import analyze, iter_data_rows
from name_matcher import (
    find_client, parse_name, _all_clients_cache,
    ACCEPT_THRESHOLD, REVIEW_THRESHOLD,
)
from normalizer import normalize_string

conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row

with open("data/incoming/TAX LOG 2025 Live.csv", "rb") as f:
    raw = f.read()

result = analyze(raw)
rows   = list(iter_data_rows(raw, result))

cache = _all_clients_cache(conn)

placeholder     = 0
exact_match     = 0
fuzzy_confident = 0
fuzzy_review    = 0
new_client      = 0
no_log          = 0

samples_review  = []
samples_new     = []

for row in rows:
    raw_last  = (normalize_string(row.get("clients.last_name","")) or "").strip()
    raw_first = (normalize_string(row.get("clients.first_name","")) or "").strip()
    log       = (row.get("returns.log_number","") or "").strip()
    yr_raw    = (row.get("returns.tax_year","") or "").strip()
    yr        = int(yr_raw) if yr_raw.isdigit() else None

    if not raw_last:
        placeholder += 1
        continue
    if not log:
        no_log += 1
        continue

    last, first = parse_name(raw_last)
    if raw_first:
        first = raw_first.upper().strip() or None
    last = last.upper().strip()

    match = find_client(conn, last, first, cache=cache)

    if match is None:
        new_client += 1
        if len(samples_new) < 12:
            samples_new.append((log, f"{last}, {first or ''}", yr))
    elif match["method"] == "exact":
        exact_match += 1
    elif not match["needs_review"]:
        fuzzy_confident += 1
    else:
        fuzzy_review += 1
        if len(samples_review) < 10:
            db_row = conn.execute(
                "SELECT last_name, first_name FROM clients WHERE id=?", (match["client_id"],)
            ).fetchone()
            db_name = f"{db_row['last_name']}, {db_row['first_name'] or ''}" if db_row else "?"
            samples_review.append((log, f"{last}, {first or ''}", yr, match["score"], db_name))

conn.close()

named = exact_match + fuzzy_confident + fuzzy_review + new_client
total = named + placeholder + no_log

print("=" * 60)
print(f"  Total rows in CSV               : {total:5d}")
print(f"  Placeholder (log# only, skip)   : {placeholder:5d}")
print(f"  No log# (skip)                  : {no_log:5d}")
print(f"  Rows with client data           : {named:5d}")
print("=" * 60)
print()
print(f"  Of {named} named rows:")
print(f"    Exact match                   : {exact_match:5d}  ({100*exact_match//named:.0f}%)")
print(f"    Fuzzy confident (>={ACCEPT_THRESHOLD})      : {fuzzy_confident:5d}  ({100*fuzzy_confident//named:.0f}%)")
print(f"    Fuzzy low-conf (review)       : {fuzzy_review:5d}  ({100*fuzzy_review//named:.0f}%)")
print(f"    New client (no match)         : {new_client:5d}  ({100*new_client//named:.0f}%)")
print()

if samples_review:
    print("Sample review-flagged matches:")
    for log, csv_name, yr, score, db_name in samples_review:
        print(f"  LOG {log:>4s}  CSV: {csv_name:<35s} DB: {db_name}  (score={score}, TY{yr})")
    print()

if samples_new:
    print("Sample new clients (will be created):")
    for log, name, yr in samples_new:
        print(f"  LOG {log:>4s}  {name:<40s}  TY{yr}")
