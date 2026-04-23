"""Diagnose why 809 rows are being skipped."""
from csv_analyzer import analyze, iter_data_rows
from normalizer import normalize_string

with open("data/incoming/TAX LOG 2025 Live.csv", "rb") as f:
    raw = f.read()

result = analyze(raw)
rows   = iter_data_rows(raw, result)

no_last = 0
no_log  = 0
both_missing = 0
samples_no_last = []
samples_no_log  = []

for row in rows:
    last  = (normalize_string(row.get("clients.last_name","")) or "").upper().strip()
    log   = row.get("returns.log_number","").strip()

    if not last and not log:
        both_missing += 1
    elif not last:
        no_last += 1
        if len(samples_no_last) < 8:
            samples_no_last.append(dict(row))
    elif not log:
        no_log += 1
        if len(samples_no_log) < 5:
            samples_no_log.append(dict(row))

print(f"Both missing (blank rows)  : {both_missing}")
print(f"Has log but no name        : {no_last}")
print(f"Has name but no log number : {no_log}")
print()
print("Sample rows missing name (showing all fields with values):")
for r in samples_no_last:
    filtered = {k: v for k, v in r.items() if v}
    print(" ", dict(list(filtered.items())[:8]))
print()
print("Sample rows missing log #:")
for r in samples_no_log:
    print(" ", {k: v for k, v in r.items() if v})
