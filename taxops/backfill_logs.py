"""
backfill_logs.py
~~~~~~~~~~~~~~~~
One-time script: reads TAX LOG 2025 Live.csv and writes log numbers,
dates, status, and processor onto every matched return in taxops.db.
For clients that exist but have no return for that year, creates the return.

Safe to run multiple times — uses COALESCE so existing data is never
overwritten with a blank value.
"""
import sqlite3
from datetime import date
from csv_analyzer import analyze, iter_data_rows
from name_matcher import find_client, parse_name, _all_clients_cache
from normalizer import normalize_string, normalize_date
from csv_analyzer import normalize_status
from utils import now

DB   = "taxops.db"
CSV  = "data/incoming/TAX LOG 2025 Live.csv"

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

with open(CSV, "rb") as f:
    raw = f.read()

result = analyze(raw)
rows   = list(iter_data_rows(raw, result))
cache  = _all_clients_cache(conn)

ts        = now()
today_iso = date.today().isoformat()

updated  = 0
created  = 0
skipped  = 0
review   = 0

for row in rows:
    raw_last  = (normalize_string(row.get("clients.last_name","")) or "").strip()
    raw_first = (normalize_string(row.get("clients.first_name","")) or "").strip()
    log       = (row.get("returns.log_number","") or "").strip()
    yr_raw    = (row.get("returns.tax_year","") or "").strip()
    yr        = int(yr_raw) if yr_raw.isdigit() else None

    if not raw_last or not log or not yr:
        skipped += 1
        continue

    last, first = parse_name(raw_last)
    if raw_first:
        first = raw_first.upper().strip() or None
    last = last.upper().strip()

    match = find_client(conn, last, first, cache=cache)
    if not match:
        skipped += 1
        continue
    if match["needs_review"]:
        review += 1
        continue

    cid = match["client_id"]

    raw_status  = (row.get("returns.client_status","") or "").strip()
    norm_status = normalize_status(raw_status) if raw_status else "LOG IN"

    processor   = normalize_string(row.get("returns.processor","")) or None
    intake_dt,  _ = normalize_date(row.get("returns.intake_date","") or "")
    pickup_dt,  _ = normalize_date(row.get("returns.pickup_date","") or "")
    logout_dt,  _ = normalize_date(row.get("returns.logout_date","") or "")

    existing = conn.execute(
        "SELECT id, log_number FROM returns WHERE client_id=? AND tax_year=?",
        (cid, yr)
    ).fetchone()

    if existing:
        conn.execute(
            """UPDATE returns SET
                 log_number   = ?,
                 client_status = ?,
                 processor    = COALESCE(NULLIF(processor,''), ?),
                 intake_date  = COALESCE(intake_date, ?),
                 pickup_date  = COALESCE(pickup_date, ?),
                 logout_date  = COALESCE(logout_date, ?),
                 updated_at   = ?
               WHERE id = ?""",
            (log, norm_status, processor,
             intake_dt or today_iso, pickup_dt, logout_dt,
             ts, existing["id"])
        )
        updated += 1
    else:
        conn.execute(
            """INSERT INTO returns
               (client_id, log_number, tax_year, client_status, processor,
                intake_date, pickup_date, logout_date, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (cid, log, yr, norm_status, processor,
             intake_dt or today_iso, pickup_dt, logout_dt,
             ts, ts)
        )
        # Add new client to cache so duplicate names later in the CSV resolve
        cache.append({"id": cid, "ln": last, "fn": (first or "").upper()})
        created += 1

conn.commit()
conn.close()

print(f"Done.")
print(f"  Updated existing returns : {updated}")
print(f"  Created new returns      : {created}")
print(f"  Skipped (no match/data)  : {skipped}")
print(f"  Queued for review        : {review}")
