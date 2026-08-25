import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, "T:/")
from audit.invoice_export import bare_log_number

p = Path(r"T:\audit\tmp\c5_import_triage_throwaway.sqlite")
c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
print("actions", list(c.execute("SELECT action, count(*) FROM import_rows GROUP BY 1")))
print(
    "with error",
    c.execute(
        "SELECT count(*) FROM import_rows WHERE error IS NOT NULL AND trim(error)!=''"
    ).fetchone(),
)
print("review_queue", c.execute("SELECT count(*) FROM review_queue").fetchone())
print("rq cols", [r[1] for r in c.execute("PRAGMA table_info(review_queue)")])

live = sqlite3.connect("file:T:/taxops/taxops.db?mode=ro", uri=True)
taxops_bares = set()
for (logn,) in live.execute(
    "SELECT log_number FROM returns WHERE tax_year=2025 AND log_number IS NOT NULL AND trim(log_number)!=''"
):
    b = bare_log_number(str(logn), 2025)
    if b:
        taxops_bares.add(b)
print("taxops bares", len(taxops_bares))

sample = c.execute(
    "SELECT raw_json, action, error FROM import_rows WHERE error IS NOT NULL AND trim(error)!='' LIMIT 3"
).fetchall()
for s in sample:
    raw = json.loads(s[0] or "{}")
    print("err keys", sorted(raw.keys())[:25], "action", s[1], "err", (s[2] or "")[:80])

absent = set()
for row in c.execute("SELECT raw_json, action, error FROM import_rows"):
    try:
        raw = json.loads(row[0] or "{}")
    except json.JSONDecodeError:
        raw = {}
    logv = (
        raw.get("log")
        or raw.get("log_number")
        or raw.get("LOG 2025")
        or raw.get("office_log")
        or raw.get("Log Number")
    )
    if not logv:
        for k, v in raw.items():
            if v and "log" in str(k).lower():
                logv = v
                break
    bare = bare_log_number(str(logv or ""), 2025)
    err = (row[2] or "").strip()
    action = (row[1] or "").upper()
    is_bad = bool(err) or action in ("REVIEW", "ERROR", "NEEDS_REVIEW")
    if is_bad and bare and bare not in taxops_bares:
        absent.add(bare)

print(
    "absent",
    len(absent),
    "sample",
    sorted(absent, key=lambda x: int(x) if x.isdigit() else 0)[:20],
)
print("rq sample", c.execute("SELECT * FROM review_queue LIMIT 2").fetchall())
c.close()
live.close()
