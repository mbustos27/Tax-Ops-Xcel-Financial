import sqlite3
from pathlib import Path

conn = sqlite3.connect(f"file:{Path(r'T:/taxops/taxops.db')}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
print("MONTANEZ")
for r in conn.execute(
    "SELECT id, last_name, first_name, ssn_last4, spouse_last_name, spouse_first_name "
    "FROM clients WHERE upper(last_name) LIKE '%MONTANEZ%'"
):
    print(dict(r))
    for s in conn.execute("SELECT * FROM spouses WHERE client_id=?", (r["id"],)):
        print("  spouse", {k: s[k] for k in s.keys()})

print("HUERTA 495 spouses + clients")
r = conn.execute(
    "SELECT id, last_name, first_name, spouse_last_name, spouse_first_name, ssn_last4 "
    "FROM clients WHERE id=495"
).fetchone()
print(dict(r))

print("W5 queue matched ids")
import json
q = json.loads(Path(r"T:\audit\investigation\W5-needs-human-queue.json").read_text())
for item in q["queue"]:
    if item["status"] == "OPEN":
        print(item["entity_key"], item.get("matched_client_ids"), item.get("has_spouses_row_now"))
conn.close()
