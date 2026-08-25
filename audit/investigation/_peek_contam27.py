"""For residual 27 contam: are owners in Drake CSM / invoice / Tax Log as singles?"""
from __future__ import annotations

import csv
import json
import re
import sqlite3
from pathlib import Path

TRIAGE = Path(r"T:\audit\investigation\W4-contam-triage.json")
TAXOPS = Path(r"T:\taxops\taxops.db")
CSVDIR = Path(r"T:\taxops\CSVFILES")


def norm(s):
    s = (s or "").upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


doc = json.loads(TRIAGE.read_text(encoding="utf-8"))
humans = doc["human_plans"]
print("residual", len(humans))

# Find CSM-like files
for p in sorted(CSVDIR.glob("*.csv")):
    print("csv", p.name)

conn = sqlite3.connect(f"file:{TAXOPS}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row

# clients.spouse_* + returns for each residual
for p in humans:
    cid = p["client_id"]
    r = conn.execute(
        "SELECT id, last_name, first_name, spouse_last_name, spouse_first_name, ssn_last4, "
        "(SELECT count(*) FROM returns WHERE client_id=c.id) AS n_ret, "
        "(SELECT group_concat(log_number) FROM returns WHERE client_id=c.id) AS logs "
        "FROM clients c WHERE id=?",
        (cid,),
    ).fetchone()
    print(
        f"cid={cid} cols={r['spouse_last_name']!r}/{r['spouse_first_name']!r} "
        f"rets={r['n_ret']} logs={r['logs']} wrong={p['wrong_spouse']}"
    )
conn.close()
