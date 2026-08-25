"""Sample why contam owners miss Drake spouse export."""
from __future__ import annotations

import csv
import json
import re
import sqlite3
from pathlib import Path

TAXOPS = Path(r"T:\taxops\taxops.db")
SPOUSE_CSV = Path(r"T:\taxops\CSVFILES\TAXPAYERspouse25.csv")
TRIAGE = Path(r"T:\audit\investigation\W4-contam-triage.json")
TAXPAYER = Path(r"T:\taxops\CSVFILES\TAXPAYER25.csv")


def norm(s):
    s = (s or "").upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def tokens(s):
    return {t for t in re.findall(r"[A-Z0-9]+", norm(s)) if len(t) > 1}


doc = json.loads(TRIAGE.read_text(encoding="utf-8"))
humans = doc["human_plans"][:15]

# load drake spouse taxpayers
with SPOUSE_CSV.open(encoding="utf-8-sig", newline="") as f:
    rows = list(csv.reader(f))
header = rows[2]
idx = {h: i for i, h in enumerate(header)}
print("spouse header:", header)

def cell(row, col):
    i = idx.get(col)
    if i is None or i >= len(row):
        return ""
    return (row[i] or "").strip()

tp_keys = set()
tp_names = []
for row in rows[3:]:
    if not row or not any(row):
        continue
    tl, tf = cell(row, "Taxpayer Last Name"), cell(row, "Taxpayer First Name")
    tp_keys.add(f"{norm(tl)}|{norm(tf)}")
    tp_names.append((tl, tf, cell(row, "Spouse Name")))

# also wrong taxpayer_name presence
print("\n--- sample human owners vs spouse-export ---")
for p in humans:
    owner = p["owner"]
    cl, cf = owner.split(",", 1)
    cl, cf = cl.strip(), cf.strip()
    key = f"{norm(cl)}|{norm(cf)}"
    in_export = key in tp_keys
    # is wrong tp name in export?
    wtp = p["taxpayer_name"]
    # fuzzy: any export row with token overlap to wrong spouse first?
    print(f"cid={p['client_id']} biz={p['business']} in_spouse_export={in_export}")
    print(f"  owner={owner} last4={p['owner_last4']}")
    print(f"  wrong={p['wrong_spouse']} via tp={wtp}")

# Check if owners exist in TAXPAYER25 at all
if TAXPAYER.exists():
    with TAXPAYER.open(encoding="utf-8-sig", newline="") as f:
        trows = list(csv.reader(f))
    print("\nTAXPAYER25 header row2:", trows[2] if len(trows) > 2 else trows[0])
else:
    # try glob
    for p in Path(r"T:\taxops\CSVFILES").glob("*TAXPAYER*"):
        print("found", p)
