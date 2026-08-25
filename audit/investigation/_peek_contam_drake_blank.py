"""Check Drake spouse rows for contam owners marked in_export but NO_DRAKE_HIT."""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path

SPOUSE_CSV = Path(r"T:\taxops\CSVFILES\TAXPAYERspouse25.csv")
TRIAGE = Path(r"T:\audit\investigation\W4-contam-triage.json")


def norm(s):
    s = (s or "").upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


with SPOUSE_CSV.open(encoding="utf-8-sig", newline="") as f:
    rows = list(csv.reader(f))
header = rows[2]
idx = {h: i for i, h in enumerate(header)}


def cell(row, col):
    i = idx.get(col)
    if i is None or i >= len(row):
        return ""
    return (row[i] or "").strip()


by = {}
for row in rows[3:]:
    if not row or not any(row):
        continue
    tl, tf = cell(row, "Taxpayer Last Name"), cell(row, "Taxpayer First Name")
    by.setdefault(f"{norm(tl)}|{norm(tf)}", []).append(
        {
            "tl": tl,
            "tf": tf,
            "spouse": cell(row, "Spouse Name"),
        }
    )

doc = json.loads(TRIAGE.read_text(encoding="utf-8"))
checked = 0
blank = 0
has = 0
for p in doc["human_plans"]:
    if p.get("business"):
        continue
    cl, cf = [x.strip() for x in p["owner"].split(",", 1)]
    key = f"{norm(cl)}|{norm(cf)}"
    hits = by.get(key) or []
    if not hits:
        # try first side of joint
        continue
    checked += 1
    spouses = [h["spouse"] for h in hits]
    nonempty = [s for s in spouses if s]
    if nonempty:
        has += 1
        if checked <= 8:
            print("HAS", p["client_id"], p["owner"], "->", nonempty[:3], "wrong was", p["wrong_spouse"])
    else:
        blank += 1
        if blank <= 8:
            print("BLANK", p["client_id"], p["owner"], "wrong was", p["wrong_spouse"])

print("checked_exact", checked, "blank_spouse", blank, "has_spouse", has)

# joint owners
print("\n--- joint first_name owners ---")
n_joint = 0
for p in doc["human_plans"]:
    if "&" in p["owner"]:
        n_joint += 1
        if n_joint <= 10:
            print(p["client_id"], p["owner"], "wrong", p["wrong_spouse"])
print("n_joint", n_joint)
