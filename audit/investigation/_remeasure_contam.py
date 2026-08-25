"""Remeasure personnel contam + F11-ish spouse counts after detach27."""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

TAXOPS = Path(r"T:\taxops\taxops.db")


def tokens(s):
    s = re.sub(r"[^A-Z0-9 ]", " ", (s or "").upper())
    return {t for t in re.findall(r"[A-Z0-9]+", s) if len(t) > 1}


conn = sqlite3.connect(f"file:{TAXOPS}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
n_sp = conn.execute("SELECT count(*) FROM spouses").fetchone()[0]
n_tp = conn.execute(
    "SELECT count(*) FROM spouses WHERE taxpayer_name IS NOT NULL AND trim(taxpayer_name)!=''"
).fetchone()[0]
n_cols = conn.execute(
    "SELECT count(*) FROM clients WHERE COALESCE(is_test,0)=0 AND "
    "(spouse_last_name IS NOT NULL OR spouse_first_name IS NOT NULL)"
).fetchone()[0]

mismatch = 0
for r in conn.execute(
    """
    SELECT s.taxpayer_name, c.last_name AS cl
      FROM spouses s JOIN clients c ON c.id=s.client_id
     WHERE COALESCE(c.is_test,0)=0
       AND s.taxpayer_name IS NOT NULL AND trim(s.taxpayer_name)!=''
    """
):
    ot = tokens(r["cl"])
    if ot and ot.isdisjoint(tokens(r["taxpayer_name"])):
        mismatch += 1

print(
    {
        "spouses": n_sp,
        "taxpayer_name_set": n_tp,
        "personnel_disjoint_mismatch": mismatch,
        "clients_with_spouse_cols": n_cols,
    }
)
conn.close()
