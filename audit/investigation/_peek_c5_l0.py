import sqlite3
from pathlib import Path

p = Path(r"T:\audit\throwaway\c5_import_triage_throwaway.sqlite")
print("c5", p.exists())
for cand in sorted(Path(r"T:\audit").rglob("*c5*"))[:20]:
    print(cand)

conn = sqlite3.connect(r"T:\audit\audit_disposition.sqlite")
n = conn.execute(
    "SELECT count(*) FROM audit_disposition WHERE finding_type='L0_DRAKE_ONLY'"
).fetchone()[0]
print("L0_DRAKE_ONLY all", n)
rows = list(
    conn.execute(
        "SELECT entity_key, status FROM audit_disposition "
        "WHERE finding_type='L0_DRAKE_ONLY' AND status='OPEN' LIMIT 5"
    )
)
print("open sample", rows)
# also check recurring via status
for st in ("OPEN", "ACKED", "FALSE_POSITIVE", "RESOLVED"):
    c = conn.execute(
        "SELECT count(*) FROM audit_disposition WHERE finding_type='L0_DRAKE_ONLY' AND status=?",
        (st,),
    ).fetchone()[0]
    print(st, c)
conn.close()
