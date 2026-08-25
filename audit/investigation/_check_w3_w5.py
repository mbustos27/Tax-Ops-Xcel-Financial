import csv
import json
import sqlite3
from pathlib import Path

disp = Path(r"T:\audit\audit_disposition.sqlite")
conn = sqlite3.connect(f"file:{disp}?mode=ro", uri=True)
for t in ("PREFILL_STUB_BURST", "DUPLICATE_CLIENT", "NEEDS_HUMAN"):
    rows = list(
        conn.execute(
            "SELECT status, COUNT(*) FROM audit_disposition WHERE finding_type=? GROUP BY status",
            (t,),
        )
    )
    print(t, rows)

for ek in (
    "HERNANDEZ|ISMAEL",
    "NUNO|JUAN",
    "ALVARADO|OSCAR",
    "LUNA|ESTEBAN",
    "VALDEZ|SANDRA",
    "HERNANDEZ|ABEL",
    "TASHAYOD|ALEX",
    "SOLOMON|LAUREN",
):
    r = conn.execute(
        "SELECT finding_id, status, resolved_by, substr(note,1,80) FROM audit_disposition "
        "WHERE finding_type='DUPLICATE_CLIENT' AND entity_key=?",
        (ek,),
    ).fetchone()
    print(ek, r)
print(
    "PREFILL",
    conn.execute(
        "SELECT finding_id, status, resolved_by FROM audit_disposition WHERE finding_type='PREFILL_STUB_BURST'"
    ).fetchall(),
)
conn.close()

# peek spouse export header
p = Path(r"T:\taxops\CSVFILES\TAXPAYERspouse25.csv")
with p.open(encoding="utf-8-sig", newline="") as f:
    rows = list(csv.reader(f))
print("spouse_csv_title", rows[0][:3], "header", rows[2] if len(rows) > 2 else None, "nrows", len(rows))
