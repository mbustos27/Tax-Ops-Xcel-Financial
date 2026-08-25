import json
import sqlite3
from pathlib import Path

conn = sqlite3.connect(f"file:{Path(r'T:/taxops/taxops.db')}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
for last4 in ("6789", "1697", "1313"):
    print("---", last4)
    for r in conn.execute(
        """
        SELECT c.id, c.last_name, c.first_name, c.spouse_last_name, c.spouse_first_name,
               s.id AS spouse_id, s.last_name AS sl, s.first_name AS sf, s.source, s.taxpayer_name
        FROM clients c LEFT JOIN spouses s ON s.client_id=c.id
        WHERE c.ssn_last4=?
        """,
        (last4,),
    ):
        print(dict(r))
conn.close()

# remaining contam needs_human sample
plans = json.loads(Path(r"T:\audit\investigation\W4-contam-fix.json").read_text(encoding="utf-8"))
human = [p for p in plans.get("plans") or [] if p.get("action") == "NEEDS_HUMAN"]
print("human_n", len(human))
print("sample", human[:8])
