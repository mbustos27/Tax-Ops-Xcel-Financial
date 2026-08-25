import json
import sqlite3

c = sqlite3.connect(r"T:\audit\audit_disposition.sqlite")
c.row_factory = sqlite3.Row
for r in c.execute(
    "select entity_key, sample_detail from audit_disposition "
    "where finding_type='SPOUSE_STORE_DIVERGENCE' and status='OPEN' limit 8"
):
    print("EK", r["entity_key"])
    print(r["sample_detail"][:500])
    print("---")
c.close()
