import sqlite3
from pathlib import Path

conn = sqlite3.connect(f"file:{Path(r'T:/audit/audit_disposition.sqlite')}?mode=ro", uri=True)
print("TEST entity keys:")
for r in conn.execute(
    "SELECT finding_type, entity_key, status, resolved_by FROM audit_disposition "
    "WHERE entity_key LIKE '%TEST%' OR entity_key LIKE '1111|%' "
    "ORDER BY entity_key LIMIT 50"
):
    print(tuple(r))
print("--- wave4_fold resolved sample ---")
for r in conn.execute(
    "SELECT finding_type, entity_key, status FROM audit_disposition "
    "WHERE resolved_by='wave4_fold' LIMIT 35"
):
    print(tuple(r))
conn.close()
