import sqlite3
from pathlib import Path

conn = sqlite3.connect(f"file:{Path(r'T:/taxops/taxops.db')}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
print("range 2155-2187")
for r in conn.execute(
    "SELECT id, last_name, first_name, ssn_last4 FROM clients "
    "WHERE id BETWEEN 2155 AND 2187 ORDER BY id"
):
    print(dict(r))
print("--- TEST name ---")
for r in conn.execute(
    "SELECT id, last_name, first_name, ssn_last4 FROM clients "
    "WHERE upper(last_name) LIKE '%TEST%' OR upper(first_name) LIKE '%TEST%' "
    "OR upper(COALESCE(display_name,'')) LIKE '%TEST%' ORDER BY id"
):
    print(dict(r))
conn.close()
