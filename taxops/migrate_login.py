import sqlite3
from utils import now

conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row
ts = now()

r = conn.execute(
    "UPDATE returns SET client_status='PROCESSING', updated_at=? WHERE client_status='LOG IN'",
    (ts,)
)
print(f"Migrated LOG IN -> PROCESSING: {r.rowcount}")

conn.execute(
    "UPDATE status_events SET old_status='PROCESSING' WHERE old_status='LOG IN'"
)
conn.execute(
    "UPDATE status_events SET new_status='PROCESSING' WHERE new_status='LOG IN'"
)

conn.commit()
conn.close()
print("Done.")
