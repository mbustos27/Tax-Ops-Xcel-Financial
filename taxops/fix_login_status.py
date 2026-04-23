import sqlite3
from utils import now

conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row
ts = now()

# Any return with an intake_date that is still sitting at LOG IN
# should be at least PROCESSING — intake means work has begun.
rows = conn.execute("""
    SELECT id FROM returns
    WHERE intake_date IS NOT NULL
      AND intake_date != ''
      AND client_status = 'LOG IN'
""").fetchall()

print(f"Returns with intake_date still at LOG IN: {len(rows)}")
for r in rows:
    conn.execute(
        "UPDATE returns SET client_status='PROCESSING', updated_at=? WHERE id=?",
        (ts, r["id"])
    )
    conn.execute(
        """INSERT INTO status_events
           (return_id, event_type, old_status, new_status, event_timestamp, source_file, note)
           VALUES (?, 'STATUS_CHANGED', 'LOG IN', 'PROCESSING', ?, 'MIGRATION', 'Fixed: had intake_date but was at LOG IN')""",
        (r["id"], ts)
    )

conn.commit()
conn.close()
print("Done.")
