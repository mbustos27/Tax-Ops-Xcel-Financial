import sqlite3
from utils import now

conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row
ts = now()

# Any return with logout_date set should be LOG OUT
rows = conn.execute("""
    SELECT id, client_status FROM returns
    WHERE logout_date IS NOT NULL
      AND logout_date != ''
      AND client_status != 'LOG OUT'
""").fetchall()

print(f"Returns with logout_date but wrong status: {len(rows)}")
for r in rows:
    conn.execute(
        "UPDATE returns SET client_status='LOG OUT', updated_at=? WHERE id=?",
        (ts, r["id"])
    )
    conn.execute(
        """INSERT INTO status_events
           (return_id, event_type, old_status, new_status, event_timestamp, source_file, note)
           VALUES (?, 'STATUS_CHANGED', ?, 'LOG OUT', ?, 'MIGRATION', 'Fixed: had logout_date but wrong status')""",
        (r["id"], r["client_status"], ts)
    )

conn.commit()
conn.close()
print("Done.")
