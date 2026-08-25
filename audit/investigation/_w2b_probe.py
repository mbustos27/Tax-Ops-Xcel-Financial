import sqlite3
import time
from pathlib import Path

p = Path(r"T:\taxops\taxops.db")
print("size", p.stat().st_size)
t0 = time.time()
conn = sqlite3.connect(str(p), timeout=5)
conn.execute("PRAGMA busy_timeout=5000")
print("connected", round(time.time() - t0, 2))
print("clients", conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0])
print(
    "history_table",
    conn.execute(
        "SELECT name FROM sqlite_master WHERE name='client_merge_history'"
    ).fetchone(),
)
for cid in (8, 754, 32, 654, 854, 517, 107, 1780, 479, 1801, 543, 1803, 220, 762):
    row = conn.execute(
        "SELECT id, last_name, first_name, ssn_last4, created_at FROM clients WHERE id=?",
        (cid,),
    ).fetchone()
    print(cid, row)
conn.close()
print("done")
