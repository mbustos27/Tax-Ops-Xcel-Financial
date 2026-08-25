import os
import shutil
import sqlite3
from pathlib import Path

src = Path(r"T:\taxops")
dest = Path(os.environ["TEMP"]) / "taxops_w2b_recover"
dest.mkdir(parents=True, exist_ok=True)

for name in ("taxops.db", "taxops.db-wal", "taxops.db-shm"):
    s = src / name
    if s.exists():
        try:
            shutil.copy2(s, dest / name)
            print("copied", name, s.stat().st_size)
        except Exception as e:
            print("copy fail", name, e)
    else:
        print("missing", name)

p = dest / "taxops.db"
print("open", p, "size", p.stat().st_size)
conn = sqlite3.connect(str(p), timeout=10)
print("quick_check", conn.execute("PRAGMA quick_check").fetchone())
print("clients", conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0])
print(
    "history",
    conn.execute(
        "SELECT name FROM sqlite_master WHERE name='client_merge_history'"
    ).fetchone(),
)
# try create on local copy
conn.execute(
    """
    CREATE TABLE IF NOT EXISTS client_merge_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      keep_id INTEGER NOT NULL,
      discard_id INTEGER NOT NULL,
      operator TEXT,
      reason_code TEXT,
      note TEXT,
      merged_at TEXT NOT NULL,
      discard_client_json TEXT NOT NULL,
      discard_returns_json TEXT NOT NULL DEFAULT '[]',
      returns_actions_json TEXT NOT NULL DEFAULT '[]',
      status_events_json TEXT NOT NULL DEFAULT '[]',
      filetrack_history_json TEXT NOT NULL DEFAULT '[]'
    )
    """
)
conn.commit()
print("create ok")
conn.close()

# retest share
print("retest share…")
try:
    c2 = sqlite3.connect(r"T:\taxops\taxops.db", timeout=5)
    print("share clients", c2.execute("SELECT COUNT(*) FROM clients").fetchone()[0])
    c2.close()
except Exception as e:
    print("share still broken:", e)
