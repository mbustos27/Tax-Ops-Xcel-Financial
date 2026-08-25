import sqlite3
import time
from pathlib import Path

paths = [
    Path(r"T:\taxops\taxops.db"),
    Path(r"\\Xcel-server\taxops\taxops\taxops.db"),
]
for p in paths:
    print("===", p, "exists", p.exists())
    if not p.exists():
        continue
    try:
        t0 = time.time()
        conn = sqlite3.connect(str(p), timeout=10)
        conn.execute("PRAGMA busy_timeout=10000")
        print("connected", round(time.time() - t0, 2))
        print("integrity", conn.execute("PRAGMA quick_check").fetchone())
        print(
            "history",
            conn.execute(
                "SELECT name FROM sqlite_master WHERE name='client_merge_history'"
            ).fetchone(),
        )
        print("clients", conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0])
        conn.close()
    except Exception as e:
        print("ERR", type(e).__name__, e)
