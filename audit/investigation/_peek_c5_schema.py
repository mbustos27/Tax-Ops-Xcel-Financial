import sqlite3
from pathlib import Path

p = Path(r"T:\audit\tmp\c5_import_triage_throwaway.sqlite")
c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
print("tables", [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")])
print("import_rows cols", [r[1] for r in c.execute("PRAGMA table_info(import_rows)")])
print("statuses", list(c.execute("SELECT status, count(*) FROM import_rows GROUP BY 1")))
print("n", c.execute("SELECT count(*) FROM import_rows").fetchone())
# sample log fields
cols = [r[1] for r in c.execute("PRAGMA table_info(import_rows)")]
print("sample", c.execute("SELECT * FROM import_rows LIMIT 1").fetchone())
c.close()
