## Parent epic
Security hardening epic

## Problem
SQLite connections only set `PRAGMA foreign_keys = ON` (`taxops/db.py` ~10–14). No WAL mode, no `busy_timeout` — contention under web + audit + extractor + mail watcher.

## Current code (`db.py`)

```python
def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn
```

## Fix (apply after verifying with EXPLAIN/load test)
```python
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
conn.execute("PRAGMA busy_timeout=10000")
conn.execute("PRAGMA temp_store=MEMORY")
```

## Definition of done
- `PRAGMA journal_mode` returns `wal`
- Busy timeout exercised under concurrent reads/writes
- python -m pytest tests/ -v passes
