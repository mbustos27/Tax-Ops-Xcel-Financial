## Parent epic
Reliability hardening epic

## Problem
`taxops/app.py` only runs `init_db`, watchers, warmup, waitress under `if __name__ == "__main__":` (~5756+). Alternate entrypoints (`waitress-serve module:app`) skip migrations + daemon startup.

Snippet:
```python
if __name__ == "__main__":
    conn = get_connection()
    init_db(conn)
    conn.close()
    start_mail_watcher(app)
    start_extraction_worker(app)
```

## Fix
App-factory/register pattern: callable `register_workers(app)` invoked when `create_app()` returns (both dev & production imports).

Also note additional startup threads nearby (chat cache classifier warm) — unify under same hook.

## Definition of done
- `waitress-serve taxops.app:app`-style launches still migrate DB + start background daemons OR documented single supported entry remains `python app.py`
- python -m pytest tests/ -v passes
