## Parent epic
Reliability hardening epic

## Problem
`audit_service._enqueue_write` spawns `threading.Thread(target=_run, ...).start()` per write (~268–315 in `audit_service.py`) — thundering herd + extra sqlite connections under burst POST traffic.

## Current pattern excerpt
```python
def _enqueue_write(...):
    def _run() -> None:
        ...
        conn = get_connection()
        try:
            conn.execute(
                """INSERT INTO audit_log ...""",
                (...)
            )
            conn.commit()
        finally:
            conn.close()

    threading.Thread(target=_run, name="audit-log-write", daemon=True).start()
```

## Fix
Single long-lived writer + bounded queue + batch `executemany`; expose backlog depth via `/health` when safe.

## Definition of done
- No per-request thread spawn for audit insert path
- python -m pytest tests/ -v passes (audit tests still green)
