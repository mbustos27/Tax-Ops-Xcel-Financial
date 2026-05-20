## Parent epic
Reliability hardening epic

## Problem
Extractor sleeps fixed `POLL_INTERVAL = 60` between cycles (`taxops/extractor.py` ~154, ~181–188) — uploads can wait unnecessarily.

Snippet:
```python
def _worker_loop(app):
    while True:
        try:
            with app.app_context():
                _process_queue()
        ...
        time.sleep(POLL_INTERVAL)
```

## Fix
`threading.Event` wake-on-enqueue from document upload enqueue path (`utils`/upload route cooperation).

## Definition of done
- New queue rows processed within seconds when worker idle
- python -m pytest tests/ -v passes
