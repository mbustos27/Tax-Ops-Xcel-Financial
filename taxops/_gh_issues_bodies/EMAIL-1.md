## Parent epic
Epic: Email watcher stability (`mail_watcher.py`)

## Symptoms
Repeated PDFs differing only by `_1`, `_2` counters for same Gmail UID.

## Code review checklist (`mail_watcher.py`)
1. **Module `_poll_lock = threading.Lock()`** — ✅ exists (~L30).
2. **`_poll_once()` acquisition** — `if not _poll_lock.acquire(blocking=False): skip` (~L98) then **`finally` release** (~L104).
3. **`_processed_uids` session memo** `(folder, uid_str)` (~L389-393 vs ~L418-427) — reconcile with **`_dispatch_classified_message`** early-return paths (e.g., `known_rule` layers now `_mark_read` after dispatch).

## Diagnostics
Instrument once per prod cycle counters:
```
Folder INBOX: N new unseen
Previous poll cycle still running — skipping
```

## Deliverable
Demonstrate logs show **≤1 classify+dispatch pipeline** per unseen UID absent genuine server restart mid-flight.

