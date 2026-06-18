## Parent epic
Epic: Email watcher stability (`mail_watcher.py`)

## Target hook
`_save_attachments(app, message, return_id:int)` (~L1429+):
- Validates MIME fragments, writes disk, **`INSERT INTO return_documents`**, invokes **`utils._enqueue_extraction`**.

## Enhancements
1. **Cheap dedupe**: same `return_id` + sanitized filename + `file_size_bytes` + `is_deleted=0`.
2. **Strong dedupe**: `file_hash TEXT` nullable column (**safe migration** via `ALTER TABLE return_documents ADD COLUMN file_hash TEXT` in **`db.py` `_migrate_existing_tables`**), compute **`hashlib.sha256(payload)`**.
3. On duplicate skip, log **`INFO Skipping duplicate attachment`**.

## Privacy
Hash is content-derived — acceptable internal dedupe artifact; ensure not exported to insecure logs beyond filename hash prefix if policy demands.

## Tests
Extend fixtures under `tests/test_*mail*` or introduce focused unit covering duplicate branch.

