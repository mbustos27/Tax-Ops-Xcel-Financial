# R1 Phase 2C — snapshot schema dry-run

Throwaway DB: `C:\Users\WINDOW~1\AppData\Local\Temp\r1_snapshot_39l92xkn\throwaway.sqlite`

## Proposed DDL (schema v28 candidate)

```sql
CREATE TABLE IF NOT EXISTS client_profile_backfill_history (
  id                    INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id             INTEGER NOT NULL,
  run_label             TEXT NOT NULL,
  applied_at            TEXT NOT NULL,
  bare_log_number       INTEGER NOT NULL,
  invoice_number        TEXT NOT NULL,
  source_export_sha256  TEXT,
  fields_written_json   TEXT NOT NULL,
  before_json           TEXT NOT NULL,
  after_json            TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cpbh_client
  ON client_profile_backfill_history(client_id, applied_at);
CREATE INDEX IF NOT EXISTS idx_cpbh_run
  ON client_profile_backfill_history(run_label);
```

## Simulated COALESCE

- before: `{"taxpayer_email": null, "spouse_email": null, "taxpayer_cell": "555-0100", "spouse_cell": null, "taxpayer_dob": null, "spouse_dob": null}`
- drake: `{"taxpayer_email": "new@example.com", "spouse_email": "spouse@example.com", "taxpayer_cell": "999-9999", "spouse_cell": "555-0200", "taxpayer_dob": "1990-01-01", "spouse_dob": null}`
- fields_written: `{"taxpayer_email": "new@example.com", "spouse_email": "spouse@example.com", "spouse_cell": "555-0200", "taxpayer_dob": "1990-01-01"}`
- after: `{"taxpayer_email": "new@example.com", "spouse_email": "spouse@example.com", "taxpayer_cell": "555-0100", "spouse_cell": "555-0200", "taxpayer_dob": "1990-01-01", "spouse_dob": null}`

## Acceptance checks

| Check | Result |
|---|---|
| COALESCE skipped filled taxpayer_cell | PASS |
| COALESCE filled empty taxpayer_email | PASS |
| COALESCE filled empty spouse_cell | PASS |
| Reconstruct from before_json restores pre-state | PASS |

**Overall:** PASS

## Live taxops notes

- Do **not** apply this DDL to live until Phase 3 is approved.
- Mirror pattern: `client_merge_history` (schema v25) in `taxops/db.py`.
- `CURRENT_SCHEMA_VERSION` is currently **27**; this would be **v28**.
- Address structured columns still blocked on Phase 0.1 City/State/ZIP re-export.
