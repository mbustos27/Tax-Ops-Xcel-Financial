# R1 Phase 2A + 2C — address schema dry-run

Throwaway: `C:\Users\WINDOW~1\AppData\Local\Temp\r1_2a_an2sczye\throwaway.sqlite`

## Proposed DDL (schema v28 candidate)

```sql
ALTER TABLE clients ADD COLUMN address_street TEXT;
ALTER TABLE clients ADD COLUMN address_city TEXT;
ALTER TABLE clients ADD COLUMN address_state TEXT;
ALTER TABLE clients ADD COLUMN address_zip TEXT;
ALTER TABLE clients ADD COLUMN address_county TEXT;
ALTER TABLE clients ADD COLUMN address_source TEXT;
ALTER TABLE clients ADD COLUMN address_verified_at TEXT;
ALTER TABLE returns ADD COLUMN filing_status_drake TEXT;
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

## Simulated COALESCE (structured address; legacy `address` untouched)

- before: `{"taxpayer_email": null, "taxpayer_cell": "555-0100", "taxpayer_dob": null, "address": "100 OLD ST", "address_street": null, "address_city": null, "address_state": null, "address_zip": null, "address_county": null, "address_source": null, "spouse_dob": null, "spouse_cell": null}`
- written: `{"taxpayer_email": "a@example.com", "taxpayer_dob": "1990-01-01", "address_street": "2900 N EASTERN AVENUE", "address_city": "LOS ANGELES", "address_state": "CA", "address_zip": "90032", "address_county": "LOS ANGELES", "address_source": "drake_taxpayer_csv", "spouse_dob": "1992-02-02", "spouse_cell": "555-0200"}`
- after: `{"taxpayer_email": "a@example.com", "taxpayer_cell": "555-0100", "taxpayer_dob": "1990-01-01", "address": "100 OLD ST", "address_street": "2900 N EASTERN AVENUE", "address_city": "LOS ANGELES", "address_state": "CA", "address_zip": "90032", "address_county": "LOS ANGELES", "address_source": "drake_taxpayer_csv", "spouse_dob": "1992-02-02", "spouse_cell": "555-0200"}`

## Acceptance

| Check | Result |
|---|---|
| `address_cols_added` | PASS |
| `filing_status_drake_added` | PASS |
| `legacy_address_intact` | PASS |
| `coalesce_skip_filled_cell` | PASS |
| `coalesce_fill_email` | PASS |
| `coalesce_fill_street` | PASS |
| `coalesce_fill_zip` | PASS |
| `reconstruct` | PASS |

**Overall:** PASS

## Live notes

- Do **not** apply to `taxops.db` until Phase 3 approved.
- `CURRENT_SCHEMA_VERSION` is **27**; this bundle is **v28** (address cols + `filing_status_drake` + `client_profile_backfill_history`).
- Wire via `_migrate_existing_tables()` with `ALTER TABLE … ADD COLUMN` defaults.
- Phase 0.1 gate cleared by `TAXPAYER.csv` sha `33bcda02…`.
