# taxops

`taxops` is a workflow-aware CSV importer for a small tax office.  
The **manual LOG spreadsheet is the source of truth** for workflow state.

## Requirements

- Python 3.9+ (standard library only)

## Project layout

- `main.py` - scans incoming files, runs one-transaction-per-file import, moves files, prints summary
- `db.py` - SQLite connection, schema bootstrap, lightweight schema migration, indexes
- `importer.py` - row parsing, matching, upsert logic, review queue routing
- `normalizer.py` - normalization helpers for strings, booleans, currency, and dates
- `events.py` - status-event generation and duplicate-event protection
- `utils.py` - helpers (`hash_file`, `now`)
- `config.py` - folders and constants

## Supported manual LOG headers

- `LOG 2025`
- `LAST`
- `FIRST`
- `TAX PAYER NAME (S)`
- `YR`
- `PROCESSOR`
- `VERIFIED`
- `CLIENT STATUS`
- `INT'D`
- `25 TRANSF`
- `26 TRANSF`
- `EMAIL`
- `DATE EMAILED`
- `PICK UP`
- `LOG OUT`
- `TOTAL FEE`
- `RECEIPT #`
- `FEE PAID`
- `CC Fee`
- `Zelle or CK #`
- `Cash, Q Pay`
- `1040`
- `SCH A & D`
- `SCHED C`
- `SCHED E`
- `1120`
- `1120S`
- `1065/LLC`
- `Corp Officer`
- `Bus Owner`
- `1040X`
- `W7`
- `990/1041`
- `EXT`
- `TRANSFER`
- `UPDATED`
- `NOTES`
- `Referral`
- `Referred By`

Unknown columns are allowed and preserved in `import_rows.raw_json`.

## Tables created

- `clients`
- `returns`
- `return_forms`
- `payments`
- `notes`
- `status_events`
- `import_batches`
- `import_rows`
- `review_queue`

## Import behavior

1. Reads `data/incoming/*.csv`
2. Computes SHA256 hash and skips files already imported (`import_batches.file_hash`)
3. Creates one `import_batches` record per file
4. Processes rows and stores raw row JSON in `import_rows`
5. Uses matching priority:
   - return by `log_number + tax_year`
   - fallback `last_name + first_name + tax_year`
6. Routes ambiguous matches to `review_queue` and marks row action `REVIEW`
7. Upserts:
   - `clients`
   - `returns`
   - `return_forms`
   - `payments`
   - `notes` (deduped by normalized note text per return)
8. Creates workflow `status_events` only when tracked values actually change, with dedupe on `return_id + event_type + event_timestamp`
9. Uses one DB transaction per file; unexpected file-level failure rolls back file writes
10. Moves files:
    - success -> `data/processed`
    - failure -> `data/error`

## Status history rules

Events are generated for changes in:
- `intake_date` -> `INTAKE_RECORDED`
- `client_status` -> `STATUS_CHANGED`
- `verified` false/null -> true -> `VERIFIED_MARKED`
- `date_emailed` -> `EMAILED_TO_CLIENT`
- `pickup_date` -> `READY_FOR_PICKUP`
- `logout_date` -> `LOGGED_OUT`
- `updated_date` -> `RECORD_UPDATED`

## How to run

From the `taxops` folder:

```bash
python main.py
```
