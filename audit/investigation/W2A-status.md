# Wave 2A status check (2026-08-11)

## `client_merge_history`

**Exists.** Live `taxops.db` has the table; **7** trail rows (Wave 2B merges). Columns include `keep_id`, `discard_id`, `operator`, `reason_code`, `note`, `merged_at`, plus JSON snapshots (`discard_client_json`, `discard_returns_json`, `returns_actions_json`, `status_events_json`, `filetrack_history_json`).

## Spouse fold reversibility

**Not snapshotted.** Wave 4 fold (`source='wave4_clients_fold'`, **354** rows → `spouses` total **520**) went through `_w4_spouse_fold.py` / schema v26 without a `spouse_fold_history` (or similar) table. Merge trail was scoped to **client merges only** — the fold is not reconstructable from `client_merge_history`.

Rollback path if needed: restore from pre-fold DB backup / Jul31 snapshot, or delete `spouses` where `source='wave4_clients_fold'` (clients.spouse_* columns were left populated as deprecated read-through, so name text still exists on `clients`).
