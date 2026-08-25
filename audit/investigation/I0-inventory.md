# I0 — Inventory (Client Record Provenance Investigation)

_Generated: 2026-08-10 (Phase I0 only — discovery, no fixes)_

## Method constraints observed

- All SQLite opens used `sqlite3.connect("file:<path>?mode=ro", uri=True)`.
- Live `T:\taxops\taxops.db` had **no** `-wal`/`-shm` companions at copy time; main file copied to `T:\audit\investigation\snapshot\taxops.db`.
- Pre-copy / post-copy UTC mtime of live DB was unchanged (`MTIME_UNCHANGED True` in snapshot shell log).
- Supporting artifacts for this phase live only under `T:\audit\investigation\` (plus pre-existing `T:\audit\` package/outputs referenced as evidence).
- **Working-tree note:** `git status` already shows many modified files under `taxops/` and repo root from prior sessions (e.g. `taxops/app.py`). This phase did not edit those paths. Acceptance criterion “zero modifications outside investigation/” cannot hold for the whole dirty tree; it holds for *this investigation’s writes*.

## Loud assumption checks (Background hypotheses)

| Hypothesis | I0 status | Evidence |
|---|---|---|
| Join is name-only across Drake / Tax Log / TaxOps | **Deferred to I3** (inventory confirms name fields dominate; no shared cross-source GUID in schema) | TaxOps `clients.id` is local PK; CSM has `ID (Last 4)` + `Client Name`; Tax Log has LOG # + names. No Drake client GUID column in TaxOps schema (`I0-table-columns.json`). |
| 40-character name cap / truncation | **Supported by prior audit code + findings sheet** (not yet re-measured on live data in I0) | `audit/normalizer.py:198-203` `detect_truncation(..., threshold=39)`; findings workbook sheet `NAME_TRUNCATED` = 71 rows (`I0-ingestion-files.json`). |
| ~368 duplicate/bulk clients on July 1 | **Partially confirmed — count is snapshot-dependent** | See §July 1 counts below. July 31 TaxOps snapshot = **368**; live snapshot today = **191**; July 1 backup = **417**. Prior write-up: `T:\audit\output\jul1_event.json` `"n": 368`. Mechanism deferred to I2. |
| No stable shared identifier across all three | **Provisionally supported by schema inventory** | No single column is the Drake CSM ID *and* Tax Log LOG # *and* TaxOps PK. Candidates for I3: SSN last4, log_number, phone, email, name. |

### July 1 client-creation counts (exact queries)

```sql
SELECT COUNT(*) FROM clients WHERE substr(created_at,1,10)='2026-07-01';
```

| Database (all under `snapshot/`, mode=ro) | clients total | Jul 1 created |
|---|---:|---:|
| `taxops.db` (live copy, 2026-08-10) | 1526 | **191** |
| `taxops_snapshot_20260731.sqlite` | 1514 | **368** |
| `taxops_backup_20260701T182519Z.sqlite` | 1586 | **417** |
| `taxops_rebuilt.db` | 1586 | **417** |
| `taxops_recovered.db` | 60 | 0 |

```sql
SELECT substr(created_at,1,10) AS d, COUNT(*) AS n
FROM clients GROUP BY d ORDER BY n DESC LIMIT 8;
```

**Live snapshot top days:** 2026-04-21=1017, **2026-08-07=244**, 2026-07-01=191, 2026-04-22=31, …

**Jul31 snapshot top days:** 2026-04-21=1075, **2026-07-01=368**, 2026-04-22=31, …

Jul31 Jul1 timestamp distribution (matches `jul1_event.json`):

| created_at | n |
|---|---:|
| `2026-07-01 18:19:30` | 344 |
| `2026-07-01T20:10:33Z` | 18 |
| `2026-07-01T20:15:12Z` | 5 |
| `2026-07-01T19:36:51+00:00` | 1 |

Live Jul1 distribution: `18:19:30`=179, `T20:10:33Z`=11, `T19:36:51+00:00`=1.

**[INFERRED]** Between 2026-07-31 and 2026-08-10, ~177 of the Jul1-dated client rows were removed (merge/delete), while 244 clients were created in one burst `2026-08-07T22:27:47+00:00` (likely `drake_prefill_importer --link-clients` — confirm in I2).

---

## 1. SQLite databases reachable by the app / on T:

Config at import time: `taxops/config.py` `DB_PATH` → `C:\TaxOps\taxops\taxops.db`. **That file does not exist on this workstation.** Only `C:\TaxOps\taxops\CSVFILES\` is present. Reachable production DB from this session: `T:\taxops\taxops.db` (UNC `\\Xcel-server\taxops\taxops\taxops.db` per `audit/config.py:20`).

| Path | Exists | Size | Pages | Journal | WAL/SHM files | mtime | Role |
|---|---|---:|---:|---|---|---|---|
| `T:\taxops\taxops.db` | yes | 10289152 | 2512 | wal | no/no | 2026-08-10 ~10:52 | Live TaxOps (share) |
| `C:\TaxOps\taxops\taxops.db` | **no** | — | — | — | — | — | Configured DB_PATH |
| `T:\taxops\taxops_demo.db` | yes | 2871296 | 701 | delete | no/no | 2026-05-04 | Demo |
| `T:\taxops\taxops_prefill_apply_test.db` | yes | 9908224 | 2419 | wal | no/no | 2026-08-07 | Prefill test |
| `T:\taxops\taxops_rebuilt.db` | yes | 6049792 | 1477 | delete | no/no | 2026-07-01 11:19 | Jul1 rebuild artifact |
| `T:\taxops\taxops_recovered.db` | yes | 5844992 | 1427 | delete | no/no | 2026-07-01 11:13 | Jul1 recovery artifact |
| `T:\taxops\taxops_test.db` | yes | 360448 | 88 | wal | no/no | 2026-06-26 | Test |
| `T:\taxops\taxops_test_ext.db` | yes | 360448 | 88 | wal | no/no | 2026-06-26 | Test |
| `T:\taxops\.codegraph\codegraph.db` | yes | 5148672 | 1257 | wal | yes/yes | 2026-05-20 | CodeGraph (not clients) |
| `T:\taxops\backups\taxops_backup_20260701T182519Z.sqlite` | yes | 6139904 | 1499 | delete | no/no | 2026-07-01 11:25 | Backup |
| `T:\audit\audit_20260731.sqlite` | yes | 1794048 | 438 | delete | no/no | 2026-07-31 | Audit findings DB |
| `T:\audit\audit_202607311100.sqlite` | yes | 1761280 | 430 | delete | no/no | 2026-07-31 | Audit findings DB |
| `T:\audit\snapshots\taxops_snapshot_20260731.sqlite` | yes | 7163904 | 1749 | wal | no/no | 2026-07-31 | TaxOps snap for audit |
| `T:\punchbridge\punchbridge.db` | yes | 0 | — | — | — | 2026-07-10 | Empty stub |
| `T:\punchbridge\data\punchbridge.db` | yes | 1257472 | — | — | — | 2026-07-13 | Punchbridge |

Source: `I0-live-db-meta.json`, `I0-schema-dump.json`.

### Snapshot copies

Under `T:\audit\investigation\snapshot\`: `taxops.db`, `taxops_rebuilt.db`, `taxops_recovered.db`, `taxops_backup_20260701T182519Z.sqlite`, `taxops_snapshot_20260731.sqlite`, `audit_20260731.sqlite`, `audit_202607311100.sqlite`.

---

## 2. Schema dumps

Query (every snapshot DB):

```sql
SELECT type, name, sql FROM sqlite_master ORDER BY type, name;
```

- Verbatim for live snapshot: [`I0-sqlite-master-taxops.md`](I0-sqlite-master-taxops.md) (149 objects)
- Raw multi-DB JSON: [`I0-schema-dump.json`](I0-schema-dump.json)
- Name indexes for other DBs: `I0-sqlite-master-*.md`

### Live snapshot row counts (selected)

| table | rows |
|---|---:|
| clients | 1526 |
| returns | 1613 |
| spouses | 166 |
| dependents | 0 |
| client_dependents | 706 |
| client_billing | 848 |
| drake_prefill_links | 1379 |
| drake_form_prefill | 1264 |
| drake_household_prefill | 1302 |
| import_batches | 5 |
| import_rows | 3374 |
| email_inbox | 268 |
| status_events | 7281 |

Audit DBs (findings-only): `stage_drake`=1155, `stage_taxops_client`=1514, `stage_log`=2820, `audit_match`≈3097–3167 (`I0-schema-dump.json`).

---

## 3. Client-identity tables

Heuristic list (56 tables whose name/SQL matched identity keywords): [`I0-identity-tables.json`](I0-identity-tables.json).

### Primary identity stores

| Table | Rows | Holds |
|---|---:|---|
| `clients` | 1526 | Names, ssn_last4, phones, emails, address, spouse name columns, prior_year_log |
| `returns` | 1613 | client_id FK, log_number, tax_year, status, banking, intake fields |
| `spouses` | 166 | Separate spouse rows; Drake import path; unique-one-per-client index in migrations |
| `client_dependents` | 706 | Client-level dependents; `drake_dependent_id` |
| `dependents` | 0 | Return-level (intake); empty in live snapshot |
| `client_billing` | 848 | Balance snapshot |
| `drake_prefill_links` | 1379 | CSM↔purple name link + optional `client_id` |
| `drake_form_prefill` | 1264 | Form counts by link_id |
| `drake_household_prefill` | 1302 | Spouse/dependent prefill by link_id |
| `import_batches` / `import_rows` | 5 / 3374 | Import staging |
| `email_inbox` | 268 | Holding-area email metadata (no body) |

### Columns (`PRAGMA table_info`)

Full dump: [`I0-table-columns.json`](I0-table-columns.json).

**clients:** id, last_name, first_name, display_name, ssn_last4, referral_flag, referred_by, created_at, updated_at, spouse_last_name, spouse_first_name, taxpayer_dob, spouse_dob, taxpayer_occupation, spouse_occupation, taxpayer_phone, taxpayer_cell, taxpayer_work_phone, spouse_cell, spouse_work_phone, taxpayer_email, spouse_email, address, is_new_client, prior_year_log, id_type

**drake_prefill_links:** id, tax_year, csm_ssn_last4, csm_name_raw, csm_name_norm, client_id, prefill_status, disposition_status, csm_status_raw, purple_name, purple_name_norm, match_tier, match_score, match_variant, source_batch_id, created_at, updated_at, … (20 cols — see JSON)

CREATE SQL excerpts: `I0-schema-dump.json` / `I0-sqlite-master-taxops.md`.

---

## 4. Code read/write touchpoints (identity tables)

Non-test production writers/readers (grep on `T:\taxops` / `T:\audit`):

| File:line | Operation | Calling context |
|---|---|---|
| `taxops/importer.py:290` | `process_csv` | Batch CSV / Tax Log import entry |
| `taxops/importer.py:668` | `INSERT INTO clients` | Import create |
| `taxops/importer.py:709` | `UPDATE clients` | Import update-on-match |
| `taxops/importer.py:752` | `INSERT INTO returns` | Import |
| `taxops/app.py:3547` / `:3583` | UPDATE/INSERT clients | `POST /intake` |
| `taxops/app.py` (returns block ~3615+) | INSERT/UPDATE returns | Intake (+ PENDING INTAKE reuse) |
| `taxops/app.py:4098-4237` | fuzzy + INSERT clients/returns | `/upload/confirm` |
| `taxops/app.py:7127+` | merge APIs | → `merge_ops.py` |
| `taxops/app.py:8868` | seed preintake | Admin season rollover UI |
| `taxops/app.py:9159+` | `process_csv` | Admin reimport |
| `taxops/merge_ops.py:157` / `:300` | UPDATE/DELETE clients | Merge keep/discard |
| `taxops/db.py:688-844` | DELETE/UPDATE clients | Dedup / merge helpers |
| `taxops/drake_prefill_importer.py:1302+` | UPSERT prefill tables | CLI `--phase 4 --apply` |
| `taxops/drake_prefill_importer.py:2016` | `INSERT INTO clients` | `--link-clients` may mint clients |
| `taxops/season_rollover.py` | INSERT returns `PENDING INTAKE` | Season seed |
| `taxops/routes/documents.py` | JOIN clients | Document APIs |
| `taxops/audit_service.py` | SELECT clients | In-app audit |
| `audit/ingest.py` | stage_* only | Findings-only audit — **does not write TaxOps** (`audit/README.md`) |
| `audit/match.py`, `audit/jul1_event.py` | read TaxOps + stage | Matcher / Jul1 analysis |

**Invocation:** NSSM TaxOps Flask hosts HTTP routes; mail watcher does not write clients (holding-area invariant). Prefill importer is **manual CLI**, not a poll cycle. Audit package is operator-run (`python -m audit …`).

---

## 5. Ingestion inputs on disk

Machine-readable: [`I0-ingestion-files.json`](I0-ingestion-files.json).

### Drake CSM exports

| Path | mtime | Rows | Cols | Header (Sheet1 row 1, verbatim) |
|---|---|---:|---:|---|
| `T:\taxops\CSVFILES\2024 CLIENTS.xlsx` | 2026-08-07 10:59:43 | 2908 | 15 | `ID (Last 4) \| Client Name \| Type \| Preparer \| Status \| Started \| Completed \| Last Change \| Changed By \| Refund \| BalDue \| Total Bill \| Bank Deposits \| Client Payments \| Amount Owed` |
| `C:\Users\Windows 10\Desktop\CLIENTS.xlsx` | 2026-06-23 12:07:05 | 1155 | 15 | same 15 columns |

`audit/config.py` expects OneDrive CLIENTS.xlsx with **1159** rows. Fixed OneDrive paths checked this session (`Shared/`, `Documents/`, `Desktop/` under the firm OneDrive) — **not found**. Desktop 1155 matches audit `stage_drake` count.

### Purple-sheet chunks (TY2024)

Drake report CSVs: 2 title lines, then CSV header. Corrected parse:

| Path | Title lines | Data rows | Cols | Header (verbatim) |
|---|---|---:|---:|---|
| `T:\taxops\CSVFILES\TY2024S.csv` | `TY2024 SCHEDA-2350`, `As of 08-07-2026` | 1298 | 23 | `Taxpayer Name,Schedule A,…,Form 2350 Indicator,Form 2210` |
| `T:\taxops\CSVFILES\TY2024S2439-5405.csv` | `TY2024 2439-5405`, `As of 08-07-2026` | 1298 | 24 | `Taxpayer Name,Form 2439,…,Form 5405` |
| `T:\taxops\CSVFILES\TY2024S8867-end.csv` | `TY2024 8867-END`, `As of 08-07-2026` | 1298 | 25 | `Taxpayer Name,…` (25 cols — matches “25-column ceiling” claim for this chunk) |
| `T:\taxops\CSVFILES\TY2024Spouses.csv` | `TY2024 Taxpayer spouse info`, `As of 08-07-2026` | 1610 | 8 | `Taxpayer Name,Taxpayer Date of Birth,Taxpayer Cell Phone,Spouse Name,…,Dependent Last Name` |
| `T:\taxops\CSVFILES\TAXPAYERspouse25.csv` | `TY2025 SPOUSE`, `As of 08-07-2026` | 1326 | 10 | (TY2025 spouse export) |

Duplicates also under `C:\TaxOps\taxops\CSVFILES\` (same sizes/mtimes for spouse files).

Reassembly key for purple chunks: **`Taxpayer Name`** (to be traced in I2 via `drake_prefill_importer.py`).

### Tax Log

| Path | mtime | Size | Notable sheets |
|---|---|---:|---|
| `C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC\Shared\Logs\TAX LOG 2025 Live.xlsx` | 2026-07-07 12:07:06 | 1924717 | `XCEL 2025` 2070×48; `1120 CORP LIST` 129; `1120 S LIST` 328; `1065 & LLC LIST` 103; extension sheets; etc. |

`audit/config.py`: individuals sheet `XCEL 2025`, data starts row 6 (`LOG_DATA_START_ROW=6`). Openpyxl row-1 “header” is mostly blank — real headers are mid-sheet.

### Prior audit outputs (not sources of truth)

`T:\audit\output\`: findings workbooks (`NAME_TRUNCATED` 71, `PHANTOM_IN_TAXOPS` 227, `DUPLICATE_CLIENT` 142, …), `jul1_event.json`, matcher diagnostics.

---

## Supporting artifacts this phase

- `snapshot/*`
- `I0-schema-dump.json`, `I0-live-db-meta.json`, `I0-identity-tables.json`, `I0-table-columns.json`, `I0-ingestion-files.json`
- `I0-sqlite-master-*.md`
- Helper scripts `_i0_*.py` (scratch; under investigation/)

---

## Phase I0 complete — stop

**Ready for Phase I1** (field-level provenance map). Say the word to continue.

### I0 headline for the next phase

1. Live TaxOps DB is on the share (`T:\taxops\taxops.db`), not `C:\TaxOps\...`.
2. July 1 “368” is real on the **Jul31** snapshot; live DB now has **191** Jul1-dated clients — population has changed.
3. Aug 7 created **244** clients in one timestamp — new ingestion event to explain in I2.
4. Purple CSVs are 23/24/25-col chunks joined on `Taxpayer Name`; CSM has only last-4 + name as identifiers.
