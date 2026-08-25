# I1 — Field-level provenance map

_Source DB: `T:\audit\investigation\snapshot\taxops.db` (live copy, mode=ro). Stats: `I1-stats.json`._

## How to read this map

Each row: column → type/constraints → source of truth → populated by → transform → fill / distinct / max length.  
**Truncation flag** when `max_len` equals a known width (39/40/50/64/…) **and** at least one value sits exactly at that length.

---

## `clients` (1526 rows)

| Column | Type / constraints | Source of truth | Populated by | Transform | Fill rate | Distinct | Max len | Truncation? |
|---|---|---|---|---|---:|---:|---:|---|
| `id` | INTEGER PK | TaxOps-minted | SQLite AUTOINCREMENT on INSERT (`importer.py:668`, `app.py:3583`, `drake_prefill_importer.py:2016`) | none | 1.0 | 1526 | — | no |
| `last_name` | TEXT | Firm / import / intake | Import `_upsert_client`; intake UPDATE/INSERT; prefill `parse_name(csm_name_raw)` | Uppercase on intake (`app.py`); import uses CSV as-parsed via `name_matcher.parse_name` / normalize; prefill from CSM `LAST, FIRST` | 1.0000 | 878 | **39** | **YES — 2 rows at 39** |
| `first_name` | TEXT | same | same | Spouse strip `& …` in matcher (`name_matcher.strip_spouse:215`); middle-initial strip for match only | 0.9010 | 1032 | 31 | no |
| `display_name` | TEXT | Import / CSM display line | `_upsert_client` stores `display_name` from import (`importer.py:668-683`); Jul1 cluster heavily uses this | Often raw CSM / log display | 0.2484 | 379 | **40** | **YES — 1 at 40, 3 at 39** |
| `ssn_last4` | TEXT | Drake CSM last-4 / intake full-SSN→last4 | Prefill link (`drake_prefill_importer.py` SSN path); intake `_ssn_last4_from_full` (`app.py:3498`); **not** filled by Jul1 rewrite cluster (0/368 on Jul31 snap) | Digits only, last 4; never store full SSN | 0.9037 | 1294 | 4 | no (by design) |
| `spouse_*` name/dob/phone/email | TEXT | Intake / CSM joint / spouse import | Intake fields; `_fill_spouse_if_empty` from CSM `&` parse (`drake_prefill_importer.py:1904+`); `spouses` table is parallel store | Uppercase names on intake; joint parse | (varies; spouse cols sparse vs taxpayer) | — | — | see table JSON |
| `taxpayer_dob` / `spouse_dob` | TEXT | Intake / purple household | Intake; `drake_household_prefill` is separate (not clients) | date strings as entered | 0.5550 (taxpayer_dob) | 830 | — | no |
| `taxpayer_cell` | TEXT | Intake / purple | Intake COALESCE update; purple spouse CSV has cell | phone formatting varies | 0.6088 | 855 | 14 | no |
| `taxpayer_phone` / work / spouse phones | TEXT | Intake | Intake | — | phone 0.0485 | 70 | — | no |
| `taxpayer_email` / `spouse_email` | TEXT | Intake | Intake COALESCE | — | **0.0360** | 53 | 36 | no |
| `address` | TEXT | Intake / import | Intake; some import paths | — | 0.6239 | 793 | 34 | no |
| `prior_year_log` | TEXT | Intake / rollover carry | `season_rollover` may set; intake `_v("prior_year_log")` | string log # | 0.0288 | 44 | 4 | no |
| `referred_by` / `referral_flag` | TEXT/INT | Intake | Intake | — | low | — | — | no |
| `is_new_client` | INT DEFAULT 0 | Intake | Intake form | — | — | — | — | no |
| `id_type` | INTEGER | later migration | sparse | — | — | — | — | no |
| `created_at` / `updated_at` | TEXT | Writer clock | `utils.now()` ISO on importer/intake; **Jul1 cluster used space-format `YYYY-MM-DD HH:MM:SS`** (not `utils.now()`) — see I2 | timestamp string formats mixed | 1.0 | — | — | n/a |

---

## `returns` (1613 rows)

| Column | Type / constraints | Source of truth | Populated by | Transform | Fill | Distinct | Max | Trunc? |
|---|---|---|---|---|---:|---:|---:|---|
| `id` | INTEGER PK | TaxOps | INSERT intake / import / rollover | — | 1.0 | 1613 | — | no |
| `client_id` | INTEGER NOT NULL FK | TaxOps | all writers | — | 1.0 | — | — | no |
| `log_number` | TEXT; UNIQUE with tax_year (`ux_returns_log_year`) | **Tax Log** (office sequence) | Import `_upsert_return` match on `(log_number, tax_year)` (`importer.py:744-745`); intake MAX+1 | normalize_string | 0.7433 | 1199 | — | no; **0 duplicate (log,year) pairs** |
| `tax_year` | INTEGER | Admin active year / import | `get_active_intake_tax_year`; import CSV | — | high | — | — | no |
| `client_status` | TEXT | Workflow | Import, intake→PROCESSING, rollover→PENDING INTAKE, FileTrack | status normalizer | high | — | — | no |
| `processor` | TEXT | Log / intake | import + intake `normalize_preparer` | — | — | — | — | no |
| `drake_status_raw` | TEXT | Drake CSM Status | Drake import / sync | raw string | — | — | **39** | weak (1 at 39) |
| banking / estimates / flags | TEXT/NUM | Intake | `POST /intake` | — | sparse | — | — | no |
| `created_at` | TEXT | Writer | import/intake/rollover | ISO vs space | 1.0 | — | — | n/a |

Unique indexes relevant to identity: `idx_returns_unique_client_year` (client_id, tax_year WHERE status≠CANCELLED) — `db.py:1487-1490`; `ux_returns_log_year` — `db.py:1665`.

---

## `spouses` (166) / `client_dependents` (706)

| Table.column | Source | Populated by | Notes |
|---|---|---|---|
| `spouses.drake_spouse_id` | Drake | spouse import path | optional external id |
| `spouses.first_name` NOT NULL | Drake / import | spouse importer | UNIQUE one-per-client index (`db.py`) |
| `client_dependents.drake_dependent_id` | Drake | dep import | UNIQUE (client_id, drake_dependent_id) |
| `client_dependents.ssn_last4` | Drake | dep import | last4 only |
| `drake_household_prefill.*` | Purple spouse/dep CSV | `drake_prefill_importer` Phase household | **Intentionally not** written to `spouses` / `client_dependents` (`drake_prefill_importer.py` docstring ~1654) |

---

## `drake_prefill_links` (1379)

| Column | Source of truth | Populated by | Transform | Fill | Distinct | Max | Trunc? |
|---|---|---|---|---:|---:|---:|---|
| `csm_ssn_last4` | CSM `ID (Last 4)` | Phase 1 CSM ingest | as text | 1.0 | 1233 | 4 | no |
| `csm_name_raw` | CSM `Client Name` | Phase 1 | stored raw | 1.0 | 1376 | **40** | **YES — 74 at 40, 24 at 39** |
| `csm_name_norm` | derived | importer normalize (comma-space collapse etc.) | upper / punctuation normalize | 1.0 | 1376 | 41 (stats) / boundary mass at 39–40 | **YES — 66–68 at 40** |
| `purple_name` | Purple `Taxpayer Name` | Phase 2 join on name | FIRST LAST order (opposite CSM) | 0.9449 | 1295 | **51** | 12 at 40; purple can exceed 40 |
| `purple_name_norm` | derived | same | — | — | — | — | — |
| `client_id` | TaxOps FK | `--link-clients` (`link_prefill_clients:1924`) | match SSN+name or fuzzy≥90 or INSERT stub | high after Aug7 | — | — | no |
| `match_tier` / `match_score` / `match_variant` | matcher | Phase 2 | local threshold **90** (not `ACCEPT_THRESHOLD` 88) | — | — | — | no |
| `prefill_status` | derived | Phase 2 CHECK enum | — | 1.0 | — | — | no |

---

## External file columns (not DB)

| Source | Field | Max observed | At 40 | Notes |
|---|---|---:|---:|---|
| Desktop `CLIENTS.xlsx` (1155) | Client Name | **40** | **54** (+17 at 39) | Confirms Drake CSM 40-char export ceiling |
| `CSVFILES/2024 CLIENTS.xlsx` | Client Name | **40** | **137** | Same ceiling; last4 collisions high (composite universe) |
| Tax Log `XCEL 2025` | last name (col C) | 39 | 0 at 40; 3 at 39 | Log names not hard-capped at 40 the same way |
| Purple CSVs | Taxpayer Name | up to 51 in DB | 12 at 40 | Often fuller than CSM |

---

## Transforms catalog (cite)

| Transform | Where |
|---|---|
| Uppercase, strip `-.,&;'`, collapse WS | `name_matcher._clean:38-44` |
| Suffix strip JR/SR/… | `name_matcher._strip_suffixes` |
| `LAST, FIRST` parse | `name_matcher.parse_name:75+` |
| Joint `&` spouse strip for matching | `name_matcher.strip_spouse:215` |
| Fuzzy `token_sort_ratio` | `name_matcher.score_client_names_pair` / `find_client` via rapidfuzz |
| Accept ≥88 (import) / ≥90 (prefill local) | `ACCEPT_THRESHOLD=88` (`name_matcher:24`); prefill comment `drake_prefill_importer.py:9,36` |
| Audit NFKD accent fold, particles, paternal variants | `audit/normalizer.py` `fold_accents`, `surname_variants`, `detect_truncation(threshold=39):198` |
| Intake SSN→last4 | `app.py:3498-3504` |

---

## Truncation suspects (summary)

From `I1-stats.json` → `truncation_suspects`:

1. **`drake_prefill_links.csm_name_raw` max=40, n_at_max=74** — primary Drake ceiling evidence  
2. `clients.display_name` max=40, n_at_max=1  
3. `clients.last_name` max=39, n_at_max=2  
4. `returns.drake_status_raw` max=39, n_at_max=1 (likely coincidental)

**Hypothesis “40-char cap somewhere” — CONFIRMED** for Drake CSM `Client Name` (xlsx max 40; 54/1155 names exactly 40 on Desktop export). TaxOps does not declare a VARCHAR(40); the cap is **upstream in Drake’s export**.
