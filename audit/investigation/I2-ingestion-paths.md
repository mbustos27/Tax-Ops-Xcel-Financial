# I2 — Ingestion path trace

Evidence: code citations + `I2-stats.json` + prior `T:\audit\output\jul1_event.json` (re-validated against snapshots).

---

## Path A — Tax Log / CSV import (`importer.process_csv`)

1. **Entry:** `taxops/importer.py:290` `process_csv(conn, csv_path, batch_id, source_file)`. Invoked from admin reimport (`app.py:9159+`) and historical `/upload` confirm flows. Manual operator action — not NSSM-scheduled.
2. **Parse:** CSV with header map including `"LOG 2025" → log_number` (`importer.py:48`). Rows missing log_number or tax_year skipped (`:256`, `:317`).
3. **Normalize:** `normalize_string`, preparer normalize, status mapping; names via `name_matcher`.
4. **Match:** `_match_return` (`:579`) — **LOG number first**, then fuzzy name vs candidates (`score_client_names_pair` ≥ `ACCEPT_THRESHOLD` 88).
5. **Persist:**
   - `_upsert_client` (`:646`): exact `lower(last_name)+lower(first_name)` SELECT; else INSERT (`:668`). **No UNIQUE on names.** Fuzzy match only when `_match_return` already forced a `client_id`.
   - `_upsert_return` (`:731`): SELECT by `(log_number, tax_year)` (`:744`); else INSERT (`:752`). Idempotent **on log+year**, not on client identity alone.
6. **Errors:** review_queue / needs_review paths for medium scores; ambiguous LOG_VS_NAME; batch counters. Failed parse rows counted in batch — not a separate reject file by default.
7. **Idempotency:** Re-run with same LOG# updates return; re-run **without** LOG match can mint a **new client** if exact name SQL misses (punctuation/`&`/spacing) even when fuzzy would merge — see Jul1 sims.

`import_batches` on live/jul31 still show Apr29 files (`TAX LOG 2025 Live.csv`, `TAXOPS.csv`, `CSMDATA.csv`) with `row_count=0` but `import_rows` has 3374 — batch accounting unreliable (`jul1_event.json` april_21 note).

---

## Path B — Drake CSM + purple prefill (`drake_prefill_importer.py`)

1. **Entry:** CLI `python drake_prefill_importer.py --phase N --apply …` (manual). Not in IMAP poll.
2. **Parse:**
   - CSM xlsx: `ID (Last 4)`, `Client Name`, Type, Status, Last Change, …
   - Purple: three CSV chunks (23/24/25 cols) + spouse CSVs; **2 title lines then header**; join key **`Taxpayer Name`**.
3. **Normalize:** CSM name normalize (comma-space collapse); purple FIRST LAST vs CSM `LAST, FIRST & SPOUSE`; fuzzy ≥ **90** local.
4. **Persist:** UPSERT `drake_prefill_links` / `drake_form_prefill` / `drake_household_prefill` keyed by link identity (`csm_ssn_last4`+name / link_id UNIQUE). Manual `match_tier='manual'` immutable.
5. **`--link-clients`:** `link_prefill_clients` (`:1924`) — match existing by SSN+name or `find_client` ≥90; else **`INSERT INTO clients`** stub (`:2016`).  
   **Live evidence:** 244 clients created `2026-08-07T22:27:47+00:00`, all with SSN, all linked in prefill (`I2-stats.json` `aug7`) — **[INFERRED]** this burst is `--link-clients`.

---

## Path C — Manual intake UI (`POST /intake`)

1. **Entry:** `app.py:3443` Flask route (NSSM TaxOps service).
2. **Parse:** form fields; tax year forced to active year.
3. **Normalize:** names uppercased; SSN→last4.
4. **Persist:** UPDATE existing `client_id` or INSERT client; INSERT return **or** UPDATE `PENDING INTAKE` shell (post–Aug 2026 fix). Unique `(client_id, tax_year)` enforced.
5. **Idempotency:** Returning client without PENDING shell → 409; with shell → reuse. New client always new id.

---

## Path D — Season rollover / preintake seed

1. **Entry:** `season_rollover.seed_preintake_commit` via admin UI (`app.py:8848+`) or post-import hook.
2. **Persist:** `INSERT` return `PENDING INTAKE`, `log_number NULL`, empty forms — one per missing `(client_id, target_year)`.
3. **Idempotency:** skips clients that already have target-year return.

---

## Path E — Findings-only audit (`python -m audit`)

1. **Entry:** `audit/__main__.py` operator CLI (`audit/README.md`).
2. **Parse:** Drake xlsx + Tax Log xlsx + TaxOps snapshot → `stage_*` in **separate** `audit_*.sqlite`.
3. **Match:** `audit/match.py` deterministic tiers (surname_first_token, paternal, transposition, entity_exact) — **no Anthropic/Claude** (`README`: “No Anthropic calls”).
4. **Persist:** audit DB only. SHA-256 is **of source files** at preflight (`audit/util.sha256_file`, `preflight.py:167+`), stored on `audit_run` — **not** a verdict cache keyed by normalized pair. **There is no Claude adjudication path and no SHA-256 verdict cache over match pairs.**

---

## Path F — Client merge

`merge_ops.py` / `api_merge_clients` — DELETE discard client after reassigning returns. Explains live Jul1 count drop 368→191 without “undoing” Aug7 inserts.

---

## July 1 event — exact counts & mechanism

### Counts (query)

```sql
SELECT COUNT(*) FROM clients WHERE substr(created_at,1,10)='2026-07-01';
```

| Snapshot | n |
|---|---:|
| `taxops_snapshot_20260731.sqlite` | **368** |
| live `taxops.db` (2026-08-10) | **191** |
| `taxops_backup_20260701T182519Z.sqlite` | **417** |

Jul31 timestamp distribution (`I2-stats.json`):

| created_at | n |
|---|---:|
| `2026-07-01 18:19:30` | 344 |
| `2026-07-01T20:10:33Z` | 18 |
| `2026-07-01T20:15:12Z` | 5 |
| `2026-07-01T19:36:51+00:00` | 1 |

### Column fill (Jul31 Jul1 clients)

`has_ssn=0`, `has_display=367`, `has_address=39`, `has_email=0`, `has_cell=39` (`I2-stats.json`).

### Pre-existing counterparts (Jul31)

Exact `upper(last)|upper(first)` vs clients with `created_at < 2026-07-01`:

| Class | n |
|---|---:|
| exact match to one pre row | **159** |
| no pre match | **209** |
| multi pre | 0 |

Samples: ORMA SERVICES INC (ids 8 vs 754), CATALPA DEVELOPMENT LLC (9 vs 164), DELGADO NORMA A (18 vs 240), …

Prior deeper census (`jul1_event.json` `duplicate_census_of_368`): duplicate_exact_lower 153 + norm-only 101 + ambiguous 5 + novel 114; **254 collide / 114 novel** among the 368 stamped IDs.

### Mechanism — LOUD CORRECTION TO BACKGROUND HYPOTHESIS

Background said: “bulk import ~368 **duplicate records**.”

**Refuted as insert-of-368-new-clients.** Prior investigation + revalidated evidence:

1. **No `import_batches` row on 2026-07-01** (only Apr29 batches).
2. Dominant cluster timestamp format `2026-07-01 18:19:30` is **SQLite `CURRENT_TIMESTAMP` / naive datetime**, **not** `utils.now()` ISO used by `importer.py` / intake (`jul1_event.json` verdict).
3. Reinterpretation (`jul1_event.json` `jul1_reinterpretation`):
   - Verdict: **`CREATED_AT_REWRITE_NOT_INSERT`**
   - All 344/344 space-format cluster clients already owned **pre-Jul1 returns**
   - 338/344 IDs fall in Apr21 id band (≤1753) — not append-only inserts
   - `updated_at` ISO at 19:36 while `created_at` space 18:19 — two writes
4. True same-day ISO creates ≈24 (`20:10`/`20:15`), not 344.
5. Twin/duplicate **minting** predates Jul1 (Apr21 foundation + Apr29 return seed). Jul1 **obscured provenance** by rewriting `created_at` onto one sibling (often null-SSN / log-bearing side).

**What Jul1 did (evidence-backed):**

| Time | Action |
|---|---|
| 18:19:30 space | Bulk `created_at` rewrite (or INSERT defaulting CURRENT_TIMESTAMP) on ~344 pre-existing clients — **[INFERRED tool: bad migration / ad-hoc SQL / rebuild script]** — not `process_csv` |
| 19:36:51 ISO | ~364 return upserts (mostly onto Apr21-stamped clients) — compatible with real importer |
| 20:10–20:15 ISO | ~24 genuine client creates |

**Code that permits duplicates generally (not Jul1-specific):** `_upsert_client` exact-name-only fallback (`importer.py:653-687`) with **no UNIQUE(last,first)**; fuzzy accept only when return match supplies `client_id`. Prefill `--link-clients` can also INSERT stubs (`drake_prefill_importer.py:2016`).

### Live residual

After merges: 191 Jul1-dated clients remain; exact-name duplicate groups live = **8 groups / 8 extra rows** (`I2-stats.json`). Aug7 +244 prefill-linked clients are a **separate** event.

---

## April 21 / 29 context (founding load)

- Apr21: 1075 clients (Jul31 snap) in two ISO bursts — likely early Drake/bootstrap (`jul1_event.json` `april_21_event`).
- Apr29: three `import_batches` seed returns onto those clients (834 of Apr21 clients get Apr29 returns).

---

## Stop note

I2 complete. July 1 “368 duplicates from bulk import” → **368 stamped rows, mostly created_at rewrite; ~254 name-twins of earlier rows; factory of twins is pre-Jul1.**
