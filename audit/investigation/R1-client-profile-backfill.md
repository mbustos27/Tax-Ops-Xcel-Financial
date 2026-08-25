# R1 — Client profile backfill (spec)

_Drafted 2026-08-12. Source: `TAXPAYERspouseaddressstatus.csv` ("INVOICE NUMBER LINK", as of 08-12-2026, 25 cols, 1119 data rows)._
_Canonical ingest path: `T:\audit\investigation\exports\TAXPAYERspouseaddressstatus.csv` (copied from Drake `F:\DRAKE25\…` at calibration; hashed with `sha256_file`). Do not join off the Drake program directory._
_Status: spec only. Phases 0–2 are read-only. No TaxOps write happens before Phase 3, and none at all before the snapshot table exists._

---

## Why this differs from Wave 4

Wave 4 folded spouse data by name match and put three known-wrong people onto live client records (TAREEN→MARTINEZ, NELSON→QUINTANA, EVELYN→HUERTA DANNY), with no snapshot to undo it. This spec exists to not repeat that. Two structural guards:

1. **Never match on name.** Join `Invoice Number → bare log → (returns.log_number, tax_year) → returns.client_id`. Names are used only as a post-hoc *disagreement check*, never to establish the link.
2. **Never overwrite.** COALESCE-only. A wrong existing value stays wrong and stays visible as a finding; it does not get silently replaced, and a correct existing value can't be clobbered.

Consequence worth accepting up front: this fills gaps, it does **not** fix the contamination. Those three clients keep their wrong spouse until someone corrects them deliberately. That is the right trade — a repair pass that overwrites is a different, riskier operation and should be its own wave with its own review.

---

## Phase 0 — Fix the export before building anything

Three defects, all fixable at the Drake report level, all cheaper to fix than to work around.

| # | Defect | Detail | Ask |
|---|---|---|---|
| 0.1 | **No city / state / zip** | `Street Address` is street-only (max 32 chars, zero rows contain a state token). `County` is a real county — 27 distinct values incl. SAN BERNARDINO, RIVERSIDE, FAYETTE, CLARK, MARICOPA — **not** a city. The address is unmailable as exported. | Add City, State, ZIP to the report selection. **Blocks the schema migration** — design the columns against the real shape, not a guess. |
| 0.2 | **110 ragged rows** | 75 rows at width 17, 35 at width 19, 1008 at 25. What drops is the trailing block — Fed Ack and the entire Filing Status group. ~10% lose the single most valuable field in the report. | Move `Filing Status` + FS flags ahead of the ack columns, same fix that worked for `link_5`. |
| 0.3 | **Invoice 81.5% filled** | 912 of 1119 rows carry an invoice; 207 cannot join deterministically. Plus 27 malformed values: `250`, `25`, `251`, `25037`, `2500892`, `2500786`, `25549`. | Re-export after 0.1/0.2 and re-measure. The malformed set overlaps the known `MALFORMED_LOG_NUMBER` findings — same office fix. |

**Gate:** do not build Phase 1 against the current file. One re-export answers all three.

### Re-export: `TAXPAYER.csv` (as of 08-13-2026) — Phase 0.1/0.2 **PASS**

Copied to `T:\audit\investigation\exports\TAXPAYER.csv` (sha256 `33bcda025ef8a4db1e7b189da13dbebbd7c7ea170d2f9c5f5220a6ca6cf529c8`). Full write-up: `R1-phase0-taxpayer-csv.md`.

| Gate | Result |
|---|---|
| 0.1 City + State (+ County) | **PASS** — 1327/1328 |
| 0.1 ZIP | **PASS** — 1327 |
| 0.1 Street | **PASS** — 1327 (`Street Address`) |
| 0.2 Filing Status | **PASS** — 1217 filled |
| 0.2 Ragged | **PASS*** — 1327×17; one junk trailer |
| Invoice strict `25XXXX` | 1072 row hits → **824** distinct after dependent collapse |

**Address schema unblocked** — Phase 2A (`address_street/city/state/zip/county`) may proceed against this shape. Collapse dependent rows before join. Still prefer spouse email / split name fields from `TAXPAYERspouseaddressstatus.csv` where this file only has `Spouse Name` + phone/DOB.

### Calibration (strict invoice, 2026-08-13) — prior `TAXPAYERspouseaddressstatus.csv`

Join gate: invoice must match **`^25\d{4}$` exactly** — no `bare_log_number` on anything else. Proof: `25141` (PEREZ, ANDRES) lenient-parses to **141** and manufactures the I7 collision; COALESCE cannot catch filling an empty field on the wrong client.

| Spec / prior | Strict measured |
|---|---|
| Joinable ~750–800 → then 686 lenient | **675** (45.2% of 1493 clients) |
| Collision bares 57 / 59 | **57** (118 rows); office-packet overlap in Phase 1 |
| Double-write risk | **0** PASS |
| Strict `25XXXX` rows | **885** |
| Non-canonical | **23** distinct / **27** rows → `R1-malformed-invoice-worklist.md` |
| Hazard | **`25141` → never parse** |
| No TaxOps (clean key) | **92** · C5 ERROR/REVIEW absent overlap **41** · L0_DRAKE_ONLY overlap **34** · only-profile **19** |
| Email COALESCE gap | **538** (55 → ~593, ~11×) |
| P4 Single+spouse / MFJ+none | **15 / 36** on joinable; extrapolate ~**33 / 80** if rate holds firm-wide |

Artifacts: `R1-phase0-calibration.md`, `R1-phase1-join.md`, `R1-phase4-fs-crosscheck.md`, `R1-malformed-invoice-worklist.md`, `R1-calibration.json`.

---

## Phase 1 — Key quality (read-only)

On the current file, for calibration:

- 912 rows with invoice → **824 distinct bare logs**
- **57 bare logs have multiple rows** — including `203`, `206`, `207`, `247`, `339`, `787`, `888`, `1095`, `1103`

That last line matters. 57 collisions here versus 19 in `TAXPAYER.csv`, and the overlap includes the exact bares in the office packet. **Any bare with >1 claimant is excluded from the backfill join** — an ambiguous key must not write demographics onto a client. Those rows drop to a manual queue and are the office packet's problem, not this wave's.

Expected joinable population after exclusions: roughly 750–800 clients. Measure it; don't assume.

**Acceptance:** join produces zero client_ids receiving data from two different invoice rows. Assert it, don't hope for it.

---

## Phase 2 — Schema migration

Additive only. No column drops in the same migration as data writes — if the backfill has to be reverted, a dropped column can't come back.

### 2A — Address (**unblocked** — Phase 0.1 PASS on `TAXPAYER.csv`)

`clients.address` today: single TEXT column, street-ish only. Add alongside it, leave the legacy column untouched:

```
address_street   TEXT
address_city     TEXT
address_state    TEXT
address_zip      TEXT
address_county   TEXT
address_source   TEXT   -- 'drake_taxpayer_csv' | 'intake' | NULL
address_verified_at TEXT
```

Keep `address` as-is and deprecated. Decide later whether legacy values are street lines that can be promoted; that is a separate reconciliation, not part of this wave.

Throwaway dry-run: `R1-phase2a-address-schema-dryrun.md` (**PASS**). Also adds `returns.filing_status_drake` + `client_profile_backfill_history` in the same v28 candidate bundle. **Not applied to live.**

### 2B — Dead columns (defer to a later migration)

Five columns are 100% empty on 1526 rows: `spouse_dob`, `spouse_cell`, `spouse_work_phone`, `spouse_email`, `taxpayer_work_phone`. This export can fill three of them (spouse DOB 345, spouse cell 201, spouse email 122). So **fill them, don't drop them** — the earlier "drop the dead columns" recommendation is superseded now that a source exists. `taxpayer_work_phone` still has no source and can go in a later cleanup.

### 2C — Snapshot table (hard prerequisite)

Wave 4's lesson. Written in the same transaction as every backfill write:

```
client_profile_backfill_history(
  id INTEGER PK,
  run_label TEXT,
  client_id INTEGER,
  invoice TEXT, bare_log TEXT, tax_year INTEGER,
  before_json TEXT,        -- full client row, pre-write
  fields_written TEXT,     -- JSON array of column names
  after_json TEXT,
  source_file TEXT, source_sha256 TEXT,
  written_at TEXT
)
```

`source_sha256` matters: it pins which export produced which value, so a bad export can be reverted selectively rather than wholesale.

**Acceptance:** a dry-run write on a throwaway copy produces a history row from which the pre-state is fully reconstructable. Same bar `client_merge_history` had to clear.

---

## Phase 3 — Backfill (first writes)

### Field map

| Export column | TaxOps target | Rows w/ invoice | Current TaxOps fill | Note |
|---|---|---:|---:|---|
| Email Address | `taxpayer_email` | 758 | **55 (3.6%)** | Highest-value field in the wave |
| Taxpayer Date of Birth | `taxpayer_dob` | 877 | 847 (55.5%) | |
| Street Address | `address_street` | 912 | — | + City/State/ZIP after 0.1 |
| County | `address_county` | 912 | — | |
| Spouse First / Last Name | `spouse_first_name` / `spouse_last_name` | 345 / 262 | 375 / 405 | **Separate columns** — no joint-name parsing |
| Spouse Date of Birth | `spouse_dob` | 345 | **0** | |
| Spouse Cell Phone | `spouse_cell` | 201 | **0** | |
| Spouse Email Address | `spouse_email` | 122 | **0** | |
| Filing Status | *(new)* `returns.filing_status_drake` | 877 | `returns.filing_status` 0.3% | Codes 1=Single 2=MFJ 3=MFS 4=HOH 5=QW |

Spouse first/last arriving as separate columns is the quiet win here — it removes the `LAST, FIRST & SPOUSE` parsing that produced `N AGUILAR`, `MARGARI, PINEDA`, `E MORALE, CLAUDIA`.

### Write rules

1. COALESCE only — write where TaxOps is NULL or empty string.
2. **Skip any row whose invoice does not match `^25\d{4}$` exactly** — no normalization, no repair, no lenient `bare_log_number`. Route non-canonical invoices to the office worklist (`R1-malformed-invoice-worklist.md`) with proposed reading per class; require confirmation. (`25141` is the hazard case: never parse.)
3. Skip any client whose bare log is in the collision set (strict unique bare only).
4. Skip any client with more than one candidate invoice row.
5. Every write logged to `client_profile_backfill_history`, same transaction.
6. `run_label` on every row so one run can be reverted independently.

### Disagreement report (no write)

Where TaxOps holds a value **and** it differs from Drake, emit a finding rather than overwriting. Split by cause, because the remedies differ:

- **`PROFILE_TRUNCATION`** — Drake is a superstring or the TaxOps value is clipped (`ALONSO GONZALE` vs `LIDIA ALONSO GONZALEZ`). Auto-resolvable; the Wave 5 applied run already handled four of these correctly.
- **`PROFILE_CONTAMINATION`** — TaxOps holds a *different real person*, one who appears elsewhere in the Log or client table. Confirmed live: MARTINEZ ARNULFO ← TAREEN FOUZIA, MUNGUIA FEDERICO ← RUIZ DE PEREZ HONORINA, HUERTA DANNY ← HUERTA EVELYN (who is Jesus Huerta's spouse, Log 855). Escalate; do not auto-anything.
- **`PROFILE_STALE`** — plausible older value (moved address, changed email). Staff judgement.

**Run the disagreement report before the writes**, on the full population including collision bares. It costs nothing, needs no schema, and its output sizes the contamination problem. Filed: `R1-disagreement-report.md` — **6** distinct contamination clients (13 findings); prior 3/22 sample was too thin. See `R1-status.md`.

---

## Phase 4 — Filing status cross-check (read-only, high leverage)

`Filing Status` independently validates spouse data. Two assertions worth running standalone:

1. Client has a spouse row **and** Drake says `1` (Single) → the spouse row is wrong. Directly relevant to the 354 `wave4_clients_fold` rows.
2. Drake says `2` (MFJ) **and** TaxOps has no spouse → a genuine `spouse_unrecovered`, now with independent confirmation rather than inference.

**Scope the counts.** Measured on the joinable set only (~45% of live clients). Do not read 15 / 36 as firm-wide: if the rate holds, expect roughly double (~33 Single+spouse, ~80 MFJ+no-spouse).

This also closes W5 items without a preparer: `RAMIREZ HUERTA, ISMAEL` is two people (inv 250603 MFJ b.1989 w/ NUBIA MEDRANO; inv 251001 Single b.1987), and `MOLINA, JOSE` Jr (b. 08/25/1983, spouse MELISSA) has **no invoice at all** and needs one assigned.

---

## Sequencing

| Phase | Writes? | Gate to next |
|---|---|---|
| 0 — re-export | no | city/state/zip present; ragged = 0; invoice fill re-measured |
| 1 — key quality | no | zero double-writes proven on the join |
| 2 — migration | schema only | snapshot dry-run reconstructable |
| 3 — backfill | **yes** | every write has a history row; disagreement report filed |
| 4 — FS cross-check | no | assertions run; W5 residue re-scoped |

Phase 4 has no dependency on 2 or 3 and is pure measurement — worth running early if you want value before the re-export lands.

## Out of scope

- Repairing the three contamination cases (deliberate correction, own wave)
- Promoting legacy `clients.address` into `address_street`
- Dropping `taxpayer_work_phone`
- The 207 invoice-less rows and 57 collision bares — office packet, not backfill
