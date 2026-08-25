# R1 status — client profile backfill

_Updated 2026-08-18 · spouse must-exist-in-Drake name corroboration applied_

## Roadmap

| Stage | What | Status |
|---|---|---|
| 0–D | Export → join → schema → live COALESCE | **DONE** |
| **E1** | Contam dossiers (6) | **DONE** — 4 REPLACE / 2 REVIEW |
| **E2** | Malformed invoice worklist | **DONE** — 23 distinct |
| **E3** | Spouse email/name/cell COALESCE (status export) | **DONE** — 106 clients |
| **E4** | Contam REPLACE_SPOUSE | **DONE** — 4 clients |
| Hold | Contam `1670` / `327` (no Drake spouse on export) | office |
| Hold | Malformed invoices (esp. `25141`) | office |
| **Accuracy** | Strict join + name gate vs TAXPAYER.csv | **DONE 2026-08-17** — see below |

---

## Profile accuracy audit (2026-08-17)

**Your example is confirmed.** Paola Resendiz (client `853`, TaxOps log `179`) received Qais Abu Taha's Drake invoice `250179` data via R1 COALESCE:

| | TaxOps (Paola) after R1 | Drake inv 250179 |
|---|---|---|
| Name | RESENDIZ SANCHEZ, PAOLA | ABU TAHA, QAIS AHMAD YOUNIS |
| Email | `QAIS.TAH10@GMAIL.COM` | same |
| Street | 16165 VETERANS WAY, Tustin | same |

Qais himself (client `7`, log `181`) got Carlos Velasquez's Drake `250181` email/address. Root cause: **TaxOps log numbers ≠ Drake invoice bares for these clients** (log reuse / remapping). R1 joined on bare log only and filled empty fields from the wrong person.

### Counts (711 unique strict joins)

| Class | Count |
|---|---:|
| OK | 601 |
| FIELD_MISMATCH (same person) | 22 |
| NAME_AMBIGUOUS | 25 |
| WRONG_PERSON | 63 |
| → of which **TRUE_SWAP** (different people, R1-poisoned) | **56** |
| → spelling variants (Abigail/Abagail etc.) | 7 |
| No TaxOps bare for Drake invoice | 94 |
| Email assigned to a different Drake person | 101 |

### Artifacts

- `T:\audit\investigation\R1-profile-accuracy-audit.md`
- `T:\audit\investigation\R1-profile-accuracy-audit.json`
- `T:\audit\investigation\R1-profile-wrong-person-TRUE.csv` ← remediation list
- `T:\audit\investigation\R1-profile-wrong-person-VARIANT.csv`

### Recommended next step (needs approval)

Rollback R1-written fields for the **56 TRUE_SWAP** clients using `client_profile_backfill_history.before_json` (restore pre-R1 empties / prior values). Do **not** re-COALESCE on bare log without a name gate.

### Rollback applied 2026-08-17

- run_label: `r1-wrong-person-rollback-2026-08-17`
- **56** clients · **465** client fields restored · **51** `filing_status_drake` cleared
- Pre-backup: `T:\audit\tmp\taxops_pre_wrong_person_rollback_20260817T210454Z.sqlite`
- Summary: `T:\audit\investigation\R1-wrong-person-rollback-summary.json`
- Paola `853`: email/street/source cleared (kept her pre-R1 Randolph address + cell/DOB)
- Qais `7`: Velasquez email/spouse/address cleared
- integrity_check: ok

**Still open:** any future Drake backfill must name-gate the join (reject when Drake taxpayer name ≠ TaxOps client name). 22 FIELD_MISMATCH + 25 NAME_AMBIGUOUS remain for office review (not auto-rolled).

## Name gate (2026-08-17) — DONE

Shared helper: `audit/profile_join.py`

- `name_affinity()` → OK / VARIANT / AMBIGUOUS / WRONG
- `allows_profile_coalesce()` → True only for OK + VARIANT
- `resolve_strict_joins(..., require_name_gate=True)` — **default on**

Wired into:
- `_r1_stage_d_live_coalesce.py`
- `_r1_stage_e.py` (`spouse_email_plan`)
- `_r1_phase3_throwaway_dryrun.py`

Live dry-run vs current DB (`R1-name-gate-dryrun.json`):

| | Count |
|---|---:|
| Ungated joinable (old bare-only) | 711 |
| **Gated joinable** | **655** |
| Blocked by name gate | **56** (exactly the TRUE_SWAP set) |
| Paola 853 / Qais 7 | blocked ✓ |

Tests: `python -m unittest audit.tests.test_profile_join -v` (7 OK)

## Filing status / MFJ autofill (2026-08-17)

**Qais example:** Drake inv `250179` = FS **1 (Single)**, no spouse. Intake was
auto-setting **MFJ** because:
1. `spouses` row `MARIA E VELASQUEZ` (`needs_review=0`, source `wave4_clients_fold`)
   from wrong log 181 fold — reintake returned as `drake_spouse`
2. `intake.html` auto-MFJs when spouse data exists and `filing_status` blank
3. `filing_status_drake` was NULL after wrong-person rollback

**Audit** (`R1-filing-status-audit.md`): 655 name-gated joins · 35 clients with
FS/spouse autofill issues · 14 Drake **Single** + spouse row → MFJ risk

**Fixes applied:**
- DB: quarantined Qais spouse row (`needs_review=1`); set `filing_status_drake='1'`
  on TY2025 return; backfilled Drake FS on other empty name-gated returns
- `app.py` reintake: map Drake FS → intake; **suppress `drake_spouse` and spouse
  client fields** when effective FS is not MFJ/MFS (Single, HH, Qual Non Dep)
- Backup: `T:\audit\tmp\taxops_pre_filing_status_fix_20260817T220356Z.sqlite`

**Verified Qais reintake:** `filing_status=SINGLE`, `drake_spouse=null`,
`has_spouse_row=false`

**Restart TaxOpsService** so reintake API changes load.

## Stage D (earlier)

707 clients · 5266 fields · run_label `r1-phase3-live-2026-08-13`

## Stage E3

- run_label: `r1-stage-e-spouse-email-2026-08-13`
- **106** history rows (spouse_email / cell / first+last when both empty / dob)
- Pre-backup from apply path (see `T:\audit\tmp\taxops_pre_stage_e3_*`)

## Stage E4

| Client | Was | Now (Drake) |
|---|---|---|
| `1583` | VERONICA OLEA | MERCEDES GAMBOA GARCIA |
| `902` | M GUADALUPE MELENDREZ | KARLA ROSAS |
| `237` | MARIA A | MARIA FLORES |
| `878` | CARIDAD RODAS | MARIO ROMERO SERRANO |

- run_label: `r1-stage-e-contam-repair-2026-08-13`
- Pre-backup: `T:\audit\tmp\taxops_pre_stage_e4_20260813T231451Z.sqlite`

## Intake autofill + profile display (fix)

**Root cause (paths):** TaxOpsService on the server opens **local** `C:\TaxOps\taxops\taxops.db` (WAL-safe). From the workstation that path is a *different* empty stub; the real DB is the share file `T:\taxops\taxops.db` (= server local when the service runs). R1 writes targeted `T:\taxops\taxops.db`.

**Why profiles looked empty:** UI only rendered legacy `clients.address`. Stage D filled `address_street/city/state/zip` and left legacy alone → **94** blank, many others street-only.

**Fixes just applied on live share DB:**
1. Promoted composed address → legacy `address` for **657** clients (`r1-promote-address-legacy-2026-08-13`)
2. Re-applied spouse COALESCE **106** (`r1-stage-e-spouse-email-2026-08-13` was missing from history)
3. Profile view composes structured address for display
4. Intake `selectClient` now surfaces reintake HTTP/JSON errors instead of failing silently

**Restart TaxOpsService** (or hard-refresh after restart) so `app.py` / `intake.html` changes load. Then re-test client pick on intake.

## Intake autofill HTTP 500 — real root cause (2026-08-17)

From `T:\logs\taxops_stderr.log` (live service, `C:\TaxOps\taxops\app.py` = this share):

```
ERROR app Exception on /api/clients/2428/reintake [GET]
  app.py:5165 in _enrich_reintake_from_prefill
    from drake_prefill_importer import _spouse_from_csm_name
ModuleNotFoundError: No module named 'openpyxl'
  → then app.py:5267  NameError: name '_log' is not defined
```

- **Primary:** `drake_prefill_importer` imported `openpyxl` at module scope; the
  service host has no `openpyxl`, so every reintake that needed the CSM spouse
  split raised. Blast radius: **870 / 1521 clients (57%)** — CSM prefill link
  present and no spouse name on file.
- **Secondary:** the new reintake `except` handler logged via `_log`, which is a
  *function-local* name in `app.py`, so the handler itself raised `NameError` and
  Flask returned its HTML 500 page — hence "server returned a web page instead
  of data".

**Fixes**

1. `drake_prefill_importer.py` — `openpyxl` import moved inside `load_csm_xlsx()`;
   module now imports on hosts without it (`_spouse_from_csm_name` still works).
2. `app.py` reintake handler — logs via `logging.getLogger("taxops")`.
3. `app.py` `_enrich_reintake_from_prefill` — the request-time importer import is
   wrapped in `try/except ImportError`; a missing optional dep degrades the CSM
   spouse split instead of failing the lookup.

**Verification**

- `_r1_sweep_reintake_all.py`: 1521 clients × privacy off/on = **3042 requests,
  0 failures**.
- `_r1_verify_importer_no_openpyxl.py`: importer loads with `openpyxl` blocked;
  `_spouse_from_csm_name('SMITH, JOHN & JANE')` → `('JANE','SMITH')`.

**Still required:** restart TaxOpsService (uptime was 3.7 d, so the running
process predates these edits). Remote `sc.exe \\Xcel-server` is Access Denied
from the workstation — run `scripts\restart_service.ps1` on the server.
Optional: `pip install openpyxl` on the server so Drake CSM `.xlsx` imports can
run there too (not needed for intake).

## Safe fix pass (2026-08-17) — DONE

After full profile verify: name-gated COALESCE for TaxOps-empty gaps + promote
composed address → legacy when structured matches Drake but legacy was stale.
**No overwrite** of DOB/cell/spouse/name mismatches (office review).

- run_label: `r1-safe-fix-pass-2026-08-17`
- **40** clients · **105** fields · **20** address promotions
- Backup: `T:\audit\tmp\taxops_pre_safe_fix_pass_20260817T225345Z.sqlite`
- Summary: `T:\audit\investigation\R1-safe-fix-pass-summary.json`
- integrity_check: ok

### Post-fix verify (655 name-gated joins)

| Field group | Result |
|---|---|
| All address_* + address_display | **655/655 OK** |
| taxpayer_email / phone / dob gaps | filled (empty→Drake) |
| Remaining **true mismatches** (not auto-fixed) | **79** clients |
| Top leftover: spouse_last_name / taxpayer_dob / taxpayer_cell | office review |
| DRAKE_EMPTY (TaxOps has value, Drake blank) | left as-is |

Artifacts: `R1-client-profile-full-verify.md` / `.csv` / `.json`

## Drake spouse source-of-truth (2026-08-18) — DONE

Policy: on **name-gated** joins, Drake spouse fields win (REPLACE, not COALESCE).

- run_label: `r1-drake-spouse-sot-2026-08-18`
- **78** clients touched · **70** client fields · **44** `spouses` table upserts · **17** spouse rows quarantined (Drake Single/HH with no spouse)
- **11** profiles cleared of TaxOps-only spouse data when Drake FS is Single/HH
- Backup: `T:\audit\tmp\taxops_pre_drake_spouse_sot_20260818T175404Z.sqlite`
- Summary: `T:\audit\investigation\R1-drake-spouse-sot-summary.json`
- integrity_check: ok

### Post-verify spouse fields (655 name-gated)

| Field | Result |
|---|---|
| `spouse_first_name` / `spouse_last_name` / `spouse_dob` | **655/655 OK** |
| `spouse_cell` | 584 OK · 63 TaxOps-only phone (Drake blank — left) · 8 Drake phone with no spouse name (not applied) |

Internal: spouse-table drift **18→7**, spouse-row-only **38→23**.

## Spouse must exist in Drake (2026-08-18) — DONE

Invoice-only audit (`_r1_spouse_must_exist_in_drake.py`): after Drake SoT, **0 clearable**
on name-gated joins. Leftovers are `NO_DRAKE_JOIN` / `UNGATED_BARE_JOIN` (remapped logs).

Name corroboration (`_r1_spouse_corroborate_by_name.py`): match TaxOps taxpayer → Drake by
`name_affinity` OK/VARIANT (not log). Unique Drake person with no spouse → CLEAR; different
spouse → REPLACE; agree → OK. Collision guard: if >1 TaxOps client would CLEAR/REPLACE from
the same Drake invoice, demote to REVIEW (`*_COLLIDED_INVOICE`).

- run_label: `r1-spouse-corroborate-by-name-2026-08-18`
- **16** clients · **25** fields · **7** spouse upserts · **9** quarantined
- Backup: `T:\audit\tmp\taxops_pre_spouse_corroborate_20260818T180117Z.sqlite`
- Artifacts: `R1-spouse-corroborate-by-name.md` / `.json`
- integrity_check: ok

### Post-apply (re-audit)

| Class | Count |
|---|---:|
| OK (TaxOps spouse corroborated in Drake) | **302** |
| CLEAR / REPLACE remaining auto | **0** |
| REVIEW (office) | **134** |

REVIEW breakdown: 123 no 2025 Drake name match · 5 ambiguous · 6 collided invoice.
Do **not** auto-clear those — often no 2025 return, joint-name clients, or entities.

Notable REPLACE applied (Drake SoT; office may want to spot-check remarriages):
Huerta `483` Alicia→Miriam; Morales `1427` Carmen→Amanda; Flores `1656` Beatrice→Angelica;
Ramirez `2267` Martha→Irma; Melendrez `2316` Nancy→Karla.

## Business entities must not have spouses (2026-08-18) — DONE

Drake 2025 strict invoices: **0 of 26** business entities (LLC/INC/CORP/ESTATE/…) list a
Spouse Name. TaxOps must match that.

Audit script: `_r1_business_spouse_audit.py` → `R1-business-spouse-audit.md`

- run_label: `r1-business-spouse-clear-2026-08-18`
- Quarantined spouse row on **LELI CONSTRUCTION INC** (`539`, Maria Nuno) — no Drake
  corroboration for a business spouse
- **L A AUTO SERVICE LLC** (`518`) already cleared/quarantined from earlier name corroboration
- All other Drake-matched business clients: spouse cols empty
- Backup: `T:\audit\tmp\taxops_pre_business_spouse_clear_20260818T194035Z.sqlite`
- Post-audit: **0** business clients with active TaxOps spouse
- integrity_check: ok

## Drake dependents source-of-truth (2026-08-18) — DONE

Policy: on **name-gated** joins, Drake `Dependent First/Last Name` from TAXPAYER.csv
wins for `client_dependents`. Matching TaxOps rows kept (DOB/relationship preserved).
Extras soft-removed (`removed_for_ty2026=1`). Missing Drake names inserted (name only).
Self-named Drake junk rows (dependent first = taxpayer first, blank last) are **not** added.

- run_label: `r1-drake-dependents-sot-2026-08-18`
- **317** clients SYNC · **304** removed · **325** added · **338** already OK · **152** REVIEW (no name-gated join)
- Business clears included: Corona Bros LLC, D&L Fiber LLC, Estate of Don Segesdy, Myco Imports, MYM Organics
- Backup: `T:\audit\tmp\taxops_pre_drake_dependents_sot_20260818T200802Z.sqlite`
- Artifacts: `R1-drake-dependents-sot.md` / `.json` / `-summary.json`
- Does **not** touch return-level `dependents` (intake-entered, 2 rows)
- integrity_check: ok

REVIEW (152): TaxOps still has active deps but no name-gated 2025 Drake join — office.

## Dependent blank last → taxpayer last (2026-08-18) — DONE

Drake convention (same as spouse UI): blank dependent last name means the taxpayer’s
last name. Matcher updated; live DB backfilled.

- run_label: `r1-dep-blank-last-backfill-2026-08-18`
- **125** active `client_dependents.last_name` filled from `clients.last_name`
- Business leftover deps cleared via `r1-business-deps-clear-2026-08-18`
  (**39** soft-removed on LLC/INC/ENTERPRISES clients — including the 13 blank-last skips)
- Backups: `taxops_pre_dep_blank_last_20260818T201615Z.sqlite`,
  `taxops_pre_business_deps_clear_20260818T201728Z.sqlite`
- Intake API (`app.py`): if last still blank when building `full_name`, falls back to
  taxpayer last (code path for next restart; **live DB already has lasts filled**)
- Post: **0** blank-last active deps; SoT still **OK 655 / SYNC 0**

## Live DB ↔ reintake wire (2026-08-18) — VERIFIED

Script: `_r1_verify_reintake_live_wire.py` → `R1-reintake-live-wire.json`

| Check | Result |
|---|---|
| `T:\taxops\taxops.db` | `\\Xcel-server\taxops\taxops\taxops.db` (12.4 MB) |
| Same as `\\Xcel-server\TaxOps\taxops\taxops.db` | **yes** (identical resolved path) |
| Workstation `C:\TaxOps\taxops\taxops.db` | **4 KB stub — not live** (do not open this) |
| Service `/health` schema | **28** (matches share `app_settings`) |
| Reintake vs share DB | Qais 7 SINGLE/no spouse · Solis 883 deps with lasts · LA Auto 518 clean · Leli 539 no spouse |

Reintake reads `client_dependents` / `spouses` / `clients` via `get_connection()` →
`TAXOPS_DB` (server: `C:\TaxOps\taxops\taxops.db` = the share file above).
**SQLite data changes are live without restart.** `app.py` health now also reports
`db.path` + `db.size_bytes` (visible after next TaxOpsService restart).

## Moises & Sandra Yanez Bustos deps (2026-08-18) — DONE

Client **124** `BUSTOS, MOISES & SANDRA YANEZ` had Isaiah + Matthew Bustos from the old
Drake import. **Neither Moises nor Sandra appears in the 2025 TAXPAYER.csv export**
(no invoice / no return), so those deps were not Drake-corroborated.

- run_label: `r1-bustos-yanez-deps-clear-2026-08-18`
- Soft-removed **2** deps (Isaiah, Matthew); reintake now returns **no dependents**
- Backup: `T:\audit\tmp\taxops_pre_bustos_yanez_deps_20260818T213219Z.sqlite`
- Related open: Isabel Bustos (`122`) still has wrong Tareen kids (Drake lists
  Evangelina Sanchez; blank invoice) — not changed in this pass

## All dependent links vs Drake (2026-08-18) — DONE

Full audit of every client with active `client_dependents` against TAXPAYER.csv
(including blank-invoice name matches). Soft-removed uncorroborated / wrong deps;
on CLEAR_EXTRAS also inserted Drake’s correct names.

- Script/report: `_r1_all_dependents_link_audit.py` → `R1-all-dependents-link-audit.md`
- run_label: `r1-all-deps-clear-no-drake-2026-08-18`
- Pre-apply: **408** clients · OK 302 · CLEAR 74 · CLEAR_EXTRAS 28 · REVIEW 4
- Applied: **102** clients · **182** removed · **43** added
- Backup: `T:\audit\tmp\taxops_pre_all_deps_clear_20260818T213844Z.sqlite`
- Examples fixed: Isabel Bustos Tareen→Evangelina Sanchez; Tareen kids restored to
  Zulqarnian Tareen; Moises/Sandra already cleared earlier
- REVIEW left (4): ambiguous Drake names / Drake has extra only — office
- integrity_check: ok

## All reintake fields vs Drake (2026-08-18) — DONE

Catalog + name-gated audit of every intake prefill field (`CLIENT_TEXT`, structured
address, `filing_status_drake`, dependents, plus RETURN fields with no Drake source).

- Script/report: `_r1_reintake_fields_verify.py` → `R1-reintake-fields-verify.md`
- Name-gated joins: **655**
- Safe apply run_label: `r1-reintake-fields-safe-2026-08-18`
  - **41** clients · **41** fields (mostly `spouse_email` from status export + a few `spouse_cell`)
- Backup: `T:\audit\tmp\taxops_pre_reintake_fields_safe_20260818T214616Z.sqlite`
- Fields with **no Drake column** (left as-is): occupation, work phones, bank_*,
  processor, form checkboxes, prior_year_log, habit_profile

### Drake REPLACE (source of truth) — applied

Policy: when Drake has a value on a name-gated join, **overwrite** TaxOps.
When Drake is blank, keep TaxOps-only values.

- run_label: `r1-reintake-fields-drake-sot-2026-08-18`
- **33** clients · **54** fields
  - `taxpayer_dob` ×21 · `taxpayer_cell` ×21 · `first_name` ×8 · `last_name` ×4
- Backup: `T:\audit\tmp\taxops_pre_reintake_drake_sot_20260818T215306Z.sqlite`
- Report: `R1-reintake-fields-drake-sot.md`
- Post-verify: DOB / cell / name **mismatches = 0**
- Remaining “issues”: Drake-empty contact (kept by policy) + dependents name-format residuals
- integrity_check: ok

## Claude-agent handoff sitemap (2026-08-18)

Full TaxOps functional audit sitemap (routes, modules, R1 status, verify checklist):

**`T:\audit\investigation\TAXOPS-FUNCTIONAL-AUDIT-SITEMAP.md`**

Regenerate: `python T:\audit\investigation\_gen_functional_sitemap.py`
