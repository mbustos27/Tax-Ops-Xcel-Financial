# Client Linkage Audit — Full Report (A0–A5)

_Compiled: 2026-08-12T22:51:14Z_

Findings-only. No TaxOps repairs, merges, or schema changes. All durable outputs under `T:\audit\` / `T:\audit\investigation\`.

---

## Executive summary

_Numbers below are parsed from the embedded A1/A4 sections (current run), not from A3 F12 baseline columns._

| Item | Result |
|---|---|
| Cross-system key | Drake Invoice Number ≡ Tax Log # (bare form); L0 three-way **571**; L0-eligible **815** |
| Identity ladder | L0 Drake↔TaxOps **721**; L1 **387**; L5 leftover **35** |
| Exact duplicate client groups | **8** (matches live baseline); merge queue is **6–7** per R0 |
| Jul1 provenance cohort | **191** still dated; **179** at stamp `18:19:30` |
| Aug7 prefill burst | **244** (all SSN + prefill-linked; **8** Group-C name collisions, not merges) |
| Invariants | **5 PASS / 1 MODIFIED / 1 FAIL** (I6=`MODIFIED`; I7=`FAIL`) |
| Disposition open | **1733** |
| Operator worklist | Priority / Phantoms / Workflow → `A5-worklist.xlsx` |
| Remediation plan | `R0-remediation-plan.md` (plan only; no TaxOps writes until Wave 2A) |

### Work this first

Per **R0** current gate (software waves 0–5 done; I7 still FAIL):

1. **Lucy / Drake** — execute `W-office-packet.md` (bare `141` first → MOVE → KEEP/CLAIM/MINT → REVIEW).
2. Re-export Drake invoices after packet; re-run A1/A3/A4 until I7 PASS and collisions=0.
3. Hold Tax Log `genuine_reuse` split until post-packet remeasure.
4. Then phantoms / workflow lanes (F8/F9) — counts already use canonical YR=25 Log keys.

### Loud corrections vs I0–I4 investigation

1. I4 “no shared Drake↔TaxOps key” is **SUPERSEDED** — Invoice Number holds the Tax Log number.
2. July 1 “~368 duplicates from bulk import” — **count real on Jul31 snap; mechanism is `created_at` rewrite**, not insert.
3. L0 string equality fails until season prefix stripped: `250141` ↔ `141`.

---

## 1. Baseline lock (A0)

# A0 — Baseline lock

_Locked at: 2026-08-11T22:01:52Z_

## Authoritative CSM (explicit decision)

- **Key:** `onedrive_ty2025`
- **Path:** `C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC\CLIENTS.xlsx`
- **Reason:** Authoritative CSM = OneDrive CLIENTS.xlsx. Investigation task1_export_direction.json: later by Drake Last Change than Desktop; historically 1159 rows. Present on disk at firm OneDrive root.
- **Rows:** 1159
- **SHA-256:** `46a43a588afc3ad8db72932d0fe19558a13afaec11a6e5845e158898b78068c9`
- **mtime (UTC):** 2026-07-31T17:07:51Z
- **Max Last Change (cell):** 07/30/2026 16:37:04
- **Types:** `{"1040": 1044, "1041": 2, "1065": 29, "1120": 55, "1120S": 28, "990": 1}`

### Candidates surveyed (no silent fallback)

| Key | Exists | Rows | SHA-256 (prefix) | mtime UTC |
|---|---|---:|---|---|
| `onedrive_ty2025` | True | 1159 | `46a43a588afc3ad8…` | 2026-07-31T17:07:51Z |
| `desktop_ty2025` | True | 1155 | `8035b0d5cac9cf1f…` | 2026-06-23T19:07:05Z |
| `csvfiles_ty2024` | True | 2514 | `0c5f526d01dce3d5…` | 2026-08-07T17:59:43Z |

**TY2024 `CSVFILES/2024 CLIENTS.xlsx` is not eligible** as the TY2025 three-way baseline.

## Tax Log

- **Path:** `C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC\Shared\Logs\TAX LOG 2025 Live.xlsx`
- **Named rows (XCEL 2025):** 1252
- **SHA-256:** `e8f6f98590ec7af09dc873d8c0c46e06ad8e7d656a9dadb223d334184ac87d73`
- **mtime (UTC):** 2026-07-07T19:07:06Z

## TaxOps DB path resolution

- **Authoritative path for audit reads:** `\\Xcel-server\taxops\taxops\taxops.db`
- **Resolution code:** `AUTHORITATIVE_DB_UNRESOLVED`
- T:\taxops\taxops.db samefile UNC: True (size=10326016, mtime_utc=2026-08-11T22:00:19Z)
- Share mapping confirmed: workstation T: and \\Xcel-server\taxops\taxops\taxops.db are the same inode.
- C:\TaxOps\taxops\taxops.db does not exist on this workstation (expected: NSSM DB_PATH on the app server only).
- Cannot prove from this workstation whether server-local C:\TaxOps\... is the same file as the share — emitting AUTHORITATIVE_DB_UNRESOLVED for that pairing.
- **SHA-256:** `e8fd5e70d707afc81f21ed64faceec972fdc9591b8da05aaa7fb635e4031bf04`
- **mtime (UTC):** 2026-08-11T22:00:19Z
- **page_count:** 2521
- **clients/returns:** 1527 / 1615

## CSM ↔ prefill export lag

Prefill importer consumed T:\taxops\CSVFILES\2024 CLIENTS.xlsx (mtime 2026-08-07T17:59:43Z, TY2024 universe). Audit authoritative CSM is onedrive_ty2025 at C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC\CLIENTS.xlsx (mtime 2026-07-31T17:07:51Z, TY2025). Three-way census vs prefill-linked TaxOps rows will mix export-lag with true data error — treat CSM↔prefill disagreements as EXPORT_LAG until seasons align. Amendment 1: prefer TAXPAYER.csv (separate first/last, Invoice Number) over CSM joint names for linkage identity when both exist.

## Amendment 1 — Drake TAXPAYER.csv (Invoice Number)

- **Path:** `T:\audit\investigation\TAXPAYER.csv` (canonical: `T:\audit\investigation\TAXPAYER.csv`)
- **Exists:** True
- **SHA-256:** `13c9dadcf5b6c071c55f95a2539b14a84af64adc129eceed7fd327d1fb741d7d`
- **mtime (UTC):** 2026-08-11T21:52:34Z
- **Title lines:** `['INVOICE NUMBER LINK', 'As of 08-11-2026']`
- **Tax year / season prefix:** 2025 / `25`
- **Layout:** `link_5` (expected 5 columns)
- **Data rows:** 1063
- **Full-width (5 fields):** 1062 (99.91% of data rows)
- **Explicit blank Invoice (full-width):** 196 — not ragged; no key on that row
- **Ragged (short) rows:** 1 — Invoice is col 3; short lines still lose trailing fields, not the invoice key
- **Field-count histogram:** `{"0": 1, "1": 1, "5": 1062}`
- **Name lengths:** first max=17 (at39=0, at40=0); last max=35 (at39=0, at40=0) — **no 40-char CSM ceiling in this export**

### Entity-specific invoice coverage

- Blank first name (entities): **111**
- Of those, ragged (key lost): **1**
- Entities with readable invoice (full-width): **35**

### Invoice format (Amendment 2 C1/C2 — bare-log eligibility)

- Length histogram (full-width only): `{"2": 1, "3": 3, "5": 12, "6": 844, "7": 6}`
- Observed bare-log ceiling: **1316** `{"bare_log_max": 1316, "n_observed": 2272, "n_tax_log": 1251, "n_taxops": 1021, "tax_year": 2025}`
- **L0 before (legacy `^\d{6}$`):** 803
- **L0 after (bare-log range, not collision):** raw invoices=**815**, distinct bare=**815**
- Malformed (empty/out-of-range bare after normalize): **11** shown (excluded from L0)
  - raw=`25` bare=`` len=2 reason=empty_bare_after_normalize — GR INDUSTRIES INC
  - raw=`250` bare=`` len=3 reason=empty_bare_after_normalize — LA MESA AUTO SALES INC
  - raw=`250` bare=`` len=3 reason=empty_bare_after_normalize — GARCIA, GILBERT
  - raw=`210273` bare=`210273` len=6 reason=out_of_range — CHEVEZ, LEONEL
  - raw=`251355` bare=`1355` len=6 reason=out_of_range — HARRIS JR, EDDIE
  - raw=`50688` bare=`50688` len=5 reason=out_of_range — ARMENTA, LUISA
  - raw=`23229` bare=`23229` len=5 reason=out_of_range — AYALA, VIRGINIA
  - raw=`250` bare=`` len=3 reason=empty_bare_after_normalize — ARRUE, SILVIA
  - raw=`251685` bare=`1685` len=6 reason=out_of_range — MORALES, JOSE
  - raw=`210729` bare=`210729` len=6 reason=out_of_range — MARQUEZ, RICARDO
  - raw=`2507598` bare=`7598` len=7 reason=out_of_range — RAMIREZ, NATALIE
- Collisions (same **bare log** → >1 distinct taxpayer): **19**
  - bare=`1095` raws=`['251095']` n=3: ['GUTIERREZ, JOAO', 'GUTIERREZ, MERCEDES', 'RODRIGUEZ, JAVIER']
  - bare=`141` raws=`['250141', '25141']` n=3: ['GOYTIA, SANDRA', 'PEREZ, ANDRES', 'VILLAREAL, RICHARD']
  - bare=`247` raws=`['250247']` n=2: ['BRAVO, VICTORIA', 'PERFECT SMILE MANAGEMENT CORP']
  - bare=`787` raws=`['250787']` n=2: ['CREATE YOUR HEALTH LLC', 'VICENTE, JOSE']
  - bare=`203` raws=`['250203']` n=2: ['LAZO, BREANNA', 'NUNO, MARIBEL']
  - bare=`207` raws=`['250207']` n=2: ['COLLAZO, MAYRA', 'RAMOS, VALERIE']
  - bare=`1103` raws=`['251103']` n=2: ['QUINTANA, ROGELIO', 'SANCHEZ ARCE, EUNICE']
  - bare=`339` raws=`['250339']` n=2: ['AGUILAR, AMBER', 'LOPEZ, SAUL']
  - bare=`206` raws=`['250206']` n=2: ['BRICENO, JUAN', 'DELGADO, TIFFANY']
  - bare=`888` raws=`['250888']` n=2: ['DURAN, AGUSTIN', 'HUERTA, JESUS']
- Proforma stale list (superseded by bare-range gate): **0**

## Drift vs previous run

- **Status:** `STABLE`
- **Previous lock:** 2026-08-10T23:33:30Z

No count changes vs previous memory (or no previous memory).

## Findings emitted

Counts by type: `{"AUTHORITATIVE_DB_UNRESOLVED": 1, "LOG_NUMBER_COLLISION": 19, "MALFORMED_LOG_NUMBER": 11, "RAGGED_EXPORT_ROW": 1, "SUPERSEDED_EXPORT": 3}`

- **AUTHORITATIVE_DB_UNRESOLVED:** `{"authoritative_path_for_audit": "\\\\Xcel-server\\taxops\\taxops\\taxops.db", "notes": ["T:\\taxops\\taxops.db samefile UNC: True (size=10326016, mtime_utc=2026-08-11T22:00:19Z)", "Share mapping confirmed: workstation T: and \\\\Xcel-server\\taxops\\taxops\\taxops.db are the same inode.", "C:\\TaxOps\\taxops\\taxops.db does not exist on this workstation (expected: NSSM DB_PATH on the app server only).", "Cannot prove from this workstation whether server-local C:\\TaxOps\\... is the same file as the share \u2014 emitting AUTHORITATIVE_DB_UNRESOLVED for that pairing."]}`
- **MALFORMED_LOG_NUMBER:** `{"bare_log": "", "invoice": "25", "length": 2, "name": "GR INDUSTRIES INC", "reason": "empty_bare_after_normalize", "season_prefix": "25"}`
- **MALFORMED_LOG_NUMBER:** `{"bare_log": "", "invoice": "250", "length": 3, "name": "LA MESA AUTO SALES INC", "reason": "empty_bare_after_normalize", "season_prefix": "25"}`
- **MALFORMED_LOG_NUMBER:** `{"bare_log": "", "invoice": "250", "length": 3, "name": "GARCIA, GILBERT", "reason": "empty_bare_after_normalize", "season_prefix": "25"}`
- **MALFORMED_LOG_NUMBER:** `{"bare_log": "210273", "bare_log_max": 1316, "invoice": "210273", "length": 6, "name": "CHEVEZ, LEONEL", "note": "raw_does_not_start_with_season_prefix", "reason": "out_of_range", "season_prefix": "25"}`
- **MALFORMED_LOG_NUMBER:** `{"bare_log": "1355", "bare_log_max": 1316, "invoice": "251355", "length": 6, "name": "HARRIS JR, EDDIE", "reason": "out_of_range", "season_prefix": "25"}`
- **MALFORMED_LOG_NUMBER:** `{"bare_log": "50688", "bare_log_max": 1316, "invoice": "50688", "length": 5, "name": "ARMENTA, LUISA", "note": "raw_does_not_start_with_season_prefix", "reason": "out_of_range", "season_prefix": "25"}`
- **MALFORMED_LOG_NUMBER:** `{"bare_log": "23229", "bare_log_max": 1316, "invoice": "23229", "length": 5, "name": "AYALA, VIRGINIA", "note": "raw_does_not_start_with_season_prefix", "reason": "out_of_range", "season_prefix": "25"}`
- **MALFORMED_LOG_NUMBER:** `{"bare_log": "", "invoice": "250", "length": 3, "name": "ARRUE, SILVIA", "reason": "empty_bare_after_normalize", "season_prefix": "25"}`
- **MALFORMED_LOG_NUMBER:** `{"bare_log": "1685", "bare_log_max": 1316, "invoice": "251685", "length": 6, "name": "MORALES, JOSE", "reason": "out_of_range", "season_prefix": "25"}`
- **MALFORMED_LOG_NUMBER:** `{"bare_log": "210729", "bare_log_max": 1316, "invoice": "210729", "length": 6, "name": "MARQUEZ, RICARDO", "note": "raw_does_not_start_with_season_prefix", "reason": "out_of_range", "season_prefix": "25"}`
- **MALFORMED_LOG_NUMBER:** `{"bare_log": "7598", "bare_log_max": 1316, "invoice": "2507598", "length": 7, "name": "RAMIREZ, NATALIE", "reason": "out_of_range", "season_prefix": "25"}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "247", "claimant_names": ["BRAVO, VICTORIA", "PERFECT SMILE MANAGEMENT CORP"], "claimants": [{"name": "BRAVO, VICTORIA", "person_key": "BRAVO|VICTORIA", "raw_invoices": ["250247"], "row_count": 1}, {"name": "PERFECT SMILE MANAGEMENT CORP", "person_key": "PERFECT SMILE MANAGEMENT CORP|", "raw_invoices": ["250247"], "row_count": 1}], "invoice": "250247", "n_taxpayers": 2, "raw_invoices": ["250247"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "787", "claimant_names": ["CREATE YOUR HEALTH LLC", "VICENTE, JOSE"], "claimants": [{"name": "CREATE YOUR HEALTH LLC", "person_key": "CREATE YOUR HEALTH LLC|", "raw_invoices": ["250787"], "row_count": 1}, {"name": "VICENTE, JOSE", "person_key": "VICENTE|JOSE", "raw_invoices": ["250787"], "row_count": 1}], "invoice": "250787", "n_taxpayers": 2, "raw_invoices": ["250787"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "1095", "claimant_names": ["GUTIERREZ, JOAO", "GUTIERREZ, MERCEDES", "RODRIGUEZ, JAVIER"], "claimants": [{"name": "GUTIERREZ, JOAO", "person_key": "GUTIERREZ|JOAO", "raw_invoices": ["251095"], "row_count": 1}, {"name": "GUTIERREZ, MERCEDES", "person_key": "GUTIERREZ|MERCEDES", "raw_invoices": ["251095"], "row_count": 1}, {"name": "RODRIGUEZ, JAVIER", "person_key": "RODRIGUEZ|JAVIER", "raw_invoices": ["251095"], "row_count": 1}], "invoice": "251095", "n_taxpayers": 3, "raw_invoices": ["251095"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "203", "claimant_names": ["LAZO, BREANNA", "NUNO, MARIBEL"], "claimants": [{"name": "LAZO, BREANNA", "person_key": "LAZO|BREANNA", "raw_invoices": ["250203"], "row_count": 1}, {"name": "NUNO, MARIBEL", "person_key": "NUNO|MARIBEL", "raw_invoices": ["250203"], "row_count": 1}], "invoice": "250203", "n_taxpayers": 2, "raw_invoices": ["250203"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "207", "claimant_names": ["COLLAZO, MAYRA", "RAMOS, VALERIE"], "claimants": [{"name": "COLLAZO, MAYRA", "person_key": "COLLAZO|MAYRA", "raw_invoices": ["250207"], "row_count": 1}, {"name": "RAMOS, VALERIE", "person_key": "RAMOS|VALERIE", "raw_invoices": ["250207"], "row_count": 1}], "invoice": "250207", "n_taxpayers": 2, "raw_invoices": ["250207"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "1103", "claimant_names": ["QUINTANA, ROGELIO", "SANCHEZ ARCE, EUNICE"], "claimants": [{"name": "QUINTANA, ROGELIO", "person_key": "QUINTANA|ROGELIO", "raw_invoices": ["251103"], "row_count": 1}, {"name": "SANCHEZ ARCE, EUNICE", "person_key": "SANCHEZ ARCE|EUNICE", "raw_invoices": ["251103"], "row_count": 1}], "invoice": "251103", "n_taxpayers": 2, "raw_invoices": ["251103"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "339", "claimant_names": ["AGUILAR, AMBER", "LOPEZ, SAUL"], "claimants": [{"name": "AGUILAR, AMBER", "person_key": "AGUILAR|AMBER", "raw_invoices": ["250339"], "row_count": 1}, {"name": "LOPEZ, SAUL", "person_key": "LOPEZ|SAUL", "raw_invoices": ["250339"], "row_count": 1}], "invoice": "250339", "n_taxpayers": 2, "raw_invoices": ["250339"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "206", "claimant_names": ["BRICENO, JUAN", "DELGADO, TIFFANY"], "claimants": [{"name": "BRICENO, JUAN", "person_key": "BRICENO|JUAN", "raw_invoices": ["250206"], "row_count": 1}, {"name": "DELGADO, TIFFANY", "person_key": "DELGADO|TIFFANY", "raw_invoices": ["250206"], "row_count": 1}], "invoice": "250206", "n_taxpayers": 2, "raw_invoices": ["250206"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "888", "claimant_names": ["DURAN, AGUSTIN", "HUERTA, JESUS"], "claimants": [{"name": "DURAN, AGUSTIN", "person_key": "DURAN|AGUSTIN", "raw_invoices": ["250888"], "row_count": 1}, {"name": "HUERTA, JESUS", "person_key": "HUERTA|JESUS", "raw_invoices": ["250888"], "row_count": 1}], "invoice": "250888", "n_taxpayers": 2, "raw_invoices": ["250888"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "141", "claimant_names": ["GOYTIA, SANDRA", "PEREZ, ANDRES", "VILLAREAL, RICHARD"], "claimants": [{"name": "GOYTIA, SANDRA", "person_key": "GOYTIA|SANDRA", "raw_invoices": ["250141"], "row_count": 1}, {"name": "PEREZ, ANDRES", "person_key": "PEREZ|ANDRES", "raw_invoices": ["25141"], "row_count": 1}, {"name": "VILLAREAL, RICHARD", "person_key": "VILLAREAL|RICHARD", "raw_invoices": ["250141"], "row_count": 1}], "invoice": null, "n_taxpayers": 3, "raw_invoices": ["250141", "25141"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "1038", "claimant_names": ["JORGE BARAJAS, ISRAEL", "MONTIEL FREYRE, SAMANTA"], "claimants": [{"name": "JORGE BARAJAS, ISRAEL", "person_key": "JORGE BARAJAS|ISRAEL", "raw_invoices": ["251038"], "row_count": 1}, {"name": "MONTIEL FREYRE, SAMANTA", "person_key": "MONTIEL FREYRE|SAMANTA", "raw_invoices": ["251038"], "row_count": 1}], "invoice": "251038", "n_taxpayers": 2, "raw_invoices": ["251038"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "1075", "claimant_names": ["LEDESMA, DAVID", "RODRIGUEZ, LEONOR"], "claimants": [{"name": "LEDESMA, DAVID", "person_key": "LEDESMA|DAVID", "raw_invoices": ["251075"], "row_count": 1}, {"name": "RODRIGUEZ, LEONOR", "person_key": "RODRIGUEZ|LEONOR", "raw_invoices": ["251075"], "row_count": 1}], "invoice": "251075", "n_taxpayers": 2, "raw_invoices": ["251075"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "910", "claimant_names": ["GALLARDO, LORENZO", "YANEZ, ELEUTERIO"], "claimants": [{"name": "GALLARDO, LORENZO", "person_key": "GALLARDO|LORENZO", "raw_invoices": ["250910"], "row_count": 1}, {"name": "YANEZ, ELEUTERIO", "person_key": "YANEZ|ELEUTERIO", "raw_invoices": ["250910"], "row_count": 1}], "invoice": "250910", "n_taxpayers": 2, "raw_invoices": ["250910"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "862", "claimant_names": ["LUNA, JACINTO", "MURILLO, JULIO"], "claimants": [{"name": "LUNA, JACINTO", "person_key": "LUNA|JACINTO", "raw_invoices": ["250862"], "row_count": 1}, {"name": "MURILLO, JULIO", "person_key": "MURILLO|JULIO", "raw_invoices": ["250862"], "row_count": 1}], "invoice": "250862", "n_taxpayers": 2, "raw_invoices": ["250862"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "634", "claimant_names": ["DAKAK, RAMI", "HERREJON, NICHOLAS"], "claimants": [{"name": "DAKAK, RAMI", "person_key": "DAKAK|RAMI", "raw_invoices": ["250634"], "row_count": 1}, {"name": "HERREJON, NICHOLAS", "person_key": "HERREJON|NICHOLAS", "raw_invoices": ["250634"], "row_count": 1}], "invoice": "250634", "n_taxpayers": 2, "raw_invoices": ["250634"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "426", "claimant_names": ["HERNANDEZ, AMADO", "HERNANDEZ, LYDIA"], "claimants": [{"name": "HERNANDEZ, AMADO", "person_key": "HERNANDEZ|AMADO", "raw_invoices": ["250426"], "row_count": 1}, {"name": "HERNANDEZ, LYDIA", "person_key": "HERNANDEZ|LYDIA", "raw_invoices": ["250426"], "row_count": 1}], "invoice": "250426", "n_taxpayers": 2, "raw_invoices": ["250426"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "353", "claimant_names": ["AVILA, LUCY", "SALAMA, ENGIE"], "claimants": [{"name": "AVILA, LUCY", "person_key": "AVILA|LUCY", "raw_invoices": ["250353"], "row_count": 1}, {"name": "SALAMA, ENGIE", "person_key": "SALAMA|ENGIE", "raw_invoices": ["250353"], "row_count": 1}], "invoice": "250353", "n_taxpayers": 2, "raw_invoices": ["250353"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "589", "claimant_names": ["SANTIAGO, DENISE", "SANTIAGO, EUNICE"], "claimants": [{"name": "SANTIAGO, DENISE", "person_key": "SANTIAGO|DENISE", "raw_invoices": ["250589"], "row_count": 1}, {"name": "SANTIAGO, EUNICE", "person_key": "SANTIAGO|EUNICE", "raw_invoices": ["250589"], "row_count": 1}], "invoice": "250589", "n_taxpayers": 2, "raw_invoices": ["250589"]}`
- **LOG_NUMBER_COLLISION:** `{"bare_log": "335", "claimant_names": ["GALLEGOS, ALEJANDRO", "ROJAS, LUIS"], "claimants": [{"name": "GALLEGOS, ALEJANDRO", "person_key": "GALLEGOS|ALEJANDRO", "raw_invoices": ["250335"], "row_count": 1}, {"name": "ROJAS, LUIS", "person_key": "ROJAS|LUIS", "raw_invoices": ["250335"], "row_count": 1}], "invoice": "250335", "n_taxpayers": 2, "raw_invoices": ["250335"]}`
- **RAGGED_EXPORT_ROW:** `{"histogram": {"0": 1, "1": 1, "5": 1062}, "n": 2, "note": "Short lines padded for parse. On spouse_11, Invoice is last col so shortfall drops the key; on link_5, Invoice is col 3."}`
- **SUPERSEDED_EXPORT:** `{"path": "T:\\taxops\\CSVFILES\\TAXPAYERspouse25.csv", "reason": "10-column spouse export without Invoice Number", "superseded_by": "T:\\audit\\investigation\\TAXPAYER.csv"}`
- **SUPERSEDED_EXPORT:** `{"path": "C:\\TaxOps\\taxops\\CSVFILES\\TAXPAYERspouse25.csv", "reason": "10-column spouse export without Invoice Number", "superseded_by": "T:\\audit\\investigation\\TAXPAYER.csv"}`
- **SUPERSEDED_EXPORT:** `{"path": "C:\\Users\\Windows 10\\Desktop\\TAXPAYERspouse25.csv", "reason": "10-column spouse export without Invoice Number", "superseded_by": "T:\\audit\\investigation\\TAXPAYER.csv"}`

## Open questions (carry to audit_run)

- Can the Drake report writer emit Invoice Number alongside Status in one export? If yes, CSM stops being load-bearing for linkage (key + clean names + workflow status). Record answer on audit_run; do not assume.

## Hard-constant retirement

`EXPECTED_DRAKE_ROWS` / `EXPECTED_LOG_NAMED_ROWS` / TaxOps expected counts in `audit/config.py` are no longer preflight crash gates. They remain as *historical reference only* (`HISTORICAL_*`); live comparison is previous-run memory (`baseline_memory.json`) → `BASELINE_DRIFT`.

## Loud correction to I4

I4 stated no shared key exists between Drake and TaxOps. **Superseded by Amendment 1:** Drake Invoice Number holds the Tax Log number and is exportable via TAXPAYER.csv. L0 is now three-way on `(invoice_number, tax_year)`.

## Files touched (A0 + Amendment 1)

- `audit/baseline.py`
- `audit/invoice_export.py` (new)
- `audit/db.py`, `audit/config.py`, `audit/preflight.py`, `audit/__main__.py`
- `audit/baseline_memory.json`
- `audit/investigation/TAXPAYER.csv` (canonical invoice export copy)
- `audit/investigation/A0-baseline.md` (this file)


---

## 2. Identity ladder (A1)

# A1 — Identity resolution ladder

_Generated: 2026-08-12T22:49:52Z_
_Audit DB: `T:\audit\audit_20260810.sqlite` run_id=5_

## Threshold policy

- Audit auto-accept fuzzy: **90** (aligned with prefill)
- Import accept (reference): **88**
- Human review band: **85–89**
- Pairs in disagreement band 88–89 this run: **0**

## L0 precondition funnel (Drake invoice keys)

| Stage | Distinct invoices |
|---|---:|
| Full-width raw keys | 844 |
| After bare-log format/range (C1) | 835 |
| After normalize (season strip in bare) | 835 |
| After bare-log collision exclusion (L0-eligible raw) | 815 |

## L0 three-way Venn (distinct invoice/log keys)

| Cell | Count |
|---|---:|
| Drake ∩ TaxOps ∩ Log | 571 |
| Drake ∩ TaxOps \ Log | 150 |
| Drake ∩ Log \ TaxOps | 52 |
| Drake only (neither) | 42 |
| TaxOps ∩ Log \ Drake L0-eligible | 194 |
| TaxOps TY keys with log# | 1017 |
| Tax Log named digit keys | 870 |

Jul31 name-based CSM Venn (Desktop 1155): all3=1009, taxops_not_log=126, log_not_taxops=13, neither=7. A1 L0 is bare-log three-way (Drake invoice `25xxxx` ↔ TaxOps/Log `xxxx`) on L0-eligible=815 bare_keys=815; three_way=571, drake∩taxops\log=150, drake∩log\taxops=52, drake_neither=42.

## CSM export lag

Authoritative CSM `C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC\CLIENTS.xlsx` rows=1159; invoice export full-width=1062 / data=1063. CSM is later by Last Change through 07/30; TAXPAYER.csv As-of 08-10 — invoice export is newer for keys/names; CSM still supplies last4 for L1.

## Per-tier link counts

| Tier | Rule | Links |
|---|---|---:|
| L0 Drake↔TaxOps | (invoice, tax_year) | 721 |
| L0 Drake↔Log | (invoice, tax_year) | 623 |
| L0 TaxOps↔Log | (log_number, tax_year) | 768 |
| Log index (info) | cut=772; all=1041→YR25=870 | — |
| L1 last4+surname | CSM ↔ TaxOps | 387 |
| L2 exact match_keys / high fuzzy≥90 | | 7 |
| L3 truncation prefix | CSM↔invoice only | 0 |
| L4 human review | | 1 |
| L5 unmatched Drake invoice (L0-eligible leftover) | | 35 |

## C6 — L3 truncation diagnosis

- **Wired sources:** authoritative CSM (≥39-char display) ↔ `TAXPAYER.csv` invoice first/last only. **Not** purple CSM, **not** Tax Log.
- **Verdict:** L3 is wired but ineffective for joint/truncated CSM strings (invoice names are shorter or differently ordered; first/last max 35/17, zero rows at 39–40). Identity for these rows is carried by **L0 invoice keys** + L1 last4+surname. `NAME_TRUNCATED` is reclassified **informational** (dropped from Priority worklist).
- L3 link count this run: **0**

## Samples (≤10 per tier)

### L0

- `drake_invoice:250449|2025|4` → `taxops_return:557` score=1.0 human=False
  evidence: `{"bare_log": "449", "client_id": 511, "drake_name": "C EST LA VIE APPAREL INC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250449", "log_number": "449", "tax_year": 2025, "taxops_na`
- `drake_invoice:250795|2025|5` → `taxops_return:536` score=1.0 human=False
  evidence: `{"bare_log": "795", "client_id": 490, "drake_name": "J & H MECHANICAL INC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250795", "log_number": "795", "tax_year": 2025, "taxops_name":`
- `drake_invoice:250545|2025|6` → `taxops_return:495` score=1.0 human=False
  evidence: `{"bare_log": "545", "client_id": 451, "drake_name": "CREATIVE 28 STUDIO", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250545", "log_number": "545", "tax_year": 2025, "taxops_name": "`
- `drake_invoice:250814|2025|11` → `taxops_return:306` score=1.0 human=False
  evidence: `{"bare_log": "814", "client_id": 269, "drake_name": "EMMANUEL AND DAVANI LLC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250814", "log_number": "814", "tax_year": 2025, "taxops_nam`
- `drake_invoice:250398|2025|12` → `taxops_return:237` score=1.0 human=False
  evidence: `{"bare_log": "398", "client_id": 201, "drake_name": "CORONA BROS INSTALLATION LLC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250398", "log_number": "398", "tax_year": 2025, "taxop`
- `drake_invoice:250570|2025|14` → `taxops_return:534` score=1.0 human=False
  evidence: `{"bare_log": "570", "client_id": 488, "drake_name": "INSTACINCH INC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250570", "log_number": "570", "tax_year": 2025, "taxops_name": "INST`
- `drake_invoice:250744|2025|20` → `taxops_return:874` score=1.0 human=False
  evidence: `{"bare_log": "744", "client_id": 815, "drake_name": "PRO WOOD FRAMING CONTRACTOR INC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250744", "log_number": "744", "tax_year": 2025, "ta`
- `drake_invoice:250512|2025|22` → `taxops_return:505` score=1.0 human=False
  evidence: `{"bare_log": "512", "client_id": 461, "drake_name": "INDUSTRIAL MACHINERY AUTOMATION SVC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250512", "log_number": "512", "tax_year": 2025,`
- `drake_invoice:250358|2025|30` → `taxops_return:302` score=1.0 human=False
  evidence: `{"bare_log": "358", "client_id": 264, "drake_name": "EERIE LANE COLLECTIVE LLC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250358", "log_number": "358", "tax_year": 2025, "taxops_n`
- `drake_invoice:251240|2025|31` → `taxops_return:941` score=1.0 human=False
  evidence: `{"bare_log": "1240", "client_id": 874, "drake_name": "ROCKET TCG LLC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "251240", "log_number": "1240", "tax_year": 2025, "taxops_name": "RO`

### L1

- `drake_csm:1743|ADAME, DEMETRIO L & ROSALINDA HERNANDEZ` → `taxops_client:1349` score=1.0 human=False
  evidence: `{"csm_name": "ADAME, DEMETRIO L & ROSALINDA HERNANDEZ", "last4": "1743", "rule": "last4+surname", "surname": "ADAME", "taxops_name": "ADAME, DEMETRIO"}`
- `drake_csm:5907|AGUAYO CORTES, JULIO G` → `taxops_client:12` score=1.0 human=False
  evidence: `{"csm_name": "AGUAYO CORTES, JULIO G", "last4": "5907", "rule": "last4+surname", "surname": "AGUAYO CORTES", "taxops_name": "AGUAYO CORTES, JULIO G"}`
- `drake_csm:5599|AGUAYO, MARGARITA P` → `taxops_client:14` score=1.0 human=False
  evidence: `{"csm_name": "AGUAYO, MARGARITA P", "last4": "5599", "rule": "last4+surname", "surname": "AGUAYO", "taxops_name": "AGUAYO, MARGARITA P"}`
- `drake_csm:9371|AGUILAR FLORES G, MONICA` → `taxops_client:17` score=1.0 human=False
  evidence: `{"csm_name": "AGUILAR FLORES G, MONICA", "last4": "9371", "rule": "last4+surname", "surname": "AGUILAR FLORES", "taxops_name": "AGUILAR FLORES G, MONICA"}`
- `drake_csm:7644|AGUILAR REYNA, ABUNDIO` → `taxops_client:22` score=1.0 human=False
  evidence: `{"csm_name": "AGUILAR REYNA, ABUNDIO", "last4": "7644", "rule": "last4+surname", "surname": "AGUILAR REYNA", "taxops_name": "AGUILAR REYNA, ABUNDIO"}`
- `drake_csm:8695|AGUILAR, AMBER A` → `taxops_client:304` score=1.0 human=False
  evidence: `{"csm_name": "AGUILAR, AMBER A", "last4": "8695", "rule": "last4+surname", "surname": "AGUILAR", "taxops_name": "AGUILAR, AMBER"}`
- `drake_csm:6720|AGUILAR, JEREMIAH & SOLEDAD` → `taxops_client:28` score=1.0 human=False
  evidence: `{"csm_name": "AGUILAR, JEREMIAH & SOLEDAD", "last4": "6720", "rule": "last4+surname", "surname": "AGUILAR", "taxops_name": "AGUILAR, JEREMIAH & SOLEDAD"}`
- `drake_csm:1344|AGUIRRE JIMENEZ, ANTONIO` → `taxops_client:35` score=1.0 human=False
  evidence: `{"csm_name": "AGUIRRE JIMENEZ, ANTONIO", "last4": "1344", "rule": "last4+surname", "surname": "AGUIRRE JIMENEZ", "taxops_name": "AGUIRRE JIMENEZ, ANTONIO"}`
- `drake_csm:1593|AGUIRRE, JOSE E & IRMA` → `taxops_client:1316` score=1.0 human=False
  evidence: `{"csm_name": "AGUIRRE, JOSE E & IRMA", "last4": "1593", "rule": "last4+surname", "surname": "AGUIRRE", "taxops_name": "AGUIRRE, JOSE"}`
- `drake_csm:9208|ALDAMA, MAYRA A` → `taxops_client:1634` score=1.0 human=False
  evidence: `{"csm_name": "ALDAMA, MAYRA A", "last4": "9208", "rule": "last4+surname", "surname": "ALDAMA", "taxops_name": "ALDAMA, MAYRA"}`

### L2

- `drake_invoice:251195|327` → `taxops_client:1706` score=1.0 human=False
  evidence: `{"bare_log": "1195", "drake": "MEDINA, YOLANDA", "keys": [["MEDINA", "YOLANDA"]], "method": "exact_match_keys", "taxops": "MEDINA, YOLANDA"}`
- `drake_invoice:251062|465` → `taxops_client:2191` score=1.0 human=False
  evidence: `{"bare_log": "1062", "drake": "ANDRADE RAZO, MIGUEL", "keys": [["ANDRADE RAZO", "MIGUEL"], ["ANDRADE", "MIGUEL"]], "method": "exact_match_keys", "taxops": "ANDRADE, MIGUEL"}`
- `drake_invoice:251270|546` → `taxops_client:2130` score=1.0 human=False
  evidence: `{"bare_log": "1270", "drake": "GONZALEZ, BRIANNA", "keys": [["GONZALEZ", "BRIANNA"]], "method": "exact_match_keys", "taxops": "GONZALEZ LOPEZ, BRIANNA S"}`
- `drake_invoice:250489|905` → `taxops_client:1440` score=1.0 human=False
  evidence: `{"bare_log": "489", "drake": "HERNANDEZ, ABEL", "keys": [["HERNANDEZ", "ABEL"]], "method": "exact_match_keys", "taxops": "HERNANDEZ, ABEL"}`
- `drake_invoice:250510|entity` → `taxops_client:503` score=100.0 human=False
  evidence: `{"bare_log": "510", "entity_exactish": "JORGE DE LA OSA DENTAL CORPORATION", "score": 100.0}`
- `drake_invoice:251189|entity` → `taxops_client:45` score=100.0 human=False
  evidence: `{"bare_log": "1189", "entity_exactish": "ALL IN ONE CONCRETE LLC", "score": 100.0}`
- `drake_invoice:251257|475` → `taxops_client:2259` score=92.85714285714286 human=False
  evidence: `{"bare_log": "1257", "drake": "MARTINEZ, MARIA", "method": "order_normalized_token_sort", "score": 92.85714285714286, "taxops": "MARTINEZ, MARIO"}`

### L4

- `drake_csm:1945|SORIA, NORMA O` → `taxops_client:972,2199` score=None human=True
  evidence: `{"last4": "1945", "n": 2, "reason": "last4+surname_ambiguous", "surname": "SORIA"}`

## Notes

- L0 compares **bare log numbers**: Drake Invoice `250141` ≡ TaxOps/Log `141` (strip season prefix `25` + leading zeros). Raw invoice still recorded in evidence.
- L0 never matches across tax years; tax_year is required on the TaxOps side.
- Malformed / collision / proforma-stale invoices are excluded from L0 (drop to name tiers).
- No link uses last-4 as sole evidence (L1 always pairs last4 with surname).
- Name precedence: TAXPAYER.csv first/last over CSM joint for identity; CSM for last4 + display.
- Fuzzy threshold chosen: **90** (prefill-aligned). Import's 88 is reported in the 88–89 band only.


---

## 3. Disposition & delta (A2)

# A2 — Stable findings & disposition delta

_Generated: 2026-08-12T22:49:56Z_
_Run label: `lucy-gate-remeasure`_
_Disposition DB: `T:\audit\audit_disposition.sqlite`_

## Headline (NEW + REGRESSED)

**0**  *(not total open findings)*

| Bucket | Count |
|---|---:|
| NEW | 0 |
| RECURRING | 133 |
| RESOLVED (absent this run) | 0 |
| REGRESSED | 0 |

### NEW by type


### RECURRING by type

- `NAME_TRUNCATED`: 72
- `L0_DRAKE_ONLY`: 33
- `LOG_NUMBER_COLLISION`: 19
- `MALFORMED_LOG_NUMBER`: 9

## Fingerprint recipe (per type)

`finding_id = sha256(finding_type || entity_key || salient_payload)`

Canonical entity keys **exclude** `created_at`, TaxOps row ids (merge-volatile), and raw untruncated names.

- **CONFIRMED_MATCH:** Skipped for disposition worklist (noise). Fingerprint exists for completeness: bare_log|last4|norm_name.
- **DUPLICATE_CLIENT:** entity_key = sorted(norm_a, norm_b) joined by '||'. Client ids excluded — merges would churn fingerprints.
- **L0_DRAKE_ONLY:** entity_key = bare_log|tax_year for L0-eligible Drake invoice with no TaxOps and no Log hit.
- **L0_KEY_PRESENT:** entity_key = bare_log|tax_year for successful invoice L0 links (informational / coverage; usually not worklist).
- **L5_UNMATCHED:** entity_key = bare_log|norm_invoice_name for leftover L0-eligible invoice taxpayers after ladder.
- **LOGGED_NOT_PREPARED:** entity_key = bare_log|tax_year (Tax Log col B normalized). Sheet row numbers churn; bare log does not.
- **LOG_NUMBER_COLLISION:** entity_key = bare_log (Amendment 2 C2). Salient = n_taxpayers. Pre-C2 collisions keyed on raw invoice are a different fingerprint space.
- **MALFORMED_LOG_NUMBER:** entity_key = raw invoice string (digits as exported). Salient unused. Bare form lives in detail only — do not put bare in entity_key (would churn when normalize rules tighten).
- **MERGE_UNTRACEABLE:** Singleton: entity_key='global'. Emitted when there is no durable merge history table. May coexist with MERGE_PARTIAL_TRAIL (audit_log keep_id/discard_id only; no discarded-identity snapshot).
- **MISSING_IN_TAXOPS:** entity_key = last4|' '|norm_drake_name or bare invoice/log if known. No Drake stage_id.
- **NAME_TRUNCATED:** entity_key = last4|' '|normalized_csm_name_prefix (folded, no stage_id). Survives CSM re-export row order and TaxOps merges; changes only if CSM last4 or truncated display string changes. Amendment 2 C6: informational — CSM 40-char display artifact; L0 invoice / L1 carry identity; not Priority.
- **NEEDS_HUMAN:** entity_key = subtype|' '|stable subject key (last4/name/bare_log). Subtype included so distinct review reasons don't collapse.
- **PHANTOM_IN_TAXOPS:** entity_key = bare_log|tax_year if log present, else norm_name(last|first). Avoids TaxOps client_id (merge attrition).
- **PREPARED_NOT_LOGGED:** entity_key = last4|' '|norm_drake_name (Drake prepared, no log link). Prefer bare_log when invoice available.
- **SPOUSE_AMBIGUOUS:** entity_key = last4|' '|norm_drake_name.
- **SPOUSE_STORE_DIVERGENCE:** entity_key = norm_client_name|tax_year (or last4 when present). No client_id.

## Jul31 backfill

`{"c1_legacy_malformed_seed": {"inserted": 0, "legacy_malformed": 22, "skipped": 22}, "existing_rows": 1850, "skipped": true}`

Weak reconstructions use `first_seen_run='pre-baseline'` (not faked as jul31).

## Merge attrition trail

- **Verdict:** `MERGE_TRAIL_COMPLETE`
- **Rationale:** client_merge_history present with discard identity snapshot columns; 7 history row(s). Merges are reconstructable from the trail.
- audit_log merge API rows: 89
- payloads with keep/discard: 86
- merge_* tables: `['client_merge_history']`

## Sample NEW findings (≤15)


## Disposition schema

```
audit_disposition(finding_id PK, finding_type, entity_key, status,
  resolved_by, resolved_at, note, first_seen_run, last_seen_run, ...)
status ∈ OPEN | ACKED | WONTFIX | RESOLVED | FALSE_POSITIVE
```

DB path is durable across runs and **never truncated** by the audit tool.


### Disposition DB rollup (live)

- Path: `T:\audit\audit_disposition.sqlite`
- By status: `{"ACKED": 8, "FALSE_POSITIVE": 24, "OPEN": 1733, "RESOLVED": 85}`

| Finding type (OPEN/ACKED) | Count |
|---|---:|
| `PHANTOM_IN_TAXOPS` | 569 |
| `LOGGED_NOT_PREPARED` | 513 |
| `PREPARED_NOT_LOGGED` | 282 |
| `SPOUSE_STORE_DIVERGENCE` | 165 |
| `NAME_TRUNCATED` | 72 |
| `L0_DRAKE_ONLY` | 35 |
| `SPOUSE_AMBIGUOUS` | 28 |
| `MISSING_IN_TAXOPS` | 20 |
| `LOG_NUMBER_COLLISION` | 19 |
| `DUPLICATE_CLIENT` | 17 |
| `MALFORMED_LOG_NUMBER` | 9 |
| `NEEDS_HUMAN` | 8 |
| `JUL1_PROVENANCE` | 2 |
| `PREFILL_STUB_BURST` | 1 |
| `MERGE_UNTRACEABLE` | 1 |

### Disposition runs

- `a2-20260810T211603` source=`a2-partial` 2026-08-10T21:16:10Z → 2026-08-10T21:16:10Z stats=`{"NEW": 37, "RECURRING": 71, "REGRESSED": 0, "RESOLVED": 0, "by_type_new": {"L0_DRAKE_ONLY": 35, "MERGE_UNTRACEABLE": 1, "NAME_TRUNCATED": 1}, "by_type_recurring": {"NAME_TRUNCATED": 71}, "headline_NEW_plus_REGRESSED": 37}`
- `a3-20260810T212908` source=`a3-partial` 2026-08-10T21:29:08Z → 2026-08-10T21:29:08Z stats=`{"NEW": 801, "RECURRING": 72, "REGRESSED": 0, "RESOLVED": 0, "by_type_new": {"DUPLICATE_CLIENT": 17, "JUL1_PROVENANCE": 1, "LOGGED_NOT_PREPARED": 300, "PHANTOM_IN_TAXOPS": 324, "PREFILL_STUB_BURST": 1, "PREPARED_NOT_LOGGED": 157, "SPOUSE_STORE_DIVERGENCE": 1}, "by_type_recurring": {"NAME_TRUNCATED": 72}, "headline_NEW_plus_REGRESSED": 801}`
- `amend2-c1c2` source=`a2-partial` 2026-08-10T23:33:39Z → 2026-08-10T23:33:39Z stats=`{"NEW": 21, "RECURRING": 113, "REGRESSED": 0, "RESOLVED": 15, "by_type_new": {"LOG_NUMBER_COLLISION": 19, "MALFORMED_LOG_NUMBER": 2}, "by_type_recurring": {"L0_DRAKE_ONLY": 33, "MALFORMED_LOG_NUMBER": 7, "MERGE_UNTRACEABLE": 1, "NAME_TRUNCATED": 72}, "headline_NEW_plus_REGRESSED": 21}`
- `a3-20260810T233346` source=`a3-partial` 2026-08-10T23:33:46Z → 2026-08-10T23:33:46Z stats=`{"NEW": 18, "RECURRING": 856, "REGRESSED": 0, "RESOLVED": 0, "by_type_new": {"LOGGED_NOT_PREPARED": 13, "PHANTOM_IN_TAXOPS": 4, "SPOUSE_STORE_DIVERGENCE": 1}, "by_type_recurring": {"DUPLICATE_CLIENT": 17, "JUL1_PROVENANCE": 1, "LOGGED_NOT_PREPARED": 287, "NAME_TRUNCATED": 72, "PHANTOM_IN_TAXOPS": 323, "PREFILL_STUB_BURST": 1, "PREPARED_NOT_LOGGED": 155}, "headline_NEW_plus_REGRESSED": 18}`
- `wave0-link5` source=`a2-partial` 2026-08-11T22:01:59Z → 2026-08-11T22:01:59Z stats=`{"NEW": 0, "RECURRING": 134, "REGRESSED": 0, "RESOLVED": 0, "by_type_new": {}, "by_type_recurring": {"L0_DRAKE_ONLY": 33, "LOG_NUMBER_COLLISION": 19, "MALFORMED_LOG_NUMBER": 9, "MERGE_UNTRACEABLE": 1, "NAME_TRUNCATED": 72}, "headline_NEW_plus_REGRESSED": 0}`
- `a3-20260811T220206` source=`a3-partial` 2026-08-11T22:02:06Z → 2026-08-11T22:02:06Z stats=`{"NEW": 2, "RECURRING": 872, "REGRESSED": 0, "RESOLVED": 0, "by_type_new": {"LOGGED_NOT_PREPARED": 2}, "by_type_recurring": {"DUPLICATE_CLIENT": 17, "JUL1_PROVENANCE": 1, "LOGGED_NOT_PREPARED": 298, "NAME_TRUNCATED": 72, "PHANTOM_IN_TAXOPS": 327, "PREFILL_STUB_BURST": 1, "PREPARED_NOT_LOGGED": 155, "SPOUSE_STORE_DIVERGENCE": 1}, "headline_NEW_plus_REGRESSED": 2}`
- `a2-is-test-20260811` source=`a2-partial` 2026-08-12T00:03:34Z → 2026-08-12T00:03:34Z stats=`{"NEW": 0, "RECURRING": 133, "REGRESSED": 0, "RESOLVED": 0, "by_type_new": {}, "by_type_recurring": {"L0_DRAKE_ONLY": 33, "LOG_NUMBER_COLLISION": 19, "MALFORMED_LOG_NUMBER": 9, "NAME_TRUNCATED": 72}, "headline_NEW_plus_REGRESSED": 0}`
- `a3-20260812T000404` source=`a3-partial` 2026-08-12T00:04:04Z → 2026-08-12T00:04:04Z stats=`{"NEW": 3, "RECURRING": 823, "REGRESSED": 9, "RESOLVED": 0, "by_type_new": {"JUL1_PROVENANCE": 1, "PHANTOM_IN_TAXOPS": 1, "SPOUSE_STORE_DIVERGENCE": 1}, "by_type_recurring": {"LOGGED_NOT_PREPARED": 300, "NAME_TRUNCATED": 72, "PHANTOM_IN_TAXOPS": 296, "PREPARED_NOT_LOGGED": 155}, "headline_NEW_plus_REGRESSED": 12}`
- `a3-20260812T221343` source=`a3-partial` 2026-08-12T22:13:43Z → 2026-08-12T22:13:44Z stats=`{"NEW": 133, "RECURRING": 722, "REGRESSED": 0, "RESOLVED": 0, "by_type_new": {"LOGGED_NOT_PREPARED": 60, "PHANTOM_IN_TAXOPS": 36, "PREPARED_NOT_LOGGED": 37}, "by_type_recurring": {"DUPLICATE_CLIENT": 8, "JUL1_PROVENANCE": 1, "LOGGED_NOT_PREPARED": 187, "NAME_TRUNCATED": 72, "PHANTOM_IN_TAXOPS": 297, "PREFILL_STUB_BURST": 1, "PREPARED_NOT_LOGGED": 155, "SPOUSE_STORE_DIVERGENCE": 1}, "headline_NEW_plus_REGRESSED": 133}`
- `a3-20260812T222226` source=`a3-partial` 2026-08-12T22:22:26Z → 2026-08-12T22:22:26Z stats=`{"NEW": 1, "RECURRING": 854, "REGRESSED": 0, "RESOLVED": 0, "by_type_new": {"SPOUSE_STORE_DIVERGENCE": 1}, "by_type_recurring": {"DUPLICATE_CLIENT": 8, "JUL1_PROVENANCE": 1, "LOGGED_NOT_PREPARED": 247, "NAME_TRUNCATED": 72, "PHANTOM_IN_TAXOPS": 333, "PREFILL_STUB_BURST": 1, "PREPARED_NOT_LOGGED": 192}, "headline_NEW_plus_REGRESSED": 1}`
- `lucy-gate-remeasure` source=`a2-partial` 2026-08-12T22:49:56Z → 2026-08-12T22:49:56Z stats=`{"NEW": 0, "RECURRING": 133, "REGRESSED": 0, "RESOLVED": 0, "by_type_new": {}, "by_type_recurring": {"L0_DRAKE_ONLY": 33, "LOG_NUMBER_COLLISION": 19, "MALFORMED_LOG_NUMBER": 9, "NAME_TRUNCATED": 72}, "headline_NEW_plus_REGRESSED": 0}`
- `a3-20260812T225001` source=`a3-partial` 2026-08-12T22:50:01Z → 2026-08-12T22:50:01Z stats=`{"NEW": 0, "RECURRING": 855, "REGRESSED": 0, "RESOLVED": 0, "by_type_new": {}, "by_type_recurring": {"DUPLICATE_CLIENT": 8, "JUL1_PROVENANCE": 1, "LOGGED_NOT_PREPARED": 247, "NAME_TRUNCATED": 72, "PHANTOM_IN_TAXOPS": 333, "PREFILL_STUB_BURST": 1, "PREPARED_NOT_LOGGED": 192, "SPOUSE_STORE_DIVERGENCE": 1}, "headline_NEW_plus_REGRESSED": 0}`

---

## 4. Failure-mode checks F1–F12 (A3)

# A3 — Failure-mode checks (F1–F12)

_Generated: 2026-08-12T22:50:01Z_
_Disposition DB: `T:\audit\audit_disposition.sqlite`_
_Delta after checks: `{"NEW": 0, "RECURRING": 855, "REGRESSED": 0, "RESOLVED": 0, "findings_emitted": 855, "headline": 0}`_

| Check | Title | Deviation | Baseline highlight | Actual highlight |
|---|---|---|---|---|
| F1 | Drake 40-char name truncation | `INFORMATIONAL` | `{"desktop_at_40": 54, "jul31_NAME_TRUNCATED": 71, "prefill_at_40": 74}` | `{"csm_at_39": 17, "csm_at_40_plus": 55, "csm_truncated_total": 72, "prefill_csm_name_raw_at_40": 74}` |
| F2 | Name-order / joint-format divergence | `INFORMATIONAL` | `{"csm_with_amp": 442, "csm_with_comma": 1254, "purple_with_comma": 0}` | `{"csm_n": 1159, "csm_with_amp": 351, "csm_with_comma": 1044, "taxops_with_amp": 249}` |
| F3 | Duplicate TaxOps clients (twins) | `INFORMATIONAL` | `{"jul31_DUPLICATE_CLIENT": 142, "live_exact_groups": 8, "normalized_multi_buckets": 14}` | `{"exact_extra_rows": 8, "exact_groups": 8, "fuzzy_88_95_pairs_surname_blocked": 37, "normalized_multi_buckets": 8}` |
| F4 | Jul1 created_at rewrite provenance | `INFORMATIONAL` | `{"jul31_cluster_approx": 344, "jul31_jul1_dated": 368, "live_jul1_dated_prior": 191}` | `{"iso_format": 12, "jul1_dated": 184, "space_format": 172, "stamp_18_19_30": 172}` |
| F5 | Asymmetric SSN twins | `INFORMATIONAL` | `{"note": "Common in Jul1 cluster (0/368 SSN on stamp set)"}` | `{"asymmetric_groups": 0, "samples": []}` |
| F6 | Shells without log numbers | `INFORMATIONAL` | `{"approx_pct_returns_without_log": 26, "fill_rate_log": 0.74}` | `{"pct_without_log": 25.33, "pending_intake_total": 117, "returns_total": 1607, "returns_without_log": 407}` |
| F7 | Last-4 collisions | `INFORMATIONAL` | `{"client_keys_surplus_approx": 78, "csm_surplus_approx": 52, "prefill_surplus_approx": 130}` | `{"csm_collision_keys": 51, "csm_surplus": 52, "taxops_collision_keys": 78, "taxops_surplus": 82}` |
| F8 | Phantom / unmatched TaxOps | `INFORMATIONAL` | `{"jul31_PHANTOM_IN_TAXOPS": 227}` | `{"by_bucket": {"closed_or_logout": 150, "unmatched_other": 183}, "log_index": {"all_bares": 1041, "canonical_yr25_bares": 870, "named_rows": 1251, "restart_cut": 772}, "note": "Log keys = YR=25 canonical per bare (restart cut); not all named rows", "phantom_returns": 333}` |
| F9 | Logged-not-prepared / prepared-not-logged | `INFORMATIONAL` | `{"LOGGED_NOT_PREPARED": 141, "PREPARED_NOT_LOGGED": 88}` | `{"csm_rows_with_status_field": 1158, "invoice_L0_not_in_log": 192, "log_index": {"all_bares": 1041, "canonical_yr25_bares": 870, "restart_cut": 772}, "log_not_in_invoice_L0": 247}` |
| F10 | Prefill stub proliferation (--link-clients) | `INFORMATIONAL` | `{"all_ssn_and_prefill_linked": true, "aug7_burst": 244}` | `{"burst_count": 244, "exact_name_older_twin": 8, "prefill_linked": 244, "ssn_bearing": 243}` |
| F11 | Spouse triple-store divergence | `INFORMATIONAL` | `{"SPOUSE_STORE_DIVERGENCE": 198, "household_prefill": 1302, "spouses": 166}` | `{"clients_with_spouse_cols": 406, "drake_household_prefill": 1302, "only_clients_cols": 0, "spouses_rows": 442}` |
| F12 | Cross-system key coverage (Amendment 1) | `SUPERSEDED` | `{"I4_claim": "no shared key", "a1_three_way": 586, "amendment1": "Invoice Number = Tax Log number (season-prefixed in Drake)"}` | `{"client_external_ids_exists": false, "client_external_ids_rows": 0, "l0_eligible_invoices": 815, "three_way_bare_log": 571}` |

## F1 — Drake 40-char name truncation

- **Query:** COUNT CSM Client Name WHERE len>=39; prefill csm_name_raw len=40; TaxOps longer bag sharing surname prefix
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"desktop_at_40": 54, "jul31_NAME_TRUNCATED": 71, "onedrive_expected_near": "\u226554 (OneDrive 1159 vs Desktop 1155)", "prefill_at_40": 74}`
- **Actual:** `{"cross_source_superstring_hits": 0, "csm_at_39": 17, "csm_at_40_plus": 55, "csm_truncated_total": 72, "prefill_csm_name_raw_at_40": 74}`
- **Note:** Cap is upstream Drake CSM, not TaxOps VARCHAR.
- **Findings emitted:** 72

## F2 — Name-order / joint-format divergence

- **Query:** Classify CSM/TaxOps for comma, ampersand, blank-first (entity)
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"clients_blank_first": 151, "csm_with_amp": 442, "csm_with_comma": 1254, "purple_with_comma": 0}`
- **Actual:** `{"clients_blank_first": 146, "csm_n": 1159, "csm_with_amp": 351, "csm_with_comma": 1044, "order_hard_pairs_sampled": 0, "taxops_name_fields_with_comma": 5, "taxops_with_amp": 249}`
- **Note:** Format classifiers only; L3/L4 resolution volume deferred to ladder stats.
- **Findings emitted:** 0

## F3 — Duplicate TaxOps clients (twins)

- **Query:** GROUP BY exact last|first; punct-stripped norm; fuzzy 88–95 surname-blocked
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"jul31_DUPLICATE_CLIENT": 142, "jul31_stamped_collisions_approx": 254, "live_exact_groups": 8, "normalized_multi_buckets": 14}`
- **Actual:** `{"exact_extra_rows": 8, "exact_groups": 8, "fuzzy_88_95_pairs_surname_blocked": 37, "normalized_multi_buckets": 8, "top_blast": [{"blast": 1, "ids": [659, 2275], "key": "HERNANDEZ|ISMAEL", "n": 2}, {"blast": 1, "ids": [731, 2276], "key": "NUNO|JUAN", "n": 2}, {"blast": 1, "ids": [1215, 2272], "key": "ALVARADO|OSCAR", "n": 2}, {"blast": 2, "ids": [1278, 2393], "key": "LUNA|ESTEBAN", "n": 2}, {"blast": 1, "ids": [1344, 2404], "key": "VALDEZ|SANDRA", "n": 2}, {"blast": 3, "ids": [1440, 2273], "key": "HERNANDEZ|ABEL", "n": 2}, {"blast": 1, "ids": [1506, 2329], "key": "TASHAYOD|ALEX", "n": 2}, {"blast": 1, "ids": [1709, 2429], "key": "SOLOMON|LAUREN", "n": 2}]}`
- **Note:** Ranked by blast radius (returns + log numbers at risk).
- **Findings emitted:** 8

## F4 — Jul1 created_at rewrite provenance

- **Query:** clients.created_at LIKE '2026-07-01%'; space vs ISO; id<=1753; pre-Jul1 returns
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"jul31_cluster_approx": 344, "jul31_jul1_dated": 368, "live_jul1_dated_prior": 191, "mechanism": "CREATED_AT_REWRITE_NOT_INSERT"}`
- **Actual:** `{"id_le_1753": 167, "iso_format": 12, "jul1_dated": 184, "space_format": 172, "stamp_18_19_30": 172, "with_pre_jul1_return": 172}`
- **Note:** Provenance flag, not a repair target.
- **Findings emitted:** 1

## F5 — Asymmetric SSN twins

- **Query:** Normalized name dup groups where some have ssn_last4 and others NULL
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"note": "Common in Jul1 cluster (0/368 SSN on stamp set)"}`
- **Actual:** `{"asymmetric_groups": 0, "samples": []}`
- **Note:** Merge direction should favor SSN-bearing row.
- **Findings emitted:** 0

## F6 — Shells without log numbers

- **Query:** returns WHERE log_number IS NULL; PENDING INTAKE subset
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"approx_pct_returns_without_log": 26, "fill_rate_log": 0.74}`
- **Actual:** `{"by_status": {"CANCELLED": {"n": 2, "no_log": 0}, "EFILE": {"n": 2, "no_log": 0}, "EFILE READY": {"n": 1, "no_log": 0}, "FINALIZE": {"n": 25, "no_log": 1}, "HOLD": {"n": 3, "no_log": 0}, "LOG OUT": {"n": 840, "no_log": 117}, "OLD PICKUP": {"n": 1, "no_log": 1}, "PENDING INTAKE": {"n": 117, "no_log": 116}, "PICK UP": {"n": 1, "no_log": 1}, "PICKUP": {"n": 75, "no_log": 13}, "PRIOR HOLD": {"n": 3, "no_log": 2}, "PRIOR PROC": {"n": 2, "no_log": 2}, "PROCESSING": {"n": 535, "no_log": 154}}, "pct_without_log": 25.33, "pending_intake_total": 117, "pending_intake_without_log": 116, "returns_total": 1607, "returns_without_log": 407}`
- **Findings emitted:** 0

## F7 — Last-4 collisions

- **Query:** GROUP BY last4 HAVING COUNT>1 on CSM, clients, prefill; assert L1 not last4-alone
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"client_keys_surplus_approx": 78, "csm_surplus_approx": 52, "prefill_surplus_approx": 130}`
- **Actual:** `{"csm_collision_keys": 51, "csm_surplus": 52, "l1_last4_only_violations": 0, "prefill_collision_keys": 130, "taxops_collision_keys": 78, "taxops_surplus": 82}`
- **Note:** Collisions are expected; last4-alone L1 would be alarming.
- **Findings emitted:** 0

## F8 — Phantom / unmatched TaxOps

- **Query:** TY2025 returns whose bare log ∉ Tax Log (YR=25 canonical) and ∉ Drake L0-eligible invoices
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"jul31_PHANTOM_IN_TAXOPS": 227}`
- **Actual:** `{"by_bucket": {"closed_or_logout": 150, "unmatched_other": 183}, "log_index": {"all_bares": 1041, "canonical_yr25_bares": 870, "named_rows": 1251, "restart_cut": 772}, "note": "Log keys = YR=25 canonical per bare (restart cut); not all named rows", "phantom_returns": 333}`
- **Note:** Split test/closed before worklist; name-only phantoms need A3+ ladder residual.
- **Findings emitted:** 333

## F9 — Logged-not-prepared / prepared-not-logged

- **Query:** bare_log: invoice L0 vs Tax Log XCEL 2025 YR=25 canonical (workflow routing)
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"LOGGED_NOT_PREPARED": 141, "PREPARED_NOT_LOGGED": 88}`
- **Actual:** `{"csm_rows_with_status_field": 1158, "invoice_L0_not_in_log": 192, "log_index": {"all_bares": 1041, "canonical_yr25_bares": 870, "restart_cut": 772}, "log_not_in_invoice_L0": 247, "method": "invoice_key_proxy_canonical_yr25_log"}`
- **Note:** Marked workflow findings — different worklist from data findings.
- **Findings emitted:** 439

## F10 — Prefill stub proliferation (--link-clients)

- **Query:** clients.created_at LIKE '2026-08-07T22:27:47%'; join drake_prefill_links
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"all_ssn_and_prefill_linked": true, "aug7_burst": 244}`
- **Actual:** `{"burst_count": 244, "exact_name_older_twin": 8, "prefill_linked": 244, "ssn_bearing": 243}`
- **Note:** Classify twin-of-older vs legitimately new before merge.
- **Findings emitted:** 1

## F11 — Spouse triple-store divergence

- **Query:** clients.spouse_* vs spouses vs drake_household_prefill
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"SPOUSE_STORE_DIVERGENCE": 198, "household_prefill": 1302, "spouses": 166}`
- **Actual:** `{"clients_with_spouse_cols": 406, "drake_household_prefill": 1302, "only_clients_cols": 0, "only_spouses_table": 36, "spouses_rows": 442}`
- **Findings emitted:** 1

## F12 — Cross-system key coverage (Amendment 1)

- **Query:** bare_log three-way on TAXPAYER.csv L0 ∩ TaxOps ∩ Log(YR=25 canonical); client_external_ids exists?
- **Deviation class:** `SUPERSEDED`
- **Baseline:** `{"I4_claim": "no shared key", "a1_three_way": 586, "amendment1": "Invoice Number = Tax Log number (season-prefixed in Drake)"}`
- **Actual:** `{"client_external_ids_exists": false, "client_external_ids_rows": 0, "drake_taxops": 721, "invoice_full_width_coverage_pct": 99.91, "l0_eligible_invoices": 815, "log_index": {"all_bares": 1041, "canonical_yr25_bares": 870, "restart_cut": 772}, "pct_l0_not_three_way": 29.94, "three_way_bare_log": 571}`
- **Note:** I4 F12 'no key' superseded by invoice/log bare-key. client_external_ids not minted yet.
- **Findings emitted:** 0

## Loud corrections vs I4

- **F12:** I4 'no cross-system key' is **SUPERSEDED** by Amendment 1 (Invoice Number / bare log).
- **F8/F9:** Counts use invoice-key proxies; Jul31 used name-match residuals — not 1:1 comparable.


---

## 5. Invariant guards (A4)

# A4 — Invariant guards

_Generated: 2026-08-12T22:50:47Z_

**Overall: 5 PASS / 1 MODIFIED / 1 FAIL**

| Guard | Title | Result |
|---|---|---|
| I1 | Zero duplicate (log_number, tax_year) in returns | `PASS` |
| I2 | Zero entity_link with sole evidence last-4 | `PASS` |
| I3 | No two clients share (norm last, norm first, ssn_last4) non-null | `PASS` |
| I4 | Every drake_prefill_links.client_id resolves to a client | `PASS` |
| I5 | ux_returns_log_year and idx_returns_unique_client_year present+UNIQUE | `PASS` |
| I6 | TaxOps DB unchanged across audit run (mtime + size) | `MODIFIED` |
| I7 | Tax Log CSV import idempotency (throwaway copy) | `FAIL` |

## I1 — Zero duplicate (log_number, tax_year) in returns

- **Result:** `PASS`
- **Detail:** `{"duplicate_groups": 0, "samples": []}`

## I2 — Zero entity_link with sole evidence last-4

- **Result:** `PASS`
- **Detail:** `{"samples": [], "violations": 0}`

## I3 — No two clients share (norm last, norm first, ssn_last4) non-null

- **Result:** `PASS`
- **Note:** Hard uniqueness on identity triple; distinct from exact-name-only twins without SSN.
- **Detail:** `{"duplicate_groups": 0, "samples": {}}`

## I4 — Every drake_prefill_links.client_id resolves to a client

- **Result:** `PASS`
- **Detail:** `{"linked_rows": 1264, "orphan_ids": [], "orphans": 0}`

## I5 — ux_returns_log_year and idx_returns_unique_client_year present+UNIQUE

- **Result:** `PASS`
- **Note:** db.py skips creating ux_returns_log_year when duplicate pairs exist.
- **Detail:** `{"blocking_log_year_dups": 0, "idx_client_year_unique": true, "idx_returns_unique_client_year": true, "index_names": ["idx_returns_client_year", "idx_returns_proc_year", "idx_returns_status_year", "idx_returns_unique_client_year", "idx_returns_updated_at", "ux_returns_log_year"], "ux_returns_log_year": true, "ux_unique": true}`

## I6 — TaxOps DB unchanged across audit run (mtime + size)

- **Result:** `MODIFIED`
- **Note:** PASS = mtime+size stable. MODIFIED = mtime drifted (NSSM/Flask) but size unchanged (RO URI + throwaway copy only). FAIL = size changed.
- **Detail:** `{"external_mtime_touch": true, "mtime_after_ns": 1786575009948095500, "mtime_before_ns": 1786574888938988500, "mtime_unchanged": false, "original_condition": "mtime_unchanged", "original_condition_held": false, "size_after": 10461184, "size_before": 10461184, "size_unchanged": true, "weaker_condition": "size_unchanged", "weaker_condition_held": true}`

## I7 — Tax Log CSV import idempotency (throwaway copy)

- **Result:** `FAIL`
- **Note:** Second pass must create 0 clients AND 0 returns (catches log#-match and _upsert_return gaps). See pass2_return_diagnosis when FAIL.
- **Detail:** `{"clients_after_pass1": 1526, "clients_after_pass2": 1526, "clients_before": 1521, "clients_idempotent": true, "csv": "C:\\Users\\Windows 10\\OneDrive - Xcel Financial Services LLC\\Shared\\Logs\\TAX LOG 2025 Live.csv", "pass1": {"created_clients": 5, "created_returns": 2, "errors": 326, "review": 604, "success": 321, "updated_clients": 41, "updated_returns": 275}, "pass2": {"created_clients": 0, "created_returns": 1, "errors": 325, "review": 604, "success": 322, "updated_clients": 4, "updated_returns": 7}, "pass2_created_clients": 0, "pass2_created_returns": 1, "pass2_new_returns": [{"client_id": 2441, "client_status": "LOG OUT", "created_at": "2026-08-12T22:50:26+00:00", "first_name": "ANDRES", "id": 2843, "last_name": "PEREZ & GARCIA VILLAREAL", "log_number": "141", "tax_year": 2025}], "pass2_return_diagnosis": [{"class": "no_prior_match_on_log_year \u2014 pass1 missed or assigned log during upsert; or non-determinism in _upsert_return / matcher", "prior_same_log_year": [], "return": {"client_id": 2441, "client_status": "LOG OUT", "created_at": "2026-08-12T22:50:26+00:00", "first_name": "ANDRES", "id": 2843, "last_name": "PEREZ & GARCIA VILLAREAL", "log_number": "141", "tax_year": 2025}}], "returns_after_pass1": 1609, "returns_after_pass2": 1610, "returns_before": 1607, "returns_idempotent": false, "throwaway": "T:\\audit\\tmp\\a4_idempotency_throwaway.sqlite"}`

## Scope

- Live TaxOps is read-only for guards I1–I6.
- I7 writes only to `T:\audit\tmp\a4_idempotency_throwaway.sqlite`.
- Violations are hard alerts — separate from A2/A3 findings.
- Status semantics (Amendment 2 C3): `PASS` = original condition held; `MODIFIED` = original failed but documented weaker condition held; `FAIL` = neither held.


---

## 6. Operator worklist (A5)

Workbook: `T:\audit\investigation\A5-worklist.xlsx`

| Sheet | Rows | Purpose |
|---|---:|---|
| Priority | 200 | Dups, L0 gaps, collisions, merge, prefill, spouse, needs-human (order via R0, not sheet sort) |
| Phantoms | 80 | TaxOps-only / unmatched (capped) |
| Workflow | 80 | Prepared↔logged process gaps (capped) |

### Priority sheet — type mix

- `SPOUSE_STORE_DIVERGENCE`: 63
- `L0_DRAKE_ONLY`: 35
- `SPOUSE_AMBIGUOUS`: 28
- `MISSING_IN_TAXOPS`: 20
- `LOG_NUMBER_COLLISION`: 19
- `DUPLICATE_CLIENT`: 17
- `MALFORMED_LOG_NUMBER`: 9
- `NEEDS_HUMAN`: 8
- `MERGE_UNTRACEABLE`: 1

### Priority samples (first 25)

| Type | Entity | Confidence | Action |
|---|---|---|---|
| `DUPLICATE_CLIENT` | `ALVARADO\|OSCAR` | high | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `D R E AND ASSOCIATES\|\|\|DRE AND ASSOCIATES\|` | medium | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `GIMENEZ GARCIA\|ENRIQUE` | medium | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `HERNANDEZ\|ABEL` | high | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `HERNANDEZ\|ISMAEL` | high | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `LUNA\|ESTEBAN` | high | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `MIAMAR FUTURE LLC\|` | medium | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `NUNO\|JUAN` | high | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `ORMA SERVICES INC\|` | medium | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `PADILLA GARCIA\|ABEL` | medium | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `SANDOVAL AUTO SERVICE TOW\|` | medium | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `SOLOMON\|LAUREN` | high | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `TASHAYOD\|ALEX` | high | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `VALDEZ\|SANDRA` | high | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `ssn_asym\|\|DREANDASSOCIATES\|` | high | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `ssn_asym\|\|MIAMARFUTURELLC\|` | high | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `DUPLICATE_CLIENT` | `ssn_asym\|\|ORMASERVICESINC\|` | high | Review merge keep/discard; favor SSN-bearing row; check filetrack/log# |
| `MERGE_UNTRACEABLE` | `global` | high | Accept partial trail (audit_log keep/discard); cannot prove all Jul1 a |
| `LOG_NUMBER_COLLISION` | `1038` | medium | Same bare log claimed by >1 taxpayer on TAXPAYER.csv — resolve before  |
| `LOG_NUMBER_COLLISION` | `1075` | medium | Same bare log claimed by >1 taxpayer on TAXPAYER.csv — resolve before  |
| `LOG_NUMBER_COLLISION` | `1095` | medium | Same bare log claimed by >1 taxpayer on TAXPAYER.csv — resolve before  |
| `LOG_NUMBER_COLLISION` | `1103` | medium | Same bare log claimed by >1 taxpayer on TAXPAYER.csv — resolve before  |
| `LOG_NUMBER_COLLISION` | `141` | medium | Same bare log claimed by >1 taxpayer on TAXPAYER.csv — resolve before  |
| `LOG_NUMBER_COLLISION` | `203` | medium | Same bare log claimed by >1 taxpayer on TAXPAYER.csv — resolve before  |
| `LOG_NUMBER_COLLISION` | `206` | medium | Same bare log claimed by >1 taxpayer on TAXPAYER.csv — resolve before  |

---

## 7. Recommendations (no TaxOps writes performed)

Follow `R0-remediation-plan.md`. Short form:

1. **Wave 0** — full-width `TAXPAYER.csv` re-export; answer Invoice+Status co-export.
2. **Wave 1** — collisions/malformed in Drake/Log; confirm I7 PASS after bare-141.
3. **Wave 2A** — `client_merge_history` before any merge (blocker).
4. **Wave 2B** — 6–7 Group A/B merges only; Group C FALSE_POSITIVE after ITIN check.
5. **Do not mass-delete Jul1-dated clients** — provenance rewrite, not duplicate-insert.
6. Spouse store decision before reconciling 228 spouse findings.
7. `client_external_ids` mint blocked on Wave 0.2 Drake custom-field answer.

---

## 8. Artifact index

| File | Role |
|---|---|
| `✓` `FULL-REPORT.md` | This consolidated report |
| `✓` `R0-remediation-plan.md` | Remediation waves 0–5 (plan only) |
| `✓` `A0-baseline.md` | Baseline lock + invoice Amendment 1 |
| `✓` `A1-ladder.md` | L0–L5 identity ladder |
| `✓` `A2-delta-report.md` | Fingerprints + disposition delta |
| `✓` `A3-checks.md` | F1–F12 checks |
| `✓` `A4-invariants.md` | Seven hard guards |
| `✓` `A5-report.md` | Operator summary |
| `✓` `A5-worklist.xlsx` | Actionable worklist |
| `✓` `C5-log-import-triage.md` | Tax Log import error/review taxonomy |
| `✓` `TAXPAYER.csv` | Canonical invoice export |
| `✓` `I0-inventory.md` | Investigation inventory |
| `✓` `I1-provenance-map.md` | Field provenance |
| `✓` `I2-ingestion-paths.md` | Ingestion + Jul1 |
| `✓` `I3-linkage-findings.md` | Linkage / Jul31 Venn |
| `✓` `I4-synthesis.md` | Failure taxonomy (partially superseded) |

| `audit_disposition.sqlite` | Persistent dispositions (`T:\audit\audit_disposition.sqlite`) |
| `audit_20260810.sqlite` | Per-run ladder / entity_link |
| `baseline_memory.json` | Cross-run count memory |
