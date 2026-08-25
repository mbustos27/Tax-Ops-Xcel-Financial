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
