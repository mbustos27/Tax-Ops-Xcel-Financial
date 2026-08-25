# C5 — Tax Log import error triage

_Generated: 2026-08-10T23:36:08Z_

Findings only. Throwaway DB: `T:\audit\tmp\c5_import_triage_throwaway.sqlite`. Live TaxOps was not written.

## Headline (pass-1 style import on throwaway)

- CSV: `C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC\Shared\Logs\TAX LOG 2025 Live.csv`
- `row_count` (importer): **2005**
- success=319, errors=322, review=610
- created_clients=5, updated_clients=41, created_returns=2, updated_returns=273

## XLSX parse sanity (Individuals sheet)

- Path: `C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC\Shared\Logs\TAX LOG 2025 Live.xlsx`
- `LOG_DATA_START_ROW`=6
- Named rows (last or first non-blank): **1252**
- Rows with log cell but blank name (spacer/subtotal candidates): **753**
- Importer CSV `row_count` vs named xlsx: 2005 vs 1252 (CSV is the import path; xlsx is audit L0 source)

## Error taxonomy (322-class)

Total ERROR import_rows: **322**

| Bucket | Count |
|---|---:|
| `db_constraint` | 321 |
| `missing_last_or_first_name` | 1 |

### `db_constraint` (321)

- Rows with parseable log cell: 321; of those in L0 invoice bare set: **235**
- Samples (up to 10):

  - row=6 log=`1` name=`CEST LA VIE APPAERL INC, ` err=`UNIQUE constraint failed: returns.log_number, returns.tax_year`
  - row=18 log=`149` name=`DORIA, RUDY J DORIA` err=`UNIQUE constraint failed: returns.log_number, returns.tax_year`
  - row=22 log=`153` name=`DEAMER, DAVID & RISE` err=`UNIQUE constraint failed: returns.log_number, returns.tax_year`
  - row=24 log=`155` name=`MANZANARES, BLANCA L` err=`UNIQUE constraint failed: returns.log_number, returns.tax_year`
  - row=27 log=`158` name=`HIRSCH, LAWRENCE M & IMELDA` err=`UNIQUE constraint failed: returns.log_number, returns.tax_year`
  - row=28 log=`159` name=`RAMON RODRIGUEZ, CRESCENCIANO` err=`UNIQUE constraint failed: returns.log_number, returns.tax_year`
  - row=33 log=`164` name=`RESENDIZ SANCHEZ, PAOLA` err=`UNIQUE constraint failed: returns.log_number, returns.tax_year`
  - row=35 log=`180` name=`MESA, MARIA` err=`UNIQUE constraint failed: returns.log_number, returns.tax_year`
  - row=60 log=`205` name=`MACIEL, NORBERTO & LETICIA` err=`UNIQUE constraint failed: returns.log_number, returns.tax_year`
  - row=61 log=`206` name=`SERRATO, ROCHELLE M` err=`UNIQUE constraint failed: returns.log_number, returns.tax_year`

### `missing_last_or_first_name` (1)

- Rows with parseable log cell: 1; of those in L0 invoice bare set: **0**
- Samples (up to 10):

  - row=293 log=`509` name=`, ` err=`Missing required values: LAST (or TAX PAYER NAME (S) LAST) and/or FIRST`

## Review taxonomy (610-class)

Total review_queue rows: **610**

| Reason | Count |
|---|---:|
| `AMBIGUOUS_MATCH` | 583 |
| `MANUAL_FUZZY_NAME_REVIEW` | 15 |
| `MANUAL_LOG_VS_NAME_MEDIUM` | 12 |

### `AMBIGUOUS_MATCH` (583)

- Rows with parseable log cell: 582; of those already present as L0 invoice bare keys: **398**
- If L0 invoice keys were written back into the Log, rows already carrying a log# would **not** newly gain a key — L0 helps TaxOps↔Drake, not Log→matcher when the Log row already has LOG 2025. Review is mostly matcher ambiguity.
- Samples (up to 10):

  - row=7 log=`2` name=`BRAVO, BYRON M`
  - row=8 log=`3` name=`CHEANG, SARAH`
  - row=9 log=`4` name=`PEREZ, NORA L`
  - row=13 log=`144` name=`RECENDEZ , FRANCISCO D & DAISY CASAS`
  - row=15 log=`146` name=`BURGOIN, LOUIE & FRANCES A`
  - row=17 log=`148` name=`TAREEN, ZULQARNIAN & FOUZIA`
  - row=19 log=`150` name=`SANDOVAL, GERARDO V`
  - row=20 log=`151` name=`GARCIA, FRANCISCO`
  - row=21 log=`152` name=`CARRANZA, MAURICIO A`
  - row=23 log=`154` name=`MORALES, IRMA`

### `MANUAL_FUZZY_NAME_REVIEW` (15)

- Rows with parseable log cell: 15; of those already present as L0 invoice bare keys: **9**
- If L0 invoice keys were written back into the Log, rows already carrying a log# would **not** newly gain a key — L0 helps TaxOps↔Drake, not Log→matcher when the Log row already has LOG 2025. Review is mostly matcher ambiguity.
- Samples (up to 10):

  - row=122 log=`280` name=`BARRIOS, SELVIN`
  - row=241 log=`409` name=`LOPEZ-PULIDO & YANEZ, ALVARO & MARIA`
  - row=242 log=`410` name=`D PRISA, `
  - row=245 log=`413` name=`MYCO IMPORTS, `
  - row=372 log=`599` name=`COVARRUBIAS & FERNANDEZ, MARIO & MONICA`
  - row=527 log=`780` name=`RODRIGUEZ GUERRERO, PATRICK T & JOSE`
  - row=605 log=`858` name=`FRIAZ & CERVANTES, NOEL R & CHRISTINA`
  - row=745 log=`998` name=`ARCINIEGA GOMEZ, CAROLINA`
  - row=876 log=`105` name=`MARINEZ, MARIA`
  - row=953 log=`287` name=`AZAMAR, MISAEL`

### `MANUAL_LOG_VS_NAME_MEDIUM` (12)

- Rows with parseable log cell: 12; of those already present as L0 invoice bare keys: **11**
- If L0 invoice keys were written back into the Log, rows already carrying a log# would **not** newly gain a key — L0 helps TaxOps↔Drake, not Log→matcher when the Log row already has LOG 2025. Review is mostly matcher ambiguity.
- Samples (up to 10):

  - row=45 log=`190` name=`GARCIA & MORALES, ALEXIS & BERENICE`
  - row=120 log=`278` name=`MONARREZ & CERVANTES, BEATRIZ & JUAN`
  - row=147 log=`308` name=`MC GILL, ROSEMARIE`
  - row=161 log=`329` name=`TINOCO & ROGERS, LILIANA & BRANDON`
  - row=162 log=`330` name=`WARNKE & SARDINAS, KARINA & MARIO`
  - row=267 log=`473` name=`MAZARIEGOS, MARILANDA I`
  - row=295 log=`516` name=`DOMINGUEZ URQUIETA, JUAN J & CLAUDIA JAIMES C`
  - row=320 log=`541` name=`ESPNOZA, MARTIZA`
  - row=354 log=`579` name=`GARCIA & CORONA, NICANOR & KRISTAL`
  - row=369 log=`596` name=`PEREZ MENDEZ, GENOVEVA `

## Would writing invoice numbers into the Log help?

- Errors with a log cell already: 322/322; overlap with Drake L0 bare keys: 235.
- Review with a log cell already: 609/610; overlap with Drake L0 bare keys: 418.
- **Argument:** Writing Drake invoice→Log is redundant for rows that already have `LOG 2025`. The import failure mode is name-match review / missing names / missing log#, not absence of Drake's season-prefixed form. Prefer fixing matcher/review reasons over Log rewrite for the 610 band.

## Cross-ref: LOGGED_NOT_PREPARED / PREPARED_NOT_LOGGED

- `LOGGED_NOT_PREPARED` disposition rows (all statuses queried): **451**
- `PREPARED_NOT_LOGGED` disposition rows (all statuses queried): **245**
- These A2 findings are computed from invoice L0 ↔ TaxOps ↔ Log set differences, not from `import_rows` ERROR/REVIEW. Import failures on the CSV path can create TaxOps under-coverage that *looks* like LOGGED_NOT_PREPARED (Log has name+log#, TaxOps never got the return). Quantify: review+error rows whose bare log is absent from TaxOps returns are the import-downstream slice.

- Distinct bare logs on ERROR/REVIEW rows still absent from TaxOps TY2025 after this import pass: **115** (sample: ['5', '12', '24', '30', '32', '33', '37', '59', '60', '61', '64', '66', '70', '71', '75'])
- Conclusion: a material fraction of Log↔TaxOps gaps are **import-path** (review/error), not solely office process gaps. Treat LOGGED_NOT_PREPARED as mixed: process + import triage.

## Scope

- No importer repairs in this pass.
- No live TaxOps writes.
