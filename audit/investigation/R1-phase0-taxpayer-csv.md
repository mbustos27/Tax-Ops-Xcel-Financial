# R1 Phase 0 — `TAXPAYER.csv` calibration (re-export)

_Generated: 2026-08-13T21:55:54Z_

- Drake source: `F:\DRAKE25\DT\5\4F24A262\Documents\TAXPAYER.csv`
- Canonical: `T:\audit\investigation\exports\TAXPAYER.csv`
- sha256: `33bcda025ef8a4db1e7b189da13dbebbd7c7ea170d2f9c5f5220a6ca6cf529c8`
- Meta: `TY2025 SPOUSE 11:43:10 14:50:24` / `As of 08-13-2026`
- Delta vs prior ingest: **added `Street Address`** (was 16 cols / sha 765bbb7b…; earlier pass added ZIP + Filing Status)

## Columns (17, ragged=1)

`Taxpayer First Name`, `Taxpayer Last Name`, `Taxpayer Date of Birth`, `Taxpayer Daytime Phone`, `Taxpayer Email Address`, `Spouse Name`, `Spouse Daytime Phone`, `Spouse Date of Birth`, `Dependent First Name`, `Dependent Last Name`, `Invoice Number`, `State`, `City`, `County`, `Filing Status`, `ZIP Code`, `Street Address`

Data rows: **1328** · widths: `{17: 1327, 1: 1}`

## Phase 0 gate check

| Gate | Status | Evidence |
|---|---|---|
| 0.1 City + State | **PASS** | city=1327 state=1327 |
| 0.1 ZIP | **PASS** | zip=1327 lens={5: 1303, 9: 24} |
| 0.1 Street | **PASS** | street=1327 (column present) |
| 0.2 Filing Status | **PASS** | fs=1217 counts={'1': 404, '4': 327, '2': 481, '3': 4, '5': 1} |
| 0.2 Ragged rows | **FAIL** | ragged_n=1 |

**Address schema fully unblocked (city+state+zip+street):** YES

## Invoice fill (strict `^25\d{4}$`)

| Strict rows | Bad | Empty | Distinct after collapse |
|---:|---:|---:|---:|
| 1072 | 30 | 226 | 824 |

Multi-row invoices: 158 (same-person 139, dep-driven 139, diff-person **19**) · bare collisions after collapse: **0**

### Malformed top

| Invoice | n |
|---|---:|
| `250` | 3 |
| `25141` | 2 |
| `25549` | 2 |
| `25537` | 2 |
| `2501177` | 2 |
| `25488` | 2 |
| `25` | 1 |
| `251` | 1 |
| `25063` | 1 |
| `2500892` | 1 |
| `25037` | 1 |
| `2500786` | 1 |

**`25141` count:** 2 — never lenient-parse.

## Field fills

| Field | n |
|---|---:|
| `city` | 1327 |
| `state` | 1327 |
| `county` | 1327 |
| `zip` | 1327 |
| `fs` | 1217 |
| `street` | 1327 |
| `email` | 1011 |
| `phone` | 1155 |
| `dob` | 1217 |
| `spouse` | 492 |
| `spouse_dob` | 493 |
| `spouse_phone` | 78 |
| `dep` | 1328 |

## Implication

- **Phase 0.1 complete** on this file: Street + City + State + ZIP + County all filled (~1327/1328).
- **Phase 0.2 Filing Status** present (1217); one junk trailer only (ignore).
- Address schema (`address_street/city/state/zip/county`) may be designed against this shape — Phase 2A unblocked.
- Collapse dependent rows before any TaxOps join (158 multi-invoice row groups; 19 true shared-invoice).
- Spouse email / split first-last still richer on `TAXPAYERspouseaddressstatus.csv`; this file has `Spouse Name` + DOB/phone only.

Machine: `T:\audit\investigation\R1-phase0-taxpayer-csv.json`
