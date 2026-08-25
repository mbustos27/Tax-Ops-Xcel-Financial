# R1 Phase 1 — join on `TAXPAYER.csv`

_Generated: 2026-08-13T22:00:02Z · sha `33bcda025ef8a4db1e7b189da13dbebbd7c7ea170d2f9c5f5220a6ca6cf529c8` · strict `^25\d{4}$` · dep-collapse_

## Acceptance

| Check | Result |
|---|---|
| Zero double-write clients | **PASS** (dup=0) |
| Bare collisions after collapse | 0 |
| Diff-person invoices excluded | 19 |

## Population

| Metric | n |
|---|---:|
| Live non-test clients | 1493 |
| Distinct strict invoices (pre-exclude) | 824 |
| Collapsed (excl. diff-person) | 805 |
| **Joinable (1:1, not contam)** | **707** (47.4%) |
| No TaxOps bare | 94 |
| Multi-client bare | 0 |
| Multi-invoice client | 0 |
| Contam skip | 4 |

## COALESCE gaps (joinable only — Drake present, TaxOps empty)

| Field | n |
|---|---:|
| `address_street` | 707 |
| `address_city` | 707 |
| `address_state` | 707 |
| `address_zip` | 707 |
| `address_county` | 707 |
| `taxpayer_email` | 552 |
| `spouse_dob` | 240 |
| `taxpayer_dob` | 97 |
| `legacy_address_empty` | 94 |
| `taxpayer_cell` | 91 |
| `spouse_cell` | 44 |

Address_* gap counts = Drake has value (new cols empty by definition until Phase 3).
`legacy_address_empty` = Drake street present and `clients.address` empty.

Machine: `T:\audit\investigation\R1-phase1-join-taxpayer.json`
