# R1 Phase 3 — throwaway COALESCE dry-run

_Generated: 2026-08-13T22:46:45Z · run_label `r1-phase3-throwaway-2026-08-13`_

- **Live DB untouched:** `T:\taxops\taxops.db`
- Throwaway copy: `T:\audit\tmp\taxops_r1_phase3_throwaway_20260813T224645Z.sqlite`
- Export sha: `33bcda025ef8a4db1e7b189da13dbebbd7c7ea170d2f9c5f5220a6ca6cf529c8`

**Overall: PASS**

## Counts

| Metric | n |
|---|---:|
| Joinable | 707 |
| Clients touched (history rows) | 707 |
| Fields written | 5266 |
| `filing_status_drake` filled | 679 |
| Reconstruct sample OK/Fail | 5/0 |
| Overwrite bugs | 0 |

## Fields written

| Field | n |
|---|---:|
| `address_street` | 707 |
| `address_city` | 707 |
| `address_state` | 707 |
| `address_zip` | 707 |
| `address_county` | 707 |
| `address_source` | 707 |
| `taxpayer_email` | 552 |
| `spouse_dob` | 240 |
| `taxpayer_dob` | 97 |
| `taxpayer_cell` | 91 |
| `spouse_cell` | 44 |

## Acceptance

| Check | Result |
|---|---|
| `history_eq_touched` | PASS |
| `reconstruct_sample` | PASS |
| `no_overwrite` | PASS |
| `legacy_untouched` | PASS |

Next gate: **Stage C** apply v28 DDL to live (schema only), then **Stage D** live COALESCE with same skip rules.

Machine: `T:\audit\investigation\R1-phase3-throwaway-dryrun.json`
