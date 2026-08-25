# R1 Phase 1 — join quality (strict invoice)

_Generated: 2026-08-13T20:20:36Z · write rule 2: `Skip any row whose invoice does not match ^25\d{4}$ exactly — no normalization, no repair. Non-canonical -> office worklist.`_

## Headline

**`taxpayer_email` COALESCE gap = 538** on joinable set — office fill ~55 → ~593 (~11×).

| Metric | n |
|---|---:|
| Strict export rows | 885 |
| Distinct strict bares | 824 |
| Collision bares | **57** |
| **Joinable clients** | **675** (45.2% of 1493) |
| Double-write risk | **0** |

### Skip reasons

- `blank_invoice`: 206
- `collision_bare`: 118
- `no_taxops_return`: 92
- `noncanonical_invoice`: 27

### COALESCE opportunities

| Field | n |
|---|---:|
| `taxpayer_email` | 538 |
| `spouse_dob` | 208 |
| `taxpayer_dob` | 89 |
| `address` | 86 |
| `spouse_cell` | 86 |
| `spouse_first_name` | 48 |
| `spouse_last_name` | 40 |
| `spouse_email` | 9 |

## No-TaxOps cross-check (strict unique bares)

| Set | n |
|---|---:|
| Profile export, clean key, no TaxOps return | **92** |
| C5 ERROR/REVIEW bares still absent from TaxOps | 102 |
| Overlap with C5 absent | **41** |
| A2 `L0_DRAKE_ONLY` OPEN | 35 |
| Overlap with L0_DRAKE_ONLY | **34** |
| Only in profile export (not C5∪L0) | **19** |

If C5 overlap is large, these are import-path damage — fixing the importer unlocks profiles as a side effect. 92 > L0_DRAKE_ONLY (35) and A1 L5 (~35), so the profile export is picking up bares those ladders do not.

C5 overlap sample: `148`, `165`, `233`, `246`, `248`, `276`, `277`, `281`, `349`, `363`, `375`, `510`, `533`, `552`, `592`, `600`, `626`, `661`, `667`, `683`, `692`, `714`, `747`, `757`, `772`

Only-profile sample: `114`, `162`, `235`, `241`, `264`, `489`, `607`, `776`, `845`, `890`, `981`, `1108`, `1111`, `1131`, `1188`, `1195`, `1213`, `1214`, `1298`

Acceptance **PASS** (zero double-writes).

Machine: `T:\audit\investigation\R1-calibration.json`
