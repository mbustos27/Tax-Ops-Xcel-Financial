# W1 — source_row banding (re-pinned)

_Generated: 2026-08-11T23:58:30Z · cut=**772** (bare `1` restart) · read-only_

## Verdict

**`BAND_RESTART_PINNED`** at row **772** (not midpoint 670).

| Metric | Value |
|---|---|
| Pair keys | 200 |
| Cross-band @ 772 | 200 |
| Same-band @ 772 | 0 |

### Canonical row rule

1. Prefer `YR=25`
2. Then Drake/TaxOps name-token agreement
3. Then later `source_row`

Do **not** prefer non-`LOGOUT` alone — that selected TY2024 for bare `141`.

### Canonical examples

- Bare `141` → row 10 `PEREZ & GARCIA VILLAREAL, ANDRES & IRAZU` (yr=25, LOGOUT, band1)
- Bare `1` → row 6 `CEST LA VIE APPAERL INC` (yr=25, LOGOUT, band1)
- Bare `203` → row 58 `COLLAZO, MIGUEL & MAYRA` (yr=25, LOGOUT, band1)
- Bare `247` → row 102 `VILLALTA, ROSALINDA V` (yr=25, LOGOUT, band1)

**Hold** Tax Log `genuine_reuse` office split until L0 uses this canonical filter.

Machine: `T:\audit\investigation\W1-source-row-banding.json`
