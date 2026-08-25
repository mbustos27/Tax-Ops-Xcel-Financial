# Lucy gate — software complete

_Generated: 2026-08-12 · after packet Lucy-fixes + A1–A4 remmeasure (`lucy-gate` / `lucy-gate-remeasure`)_

## Gate

**Office:** execute [`W-office-packet.md`](W-office-packet.md) (CSV + canvas).  
**Software:** no further TaxOps writes required for this gate.

| Packet | n |
|---|---:|
| MOVE (checklist) | 27 |
| MOVE (141 only in §1) | 1 |
| KEEP | 2 |
| CLAIM | 1 |
| MINT | 2 |
| REVIEW | 7 |
| `do_after` sequenced | 3 |

Hold: Tax Log `genuine_reuse` split.

## Remeasure (canonical Log cut=772)

| Metric | Value |
|---|---:|
| A1 L0 Drake↔TaxOps | 721 |
| A1 three-way | **571** |
| A1 L1 | 387 |
| A1 L5 | 35 |
| A2 NEW / REGRESSED | **0 / 0** |
| A3 alarming | none |
| A4 | 5 PASS / 1 MODIFIED (I6 — TaxOps writes this wave) / **I7 FAIL** |

I7 clears when bare `141` (+ collision packet) is fixed in Drake and Tax Log import re-runs clean.

## Software waves (done)

| Wave | Status |
|---|---|
| 0 key space | Done |
| 1 office packet | **Ready for Lucy** (I7 still FAIL) |
| 2A merge trail | Done |
| 2B merges | Done (7) |
| 3 prefill | Done (Group C FP) |
| 4 spouse store + contam | Done (personnel mismatch **0**) |
| 5 spouse NEEDS_HUMAN | Lucy OPEN **0** |

## After Lucy finishes the packet

1. Re-export `TAXPAYER.csv` / invoice link export.
2. Re-run A1 → A3 → A4 (expect I7 PASS, collisions ↓).
3. Re-measure F8/F9 phantom + workflow lanes.
4. Only then consider Tax Log `genuine_reuse` split.

Sources: `W-office-packet.md`, `A1-ladder.md`, `A2-delta-report.md`, `A3-checks.md`, `A4-invariants.md`, `FULL-REPORT.md`.
