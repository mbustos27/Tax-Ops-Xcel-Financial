# A5 — Operator report

_Generated: 2026-08-10T23:38:44Z_

## Delta headline (work this first)

**NEW + REGRESSED = 18** *(not total open)*

- Latest disposition run: `a3-20260810T233346` finished `2026-08-10T23:33:46Z`
- Latest run stats: `{"NEW": 18, "RECURRING": 856, "REGRESSED": 0, "RESOLVED": 0, "by_type_new": {"LOGGED_NOT_PREPARED": 13, "PHANTOM_IN_TAXOPS": 4, "SPOUSE_STORE_DIVERGENCE": 1}, "by_type_recurring": {"DUPLICATE_CLIENT": 17, "JUL1_PROVENANCE": 1, "LOGGED_NOT_PREPARED": 287, "NAME_TRUNCATED": 72, "PHANTOM_IN_TAXOPS": 323, "PREFILL_STUB_BURST": 1, "PREPARED_NOT_LOGGED": 155}, "headline_NEW_plus_REGRESSED": 18}`
- Open/ACKED in disposition DB: **1696**
- Worklist workbook: `T:\audit\investigation\A5-worklist.xlsx`
  - **Priority** sheet — dups, L0 gaps, collisions, merge, prefill (work this first)
  - **Phantoms** / **Workflow** sheets — capped secondary queues
  - `NAME_TRUNCATED` is informational (C6) — not on Priority

### Priority-queue types in collected set

- `NEEDS_HUMAN`: 42
- `L0_DRAKE_ONLY`: 35
- `SPOUSE_AMBIGUOUS`: 28
- `SPOUSE_STORE_DIVERGENCE`: 28
- `MISSING_IN_TAXOPS`: 20
- `LOG_NUMBER_COLLISION`: 19
- `DUPLICATE_CLIENT`: 17
- `MALFORMED_LOG_NUMBER`: 9
- `MERGE_UNTRACEABLE`: 1
- `PREFILL_STUB_BURST`: 1

## Baseline (A0)

Authoritative CSM: OneDrive TY2025 CLIENTS.xlsx (1159). Tax Log: 1252 named. Invoice: `T:\audit\investigation\TAXPAYER.csv` (1097 full-width / **813** L0-eligible bare; was 801 under legacy `^\d{6}$`). TaxOps path: UNC share; `AUTHORITATIVE_DB_UNRESOLVED` for server C:\ pairing. Drift status at last lock: STABLE (see `A0-baseline.md`).

See full lock: `A0-baseline.md`.

## Identity ladder (A1)

| Tier / cell | Count |
|---|---:|
| L0 three-way (bare log) | 597 |
| Drake∩TaxOps \ Log | 122 |
| Drake∩Log \ TaxOps | 61 |
| Drake only | 33 |
| L0 Drake↔TaxOps links | 719 |
| L0 Drake↔Log links | 658 |
| L0 TaxOps↔Log links | 835 |
| L1 last4+surname | 389 |
| L2 / L3 / L4 / L5 | 5 / 0 / 1 / 28 |

Jul31 name Venn (1009/126/13/7) is a different universe (CSM name match on Desktop 1155). A1 is invoice bare-log on TAXPAYER.csv — quantify export lag separately, do not absorb.

Key normalization: Drake Invoice `250141` ≡ TaxOps/Log `141`. C6: L3=0 (wired CSM↔invoice only; ineffective); NAME_TRUNCATED informational.

## Disposition memory (A2)

- DB: `T:\audit\audit_disposition.sqlite` (never truncated)
- Status mix: `{"OPEN": 1696, "RESOLVED": 15}`
- Fingerprints exclude TaxOps row ids / created_at / raw untruncated names
- Merge trail: **MERGE_PARTIAL_TRAIL** (86/89 audit_log rows have keep_id/discard_id; no discard snapshot)
- Amend2 C1/C2 delta (`amend2-c1c2`): RESOLVED 15 MALFORMED; NEW 19 COLLISION + 2 MALFORMED; REGRESSED 0

## Failure-mode checks (A3) — actual vs baseline

| Check | Deviation | Actual highlight | Baseline |
|---|---|---|---|
| F1 trunc | INFORMATIONAL | CSM ≥39: 72; prefill@40: 74 | 71 / 74 |
| F2 format | INFORMATIONAL | CSM &: 351; blank first: 151 | &:442; blank:151 |
| F3 dups | INFORMATIONAL | exact groups **8**; norm buckets **14** | 8 / 14 |
| F4 Jul1 | INFORMATIONAL | **191** dated; 179 stamp 18:19:30 | 191 live |
| F5 SSN asym | INFORMATIONAL | **3** groups | common in Jul1 cluster |
| F6 no log# | INFORMATIONAL | **25.67%** (414/1613) | ~26% |
| F7 last4 | INFORMATIONAL | CSM surplus 52; L1 last4-only **0** | 52 |
| F8 phantom | INFORMATIONAL | 324 key-proxy (not Jul31 name residual 227) | 227 |
| F9 workflow | INFORMATIONAL | invoice∉log 157; log∉invoice 398 | 88 / 141 |
| F10 prefill | INFORMATIONAL | **244** burst; 8 older twins | 244 |
| F11 spouse | INFORMATIONAL | spouses 166; hh 1302; only_clients_cols 353 | 198 findings |
| F12 key | **SUPERSEDED** | three-way **597**; no client_external_ids | I4 'no key' |

Full detail: `A3-checks.md`.

## Invariants (A4)

**6 PASS / 0 MODIFIED / 1 FAIL** this run — I6 mtime+size both held (`PASS`). I7 **FAIL**: pass2 created return id=2842 log=`141` `PEREZ & GARCIA VILLAREAL, ANDRES` (bare-141 collision claimant). Status semantics: PASS | MODIFIED | FAIL (C3).

See `A4-invariants.md`.

## Finding lanes (how to route work)

| Lane | Meaning | Examples |
|---|---|---|
| data | Identity / store corruption | DUPLICATE_CLIENT, SPOUSE_*, MISSING_IN_TAXOPS |
| workflow | Process lag between systems | PREPARED_NOT_LOGGED, LOGGED_NOT_PREPARED |
| export_lag_or_linkage | Export defect or key gap | L0_DRAKE_ONLY (NAME_TRUNCATED = informational C6) |
| provenance | Historical stamp; not a repair target | JUL1_PROVENANCE |

## Known limitations (carry-forward)

1. **Jul1 rewrite tool unknown** — space-format `2026-07-01 18:19:30` on ~179 live rows; mechanism is rewrite not insert; script/operator not identified.
2. **Can Drake hold a firm-assigned client ID?** — open; if Invoice+Status can co-export, CSM becomes less load-bearing.
3. **C:\TaxOps vs T:\ / UNC** — share samefile confirmed on workstation; server-local NSSM path pairing remains `AUTHORITATIVE_DB_UNRESOLVED`.
4. **Merge attrition** — partial trail only; resolved-vs-deleted not fully distinguishable.
5. **F8/F9 vs Jul31** — A3 uses invoice-key proxies; Jul31 used name-match residuals — not 1:1.
6. **Ragged TAXPAYER.csv** — 230 short rows drop Invoice Number (last col); export defect.

## Artifacts

- `✓` `T:\audit\investigation\A0-baseline.md`
- `✓` `T:\audit\investigation\A1-ladder.md`
- `✓` `T:\audit\investigation\A2-delta-report.md`
- `✓` `T:\audit\investigation\A3-checks.md`
- `✓` `T:\audit\investigation\A4-invariants.md`
- `✓` `T:\audit\investigation\A5-report.md`
- `✓` `T:\audit\investigation\A5-worklist.xlsx`
- `✓` `T:\audit\investigation\TAXPAYER.csv`

## Acceptance reminders

- No TaxOps repairs/merges/schema changes performed — recommendations only.
- Live DB size unchanged across A4; I7 used throwaway copy only.
- Two consecutive identical-input runs should yield stable fingerprints (re-run A2/A3 to verify zero NEW if needed).
