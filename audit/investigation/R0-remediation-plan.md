# R0 — Remediation plan (from A5 Priority worklist)
_Drafted: 2026-08-11. Input: A0–A5 + I0–I4 audit package, run `a3-20260810T233346`._
_Status: plan only. No TaxOps writes proposed until Wave 2, and none without the trail built in Wave 2A._

---

## The ordering argument

The Priority sheet lists 200 rows across 10 finding types. Working them in listed order is wrong, because three dependencies cut across the list:

1. **~1,251 of the 1,696 open findings are computed off the L0 bare-log key space.** `PHANTOM_IN_TAXOPS` (555), `LOGGED_NOT_PREPARED` (451), and `PREPARED_NOT_LOGGED` (245) are all invoice-key proxies (A3 F8/F9 note). The key space itself is known-defective right now: 230 ragged export rows dropped their Invoice Number, 19 bare logs have multiple claimants, and 9 are malformed. Any human time spent on those three types before the key space is clean is time spent on findings that will move underneath you.

2. **`MERGE_UNTRACEABLE` is a prerequisite, not a low-priority singleton.** It has a count of 1, so it sorts to the bottom — but it says merge_ops DELETEs the discard client with no identity snapshot. You are about to perform 17 merges. Doing them before the trail exists makes 17 irreversible, unauditable changes to client identity in a system of record for tax filings.

3. **The 56 spouse findings are one design decision, not 56 fixes.** `SPOUSE_STORE_DIVERGENCE` (28 on Priority, 200 open) and `SPOUSE_AMBIGUOUS` (28) come from three parallel stores. Reconciling rows before choosing the canonical store guarantees rework.

So: fix the key space → build the merge trail → merge → decide the spouse store → then human review. Detail below.

---

## Wave 0 — Re-lock the key space (no writes anywhere)

**Status: DONE 2026-08-11** (`run_id=4`, operator=`wave0`, disposition label=`wave0-link5`).

**Why first:** cheapest action with the largest blast radius on finding counts.

| Task | Detail |
|---|---|
| 0.1 | Adopt Wave 0 `INVOICE NUMBER LINK` export (5 cols: Last, First, Invoice, DOB, Address). Invoice is col 3 — blank Invoice is an explicit empty cell, not a ragged drop. Canonical path: `T:\audit\investigation\TAXPAYER.csv` (Desktop 2026-08-11). |
| 0.2 | Answer the A0 open question: **can the Drake report writer emit Invoice Number and Status in one export?** If yes, CSM stops being load-bearing for linkage and the 40-char joint-name ceiling stops mattering for identity entirely. *(Still open.)* |
| 0.3 | Re-run `A0 → A1 → A2 → A3` against the new export. Do not suppress `BASELINE_DRIFT` — read it. **Done.** |

**Observed (Wave 0 lock):** layout=`link_5`; data=1063 (1062 taxpayers + Totals footer); full-width=1062; ragged=1 (= Drake `Totals (1062)` footer — explained); blank Invoice=196; L0-eligible=815; malformed=11; collisions=19. Ladder: Drake∩TaxOps **721** (was 719); three-way **599** (was 597); L5=28. A2 delta: NEW=0 / RESOLVED=0 / REGRESSED=0 (fingerprint stable). A3: no ALARMING.

**Acceptance:** ragged rows = 0 or individually explained ✓; full-width coverage ≥ 99% ✓; A1 ladder re-run with new L0 count ✓; `layout=link_5` on A0 ✓.

**Also fix here:** the `FULL-REPORT.md` compile step. Its executive summary reports `7/7 PASS`, three-way 586, L0 708, L1 399, L5 29 — while the sections it embeds report 6 PASS / 1 FAIL, 597, 719, 389, 28. It is pulling the A3 F12 *baseline* column instead of the current run. Left alone, the report's front page asserts the importer is proven idempotent when the invariant that tests that failed.

---

## Wave 1 — Log-number integrity (Drake + Tax Log data, still no TaxOps writes)

**Status: DEFERRED 2026-08-11** — office cleanup not required as a gate. L0 already excludes collisions/malformed; TaxOps rejects duplicate logs. Triage retained at `W1-log-integrity-triage.md` for opportunistic fixes (prefer bare `141` if touched). Proceed to Wave 2A.

Covers `LOG_NUMBER_COLLISION` (19) and `MALFORMED_LOG_NUMBER` (11 on Wave 0 export). These are office data-entry fixes, not code fixes.

### Drake collisions — classified

| Class | n | Action |
|---|---:|---|
| `b_genuine_reuse` | 16 | Issue new logs to non-keepers (TaxOps name used as keeper **only** when it overlaps Drake/Log tokens) |
| `c_family` | 2 | `426` HERNANDEZ AMADO/LYDIA; `589` SANTIAGO DENISE/EUNICE — decide household rule |
| `c_family_plus_other` | 1 | `1095` GUTIERREZ JOAO/MERCEDES + RODRIGUEZ JAVIER |
| Format variant (flag) | 1 | Bare `141` also has `250141`/`25141` pad typo under the reuse |

**I7 blocker:** bare `141` — Drake GOYTIA / PEREZ@`25141` / VILLAREAL; Log has PEREZ&GARCIA VILLAREAL + CHAN THY; TaxOps holds VERDUZCO BALTAZAR (name **does not overlap** — do not auto-keep).

### Malformed (11)

4 truncated (`25`/`250`), 4 prior/non-season prefix, 3 out-of-range bare (`1355`, `1685`, `7598`).

### Tax Log internal repeats — measured

XCEL 2025 named rows: **1251** → **1041** distinct bare → **206** repeated keys. Of those: **204 genuine_reuse** (412 rows, almost all exact pairs of unrelated names) + **2 family_same_surname**. `(log_number, tax_year)` is weak as an L0 key on the Log side; TaxOps stays clean only because `ux_returns_log_year` rejects collisions (feeds C5's 321 UNIQUE failures).

**Acceptance:** zero Drake collisions on re-export — **BLOCKED** (office); Log repeats classified — **DONE**; A4 I7 PASS — **BLOCKED** on `141`.

---

## Wave 2A — Build the merge trail (code, before any merge)

**Status: DONE 2026-08-11** — `client_merge_history` (schema v25); unit tests + throwaway-copy dry-run reconstruct discarded client. **No production merges yet.** Table appears on next TaxOps startup (`init_db` / `_migrate_existing_tables`).

**Blocker for Wave 2B. Do not skip.**

Today: `merge_ops.py` DELETEs the discarded client. `audit_log` holds 86 of 89 `api_merge_clients` calls with keep_id/discard_id, but no snapshot of the discarded identity — so "resolved by merge" and "silently deleted" are indistinguishable after the fact. That is exactly why Jul1 attrition (368 → 191) can only be *partially* attributed.

Built `client_merge_history`, written in the **same transaction** as the merge:

- full JSON snapshot of the discarded client row (all columns, pre-delete)
- keep_id, discard_id, operator, timestamp
- list of `returns` pre-merge rows + per-return actions (`repointed` / merge survivor)
- pre-merge snapshots of `status_events` / `filetrack_status_history` for discard returns
- reason code + free-text note

Wired from `api_merge_clients`, bulk, and `api_audit_merge_client` with session operator. A2 `assess_merge_trail` → `MERGE_TRAIL_COMPLETE` when the table + snapshot columns exist.

**Acceptance:** a dry-run merge on a throwaway copy produces a history row from which the discarded client is fully reconstructable. ✓ (`_w2a_dry_run_merge.py`, `test_merge_history_reconstructs_discarded_client`). Then, and only then, Wave 2B.


---

## Wave 2B — Duplicate clients (**6 merges, not 17**)

**Status: DONE 2026-08-11** — 7 merges applied on live TaxOps with `client_merge_history` (ids 1–7). Clients 1527→1520; returns 1615→1606 (−9 from same-year consolidations). Results: `W2B-merge-results.json`.

| # | Key | keep → discard | Group |
|---|---|---|---|
| 1 | ORMA SERVICES INC | 754 → 8 | A |
| 2 | MIAMAR FUTURE LLC | 654 → 32 | A |
| 3 | KLEAN SOLAR SOLUTIONS LLC | 517 → 854 | A |
| 4 | D R E AND ASSOCIATES | 220 → 762 | A7 |
| 5 | SANDOVAL AUTO SERVICE TOW | 1780 → 107 | B |
| 6 | PADILLA GARCIA\|ABEL CLAUDIA E | 1801 → 479 | B |
| 7 | GIMENEZ GARCIA\|ENRIQUE P LUVIA | 1803 → 543 | B |

**Revised against `I2-stats.json` / `I3-stats.json`.** The 14 normalized-name buckets split cleanly by SSN pattern, and only one of the three groups is a merge queue:

| Group | n | Pattern | Action |
|---|---:|---|---|
| **A — asymmetric SSN** | 3 | one row has last4, twin is NULL | **Merge**, keep SSN-bearing |
| **B — both NULL** | 3 | neither row has last4 | **Merge** after human confirms |
| **C — both have last4, different** | 8 | two distinct SSNs | **Do not merge** — see below |

**Group A (merge, keep direction unambiguous):**

| Key | Jul1 row (discard) | Older row (keep) | Keep's last4 |
|---|---|---|---|
| `ORMA SERVICES INC` | 8 | **754** | 2100 |
| `MIAMAR FUTURE LLC` | 32 | **654** | 4898 |
| `KLEAN SOLAR SOLUTIONS LLC` | 854 | **517** | 7813 |

**Group B (merge, but confirm first — no SSN on either side to arbitrate):**

| Key | ids | Note |
|---|---|---|
| `SANDOVAL AUTO SERVICE TOW` | 107, 1780 | entity |
| `PADILLA GARCIA\|ABEL CLAUDIA E` | 479, 1801 | joint |
| `GIMENEZ GARCIA\|ENRIQUE P LUVIA` | 543, 1803 | joint |

Add `D R E AND ASSOCIATES` (220, 762) as a seventh — it only surfaces under A3's space-stripped key, not I3's, so it's a spacing variant of one entity.

These six are **exactly** the 6 remaining Jul1-dated clients with an exact-name pre-Jul1 counterpart (`I2-stats.json` `exact_name_match_to_pre_jul1: 6`, down from 159 on the Jul31 snapshot). Every left-hand id is Jul1-stamped; every right-hand id was created Apr 21/22/28. The Jul1 twin cleanup is nearly finished — this is the tail of it.

### Group C: probably not duplicates at all

| Key | ids | last4s |
|---|---|---|
| `HERNANDEZ\|ISMAEL` | 659, 2275 | 2720 / 3403 |
| `NUNO\|JUAN` | 731, 2276 | 8143 / 0775 |
| `ALVARADO\|OSCAR` | 1215, 2272 | 3038 / 2401 |
| `LUNA\|ESTEBAN` | 1278, 2393 | 8818 / 6722 |
| `VALDEZ\|SANDRA` | 1344, 2404 | 6184 / 9983 |
| `HERNANDEZ\|ABEL` | 1440, 2273 | 5426 / 2665 |
| `TASHAYOD\|ALEX` | 1506, 2329 | 4759 / 0956 |
| `SOLOMON\|LAUREN` | 1709, 2429 | 9376 / 9179 |

Both rows carry a populated, **different** SSN last-4. Two distinct last-4s is strong evidence of two distinct taxpayers who happen to share a common name — merging them would collapse two real clients into one filing record. This is also why A4 invariant I3 PASSes: the identity triple `(last, first, last4)` is not violated.

Every second id (2272–2429) falls inside the Aug 7 `--link-clients` burst range (2195+). So F10's "8 exact-name older twins" are **name collisions the prefill matcher correctly declined to merge**, not stub duplicates — it saw the SSN disagreement and minted a new client, which is the right behavior.

**Disposition:** `FALSE_POSITIVE`, with one exception to check by hand first — an **ITIN → SSN transition** would produce exactly this signature for one real person. Common enough in this client base to be worth a look before closing all eight. Check whether the older row's returns stop when the newer row's begin.

**Acceptance after each Group A/B merge:** I1, I3, I4, I5 still PASS; `returns` total unchanged (reassigned, not lost); re-run A2 shows that pair's `DUPLICATE_CLIENT` fingerprint RESOLVED. Fingerprints exclude client_ids by design, so a genuine merge produces a genuine RESOLVED rather than churn — that's your verification signal.

---

## Wave 3 — Prefill stubs (1 finding, 244 rows)

**Status: DONE 2026-08-11.** Group C ITIN check: all 8 newer ids have **zero returns** → not ITIN→SSN handoffs → `FALSE_POSITIVE` (8 `DUPLICATE_CLIENT` + `PREFILL_STUB_BURST`). Worklist: burst downgraded to informational. Guard: `--link-clients` alone is dry-run; mint only with `--link-clients --apply`.

The Aug 7 `--link-clients` burst created 244 clients in one timestamp. `I2-stats.json` shows **244/244 with an SSN** and 244/244 prefill-linked (A3's "243" undercounts by one). The 8 exact-name older twins are Group C above — all SSN-disagreeing, all correctly declined. **On this evidence the burst did the right thing and needs no cleanup.**

Batch timing, for the record: prefill import batch id=4 ran `22:04:40`, the client burst stamped `22:27:47`, batch id=5 re-ran `23:38:12`. The burst sits between two prefill imports of the same 1,379-row file.

**Mechanism fix so it doesn't recur:** `--link-clients` alone prints intended creates and writes nothing; `--link-clients --apply` mints. (Previously `--link-clients` without `--apply` still called `link_prefill_clients` and committed.)

---

## Wave 4 — Spouse store decision (design)

**Status: DONE 2026-08-11.** Decision: `W4-spouse-store-decision.md`. Fold applied: **354** clients-only spouse names → `spouses` (`source=wave4_clients_fold`); table 166→**520**. Prefill `_fill_spouse_if_empty` now upserts `spouses` first. Dead `clients.spouse_dob/cell/work/email` columns left in place (drop deferred). `SPOUSE_AMBIGUOUS` remains human.

56 Priority rows, 228 open findings, three stores: `clients.spouse_*` (406 clients populated, 354 of them *only* there), `spouses` (166 rows, 114 only there), `drake_household_prefill` (1,302 rows, intentionally never written through).

**The migration is far cheaper than the finding count implies.** Per `I1-stats.json`, four of the six `clients.spouse_*` columns are **100% empty** — `spouse_dob`, `spouse_cell`, `spouse_work_phone`, `spouse_email` all have fill_rate 0.0. Only `spouse_last_name` (405) and `spouse_first_name` (375) carry anything. So folding the clients-side store into `spouses` means moving two name columns, not reconciling six attributes across 406 rows. Drop the four dead columns in the same migration.

Decide first, reconcile second. Suggested shape:

- `spouses` becomes canonical relational store
- `clients.spouse_*` becomes deprecated / read-through, frozen against new writes
- `drake_household_prefill` stays staging-only (already the documented intent)

Then a one-time reconciliation migration folds the 354 clients-only rows into `spouses`. The 28 `SPOUSE_AMBIGUOUS` findings survive that migration — they are genuine ambiguity and stay human.

---

## Wave 5 — `NEEDS_HUMAN` (42)

**Status: QUEUE READY 2026-08-11** — staff adjudication; not code. Deliverable: `W5-needs-human-queue.md`.

All 42 open items are subtype **`spouse_unrecovered`**. After Wave 4 fold, **33/42** already have a `spouses` row — confirm and mark `RESOLVED` (`wave4_fold_confirmed`); the other **9** need Drake/intake lookup or `WONTFIX` if single. Also open: **28 `SPOUSE_AMBIGUOUS`** (genuine multi-candidate). Wave 4 follow-up: **37** `SPOUSE_STORE_DIVERGENCE` auto-`RESOLVED` where fold cured clients-only gaps (**163** still open — name disagreements / only-spouses).

Batch by subtype (the entity_key includes subtype specifically so distinct review reasons don't collapse). These need someone who knows the clients — Lucy or whoever prepared the return — not code. Time-box it.

---

## Explicitly not on this plan

| Item | Why |
|---|---|
| Jul1 cohort (191 dated / 179 stamped) | Provenance artifact, not a duplicate-insert bug. **Do not mass-delete.** A5 already lanes it as `provenance`. |
| `NAME_TRUNCATED` (72) | Informational per C6. Identity is carried by L0 invoice + L1 last4+surname; L3 truncation links = 0. |
| `PHANTOM_IN_TAXOPS` (555) | Re-measure after Wave 0. F8 buckets already show 126 closed/logout and 14 test — split those out before any human touches the queue. |
| `LOGGED_NOT_PREPARED` (451) / `PREPARED_NOT_LOGGED` (245) | Workflow lane, and C5 showed a material slice is import-path damage (115 bare logs on ERROR/REVIEW rows absent from TaxOps), not office process. Re-measure after the importer work. |
| `client_external_ids` mint | Blocked on the Wave 0.2 answer about Drake custom fields. |

---

## Schema documentation drift (fix opportunistically)

`I1-stats.json` returned `no such column` for three fields the written artifacts describe as populated:

| Documented in | Column | Reality |
|---|---|---|
| `I1-provenance-map.md` | `client_dependents.ssn_last4` | does not exist |
| `I0-inventory.md`, `I1` | `drake_prefill_links.purple_name_norm` | does not exist |
| `I0-inventory.md`, `I1` | `drake_prefill_links.match_variant` | does not exist |

The provenance map documents a schema that isn't the live one. Low severity, but anyone writing a migration off `I1-provenance-map.md` will write it against columns that aren't there. Worth a correction pass on the artifact.

Also worth noting for F1: truncation propagates further than the CSM. `client_dependents.taxpayer_name` has max length 51 with **13 rows at exactly 40**, and `spouses.taxpayer_name` has 2 at 40 — the 40-char ceiling is baked into child tables too, not just `drake_prefill_links`.

---

## Disposition hygiene (applies throughout)

- **A rule change is not a fix.** The 15 MALFORMED findings marked RESOLVED in `amend2-c1c2` disappeared because the bare-log range gate replaced `^\d{6}$` — the underlying Drake records did not change. Mark that class `FALSE_POSITIVE` or `WONTFIX` explicitly rather than letting a normalization change silently register as remediation, or your RESOLVED count stops meaning anything.
- Re-run A2/A3 with identical inputs twice after each wave. Zero NEW on the second run is your proof that fingerprints are stable and the wave's RESOLVEDs are real.
- `MALFORMED_LOG_NUMBER` keys on the **raw** invoice string on purpose. If you tighten normalize rules in Wave 1, expect that fingerprint space to churn — that's documented behavior, not a regression.

---

## Suggested sequencing

| Wave | Blocks | Gate to next |
|---|---|---|
| 0 — re-export + re-lock | everything downstream | ragged = 0, A1 re-run recorded |
| 1 — log-number integrity | I7, all L0-derived counts | I7 PASS, zero collisions |
| 2A — merge trail | 2B | reconstructable dry-run merge |
| 2B — **6–7 merges** + 8 FALSE_POSITIVE | — | invariants hold, fingerprints RESOLVED |
| 3 — prefill guard | — | Group C dispositioned; burst downgraded |
| 4 — spouse store | 228 findings | store decision written down |
| 5 — human review | — | time-boxed |
