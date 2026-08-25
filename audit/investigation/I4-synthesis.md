# I4 — Synthesis

Derived from I0–I3. Proposals only — no fixes executed.

---

## 1. Failure taxonomy

| # | Failure mode | Mechanism | Est. affected | Auto-detectable? | Auto-repairable? |
|---|---|---|---:|---|---|
| F1 | **Drake 40-char name truncation** | CSM export caps `Client Name` at 40; joints clip mid-token | **54** Desktop CSM @40; **74** prefill `csm_name_raw` @40; 71 audit NAME_TRUNCATED | Yes (len≥39/40) | Partial: join via purple longer name / last4+tokens; else human |
| F2 | **Name-order / joint-format divergence** | CSM `LAST, FIRST & SPOUSE` vs purple `FIRST & SPOUSE LAST` vs Log separate cols | 442 CSM `&`; purple 0 commas | Yes (format classifiers) | Mostly yes with existing audit normalizer + prefill variants |
| F3 | **Duplicate TaxOps clients (twins)** | `_upsert_client` exact-name only; no UNIQUE(name); fuzzy only if return match supplies id; historical mint pre-Jul1 | Live **8** exact groups; Jul31 census **~254** stamped↔unstamped collisions; audit DUPLICATE_CLIENT **142** | Yes (name(+last4) clustering) | Semi: merge_ops with human keep/discard; risky for filetrack logs |
| F4 | **Jul1 `created_at` rewrite** | Bulk stamp `2026-07-01 18:19:30` on ~344 pre-existing rows (not importer ISO) | **344** cluster (Jul31); **191** still Jul1-dated live | Yes (timestamp format + id-band) | Provenance restore from backup/`updated_at`/returns — needs human policy |
| F5 | **Null SSN on one twin** | One sibling has last4, other null — matcher/tie-break asymmetric | Common in Jul1 cluster (0/368 SSN on Jul31 stamp set) | Yes | Merge toward SSN-bearing row |
| F6 | **Log# missing on shells** | PENDING INTAKE / preintake `log_number NULL` | Non-trivial share of returns (fill 0.74) | Yes | Assign at intake (now does on shell reuse) |
| F7 | **Last4 collisions** | SSN/EIN last4 not unique (52 surplus in CSM 1155; 78 client keys; 130 prefill) | dozens–hundreds | Yes | Never match last4-alone (already policy) |
| F8 | **Phantom / unmatched TaxOps** | Clients/returns with no Drake/Log name match | audit PHANTOM 227; unmatched tiers in findings | Yes via three-way | Human: test clients, closed, spelling |
| F9 | **Prepared not logged / logged not prepared** | Process lag or name fail between Drake status and Log | 88 / 141 findings | Yes | Workflow, not always data bug |
| F10 | **Prefill stub proliferation** | `--link-clients` INSERT when no match (Aug7 **+244**) | 244 linked stubs | Yes (created_at burst + prefill FK) | OK if intentional; merge if duplicate of older client |
| F11 | **Spouse store divergence** | `clients.spouse_*` vs `spouses` vs `drake_household_prefill` | 198 findings | Yes | Unify store (design change) |
| F12 | **No cross-system primary key** | Nothing shared across Drake/Log/TaxOps except weak last4+name | **all** ~1.1–1.5k entities | Structural | Requires mint + backfill (below) |

---

## 2. Stable identifier proposal (proposal only)

**What exists today cannot be a single primary key:**

- Last4 fails uniqueness (F7).  
- Name fails truncation + format (F1–F2).  
- Log# is excellent **within a tax year for logged returns** but absent on shells and not in Drake CSM.  
- TaxOps `clients.id` is stable locally but invisible to Drake/Log.

**Recommended strategy:**

1. **Canonical key:** keep TaxOps `clients.id` (integer) as system of record; optionally add `clients.public_uuid` TEXT UNIQUE for export (feasibility: high — additive column via `_migrate_existing_tables`, safe default NULL then backfill).
2. **Backfill linkage table** (not overwrite sources): e.g. `client_external_ids(client_id, system, external_key, tax_year, confidence, verified_at)` with systems `drake_csm_last4+name_hash`, `tax_log_number`, `drake_prefill_link_id`.
3. **Push-back:**
   - Tax Log: add a hidden/admin column or note field for TaxOps id — **office process change**; Log is Excel today (`TAX LOG 2025 Live.xlsx`).
   - Drake: only if a custom Client field can hold TaxOps id — **unknown without Drake admin** → open question.
4. **Matching policy going forward:** LOG+year → SSN full (if ever authorized) → last4+normalized name → fuzzy ≥90 with review band 85–89 → human. Never last4 alone.
5. **`proposed_migration.sql`** already sketches `is_test`, spouse provenance — compatible; identifier column is additive and **feasible** against current data (no need to rewrite 1526 names first).

---

## 3. Open questions

1. **What process rewrote `created_at` on 2026-07-01 18:19:30?** Blocks root-cause closure for F4 (script name / operator / migration).  
2. **Where is the OneDrive CSM export with 1159 rows** (`EXPECTED_DRAKE_ROWS`)? Desktop has 1155 — blocks exact three-way baseline lock.  
3. **Can Drake store a firm-assigned client id?** Blocks push-back design.  
4. **Should Aug7 prefill stubs (244) be merged into older twins when names collide?** Blocks cleanup scope (live only 8 exact dups now).  
5. **Tax Log column layout drift** (openpyxl row1 blank; data row 6) — confirm log# column index for any automated Log write-back.  
6. **Server local `C:\TaxOps\taxops\taxops.db` vs share `T:\taxops\taxops.db`:** are they always the same file via symlink/share, or can they diverge? Blocks which snapshot is authoritative for ops.

---

## 4. Audit-spec seed (input to follow-up audit prompt)

Check / measure each:

- [ ] Recompute Jul1 cluster: space-format vs ISO; id-band ≤1753; fraction with pre-Jul1 returns (expect ~344 rewrite, not inserts).  
- [ ] Twin census: exact / normalized / fuzzy 88–95 bands; classify null-SSN vs SSN-bearing keeper.  
- [ ] CSM names with `len==40` and purple longer superstring (cross-source truncation pairs).  
- [ ] Three-way Venn refresh against locked CSM+Log+TaxOps snapshots; flag baseline drift.  
- [ ] last4 collision lists (CSM, TaxOps, prefill) — ensure no matcher uses last4 alone.  
- [ ] Duplicate `(last,first)` remaining after merge_ops; filetrack impact per merge.  
- [ ] Prefill `--link-clients` creations: count by day; overlap with pre-existing clients.  
- [ ] PENDING INTAKE without log# vs completed returns.  
- [ ] Spouse triple-store consistency (`clients` / `spouses` / `drake_household_prefill`).  
- [ ] Email/phone/DOB fill rates as secondary link features (expect email ~3.6%).  
- [ ] Confirm audit package still has **no** LLM verdict cache; file SHA256 only.  
- [ ] Import idempotency proof: re-run Tax Log CSV twice → zero new clients when LOG matches.  
- [ ] Document authoritative DB path on server vs workstation share.  
- [ ] Test-client flag coverage (`is_test` proposal) vs PHANTOM set.

---

## Hypothesis scorecard (Background)

| Hypothesis | Result |
|---|---|
| Join is name-only | **Mostly true** for Drake↔TaxOps↔purple; **false** for Tax Log import (LOG-first). |
| 40-char truncation | **Confirmed** (Drake CSM). |
| ~368 Jul1 duplicates from bulk import | **Count confirmed on Jul31 snap; mechanism refuted** — primarily `created_at` rewrite; twins older than Jul1. |
| No stable shared identifier | **Confirmed.** |

---

## Artifact index

| File | Phase |
|---|---|
| `I0-inventory.md` + snapshot/JSON | Inventory |
| `I1-provenance-map.md` + `I1-stats.json` | Provenance |
| `I2-ingestion-paths.md` + `I2-stats.json` | Ingestion + Jul1 |
| `I3-linkage-findings.md` + `I3-stats.json` + `I3-venn.json` | Linkage |
| `I4-synthesis.md` | This file |
