# I3 — Linkage findings

Supporting numbers: `I3-stats.json`, `I3-venn.json`, `I1-stats.json`, audit DB `audit_202607311100.sqlite`.

---

## 1. Join keys actually used

| Location | Join / match key | Quote / cite |
|---|---|---|
| Tax Log CSV import | **`log_number` + `tax_year`**, then fuzzy name | `importer.py:744` `WHERE log_number = ? AND tax_year = ?`; `_match_return` LOG-first `:601-605` |
| Client upsert (import) | Exact `lower(last_name)+lower(first_name)` | `importer.py:653-661` |
| Fuzzy client match | Normalized full/last `token_sort_ratio` ≥ 88 | `name_matcher.find_client` / `score_client_names_pair:150-155` |
| Prefill CSM↔purple | **Normalized taxpayer name** (+ variant ranks); SSN last4 for client link | `drake_prefill_importer` Phase 2; `link_prefill_clients` SSN then fuzzy ≥90 `:1972+` |
| Findings audit | Deterministic name tiers; last4 **tie-break only** | `audit/match.py:56-64` `_break_ties`; tiers `surname_first_token`, `paternal_surname_first_token`, `full_normalized_transposition`, `entity_exact_name` |
| Intake search / reintake | client id + prefill names | `/api/clients/search` |

**Is name the only cross-source key?**  
**Nearly — with two exceptions:** (1) TaxOps↔Tax Log operational join prefers **log_number**; (2) CSM provides **SSN/EIN last-4**, used as secondary (tie-break / prefill link), **never alone** as a match (`audit/match.py` module docstring: “no last-4-alone matches”). There is **no** shared Drake client GUID, no Tax Log GUID, and TaxOps `clients.id` is not pushed back to Drake or the Log.

---

## 2. Candidate identifier scores

### TaxOps live (`clients` / `returns` / prefill)

| Candidate | Present | Fill rate | Distinct/filled | Collisions | Viability |
|---|---|---:|---:|---:|---|
| `returns.log_number` (+ year) | Tax Log + TaxOps | 0.743 of returns | **1.000** (1199/1199); 0 dup (log,year) | 0 | **Best operational key within TaxOps↔Log for filed returns**; null on PENDING INTAKE shells |
| `clients.ssn_last4` | CSM last4 / intake | 0.904 | 0.938 (1294/1379) | **78** keys | Strong secondary; collisions from family/shared last4 & blanks history |
| `csm_ssn_last4` (prefill) | CSM | 1.0 of 1379 links | 0.894 | **130** | Same last4 reused across years/entities in CSM universe |
| `taxpayer_cell` | intake/purple | 0.609 | 0.920 | 65 | Moderate; format unstable |
| `taxpayer_dob` | intake/purple | 0.555 | 0.980 | 15 | Good when present; sparse |
| `address` | intake | 0.624 | 0.833 | **132** | Weak (shared addresses) |
| `taxpayer_email` | intake | **0.036** | 0.964 | 2 | Too sparse |
| `prior_year_log` | intake/rollover | 0.029 | 1.0 | 0 | Too sparse |
| `taxpayer_phone` | intake | 0.049 | 0.946 | 3 | Too sparse |
| Drake client GUID | — | **absent** | — | — | **Useless — not in exports** |
| TaxOps `clients.id` | TaxOps only | 1.0 | 1.0 | 0 | Stable **inside** TaxOps; not in Drake/Log |

### CSM xlsx (Desktop 1155)

| Candidate | Fill | Distinct | Notes |
|---|---:|---:|---|
| `ID (Last 4)` | 1155 | 1103 | 52 collision surplus — last4 not unique |
| `Client Name` | 1155 | — | max len **40**; 54 exactly 40 |

### Tax Log XCEL 2025

| Candidate | n | Notes |
|---|---:|---|
| Named rows | 1252 | matches `EXPECTED_LOG_NAMED_ROWS` |
| `log_2025` (staged) | non-empty ≈1252; distinct 1048 in raw col-B probe | Log # is the office primary key for the season |
| Last name length | max 39; 3 at 39; **0 at 40** | Not Drake-capped the same way |

**Rank for a future stable link key:**  
1) Mint TaxOps UUID/id and **write back** to Log + Drake custom field (none exists today)  
2) `(log_number, tax_year)` where both exist  
3) SSN/EIN full (not last4) — **not stored** in TaxOps by design  
4) last4 + normalized name composite  
5) Name alone — last resort (current default for Drake↔purple)

---

## 3. Name-key failure quantification

### Length at truncation boundary

| Source | n @39 | n @40 | max |
|---|---:|---:|---:|
| CSM Desktop Client Name | 17 | **54** | 40 |
| CSM 2024 xlsx | 49 | **137** | 40 |
| `drake_prefill_links.csm_name_raw` | 24 | **74** | 40 |
| `purple_name` | 7 | 12 | **51** |
| Tax Log last | 3 | 0 | 39 |
| `clients.display_name` | 3 | 1 | 40 |

Samples at 40 (CSM): `ABU TAHA, ZAID AHMED Y & GIULIANA CASSIN`, `AGUILAR FLORES, JOSE A & ANGELICA AGUILA`, … — joint names clipped mid-token.

### Normalized-name collisions (TaxOps clients)

Key = `upper(trim(last))|upper(trim(first))` with punct stripped (`I3-stats`).

| Metric | Value |
|---|---:|
| Multi-row buckets | **14** |
| Extra rows | **14** |

Largest buckets (ssn last4 redacted to values present):

| key | n | ids | ssn4s |
|---|---:|---|---|
| ORMA SERVICES INC | 2 | 8,754 | null,2100 |
| MIAMAR FUTURE LLC | 2 | 32,654 | null,4898 |
| SANDOVAL AUTO SERVICE TOW | 2 | 107,1780 | null,null |
| HERNANDEZ ISMAEL | 2 | 659,2275 | 2720,3403 |
| NUNO JUAN | 2 | 731,2276 | 8143,0775 |
| … | | | (full top 25 in `I3-stats.json`) |

Exact SQL duplicate groups (no punct strip): **8 groups / 8 extras** (`I2-stats`).

### Format divergence

| Pattern | Count |
|---|---:|
| CSM names with `,` | 1254 / 1379 |
| CSM names with `&` (joint) | 442 |
| Purple with `&` | 404 |
| Purple with `,` | **0** |
| Clients blank first_name (entities) | 151 |

CSM sample: `BUSTOS, MOISES & YANEZ, SANDRA`  
Purple sample: `LAILA & AKRAM HAMIDEH`, `ROCIO GARAY` — **order flipped** (FIRST LAST / joint FIRST).

### Truncated-vs-full pairs (CSM norm prefix of longer CSM norm)

Among 98 names with len≥39: **0** strict longer prefix hits inside `csm_name_norm` (`I3-stats`). Truncation pairs are **cross-source** (CSM 40 vs purple ≤51), not within CSM alone.

---

## 4. Three-way reconciliation census (audit run Jul31)

From `audit_match` on `audit_202607311100.sqlite` (`I3-venn.json`):

| Set | Count |
|---|---:|
| stage_drake | 1155 |
| stage_log | 2820 (all sheets; named XCEL subset ~1252) |
| stage_taxops_client | 1514 |
| Drake matched TaxOps | 1135 pair rows / **drake_taxops set** |
| Drake matched Log | 1022 |
| Log matched TaxOps | 1010 |
| **Drake in both TaxOps and Log** | **1009** |
| Drake→TaxOps only (not Log) | **126** |
| Drake→Log only (not TaxOps) | **13** |
| Drake matched neither | **7** |

Match tiers used: `surname_first_token` 2543, `full_normalized_transposition` 518, `entity_exact_name` 70, `paternal_surname_first_token` 36.

Finding workbook categories (Jul31 output): NAME_TRUNCATED 71, PHANTOM_IN_TAXOPS 227, LOGGED_NOT_PREPARED 141, PREPARED_NOT_LOGGED 88, DUPLICATE_CLIENT 142, etc.

Samples (SSN=last4 only): see `I3-venn.json` `samples_*` — e.g. all-three proxy `ABDEL HADY, OMAR` last4 `5921`; Drake-neither includes `RANGEL, DANIEL` / `HARRIS JR, EDDIE D` (In Progress).

---

## 5. Matcher audit (rapidfuzz + “Claude”)

### Production (`taxops/name_matcher.py`)

- Input: **normalized** strings (`normalize_name` → upper, punct→space, suffix strip); joint spouse stripped before match.
- Scorer: `rapidfuzz.fuzz.token_sort_ratio` on full name and on last name (`:150-155`).
- Thresholds: ACCEPT **88**, REVIEW **70**.
- Prefill overrides accept to **90** locally.

### Findings audit (`audit/match.py`)

- **Deterministic tiers only** — not fuzzy rapidfuzz for primary match.
- **No Anthropic/Claude** (`audit/README.md` line 32). Background assumption of “rapidfuzz + Claude adjudication” is **FALSE for the current audit package**.
- SHA-256: `audit/util.sha256_file` hashes **entire source files** into `audit_run` for reproducibility (`preflight.py`). **Not** a per-pair verdict cache. Therefore: **no stale-verdict-cache problem of that form**; also **no LLM cache to invalidate** when normalization changes — re-run audit regenerates `audit_match` rows.

### Fragile band 85–95

Production ACCEPT=88 → pairs scoring 85–87 fall to review/new-client; 88–95 auto-merge.  
Prefill ACCEPT=90 → 88–89 would accept in import but not in prefill.  
Stored `drake_prefill_links.match_score` bands should be read from DB when non-null; CSM↔purple raw `token_sort` often lower because of LAST,FIRST vs FIRST LAST (need order-aware normalize — audit tiers handle this better than raw token_sort).

---

## Appendix — queries (verbatim)

```sql
-- Candidate fill (example)
SELECT COUNT(*) FROM clients;
SELECT COUNT(*) FROM clients WHERE ssn_last4 IS NOT NULL AND TRIM(ssn_last4)!='';
SELECT COUNT(DISTINCT ssn_last4) FROM clients WHERE ssn_last4 IS NOT NULL AND TRIM(ssn_last4)!='';

-- Name length boundary
SELECT LENGTH(TRIM(csm_name_raw)) L, COUNT(*) FROM drake_prefill_links GROUP BY 1 HAVING L IN (39,40);

-- Audit match pairs
SELECT left_kind, right_kind, COUNT(*) n FROM audit_match GROUP BY 1,2 ORDER BY n DESC;
SELECT tier, COUNT(*) n FROM audit_match GROUP BY 1 ORDER BY n DESC;
```

Full numeric dumps: `I3-stats.json`, `I3-venn.json`.
