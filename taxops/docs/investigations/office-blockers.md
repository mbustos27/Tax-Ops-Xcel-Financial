# Office blockers — answers from data

**Date:** 2026-07-31  
**Mode:** Read-only.  
**Priors:** [`gruntworx-feasibility.md`](./gruntworx-feasibility.md), [`document-custody.md`](./document-custody.md). Baselines (2 docs, 236 unassigned inbox, scan 10×502/2×200, form nulls) are taken from those reports and not re-derived.

---

## Q1. Why do intake scans return 502?

### Answer

**TaxOps generates the HTTP 502** when its server-side call to the Scan Agent raises `RuntimeError`. That wraps agent-unreachable, agent HTTP errors, and missing config — it is **not** Waitress/proxy pass-through of a 502 from the agent. Logs and audit **do not preserve the error string** for the 2026-07-30 failures, so device-side vs agent-down vs timeout **cannot be distinguished** from data alone. Circumstantial evidence points to **agent/reception-side flapping** (configured URL is reception `192.168.1.9:8766`; successes sit in a pocket between two failure clusters; reception bootstrap logged `RC=1` that morning).

### Evidence

**502 origin in code** (`routes/documents.py:715-728`):

```python
try:
    pdf, page_count = _call_scan_agent(handwriting=handwriting)
except RuntimeError as exc:
    health = _call_scan_agent_health(timeout=5)
    ...
    return jsonify(payload), 502
```

`_call_scan_agent` (`:520-563`):

- URL: `{SCAN_AGENT_URL}/scan`, default base `http://127.0.0.1:8766` — `config.py:338`
- Live share config: `taxops/.env` sets `SCAN_AGENT_URL=http://192.168.1.9:8766` and a non-empty `SCAN_AGENT_TOKEN` (value redacted here). Docs expect that host — `docs/reception_agents_baseline.md:72`
- Timeout: **180s**, **no retry** — `documents.py:520,546`
- `URLError` → `RuntimeError("Cannot reach Scan Agent at …")` — `:560-563`
- Agent `HTTPError` → `RuntimeError(detail or f"Scan Agent HTTP {exc.code}")` — `:550-558`
- Missing URL/token → `RuntimeError` before any network call — `:527-531`

So 502 = **TaxOps choosing status 502** after its own exception handler. A successful agent response never returns 502 from this route.

**`/api/scan-agent/status`** (`:686-691`, `_call_scan_agent_health` `:566-683`): GET `{SCAN_AGENT_URL}/health?wia=1`, timeout ≥12s. Returns JSON (`ok`, `scanner_found`, tips, etc.). **Not persisted** to DB; **not written** to `audit_log` (GET skipped — `audit_service.py:220-227`). No evidence of status calls logged in stderr for Jul 30.

**Timeline** (all `user=admin`, `ip=127.0.0.1` — browser on TaxOps host or local loopback):

| UTC | return_id | HTTP |
|-----|-----------|------|
| 17:59:59 | 2692 | 502 |
| 18:04:35 | 2693 | 502 |
| 18:44:49–18:48:17 | 2693 (×4) | 502 |
| **21:11:42** | **2695** | **200** |
| **21:25:33** | **2696** | **200** |
| 22:14:46–22:19:44 | 2699, 2703–2705 | 502 |

Separable by **time pocket** (two successes ~21:11–21:25 UTC); not by user; not by workstation beyond `127.0.0.1`.

**Log correlation:** `audit_log` after-snapshot for 502s is only `{http_status, skipped_full_after}` — **no error body** (`audit_service.py:462-463`). `T:\logs\taxops_stderr.log`: **0** lines dated `2026-07-30` containing scan/WIA/Epson/8766/Cannot reach; route does not `logger.exception` on the 502 path. Scan agent logs to **process stdout/stderr** on the reception PC (`scan_agent/server.py:703-705`) — **not** a share file. Only related share crumb: `C:\TaxOps\logs\go_reception_last.log` — `2026-07-30 12:51` LOCAL start, **finished RC=1** (agents unhealthy that morning, hours before the scan cluster).

**Failure-class matrix**

| Class | Determinable from data? |
|-------|-------------------------|
| TaxOps bug inventing 502 without agent call | Unlikely for this cluster — code only 502s on `RuntimeError` from `_call_scan_agent` |
| Agent process down / wrong host / firewall | **Plausible**; error text would say “Cannot reach…” — **text missing** |
| Scanner / WIA / TWAIN session | **Plausible** if agent returned HTTP error with WIA detail — **text missing** |
| Timeout on large scan | **Possible** (180s); no duration logged |
| Misconfigured token | Possible (`HTTP 401/403` wrapped); not separable without body |

### Confidence

**High** that 502 is TaxOps-generated after agent-call failure. **UNKNOWN** for root cause subtype.  

### What a human must still check

On reception during a failing scan: read the Scan Agent console for the matching timestamp; from TaxOps host, `curl http://192.168.1.9:8766/health?wia=1` with the token header; confirm NSSM’s live `SCAN_AGENT_*` matches `taxops/.env`.

---

## Q5. Brokerage-heavy volume (GruntWorx Trades)

### Answer

**UNKNOWN as a brokerage count.** Hard upper bound from data: **`sched_a_d = 1` on 115 / 1413 TY2025 returns (8.1%)** — and that flag is **Schedule A and/or D**, never split. Free-text and fee line-items give **essentially zero** brokerage signal. Lower bound from TaxOps data: **0** confirmed 1099-B / named-broker returns.

### Evidence

**`return_forms` schema** (`db.py:196-210`):  
`id, return_id, form_1040, sched_a_d, sched_c, sched_e, form_1120, form_1120s, form_1065_llc, corp_officer, business_owner, form_990_1041`.

**What populates it**

| Source | Behavior |
|--------|----------|
| Drake `Type` via `DRAKE_TYPE_FORMS` | Only return **family** flags (1040/1120/…); **does not set `sched_a_d`** — `config.py:658-667`, `_type_to_forms` `drake_importer.py:458-470` |
| Tax log CSV | `SCH A & D` → `sched_a_d` etc. — `importer.py:553-564` |
| Intake checkboxes | `1` if checked else **NULL** — `app.py:3361-3370` |
| Upload confirm / preparer forms UI | Writes form flags — `app.py:3815-3836`; template labels “Sch A&D” — `templates/upload.html:285` |

**TY2025 `sched_a_d`:** `1` = **115**, `0` = **39**, `NULL` = **1236** (of 1390 forms rows). **Nothing in schema distinguishes A from D.**

**Drake field maps:** CSM/TAX_OPS columns are name, status, dates, fees, `Type` / `Return Type` — **no** Schedule D, capital gains, or brokerage fields (`drake_importer.py:58-91`).

**Free-text search** (terms: Schwab, Fidelity, Vanguard, Merrill, Edward Jones, Raymond James, Ameritrade, E*Trade, Robinhood, 1099-B, consolidated, capital gain, brokerage, broker, sch d, schedule d):

| Source | Hits |
|--------|------|
| `notes.note_text` | **0** |
| `missing_docs.item_text` | **0** (matches gruntworx §8) |
| `returns.notes_intake` | **0** |
| `work_order_items.description` | **0** |
| `email_inbox` subject/filename | **1** — “Business Broker / CPA Relationship” (Iconic promo), not a client 1099-B |

**Fees (TY2025):** `payments.total_fee` n=1288 non-null; p50=**$225**, p90=**$560**, p95=**$1120**, p99=**$1450**, max=**$3985**. Line items `form_1099_fee` / `accounting_fee` / `w7_fee` / etc.: **all zero counts** for `>0`. Among `total_fee ≥ 500`: 147 returns, only **17** with `sched_a_d=1`; at p95+ ($1120): 65 returns, **7** with `sched_a_d=1`. Fee tail is a **weak** complexity proxy, not brokerage.

**Bounds**

| Bound | Value | Basis |
|-------|------:|-------|
| Lower | **0** | No 1099-B / broker-name evidence in TaxOps |
| Upper (A∨D) | **115 (8.1%)** | `sched_a_d=1`; estimate 115/1413 |
| Upper (high fee ∩ A∨D) | **17** at ≥$500 | Even smaller; still not “Trades” |

**Human count:** Sample Drake returns or paper files for 1099-B / consolidated 1099; or export a Drake report that lists Schedule D / brokerage — TaxOps cannot.

### Confidence

**High** on upper bound and empty text search; **UNKNOWN** on true Trades volume.  

### What a human must still check

Ask a preparer: “Roughly what fraction of 2025 individual returns had a brokerage 1099-B / consolidated 1099?” or pull that from Drake.

---

## Q6. Entity vs 1040 mix (resolving NULLs)

### Answer

**NULL `form_1040` means “never marked,” not “not a 1040.”** Best available TY2025 split (mutually exclusive):

| Bucket | n | % of 1413 |
|--------|--:|----------:|
| Confirmed 1040 only | 628 | 44.4% |
| Confirmed 1040 + entity flags | 9 | 0.6% |
| Confirmed 1040 + trust/990/1041 | 8 | 0.6% |
| **Confirmed 1040 (any)** | **645** | **45.6%** |
| Confirmed entity only (1120/1120S/1065, no form_1040=1) | 33 | 2.3% |
| **Confirmed entity (any entity flag)** | **42** | **3.0%** |
| Unresolved (no positive type flag) | **735** | **52.0%** |

Trust flags (8) all co-occur with `form_1040=1`. NULLs do **not** cluster in one exotic status — large shares in LOG OUT (406) and PROCESSING (269).

### Evidence

**Writers**

1. **Intake:** unchecked → `None` — `app.py:3366-3370`  
2. **Drake:** `_type_to_forms` always returns **0/1** ints for all flags; known `Type` sets the matching bit — `drake_importer.py:458-470`, `DRAKE_TYPE_FORMS` `config.py:658-667`. Upsert INSERT/UPDATE — `drake_importer.py:752-789`. UPDATE uses `COALESCE(?, col)` so a Drake `0` **will** overwrite NULL.  
3. **Tax log:** `normalize_bool_flag` → `True`/`False`/`None` — `normalizer.py:136-145`; `_bool_to_int` keeps `None` — `importer.py:998-1001`. Empty CSV cells → NULL left in place on COALESCE update.  
4. **Upload confirm / UI:** writes checkbox flags — `app.py:3815-3836`.

**Why 730 NULL:** Rows exist (1390 forms rows; 23 returns lack a row entirely) but **no path wrote 0 or 1** for `form_1040`. Drake always writes 0/1 on touch — so those returns were **not** form-updated by a successful Drake type mapping (or only tax-log/intake with empty 1040 column). **NULL ≠ non-1040.** By contrast, all **15** rows with `form_1040=0` are entity-flagged (explicit non-1040 from Drake-style zeros).

**Better signals?**

| Signal | Useful? |
|--------|---------|
| `drake_status_raw` | Status crumbs (`EF Accepted`, `E-Filed: YES`, …) — **not** return type |
| Drake `Type` | Only as already mapped into `return_forms` |
| `log_number` bands | Entity flags appear across bands; **no** clean entity number-block |
| `clients.id_type` | Exists; not used here as a return-type oracle |

**Cross-tab `form_1040` null vs status (TY2025):** LOG OUT 406 null / 525 ones; PROCESSING 269 null / 89 ones; PICKUP 43 null; FINALIZE 33 null — nulls are ordinary workflow mass, not a single abandoned slice.

**Trustworthiness:** Positive flags (`form_1040=1`, entity=1`) are trustworthy when set (Drake/tax-log/intake checked). **Absence/NULL is not evidence of type.** For GruntWorx entity thinness: use **≥42 confirmed entity** as lower bound; do **not** treat 1413−645 as entity.

### Confidence

**High** on NULL semantics and the 645 / 42 / 735 split.  

### What a human must still check

Whether unresolved ~735 are almost all 1040s in practice (preparer gut check), or re-import Drake CSM `Type` for TY2025 to fill flags.

---

## Q7. pytest against the live tree

### Answer

**Ongoing contamination (May 19 – Jul 2, 2026), not one incident.** Tests isolate the **DB** via `taxops_db_path` but several upload tests **do not** isolate `DOCUMENTS_BASE_PATH`, so PDFs land in the live `documents/returns/{6001,…}` tree. `__test_user__` rows in **production** `audit_log` are explained by the **async audit writer** calling `get_connection()` at flush time — after monkeypatches can revert — while document INSERTs stayed on the temp DB (`return_documents.seq` still 2).

### Evidence

**Upload / document-writing tests** (non-exhaustive but complete for upload routes):

| File | Isolates docs path? |
|------|---------------------|
| `tests/test_doc_hard.py` | **No** — posts to `/documents/upload` & `bulk-upload` with ids 6001/6002/7001/8001/8002 |
| `tests/test_upload_limit.py` | **No** path patch (413 may not write) |
| `tests/test_debt2_coverage_gaps.py` | **Yes** — monkeypatches `get_return_documents_path` → `tmp_path` |
| `tests/test_intake_scanning.py` | **Yes** — patches docs dir |
| `tests/test_email_inbox_routes.py` / `test_email_suggest.py` | **Yes** — patch assign dest |
| `tests/test_accounting.py` | Receipt uploads; uses tmp images; `__test_user__` receipt audits in prod trail |

**Fixtures** (`tests/conftest.py:27-40`): patches `TAXOPS_DB` / `cfg.DB_PATH` / `db.DB_PATH` to `tmp_path` — **does not** patch `DOCUMENTS_BASE_PATH`. Default documents root = package `documents/` — `config.py:259`.

**Can tests reach prod DB?**  
- Happy path: no — fixture redirects DB.  
- **Unpatched entry points:** importing `app` / `db.get_connection()` outside fixtures uses env/`taxops.db` on the share. Scripts under `scripts/` are not pytest-isolated.  
- **Audit race:** writer thread `get_connection()` at flush — `audit_service.py:64-76`; daemon thread can outlive patch. Matches observation: prod `audit_log` has `__test_user__` document actions for 6001…8002, but prod `return_documents` never gained those rows (`seq=2`, document-custody).

**Dating**

| Signal | Range |
|--------|-------|
| `__test_user__` audit | **2026-05-19 → 2026-07-02** (82 rows; days: 5/19, 5/20, 5/22, 5/28, 5/29, 6/18, 7/2) |
| Fixture folder mtimes | **2026-05-19 → 2026-07-27** (files still written/touched after last audit day) |

**CI / hooks:** `pytest.ini` present (`taxops/pytest.ini`) with `testpaths = tests filetrack`. **No** `.github` workflows found under `taxops/`; **no** pre-commit config found in a shallow search. Documented local pytest is the implied run mode (share checkout).

**Other prod side effects from tests**

| Area | Evidence |
|------|----------|
| `returns`/`clients` 6001+ | **Not present** now (ids outside range / deleted with temp DB) |
| `email_inbox` | 4 filename/sender “test” rows — weak; not clearly pytest |
| `import_batches` | unchanged story (3 Apr-29 batches) — not test-driven |
| Disk | **624** fixture PDFs — confirmed |
| `audit_log` | **82** `__test_user__` rows including uploads, receipt, user admin APIs |
| `accounting` settings / receipt actions | yes in test action list |

**Blast radius (worst case under current isolation):** fill/delete noise under `documents/`; pollute `audit_log`; if someone runs the app modules **without** the DB fixture against prod, full CRUD on clients/returns/settings (test actions already show deactivate-user / bulk status / accounting settings attempts in the prod audit trail under `__test_user__`). Document rows in prod were **not** left behind for the 600x uploads (temp DB), which limited structural damage.

### Confidence

**High** for disk contamination mechanism and date span; **medium** for audit-writer race as the exact prod-audit path (best fit; not proven with a failing test capture).  

### What a human must still check

Whether anyone still runs `pytest` from `T:\taxops` on the live share; stop that until docs path is sandboxed. Optionally quarantine/delete `documents/returns/{1,6001,6002,7001,8001,8002}` fixture piles after backup.

---

## Answered from data vs requires a human

| Q | From data | Needs a human |
|---|-----------|---------------|
| **Q1** 502 | TaxOps-generated after Scan Agent call failure; timeline; URL config | Exact subtype (agent down vs WIA vs timeout) — read reception agent console / live curl |
| **Q5** Brokerage | Upper bound 115 A∨D; lower 0; no text/fee signal | True 1099-B / Trades fraction for quote |
| **Q6** Entity/1040 | NULL=unmarked; 645 / 42 / 735 split | Whether unresolved 735 are nearly all 1040s in Drake |
| **Q7** pytest | Ongoing May–Jul disk+audit contamination; missing docs isolation | Confirm who runs pytest on the share and stop/relocate |

---

## Contradictions vs prior reports

| Prior claim | This report |
|-------------|-------------|
| document-custody: stderr has “email_inbox” watcher noise but no assign | **Confirmed**; additionally **no Jul-30 scan error lines** — 502 reason not in logs |
| document-custody / gruntworx: orphans are test debris for 6001+ | **Confirmed** with audit date span and `test_doc_hard.py` linkage |
| gruntworx: 730 NULL `form_1040` weakens entity split | **Resolved:** NULL ≠ non-1040; use 645 / 42 / 735 |
| gruntworx: no brokerage signal in `missing_docs` | **Confirmed** and extended to notes / WO / fees |
| document-custody: SCAN path “flaky 502” | **Sharpened:** 502 is always TaxOps-side wrapper; root cause subtype still UNKNOWN |
| Implied “T: vs C:” confusion | Unchanged; not re-opened here |

No finding overturns the custody conclusion that TaxOps is not the season document custodian, or the GruntWorx conclusion that assembly-from-`return_documents` is not feasible at volume today.
