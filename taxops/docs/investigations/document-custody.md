# Why TaxOps holds almost no documents

**Date:** 2026-07-31  
**Mode:** Read-only.  
**Prior art:** [`gruntworx-feasibility.md`](./gruntworx-feasibility.md) §§3–4 (counts, path samples, extraction emptiness). This report explains **why**, not re-derives those baselines.

**Baselines already established (do not re-litigate):**

| Fact | Source |
|------|--------|
| 2 active `return_documents` vs 1413 TY2025 returns | gruntworx §4 / §8 |
| 236 `email_inbox`, 0 assigned | gruntworx §4 |
| `missing_docs` on 1/1748 returns | gruntworx §3 |
| Disk 624 PDFs / 8 nonempty folders; DB `file_path` uses `C:\TaxOps\...` | gruntworx §4 |
| `extraction_queue`: 2 failed + 1 stuck `processing` | gruntworx §1 |

---

## 1. Document root divergence (H1)

### Finding

`DOCUMENTS_BASE_PATH` defaults to `{taxops package dir}/documents` with no `.env` override on the share. Live rows store **absolute** `C:\TaxOps\taxops\documents\returns\...` paths — i.e. the NSSM host’s local view of the app tree. From this workstation, that `C:\` tree is **not** the documents store (`C:\TaxOps` has only `backups`, `diagnostics`, `logs`, `Reception`); the live files including the two July-30 scans sit on `T:\taxops\documents` → `\\Xcel-server\taxops\taxops\documents`. The large folders (`6001`, `8002`, …) are **outside** `returns.id` range and are **test debris**, not a hidden production corpus on a second root.

### Evidence

**Resolution chain**

1. `config._HERE = Path(__file__).parent` → `taxops/` package dir — `config.py:10`
2. Env load: repo `.env` then `taxops/.env` overlay; OS/NSSM wins over file for already-set keys — `config.py:94-96`
3. `DOCUMENTS_BASE_PATH = os.environ.get("DOCUMENTS_BASE_PATH", str(_HERE / "documents"))` — `config.py:259`
4. Folder for a return: `os.path.join(DOCUMENTS_BASE_PATH, "returns", str(return_id))` — `utils.py:58-62`
5. Upload stores `os.path.abspath(...)` into `file_path` — `routes/documents.py:214-272`

**Share `.env`:** no `DOCUMENTS_BASE_PATH` line (grep). **NSSM `AppEnvironmentExtra`:** UNKNOWN from repo (`nssm dump` not available on this workstation).

**This session’s resolution:** `ENV_DOCUMENTS=None` → default abspath `T:\taxops\documents` when config is imported from the share. Service-written DB paths still say `C:\TaxOps\taxops\documents\...` — consistent with AppDirectory `C:\TaxOps\taxops` on the service host (`scripts/nssm-taxops-snippet.ps1:18-19,45-46`).

**Tree compare**

| Folder ID | In `returns`? | `returns.id` range 1–2705 | Disk role |
|-----------|---------------|---------------------------|-----------|
| 2695, 2696 | yes | inside | Live scans (match DB filenames) |
| 1 | yes | inside | 83× `test.pdf` / `test_N.pdf` |
| 6001, 6002, 7001, 8001, 8002 | **no** (0 rows) | **outside** (max id 2705) | Fixture names (`file1`, `dup`, `audit_test`, `test_w2`) |
| ~20 empty numeric dirs | mostly yes | inside | Empty shells (Jun 18 mtimes) |

**Timestamps (T: tree):** live scans 2026-07-30 afternoon; fixture piles May 19 → Jul 27; email_inbox newest **2026-07-31 12:33** local.

**`file_path` prefixes:** all 2 `return_documents` and all 236 `email_inbox` rows are `C:\...`. **No `T:\` mixture** → no evidence of a mid-season root flip in the DB; only host-local absolute paths.

**H1 implication check:** The two scan PDFs named in the DB **exist on T:** with matching sizes. Therefore T: is **not** a stale unused copy of a richer C: corpus. The 624 PDFs are mostly **orphans on the live tree** (see §4), not “real docs the share can’t see.”

### Confidence

**High** for default/resolution and orphan ID range; **UNKNOWN** for whether server `C:\TaxOps\taxops` is a local directory identical to the share (likely, but needs a check on the NSSM host).

---

## 2. Email assign path (H2 vs H3)

### Finding

Assign is **implemented, UI-wired, and gated by the same admin-only permission as the page**. There is **no evidence of any assign attempt** in `audit_log` or `taxops_stderr.log`. The inbox has grown continuously since mid-June while the watcher still saves attachments. **H3 fits; H2 does not** (for “broken endpoint”), with one residual runtime UNKNOWN (assign never exercised in prod).

### Evidence

**Endpoint** — `app.py:2725-2807`:

- Gate: `@permission_required("can_use_email_tools")` (admin only — `config.py:567-568`)
- Body: JSON `{return_id}`; 400 if missing; 404 if inbox row missing/assigned/deleted or return missing
- `shutil.copy2(src_path → get_return_documents_path)`; `INSERT return_documents` with `source='email_inbox'`, `match_confirmed=1`, `match_method` `email_suggested`|`email_manual`; mark inbox assigned; `_enqueue_extraction`
- Errors → rollback + 500 `"Assignment failed"` (logged as `email_inbox assign error`)

**Page / API same gate:** `/email-inbox` and `/api/email-inbox/items` also `can_use_email_tools` — `app.py:2628-2629`, `2668-2669`. Nav link only if `has_permission('can_use_email_tools')` — `templates/base.html:71,134-136`.

**UI calls assign:** `templates/email_inbox.html` — Assign button `439-439`, `assignToSuggested` `472-477` → `POST /api/email-inbox/${id}/assign`. Not a dead route.

**Attempt evidence**

| Signal | Result |
|--------|--------|
| `audit_log` actions matching email-inbox assign | **0** (POST audit would be `POST api_email_inbox_assign`; GETs are not audited — `audit_service.py:220-227`) |
| `taxops_stderr.log` mentions of `email_inbox assign` / `Assignment failed` / `api_email_inbox_assign` | **0** (120 MB scanned) |
| Historical email UI | Only pre-simplification `email_review.api_email_classification_confirm` (May 28–29), not holding-area assign |

**Inbox chronology:** `received_at` min `2026-06-18` → max `2026-07-31T19:33:05Z`; daily accumulation through today (e.g. Jul 31: 5 items). `email_processing_log` `outcome='success'` max same timestamp; stderr still logs `mail_watcher email_inbox: N file(s) saved`. Watcher is alive.

**Who can work it:** `auth_users` with `role='admin'`: `admin` (last login today), `info` (Jul 20), `verifyuser` (May). Receptionists (`marlin`, etc.) **cannot** open the tool under current RBAC.

### Confidence

**High** for H3 over H2 on attempt/permission evidence; **medium** that assign would succeed on first real try (path/`copy2` unproven in prod).

---

## 3. Upload paths that DO work

### Finding

The only production path that has produced `return_documents` rows is **scan-intake via the Scan Agent**, and only **2 of 12** audited attempts returned HTTP 200; the rest were **502**. Walk-in upload UI exists for any logged-in user but has **zero non-test** audit hits. Intake redirects to `?scan=1`, but staff routinely **Skip** → `scan_deferred=1` (13 returns currently).

### Evidence

**Contrast**

| Path | Permission | Writes `return_documents`? | Prod outcome |
|------|------------|----------------------------|--------------|
| `POST .../scan-intake` | `can_scan_intake_docs` (receptionist+) | yes, `source/match_method=scan_agent` — `routes/documents.py:694-769` | 2× HTTP 200 (2695, 2696); 10× HTTP **502** |
| `POST .../documents/upload` | `@login_required` only — `routes/documents.py:183-185` | yes, walk-in / optional scan_agent form source — `:50-56` | **0** non-`__test_user__` audits |
| `POST .../documents/bulk-upload` | login | yes | test-only audits |
| Email assign | `can_use_email_tools` | yes | 0 attempts (§2) |

**Why only scan_agent rows:** It is the path reception/`admin` actually invoked after intake (`app.py:3464-3465` redirect `?scan=1`). Walk-in file picker exists in `return_detail.html` (`~3143-3208`) but was never used by staff accounts. Email path unused.

**Scan UI:** `can_scan_intake_docs` + `prompt_scan` from `?scan=1` — `app.py:2343-2344`, `return_detail.html:1403-1405,3280`. Health via `/api/scan-agent/status`. Runbook: Scan Agent on RECEPTION as Scheduled Task (not NSSM) — `docs/reception_agents_runbook.md:12-13,57-64`. **502 cluster on 2026-07-30** shows the UI is reachable but the agent often fails (scanner/session/agent down).

**`scan_deferred`:** 13 returns still flagged (2705…2686, all TY2025 PROCESSING) after `POST api_return_scan_deferred` — intake Skip path — `app.py:2349-2365`.

**`sqlite_sequence return_documents.seq = 2`:** production table has only ever allocated ids 1–2. Not a mass soft-delete mystery.

### Confidence

**High**.

---

## 4. Orphan tree provenance

### Finding

Folders `6001` / `8002` / `7001` / `8001` / `6002` / `1` are **pytest fixture output** written into the live `documents/returns` tree because tests do **not** isolate `DOCUMENTS_BASE_PATH`. Filenames match `tests/test_doc_hard.py`. No `is_test` column on `clients`/`returns`.

### Evidence

**Filename samples:** `file1.pdf`, `dup.pdf`, `audit_test.pdf`, `test_w2.pdf`, `test.pdf` with `_N` collision suffixes — classic upload-dedup pattern from `routes/documents.py:209-212`.

**Test source:** `tests/test_doc_hard.py` seeds clients/returns **6001, 7001, 8002** and uploads those exact names (e.g. bulk `file1.pdf` at ~346-363; `dup.pdf` ~245-259; `audit_test.pdf` ~288-307).

**Fixture gap:** `tests/conftest.py` patches `TAXOPS_DB` to `tmp_path` (`:27-40`) but **never** monkeypatches `DOCUMENTS_BASE_PATH` / `cfg.DOCUMENTS_BASE_PATH`. Uploads therefore land under the real package `documents/` while DB rows die with the temp DB → disk orphans, `seq` stays 2 in prod.

**Prod audit smell:** 58 `__test_user__` document upload/bulk actions targeting those entity_ids in **this** `audit_log`. That means some test runs also touched the production DB’s audit trail (or shared it). Files + audit IDs align; `return_documents` rows do not remain.

**Seeder/migration:** no production seeder found that creates 6001-style trees; only tests.

### Confidence

**High** for fixture provenance; **medium** for exactly how `__test_user__` landed in prod `audit_log` (path clear: tests against this tree; DB coupling details less certain).

---

## 5. `extraction_queue` stuck row

### Finding

**No reaper** for `status='processing'`. A worker crash after the status flip leaves the row forever non-`pending`. The two `failed` rows share the same terminal message: no fields after 3 attempts.

### Evidence

- Claim work: `UPDATE ... SET status='processing', attempts = attempts + 1` then extract — `extractor.py:387-400`
- Retry only resets to `pending` when extraction returns no fields **inside** the try path — `extractor.py:421-431`
- Worker select: `WHERE status='pending' AND attempts < MAX` — `extractor.py:285-296` — **never** selects `processing`
- No timeout/reaper symbol in `extractor.py` (POLL_INTERVAL is idle wait only — `:188`)

**Rows**

| id | doc_id | return_id | status | attempts | error_message |
|----|--------|-----------|--------|----------|---------------|
| 2 | 5 (missing from `return_documents`) | 1149 | **processing** | 2 | `No fields extracted (attempt 1/3)` |
| 3 | 1 | 2695 | failed | 3 | `No fields extracted after 3 attempts` |
| 4 | 2 | 2696 | failed | 3 | `No fields extracted after 3 attempts` |

Failed pair: **same root cause** (empty extract / vision or text path yielded nothing on scan PDFs). Stuck row: orphaned queue item for deleted/never-kept `doc_id=5` (May 12), unrecoverable without manual SQL/admin requeue.

### Confidence

**High**.

---

## 6. What “working” would look like

### Finding

Working the email backlog tomorrow can add at most **236** `return_documents` rows (one per inbox item), not season coverage. Only **12** items have a cached `suggested_return_id` (all TY2025, `exact_email`); **13** senders match a client email column. Season custody metric is ~0% across 2024–2026.

### Evidence

**Ceiling (estimate, not silent guess):**

| Bucket | n | Notes |
|--------|---|--------|
| Inbox items (undeleted) | 236 | Hard ceiling for assign→`return_documents` |
| With suggestion | 12 | Plausible one-click map to TY2025 |
| Exact email on `clients` | 13 | Close to suggestion count |
| Subject/filename tax-ish keywords | 28 | Weak signal; many false friends |
| Bank-ish domains (bofa/cnb/…) | 21 | Often statements, not return packets |
| Personal webmail (gmail/yahoo/…) | 108+23+… | Need human match; majority of queue |
| PNG/JPEG attachments | 69 | Assignable but vision-skipped unless scan_agent / vision on |

**Realistic assignable-to-known-return band:** ~**12–40** with low staff effort (suggestions + obvious client mail); remainder needs search-by-name. **None** of this repairs the 1413 TY2025 gap by itself.

**Forward metric — % returns with ≥1 active `return_documents` row**

| tax_year | returns | with ≥1 doc | pct |
|----------|--------:|------------:|----:|
| 2024 | 132 | 0 | **0.0%** |
| 2025 | 1413 | 2 | **0.142%** |
| 2026 | 16 | 0 | **0.0%** |

### Confidence

**High** for metric; **medium** for the 12–40 assignable band (suggestion/email overlap is hard data; “obvious” remainder is judgment).

---

## Hypothesis verdict

| ID | Verdict | Strongest single evidence |
|----|---------|---------------------------|
| **H1** Two roots / T: stale | **Partially supported / partially refuted** | Supported: DB paths are host-local `C:\TaxOps\...` while staff browse `T:\`. **Refuted:** T: holds the live scan files and is `\\Xcel-server\taxops\taxops`; the 624 PDFs are not a richer hidden corpus — mostly test orphans outside `returns.id` range. |
| **H2** Assign technically broken | **Refuted** (as explanation of 0/236) | Endpoint + template JS present; **0** assign attempts in audit/stderr — failure modes never triggered. |
| **H3** Assign works, unused (admin-only) | **Supported** | `can_use_email_tools` admin-only; continuous inbox growth; zero assign POSTs; watcher still saving today. |
| **H4** Docs bypass TaxOps | **Supported** (practice); Drake-specific target **open** | `return_documents.seq=2`; 10/12 scans 502 + 13 `scan_deferred`; zero staff walk-in uploads; email unworked. Whether paper/Drake is the alternate custodian is a human question — code only proves TaxOps is not. |

---

## Bugs found

| Severity | Bug | Evidence |
|----------|-----|----------|
| **High** | Pytest does not isolate `DOCUMENTS_BASE_PATH` → fixture PDFs pollute production `documents/returns` | `conftest.py` vs `test_doc_hard.py` filenames on disk; IDs 6001+ outside prod range |
| **High** | Scan Agent flaky at intake (majority 502) → staff Skip → `scan_deferred`, no documents | audit HTTP statuses 502 vs 200 on 2026-07-30 |
| **Medium** | `extraction_queue` rows stuck in `processing` forever (no reaper); orphan `doc_id=5` | `extractor.py` select only `pending`; row id=2 since May |
| **Medium** | Email custody admin-only while intake scanning is receptionist-level → structural underuse of the largest inbound doc pipe | `ROLE_PERMISSIONS` asymmetry |
| **Low** | `__test_user__` document actions present in production `audit_log` | contamination / tests against live DB trail |
| **Low** | Failed extractions on the only two live scans (`No fields extracted after 3 attempts`) | queue rows 3–4; empty typed form tables (gruntworx §1) |

*(Reported only — not fixed.)*

---

## Human / runtime checks

1. On the **TaxOps server** (NSSM host): open Explorer to `C:\TaxOps\taxops\documents\returns\2695\` — does `scan_1785445902.pdf` exist there? (Confirms C: and share are the same tree.)
2. Admin PowerShell on that host: `nssm dump TaxOpsService` — note `AppDirectory`, `ObjectName`, and any `DOCUMENTS_BASE_PATH=`.
3. As user **`admin`**: open TaxOps → Tools → **Email Inbox**. Do you see ~236 unassigned items? Click one → Assign to a known return → does it succeed?
4. As receptionist **`marlin`**: is **Email Inbox** missing from Tools? (Expected under current RBAC.)
5. On **RECEPTION**: run `T:\check_reception.bat` — does it say `SCAN OK`? Retry intake scan on a `scan_deferred` return.
6. Ask preparers: for a typical 2025 return, where do W-2 PDFs live today — Drake Documents, paper file, email only, or TaxOps?
7. IT: were `pytest` runs ever executed from `T:\taxops` or `C:\TaxOps\taxops` against the live tree? (Explains 6001/8002 piles.)

---

## Smallest change toward “documents reliably land on `return_documents`”

Make email-inbox assign a **receptionist-level daily queue** (same permission tier as `can_manage_efile_queue` / `can_scan_intake_docs`), and treat Scan Agent **502 + Skip** as a broken intake path to fix on the reception PC—not an optional nicety—so the two paths that already write `return_documents` (scan-intake and email assign) are the ones front-desk actually uses every day; leave walk-in upload as backup. No schema required.
