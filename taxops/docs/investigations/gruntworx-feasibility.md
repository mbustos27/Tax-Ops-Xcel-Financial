# GruntWorx feasibility in TaxOps

**Date:** 2026-07-31  
**Mode:** Read-only investigation (no code, migrations, or schema proposals).  
**DB queried:** `T:\taxops\taxops.db` (read-only URI). Path samples and document disk tree also inspected under `T:\taxops\documents\`.

---

## 1. `extraction_queue` — does it already do this job?

### Finding

`extraction_queue` is a **working, incomplete extraction pipeline** aimed at tagging uploaded return documents (W-2 / 1099 family / paystub hints) and optionally persisting typed form rows. It is **not** a GruntWorx-style return assembly / Populate-XML pipeline. The consumer path is fully wired (enqueue → daemon worker → extract → persist or `needs_review`), but production data shows almost no successful completions and empty typed form tables.

### Evidence

**Schema** (`db.py:427-442`):

```
extraction_queue (
  id, doc_id → return_documents, return_id → returns,
  status DEFAULT 'pending', confidence, detected_form_type,
  extracted_fields, extraction_method, error_message,
  attempts DEFAULT 0, created_at, processed_at, reviewed_by, reviewed_at
)
```

Related persist targets (not part of `extraction_queue` DDL, but written by the worker via `form_store`):

- `return_documents.doc_type` (auto-tag when unset/unknown) — `form_store.py:51-77`
- Typed tables: `w2_records`, `f1099_nec_records`, `f1099_misc_records`, `f1099_int_records`, `f1099_div_records` — `form_store.py:27-35`, `_save_form_data` at `form_store.py:91+`
- FTS index via `document_fts.index_document_text` after processing — `extractor.py:567+`
- Staff confirm path: `POST .../confirm-extraction` — `routes/documents.py:1027-1048`

**Consumer end-to-end:**

1. Enqueue: `utils._enqueue_extraction` inserts `status='pending'` if no row for `doc_id`, then wakes worker — `utils.py:219-252`
2. Call sites: walk-in / scan upload — `routes/documents.py:278,450,769`; email assign copy — `app.py:2800`
3. Startup: `start_extraction_worker(flask_app)` — `app.py:7787`
4. Loop: `_worker_loop` waits on event / poll → `_process_queue` selects up to 10 `pending` with `attempts < MAX` — `extractor.py:243-304`
5. Item: `_process_item` → `_extract_fields` → confidence gate → `_save_form_data` / tag or `needs_review` / retry / `failed` / `skipped` — `extractor.py:340-565`, vision skip at `402-419`

**Document types / emit shape:**

- Resolved form tables: W-2, 1099-NEC/MISC/INT/DIV, paystub (tag only, no typed SQL row) — `extractor.py:307-328`, `form_store.py:38-48`
- Emit: **structured field dict** (JSON in `extracted_fields`, scrubbed of SSN) — not raw OCR dump alone; methods `native` / `text` / `vision` / Claude for `scan_agent` — `extractor.py:842-940`, `HIGH_CONFIDENCE = 0.85` at `extractor.py:190`

**Where output lands:**

- High confidence + successful save → typed form table + optional `return_documents.doc_type` + queue `completed` — `extractor.py:456-510`
- Else → `needs_review` with JSON fields retained on the queue row — `extractor.py:539-565`
- **Not** written onto `returns` columns as a form payload. Paystub never gets a typed table row — `extractor.py:457-459`

**`EXTRACTOR_VISION_ENABLED`:**

- Default `false` — `config.py:334`
- When false: raster `.jpg/.jpeg/.png` (non–scan-agent) return `(None, "image_skipped")` → queue `skipped` — `extractor.py:921-929`, `402-419`; image-only PDFs without text also skip — `extractor.py:900-905`
- When true (or `source == 'scan_agent'`): vision / Claude path runs — `extractor.py:860-863`, `878-880`

**Live DB (2026-07-31):**

| Metric | Value |
|--------|-------|
| `extraction_queue` rows | 3 (`failed`×2, stuck `processing`×1) |
| Typed form tables (`w2_records`, `f1099_*`) | all **0** rows |

**Bottom line:** Consumer path is **complete and started with the app** — not a stub. Operationally it is **barely exercised / failing** on the two live scan PDFs; it does **not** assemble GruntWorx packets or drive Drake Populate XML.

### Confidence

**High** for architecture and defaults; **high** for empty typed-table / queue outcome on this DB.

---

## 2. Drake client identity mapping

### Finding

Drake CSV rows are identified by **taxpayer name** (and optionally **SSN last4** on CSM format). `find_client` matches TaxOps `clients` by normalized last/first name (exact → fuzzy). Match outcome is **not** stored as a Drake client ID; it **upserts** `clients` / `returns` identity fields. There is **no stable key** that deterministically maps a TaxOps return to a Drake Documents client folder.

### Evidence

**CSV identity** (`drake_importer.py:8-17`, field maps `58-91`):

- CSM: `ID (Last 4)` → `ssn_last4`, `Client Name` → split last/first, `Type`, `Status`, …
- TAX_OPS: `Taxpayer Last Name` / `Taxpayer First Name`, no SSN in that format (`ssn_last4: None` at `drake_importer.py:387`)

**`find_client`** (`name_matcher.py:225-298`):

- Matches on normalized last + first (exact, middle-initial-stripped, fuzzy full / fuzzy last)
- Thresholds: `ACCEPT_THRESHOLD = 88`, `REVIEW_THRESHOLD = 70` — `name_matcher.py:24-25`
- Returns `{client_id, score, method, needs_review}` — no Drake folder key

**Drake importer match path** (`drake_importer.py:500-549`):

- Prefer unique SQL match on name + `tax_year` (SSN last4 disambiguates multiples)
- Else fuzzy if `not needs_review` and `score >= ACCEPT_THRESHOLD`
- Ambiguous → review path via `_insert_review_row` — `drake_importer.py:850-857`

**Persisted columns (not a Drake ID):**

- `clients`: `last_name`, `first_name`, `ssn_last4`, … — `db.py:154-164`; upsert fills names/SSN — `drake_importer.py:552-598`
- `returns`: workflow fields + `drake_status_raw` (raw E-Filed text, not a client key) — `db.py:166-194`, set at `drake_importer.py:401`
- `return_forms` flags from Drake `Type` via `DRAKE_TYPE_FORMS` — `config.py:658-667`
- Import match **score/method** land on `review_queue` when deferred (`match_score`, `match_method`, `proposed_client_id`) — `db.py:285-304`; auto-accept path binds to existing/created `client_id` without a dedicated “drake_match_*” column on `clients`/`returns`

**What goes to `review_queue` vs auto-accept:**

- Score ≥ 88 and `needs_review=False` → auto-bind client — `drake_importer.py:537-547`, `name_matcher.py:24-25,296-297`
- Score in [70, 88) → `needs_review=True` from matcher; importer review insert for ambiguous/unresolved rows — `name_matcher.py:13-14,290-297`, `_insert_review_row` `drake_importer.py:850-857`
- Below 70 → no fuzzy client; importer creates/links via other paths (new client upsert) — `name_matcher.py:14-15,290-291`

**Stable Drake folder key?**

- **No.** TaxOps `clients.id` / `returns.id` / `log_number` are internal. Staging sync invents folders from name + `client_id` / log + `return_id` — `drake_documents_sync.py:36-54`, `utils.get_drake_documents_path` uses `LastName_ReturnID` — `utils.py:65-91`. Neither is Drake’s native Documents cabinet key. No `drake_client_id` (or equivalent) column on `clients`/`returns`.

### Confidence

**High** for match mechanics and absence of a Drake folder id; **medium** for every edge-case review reason string (not fully enumerated here).

---

## 3. `missing_docs` — trustworthy completeness signal?

### Finding

**No.** The table is a manual checklist (intake + preparer API). It is almost unused in production data. “`missing_docs` empty” is **not** a reliable precondition for an expensive external submission.

### Evidence

**Schema** (`db.py:235-243`):

```
missing_docs (id, return_id, item_text, is_resolved DEFAULT 0, created_at, resolved_at)
```

**Write sites:**

| Site | Action | Location |
|------|--------|----------|
| Intake form | `INSERT` unresolved items | `app.py:3420-3434` |
| API add | `INSERT` | `app.py:5333-5348` (`@role_required("preparer")`) |
| API toggle | `UPDATE is_resolved` / `resolved_at` | `app.py:5352-5370` |
| API delete | `DELETE` | `app.py:5373-5380` |
| Client merge | reassigns rows | `merge_ops.py:107` |

No automated population from document inventory or Drake forms was found.

**Populated → empty path:** staff toggle resolve and/or delete via return UI APIs (`app.py:5352-5380`). Intake only inserts; nothing auto-clears when a matching `return_documents` row appears.

**Live DB query:**

| Scope | Returns | With any `missing_docs` row |
|-------|---------|----------------------------|
| All returns | 1748 | **1** (return_id 2696; 4 open items: W-2, 1098, bank routing/account) |
| tax_year 2025 | 1413 | **1** |
| By `client_status` | — | only that one `PROCESSING` return |

Brokerage-related `item_text` patterns (`1099-B`, broker, consolidated): **0** rows.

### Confidence

**High**.

---

## 4. `return_documents` — can we assemble a merged PDF?

### Finding

**Technically yes for PDF merge tooling; practically no as a season-scale corpus today.** Schema and ingest keep originals on disk with absolute paths. Live DB has essentially no assigned documents. Dependencies include **PyMuPDF (`fitz`)**, **pdfplumber**, **Pillow**, **fpdf2** — sufficient to merge PDFs (scan agent already concatenates page PDFs with `fitz`). There is **no** existing “merge all docs for return → one GruntWorx PDF” feature.

### Evidence

**Schema** (base `db.py:377-391` + migrations for hash/match/OCR flags — live `PRAGMA`):

`id, return_id, filename, original_filename, doc_type, source, file_path, file_size_bytes, file_hash, uploaded_by, uploaded_at, notes, is_deleted, match_confirmed, match_score, match_method, ocr_text_indexed`

**Path storage:** absolute Windows path written at upload — `routes/documents.py:214-272` (`full_path = os.path.abspath(...)` stored in `file_path`). Physical tree: `{DOCUMENTS_BASE_PATH}/returns/{return_id}/` — `utils.py:58-62`; default `DOCUMENTS_BASE_PATH` = `taxops/documents` — `config.py:259`.

**Live path samples:** `C:\TaxOps\taxops\documents\returns\2695\...` (service-local). From the share view, `T:\taxops\documents\returns` exists; `C:\TaxOps\...` is **not** visible on this workstation (`DISK_MISS`) — path reachability is host-dependent.

**Originals vs transform:** upload `save()` of original bytes to sanitized filename — `routes/documents.py:228-229`. Scan agent may already produce PDF before upload (`scan_agent` + `fitz` page merge — `scan_agent/server.py:492-512`). No general ingest conversion of all formats to a single PDF.

**DB mix by `match_method` (active docs):**

| match_method | format | n |
|--------------|--------|---|
| `scan_agent` | pdf | **2** |

(No jpg/png rows in `return_documents`.)

**Doc-count distribution (DB, returns with docs):** n=2, median=1, p90=1, max=1.

**Disk under `T:\taxops\documents\returns` (not the same as DB):** 624 PDFs across 8 nonempty folders (top IDs `6001`/`8002` with 154 files each — look like bulk/test dumps; only `2695`/`2696` align with the two live DB docs). **Do not treat disk orphans as a trustworthy inventory** without DB rows.

**Email holding area (pre-assign):** `email_inbox` = 236 rows, **0 assigned**; files under `documents/email_inbox` with absolute `C:\TaxOps\...` paths. Until `/api/email-inbox/<id>/assign`, they are **not** in `return_documents`.

**PDF libraries:** `requirements.txt` lines 6-9: `pdfplumber`, `pymupdf`, `Pillow`, `fpdf2`. Imports: `fitz` in `form_store.py`, `ocr/claude_extract.py`, `scan_agent/server.py`; `pdfplumber` in `form_store.py`; `fpdf` in `multiyear_comparison.py`. No `pypdf` / `pikepdf` / `reportlab` / ghostscript in requirements.

### Confidence

**High** for schema/ingest/deps; **high** that DB cannot support season-scale merge today; **medium** on whether orphan folders are disposable test data (names suggest yes, not proven).

---

## 5. Drake Documents filesystem reachability

### Finding

Code and templates reference **optional env-configured staging paths** only. No hardcoded `DrakeXX` cabinet path. Service identity / write ACL to a live Drake share on `\\Xcel-server` is **UNKNOWN** from repo artifacts.

### Evidence

| Knob | Role | Source |
|------|------|--------|
| `DRAKE_DOCUMENTS_PATH` | Bulk copy + manifest (`sync_to_drake`); blank → API 400 | `config.py:277-279`, `app.py:4873-4890`, `.env.example:94` example `G:/Drake/DT/Data` |
| `DRAKE_DOCUMENTS_BASE` + `DRAKE_FOLDER_STRUCTURE_ENABLED` (default false) | Per-return mirror `TaxYear/LastName_ReturnID` | `config.py:281-285`, `utils.py:65-91`, `.env.example:87-91` example `C:\Drake\Documents` |

Sync is **file copy only; no Drake API** — `drake_documents_sync.py:1-13`, `70-77`.

Repo/docs search: `DRAKE_*` in config/overview/runbook/.env.example; **no** `DrakeXX` string. `OFFICE_NETWORK.md` covers LAN bind/firewall for TaxOps HTTP — **not** Drake Documents ACLs (`docs/OFFICE_NETWORK.md:1-72`). NSSM snippet sets `AppDirectory` / env vars but **does not set `ObjectName`** (service account) — `scripts/nssm-taxops-snippet.ps1:15-47`. RUNBOOK names service `TaxOpsService` and log paths under `C:\TaxOps\logs` — `docs/RUNBOOK.md:5,111-112`.

Live `.env` on the share: **no** `DRAKE_*` lines found (grep). Whether NSSM `AppEnvironmentExtra` sets them on the host: **UNKNOWN** — verify with `nssm dump TaxOpsService` on the TaxOps host.

Write access to Drake on `\\Xcel-server`: **UNKNOWN** — requires runtime ACL test as the service identity against the real Drake Documents root.

### Confidence

**High** for “staging env vars exist, API is copy-only”; **UNKNOWN** for production path values and NTFS/share permissions.

---

## 6. Where an async job table would fit

### Finding

**Better template: `extraction_queue`** (async worker, attempts, status machine, external processing). **`import_batches` / `import_rows`** model one-shot CSV ingest, not long-lived external jobs. Do **not** overload `client_status`. Existing shadow/adjacent status fields already complicate the “one workflow status” rule.

### Evidence

**`extraction_queue` lifecycle** (doc-scoped):

`pending` → `processing` → (`completed` | `needs_review` | `skipped` | `failed`), with retry via reset to `pending` — `extractor.py:282-448`, enqueue `utils.py:234-241`. Worker is return-document scoped (`doc_id` + `return_id`).

**`import_batches` / `import_rows` lifecycle** (file-scoped, synchronous in `main.py`):

- Insert batch `status='PROCESSING'` — `main.py:135-144`
- On success → `SUCCESS` + counters; on exception → `FAILED` — `main.py:80-132`
- Per-row audit in `import_rows` (`action`, `error`) — `db.py:275-283`, `drake_importer.py:860-867`
- Live batches show legacy `status='ok'` with zeroed counters — import history, not a job runner

**Why extraction_queue wins as a template:** daemon thread, wake-on-enqueue, attempts/dead-letter, processed timestamps, human `needs_review` — same shape as “submit externally → wait → record cost/result.” Import batches lack polling workers and return-scoped job identity.

**Also note (pattern, not asked as schema):** `efile_batches` / `efile_batch_items` are the closest **return-scoped worklist** UI pattern (`open` → transmit → `closed`) — `db.py:344-375`, queue at `app.py:2899-2943`. `receipt_queue` is another async OCR queue — `db.py:475-504`.

**Return-level fields that must not become a second GruntWorx workflow status:**

| Field | Role | Risk |
|-------|------|------|
| `returns.client_status` | Canonical workflow | Do not fork |
| `returns.filetrack_status` (+ history) | Scan shadow; **also writes `client_status`** since 2026-07-22 | Already a dual-write pattern — `filetrack_service.py:73-119` |
| `returns.contact_status` / `last_contacted_date` | REJECTED contact workflow | Adjacent status — `db.py:189-190` |
| `returns.drake_status_raw` | Imported Drake text crumb | Not a job state machine, but status-adjacent — `db.py:188` |
| Extension ack fields used by `/extension-queue` | Separate filing track | Filter flags, not `client_status` — `app.py:6526-6528` |

### Confidence

**High**.

---

## 7. RBAC and UI surface

### Finding

Closest existing permission for a receptionist-level GruntWorx **submit worklist** is **`can_manage_efile_queue`**. Established pattern: route in `app.py` (or a small blueprint) + template + filter by year/status + optional CSV export — mirrored by `/efile-queue` and `/logout` (pickup).

### Evidence

**`ROLE_PERMISSIONS`** (`config.py:561-577`):

| Permission | Roles |
|------------|-------|
| `can_manage_efile_queue` | receptionist, preparer, admin |
| `can_manage_extension_queue` | preparer, admin |
| `can_use_email_tools` | admin |
| `can_manage_compliance_filings` | preparer, admin |
| `can_scan_intake_docs` | receptionist, preparer, admin |

**Closest fit:** `can_manage_efile_queue` — comment literally: “Receptionist may view and work the pickup queue and e-file queue” — `config.py:562-563`. Extension queue is preparer+. Email tools admin-only. Scan permission is device upload, not a batch worklist.

**Queue/worklist homes today:**

| View | Route | Gate | Filter pattern |
|------|-------|------|----------------|
| E-file | `/efile-queue` | `can_manage_efile_queue` | `client_status = 'EFILE READY'` + year — `app.py:2899-2943` |
| Pickup / logout | pickup & `/logout` queue | same permission family | `client_status = 'PICKUP'` — `app.py:2877-2885` |
| Extension | `/extension-queue` | `can_manage_extension_queue` | `extension_requested` + ack filter — `app.py:6515-6540` |
| Email inbox | `/email-inbox` | `can_use_email_tools` | holding-area filters — `app.py:2628+` |
| Import review | `review_queue_page` | (import ops) | `review_queue.status='pending'` — `app.py:4265-4278` |

Blueprints already used for documents/filetrack/etc. (`app.py:175-181`); e-file/extension queues still live as routes on `app.py` with `render_template(...)` + `base_ctx(year)`.

### Confidence

**High**.

---

## 8. Volume and mix (pricing)

### Finding

Most recent substantial season in DB is **tax_year 2025** (1413 returns). Roughly half of `return_forms` rows mark 1040; entity flags are small but non-zero. **No brokerage signal** in `missing_docs` / `return_forms` (no 1099-B column). Document attachment volume in `return_documents` is negligible — pricing GruntWorx on “docs already in TaxOps” is not supported by current data.

### Evidence

**Query source:** `T:\taxops\taxops.db` read-only, 2026-07-31.

**tax_year 2025 — totals / `client_status`:**

| client_status | n |
|---------------|---|
| LOG OUT | 932 |
| PROCESSING | 371 |
| PICKUP | 70 |
| FINALIZE | 34 |
| HOLD | 2 |
| EFILE | 2 |
| PICK UP | 1 |
| CANCELLED | 1 |
| **Total** | **1413** |

(Other years: 2024=132, 2026=16, … — 2025 is the clear season corpus.)

**`return_forms` for TY2025** (1390 rows; 23 returns lack a forms row):

| Flag | Count |
|------|------:|
| `form_1040 = 1` | 645 |
| `form_1040 = 0` | 15 |
| `form_1040 IS NULL` | 730 |
| `form_1120 = 1` | 21 |
| `form_1120s = 1` | 12 |
| `form_1065_llc = 1` | 9 |
| Entity union (1120\|1120S\|1065) | 42 |
| `form_990_1041 = 1` | 8 |
| `sched_c = 1` | 69 |
| `sched_e = 1` | 57 |
| `sched_a_d = 1` | 115 |

**Caveat:** 730 NULL `form_1040` means “not marked,” not “proven non-1040.” Entity coverage for GruntWorx should treat **≥42** entity-flagged returns as a lower bound, not the full non-1040 population.

**Brokerage (1099-B / consolidated):** no column on `return_forms`; `missing_docs` text search → **0** hits. **UNKNOWN** true brokerage mix — would need Drake export fields, doc_type taxonomy, or staff sampling.

### Confidence

**High** for counts; **medium** for 1040 vs entity split (nulls); **UNKNOWN** for brokerage-heavy fraction.

---

## Blockers

1. **No Drake Documents identity key** — cannot deterministically write into the real Drake client cabinet from TaxOps `return_id` / name alone (`§2`, `§5`).
2. **`return_documents` is not a season document store today** — 2 active rows vs 1413 TY2025 returns; 236 email_inbox files still unassigned (`§4`). Assembly from TaxOps inventory is not feasible at volume without changing intake/assign behavior.
3. **Drake path + service ACL unverified** — `DRAKE_DOCUMENTS_*` unset in share `.env`; NSSM account and write rights to Drake share **UNKNOWN** (`§5`).
4. **GruntWorx submit/import remain UI-driven in Drake** — TaxOps can at best stage files and emit a worklist; it cannot close the loop to Populate XML (`context` + `drake_documents_sync.py` copy-only).
5. **`missing_docs` cannot gate eligibility** — 1/1748 returns populated (`§3`).
6. **`extraction_queue` is the wrong product surface** — field extraction / tagging, not packet assembly; typed tables empty (`§1`).

---

## Open questions (human / runtime)

1. On the TaxOps NSSM host: `nssm dump TaxOpsService` — is `ObjectName` LocalSystem or a domain user? Are `DRAKE_DOCUMENTS_PATH` / `DRAKE_DOCUMENTS_BASE` set?
2. From that service identity: can it create a file under the real Drake Documents data root (path IT names for this office)?
3. What is the office’s actual Drake Documents folder naming convention vs TaxOps staging (`LastName_fn_c{id}` vs Drake cabinet)?
4. Are the large `documents/returns/{6001,8002,...}` trees disposable test data, or a missing DB registration bug?
5. Why are **0 / 236** `email_inbox` items assigned — process gap or intentional hold?
6. For TY2025, sample N returns: how many source documents live only in Drake / paper / email and never in TaxOps?
7. Confirm brokerage-heavy volume from Drake CSM/Tax Ops export or staff estimate (not in DB).
8. Clarify whether “complete tax_year” for pricing should be 2025 LOG OUT–heavy mix or only in-progress PROCESSING/PICKUP (pricing denominator).

---

## Smallest viable slice

**Staff worklist only — no merge, no Drake write, no new workflow status:**

1. Read-only page gated by `can_manage_efile_queue`, filtered like `/efile-queue` (e.g. `client_status='PROCESSING'` + optional `return_forms.form_1040=1` + has ≥1 `return_documents` **once that inventory exists**).
2. Columns: log number, client name, tax year, doc count, link to return documents UI.
3. Optional CSV export (same pattern as `efile_queue_export` — `app.py:2946+`).
4. Track GruntWorx job/cost **outside** `client_status` (separate concern; do not design schema here).

That validates eligibility UX and volume assumptions before investing in PDF assembly or Drake staging permissions. Assembly/sync only becomes meaningful after documents are consistently landed on `return_documents` (email assign + scan/upload) and Drake write access is proven.
