# TaxOps Codebase Overview
> **Date:** May 2026 — Schema v4, 380 tests passing  
> **Purpose:** Comprehensive context document for AI-assisted ideation on next steps.

---

## 1. What the Application Is

**TaxOps** is an internal tax-office management platform built for a small US tax preparation firm.  
It runs as a **single Python process** (Flask + Waitress) on a Windows Server LAN machine (`192.168.1.173:5000`), managed by **NSSM** as a Windows service (`TaxOpsService`).  
There is no public internet exposure. Staff access it from workstations on the same LAN.

**Core jobs the software performs:**
- Track clients, tax returns, filing status, and payment collection across multiple tax years
- Watch a Gmail inbox for incoming client emails and auto-route PDF/image attachments to the correct return
- Run LLM-based document field extraction (W-2, 1099, paystub) and AI chat against an office data snapshot
- Import return data from Drake Tax (CSV export) and a legacy CSMDATA source
- Stage e-file batches and generate pickup/logout workflows
- Manage audit logs with configurable retention
- OCR business receipts and export categorized transactions to QuickBooks

---

## 2. Repository Layout

```
taxops/
├── app.py                    # ~5 500-line Flask core — routing, lifecycle, startup
│                              #   (/email-inbox routes live here — see §6)
├── routes/
│   ├── documents.py          # Document intake Blueprint (/api/documents/*, /admin/failed-docs)
│   └── accounting.py         # Accounting Blueprint (/accounting/*, /api/accounting/*)
├── db.py                     # Schema init, migrations (CURRENT_SCHEMA_VERSION = 4)
├── config.py                 # All env-var defaults (~80 variables)
├── auth.py                   # login_required decorator + session helpers
├── extractor.py              # Background extraction worker + queue logic
├── mail_watcher.py           # Background IMAP poller + email routing
├── accounting_worker.py      # Background receipt OCR worker
├── audit_service.py          # Single long-lived audit log writer (bounded queue)
├── name_matcher.py           # Fuzzy client name matching (rapidfuzz)
├── normalizer.py             # Data normalization utilities
├── llm.py                    # Ollama HTTP client wrapper
├── form_schema.py            # Tax form field definitions (W-2, 1099, paystub, etc.)
├── services/
│   ├── receipt_ocr.py        # Ollama vision OCR for business receipts
│   ├── coa_matcher.py        # Chart-of-accounts embedding matcher (sentence-transformers)
│   └── qb_export.py          # QuickBooks CSV/IIF export
├── backup.py                 # Database backup runner
├── smoke_test.py             # Production smoke probe script
├── season_rollover.py        # Year-end rollover helpers
├── multiyear_comparison.py   # AGI/refund year-over-year diff
├── bulk_returns.py           # Bulk status/processor updates
├── merge_ops.py              # Client-merge logic
├── preparer.py               # Preparer assignment helpers
├── utils.py                  # Shared utilities (file paths, SSN scrub, enqueue, etc.)
├── templates/                # Jinja2 HTML templates (35+)
├── static/
│   └── app.js                # Global JS helpers: _csrfFetch, escHtml, scheduleTableQuickFilter
├── tests/                    # 50 test files, 380 tests
├── scripts/                  # PowerShell + Python operational scripts
└── docs/
    ├── RUNBOOK.md            # Operations manual (10 sections)
    └── OFFICE_NETWORK.md     # LAN / firewall setup guide
```

---

## 3. Database Schema (SQLite, WAL mode)

Schema version: **4** (checked at startup via `app_settings`, surfaced in `/health`).

| Table | Purpose |
|-------|---------|
| `clients` | Master client list — last/first name, display name, SSN last-4 |
| `returns` | One row per tax year per client — status, processor, tax year |
| `return_forms` | Structured field storage per return (W-2 boxes, 1099 fields, etc.) |
| `payments` | Payment records linked to returns |
| `notes` | Return-level staff notes |
| `missing_docs` | Checklist of expected but not-yet-received documents |
| `status_events` | Audit trail for return status changes |
| `import_batches` / `import_rows` | CSV import history |
| `review_queue` | Rows needing manual review after CSV import |
| `dependents` | Dependent info per return |
| `efile_batches` / `efile_batch_items` | E-file batch management |
| `return_documents` | Files attached to returns — path, hash, source, extraction status |
| `extraction_queue` | Pending/completed LLM field-extraction jobs |
| `email_inbox` | Holding area for IMAP attachments awaiting staff assignment to a return |
| `email_processing_log` | Restart-safe IMAP UID dedup log — (uid, folder) → terminal outcome + `suppression_reason` |
| `email_sender_rules` | Staff allow/block rules for sender suppression, wired into `mail_watcher.py` suppression (see §6); managed via `/admin/sender-rules` |
| `archive_email_classifications` | **Dead, do not read/write.** Renamed from `email_classifications` (Phase 2.2) — history from the pre-simplification 6-layer classifier. See `pre-email-simplification` git tag for the historical implementation |
| `archive_domain_classifications` | **Dead, do not read/write.** Renamed from `domain_classifications` (Phase 2.2) — same era as above |
| `ai_chat_common_answers` | **Dead table** — disk cache for the removed AI chat assistant (see §5) |
| `audit_log` | Immutable write audit trail with retention |
| `app_settings` | Key-value store (schema_version, audit retention, etc.) |
| `auth_users` | Hashed login credentials (bcrypt via Werkzeug) |
| `receipt_queue` | Pending OCR + categorization jobs for business receipts |
| `dashboard_saved_filters` | Staff-saved dashboard filter presets |

**Schema migration pattern:** Additive-only `ALTER TABLE ADD COLUMN` in `_migrate_existing_tables()`, guarded by `PRAGMA table_info`. `CURRENT_SCHEMA_VERSION` is stored in `app_settings` and compared on every startup.

---

## 4. Background Workers (all daemon threads)

| Worker | Module | Wake mechanism | Purpose |
|--------|--------|----------------|---------|
| `extraction-worker` | `extractor.py` | `threading.Event` (signalled on upload) + 60 s fallback | Polls `extraction_queue`, runs Ollama text/vision extraction, writes `return_forms`, tags or queues for review |
| `mail-watcher` | `mail_watcher.py` | `time.sleep(IMAP_POLL_INTERVAL)` (default 120 s) | IMAP SEARCH UNSEEN → classify → route attachments to returns |
| `accounting-worker` | `accounting_worker.py` | `threading.Event` + 60 s fallback | Polls `receipt_queue`, runs OCR, categorizes via COA matcher |
| `audit-log-writer` | `audit_service.py` | Bounded `queue.Queue` with single consumer | Batches `audit_log` inserts; exposes backlog depth to `/health` |

All four threads are started from `register_workers(app)`, called at module import time (not inside `if __name__ == "__main__":`), so they start under any entry point.

Worker liveness is reported in `GET /health` → `workers.*`:
```json
"workers": {
  "extraction":   {"started": true, "running": true},
  "mail_watcher": {"started": true, "running": true, "configured": true, "poll_skipped": 0},
  "accounting":   {"started": true, "running": true}
}
```

---

## 5. LLM Integration (Ollama)

All LLM calls route through `llm.py` → HTTP POST to `OLLAMA_BASE_URL` (default `http://localhost:11434`).  
The GPU server is a separate box on the same LAN; the app talks to it over the network.

**Models configured:**

| Variable | Default | Used for |
|----------|---------|----------|
| `OLLAMA_MODEL` | `llama3.2` | Fallback |
| `OLLAMA_ROUTER_MODEL` | same | Chat intent routing |
| `OLLAMA_CHAT_MODEL` | same | Chat answers |
| `OLLAMA_EXTRACT_MODEL_TEXT` | `llama3.2` | Text-layer field extraction |
| `OLLAMA_EXTRACT_MODEL_VISION` | `llama3.2-vision` | Vision-layer extraction for scanned docs |
| Receipt OCR model | `llava` | Business receipt reading |

**Extraction pipeline** (`extractor.py`):
1. Pull item from `extraction_queue` (status = `pending`)
2. Try text extraction (`pdfplumber`) → LLM prompt → JSON fields
3. If text fails or low-confidence → vision path (`PyMuPDF` render → base64 image → vision LLM)
4. `_compute_confidence(fields, detected_type)` → float 0–1
5. Score ≥ 0.85 → auto-tag + write `return_forms`; below → status = `needs_review`
6. Max 3 attempts before `failed` (dead-letter)

**Chat pipeline:** removed along with the AI chat assistant subsystem (`ai_routes.py`,
`classifier.py`, `chat_cache.py`, `chat_training_log.py`, `rag.py`) in the same change that
simplified the email system — see `pre-email-simplification` git tag for the prior
implementation if this is ever revisited.

---

## 6. Mail Watcher Pipeline — holding-area model

The email pipeline was rewritten from a 6-layer ML classifier (sender-rule lookup, fuzzy
client matching, auto-confirmation queue) to a simpler holding-area model. There is no
automated client matching anywhere in the current design — staff manually pick the return.
The prior architecture is preserved at git tag `pre-email-simplification` for reference.

```
mail-watcher poll cycle (every IMAP_POLL_INTERVAL, default 120s)
  └─ IMAP UID SEARCH UNSEEN per folder (Gmail category folders skipped/routed per
     GMAIL_CATEGORY_FOLDERS; only INBOX gets full_processing by default)
       └─ dedup: in-process memo (_processed_uids, FIFO-capped) →
                 email_processing_log (authoritative; terminal outcomes never re-fetched)
       └─ _fetch_message_data()          ← BODY.PEEK[] — never RFC822, never marks \Seen
       └─ _suppression_decision()        ← no LLM; ≤1 email_sender_rules SELECT per cycle
                                            (_load_sender_rules), precedence:
                                            staff allow > staff block > _is_drive_share()
                                            > _is_promotional() (KNOWN_PROMOTIONAL_DOMAINS /
                                            MASS_MAILING_PREFIXES) > default pass-through
       └─ _save_to_inbox()               ← saves .pdf/.jpg/.jpeg/.png parts to EMAIL_INBOX_DIR,
                                            inserts one email_inbox row per file (is_assigned=0)
       └─ email_processing_log upsert    ← outcome: success | skip | no_attachment | retry
                                            + suppression_reason (sender_rule_block |
                                            drive_share | known_promotional | NULL)
```

**Staff assignment (`/email-inbox` UI):**
1. Staff views unassigned `email_inbox` rows, previews the file, picks a `return_id`
2. `POST /api/email-inbox/<id>/assign` copies the file into the return's document folder,
   inserts a `return_documents` row (`source='email_inbox'`, `match_confirmed=1`,
   `match_method='email_manual'` — set explicitly, never via column default), enqueues
   extraction, marks the `email_inbox` row `is_assigned=1`
3. `POST /api/email-inbox/<id>/delete` soft-deletes (`is_deleted=1`) — the file stays on disk

**Sender rules (Phase 2.1):** `email_sender_rules` (`rule_scope` domain|address, `action`
allow|block) is loaded once per poll cycle by `_load_sender_rules()` and consulted by
`_suppression_decision()` ahead of the hardcoded promotional lists. Personal email domains
(`gmail.com`, etc.) can only be blocked at the address level — enforced both at suppression
time and by the admin UI. Staff manage rules at `/admin/sender-rules`
(`routes/sender_rules.py`, Admin-only: list/add/remove).

**Manual-tagging indicator (Phase 2.3):** `utils.needs_manual_tagging(filename)` mirrors
`extractor.py`'s image-skip condition (`.jpg/.jpeg/.png` + `EXTRACTOR_VISION_ENABLED=false`).
`/api/email-inbox/items` and the `/email-inbox` page expose `needs_manual_tagging` per item;
`/return/<id>` shows a "Needs manual tag" badge on documents with `extraction_status='skipped'`
and no assigned form type.

**Diagnostic log per poll cycle:**
`Folder INBOX: 2 new of 5 unseen (3 already seen)`

---

## 7. Security Controls Implemented

| Control | Where | Notes |
|---------|-------|-------|
| CSRF protection | `Flask-WTF` + `_csrfFetch()` in `app.js` | All mutating endpoints; token from `<meta name="csrf-token">` |
| Hashed passwords | `auth_users` table, Werkzeug bcrypt | Replaces plaintext env-var credentials |
| Login rate limiting | `app.py` | 5 attempts → 15-minute lockout per IP |
| Session hardening | `app.py` | `HttpOnly`, `SameSite=Lax`, stable `TAXOPS_SECRET` in `.secret_key` file |
| Content Security Policy | `_security_headers()` | `default-src 'self'` with minimal exceptions |
| Upload limits | `MAX_CONTENT_LENGTH = 50 MB` | Extension + MIME type whitelist |
| SSN scrubbing | `scrub_ssn_from_dict()` in `utils.py` | Applied to all user-supplied strings before DB storage |
| WAL mode | `db.py` | Concurrent reader safety, no EXCLUSIVE lock during reads |
| `X-Frame-Options: DENY` | `_security_headers()` | Clickjacking prevention |
| Cache-Control static | `_security_headers()` | `no-store` for HTML/API; long-lived for `/static/` assets |
| Stable secret key | `.secret_key` file (gitignored) | Survives service restarts without invalidating sessions |

---

## 8. API Surface

### `app.py` (main routes, all `@login_required` except `/health` and `/login`)

**Pages:** `/` (dashboard), `/return/<id>`, `/clients/<id>`, `/intake`, `/upload`, `/review`, `/payments`, `/logout-queue`, `/efile-queue`, `/efile-batch`, `/merge-clients`, `/import-audit`, `/source-compare`, `/email-inbox`, `/admin/*`

**APIs:**
- `POST /api/return/<id>/field` — field update with type validation (REL-6)
- `POST /api/return/<id>/status` / `/note` / `/contact` / `/missing-doc`
- `GET /api/clients/search`, `GET /api/search` — global search
- `GET/POST /api/filters` — saved dashboard filters
- `POST /api/merge-clients*` — client merge
- `POST /api/efile-batch/*` — e-file batch management
- `POST /api/audit/merge-client`
- `POST /api/admin/backup/run` — on-demand backup
- `POST /api/admin/season-rollover/*`
- `GET /health` — anonymous, returns full system status JSON

### `routes/documents.py` Blueprint

- `GET /api/documents/<id>/status` — extraction status poll
- `POST /api/documents/<id>/delete`
- `POST /api/admin/documents/<id>/retry` — retry failed extraction
- `POST /api/returns/<id>/documents/upload` — multipart upload with dedup

### Email inbox routes (`app.py`, not a separate Blueprint)

- `GET /email-inbox` — staff holding-area page (unassigned attachments)
- `GET /api/email-inbox/items` — JSON list, excludes assigned/deleted, never includes `file_path`
- `GET /api/email-inbox/<id>/file` — serves the file, path-confined to `EMAIL_INBOX_DIR`
- `POST /api/email-inbox/<id>/assign` — copies file to the return, inserts `return_documents`
  (`match_confirmed=1, match_method='email_manual'`), enqueues extraction
- `POST /api/email-inbox/<id>/delete` — soft-delete (`is_deleted=1`), file stays on disk

### `routes/accounting.py` Blueprint

- `GET /accounting/receipts` / `/<id>` / `/<id>/image`
- `POST /accounting/receipts/upload`
- `POST /api/accounting/receipts/<id>/approve` / `reject`
- `POST /api/accounting/receipts/export` — QB CSV or IIF
- `GET/POST /accounting/settings`
- `POST /api/accounting/coa/rebuild`

---

## 9. Frontend Architecture

- **Tailwind CSS** (compiled to `static/app.css` via `scripts/build-tailwind.ps1`)
- **No frontend framework** — server-rendered Jinja2 templates + vanilla JS
- **`static/app.js`** — global helpers: `_csrfToken()`, `_csrfFetch()`, `escHtml()`, `scheduleTableQuickFilter()`, `taxops_asset_cache_version()`
- **All inline `<script>` blocks wrapped in IIFEs** to prevent `let`/`const` redeclaration errors across pages
- **ESLint** (`eslint.config.mjs`) enforces `no-redeclare`, `no-undef`, `no-var`, `no-unused-vars`
- **Asset cache busting** via `?v=<git-sha>` query string on all static references
- **Cache-Control** — `/static/` gets long-lived immutable; HTML/JSON gets `no-store`
- **Marked.js** used for LLM chat markdown rendering
- **No WebSockets** — all interactivity via polling (`fetch`) or page reload

**Key UI pages:**
| Page | Path | What it does |
|------|------|--------------|
| Dashboard | `/` | All clients, status filters, quick search, payment summary |
| Return Detail | `/return/<id>` | Documents, form fields, notes, extraction feedback |
| Review Queue | `/review` | Flagged extraction results needing human review |
| Email Inbox | `/email-inbox` | Unassigned IMAP attachments awaiting staff assignment to a return |
| Upload | `/upload` | CSV preview/confirm import |
| E-file Queue | `/efile-queue` | Returns ready for e-file |
| Accounting | `/accounting/receipts` | Receipt OCR queue + QB export |
| Admin | `/admin/*` | Backup, runbook, failed docs, season rollover, audit log |

---

## 10. Configuration (key env vars via NSSM `AppEnvironmentExtra`)

| Variable | Default | Notes |
|----------|---------|-------|
| `TAXOPS_DB` | `taxops.db` | SQLite file path |
| `TAXOPS_USER` / `TAXOPS_PASS` | — | Legacy single-user credentials (superceded by `auth_users` table) |
| `TAXOPS_SECRET` | file `.secret_key` | Flask session signing key — must persist across restarts |
| `TAXOPS_VERSION` | git SHA | Version string shown in `/health` and footer |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama API endpoint |
| `OLLAMA_MODEL` / `OLLAMA_CHAT_MODEL` / `OLLAMA_EXTRACT_MODEL_*` | `llama3.2` | Per-task model tags |
| `IMAP_HOST` / `IMAP_PORT` / `IMAP_USER` / `IMAP_PASS` | — | Gmail IMAP credentials |
| `IMAP_POLL_INTERVAL` | `120` | Seconds between mail polls |
| `MAIL_LOW_CONF_THRESHOLD` | `88` | Below this score → `pending_review` |
| `MAIL_WATCHER_CLIENT_MATCH_MIN_SCORE` | `82` | Below this → no routing |
| `WAITRESS_THREADS` | `8` | WSGI thread pool size |
| `DOCUMENTS_BASE_PATH` | `./documents` | Root for uploaded files |
| `TAXOPS_BACKUP_DIR` | `C:\TaxOps\backups` | Nightly backup destination |
| `DRAKE_DOCUMENTS_PATH` | — | If set, enables Drake document sync |
| `AUDIT_LOGGING_ENABLED` | `true` | Toggle audit write path |
| `AUDIT_RETENTION_YEARS` | `7` | Audit log retention |

---

## 11. Test Suite (380 tests across 50 files)

**Coverage areas:**

| File(s) | What's covered |
|---------|----------------|
| `test_health.py` | `/health` endpoint, worker liveness fields |
| `test_auth_users.py`, `test_csrf.py`, `test_session_security.py`, `test_login_lockout.py` | SEC-1..7 security controls |
| `test_upload_limit.py` | File size + MIME type rejection |
| `test_rel1..6_*.py` | LLM SLA, audit queue, worker startup, DB lifecycle, extraction event, field validators |
| `test_debt2..8_*.py` | Test coverage gaps, name matcher, cache-control, schema version, LRU cap, confidence scoring |
| `test_doc_hard.py` | Document intake hardening (dedup, failed docs, audit trail, bulk upload) |
| `test_mail_attachments.py`, `test_email_stability.py` | Mail watcher UID dedup, diagnostic counters, skip counter, match_score storage, pending-review API |
| `test_accounting.py` | Receipt OCR pipeline, COA matcher, QB export, schema |
| `test_backup_admin.py`, `test_nightly_backup_db.py` | Backup + retention |
| `test_smoke.py`, `test_smoke_deploy.py` | Smoke test probe script |
| `test_audit_service.py`, `test_audit_log_schema.py` | Audit writer queue, schema |
| `test_season_rollover.py`, `test_multiyear_comparison.py`, `test_bulk_returns.py` | Business logic |
| `test_db_wal.py`, `test_db_indexes.py` | WAL mode, index existence |
| `test_debt6_schema_version.py` | Schema version tracking in `app_settings` |

**Known gaps** (not yet covered by tests):
- AI chat routes (mocking Ollama in tests is complex)
- Drake importer edge cases
- E-file batch workflow end-to-end
- Dashboard filter persistence
- Multi-user concurrent write scenarios

---

## 12. Operational Tooling

| Script | What it does |
|--------|--------------|
| `scripts/restart_service.ps1` | Stop → start `TaxOpsService` → smoke test → print `/health` JSON |
| `restart_taxops.bat` | Dev restart (polls port 5000 then runs smoke test) |
| `scripts/nightly_backup_db.py` | Copies `taxops.db` to `C:\TaxOps\backups\taxops_YYYYMMDD_HHMMSS.db`, prunes >30 days |
| `scripts/register-nightly-backup-task.ps1` | Registers Windows Task Scheduler job at 2 AM |
| `scripts/smoke_waitress_concurrency.py` | Concurrent load smoke test |
| `scripts/build-tailwind.ps1` | Rebuilds `static/app.css` from Tailwind source |
| `scripts/nssm-taxops-snippet.ps1` | Full NSSM install + all env vars reference |
| `scripts/nssm-set-ollama-url.ps1` | One-liner to update `OLLAMA_BASE_URL` in NSSM |
| `smoke_test.py` | Standalone probe: `/health`, `/`, `/login`, `/review`, `/payments`, `/ai/status` |
| `backup.py` | Callable from admin UI (`Tools → Backup`) |
| `docs/RUNBOOK.md` | 12-section ops manual (restart, backup, smoke, IMAP setup, health, Gmail rotation) |

---

## 13. Known Open Issues / Technical Debt Still on Backlog

*(From GitHub project board — not yet implemented)*

**Security**
- No role-based access control beyond admin/staff distinction
- No per-user action logging (audit log has action type but not always attributed user)
- Session fixation on login not explicitly reset

**Reliability**
- `app.py` is still ~5 500 lines — DEBT-1 blueprint refactor partially done (4 blueprints extracted), but payments, dashboard, efile, and admin routes still inline
- No retry/dead-letter for `accounting_worker` (unlike `extraction_queue` which has max 3 attempts)
- Chat LLM calls still synchronous in request handler (REL-1 — 202+poll partially mitigated by threadpool but not fully async)
- No health degraded → alert path (no webhook/email on `/health` returning 503)

**Performance**
- Dashboard still has N+1 patterns for large return lists (DEBT-4 partially addressed)
- No pagination on `/` dashboard — loads all clients

**Features not yet built**
- Multi-user login (currently single or small `auth_users` set, no invite flow)
- Client portal / self-service document upload
- Email reply / outbound email from draft (`/ai/return/<id>/draft-email` creates draft but no send)
- E-file transmission (batch UI exists but actual transmission to IRS is a stub)
- Two-factor authentication
- Mobile-responsive layouts (current CSS is desktop-first)
- PDF generation for client letters/pickup slips beyond the current simple FPDF output
- Notification system (in-app or email) for staff when new documents arrive

**Data**
- `return_forms` field storage is a flat key-value blob per form type — no relational integrity on individual box values
- No soft-delete on clients or returns (only on `return_documents`)
- Drake sync is one-directional (import only; edits in TaxOps don't push back)

---

## 14. Data Flow Diagrams (text)

### Document lifecycle
```
Staff uploads / email arrives
        │
        ▼
return_documents (source='upload'|'email'|'mail_pending_review')
        │
        ▼
extraction_queue (status='pending')
        │    ← signalled immediately via threading.Event
        ▼
extraction-worker
  ├─ text path: pdfplumber → LLM → JSON fields
  └─ vision path: PyMuPDF render → vision LLM → JSON fields
        │
        ├─ confidence ≥ 0.85 → return_forms + auto-tag
        └─ confidence < 0.85 → extraction_queue status='needs_review'
                                       │
                                       ▼
                               /review page (staff)
                                       │
                                       ▼
                               Review resolved → return_forms
```

### Mail watcher lifecycle (holding-area model)
```
Gmail IMAP (every IMAP_POLL_INTERVAL, default 120 s)
        │
        ▼
dedup (memo → email_processing_log) → BODY.PEEK[] fetch
        │
        ├─ _is_promotional() / _is_drive_share() → log + skip (no LLM, no matching)
        └─ has attachment → _save_to_inbox()
                                 │
                                 ▼
                          email_inbox (is_assigned=0)
                                 │
                                 ▼
                       staff reviews at /email-inbox, picks return
                                 │
                                 ▼
              POST /api/email-inbox/<id>/assign
                    → return_documents (match_confirmed=1, match_method='email_manual')
                    → extraction_queue (auto-enqueued)
```

### Receipt accounting lifecycle
```
Staff uploads receipt image / mail_watcher detects receipt doc_type
        │
        ▼
receipt_queue (status='pending')
        │
        ▼
accounting-worker
  └─ receipt_ocr.py → Ollama vision → {vendor, date, amount, description}
  └─ coa_matcher.py → sentence-transformers embeddings → QB category
        │
        ▼
receipt_queue (status='needs_review', ocr_data, suggested_category)
        │
        ▼
/accounting/receipts → staff approve / reject / override category
        │
        ▼
/api/accounting/receipts/export → QB CSV or IIF download
```

---

## 15. Things That Work Well (preserve these)

- **Zero-dependency deployment** — single Python process, SQLite, no Redis/Celery/Postgres
- **Deterministic extraction confidence** — `_compute_confidence` is documented, tested, and tunable
- **Pre-UID-claim dedup** in mail watcher — no double dispatch even under concurrent polls
- **Stable CSRF** — `_csrfFetch` wrapper used consistently across all 35+ templates
- **Bounded memory** — `_processed_uids` LRU cap, audit writer bounded queue, LLM cache LRU
- **Schema migrations** are additive-only and idempotent — safe to restart mid-deploy
- **`/health` endpoint** surfaces DB, worker liveness, schema version, queue depth — machine-readable for any monitoring tool
- **All inline JS in IIFEs** — no global scope pollution, ESLint enforced
- **RUNBOOK.md** covers every common ops scenario with copy-pasteable commands
