# TaxOps Dev Board — Backlog (Project 1, mbustos27)

> Generated 2026-05-19 · 70 issues · Status: **Backlog**

---

## #145 — CACHE-4: Verify cache busting works across all office workstations after restart

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/145  
**Labels:** cache  

### Description

Part of #141

---

## #141 — Epic: Static Asset Cache Busting

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/141  
**Labels:** epic, cache  

### Description

Goal: Ensure staff always get fresh JS/CSS after every deploy without manual hard refresh.
Child issues: CACHE-1 through CACHE-4

---

## #142 — CACHE-1: Add APP_VERSION and SEND_FILE_MAX_AGE_DEFAULT=0 to app.py

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/142  
**Labels:** cache  

### Description

Part of #141

---

## #143 — CACHE-2: Update all script and link tags in templates to use url_for + version query string

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/143  
**Labels:** cache  

### Description

Part of #141

---

## #144 — CACHE-3: Add version bump step to deployment and restart documentation

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/144  
**Labels:** cache  

### Description

Part of #141

---

## #146 — Epic: Frontend JS Stability

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/146  
**Labels:** epic, js  

### Description

Goal: Fix all client-side JavaScript errors currently being logged to /api/client-error.
Child issues: JS-1 through JS-5

---

## #147 — JS-1: Fix duplicate _searchTimer declaration on /review page (SyntaxError)

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/147  
**Labels:** js  

### Description

Part of #146

---

## #148 — JS-2: Audit all templates for duplicate let/const declarations across included scripts

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/148  
**Labels:** js  

### Description

Part of #146

---

## #149 — JS-3: Add ESLint or similar static check to catch duplicate declarations before deploy

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/149  
**Labels:** js  

### Description

Part of #146

---

## #150 — JS-4: Fix navbar search bar flex layout ΓÇö search input squished by adjacent elements

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/150  
**Labels:** js  

### Description

Part of #146

---

## #151 — JS-5: Verify all pages load without console errors on Chrome and Edge at 1080p

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/151  
**Labels:** js  

### Description

Part of #146

---

## #174 — EMAIL-4: Add previous poll still running ΓÇö skipping log when _poll_lock is held

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/174  
**Labels:** email-matching  

### Description

Part of #170

---

## #175 — EMAIL-5: Integration test ΓÇö send same email UID twice in sequence, assert single dispatch

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/175  
**Labels:** email-matching  

### Description

Part of #170

---

## #176 — EMAIL-6: Add email match confidence score to document intake UI

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/176  
**Labels:** email-matching  

### Description

Part of #170

---

## #177 — EMAIL-7: Staff confirm/reject UI for low-confidence matches before attaching to a return

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/177  
**Labels:** email-matching  

### Description

Part of #170

---

## #178 — Epic: Automated Backup

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/178  
**Labels:** epic, backup  

### Description

Goal: Nightly database backup with retention and failure alerting.
Corresponds to PROD-4.
Child issues: BACKUP-1 through BACKUP-6

---

## #179 — BACKUP-1: Write backup.py ΓÇö database copy to C:\TaxOps\backups\ with datestamp filename

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/179  
**Labels:** backup  

### Description

Part of #178

---

## #180 — BACKUP-2: Windows Task Scheduler job runs backup.py nightly at 2am

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/180  
**Labels:** backup  

### Description

Part of #178

---

## #181 — BACKUP-3: 30-day retention ΓÇö backup.py deletes files older than 30 days on each run

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/181  
**Labels:** backup  

### Description

Part of #178

---

## #182 — BACKUP-4: Failure alert ΓÇö write to backup_error.log and log WARNING on failure

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/182  
**Labels:** backup  

### Description

Part of #178

---

## #183 — BACKUP-5: Manual backup trigger ΓÇö admin UI button that runs backup on demand

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/183  
**Labels:** backup  

### Description

Part of #178

---

## #184 — BACKUP-6: Verify restore procedure ΓÇö document how to restore from backup file

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/184  
**Labels:** backup  

### Description

Part of #178

---

## #185 — Epic: Document Intake Hardening

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/185  
**Labels:** epic, doc-intake  

### Description

Goal: Make document intake reliable, observable, and recoverable for high-volume season use.
Child issues: DOC-HARD-1 through DOC-HARD-7
Dependency: EMAIL matching stability and MAIL watcher should be live first.

---

## #186 — DOC-HARD-1: Add intake queue depth to /health ΓÇö pending docs count

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/186  
**Labels:** doc-intake  

### Description

Part of #185

---

## #187 — DOC-HARD-2: Dead letter queue ΓÇö docs failing extraction 3x move to failed queue visible in UI

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/187  
**Labels:** doc-intake  

### Description

Part of #185

---

## #188 — DOC-HARD-3: Staff UI for failed documents ΓÇö shows reason, allows manual retry or assignment

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/188  
**Labels:** doc-intake  

### Description

Part of #185

---

## #189 — DOC-HARD-4: Duplicate document detection ΓÇö hash incoming file, warn if already attached to return

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/189  
**Labels:** doc-intake  

### Description

Part of #185

---

## #190 — DOC-HARD-5: Intake audit trail ΓÇö log who uploaded what, when, and extraction result

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/190  
**Labels:** doc-intake  

### Description

Part of #185

---

## #191 — DOC-HARD-6: Max file size and type validation on upload ΓÇö reject non-PDF/image with clear error

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/191  
**Labels:** doc-intake  

### Description

Part of #185

---

## #192 — DOC-HARD-7: Bulk document upload ΓÇö allow staff to drop multiple files at once

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/192  
**Labels:** doc-intake  

### Description

Part of #185

---

## #193 — Epic: Ops Runbook

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/193  
**Labels:** epic, ops  

### Description

Goal: Document all operational procedures so any staff member can manage the server.
Child issues: OPS-1 through OPS-5
Dependency: All other epics should be complete or in progress before finalizing.

---

## #194 — OPS-1: Write RUNBOOK.md ΓÇö restart, backup, smoke test, log locations, health check

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/194  
**Labels:** ops  

### Description

Part of #193

---

## #195 — OPS-2: Document NSSM env var update procedure ΓÇö add/change secrets without reinstalling

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/195  
**Labels:** ops  

### Description

Part of #193

---

## #196 — OPS-3: Document Gmail app password rotation ΓÇö step by step

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/196  
**Labels:** ops  

### Description

Part of #193

---

## #197 — OPS-4: Document how to check if service is running from any workstation browser

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/197  
**Labels:** ops  

### Description

Part of #193

---

## #198 — OPS-5: Add RUNBOOK.md link to admin UI footer

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/198  
**Labels:** ops  

### Description

Part of #193

---

## #152 — Epic: Mail Watcher Production Config

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/152  
**Labels:** epic, mail  

### Description

Goal: Get IMAP mail watcher running in production with the office Gmail account.
Child issues: MAIL-1 through MAIL-5

---

## #153 — MAIL-1: Add IMAP env vars to NSSM AppEnvironmentExtra ΓÇö IMAP_HOST, IMAP_PORT, IMAP_USER, IMAP_PASS

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/153  
**Labels:** mail  

### Description

Part of #152

---

## #154 — MAIL-2: Verify mail watcher starts cleanly in service context ΓÇö confirm in logs on restart

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/154  
**Labels:** mail  

### Description

Part of #152

---

## #155 — MAIL-3: Add mail watcher status to /health endpoint

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/155  
**Labels:** mail  

### Description

Part of #152

---

## #156 — MAIL-4: Test end-to-end ΓÇö send test email with PDF, verify it appears in document intake

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/156  
**Labels:** mail  

### Description

Part of #152

---

## #157 — MAIL-5: Document Gmail app password rotation procedure for when credentials expire

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/157  
**Labels:** mail  

### Description

Part of #152

---

## #158 — Epic: Health Check and Monitoring

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/158  
**Labels:** epic, health  

### Description

Goal: Make it easy to confirm the service is healthy without logging into the app.
Corresponds to PROD-3.
Child issues: HEALTH-1 through HEALTH-5
Dependency: MAIL epic should be live first so mail watcher status is meaningful.

---

## #159 — HEALTH-1: Implement GET /health ΓÇö returns JSON { status, db, uptime, version, workers }

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/159  
**Labels:** health  

### Description

Part of #158

---

## #160 — HEALTH-2: Include mail watcher status in /health response (running/stopped/error)

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/160  
**Labels:** health  

### Description

Part of #158

---

## #161 — HEALTH-3: Include extraction worker status in /health response

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/161  
**Labels:** health  

### Description

Part of #158

---

## #162 — HEALTH-4: Add /health check to restart.bat ΓÇö prints status after service start

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/162  
**Labels:** health  

### Description

Part of #158

---

## #163 — HEALTH-5: Document /health endpoint in internal ops runbook

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/163  
**Labels:** health  

### Description

Part of #158

---

## #164 — Epic: Smoke Test Suite

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/164  
**Labels:** epic, smoke  

### Description

Goal: Automated post-deploy verification that key endpoints are healthy.
Corresponds to PROD-8.
Child issues: SMOKE-1 through SMOKE-5
Dependency: HEALTH epic should be complete first.

---

## #165 — SMOKE-1: Write smoke_test.py ΓÇö hits /health, /, /login, /review, /payments, /ai/status

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/165  
**Labels:** smoke  

### Description

Part of #164

---

## #166 — SMOKE-2: Assert HTTP 200 or 302 on each endpoint, exit non-zero on any failure

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/166  
**Labels:** smoke  

### Description

Part of #164

---

## #167 — SMOKE-3: Add smoke test run to restart.bat after service start

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/167  
**Labels:** smoke  

### Description

Part of #164

---

## #168 — SMOKE-4: Log smoke test results to C:\TaxOps\logs\smoke.log with timestamp

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/168  
**Labels:** smoke  

### Description

Part of #164

---

## #169 — SMOKE-5: Document how to add new endpoints to the smoke test suite

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/169  
**Labels:** smoke  

### Description

Part of #164

---

## #170 — Epic: Email Matching Stability

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/170  
**Labels:** epic, email-matching  

### Description

Goal: Harden the IMAP email-to-client matching pipeline against edge cases and duplicates.
Child issues: EMAIL-1 through EMAIL-7
Dependency: Mail watcher must be live in production (MAIL epic complete).

---

## #171 — EMAIL-1: Add per-UID processed lock ΓÇö write to _processed_uids before dispatch, not after

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/171  
**Labels:** email-matching  

### Description

Part of #170

---

## #172 — EMAIL-2: Audit all early-return paths in _dispatch_classified_message ΓÇö ensure every path registers UID

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/172  
**Labels:** email-matching  

### Description

Part of #170

---

## #173 — EMAIL-3: Add diagnostic counters ΓÇö log N new unseen per folder per poll cycle

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/173  
**Labels:** email-matching  

### Description

Part of #170

---

## #66 — Epic: Chat performance ΓÇö eliminate LLM calls for common questions

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/66  
**Labels:** chatbot, performance  

### Description

## Goal
The current /ai/chat route makes 2 LLM calls per question. For common staff questions like 'how many returns are in PROCESSING?' the LLM is unnecessary ΓÇö the answer is often a deterministic path. This epic pushes more traffic to **instant** answers (`try_deterministic_response`, `chat_cache.ensure_chat_cache_for_year`, and related helpers wired from `POST /ai/chat` in `ai_routes.py` alongside `CHAT_TOOLS` / `CHAT_TOOL_ALLOWLIST`).

## Current code touchpoints (read before changing)
- `ai_routes.py`: `POST /ai/chat` ΓåÆ `_ai_chat_submit()`, `extract_json(...)` router, `chat(...)` answer path.
- Existing `chat_cache.py` already holds intent routing, KPI snapshot material, deterministic fast paths ΓÇö extend rather than reinvent.

## Current state (typical LAN)
- Tool selection (`extract_json`): often tens of seconds on remote Ollama.
- Answer generation (`chat`): another chunk of latency unless short-circuit or cache wins.

## Goal state
- Count/status-class questions: sub-100ms where classified + cache allows.
- Repeated questions: answer cache hit (already partially implemented ΓÇö extend/TTL strategy per team).
- Complex/novel questions: minimize redundant router work where safe.

## Child issues
- CHAT-1: In-memory hash tables and stats cache
- CHAT-2: Intent classifier ΓÇö regex pattern routing
- CHAT-3: Answer cache with TTL
- CHAT-4: Reduce to single LLM call for complex questions

## Non-goals
- No external cache (Redis, Memcached) ΓÇö prefer in-process + existing SQLite patterns where already used.
- No schema changes unless a child issue explicitly adds one.
- No new pip dependencies beyond team approval.

---

## #67 — CHAT-1: In-memory hash tables and stats cache ΓÇö O(1) lookup for common queries

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/67  
**Labels:** chatbot, performance  

### Description

## Parent epic
Epic: Chat performance ΓÇö eliminate LLM calls for common questions

## Depends on
Coordinated with merged LLM-6 `/ai/chat` work (`ai_routes.py`, `POST /ai/chat`, `CHAT_TOOL_ALLOWLIST`).

## What to build / extend
**Note:** `taxops/chat_cache.py` already exists (refresh snapshots, KPI material, deterministic routing). Extend it ΓÇö do not blindly replace wholesale.

Implement or strengthen **structured in-process indexes** rebuilt on a cadence:

- **`STATUS_COUNTS`**-style aggregates (often sourced from refreshed cache / `get_system_context`).
- **`PROCESSOR`/preparer rollup** maps for workload questions.
- **Financial roll-ups** coherent with dashboard math.
- **`BALANCE_DUE_RETURN_IDS` or equivalent** membership only if Privacy rules satisfied (never store/export `ssn_last4`; no new PII vectors).

Suggested API shapes (adapt to existing module layout):
```python
def build_cache(...)  # startup + periodic
def refresh_cache(...)  # on-demand/admin
def get_status_count(status: str) -> int  # backed by aggregates
```

## Privacy rules (non-negotiable)
- Matches `.cursor/rules.md`: no **full** SSN; treat `ssn_last4` with extreme care ΓÇö not in caches exposed to prompts/exports/logs.
- In-process aggregates only unless product decision says otherwise.

## Wire into routing
Before `extract_json(...)` router in `_ai_chat_submit()`, exploit `classify_intent`, `try_deterministic_response`, and snapshot helpers already imported from `chat_cache.py`.

## Definition of done
- Measurable reduction in `/ai/chat` Ollama round-trips on golden questions.
- `python -m pytest tests/ -v` green.
- `/ai/status` reports useful cache freshness if applicable.

---

## #68 — CHAT-2: Intent classifier ΓÇö regex pattern routing eliminates first LLM call

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/68  
**Labels:** chatbot, performance  

### Description

## Parent epic
Epic: Chat performance

## Depends on
CHAT-1 (indexes/cache foundations) landed or parallel-safe.

## Existing code (`chat_cache.py`)
`classify_intent()`, `normalize_question`, and deterministic helpers feed `try_deterministic_response(...)`. Prefer **extend** regex/heuristic buckets here rather than a second classifier.

## What to build
Tight pattern routing documented in-module:
- **`count_by_status`**, **`balance_due`**, **`processor` workload**, **`search_client`**, **`financial_totals`**, etc.
- Return `(intent, entities)` tuples compatible with downstream `try_deterministic_response(...)`.

Wire order in `_ai_chat_submit()` (**`ai_routes.py`**):
```
Question ΓåÆ (existing cache hit) ΓåÆ classify_intent / try_deterministic_response
       ΓåÆ optional scope gate (`chat_scope_classifier.should_block_tool_router_llm`)
       ΓåÆ extract_json(...) tool router only when needed
```

## Definition of done
- Golden prompts hit deterministic lane without router call when classified.
- Unmatched prompts still behave as today (fallback to router).
- Tests extended for classifier edges.

---

## #69 — CHAT-3: Answer cache with TTL ΓÇö repeated questions return instantly

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/69  
**Labels:** chatbot, performance  

### Description

## Parent epic
Epic: Chat performance

## Depends on
Coordinate with CHAT-1 / CHAT-2 to avoid duplicate cache layers.

## Current code
SQLite-backed answer cache paths already exist (`get_cached_answer`, `set_cached_answer`, TTL env). Verify behavior vs epic requirements (TTL, invalidation hooks, `normalize_question` keyed).

Locations: **`chat_cache.py`**, exercised from `_ai_chat_submit()` in **`ai_routes.py`**.

## What to extend
- TTL / invalidation semantics after **payments**, **status edits**, **`refresh_chat_cache`**.
- Telemetry: **`/ai/status`** already surfaces extraction queue counts ΓÇö add **answer cache** subsection if Product wants parity with prompt spec (`size`, `ttl_seconds`).
- Explicit `cached: true` payloads where appropriate.

## Definition of done
- Repeat question within TTL: **no** redundant Ollama work.
- Stale TTL or invalidation clears bad answers deterministically.

---

## #70 — CHAT-4: Single LLM call path ΓÇö skip tool selection for matched intents

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/70  
**Labels:** chatbot, performance  

### Description

## Parent epic
Epic: Chat performance

## Depends on
CHAT-1ΓÇôCHAT-3 baseline.

## Current flow (`_ai_chat_submit` in **`ai_routes.py`**)
Typical latency stack:
1. `extract_json(...)` ΓÇö tool router (**Ollama**)
2. `db_tools.<tool>`
3. `chat(...)` ΓÇö narrative answer (**Ollama**)

Environmental toggles (`CHAT_ROUTER_TRANSPORT_FAIL_AGGREGATES`, `CHAT_ANSWER_ON_ROUTER_TIMEOUT_SKIP_LLM`) already degrade when router dies.

## What to build
When intent + tool + args resolved **deterministically** (no speculative router creativity required), skip `extract_json` entirely and annotate metadata (`telemetry` / payload field) distinguishing **deterministic-router** vs **llm-router**.

## Definition of done
- Log evidence: deterministic path ΓåÆ **single** downstream model call maximum (often zero when cache wins).
- No regression on ambiguous prompts where router genuinely needed.

---

## #80 — CHAT-5: Generate Q&A training pairs from live DB for LoRA fine-tune

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/80  
**Labels:** chatbot, performance  

### Description

## Parent epic
Epic: Chat performance

## Depends on
CHAT-RAG must be merged first

## What to build
A script taxops/training_data_generator.py that reads the live DB and generates hundreds of Q&A pairs for fine-tuning. Pairs cover: status counts, financial summaries, preparer workloads, IRS rejection codes, form data interpretation.

## Output format
JSONL file compatible with Unsloth/llama-factory fine-tuning:
{\"prompt\": \"How many returns are in PROCESSING?\", \"completion\": \"There are 357 returns in PROCESSING status for 2026.\"}

## Privacy rules
- Never include ssn_last4 or identification numbers in training pairs
- Client names in training pairs are anonymized or use display names only
- Training file never committed to git

## Definition of done
- Script generates at least 500 unique Q&A pairs
- Pairs cover all 6 db_tools functions
- Pairs cover IRS_REJECTION_CODES
- Pairs cover form data interpretation (W-2 boxes, 1099 types)
- Output file is valid JSONL
- python -m pytest tests/ -v passes

---

## #81 — CHAT-6: LoRA fine-tune adapter for llama3.2 on TaxOps training data

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/81  
**Labels:** chatbot, performance  

### Description

## Parent epic
Epic: Chat performance

## Depends on
CHAT-5 must produce training data first

## What to build
Fine-tune a LoRA adapter on llama3.2 using the Q&A pairs from CHAT-5. The adapter makes the model understand TaxOps-specific concepts, terminology, and workflows without needing the data plane for common questions.

## Tooling
- Unsloth (free, CPU/GPU compatible, supports llama3.2)
- Ollama Modelfile to import the fine-tuned adapter back into Ollama

## Training on the server
The server has sufficient RAM for 3B parameter LoRA training. Estimated training time: 2-4 hours on CPU for 500 pairs.

## Definition of done
- LoRA adapter trained and importable via Ollama Modelfile
- Fine-tuned model answers TaxOps status questions correctly without data plane
- Fine-tuned model integrated into /ai/chat as an option alongside base llama3.2
- A/B comparison: base model vs fine-tuned model on 20 golden questions
- python -m pytest tests/ -v passes

---

## #199 — Epic: Receipt OCR ΓåÆ QuickBooks categorization

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/199  
**Labels:** epic, accounting  

### Description

Goal: Add an Accounting area to TaxOps that OCRs receipts via local Ollama vision, matches vendors/lines to Chart of Accounts with sentence-transformers embeddings, queues drafts for staff review, and exports approved rows as QB CSV/IIF.

Constraints:
- LAN-only: Ollama + local models only; no cloud OCR/accounting APIs.
- No automatic QB writes ΓÇö approve/reject/export are explicit staff actions.
- Optional hook: when document intake classifies doc type as receipt, enqueue receipt_queue (still requires staff review; never auto-approve).

Implementation note: extend existing SQLite schema in taxops/db.py (and migrations) consistent with this repo ΓÇö not a separate SQLAlchemy stack unless the codebase has already standardized on it.

Child issues: ACCOUNTING-1 through ACCOUNTING-12.

---

## #200 — ACCOUNTING-1: Add receipt_queue SQLite table + migration pattern

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/200  
**Labels:** accounting  

### Description

Part of #199

Add `receipt_queue` with statuses pending ΓåÆ processing ΓåÆ review ΓåÆ approved ΓåÆ rejected ΓåÆ exported; JSON for line_items, category_candidates, ocr_raw; align with existing db.py / init_db / ALTER patterns.

---

## #201 — ACCOUNTING-2: Config + .env.example for Ollama vision, COA paths, confidence bands

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/201  
**Labels:** accounting  

### Description

Part of #199

OLLAMA_VISION_MODEL, QB_EXPORT_MODE (csv|iif), COA_CSV_PATH, HISTORY_CSV_PATH, CONFIDENCE_HIGH/MEDIUM; document NSSM restart after new Python deps.

---

## #202 — ACCOUNTING-3: services/receipt_ocr.py ΓÇö extract_receipt_data(image_path)

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/202  
**Labels:** accounting  

### Description

Part of #199

Ollama vision chat following existing llm/ollama HTTP patterns; return vendor, date, total_amount, line_items[], payment_method; partial dict + _error on failure.

---

## #203 — ACCOUNTING-4: services/coa_matcher.py ΓÇö CSV load, embeddings, top-k, categorize_transaction

**Type:** Issue · **State:** OPEN  
**URL:** https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/203  
**Labels:** accounting  

### Description

Part of #199

sentence-transformers all-MiniLM-L6-v2; save data/coa_embeddings.npy + data/coa_labels.json; suggested_category, candidates, embedding_score, confidence high/medium/low.

---
