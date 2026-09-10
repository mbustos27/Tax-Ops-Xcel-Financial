# TaxOps — agent system handoff overview

**Audience:** another coding agent (Claude / Cursor) picking up work on this repo  
**Repo / share:** `T:\taxops` ≡ `\\Xcel-server\taxops`  
**Date of this handoff:** 2026-09-10  
**Live app (office LAN):** `http://192.168.1.173:5000` (also `http://taxlog:5000` after workstation hosts setup)  
**Product name in UI:** “Tax Log” (`APP_NAME` in `config.py`)

Use this document first. Prefer **current code + `.cursor/rules/taxops-invariants.mdc`** over older `docs/CODEBASE_OVERVIEW.md` (schema/email sections there are stale).

---

## 1. What TaxOps is

Internal tax-office operations app for a small US prep firm:

- Client / return lifecycle (intake → prep → e-file / pickup)
- Document scan/upload + extraction queue
- Inbound email **holding area** (staff assigns attachments to returns)
- Lobby **Now Serving** take-a-number (kiosk + lobby display + staff panel)
- Work orders, compliance, bookkeeping/accounting helpers
- In-app **Messages** panel (notifications + staff mini-chats)

**Stack:** Flask + Jinja2 + SQLite (WAL) + Waitress in production. Background threads: IMAP mail watcher, document extractor, accounting worker.

**Run:** from `T:\taxops`, `python app.py` (or NSSM service `TaxOpsService`). Default bind `0.0.0.0:5000`. DB: `TAXOPS_DB` env or `T:\taxops\taxops.db`.

**Do not** treat root `README.md` CSV importer (`main.py`) as the product entry point — the live surface is `app.py`.

---

## 2. How to work in this repo (agent habits)

1. **Read invariants first:** `T:\taxops\.cursor\rules\taxops-invariants.mdc` (alwaysApply).
2. **CodeGraph:** project expects `user-codegraph` MCP. If tools say “not initialized,” ask human to run `codegraph init -i` under `T:\taxops`. Prefer `codegraph_context` / `codegraph_search` over grep for symbols.
3. **Migrations:** new columns only via `_migrate_existing_tables()` in `db.py`; bump `CURRENT_SCHEMA_VERSION`; additive `ALTER TABLE … ADD COLUMN` with safe defaults. Test on a DB **copy** first.
4. **Privacy:** never return `file_path`, `ssn_last4`, or raw ID numbers in APIs; never store email body in DB; never log `IMAP_PASS`.
5. **Tests:** `cd T:\taxops` then `python -m pytest tests/<file> -q` (full suite is large; prefer targeted). Do not point pytest at the live production DB.
6. **Commits / PRs:** only when the human asks.
7. **Tailwind:** arbitrary colors like `bg-[#6B2233]` often **are not** in `static/tw.min.css`. Prefer `btn-primary` / `var(--color-primary)` from `static/app.css`. Rebuild with `npm run build:tw` only if intentional.

---

## 3. Directory map

| Path | Owns |
|------|------|
| `app.py` | Flask app, many core routes still in-file (dashboard, intake, return detail, email-inbox pages/APIs), `base_ctx()`, worker registration, Waitress entry |
| `db.py` | SQLite connection, `init_db()`, `_migrate_existing_tables()`, **`CURRENT_SCHEMA_VERSION = 39`** |
| `config.py` | Env/`.env`, paths, roles/permissions flags, IMAP, vision toggle, etc. |
| `auth.py` | `login_required`, `permission_required`, `has_permission`, roles (avoid circular imports) |
| `mail_watcher.py` | IMAP poll → `email_inbox` only (no classifier) |
| `email_suggest.py` | Non-binding suggestions for inbox UI only — **never** import from `mail_watcher` |
| `extractor.py` | Document extraction queue worker |
| `now_serving.py` | Queue data layer (tickets, transfer, auto-rebalance) |
| `routes/` | Blueprints (documents, now_serving, staff_messages, work_orders, notifications, …) |
| `templates/` | Jinja2 UI; `base.html` is the chrome |
| `static/` | `app.js`, `app.css`, `tw.min.css` |
| `tests/` | pytest |
| `docs/` | Runbooks, onboarding sheets, investigations; this handoff under `docs/handoffs/` |
| `scripts/` | Ops / kiosk / NSSM helpers |
| `scan_agent/`, `filetrack/` | Side processes (Epson scan agent, label print relay) |
| `.cursor/rules/` | Invariants + codegraph-first |

**Blueprints registered in `app.py`:**  
`documents`, `accounting`, `users`, `reports`, `sender_rules`, `email_health`, `filetrack`, `reception_agents`, `email_campaigns`, `now_serving`, `work_orders`, `wo_quick_picks`, `notifications`, `announcements`, `staff_messages`, `compliance`.

---

## 4. Feature map (where to edit)

### Returns / intake / dashboard
- Routes mostly in `app.py`: `/`, `/return/<id>`, `/intake`, queues (pickup / e-file / extension), client profile.
- Templates: `dashboard.html`, `return_detail.html`, `intake.html`, `client_profile.html`, `prep_workspace.html`.
- Status / events: `events.py`, importer: `importer.py`.

### Documents / scan / extraction
- `routes/documents.py` — upload, view, tag, prep-review, intake scan APIs.
- Scan UI on return / prep / client profile; Epson via `SCAN_AGENT_URL` + `scan_agent/`.
- `extractor.py` — vision gated by `EXTRACTOR_VISION_ENABLED` (default **false** → images `skipped`, not `failed`).
- **UI gotcha (fixed 2026-09):** scan CTAs must use `btn-primary` / CSS vars — not uncompiled `bg-[#6B2233]` (white text vanished on light cards).

### Email (holding-area model)
- Pipeline: IMAP → `mail_watcher._save_to_inbox()` → `email_inbox` → staff UI `/email-inbox` → `POST /api/email-inbox/<id>/assign` → `return_documents` + extract enqueue.
- **Hard rules:** `BODY.PEEK[]` only; never `_mark_read` / `\Seen`; dedup `email_processing_log`; assign must set `match_confirmed=1` + `match_method` (`email_manual` / `email_suggested`); walk-in uploads `match_method='manual'`.
- Do **not** revive classifier / `known_sender_rules` / `/api/email-review/*`. History at git tag **`pre-email-simplification`**.
- Suggestions: `email_suggest.py` from inbox APIs only.

### Now Serving
- Data: `now_serving.py` (plain ticket labels `"1"`, `"14"` — not `A-###`; transfer + auto-rebalance; `transferred` flag).
- Routes: `routes/now_serving.py` — kiosk, lobby display, snapshot/events, call-next, transfer, admin day-reset.
- Staff UI: floating panel `templates/now_serving_modal.html` included from `base.html` (draggable, z-index ~100050).
- Lobby: `now_serving_display.html` (EN/ES TTS); Pi script `scripts/now_serving_display_kiosk.sh`.
- Permission: `can_manage_now_serving` (receptionist / preparer / admin).
- Tests: `tests/test_now_serving_m1.py`, `tests/test_now_serving_routes.py`.

### Staff messages + Messages panel (schema v39)
- Backend: `routes/staff_messages.py`
  - Full page `/staff-messages`
  - `POST /api/staff-messages/send` — starts thread (`thread_id = id`)
  - `POST /api/staff-messages/reply` — continue thread
  - `GET /api/staff-messages/panel` — notifications + conversations + staff list
  - `GET /api/staff-messages/thread/<id>` — messages; marks inbound read
- UI: `templates/messages_panel.html` (floating, included for all logged-in users from `base.html`); header icon opens panel (Notifications | Chats).
- Also writes `notifications` via `notify_user` (`routes/notifications.py`).
- **Recent bugfixes:** poll must not rebuild compose `<select>` (preserve selection); Back must invalidate in-flight thread fetches so poll does not yank user back into chat.
- Tests: `tests/test_staff_messages.py`.

### Work orders / announcements / notifications
- `routes/work_orders.py`, `routes/work_order_quick_picks.py`
- `routes/announcements.py`, `routes/notifications.py`
- Nav badge count from `base_ctx()` → `my_unread_notification_count`

### Auth / roles
- Roles include admin, preparer, receptionist (see `config.py` / `auth.py`).
- Onboarding accounts documented for Lucy / Sandra / Lorena (preparer, temp password handoffs — treat as sensitive).

---

## 5. Schema versioning

- `CURRENT_SCHEMA_VERSION = 39` in `db.py`.
- Stamped in `app_settings`; visible on `/health`.
- Recent: **v36–v38** Now Serving; **v39** `staff_messages.thread_id` / `parent_id` (+ backfill `thread_id = id`).
- After deploy/restart, confirm `/health` shows expected schema version.

---

## 6. Hard invariants (summary — full text in rules file)

| Topic | Rule |
|-------|------|
| Email architecture | Holding area only; no auto-classifier revival |
| IMAP | `BODY.PEEK[]`; never mark Seen; never call `_mark_read` |
| Assign | Explicit `match_confirmed=1` + method; no silent defaults |
| Suggest | UI-only; never from mail watcher |
| Privacy | No paths/SSN in APIs; no email body in DB |
| Vision | Off by default → `skipped` |
| Watcher | Single-start guard (`_watcher_started`) |
| Migrations | `_migrate_existing_tables` + version bump |

---

## 7. Office / ops context

- Share root `T:\` = taxops share; workstation setup: `T:\SETUP_WORKSTATION.bat` / `setup_workstation.ps1` (maps F/P/Q/T, Tax Log shortcut).
- Staff onboarding printables (internal; contain temp passwords when freshly issued):
  - `T:\ONBOARD_LUCY_SANDRA.html`
  - `T:\ONBOARD_LORENA.html` (Spanish MX prose; Windows UI words kept in English)
  - Markdown under `docs/onboarding/`
- Orientation: `templates/orientation.html`
- Network notes: `docs/OFFICE_NETWORK.md`, `docs/RUNBOOK.md`

---

## 8. Known gaps / good next work (as of handoff)

- **Close/open Now Serving window** (offline a serving window) — discussed, **not implemented**.
- CodeGraph may need `codegraph init -i` if MCP says uninitialized.
- `docs/CODEBASE_OVERVIEW.md` lags (still mentions old email routing / old schema) — trust this handoff + invariants + code.
- Live DB user roster may differ from what a local shell shows; Staff Accounts UI is source of truth for auth users.

---

## 9. Quick verification checklist for a new agent

```text
[ ] Open T:\taxops; confirm git status clean or note dirty files
[ ] Read .cursor/rules/taxops-invariants.mdc
[ ] Confirm CURRENT_SCHEMA_VERSION in db.py and /health after app restart
[ ] Smoke: login → Messages panel opens → New chat To dropdown keeps selection across ~4s
[ ] Smoke: return page Scan documents button visible when deferred (btn-primary, not invisible white text)
[ ] Targeted pytest for area you touch
```

Example tests:

```bash
cd T:\taxops
python -m pytest tests/test_staff_messages.py tests/test_now_serving_m1.py -q
```

---

## 10. Conversation context that led to this handoff (2026-09)

Recent completed work in the prior agent thread:

1. Now Serving feature set (tickets, transfer, rebalance, lobby TTS, floating staff panel).
2. Onboarding sheets for Lucy/Sandra (EN) and Lorena (ES-MX printable HTML).
3. Staff Messages → floating Messages panel (notifications + threaded mini-chats); schema v39; poll/Back/dropdown fixes.
4. Scan button invisible-text fix (`btn-primary` / CSS variables).

If continuing Now Serving “close window” or Messages polish, start from `now_serving.py` / `messages_panel.html` respectively.

---

*End of handoff. When in doubt: invariants > this doc > older overview markdown > comments in archived email code.*
