# Scoping: five TaxOps workflow changes

**Date:** 2026-08-06  
**Mode:** Read-only investigation + design proposal (no code, no migrations).  
**Priors (cited, not re-derived):**

| Prior | What we reuse |
|-------|----------------|
| [`gruntworx-feasibility.md`](./gruntworx-feasibility.md) §3 | `missing_docs` is a manual checklist; **1/1748** returns populated (2026-07-31); not a trustworthy completeness signal |
| [`document-custody.md`](./document-custody.md) | Scan-agent is the only prod path that produced `return_documents`; intake `?scan=1` / Skip → `scan_deferred`; email assign unused |
| [`office-blockers.md`](./office-blockers.md) Q1 | Scan 502s are TaxOps-generated after agent failure; `SCAN_AGENT_URL` → reception `192.168.1.9:8766` |

**Live DB snapshot (2026-08-06, `PRAGMA query_only=ON`):** TY2025 returns **1282**; `missing_docs` **2 rows / 1 return** (still dormant); HOLD **3**; PROCESSING **275**; `status_events` **7276** rows covering **1055/1282** TY2025 returns; `spouses` **166**; clients with spouse name on `clients` **32**.

---

## Item 1. Reception must be able to change statuses

### Current state

**Workflow status field:** `returns.client_status` only (`STATUS_FLOW` at `app.py:540`).

**Receptionist gate (hardcoded whitelist, not `ROLE_PERMISSIONS`):**

```542:570:taxops/app.py
RECEPTIONIST_STATUS_ALWAYS: frozenset[str] = frozenset({"HOLD", "PROCESSING", "PICKUP"})
RECEPTIONIST_STATUS_FROM: dict[str, frozenset[str]] = {
    "PICKUP": frozenset({"EFILE READY"}),
}
...
def receptionist_may_set_status(...):
    return new in receptionist_allowed_statuses(current_status)
```

Enforced **server-side** only on `POST /api/return/<id>/status` when `get_effective_role() == "receptionist"` → 403 (`app.py:5265-5285`).

**UI hides disallowed targets** on return detail and dashboard row menus (`templates/return_detail.html:52-70`, `templates/dashboard.html:404-405`, JS `_statusOptionsFor`). Bulk status `<select>` still lists full `STATUS_FLOW`; receptionists get 403 because bulk API is preparer-only.

#### Entry points

| Entry | Location | Permission / gate | Writes `status_events`? |
|-------|----------|-------------------|-------------------------|
| Single status API | `POST /api/return/<id>/status` `app.py:5265` | `@login_required` + receptionist whitelist | Yes, `source_file='APP'` |
| Bulk status | `POST /api/returns/bulk-status` `app.py:5339` | `@role_required("preparer")` | Yes, `'APP_BULK'` (`bulk_returns.py:85-91`) |
| Bulk update (incl. status) | `POST /api/returns/bulk-update` `app.py:5515` | preparer+ | Yes, via same helper |
| Pickup workflow | `POST /pickup/<id>` `app.py:3291` | `@login_required` only | **No** (gap) |
| FileTrack scan | `POST /filetrack/status` → `apply_filetrack_status` | token / localhost; **no RBAC**; **no** receptionist whitelist | Yes, `'FILETRACK'` (`filetrack_service.py:193-199`) — also dual-writes `filetrack_status` (prior: gruntworx § / custody note; **existing dual-write, not a precedent for a second workflow field**) |
| Manual / Drake import | `importer` / `drake_importer` | CLI / admin | Yes when status changes (`events.create_status_events`) |
| Intake create | `POST /intake` | `@login_required` | Yes, `'INTAKE'` → PROCESSING |
| Cancel / uncancel | `app.py` cancel routes | preparer+ | Yes |
| Field auto LOG OUT | `ack_date` / `logout_date` via `api_field` | preparer+ | Yes |
| E-file batch ACK / logout | `app.py:7101+` | `@login_required` | **No** (gap) |
| Admin upload confirm / source-compare apply | admin routes | admin | **No** (gap) |

#### `is_locked_status`

```117:129:taxops/normalizer.py
_LOCKED_STATUSES: frozenset[str] = frozenset({"CANCELLED"})
```

Import/bulk/efile/pickup skip overwriting CANCELLED. **`api_status` does not check it** (preparer can move a CANCELLED return via status API). FileTrack also does not check it. Uncancel is the supported preparer path.

#### Transition logic: centralized?

**No global transition graph.** Preparer/admin may jump to any `STATUS_FLOW` value. Receptionist whitelist lives in `app.py` and is **copied into UI** (Jinja + dashboard JS). FileTrack uses a parallel `ALLOWED_STATUSES` / date-stamp map (`filetrack/config.py`, `filetrack_service.py`).

#### Permissions

Return status is **not** in `ROLE_PERMISSIONS` (`config.py:560-576`). Related: `can_manage_efile_queue` opens pickup/logout **pages**, not which statuses may be set.

### What must change

Widen what reception can set on the **single staff path** (`api_status` + UI filters), and decide whether pickup / e-file ACK gaps need event writing (invariant 3) separately.

### Options with trade-offs

| Option | Approach | Pros | Cons |
|--------|----------|------|------|
| **(a) Widen whitelist** | Expand `RECEPTIONIST_STATUS_ALWAYS` / `_FROM` | Smallest diff; matches Feature 1 pattern; UI already driven by same helpers | Still ad-hoc; every new desk need = code change; easy to over-widen |
| **(b) Role × transition matrix** | Explicit `from→to` allowed per role | Auditable; can ban dangerous jumps for all roles | Larger design; must keep UI + server + FileTrack story aligned |
| **(c) Per-transition permissions** | e.g. `can_set_status_finalize` in `ROLE_PERMISSIONS` | Fits named-permission style in `config.py` | Many keys; still need from→to rules or reception can jump to FINALIZE from anywhere |

**Dangerous for reception to drive (recommend keep blocked unless paired with checklist):**

| Status | Why |
|--------|-----|
| **FINALIZE** | Signals prep complete; desk should not mark work done |
| **LOG OUT** | Terminal office closeout; e-file/logout queue owns most of this |
| **REJECTED** | Should come from IRS ACK / preparer context + contact workflow |
| **PENDING INTAKE** | Re-opens preintake; can hide a live file |
| **EFILE READY** from non-PICKUP | Skips pickup checklist (signatures/payment) — today only allowed from PICKUP |

**Safer expansions often requested at the desk:** HOLD ↔ PROCESSING (already allowed), PICKUP from PROCESSING/HOLD (already), maybe **FINALIZE → PICKUP** only (handoff), or allowing return from EFILE READY → PICKUP/HOLD when a client walks back — product call.

### Risks

- Widening without server+UI sync → 403 confusion or hidden capability.
- Pickup / e-file paths already let reception move statuses **outside** the whitelist (pickup → EFILE READY; e-file ACK → LOG OUT/REJECTED) — expanding the whitelist without fixing those gaps deepens inconsistency.
- FileTrack can still set any allowed scan status with no role gate (documented intentional).

### Effort

**S** for (a) with a short explicit allow-list update + tests. **M** for (b)/(c).

### Open questions for a human

1. Exact transitions reception needs beyond HOLD/PROCESSING/PICKUP and PICKUP→EFILE READY?
2. Should reception ever set FINALIZE?
3. Should pickup workflow and e-file ACK start writing `status_events` (invariant 3 debt)?

---

## Item 2. Missing-docs report + "needs attention"

### Current state

**Schema** (`db.py:235-243`):

```
missing_docs (
  id, return_id, item_text, is_resolved DEFAULT 0,
  created_at, resolved_at
)
```

Indexes: `idx_missing_docs_return`, `idx_missing_docs_open` (`db.py:571-572`).

**Writes**

| Path | Who | Location |
|------|-----|----------|
| Intake form checkboxes + custom lines | Any logged-in intake user | `_run_intake_write` `app.py:3720-3734` |
| Add item API | **preparer+** | `POST .../missing-doc` `app.py:5802-5818` |
| Toggle resolved | **preparer+** | `.../toggle` `app.py:5821-5838` |
| Delete | `@login_required` (asymmetric) | `DELETE ...` `app.py:5842-5848` |

**Reads / UI:** return detail card (`app.py:2613-2635`, `templates/return_detail.html:588-658`). Receptionists see the card but `@view_only_for("receptionist")` hides add/toggle/delete controls (`auth.py:71-82`). No list/report route keyed on open `missing_docs`.

**Why empty (agree with priors):** gruntworx §3 — checklist is optional at intake; staff rarely check boxes; post-intake edits are preparer-only and buried on the return page. Live 2026-08-06: still **1 return / 2 open items**.

**Existing "needs attention"** (already shipped Feature 2): derived query `fetch_needs_attention` (`app.py:809-945`) — reasons `ef_rejected`, `client_contact`, `stale_processing` (PROCESSING + named client + intake > **60 days** + Drake not done). Dashboard badge + admin `/admin/audit-alerts` (`app.py:8151`) overlap stale/EF Rejected. **Does not consider `missing_docs`.**

**Print / display patterns already touching missing docs**

- Label stickers: `try_print_log_label` / relay ZPL (not HTML) — no checklist.
- Print-oriented HTML: `templates/intake_print.html`, `work_order_print.html` — `@media print` + `window.print()`. **Intake sheet does not render `missing_docs`.**
- **Year-comparison PDF already lists open missing-doc items** (`multiyear_comparison.py:358-373`) — closest existing printable reuse.
- Client profile comparison grid shows a “Missing-doc checklist” row (`client_profile.html:467-478`) — screen only, not a dedicated printout.
- Reports page: `templates/reports.html` has `@media print`.
- **No shared print stylesheet module** — copy intake/work-order print template pattern for a dedicated checklist sheet if needed.

**Intake already collects** missing-doc checklist labels + free-text (`templates/intake.html` ~750-799): enough for a per-return printout (client name, log #, tax year, item list, intake/created dates). There is **no** post-insert update of `item_text` — only add / toggle / delete.

### What must change

1. Make missing-docs editable by the roles that actually intake (likely reception) **or** accept intake-only population.
2. A printable / list view of open items.
3. Optionally fold open-missing-docs into needs-attention reasons.

### Options with trade-offs

**Needs attention = derived vs stored**

| | Derived (preferred) | Stored flag |
|--|---------------------|-------------|
| | `EXISTS (open missing_docs)` join / subquery | New column or status |
| Pros | Always true; no sync bugs; fits invariant 1 | Fast badge |
| Cons | Extra join on dashboard load | Drift; tempts a second status |

**Agree with derived.** Cost: one indexed existence check on `missing_docs(return_id, is_resolved)` — cheap at current volume; even at full season scale the open index exists.

**Report shape options:** (1) extend `fetch_needs_attention` with reason `missing_docs`; (2) separate Daily Tools report page; (3) print-only from return detail. (1)+(3) is the smallest useful pair.

### Risks

- Treating empty `missing_docs` as “complete” (gruntworx already warned).
- Reception edit rights vs `view_only` — status dropdown is already an exception pattern; missing-docs would need the same.
- Printouts must omit SSN / `file_path` (invariant 5).

### Effort

**M** (API permission + list/print + optional needs-attention reason). **S** if print-from-return only with no dashboard change.

### Open questions for a human

1. Should reception add/resolve missing-doc lines, or only preparers?
2. Is a paper checklist per client the primary deliverable, or the dashboard badge?
3. Auto-HOLD when open missing docs exist? (**Do not** — would invent workflow coupling; see Item 4.)

---

## Item 3. Spouse info on the client profile, propagating to returns

### Current state

**Split (authoritative picture):**

| Store | Role | Population |
|-------|------|------------|
| `clients.spouse_*` (name, dob, occupation, phones, email) | **Staff-facing / intake / profile edit path** | Intake write `app.py:3542-3604`; `CLIENT_EDITABLE` via `POST /api/return/<id>/field` `app.py:5659-5711` |
| `spouses` table (Drake purple-sheet import; 1:1 `client_id`) | **Import shadow + intake prefill fallback** | Created in `db.py:1564+`; intake prefill merges when clients spouse blank (`app.py:4903-4936`) |
| `returns.*` | **No spouse columns** (live PRAGMA) | N/A |

**Authoritative for office UI today:** `clients.spouse_*` once set. `spouses` fills gaps and carries Drake `id_type` / review flags. They are **not kept in sync** after intake.

**Client profile UI gap:** `templates/client_profile.html` edits taxpayer fields + `filing_status` on an anchor return — **no spouse inputs** even though `CLIENT_EDITABLE` includes spouse fields. Spouse is editable on **return detail** field saves (preparer+), which update `clients` only.

**Return-level derived use of spouse data**

- `display_name` / title helpers use spouse names at compute time (`app.py:743+`, profile title) — not a stored return column of spouse identity.
- `filing_status` is return-scoped and edited separately; not auto-derived from spouse presence.
- No automatic recompute of `display_name` when spouse fields change via `api_field`.

**Merge vs dedupe (correcting prior brief):**

Both **do** reassign/merge spouse rows:

- `merge_client_into` → `_repoint_client_children` includes `spouses` / `client_spouse_import` (keep wins) — `merge_ops.py:207-220`, `:299`.
- `_deduplicate_existing_records` same pattern — `db.py:822-842`.

Prior statement “merge does NOT reassign spouses” is **false for current code**.

**Propagation pattern today:** client fields edited through return-scoped `api_field` update `clients` immediately. Other returns for that client **see the change via JOIN** (shared `client_id`) — there is no row-by-row fan-out job, and none is needed for spouse identity columns (they do not exist on `returns`). Tour copy that “edits apply to every return” means this shared-client semantics (`app.py` tour ~2105). First *additional* propagation would be inventing return-level copies or rewriting `display_name` / `filing_status` (the latter is already return-scoped and independent).

**Audit:** `status_events` is workflow-only. Client field edits via `api_field` do **not** write `status_events`. HTTP-level `audit_log` (`audit_service`) may capture the POST with before/after JSON when enabled — schema `id, user_id, action, entity_type, entity_id, before_json, after_json, ip_address, created_at`. Prefer **`audit_log` (or a dedicated note)** for spouse edits, not `status_events`.

### What must change

1. Surface spouse fields on `/clients/<id>` (and optionally keep return-detail edits).
2. Define sync rules: `clients` ↔ `spouses` table.
3. Define whether any **return-scoped** field should change when spouse data changes.

### Options with trade-offs — “should update returns”

| Option | Semantics | Safety |
|--------|-----------|--------|
| **(a) All returns for client** | Rewrite something on every year | **Unsafe** for LOG OUT / EFILE READY / CANCELLED history |
| **(b) Current tax year only** | Touch active season | Better; still wrong if that year’s return is already filed |
| **(c) Only unlocked / non-terminal statuses** | Skip LOG OUT, CANCELLED, maybe EFILE READY | Safest if any return field must change |

**Agree:** amending spouse identity on a **closed** return (LOG OUT / CANCELLED) is probably wrong. Prefer:

- Treat spouse identity as **client-scoped** (update `clients` + optionally refresh `spouses` row).
- Propagate to returns **only** for soft derived fields you explicitly list (e.g. refresh `display_name` on open-year returns), using **(c)**.
- Do **not** invent return-level spouse columns (invariant 1/2; schema not required).

### Risks

- Dual write `clients` vs `spouses` without a single writer → intake prefill fights profile edits.
- Privacy mode masking on profile.
- Merge keep-wins can drop Drake spouse row silently.

### Effort

**M–L** (UI + sync policy + audit). Schema change **not required** if spouse stays client-scoped.

### Open questions for a human

1. Is the goal “edit spouse on profile” (clients columns), or “replace Drake `spouses` table as system of record”?
2. Should `display_name` auto-rebuild when spouse name changes?
3. Should `filing_status` ever auto-flip with spouse presence? (Recommend **no**.)

---

## Item 4. Missing-docs / long-cycle visibility

### Current state — HOLD usage

TY2025 distribution (live): LOG OUT 792, PROCESSING 275, PENDING INTAKE 121, PICKUP 61, FINALIZE 25, **HOLD 3**, CANCELLED 2, odd `EFILE` 2, EFILE READY 1.

**HOLD is not the de facto “waiting on documents” state.** UI copy on the missing-docs card implies HOLD∩resolved docs (“clear HOLD when ready”) — `return_detail.html:608-609` — but almost nobody uses HOLD. Waiting files sit in **PROCESSING** (and are partially covered by `stale_processing` at 60 days).

**Time-in-status from `status_events`**

- Global coverage: **1055/1282** TY2025 returns have ≥1 event (~18% gap — imports/upload/efile/pickup paths that skip events; see Item 1).
- All **3** current HOLD rows have a `new_status='HOLD'` event (lucky / small-n).
- For PROCESSING majority, aging by `intake_date` (already used by needs-attention) is more complete than “last status_events enter PROCESSING.”

**Existing aging / stale views**

- Dashboard needs-attention: stale PROCESSING >60d (`app.py:821-828`).
- Admin audit alerts: same 60d PROCESSING query (`app.py:8181-8206`).
- Dashboard filters: `late_intake`, `slow_cycle` query args exist in filter plumbing (`app.py:2360`) — confirm product meaning before reuse.

**Config home for thresholds today:** env-backed constants in `config.py` (e.g. `MULTIYEAR_*`); needs-attention **60** is hardcoded in `app.py`, not config.

### Design constraint (honored)

**Do not** add a new `client_status` or a second status field. `filetrack_status` dual-write is an **existing violation to live with**, not a template ([gruntworx](./gruntworx-feasibility.md) dual-write note; `filetrack_service.py:73-99`).

**Propose:** derived views:

1. Open `missing_docs` (Item 2).
2. Age = `now - COALESCE(last status_events.created_at for current status, intake_date, created_at)` with explicit “event gap” badge when falling back.
3. Optionally filter `client_status IN ('PROCESSING','HOLD','PICKUP')` — still one status field.

### Options

| Threshold | Pros | Cons |
|-----------|------|------|
| Reuse 60d | Consistent with needs-attention | May be long for missing W-2 season rush |
| Configurable `STALE_PROCESSING_DAYS` in `config.py` / env | Tunable without code | Need admin awareness |
| Separate shorter threshold for open-missing-docs | Matches “waiting on client” | Two knobs |

### Risks

- Event gaps understate age if you trust only `status_events`.
- Colliding UX with needs-attention if Item 2 and 4 ship as two badges.

### Effort

**S–M** if folded into needs-attention reasons; **M** as a standalone aging report.

### Open questions for a human

1. What is “long” in days for this office (14 / 30 / 60)?
2. Should HOLD be **encouraged** as the waiting bucket, or leave PROCESSING + missing-docs flags?

---

## Item 5. Document scan from the return page

### Current state

**Intake redirect:** after successful intake, if `can_scan_intake_docs`, redirect `/return/<id>?scan=1` (`app.py:3766`).

**What `scan=1` does:** `prompt_scan = (request.args.get("scan") == "1")` (`app.py:2643`) → template `PROMPT_SCAN` → on load `openScanModal({ autoStart: true })` (`return_detail.html:1351`, `3162-3163`).

**Outside intake:** Scan control already exists on return detail when `can_scan_intake_docs` (`return_detail.html:776`, `openScanModal`). **Not coupled to intake state** beyond auto-open. This is primarily an **entry-point / discoverability** issue, not new plumbing — consistent with Feature 6 already in tree and [document-custody.md](./document-custody.md) §3.

**Upload endpoints (`routes/documents.py`)**

| Endpoint | Permission | Provenance |
|----------|------------|------------|
| `POST .../documents/upload` | `@login_required`; if `source=scan_agent` needs `can_scan_intake_docs` | `match_confirmed=1`, `match_method` `manual` or `scan_agent` (`:50-56`, `:218-273`) |
| `POST .../documents/bulk-upload` | login (+ scan_agent gate same) | same |
| `POST .../scan-intake` | `can_scan_intake_docs` | **hardcoded** `match_confirmed=1`, `match_method='scan_agent'` (`:694-753`) |
| Email assign (app.py) | `can_use_email_tools` | `email_manual` / `email_suggested` (invariants) |

**`match_method` for return-page scan:** keep **`scan_agent`** with `match_confirmed=1` — same physical path as intake scan; staff is on the return (human-confirmed context). Do **not** invent `scan_return_page`; that would fragment audit without a behavior difference.

**Scan agent host:** single `SCAN_AGENT_URL` (`config.py:337`), live `.env` → `http://192.168.1.9:8766` ([office-blockers](./office-blockers.md) Q1). Scanning from a non-reception workstation still hits reception’s agent; if agent/scanner down → **502** with tips. Behavior should stay: clear error + health tips; no silent queue. Optional future: disable Scan button when `/api/scan-agent/status` reports unreachable (UX only).

**Drake Documents note:** return-page scans are exactly the corpus for a future DDM handoff; new clients may lack Drake folders until a preparer creates the file ([gruntworx](./gruntworx-feasibility.md) identity §).

### What must change

Likely **nothing structural** if the Scan button is already visible. Confirm reception role sees it (permission includes receptionist). Polish: status tip when agent down; ensure Skip/`scan_deferred` still available.

### Options

| | |
|--|--|
| **A. Document + QA only** | Treat Feature 6 as done | **S** |
| **B. UX harden** | Disable/hide Scan when health fails; always-on button copy | **S** |
| **C. Multi-agent URL** | Per-workstation agent | **L**, out of scope unless second scanner |

### Risks

- 502 clusters (office-blockers) — process/ops, not missing route.
- Invariant 4 already satisfied on scan-intake path.

### Effort

**S** (verify + minor UX).

### Open questions for a human

1. Is the Scan button missing in prod UI for some roles, or is the pain “agent down”?
2. Any second scanner location planned?

---

## Cross-cutting

### Interactions

| Pair | Interaction |
|------|-------------|
| **2 ↔ 4** | Both are “why is this file stuck?” — **should be one derived attention model** with multiple reason codes (`missing_docs`, `stale_processing`, future `aged_in_status`), not two dashboards |
| **1 ↔ 4** | If reception can set HOLD more freely, HOLD counts may rise; aging view must not assume HOLD means missing docs |
| **2 ↔ 5** | Scanning docs ≠ clearing `missing_docs` checklist (manual resolve still required unless you add heuristics — do not in v1) |
| **3** | Mostly independent; touches profile/intake identity only |
| **5 ↔ Drake** | Document-custody / gruntworx future handoff |

### Ship independently? Recommended sequence

1. **Item 5** — confirm/polish return-page scan (**S**) — already mostly present.  
2. **Item 1** — widen reception whitelist to the human-agreed set (**S**) — immediate desk value; fix `status_events` on pickup/efile as follow-up debt.  
3. **Item 2 + 4 together** — activate missing-docs (permission + print + needs-attention reason) and expose age with intake/event fallback (**M**) — one UX surface.  
4. **Item 3** last — spouse profile + sync policy (**M–L**).

### Schema changes?

| Item | Schema required? |
|------|------------------|
| 1 | No |
| 2 | No (table exists) |
| 3 | No if spouse stays on `clients` (+ optional sync to `spouses`) |
| 4 | No (derived) |
| 5 | No |

### `ROLE_PERMISSIONS` reuse vs add

| Permission | Reuse? |
|------------|--------|
| `can_scan_intake_docs` | Reuse for Item 5 |
| `can_manage_efile_queue` | Unrelated to status whitelist |
| *(none)* for return status | Item 1 stays whitelist **or** add something like `can_set_desk_statuses` if choosing option (c) |
| Missing-docs APIs | Today preparer via `@role_required`; either keep or add `can_manage_missing_docs` including receptionist |

---

## Invariants — conflict flags

| # | Invariant | Flags in this scope |
|---|-----------|---------------------|
| 1 | Single workflow status | Item 4 must not add status values/fields. FileTrack dual-write already violates spirit — do not extend. |
| 2 | Workflow return-scoped; client changes state return effect | Item 3 must document client-only vs display_name/filing_status effects |
| 3 | Every status change → `status_events` + accurate `source_file` | **Existing gaps:** pickup POST, e-file ACK/logout, admin upload confirm, source-compare. Widening reception does not create new gaps on `api_status` (already writes). |
| 4 | Uploads set `match_confirmed` / `match_method` | Item 5 OK if keeping `scan_agent` |
| 5 | No PII in API/logs | Missing-docs print / attention APIs: name + log # + item text only |
| 6 | Intake log-number `BEGIN IMMEDIATE` | Preserve in any scan/intake touch (`_after_scan_agent_upload` / `ensure_return_log_number`) |
| 7 | Prefer `is_test`; no pytest on share | Scoping only — N/A |

---

## Recommended sequence and smallest valuable change

**Sequence:** 5 (verify) → 1 (whitelist) → 2+4 (derived attention + print) → 3 (spouse).

**Single smallest change that delivers real desk value:**  
**Item 1 option (a)** — expand `RECEPTIONIST_STATUS_*` to the exact transitions the office names in a one-line answer (and mirror UI via existing helpers). No schema, no new permission key, tests already encode the pack (`tests` receptionist status suite).

**If the office’s real pain is “stuck waiting on W-2s” rather than status clicks:** skip to **Item 2** — allow reception to maintain `missing_docs` and add one derived needs-attention reason + print template modeled on `intake_print.html`.
