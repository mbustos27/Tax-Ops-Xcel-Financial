# taxops

`taxops` is a workflow-aware CSV importer for a small tax office.  
The **manual LOG spreadsheet is the source of truth** for workflow state.

## Requirements

- Python 3.9+ (standard library only)

## Project layout

- `main.py` - scans incoming files, runs one-transaction-per-file import, moves files, prints summary
- `db.py` - SQLite connection, schema bootstrap, lightweight schema migration, indexes
- `importer.py` - row parsing, matching, upsert logic, review queue routing
- `normalizer.py` - normalization helpers for strings, booleans, currency, and dates
- `events.py` - status-event generation and duplicate-event protection
- `utils.py` - helpers (`hash_file`, `now`)
- `config.py` - folders and constants

## Supported manual LOG headers

- `LOG 2025`
- `LAST`
- `FIRST`
- `TAX PAYER NAME (S)`
- `YR`
- `PROCESSOR`
- `VERIFIED`
- `CLIENT STATUS`
- `INT'D`
- `25 TRANSF`
- `26 TRANSF`
- `EMAIL`
- `DATE EMAILED`
- `PICK UP`
- `LOG OUT`
- `TOTAL FEE`
- `RECEIPT #`
- `FEE PAID`
- `CC Fee`
- `Zelle or CK #`
- `Cash, Q Pay`
- `1040`
- `SCH A & D`
- `SCHED C`
- `SCHED E`
- `1120`
- `1120S`
- `1065/LLC`
- `Corp Officer`
- `Bus Owner`
- `1040X`
- `W7`
- `990/1041`
- `EXT`
- `TRANSFER`
- `UPDATED`
- `NOTES`
- `Referral`
- `Referred By`

Unknown columns are allowed and preserved in `import_rows.raw_json`.

## Tables created

- `clients`
- `returns`
- `return_forms`
- `payments`
- `notes`
- `status_events`
- `import_batches`
- `import_rows`
- `review_queue`

## Import behavior

1. Reads `data/incoming/*.csv`
2. Computes SHA256 hash and skips files already imported (`import_batches.file_hash`)
3. Creates one `import_batches` record per file
4. Processes rows and stores raw row JSON in `import_rows`
5. Uses matching priority:
   - return by `log_number + tax_year`
   - fallback `last_name + first_name + tax_year`
6. Routes ambiguous matches to `review_queue` and marks row action `REVIEW`
7. Upserts:
   - `clients`
   - `returns`
   - `return_forms`
   - `payments`
   - `notes` (deduped by normalized note text per return)
8. Creates workflow `status_events` only when tracked values actually change, with dedupe on `return_id + event_type + event_timestamp`
9. Uses one DB transaction per file; unexpected file-level failure rolls back file writes
10. Moves files:
    - success -> `data/processed`
    - failure -> `data/error`

## Status history rules

Events are generated for changes in:
- `intake_date` -> `INTAKE_RECORDED`
- `client_status` -> `STATUS_CHANGED`
- `verified` false/null -> true -> `VERIFIED_MARKED`
- `date_emailed` -> `EMAILED_TO_CLIENT`
- `pickup_date` -> `READY_FOR_PICKUP`
- `logout_date` -> `LOGGED_OUT`
- `updated_date` -> `RECORD_UPDATED`

## How to run

From the `taxops` folder:

```bash
python main.py
```

## Production / office LAN (Epic #82 · #95 / #96)

NSSM-hosted deployments reachable from multiple PCs:

- **`python app.py`** serves with **Waitress** (multi-threaded WSGI) when **`FLASK_DEBUG` is not `1`**; leave production on Waitress and use `FLASK_DEBUG=1` only for local development. See GitHub **[#136](https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/136)** (**WSGI** epic). Optional concurrency check: **`python scripts/smoke_waitress_concurrency.py`**.

- **`docs/OFFICE_NETWORK.md`** — LAN IP/firewall (**Private** profile), bookmarks, staff onboarding, **`/health`**, troubleshooting matrix.
- **`scripts/smoke_deploy.py`** and **`scripts/smoke_deploy.ps1`** — probes **`GET /health`**, **`GET /login`** (versioned static CSS `?v=` links), and **`GET /static/app.js?v=…`** matching that token; non-zero exit when something is wrong. See GitHub **[#141](https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/141)** (CACHE epic).

- **Static cache busting:** set **`TAXOPS_VERSION`** or **`TAXOPS_APP_VERSION`** when you deploy so `?v=` on JS/CSS changes (#142–#144). Without git on the server, the app falls back to filesystem mtime for the version token.

See GitHub **[Production Hardening #82](https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/82)** and child issues (**#83** audit epic, PROD issues **#89–#96**).

## Now Serving (lobby take-a-number)

Single queue, two windows. Ships with the normal TaxOps NSSM restart — no extra service.

| Screen | URL | Auth |
|--------|-----|------|
| Public kiosk (iPad) | `/now-serving/kiosk` | **None** (intentional — not behind login) |
| Lobby display (TV / Pi) | `/now-serving/display` | **None** — large board + English/Spanish voice |
| Staff controls | Header **Now Serving** / Daily menu (modal on any page) | Receptionist / Preparer / Admin |
| End-of-day reset | modal button | Admin only |

`/now-serving/board` redirects to the dashboard with the modal open (bookmark-compatible).

### Lobby display (Raspberry Pi → HDMI TV)

Prefer a **Raspberry Pi + Chromium kiosk** over a Roku channel for v1: same live SSE feed as the staff board, bilingual TTS in the browser, no app-store review.

1. Point the Pi at TaxOps on the LAN, e.g. `http://192.168.1.173:5000/now-serving/display?autosound=1`.
2. Copy `scripts/now_serving_display_kiosk.sh` to the Pi, `chmod +x` it, set `TAXOPS_URL`, and run (or add to autostart).
3. Chromium flag `--autoplay-policy=no-user-gesture-required` plus `?autosound=1` skips the tap-to-enable gate so voice works after reboot.
4. Install Spanish voices on the Pi if needed (`espeak-ng` / Chromium language packs) so both `en-US` and `es-MX` announcements play.

**Roku:** a native BrightScript channel is not built yet. Short-term options: HDMI from the Pi, or any device that can open the display URL in a browser. A Roku channel can consume the same `/now-serving/api/snapshot` + `/now-serving/api/events` later.

Voice announces only when a window’s **serving** ticket changes (Call Next), English then Spanish — e.g. “Now serving number 12 at window 1” / “Ahora sirviendo el número 12 en ventanilla 1”. Tickets are plain day numbers (1, 2, 3…), not `A-###`.

### Lobby iPad setup

1. On the lobby iPad, open Safari to `http://<TaxOps-LAN-address>/now-serving/kiosk`.
2. Share → **Add to Home Screen** (full-screen web app metas are already on the page).
3. Enable **Guided Access** (Settings → Accessibility → Guided Access) and lock the iPad to that Home Screen icon so guests cannot leave the kiosk.
4. After a power loss, Guided Access must be started again manually. A future option is Apple Configurator 2 + Supervised **Single App Mode** — not built into TaxOps.

Ticket stubs print through the existing Filetrack print relay (`FILETRACK_RELAY_URL`, TCP 8765) as raw ZPL with the ticket label and window. Confirm with Moises before pointing at a live-traffic printer.
