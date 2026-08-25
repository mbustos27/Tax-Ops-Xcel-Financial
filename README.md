# Tax-Ops-Xcel-Financial

Shared drive / repo root for Xcel Financial's internal tooling. Mapped locally as `T:\` (git remote: `//Xcel-server/taxops`).

This root has grown organically — treat this file as a map, not a guarantee that everything below `T:\` is production code. See **§3** for an honest accounting of what's here.

---

## 1. The real application: `taxops/`

Everything that actually runs in production lives in **[`taxops/`](taxops/)** — a single Flask + Waitress process (`taxops/app.py`, ~5,500 lines) managed as the Windows service `TaxOpsService`, serving the office LAN at `http://<server-ip>:5000`. No public internet exposure.

**Start here, in this order, depending on what you need:**

| Need | Doc |
|---|---|
| Restart the service, read logs, check health, rotate secrets, troubleshoot | **[`taxops/docs/RUNBOOK.md`](taxops/docs/RUNBOOK.md)** |
| Understand the whole app — modules, DB schema, background workers | **[`taxops/docs/CODEBASE_OVERVIEW.md`](taxops/docs/CODEBASE_OVERVIEW.md)** |
| Set up LAN access / firewall for a new workstation | **[`taxops/docs/OFFICE_NETWORK.md`](taxops/docs/OFFICE_NETWORK.md)** |
| Legacy importer-era notes (mostly superseded by the Flask app, kept for the CSV/manual-LOG-spreadsheet history) | **[`taxops/README.md`](taxops/README.md)** |
| Hard invariants the app must never violate (email architecture, privacy, DB migrations) | **[`taxops/.cursor/rules/taxops-invariants.mdc`](taxops/.cursor/rules/taxops-invariants.mdc)** |

Quick health check from anywhere on the LAN: `http://<server-ip>:5000/health`.

---

## 2. Filetrack — barcode label printing & file scanning (`taxops/filetrack/`)

A subsystem for printing barcode labels on physical client files and (in progress) scanning them back to bulk-update a file's status, without touching a keyboard. Not yet covered by `RUNBOOK.md` — this section is the current source of truth until it's folded in there.

### 2.1 Architecture — four phases

| Phase | Module | What it does | Status |
|---|---|---|---|
| **M0** | `filetrack/hardware_validation/` | Validate the *real* printer/scanner hardware before trusting anything built on top of it | **Confirmed 2026-07-22** — see §2.3, `FINDINGS.md` |
| **M1** | `filetrack/labels/` | Render a log number into a ZPL barcode label and send it to the printer (`print_label.py`), or relay the job over HTTP to a printer on a different machine (`relay_client.py` + `relay/server.py`) | Done — verified working (§2.2) |
| **M2** | `filetrack/listener/` | Turn a live stream of barcode scans into `Assignment(log_number, status)` decisions via a sticky status state machine (`parser.py`, `state.py`, `run_listener.py`) | Code + tests complete, hardware confirmed — **not run as a live process yet**, see §2.5 |
| **M3** | `filetrack_service.py`, `routes/filetrack.py`, `filetrack/listener/http_sink.py` | Wire M2's Assignments into TaxOps: `POST /filetrack/status` → `filetrack_service.apply_filetrack_status()` → updates `returns.filetrack_status`/`filetrack_status_updated_at` **and** `returns.client_status` (the real status shown throughout the app — see the 2026-07-22 incident in §2.5) when matched by `log_number`, and always appends one row to `filetrack_status_history`, even for an unmatched log number | **Already built, tested, and live** (see §2.4/§2.5) |

> Corrects an earlier version of this section written 2026-07-22 that said M3 was "not started" — it was already committed in `f23aefb` ("Add Work Order Creator... plus filetrack barcode tracking..."). Verified by reading the actual code, not by trusting that old note.

**Why the printer runs "relay mode":** `TaxOpsService` runs as a Windows service (Session 0) with no access to a printer physically attached to a different front-desk workstation. So `app.py`'s intake hook POSTs `{"log_number": "..."}` to `filetrack.relay.server`, a small Flask app running *on the print-station PC*, which does the real `win32print` call locally. Config: `FILETRACK_PRINT_MODE=relay`, `FILETRACK_RELAY_URL`, `FILETRACK_RELAY_TOKEN` in `taxops/.env`.

### 2.2 How to test the print relay

1. **Health check** (relay process up + sees the printer — does *not* print anything):
   ```powershell
   Invoke-WebRequest http://<print-station-ip>:8765/health -UseBasicParsing
   # {"status":"ok","printer_configured":"4BARCODE 4B-2054A","printer_found":true}
   ```
2. **Real end-to-end test print** (exercises the exact path a live intake uses — network → relay auth → render → `win32print`):
   ```powershell
   $headers = @{ "Content-Type"="application/json"; "X-Filetrack-Token"="<FILETRACK_RELAY_TOKEN from taxops/.env>" }
   Invoke-WebRequest -Uri "http://<print-station-ip>:8765/print" -Method Post -Headers $headers -Body '{"log_number":"TESTPRINT"}' -UseBasicParsing
   ```
   `"TESTPRINT"` is non-numeric, so it's never confused with a real log number and never touches client data — it just prints literally as-is on the label.
3. **Local-only test, on the print station itself** (bypasses the relay/network — tests printer + driver only):
   ```
   python -m filetrack.labels.print_label --log TESTPRINT
   python -m filetrack.labels.print_label --log TESTPRINT --dry-run   # render ZPL to console, don't print
   ```
4. **Automated diagnose + auto-fix**, run *on the print station* (checks Python/deps/printer visibility/relay process/NSSM persistence/firewall, fixes what it safely can):
   ```
   T:\diagnose_fix_print_relay.bat
   ```

Common failure signatures in `taxops_stderr.log` (see `filetrack/labels/relay_client.py::_classify_network_error` for the full decoder):

| Error | Meaning |
|---|---|
| `ConnectTimeout` — no response at all | Print-station PC is off/asleep/disconnected, or its IP changed, or firewall is silently dropping the connection |
| `ConnectionError` / connection refused | PC is up and reachable, but the relay process itself isn't running |
| `ReadTimeout` | Relay accepted the connection but never responded — likely a stuck print spooler |

### 2.3 M0 hardware validation procedure

**Run this before trusting filetrack in production, and before ever wiring M2/M3 in.** Standalone — no dependency on the rest of `filetrack/`. Run on the Windows box with the printer/scanner physically attached. Full version: `taxops/filetrack/hardware_validation/README.md`.

**Step 1 — confirm the printer sends/receives ZPL correctly:**
```
python -m filetrack.hardware_validation.print_test --printer "4BARCODE 4B-2054A"
```
Prints 3 test labels: `LOG:00123`, `STATUS:FINALIZE`, and one with tricky characters (colon, hyphen) to confirm the barcode delimiter survives. Add `--dry-run` to preview the ZPL without printing; add `--list-printers` first to confirm the exact installed name (`4BARCODE 4B-2054A` is the name in `taxops/.env`'s `FILETRACK_PRINTER` — the module's own docstrings use a generic example name `"Arkscan 2054A"` for the same physical printer, so trust `--list-printers`/`.env` over the docstrings).

**Step 2 — confirm what the scanner actually sends:**
```
python -m filetrack.hardware_validation.scan_probe --mode hid
```
(or `--mode serial --port COM3` if the scanner outputs over a COM port instead of HID keyboard-wedge). Scan the three labels from Step 1, one at a time. For each scan it prints and appends to `scan_probe_log.jsonl`:
- the exact `repr()` received, including terminator characters, so you can tell CR vs LF vs CRLF vs TAB;
- whether the `:` delimiter survived (`LOG:00123` vs. a mangled `LOG00123`/`LOG;00123`);
- the wall-clock gap since the previous scan — also use this to do a burst test (buffer several scans in the scanner's storage mode, release them all at once) vs. a slow one-at-a-time test, and compare the timing pattern.

**Step 3 — record findings:**
Copy `filetrack/hardware_validation/FINDINGS_TEMPLATE.md` → `FINDINGS.md` in the same directory and fill it in from what Step 2 showed. This is the file `filetrack/config.py`'s defaults (`DEFAULT_SCAN_SUFFIX`, `DEFAULT_SCAN_MODE`, `PREFIX_DELIMITER`) are meant to be reconciled against — once confirmed, flip `HARDWARE_FINDINGS_CONFIRMED = True` there. **If the colon was mangled, don't just pick a new delimiter — that requires updating `filetrack.labels.template` and `filetrack.listener.parser` together, in the same change.**

**Done — confirmed 2026-07-22.** `FINDINGS.md` now exists and is complete: terminator (`\n`), scan mode (HID), delimiter (`:`) survival, `print_test.py`'s 3 test labels, label geometry, and a burst-vs-live comparison all checked out against the real hardware. `HARDWARE_FINDINGS_CONFIRMED = True` in `filetrack/config.py`.

### 2.4 M3 — wiring the scan listener into TaxOps

**M0 is confirmed** (2026-07-22, `filetrack/hardware_validation/FINDINGS.md`, `HARDWARE_FINDINGS_CONFIRMED = True` in `filetrack/config.py`) — nothing below is blocked on hardware validation anymore.

**Endpoint:** `POST /filetrack/status` (`taxops/routes/filetrack.py`) — takes `{"log_number", "status", "scanned_at"?, "source"?}`, requires header `X-Filetrack-Token` matching `FILETRACK_TOKEN`, requires `FILETRACK_ENABLED=true` (404s otherwise, so it doesn't even advertise the feature when off). On a matched `log_number`, this now updates **both** `returns.filetrack_status` (a scan-specific shadow field/audit trail) **and** `returns.client_status` — the real workflow status shown on the return detail page, reports, etc. — mirroring `app.py`'s own `/api/return/<id>/status` side effects (date stamps, `status_events`, `REJECTED` contact-field clearing). See the 2026-07-22 incident below for why the client_status sync had to be added explicitly. Diagnostic companion: `GET /api/admin/filetrack-config` (admin-only) reports what config *this running server process* actually has loaded right now — never the secrets themselves, just whether they're set — because a `.env` edit only takes effect after the next service restart, and this is how you confirm a restart actually picked it up. `GET /api/admin/filetrack-history/<log_number>` (admin-only) shows the `filetrack_status_history` shadow-field timeline (still useful, e.g. for unmatched scans) — for confirming the *visible* status, just check the return's page directly, or `status_events` for that return.

**Test the endpoint directly** (no scanner needed — proves the DB-writing path end to end):
```powershell
$headers = @{ "Content-Type"="application/json"; "X-Filetrack-Token"="<FILETRACK_TOKEN from taxops/.env>" }
Invoke-WebRequest -Uri "http://<taxops-server-ip>:5000/filetrack/status" -Method Post -Headers $headers `
  -Body '{"log_number":"123","status":"FINALIZE","source":"manual-test"}' -UseBasicParsing
# {"success":true,"matched":true,"return_id":..,"old_status":null,"new_status":"FINALIZE","client_status_synced":true}
```

**Run the actual listener process:** on the scan-station PC (same machine as the print relay, physically attached ScanAvenger scanner),
```
T:\start_filetrack_listener.bat
```
This sets `FILETRACK_TOKEN`/`FILETRACK_ENDPOINT_URL` and runs `python -m filetrack.listener.run_listener --sink http`, which POSTs every confirmed Assignment to the endpoint above (with retry/backoff — `filetrack/listener/http_sink.py`). To make it start automatically at login, run `T:\install_filetrack_listener_autostart.bat` once.

**⚠ In `--mode hid`, do not run the listener as a headless NSSM service, unlike the print relay** — a barcode scanner in HID mode "types" into whichever window has UI focus on the interactive desktop; an NSSM service has no desktop and no focus, so scans would go nowhere (or wherever an interactive user happened to have clicked). That's why `install_filetrack_listener_autostart.*` uses a normal, non-minimized Startup-folder shortcut instead of NSSM. **This limitation goes away entirely in `--mode serial` — see §2.6.**

### 2.5 M3 go-live checklist

1. **Restart `TaxOpsService`** (RUNBOOK §1) so it picks up `FILETRACK_TOKEN`/`FILETRACK_ENDPOINT_URL` from `taxops/.env`, **and** the CSRF-exemption fix below.
2. **Confirm the restart picked them up:** log in as admin → `GET /api/admin/filetrack-config` → check `status_token_configured: true` and `status_endpoint_url` is the real server address, not localhost. `config_loaded_seconds_ago` should be small (just restarted).
3. **Start the listener** on the scan-station PC: `T:\start_filetrack_listener.bat` (or install autostart once — `T:\install_filetrack_listener_autostart.bat`). Click into that console window before scanning anything.
4. **Do one real end-to-end test**, on an existing test/dummy return if you have one: scan a `STATUS:` label, then a `LOG:` label for that return's log number. Watch the listener window log `assignment log=... status=...` and (with `--sink http`) `delivered log=... status=...`.
5. **Verify the DB actually changed AND is visible:** admin → `GET /api/admin/filetrack-history/<log_number>` → should show your test scan as the most recent entry with the right `old_status`/`new_status`. **Also open that return's page in the app** and confirm the status badge itself shows the new status — see the 2026-07-22 client_status-sync incident below; a green `filetrack-history` result alone does not prove the visible status moved.

Everything else (schema, service function, endpoint, auth, retry/backoff, tests) is already done — this checklist is only the operational "turn it on" steps.

**Incident (2026-07-22, fixed): first live listener run rejected every scan with `400 CSRF token missing or invalid`.** `app.py` applies Flask-WTF's `CSRFProtect(app)` globally, which protects every POST route by default — including `/filetrack/status`, even though it's a headless machine endpoint with no browser session and no CSRF token to send (it authenticates via `X-Filetrack-Token` instead). Fixed with `_csrf.exempt(api_filetrack_status)` in `app.py`, right after the blueprint is registered (exempts that one view function specifically, not the whole blueprint, so any *future* POST route added to `routes/filetrack.py` still needs its own deliberate exemption decision). Covered by a new regression test, `test_endpoint_is_csrf_exempt` in `tests/test_filetrack_endpoint.py` — the rest of the suite never caught this because `conftest.py` disables CSRF enforcement for the whole test suite by default (`WTF_CSRF_ENABLED = False`); this test deliberately re-enables it, the same pattern `tests/test_csrf.py`'s `csrf_client` fixture uses. **Requires a `TaxOpsService` restart to take effect** (step 1 above) — this fix is in `app.py`, not `.env`.

**Incident (2026-07-22, fixed): after the CSRF fix, scans were "delivered" successfully but the visible return status never changed.** Root cause: `filetrack_service.apply_filetrack_status()` only ever wrote to `returns.filetrack_status` — a separate shadow column nothing in the UI reads. `return_detail.html` and every other status badge in the app read `returns.client_status`, updated only by `app.py`'s `POST /api/return/<id>/status` (`api_status`). A `delivered` log line from the listener only proves the HTTP round-trip returned <400 — it does not prove `matched: true`, and even when matched, it never touched `client_status`. Fixed by having `apply_filetrack_status()` also update `client_status` on a matched scan, mirroring `api_status`'s side effects exactly (date-stamp fields via a mirrored `_STATUS_DATE_STAMP` dict, a `status_events` row with `source_file='FILETRACK'`, `REJECTED` contact-field clearing) — minus the receptionist role-gate, which doesn't apply to a session-less machine event. `filetrack_status`/`filetrack_status_history` keep their original, unchanged meaning (still catches unmatched scans, which `status_events` can't since it requires a real `return_id`). Covered by new tests in `tests/test_filetrack_service.py` (`test_scan_sets_date_stamp_like_api_status`, `test_scan_to_rejected_clears_contact_fields_like_api_status`, etc.) and an assertion in `test_filetrack_endpoint.py`. **Requires a `TaxOpsService` restart to take effect** — this fix is in `filetrack_service.py`, not `.env`.

**Incident (2026-07-22, fixed): after both fixes above, live return `1282` still never updated across 6 real scans — every one recorded with `return_id: null` in `filetrack_status_history`.** Root cause: `1282`'s `log_number` was stored in the DB unpadded (`"1282"`, 4 chars), but a real scan's `log_number` argument is **always already zero-padded** (`"01282"`) because the barcode itself was printed via `filetrack.config.format_log_number()`. `apply_filetrack_status()`'s existing "try the raw/unpadded form too" fallback only helps when the *caller's* input is unpadded (e.g. a manual API test with `log_number="7"`) — for a real scan, `raw_log == formatted_log` (both `"01282"`), so that fallback silently never fires, and neither form matches the DB's `"1282"`. Fixed by adding a third fallback that strips leading zeros (`str(int(formatted_log))`) and tries that too, closing the gap regardless of which side has the padding. **This means any return whose `log_number` predates the 5-digit zero-pad convention (unpadded, 2+ digits) was silently unmatchable by every scan until this fix** — not just return 1282, likely other older returns too. Covered by `test_matches_unpadded_multidigit_log_number_scanned_as_padded` in `tests/test_filetrack_service.py`. **Requires a `TaxOpsService` restart to take effect** — this fix is in `filetrack_service.py`, not `.env`. If a scan still shows `return_id: null` in `filetrack-history` after this restart, the log_number truly doesn't exist in `returns` at all (typo on the label, wrong return, or barcode misread) — check the return's `log_number` field directly against what you scanned.

### 2.6 Removing the HID focus requirement — switching the ScanAvenger to USB-COM (serial) mode

**Why:** in `--mode hid` (M0's confirmed default), the scanner "types" into whatever window has UI focus on the interactive desktop, which is why `start_filetrack_listener.bat`'s console window has to stay focused and can't be a headless service. The ScanAvenger hardware itself supports a **USB-COM (virtual serial port)** mode that eliminates this — the listener then reads from a COM port directly, independent of window focus, and can run as a real background NSSM service like the print relay already does.

**Step 1 — install ScanAvenger's virtual COM-port driver, on the scan-station PC:**
1. Download `Virtual-COM-Port-Driver.exe` from ScanAvenger (see their [COM Port Configuration guide](https://scanavenger.com/wp-content/uploads/2024/03/COMPort-Configuration-1D-1D2D-v4.pdf)).
2. **Disconnect the USB dongle first.**
3. Right-click the installer → "Run as Administrator".

**Step 2 — switch the scanner to USB-COM mode:**
Scan the "USB serial (USB-COM)" configuration barcode from the scanner's own manual (not a product barcode — a printed setting barcode; see the [1D/1D&2D manual](https://scanavenger.com/wp-content/uploads/2023/10/ScanAvenger-User-Manual-v5.0-1D2D-Scanners.pdf), "Communication mode switching" section). Reconnect the dongle afterward — Windows will enumerate it as a new entry under Device Manager → **Ports (COM & LPT)**. Note that COM port number, e.g. `COM5`.

Baud rate: left at the scanner's default (**9600**), which matches `filetrack.config.DEFAULT_SERIAL_BAUD` — no override needed on either side.

**Step 3 — test manually before committing to a service:** edit `T:\start_filetrack_listener.bat`, set `MODE=serial` and `COMPORT=<the port from step 2>` near the top, then run it. It should log `Serial connected: COM<n> @ 9600 baud` and no longer print the "keep this window focused" warning. Scan a `STATUS:`/`LOG:` pair exactly like the HID go-live test (§2.5 step 4) and confirm the same `assignment`/`delivered` log lines, and that the return's status badge actually updates in the app.

**Step 4 — once step 3 works, make it a real background service** (no more focus requirement, no more Startup-folder shortcut fragility):
```
T:\install_filetrack_listener_nssm.bat COM5
```
(replace `COM5` with your actual port). This installs/starts a Windows service `FiletrackListener` — auto-starts at boot, restarts itself on crash, needs no one logged in. Mirrors `install_print_relay_nssm.ps1`'s already-proven pattern exactly (UNC `AppDirectory`, log rotation, `-RunAs CurrentUser`/`LocalSystem` choice). Afterward, remove the old Startup-folder shortcut (`install_filetrack_listener_autostart.ps1`'s shortcut) and close any manually-run console window — running two listeners at once means double-processing every scan.

**Not yet done as of this writing:** step 3's manual serial-mode test hasn't been run against the real hardware yet. `filetrack/hardware_validation/FINDINGS.md`'s "Scanner output mode confirmed" checkbox still shows HID only — update it (or add a serial-mode addendum) once step 3 is confirmed working, same rigor as the original M0 HID validation.

---

### 2.7 Live status-count polling in the nav bar (2026-07-22)

**Why:** once M3 went live, a scan at the station changes a return's `client_status` server-side at any moment, independent of anyone in the office navigating or reloading a page. Before this, the per-status counts in the nav bar's "status summary bar" (the row of colored dots + numbers under the top nav, present on every page via `base.html`) were computed once at render time in `base_ctx()` and only changed on the next page load — so a scan could move a folder from `EFILE READY` to `PICKUP` and the nav bar would keep showing the stale count until someone happened to refresh.

**What was built:**
- `GET /api/dashboard/status-counts?year=<n>` (`app.py`, `@login_required`) — returns `{"status_counts": {...}, "total": n}` using the exact same `get_status_counts()` query `base_ctx()` already runs on every page load. Benchmarked at **~1.3ms** against the live `returns` table (1,727 rows) — negligible even polled every few seconds, since it does nothing else (no template render, no other nav-badge queries).
- `base.html`'s status summary bar now has `id="status-summary-bar"` with `data-year`, each status `<a>` has `data-status="<STATUS NAME>"`, its count `<span>` has class `status-count-value`, and the total has `id="status-total-count"`.
- A small polling script (same file, guarded by `{% if current_user_name %}` like the existing unread-documents badge poller right above it) calls the endpoint every **5 seconds** and updates those elements in place, plus each item's `aria-label`.
- Covered by `tests/test_dashboard_status_counts.py`, including a test that calls `filetrack_service.apply_filetrack_status()` (the real M3 scan path) directly and confirms the endpoint reflects the change immediately — proving this closes the exact gap described above, not just that the endpoint returns *some* JSON.

**Scope note:** only the status summary bar (row 1, the dots+counts) polls live. The Tools-dropdown "Pickup Queue"/"E-File Queue" badges (also `status_counts`-derived, further up in `base.html`) were deliberately left on page-load-only — they're conditionally rendered (`{% if pc > 0 %}`, no element exists in the DOM at all when the count is 0), which would need a different, riskier JS pattern (create/destroy nodes, not just update `textContent`) to poll safely. Revisit if that staleness turns out to matter in practice.

**No restart-required caveat here** unlike the `filetrack_service.py` fixes above — this is a new endpoint + template change, live as soon as the app process picks up the edited files (still needs the normal `TaxOpsService` restart to deploy, just not for a *behavioral* reason like the CSRF/client_status fixes had).

---

## 3. Everything else at the repo root

Honest inventory, not an endorsement:

- **`tools/`** — standalone one-off utilities (e.g. `name_reconciliation/`, `color_palette_analysis/`). Not imported by `taxops/`.
- **`scripts/`** — machine setup / networking helpers (`map_t_drive.*`, `setup_this_pc.*`, `add_taxlog_host.*`) for provisioning a new workstation to reach this share and the TaxOps server. Distinct from `taxops/scripts/`, which are operational scripts for the app itself (see RUNBOOK §12).
- **`punchbridge/`** — currently empty; reserved/in-progress.
- **A large number of loose, ad-hoc `.py`/`.ps1`/`.bat` scripts directly at `T:\`** (`cleanup_*.py`, `fix_*.py`, `check_*.py`, `diag*.py`, `restart_ledgerbridge.*`, `install_print_relay_*`, etc.) — one-off diagnostic, data-repair, or incident-response scripts written during specific investigations. They are **not a maintained toolkit**; most are not referenced by anything else and many are safe to delete once their investigation is closed out. If you're not sure whether one is still needed, check `git log` on it before relying on it.
- **`AUDIT_REPORT.md` / `REMEDIATION_REPORT.md`** — a point-in-time adversarial audit + remediation record for a separate `ledgerbridge` module, not the main `taxops` app.

If you're trying to figure out "is this script still relevant," prefer asking in-context (or checking git blame/log) over assuming — this root has accumulated faster than it's been cleaned up.
