# TaxOps Operations Runbook

**Audience:** Office staff / server admin  
**Server path:** `C:\TaxOps\taxops\` (adjust if deployed elsewhere)  
**Service name:** `TaxOpsService` (NSSM Windows service)  
**Web port:** `5000` (default; see `WAITRESS_PORT` env var)

> **Quick reference** — jump to a section:
> [Restart](#1-service-restart) · [Verify online](#2-verify-the-service-is-running) · [Logs](#3-log-locations) · [Health endpoint](#4-health-check-healthendpoint) · [Backup / Restore](#5-backup-and-restore) · [Smoke test](#6-smoke-test-after-deploy-or-restart) · [NSSM env vars](#7-updating-nssm-environment-variables) · [Gmail rotation](#8-gmail-app-password-rotation) · [Troubleshooting](#9-troubleshooting-matrix)

---

## 1. Service Restart

**Recommended — use the restart script (stops service, waits for RUNNING, then runs smoke test automatically):**

```powershell
cd C:\TaxOps\taxops
.\scripts\restart_service.ps1
```

Optional flags:
```powershell
.\scripts\restart_service.ps1 -SkipSmoke              # skip smoke test
.\scripts\restart_service.ps1 -BaseUrl http://192.168.1.141:5000  # remote target
```

**Manual stop / start with `sc.exe` (same as the script does internally):**

```powershell
sc.exe stop  TaxOpsService
Start-Sleep -Seconds 5
sc.exe start TaxOpsService
Start-Sleep -Seconds 10
sc.exe query TaxOpsService   # expect STATE: 4  RUNNING
```

**PowerShell service cmdlets (alternative):**

```powershell
Stop-Service  TaxOpsService
Start-Service TaxOpsService
Get-Service   TaxOpsService   # expect Status: Running
```

**If the service is not registered (first install or broken registration):**

```powershell
# Refer to scripts/nssm-taxops-snippet.ps1 for full setup.
# Minimal re-register (edit paths to match your deployment):
nssm install TaxOpsService "C:\TaxOps\taxops\.venv\Scripts\python.exe" "app.py"
nssm set TaxOpsService AppDirectory "C:\TaxOps\taxops"
nssm set TaxOpsService Start SERVICE_AUTO_START
Start-Service TaxOpsService
```

**After any manual restart — run the smoke test** (see §6).

---

## 2. Verify the Service is Running

### From the server itself

```powershell
# Health endpoint — returns JSON with status "ok"
Invoke-WebRequest http://127.0.0.1:5000/health | Select-Object -Expand Content
# Or via curl:
curl.exe http://127.0.0.1:5000/health
```

Expected response:
```json
{ "status": "ok", "db": { "ok": true }, "uptime_seconds": 42, ... }
```

`"status": "degraded"` or HTTP 503 means the SQLite database is unreachable — see §9.

### From any workstation browser on the office LAN

1. Open a browser and navigate to **`http://<SERVER_IP>:5000/health`**  
   Replace `<SERVER_IP>` with the server's LAN IP address (e.g., `192.168.1.50`).
2. You should see the JSON health response directly in the browser.
3. Navigate to **`http://<SERVER_IP>:5000`** for the full login page.

**Finding the server IP:**

```powershell
# Run on the TaxOps server:
Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -notmatch 'Loopback' } | Select IPAddress
```

Bookmark `http://<SERVER_IP>:5000` on every workstation. Use a **static DHCP reservation** or static IP to keep the address stable.

**Firewall prerequisite** (if LAN access isn't working):

```powershell
# Run once as Administrator on the TaxOps server:
New-NetFirewallRule `
  -DisplayName 'TaxOps Web (TCP 5000)' `
  -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5000 `
  -Profile Domain,Private
```

---

## 3. Log Locations

| Log | Default Path | Notes |
|-----|-------------|-------|
| NSSM stdout | `C:\TaxOps\logs\taxops_stdout.log` | Python `print()` + startup messages |
| NSSM stderr | `C:\TaxOps\logs\taxops_stderr.log` | Exceptions, traceback |
| App JSON log | `C:\TaxOps\logs\taxops-app.jsonl` | Structured lines — set `TAXOPS_LOG_JSON_PATH` |
| Backup errors | `C:\TaxOps\backups\backup_error.log` | Written on each backup failure |
| Import trace | `logs/import_trace.txt` | CSV import debug (relative to `taxops/`) |

**Tail the live log (PowerShell):**

```powershell
Get-Content "C:\TaxOps\logs\taxops_stderr.log" -Wait -Tail 50
```

**Tail structured JSON log** (if enabled):

```powershell
Get-Content "C:\TaxOps\logs\taxops-app.jsonl" -Tail 20 | ForEach-Object { $_ | ConvertFrom-Json }
```

---

## 4. Health Check (`/health` Endpoint)

`GET /health` is an unauthenticated JSON endpoint for ops monitoring.

**Response fields:**

| Field | Meaning |
|-------|---------|
| `status` | `"ok"` or `"degraded"` |
| `db.ok` | SQLite reachable |
| `db.latency_ms` | Round-trip time for a `SELECT 1` |
| `uptime_seconds` | Process uptime since last restart |
| `version` | App version string (`TAXOPS_VERSION`) |
| `audit_queue_depth` | Pending async audit writes |
| `schema_version` | Applied DB migration version |
| `schema_version_expected` | Version expected by this code build |
| `extraction_queue` | `{pending, processing, needs_review, failed, completed_today}` |

**Quick health check from any workstation:**

```
http://<SERVER_IP>:5000/health
```

**Script-friendly check (exits non-zero on failure):**

```powershell
$r = Invoke-WebRequest http://127.0.0.1:5000/health
$j = $r.Content | ConvertFrom-Json
if ($j.status -ne 'ok') { throw "TaxOps health degraded: $($j.db)" }
Write-Host "OK — uptime $($j.uptime_seconds)s, schema v$($j.schema_version)"
```

---

## 5. Backup and Restore

### Running a backup

**Automatic (nightly at 2:10 AM):** The Task Scheduler job `TaxOpsNightlyDbBackup` runs automatically. To register it for the first time (run once as Administrator):

```powershell
cd C:\TaxOps\taxops
powershell -ExecutionPolicy Bypass -File scripts\register-nightly-backup-task.ps1
# Verify registration:
Get-ScheduledTask -TaskName TaxOpsNightlyDbBackup
```

**Manual trigger from the admin UI:** Log in → Tools → **Backup** → click **Run backup now**.

**Manual trigger from command line:**

```powershell
cd C:\TaxOps\taxops
.\.venv\Scripts\python.exe scripts\nightly_backup_db.py
```

Backup files land in `C:\TaxOps\backups\` (configurable via `TAXOPS_BACKUP_DIR`), named:

```
taxops_backup_20260519T021012Z.sqlite
```

Files older than 30 days are pruned automatically each run (`TAXOPS_BACKUP_RETENTION_DAYS`).

### Checking for backup failures

```powershell
# View the error log:
Get-Content "C:\TaxOps\backups\backup_error.log" -Tail 20
# List recent backup files:
Get-ChildItem "C:\TaxOps\backups\*.sqlite" | Sort-Object LastWriteTime -Descending | Select -First 5
```

No backup file from today + entries in `backup_error.log` → investigate `taxops_stderr.log`.

### Restore procedure

1. **Stop the service:**
   ```powershell
   Stop-Service TaxOpsService
   ```

2. **Locate the backup file** you want to restore:
   ```powershell
   Get-ChildItem "C:\TaxOps\backups\*.sqlite" | Sort-Object LastWriteTime -Descending
   ```

3. **Copy it over the live database** (adjust path to match `TAXOPS_DB` env var, default is `taxops/taxops.db`):
   ```powershell
   # Replace with actual backup filename:
   Copy-Item "C:\TaxOps\backups\taxops_backup_20260519T021012Z.sqlite" `
             "C:\TaxOps\taxops\taxops.db" -Force
   ```

4. **Verify the restored file:**
   ```powershell
   $conn = [System.Data.SQLite.SQLiteConnection]::new("Data Source=C:\TaxOps\taxops\taxops.db")
   # Or verify with Python:
   .\.venv\Scripts\python.exe -c "
   import sqlite3, sys
   conn = sqlite3.connect('taxops.db')
   n = conn.execute('SELECT COUNT(*) FROM returns').fetchone()[0]
   print(f'Restored OK — {n} returns')
   "
   ```

5. **Start the service:**
   ```powershell
   Start-Service TaxOpsService
   # Run smoke test:
   .\.venv\Scripts\python.exe scripts\smoke_deploy.py http://127.0.0.1:5000
   ```

---

## 6. Smoke Test After Deploy or Restart

The restart script (`scripts/restart_service.ps1`) runs the smoke test automatically. To run it manually:

```powershell
cd C:\TaxOps\taxops
python smoke_test.py
# Override if running from a different machine:
python smoke_test.py http://192.168.1.141:5000
```

Expected output: `SMOKE OK    target=http://192.168.1.141:5000  endpoints=6`  
Results are always appended to `C:\TaxOps\logs\smoke.log` with an ISO timestamp.

**What `smoke_test.py` checks:**

| Endpoint | Accepted |
|---|---|
| `GET /health` | 200 |
| `GET /` | 200 or 302 |
| `GET /login` | 200 |
| `GET /review` | 200 or 302 |
| `GET /payments` | 200 or 302 |
| `GET /ai/status` | 200 or 302 |

302 is a pass for auth-protected pages (they redirect to `/login` when not logged in).

**Deep static-asset and health check (extended probe):**

```powershell
python scripts\smoke_deploy.py
```

Checks `?v=` cache-busting on CSS/JS and validates `/health` JSON payload.

**Adding new endpoints to the smoke test:**  
Edit `smoke_test.py` — add a `(path, (200, 302))` tuple to the `ENDPOINTS` list.

**Concurrent load smoke:**

```powershell
.\.venv\Scripts\python.exe scripts\smoke_waitress_concurrency.py http://127.0.0.1:5000
```

---

## 7. Updating NSSM Environment Variables

Use this procedure to **add, change, or rotate** any secret or config value (Ollama URL, passwords, API keys, backup paths) **without uninstalling or re-registering the service.**

### Change a single variable

```powershell
$NSSM    = 'nssm.exe'           # or full path
$Service = 'TaxOpsService'

# Update an existing variable:
nssm set $Service AppEnvironmentExtra "+TAXOPS_PASS=new_strong_password_here"

# Add a new variable (first variable must not have "+" prefix — use for initial setup only):
# nssm set $Service AppEnvironmentExtra "OLLAMA_BASE_URL=http://192.168.1.141:11434"
# For additional variables, always prefix with "+" to append rather than replace:
nssm set $Service AppEnvironmentExtra "+NEW_VAR=value"
```

> **Important:** Each `nssm set AppEnvironmentExtra` call with `+VAR=value` **appends** to the existing list. Without the `+` prefix it **replaces** the entire environment block. Always inspect the current block before editing (see below).

### View all current env vars

```powershell
nssm dump TaxOpsService
# Or specifically the environment block:
nssm get TaxOpsService AppEnvironmentExtra
```

### Apply the change

```powershell
Restart-Service TaxOpsService
# Verify:
.\.venv\Scripts\python.exe scripts\smoke_deploy.py
```

### Update only the Ollama URL (common task)

```powershell
cd C:\TaxOps\taxops
.\scripts\nssm-set-ollama-url.ps1 -Service TaxOpsService -OllamaUrl http://192.168.1.141:11434
```

### Rotate the app secret (`TAXOPS_SECRET`)

The Flask session signing key — must be stable across restarts (all logged-in sessions invalidate on change).

```powershell
# Generate a new secret:
.\.venv\Scripts\python.exe -c "import secrets; print(secrets.token_urlsafe(32))"
# Set it:
nssm set TaxOpsService AppEnvironmentExtra "+TAXOPS_SECRET=PASTE_OUTPUT_HERE"
Restart-Service TaxOpsService
```

### Full reference

See `scripts/nssm-taxops-snippet.ps1` for the complete environment variable reference, including all Ollama model tags, logging settings, and backup configuration.

---

## 8. Gmail App Password Rotation

TaxOps uses an **app password** (not the Gmail account password) for IMAP access. Rotate it whenever:

- A staff member with access to the mailbox account departs
- Google prompts a security review
- Routine quarterly rotation policy

### Step-by-step

1. **Log into the Google account** that owns the Gmail inbox TaxOps reads  
   (the account set in `IMAP_USER`, typically `office@...gmail.com`).

2. Navigate to **[myaccount.google.com → Security → App passwords](https://myaccount.google.com/apppasswords)**  
   (requires 2-Step Verification enabled on the account).

3. Find the existing **TaxOps** app password entry. Click **Revoke** / **Delete**.

4. Click **Create a new app password** → choose **Mail** + **Windows Computer** (or Other → name it "TaxOps").  
   Google generates a **16-character password** (spaces are decorative — omit them).

5. **Copy the password immediately** — Google will not show it again.

6. Update the NSSM env var on the TaxOps server:

   ```powershell
   nssm set TaxOpsService AppEnvironmentExtra "+IMAP_PASS=abcdefghijklmnop"
   Restart-Service TaxOpsService
   ```

7. **Verify mail watcher resumed:**

   ```powershell
   Get-Content "C:\TaxOps\logs\taxops_stderr.log" -Tail 30
   # Look for: "Mail watcher started" and no IMAP authentication errors.
   curl.exe http://127.0.0.1:5000/health
   # Check that "status": "ok" — no DB or worker errors.
   ```

8. If the watcher fails to connect, double-check:
   - `IMAP_HOST=imap.gmail.com` and `IMAP_PORT=993`
   - The Google account still has **2-Step Verification** enabled (required for app passwords)
   - The app password was entered **without spaces**

> **Fallback:** If mail watcher auth fails on start, TaxOps continues serving normally — it just won't import new emails until IMAP credentials are corrected. Staff can still upload documents manually.

---

## 9. Mail Watcher Setup and Verification

### 9a. Initial IMAP Configuration (MAIL-1)

The mail watcher reads `IMAP_HOST`, `IMAP_PORT`, `IMAP_USER`, and `IMAP_PASS` from the NSSM environment block.  Set them once on first deploy (or after a full reinstall):

```powershell
# Run on the server as Administrator
$Svc = 'TaxOpsService'

nssm set $Svc AppEnvironmentExtra "+IMAP_HOST=imap.gmail.com"
nssm set $Svc AppEnvironmentExtra "+IMAP_PORT=993"
nssm set $Svc AppEnvironmentExtra "+IMAP_USER=office@yourdomain.com"
nssm set $Svc AppEnvironmentExtra "+IMAP_PASS=xxxx xxxx xxxx xxxx"   # Gmail 16-char app password

# Verify the block looks correct before restarting:
nssm get $Svc AppEnvironmentExtra

Restart-Service $Svc
```

> **Gmail requirement:** 2-Step Verification must be enabled on the Google account.  
> Generate the app password at: **myaccount.google.com → Security → App passwords** → name it "TaxOps".  
> Enter the password **without spaces**.

Optional tuning variables (safe to omit — defaults shown):

```powershell
nssm set $Svc AppEnvironmentExtra "+IMAP_FOLDER=INBOX"
nssm set $Svc AppEnvironmentExtra "+IMAP_POLL_INTERVAL=60"         # seconds
nssm set $Svc AppEnvironmentExtra "+MAIL_WATCHER_CLIENT_MATCH_MIN_SCORE=82"
nssm set $Svc AppEnvironmentExtra "+MAIL_LOW_CONF_THRESHOLD=88"    # below this → pending review
```

### 9b. Verifying Mail Watcher Starts in Service Context (MAIL-2)

After restarting the service, confirm the watcher thread is alive:

1. **Check the log** for the startup line:

   ```powershell
   Select-String "Mail watcher" "C:\TaxOps\logs\taxops_stderr.log" | Select-Object -Last 5
   # Expected: Mail watcher started
   # Bad sign: IMAP authentication failed / Connection refused
   ```

2. **Query `/health`** — the `workers.mail_watcher` block reports thread liveness:

   ```powershell
   (Invoke-RestMethod http://127.0.0.1:5000/health).workers.mail_watcher
   # Expected: @{started=True; running=True; configured=True}
   # If configured=False → IMAP_HOST env var not set (check NSSM block)
   # If running=False   → thread crashed; tail taxops_stderr.log for exception
   ```

3. **Tail logs** during a live poll cycle (runs every `IMAP_POLL_INTERVAL` seconds):

   ```powershell
   Get-Content "C:\TaxOps\logs\taxops_stderr.log" -Wait | Select-String "Folder|unseen|watcher"
   # Expected per cycle:  "Folder INBOX: N new of M unseen (0 already seen)"
   ```

### 9c. End-to-End Mail Import Smoke Test (MAIL-4)

Use this procedure to confirm the full pipeline — from incoming email to document appearing in a return — works after a deploy or reconfiguration.

**Prerequisites:** Mail watcher is running (§9b), at least one client exists in the system with a current open return.

**Steps:**

1. From any email account, send a test email **to the TaxOps Gmail inbox** (`IMAP_USER`):
   - **Subject:** `Test document – <Client Last Name> <Client First Name>` (e.g. `Test document – Garcia Yakelin`)
   - **Body:** any text
   - **Attachment:** any small PDF or image

2. Wait one poll cycle (default 60 s) and check the log:

   ```powershell
   Get-Content "C:\TaxOps\logs\taxops_stderr.log" -Tail 20
   # Look for: "saved 1 attachment(s)" or "Routed OK" for that client name
   ```

3. Log into TaxOps → open the client's return → confirm the attachment appears under **Documents**.

4. **Low-confidence match check:** If the client name in the subject is slightly misspelled, the match score will land between 82–88 and the email will appear in **Email Review → Pending Review** instead of auto-attaching.  A staff member must confirm or reject it from that queue.

**Automated script (optional):**  
`scripts/smoke_deploy.py` hits all critical HTTP endpoints; it does not send test emails.  For a full IMAP round-trip test, use the manual steps above or a dedicated integration fixture.

---

## 10. Troubleshooting Matrix

| Symptom | Check | Fix |
|---------|-------|-----|
| Other workstations can't reach the app | Firewall rule (§2), server IP changed | Add firewall rule; update bookmarks |
| `GET /health` returns 503 | `db.ok: false` in response | Check `TAXOPS_DB` path, disk space, SQLite lock |
| Service starts then stops immediately | `taxops_stderr.log` tail | Missing env var, bad Python path, port conflict |
| Login fails with valid credentials | `TAXOPS_USER`/`TAXOPS_PASS` mismatch | Check NSSM AppEnvironmentExtra with `nssm dump` |
| AI / chat not responding | Ollama unreachable | Verify `OLLAMA_BASE_URL`, run `ollama list` on GPU host |
| Email not importing | IMAP auth error in stderr log | Rotate Gmail app password (§8) |
| Slow page loads | Worker threads exhausted | Raise `WAITRESS_THREADS` (default 8) via NSSM |
| `schema_version` mismatch in `/health` | Pending migrations | Restart service — `init_db` runs on startup |
| Backup error log has entries | Disk full, path missing | Check `TAXOPS_BACKUP_DIR` path; free disk space |
| `Failed Documents` badge count is high | Extraction worker failing | Check stderr log; verify Ollama model tags match `ollama list` |
| Session resets on every restart | `TAXOPS_SECRET` not set or changes | Set a **stable** secret in NSSM (§7) |

---

## 11. Static Asset Cache Busting (After Code Deploy)

When you ship new CSS or JavaScript, browsers cache the old version until the query string changes.

```powershell
# Option A — bump full version (semantic release label):
nssm set TaxOpsService AppEnvironmentExtra "+TAXOPS_VERSION=1.5.0"

# Option B — bump static-only token (no version change):
nssm set TaxOpsService AppEnvironmentExtra "+TAXOPS_APP_VERSION=20260519a"

Restart-Service TaxOpsService
```

Verify from a workstation: open `/login` → DevTools → Network → confirm CSS/JS requests include `?v=` query string and return `200`.

---

## 12. Related Scripts and Files

| File | Purpose |
|------|---------|
| `scripts/nssm-taxops-snippet.ps1` | Full NSSM install / environment reference |
| `scripts/nssm-set-ollama-url.ps1` | One-liner Ollama URL update |
| `scripts/register-nightly-backup-task.ps1` | Register Task Scheduler backup job |
| `scripts/nightly_backup_db.py` | Manual backup (also called by Task Scheduler) |
| `scripts/smoke_deploy.ps1` | PowerShell wrapper for smoke test |
| `scripts/smoke_deploy.py` | Python smoke probe — all critical endpoints |
| `scripts/smoke_waitress_concurrency.py` | Concurrent load smoke test |
| `.env.example` | All supported env vars with documentation |
| `docs/OFFICE_NETWORK.md` | LAN firewall / multi-workstation setup |
| `taxops/backup.py` | Admin UI backup runner (used by Tools → Backup) |

---

*Last updated: 2026-05 · OPS epic #193*
