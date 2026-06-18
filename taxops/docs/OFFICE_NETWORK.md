# Office LAN deployment (PROD-7 · Epic Production Hardening #82)

This note is for mounting TaxOps where **multiple workstations on the LAN** reach a **single Python/NSSM host** running `app.py`. It complements `scripts/nssm-taxops-snippet.ps1`, `.env.example`, and the PROD checklist issues **#89–#96**.

## 1. Bind to the LAN (already default)

TaxOps Flask uses `host='0.0.0.0'` in `app.py`, so it listens on **all IPv4 interfaces** on the configured port (**5000** unless you wrap it differently). Clients use:

`http://<SERVER_LAN_IP>:5000`

Find the office server address (PowerShell):

`Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -notmatch 'Loopback' }`

Prefer a **static DHCP reservation** or static IP so bookmarks and Ollama URLs stay stable.

## 2. Windows Defender Firewall — allow inbound TCP

On the TaxOps/NSSM host (Administrator PowerShell):

```powershell
New-NetFirewallRule `
  -DisplayName 'TaxOps Web (TCP 5000)' `
  -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5000 `
  -Profile Domain,Private
```

- **Private/Domain** restricts exposure vs **Public** Wi‑Fi. Adjust **`-Profile`** for your posture.
- If you expose a **different port** (reverse proxy), open that instead.

(Optional) tighten source to staff subnet only using **`-RemoteAddress 192.168.1.0/24`** (example).

## 3. Trusted LAN assumptions

TaxOps ships as an **internal** tool: session cookie auth and **HTTPS is not configured** out of the box. Keep it on VLAN/office LAN; do not port-forward to the public Internet without TLS and hardening beyond this checklist.

## 4. Smoke test after deploy/restart

```powershell
cd C:\TaxOps\taxops
.\.venv\Scripts\python.exe scripts\smoke_deploy.py http://127.0.0.1:5000
# From another workstation (replace IP):
curl.exe http://192.168.1.50:5000/health
```

Automate with Task Scheduler chained after NSSM restart, or run manually (PROD-8 / #96).

### 4a. Static JS/CSS after a deploy (CACHE / GitHub #141–#145)

TaxOps adds a **`?v=<token>`** query string to **`app.js`**, **`app.css`**, and **`tw.min.css`**, and Flask uses **`SEND_FILE_MAX_AGE_DEFAULT=0`** on `send_file`-backed responses.

1. On each production deploy, bump **`TAXOPS_VERSION`** (release label) or **`TAXOPS_APP_VERSION`** (**static-only** bump) in **NSSM AppEnvironmentExtra** or **`taxops/.env`**, then **restart** the service.
2. If you ship a **folder copy without `.git`**, the token falls back to bundled static file mtimes — still bump env vars when you want every workstation to hard-refresh behavior without relying on mtimes.
3. **Verify**: from any workstation, open **`/login`** → DevTools Network → confirm CSS requests include **`?v=`** and **`200`** responses. Run **`python scripts/smoke_deploy.py`** on the server for an automated check.


## 5. Staff onboarding (quick)

1. **Bookmark** `http://<SERVER_IP>:5000` (HTTPS only if you add a reverse proxy).
2. Credentials come from **`TAXOPS_USER` / `TAXOPS_PASS`** (`.env` or NSSM) — rotate via NSSM/office policy, never email passwords in plain text.
3. **`TAXOPS_SECRET`** must remain stable across service restarts (PROD-5 / #93) or sessions reset for everyone.
4. **AI Assistant** relies on **`OLLAMA_BASE_URL`** reachable from the TaxOps host (often another workstation or GPU PC on the LAN).
5. **`/health`** (#91) — green **200 + `"status":"ok"`** means app + SQLite are up; ops can monitor or script checks.

## 6. Troubleshooting matrix

| Symptom | Check |
|---------|------|
| Other PCs timeout | Firewall rule, ping, wrong IP, Wi‑Fi **guest isolation** |
| Same machine OK, LAN fails | `Get-NetFirewallRule`, binding, VLAN |
| 401/API JSON from fetch | Normal when session expired; user re-logs in |
| Health 503 | SQLite path/disks; NSSM **`TAXOPS_DB`** |

## Related issues

- **#94** Frontend error boundary + `/api/client-error` logs
- **#92** Nightly SQLite backup (`scripts/nightly_backup_db.py`)

Track full PROD scope on Epic **[Production Hardening — Office Deployment #82](https://github.com/mbustos27/Tax-Ops-Xcel-Financial/issues/82)**.
