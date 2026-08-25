# Restart TaxOpsService after template / deploy changes

**Audience:** Ops deploying `app.py` / templates  
**Service:** `TaxOpsService` · **Health:** `http://192.168.1.173:5000/health`  
**Related:** [Operations Runbook §1](../RUNBOOK.md#1-service-restart) · `scripts/restart_service.ps1` · `scripts/check_service_health.py`

> **Same share:** Workstation `T:\taxops\...` ≡ server `C:\TaxOps\taxops\...`.  
> Service DB must be the **local** path `C:\TaxOps\taxops\taxops.db` (not UNC). SQLite WAL over SMB fails.

This checklist is **manual**. Do not auto-restart without a human running each step. Never put `.env`, `SECRET.txt`, or secret values in this doc or script output.

---

## Checklist

### 1. Record pre-deploy health

```powershell
Invoke-RestMethod http://192.168.1.173:5000/health | Select-Object -ExpandProperty db
# or:
python T:\taxops\scripts\check_service_health.py
```

Confirm:

- `path` is `C:\TaxOps\taxops\taxops.db` (local, not `\\Xcel-server\...`)
- `size_bytes` matches `(Get-Item T:\taxops\taxops.db).Length` on the workstation
- `ok` is true

Write down path + size before continuing.

### 2. Stop the service

```powershell
# On the server, or via remote admin:
sc.exe stop TaxOpsService
# wait until STATE = STOPPED
```

Or use `.\scripts\restart_service.ps1` later for stop+start+smoke in one go — still complete steps 3–6 consciously.

### 3. Deploy template / code changes

Copy or pull updated files onto the share (`T:\` / `C:\TaxOps\taxops`). Typical: `templates\`, `app.py`, `static\`.

### 4. Restart the service

```powershell
cd C:\TaxOps\taxops
.\scripts\restart_service.ps1
# or:
sc.exe start TaxOpsService
```

### 5. Re-check health

```powershell
python T:\taxops\scripts\check_service_health.py
```

Confirm **DB path and size unchanged** vs step 1 (same file; size may grow slightly from WAL — large drops are suspicious).

### 6. Spot-check UI

With a logged-in browser session:

- `/email-inbox` loads
- Prep mode toggle / opening a return into prep workspace works (preparer/admin)
- `/admin/spouses-review` loads (expect ~0 pending after spouse remediation)

---

## Optional health script

```powershell
python T:\taxops\scripts\check_service_health.py
python T:\taxops\scripts\check_service_health.py --expect-path "C:\TaxOps\taxops\taxops.db"
python T:\taxops\scripts\check_service_health.py --expect-size-file T:\taxops\taxops.db
```

Exit **0** if `/health` is ok and path (and optional size) match; **non-zero** otherwise.
