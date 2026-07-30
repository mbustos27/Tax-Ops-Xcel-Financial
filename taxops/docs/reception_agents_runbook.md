# Reception agents runbook

**Audience:** office staff (EN / ES) + IT (EN).  
**Machine:** RECEPTION `192.168.1.9` — share `\\Xcel-server\taxops` (`T:`).

---

## Why two start mechanisms (IT)

| Agent | Port | Mechanism | Why |
|-------|------|-----------|-----|
| Print relay | 8765 | **NSSM Windows service** `FiletrackRelay` | `win32print` works in session 0. Survives logoff/reboot so other desks can print labels when reception is empty. |
| Scan agent | 8766 | **Scheduled Task** `TaxOps Scan Agent` at **interactive logon** | **WIA requires an interactive desktop.** A session-0 service will not see the Epson correctly. Do **not** "consolidate" the scan agent into NSSM. |

Path note: the Windows user is literally named `Windows 10` — always quote paths like `"C:\Users\Windows 10\AppData\Local\Programs\Python\Python314\python.exe"`.

APIPA note: RECEPTION may show `169.254.x` adapters — disable unused NICs separately; do not change network here.

---

## Staff — restart (EN)

### Print labels not working
1. On reception PC, open PowerShell **as Administrator**.
2. Run:
   ```text
   Restart-Service FiletrackRelay
   ```
3. Or double-click: `T:\check_reception.bat` — look for `PRINT OK`.
4. Manual fallback: `T:\start_print_relay.bat` (leave window open).

### Scanner / intake scan not working
1. Log in as the reception Windows user (scanning needs a logged-in session).
2. Double-click: `T:\GO_SCAN_AGENT.bat` **or** run:
   ```text
   Start-ScheduledTask -TaskName "TaxOps Scan Agent"
   ```
3. Confirm `T:\check_reception.bat` shows `SCAN OK`.
4. Leave any minimized agent running; do not end the `python` process for scan_agent unless restarting.

### Quick health
```text
T:\check_reception.bat
```
- Exit code **0** = both OK  
- Exit code **1** = one or both failed (read the PRINT/SCAN lines)

Deep report (opens Notepad):
```text
T:\diagnose_reception_relays.bat
```

---

## Personal — reinicio (ES)

### No imprime etiquetas
1. En la PC de recepción, PowerShell **como Administrador**.
2. Ejecutar: `Restart-Service FiletrackRelay`
3. O abrir `T:\check_reception.bat` y verificar `PRINT OK`.
4. Respaldo manual: `T:\start_print_relay.bat` (dejar la ventana abierta).

### No escanea (intake)
1. Iniciar sesión en Windows en recepción (el escáner necesita sesión interactiva).
2. Abrir `T:\GO_SCAN_AGENT.bat` o ejecutar: `Start-ScheduledTask -TaskName "TaxOps Scan Agent"`
3. Verificar `SCAN OK` con `T:\check_reception.bat`.

### Chequeo rápido
`T:\check_reception.bat` — código **0** = bien; **1** = falló algo.

---

## Reading `check_reception.ps1` failures (IT)

| Line | Meaning |
|------|---------|
| `PRINT FAIL Unable to connect` | Service down or port 8765 blocked — `Restart-Service FiletrackRelay`, check `C:\TaxOps\logs\print_relay.log` |
| `PRINT WARN printer_found=false` | Relay up; Windows printer missing/renamed — Device Manager / 4BARCODE queue |
| `SCAN FAIL ... token` | `SCAN_AGENT_TOKEN` mismatch — fix `C:\TaxOps\ScanAgent\token.env` and TaxOps `.env` |
| `SCAN FAIL Unable to connect` | Task not running / user not logged on — log in, `Start-ScheduledTask` or `GO_SCAN_AGENT.bat` |
| `SCAN FAIL code_rev=...` | Old agent process — kill :8766 listeners, re-run task / `GO_SCAN_AGENT.bat` |

---

## Install (IT) — once per machine

```powershell
# Elevated on RECEPTION
cd \\Xcel-server\taxops
powershell -ExecutionPolicy Bypass -File .\taxops\scripts\install_print_relay_service.ps1
powershell -ExecutionPolicy Bypass -File .\taxops\scripts\install_scan_agent_task.ps1
```

Python must be 3.14 at  
`"C:\Users\Windows 10\AppData\Local\Programs\Python\Python314\python.exe"`.

### Autologon + screen lock
- Without autologon, **scanning is unavailable until someone logs in** (by design).
- If enabling autologon for unattended reboot: pair with **screen lock on idle** (client-facing area). Reception holds no PII locally beyond what Windows already has; still lock the screen.
- Autologon is an office policy decision — this repo does not force it on.

---

## Token rotation

### `FILETRACK_RELAY_TOKEN`
1. Generate a new secret; set on **TaxOps server** `.env` as `FILETRACK_RELAY_TOKEN=...` and restart TaxOpsService.
2. On reception: update `C:\TaxOps\PrintRelay\relay.env` and re-run `install_print_relay_service.ps1` (or `nssm set FiletrackRelay AppEnvironmentExtra ...`).
3. `Restart-Service FiletrackRelay`.

### `SCAN_AGENT_TOKEN`
1. Update TaxOps server `.env` `SCAN_AGENT_TOKEN`; restart TaxOpsService.
2. Update `C:\TaxOps\ScanAgent\token.env` on reception.
3. Restart scan task / `GO_SCAN_AGENT.bat`.

Never commit tokens. Never put them in `.bat` files.

---

## Logs

| Agent | Log |
|-------|-----|
| Print (NSSM) | `C:\TaxOps\logs\print_relay.log` (rotated ~10 MB) |
| Scan (task wrapper) | `C:\TaxOps\logs\scan_agent_YYYYMMDD.log` |
| Full diagnostic | `C:\TaxOps\diagnostics\reception_relays_LATEST.txt` |

---

## Rollback (&lt; 5 minutes)

```powershell
# Elevated
powershell -ExecutionPolicy Bypass -File \\Xcel-server\taxops\taxops\scripts\uninstall_print_relay_service.ps1
powershell -ExecutionPolicy Bypass -File \\Xcel-server\taxops\taxops\scripts\uninstall_scan_agent_task.ps1
```

Then manual start (previous behavior):
- `T:\start_print_relay.bat`
- `T:\GO_SCAN_AGENT.bat`

Those bats remain supported.

---

## Admin UI

TaxOps **Tools → Reception Agents** (admin only). Green/red dots poll every 60s with a 3s timeout and do not block page render.
