# Reception agents — baseline (pre-hardening)

Recorded for branch `reception-agents-hardening` from share `\\Xcel-server\taxops` (mapped `T:`).
Machine of record: **RECEPTION** `192.168.1.9` (see hardening prompt). This doc describes **current** behavior before NSSM / Scheduled Task work.

## Inventory — real paths

| Role | Path |
|------|------|
| Full diagnostic (produced `reception_relays_LATEST.txt`) | `T:\diagnose_reception_relays.ps1` (+ `T:\diagnose_reception_relays.bat`) |
| Diagnostic output folder | `C:\TaxOps\diagnostics\` (`reception_relays_*.txt`, `reception_relays_LATEST.txt`) |
| Print relay source | `T:\taxops\filetrack\relay\server.py` |
| Scan agent source | `T:\taxops\scan_agent\server.py` (`REQUIRED_CODE_REV = "com_sta_v4"`) |
| Local scan copy (runtime) | `C:\TaxOps\ScanAgent\app\scan_agent\server.py` |
| `start_print_relay.bat` | `T:\start_print_relay.bat` |
| `start_scan_agent.bat` | `T:\start_scan_agent.bat` |
| `GO_SCAN_AGENT.bat` | `T:\GO_SCAN_AGENT.bat` |
| `sync_scan_agent_local.ps1` | `T:\sync_scan_agent_local.ps1` |
| Existing NSSM helper (print) | `T:\install_print_relay_nssm.ps1` |
| Reception setup umbrella | `T:\setup_reception_pc.ps1` / `.bat` |
| Scan wizard | `T:\scan_agent_wizard.ps1` / `.bat` |
| Python on RECEPTION | `C:\Users\Windows 10\AppData\Local\Programs\Python\Python314\python.exe` |

## Print relay (TCP 8765) — current start mechanism

- **Primary today:** interactive console via `start_print_relay.bat`.
  - `cd` into `T:\taxops`, runs `python -m filetrack.relay.server --port 8765`.
  - Window must stay open; closing it stops the relay.
- **Optional:** `install_print_relay_nssm.ps1` can install service name `FiletrackRelay`, but reception was not reliably using that path; diagnostic often saw no service or a stopped one while a bat window served traffic.
- **Login auto-start (setup):** `setup_reception_pc.ps1` may create a Startup-folder shortcut to `start_print_relay.bat` (user-session only — dies on logoff).

### Env vars today

| Var | How supplied |
|-----|----------------|
| `FILETRACK_PRINTER` | Hardcoded in `start_print_relay.bat` to `4BARCODE 4B-2054A` (overridable if already set) |
| `FILETRACK_RELAY_TOKEN` | Process env if set; else bat default (legacy); TaxOps server reads matching token from `taxops/.env` |
| `FILETRACK_RELAY_HOST` / `PORT` | Set in bat to `0.0.0.0` / `8765` |

Session-0 services do **not** inherit the interactive user’s environment — any future service install must use NSSM `AppEnvironmentExtra` or an explicit `.env` read (hardening M2).

### WSGI / HTTP server today

- Flask **`app.run()`** development server (`filetrack/relay/server.py` `main()`).
- No waitress / production WSGI yet. Flask’s own banner warns against this in production.

### Logs today

- Console stdout/stderr of the bat window only.
- Diagnostic may redirect a one-shot start to `C:\TaxOps\diagnostics\print_relay_stdout_*.txt` / `stderr_*.txt`.
- No rotated service log under `C:\TaxOps\logs\` yet.

### Health

- `GET http://127.0.0.1:8765/health` → `{ status, printer_configured, printer_found, ... }` (no token required on health).

## Scan agent (TCP 8766) — current start mechanism

- **Primary today:** interactive console via `start_scan_agent.bat` / `GO_SCAN_AGENT.bat`.
  - Syncs share → `C:\TaxOps\ScanAgent\app\scan_agent` (robocopy; trailing-`\` UNC quote bug fixed).
  - Sets `PYTHONPATH=C:\TaxOps\ScanAgent\app`, runs `python -m scan_agent.server`.
  - Visible window; staff can close it and break scanning.
- **Why not a Windows service:** WIA device enumeration needs an **interactive desktop**. A stopped `ScanAgent` NSSM service can coexist with a working bat — session-0 will not fix that.
- Code rev marker: `com_sta_v4` dedicated STA pump in `scan_agent/server.py` (do not rewrite in hardening).

### Env vars today

| Var | How supplied |
|-----|----------------|
| `SCAN_AGENT_TOKEN` | `C:\TaxOps\ScanAgent\token.env` and/or `T:\taxops\.env`; required by agent |
| `SCAN_AGENT_HOST` / `PORT` | Bat defaults `0.0.0.0` / `8766` |
| TaxOps server | `SCAN_AGENT_URL=http://192.168.1.9:8766`, `SCAN_AGENT_TOKEN` in server `.env` |

### WSGI today

- Werkzeug `make_server(..., threaded=True)` then `serve_forever()` (not Flask `app.run`). Still a development-style server, acceptable for local WIA agent.

### Logs today

- Console of the Scan Agent window.
- Wizard/minimized path may use `C:\TaxOps\ScanAgent\logs\`.
- No `C:\TaxOps\logs\scan_agent.log` rotation yet.

### Health

- `GET /health` requires `X-Scan-Agent-Token`.
- Fast path returns `code_rev`, `com_sta`, `agent_ok` without WIA.
- `?wia=1` probes scanners (timed); `com_detail` may contain the breadcrumb `CoInitializeEx hr=None` on **success**.

## Diagnostic false positives (known before M1)

1. Substring match on `CoInitialize` matches success breadcrumb `com_detail`.
2. Initial `/health` miss counted as `[FAIL]` even when the script then starts the agent successfully.
3. Python version probe quoting can emit `File "<string>", line 1`.
4. Firewall section blank when not elevated (looks like “no rules”).
5. Hung-port kill logged as routine `[..]` instead of `[WARN]` with process metadata.

## Intended target architecture (hardening — not yet implemented)

| Agent | Mechanism | Reason |
|-------|-----------|--------|
| Print 8765 | NSSM service `FiletrackRelay`, waitress, auto-start | `win32print` OK in session 0; survives logoff |
| Scan 8766 | Scheduled Task at interactive logon + watchdog | WIA requires interactive session — **must not** be a session-0 service |

Existing `.bat` files must keep working until runbook rollback is accepted.
