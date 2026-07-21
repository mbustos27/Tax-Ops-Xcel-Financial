# filetrack — Deployment (M3)

Deploys the M2 scan listener (`filetrack.listener.run_listener`) as a
Windows service via [NSSM](https://nssm.cc/), and wires it to the M3
TaxOps endpoint. Read `filetrack/hardware_validation/README.md` first —
M0 has NOT been physically run in this repo; confirm scanner mode/suffix
before relying on any of this in production.

## 1. Prerequisites

On the scan-station PC (does **not** need to be the TaxOps server itself —
just needs network access to it):

```powershell
# From the taxops/ repo root, on the scan-station machine:
pip install -r filetrack\requirements.txt
```

This installs `pywin32` (label printing only — skip if this machine never
prints, only scans), `pyserial` (only if using `--mode serial`), and
`requests` (required for `--sink http`, i.e. talking to TaxOps).

Download NSSM ([https://nssm.cc/download](https://nssm.cc/download)) and put `nssm.exe` somewhere on
`PATH`, e.g. `C:\nssm\nssm.exe`.

## 2. Configure TaxOps (server side)

On the TaxOps server, set these environment variables before starting
`app.py` (e.g. in the same place `IMAP_HOST`/`DB_PATH` etc. are already
configured — TaxOps's `.env` or service environment):


| Variable            | Required                                | Purpose                                                                                                                                                                                                                                                                                                                                                      |
| ------------------- | --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `FILETRACK_ENABLED` | yes — set `true` to turn the feature on | Kill-switch for `POST /filetrack/status` (404s when false) and the intake print hook                                                                                                                                                                                                                                                                         |
| `FILETRACK_TOKEN`   | strongly recommended                    | Shared secret the listener sends as `X-Filetrack-Token`. If unset, the endpoint only accepts requests from `127.0.0.1`/`::1` — fine for same-box testing, **not** for a listener running on a separate scan-station PC                                                                                                                                       |
| `FILETRACK_PRINTER` | **required** to actually print          | Windows printer name for the label print hook (`filetrack.labels.print_label`). There is no OS-default-printer fallback — `filetrack/labels/printer.py`'s `send_zpl()` raises `PrinterNotFoundError` when this is unset, which the intake hook logs and swallows, so leaving it blank means every intake silently fails to print (no error visible to staff) |


Restart the TaxOps app process after setting these (they're read once at
import time via `filetrack.config`).

Verify: `curl -X POST http://<taxops-host>:5000/filetrack/status` (no
token) should return `404` if `FILETRACK_ENABLED` is still false, or `401`
once enabled without a valid token from a non-localhost caller.

## 2a. Print relay mode — when the printer is NOT on the TaxOps server

**Discovered in production the first time `FILETRACK_ENABLED=true` actually
ran against a real intake:** the intake print hook runs inside whatever
process is `app.py` — on a production deployment that's `TaxOpsService`, a
Windows **service** running in Session 0. `win32print.OpenPrinter()` from
that process can only see printers that are truly installed as local queues
*on that same machine*. It CANNOT see:

- A printer only visible because someone is (or was) logged into that
machine over Remote Desktop — RDP's Easy Print redirects a *client's*
local printer into that one interactive session only, showing up as
`"<name> (redirected N)"` in `win32print.EnumPrinters()` — never as a
real queue the service can open. This is exactly what a manual CLI test
run over RDP will falsely appear to confirm as "working."
- A printer physically attached to a different workstation, shared over
the network — technically possible, but service-account SMB auth to a
printer share on a workgroup (non-domain) network is fragile and easy to
get subtly wrong.

If the printer is attached to a different machine than the one running
`app.py`/`TaxOpsService` (e.g. a front-desk workstation, server in a back
room), use **relay mode** instead of fighting Windows printer sharing:

```
                 FILETRACK_ENABLED=true                    printer attached
                 FILETRACK_PRINT_MODE=relay                here — runs the
                 FILETRACK_RELAY_URL=http://<workstation>:8765/print   relay
  TaxOps server  FILETRACK_RELAY_TOKEN=<shared secret>  ─────────────▶  process
  (app.py /                    HTTP POST /print                    (front-desk
   TaxOpsService)               {"log_number": "..."}                workstation)
```

**On the workstation with the physical printer** — install filetrack's deps
(same as step 1, `pywin32` is required here) and confirm the printer prints
locally first (`python -m filetrack.labels.print_label --log 1`, see step 5
below), then run the relay:

```powershell
cd T:\taxops
$env:FILETRACK_PRINTER = "4BARCODE 4B-2054A"       # exact local queue name on THIS machine
$env:FILETRACK_RELAY_TOKEN = "<shared secret, same value as the server below>"
python -m filetrack.relay.server --port 8765
```

Requires `flask` in addition to `pywin32` on this machine — both are in
`filetrack/requirements.txt`.

**On the TaxOps server**, set (in `taxops/.env` or the service environment):


| Variable                | Purpose                                                                                                                  |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `FILETRACK_PRINT_MODE`  | `relay` (instead of the default `local`, which calls `win32print` directly and requires the printer on the same machine) |
| `FILETRACK_RELAY_URL`   | `http://<workstation-ip>:8765/print`                                                                                     |
| `FILETRACK_RELAY_TOKEN` | must match the relay's `FILETRACK_RELAY_TOKEN` exactly                                                                   |


`FILETRACK_PRINTER` is **not** used by the server in relay mode — the
relay reads its own `FILETRACK_PRINTER` locally. Restart `TaxOpsService`
after changing these.

Verify end-to-end: `curl http://<workstation-ip>:8765/health` should return
`{"status": "ok"}` from the server (network reachability check), then do
one real intake and confirm a label prints. If it doesn't, `T:\logs\taxops_stderr.log`
on the server will show either a `RelayError` (relay unreachable/rejected —
check the relay is running and the token matches) or nothing at all if the
POST never happened (check `FILETRACK_PRINT_MODE` actually reads back as
`relay`, e.g. via a temporary `/health`-style check, since it's read once at
import time).

Keep the relay running long-term the same way as the listener (step 4
below) — as an NSSM service on the workstation:

```powershell
nssm install FiletrackRelay "C:\path\to\python.exe"
nssm set FiletrackRelay AppParameters "-m filetrack.relay.server --port 8765"
nssm set FiletrackRelay AppDirectory "T:\taxops"
nssm set FiletrackRelay AppEnvironmentExtra "FILETRACK_PRINTER=4BARCODE 4B-2054A" "FILETRACK_RELAY_TOKEN=<shared secret>"
nssm set FiletrackRelay AppStdout "T:\taxops\filetrack\relay\logs\relay_stdout.log"
nssm set FiletrackRelay AppStderr "T:\taxops\filetrack\relay\logs\relay_stderr.log"
nssm start FiletrackRelay
```

Rollback: set `FILETRACK_PRINT_MODE=local` (or `FILETRACK_ENABLED=false`)
on the server and restart — the relay itself printing nothing is otherwise
harmless and can be left running.

## 3. Configure the listener (scan-station side)

Set these environment variables on the scan-station PC (or pass the
equivalent `--http-url`/`--http-token` CLI flags):


| Variable                                          | Purpose                                                                           |
| ------------------------------------------------- | --------------------------------------------------------------------------------- |
| `FILETRACK_ENDPOINT_URL`                          | e.g. `http://<taxops-host>:5000/filetrack/status`                                 |
| `FILETRACK_TOKEN`                                 | must match the server's `FILETRACK_TOKEN` exactly                                 |
| `FILETRACK_SCAN_MODE`                             | `hid` (default) or `serial` — see hardware_validation/FINDINGS.md once M0 has run |
| `FILETRACK_SCAN_SUFFIX`                           | the scanner's confirmed terminator (default `"\n"`, PENDING M0)                   |
| `FILETRACK_SERIAL_PORT` / `FILETRACK_SERIAL_BAUD` | `--mode serial` only                                                              |


Smoke-test manually first, in a normal console (NOT yet as a service), so
you can see scans land in real time:

```powershell
cd T:\taxops
python -m filetrack.listener.run_listener --sink http --log-level INFO
```

Scan a STATUS label, then a LOG label, and confirm:

1. The listener's console shows `scan kind=status ...` then
  `assignment log=... status=...` then `filetrack http_sink: delivered ...`.
2. `GET /return/<id>` (or the DB directly) on the TaxOps side shows
  `filetrack_status` updated, and a new row in `filetrack_status_history`.

If `--mode hid` (default), this console window must have OS input focus for
the scanner's keyboard-wedge input to reach it — plan the station's physical
setup (and Windows auto-logon / no screen lock) around that. `--mode serial`
avoids the focus requirement but needs `FILETRACK_SERIAL_PORT` confirmed
against Device Manager.

Stop the manual test (Ctrl+C) once confirmed before installing as a service.

## 4. Install as an NSSM service

```powershell
nssm install FiletrackListener
```

In the NSSM GUI (or via `nssm set` non-interactively — see below):

- **Path**: full path to `python.exe` inside the venv/interpreter that has
`filetrack/requirements.txt` installed.
- **Startup directory**: `T:\taxops` (so `python -m filetrack.listener.run_listener`
resolves the `filetrack` package correctly).
- **Arguments**: `-m filetrack.listener.run_listener --sink http --log-level INFO`
(add `--mode serial --port COM3` etc. as confirmed in step 3).
- **Environment tab / `nssm set FiletrackListener AppEnvironmentExtra`**: set
`FILETRACK_ENDPOINT_URL`, `FILETRACK_TOKEN`, and any scan-mode overrides
from step 3 — NSSM services don't inherit a logged-in user's shell env.

Non-interactive equivalent:

```powershell
nssm install FiletrackListener "C:\path\to\python.exe"
nssm set FiletrackListener AppParameters "-m filetrack.listener.run_listener --sink http --log-level INFO"
nssm set FiletrackListener AppDirectory "T:\taxops"
nssm set FiletrackListener AppEnvironmentExtra "FILETRACK_ENDPOINT_URL=http://<taxops-host>:5000/filetrack/status" "FILETRACK_TOKEN=<same secret as server>"
nssm set FiletrackListener AppStdout "T:\taxops\filetrack\listener\logs\listener_stdout.log"
nssm set FiletrackListener AppStderr "T:\taxops\filetrack\listener\logs\listener_stderr.log"
nssm set FiletrackListener AppRotateFiles 1
nssm set FiletrackListener AppRotateBytes 5242880
nssm start FiletrackListener
```

Notes:

- `**--mode hid` + NSSM is almost certainly wrong.** A Windows service has
no desktop/input focus, so keyboard-wedge scans have nowhere to land. If
the ScanAvenger only supports HID mode, run the listener as a normal
console app under a station-specific auto-login user instead of an NSSM
service (Startup folder shortcut, not `nssm install`). NSSM is the right
tool once M0 confirms `--mode serial` works — a service can read a COM
port with no desktop session.
- Create `filetrack\listener\logs\` before starting the service (NSSM does
not create the directory for you).
- `nssm restart FiletrackListener` after any config/env change.

## 5. Printer setup (label printing, if this box also prints)

The Arkscan 2054A must already be installed as a normal Windows printer
(RAW/passthrough driver, not a generic-text driver that would mangle ZPL).
Confirm with:

```powershell
python -m filetrack.labels.print_label --log 1 --dry-run   # sanity: renders fine with no printer
python -m filetrack.hardware_validation.print_test --list-printers
python -m filetrack.hardware_validation.print_test --printer "Arkscan 2054A"
```

Set `FILETRACK_PRINTER` to the exact name shown by `--list-printers` if it
isn't already the Windows default printer.

## 6. Rollback

Set `FILETRACK_ENABLED=false` on the TaxOps server and restart the app —
this immediately 404s the endpoint and disables the intake print hook with
no code changes. `nssm stop FiletrackListener` (or just unplug the scanner)
stops the listener side independently; TaxOps itself is unaffected either
way since printing/scanning are both best-effort, never a hard dependency
of intake or any other core workflow.