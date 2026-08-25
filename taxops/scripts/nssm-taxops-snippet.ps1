# Reference commands for NSSM + TaxOps (Windows). Edit paths and service name before use.
#
# Prerequisites:
#   - nssm on PATH ("nssm.exe") or replace with full path, e.g.:
#       $NSSM = 'C:\Users\Administrator\AppData\Local\Microsoft\WinGet\Links\nssm.exe'
#   - Python + taxops deployed (example uses C:\TaxOps\taxops)
#
# AppEnvironmentExtra: first assignment is KEY=value. Additional vars use "+" prefix per NSSM CLI.
# If you pasted a typo like ":OLLAMA_BASE_URL", TaxOps maps it — but fixing NSSM is still best practice.
#
# NEVER commit real passwords; put secrets only in NSSM GUI or OS env — not this file.

$ErrorActionPreference = 'Stop'

$NSSM       = 'nssm.exe'              # <- or full path
$Service    = 'TaxOpsService'
$Py         = 'C:\TaxOps\taxops\.venv\Scripts\python.exe'   # <- prefer venv over global Python313
$AppDir     = 'C:\TaxOps\taxops'
$Logs       = 'C:\TaxOps\logs'

if (-not (Test-Path $AppDir)) { throw "App directory missing: $AppDir" }

New-Item -ItemType Directory -Force -Path $Logs | Out-Null

Write-Host "[example] Installing service first time — skip if TaxOps already registered."
Write-Host "  `"& '$NSSM' install $Service `'$Py`' '$AppDir\app.py'`""
Write-Host ""

# ── WSGI / Waitress (GitHub Epic #136) ────────────────────────────────────────
# app.py launches Waitress (multi-threaded) when FLASK_DEBUG is unset or not 1.
# Do NOT set FLASK_DEBUG=1 on NSSM/production — it restores the werkzeug development server.
# Optional tuning (defaults shown):
#     nssm set $Service AppEnvironmentExtra +WAITRESS_THREADS=8
#     nssm set $Service AppEnvironmentExtra +WAITRESS_HOST=0.0.0.0
#     nssm set $Service AppEnvironmentExtra +WAITRESS_PORT=5000
# Concurrent smoke (WSGI #139): python scripts/smoke_waitress_concurrency.py http://127.0.0.1:5000

# CACHE / static assets (GitHub Epic #141 — #145):
# Templates use ?v=<APP_VERSION> on app.js / app.css / tw.min.css. Bump one of:
#   nssm set $Service AppEnvironmentExtra +TAXOPS_VERSION=25.2.1
#   nssm set $Service AppEnvironmentExtra +TAXOPS_APP_VERSION=20260207a
# (+ prefix for additional AppEnvironmentExtra lines). Then Restart-Service TaxOpsService.

Write-Host "Core paths:"
& $NSSM set $Service Application $Py
& $NSSM set $Service AppDirectory $AppDir
& $NSSM set $Service AppParameters "app.py"

Write-Host "Log files:"
& $NSSM set $Service AppStdout "$Logs\taxops_stdout.log"
& $NSSM set $Service AppStderr "$Logs\taxops_stderr.log"
& $NSSM set $Service AppStdoutCreationDisposition 4
& $NSSM set $Service AppStderrCreationDisposition 4

# ── PROD-1 (NSSM Windows): restart on crash + start at boot ───────────────────
# GitHub Tax-Ops-Xcel-Financial #89. Linux deployments use systemd with Restart=always
# instead; this snippet targets the office NSSM deployment.
Write-Host "PROD-1 — NSSM: AppExit Default Restart, staggered restart delay, Automatic start:"
& $NSSM set $Service AppExit Default Restart
& $NSSM set $Service AppRestartDelay 5000
& $NSSM set $Service Start SERVICE_AUTO_START

Write-Host 'Base environment — first KEY=value MUST NOT start with ":"'
Write-Host '  To ONLY fix OLLAMA_BASE_URL without retyping other NSSM vars, use:' -ForegroundColor Cyan
Write-Host "    .\scripts\nssm-set-ollama-url.ps1 -Service $Service -OllamaUrl http://192.168.1.173:11434" -ForegroundColor Gray
Write-Host ""
& $NSSM set $Service AppEnvironmentExtra "OLLAMA_BASE_URL=http://192.168.1.173:11434"

# ── PROD-2: structured rotating JSON logs (GitHub Tax-Ops-Xcel-Financial #90) ─
# Mirrors .env.example: path under $Logs matches stdout/stderr; console off avoids duplicate NSSM dumps.
Write-Host 'PROD-2 — optional NSSM extras (logging):'
foreach ($line in @(
        '+TAXOPS_LOG_LEVEL=INFO',
        "+TAXOPS_LOG_JSON_PATH=$Logs\taxops-app.jsonl",
        '+TAXOPS_LOG_JSON_MAX_MB=50',
        '+TAXOPS_LOG_JSON_BACKUPS=10',
        '+TAXOPS_LOG_CONSOLE=false',
        '+TAXOPS_VERSION=REPLACE_WITH_SEMVER_OR_GIT_LABEL'
    )) {
    Write-Host ("  nssm set $Service AppEnvironmentExtra $line") -ForegroundColor Gray
}

Write-Host ''
Write-Host 'PROD-4 — optional NSSM echoes (SQLite backups — GitHub #92):'
foreach ($line in @(
        '+TAXOPS_BACKUP_DIR=C:\TaxOps\backups\sqlite',
        '+TAXOPS_BACKUP_RETENTION_DAYS=30',
        '+TAXOPS_BACKUP_ALERT_WEBHOOK=https://hooks.slack.com/services/REPLACE/REPLACE/REPLACE'
    )) {
    Write-Host ("  nssm set $Service AppEnvironmentExtra $line") -ForegroundColor Gray
}

Write-Host ''
Write-Host 'PROD-5 — production secrets checklist (validated on Flask boot — GitHub #93):'
foreach ($line in @(
        '+TAXOPS_SECRET=PASTE_min_16_char_random_stable_across_service_restarts',
        '+TAXOPS_USER=PASTE_unique_staff_username',
        '+TAXOPS_PASS=PASTE_strong_unique_password_longer_than_9_chars_not_changeme'
    )) {
    Write-Host ("  nssm set $Service AppEnvironmentExtra $line") -ForegroundColor Gray
}

Write-Host 'Optional: put most settings in taxops\.env on the server; NSSM overrides when set.'
Write-Host 'Append more TaxOps vars (exact Ollama tags from your ollama list):'

$extras = @(
    '+OLLAMA_MODEL=llama3.2:latest',
    '+OLLAMA_ROUTER_MODEL=qwen2.5:7b-instruct-q4_K_M',
    '+OLLAMA_CHAT_MODEL=llama3.1:8b-instruct-q4_K_M',
    '+OLLAMA_CHAT_ROUTER_TIMEOUT=120',
    '+OLLAMA_CHAT_ANSWER_TIMEOUT=120',
    '+OLLAMA_EXTRACT_MODEL_TEXT=llama3.2:latest',
    '+OLLAMA_EXTRACT_MODEL_VISION=llama3.2-vision:latest',
    '+FLASK_DEBUG=0',
    '+PYTHONUNBUFFERED=1'
)

Write-Host '  (PROD-5 #93: production deployments need the NSSM secrets block above — not placeholders.)'

foreach ($line in $extras) {
    Write-Host ("  nssm set $Service AppEnvironmentExtra $line") -ForegroundColor Gray
}

Write-Host ""
Write-Host "After editing NSSM:"
Write-Host "  Restart-Service $Service"
Write-Host "Verify:"
Write-Host "  nssm dump $Service"
Write-Host "  Get-Content $Logs\taxops_stderr.log -Tail 50"
Write-Host '  .\scripts\smoke_deploy.ps1 -BaseUrl http://127.0.0.1:5000'
