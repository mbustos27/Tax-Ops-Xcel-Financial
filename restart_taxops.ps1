$host.UI.RawUI.WindowTitle = "Tax Log Server"
$port = 5000
$appDir = Join-Path $PSScriptRoot "taxops"

Write-Host ""
Write-Host "  Tax Log Server" -ForegroundColor Cyan
Write-Host "  ──────────────────────────────────" -ForegroundColor DarkGray

# Pre-restart backup — keep last 7 daily copies in C:\TaxOps\backups\
$dbPath  = "C:\TaxOps\taxops\taxops.db"
$bakDir  = "C:\TaxOps\backups"
if (Test-Path $dbPath) {
    try {
        if (-not (Test-Path $bakDir)) { New-Item -ItemType Directory -Path $bakDir -Force | Out-Null }
        $stamp   = (Get-Date -Format "yyyyMMdd_HHmmss")
        $bakFile = Join-Path $bakDir "taxops_backup_$stamp.sqlite"
        Copy-Item $dbPath $bakFile -Force
        Write-Host "  Backup saved: $bakFile" -ForegroundColor DarkGray
        # Remove backups older than 7 days
        Get-ChildItem $bakDir -Filter "taxops_backup_*.sqlite" |
            Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-7) } |
            Remove-Item -Force -ErrorAction SilentlyContinue
        # Also keep taxops.db.bak in sync (legacy path)
        Copy-Item $dbPath (Join-Path $PSScriptRoot "taxops\taxops.db.bak") -Force -ErrorAction SilentlyContinue
    } catch {
        Write-Host "  Warning: pre-restart backup failed: $_" -ForegroundColor Yellow
    }
} else {
    Write-Host "  Warning: DB not found at $dbPath - skipping backup" -ForegroundColor Yellow
}

# Find and kill any process currently listening on port 5000
$listening = netstat -aon | Select-String ":$port\s.*LISTENING"
if ($listening) {
    $pid_ = ($listening -split '\s+')[-1]
    Write-Host "  Stopping existing process on port $port (PID $pid_)..." -ForegroundColor Yellow
    try { Stop-Process -Id $pid_ -Force -ErrorAction Stop }
    catch { Write-Host "  Could not stop PID ${pid_}: ${_}" -ForegroundColor Red }
    Start-Sleep -Seconds 1
} else {
    Write-Host "  No process found on port $port." -ForegroundColor DarkGray
}

# Start app (Waitress multi-thread worker when FLASK_DEBUG is not 1 — Epic WSGI / GitHub #136)
Write-Host "  Starting Tax Log..." -ForegroundColor Green
$env:OLLAMA_BASE_URL = "http://localhost:11434"
$env:FLASK_DEBUG = "0"
$env:IMAP_HOST = "imap.gmail.com"
$env:IMAP_PORT = "993"
$env:IMAP_USER = "info@xcelfinancial.com"
$env:IMAP_PASS = "REPLACE_WITH_NEW_APP_PASSWORD"
$env:IMAP_POLL_INTERVAL = "120"
$env:IMAP_FOLDER = "INBOX"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$appDir'; `$env:FLASK_DEBUG='0'; `$env:OLLAMA_BASE_URL='http://localhost:11434'; `$env:IMAP_HOST='imap.gmail.com'; `$env:IMAP_PORT='993'; `$env:IMAP_USER='info@xcelfinancial.com'; `$env:IMAP_PASS='REPLACE_WITH_NEW_APP_PASSWORD'; `$env:IMAP_POLL_INTERVAL='120'; `$env:IMAP_FOLDER='INBOX'; `$env:TAXOPS_DB='C:\TaxOps\taxops\taxops.db'; python app.py"

# Wait and confirm
Start-Sleep -Seconds 3
$check = netstat -aon | Select-String ":$port\s.*LISTENING"
if ($check) {
    $newpid = ($check -split '\s+')[-1]
    Write-Host ""
    Write-Host "  ✓ Tax Log is running at http://taxlog/  or  http://localhost:$port  (PID $newpid)" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "  ⚠ Tax Log may not have started — check the new window for errors." -ForegroundColor Red
}

Write-Host ""
Write-Host "  Press any key to close this window..." -ForegroundColor DarkGray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

