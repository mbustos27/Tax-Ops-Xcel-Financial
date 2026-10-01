$host.UI.RawUI.WindowTitle = "TaxOps Dev Server"
$port = 5000
$appDir = Join-Path $PSScriptRoot "taxops"
$checkScript = Join-Path $appDir "check_restart_safe.py"

Write-Host ""
Write-Host "  TaxOps Dev Server" -ForegroundColor Cyan
Write-Host "  ──────────────────────────────────" -ForegroundColor DarkGray

# Ask the live DB whether anyone is mid-save before we kill the process.
if (Test-Path $checkScript) {
    Write-Host "  Checking if restart is safe..." -ForegroundColor DarkGray
    & python $checkScript
    $checkCode = $LASTEXITCODE
    if ($checkCode -eq 1) {
        Write-Host ""
        Write-Host "  WAIT — active saves / busy DB. Not restarting." -ForegroundColor Red
        Write-Host "  Finish the current client, or open Tools → Restart check." -ForegroundColor Yellow
        Write-Host ""
        Write-Host "  Press any key to close..." -ForegroundColor DarkGray
        $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
        exit 1
    }
    if ($checkCode -eq 2) {
        Write-Host ""
        Write-Host "  CAUTION — people may still have TaxOps open (no recent saves)." -ForegroundColor Yellow
        Write-Host "  Announce a quick restart, then press Y to continue (any other key aborts)." -ForegroundColor Yellow
        $key = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
        if ($key.Character -ne 'y' -and $key.Character -ne 'Y') {
            Write-Host "  Aborted." -ForegroundColor DarkGray
            exit 2
        }
    } elseif ($checkCode -ne 0) {
        Write-Host "  Restart check failed (exit $checkCode). Continuing with caution..." -ForegroundColor Yellow
    } else {
        Write-Host "  SAFE — no active staff or recent writes." -ForegroundColor Green
    }
    Write-Host ""
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

# Start Flask
Write-Host "  Starting TaxOps..." -ForegroundColor Green
$env:OLLAMA_BASE_URL = "http://192.168.1.141:11434"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$appDir'; `$env:OLLAMA_BASE_URL='http://192.168.1.141:11434'; python app.py"

# Wait and confirm
Start-Sleep -Seconds 3
$check = netstat -aon | Select-String ":$port\s.*LISTENING"
if ($check) {
    $newpid = ($check -split '\s+')[-1]
    Write-Host ""
    Write-Host "  ✓ TaxOps is running at http://localhost:$port  (PID $newpid)" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "  ⚠ TaxOps may not have started — check the new window for errors." -ForegroundColor Red
}

Write-Host ""
Write-Host "  Press any key to close this window..." -ForegroundColor DarkGray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
