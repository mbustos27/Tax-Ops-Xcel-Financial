$host.UI.RawUI.WindowTitle = "TaxOps Dev Server"
$port = 5000
$appDir = Join-Path $PSScriptRoot "taxops"

Write-Host ""
Write-Host "  TaxOps Dev Server" -ForegroundColor Cyan
Write-Host "  ──────────────────────────────────" -ForegroundColor DarkGray

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
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$appDir'; python app.py"

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
