#Requires -Version 5.1
<#
.SYNOPSIS
  Wrapper launched by Scheduled Task "TaxOps Scan Agent" (interactive logon).

.DESCRIPTION
  Syncs local copy, verifies com_sta_v4, clears stale :8766, starts agent minimized.
  IMPORTANT: Must run in an interactive user session — WIA does not work in session 0.
#>
param(
    [string]$ShareRoot = "\\Xcel-server\taxops",
    [int]$Port = 8766
)

$ErrorActionPreference = "Continue"
$ShareRoot = $ShareRoot.Trim().TrimEnd('\')
$logDir = "C:\TaxOps\logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$log = Join-Path $logDir ("scan_agent_{0:yyyyMMdd}.log" -f (Get-Date))

function Log([string]$Msg) {
    $line = "{0:yyyy-MM-dd HH:mm:ss} {1}" -f (Get-Date), $Msg
    Add-Content -LiteralPath $log -Value $line -Encoding UTF8
    Write-Host $line
}

Log "=== run_scan_agent start ==="
Log ("ShareRoot={0}" -f $ShareRoot)

# Rotate: keep ~14 dated logs
Get-ChildItem $logDir -Filter "scan_agent_*.log" -EA SilentlyContinue |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-14) } |
    Remove-Item -Force -EA SilentlyContinue

$sync = Join-Path $ShareRoot "sync_scan_agent_local.ps1"
if (Test-Path -LiteralPath $sync) {
    Log "Syncing local scan_agent copy..."
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $sync -ShareRoot $ShareRoot -LocalRoot "C:\TaxOps\ScanAgent" -Quiet
    if ($LASTEXITCODE -ge 4) {
        Log ("FAIL sync exit {0}" -f $LASTEXITCODE)
        exit 5
    }
} else {
    Log "WARN sync_scan_agent_local.ps1 missing - using existing local copy"
}

$localServer = "C:\TaxOps\ScanAgent\app\scan_agent\server.py"
if (-not (Test-Path -LiteralPath $localServer)) {
    Log "FAIL local server.py missing"
    exit 6
}
if (-not (Select-String -Path $localServer -Pattern "com_sta_v4" -SimpleMatch -Quiet)) {
    Log "FAIL local server.py missing com_sta_v4 - abort"
    exit 7
}
Log "Local com_sta_v4 OK"

# Clear stale listeners (1e-style detail)
try {
    $rows = @(Get-NetTCPConnection -LocalPort $Port -State Listen -EA SilentlyContinue)
} catch { $rows = @() }
foreach ($row in $rows) {
    $procId = [int]$row.OwningProcess
    if ($procId -le 4) { continue }
    $name = "?"; $path = ""; $started = $null
    try {
        $p = Get-Process -Id $procId -EA Stop
        $name = $p.ProcessName
        $started = $p.StartTime
        try { $path = $p.Path } catch {}
    } catch {}
    Log ("WARN stale listener on {0} killed PID={1} name={2} path={3} started={4}" -f $Port, $procId, $name, $path, $started)
    Stop-Process -Id $procId -Force -EA SilentlyContinue
}
Start-Sleep -Seconds 1

$py = $null
foreach ($c in @(
    "C:\Users\Windows 10\AppData\Local\Programs\Python\Python314\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python314\python.exe"
)) {
    if ($c -and (Test-Path -LiteralPath $c)) { $py = $c; break }
}
if (-not $py) {
    Log "FAIL Python 3.14 not found"
    exit 3
}

$token = $env:SCAN_AGENT_TOKEN
if (-not $token) {
    $tf = "C:\TaxOps\ScanAgent\token.env"
    if (Test-Path -LiteralPath $tf) {
        foreach ($line in Get-Content -LiteralPath $tf) {
            if ($line -match '^\s*SCAN_AGENT_TOKEN=(.+)$') { $token = $Matches[1].Trim().Trim('"'); break }
        }
    }
}
if (-not $token) {
    $shareEnv = Join-Path $ShareRoot "taxops\.env"
    if (Test-Path -LiteralPath $shareEnv) {
        foreach ($line in Get-Content -LiteralPath $shareEnv) {
            if ($line -match '^\s*SCAN_AGENT_TOKEN=(.+)$') { $token = $Matches[1].Trim().Trim('"'); break }
        }
    }
}
if (-not $token) {
    Log "FAIL SCAN_AGENT_TOKEN missing"
    exit 2
}

$env:SCAN_AGENT_TOKEN = $token
$env:SCAN_AGENT_HOST = "0.0.0.0"
$env:SCAN_AGENT_PORT = "$Port"
$env:PYTHONPATH = "C:\TaxOps\ScanAgent\app"

Log ("Starting minimized agent with PYTHONPATH={0}" -f $env:PYTHONPATH)
# Minimized - no console for staff to close accidentally. Task Scheduler restart covers crashes.
$proc = Start-Process -FilePath $py `
    -ArgumentList @("-m", "scan_agent.server", "--host", "0.0.0.0", "--port", "$Port") `
    -WorkingDirectory "C:\TaxOps\ScanAgent\app" `
    -WindowStyle Minimized `
    -PassThru

Log ("Started PID {0}" -f $proc.Id)

# Wait for health briefly then exit wrapper (agent keeps running)
$ok = $false
for ($i = 1; $i -le 30; $i++) {
    Start-Sleep -Seconds 1
    try {
        $r = Invoke-WebRequest -Uri ("http://127.0.0.1:{0}/health" -f $Port) `
            -Headers @{ "X-Scan-Agent-Token" = $token } -UseBasicParsing -TimeoutSec 2
        if ($r.Content -match "com_sta_v4") { $ok = $true; break }
    } catch {}
}
if ($ok) { Log "READY com_sta_v4"; exit 0 }
Log "WARN agent started but health not ready yet - task restart will retry if needed"
exit 0
