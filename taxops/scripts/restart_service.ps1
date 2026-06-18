<#
.SYNOPSIS
    Stop, start, and smoke-test the TaxOpsService Windows service.

.DESCRIPTION
    Safe restart script for the production TaxOps service.
    1. Stops TaxOpsService and waits until STATE = STOPPED (up to 30s).
    2. Starts TaxOpsService and waits until STATE = RUNNING (up to 30s).
    3. Runs smoke_test.py against http://127.0.0.1:5000.
    4. Prints pass/fail. Smoke results also append to C:\TaxOps\logs\smoke.log.

.PARAMETER ServiceName
    Windows service name. Default: TaxOpsService

.PARAMETER BaseUrl
    URL passed to smoke_test.py. Default: http://127.0.0.1:5000

.PARAMETER SkipSmoke
    Skip the smoke test (useful for quick restarts during development).

.EXAMPLE
    .\scripts\restart_service.ps1
    .\scripts\restart_service.ps1 -SkipSmoke
    .\scripts\restart_service.ps1 -BaseUrl http://192.168.1.141:5000
#>
param(
    [string]$ServiceName = "TaxOpsService",
    [string]$BaseUrl     = "http://192.168.1.141:5000",
    [switch]$SkipSmoke
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Wait-ServiceState {
    param([string]$Name, [string]$TargetState, [int]$TimeoutSec = 30)
    $elapsed = 0
    while ($elapsed -lt $TimeoutSec) {
        $svc = sc.exe query $Name 2>$null | Select-String "STATE"
        if ($svc -match $TargetState) { return $true }
        Start-Sleep -Seconds 1
        $elapsed++
    }
    return $false
}

Write-Host ""
Write-Host "==> Stopping $ServiceName..." -ForegroundColor Cyan
sc.exe stop $ServiceName | Out-Null
if (-not (Wait-ServiceState -Name $ServiceName -TargetState "STOPPED")) {
    Write-Warning "Service did not reach STOPPED within 30s. Proceeding anyway."
}

Write-Host "==> Starting $ServiceName..." -ForegroundColor Cyan
sc.exe start $ServiceName | Out-Null
if (-not (Wait-ServiceState -Name $ServiceName -TargetState "RUNNING")) {
    Write-Error "Service failed to reach RUNNING state within 30s."
    exit 1
}
Write-Host "    Service is RUNNING." -ForegroundColor Green

if ($SkipSmoke) {
    Write-Host "    Smoke test skipped (-SkipSmoke)." -ForegroundColor Yellow
    exit 0
}

# Give Waitress a moment to finish binding before probing
Start-Sleep -Seconds 2

Write-Host ""
Write-Host "==> Running smoke tests against $BaseUrl ..." -ForegroundColor Cyan
$scriptDir = Split-Path -Parent $PSScriptRoot
Push-Location $scriptDir
try {
    python smoke_test.py $BaseUrl
    $rc = $LASTEXITCODE
} finally {
    Pop-Location
}

Write-Host ""
if ($rc -eq 0) {
    Write-Host "    All smoke checks passed." -ForegroundColor Green
} else {
    Write-Host "    SMOKE TEST FAILED. See C:\TaxOps\logs\smoke.log for details." -ForegroundColor Red
}

# HEALTH-4: print /health JSON so ops can see worker and DB status at a glance
Write-Host ""
Write-Host "==> Health check $BaseUrl/health ..." -ForegroundColor Cyan
try {
    $health = Invoke-RestMethod -Uri "$BaseUrl/health" -Method Get -TimeoutSec 10
    $workerLines = @()
    if ($health.workers) {
        foreach ($kv in $health.workers.PSObject.Properties) {
            $running = if ($kv.Value.running) { "running" } else { "STOPPED" }
            $workerLines += "       $($kv.Name): $running"
        }
    }
    $dbStatus  = if ($health.db.ok) { "ok ($($health.db.latency_ms) ms)" } else { "FAIL: $($health.db.error)" }
    $color     = if ($health.status -eq "ok") { "Green" } else { "Red" }
    Write-Host "    status:  $($health.status)"   -ForegroundColor $color
    Write-Host "    db:      $dbStatus"
    Write-Host "    uptime:  $($health.uptime_seconds)s"
    Write-Host "    version: $($health.version)"
    Write-Host "    schema:  $($health.schema_version) / $($health.schema_version_expected)"
    if ($workerLines) {
        Write-Host "    workers:"
        $workerLines | ForEach-Object { Write-Host $_ }
    }
} catch {
    Write-Host "    Could not reach /health: $_" -ForegroundColor Yellow
}

exit $rc
