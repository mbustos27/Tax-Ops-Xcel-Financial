#Requires -Version 5.1
<#
.SYNOPSIS
  Install / start FiletrackRelay (label print) on the print-station PC.

.DESCRIPTION
  Same NSSM pattern as setup_scan_agent.ps1. Run ON the workstation that has
  the 4BARCODE printer (Admin / UAC Yes). See taxops/filetrack/DEPLOYMENT.md §4.

  Does NOT touch TaxOpsService on the server.

.EXAMPLE
  \\Xcel-server\taxops\install_print_relay_nssm.ps1
#>
param(
    [string]$UncRoot = "\\Xcel-server\taxops",
    [string]$Token = $env:FILETRACK_RELAY_TOKEN,
    [string]$Printer = $env:FILETRACK_PRINTER,
    [string]$HostBind = "0.0.0.0",
    [int]$Port = 8765,
    [string]$ServiceName = "FiletrackRelay",
    [switch]$NoNssm
)

$ErrorActionPreference = "Continue"

function Test-IsAdmin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

$thisScript = $MyInvocation.MyCommand.Path
if (-not $thisScript) { $thisScript = Join-Path $UncRoot "install_print_relay_nssm.ps1" }
$root = Split-Path -Parent $thisScript
if (-not (Test-Path $root)) { $root = $UncRoot }
$appDir = Join-Path $root "taxops"

if (-not (Test-IsAdmin)) {
    Write-Host "Need Administrator - relaunching (click Yes on UAC)..." -ForegroundColor Yellow
    $arg = @(
        "-NoExit", "-NoProfile", "-ExecutionPolicy", "Bypass",
        "-File", $thisScript,
        "-UncRoot", $UncRoot, "-Port", "$Port", "-HostBind", $HostBind, "-ServiceName", $ServiceName
    )
    if ($Token) { $arg += @("-Token", $Token) }
    if ($Printer) { $arg += @("-Printer", $Printer) }
    if ($NoNssm) { $arg += "-NoNssm" }
    Start-Process powershell.exe -Verb RunAs -WorkingDirectory $root -ArgumentList $arg
    exit 0
}

Write-Host ""
Write-Host "========================================================" -ForegroundColor DarkCyan
Write-Host "  FiletrackRelay setup (print station)" -ForegroundColor White
Write-Host "========================================================" -ForegroundColor DarkCyan

if (-not $Token) {
    Write-Host "  FILETRACK_RELAY_TOKEN is empty." -ForegroundColor Yellow
    $Token = Read-Host "  Enter shared token (same as TaxOps FILETRACK_RELAY_TOKEN)"
}
if (-not $Printer) {
    $Printer = Read-Host "  Printer name (default: 4BARCODE 4B-2054A)"
    if (-not $Printer) { $Printer = "4BARCODE 4B-2054A" }
}
if (-not $Token) {
    Write-Host "  [FAIL] Token required." -ForegroundColor Red
    Read-Host "Press Enter to close"
    exit 2
}

$py = $null
foreach ($c in @(
    (Join-Path $appDir ".venv\Scripts\python.exe"),
    "C:\TaxOps\taxops\.venv\Scripts\python.exe",
    (Get-Command python.exe -EA SilentlyContinue | Select-Object -ExpandProperty Source)
)) {
    if ($c -and (Test-Path $c) -and ($c -notmatch "WindowsApps")) { $py = $c; break }
}
if (-not $py) {
    Write-Host "  [FAIL] python.exe not found." -ForegroundColor Red
    Read-Host "Press Enter to close"
    exit 3
}
Write-Host "  [OK] Python: $py" -ForegroundColor Green

Push-Location $appDir
& $py -m pip install -q flask "pywin32>=306" 2>&1 | Out-Null
Pop-Location

$nssm = Get-Command nssm.exe -EA SilentlyContinue | Select-Object -ExpandProperty Source
if (-not $nssm) {
    foreach ($p in @("C:\Tools\nssm-2.24\win64\nssm.exe", "C:\nssm\nssm.exe")) {
        if (Test-Path $p) { $nssm = $p; break }
    }
}

$envExtra = @(
    "FILETRACK_RELAY_TOKEN=$Token",
    "FILETRACK_PRINTER=$Printer",
    "FILETRACK_RELAY_HOST=$HostBind",
    "FILETRACK_RELAY_PORT=$Port"
)

if ($NoNssm -or -not $nssm) {
    Write-Host "  Starting FiletrackRelay in this window (no NSSM)..." -ForegroundColor Cyan
    $env:FILETRACK_RELAY_TOKEN = $Token
    $env:FILETRACK_PRINTER = $Printer
    Set-Location $appDir
    & $py -m filetrack.relay.server --host $HostBind --port $Port
    exit $LASTEXITCODE
}

Write-Host "  NSSM: $nssm" -ForegroundColor Green
$existing = Get-Service $ServiceName -EA SilentlyContinue
if (-not $existing) {
    & $nssm install $ServiceName $py
    & $nssm set $ServiceName AppParameters "-m filetrack.relay.server --port $Port"
    & $nssm set $ServiceName AppDirectory $appDir
}
& $nssm set $ServiceName AppEnvironmentExtra $envExtra
$logDir = Join-Path $appDir "filetrack\relay\logs"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }
& $nssm set $ServiceName AppStdout (Join-Path $logDir "relay_stdout.log")
& $nssm set $ServiceName AppStderr (Join-Path $logDir "relay_stderr.log")
& $nssm set $ServiceName Start SERVICE_AUTO_START
& $nssm restart $ServiceName 2>$null
Start-Service $ServiceName -EA SilentlyContinue
Start-Sleep 2
$svc = Get-Service $ServiceName -EA SilentlyContinue
if ($svc -and $svc.Status -eq "Running") {
    Write-Host "  [OK] $ServiceName is RUNNING on port $Port" -ForegroundColor Green
} else {
    Write-Host "  [WARN] Service not running — check $logDir\relay_stderr.log" -ForegroundColor Yellow
}

$fwName = "TaxOps Filetrack Relay"
$rule = Get-NetFirewallRule -DisplayName $fwName -EA SilentlyContinue
if (-not $rule) {
    New-NetFirewallRule -DisplayName $fwName -Direction Inbound -Protocol TCP -LocalPort $Port -Action Allow -Profile Any | Out-Null
    Write-Host "  [OK] Firewall rule added for TCP $Port" -ForegroundColor Green
}

Write-Host ""
Write-Host "  On TaxOps server set FILETRACK_RELAY_URL=http://<this-pc-ip>:$Port/print" -ForegroundColor Gray
Write-Host "  and FILETRACK_RELAY_TOKEN=<same token>, then restart TaxOpsService." -ForegroundColor Gray
Write-Host ""
Read-Host "Press Enter to close"
