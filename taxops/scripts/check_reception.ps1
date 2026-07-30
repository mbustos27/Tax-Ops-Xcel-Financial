#Requires -Version 5.1
<#
.SYNOPSIS
  Fast one-line-per-agent health check for reception (staff-friendly).

.DESCRIPTION
  Completes in a few seconds. Exit 0 = both OK, 1 = one or more down.
  Deep troubleshooting: diagnose_reception_relays.bat
#>
param(
    [int]$PrintPort = 8765,
    [int]$ScanPort = 8766,
    [int]$TimeoutSec = 3
)

$ErrorActionPreference = "Continue"
$fail = 0

function Get-DotEnv([string]$Path, [string]$Key) {
    if (-not (Test-Path -LiteralPath $Path)) { return "" }
    foreach ($line in Get-Content -LiteralPath $Path -EA SilentlyContinue) {
        if ($line -match ("^\s*{0}=(.+)$" -f [regex]::Escape($Key))) {
            return $Matches[1].Trim().Trim('"')
        }
    }
    return ""
}

# Print relay
try {
    $h = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/health" -f $PrintPort) -TimeoutSec $TimeoutSec
    if ($h.printer_found) {
        Write-Host ("PRINT  OK    printer={0}" -f $h.printer_configured) -ForegroundColor Green
    } else {
        Write-Host ("PRINT  WARN  up but printer_found=false configured={0}" -f $h.printer_configured) -ForegroundColor Yellow
        $fail++
    }
} catch {
    Write-Host ("PRINT  FAIL  {0}" -f $_.Exception.Message) -ForegroundColor Red
    $fail++
}

# Scan agent
$token = $env:SCAN_AGENT_TOKEN
if (-not $token) { $token = Get-DotEnv "C:\TaxOps\ScanAgent\token.env" "SCAN_AGENT_TOKEN" }
if (-not $token) { $token = Get-DotEnv "T:\taxops\.env" "SCAN_AGENT_TOKEN" }
if (-not $token) { $token = Get-DotEnv "\\Xcel-server\taxops\taxops\.env" "SCAN_AGENT_TOKEN" }

try {
    if (-not $token) { throw "SCAN_AGENT_TOKEN missing" }
    $s = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/health" -f $ScanPort) `
        -Headers @{ "X-Scan-Agent-Token" = $token } -TimeoutSec $TimeoutSec
    if ($s.code_rev -eq "com_sta_v4" -and $s.com_sta -eq $true) {
        Write-Host ("SCAN   OK    code_rev={0} com_sta={1}" -f $s.code_rev, $s.com_sta) -ForegroundColor Green
    } else {
        Write-Host ("SCAN   FAIL  code_rev={0} com_sta={1}" -f $s.code_rev, $s.com_sta) -ForegroundColor Red
        $fail++
    }
} catch {
    Write-Host ("SCAN   FAIL  {0}" -f $_.Exception.Message) -ForegroundColor Red
    $fail++
}

if ($fail -eq 0) { exit 0 }
exit 1
