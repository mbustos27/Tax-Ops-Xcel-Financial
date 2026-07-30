#Requires -Version 5.1
<#
.SYNOPSIS
  Register interactive Scheduled Task "TaxOps Scan Agent" (logon trigger).

.DESCRIPTION
  WIA requires an interactive desktop. Therefore:
    - LogonType MUST be Interactive
    - "Run whether user is logged on or not" MUST stay OFF
      (that setting forces a non-interactive session and breaks WIA)
  Do NOT convert this to an NSSM / session-0 service.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File \\Xcel-server\taxops\taxops\scripts\install_scan_agent_task.ps1
#>
param(
    [string]$ShareRoot = "",
    [string]$TaskName = "TaxOps Scan Agent",
    [string]$UserId = "",
    [int]$Port = 8766
)

$ErrorActionPreference = "Stop"

function Test-IsAdmin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

$thisScript = $MyInvocation.MyCommand.Path
if (-not $ShareRoot) {
    $ShareRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $thisScript))
}
$ShareRoot = $ShareRoot.Trim().TrimEnd('\')
$wrapper = Join-Path $ShareRoot "taxops\scripts\run_scan_agent.ps1"
if (-not (Test-Path -LiteralPath $wrapper)) { throw "Missing $wrapper" }

if (-not $UserId) {
    $UserId = "{0}\{1}" -f $env:USERDOMAIN, $env:USERNAME
}

if (-not (Test-IsAdmin)) {
    Write-Host "Need Administrator - relaunching (UAC)..." -ForegroundColor Yellow
    Start-Process powershell.exe -Verb RunAs -ArgumentList @(
        "-NoProfile", "-ExecutionPolicy", "Bypass",
        "-File", "`"$thisScript`"",
        "-ShareRoot", "`"$ShareRoot`"",
        "-TaskName", "`"$TaskName`"",
        "-UserId", "`"$UserId`"",
        "-Port", "$Port"
    )
    exit 0
}

# Remove prior registration (idempotent)
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -EA SilentlyContinue

# CRITICAL: -LogonType Interactive. Do NOT use Password/"whether logged on or not"
# — that switches to a non-interactive session and WIA DeviceManager fails.
$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument ("-NoProfile -ExecutionPolicy Bypass -File `"{0}`" -ShareRoot `"{1}`" -Port {2}" -f $wrapper, $ShareRoot, $Port)

$trigger = New-ScheduledTaskTrigger -AtLogOn -User $UserId

$principal = New-ScheduledTaskPrincipal `
    -UserId $UserId `
    -LogonType Interactive `
    -RunLevel Limited

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Settings $settings `
    -Description "TaxOps WIA scan agent (interactive). Do not change to 'run whether logged on or not'." `
    -Force | Out-Null

# Firewall for scan port (LAN)
$fwName = "TaxOps Scan Agent 8766"
if (-not (Get-NetFirewallRule -DisplayName $fwName -EA SilentlyContinue)) {
    New-NetFirewallRule -DisplayName $fwName `
        -Direction Inbound -Protocol TCP -LocalPort $Port `
        -RemoteAddress 192.168.1.0/24 -Action Allow -Profile Any | Out-Null
}

Write-Host "[OK] Scheduled task registered: $TaskName" -ForegroundColor Green
Write-Host "  User: $UserId  (Interactive logon)"
Write-Host "  Wrapper: $wrapper"
Write-Host ""
Write-Host "Autologon (optional): without it, scanning is unavailable until someone logs in."
Write-Host "  Pair autologon with screen lock on idle - see docs/reception_agents_runbook.md"
Write-Host "Start now:  Start-ScheduledTask -TaskName `"$TaskName`""
