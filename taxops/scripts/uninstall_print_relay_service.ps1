#Requires -Version 5.1
<#
.SYNOPSIS
  Stop and remove NSSM service FiletrackRelay. Leaves .bat launchers intact.
#>
param(
    [string]$ServiceName = "FiletrackRelay",
    [string]$NssmExe = "",
    [switch]$RemoveFirewall,
    [switch]$RemoveEnvFile
)

$ErrorActionPreference = "Continue"

function Test-IsAdmin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Find-Nssm([string]$Hint) {
    if ($Hint -and (Test-Path -LiteralPath $Hint)) { return $Hint }
    $cmd = Get-Command nssm.exe -EA SilentlyContinue
    if ($cmd) { return $cmd.Source }
    foreach ($c in @("C:\TaxOps\nssm\nssm.exe", "C:\Tools\nssm\nssm.exe")) {
        if (Test-Path -LiteralPath $c) { return $c }
    }
    return $null
}

if (-not (Test-IsAdmin)) {
    $thisScript = $MyInvocation.MyCommand.Path
    Start-Process powershell.exe -Verb RunAs -ArgumentList @(
        "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "`"$thisScript`"",
        "-ServiceName", $ServiceName
    )
    exit 0
}

$svc = Get-Service -Name $ServiceName -EA SilentlyContinue
if ($svc) {
    Stop-Service -Name $ServiceName -Force -EA SilentlyContinue
    $nssm = Find-Nssm $NssmExe
    if ($nssm) {
        & $nssm remove $ServiceName confirm
    } else {
        sc.exe delete $ServiceName | Out-Null
    }
    Write-Host "Removed service $ServiceName"
} else {
    Write-Host "Service $ServiceName not installed"
}

if ($RemoveFirewall) {
    Remove-NetFirewallRule -DisplayName "TaxOps Filetrack Print Relay 8765" -EA SilentlyContinue
    Write-Host "Removed firewall rule"
}

if ($RemoveEnvFile) {
    Remove-Item -LiteralPath "C:\TaxOps\PrintRelay\relay.env" -Force -EA SilentlyContinue
    Write-Host "Removed relay.env"
}

Write-Host "Rollback: use start_print_relay.bat for manual console start."
