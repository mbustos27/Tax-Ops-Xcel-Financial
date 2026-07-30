#Requires -Version 5.1
<#
.SYNOPSIS
  Unregister Scheduled Task "TaxOps Scan Agent". Leaves .bat launchers intact.
#>
param(
    [string]$TaskName = "TaxOps Scan Agent",
    [switch]$RemoveFirewall
)

Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -EA SilentlyContinue
Write-Host "Unregistered task $TaskName (if it existed)."

if ($RemoveFirewall) {
    Remove-NetFirewallRule -DisplayName "TaxOps Scan Agent 8766" -EA SilentlyContinue
}

Write-Host "Rollback: use GO_SCAN_AGENT.bat for manual start."
