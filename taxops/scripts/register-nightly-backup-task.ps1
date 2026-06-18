# PROD-4 — Register Windows Task Scheduler job for nightly TaxOps SQLite backup (GitHub #92).
#
# Prerequisites: elevate PowerShell ("Run as administrator") once for Register-ScheduledTask.
# Edit paths below; retention / webhook URLs live in NSSM `.env`, task env, or system vars.
#
# Task runs as SYSTEM — local disks only. If outbound HTTPS alerts fail on SYSTEM, prefer a domain user Principal.

$ErrorActionPreference = 'Stop'

$TaskName = 'TaxOpsNightlyDbBackup'
$Py       = 'C:\TaxOps\taxops\.venv\Scripts\python.exe'   # prefer venv
$AppDir   = 'C:\TaxOps\taxops'

if (-not (Test-Path $Py)) { throw "Python missing: $Py" }
if (-not (Test-Path $AppDir)) { throw "App directory missing: $AppDir" }

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Removing existing scheduled task '$TaskName'."
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

$action = New-ScheduledTaskAction -Execute $Py -Argument 'scripts/nightly_backup_db.py' `
    -WorkingDirectory $AppDir

# Local server time — edit as needed.
$trigger = New-ScheduledTaskTrigger -Daily -At '2:10AM'

$principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' `
    -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Description 'PROD-4 TaxOps nightly SQLite backup; exit 1 alerts Task Scheduler History / monitoring.'

Write-Host "Registered '$TaskName'. Test run now:" -ForegroundColor Cyan
Write-Host "  schtasks.exe /Run /TN `"$TaskName`""
Write-Host ""
Write-Host "Task Scheduler History shows success/failure; exit code 1 is failure (see scripts/nightly_backup_db.py)."
Write-Host "Optional webhook: set TAXOPS_BACKUP_ALERT_WEBHOOK in machine env or a wrapper script."
