#Requires -Version 5.1
<#
.SYNOPSIS
  Copy reception relay scripts from the share to local disk.

.DESCRIPTION
  Running ensure/check scripts from T: / UNC is flaky (window closes, -File on UNC).
  Syncs to:
    C:\TaxOps\Reception\scripts\   (PowerShell)
    C:\TaxOps\Reception\GO_RECEPTION.bat  (local runner)

  Called by share-root GO_RECEPTION.bat before execution.

.EXAMPLE
  powershell -File sync_reception_scripts_local.ps1 -ShareRoot T:
#>
param(
    [string]$ShareRoot = "",
    [string]$LocalRoot = "C:\TaxOps\Reception",
    [switch]$Quiet
)

$ErrorActionPreference = "Continue"

function Say([string]$Msg, [string]$Color = "Gray") {
    if (-not $Quiet) { Write-Host $Msg -ForegroundColor $Color }
}

if (-not $ShareRoot) {
    if ($MyInvocation.MyCommand.Path) {
        $ShareRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
    } else {
        $ShareRoot = "\\Xcel-server\taxops"
    }
}
$ShareRoot = $ShareRoot.Trim().TrimEnd('\')
if ($ShareRoot -match '[<>\|\?\*"]') {
    Say ("  [FAIL] Illegal characters in ShareRoot: {0}" -f $ShareRoot) "Red"
    exit 3
}

$shareScripts = Join-Path $ShareRoot "taxops\scripts"
$localScripts = Join-Path $LocalRoot "scripts"

Say ""
Say "======== Sync reception scripts to local disk ========" "Cyan"
Say ("  From: {0}" -f $shareScripts) "DarkGray"
Say ("  To:   {0}" -f $localScripts) "DarkGray"

if (-not (Test-Path -LiteralPath (Join-Path $shareScripts "ensure_reception_relays.ps1"))) {
    Say ("  [FAIL] Source missing: {0}\ensure_reception_relays.ps1" -f $shareScripts) "Red"
    exit 4
}

foreach ($dir in @($LocalRoot, $localScripts, "C:\TaxOps\logs")) {
    if (-not (Test-Path -LiteralPath $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
}

$files = @(
    "ensure_reception_relays.ps1",
    "check_reception.ps1",
    "install_print_relay_service.ps1",
    "install_scan_agent_task.ps1",
    "uninstall_print_relay_service.ps1",
    "uninstall_scan_agent_task.ps1"
)

$robocopy = Join-Path $env:SystemRoot "System32\robocopy.exe"
if (-not (Test-Path -LiteralPath $robocopy)) { $robocopy = "robocopy" }

$rcArgs = @($shareScripts, $localScripts) + $files + @(
    "/R:2", "/W:2", "/IS", "/IT",
    "/NFL", "/NDL", "/NJH", "/NJS", "/NC", "/NS", "/NP"
)
& $robocopy @rcArgs | Out-Null
$rc = $LASTEXITCODE
if ($rc -ge 8) {
    Say ("  [FAIL] robocopy scripts exit {0}" -f $rc) "Red"
    exit 5
}

# Local runner (from share)
$localBatSrc = Join-Path $ShareRoot "GO_RECEPTION_local.bat"
$localBatDst = Join-Path $LocalRoot "GO_RECEPTION.bat"
if (-not (Test-Path -LiteralPath $localBatSrc)) {
    Say ("  [FAIL] Missing {0}" -f $localBatSrc) "Red"
    exit 6
}
Copy-Item -LiteralPath $localBatSrc -Destination $localBatDst -Force

# Remember share for local runs / Repair install scripts — always UNC (Admin/services cannot see T:)
$shareMark = Join-Path $LocalRoot "share_root.txt"
$markValue = $ShareRoot
if ($markValue -match '^[A-Za-z]:') { $markValue = "\\Xcel-server\taxops" }
Set-Content -LiteralPath $shareMark -Value $markValue -Encoding ASCII

# Also keep a local copy of this sync script
$syncSelf = $MyInvocation.MyCommand.Path
if ($syncSelf -and (Test-Path -LiteralPath $syncSelf)) {
    Copy-Item -LiteralPath $syncSelf -Destination (Join-Path $LocalRoot "sync_reception_scripts_local.ps1") -Force
}

$missing = @()
foreach ($f in @("ensure_reception_relays.ps1", "check_reception.ps1")) {
    if (-not (Test-Path -LiteralPath (Join-Path $localScripts $f))) { $missing += $f }
}
if (-not (Test-Path -LiteralPath $localBatDst)) { $missing += "GO_RECEPTION.bat" }
if ($missing.Count) {
    Say ("  [FAIL] Local copy missing: {0}" -f ($missing -join ", ")) "Red"
    exit 7
}

Say "  [OK] Local reception scripts ready" "Green"
Say ("       {0}" -f $LocalRoot) "DarkGray"
Say "==============================================" "Cyan"
Write-Output $LocalRoot
exit 0
