#Requires -Version 5.1
<#
.SYNOPSIS
  Copy TaxOps scan_agent code from the share to a local folder on this PC.

.DESCRIPTION
  Running Python from T: / UNC is slow and looks hung. This syncs:
    <Share>\taxops\scan_agent  ->  C:\TaxOps\ScanAgent\app\scan_agent

  Called by scan_agent_wizard.ps1 and start_scan_agent.bat.

.EXAMPLE
  powershell -File sync_scan_agent_local.ps1
  powershell -File sync_scan_agent_local.ps1 -ShareRoot \\Xcel-server\taxops
#>
param(
    [string]$ShareRoot = "",
    [string]$LocalRoot = "C:\TaxOps\ScanAgent",
    [switch]$Quiet
)

$ErrorActionPreference = "Continue"

function Say([string]$Msg, [string]$Color = "Gray") {
    if (-not $Quiet) { Write-Host $Msg -ForegroundColor $Color }
}

# Resolve share root (folder that contains taxops\scan_agent)
if (-not $ShareRoot) {
    if ($MyInvocation.MyCommand.Path) {
        $ShareRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
    } else {
        $ShareRoot = "\\Xcel-server\taxops"
    }
}
$ShareRoot = $ShareRoot.Trim().TrimEnd('\')
# Guard against cmd trailing-backslash quote eat: path may arrive as \\server\share" junk
if ($ShareRoot -match '[<>\|\?\*"]') {
    Say ("  [FAIL] Illegal characters in ShareRoot: {0}" -f $ShareRoot) "Red"
    exit 3
}
$shareAgent = Join-Path $ShareRoot "taxops\scan_agent"
if (-not (Test-Path -LiteralPath (Join-Path $shareAgent "server.py"))) {
    # Maybe ShareRoot already points at taxops\
    $alt = Join-Path $ShareRoot "scan_agent"
    if (Test-Path -LiteralPath (Join-Path $alt "server.py")) {
        $shareAgent = $alt
    }
}

$localApp = Join-Path $LocalRoot "app"
$localAgent = Join-Path $localApp "scan_agent"

Say ""
Say "======== Sync scan_agent to local disk ========" "Cyan"
Say ("  From: {0}" -f $shareAgent) "DarkGray"
Say ("  To:   {0}" -f $localAgent) "DarkGray"

if (-not (Test-Path -LiteralPath (Join-Path $shareAgent "server.py"))) {
    Say ("  [FAIL] Source missing: {0}\server.py" -f $shareAgent) "Red"
    exit 4
}

foreach ($dir in @($LocalRoot, $localApp)) {
    if (-not (Test-Path -LiteralPath $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
}

$robocopy = Join-Path $env:SystemRoot "System32\robocopy.exe"
if (-not (Test-Path -LiteralPath $robocopy)) { $robocopy = "robocopy" }

# Wipe local package first so a stale CoInitialize build cannot stick (timestamps /XO).
if (Test-Path -LiteralPath $localAgent) {
    Remove-Item -LiteralPath $localAgent -Recurse -Force -EA SilentlyContinue
}
New-Item -ItemType Directory -Path $localAgent -Force | Out-Null

# Always refresh from share
$rcArgs = @(
    $shareAgent, $localAgent,
    "/E", "/IS", "/IT", "/R:2", "/W:2",
    "/NFL", "/NDL", "/NJH", "/NJS", "/NC", "/NS", "/NP"
)
& $robocopy @rcArgs | Out-Null
$rc = $LASTEXITCODE
# robocopy 0-7 = success
if ($rc -ge 8) {
    Say ("  [FAIL] robocopy exit {0}" -f $rc) "Red"
    exit 5
}

$initPy = Join-Path $localAgent "__init__.py"
if (-not (Test-Path -LiteralPath $initPy)) {
    Set-Content -LiteralPath $initPy -Value "" -Encoding ASCII
}

$cache = Join-Path $localAgent "__pycache__"
if (Test-Path -LiteralPath $cache) {
    Remove-Item -LiteralPath $cache -Recurse -Force -EA SilentlyContinue
}

# Sanity: required files present locally
$need = @("server.py", "selftest.py", "__init__.py")
$missing = @()
foreach ($f in $need) {
    if (-not (Test-Path -LiteralPath (Join-Path $localAgent $f))) { $missing += $f }
}
if ($missing.Count) {
    Say ("  [FAIL] Local copy missing: {0}" -f ($missing -join ", ")) "Red"
    exit 6
}

$rev = Select-String -Path (Join-Path $localAgent "server.py") -Pattern "com_sta_v" -SimpleMatch -EA SilentlyContinue |
    Select-Object -First 1
$revText = if ($rev) { $rev.Line.Trim() } else { "(no com_sta marker)" }

Say "  [OK] Local copy ready" "Green"
Say ("       {0}" -f $revText) "DarkGray"
Say ("       PYTHONPATH should be: {0}" -f $localApp) "DarkGray"
Say "==============================================" "Cyan"

# Machine-readable path for callers
Write-Output $localApp
exit 0
