#Requires -Version 5.1
<#
.SYNOPSIS
  DEPRECATED — use T:\GO_RECEPTION.bat (and -Repair for auto-start).

.DESCRIPTION
  Frozen. This wizard installs outdated auto-start (Startup-folder print +
  NSSM ScanAgent) that conflicts with the current architecture:
    Print  = NSSM FiletrackRelay (UNC AppDirectory)
    Scan   = Scheduled Task Interactive AtLogOn (not session-0)

  Pass -ForceDeprecated to run the old wizard anyway.

  Canonical: T:\GO_RECEPTION.bat
  Docs: taxops\docs\reception_agents_runbook.md
#>
param(
    [string]$ServerIP        = "192.168.1.173",
    [string]$Hostname        = "taxlog",
    [string]$UncRoot         = "\\Xcel-server\taxops",
    [string]$RelayExpectedIP = "192.168.1.9",
    [string]$PrinterName     = "4BARCODE 4B-2054A",
    [string]$ScannerNameHint = "EPSON ES-500WII",
    [string]$ScanAgentToken  = $env:SCAN_AGENT_TOKEN,
    [int]$AppPort            = 5000,
    [int]$RelayPort          = 8765,
    [int]$ScanAgentPort      = 8766,
    [switch]$ForceDeprecated
)

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "========================================================" -ForegroundColor Yellow
Write-Host " DEPRECATED: setup_reception_pc.ps1" -ForegroundColor Yellow
Write-Host "========================================================" -ForegroundColor Yellow
Write-Host " Use instead:  T:\GO_RECEPTION.bat" -ForegroundColor Cyan
Write-Host "               T:\GO_RECEPTION.bat -Repair   (Admin)" -ForegroundColor Cyan
Write-Host "               T:\GO_SCAN_AGENT.bat" -ForegroundColor Cyan
Write-Host ""
if (-not $ForceDeprecated) {
    Write-Host "Refusing to run. Pass -ForceDeprecated only if you must." -ForegroundColor Red
    exit 2
}
Write-Host "[WARN] Continuing with deprecated reception setup..." -ForegroundColor Yellow
Write-Host ""

$UncRoot = $UncRoot.TrimEnd('\')
$totalSteps = 11
$script:step = 0
$script:fail = 0
$script:warn = 0
$script:ok = 0
$EpsonGuideUrl = "https://files.support.epson.com/docid/cpd5/cpd59603/index.html"
$EpsonGuideIcon = "C:\Program Files (x86)\Epson\guide\ES-400II_ES-500WII_el\book.ico"

function Test-IsAdmin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Get-DotEnvValue([string]$Path, [string]$Key) {
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    $prefix = "$Key="
    foreach ($line in Get-Content -LiteralPath $Path -EA SilentlyContinue) {
        $t = $line.Trim()
        if ($t.StartsWith("#")) { continue }
        if ($t.StartsWith($prefix)) {
            return $t.Substring($prefix.Length).Trim().Trim('"').Trim("'")
        }
    }
    return $null
}

$thisScript = $MyInvocation.MyCommand.Path
if (-not $thisScript -or -not (Test-Path -LiteralPath $thisScript)) {
    $thisScript = Join-Path $UncRoot "setup_reception_pc.ps1"
}
$root = Split-Path -Parent $thisScript
if (-not (Test-Path $root)) { $root = $UncRoot }
$appDir = Join-Path $root "taxops"
$envFile = Join-Path $appDir ".env"
if (-not $ScanAgentToken) {
    $ScanAgentToken = Get-DotEnvValue $envFile "SCAN_AGENT_TOKEN"
}
if (-not $ScanAgentToken) {
    $ScanAgentToken = Get-DotEnvValue (Join-Path $root ".env") "SCAN_AGENT_TOKEN"
}
try { Set-Location $root } catch {}

if (-not (Test-IsAdmin)) {
    Write-Host "Need Administrator - relaunching (click Yes on UAC)..." -ForegroundColor Yellow
    $arg = @(
        "-NoExit", "-NoProfile", "-ExecutionPolicy", "Bypass",
        "-File", $thisScript,
        "-ServerIP", $ServerIP, "-Hostname", $Hostname, "-UncRoot", $UncRoot,
        "-RelayExpectedIP", $RelayExpectedIP, "-PrinterName", $PrinterName,
        "-ScannerNameHint", $ScannerNameHint, "-AppPort", "$AppPort", "-RelayPort", "$RelayPort",
        "-ScanAgentPort", "$ScanAgentPort"
    )
    if ($ScanAgentToken) { $arg += @("-ScanAgentToken", $ScanAgentToken) }
    if ($ForceDeprecated) { $arg += "-ForceDeprecated" }
    Start-Process powershell.exe -Verb RunAs -WorkingDirectory $root -ArgumentList $arg
    exit 0
}

try {
    $Host.UI.RawUI.WindowTitle = "TaxOps Reception Setup"
    Clear-Host
} catch {}

function Say($m, $c = "White") { Write-Host $m -ForegroundColor $c }
function Good($m) { Say ("  [OK]   {0}" -f $m) "Green";  $script:ok++ }
function Bad($m)  { Say ("  [FAIL] {0}" -f $m) "Red";    $script:fail++ }
function Warn($m) { Say ("  [WARN] {0}" -f $m) "Yellow"; $script:warn++ }
function Info($m) { Say ("         {0}" -f $m) "DarkGray" }
function Work($m) { Say ("  ...    {0}" -f $m) "Cyan" }
function Step($title) {
    $script:step++
    Write-Host ""
    Say ("--------------------------------------------------------") "DarkGray"
    Say ("  Step {0} of {1}: {2}" -f $script:step, $totalSteps, $title) "Cyan"
    Say ("--------------------------------------------------------") "DarkGray"
    try { $Host.UI.RawUI.WindowTitle = ("Reception Setup {0}/{1}" -f $script:step, $totalSteps) } catch {}
}
function Pause-Step([string]$msg = "Press Enter to continue...") {
    Write-Host ""
    Read-Host "  $msg" | Out-Null
}

function Test-TcpPort([string]$Computer, [int]$Port, [int]$Ms = 3000) {
    try {
        $c = New-Object System.Net.Sockets.TcpClient
        $ar = $c.BeginConnect($Computer, $Port, $null, $null)
        if (-not $ar.AsyncWaitHandle.WaitOne($Ms, $false)) { try { $c.Close() } catch {}; return $false }
        try { $c.EndConnect($ar) | Out-Null; $c.Close(); return $true }
        catch { try { $c.Close() } catch {}; return $false }
    } catch { return $false }
}

function Invoke-NetUseTimed([string]$ArgsLine, [int]$TimeoutSec = 8) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "cmd.exe"
    $psi.Arguments = "/c net use $ArgsLine"
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.CreateNoWindow = $true
    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $psi
    [void]$p.Start()
    if (-not $p.WaitForExit($TimeoutSec * 1000)) {
        try { $p.Kill() } catch {}
        return @{ TimedOut = $true; Text = "timed out"; ExitCode = -1 }
    }
    return @{
        TimedOut = $false
        ExitCode = $p.ExitCode
        Text = $p.StandardOutput.ReadToEnd() + $p.StandardError.ReadToEnd()
    }
}

function Get-HostsIP([string]$Name) {
    $hf = Join-Path $env:windir "System32\drivers\etc\hosts"
    $esc = [regex]::Escape($Name)
    foreach ($line in Get-Content $hf -EA SilentlyContinue) {
        if ($line -match "^\s*#") { continue }
        if ($line -match "^\s*([0-9a-fA-F.:]+)\s+.*\b$esc\b") { return $Matches[1] }
    }
    return $null
}

function Set-HostsIP([string]$IP, [string]$Name) {
    $hf = Join-Path $env:windir "System32\drivers\etc\hosts"
    $stamp = Get-Date -Format "yyyyMMdd_HHmm"
    Copy-Item $hf ("{0}.bak_{1}" -f $hf, $stamp) -Force -EA SilentlyContinue
    $esc = [regex]::Escape($Name)
    $out = foreach ($line in Get-Content $hf) {
        if ($line -notmatch "^\s*#" -and $line -match "^\s*([0-9a-fA-F.:]+)\s+.*\b$esc\b") { "# old: $line" }
        else { $line }
    }
    ($out + ("{0}`t{1}`t# reception setup {2}" -f $IP, $Name, $stamp)) |
        Set-Content $hf -Encoding ASCII
}

function Find-Python {
    $candidates = @(
        (Join-Path $appDir ".venv\Scripts\python.exe"),
        "C:\TaxOps\taxops\.venv\Scripts\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python314\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
        "C:\Python313\python.exe",
        "C:\Python312\python.exe",
        "C:\Python311\python.exe"
    )
    foreach ($p in $candidates) {
        if ($p -and (Test-Path -LiteralPath $p)) { return $p }
    }
    $pyLauncher = Get-Command py -EA SilentlyContinue
    if ($pyLauncher -and $pyLauncher.Source -notmatch "WindowsApps") {
        $out = & py -3 -c "import sys; print(sys.executable)" 2>$null
        if ($LASTEXITCODE -eq 0 -and $out) {
            $exe = $out.Trim()
            if (Test-Path -LiteralPath $exe) { return $exe }
        }
    }
    foreach ($c in @(Get-Command python.exe -EA SilentlyContinue)) {
        if ($c.Source -notmatch "WindowsApps") { return $c.Source }
    }
    return $null
}

function Get-RelayHealth {
    try {
        $r = Invoke-WebRequest ("http://127.0.0.1:{0}/health" -f $RelayPort) -UseBasicParsing -TimeoutSec 4
        $j = $r.Content | ConvertFrom-Json -EA SilentlyContinue
        if ($j -and $j.status -eq "ok") { return $j }
    } catch {}
    return $null
}

function Get-EpsonPnPDevices {
    <#
      Return all Epson-related PnP nodes (present OR error/phantom).
      PresentOnly alone misses ES-500WII when Status=Unknown / CM_PROB_PHANTOM.
    #>
    $all = @()
    try {
        $all = @(Get-PnpDevice -EA SilentlyContinue | Where-Object {
            ($_.InstanceId -match 'VID_04B8') -or
            ($_.FriendlyName -match 'EPSON|Epson')
        })
    } catch {}
    return $all
}

function Get-EpsonScanners {
    $devs = @(Get-EpsonPnPDevices)
    # Prefer imaging / named document scanners; keep Utility separate
    $scanners = @($devs | Where-Object {
        $_.FriendlyName -match 'ES-500|ES-400|DS-|Epson Scan|EPSON ES-' -or
        ($_.Class -eq 'Image' -and $_.FriendlyName -match 'EPSON|Epson' -and $_.FriendlyName -notmatch 'WSD')
    })
    return $scanners
}

function Get-EpsonUtilityDevice {
    @(Get-EpsonPnPDevices) | Where-Object { $_.FriendlyName -match 'EPSON Utility|Epson Utility' } | Select-Object -First 1
}

function Test-WiaEpson {
    $names = @()
    try {
        $dm = New-Object -ComObject WIA.DeviceManager
        for ($i = 1; $i -le $dm.DeviceInfos.Count; $i++) {
            $info = $dm.DeviceInfos.Item($i)
            $name = $null
            $mfg = $null
            try { $name = [string]$info.Properties.Item('Name').Value } catch {}
            try { $mfg = [string]$info.Properties.Item('Manufacturer').Value } catch {}
            if (("$name $mfg") -match 'EPSON|Epson|ES-500|ES-400') {
                $names += ($(if ($name) { $name } else { $mfg }))
            }
        }
    } catch {}
    return $names
}

function Invoke-EpsonHardwareRescan {
    Work "Rescanning PnP hardware (pnputil /scan-devices)..."
    try {
        & pnputil.exe /scan-devices 2>&1 | Out-Null
    } catch {}
    Start-Sleep 3
}

function Repair-EpsonPhantomDevices {
    <#
      Phantom Epson USB nodes (unplugged-while-listed / bad enumerate).
      Remove phantom nodes then rescan so a live plug can reappear cleanly.
    #>
    $phantoms = @(Get-EpsonPnPDevices | Where-Object {
        $_.Status -eq 'Unknown' -or
        ($_.Problem -and ("$($_.Problem)" -match 'PHANTOM|CM_PROB'))
    })
    if ($phantoms.Count -eq 0) { return 0 }
    Work ("Removing {0} phantom/error Epson USB node(s) then rescanning..." -f $phantoms.Count)
    foreach ($d in $phantoms) {
        try {
            & pnputil.exe /remove-device "$($d.InstanceId)" 2>&1 | Out-Null
        } catch {
            try { Remove-PnpDevice -InstanceId $d.InstanceId -Confirm:$false -EA SilentlyContinue } catch {}
        }
    }
    Start-Sleep 2
    Invoke-EpsonHardwareRescan
    return $phantoms.Count
}

function Test-EpsonSoftware {
    $scan2Dirs = @(
        "C:\Program Files (x86)\Epson\Epson Scan 2",
        "C:\Program Files\Epson\Epson Scan 2",
        "C:\Program Files (x86)\epson\Epson Scan 2",
        "C:\Program Files\epson\Epson Scan 2"
    )
    $scan2 = $scan2Dirs | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
    $smart = $null
    try {
        $smart = Get-ItemProperty `
            "HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*",
            "HKLM:\Software\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*" `
            -EA SilentlyContinue |
            Where-Object { $_.DisplayName -match "Epson ScanSmart" } |
            Select-Object -First 1
    } catch {}
    $launcher = @(
        "C:\Program Files (x86)\Epson\Epson Scan 2\Core\es2launcher.exe",
        "C:\Program Files\Epson\Epson Scan 2\Core\es2launcher.exe",
        "C:\Program Files (x86)\epson\Epson Scan 2\Core\es2launcher.exe",
        "C:\Program Files\epson\Epson Scan 2\Core\es2launcher.exe"
    ) | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
    return [pscustomobject]@{
        Scan2Path     = $scan2
        ScanSmartName = if ($smart) { $smart.DisplayName } else { $null }
        ScanSmartVer  = if ($smart) { $smart.DisplayVersion } else { $null }
        Launcher      = $launcher
    }
}

function Ensure-EpsonGuideShortcut {
    $pubDesktop = [Environment]::GetFolderPath("CommonDesktopDirectory")
    if (-not $pubDesktop) { $pubDesktop = Join-Path $env:PUBLIC "Desktop" }
    $urlPath = Join-Path $pubDesktop "Epson ES-400 II_ES-500W II Guide.url"
    if (Test-Path -LiteralPath $urlPath) { return $urlPath }
    $lines = @(
        "[InternetShortcut]",
        "URL=$EpsonGuideUrl"
    )
    if (Test-Path -LiteralPath $EpsonGuideIcon) {
        $lines += "IconFile=$EpsonGuideIcon"
        $lines += "IconIndex=0"
    }
    Set-Content -Path $urlPath -Value ($lines -join "`r`n") -Encoding ASCII
    return $urlPath
}

function Write-ReceptionGuideHtml([string]$Path) {
    $html = @"
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>TaxOps Reception PC - Quick Guide</title>
<style>
  body { font-family: Segoe UI, Arial, sans-serif; max-width: 720px; margin: 40px auto;
         padding: 0 24px; color: #3D1019; background: #F4EFE9; line-height: 1.45; }
  h1 { color: #6B2233; font-size: 1.6rem; margin-bottom: 0.25rem; }
  .sub { color: #666; margin-bottom: 1.5rem; }
  h2 { color: #6B2233; font-size: 1.15rem; margin-top: 1.75rem; border-bottom: 1px solid #d4c4b8; padding-bottom: 4px; }
  ol, ul { padding-left: 1.3rem; }
  li { margin: 0.4rem 0; }
  code, .url { background: #fff; padding: 2px 8px; border-radius: 4px; font-family: Consolas, monospace; }
  .box { background: #fff; border-left: 4px solid #6B2233; padding: 12px 16px; margin: 1rem 0; }
  .warn { border-left-color: #b45309; }
</style>
</head>
<body>
  <h1>TaxOps Reception Workstation</h1>
  <p class="sub">Tax Log + label printer/relay + Epson scanner + Scan Agent</p>

  <h2>Daily use</h2>
  <ol>
    <li>Open Tax Log: <span class="url">http://$Hostname/</span>
        (or use the <b>Tax Log</b> desktop shortcut).</li>
    <li>Use <b>http://</b> only &mdash; never https://.</li>
    <li>The label print relay should start automatically at login
        (minimized). If labels stop printing, double-click
        <span class="url">T:/start_print_relay.bat</span>
        or <span class="url">\\Xcel-server\taxops\start_print_relay.bat</span>.</li>
    <li>Intake document scanning uses the <b>Scan Agent</b> service
        (port $ScanAgentPort) from Tax Log - load pages in the Epson ADF,
        then use <b>Scan client documents</b> after intake (or on the return).</li>
    <li>Fallback: Epson ScanSmart / Scan 2 for ad-hoc scans.
        Official help: desktop shortcut
        <b>Epson ES-400 II_ES-500W II Guide</b>.</li>
  </ol>

  <h2>Hardware on this PC</h2>
  <ul>
    <li>Label printer: <b>$PrinterName</b> (USB) + print relay on port <b>$RelayPort</b></li>
    <li>Document scanner: <b>$ScannerNameHint</b> (USB) with Epson Scan 2 / ScanSmart</li>
    <li>Scan Agent service: TCP <b>$ScanAgentPort</b> (NSSM <b>ScanAgent</b>)</li>
    <li>TaxOps server: <b>$ServerIP</b> &mdash; ideal fixed IP for this PC: <b>$RelayExpectedIP</b></li>
  </ul>

  <div class="box warn">
    <b>Labels not printing or scanner missing?</b> Run
    <span class="url">\\Xcel-server\taxops\TaxOps_Setup_Diagnose.bat</span>
    (click Yes on UAC) and use <b>Copy log</b> if you need help.
    For Epson drivers/software, use the desktop Epson guide or
    <span class="url">$EpsonGuideUrl</span>.
  </div>

  <h2>After setup &mdash; quick check</h2>
  <ol>
    <li>Open <span class="url">http://$Hostname/</span> and sign in.</li>
    <li>Do a test intake (or ask admin) and confirm a label prints.</li>
    <li>Confirm a small relay window/taskbar item is running.</li>
    <li>Confirm Scan Agent via <span class="url">T:\scan_agent_wizard.bat</span> (com_sta_v1) and a post-intake scan works.</li>
  </ol>

  <p class="sub">Generated by setup_reception_pc.ps1</p>
</body>
</html>
"@
    $dir = Split-Path -Parent $Path
    if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
    Set-Content -Path $Path -Value $html -Encoding UTF8
}

# ========== WELCOME ==========
Say ""
Say "========================================================" "DarkRed"
Say "  TaxOps Reception Workstation Setup" "White"
Say "========================================================" "DarkRed"
Say ("  PC:     {0}" -f $env:COMPUTERNAME) "DarkGray"
Say ("  User:   {0}" -f $env:USERNAME) "DarkGray"
Say ("  Time:   {0}" -f (Get-Date -Format "yyyy-MM-dd HH:mm")) "DarkGray"
Say ("  Server: {0}   Tax Log: http://{1}/" -f $ServerIP, $Hostname) "DarkGray"
Say ""
Say "  This wizard sets up the FRONT DESK PC:" "White"
Say "    - Tax Log access (T: drive, hosts, desktop shortcut)" "Gray"
Say "    - 4BARCODE label printer + print relay" "Gray"
Say "    - Epson document scanner drivers / ScanSmart checks" "Gray"
Say "    - Scan Agent (WIA PDF service for Tax Log intake scanning)" "Gray"
Say ""
Say "  Before continuing, please confirm:" "Yellow"
Say "    1. This PC is on the office Wi-Fi / network (192.168.1.x)" "Gray"
Say "    2. The $PrinterName label printer is plugged in via USB" "Gray"
Say "    3. The Epson scanner (ES-500W II / ES-400 II) is plugged in via USB" "Gray"
Say "    4. You know the shared SCAN_AGENT_TOKEN (same as TaxOps server)" "Gray"
Say "    5. You clicked Yes on the Administrator (UAC) prompt" "Gray"
Pause-Step "Press Enter to start setup"

# ----- 1 network -----
Step "Office network"
$ips = @()
try {
    $ips = @(Get-NetIPAddress -AddressFamily IPv4 -EA SilentlyContinue |
        Where-Object { $_.IPAddress -notlike "127.*" -and $_.IPAddress -notlike "169.254.*" } |
        Select-Object -ExpandProperty IPAddress)
} catch {}
if ($ips.Count -eq 0) { Bad "No network address - connect to office Wi-Fi first" }
else {
    Good ("IP address(es): {0}" -f ($ips -join ", "))
    if ($ips | Where-Object { $_ -like "192.168.1.*" }) { Good "On office LAN 192.168.1.x" }
    else { Warn "Not on 192.168.1.x - setup may fail until you join the office network" }
}
if ($ips -contains $RelayExpectedIP) {
    Good "This PC already has the expected relay IP $RelayExpectedIP"
} else {
    Warn ("Current IP is not {0} (have: {1})" -f $RelayExpectedIP, ($ips -join ", "))
    Info "Ask IT for a DHCP reservation to $RelayExpectedIP so the TaxOps server can always find this printer PC."
    Info "Until then, update FILETRACK_RELAY_URL on the server if this PC's IP changes."
}

# ----- 2 T: drive -----
Step "Map T: drive (TaxOps share)"
Work "Checking share $UncRoot (timeout protected)..."
$uncOk = $false
try {
    if (Test-Path -LiteralPath (Join-Path $UncRoot "taxops\app.py")) { $uncOk = $true }
} catch {}
if ($uncOk) { Good "Network share reachable" } else { Warn "UNC path not visible yet - will still try map" }

$st = Invoke-NetUseTimed "T:" 8
if ($st.TimedOut) {
    Warn "Checking T: timed out - continuing"
} elseif ($st.Text -match [regex]::Escape($UncRoot)) {
    Good "T: already mapped"
} else {
    if ($st.Text -match "OK|Unavailable") {
        Work "Removing old T: mapping..."
        $null = Invoke-NetUseTimed "T: /delete /yes" 8
    }
    Work "Mapping T: -> $UncRoot ..."
    $map = Invoke-NetUseTimed ("T: `"$UncRoot`" /persistent:yes") 10
    if ($map.TimedOut) { Warn "Map timed out - you can map T: manually later" }
    elseif (Test-Path "T:\taxops\app.py") { Good "T: mapped successfully" }
    else { Warn "T: map may have failed - UNC still works for scripts" }
}
if (Test-Path "T:\taxops\app.py") { Good "T:\taxops\app.py OK" }
elseif ($uncOk) { Good "Share OK via UNC (enough for TaxOps scripts)" }

# ----- 3 hosts -----
Step "Tax Log address (http://$Hostname/)"
$hip = Get-HostsIP $Hostname
if ($hip -eq $ServerIP) { Good "hosts already points $Hostname -> $ServerIP" }
else {
    Work "Updating hosts file..."
    try {
        Set-HostsIP $ServerIP $Hostname
        ipconfig /flushdns | Out-Null
        if ((Get-HostsIP $Hostname) -eq $ServerIP) { Good "hosts updated: $Hostname -> $ServerIP" }
        else { Bad "Could not write hosts entry" }
    } catch { Bad $_.Exception.Message }
}

# ----- 4 Tax Log shortcut -----
Step "Desktop Tax Log shortcut"
try {
    $chrome = @(
        "$env:ProgramFiles\Google\Chrome\Application\chrome.exe",
        "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
        "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
    ) | Where-Object { Test-Path $_ } | Select-Object -First 1
    $desktop = [Environment]::GetFolderPath("CommonDesktopDirectory")
    if (-not $desktop) { $desktop = [Environment]::GetFolderPath("Desktop") }
    $lnkPath = Join-Path $desktop "Tax Log.lnk"
    $w = New-Object -ComObject WScript.Shell
    $sc = $w.CreateShortcut($lnkPath)
    if ($chrome) {
        $sc.TargetPath = $chrome
        $sc.Arguments = "http://$Hostname/"
        $sc.IconLocation = "$chrome,4"
    } else {
        $sc.TargetPath = "http://$Hostname/"
    }
    $sc.Description = "Tax Log"
    $sc.Save()
    Good "Created: $lnkPath"
} catch { Warn "Shortcut skipped: $($_.Exception.Message)" }

# ----- 5 label printer -----
Step "Label printer ($PrinterName)"
$printerOk = $false
try {
    $p = Get-Printer -Name $PrinterName -EA SilentlyContinue
    if ($p) { Good "Windows sees printer '$PrinterName'"; $printerOk = $true }
    else {
        Bad "Printer '$PrinterName' not installed on this PC"
        Info "Install the 4BARCODE / Arkscan driver, plug USB in, then re-run this setup."
        $all = Get-Printer -EA SilentlyContinue | Select-Object -ExpandProperty Name
        if ($all) { Info ("Other printers: {0}" -f ($all -join ", ")) }
    }
} catch { Warn "Could not query printers: $($_.Exception.Message)" }

# ----- 6 Python / deps -----
Step "Print relay software (Python)"
$py = Find-Python
if (-not $py) {
    Bad "Python not found (Windows Store stub does not count)"
    Info "Install Python 3 from python.org (check 'Add to PATH'), then re-run this setup."
} else {
    Good "Python: $py"
    if (Test-Path $appDir) {
        Push-Location $appDir
        Work "Checking packages (pywin32, flask, requests)..."
        & $py -c "import win32print, flask, requests" 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) { Good "Relay packages already installed" }
        else {
            Work "Installing from filetrack\requirements.txt (may take a minute)..."
            & $py -m pip install -q -r "filetrack\requirements.txt" 2>&1 | Out-Null
            & $py -c "import win32print, flask, requests" 2>&1 | Out-Null
            if ($LASTEXITCODE -eq 0) { Good "Packages installed" }
            else { Bad "pip install failed - check network / Python install" }
        }
        if ($printerOk) {
            Work "Confirming printer via filetrack..."
            $list = & $py -c "from filetrack.labels.printer import list_printers; print(chr(10).join(list_printers()))" 2>&1
            if ($LASTEXITCODE -eq 0 -and ("$list" -match [regex]::Escape($PrinterName))) {
                Good "filetrack can see '$PrinterName'"
            } else {
                Warn "filetrack did not list '$PrinterName'"
            }
        }
        Pop-Location
    } else {
        Warn "taxops folder not found at $appDir"
    }
}

# ----- 7 start relay + autostart -----
Step "Start print relay + auto-start at login"
$relayBat = Join-Path $root "start_print_relay.bat"
if (-not (Test-Path $relayBat)) { $relayBat = Join-Path $UncRoot "start_print_relay.bat" }

$relayToken = $env:FILETRACK_RELAY_TOKEN
if (-not $relayToken) { $relayToken = Get-DotEnvValue $envFile "FILETRACK_RELAY_TOKEN" }
if (-not $relayToken) { $relayToken = "zv8z42FQcfufRAvQDrJMXMXgb7Mpttdq" }

$health = Get-RelayHealth
if ($health) { Good "Print relay already answering on port $RelayPort" }
else {
    $pyRelay = Find-Python
    if ($pyRelay -and (Test-Path $appDir)) {
        Work ("Starting filetrack.relay.server with {0} ..." -f $pyRelay)
        $env:FILETRACK_PRINTER = $PrinterName
        $env:FILETRACK_RELAY_TOKEN = $relayToken
        $env:FILETRACK_RELAY_HOST = "0.0.0.0"
        $env:FILETRACK_RELAY_PORT = "$RelayPort"
        Start-Process -FilePath $pyRelay `
            -ArgumentList @("-m", "filetrack.relay.server", "--port", "$RelayPort") `
            -WorkingDirectory $appDir `
            -WindowStyle Minimized
        for ($i = 1; $i -le 20; $i++) {
            Work "Waiting for relay... $i/20"
            Start-Sleep 2
            $health = Get-RelayHealth
            if ($health) { break }
        }
    }
    if (-not $health -and (Test-Path $relayBat)) {
        Work "Fallback: start_print_relay.bat (normal window - read any errors)..."
        Start-Process -FilePath $relayBat -WorkingDirectory (Split-Path $relayBat) -WindowStyle Normal
        for ($i = 1; $i -le 15; $i++) {
            Work "Waiting for relay (bat)... $i/15"
            Start-Sleep 2
            $health = Get-RelayHealth
            if ($health) { break }
        }
    }
    if ($health) { Good "Print relay is running" }
    else {
        Bad "Relay did not become healthy - open start_print_relay.bat manually and read the window"
        Info "Common causes: port 8765 in use, or an import error in the relay window"
    }
}

# Startup folder shortcut (same pattern as install_print_relay_autostart.ps1)
try {
    $startup = [Environment]::GetFolderPath("Startup")
    $startupLnk = Join-Path $startup "TaxOps Print Relay.lnk"
    $w = New-Object -ComObject WScript.Shell
    $sc = $w.CreateShortcut($startupLnk)
    $sc.TargetPath = $relayBat
    $sc.WorkingDirectory = Split-Path $relayBat
    $sc.WindowStyle = 7
    $sc.Description = "TaxOps label print relay (auto-start)"
    $sc.Save()
    Good "Login auto-start installed: $startupLnk"
    Info "Relay will start minimized every time this Windows user logs in."
} catch {
    Warn "Could not create Startup shortcut: $($_.Exception.Message)"
}

$svc = Get-Service FiletrackRelay -EA SilentlyContinue
if ($svc -and $svc.Status -eq "Running") {
    Good "FiletrackRelay Windows service is also running (best)"
} else {
    Info "Optional later: install_print_relay_nssm.ps1 for a true Windows service (survives logout)."
}

# ----- 8 firewall -----
Step "Firewall (allow TaxOps server to reach this PC)"
$fwName = "TaxOps Print Relay"
$rule = Get-NetFirewallRule -DisplayName $fwName -EA SilentlyContinue
$okFw = $false
if ($rule) {
    $f = Get-NetFirewallPortFilter -AssociatedNetFirewallRule $rule -EA SilentlyContinue
    $okFw = ($rule.Enabled -eq $true -and $rule.Action -eq "Allow" -and
             $f -and ($f.LocalPort -contains "$RelayPort" -or $f.LocalPort -contains "Any"))
}
if ($okFw) { Good "Firewall already allows TCP $RelayPort" }
else {
    Work "Adding inbound rule for TCP $RelayPort..."
    if ($rule) { Remove-NetFirewallRule -DisplayName $fwName -EA SilentlyContinue }
    New-NetFirewallRule -DisplayName $fwName -Direction Inbound -Protocol TCP -LocalPort $RelayPort -Action Allow -Profile Any | Out-Null
    Good "Firewall rule added"
}

# ----- 9 Epson scanner -----
Step "Epson document scanner (device + drivers)"
Work "Looking for Epson USB / Image devices (including error/phantom nodes)..."

# Software first - drivers package may already be present even if USB is flaky
$sw = Test-EpsonSoftware
if ($sw.Scan2Path) { Good "Epson Scan 2 installed: $($sw.Scan2Path)" }
else {
    Bad "Epson Scan 2 not found under Program Files"
    Info "Install Epson Scan 2 from the ES-400 II / ES-500W II software package."
    Info ("Guide / downloads: {0}" -f $EpsonGuideUrl)
}
if ($sw.ScanSmartName) {
    $smartLabel = $sw.ScanSmartName
    if ($sw.ScanSmartVer) { $smartLabel = "{0} v{1}" -f $sw.ScanSmartName, $sw.ScanSmartVer }
    Good $smartLabel
} else {
    Warn "Epson ScanSmart not listed in installed programs"
    Info "Install ScanSmart for walk-in scans; Scan Agent uses WIA once the device is OK."
}
if ($sw.Launcher) { Good "Scan 2 launcher OK: $($sw.Launcher)" }

# Initial PnP inventory
$allEpson = @(Get-EpsonPnPDevices)
if ($allEpson.Count -gt 0) {
    foreach ($d in $allEpson) {
        $prob = if ($d.Problem) { " problem=$($d.Problem)" } else { "" }
        Info ("PnP: {0} | class={1} | status={2}{3}" -f $d.FriendlyName, $d.Class, $d.Status, $prob)
    }
} else {
    Info "No Epson VID_04B8 nodes in Device Manager yet."
}

Invoke-EpsonHardwareRescan

$scanners = @(Get-EpsonScanners)
$liveOk = @($scanners | Where-Object { $_.Status -eq 'OK' })
$broken = @($scanners | Where-Object { $_.Status -ne 'OK' })

# If only phantoms / errors, strip them and rescan so a live USB plug re-enumerates
if ($liveOk.Count -eq 0) {
    $removed = Repair-EpsonPhantomDevices
    if ($removed -gt 0) {
        Info "If the scanner is powered on, unplug USB 5 seconds, plug back into a rear USB port, wait 10s."
        Pause-Step "Press Enter after the scanner is powered and re-plugged"
        Invoke-EpsonHardwareRescan
    }
    $scanners = @(Get-EpsonScanners)
    $liveOk = @($scanners | Where-Object { $_.Status -eq 'OK' })
    $broken = @($scanners | Where-Object { $_.Status -ne 'OK' })
}

$matchedHint = $false
if ($liveOk.Count -gt 0) {
    foreach ($s in $liveOk) {
        Good ("Scanner live: {0} (status OK)" -f $s.FriendlyName)
        if ($s.FriendlyName -match [regex]::Escape($ScannerNameHint) -or
            $s.FriendlyName -match "ES-500W|ES-400") {
            $matchedHint = $true
        }
    }
    if (-not $matchedHint) {
        Warn ("Expected something like '{0}' - found other Epson imaging device(s)" -f $ScannerNameHint)
    }
} elseif ($broken.Count -gt 0) {
    foreach ($s in $broken) {
        Bad ("Scanner listed but NOT live: {0} (status={1}, problem={2})" -f $s.FriendlyName, $s.Status, $s.Problem)
    }
    Info "Windows sees an Epson node but it is phantom/error - usually USB power, cable, or hub."
    Info "Fix: power ON scanner -> direct motherboard USB (not front hub) -> Device Manager rescan."
    Info "With Epson Scan 2 installed: right-click the device -> Update driver -> Search automatically."
} else {
    Bad "No Epson document scanner device in Device Manager"
    Info "Power on the ES-500W II, use a direct USB cable, then re-run this step."
    Info ("Driver package / guide: {0}" -f $EpsonGuideUrl)
}

$util = Get-EpsonUtilityDevice
if ($util -and $util.Status -eq "OK") { Good "EPSON Utility USB interface OK" }
elseif ($util) { Warn ("EPSON Utility present but status={0} (problem={1})" -f $util.Status, $util.Problem) }
elseif ($liveOk.Count -gt 0) { Warn "EPSON Utility not listed - scanner may still work for WIA" }

$wiaNames = @(Test-WiaEpson)
if ($wiaNames.Count -gt 0) {
    foreach ($n in $wiaNames) { Good ("WIA sees Epson: {0}" -f $n) }
} else {
    if ($liveOk.Count -gt 0) {
        Warn "WIA does not list Epson yet - Scan Agent may still fail until WIA picks it up"
        Info "Open Epson Scan 2 once, or reboot after driver install."
    } else {
        Info "WIA has no Epson device (expected until the USB scanner status is OK)."
    }
}

# Offer Device Manager jump if still broken
if ($liveOk.Count -eq 0) {
    $openDm = Read-Host "  Open Device Manager to Imaging devices now? (Y/n)"
    if ($openDm -notmatch '^[nN]') {
        try { Start-Process devmgmt.msc } catch {}
    }
}

try {
    $guidePath = Ensure-EpsonGuideShortcut
    if (Test-Path -LiteralPath $guidePath) {
        Good "Desktop Epson guide shortcut: $guidePath"
    }
} catch {
    Warn "Could not create Epson guide shortcut: $($_.Exception.Message)"
    Info ("Open manually: {0}" -f $EpsonGuideUrl)
}

# ----- 10 Scan Agent -----
Step "Scan Agent (Tax Log intake scanning on TCP $ScanAgentPort)"
$scanSetup = Join-Path $root "setup_scan_agent.ps1"
if (-not (Test-Path -LiteralPath $scanSetup)) {
    $scanSetup = Join-Path $UncRoot "setup_scan_agent.ps1"
}
if (-not (Test-Path -LiteralPath $scanSetup)) {
    Bad "setup_scan_agent.ps1 not found on share"
    Info "Re-copy from the TaxOps share, then re-run this wizard."
} else {
    if (-not $ScanAgentToken) {
        Say "  SCAN_AGENT_TOKEN is empty." "Yellow"
        $ScanAgentToken = Read-Host "  Enter shared token (must match TaxOps SCAN_AGENT_TOKEN)"
    }
    if (-not $ScanAgentToken) {
        Bad "Token required - Scan Agent will not start without it"
        Info "Set SCAN_AGENT_TOKEN on TaxOps server and re-run, or run setup_scan_agent.bat alone."
    } else {
        Work "Installing / starting ScanAgent via scan_agent_wizard.ps1 ..."
        try {
            $wiz = Join-Path $root "scan_agent_wizard.ps1"
            if (-not (Test-Path -LiteralPath $wiz)) { $wiz = Join-Path $UncRoot "scan_agent_wizard.ps1" }
            if (Test-Path -LiteralPath $wiz) {
                & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $wiz -UncRoot $UncRoot -Token $ScanAgentToken -Port $ScanAgentPort -Mode Install -Quiet
            } else {
                & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $scanSetup -UncRoot $UncRoot -Token $ScanAgentToken -Port $ScanAgentPort -Quiet
            }
            if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) {
                Warn ("scan agent wizard exited with code {0} - check output / C:\TaxOps\ScanAgent\logs" -f $LASTEXITCODE)
            }
        } catch {
            Bad "Scan Agent setup failed: $($_.Exception.Message)"
        }
        $healthOk = $false
        try {
            $tok = $ScanAgentToken
            $hr = Invoke-WebRequest -Uri ("http://127.0.0.1:{0}/health" -f $ScanAgentPort) -Headers @{ "X-Scan-Agent-Token" = $tok } -UseBasicParsing -TimeoutSec 5
            if ($hr.Content -match "com_sta_v1" -and $hr.Content -notmatch "CoInitialize") { $healthOk = $true }
        } catch {}
        if ($healthOk) {
            Good "ScanAgent healthy on port $ScanAgentPort (com_sta_v1)"
        } else {
            Warn 'ScanAgent not healthy - on reception run: T:\scan_agent_wizard.bat'
        }
        Info ("On TaxOps server: SCAN_AGENT_URL=http://{0}:{1}  SCAN_AGENT_TOKEN=<same>" -f $RelayExpectedIP, $ScanAgentPort)
        Info "Then restart TaxOpsService so the browser gets the token/URL."
    }
}

# ----- 11 guide -----
Step "Reception guide on Desktop"
$guideDir = Join-Path $env:PUBLIC "TaxOps"
$guideHtml = Join-Path $guideDir "Reception_Setup_Guide.html"
try {
    Write-ReceptionGuideHtml $guideHtml
    $pubDesktop = [Environment]::GetFolderPath("CommonDesktopDirectory")
    if (-not $pubDesktop) { $pubDesktop = Join-Path $env:PUBLIC "Desktop" }
    $guideLnk = Join-Path $pubDesktop "TaxOps Reception Guide.lnk"
    $w = New-Object -ComObject WScript.Shell
    $sc = $w.CreateShortcut($guideLnk)
    $sc.TargetPath = $guideHtml
    $sc.Description = "TaxOps reception workstation quick guide"
    $sc.Save()
    Good "Guide: $guideHtml"
    Good "Desktop shortcut: $guideLnk"
} catch {
    Warn "Guide shortcut skipped: $($_.Exception.Message)"
}

# ========== DONE ==========
Write-Host ""
Say "========================================================" "DarkRed"
Say ("  DONE   OK={0}  WARN={1}  FAIL={2}" -f $script:ok, $script:warn, $script:fail) $(if ($script:fail) { "Yellow" } else { "Green" })
Say "========================================================" "DarkRed"
Write-Host ""
Say "  How to verify:" "White"
Say "    1. Double-click desktop 'Tax Log'  -> http://$Hostname/" "Gray"
Say "    2. Sign in and confirm the site loads (http only)" "Gray"
Say "    3. Do a test intake / ask for a test label print" "Gray"
Say "    4. On reception: T:\scan_agent_wizard.bat  (must show com_sta_v1)" "Gray"
Say "    5. After intake, choose Yes on Scan client documents now?" "Gray"
Say "    6. Open 'TaxOps Reception Guide' on the desktop anytime" "Gray"
Write-Host ""
if ($script:fail -gt 0) {
    Say "  Some steps failed. Fix those items, then re-run:" "Yellow"
    Say "    \\Xcel-server\taxops\setup_reception_pc.bat" "Yellow"
    Say "  Or Scan Agent only:" "Yellow"
    Say "    \\Xcel-server\taxops\scan_agent_wizard.bat" "Yellow"
} else {
    Say "  Reception PC is ready (Tax Log + labels + Epson + Scan Agent)." "Green"
}

$open = Read-Host "  Open Tax Log in the browser now? (Y/n)"
if ($open -notmatch '^[nN]') {
    try { Start-Process ("http://{0}/" -f $Hostname) } catch {}
}

Write-Host ""
Say "  This window stays open. Press Enter to close." "DarkGray"
Read-Host | Out-Null
