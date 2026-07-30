#Requires -Version 5.1
<#
.SYNOPSIS
  One-shot diagnostic for TaxOps reception: print relay (8765) + scan agent (8766).

.DESCRIPTION
  Run ON the reception PC (printer + Epson attached):
    \\Xcel-server\taxops\diagnose_reception_relays.bat

  - Collects environment, ports, services, Python, packages, tokens (masked)
  - Ensures print relay is listening + /health OK (starts it if down)
  - Ensures scan agent is listening + /health shows com_sta_v4 (starts it if down)
  - Writes a full report to C:\TaxOps\diagnostics\ and opens it

.PARAMETER ShareRoot
  Folder that contains taxops\ and start_*.bat (default: script directory).

.PARAMETER StartMissing
  Attempt to start print/scan relays if they are down (default: true).

.PARAMETER NoStart
  Diagnose only - do not start anything.

.PARAMETER OpenLog
  Open the report in Notepad when finished (default: true).
#>
param(
    [string]$ShareRoot = "",
    [switch]$NoStart,
    [switch]$NoOpenLog,
    [int]$PrintPort = 8765,
    [int]$ScanPort = 8766,
    [string]$PrinterName = "4BARCODE 4B-2054A"
)

$ErrorActionPreference = "Continue"
$StartMissing = -not $NoStart
$OpenLog = -not $NoOpenLog
# ---------- paths / log ----------
$scriptPath = $MyInvocation.MyCommand.Path
if (-not $scriptPath) { $scriptPath = "\\Xcel-server\taxops\diagnose_reception_relays.ps1" }
if (-not $ShareRoot) {
    $ShareRoot = Split-Path -Parent $scriptPath
}
$ShareRoot = $ShareRoot.Trim().TrimEnd('\')
if ($ShareRoot -match '[<>\|\?\*"]') {
    Write-Host "FATAL: Illegal characters in ShareRoot: $ShareRoot" -ForegroundColor Red
    exit 3
}

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$diagRoot = "C:\TaxOps\diagnostics"
if (-not (Test-Path -LiteralPath $diagRoot)) {
    New-Item -ItemType Directory -Path $diagRoot -Force | Out-Null
}
$logPath = Join-Path $diagRoot ("reception_relays_{0}.txt" -f $stamp)
$latestPath = Join-Path $diagRoot "reception_relays_LATEST.txt"

$script:fail = 0
$script:warn = 0
$script:ok = 0
$script:lines = New-Object System.Collections.Generic.List[string]

function L([string]$Msg) {
    $script:lines.Add($Msg)
    Write-Host $Msg
}
function Section([string]$Title) {
    L ""
    L ("======== {0} ========" -f $Title)
}
function Ok([string]$Msg) {
    $script:ok++
    L ("  [OK]   {0}" -f $Msg)
}
function Warn([string]$Msg) {
    $script:warn++
    L ("  [WARN] {0}" -f $Msg)
}
function Bad([string]$Msg) {
    $script:fail++
    L ("  [FAIL] {0}" -f $Msg)
}
function Info([string]$Msg) { L ("  [..]   {0}" -f $Msg) }
function Mask([string]$s) {
    if (-not $s) { return "(empty)" }
    if ($s.Length -le 8) { return ("***len={0}***" -f $s.Length) }
    return ("{0}...{1} (len={2})" -f $s.Substring(0, 4), $s.Substring($s.Length - 4), $s.Length)
}
function Save-Log {
    $text = ($script:lines -join "`r`n") + "`r`n"
    Set-Content -LiteralPath $logPath -Value $text -Encoding UTF8
    Set-Content -LiteralPath $latestPath -Value $text -Encoding UTF8
}

function Get-DotEnv([string]$Path, [string]$Key) {
    if (-not (Test-Path -LiteralPath $Path)) { return "" }
    foreach ($line in Get-Content -LiteralPath $Path -EA SilentlyContinue) {
        if ($line -match "^\s*#" -or $line -notmatch "=") { continue }
        $p = $line.Split("=", 2)
        if ($p[0].Trim() -eq $Key) { return $p[1].Trim().Trim('"').Trim("'") }
    }
    return ""
}

function Find-Python {
    $candidates = @(
        "$env:LOCALAPPDATA\Programs\Python\Python314\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
        "C:\TaxOps\taxops\.venv\Scripts\python.exe",
        (Join-Path $ShareRoot "taxops\.venv\Scripts\python.exe")
    )
    foreach ($c in $candidates) {
        if ($c -and (Test-Path -LiteralPath $c) -and ($c -notmatch "WindowsApps")) { return $c }
    }
    try {
        $py = & py -3 -c "import sys; print(sys.executable)" 2>$null
        if ($py -and (Test-Path $py) -and ($py -notmatch "WindowsApps")) { return $py.Trim() }
    } catch {}
    $cmd = Get-Command python.exe -EA SilentlyContinue
    if ($cmd -and $cmd.Source -notmatch "WindowsApps") { return $cmd.Source }
    return $null
}

function Get-Listeners([int]$Port) {
    $rows = @()
    try {
        $rows = @(Get-NetTCPConnection -LocalPort $Port -State Listen -EA SilentlyContinue)
    } catch {}
    if (-not $rows) {
        # fallback netstat parse
        $raw = netstat -ano 2>$null | Select-String (":{0}\s+.*LISTENING" -f $Port)
        foreach ($m in $raw) {
            if ($m.Line -match "\s+(\d+)\s*$") {
                $rows += [pscustomobject]@{ OwningProcess = [int]$Matches[1] }
            }
        }
    }
    return $rows
}

function Invoke-JsonGet([string]$Url, [hashtable]$Headers, [int]$TimeoutSec = 5) {
    try {
        $r = Invoke-WebRequest -Uri $Url -Headers $Headers -UseBasicParsing -TimeoutSec $TimeoutSec
        $body = $r.Content
        $json = $null
        try { $json = $body | ConvertFrom-Json } catch {}
        return @{ ok = $true; status = [int]$r.StatusCode; body = $body; json = $json; error = "" }
    } catch {
        $msg = $_.Exception.Message
        $status = 0
        if ($_.Exception.Response) {
            try { $status = [int]$_.Exception.Response.StatusCode } catch {}
        }
        return @{ ok = $false; status = $status; body = ""; json = $null; error = $msg }
    }
}

function Stop-Port([int]$Port) {
    $killed = @()
    foreach ($row in (Get-Listeners $Port)) {
        $procId = [int]$row.OwningProcess
        if ($procId -le 4) { continue }
        try {
            Stop-Process -Id $procId -Force -EA Stop
            $killed += $procId
            Info ("Killed PID {0} on port {1}" -f $procId, $Port)
        } catch {
            Warn ("Could not kill PID {0} on {1}: {2}" -f $procId, $Port, $_.Exception.Message)
        }
    }
    if ($killed.Count) { Start-Sleep -Seconds 2 }
    return $killed
}

# ---------- header ----------
L "TaxOps reception relay diagnostic"
L ("Started: {0}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
L ("Log file: {0}" -f $logPath)
L ("ShareRoot: {0}" -f $ShareRoot)
L ("StartMissing: {0}" -f $StartMissing)

Section "Machine"
try {
    L ("  Computer: {0}" -f $env:COMPUTERNAME)
    L ("  User:     {0}\{1}" -f $env:USERDOMAIN, $env:USERNAME)
    L ("  OS:       {0}" -f [Environment]::OSVersion.VersionString)
    L ("  PS:       {0}" -f $PSVersionTable.PSVersion)
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $prin = New-Object Security.Principal.WindowsPrincipal($id)
    $isAdmin = $prin.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    L ("  Admin:    {0}" -f $isAdmin)
    try {
        $ips = Get-NetIPAddress -AddressFamily IPv4 -EA SilentlyContinue |
            Where-Object { $_.IPAddress -notlike "127.*" } |
            Select-Object -ExpandProperty IPAddress
        L ("  IPv4:     {0}" -f (($ips | Select-Object -Unique) -join ", "))
    } catch {
        L ("  IPv4:     (unavailable) {0}" -f $_.Exception.Message)
    }
} catch {
    Bad ("Machine info failed: {0}" -f $_.Exception.Message)
}

Section "Share / scripts present"
$needFiles = @(
    "taxops\filetrack\relay\server.py",
    "taxops\scan_agent\server.py",
    "start_print_relay.bat",
    "start_scan_agent.bat",
    "GO_SCAN_AGENT.bat",
    "sync_scan_agent_local.ps1"
)
foreach ($rel in $needFiles) {
    $p = Join-Path $ShareRoot $rel
    if (Test-Path -LiteralPath $p) { Ok $rel }
    else { Bad ("Missing: {0}" -f $p) }
}

Section "Python"
$py = Find-Python
if (-not $py) {
    Bad "No usable python.exe (avoid Windows Store stub)"
} else {
    Ok ("Python: {0}" -f $py)
    try {
        $ver = & $py -c 'import sys; print("{0}.{1}.{2}".format(*sys.version_info[:3]))' 2>&1
        Info ("Version: {0}" -f $ver)
    } catch {
        Warn ("Version probe failed: {0}" -f $_.Exception.Message)
    }
    foreach ($mod in @("flask", "win32print", "win32com.client", "pythoncom", "PIL")) {
        & $py -c "import $mod" 2>$null
        if ($LASTEXITCODE -eq 0) { Ok ("import {0}" -f $mod) }
        else { Bad ("import {0} FAILED - pip install flask pywin32 Pillow" -f $mod) }
    }
}

# ---------- tokens ----------
Section "Tokens (masked)"
$envTaxops = Join-Path $ShareRoot "taxops\.env"
$tokenPrint = $env:FILETRACK_RELAY_TOKEN
if (-not $tokenPrint) { $tokenPrint = Get-DotEnv $envTaxops "FILETRACK_RELAY_TOKEN" }
if (-not $tokenPrint) { $tokenPrint = "zv8z42FQcfufRAvQDrJMXMXgb7Mpttdq" }  # start_print_relay.bat default

$tokenScan = $env:SCAN_AGENT_TOKEN
if (-not $tokenScan) { $tokenScan = Get-DotEnv (Join-Path "C:\TaxOps\ScanAgent" "token.env") "SCAN_AGENT_TOKEN" }
if (-not $tokenScan) { $tokenScan = Get-DotEnv $envTaxops "SCAN_AGENT_TOKEN" }

Info ("FILETRACK_RELAY_TOKEN: {0}" -f (Mask $tokenPrint))
if ($tokenScan) { Info ("SCAN_AGENT_TOKEN:      {0}" -f (Mask $tokenScan)) }
else { Bad "SCAN_AGENT_TOKEN missing - run scan_agent_wizard.bat once" }

$printerEnv = $env:FILETRACK_PRINTER
if (-not $printerEnv) { $printerEnv = Get-DotEnv $envTaxops "FILETRACK_PRINTER" }
if (-not $printerEnv) { $printerEnv = $PrinterName }
Info ("FILETRACK_PRINTER:     {0}" -f $printerEnv)

# ============================================================
# PRINT RELAY
# ============================================================
Section "PRINT RELAY (TCP $PrintPort)"

$printSvc = Get-Service -Name "FiletrackRelay" -EA SilentlyContinue
if ($printSvc) {
    Info ("Service FiletrackRelay: {0}" -f $printSvc.Status)
    if ($printSvc.Status -ne "Running" -and $StartMissing) {
        try {
            Start-Service FiletrackRelay -EA Stop
            Start-Sleep 2
            Ok "Started FiletrackRelay service"
        } catch {
            Warn ("Could not start FiletrackRelay service: {0}" -f $_.Exception.Message)
        }
    }
} else {
    Info "No FiletrackRelay Windows service (console/startup bat is OK)"
}

$printHealthUrl = "http://127.0.0.1:{0}/health" -f $PrintPort
$h = Invoke-JsonGet $printHealthUrl @{} 4
if ($h.ok) {
    Ok ("Print /health HTTP {0}" -f $h.status)
    Info ("Body: {0}" -f $h.body)
    if ($h.json) {
        if ($h.json.printer_found) { Ok ("Printer found: {0}" -f $h.json.printer_configured) }
        else {
            Warn ("Printer NOT found (configured={0})" -f $h.json.printer_configured)
            if ($h.json.printer_check_error) { Info ("printer_check_error: {0}" -f $h.json.printer_check_error) }
        }
    }
} else {
    Bad ("Print /health failed: {0}" -f $h.error)
    $listeners = Get-Listeners $PrintPort
    if ($listeners.Count) {
        Info ("Port {0} IS listening (PIDs: {1}) but health failed - wrong process?" -f `
            $PrintPort, (($listeners | ForEach-Object { $_.OwningProcess }) -join ","))
    } else {
        Info ("Port {0} not listening" -f $PrintPort)
    }

    if ($StartMissing) {
        Info "Attempting to start print relay..."
        $printBat = Join-Path $ShareRoot "start_print_relay.bat"
        $appDir = Join-Path $ShareRoot "taxops"
        if ($py -and (Test-Path (Join-Path $appDir "filetrack\relay\server.py"))) {
            $env:FILETRACK_PRINTER = $printerEnv
            $env:FILETRACK_RELAY_TOKEN = $tokenPrint
            $env:FILETRACK_RELAY_HOST = "0.0.0.0"
            $env:FILETRACK_RELAY_PORT = "$PrintPort"
            $outLog = Join-Path $diagRoot ("print_relay_stdout_{0}.txt" -f $stamp)
            $errLog = Join-Path $diagRoot ("print_relay_stderr_{0}.txt" -f $stamp)
            $arg = "-m filetrack.relay.server --port $PrintPort"
            try {
                $proc = Start-Process -FilePath $py `
                    -ArgumentList $arg `
                    -WorkingDirectory $appDir `
                    -RedirectStandardOutput $outLog `
                    -RedirectStandardError $errLog `
                    -WindowStyle Minimized `
                    -PassThru
                Info ("Started PID {0} (logs {1} / {2})" -f $proc.Id, $outLog, $errLog)
            } catch {
                Bad ("Start-Process print relay failed: {0}" -f $_.Exception.Message)
                if (Test-Path $printBat) {
                    Info "Fallback: start_print_relay.bat visible window"
                    Start-Process -FilePath $printBat -WorkingDirectory $ShareRoot
                }
            }
        } elseif (Test-Path $printBat) {
            Start-Process -FilePath $printBat -WorkingDirectory $ShareRoot
            Info "Launched start_print_relay.bat"
        } else {
            Bad "Cannot start print relay - no python path / bat"
        }

        for ($i = 1; $i -le 20; $i++) {
            Start-Sleep -Seconds 1
            $h = Invoke-JsonGet $printHealthUrl @{} 3
            if ($h.ok) { break }
            if ($i % 5 -eq 0) { Info ("Waiting print health... {0}/20" -f $i) }
        }
        if ($h.ok) {
            Ok ("Print relay UP after start - {0}" -f $h.body)
        } else {
            Bad ("Print relay still down after start: {0}" -f $h.error)
            $errLog2 = Get-ChildItem $diagRoot -Filter "print_relay_stderr_*.txt" -EA SilentlyContinue |
                Sort-Object LastWriteTime -Descending | Select-Object -First 1
            if ($errLog2) {
                Info ("--- stderr tail ({0}) ---" -f $errLog2.FullName)
                Get-Content $errLog2.FullName -Tail 40 -EA SilentlyContinue | ForEach-Object { L ("    {0}" -f $_) }
            }
        }
    }
}

# Printer list via Python (extra detail)
if ($py) {
    Info "Enumerating Windows printers via win32print..."
    try {
        $plist = & $py -c 'import win32print; print(chr(10).join(p[2] for p in win32print.EnumPrinters(6)))' 2>&1
        if ($LASTEXITCODE -eq 0) {
            $names = @($plist -split "`r?`n" | Where-Object { $_ })
            Info ("Printer count: {0}" -f $names.Count)
            foreach ($n in $names) { L ("    - {0}" -f $n) }
            if ($names -contains $printerEnv) { Ok ("Configured printer present: {0}" -f $printerEnv) }
            else { Warn ("Configured printer '{0}' not in EnumPrinters list" -f $printerEnv) }
        } else {
            Warn ("win32print EnumPrinters failed: {0}" -f $plist)
        }
    } catch {
        Warn ("Printer enum exception: {0}" -f $_.Exception.Message)
    }
}

# ============================================================
# SCAN AGENT
# ============================================================
Section "SCAN AGENT (TCP $ScanPort)"

$scanLocal = "C:\TaxOps\ScanAgent\app\scan_agent\server.py"
$scanApp = "C:\TaxOps\ScanAgent\app"
if (Test-Path -LiteralPath $scanLocal) {
    Ok ("Local server.py: {0}" -f $scanLocal)
    $revLine = Select-String -Path $scanLocal -Pattern "REQUIRED_CODE_REV" | Select-Object -First 1
    if ($revLine) { Info ("Local: {0}" -f $revLine.Line.Trim()) }
    if (Select-String -Path $scanLocal -Pattern "com_sta_v4" -SimpleMatch -Quiet) {
        Ok "Local copy contains com_sta_v4"
    } else {
        Bad "Local copy missing com_sta_v4 - re-run GO_SCAN_AGENT.bat to re-sync"
    }
} else {
    Warn "No local scan_agent copy yet (C:\TaxOps\ScanAgent\app\...) - will sync on start"
}

$shareScan = Join-Path $ShareRoot "taxops\scan_agent\server.py"
if (Test-Path -LiteralPath $shareScan) {
    if (Select-String -Path $shareScan -Pattern "com_sta_v4" -SimpleMatch -Quiet) {
        Ok "Share server.py has com_sta_v4"
    } else {
        Bad "Share server.py missing com_sta_v4"
    }
}

$scanSvc = Get-Service -Name "ScanAgent" -EA SilentlyContinue
if ($scanSvc) {
    Info ("Service ScanAgent: {0}" -f $scanSvc.Status)
} else {
    Info "No ScanAgent Windows service (visible GO_SCAN_AGENT window is OK)"
}

$scanHeaders = @{}
if ($tokenScan) { $scanHeaders["X-Scan-Agent-Token"] = $tokenScan }

$scanHealthUrl = "http://127.0.0.1:{0}/health" -f $ScanPort
$sh = Invoke-JsonGet $scanHealthUrl $scanHeaders 5
if ($sh.ok) {
    Ok ("Scan /health HTTP {0}" -f $sh.status)
    Info ("Body: {0}" -f $sh.body)
    $rev = $null
    if ($sh.json) { $rev = [string]$sh.json.code_rev }
    if ($rev -eq "com_sta_v4") { Ok "Running build com_sta_v4" }
    elseif ($rev) { Bad ("Running OLD build code_rev={0} - need com_sta_v4; close agent windows + GO_SCAN_AGENT.bat" -f $rev) }
    else { Bad "No code_rev in health - very old agent" }

    if ($sh.json -and $sh.json.com_sta -eq $true) { Ok "com_sta=true (STA pump initialized)" }
    elseif ($sh.json -and ($sh.json.PSObject.Properties.Name -contains "com_sta")) {
        Bad ("com_sta={0} error={1}" -f $sh.json.com_sta, $sh.json.com_error)
    }

    if ($sh.body -match "CoInitialize") {
        Bad "Health body still mentions CoInitialize - old process or COM still broken"
    }
} else {
    Bad ("Scan /health failed: {0}" -f $sh.error)
    if ($sh.status -eq 401) {
        Bad "Unauthorized - SCAN_AGENT_TOKEN mismatch vs running agent"
    }
    $listeners = Get-Listeners $ScanPort
    if ($listeners.Count) {
        Info ("Port {0} listening PIDs: {1}" -f $ScanPort, (($listeners | ForEach-Object OwningProcess) -join ","))
    } else {
        Info ("Port {0} not listening" -f $ScanPort)
    }

    if ($StartMissing) {
        Info "Attempting to start scan agent via GO_SCAN_AGENT / start_scan_agent..."
        $goBat = Join-Path $ShareRoot "GO_SCAN_AGENT.bat"
        $startBat = Join-Path $ShareRoot "start_scan_agent.bat"
        # Kill stale first so new code can bind
        [void](Stop-Port $ScanPort)
        if (Test-Path -LiteralPath $startBat) {
            try {
                Start-Process -FilePath "cmd.exe" `
                    -ArgumentList @("/c", "call `"$startBat`"") `
                    -WorkingDirectory $ShareRoot
                Info "Launched start_scan_agent.bat (visible window - leave it open)"
            } catch {
                Bad ("Failed to launch start_scan_agent.bat: {0}" -f $_.Exception.Message)
            }
        } elseif (Test-Path -LiteralPath $goBat) {
            Start-Process -FilePath $goBat -WorkingDirectory $ShareRoot
        } else {
            Bad "start_scan_agent.bat / GO_SCAN_AGENT.bat missing"
        }

        for ($i = 1; $i -le 45; $i++) {
            Start-Sleep -Seconds 1
            $sh = Invoke-JsonGet $scanHealthUrl $scanHeaders 3
            if ($sh.ok -and $sh.body -match "com_sta_v4") { break }
            if ($i % 5 -eq 0) {
                Info ("Waiting scan health com_sta_v4... {0}/45  last={1}" -f $i, $(if ($sh.ok) { "HTTP OK" } else { $sh.error }))
            }
        }
        if ($sh.ok -and $sh.body -match "com_sta_v4") {
            Ok ("Scan agent UP - {0}" -f $sh.body)
        } elseif ($sh.ok) {
            Bad ("Scan agent answered but not com_sta_v4: {0}" -f $sh.body)
        } else {
            Bad ("Scan agent still down: {0}" -f $sh.error)
            Info "Look at the TaxOps Scan Agent window for [FAIL] lines (illegal path / no module)."
            if (Test-Path $scanApp) {
                Info ("Local app dir listing ({0}):" -f $scanApp)
                Get-ChildItem $scanApp -EA SilentlyContinue | ForEach-Object { L ("    {0}" -f $_.Name) }
                $agentDir = Join-Path $scanApp "scan_agent"
                if (Test-Path $agentDir) {
                    Get-ChildItem $agentDir -EA SilentlyContinue | ForEach-Object { L ("    scan_agent\{0}" -f $_.Name) }
                }
            }
        }
    }
}

# Optional WIA probe (short)
if ($sh.ok -and $tokenScan) {
    Section "SCAN WIA probe (?wia=1, 6s cap)"
    $wiaUrl = "http://127.0.0.1:{0}/health?wia=1" -f $ScanPort
    $wh = Invoke-JsonGet $wiaUrl $scanHeaders 8
    if ($wh.ok) {
        Info ("WIA body: {0}" -f $wh.body)
        if ($wh.body -match "CoInitialize") {
            Bad "WIA path still reports CoInitialize"
        } elseif ($wh.json -and ($wh.json.scanner_found -or $wh.json.scanner_ok)) {
            Ok ("Scanner found: {0}" -f $wh.json.scanner_names)
        } else {
            Warn "Agent OK but no WIA scanner - power Epson, fix USB (Device Manager), Epson Scan 2"
            if ($wh.json.scanner_error) { Info ("scanner_error: {0}" -f $wh.json.scanner_error) }
            if ($wh.json.tips) {
                foreach ($t in @($wh.json.tips)) { Info ("tip: {0}" -f $t) }
            }
        }
    } else {
        Warn ("WIA health failed/timeout: {0}" -f $wh.error)
    }
}

# Epson PnP snapshot
Section "Epson Device Manager snapshot"
try {
    $devs = Get-PnpDevice -EA SilentlyContinue | Where-Object {
        $_.FriendlyName -match "EPSON|Epson|ES-500|ES-400|WIA"
    }
    if (-not $devs) {
        Warn "No Epson/WIA-named PnP devices"
    } else {
        foreach ($d in $devs) {
            $line = "  {0} | Status={1} | Class={2} | Problem={3}" -f `
                $d.FriendlyName, $d.Status, $d.Class, $d.Problem
            if ($d.Status -eq "OK") { Ok $line.Trim() } else { Warn $line.Trim() }
        }
    }
} catch {
    Warn ("Get-PnpDevice failed: {0}" -f $_.Exception.Message)
}

# Firewall
Section "Firewall rules"
foreach ($name in @("TaxOps Print Relay", "TaxOps Scan Agent", "ScanAgent", "FiletrackRelay")) {
    $rule = Get-NetFirewallRule -DisplayName $name -EA SilentlyContinue | Select-Object -First 1
    if ($rule) {
        Info ("{0}: Enabled={1} Action={2}" -f $name, $rule.Enabled, $rule.Action)
    }
}
try {
    $ports = Get-NetFirewallPortFilter -EA SilentlyContinue |
        Where-Object { $_.LocalPort -in @("$PrintPort", "$ScanPort") }
    # Just note presence via rules is enough; detailed filter can be noisy
} catch {}

# Import check for scan_agent with local PYTHONPATH
Section "scan_agent import check (local PYTHONPATH)"
if ($py -and (Test-Path $scanApp)) {
    $env:PYTHONPATH = $scanApp
    $imp = & $py -c "import scan_agent.server as s; print(s.REQUIRED_CODE_REV)" 2>&1
    if ($LASTEXITCODE -eq 0) { Ok ("import scan_agent.server -> {0}" -f ($imp | Out-String).Trim()) }
    else {
        Bad ("import scan_agent failed: {0}" -f ($imp | Out-String).Trim())
        Info "This is the 'No module named scan_agent' failure mode - sync/PYTHONPATH broken."
    }
} else {
    Warn "Skip import check - no local app or python"
}

# Summary
Section "SUMMARY"
L ("  OK={0}  WARN={1}  FAIL={2}" -f $script:ok, $script:warn, $script:fail)
L ("  Report: {0}" -f $logPath)
L ("  Latest: {0}" -f $latestPath)
if ($script:fail -eq 0) {
    L "  RESULT: PASS (relays reachable; check WARNs for scanner/printer hardware)"
} else {
    L "  RESULT: FAIL - read [FAIL] lines above; leave relay windows open if started"
    L "  Print start:  start_print_relay.bat"
    L "  Scan start:   GO_SCAN_AGENT.bat   (must show READY com_sta_v4)"
}

Save-Log
Write-Host ""
Write-Host ("Full report saved to:`n  {0}" -f $logPath) -ForegroundColor Cyan

if ($OpenLog) {
    try { Start-Process notepad.exe -ArgumentList $logPath } catch {}
}

if ($script:fail -gt 0) { exit 1 }
exit 0
