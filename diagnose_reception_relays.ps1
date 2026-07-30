#Requires -Version 5.1
<#
.SYNOPSIS
  One-shot diagnostic for TaxOps reception: print relay (8765) + scan agent (8766).

.DESCRIPTION
  Run ON the reception PC:
    \\Xcel-server\taxops\diagnose_reception_relays.bat

  Writes C:\TaxOps\diagnostics\reception_relays_*.txt and opens Notepad.

  Milestone 1 outcomes:
  - Healthy agents already up  -> FAIL=0 WARN=0 RESULT: PASS
  - Agents were down, started  -> FAIL=0 WARN=2 RESULT: PASS
  - Epson unplugged            -> exactly one FAIL naming the scanner
#>
param(
    [string]$ShareRoot = "",
    [switch]$NoStart,
    [switch]$NoOpenLog,
    [int]$PrintPort = 8765,
    [int]$ScanPort = 8766,
    [string]$PrinterName = "4BARCODE 4B-2054A",
    [string]$ExpectedEpson = "EPSON ES-500"
)

$ErrorActionPreference = "Continue"
$StartMissing = -not $NoStart
$OpenLog = -not $NoOpenLog

$scriptPath = $MyInvocation.MyCommand.Path
if (-not $scriptPath) { $scriptPath = "\\Xcel-server\taxops\diagnose_reception_relays.ps1" }
if (-not $ShareRoot) { $ShareRoot = Split-Path -Parent $scriptPath }
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
function Ok([string]$Msg) { $script:ok++; L ("  [OK]   {0}" -f $Msg) }
function Warn([string]$Msg) { $script:warn++; L ("  [WARN] {0}" -f $Msg) }
function Bad([string]$Msg) { $script:fail++; L ("  [FAIL] {0}" -f $Msg) }
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

function Test-IsAdmin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Find-Python {
    $candidates = @(
        "$env:LOCALAPPDATA\Programs\Python\Python314\python.exe",
        "C:\Users\Windows 10\AppData\Local\Programs\Python\Python314\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe",
        "C:\TaxOps\taxops\.venv\Scripts\python.exe",
        (Join-Path $ShareRoot "taxops\.venv\Scripts\python.exe")
    )
    foreach ($c in $candidates) {
        if ($c -and (Test-Path -LiteralPath $c) -and ($c -notmatch "WindowsApps")) { return $c }
    }
    try {
        $out = & py -3 -c "import sys; print(sys.executable)" 2>$null
        if ($out -and (Test-Path $out.Trim()) -and ($out -notmatch "WindowsApps")) { return $out.Trim() }
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
        $raw = netstat -ano 2>$null | Select-String (":{0}\s+.*LISTENING" -f $Port)
        foreach ($m in $raw) {
            if ($m.Line -match "\s+(\d+)\s*$") {
                $rows += [pscustomobject]@{ OwningProcess = [int]$Matches[1] }
            }
        }
    }
    return $rows
}

function Get-ProcessMeta([int]$ProcId) {
    try {
        $p = Get-Process -Id $ProcId -EA Stop
        $path = ""
        try { $path = $p.Path } catch {}
        if (-not $path) {
            try {
                $path = (Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $ProcId) -EA SilentlyContinue).ExecutablePath
            } catch {}
        }
        return [pscustomobject]@{
            Id        = $ProcId
            Name      = $p.ProcessName
            Path      = $path
            StartTime = $p.StartTime
        }
    } catch {
        return [pscustomobject]@{ Id = $ProcId; Name = "?"; Path = ""; StartTime = $null }
    }
}

function Stop-Port([int]$Port) {
    $killed = @()
    foreach ($row in (Get-Listeners $Port)) {
        $procId = [int]$row.OwningProcess
        if ($procId -le 4) { continue }
        $meta = Get-ProcessMeta $procId
        Warn ("stale listener on {0} killed - PID={1} name={2} path={3} started={4}" -f `
            $Port, $meta.Id, $meta.Name, $meta.Path, $meta.StartTime)
        try {
            Stop-Process -Id $procId -Force -EA Stop
            $killed += $procId
        } catch {
            Warn ("Could not kill PID {0} on {1}: {2}" -f $procId, $Port, $_.Exception.Message)
        }
    }
    if ($killed.Count) { Start-Sleep -Seconds 2 }
    return $killed
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

# ---------- header ----------
L "TaxOps reception relay diagnostic"
L ("Started: {0}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
L ("Log file: {0}" -f $logPath)
L ("ShareRoot: {0}" -f $ShareRoot)
L ("StartMissing: {0}" -f $StartMissing)

Section "Machine"
$isAdmin = Test-IsAdmin
try {
    L ("  Computer: {0}" -f $env:COMPUTERNAME)
    L ("  User:     {0}\{1}" -f $env:USERDOMAIN, $env:USERNAME)
    L ("  OS:       {0}" -f [Environment]::OSVersion.VersionString)
    L ("  PS:       {0}" -f $PSVersionTable.PSVersion)
    L ("  Admin:    {0}" -f $isAdmin)
    try {
        $ips = Get-NetIPAddress -AddressFamily IPv4 -EA SilentlyContinue |
            Where-Object { $_.IPAddress -notlike "127.*" -and $_.IPAddress -notlike "169.254.*" } |
            Select-Object -ExpandProperty IPAddress
        L ("  IPv4:     {0}" -f (($ips | Select-Object -Unique) -join ", "))
    } catch {
        L ("  IPv4:     (unavailable)")
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
    # 1c: use --version (no fragile -c quoting)
    $verOut = & $py --version 2>&1 | Out-String
    $verOut = $verOut.Trim()
    Info ("Version: {0}" -f $verOut)
    if ($verOut -match 'Python\s+(\d+)\.(\d+)') {
        $maj = [int]$Matches[1]; $min = [int]$Matches[2]
        if ($maj -eq 3 -and $min -eq 14) {
            Ok "Python 3.14 required major.minor"
        } else {
            Bad ("Python {0}.{1} found - reception requires 3.14" -f $maj, $min)
        }
    } else {
        Bad ("Could not parse python --version output: {0}" -f $verOut)
    }
    foreach ($mod in @("flask", "win32print", "win32com.client", "pythoncom", "PIL")) {
        & $py -c "import $mod" 2>$null
        if ($LASTEXITCODE -eq 0) { Ok ("import {0}" -f $mod) }
        else { Bad ("import {0} FAILED - pip install flask pywin32 Pillow" -f $mod) }
    }
}

Section "Tokens (masked)"
$envTaxops = Join-Path $ShareRoot "taxops\.env"
$tokenPrint = $env:FILETRACK_RELAY_TOKEN
if (-not $tokenPrint) { $tokenPrint = Get-DotEnv $envTaxops "FILETRACK_RELAY_TOKEN" }
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
} else {
    Info "No FiletrackRelay Windows service (console/startup bat is OK until M2)"
}

$printHealthUrl = "http://127.0.0.1:{0}/health" -f $PrintPort
$printStartedByUs = $false
$h = Invoke-JsonGet $printHealthUrl @{} 4
$printFirstOk = [bool]$h.ok

if ($printFirstOk) {
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
    # 1b: do not FAIL yet - may start successfully
    Info ("Print /health first probe failed: {0}" -f $h.error)
    $listeners = Get-Listeners $PrintPort
    if ($listeners.Count) {
        foreach ($row in $listeners) {
            $meta = Get-ProcessMeta ([int]$row.OwningProcess)
            Warn ("stale listener on {0} (health failed) - PID={1} name={2} path={3} started={4}" -f `
                $PrintPort, $meta.Id, $meta.Name, $meta.Path, $meta.StartTime)
        }
    } else {
        Info ("Port {0} not listening" -f $PrintPort)
    }

    if ($StartMissing) {
        Info "Attempting to start print relay..."
        $printBat = Join-Path $ShareRoot "start_print_relay.bat"
        $appDir = Join-Path $ShareRoot "taxops"
        if ($py -and (Test-Path (Join-Path $appDir "filetrack\relay\server.py"))) {
            if ($tokenPrint) { $env:FILETRACK_RELAY_TOKEN = $tokenPrint }
            $env:FILETRACK_PRINTER = $printerEnv
            $env:FILETRACK_RELAY_HOST = "0.0.0.0"
            $env:FILETRACK_RELAY_PORT = "$PrintPort"
            $outLog = Join-Path $diagRoot ("print_relay_stdout_{0}.txt" -f $stamp)
            $errLog = Join-Path $diagRoot ("print_relay_stderr_{0}.txt" -f $stamp)
            try {
                $proc = Start-Process -FilePath $py `
                    -ArgumentList @("-m", "filetrack.relay.server", "--port", "$PrintPort") `
                    -WorkingDirectory $appDir `
                    -RedirectStandardOutput $outLog `
                    -RedirectStandardError $errLog `
                    -WindowStyle Minimized `
                    -PassThru
                Info ("Started PID {0}" -f $proc.Id)
                $printStartedByUs = $true
            } catch {
                if (Test-Path $printBat) {
                    Start-Process -FilePath $printBat -WorkingDirectory $ShareRoot
                    $printStartedByUs = $true
                } else {
                    Bad ("Cannot start print relay: {0}" -f $_.Exception.Message)
                }
            }
        } elseif (Test-Path $printBat) {
            Start-Process -FilePath $printBat -WorkingDirectory $ShareRoot
            $printStartedByUs = $true
        } else {
            Bad "Cannot start print relay - no python path / bat"
        }

        for ($i = 1; $i -le 20; $i++) {
            Start-Sleep -Seconds 1
            $h = Invoke-JsonGet $printHealthUrl @{} 3
            if ($h.ok) { break }
        }
        if ($h.ok) {
            Warn "Print relay was not running; started by diagnostic"
            Info ("Body: {0}" -f $h.body)
        } else {
            Bad ("Print /health failed after start attempt: {0}" -f $h.error)
            $errLog2 = Get-ChildItem $diagRoot -Filter "print_relay_stderr_*.txt" -EA SilentlyContinue |
                Sort-Object LastWriteTime -Descending | Select-Object -First 1
            if ($errLog2) {
                Info ("--- stderr tail ({0}) ---" -f $errLog2.FullName)
                Get-Content $errLog2.FullName -Tail 40 -EA SilentlyContinue | ForEach-Object { L ("    {0}" -f $_) }
            }
        }
    } else {
        Bad ("Print /health failed (NoStart): {0}" -f $h.error)
    }
}

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
    if (Select-String -Path $scanLocal -Pattern "com_sta_v4" -SimpleMatch -Quiet) {
        Ok "Local copy contains com_sta_v4"
    } else {
        Bad "Local copy missing com_sta_v4 - re-run GO_SCAN_AGENT.bat to re-sync"
    }
} else {
    Warn "No local scan_agent copy yet - will sync on start"
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
    Info ("Service ScanAgent: {0} (session-0 service is NOT the interactive WIA path)" -f $scanSvc.Status)
}

$scanHeaders = @{}
if ($tokenScan) { $scanHeaders["X-Scan-Agent-Token"] = $tokenScan }

$scanHealthUrl = "http://127.0.0.1:{0}/health" -f $ScanPort
$sh = Invoke-JsonGet $scanHealthUrl $scanHeaders 5
$scanFirstOk = [bool]$sh.ok

if ($scanFirstOk) {
    Ok ("Scan /health HTTP {0}" -f $sh.status)
    Info ("Body: {0}" -f $sh.body)
    $rev = if ($sh.json) { [string]$sh.json.code_rev } else { "" }
    if ($rev -eq "com_sta_v4") { Ok "Running build com_sta_v4" }
    elseif ($rev) { Bad ("Running OLD build code_rev={0} - need com_sta_v4" -f $rev) }
    else { Bad "No code_rev in health - very old agent" }

    if ($sh.json -and $sh.json.com_sta -eq $true) { Ok "com_sta=true (STA pump initialized)" }
    elseif ($sh.json -and ($sh.json.PSObject.Properties.Name -contains "com_sta")) {
        Bad ("com_sta={0} error={1}" -f $sh.json.com_sta, $sh.json.com_error)
    }
    # Do NOT substring-match CoInitialize - com_detail success is "CoInitializeEx hr=None"
} else {
    Info ("Scan /health first probe failed: {0}" -f $sh.error)
    if ($sh.status -eq 401) {
        Bad "Unauthorized - SCAN_AGENT_TOKEN mismatch vs running agent"
    }
    $listeners = Get-Listeners $ScanPort
    if ($listeners.Count) {
        foreach ($row in $listeners) {
            $meta = Get-ProcessMeta ([int]$row.OwningProcess)
            Warn ("stale listener on {0} (health failed) - PID={1} name={2} path={3} started={4}" -f `
                $ScanPort, $meta.Id, $meta.Name, $meta.Path, $meta.StartTime)
        }
        if ($StartMissing) { [void](Stop-Port $ScanPort) }
    } else {
        Info ("Port {0} not listening" -f $ScanPort)
    }

    if ($StartMissing -and $sh.status -ne 401) {
        Info "Attempting to start scan agent..."
        $startBat = Join-Path $ShareRoot "start_scan_agent.bat"
        if (Test-Path -LiteralPath $startBat) {
            try {
                Start-Process -FilePath "cmd.exe" `
                    -ArgumentList @("/c", "call `"$startBat`"") `
                    -WorkingDirectory $ShareRoot
                Info "Launched start_scan_agent.bat"
            } catch {
                Bad ("Failed to launch start_scan_agent.bat: {0}" -f $_.Exception.Message)
            }
        } else {
            Bad "start_scan_agent.bat missing"
        }

        for ($i = 1; $i -le 45; $i++) {
            Start-Sleep -Seconds 1
            $sh = Invoke-JsonGet $scanHealthUrl $scanHeaders 3
            if ($sh.ok -and $sh.body -match "com_sta_v4") { break }
            if ($i % 5 -eq 0) {
                Info ("Waiting scan health com_sta_v4... {0}/45" -f $i)
            }
        }
        if ($sh.ok -and $sh.body -match "com_sta_v4") {
            Warn "Scan agent was not running; started by diagnostic"
            Info ("Body: {0}" -f $sh.body)
        } elseif ($sh.ok) {
            Bad ("Scan agent answered but not com_sta_v4: {0}" -f $sh.body)
        } else {
            Bad ("Scan /health failed after start attempt: {0}" -f $sh.error)
        }
    } elseif (-not $StartMissing) {
        Bad ("Scan /health failed (NoStart): {0}" -f $sh.error)
    }
}

# Optional WIA probe - field-based (1a)
$wiaFailScanner = $false
if ($sh.ok -and $tokenScan) {
    Section "SCAN WIA probe (?wia=1)"
    $wiaUrl = "http://127.0.0.1:{0}/health?wia=1" -f $ScanPort
    $wh = Invoke-JsonGet $wiaUrl $scanHeaders 8
    if ($wh.ok -and $wh.json) {
        $wia = $wh.json
        Info ("WIA body: {0}" -f $wh.body)
        # Success breadcrumb may contain the text CoInitializeEx - ignore string match.
        if ($wia.wia_probed -and $wia.com_sta -and $wia.scanner_ok -and $wia.scanner_found) {
            Ok ("WIA probe: {0}" -f $wia.scanner_names)
            $namesUpper = ([string]$wia.scanner_names).ToUpperInvariant()
            if ($ExpectedEpson -and ($namesUpper -notmatch [regex]::Escape($ExpectedEpson.ToUpperInvariant()))) {
                Warn ("com_sta OK but expected Epson '{0}' absent from scanner_names={1}" -f `
                    $ExpectedEpson, $wia.scanner_names)
            }
        } else {
            $detail = $wia.com_detail
            if (-not $detail) { $detail = $wia.scanner_error }
            if (-not $detail) { $detail = $wia.com_error }
            Bad ("WIA probe failed - scanner not OK (Epson ES-500WII). com_sta=$($wia.com_sta) scanner_ok=$($wia.scanner_ok) scanner_found=$($wia.scanner_found) detail=$detail")
            $wiaFailScanner = $true
            if ($wia.tips) {
                foreach ($t in @($wia.tips)) { Info ("tip: {0}" -f $t) }
            }
        }
    } else {
        Warn ("WIA health failed/timeout: {0}" -f $wh.error)
    }
}

Section "Epson Device Manager snapshot"
try {
    $devs = Get-PnpDevice -EA SilentlyContinue | Where-Object {
        $_.FriendlyName -match "EPSON|Epson|ES-500|ES-400"
    }
    if (-not $devs) {
        if (-not $wiaFailScanner) { Warn "No Epson-named PnP devices" }
        else { Info "No Epson-named PnP devices (already FAILed on WIA probe)" }
    } else {
        foreach ($d in $devs) {
            $line = "{0} | Status={1} | Class={2} | Problem={3}" -f `
                $d.FriendlyName, $d.Status, $d.Class, $d.Problem
            if ($d.Status -eq "OK") { Ok $line }
            else { Warn $line }
        }
    }
} catch {
    Warn ("Get-PnpDevice failed: {0}" -f $_.Exception.Message)
}

# 1d Firewall
Section "Firewall rules"
if (-not $isAdmin) {
    Warn "Firewall rules not enumerated - re-run elevated to verify"
} else {
    $needPorts = @($PrintPort, $ScanPort)
    foreach ($port in $needPorts) {
        $found = $false
        try {
            $rules = Get-NetFirewallRule -Direction Inbound -Enabled True -Action Allow -EA SilentlyContinue
            foreach ($rule in $rules) {
                $pf = Get-NetFirewallPortFilter -AssociatedNetFirewallRule $rule -EA SilentlyContinue
                if (-not $pf) { continue }
                $lp = @($pf.LocalPort)
                if ($lp -contains "$port" -or $lp -contains $port -or $lp -contains "Any") {
                    Ok ("Inbound allow for TCP {0}: {1}" -f $port, $rule.DisplayName)
                    $found = $true
                    break
                }
            }
        } catch {
            Warn ("Firewall enum error for {0}: {1}" -f $port, $_.Exception.Message)
        }
        if (-not $found) {
            Bad ("No enabled inbound Allow rule found for TCP {0}" -f $port)
        }
    }
}

Section "scan_agent import check (local PYTHONPATH)"
if ($py -and (Test-Path $scanApp)) {
    $env:PYTHONPATH = $scanApp
    $imp = & $py -c "import scan_agent.server as s; print(s.REQUIRED_CODE_REV)" 2>&1
    if ($LASTEXITCODE -eq 0) { Ok ("import scan_agent.server -> {0}" -f ($imp | Out-String).Trim()) }
    else {
        Bad ("import scan_agent failed: {0}" -f ($imp | Out-String).Trim())
    }
} else {
    Info "Skip import check - no local app or python"
}

Section "SUMMARY"
L ("  OK={0}  WARN={1}  FAIL={2}" -f $script:ok, $script:warn, $script:fail)
L ("  Report: {0}" -f $logPath)
L ("  Latest: {0}" -f $latestPath)
# PASS when FAIL=0 even if WARN>0 (cold-start WARNs are expected)
if ($script:fail -eq 0) {
    L "  RESULT: PASS"
} else {
    L "  RESULT: FAIL - read [FAIL] lines above"
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
