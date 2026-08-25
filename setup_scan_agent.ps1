#Requires -Version 5.1
<#
.SYNOPSIS
  DEPRECATED — use T:\GO_SCAN_AGENT.bat / install_scan_agent_task.ps1 / GO_RECEPTION.bat

.DESCRIPTION
  Legacy scan install (NSSM-oriented). Scan must use Interactive AtLogOn task.
  Pass -ForceDeprecated to run anyway.
#>
param(
    [string]$UncRoot = "\\Xcel-server\taxops",
    [string]$Token = $env:SCAN_AGENT_TOKEN,
    [string]$HostBind = "0.0.0.0",
    [int]$Port = 8766,
    [string]$ServiceName = "ScanAgent",
    [switch]$NoNssm,
    [switch]$Quiet,
    [string]$TokenFile = "",   # internal: elevated relaunch reads token from file
    [switch]$ForceDeprecated
)

$ErrorActionPreference = "Continue"

if (-not $Quiet) {
    Write-Host ""
    Write-Host "========================================================" -ForegroundColor Yellow
    Write-Host " DEPRECATED: setup_scan_agent.ps1" -ForegroundColor Yellow
    Write-Host "========================================================" -ForegroundColor Yellow
    Write-Host " Use:  T:\GO_RECEPTION.bat  |  T:\GO_SCAN_AGENT.bat" -ForegroundColor Cyan
    Write-Host "       taxops\scripts\install_scan_agent_task.ps1" -ForegroundColor Cyan
    Write-Host ""
}
if (-not $ForceDeprecated) {
    Write-Host "Refusing to run. Pass -ForceDeprecated only if you must." -ForegroundColor Red
    exit 2
}
if (-not $Quiet) {
    Write-Host "[WARN] Continuing with deprecated setup_scan_agent..." -ForegroundColor Yellow
}

$localRoot = "C:\TaxOps\ScanAgent"
$tokenEnvPath = Join-Path $localRoot "token.env"

function Say([string]$Msg, [string]$Color = "Gray") {
    Write-Host $Msg -ForegroundColor $Color
}
function Ok([string]$Msg) { Say "  [OK] $Msg" "Green" }
function Warn([string]$Msg) { Say "  [WARN] $Msg" "Yellow" }
function Fail([string]$Msg) { Say "  [FAIL] $Msg" "Red" }

function Test-IsAdmin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Get-DotEnvValue([string]$Path, [string]$Key) {
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    $prefix = "$Key="
    foreach ($line in Get-Content -LiteralPath $Path -EA SilentlyContinue) {
        $t = ($line -as [string]).Trim()
        if (-not $t -or $t.StartsWith("#")) { continue }
        if ($t.StartsWith($prefix)) {
            return $t.Substring($prefix.Length).Trim().Trim('"').Trim("'")
        }
    }
    return $null
}

function Find-Python {
    $candidates = @(
        (Join-Path $script:appDir ".venv\Scripts\python.exe"),
        "C:\TaxOps\taxops\.venv\Scripts\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python314\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
        "C:\Python313\python.exe",
        "C:\Python312\python.exe",
        "C:\Python311\python.exe"
    )
    foreach ($c in $candidates) {
        if ($c -and (Test-Path -LiteralPath $c)) { return $c }
    }
    foreach ($cmd in @("py", "python")) {
        $g = Get-Command $cmd -EA SilentlyContinue
        if (-not $g) { continue }
        $src = $g.Source
        if ($src -match "WindowsApps") { continue }
        if ($cmd -eq "py") {
            $out = & py -3 -c "import sys; print(sys.executable)" 2>$null
            if ($LASTEXITCODE -eq 0 -and $out -and (Test-Path -LiteralPath $out.Trim())) {
                return $out.Trim()
            }
        } elseif (Test-Path -LiteralPath $src) {
            return $src
        }
    }
    return $null
}

function Resolve-ShareRoot {
    param([string]$ScriptPath, [string]$FallbackUnc)
    $root = $null
    if ($ScriptPath) { $root = Split-Path -Parent $ScriptPath }
    if (-not $root -or -not (Test-Path -LiteralPath $root)) { $root = $FallbackUnc }
    # Prefer UNC so services / elevated shells are not stuck on a missing T: mapping
    try {
        $item = Get-Item -LiteralPath $root -EA Stop
        if ($item.FullName -match '^[A-Za-z]:\\') {
            $unc = $FallbackUnc.TrimEnd('\')
            if (Test-Path -LiteralPath (Join-Path $unc "taxops\scan_agent\server.py")) {
                return $unc
            }
        }
    } catch {}
    return $root.TrimEnd('\')
}

function Save-TokenFile([string]$Tok) {
    if (-not (Test-Path -LiteralPath $localRoot)) {
        New-Item -ItemType Directory -Path $localRoot -Force | Out-Null
    }
    # One KEY=VALUE line; value may contain + and trailing =
    Set-Content -LiteralPath $tokenEnvPath -Value ("SCAN_AGENT_TOKEN={0}" -f $Tok) -Encoding ASCII
    try {
        icacls $tokenEnvPath /inheritance:r /grant:r "SYSTEM:(R)" "Administrators:(F)" "$env:USERNAME:(R)" | Out-Null
    } catch {}
}

function Get-LanIPv4 {
    try {
        $ip = Get-NetIPAddress -AddressFamily IPv4 -EA SilentlyContinue |
            Where-Object {
                $_.IPAddress -like '192.168.*' -and
                $_.PrefixOrigin -ne 'WellKnown' -and
                $_.IPAddress -notlike '169.254.*'
            } |
            Select-Object -First 1 -ExpandProperty IPAddress
        if ($ip) { return $ip }
    } catch {}
    return "192.168.1.9"
}

function Test-LocalHealth([string]$Tok, [int]$P, [int]$TimeoutSec = 8) {
    try {
        $req = [System.Net.HttpWebRequest]::Create(("http://127.0.0.1:{0}/health" -f $P))
        $req.Method = "GET"
        $req.Timeout = $TimeoutSec * 1000
        $req.ReadWriteTimeout = $TimeoutSec * 1000
        $req.Headers.Add("X-Scan-Agent-Token", $Tok)
        $resp = $req.GetResponse()
        try {
            $reader = New-Object System.IO.StreamReader($resp.GetResponseStream())
            $body = $reader.ReadToEnd()
            $reader.Close()
            return @{ ok = $true; body = $body; code = [int]$resp.StatusCode }
        } finally {
            $resp.Close()
        }
    } catch [System.Net.WebException] {
        $code = 0
        if ($_.Exception.Response) {
            $code = [int]$_.Exception.Response.StatusCode
        }
        return @{ ok = $false; body = $_.Exception.Message; code = $code }
    } catch {
        return @{ ok = $false; body = $_.Exception.Message; code = 0 }
    }
}

function Ensure-Firewall([int]$P) {
    $fwName = "TaxOps Scan Agent"
    $rule = Get-NetFirewallRule -DisplayName $fwName -EA SilentlyContinue
    if (-not $rule) {
        New-NetFirewallRule -DisplayName $fwName -Direction Inbound -Protocol TCP `
            -LocalPort $P -Action Allow -Profile Any | Out-Null
        Ok "Firewall rule added for TCP $P"
    } else {
        Ok "Firewall rule already present"
    }
}

function Install-StartupShortcut([string]$BatPath) {
    $startup = [Environment]::GetFolderPath("Startup")
    if (-not $startup) { return $false }
    $lnk = Join-Path $startup "TaxOps Scan Agent.lnk"
    try {
        $w = New-Object -ComObject WScript.Shell
        $sc = $w.CreateShortcut($lnk)
        $sc.TargetPath = $BatPath
        $sc.WorkingDirectory = Split-Path -Parent $BatPath
        $sc.WindowStyle = 7  # minimized
        $sc.Description = "TaxOps Scan Agent (Epson WIA)"
        $sc.Save()
        Ok "Startup shortcut: $lnk"
        return $true
    } catch {
        Warn "Startup shortcut failed: $($_.Exception.Message)"
        return $false
    }
}

function Start-AgentBackground([string]$Py, [string]$AppDir, [string]$Tok, [string]$Bind, [int]$P) {
    # Kill anything already bound to the port
    try {
        Get-NetTCPConnection -LocalPort $P -State Listen -EA SilentlyContinue |
            ForEach-Object {
                if ($_.OwningProcess) {
                    Stop-Process -Id $_.OwningProcess -Force -EA SilentlyContinue
                }
            }
    } catch {}

    $logDir = Join-Path $localRoot "logs"
    if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }

    $prevTok = $env:SCAN_AGENT_TOKEN
    $prevHost = $env:SCAN_AGENT_HOST
    $prevPort = $env:SCAN_AGENT_PORT
    $prevPyPath = $env:PYTHONPATH
    $env:SCAN_AGENT_TOKEN = $Tok
    $env:SCAN_AGENT_HOST = $Bind
    $env:SCAN_AGENT_PORT = "$P"
    if ($prevPyPath) {
        $env:PYTHONPATH = "$AppDir;$prevPyPath"
    } else {
        $env:PYTHONPATH = $AppDir
    }

    try {
        $proc = Start-Process -FilePath $Py `
            -ArgumentList @("-m", "scan_agent.server", "--host", $Bind, "--port", "$P") `
            -WorkingDirectory $AppDir `
            -WindowStyle Hidden `
            -PassThru
        Start-Sleep -Seconds 2
        return $proc
    } finally {
        if ($null -ne $prevTok) { $env:SCAN_AGENT_TOKEN = $prevTok } else { Remove-Item Env:SCAN_AGENT_TOKEN -EA SilentlyContinue }
        if ($null -ne $prevHost) { $env:SCAN_AGENT_HOST = $prevHost } else { Remove-Item Env:SCAN_AGENT_HOST -EA SilentlyContinue }
        if ($null -ne $prevPort) { $env:SCAN_AGENT_PORT = $prevPort } else { Remove-Item Env:SCAN_AGENT_PORT -EA SilentlyContinue }
        if ($null -ne $prevPyPath) { $env:PYTHONPATH = $prevPyPath } else { Remove-Item Env:PYTHONPATH -EA SilentlyContinue }
    }
}

# ---------- resolve paths ----------
$thisScript = $MyInvocation.MyCommand.Path
if (-not $thisScript) { $thisScript = Join-Path $UncRoot "setup_scan_agent.ps1" }
$root = Resolve-ShareRoot -ScriptPath $thisScript -FallbackUnc $UncRoot
$script:appDir = Join-Path $root "taxops"
$appDir = $script:appDir
$startBat = Join-Path $root "start_scan_agent.bat"
if (-not (Test-Path -LiteralPath $startBat)) {
    $startBat = Join-Path $UncRoot "start_scan_agent.bat"
}

# Token from file (elevated relaunch) or .env
if ($TokenFile -and (Test-Path -LiteralPath $TokenFile)) {
    $Token = (Get-Content -LiteralPath $TokenFile -Raw).Trim()
}
if (-not $Token) {
    $Token = Get-DotEnvValue (Join-Path $appDir ".env") "SCAN_AGENT_TOKEN"
}
if (-not $Token) {
    $Token = Get-DotEnvValue (Join-Path $root ".env") "SCAN_AGENT_TOKEN"
}
if (-not $Token -and (Test-Path -LiteralPath $tokenEnvPath)) {
    $Token = Get-DotEnvValue $tokenEnvPath "SCAN_AGENT_TOKEN"
}

# Elevate without putting token on the command line (base64 tokens end with =)
if (-not (Test-IsAdmin)) {
    Say "Need Administrator - relaunching (click Yes on UAC)..." "Yellow"
    if (-not (Test-Path -LiteralPath $localRoot)) {
        New-Item -ItemType Directory -Path $localRoot -Force | Out-Null
    }
    $tf = Join-Path $localRoot "_elevate_token.txt"
    if ($Token) { Set-Content -LiteralPath $tf -Value $Token -Encoding ASCII }
    # WorkingDirectory: use a local path — UNC as WD often fails under elevation
    $wd = if (Test-Path "C:\TaxOps") { "C:\TaxOps" } else { $env:SystemRoot }
    $argList = @(
        "-NoExit", "-NoProfile", "-ExecutionPolicy", "Bypass",
        "-File", $thisScript,
        "-UncRoot", $UncRoot,
        "-Port", "$Port",
        "-HostBind", $HostBind,
        "-ServiceName", $ServiceName,
        "-TokenFile", $tf
    )
    if ($NoNssm) { $argList += "-NoNssm" }
    if ($Quiet) { $argList += "-Quiet" }
    if ($ForceDeprecated) { $argList += "-ForceDeprecated" }
    Start-Process powershell.exe -Verb RunAs -WorkingDirectory $wd -ArgumentList $argList
    exit 0
}

Say ""
Say "========================================================" "DarkRed"
Say "  TaxOps Scan Agent setup (reception PC)" "White"
Say "========================================================" "DarkRed"
Say ("  Share app dir: {0}" -f $appDir) "DarkGray"

if (-not (Test-Path -LiteralPath (Join-Path $appDir "scan_agent\server.py"))) {
    Fail "scan_agent\server.py not found under $appDir"
    Fail "Map T: to \\Xcel-server\taxops or fix -UncRoot, then re-run."
    if (-not $Quiet) { Read-Host "Press Enter to close" | Out-Null }
    exit 4
}

if (-not $Token) {
    Say "  SCAN_AGENT_TOKEN is empty." "Yellow"
    $Token = Read-Host "  Enter shared token (same value as TaxOps SCAN_AGENT_TOKEN)"
}
if (-not $Token) {
    Fail "Token required - agent will not start without it."
    if (-not $Quiet) { Read-Host "Press Enter to close" | Out-Null }
    exit 2
}

Save-TokenFile $Token
Ok "Token saved to $tokenEnvPath"

$py = Find-Python
if (-not $py) {
    Fail "python.exe not found. Install Python 3 from python.org (Add to PATH), then re-run."
    if (-not $Quiet) { Read-Host "Press Enter to close" | Out-Null }
    exit 3
}
Ok "Python: $py"

Say "  ... installing flask / pywin32 / Pillow / pymupdf ..." "Cyan"
& $py -m pip install -q flask "pywin32>=306" Pillow pymupdf 2>&1 | Out-Null
& $py -c "import win32com.client, flask, PIL" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Warn "Import check failed - install may be incomplete."
} else {
    Ok "Agent dependencies importable"
}

# Prove WIA COM init works in this Python before launching the HTTP server
Say "  ... probing WIA via list_wia_scanners (CoInitialize) ..." "Cyan"
$env:PYTHONPATH = $appDir
$wiaProbe = & $py -c "from scan_agent.server import list_wia_scanners; d=list_wia_scanners(); print('SCANNERS', len(d)); print('NAMES', '|'.join((x.get('name') or '') for x in d))" 2>&1
if ($LASTEXITCODE -ne 0) {
    Warn "WIA probe failed - agent may start but scanner will not work:"
    Say ("         {0}" -f ($wiaProbe | Out-String).Trim()) "Yellow"
} else {
    Ok ("WIA probe: {0}" -f (($wiaProbe | Out-String).Trim() -replace '\s+', ' '))
}

Ensure-Firewall $Port

# Prefer user-session launcher (WIA). NSSM LocalSystem often cannot see USB scanners.
if (Test-Path -LiteralPath $startBat) {
    Install-StartupShortcut $startBat | Out-Null
} else {
    Warn "start_scan_agent.bat not found on share - skipping Startup shortcut"
}

$nssm = $null
if (-not $NoNssm) {
    $nssm = Get-Command nssm.exe -EA SilentlyContinue | Select-Object -ExpandProperty Source
    if (-not $nssm) {
        foreach ($p in @(
            "C:\Tools\nssm-2.24\win64\nssm.exe",
            "C:\Tools\nssm\win64\nssm.exe",
            "C:\nssm\nssm.exe",
            "C:\nssm\win64\nssm.exe"
        )) {
            if (Test-Path $p) { $nssm = $p; break }
        }
    }
}

$mode = "user-session"
if ($nssm -and -not $NoNssm) {
    Say "  NSSM found - configuring service (still starting a user-session agent for WIA)..." "Cyan"
    # Use UNC app dir, not T:
    $svcAppDir = $appDir
    if ($svcAppDir -match '^[A-Za-z]:\\' -and (Test-Path -LiteralPath (Join-Path $UncRoot "taxops\scan_agent\server.py"))) {
        $svcAppDir = Join-Path $UncRoot.TrimEnd('\') "taxops"
    }
    $existing = Get-Service $ServiceName -EA SilentlyContinue
    if (-not $existing) {
        & $nssm install $ServiceName $py
    }
    # Token via --token-file style: load from env file through a wrapper cmd
    $wrapper = Join-Path $localRoot "run_service.cmd"
    $wrapperBody = @"
@echo off
setlocal
for /f "usebackq tokens=1,* delims==" %%a in ("$tokenEnvPath") do if /I "%%a"=="SCAN_AGENT_TOKEN" set "SCAN_AGENT_TOKEN=%%b"
set "SCAN_AGENT_HOST=$HostBind"
set "SCAN_AGENT_PORT=$Port"
set "PYTHONPATH=$svcAppDir"
cd /d "$svcAppDir"
"$py" -m scan_agent.server --host $HostBind --port $Port
"@
    Set-Content -LiteralPath $wrapper -Value $wrapperBody -Encoding ASCII
    & $nssm set $ServiceName Application "$env:SystemRoot\System32\cmd.exe"
    & $nssm set $ServiceName AppParameters "/c `"$wrapper`""
    & $nssm set $ServiceName AppDirectory $localRoot
    $logDir = Join-Path $localRoot "logs"
    if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }
    & $nssm set $ServiceName AppStdout (Join-Path $logDir "stdout.log")
    & $nssm set $ServiceName AppStderr (Join-Path $logDir "stderr.log")
    & $nssm set $ServiceName Start SERVICE_DEMAND_START
    # Primary runtime is user-session (WIA). NSSM left installed for optional use.
    Ok "NSSM service registered (demand-start wrapper: $wrapper)"
    $mode = "user-session+nssm"
}

# Always clear bytecode + start (or restart) a user-session agent
$pycache = Join-Path $appDir "scan_agent\__pycache__"
if (Test-Path -LiteralPath $pycache) {
    Remove-Item -LiteralPath $pycache -Recurse -Force -EA SilentlyContinue
    Ok "Cleared scan_agent __pycache__"
}

Say "  Starting Scan Agent in background (user session)..." "Cyan"
$proc = Start-AgentBackground -Py $py -AppDir $appDir -Tok $Token -Bind $HostBind -P $Port
Start-Sleep -Seconds 3

$health = Test-LocalHealth -Tok $Token -P $Port
$healthy = $false
if ($health.ok) {
    try {
        $j = $health.body | ConvertFrom-Json
        if ($j.code_rev -ne "com_sta_v1") {
            Fail "Agent running OLD code (code_rev=$($j.code_rev)). Kill the old window and re-run setup / restart_scan_agent.bat"
        } elseif (("$($j.scanner_error)$($j.tips)") -match "CoInitialize") {
            Fail "CoInitialize still failing - pywin32/COM broken on this PC"
            if ($j.tips) { foreach ($t in $j.tips) { Say "         tip: $t" "Yellow" } }
        } else {
            $healthy = $true
            Ok "Health check passed (com_sta_v1) on http://127.0.0.1:$Port/health"
            if ($j.scanner_found) {
                Ok ("Scanner: {0}" -f ($j.scanner_names -join ", "))
            } else {
                Warn "Agent is up but no WIA scanner yet - power Epson / fix USB, then retry."
                if ($j.tips) { foreach ($t in $j.tips) { Say "         tip: $t" "Yellow" } }
            }
        }
    } catch {
        Fail "Health JSON parse failed: $($_.Exception.Message)"
    }
} else {
    Fail "Health check failed: $($health.body)"
    Warn "Try manually: $startBat  or  restart_scan_agent.bat"
    if (Test-Path (Join-Path $localRoot "logs\stderr.log")) {
        Say "  --- stderr.log (tail) ---" "DarkGray"
        Get-Content (Join-Path $localRoot "logs\stderr.log") -Tail 15 -EA SilentlyContinue
    }
}

$lan = Get-LanIPv4
Say ""
Say "  On the TaxOps server, confirm:" "White"
Say ("    SCAN_AGENT_URL=http://{0}:{1}" -f $lan, $Port) "Gray"
Say "    SCAN_AGENT_TOKEN=<same token as this PC>" "Gray"
Say "  Then restart TaxOpsService." "Gray"
Say ("  Mode: {0}  |  Daily start: {1}" -f $mode, $startBat) "DarkGray"
$restartBat = Join-Path $root "restart_scan_agent.bat"
if (Test-Path -LiteralPath $restartBat) {
    Say ("  After code updates: {0}" -f $restartBat) "DarkGray"
}
Say ""

# Clean elevate token file
$tfClean = Join-Path $localRoot "_elevate_token.txt"
if (Test-Path -LiteralPath $tfClean) {
    Remove-Item -LiteralPath $tfClean -Force -EA SilentlyContinue
}

if (-not $Quiet) {
    Read-Host "Press Enter to close" | Out-Null
}

if ($healthy) { exit 0 } else { exit 5 }
