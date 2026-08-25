#Requires -Version 5.1
<#
.SYNOPSIS
  DEPRECATED — use T:\GO_SCAN_AGENT.bat / install_scan_agent_task.ps1 / GO_RECEPTION.bat

.DESCRIPTION
  NSSM-era guided wizard. Scan requires Interactive AtLogOn scheduled task.
  Pass -ForceDeprecated to run anyway.
#>
param(
    [ValidateSet("Install", "Repair", "TestOnly")]
    [string]$Mode = "Install",
    [string]$UncRoot = "\\Xcel-server\taxops",
    [string]$Token = $env:SCAN_AGENT_TOKEN,
    [string]$TokenFile = "",
    [string]$HostBind = "0.0.0.0",
    [int]$Port = 8766,
    [switch]$Quiet,
    [switch]$ForceDeprecated
)

$ErrorActionPreference = "Continue"

if (-not $Quiet) {
    Write-Host ""
    Write-Host "========================================================" -ForegroundColor Yellow
    Write-Host " DEPRECATED: scan_agent_wizard.ps1" -ForegroundColor Yellow
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
    Write-Host "[WARN] Continuing with deprecated scan_agent_wizard..." -ForegroundColor Yellow
}

$localRoot = "C:\TaxOps\ScanAgent"
$tokenEnvPath = Join-Path $localRoot "token.env"
$pidFile = Join-Path $localRoot "agent.pid"
$logDir = Join-Path $localRoot "logs"
$script:fail = 0
$script:warn = 0
$script:ok = 0
$script:step = 0
$script:healthy = $false

function Say([string]$Msg, [string]$Color = "Gray") { Write-Host $Msg -ForegroundColor $Color }
function Ok([string]$Msg) { $script:ok++; Say ("  [OK]   {0}" -f $Msg) "Green" }
function Warn([string]$Msg) { $script:warn++; Say ("  [WARN] {0}" -f $Msg) "Yellow" }
function Bad([string]$Msg) { $script:fail++; Say ("  [FAIL] {0}" -f $Msg) "Red" }
function Step([string]$Title) {
    $script:step++
    Write-Host ""
    Say ("-------- Step {0}: {1} --------" -f $script:step, $Title) "Cyan"
}

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
        "C:\Python314\python.exe",
        "C:\Python313\python.exe",
        "C:\Python312\python.exe",
        "C:\Program Files\Python314\python.exe",
        "C:\Program Files\Python313\python.exe",
        "C:\Program Files\Python312\python.exe"
    )
    try {
        $root = Join-Path $env:LOCALAPPDATA "Programs\Python"
        if (Test-Path $root) {
            Get-ChildItem $root -Filter "python.exe" -Recurse -EA SilentlyContinue |
                Select-Object -First 8 -ExpandProperty FullName |
                ForEach-Object { $candidates += $_ }
        }
    } catch {}
    foreach ($c in $candidates) {
        if ($c -and (Test-Path -LiteralPath $c) -and ($c -notmatch "WindowsApps")) { return $c }
    }
    foreach ($cmdName in @("py", "python")) {
        $g = Get-Command $cmdName -EA SilentlyContinue
        if (-not $g) { continue }
        if ($cmdName -eq "py") {
            $out = & py -3 -c "import sys; print(sys.executable)" 2>$null
            if ($LASTEXITCODE -eq 0 -and $out) {
                $p = $out.Trim()
                if ((Test-Path -LiteralPath $p) -and ($p -notmatch "WindowsApps")) { return $p }
            }
        } elseif ($g.Source -notmatch "WindowsApps" -and (Test-Path $g.Source)) {
            return $g.Source
        }
    }
    return $null
}

function Save-Token([string]$Tok) {
    if (-not (Test-Path $localRoot)) { New-Item -ItemType Directory -Path $localRoot -Force | Out-Null }
    Set-Content -LiteralPath $tokenEnvPath -Value ("SCAN_AGENT_TOKEN={0}" -f $Tok) -Encoding ASCII
    try {
        icacls $tokenEnvPath /inheritance:r /grant:r "SYSTEM:(R)" "Administrators:(F)" "$env:USERNAME:(R)" | Out-Null
    } catch {}
}

function Stop-PortListeners([int]$P) {
    $killed = @()
    try {
        Get-NetTCPConnection -LocalPort $P -State Listen -EA SilentlyContinue | ForEach-Object {
            $procId = $_.OwningProcess
            if ($procId -and $procId -gt 0) {
                try {
                    $procObj = Get-Process -Id $procId -EA SilentlyContinue
                    Stop-Process -Id $procId -Force -EA Stop
                    $name = if ($procObj) { $procObj.ProcessName } else { "pid" }
                    $killed += ("{0}({1})" -f $name, $procId)
                } catch {
                    Warn ("Could not kill PID {0}: {1}" -f $procId, $_.Exception.Message)
                }
            }
        }
    } catch {}
    try {
        $lines = netstat -ano | Select-String (":{0}\s+.*LISTENING" -f $P)
        foreach ($line in $lines) {
            $parts = ($line.ToString() -split "\s+") | Where-Object { $_ }
            $procId = $parts[-1]
            if ($procId -match "^\d+$" -and [int]$procId -gt 0) {
                Stop-Process -Id ([int]$procId) -Force -EA SilentlyContinue
                $killed += ("pid={0}" -f $procId)
            }
        }
    } catch {}
    Start-Sleep -Seconds 2
    return $killed
}

function Ensure-Firewall([int]$P) {
    $fwName = "TaxOps Scan Agent"
    $rule = Get-NetFirewallRule -DisplayName $fwName -EA SilentlyContinue
    if (-not $rule) {
        New-NetFirewallRule -DisplayName $fwName -Direction Inbound -Protocol TCP `
            -LocalPort $P -Action Allow -Profile Any | Out-Null
        return $true
    }
    return $false
}

function Install-Startup([string]$BatPath) {
    $startup = [Environment]::GetFolderPath("Startup")
    if (-not $startup) { return $false }
    $lnk = Join-Path $startup "TaxOps Scan Agent.lnk"
    $w = New-Object -ComObject WScript.Shell
    $sc = $w.CreateShortcut($lnk)
    $sc.TargetPath = $BatPath
    $sc.WorkingDirectory = Split-Path -Parent $BatPath
    $sc.WindowStyle = 7
    $sc.Description = "TaxOps Scan Agent"
    $sc.Save()
    return $true
}

function Test-AgentHealth([string]$Tok, [int]$P, [int]$TimeoutSec = 3) {
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
        } finally { $resp.Close() }
    } catch [System.Net.WebException] {
        $code = 0
        if ($_.Exception.Response) { $code = [int]$_.Exception.Response.StatusCode }
        return @{ ok = $false; body = $_.Exception.Message; code = $code }
    } catch {
        return @{ ok = $false; body = $_.Exception.Message; code = 0 }
    }
}

function Start-AgentProcess([string]$Py, [string]$AppDir, [string]$Tok, [string]$Bind, [int]$P) {
    if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }
    $outLog = Join-Path $logDir "stdout.log"
    $errLog = Join-Path $logDir "stderr.log"
    $runCmd = Join-Path $localRoot "run_agent.cmd"

    $cache = Join-Path $AppDir "scan_agent\__pycache__"
    if (Test-Path $cache) { Remove-Item $cache -Recurse -Force -EA SilentlyContinue }

    # Build cmd with -f so token special chars are less likely to break the script parser.
    $cmd = @(
        "@echo off"
        "setlocal"
        ("set `"SCAN_AGENT_TOKEN={0}`"" -f $Tok)
        ("set `"SCAN_AGENT_HOST={0}`"" -f $Bind)
        ("set `"SCAN_AGENT_PORT={0}`"" -f $P)
        ("set `"PYTHONPATH={0}`"" -f $AppDir)
        ("cd /d `"{0}`"" -f $AppDir)
        ("`"{0}`" -m scan_agent.server --host {1} --port {2} >> `"{3}`" 2>> `"{4}`"" -f $Py, $Bind, $P, $outLog, $errLog)
    ) -join "`r`n"
    Set-Content -LiteralPath $runCmd -Value $cmd -Encoding ASCII

    $proc = Start-Process -FilePath "$env:SystemRoot\System32\cmd.exe" `
        -ArgumentList @("/c", "`"$runCmd`"") `
        -WorkingDirectory $localRoot `
        -WindowStyle Minimized `
        -PassThru
    Set-Content -LiteralPath $pidFile -Value "$($proc.Id)" -Encoding ASCII
    return $proc
}

# ---------- resolve paths ----------
$thisScript = $MyInvocation.MyCommand.Path
if (-not $thisScript) { $thisScript = Join-Path $UncRoot "scan_agent_wizard.ps1" }
$root = Split-Path -Parent $thisScript
if (-not (Test-Path (Join-Path $root "taxops\scan_agent\server.py"))) {
    if (Test-Path (Join-Path $UncRoot "taxops\scan_agent\server.py")) { $root = $UncRoot.TrimEnd('\') }
}
$script:appDir = Join-Path $root "taxops"
$appDir = $script:appDir
$startBat = Join-Path $root "start_scan_agent.bat"

if ($TokenFile -and (Test-Path -LiteralPath $TokenFile)) {
    $Token = (Get-Content -LiteralPath $TokenFile -Raw).Trim()
}
if (-not $Token) { $Token = Get-DotEnvValue (Join-Path $appDir ".env") "SCAN_AGENT_TOKEN" }
if (-not $Token) { $Token = Get-DotEnvValue $tokenEnvPath "SCAN_AGENT_TOKEN" }

# Elevate for firewall / pywin32 postinstall (token via file; base64 may end with =)
if (-not (Test-IsAdmin) -and $Mode -ne "TestOnly") {
    Say "Need Administrator - relaunching (click Yes on UAC)..." "Yellow"
    if (-not (Test-Path $localRoot)) { New-Item -ItemType Directory -Path $localRoot -Force | Out-Null }
    $tf = Join-Path $localRoot "_elevate_token.txt"
    if ($Token) { Set-Content -LiteralPath $tf -Value $Token -Encoding ASCII }
    $wd = if (Test-Path "C:\TaxOps") { "C:\TaxOps" } else { $env:SystemRoot }
    $argList = @(
        "-NoExit", "-NoProfile", "-ExecutionPolicy", "Bypass",
        "-File", $thisScript,
        "-UncRoot", $UncRoot,
        "-Port", "$Port",
        "-HostBind", $HostBind,
        "-Mode", $Mode,
        "-TokenFile", $tf
    )
    if ($Quiet) { $argList += "-Quiet" }
    if ($ForceDeprecated) { $argList += "-ForceDeprecated" }
    Start-Process powershell.exe -Verb RunAs -WorkingDirectory $wd -ArgumentList $argList
    exit 0
}

Say ""
Say "========================================================" "DarkRed"
Say ("  TaxOps Scan Agent WIZARD  ({0})" -f $Mode) "White"
Say "========================================================" "DarkRed"
Say ("  Share: {0}" -f $appDir) "DarkGray"
Say ("  Local: {0}" -f $localRoot) "DarkGray"

# Step 1: share
Step "TaxOps share / scan_agent code"
if (-not (Test-Path (Join-Path $appDir "scan_agent\server.py"))) {
    Bad ("taxops\scan_agent\server.py not found under {0}" -f $appDir)
    Bad "Map T: to \\Xcel-server\taxops or fix path, then re-run."
    if (-not $Quiet) { Read-Host "Press Enter to close" | Out-Null }
    exit 4
}
Ok "Found scan_agent\server.py"
$revLine = Select-String -Path (Join-Path $appDir "scan_agent\server.py") -Pattern "com_sta_v4" -SimpleMatch -EA SilentlyContinue
if ($revLine) {
    Ok "server.py includes com_sta_v4"
} else {
    Bad "server.py missing com_sta_v4 - update share from server"
}

# Sync share -> local disk (Python from T: looks hung)
Step "Sync scan_agent to C:\TaxOps\ScanAgent\app"
$syncScript = Join-Path $root "sync_scan_agent_local.ps1"
if (-not (Test-Path -LiteralPath $syncScript)) {
    $candidates = @(
        (Join-Path $PSScriptRoot "sync_scan_agent_local.ps1"),
        "T:\sync_scan_agent_local.ps1",
        "\\Xcel-server\taxops\sync_scan_agent_local.ps1"
    )
    foreach ($c in $candidates) {
        if (Test-Path -LiteralPath $c) { $syncScript = $c; break }
    }
}
if (Test-Path -LiteralPath $syncScript) {
    & powershell -NoProfile -ExecutionPolicy Bypass -File $syncScript -ShareRoot $root -LocalRoot $localRoot
    if ($LASTEXITCODE -ge 4) {
        Bad ("Local sync failed exit {0}" -f $LASTEXITCODE)
    } else {
        Ok "Local copy ready under C:\TaxOps\ScanAgent\app"
        # Prefer local package for PYTHONPATH / selftest from here on
        $appDir = Join-Path $localRoot "app"
        $script:appDir = $appDir
    }
} else {
    Say "  [WARN] sync_scan_agent_local.ps1 not found — agent may run slow from share" "Yellow"
}

# Step 2: Python
Step "Python 3.10+ (not Windows Store stub)"
$py = Find-Python
if (-not $py) {
    Bad "python.exe not found"
    Say "  Install from https://www.python.org/downloads/windows/" "Yellow"
    Say "  Check Add python.exe to PATH, then re-run this wizard." "Yellow"
    try { Start-Process "https://www.python.org/downloads/windows/" } catch {}
    if (-not $Quiet) { Read-Host "Press Enter to close" | Out-Null }
    exit 3
}
if ($py -match "WindowsApps") {
    Bad ("Windows Store Python stub: {0}" -f $py)
    Say "  Disable App execution aliases for python.exe, install python.org build." "Yellow"
    if (-not $Quiet) { Read-Host "Press Enter to close" | Out-Null }
    exit 3
}
Ok ("Python: {0}" -f $py)
# Use single-quoted -c so PowerShell does not treat % as modulo
$ver = & $py -c 'import sys; print("{0}.{1}".format(sys.version_info[0], sys.version_info[1]))' 2>$null
Ok ("Version: {0}" -f $ver)

if ($Mode -eq "TestOnly") {
    Step "Selftest only"
    Push-Location $appDir
    $env:PYTHONPATH = $appDir
    if ($Token) { $env:SCAN_AGENT_TOKEN = $Token }
    & $py -m scan_agent.selftest --port $Port
    $code = $LASTEXITCODE
    Pop-Location
    if (-not $Quiet) { Read-Host "Press Enter to close" | Out-Null }
    exit $code
}

# Step 3: token
Step "SCAN_AGENT_TOKEN"
if (-not $Token) {
    Say "  Token empty - must match TaxOps server SCAN_AGENT_TOKEN" "Yellow"
    $Token = Read-Host "  Paste token"
}
if (-not $Token) {
    Bad "Token required"
    if (-not $Quiet) { Read-Host "Press Enter to close" | Out-Null }
    exit 2
}
Save-Token $Token
Ok ("Saved {0}" -f $tokenEnvPath)

# Step 4: pip packages
Step "Install Python packages (flask, pywin32, Pillow, pymupdf)"
& $py -m pip install -q --upgrade pip 2>&1 | Out-Null
& $py -m pip install -q flask "pywin32>=306" Pillow pymupdf 2>&1 | Out-Null
$imp = & $py -c "import flask,win32com.client,pythoncom,PIL; print('ok')" 2>&1
if ("$imp" -match "ok") {
    Ok "Imports OK"
} else {
    Bad ("Import failed: {0}" -f $imp)
}

# Step 5: pywin32 COM registration (usual CoInitialize root cause)
Step "Register pywin32 COM (postinstall)"
$null = & $py -m pywin32_postinstall -install 2>&1
if ($LASTEXITCODE -eq 0) {
    Ok "pywin32_postinstall completed"
} else {
    $scriptPath = Join-Path (Split-Path $py) "Scripts\pywin32_postinstall.py"
    if (Test-Path $scriptPath) {
        $null = & $py $scriptPath -install 2>&1
        if ($LASTEXITCODE -eq 0) {
            Ok "pywin32_postinstall via Scripts folder"
        } else {
            Warn ("postinstall exit {0} - WIA may still fail" -f $LASTEXITCODE)
        }
    } else {
        Warn "pywin32_postinstall not found - continuing"
    }
}

# Step 6: firewall
Step ("Firewall TCP {0}" -f $Port)
try {
    $added = Ensure-Firewall $Port
    if ($added) { Ok "Rule added" } else { Ok "Rule already present" }
} catch {
    Warn ("Firewall: {0}" -f $_.Exception.Message)
}

# Step 7: kill stale
Step ("Stop anything already on port {0}" -f $Port)
$killed = Stop-PortListeners $Port
if ($killed.Count) {
    Ok ("Stopped: {0}" -f ($killed -join ", "))
} else {
    Ok "Port was free (or no permission to list)"
}

# Step 8: offline selftest (no HTTP yet)
Step "Selftest (packages + CoInitialize + WIA)"
Push-Location $appDir
$env:PYTHONPATH = $appDir
$env:SCAN_AGENT_TOKEN = $Token
& $py -m scan_agent.selftest --port $Port --fix --no-http
$st = $LASTEXITCODE
Pop-Location
if ($st -eq 0) {
    Ok "Selftest PASS (required checks)"
} else {
    Warn "Selftest reported failures - see above. Agent will still start; fix Epson/USB if WIA empty."
}

# Step 9: start agent in a VISIBLE window (user can see it is running)
Step "Start Scan Agent (visible window)"
# Prefer the share launcher so the console stays open and titled.
if (Test-Path -LiteralPath $startBat) {
    # Ensure token file exists for start_scan_agent.bat
    Save-Token $Token
    Start-Process -FilePath $startBat -WorkingDirectory $root
    Ok "Launched start_scan_agent.bat in a new window - leave it open"
    # Give the new window time to kill old listeners + bind
    Start-Sleep -Seconds 4
    $proc = $null
} else {
    $proc = Start-AgentProcess -Py $py -AppDir $appDir -Tok $Token -Bind $HostBind -P $Port
    Ok ("Started PID {0} (minimized) - logs {1}" -f $proc.Id, $logDir)
}

# Step 10: health gate
Step "Verify /health returns com_sta_v4 (fast, no WIA)"
$script:healthy = $false
$body = ""
for ($i = 1; $i -le 25; $i++) {
    Start-Sleep -Seconds 1
    if ($proc -and $proc.HasExited) {
        Bad ("Agent process exited early (code {0}) - see {1}\stderr.log" -f $proc.ExitCode, $logDir)
        $errLogPath = Join-Path $logDir "stderr.log"
        if (Test-Path $errLogPath) {
            Get-Content $errLogPath -Tail 20 -EA SilentlyContinue
        }
        break
    }
    $h = Test-AgentHealth -Tok $Token -P $Port
    if (-not $h.ok) { continue }
    $body = $h.body
    if ($body -match "CoInitialize" -and $body -notmatch "agent_ok") {
        $null = Stop-PortListeners $Port
        if (Test-Path -LiteralPath $startBat) {
            Start-Process -FilePath $startBat -WorkingDirectory $root
        }
        continue
    }
    if ($body -match "com_sta_v4") {
        $script:healthy = $true
        break
    }
}
if ($script:healthy) {
    Ok "Health OK with com_sta_v4 (agent_ok)"
    try {
        $j = $body | ConvertFrom-Json
        Ok ("build={0} agent_ok={1} com_sta={2}" -f $j.code_rev, $j.agent_ok, $j.com_sta)
    } catch {}
} else {
    Bad "Health gate failed (need code_rev=com_sta_v4 / agent_ok)"
    if ($body) { Say ("         {0}" -f $body) "DarkGray" }
    $errLogPath = Join-Path $logDir "stderr.log"
    if (Test-Path $errLogPath) {
        Say "  --- stderr.log ---" "DarkGray"
        Get-Content $errLogPath -Tail 25 -EA SilentlyContinue
    }
}

# Step 11: startup + shortcuts
Step "Startup shortcut + desktop helpers"
if (Test-Path $startBat) {
    if (Install-Startup $startBat) {
        Ok "Startup: TaxOps Scan Agent.lnk"
    } else {
        Warn "Startup shortcut failed"
    }
} else {
    Warn "start_scan_agent.bat missing on share"
}
try {
    $desk = [Environment]::GetFolderPath("CommonDesktopDirectory")
    if (-not $desk) { $desk = Join-Path $env:PUBLIC "Desktop" }
    $wizBat = Join-Path $root "scan_agent_wizard.bat"
    if ((Test-Path $wizBat) -and (Test-Path $desk)) {
        $w = New-Object -ComObject WScript.Shell
        $sc = $w.CreateShortcut((Join-Path $desk "TaxOps Scan Agent Wizard.lnk"))
        $sc.TargetPath = $wizBat
        $sc.WorkingDirectory = $root
        $sc.Description = "Install / repair Scan Agent"
        $sc.Save()
        Ok "Desktop: TaxOps Scan Agent Wizard"
    }
} catch {
    Warn ("Desktop shortcut: {0}" -f $_.Exception.Message)
}

# Final selftest with HTTP
Step "Final selftest (includes live /health)"
Push-Location $appDir
$env:PYTHONPATH = $appDir
$env:SCAN_AGENT_TOKEN = $Token
& $py -m scan_agent.selftest --port $Port
$final = $LASTEXITCODE
Pop-Location
if ($final -eq 0) {
    Ok "Final selftest PASS"
} else {
    Warn "Final selftest still has failures - read FIX lines above"
}

$tfClean = Join-Path $localRoot "_elevate_token.txt"
if (Test-Path $tfClean) { Remove-Item $tfClean -Force -EA SilentlyContinue }

Write-Host ""
Say "========================================================" "DarkRed"
$doneColor = if ($script:fail) { "Yellow" } else { "Green" }
Say ("  DONE   OK={0}  WARN={1}  FAIL={2}" -f $script:ok, $script:warn, $script:fail) $doneColor
Say "========================================================" "DarkRed"
Say ""
Say "  TaxOps server .env should have:" "White"
Say ("    SCAN_AGENT_URL=http://<this-pc-ip>:{0}" -f $Port) "Gray"
Say "    SCAN_AGENT_TOKEN=<same token>" "Gray"
Say "  Then restart TaxOpsService on the server." "Gray"
Say "  Leave the minimized Scan Agent process running." "Gray"
Say "  Re-run anytime: scan_agent_wizard.bat   (or -Mode Repair)" "DarkGray"
Say ""

if (-not $Quiet) { Read-Host "Press Enter to close" | Out-Null }

if ($script:healthy) { exit 0 }
exit 5
