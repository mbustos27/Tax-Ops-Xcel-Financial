#Requires -Version 5.1
<#
.SYNOPSIS
  One-shot: ensure print relay + scan agent are up, and verify auto-start.

.DESCRIPTION
  Run ON the reception PC:
    \\Xcel-server\taxops\GO_RECEPTION.bat

  Reuses:
    start_print_relay.bat / FiletrackRelay service
    start_scan_agent.bat / Scheduled Task "TaxOps Scan Agent"
    check_reception.ps1
    install_print_relay_service.ps1 / install_scan_agent_task.ps1 (with -Repair)

.PARAMETER Repair
  If auto-start is missing, run the install scripts (needs Admin / UAC).

.PARAMETER NoStart
  Only verify  -  do not start anything.
#>
param(
    [string]$ShareRoot = "",
    [switch]$Repair,
    [switch]$NoStart,
    [int]$PrintPort = 8765,
    [int]$ScanPort = 8766
)

$ErrorActionPreference = "Continue"
$script:fail = 0
$script:warn = 0

function Say([string]$Msg, [string]$Color = "Gray") {
    Write-Host $Msg -ForegroundColor $Color
}
function Ok([string]$Msg) { Say ("  [OK]   {0}" -f $Msg) "Green" }
function Warn([string]$Msg) { $script:warn++; Say ("  [WARN] {0}" -f $Msg) "Yellow" }
function Bad([string]$Msg) { $script:fail++; Say ("  [FAIL] {0}" -f $Msg) "Red" }
function Info([string]$Msg) { Say ("  [..]   {0}" -f $Msg) "DarkGray" }
function Section([string]$Title) {
    Write-Host ""
    Say ("======== {0} ========" -f $Title) "Cyan"
}

function Test-IsAdmin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Get-DotEnv([string]$Path, [string]$Key) {
    if (-not (Test-Path -LiteralPath $Path)) { return "" }
    foreach ($line in Get-Content -LiteralPath $Path -EA SilentlyContinue) {
        if ($line -match ("^\s*{0}=(.+)$" -f [regex]::Escape($Key))) {
            return $Matches[1].Trim().Trim('"')
        }
    }
    return ""
}

function Get-ScanToken([string]$Root) {
    $t = $env:SCAN_AGENT_TOKEN
    if (-not $t) { $t = Get-DotEnv "C:\TaxOps\ScanAgent\token.env" "SCAN_AGENT_TOKEN" }
    if (-not $t -and $Root) {
        $envPath = "{0}\taxops\.env" -f $Root.Trim().TrimEnd('\')
        $t = Get-DotEnv $envPath "SCAN_AGENT_TOKEN"
    }
    return $t
}

function Test-PrintHealth([int]$Port, [int]$TimeoutSec = 4) {
    try {
        $h = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/health" -f $Port) -TimeoutSec $TimeoutSec
        return @{ ok = [bool]$h.printer_found; up = $true; body = $h; error = "" }
    } catch {
        return @{ ok = $false; up = $false; body = $null; error = $_.Exception.Message }
    }
}

function Test-ScanHealth([int]$Port, [string]$Token, [int]$TimeoutSec = 4) {
    if (-not $Token) {
        return @{ ok = $false; up = $false; body = $null; error = "SCAN_AGENT_TOKEN missing" }
    }
    try {
        $h = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/health" -f $Port) `
            -Headers @{ "X-Scan-Agent-Token" = $Token } -TimeoutSec $TimeoutSec
        $ok = ($h.code_rev -eq "com_sta_v4") -and ($h.com_sta -eq $true -or $h.agent_ok -eq $true -or $h.status -eq "ok")
        return @{ ok = [bool]$ok; up = $true; body = $h; error = "" }
    } catch {
        return @{ ok = $false; up = $false; body = $null; error = $_.Exception.Message }
    }
}

function Wait-Print([int]$Port, [int]$Seconds = 10) {
    for ($i = 1; $i -le $Seconds; $i++) {
        $r = Test-PrintHealth $Port 2
        if ($r.up) { return $r }
        if ($i -eq 1 -or $i % 3 -eq 0) {
            Info ("waiting print health... {0}/{1}s" -f $i, $Seconds)
        }
        Start-Sleep -Seconds 1
    }
    return (Test-PrintHealth $Port 2)
}

function Try-StartWindowsService([string]$Name, [int]$TimeoutSec = 8) {
    # Start-Service waits forever on StartPending. Use .Start() + timed WaitForStatus.
    $svc = Get-Service -Name $Name -EA SilentlyContinue
    if (-not $svc) { return @{ ok = $false; status = "Missing"; error = "service not found" } }
    try { $svc.Refresh() } catch {}
    if ($svc.Status -eq "Running") {
        return @{ ok = $true; status = "Running"; error = "" }
    }
    # Clear a wedged StartPending so we can retry
    if ($svc.Status -eq "StartPending" -or $svc.Status -eq "StopPending") {
        Info ("Service {0} is {1} - forcing stop via sc.exe..." -f $Name, $svc.Status)
        & sc.exe stop $Name | Out-Null
        Start-Sleep -Seconds 2
        try { $svc.Refresh() } catch {}
    }
    try {
        if ($svc.Status -ne "Running") {
            Info ("Requesting start for {0} (timeout {1}s)..." -f $Name, $TimeoutSec)
            $svc.Start()
            $svc.WaitForStatus([System.ServiceProcess.ServiceControllerStatus]::Running,
                [TimeSpan]::FromSeconds($TimeoutSec))
        }
        $svc.Refresh()
        return @{ ok = ($svc.Status -eq "Running"); status = "$($svc.Status)"; error = "" }
    } catch {
        try { $svc.Refresh() } catch {}
        return @{
            ok = $false
            status = "$($svc.Status)"
            error = $_.Exception.Message
        }
    }
}

function Wait-Scan([int]$Port, [string]$Token, [int]$Seconds = 45) {
    for ($i = 1; $i -le $Seconds; $i++) {
        $r = Test-ScanHealth $Port $Token 2
        if ($r.ok) { return $r }
        if ($i % 10 -eq 0) { Info ("waiting scan... {0}/{1}  {2}" -f $i, $Seconds, $r.error) }
        Start-Sleep -Seconds 1
    }
    return (Test-ScanHealth $Port $Token 2)
}

# ---------- resolve share ----------
$scriptPath = $MyInvocation.MyCommand.Path
$scriptDir = ""
if ($scriptPath) { $scriptDir = Split-Path -Parent $scriptPath }

$TaxOpsUncRoot = "\\Xcel-server\taxops"

function Join-SharePath([string]$Root, [string]$Child) {
    # Join-Path throws "Cannot find drive" when a mapped letter (T:) is missing
    # under elevation. String concat works for both UNC and drive letters.
    if ([string]::IsNullOrWhiteSpace($Root)) { return $null }
    $r = $Root.Trim().TrimEnd('\')
    $c = $Child.TrimStart('\')
    return "$r\$c"
}

function Test-ShareAlive([string]$Root) {
    $probe = Join-SharePath $Root "start_print_relay.bat"
    if (-not $probe) { return $false }
    try { return [System.IO.File]::Exists($probe) } catch { return $false }
}

function Resolve-TaxOpsShareRoot([string]$Hint) {
    # Prefer UNC: mapped drives (T:) vanish under Admin / session-0 services.
    $candidates = New-Object System.Collections.Generic.List[string]
    if ($Hint) {
        $h = $Hint.Trim().TrimEnd('\')
        if ($h -match '^[A-Za-z]:$') {
            $candidates.Add($TaxOpsUncRoot) | Out-Null
            $candidates.Add($h) | Out-Null
        } else {
            $candidates.Add($h) | Out-Null
            $candidates.Add($TaxOpsUncRoot) | Out-Null
        }
    } else {
        $candidates.Add($TaxOpsUncRoot) | Out-Null
        $candidates.Add("T:") | Out-Null
    }
    foreach ($c in $candidates) {
        if (Test-ShareAlive $c) { return $c.TrimEnd('\') }
    }
    return $TaxOpsUncRoot
}

function Resolve-ReceptionScript([string]$Name) {
    if ($scriptDir) {
        $local = Join-Path $scriptDir $Name
        if (Test-Path -LiteralPath $local) { return $local }
    }
    if ($ShareRoot) {
        $share = Join-SharePath $ShareRoot ("taxops\scripts\{0}" -f $Name)
        if ($share -and [System.IO.File]::Exists($share)) { return $share }
    }
    return $null
}

if (-not $ShareRoot) {
    $mark = "C:\TaxOps\Reception\share_root.txt"
    if (Test-Path -LiteralPath $mark) {
        $ShareRoot = (Get-Content -LiteralPath $mark -Raw -EA SilentlyContinue).Trim()
    }
}
if (-not $ShareRoot) {
    # Only walk up when living under share ...\taxops\scripts\ (not C:\TaxOps\Reception\scripts)
    if ($scriptPath -and ($scriptPath -match '[\\/]taxops[\\/]scripts[\\/]')) {
        $ShareRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $scriptPath))
    }
}
$ShareRoot = Resolve-TaxOpsShareRoot $ShareRoot
if ($ShareRoot -match '[<>\|\?\*"]') {
    Bad ("Illegal characters in ShareRoot: {0}" -f $ShareRoot)
    exit 3
}

# Persist UNC so the next elevated run does not revive a dead T:
try {
    $markDir = "C:\TaxOps\Reception"
    if (-not (Test-Path -LiteralPath $markDir)) {
        New-Item -ItemType Directory -Path $markDir -Force | Out-Null
    }
    Set-Content -LiteralPath (Join-Path $markDir "share_root.txt") -Value $ShareRoot -Encoding ASCII
} catch {}

$isAdmin = Test-IsAdmin
$scanToken = Get-ScanToken $ShareRoot

Say "TaxOps reception  -  ensure relays + verify auto-start" "White"
Say ("ShareRoot: {0}" -f $ShareRoot) "DarkGray"
Say ("ScriptDir: {0}" -f $scriptDir) "DarkGray"
Say ("Admin: {0}  Repair: {1}  NoStart: {2}" -f $isAdmin, [bool]$Repair, [bool]$NoStart) "DarkGray"
if ($isAdmin -and $ShareRoot -match '^[A-Za-z]:') {
    Warn "Elevated session + mapped drive ShareRoot - prefer UNC \\Xcel-server\taxops"
}

# ============================================================
Section "PRINT RELAY (TCP $PrintPort)"
$printSvc = Get-Service -Name "FiletrackRelay" -EA SilentlyContinue
if ($printSvc) {
    Info ("Service FiletrackRelay: Status={0} StartType={1}" -f $printSvc.Status, $printSvc.StartType)
} else {
    Warn "FiletrackRelay Windows service not installed"
}

$ph = Test-PrintHealth $PrintPort
if ($ph.ok) {
    Ok ("Print healthy  -  printer={0}" -f $ph.body.printer_configured)
} elseif ($ph.up) {
    Warn ("Print up but printer_found=false configured={0}" -f $ph.body.printer_configured)
} else {
    Info ("Print down: {0}" -f $ph.error)
    if (-not $NoStart) {
        if ($printSvc) {
            $started = Try-StartWindowsService "FiletrackRelay" 8
            if ($started.ok) {
                Info "FiletrackRelay Running"
            } else {
                Warn ("FiletrackRelay start incomplete: status={0} err={1}" -f $started.status, $started.error)
                # Common cause: NSSM AppDirectory set to T:\taxops (mapped drives invisible in session 0)
                $nssmFix = $null
                foreach ($c in @("C:\TaxOps\nssm\nssm.exe", "C:\Tools\nssm\nssm.exe")) {
                    if (Test-Path -LiteralPath $c) { $nssmFix = $c; break }
                }
                if (-not $nssmFix) {
                    $nssmFix = (Get-Command nssm.exe -EA SilentlyContinue | Select-Object -ExpandProperty Source)
                }
                if ($nssmFix -and $isAdmin) {
                    $uncApp = Join-SharePath $TaxOpsUncRoot "taxops"
                    Info ("Repairing FiletrackRelay AppDirectory -> {0}" -f $uncApp)
                    try {
                        & $nssmFix set FiletrackRelay AppDirectory $uncApp | Out-Null
                        # Kill wedged service process then timed start
                        & sc.exe stop FiletrackRelay | Out-Null
                        Start-Sleep -Seconds 1
                        $started = Try-StartWindowsService "FiletrackRelay" 8
                        if (-not $started.ok) {
                            Warn ("Still not Running after UNC repair: {0}" -f $started.status)
                        }
                    } catch {
                        Warn ("NSSM AppDirectory repair failed: {0}" -f $_.Exception.Message)
                        Warn "Re-run: GO_RECEPTION.bat -Repair"
                    }
                } else {
                    Warn "If AppDirectory is on mapped T:, re-run GO_RECEPTION.bat -Repair"
                }
            }
            $ph = Wait-Print $PrintPort 8
        }
        if (-not $ph.up) {
            $printBat = Join-SharePath $ShareRoot "start_print_relay.bat"
            if ($printBat -and [System.IO.File]::Exists($printBat)) {
                Info "Launching start_print_relay.bat (visible window)..."
                # Do NOT set WorkingDirectory to UNC — cmd.exe rejects it before the bat runs.
                Start-Process -FilePath $printBat
                $ph = Wait-Print $PrintPort 10
            } else {
                Bad "start_print_relay.bat missing"
            }
        }
        if ($ph.ok) { Ok "Print relay started" }
        elseif ($ph.up) { Warn "Print started but printer not found" }
        else {
            Bad ("Print still down: {0}" -f $ph.error)
            Info "Continuing with scan agent check..."
        }
    } else {
        Bad "Print down (NoStart)"
    }
}

# ============================================================
Section "SCAN AGENT (TCP $ScanPort)"
$taskName = "TaxOps Scan Agent"
$task = Get-ScheduledTask -TaskName $taskName -EA SilentlyContinue
if ($task) {
    Info ("Scheduled task '{0}': State={1}" -f $taskName, $task.State)
} else {
    Warn "Scheduled task 'TaxOps Scan Agent' not installed (needed for logon auto-start)"
}

$scanSvc = Get-Service -Name "ScanAgent" -EA SilentlyContinue
if ($scanSvc) {
    Info ("Service ScanAgent: {0} (session-0  -  NOT used for WIA; ignore if Stopped)" -f $scanSvc.Status)
}

$sh = Test-ScanHealth $ScanPort $scanToken
if ($sh.ok) {
    Ok ("Scan healthy  -  code_rev={0} com_sta={1} wia_save={2}" -f `
        $sh.body.code_rev, $sh.body.com_sta, $sh.body.wia_save)
} else {
    Info ("Scan down: {0}" -f $sh.error)
    if (-not $NoStart) {
        if ($task) {
            Info "Starting Scheduled Task '$taskName'..."
            try {
                Start-ScheduledTask -TaskName $taskName -EA Stop
            } catch {
                Warn ("Start-ScheduledTask failed: {0}" -f $_.Exception.Message)
            }
            $sh = Wait-Scan $ScanPort $scanToken 45
        }
        if (-not $sh.ok) {
            $goBat = Join-SharePath $ShareRoot "GO_SCAN_AGENT.bat"
            $startBat = Join-SharePath $ShareRoot "start_scan_agent.bat"
            if ($startBat -and [System.IO.File]::Exists($startBat)) {
                Info "Launching start_scan_agent.bat (visible window - leave open)..."
                # Do NOT set WorkingDirectory to UNC — cmd.exe rejects it.
                Start-Process -FilePath $startBat
                $sh = Wait-Scan $ScanPort $scanToken 90
            } elseif ($goBat -and [System.IO.File]::Exists($goBat)) {
                Info "Launching GO_SCAN_AGENT.bat..."
                Start-Process -FilePath $goBat
                $sh = Wait-Scan $ScanPort $scanToken 90
            } else {
                Bad "start_scan_agent.bat / GO_SCAN_AGENT.bat missing"
            }
        }
        if ($sh.ok) { Ok "Scan agent started" }
        else { Bad ("Scan still down: {0}" -f $sh.error) }
    } else {
        Bad "Scan down (NoStart)"
    }
}

# ============================================================
Section "AUTO-START verification"
$autoOk = $true

# Print: NSSM service Auto
if ($printSvc) {
    $startType = "$($printSvc.StartType)"
    if (-not $startType -or $startType -eq "") {
        try { $startType = "$((Get-Service FiletrackRelay).StartType)" } catch { $startType = "?" }
    }
    if ($startType -eq "Auto" -or $startType -eq "Automatic") {
        Ok "Print auto-start: FiletrackRelay StartType=Automatic"
    } else {
        Bad ("Print auto-start: FiletrackRelay StartType={0} (want Automatic)" -f $startType)
        $autoOk = $false
    }
} else {
    Bad "Print auto-start: FiletrackRelay service missing"
    $autoOk = $false
}

# Scan: Scheduled Task at logon, Interactive
if ($task) {
    $okTrigger = $false
    foreach ($tr in @($task.Triggers)) {
        # LogonTrigger = 9 in CIM; AtLogOn in ScheduledTask cmdlets
        if ($tr.CimClass.CimClassName -match "Logon" -or "$($tr.GetType().Name)" -match "Logon") {
            $okTrigger = $true
        }
    }
    # Fallback: XML check
    if (-not $okTrigger) {
        try {
            $xml = Export-ScheduledTask -TaskName $taskName -EA Stop
            if ($xml -match "LogonTrigger" -or $xml -match "<LogonTrigger>") { $okTrigger = $true }
        } catch {}
    }
    $principal = $task.Principal
    $logonType = "$($principal.LogonType)"
    if ($okTrigger) {
        Ok ("Scan auto-start: task '{0}' has AtLogOn trigger" -f $taskName)
    } else {
        Bad "Scan auto-start: task exists but AtLogOn trigger not found"
        $autoOk = $false
    }
    if ($logonType -match "Interactive") {
        Ok ("Scan logon type: Interactive (required for WIA)")
    } else {
        Warn ("Scan logon type: {0} (Interactive recommended for WIA)" -f $logonType)
    }
    Info "Note: scanning needs a logged-on Windows session (not a session-0 service)."
} else {
    Bad "Scan auto-start: Scheduled Task 'TaxOps Scan Agent' missing"
    $autoOk = $false
}

# Firewall named rules
foreach ($pair in @(
    @{ Port = $PrintPort; Names = @("TaxOps Filetrack Print Relay 8765", "TaxOps Print Relay") },
    @{ Port = $ScanPort; Names = @("TaxOps Scan Agent 8766", "TaxOps Scan Agent") }
)) {
    $found = $false
    foreach ($n in $pair.Names) {
        $r = Get-NetFirewallRule -DisplayName $n -EA SilentlyContinue |
            Where-Object { $_.Enabled -eq $true -and $_.Direction -eq "Inbound" } |
            Select-Object -First 1
        if ($r) { $found = $true; Ok ("Firewall: {0}" -f $n); break }
    }
    if (-not $found) {
        Warn ("Firewall: no TaxOps inbound rule for TCP {0}" -f $pair.Port)
    }
}

# ============================================================
if ($Repair -and -not $autoOk) {
    Section "REPAIR auto-start"
    if (-not $isAdmin) {
        Bad "Repair needs Administrator  -  re-run GO_RECEPTION.bat and approve UAC"
    } else {
        $printInstall = Resolve-ReceptionScript "install_print_relay_service.ps1"
        $scanInstall = Resolve-ReceptionScript "install_scan_agent_task.ps1"
        if (-not $printSvc -or ($printSvc.StartType -ne "Automatic")) {
            if ($printInstall) {
                Info "Running install_print_relay_service.ps1 ..."
                & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $printInstall -ShareRoot $ShareRoot
            } else { Bad "Missing install_print_relay_service.ps1" }
        }
        if (-not $task) {
            if ($scanInstall) {
                Info "Running install_scan_agent_task.ps1 ..."
                & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $scanInstall -ShareRoot $ShareRoot
            } else { Bad "Missing install_scan_agent_task.ps1" }
        }
        # Re-check
        $printSvc = Get-Service FiletrackRelay -EA SilentlyContinue
        $task = Get-ScheduledTask -TaskName $taskName -EA SilentlyContinue
        if ($printSvc -and $printSvc.StartType -eq "Automatic") { Ok "Print service repaired" }
        else { Bad "Print service still not Automatic" }
        if ($task) { Ok "Scan task repaired" }
        else { Bad "Scan task still missing" }
    }
}

# ============================================================
Section "FINAL health (check_reception)"
$check = Resolve-ReceptionScript "check_reception.ps1"
if ($check) {
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $check -PrintPort $PrintPort -ScanPort $ScanPort
    if ($LASTEXITCODE -ne 0) { $script:fail++ }
} else {
    $ph2 = Test-PrintHealth $PrintPort
    $sh2 = Test-ScanHealth $ScanPort $scanToken
    if ($ph2.ok) { Ok "PRINT OK" } else { Bad ("PRINT FAIL {0}" -f $ph2.error) }
    if ($sh2.ok) { Ok "SCAN OK" } else { Bad ("SCAN FAIL {0}" -f $sh2.error) }
}

Section "SUMMARY"
Say ("  FAIL={0}  WARN={1}" -f $script:fail, $script:warn) "White"
if ($script:fail -eq 0) {
    Say "  RESULT: PASS  -  relays up; auto-start checked" "Green"
    Say "  Print survives reboot via FiletrackRelay service." "DarkGray"
    Say "  Scan starts at Windows logon via task (needs interactive login)." "DarkGray"
} else {
    Say "  RESULT: FAIL  -  see [FAIL] lines above" "Red"
    Say "  Deep dive: diagnose_reception_relays.bat" "Yellow"
    Say "  Repair auto-start: GO_RECEPTION.bat -Repair  (Admin)" "Yellow"
}

if ($script:fail -gt 0) { exit 1 }
exit 0
