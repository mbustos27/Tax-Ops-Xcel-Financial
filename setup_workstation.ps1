#Requires -Version 5.1
<#
.SYNOPSIS
  One-time desk setup: map T: to the TaxOps share, route hostname "taxlog",
  and put a Tax Log shortcut on the Desktop.

.DESCRIPTION
  For any office workstation (not reception-specific). Double-click:
    \\Xcel-server\taxops\SETUP_WORKSTATION.bat
  or, once T: is mapped:
    T:\SETUP_WORKSTATION.bat

  Does NOT install scan/print relays - use T:\GO_RECEPTION.bat on reception only.

.PARAMETER ServerIP
  TaxOps / Tax Log LAN IP (NSSM host). Default 192.168.1.173

.PARAMETER Hostname
  Friendly name written to hosts. Default taxlog -> http://taxlog:5000

.PARAMETER UncRoot
  Share to map as T:. Default \\Xcel-server\taxops

.PARAMETER DriveLetter
  Drive letter for the share. Default T

.PARAMETER AppPort
  TaxOps HTTP port. Default 5000

.PARAMETER SkipHosts
  Do not edit hosts (no Admin prompt for that step).

.PARAMETER SkipShortcut
  Do not create Desktop shortcut.
#>
param(
    [string]$ServerIP     = "192.168.1.173",
    [string]$Hostname     = "taxlog",
    [string]$UncRoot      = "\\Xcel-server\taxops",
    [string]$DriveLetter  = "T",
    [int]$AppPort         = 5000,
    [switch]$SkipHosts,
    [switch]$SkipShortcut,
    [switch]$ElevatedHostsPass
)

$ErrorActionPreference = "Continue"
$UncRoot = $UncRoot.TrimEnd('\')
$DriveLetter = $DriveLetter.TrimEnd(':').ToUpperInvariant()
$Drive = "${DriveLetter}:"
$TaxLogUrl = "http://${Hostname}:${AppPort}"
$TaxLogUrlIp = "http://${ServerIP}:${AppPort}"

$script:ok = 0
$script:warn = 0
$script:fail = 0

function Say($m, $c = "White") { Write-Host $m -ForegroundColor $c }
function Good($m) { Say ("  [OK]   {0}" -f $m) "Green";  $script:ok++ }
function Bad($m)  { Say ("  [FAIL] {0}" -f $m) "Red";    $script:fail++ }
function Warn($m) { Say ("  [WARN] {0}" -f $m) "Yellow"; $script:warn++ }
function Info($m) { Say ("         {0}" -f $m) "DarkGray" }
function Work($m) { Say ("  ...    {0}" -f $m) "Cyan" }

function Test-IsAdmin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Invoke-NetUseTimed([string]$ArgsLine, [int]$TimeoutSec = 12) {
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
        Text     = ($p.StandardOutput.ReadToEnd() + $p.StandardError.ReadToEnd()).Trim()
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
        if ($line -notmatch "^\s*#" -and $line -match "^\s*([0-9a-fA-F.:]+)\s+.*\b$esc\b") {
            "# old: $line"
        } else {
            $line
        }
    }
    ($out + ("{0}`t{1}`t# TaxOps workstation setup {2}" -f $IP, $Name, $stamp)) |
        Set-Content $hf -Encoding ASCII
}

function Test-TcpPort([string]$Computer, [int]$Port, [int]$Ms = 3000) {
    try {
        $c = New-Object System.Net.Sockets.TcpClient
        $ar = $c.BeginConnect($Computer, $Port, $null, $null)
        if (-not $ar.AsyncWaitHandle.WaitOne($Ms, $false)) {
            try { $c.Close() } catch {}
            return $false
        }
        try { $c.EndConnect($ar) | Out-Null; $c.Close(); return $true }
        catch { try { $c.Close() } catch {}; return $false }
    } catch { return $false }
}

function Ensure-DriveMapping {
    Work "Checking $Drive -> $UncRoot ..."

    # Already correct?
    try {
        $existing = Get-PSDrive -Name $DriveLetter -PSProvider FileSystem -EA SilentlyContinue
        if ($existing -and $existing.DisplayRoot) {
            $root = $existing.DisplayRoot.TrimEnd('\')
            if ($root -ieq $UncRoot) {
                if (Test-Path -LiteralPath (Join-Path $Drive "taxops")) {
                    Good "$Drive already maps to $UncRoot"
                    return
                }
            } else {
                Warn "$Drive currently maps to $root - reconnecting to $UncRoot"
                $null = Invoke-NetUseTimed ("{0} /delete /y" -f $Drive)
            }
        }
    } catch {}

    # net use may already have it
    $view = Invoke-NetUseTimed $Drive
    if ($view.ExitCode -eq 0 -and $view.Text -match [regex]::Escape($UncRoot)) {
        Good "$Drive already connected to $UncRoot"
        return
    }

    # Disconnect stale letter then map persistent
    $null = Invoke-NetUseTimed ("{0} /delete /y" -f $Drive)
    $map = Invoke-NetUseTimed ("{0} `"{1}`" /persistent:yes" -f $Drive, $UncRoot)
    if ($map.TimedOut) {
        Bad "Mapping $Drive timed out - is \\Xcel-server reachable on the LAN?"
        Info $map.Text
        return
    }
    if ($map.ExitCode -ne 0) {
        Bad "net use $Drive failed (exit $($map.ExitCode))"
        Info $map.Text
        Info "Open File Explorer -> \\Xcel-server\taxops and confirm you can browse it."
        return
    }
    if (Test-Path -LiteralPath (Join-Path $Drive "taxops")) {
        Good "Mapped $Drive -> $UncRoot (persistent)"
    } else {
        Warn "Mapped $Drive but taxops\ folder not visible yet - refresh Explorer"
    }
}

function Ensure-HostsEntry {
    if ($SkipHosts) {
        Info "Skipping hosts (-SkipHosts)"
        return
    }

    $cur = Get-HostsIP $Hostname
    if ($cur -eq $ServerIP) {
        Good "hosts already has $Hostname -> $ServerIP"
        return
    }

    if (-not (Test-IsAdmin)) {
        Work "hosts needs Administrator - prompting UAC..."
        $self = $PSCommandPath
        if (-not $self) { $self = $MyInvocation.MyCommand.Path }
        $arg = @(
            "-NoProfile", "-ExecutionPolicy", "Bypass",
            "-File", $self,
            "-ServerIP", $ServerIP,
            "-Hostname", $Hostname,
            "-UncRoot", $UncRoot,
            "-DriveLetter", $DriveLetter,
            "-AppPort", "$AppPort",
            "-ElevatedHostsPass"
        )
        if ($SkipShortcut) { $arg += "-SkipShortcut" }
        $p = Start-Process -FilePath "powershell.exe" -Verb RunAs -Wait -PassThru -ArgumentList $arg
        if ($p.ExitCode -eq 0 -and (Get-HostsIP $Hostname) -eq $ServerIP) {
            Good "hosts updated: $Hostname -> $ServerIP"
        } else {
            Warn "hosts not updated (UAC cancelled or write failed)"
            Info "You can still use $TaxLogUrlIp"
        }
        return
    }

    try {
        Set-HostsIP $ServerIP $Hostname
        if ((Get-HostsIP $Hostname) -eq $ServerIP) {
            Good "hosts updated: $Hostname -> $ServerIP"
        } else {
            Bad "hosts write did not stick"
        }
    } catch {
        Bad "hosts write failed: $($_.Exception.Message)"
    }
}

function Ensure-DesktopShortcut {
    if ($SkipShortcut) {
        Info "Skipping Desktop shortcut (-SkipShortcut)"
        return
    }

    $desktop = [Environment]::GetFolderPath("Desktop")
    if (-not $desktop) {
        Warn "No Desktop folder found"
        return
    }

    $lnkPath = Join-Path $desktop "Tax Log.lnk"
    try {
        $w = New-Object -ComObject WScript.Shell
        $sc = $w.CreateShortcut($lnkPath)
        $sc.TargetPath = $TaxLogUrl
        $sc.Description = "TaxOps / Tax Log ($TaxLogUrl)"
        # Prefer edge/default browser via URL-style .lnk TargetPath = http://...
        $sc.Save()
        Good "Desktop shortcut: $lnkPath"
        Info "Opens $TaxLogUrl"
    } catch {
        # Some Windows builds dislike http TargetPath on .lnk - fall back to .url
        try {
            $urlPath = Join-Path $desktop "Tax Log.url"
            @(
                "[InternetShortcut]"
                "URL=$TaxLogUrl"
            ) | Set-Content -LiteralPath $urlPath -Encoding ASCII
            Good "Desktop shortcut: $urlPath"
        } catch {
            Warn "Shortcut skipped: $($_.Exception.Message)"
        }
    }

    # Share explorer shortcut (optional convenience)
    try {
        $shareLnk = Join-Path $desktop "TaxOps Share (T).lnk"
        $w2 = New-Object -ComObject WScript.Shell
        $sc2 = $w2.CreateShortcut($shareLnk)
        $sc2.TargetPath = $Drive
        $sc2.Description = "TaxOps share $UncRoot"
        $sc2.Save()
        Good "Desktop shortcut: $shareLnk"
    } catch {
        Info "Share shortcut skipped"
    }
}

function Test-TaxLogReachable {
    $healthIp = $TaxLogUrlIp + '/health'
    $healthName = $TaxLogUrl + '/health'
    Work ("Checking Tax Log at {0}:{1} ..." -f $ServerIP, $AppPort)
    if (-not (Test-TcpPort $ServerIP $AppPort 4000)) {
        Bad ("Cannot reach {0}:{1} - TaxOps service down, wrong IP, or firewall" -f $ServerIP, $AppPort)
        Info ("From this PC try:  curl.exe {0}" -f $healthIp)
        return
    }
    try {
        $r = Invoke-WebRequest -Uri $healthIp -UseBasicParsing -TimeoutSec 8
        if ($r.StatusCode -eq 200) {
            Good ("Tax Log health OK ({0})" -f $healthIp)
        } else {
            Warn ("Tax Log responded HTTP {0}" -f $r.StatusCode)
        }
    } catch {
        Warn ("Port open but /health failed: {0}" -f $_.Exception.Message)
        Info ("Browser may still work at {0}" -f $TaxLogUrlIp)
    }

    # Name route (after hosts)
    if ((Get-HostsIP $Hostname) -eq $ServerIP) {
        try {
            $r2 = Invoke-WebRequest -Uri $healthName -UseBasicParsing -TimeoutSec 8
            if ($r2.StatusCode -eq 200) {
                Good ("Hostname route OK ({0})" -f $healthName)
            }
        } catch {
            Warn ("hosts set but {0} failed - try IP URL or flush DNS: ipconfig /flushdns" -f $healthName)
        }
    }
}

# ----- Elevated hosts-only pass (UAC child) -----
if ($ElevatedHostsPass) {
    try {
        Set-HostsIP $ServerIP $Hostname
        if ((Get-HostsIP $Hostname) -eq $ServerIP) { exit 0 }
    } catch {}
    exit 1
}

# ----- Main -----
try { $Host.UI.RawUI.WindowTitle = "TaxOps Workstation Setup" } catch {}

Say ""
Say "========================================================" "Cyan"
Say "  TaxOps workstation setup" "Cyan"
Say "========================================================" "Cyan"
Say "  Share : $UncRoot  ->  $Drive" "Gray"
Say "  Tax Log : $TaxLogUrl  ($ServerIP)" "Gray"
Say ""

# Prefer UNC reachability first (works before T: exists)
Work "Probing share $UncRoot ..."
if (Test-Path -LiteralPath $UncRoot) {
    Good "Share reachable: $UncRoot"
} else {
    Bad "Cannot reach $UncRoot"
    Info "Check LAN / Wi-Fi (not guest), VPN, and that Xcel-server is on."
    Info "You can still map the drive if credentials are needed - Explorer may prompt."
}

Ensure-DriveMapping
Ensure-HostsEntry
Ensure-DesktopShortcut
Test-TaxLogReachable

Say ""
Say "========================================================" "Cyan"
Say ("  Done.  OK={0}  WARN={1}  FAIL={2}" -f $script:ok, $script:warn, $script:fail) "Cyan"
Say "========================================================" "Cyan"
Say ""
Say "  Open Tax Log:  $TaxLogUrl" "White"
Say "  Or by IP:      $TaxLogUrlIp" "White"
Say "  Share:         $Drive   ($UncRoot)" "White"
Say ""
if ($script:fail -gt 0) {
    Say "  Fix FAIL lines above, then re-run this script." "Yellow"
    exit 1
}
exit 0
