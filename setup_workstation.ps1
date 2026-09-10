#Requires -Version 5.1
<#
.SYNOPSIS
  One-time desk setup: map office network drives, route hostname "taxlog",
  and put a Tax Log shortcut on the Desktop.

.DESCRIPTION
  For any office workstation (not reception-specific). Double-click:
    \\Xcel-server\taxops\SETUP_WORKSTATION.bat
  or, once T: is mapped:
    T:\SETUP_WORKSTATION.bat

  Maps and verifies all standard Xcel-server drives:
    F:  \\Xcel-server\ACCNTING
    P:  \\Xcel-server\PUBLIC
    Q:  \\Xcel-server\QUICKBOOKS
    T:  \\Xcel-server\taxops

  Tax Log URL (default): http://192.168.1.173:5000
  Friendly hosts name:   http://taxlog:5000

  Does NOT install scan/print relays — use T:\GO_RECEPTION.bat on reception only.

.PARAMETER ServerIP
  TaxOps / Tax Log LAN IP (NSSM host). Default 192.168.1.173

.PARAMETER Hostname
  Friendly name written to hosts. Default taxlog -> http://taxlog:5000

.PARAMETER AppPort
  TaxOps HTTP port. Default 5000

.PARAMETER SkipHosts
  Do not edit hosts (no Admin prompt for that step).

.PARAMETER SkipShortcut
  Do not create Desktop shortcut.

.PARAMETER SkipDriveMap
  Only check drives; do not create/fix mappings.
#>
param(
    [string]$ServerIP    = "192.168.1.173",
    [string]$Hostname    = "taxlog",
    [string]$FileServer  = "Xcel-server",
    [int]$AppPort        = 5000,
    [switch]$SkipHosts,
    [switch]$SkipShortcut,
    [switch]$SkipDriveMap,
    [switch]$ElevatedHostsPass
)

$ErrorActionPreference = "Continue"

# Canonical office drive letters (from \\Xcel-server share list + existing net use).
$script:RequiredDrives = @(
    @{ Letter = "F"; Unc = "\\$FileServer\ACCNTING";   Label = "Accounting" }
    @{ Letter = "P"; Unc = "\\$FileServer\PUBLIC";     Label = "Public" }
    @{ Letter = "Q"; Unc = "\\$FileServer\QUICKBOOKS"; Label = "QuickBooks" }
    @{ Letter = "T"; Unc = "\\$FileServer\taxops";     Label = "TaxOps" }
)

$TaxLogUrl   = "http://${Hostname}:${AppPort}"
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

function Invoke-NetUseTimed([string]$ArgsLine, [int]$TimeoutSec = 15) {
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

function Get-MappedRoot([string]$Letter) {
    $drive = "${Letter}:"
    try {
        $ps = Get-PSDrive -Name $Letter -PSProvider FileSystem -EA SilentlyContinue
        if ($ps -and $ps.DisplayRoot) {
            return $ps.DisplayRoot.TrimEnd('\')
        }
    } catch {}
    $view = Invoke-NetUseTimed $drive
    if ($view.ExitCode -eq 0 -and $view.Text -match '\\\\[^\s]+') {
        $m = [regex]::Match($view.Text, '(\\\\[^\s]+)')
        if ($m.Success) { return $m.Groups[1].Value.TrimEnd('\') }
    }
    return $null
}

function Test-UncReachable([string]$Unc) {
    try {
        return [bool](Test-Path -LiteralPath $Unc -EA SilentlyContinue)
    } catch {
        return $false
    }
}

function Ensure-NetworkDriveMapping {
    param(
        [Parameter(Mandatory)] [string]$Letter,
        [Parameter(Mandatory)] [string]$Unc,
        [Parameter(Mandatory)] [string]$Label
    )
    $drive = "${Letter}:"
    $uncNorm = $Unc.TrimEnd('\')
    Work ("Checking {0} -> {1} ({2}) ..." -f $drive, $uncNorm, $Label)

    if (-not (Test-UncReachable $uncNorm)) {
        # Try alternate casing for PUBLIC/public etc.
        $alt = $uncNorm
        if ($uncNorm -match '\\PUBLIC$') {
            $alt = ($uncNorm -replace '\\PUBLIC$', '\public')
        }
        if ($alt -ne $uncNorm -and (Test-UncReachable $alt)) {
            $uncNorm = $alt
        } else {
            Bad ("Share not reachable: {0}" -f $uncNorm)
            Info ("Confirm you can open {0} in File Explorer (LAN / credentials)." -f $uncNorm)
            return $false
        }
    }

    $current = Get-MappedRoot $Letter
    if ($current) {
        if ($current -ieq $uncNorm) {
            if (Test-Path -LiteralPath $drive -EA SilentlyContinue) {
                Good ("{0} already maps to {1}" -f $drive, $uncNorm)
                return $true
            }
        } else {
            Warn ("{0} currently maps to {1} — reconnecting to {2}" -f $drive, $current, $uncNorm)
            if ($SkipDriveMap) {
                Bad ("Wrong mapping on {0} (expected {1})" -f $drive, $uncNorm)
                return $false
            }
            $null = Invoke-NetUseTimed ("{0} /delete /y" -f $drive)
        }
    }

    if ($SkipDriveMap) {
        if ($current -and ($current -ieq $uncNorm)) {
            Good ("{0} OK (check-only)" -f $drive)
            return $true
        }
        Bad ("{0} not mapped to {1} (check-only; re-run without -SkipDriveMap)" -f $drive, $uncNorm)
        return $false
    }

    # Fresh persistent map
    $null = Invoke-NetUseTimed ("{0} /delete /y" -f $drive)
    $map = Invoke-NetUseTimed ('{0} "{1}" /persistent:yes' -f $drive, $uncNorm)
    if ($map.TimedOut) {
        Bad ("Mapping {0} timed out — is \\{1} reachable?" -f $drive, $FileServer)
        Info $map.Text
        return $false
    }
    if ($map.ExitCode -ne 0) {
        Bad ("net use {0} failed (exit {1})" -f $drive, $map.ExitCode)
        Info $map.Text
        return $false
    }
    if (Test-Path -LiteralPath $drive -EA SilentlyContinue) {
        Good ("Mapped {0} -> {1} (persistent)" -f $drive, $uncNorm)
        return $true
    }
    Warn ("Mapped {0} but path not visible yet — refresh Explorer" -f $drive)
    return $true
}

function Test-AllNetworkDrives {
    Say ""
    Say "  Network drives" "Cyan"
    $allOk = $true
    foreach ($d in $script:RequiredDrives) {
        $ok = Ensure-NetworkDriveMapping -Letter $d.Letter -Unc $d.Unc -Label $d.Label
        if (-not $ok) { $allOk = $false }
    }

    # Extra shares on the server (informational — not required on every desk)
    Work ("Listing other shares on \\{0} ..." -f $FileServer)
    $view = Invoke-NetUseTimed ("view \\{0}" -f $FileServer) 20
    if ($view.ExitCode -eq 0 -and $view.Text) {
        $known = @{}
        foreach ($d in $script:RequiredDrives) {
            $share = ($d.Unc -split '\\')[-1]
            $known[$share.ToUpperInvariant()] = $true
        }
        $lines = $view.Text -split "`r?`n"
        $extras = @()
        foreach ($line in $lines) {
            if ($line -match '^\s*([A-Za-z0-9][A-Za-z0-9 _-]+)\s+Disk\b') {
                $name = $Matches[1].Trim()
                if (-not $known.ContainsKey($name.ToUpperInvariant())) {
                    $extras += $name
                }
            }
        }
        if ($extras.Count -gt 0) {
            Info ("Other Disk shares (not auto-mapped): {0}" -f ($extras -join ", "))
            Info "Scan folders (FRONT SCAN, etc.) stay per-user — map manually if needed."
        } else {
            Info "No extra Disk shares beyond F/P/Q/T."
        }
    } else {
        Warn ("Could not list shares on \\{0} (net view failed)" -f $FileServer)
    }
    return $allOk
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
        Work "hosts needs Administrator — prompting UAC..."
        $self = $PSCommandPath
        if (-not $self) { $self = $MyInvocation.MyCommand.Path }
        $arg = @(
            "-NoProfile", "-ExecutionPolicy", "Bypass",
            "-File", $self,
            "-ServerIP", $ServerIP,
            "-Hostname", $Hostname,
            "-FileServer", $FileServer,
            "-AppPort", "$AppPort",
            "-ElevatedHostsPass"
        )
        if ($SkipShortcut) { $arg += "-SkipShortcut" }
        if ($SkipDriveMap) { $arg += "-SkipDriveMap" }
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

    # Prefer IP URL so the shortcut works even before hosts is fixed.
    $primaryUrl = $TaxLogUrlIp
    $urlPath = Join-Path $desktop "Tax Log.url"
    try {
        @(
            "[InternetShortcut]"
            "URL=$primaryUrl"
        ) | Set-Content -LiteralPath $urlPath -Encoding ASCII
        Good "Desktop shortcut: $urlPath"
        Info "Opens $primaryUrl"
    } catch {
        Warn "URL shortcut failed: $($_.Exception.Message)"
    }

    # Also write friendly-name .url if hosts is set
    if ((Get-HostsIP $Hostname) -eq $ServerIP) {
        try {
            $namePath = Join-Path $desktop "Tax Log (taxlog).url"
            @(
                "[InternetShortcut]"
                "URL=$TaxLogUrl"
            ) | Set-Content -LiteralPath $namePath -Encoding ASCII
            Good "Desktop shortcut: $namePath"
        } catch {}
    }

    try {
        $shareLnk = Join-Path $desktop "TaxOps Share (T).lnk"
        $w2 = New-Object -ComObject WScript.Shell
        $sc2 = $w2.CreateShortcut($shareLnk)
        $sc2.TargetPath = "T:\"
        $sc2.Description = "TaxOps share \\$FileServer\taxops"
        $sc2.Save()
        Good "Desktop shortcut: $shareLnk"
    } catch {
        Info "Share shortcut skipped"
    }
}

function Test-TaxLogReachable {
    $healthIp = $TaxLogUrlIp + "/health"
    $healthName = $TaxLogUrl + "/health"
    Work ("Checking Tax Log at {0}:{1} ..." -f $ServerIP, $AppPort)
    if (-not (Test-TcpPort $ServerIP $AppPort 4000)) {
        Bad ("Cannot reach {0}:{1} — TaxOps service down, wrong IP, or firewall" -f $ServerIP, $AppPort)
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

    if ((Get-HostsIP $Hostname) -eq $ServerIP) {
        try {
            $r2 = Invoke-WebRequest -Uri $healthName -UseBasicParsing -TimeoutSec 8
            if ($r2.StatusCode -eq 200) {
                Good ("Hostname route OK ({0})" -f $healthName)
            }
        } catch {
            Warn ("hosts set but {0} failed — try IP URL or: ipconfig /flushdns" -f $healthName)
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
Say "  File server : \\$FileServer" "Gray"
Say "  Drives      : F: ACCNTING  P: PUBLIC  Q: QUICKBOOKS  T: taxops" "Gray"
Say "  Tax Log URL : $TaxLogUrlIp" "Gray"
Say "  Hostname    : $TaxLogUrl  ($ServerIP)" "Gray"
Say ""

Work ("Probing file server \\{0} ..." -f $FileServer)
if ((Test-TcpPort $FileServer 445 4000) -or (Test-UncReachable "\\$FileServer\taxops")) {
    Good ("File server reachable: \\{0}" -f $FileServer)
} else {
    Bad ("Cannot reach \\{0} (SMB / LAN)" -f $FileServer)
    Info "Check LAN / Wi-Fi (not guest), VPN, and that Xcel-server is on."
}

$null = Test-AllNetworkDrives
Ensure-HostsEntry
Ensure-DesktopShortcut
Test-TaxLogReachable

# Final T: sanity for TaxOps tree
if (Test-Path -LiteralPath "T:\taxops" -EA SilentlyContinue) {
    Good "T:\taxops folder visible"
} elseif (Test-Path -LiteralPath "T:\" -EA SilentlyContinue) {
    Warn "T: mapped but taxops\ folder not visible yet"
} else {
    Bad "T: not available — TaxOps share mapping failed"
}

Say ""
Say "========================================================" "Cyan"
Say ("  Done.  OK={0}  WARN={1}  FAIL={2}" -f $script:ok, $script:warn, $script:fail) "Cyan"
Say "========================================================" "Cyan"
Say ""
Say "  Open Tax Log:  $TaxLogUrlIp" "White"
Say "  Or by name:    $TaxLogUrl" "White"
Say "  Drives:        F:  P:  Q:  T:" "White"
Say ""
if ($script:fail -gt 0) {
    Say "  Fix FAIL lines above, then re-run this script." "Yellow"
    exit 1
}
exit 0
