<#
.SYNOPSIS
    Sets up the Brother DCP-8065DN on a workstation: driver, port, queue, test.

.DESCRIPTION
    Run this ON THE NEW WORKSTATION. It works through the setup in order and
    stops with a clear message if a step cannot complete:

      1. Verifies the printer answers on the network
      2. Locates or stages the Brother print driver
      3. Creates the TCP/IP port
      4. Creates the print queue (or repoints an existing one)
      5. Optionally sends a test page

    If the driver is not already present, point -DriverSource at a folder
    containing the Brother INF -- copied from a machine that already has it,
    or extracted from Brother's download.

.PARAMETER PrinterIP
    Address of the printer. Leading zeros are stripped.

.PARAMETER PrinterName
    Display name for the queue in Windows.

.PARAMETER DriverSource
    Folder holding the Brother .inf. Defaults to the copy staged on the T: share.
    If T: is not mapped, the script falls back to the UNC path automatically.

.PARAMETER TestPage
    Send a test page once the queue exists.

.PARAMETER Force
    Recreate the queue even if one already exists on this name.

.EXAMPLE
    .\Setup-BrotherPrinter.ps1
    .\Setup-BrotherPrinter.ps1 -PrinterIP 192.168.1.35 -TestPage
    .\Setup-BrotherPrinter.ps1 -DriverSource "D:\BrotherDriver" -TestPage
#>

[CmdletBinding()]
param(
    [string] $PrinterIP    = "169.254.137.91",
    [string] $PrinterName  = "Brother DCP-8065DN",
    [string] $DriverSource = "T:\Drivers\prnbr001.inf_amd64_aad7da6d7cd23042",
    [switch] $TestPage,
    [switch] $Force
)

# If the mapped drive is not present on this workstation, fall back to UNC.
$DriverSourceFallback = "\\Xcel-server\taxops\Drivers\prnbr001.inf_amd64_aad7da6d7cd23042"

$ErrorActionPreference = 'Stop'

# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------

function Write-Step {
    param([string]$Text)
    Write-Host ""
    Write-Host "--- $Text " -ForegroundColor Cyan -NoNewline
    Write-Host ("-" * [Math]::Max(0, 58 - $Text.Length)) -ForegroundColor Cyan
}

function Write-Ok   { param([string]$m) Write-Host "  [ OK ] $m" -ForegroundColor Green }
function Write-Warn { param([string]$m) Write-Host "  [WARN] $m" -ForegroundColor Yellow }
function Write-Bad  { param([string]$m) Write-Host "  [FAIL] $m" -ForegroundColor Red }
function Write-Info { param([string]$m) Write-Host "         $m" -ForegroundColor DarkGray }

function Stop-Setup {
    param([string]$Reason, [string[]]$NextSteps)
    Write-Host ""
    Write-Host "Setup stopped: $Reason" -ForegroundColor Red
    if ($NextSteps) {
        Write-Host ""
        Write-Host "Next steps:" -ForegroundColor Yellow
        $i = 1
        foreach ($s in $NextSteps) { Write-Host "  $i. $s" -ForegroundColor Yellow; $i++ }
    }
    Write-Host ""
    exit 1
}

# ------------------------------------------------------------------
# preflight
# ------------------------------------------------------------------

Write-Host ""
Write-Host "Brother DCP-8065DN workstation setup" -ForegroundColor White
Write-Host "Host: $env:COMPUTERNAME    User: $env:USERNAME    $(Get-Date -Format 'yyyy-MM-dd HH:mm')" -ForegroundColor DarkGray

$isAdmin = ([Security.Principal.WindowsPrincipal] `
            [Security.Principal.WindowsIdentity]::GetCurrent()
           ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Stop-Setup "this script needs elevation" @(
        "Close this window.",
        "Right-click PowerShell and choose 'Run as Administrator'.",
        "Re-run the script."
    )
}

# Strip leading zeros -- panels pad octets to three digits
$rawIP = $PrinterIP
$PrinterIP = ($PrinterIP -split '\.' | ForEach-Object { [int]($_ -replace '^0+(?=\d)', '') }) -join '.'
if ($rawIP -ne $PrinterIP) {
    Write-Info "Normalized '$rawIP' to '$PrinterIP'"
}

$PortName = "IP_$PrinterIP"

# ------------------------------------------------------------------
Write-Step "1. Reaching the printer"

function Test-Port {
    param([string]$IP, [int]$Port, [int]$Timeout = 2000)
    $c = New-Object System.Net.Sockets.TcpClient
    try {
        $iar = $c.BeginConnect($IP, $Port, $null, $null)
        if (-not $iar.AsyncWaitHandle.WaitOne($Timeout, $false)) { return $false }
        $c.EndConnect($iar); return $c.Connected
    } catch { return $false } finally { $c.Close() }
}

$raw  = Test-Port -IP $PrinterIP -Port 9100
$web  = Test-Port -IP $PrinterIP -Port 80

if ($raw) { Write-Ok "Port 9100 (RAW printing) is open" }
else      { Write-Bad "Port 9100 is not reachable" }

if ($web) { Write-Ok "Port 80 (web admin) is open -- http://$PrinterIP" }

if (-not $raw) {
    $hints = @()
    if ($PrinterIP -like '169.254.*') {
        $local = Get-NetIPAddress -AddressFamily IPv4 |
                 Where-Object { $_.IPAddress -like '169.254.*' }
        if ($local) {
            $hints += "This is a link-local address and this machine has one too. Pin a route: New-NetRoute -DestinationPrefix $PrinterIP/32 -InterfaceAlias '$($local[0].InterfaceAlias)' -RouteMetric 1"
        } else {
            $hints += "This is a link-local (169.254.x) address but this machine has none, so it cannot be reached from here."
        }
        $hints += "Better: move the printer to a switch port, set BOOT Method to Auto on the panel, power-cycle, and use the DHCP address instead."
    } else {
        $hints += "Confirm the address on the printer panel under LAN > TCP/IP > IP Address."
        $hints += "Check the link LED on the printer's ethernet jack."
    }
    $hints += "Re-run with the correct address: .\Setup-BrotherPrinter.ps1 -PrinterIP <address>"
    Stop-Setup "the printer is not reachable at $PrinterIP" $hints
}

# ------------------------------------------------------------------
Write-Step "2. Print driver"

$driver = Get-PrinterDriver | Where-Object { $_.Name -like "*DCP-8065*" } | Select-Object -First 1

if ($driver) {
    Write-Ok "Driver already installed: $($driver.Name)"
} else {
    Write-Warn "No DCP-8065DN driver installed"

    # Try the local driver store first
    $store = Get-ChildItem "C:\Windows\System32\DriverStore\FileRepository" -Directory -Filter "prnbr*" -ErrorAction SilentlyContinue
    $found = $null

    foreach ($dir in $store) {
        $inf = Get-ChildItem $dir.FullName -Filter "*.inf" -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($inf -and (Select-String -Path $inf.FullName -Pattern "8065" -Quiet -ErrorAction SilentlyContinue)) {
            $found = $inf.FullName
            break
        }
    }

    if (-not $found) {
        # Try the mapped drive first, then the UNC path if T: is not available here
        $sources = @($DriverSource, $DriverSourceFallback) | Where-Object { $_ }
        $usable  = $null

        foreach ($s in $sources) {
            if (Test-Path $s -ErrorAction SilentlyContinue) {
                $usable = $s
                Write-Info "Driver source: $s"
                break
            }
            Write-Info "Not reachable: $s"
        }

        if ($usable) {
            $cand = Get-ChildItem $usable -Filter "*.inf" -Recurse -ErrorAction SilentlyContinue |
                    Where-Object { Select-String -Path $_.FullName -Pattern "8065" -Quiet -ErrorAction SilentlyContinue } |
                    Select-Object -First 1
            if ($cand) {
                $found = $cand.FullName
            } else {
                Write-Warn "Folder found but no INF in it lists the DCP-8065DN"
            }
        }
    }

    if ($found) {
        Write-Info "Staging: $found"
        & pnputil.exe /add-driver "$found" /install | Out-Null

        $driver = Get-PrinterDriver | Where-Object { $_.Name -like "*DCP-8065*" } | Select-Object -First 1

        if (-not $driver) {
            foreach ($try in @("Brother DCP-8065DN", "Brother DCP-8065DN USB", "Brother DCP-8065DN Printer")) {
                try {
                    Add-PrinterDriver -Name $try -ErrorAction Stop
                    $driver = Get-PrinterDriver | Where-Object { $_.Name -eq $try } | Select-Object -First 1
                    if ($driver) { break }
                } catch { }
            }
        }
    }

    if ($driver) {
        Write-Ok "Driver installed: $($driver.Name)"
    } else {
        Write-Bad "Could not install a DCP-8065DN driver"
        Write-Host ""
        Write-Host "  Available Brother drivers on this machine:" -ForegroundColor Gray
        $others = Get-PrinterDriver | Where-Object { $_.Name -like "*Brother*" }
        if ($others) { $others | ForEach-Object { Write-Host "    $($_.Name)" -ForegroundColor Gray } }
        else { Write-Host "    (none)" -ForegroundColor Gray }

        Stop-Setup "no usable driver" @(
            "Confirm the driver is staged on the share: Test-Path '$DriverSource'",
            "If T: is not mapped here, map it: net use T: \\Xcel-server\taxops /persistent:yes",
            "Or pass a local copy directly: -DriverSource '<folder containing the INF>'",
            "Or download the DCP-8065DN driver from Brother's support site (choose Windows 7 x64 if Windows 10 is not listed)."
        )
    }
}

# ------------------------------------------------------------------
Write-Step "3. TCP/IP port"

$existingPort = Get-PrinterPort -Name $PortName -ErrorAction SilentlyContinue
if ($existingPort) {
    Write-Ok "Port already exists: $PortName"
} else {
    Add-PrinterPort -Name $PortName -PrinterHostAddress $PrinterIP -PortNumber 9100
    Write-Ok "Created port: $PortName -> $PrinterIP:9100"
}

# ------------------------------------------------------------------
Write-Step "4. Print queue"

$existing = Get-Printer -Name $PrinterName -ErrorAction SilentlyContinue

if ($existing -and -not $Force) {
    if ($existing.PortName -eq $PortName) {
        Write-Ok "Queue '$PrinterName' already points at $PortName"
    } else {
        Write-Warn "Queue exists on port '$($existing.PortName)' -- repointing"
        Set-Printer -Name $PrinterName -PortName $PortName
        Write-Ok "Repointed '$PrinterName' to $PortName"
    }
} else {
    if ($existing) {
        Write-Info "Removing existing queue (-Force)"
        Remove-Printer -Name $PrinterName
    }
    Add-Printer -Name $PrinterName -DriverName $driver.Name -PortName $PortName
    Write-Ok "Created queue: $PrinterName"
}

# ------------------------------------------------------------------
Write-Step "5. Verification"

$final = Get-Printer -Name $PrinterName
Write-Host "  Name   : $($final.Name)"
Write-Host "  Port   : $($final.PortName)"
Write-Host "  Driver : $($final.DriverName)"
Write-Host "  Status : $($final.PrinterStatus)"

if ($TestPage) {
    Write-Step "6. Test page"
    try {
        $tmp = Join-Path $env:TEMP "printer_test_$PID.txt"
        @"
Brother DCP-8065DN connectivity test

Workstation : $env:COMPUTERNAME
User        : $env:USERNAME
Printer IP  : $PrinterIP
Queue       : $PrinterName
Time        : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
"@ | Out-File -FilePath $tmp -Encoding ASCII

        Get-Content $tmp | Out-Printer -Name $PrinterName
        Write-Ok "Test page sent -- check the output tray"
        Remove-Item $tmp -Force -ErrorAction SilentlyContinue
    } catch {
        Write-Bad "Test page failed: $($_.Exception.Message)"
    }
}

# ------------------------------------------------------------------
Write-Host ""
Write-Host "Setup complete." -ForegroundColor Green

if ($PrinterIP -like '169.254.*') {
    Write-Host ""
    Write-Host "IMPORTANT: $PrinterIP is a link-local (APIPA) address." -ForegroundColor Yellow
    Write-Host "The printer re-picks this address at random on every power cycle," -ForegroundColor Yellow
    Write-Host "so this queue will break the next time it reboots. To make it stable:" -ForegroundColor Yellow
    Write-Host "  - Move the printer to a switch port on the main network" -ForegroundColor Yellow
    Write-Host "  - On the panel: LAN > TCP/IP > BOOT Method > Auto, then power-cycle" -ForegroundColor Yellow
    Write-Host "  - Add a DHCP reservation for MAC 00-80-77-DF-53-E0" -ForegroundColor Yellow
    Write-Host "  - Re-run this script with the new address" -ForegroundColor Yellow
}

Write-Host ""
