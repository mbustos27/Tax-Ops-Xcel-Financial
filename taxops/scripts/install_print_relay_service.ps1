#Requires -Version 5.1
<#
.SYNOPSIS
  Install or update NSSM service FiletrackRelay (print labels on reception).

.DESCRIPTION
  Idempotent. Run elevated on RECEPTION.
  Paths with spaces (e.g. C:\Users\Windows 10\...) are always quoted.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File \\Xcel-server\taxops\taxops\scripts\install_print_relay_service.ps1
#>
param(
    [string]$ShareRoot = "",
    [string]$PythonExe = "",
    [string]$Token = $env:FILETRACK_RELAY_TOKEN,
    [string]$Printer = $env:FILETRACK_PRINTER,
    [string]$HostBind = "0.0.0.0",
    [int]$Port = 8765,
    [string]$ServiceName = "FiletrackRelay",
    [string]$DisplayName = "TaxOps Filetrack Print Relay",
    [string]$NssmExe = ""
)

$ErrorActionPreference = "Stop"

function Test-IsAdmin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Find-Nssm([string]$Hint) {
    if ($Hint -and (Test-Path -LiteralPath $Hint)) { return $Hint }
    foreach ($c in @(
        "C:\TaxOps\nssm\nssm.exe",
        "C:\Tools\nssm\nssm.exe",
        "${env:ProgramFiles}\nssm\nssm.exe",
        (Get-Command nssm.exe -EA SilentlyContinue | Select-Object -ExpandProperty Source)
    )) {
        if ($c -and (Test-Path -LiteralPath $c)) { return $c }
    }
    return $null
}

$thisScript = $MyInvocation.MyCommand.Path
if (-not $ShareRoot) {
    # ...\taxops\scripts\this.ps1 -> share root two levels up
    $ShareRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $thisScript))
}
$ShareRoot = $ShareRoot.Trim().TrimEnd('\')
$appDir = Join-Path $ShareRoot "taxops"
$relayPy = Join-Path $appDir "filetrack\relay\server.py"
if (-not (Test-Path -LiteralPath $relayPy)) {
    throw "Relay source missing: $relayPy"
}

if (-not (Test-IsAdmin)) {
    Write-Host "Need Administrator - relaunching (UAC)..." -ForegroundColor Yellow
    $arg = @(
        "-NoProfile", "-ExecutionPolicy", "Bypass",
        "-File", "`"$thisScript`"",
        "-ShareRoot", "`"$ShareRoot`"",
        "-Port", "$Port", "-HostBind", $HostBind,
        "-ServiceName", $ServiceName, "-DisplayName", "`"$DisplayName`""
    )
    if ($Token) { $arg += @("-Token", "`"$Token`"") }
    if ($Printer) { $arg += @("-Printer", "`"$Printer`"") }
    if ($PythonExe) { $arg += @("-PythonExe", "`"$PythonExe`"") }
    if ($NssmExe) { $arg += @("-NssmExe", "`"$NssmExe`"") }
    Start-Process powershell.exe -Verb RunAs -WorkingDirectory $ShareRoot -ArgumentList $arg
    exit 0
}

if (-not $Printer) { $Printer = "4BARCODE 4B-2054A" }

$envDir = "C:\TaxOps\PrintRelay"
$envFile = Join-Path $envDir "relay.env"
$logDir = "C:\TaxOps\logs"
New-Item -ItemType Directory -Force -Path $envDir | Out-Null
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

# Token: prefer param, else existing relay.env, else taxops\.env, else prompt
if (-not $Token -and (Test-Path -LiteralPath $envFile)) {
    foreach ($line in Get-Content -LiteralPath $envFile) {
        if ($line -match '^\s*FILETRACK_RELAY_TOKEN=(.+)$') { $Token = $Matches[1].Trim().Trim('"'); break }
    }
}
if (-not $Token) {
    $shareEnv = Join-Path $appDir ".env"
    if (Test-Path -LiteralPath $shareEnv) {
        foreach ($line in Get-Content -LiteralPath $shareEnv) {
            if ($line -match '^\s*FILETRACK_RELAY_TOKEN=(.+)$') { $Token = $Matches[1].Trim().Trim('"'); break }
        }
    }
}
if (-not $Token) {
    $Token = Read-Host "Enter FILETRACK_RELAY_TOKEN (same as TaxOps server)"
}
if (-not $Token) { throw "FILETRACK_RELAY_TOKEN required" }

# Write machine-scope env file (not committed). Do not echo token.
@(
    "FILETRACK_RELAY_TOKEN=$Token"
    "FILETRACK_PRINTER=$Printer"
    "FILETRACK_RELAY_HOST=$HostBind"
    "FILETRACK_RELAY_PORT=$Port"
) | Set-Content -LiteralPath $envFile -Encoding ASCII

if (-not $PythonExe) {
    foreach ($c in @(
        "C:\Users\Windows 10\AppData\Local\Programs\Python\Python314\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python314\python.exe",
        (Join-Path $appDir ".venv\Scripts\python.exe")
    )) {
        if ($c -and (Test-Path -LiteralPath $c)) { $PythonExe = $c; break }
    }
}
if (-not $PythonExe -or -not (Test-Path -LiteralPath $PythonExe)) {
    throw "Python 3.14 not found. Pass -PythonExe with a quoted path."
}

Write-Host "Ensuring waitress / flask / pywin32..."
& $PythonExe -m pip install -q "waitress>=3.0.0" "flask>=2.3" "pywin32>=306" "requests>=2.31"
if ($LASTEXITCODE -ne 0) { throw "pip install failed" }

$nssm = Find-Nssm $NssmExe
if (-not $nssm) {
    throw "nssm.exe not found. Install NSSM and re-run, or pass -NssmExe `"C:\path\nssm.exe`""
}

$svc = Get-Service -Name $ServiceName -EA SilentlyContinue
if (-not $svc) {
    Write-Host "Installing service $ServiceName ..."
    & $nssm install $ServiceName $PythonExe "-m" "filetrack.relay.server" "--port" "$Port"
    if ($LASTEXITCODE -ne 0) { throw "nssm install failed" }
} else {
    Write-Host "Updating existing service $ServiceName ..."
    & $nssm set $ServiceName Application $PythonExe
    & $nssm set $ServiceName AppParameters "-m filetrack.relay.server --port $Port"
}

& $nssm set $ServiceName DisplayName $DisplayName
& $nssm set $ServiceName AppDirectory $appDir
& $nssm set $ServiceName Start SERVICE_AUTO_START
& $nssm set $ServiceName AppStdout (Join-Path $logDir "print_relay.log")
& $nssm set $ServiceName AppStderr (Join-Path $logDir "print_relay.log")
& $nssm set $ServiceName AppRotateFiles 1
& $nssm set $ServiceName AppRotateBytes 10485760
& $nssm set $ServiceName AppExit Default Restart
& $nssm set $ServiceName AppRestartDelay 5000
& $nssm set $ServiceName AppEnvironmentExtra "FILETRACK_RELAY_TOKEN=$Token"
& $nssm set $ServiceName AppEnvironmentExtra "+FILETRACK_PRINTER=$Printer"
& $nssm set $ServiceName AppEnvironmentExtra "+FILETRACK_RELAY_HOST=$HostBind"
& $nssm set $ServiceName AppEnvironmentExtra "+FILETRACK_RELAY_PORT=$Port"
& $nssm set $ServiceName AppEnvironmentExtra "+FILETRACK_RELAY_ENV=$envFile"

# Firewall: LAN only
$fwName = "TaxOps Filetrack Print Relay 8765"
$existing = Get-NetFirewallRule -DisplayName $fwName -EA SilentlyContinue
if ($existing) {
    Write-Host "Firewall rule already present: $fwName"
} else {
    New-NetFirewallRule -DisplayName $fwName `
        -Direction Inbound -Protocol TCP -LocalPort $Port `
        -RemoteAddress 192.168.1.0/24 -Action Allow -Profile Any | Out-Null
    Write-Host "Created firewall rule $fwName (LAN 192.168.1.0/24)"
}

Restart-Service -Name $ServiceName -Force -EA SilentlyContinue
Start-Service -Name $ServiceName -EA SilentlyContinue
Start-Sleep -Seconds 3

try {
    $h = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/health" -f $Port) -TimeoutSec 5
    Write-Host ("[OK] health: printer_found={0} configured={1}" -f $h.printer_found, $h.printer_configured) -ForegroundColor Green
} catch {
    Write-Host ("[WARN] service installed but health failed: {0}" -f $_.Exception.Message) -ForegroundColor Yellow
}

Write-Host "Done. Service=$ServiceName  Python=$PythonExe  AppDirectory=$appDir"
Write-Host "Env file=$envFile  Log=$(Join-Path $logDir 'print_relay.log')"
