#requires -Version 5.1
<#
MERGE OLLAMA_BASE_URL into NSSM AppEnvironmentExtra (keeps existing +KEY lines).

NSSM resets the extras block when you assign the first KEY=value - this script reads
what is there, strips any old OLLAMA_BASE_URL, writes the new URL as the head line,
then re-applies remaining lines with the required "+" prefix.

Run in an elevated PowerShell (Administrator).

If -NssmExe is omitted, the script looks for nssm.exe on PATH, then common install paths.
You can also pass the real full path, e.g. C:\Tools\nssm-2.24\win64\nssm.exe (do not use a fake path).

Example:
  .\nssm-set-ollama-url.ps1 -Service TaxOpsService -OllamaUrl http://192.168.1.141:11434
  Restart-Service TaxOpsService
#>
param(
    [Parameter(Mandatory = $false)]
    [string] $Service = 'TaxOpsService',

    [Parameter(Mandatory = $false)]
    [string] $OllamaUrl = 'http://192.168.1.141:11434',

    # Default 'nssm.exe' = search PATH and common folders. Or pass the real full path to nssm.exe.
    [string] $NssmExe = 'nssm.exe'
)

$ErrorActionPreference = 'Stop'

function Resolve-NssmExecutable {
    param([string] $UserPath)

    if ($UserPath -and ($UserPath -ne 'nssm.exe')) {
        if (Test-Path -LiteralPath $UserPath -PathType Leaf) {
            return (Resolve-Path -LiteralPath $UserPath).Path
        }
        Write-Warning ("NssmExe path not found (will try PATH and common locations): " + $UserPath)
    }

    $cmd = Get-Command 'nssm.exe' -CommandType Application -ErrorAction SilentlyContinue
    if ($cmd -and (Test-Path -LiteralPath $cmd.Source -PathType Leaf)) {
        return $cmd.Source
    }

    $whereOut = & $env:ComSpec /c "where nssm.exe 2>nul" 2>$null
    if ($whereOut) {
        foreach ($line in ($whereOut -split '\r?\n')) {
            $t = $line.Trim()
            if ($t -and (Test-Path -LiteralPath $t -PathType Leaf)) {
                return (Resolve-Path -LiteralPath $t).Path
            }
        }
    }

    $roots = @(
        (Join-Path $PSScriptRoot 'nssm.exe'),
        (Join-Path $PSScriptRoot '..\nssm.exe'),
        (Join-Path $PSScriptRoot '..\..\nssm.exe'),
        'C:\nssm\win64\nssm.exe',
        'C:\nssm\nssm.exe',
        'C:\Program Files\nssm\win64\nssm.exe',
        'C:\Program Files\nssm\nssm.exe',
        (Join-Path $env:LOCALAPPDATA 'Microsoft\WinGet\Links\nssm.exe')
    )
    foreach ($p in $roots) {
        if ($p -and (Test-Path -LiteralPath $p -PathType Leaf)) {
            return (Resolve-Path -LiteralPath $p).Path
        }
    }

    return $null
}

$nssmResolved = Resolve-NssmExecutable -UserPath $NssmExe
if (-not $nssmResolved) {
    $msg = @'
Could not find nssm.exe.

1) Install or locate NSSM: https://nssm.cc/download (zip contains win64\nssm.exe).

2) Find it on this PC (PowerShell):
     Get-Command nssm.exe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source

3) Or search (may take a minute):
     Get-ChildItem -Path C:\ -Filter nssm.exe -Recurse -ErrorAction SilentlyContinue -Depth 6 | Select-Object -First 3 -ExpandProperty FullName

4) Then run again with the real path, e.g.:
     .\scripts\nssm-set-ollama-url.ps1 -NssmExe 'C:\Tools\nssm-2.24\win64\nssm.exe' -Service TaxOpsService
'@
    throw $msg
}
$NssmExe = $nssmResolved
Write-Host ('[nssm] Using: ' + $NssmExe) -ForegroundColor DarkGray

# Quick sanity: wrong service name fails fast
$appProbe = & $NssmExe get $Service Application 2>&1
if ($LASTEXITCODE -ne 0) {
    throw ('NSSM cannot read service ''' + $Service + '''. Wrong name? Output: ' + $appProbe)
}

$OllamaUrl = ($OllamaUrl.Trim()).TrimEnd('/')
if ($OllamaUrl -notmatch '^https?://') {
    throw ('OllamaUrl must start with http:// or https:// (got: ' + $OllamaUrl + ')')
}

$raw = & $NssmExe get $Service AppEnvironmentExtra 2>&1
if ($LASTEXITCODE -ne 0) {
    throw ('nssm get ' + $Service + ' AppEnvironmentExtra failed: ' + $raw)
}

$rest = New-Object System.Collections.Generic.List[string]
$blob = ($raw | Out-String).Trim()
foreach ($row in ($blob -split '\r?\n')) {
    $t = $row.Trim()
    if (-not $t) { continue }
    if ($t -match '(?i)^OLLAMA_BASE_URL\s*=') { continue }
    if ($t.StartsWith('+')) { $t = $t.Substring(1).Trim() }
    if ($t -and $t.Contains('=')) {
        [void]$rest.Add($t)
    }
}

$head = 'OLLAMA_BASE_URL=' + $OllamaUrl
Write-Host ('[nssm] ' + $Service + ' AppEnvironmentExtra head: ' + $head) -ForegroundColor Cyan

& $NssmExe set $Service AppEnvironmentExtra $head
if ($LASTEXITCODE -ne 0) {
    throw ('nssm set AppEnvironmentExtra (head) failed (exit ' + $LASTEXITCODE + ').')
}

foreach ($ln in $rest) {
    $arg = '+' + $ln
    Write-Host ('[nssm]   + ' + $ln) -ForegroundColor Gray
    & $NssmExe set $Service AppEnvironmentExtra $arg
    if ($LASTEXITCODE -ne 0) {
        throw ('nssm set AppEnvironmentExtra failed (' + $arg + ') exit ' + $LASTEXITCODE + '.')
    }
}

Write-Host ''
Write-Host ('Done. NSSM extras now include OLLAMA URL and ' + $rest.Count + ' other line(s).') -ForegroundColor Green
Write-Host 'Restart TaxOps:' -ForegroundColor Yellow
Write-Host ('  Restart-Service ' + $Service)
