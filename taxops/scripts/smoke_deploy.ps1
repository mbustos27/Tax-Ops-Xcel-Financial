# Quick smoke probe (calls scripts/smoke_deploy.py)
#
# Usage (after TaxOps NSSM restart):
#   cd C:\TaxOps\taxops
#   powershell -File scripts\smoke_deploy.ps1
#   .\scripts\smoke_deploy.ps1 -BaseUrl http://192.168.1.50:5000

param(
    [string] $BaseUrl = 'http://127.0.0.1:5000'
)

$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$taxopsRoot = Split-Path -Parent $scriptDir
$venvPy = Join-Path $taxopsRoot '.venv\Scripts\python.exe'
if (Test-Path $venvPy) {
    $py = $venvPy
}
else {
    $py = 'python'
}

& $py (Join-Path $scriptDir 'smoke_deploy.py') $BaseUrl
exit $LASTEXITCODE
