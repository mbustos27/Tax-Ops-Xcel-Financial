param(
    [ValidateSet("demo", "production")]
    [string]$Mode = "demo",

    [int]$Port = 5001
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvActivate = Join-Path $repoRoot ".venv\Scripts\Activate.ps1"
$demoDb = Join-Path $repoRoot "taxops\taxops_demo.db"
$prodDb = Join-Path $repoRoot "taxops\taxops.db"

if (-not (Test-Path $venvActivate)) {
    Write-Host "Virtual environment not found at .venv." -ForegroundColor Red
    Write-Host "Run dependency setup first, then try again." -ForegroundColor Yellow
    exit 1
}

. $venvActivate

if ($Mode -eq "demo") {
    $env:TAXOPS_ENV = "demo"
    $env:TAXOPS_DB = $demoDb
} else {
    $env:TAXOPS_ENV = "production"
    $env:TAXOPS_DB = $prodDb
}

Write-Host ""
Write-Host "Starting TaxOps..." -ForegroundColor Cyan
Write-Host "  Mode : $Mode"
Write-Host "  Port : $Port"
Write-Host "  DB   : $env:TAXOPS_DB"
Write-Host "  URL  : http://localhost:$Port"
Write-Host ""

python -c "from taxops.app import app; app.run(debug=True, host='0.0.0.0', port=$Port)"
