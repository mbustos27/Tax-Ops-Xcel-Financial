# Rebuild production Tailwind bundle (taxops/static/tw.min.css).
# No Node required: caches the standalone CLI under taxops/.tailwind/ (gitignored).
$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root
$ver = "3.4.17"
$cacheDir = Join-Path $root ".tailwind"
$exePath = Join-Path $cacheDir "tailwindcss-windows-x64.exe"
if (-not (Test-Path $exePath)) {
  New-Item -ItemType Directory -Force -Path $cacheDir | Out-Null
  $url = "https://github.com/tailwindlabs/tailwindcss/releases/download/v$ver/tailwindcss-windows-x64.exe"
  Write-Host "Downloading Tailwind standalone $ver ..."
  Invoke-WebRequest -Uri $url -OutFile $exePath -UseBasicParsing
}
Write-Host "Building tw.min.css ..."
& $exePath -i (Join-Path $root "src\input.css") -o (Join-Path $root "static\tw.min.css") --minify
Write-Host "Done."
