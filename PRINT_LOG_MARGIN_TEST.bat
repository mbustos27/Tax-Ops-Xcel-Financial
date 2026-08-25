@echo off
:: Print the margin-test LOG label on THIS PC (run from Reception — printer attached here).
:: UNC-safe: pushd first so we can cd into the share.
setlocal EnableExtensions
title TaxOps — LOG margin test print

set "SHARE_ROOT=%~dp0"
if "%SHARE_ROOT:~-1%"=="\" set "SHARE_ROOT=%SHARE_ROOT:~0,-1%"

pushd "%SHARE_ROOT%" 2>nul
if errorlevel 1 (
  echo [FAIL] Cannot access share: %SHARE_ROOT%
  pause
  exit /b 4
)

cd /d "%SHARE_ROOT%\taxops" 2>nul
if errorlevel 1 (
  echo [FAIL] Cannot cd to %SHARE_ROOT%\taxops
  popd 2>nul
  pause
  exit /b 1
)

if not defined FILETRACK_PRINTER set "FILETRACK_PRINTER=4BARCODE 4B-2054A"

set "ZPL=%SHARE_ROOT%\taxops\filetrack\labels\samples\sample_LOG_MARGIN_TEST.zpl"
if not exist "%ZPL%" (
  echo [FAIL] Missing ZPL: %ZPL%
  popd 2>nul
  pause
  exit /b 1
)

echo Printing: %ZPL%
echo Printer:  %FILETRACK_PRINTER%
echo.

python -m filetrack.labels.send_raw_zpl --file "%ZPL%" --printer "%FILETRACK_PRINTER%"
set "RC=%ERRORLEVEL%"

echo.
if "%RC%"=="0" (
  echo [OK] Sent to printer.
) else (
  echo [FAIL] Print failed ^(exit %RC%^).
  echo        Check printer name with:
  echo        python -m filetrack.labels.send_raw_zpl --list-printers
)
popd 2>nul
pause
exit /b %RC%
