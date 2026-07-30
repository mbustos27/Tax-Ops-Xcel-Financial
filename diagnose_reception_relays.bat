@echo off
:: TaxOps reception diagnostic - print relay (8765) + scan agent (8766).
:: Writes a full report under C:\TaxOps\diagnostics\ and opens it in Notepad.
:: Run ON the reception PC:  \\Xcel-server\taxops\diagnose_reception_relays.bat
setlocal EnableExtensions
title TaxOps Reception Relay Diagnostic

set "SHARE_ROOT=%~dp0"
if "%SHARE_ROOT:~-1%"=="\" set "SHARE_ROOT=%SHARE_ROOT:~0,-1%"

echo ========================================================
echo   TaxOps reception relay diagnostic
echo ========================================================
echo   Share: %SHARE_ROOT%
echo   Log:   C:\TaxOps\diagnostics\reception_relays_*.txt
echo.

if not exist "%SHARE_ROOT%\diagnose_reception_relays.ps1" (
  echo [FAIL] diagnose_reception_relays.ps1 not found next to this bat
  pause
  exit /b 4
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%SHARE_ROOT%\diagnose_reception_relays.ps1" -ShareRoot "%SHARE_ROOT%"
set "RC=%ERRORLEVEL%"
echo.
echo Exit code %RC%  (0=pass, 1=fail)
echo Report folder: C:\TaxOps\diagnostics\
echo.
pause
exit /b %RC%
