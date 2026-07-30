@echo off
:: TaxOps Scan Agent — guided install / repair wizard (run on RECEPTION PC).
:: Double-click or:  \\Xcel-server\taxops\scan_agent_wizard.bat
:: Optional: scan_agent_wizard.bat -Mode Repair
::           scan_agent_wizard.bat -Mode TestOnly
setlocal EnableExtensions
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scan_agent_wizard.ps1" %*
set ERR=%ERRORLEVEL%
if not "%ERR%"=="0" (
  echo.
  echo Wizard exited with code %ERR%.
  pause
)
exit /b %ERR%
