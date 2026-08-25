@echo off
:: DEPRECATED — use GO_RECEPTION.bat / GO_SCAN_AGENT.bat / install_scan_agent_task.ps1
setlocal EnableExtensions
title DEPRECATED - use GO_SCAN_AGENT.bat
color 0E
echo.
echo  ========================================================
echo   DEPRECATED - do not use scan_agent_wizard
echo  ========================================================
echo.
echo  This wizard is from the NSSM ScanAgent era. Scan must run
echo  as an Interactive logon scheduled task (WIA needs desktop).
echo.
echo  Prefer:
echo    T:\GO_RECEPTION.bat
echo    T:\GO_SCAN_AGENT.bat
echo    taxops\scripts\install_scan_agent_task.ps1  (Admin)
echo.
echo  To force old wizard:  scan_agent_wizard.bat -ForceDeprecated
echo.
if /I "%~1"=="-ForceDeprecated" goto :run
if /I "%~1"=="ForceDeprecated" goto :run
pause
exit /b 2

:run
echo  [WARN] Running deprecated scan_agent_wizard anyway...
cd /d "%~dp0"
:: Drop the force flag; pass remaining args (e.g. -Mode Repair)
if /I "%~1"=="-ForceDeprecated" shift /1
if /I "%~1"=="ForceDeprecated" shift /1
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scan_agent_wizard.ps1" -ForceDeprecated %1 %2 %3 %4
set ERR=%ERRORLEVEL%
if not "%ERR%"=="0" (
  echo.
  echo Wizard exited with code %ERR%.
  pause
)
exit /b %ERR%
