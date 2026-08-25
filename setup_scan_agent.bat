@echo off
:: DEPRECATED — use GO_RECEPTION.bat / install_scan_agent_task.ps1 / GO_SCAN_AGENT.bat
setlocal EnableExtensions
title DEPRECATED - use GO_SCAN_AGENT.bat
color 0E
echo.
echo  ========================================================
echo   DEPRECATED - do not use setup_scan_agent
echo  ========================================================
echo.
echo  Prefer:
echo    T:\GO_RECEPTION.bat
echo    T:\GO_SCAN_AGENT.bat
echo    taxops\scripts\install_scan_agent_task.ps1  (Admin)
echo.
echo  To force old setup:  setup_scan_agent.bat -ForceDeprecated
echo.
if /I "%~1"=="-ForceDeprecated" goto :run
if /I "%~1"=="ForceDeprecated" goto :run
pause
exit /b 2

:run
echo  [WARN] Running deprecated setup_scan_agent anyway...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup_scan_agent.ps1" -ForceDeprecated
set ERR=%ERRORLEVEL%
if not "%ERR%"=="0" pause
exit /b %ERR%
