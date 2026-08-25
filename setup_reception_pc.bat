@echo off
setlocal EnableExtensions
title DEPRECATED - use GO_RECEPTION.bat
color 0E
echo.
echo  ========================================================
echo   DEPRECATED - do not use setup_reception_pc
echo  ========================================================
echo.
echo  This wizard is frozen. It installs outdated auto-start
echo  (Startup-folder print + NSSM ScanAgent) that fights the
echo  current reception architecture.
echo.
echo  Use instead (on the reception PC):
echo.
echo    T:\GO_RECEPTION.bat
echo    T:\GO_RECEPTION.bat -Repair     (Admin - fix auto-start)
echo    T:\GO_SCAN_AGENT.bat            (scan-only restart)
echo.
echo  Docs: taxops\docs\reception_agents_runbook.md
echo.
echo  To force the old wizard anyway:
echo    setup_reception_pc.bat -ForceDeprecated
echo.
if /I "%~1"=="-ForceDeprecated" goto :run
if /I "%~1"=="ForceDeprecated" goto :run
pause
exit /b 2

:run
echo  [WARN] Running deprecated setup_reception_pc anyway...
echo.
color 0A
title Reception PC Setup - TaxOps (DEPRECATED)

:: Admin cannot see mapped T: - always launch from UNC
set "SCRIPT=\\Xcel-server\taxops\setup_reception_pc.ps1"
if not exist "%SCRIPT%" (
  echo  ERROR: Cannot find %SCRIPT%
  echo  Open File Explorer and check \\Xcel-server\taxops is reachable.
  echo.
  pause
  exit /b 1
)

powershell.exe -NoProfile -ExecutionPolicy Bypass -Command ^
  "Start-Process -FilePath powershell.exe -Verb RunAs -ArgumentList '-NoExit','-NoProfile','-ExecutionPolicy','Bypass','-File','\"%SCRIPT%\"','-ForceDeprecated'"

echo.
echo  Elevated window should open. Close this one when done.
pause
