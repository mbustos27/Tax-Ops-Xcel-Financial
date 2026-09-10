@echo off
:: Desk setup: map F/P/Q/T network drives, hosts taxlog, Tax Log desktop shortcut.
:: Double-click from Explorer (UNC or T:):
::   \\Xcel-server\taxops\SETUP_WORKSTATION.bat
::   T:\SETUP_WORKSTATION.bat
setlocal EnableExtensions
title TaxOps Workstation Setup

:: Prefer UNC so this still works before T: exists, and when elevated
set "SCRIPT=\\Xcel-server\taxops\setup_workstation.ps1"
if not exist "%SCRIPT%" (
  set "SCRIPT=%~dp0setup_workstation.ps1"
)
if not exist "%SCRIPT%" (
  echo.
  echo  [FAIL] Cannot find setup_workstation.ps1
  echo         Expected \\Xcel-server\taxops\setup_workstation.ps1
  echo.
  pause
  exit /b 4
)

echo.
echo  ========================================================
echo    TaxOps workstation setup
echo  ========================================================
echo    Maps F: \\Xcel-server\ACCNTING
echo    Maps P: \\Xcel-server\PUBLIC
echo    Maps Q: \\Xcel-server\QUICKBOOKS
echo    Maps T: \\Xcel-server\taxops
echo    Tax Log URL: http://192.168.1.173:5000
echo    Routes hostname taxlog -^> TaxOps server
echo    Adds Desktop "Tax Log" shortcut
echo  ========================================================
echo.

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT%" %*
set "RC=%ERRORLEVEL%"

echo.
if not "%RC%"=="0" (
  echo  Finished with errors ^(exit %RC%^). See messages above.
) else (
  echo  Setup finished.
)
echo.
pause
exit /b %RC%
