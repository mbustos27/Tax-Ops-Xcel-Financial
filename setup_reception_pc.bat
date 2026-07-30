@echo off
setlocal EnableExtensions
title Reception PC Setup - TaxOps
color 0A
echo.
echo  ========================================================
echo   TaxOps Reception Workstation Setup
echo  ========================================================
echo.
echo  Run this ON the front desk PC (label printer + Epson scanner).
echo  Click YES on the UAC prompt when it appears.
echo.

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
  "Start-Process -FilePath powershell.exe -Verb RunAs -ArgumentList '-NoExit','-NoProfile','-ExecutionPolicy','Bypass','-File','\"%SCRIPT%\"'"

echo.
echo  If nothing opened, UAC was cancelled - run again and click Yes.
timeout /t 5 >nul
exit /b 0
