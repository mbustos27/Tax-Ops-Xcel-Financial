@echo off
:: One-click: kill old agent, start visible Scan Agent, verify fast health.
:: Run on RECEPTION PC:  \\Xcel-server\taxops\GO_SCAN_AGENT.bat
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"
title TaxOps Scan Agent Launcher

echo ========================================================
echo   START Scan Agent
echo ========================================================
echo.
echo Note: 0.0.0.0 = listen on all NICs (normal). Health uses 127.0.0.1.
echo.

set "SHARE_ROOT=%~dp0"
if "%SHARE_ROOT:~-1%"=="\" set "SHARE_ROOT=%SHARE_ROOT:~0,-1%"

echo [1] Killing listeners on TCP 8766...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8766" ^| findstr "LISTENING"') do (
  echo     taskkill PID %%a
  taskkill /F /PID %%a >nul 2>&1
)
timeout /t 2 /nobreak >nul

if not exist "%SHARE_ROOT%\start_scan_agent.bat" (
  echo [FAIL] start_scan_agent.bat missing under %SHARE_ROOT%
  pause
  exit /b 4
)

echo [2] Starting Scan Agent window (leave it open)...
:: Use cmd /c so UNC paths work reliably with start
start "TaxOps Scan Agent" cmd /c "call \"%SHARE_ROOT%\start_scan_agent.bat\""

echo [3] Waiting for READY / com_sta_v4 on http://127.0.0.1:8766/health ...
set "TOKEN="
if exist "C:\TaxOps\ScanAgent\token.env" (
  for /f "usebackq tokens=1,* delims==" %%a in ("C:\TaxOps\ScanAgent\token.env") do (
    if /I "%%a"=="SCAN_AGENT_TOKEN" set "TOKEN=%%b"
  )
)
if not defined TOKEN if exist "%SHARE_ROOT%\taxops\.env" (
  for /f "usebackq tokens=1,* delims==" %%a in ("%SHARE_ROOT%\taxops\.env") do (
    if /I "%%a"=="SCAN_AGENT_TOKEN" set "TOKEN=%%b"
  )
)
if not defined TOKEN (
  echo [FAIL] No SCAN_AGENT_TOKEN in C:\TaxOps\ScanAgent\token.env or taxops\.env
  echo        Run scan_agent_wizard.bat once, then re-run this.
  pause
  exit /b 2
)

set "OK=0"
set "LAST=waiting"
for /L %%i in (1,1,45) do (
  powershell -NoProfile -Command "$ProgressPreference='SilentlyContinue'; $t=$env:TOKEN; try { $r=Invoke-WebRequest -Uri 'http://127.0.0.1:8766/health' -Headers @{ 'X-Scan-Agent-Token' = $t } -UseBasicParsing -TimeoutSec 3; $c=$r.Content; if ($c -match 'com_sta_v4') { [Console]::Out.Write('OK'); exit 0 }; [Console]::Out.Write(('BODY '+$c.Substring(0,[Math]::Min(100,$c.Length)))); exit 2 } catch { [Console]::Out.Write($_.Exception.Message); exit 1 }" > "%TEMP%\scan_agent_health_try.txt" 2>&1
  set "HC=!ERRORLEVEL!"
  set /p LAST=<"%TEMP%\scan_agent_health_try.txt"
  if "!HC!"=="0" (
    set "OK=1"
    goto :done
  )
  echo     try %%i/45  err=!HC!  !LAST!
  timeout /t 1 /nobreak >nul
)

:done
echo.
if "!OK!"=="1" (
  echo [OK] Scan Agent is UP (com_sta_v4)
  echo      Leave the window titled "TaxOps Scan Agent" open.
  powershell -NoProfile -Command "try { Invoke-RestMethod -Uri 'http://127.0.0.1:8766/health' -Headers @{ 'X-Scan-Agent-Token' = $env:TOKEN } -TimeoutSec 5 | ConvertTo-Json -Compress } catch { $_.Exception.Message }"
) else (
  echo [FAIL] Health never returned com_sta_v4.
  echo        Last status: !LAST!
  echo        Look at the TaxOps Scan Agent window - you want: READY com_sta_v4
  echo        If import failed, the window will say [FAIL] and list the local folder.
  echo        Close EVERY Scan Agent window, then re-run this bat.
)
echo.
pause
