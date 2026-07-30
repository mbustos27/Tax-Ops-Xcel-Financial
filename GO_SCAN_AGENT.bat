@echo off
:: One-click: kill old agent, start visible Scan Agent, verify fast health.
:: Run on RECEPTION PC:  \\Xcel-server\taxops\GO_SCAN_AGENT.bat
setlocal EnableExtensions EnableDelayedExpansion
title TaxOps Scan Agent Launcher

echo ========================================================
echo   START Scan Agent
echo ========================================================
echo.
echo Note: 0.0.0.0 = listen on all NICs (normal). Health uses 127.0.0.1.
echo.

set "SHARE_ROOT=%~dp0"
if "%SHARE_ROOT:~-1%"=="\" set "SHARE_ROOT=%SHARE_ROOT:~0,-1%"

:: UNC shares need pushd (maps a drive letter) or start/call fails silently.
pushd "%SHARE_ROOT%" 2>nul
if errorlevel 1 (
  echo [FAIL] Cannot access share: %SHARE_ROOT%
  pause
  exit /b 4
)
set "SHARE_ROOT=%CD%"
if "%SHARE_ROOT:~-1%"=="\" set "SHARE_ROOT=%SHARE_ROOT:~0,-1%"

echo [1] Killing listeners on TCP 8766...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8766" ^| findstr "LISTENING"') do (
  echo     taskkill PID %%a
  taskkill /F /PID %%a >nul 2>&1
)
timeout /t 2 /nobreak >nul

if not exist "%SHARE_ROOT%\start_scan_agent.bat" (
  echo [FAIL] start_scan_agent.bat missing under %SHARE_ROOT%
  popd
  pause
  exit /b 4
)

echo [2] Starting Scan Agent window (leave it open)...
echo     Watch that window for [FAIL] / READY lines.
:: /D sets working dir; /k keeps window open on errors
start "TaxOps Scan Agent" /D "%SHARE_ROOT%" cmd /k call "%SHARE_ROOT%\start_scan_agent.bat"

echo [3] Waiting for READY / com_sta_v4 on http://127.0.0.1:8766/health ...
echo     First sync + package check can take 30-90s — keep waiting.
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
  popd
  pause
  exit /b 2
)

set "OK=0"
set "LAST=waiting"
for /L %%i in (1,1,90) do (
  powershell -NoProfile -Command "$ProgressPreference='SilentlyContinue'; $t=@'
%TOKEN%
'@; $t=$t.Trim(); try { $r=Invoke-WebRequest -Uri 'http://127.0.0.1:8766/health' -Headers @{ 'X-Scan-Agent-Token' = $t } -UseBasicParsing -TimeoutSec 3; $c=$r.Content; if ($c -match 'com_sta_v4') { [Console]::Out.Write('OK'); exit 0 }; [Console]::Out.Write(('BODY '+$c.Substring(0,[Math]::Min(120,$c.Length)))); exit 2 } catch { [Console]::Out.Write($_.Exception.Message); exit 1 }" > "%TEMP%\scan_agent_health_try.txt" 2>&1
  set "HC=!ERRORLEVEL!"
  set /p LAST=<"%TEMP%\scan_agent_health_try.txt"
  if "!HC!"=="0" (
    set "OK=1"
    goto :done
  )
  echo     try %%i/90  err=!HC!  !LAST!
  timeout /t 1 /nobreak >nul
)

:done
echo.
if "!OK!"=="1" (
  echo [OK] Scan Agent is UP (com_sta_v4)
  echo      Leave the window titled "TaxOps Scan Agent" open.
  powershell -NoProfile -Command "$t=@'
%TOKEN%
'@; $t=$t.Trim(); try { Invoke-RestMethod -Uri 'http://127.0.0.1:8766/health' -Headers @{ 'X-Scan-Agent-Token' = $t } -TimeoutSec 5 | ConvertTo-Json -Compress } catch { $_.Exception.Message }"
) else (
  echo [FAIL] Health never returned com_sta_v4.
  echo        Last status: !LAST!
  echo.
  echo        Look at the OTHER window titled "TaxOps Scan Agent":
  echo          - [FAIL] lines = token / Python / sync problem
  echo          - READY com_sta_v4 = success
  echo          - Stuck on Syncing = wait or check share access
  echo        Close that window, fix the [FAIL], then re-run this bat.
)
echo.
popd
pause
