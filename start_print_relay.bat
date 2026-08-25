@echo off
:: TaxOps print relay - run ON the reception PC (label printer attached).
:: Prefer NSSM service (install_print_relay_service.ps1). This bat is manual/fallback.
:: UNC-safe: cmd cannot use \\server\share as cwd — always pushd first.
setlocal EnableExtensions
title TaxOps Print Relay

set "SHARE_ROOT=%~dp0"
if "%SHARE_ROOT:~-1%"=="\" set "SHARE_ROOT=%SHARE_ROOT:~0,-1%"

pushd "%SHARE_ROOT%" 2>nul
if errorlevel 1 (
  echo [FAIL] Cannot access share: %SHARE_ROOT%
  echo        Try mapped T: or confirm \\Xcel-server\taxops is reachable.
  pause
  exit /b 4
)
set "SHARE_ROOT=%CD%"
if "%SHARE_ROOT:~-1%"=="\" set "SHARE_ROOT=%SHARE_ROOT:~0,-1%"

cd /d "%SHARE_ROOT%\taxops" 2>nul
if errorlevel 1 (
  echo [FAIL] Cannot cd to %SHARE_ROOT%\taxops
  popd 2>nul
  pause
  exit /b 1
)
if not exist "filetrack\relay\server.py" (
  echo [FAIL] Cannot find taxops\filetrack\relay\server.py under %SHARE_ROOT%
  popd 2>nul
  pause
  exit /b 1
)

if not defined FILETRACK_PRINTER set "FILETRACK_PRINTER=4BARCODE 4B-2054A"
set "FILETRACK_RELAY_HOST=0.0.0.0"
set "FILETRACK_RELAY_PORT=8765"

:: Token from machine env file or taxops\.env — never hardcode secrets in this bat.
if not defined FILETRACK_RELAY_TOKEN if exist "C:\TaxOps\PrintRelay\relay.env" (
  for /f "usebackq tokens=1,* delims==" %%a in ("C:\TaxOps\PrintRelay\relay.env") do (
    if /I "%%a"=="FILETRACK_RELAY_TOKEN" set "FILETRACK_RELAY_TOKEN=%%b"
    if /I "%%a"=="FILETRACK_PRINTER" set "FILETRACK_PRINTER=%%b"
  )
)
if not defined FILETRACK_RELAY_TOKEN if exist ".env" (
  for /f "usebackq tokens=1,* delims==" %%a in (".env") do (
    if /I "%%a"=="FILETRACK_RELAY_TOKEN" set "FILETRACK_RELAY_TOKEN=%%b"
  )
)
if not defined FILETRACK_RELAY_TOKEN (
  echo [FAIL] FILETRACK_RELAY_TOKEN not set.
  echo        Create C:\TaxOps\PrintRelay\relay.env or set the env var.
  echo        Or run: taxops\scripts\install_print_relay_service.ps1
  popd 2>nul
  pause
  exit /b 2
)

set "PY="
if exist "C:\Users\Windows 10\AppData\Local\Programs\Python\Python314\python.exe" set "PY=C:\Users\Windows 10\AppData\Local\Programs\Python\Python314\python.exe"
if not defined PY if exist "%LOCALAPPDATA%\Programs\Python\Python314\python.exe" set "PY=%LOCALAPPDATA%\Programs\Python\Python314\python.exe"
if not defined PY if exist "%LOCALAPPDATA%\Programs\Python\Python313\python.exe" set "PY=%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
if not defined PY (
  where py >nul 2>&1 && (for /f "delims=" %%i in ('py -3 -c "import sys; print(sys.executable)"') do set "PY=%%i")
)
if not defined PY where python >nul 2>&1 && set "PY=python"
if not defined PY (
  echo [FAIL] Python not found. Install from python.org and check Add to PATH.
  popd 2>nul
  pause
  exit /b 1
)

echo Share: %SHARE_ROOT%
echo Using: %PY%
echo Checking relay packages...
"%PY%" -c "import flask, win32print, waitress" 2>nul
if errorlevel 1 (
  echo Installing flask / pywin32 / requests / waitress...
  "%PY%" -m pip install -q flask "pywin32>=306" requests "waitress>=3.0.0"
)

for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8765" ^| findstr "LISTENING"') do (
  echo Stopping stale PID %%a on 8765...
  taskkill /F /PID %%a >nul 2>&1
)

echo Starting FiletrackRelay on http://127.0.0.1:8765/health ...
echo Keep this window open while labels need to print (or use NSSM service).
"%PY%" -m filetrack.relay.server --port 8765
echo.
echo Relay exited. Press any key to close.
popd 2>nul
pause >nul
