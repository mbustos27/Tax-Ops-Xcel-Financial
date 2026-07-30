@echo off
:: TaxOps print relay - run ON the reception PC (label printer attached).
setlocal EnableExtensions
cd /d "%~dp0taxops"
if not exist "filetrack\relay\server.py" (
  echo [FAIL] Cannot find taxops\filetrack\relay\server.py under %~dp0
  pause
  exit /b 1
)

set "FILETRACK_PRINTER=4BARCODE 4B-2054A"
if not defined FILETRACK_RELAY_TOKEN set "FILETRACK_RELAY_TOKEN=zv8z42FQcfufRAvQDrJMXMXgb7Mpttdq"
set "FILETRACK_RELAY_HOST=0.0.0.0"
set "FILETRACK_RELAY_PORT=8765"

set "PY="
if exist "%LOCALAPPDATA%\Programs\Python\Python314\python.exe" set "PY=%LOCALAPPDATA%\Programs\Python\Python314\python.exe"
if not defined PY if exist "%LOCALAPPDATA%\Programs\Python\Python313\python.exe" set "PY=%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
if not defined PY (
  where py >nul 2>&1 && (for /f "delims=" %%i in ('py -3 -c "import sys; print(sys.executable)"') do set "PY=%%i")
)
if not defined PY where python >nul 2>&1 && set "PY=python"
if not defined PY (
  echo [FAIL] Python not found. Install from python.org and check Add to PATH.
  pause
  exit /b 1
)

echo Using: %PY%
echo Checking relay packages...
"%PY%" -c "import flask, win32print" 2>nul
if errorlevel 1 (
  echo Installing flask / pywin32 / requests...
  "%PY%" -m pip install -q flask "pywin32>=306" requests
)

for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8765" ^| findstr "LISTENING"') do (
  echo Stopping stale PID %%a on 8765...
  taskkill /F /PID %%a >nul 2>&1
)

echo Starting FiletrackRelay on http://127.0.0.1:8765/health ...
echo Keep this window open while labels need to print.
"%PY%" -m filetrack.relay.server --port 8765
echo.
echo Relay exited. Press any key to close.
pause >nul
