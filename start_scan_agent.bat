@echo off
:: TaxOps Scan Agent - KEEP THIS WINDOW OPEN on the reception PC.
:: Runs from a LOCAL copy under C:\TaxOps\ScanAgent (share T: / UNC is too slow).
setlocal EnableExtensions
title TaxOps Scan Agent

echo ========================================================
echo   TaxOps Scan Agent
echo ========================================================
echo.

:: %~dp0 always has a trailing backslash. Stripping it is REQUIRED before
:: passing paths into PowerShell quotes (otherwise \" eats the closing quote
:: and you get "Illegal characters in path").
set "SHARE_ROOT=%~dp0"
if "%SHARE_ROOT:~-1%"=="\" set "SHARE_ROOT=%SHARE_ROOT:~0,-1%"
set "SHARE_APP=%SHARE_ROOT%\taxops"
set "LOCAL_ROOT=C:\TaxOps\ScanAgent"
set "LOCAL_APP=%LOCAL_ROOT%\app"
set "LOCAL_AGENT=%LOCAL_APP%\scan_agent"

if not exist "%SHARE_APP%\scan_agent\server.py" (
  echo [FAIL] Cannot find taxops\scan_agent\server.py next to this bat
  echo        Expected: %SHARE_APP%\scan_agent\server.py
  echo        SHARE_ROOT=%SHARE_ROOT%
  pause
  exit /b 1
)

if not exist "%LOCAL_ROOT%" mkdir "%LOCAL_ROOT%" >nul 2>&1
if not exist "%LOCAL_APP%" mkdir "%LOCAL_APP%" >nul 2>&1

echo Syncing scan_agent to local disk...
echo   From: %SHARE_APP%\scan_agent
echo   To:   %LOCAL_AGENT%

:: Prefer robocopy in this bat (no PowerShell path quoting). Wipe first so
:: stale builds cannot stick.
if exist "%LOCAL_AGENT%" rd /s /q "%LOCAL_AGENT%" 2>nul
mkdir "%LOCAL_AGENT%" >nul 2>&1
robocopy "%SHARE_APP%\scan_agent" "%LOCAL_AGENT%" /E /IS /IT /R:2 /W:2 /NFL /NDL /NJH /NJS /NC /NS /NP >nul
set "RC=%ERRORLEVEL%"
if %RC% GEQ 8 (
  echo [FAIL] robocopy failed exit %RC%
  echo        Trying PowerShell sync fallback...
  powershell -NoProfile -ExecutionPolicy Bypass -File "%SHARE_ROOT%\sync_scan_agent_local.ps1" -ShareRoot "%SHARE_ROOT%" -LocalRoot "%LOCAL_ROOT%"
  if errorlevel 4 (
    echo [FAIL] Local sync failed.
    pause
    exit /b 5
  )
)

if not exist "%LOCAL_AGENT%\__init__.py" (
  echo. > "%LOCAL_AGENT%\__init__.py"
)
if exist "%LOCAL_AGENT%\__pycache__" rd /s /q "%LOCAL_AGENT%\__pycache__" 2>nul

if not exist "%LOCAL_AGENT%\server.py" (
  echo [FAIL] Local copy missing server.py after sync
  echo        %LOCAL_AGENT%\server.py
  pause
  exit /b 6
)

findstr /C:"com_sta_v4" "%LOCAL_AGENT%\server.py" >nul
if errorlevel 1 (
  echo [WARN] Local server.py does not contain com_sta_v4 - share may be stale
) else (
  echo [OK] Local copy has com_sta_v4
)

set "SCAN_AGENT_HOST=0.0.0.0"
set "SCAN_AGENT_PORT=8766"
set "PYTHONPATH=%LOCAL_APP%"

set "SCAN_AGENT_TOKEN="
if exist "%LOCAL_ROOT%\token.env" (
  for /f "usebackq tokens=1,* delims==" %%a in ("%LOCAL_ROOT%\token.env") do (
    if /I "%%a"=="SCAN_AGENT_TOKEN" set "SCAN_AGENT_TOKEN=%%b"
  )
)
if not defined SCAN_AGENT_TOKEN if exist "%SHARE_APP%\.env" (
  for /f "usebackq tokens=1,* delims==" %%a in ("%SHARE_APP%\.env") do (
    if /I "%%a"=="SCAN_AGENT_TOKEN" set "SCAN_AGENT_TOKEN=%%b"
  )
)
if not defined SCAN_AGENT_TOKEN (
  echo [FAIL] SCAN_AGENT_TOKEN not set.
  echo        Run scan_agent_wizard.bat once first.
  pause
  exit /b 2
)

set "PY="
if exist "%LOCALAPPDATA%\Programs\Python\Python314\python.exe" set "PY=%LOCALAPPDATA%\Programs\Python\Python314\python.exe"
if not defined PY if exist "%LOCALAPPDATA%\Programs\Python\Python313\python.exe" set "PY=%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
if not defined PY if exist "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" set "PY=%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
if not defined PY if exist "C:\TaxOps\taxops\.venv\Scripts\python.exe" set "PY=C:\TaxOps\taxops\.venv\Scripts\python.exe"
if not defined PY (
  where py >nul 2>&1 && (for /f "delims=" %%i in ('py -3 -c "import sys; print(sys.executable)"') do set "PY=%%i")
)
if not defined PY (
  echo [FAIL] Python not found. Install from python.org ^(Add to PATH^).
  pause
  exit /b 3
)

echo Python: %PY%
echo PYTHONPATH=%PYTHONPATH%
echo.

echo Stopping anything on port 8766...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8766" ^| findstr "LISTENING"') do (
  echo   kill PID %%a
  taskkill /F /PID %%a >nul 2>&1
)
timeout /t 2 /nobreak >nul

echo Checking packages (may take a minute on first run)...
"%PY%" -c "import flask, win32com.client, pythoncom, PIL" 2>nul
if errorlevel 1 (
  echo Installing flask / pywin32 / Pillow / pymupdf...
  "%PY%" -m pip install -q flask "pywin32>=306" Pillow pymupdf
  "%PY%" -m pywin32_postinstall -install >nul 2>&1
)

echo Verifying import scan_agent...
"%PY%" -c "import scan_agent.server; print('import_ok', scan_agent.server.REQUIRED_CODE_REV)"
if errorlevel 1 (
  echo [FAIL] Python cannot import scan_agent with PYTHONPATH=%PYTHONPATH%
  echo        Dir listing of local app:
  dir /b "%LOCAL_APP%"
  dir /b "%LOCAL_AGENT%"
  pause
  exit /b 7
)

echo.
echo 0.0.0.0 means accept connections from the office LAN - that is normal.
echo Health check URL: http://127.0.0.1:8766/health
echo Build expected:   com_sta_v4
echo.
echo KEEP THIS WINDOW OPEN. Close it only to stop the Scan Agent.
echo Binding port then printing READY (do not wait for WIA)...
echo.
cd /d "%LOCAL_APP%"
"%PY%" -m scan_agent.server --host 0.0.0.0 --port 8766
set "EXITCODE=%ERRORLEVEL%"
echo.
echo Scan Agent exited with code %EXITCODE%.
echo If that was unexpected, scroll up for the Python traceback.
pause
exit /b %EXITCODE%
