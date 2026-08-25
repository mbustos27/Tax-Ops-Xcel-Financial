@echo off
title TaxOps Dev Server

:: Kill any existing python process running app.py on port 5000
echo Checking for running TaxOps instance...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":5000 " ^| findstr "LISTENING"') do (
    echo Found process on port 5000 (PID %%a) — stopping...
    taskkill /PID %%a /F >nul 2>&1
    timeout /t 1 /nobreak >nul
)

:: Also kill any stray python app.py processes
taskkill /F /IM python.exe /FI "WINDOWTITLE eq TaxOps*" >nul 2>&1

echo Starting TaxOps...
cd /d "%~dp0taxops"
REM CACHE #141: After JS/CSS changes, bump TAXOPS_VERSION or TAXOPS_APP_VERSION (NSSM / taxops\.env) and restart.
:: WSGI: app.py serves with Waitress (multi-threaded) unless FLASK_DEBUG=1 — see GitHub #136
set FLASK_DEBUG=0
set OLLAMA_BASE_URL=http://192.168.1.173:11434
start "" python app.py

:: Wait up to 15 s for Waitress to bind port 5000 (SMOKE-3)
echo Waiting for service to start...
set /a _tries=0
:waitloop
timeout /t 1 /nobreak >nul
set /a _tries+=1
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":5000 " ^| findstr "LISTENING" 2^>nul') do (
    echo.
    echo  TaxOps is running on http://localhost:5000  (PID %%a)
    echo.
    goto :smoke
)
if %_tries% LSS 15 goto :waitloop
echo  WARNING: TaxOps may not have started — smoke test skipped.
goto :done

:: SMOKE-3: run smoke_test.py after confirmed startup
:smoke
echo Running smoke tests...
python smoke_test.py http://192.168.1.173:5000
if %errorlevel% NEQ 0 (
    echo.
    echo  SMOKE TEST FAILED — check C:\TaxOps\logs\smoke.log for details.
    echo.
) else (
    echo  All smoke checks passed.
)

:done
pause
