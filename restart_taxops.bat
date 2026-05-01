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
start "" python app.py

timeout /t 3 /nobreak >nul

:: Confirm it's up
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":5000 " ^| findstr "LISTENING"') do (
    echo.
    echo  TaxOps is running on http://localhost:5000  (PID %%a)
    echo.
    goto :done
)
echo  WARNING: TaxOps may not have started. Check for errors.
:done
pause
