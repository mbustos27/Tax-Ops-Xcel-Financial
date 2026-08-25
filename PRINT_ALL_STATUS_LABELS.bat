@echo off
:: Print one STATUS station label for every allowed status (sample set).
:: Run ON reception (label printer attached). UNC-safe via pushd.
::
::   T:\PRINT_ALL_STATUS_LABELS.bat
::   T:\PRINT_ALL_STATUS_LABELS.bat --dry-run
::
:: Uses: python -m filetrack.labels.status_codes --all
setlocal EnableExtensions EnableDelayedExpansion
title TaxOps - Print all STATUS labels

set "SHARE_ROOT=%~dp0"
if "%SHARE_ROOT:~-1%"=="\" set "SHARE_ROOT=%SHARE_ROOT:~0,-1%"

pushd "%SHARE_ROOT%" 2>nul
if errorlevel 1 (
  echo [FAIL] Cannot access share: %SHARE_ROOT%
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

if not exist "filetrack\labels\status_codes.py" (
  echo [FAIL] filetrack\labels\status_codes.py missing under %SHARE_ROOT%\taxops
  popd 2>nul
  pause
  exit /b 1
)

:: Printer + token from machine env (same as print relay)
if not defined FILETRACK_PRINTER if exist "C:\TaxOps\PrintRelay\relay.env" (
  for /f "usebackq tokens=1,* delims==" %%a in ("C:\TaxOps\PrintRelay\relay.env") do (
    if /I "%%a"=="FILETRACK_PRINTER" set "FILETRACK_PRINTER=%%b"
  )
)
if not defined FILETRACK_PRINTER set "FILETRACK_PRINTER=4BARCODE 4B-2054A"

set "PY="
if exist "C:\Users\Windows 10\AppData\Local\Programs\Python\Python314\python.exe" set "PY=C:\Users\Windows 10\AppData\Local\Programs\Python\Python314\python.exe"
if not defined PY if exist "%LOCALAPPDATA%\Programs\Python\Python314\python.exe" set "PY=%LOCALAPPDATA%\Programs\Python\Python314\python.exe"
if not defined PY where python >nul 2>&1 && set "PY=python"
if not defined PY (
  echo [FAIL] Python not found.
  popd 2>nul
  pause
  exit /b 1
)

echo ========================================================
echo   Print ALL status sample labels
echo   Size LOCKED: 2.625 in x 1 in  (532 x 203 dots @ 203dpi)
echo ========================================================
echo   Share:   %SHARE_ROOT%
echo   Printer: %FILETRACK_PRINTER%
echo   Python:  %PY%
echo.
echo   One label each (single print job, no extra feed between):
echo     PENDING INTAKE, PROCESSING, HOLD, FINALIZE,
echo     PICKUP, EFILE READY, LOG OUT, REJECTED
echo.
echo   Expect exactly 8 labels of stock — not larger gaps.
echo.

set "EXTRA=%*"
if /I "%~1"=="--dry-run" set "EXTRA=--dry-run"
if /I "%~1"=="-dry-run" set "EXTRA=--dry-run"

if /I "%EXTRA%"=="--dry-run" (
  echo DRY RUN - ZPL to screen only
  "%PY%" -m filetrack.labels.status_codes --all --dry-run
) else (
  echo Sending to printer...
  "%PY%" -m filetrack.labels.status_codes --all --printer "%FILETRACK_PRINTER%"
)
set "RC=%ERRORLEVEL%"
echo.
if "%RC%"=="0" (
  echo [OK] Done.
) else (
  echo [FAIL] exit %RC%
  echo        Is the print relay / printer up?  T:\GO_RECEPTION.bat
)
popd 2>nul
echo.
pause
exit /b %RC%
