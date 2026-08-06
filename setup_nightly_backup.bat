@echo off
:: ============================================================
::  TaxOps — Nightly Backup Setup / Run
::
::  On the FILE SERVER (preferred), elevated:
::    setup_nightly_backup.bat
::        → registers Task Scheduler job TaxOpsNightlyDbBackup
::          (daily 2:10 AM, SYSTEM, WAL-safe Python backup)
::
::  Anywhere with share access:
::    setup_nightly_backup.bat /run
::        → runs one backup now into <share>\backups\
::
::  Share layout expected:
::    <share>\taxops\          app + taxops.db
::    <share>\backups\         backup destination
:: ============================================================

setlocal EnableExtensions
set "ROOT=%~dp0"
set "ROOT=%ROOT:~0,-1%"
set "APP=%ROOT%\taxops"
set "BACKUP_DIR=%ROOT%\backups"
set "ERRLOG=%BACKUP_DIR%\backup_error.log"
set "PS1=%APP%\scripts\register-nightly-backup-task.ps1"

if /i "%~1"=="/run" goto :do_backup
if /i "%~1"=="run" goto :do_backup

if not exist "%PS1%" (
  echo ERROR: missing %PS1%
  exit /b 1
)

echo Registering TaxOpsNightlyDbBackup via:
echo   %PS1%
echo.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%PS1%" -RunNow
set ERR=%ERRORLEVEL%
if %ERR% NEQ 0 (
  echo.
  echo Registration failed ^(exit %ERR%^). Re-run elevated on the file server.
  echo If this is a workstation, the task would run here — prefer the server.
  exit /b %ERR%
)
echo.
echo Done. Nightly job is TaxOpsNightlyDbBackup at 2:10 AM.
exit /b 0

:do_backup
if not exist "%APP%\scripts\nightly_backup_db.py" (
  echo ERROR: backup script not found at %APP%\scripts\nightly_backup_db.py
  exit /b 1
)
if not exist "%APP%\taxops.db" (
  echo ERROR: database not found at %APP%\taxops.db
  exit /b 1
)
if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"

set "TAXOPS_DB=%APP%\taxops.db"
set "TAXOPS_BACKUP_DIR=%BACKUP_DIR%"
set "TAXOPS_BACKUP_ERROR_LOG=%ERRLOG%"

set "PY="
if exist "%APP%\.venv\Scripts\python.exe" set "PY=%APP%\.venv\Scripts\python.exe"
if not defined PY if exist "C:\TaxOps\taxops\.venv\Scripts\python.exe" set "PY=C:\TaxOps\taxops\.venv\Scripts\python.exe"
if not defined PY where py >nul 2>&1 && set "PY=py"
if not defined PY where python >nul 2>&1 && set "PY=python"
if not defined PY (
  echo ERROR: no Python found
  exit /b 1
)

echo Running backup...
echo   DB=%TAXOPS_DB%
echo   OUT=%TAXOPS_BACKUP_DIR%
echo   PY=%PY%
pushd "%APP%"
"%PY%" scripts\nightly_backup_db.py
set ERR=%ERRORLEVEL%
popd
if %ERR% NEQ 0 (
  echo BACKUP FAILED — see %ERRLOG%
  exit /b %ERR%
)
echo BACKUP OK
dir /o-d "%BACKUP_DIR%\taxops_backup_*.sqlite" | more +0
exit /b 0
