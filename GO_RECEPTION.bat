@echo off
:: BOOTSTRAP (share) - sync scripts to C:\TaxOps\Reception then run LOCAL copy.
:: Double-click: T:\GO_RECEPTION.bat  or  \\Xcel-server\taxops\GO_RECEPTION.bat
:: Optional: -Repair  -NoStart
setlocal EnableExtensions

if /I "%~1"=="--stay" goto :stay
start "TaxOps Reception Relays" cmd.exe /k call "%~f0" --stay %*
exit /b 0

:stay
:: %1 == --stay ; real args from %2
title TaxOps Reception Relays - sync then local
set "RC=0"

mkdir "C:\TaxOps\logs" 2>nul
echo %DATE% %TIME% BOOTSTRAP "%~f0" > "C:\TaxOps\logs\go_reception_last.log"

echo ========================================================
echo   TaxOps Reception - deploy local + run
echo ========================================================
echo.

set "SHARE_ROOT=%~dp0"
if "%SHARE_ROOT:~-1%"=="\" set "SHARE_ROOT=%SHARE_ROOT:~0,-1%"

pushd "%SHARE_ROOT%" 2>nul
if errorlevel 1 (
  echo [FAIL] Cannot access share: %SHARE_ROOT%
  set "RC=4"
  goto :fail
)
set "SHARE_ROOT=%CD%"
if "%SHARE_ROOT:~-1%"=="\" set "SHARE_ROOT=%SHARE_ROOT:~0,-1%"

echo   Share: %SHARE_ROOT%
echo.

set "LOCAL_ROOT=C:\TaxOps\Reception"
set "LOCAL_SCRIPTS=%LOCAL_ROOT%\scripts"
mkdir "%LOCAL_SCRIPTS%" 2>nul

echo [1/2] Syncing scripts to %LOCAL_ROOT% ...

:: Prefer PowerShell sync; fall back to robocopy+copy if PS -File on share fails
if exist "%SHARE_ROOT%\sync_reception_scripts_local.ps1" (
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%SHARE_ROOT%\sync_reception_scripts_local.ps1" -ShareRoot "%SHARE_ROOT%" -LocalRoot "%LOCAL_ROOT%"
  if errorlevel 1 (
    echo [WARN] PS sync failed - trying robocopy fallback
    goto :fallback_copy
  )
  goto :synced
)

:fallback_copy
robocopy "%SHARE_ROOT%\taxops\scripts" "%LOCAL_SCRIPTS%" ensure_reception_relays.ps1 check_reception.ps1 install_print_relay_service.ps1 install_scan_agent_task.ps1 uninstall_print_relay_service.ps1 uninstall_scan_agent_task.ps1 /R:2 /W:2 /IS /IT /NFL /NDL /NJH /NJS /NC /NS /NP >nul
if errorlevel 8 (
  echo [FAIL] robocopy could not copy scripts
  set "RC=5"
  goto :fail
)
if not exist "%SHARE_ROOT%\GO_RECEPTION_local.bat" (
  echo [FAIL] GO_RECEPTION_local.bat missing on share
  set "RC=6"
  goto :fail
)
copy /Y "%SHARE_ROOT%\GO_RECEPTION_local.bat" "%LOCAL_ROOT%\GO_RECEPTION.bat" >nul
echo %SHARE_ROOT%> "%LOCAL_ROOT%\share_root.txt"

:synced
if not exist "%LOCAL_ROOT%\GO_RECEPTION.bat" (
  echo [FAIL] Local runner missing after sync: %LOCAL_ROOT%\GO_RECEPTION.bat
  set "RC=7"
  goto :fail
)
if not exist "%LOCAL_SCRIPTS%\ensure_reception_relays.ps1" (
  echo [FAIL] Local ensure script missing
  set "RC=7"
  goto :fail
)

echo [OK] Local copy ready
echo.
echo [2/2] Launching LOCAL runner ...
echo %DATE% %TIME% handing off to local >> "C:\TaxOps\logs\go_reception_last.log"
popd 2>nul

:: Always hand UNC to local/PS - mapped T: disappears under Admin / services
set "SHARE_UNC=\\Xcel-server\taxops"
if /I "%SHARE_ROOT%"=="T:" set "SHARE_ROOT=%SHARE_UNC%"
if /I "%SHARE_ROOT:~0,2%"=="T:" set "SHARE_ROOT=%SHARE_UNC%"
if not exist "%SHARE_ROOT%\start_print_relay.bat" set "SHARE_ROOT=%SHARE_UNC%"

:: Stay in this cmd /k window - run local with --stay (no second window)
call "%LOCAL_ROOT%\GO_RECEPTION.bat" --stay --share "%SHARE_ROOT%" %~2 %~3 %~4
set "RC=%ERRORLEVEL%"
exit /b %RC%

:fail
popd 2>nul
echo.
echo --------------------------------------------------------
echo BOOTSTRAP FAILED  RC=%RC%
echo Log: C:\TaxOps\logs\go_reception_last.log
echo Window stays open. Type  exit  when done.
echo --------------------------------------------------------
endlocal
