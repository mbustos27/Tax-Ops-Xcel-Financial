@echo off
:: LOCAL runner - deployed to C:\TaxOps\Reception\GO_RECEPTION.bat by share bootstrap.
:: Args: [--stay] [--share SHARE_ROOT] [-Repair|-NoStart]
if /I "%~1"=="--stay" goto :stay
start "TaxOps Reception Relays" cmd.exe /k call "%~f0" --stay %*
exit /b 0

:stay
setlocal EnableExtensions EnableDelayedExpansion
title TaxOps Reception Relays
set "RC=0"
set "LOCAL_ROOT=C:\TaxOps\Reception"
set "LOCAL_SCRIPTS=%LOCAL_ROOT%\scripts"
set "SHARE_ROOT="
set "EXTRA="

mkdir "C:\TaxOps\logs" 2>nul
echo %DATE% %TIME% LOCAL start "%~f0" args=%* > "C:\TaxOps\logs\go_reception_last.log"

echo ========================================================
echo   TaxOps Reception - LOCAL ensure relays + auto-start
echo ========================================================
echo   Running from: %LOCAL_ROOT%
echo.

:parse
if "%~1"=="" goto :parsed
if /I "%~1"=="--stay" (
  shift
  goto :parse
)
if /I "%~1"=="--share" (
  set "SHARE_ROOT=%~2"
  shift
  shift
  goto :parse
)
if /I "%~1"=="-ShareRoot" (
  set "SHARE_ROOT=%~2"
  shift
  shift
  goto :parse
)
if /I "%~1"=="-Repair" set "EXTRA=-Repair"
if /I "%~1"=="Repair" set "EXTRA=-Repair"
if /I "%~1"=="-NoStart" set "EXTRA=-NoStart"
shift
goto :parse

:parsed
if not defined SHARE_ROOT (
  if exist "%LOCAL_ROOT%\share_root.txt" (
    set /p SHARE_ROOT=<"%LOCAL_ROOT%\share_root.txt"
  )
)
:: Prefer UNC: elevated/Admin sessions cannot see mapped T:
if /I "%SHARE_ROOT%"=="T:" set "SHARE_ROOT=\\Xcel-server\taxops"
if /I "%SHARE_ROOT%"=="T:\" set "SHARE_ROOT=\\Xcel-server\taxops"
if not defined SHARE_ROOT set "SHARE_ROOT=\\Xcel-server\taxops"
if not exist "%SHARE_ROOT%\start_print_relay.bat" (
  if exist "T:\start_print_relay.bat" set "SHARE_ROOT=T:"
)
if not exist "%SHARE_ROOT%\start_print_relay.bat" (
  echo [FAIL] Cannot reach taxops share via UNC or T:
  echo        Expected \\Xcel-server\taxops\start_print_relay.bat
  set "RC=4"
  goto :done
)

if "%SHARE_ROOT:~-1%"=="\" set "SHARE_ROOT=%SHARE_ROOT:~0,-1%"

echo   Share: %SHARE_ROOT%
echo   Log:   C:\TaxOps\logs\go_reception_last.log
echo.

if not exist "%LOCAL_SCRIPTS%\ensure_reception_relays.ps1" (
  echo [FAIL] Missing %LOCAL_SCRIPTS%\ensure_reception_relays.ps1
  echo Re-run T:\GO_RECEPTION.bat to sync.
  set "RC=4"
  goto :done
)

echo Running LOCAL ensure_reception_relays.ps1 %EXTRA% ...
echo.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%LOCAL_SCRIPTS%\ensure_reception_relays.ps1" -ShareRoot "%SHARE_ROOT%" %EXTRA%
set "RC=%ERRORLEVEL%"
echo.
echo Exit code %RC%  (0=pass)
echo %DATE% %TIME% finished RC=%RC% >> "C:\TaxOps\logs\go_reception_last.log"

:done
echo.
echo --------------------------------------------------------
if "%RC%"=="0" (echo RESULT: PASS) else (echo RESULT: FAIL  RC=%RC%)
echo Window stays open. Type  exit  when done.
echo --------------------------------------------------------
endlocal & exit /b %RC%
