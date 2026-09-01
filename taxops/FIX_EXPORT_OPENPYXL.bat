@echo off
REM Fix TaxLog Export 500 (missing openpyxl) + restart Tax Log.
REM Run this ON the TaxOps server (as Administrator).

setlocal
echo.
echo  TaxLog export fix — install openpyxl + restart
echo  ----------------------------------------------

set "PY=C:\Users\Administrator\AppData\Local\Programs\Python\Python313\python.exe"
if not exist "%PY%" set "PY=%LocalAppData%\Programs\Python\Python313\python.exe"
if not exist "%PY%" set "PY=python"

echo Using: %PY%
"%PY%" -m pip install "openpyxl>=3.1.0"
if errorlevel 1 (
  echo pip install failed
  pause
  exit /b 1
)

"%PY%" -c "import openpyxl; print('openpyxl', openpyxl.__version__, 'OK')"
if errorlevel 1 (
  echo openpyxl still not importable
  pause
  exit /b 1
)

REM Clean orphan billing rows that break init_db on restart
set "DB=C:\TaxOps\taxops\taxops.db"
if exist "%DB%" (
  "%PY%" "C:\TaxOps\taxops\scripts\fix_orphan_client_billing.py" "%DB%"
)

echo.
echo Restart Tax Log now? Closing this will not restart.
echo Run: C:\TaxOps\restart_taxops.ps1   OR   T:\restart_taxops.ps1
echo.
pause
endlocal
