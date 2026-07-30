@echo off
:: TaxOps Scan Agent setup — launches the guided wizard on the RECEPTION PC.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scan_agent_wizard.ps1" -Mode Install %*
set ERR=%ERRORLEVEL%
if not "%ERR%"=="0" pause
exit /b %ERR%
