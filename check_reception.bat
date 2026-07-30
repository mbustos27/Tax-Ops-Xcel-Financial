@echo off
:: Fast reception health (print 8765 + scan 8766). Exit 0=OK 1=FAIL
setlocal EnableExtensions
set "SHARE_ROOT=%~dp0"
if "%SHARE_ROOT:~-1%"=="\" set "SHARE_ROOT=%SHARE_ROOT:~0,-1%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SHARE_ROOT%\taxops\scripts\check_reception.ps1"
exit /b %ERRORLEVEL%
