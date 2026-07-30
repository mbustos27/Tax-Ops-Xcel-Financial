@echo off
:: Kill + start visible Scan Agent (same as GO_SCAN_AGENT.bat).
setlocal EnableExtensions
cd /d "%~dp0"
call "%~dp0GO_SCAN_AGENT.bat" %*
