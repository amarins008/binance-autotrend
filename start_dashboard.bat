@echo off
setlocal enableextensions

REM One-click clean launcher: ensure backend + cmux + dashboard are up,
REM then open the dashboard in the default browser.

set "ROOT=%~dp0"
cd /d "%ROOT%"

echo [1/3] Starting or checking Hermes service + cmux...
call python backend/cmux_cli.py start >nul 2>&1
if errorlevel 1 (
  echo Failed to start Hermes/cmux.
  exit /b 1
)

echo [2/3] Waiting for dashboard proxy...
set "READY="
for /l %%i in (1,1,15) do (
  curl -s http://127.0.0.1:8040/status >nul 2>&1 && set "READY=1" && goto :ready
  timeout /t 1 /nobreak >nul
)
:ready
if not defined READY (
  echo Dashboard not ready on 8040.
  exit /b 1
)

echo [3/3] Opening dashboard...
start "" http://127.0.0.1:8040/

echo Dashboard ready: http://127.0.0.1:8040/
endlocal