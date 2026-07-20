@echo off
setlocal
set "ROOT=%~dp0"
cd /d "%ROOT%"

REM Wrapper to start launcher (port 8021) with stdout/stderr captured
set "PY=%ROOT%.venv\Scripts\python.exe"
set "RUNNER=%ROOT%launcher.py"
set "LOG=%ROOT%launcher.out.log"

echo [%date% %time%] Starting launcher via %PY% %RUNNER% > "%LOG%"
"%PY%" "%RUNNER%" >> "%LOG%" 2>&1
exit /b %ERRORLEVEL%
