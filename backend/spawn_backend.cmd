@echo off
setlocal
set "ROOT=%~dp0"
cd /d "%ROOT%"

REM Wrapper to start backend (port 8020) with stdout/stderr captured to a log
REM Use run_backend.py so WindowsSelectorEventLoopPolicy is applied before uvicorn spins up.
set "PY=%ROOT%.venv\Scripts\python.exe"
set "RUNNER=%ROOT%run_backend.py"
set "LOG=%ROOT%uvicorn.launcher.out.log"

REM Truncate log on each start
echo [%date% %time%] Starting backend via %PY% %RUNNER% > "%LOG%"

"%PY%" "%RUNNER%" >> "%LOG%" 2>&1
exit /b %ERRORLEVEL%
