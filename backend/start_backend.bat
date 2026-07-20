@echo off
REM Backend launcher — sets working directory and env, then starts uvicorn.
REM Called by "Start Binance AutoTrade.bat".

cd /d "%~dp0"
if errorlevel 1 (
    echo [ERROR] Cannot cd to %~dp0
    exit /b 1
)

set BACKEND_HOST=0.0.0.0
set PYTHONUTF8=1

REM Use full path to venv python to avoid path resolution issues
"%~dp0.venv\Scripts\python.exe" "%~dp0run_backend.py"
