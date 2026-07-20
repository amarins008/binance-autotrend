@echo off
REM ===========================================================
REM   Start Binance AutoTrade - One-click launcher
REM   Starts single-process backend on port 8020 (dashboard
REM   is served at /dashboard/ by the same process).
REM   Double-click from Desktop to start everything.
REM   Binds to 0.0.0.0 for LAN / Tailscale access.
REM
REM   This file lives in  backend\  — all paths resolve from here.
REM ===========================================================
setlocal enableextensions enabledelayedexpansion

REM --- Resolve BACKEND dir to this file's own directory -----------------
pushd "%~dp0"
set "BACKEND=%CD%"
popd
set "PYTHON=%BACKEND%\.venv\Scripts\python.exe"
set "HEALTH_TIMEOUT=30"

echo.
echo =====================================================
echo   Binance AutoTrade - One-click launcher
echo   Backend: %BACKEND%
echo =====================================================
echo.

cd /d "%BACKEND%"
if errorlevel 1 (
    echo [ERROR] Cannot enter backend directory: %BACKEND%
    pause
    exit /b 1
)

REM --- Step 1: Stop any pre-existing services -------------------------
echo [1/4] Stopping any existing services on 8020...
for %%P in (8020) do (
    for /f "tokens=5" %%I in ('netstat -aon ^| findstr ":%%P " ^| findstr LISTENING 2^>nul') do (
        taskkill /PID %%I /F /T >nul 2>&1
    )
)
timeout /t 2 /nobreak >nul
echo       done.
echo.

REM --- Step 2: Ensure firewall rule for port 8020 ---------------------
echo [2/4] Checking firewall rule for port 8020...
netsh advfirewall firewall show rule name="BinanceAutoTrade-8020" >nul 2>&1
if errorlevel 1 (
    echo       Creating firewall rule...
    netsh advfirewall firewall add rule name="BinanceAutoTrade-8020" dir=in action=allow protocol=TCP localport=8020 profile=any >nul 2>&1
    if errorlevel 1 (
        echo       [WARN] Could not create firewall rule. You may see a popup.
    ) else (
        echo       rule created.
    )
) else (
    echo       rule exists.
)
echo.

REM --- Step 3: Start backend on port 8020 ----------------------------
REM    Run python directly — no nested .bat to avoid path-with-spaces issues.
echo [3/4] Starting backend (port 8020, binding 0.0.0.0)...
set "BACKEND_HOST=0.0.0.0"
set "PYTHONUTF8=1"
start "Binance Backend (8020)" cmd /k ""%PYTHON%" run_backend.py"
echo       waiting for port 8020...
call :wait_for_http 8020 %HEALTH_TIMEOUT%
if errorlevel 1 (
    echo [ERROR] Backend did not start within %HEALTH_TIMEOUT%s. Check the backend window for errors.
    pause
    exit /b 1
)
echo       backend ready.
echo.

REM --- Step 4: Open dashboard in browser -----------------------------
echo [4/4] Opening dashboard...
start "" "http://127.0.0.1:8020/dashboard/"
echo.

echo =====================================================
echo   Service started:
echo     - Dashboard : http://127.0.0.1:8020/dashboard/
echo     - LAN       : http://[your-ip]:8020/dashboard/
echo     - Tailscale : http://100.x.x.x:8020/dashboard/
echo.
echo   To stop: run "Kill Binance AutoTrade.bat" from Desktop.
echo =====================================================
echo.
endlocal
exit /b 0

REM --- Subroutines -------------------------------------------------------

:wait_for_http
set "WP_PORT=%~1"
set "WP_TIMEOUT=%~2"
set "WP_ELAPSED=0"
:http_loop
set /a "WP_ELAPSED+=1"
curl -s -o nul --max-time 2 "http://127.0.0.1:%WP_PORT%/health" >nul 2>&1
if not errorlevel 1 exit /b 0
if !WP_ELAPSED! GEQ %WP_TIMEOUT% exit /b 1
timeout /t 1 /nobreak >nul
goto :http_loop
