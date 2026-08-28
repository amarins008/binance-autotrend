@echo off
REM ===========================================================
REM   Start Binance AutoTrade - One-click launcher (Desktop)
REM   Starts BOTH backend (8020) AND launcher (8021) so the
REM   watchdog always finds both services healthy.
REM   Binds to 0.0.0.0 for LAN / Tailscale access.
REM ===========================================================
setlocal enableextensions enabledelayedexpansion

REM --- Auto-sync with the other copy (Desktop <-> working folder) ---
call "E:\My Project\Binance autotrend\sync_start_autotrade.bat"

set "ROOT=E:\My Project\Binance autotrend"
set "BACKEND=%ROOT%\backend"
set "HEALTH_TIMEOUT=30"

REM Performance tuning
set "SCAN_ANALYZE_CONCURRENCY=2"
set "DATA_GET_TIMEOUT_SEC=4.5"
set "DATA_GET_CONNECT_TIMEOUT_SEC=2.0"

REM Enable TradingView MCP integration inside the backend
set "TRADINGVIEW_ENABLED=true"

REM Bind to all interfaces so the dashboard is reachable from mobile / Tailscale
set "BACKEND_HOST=0.0.0.0"

echo.
echo =====================================================
echo   Binance AutoTrade - One-click launcher
echo   Project: %ROOT%
echo =====================================================
echo.

cd /d "%BACKEND%"
if errorlevel 1 (
    echo [ERROR] Cannot enter backend directory: %BACKEND%
    pause
    exit /b 1
)

REM --- Step 1: Stop any pre-existing services -------------------------
echo [1/5] Stopping any existing services on 8020/8021...
for %%P in (8020 8021) do (
    for /f "tokens=5" %%I in ('netstat -aon ^| findstr ":%P " ^| findstr LISTENING 2^>nul') do (
        taskkill /PID %%I /F /T >nul 2>&1
    )
)
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { ($_.Name -eq 'python.exe' -or $_.Name -eq 'cmd.exe') -and ($_.CommandLine -like '*run_backend.py*' -or $_.CommandLine -like '*launcher.py*') } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }" >nul 2>&1
ping -n 3 -w 1000 >nul
echo       done.
echo.

REM --- Step 2: Ensure firewall rule for port 8020 ---------------------
echo [2/5] Checking firewall rule for port 8020...
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
echo [3/5] Starting backend (port 8020, binding 0.0.0.0)...
set "PYTHONUTF8=1"
start "Binance Backend (8020)" ".venv\Scripts\python.exe" run_backend.py
echo       waiting for port 8020...
call :wait_for_http 8020 %HEALTH_TIMEOUT%
if errorlevel 1 (
    echo [ERROR] Backend did not start within %HEALTH_TIMEOUT%s. Check the backend window for errors.
    pause
    exit /b 1
)
echo       backend ready.
echo.

REM --- Step 4: Start launcher on port 8021 ---------------------------
echo [4/5] Starting launcher (port 8021)...
start "Binance Launcher (8021)" ".venv\Scripts\python.exe" launcher.py
echo       waiting for port 8021...
call :wait_for_port 8021 %HEALTH_TIMEOUT%
if errorlevel 1 (
    echo       [WARN] Launcher did not start within %HEALTH_TIMEOUT%s. Watchdog will report :8021 as DOWN.
    echo       (backend is still running, you can investigate the launcher window)
) else (
    echo       launcher ready.
)
echo.

REM --- Step 5: Open dashboard in browser -----------------------------
echo [5/5] Opening dashboard...
start "" "http://127.0.0.1:8020/dashboard/"
echo.

echo =====================================================
echo   Service started:
echo     - Dashboard (local)  : http://127.0.0.1:8020/dashboard/
echo     - Dashboard (mobile) : use this PC's Tailscale IP
echo     - Backend health     : http://127.0.0.1:8020/health
echo     - Launcher (8021)    : watchdog target
echo     - TradingView MCP    : ENABLED
echo.
echo   To stop: run "Kill Binance AutoTrade.bat" from Desktop.
echo =====================================================
echo.
endlocal
exit /b 0

REM --- Subroutines (use FOR /L loops, no nested GOTO labels) ----------
:wait_for_http
set "WP_PORT=%~1"
set "WP_TIMEOUT=%~2"
for /l %%i in (1,1,%WP_TIMEOUT%) do (
    curl.exe -s -o nul -m 2 http://127.0.0.1:%WP_PORT%/health >nul 2>&1
    if not errorlevel 1 exit /b 0
    ping -n 2 -w 1000 >nul
)
exit /b 1

:wait_for_port
set "WP_PORT=%~1"
set "WP_TIMEOUT=%~2"
for /l %%i in (1,1,%WP_TIMEOUT%) do (
    netstat -aon | findstr ":%WP_PORT% " | findstr LISTENING >nul 2>&1
    if not errorlevel 1 exit /b 0
    ping -n 2 -w 1000 >nul
)
exit /b 1
