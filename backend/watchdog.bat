@echo off
REM ===========================================================
REM   Binance AutoTrade Watchdog
REM   - Check port 8021 (launcher) and 8020 (backend)
REM   - Auto-restart if down
REM   - Run by Task Scheduler every 5 minutes (outer supervisor)
REM   - Logs to watchdog.log
REM ===========================================================
setlocal enableextensions enabledelayedexpansion

set "ROOT=E:\My Project\Binance autotrend"
set "BACKEND=%ROOT%\backend"
set "LOG=%BACKEND%\watchdog.log"
set "LAUNCHER_PORT=8021"
set "BACKEND_PORT=8020"
REM Bind the spawned backend to all interfaces so the dashboard is
REM reachable from a phone over Tailscale (http://100.89.42.68:8020/dashboard/).
set "BACKEND_HOST=0.0.0.0"

call :log "[watchdog] tick start"

REM ===========================================================
REM   Rotate watchdog.log once it exceeds 5 MB (keep 3 generations)
REM ===========================================================
for %%A in ("%LOG%") do if %%~zA GEQ 5242880 (
    if exist "%LOG%.2" del "%LOG%.2"
    if exist "%LOG%.1" ren "%LOG%.1" "%LOG%.2"
    ren "%LOG%" "%LOG%.1"
    call :log "[watchdog] rotated watchdog.log (was >5 MB)"
)

REM ===========================================================
REM   [1/3] Check launcher (8021)
REM ===========================================================
powershell -NoProfile -Command "$p=%LAUNCHER_PORT%; if (Test-NetConnection -ComputerName 127.0.0.1 -Port $p -WarningAction SilentlyContinue -InformationLevel Quiet) { exit 0 } else { exit 1 }" >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    call :log "[watchdog] launcher :%LAUNCHER_PORT% OK"
) else (
    call :log "[watchdog] launcher :%LAUNCHER_PORT% DOWN - restarting"
    for /f "tokens=5" %%I in ('netstat -aon ^| findstr ":%LAUNCHER_PORT% " ^| findstr LISTENING 2^>nul') do (
        taskkill /PID %%I /T /F >nul 2>&1
    )
    start "BinanceLauncher" /MIN cmd /c "cd /d %BACKEND% && set BACKEND_HOST=0.0.0.0 && .venv\Scripts\python.exe launcher.py"
    timeout /t 3 /nobreak >nul
    powershell -NoProfile -Command "if (Test-NetConnection -ComputerName 127.0.0.1 -Port %LAUNCHER_PORT% -WarningAction SilentlyContinue -InformationLevel Quiet) { exit 0 } else { exit 1 }" >nul 2>&1
    if !ERRORLEVEL! EQU 0 (
        call :log "[watchdog] launcher restarted OK"
    ) else (
        call :log "[watchdog] launcher RESTART FAILED - check start_launcher.bat"
    )
)

REM ===========================================================
REM   [2/3] Check backend (8020) - ask launcher to restart if down
REM ===========================================================
powershell -NoProfile -Command "$p=%BACKEND_PORT%; if (Test-NetConnection -ComputerName 127.0.0.1 -Port $p -WarningAction SilentlyContinue -InformationLevel Quiet) { exit 0 } else { exit 1 }" >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    call :log "[watchdog] backend  :%BACKEND_PORT% OK"
) else (
    call :log "[watchdog] backend  :%BACKEND_PORT% DOWN - POST /restart to launcher"
    powershell -NoProfile -Command "try { (Invoke-WebRequest -Uri 'http://127.0.0.1:%LAUNCHER_PORT%/restart' -Method POST -ContentType 'application/json' -Body '{}' -TimeoutSec 8 -UseBasicParsing -ErrorAction Stop).StatusCode; exit 0 } catch { exit 1 }" >nul 2>&1
    if !ERRORLEVEL! EQU 0 (
        call :log "[watchdog] backend restart requested via launcher"
        timeout /t 4 /nobreak >nul
        powershell -NoProfile -Command "if (Test-NetConnection -ComputerName 127.0.0.1 -Port %BACKEND_PORT% -WarningAction SilentlyContinue -InformationLevel Quiet) { exit 0 } else { exit 1 }" >nul 2>&1
        if !ERRORLEVEL! EQU 0 (
            call :log "[watchdog] backend restarted OK"
        ) else (
            call :log "[watchdog] backend still DOWN after launcher restart - manual check needed"
        )
    ) else (
        call :log "[watchdog] launcher also DOWN or unreachable - skipping backend restart"
    )
)

call :log "[watchdog] tick done"
endlocal
exit /b 0

REM ===========================================================
REM   :log helper - append timestamped line to watchdog.log
REM ===========================================================
:log
set "TS=%date% %time%"
echo %TS% %~1 >> "%LOG%"
exit /b 0
