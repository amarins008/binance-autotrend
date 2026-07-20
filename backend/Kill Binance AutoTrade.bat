@echo off
REM ===========================================================
REM   Kill Binance AutoTrade - One-click shutdown
REM   Stops backend (8020), launcher (8021).
REM   Double-click from Desktop to stop everything.
REM ===========================================================
setlocal enableextensions enabledelayedexpansion
echo.
echo =====================================================
echo   Binance AutoTrade - One-click shutdown
echo =====================================================
echo.

REM --- Step 1: Kill by port -------------------------------------------
for %%P in (8020 8021) do (
    echo [%%P] Stopping processes on port %%P...
    set "FOUND=0"
    for /f "tokens=5" %%I in ('netstat -aon ^| findstr ":%%P " ^| findstr LISTENING 2^>nul') do (
        set "FOUND=1"
        echo       killing PID %%I
        taskkill /PID %%I /F /T >nul 2>&1
    )
    if "!FOUND!"=="0" echo       nothing running.
)

REM --- Step 2: Kill named python processes from our project -----------
echo.
echo [cleanup] Killing python processes from Binance autotrend project...
set "PIDS_TEMP=%TEMP%\bat_kill_pids_%RANDOM%.txt"
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*Binance autotrend*' } | ForEach-Object { $_.ProcessId }" 2>nul > "%PIDS_TEMP%"
set "FOUND=0"
for /f %%I in (%PIDS_TEMP%) do (
    set "FOUND=1"
    echo       killing PID %%I
    taskkill /PID %%I /F /T >nul 2>&1
)
if "!FOUND!"=="0" echo       nothing running.
del "%PIDS_TEMP%" 2>nul

timeout /t 2 /nobreak >nul
echo.
echo =====================================================
echo   All Binance AutoTrade services stopped.
echo =====================================================
echo.
timeout /t 2 /nobreak >nul
endlocal
exit /b 0
