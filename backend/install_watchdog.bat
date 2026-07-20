@echo off
REM ===========================================================
REM   Install Binance AutoTrade Watchdog as a Scheduled Task
REM   - Runs watchdog.bat every 5 minutes
REM   - Also runs at system startup
REM   - Run this AS ADMINISTRATOR (right-click -> Run as administrator)
REM ===========================================================
setlocal

set "WD_BAT=E:\My Project\Binance autotrend\backend\watchdog.bat"
set "TASK_NAME=BinanceAutotrendWatchdog"

if not exist "%WD_BAT%" (
    echo ERROR: Missing script: %WD_BAT%
    pause
    exit /b 1
)

echo.
echo === Removing old task (if any) ===
schtasks /Delete /TN "%TASK_NAME%" /F >nul 2>&1

echo === Creating scheduled task: %TASK_NAME% ===
schtasks /Create ^
  /TN "%TASK_NAME%" ^
  /TR "\"%WD_BAT%\"" ^
  /SC MINUTE ^
  /MO 5 ^
  /RL LIMITED ^
  /F ^
  /RU "%USERNAME%"
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo WARNING: Failed to create task. Need admin rights.
    echo Try: right-click this file and choose "Run as administrator".
    pause
    exit /b 1
)

echo.
echo === Verifying task ===
schtasks /Query /TN "%TASK_NAME%" /V /FO LIST | findstr /R /C:"TaskName" /C:"Status" /C:"Schedule Type" /C:"Run As User"
echo.
echo SUCCESS: Watchdog task installed.
echo   Task:    %TASK_NAME%
echo   Script:  %WD_BAT%
echo   Runs:    every 5 minutes + on demand
echo   Purpose: auto-restart launcher (8021) and backend (8020) if either dies
echo.
echo Test it now by running:
echo   schtasks /Run /TN "%TASK_NAME%"
echo.
endlocal
exit /b 0
