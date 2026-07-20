@echo off
REM Stop backend + dashboard, then restart via start_all.bat.
REM Right-click -> Run as administrator if taskkill gives "Access denied".

setlocal
set ROOT=E:\My Project\Binance autotrend
set BACKEND=%ROOT%\backend

echo === Killing existing services on 8020 / 8021 / 8040 ===
for %%P in (8020 8021 8040) do (
  for /f "tokens=5" %%A in ('netstat -ano ^| findstr ":%%P " ^| findstr LISTENING') do (
    echo   Killing PID %%A on port %%P
    taskkill /PID %%A /T /F >nul 2>&1
  )
)

timeout /t 3 /nobreak >nul

echo.
echo === Verifying ports are free ===
netstat -ano | findstr "LISTENING" | findstr ":8020 :8040" >nul
if %ERRORLEVEL% EQU 0 (
  echo WARNING: some ports still busy. Check Task Manager.
) else (
  echo All ports free.
)

echo.
echo === Starting fresh services via start_all.bat ===
call "%ROOT%\start_all.bat"

endlocal
