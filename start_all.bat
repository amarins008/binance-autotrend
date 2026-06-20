@echo off
REM Start all Hermes services in background windows
REM Close each window to stop that service, or close this launcher to stop all.

set ROOT=%~dp0
set BACKEND=%ROOT%backend

echo Starting Hermes backend (port 8020)...
start "Hermes Backend" cmd /k "cd /d %BACKEND% && python main.py"

timeout /t 2 /nobreak >nul

echo Starting cmux service (port 8030)...
start "cmux Service" cmd /k "cd /d %BACKEND% && python cmux_service.py serve"

timeout /t 2 /nobreak >nul

echo Starting Dashboard (port 8040)...
start "Hermes Dashboard" cmd /k "cd /d %BACKEND% && python dashboard_server.py"

echo.
echo All services starting. Give them ~5 seconds to bind their ports.
echo Then open: http://localhost:8040/
echo.
echo To check ports: netstat -ano ^| findstr ":8020 :8030 :8040"
echo To stop: close the individual service windows.
pause
