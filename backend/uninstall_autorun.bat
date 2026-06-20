@echo off
setlocal
set TASK_NAME=CmuxHermesLauncher
schtasks /Delete /TN "%TASK_NAME%" /F >nul
if %ERRORLEVEL% NEQ 0 (
  echo WARNING: Task not found or delete failed.
  exit /b 1
)
echo SUCCESS: Autorun task removed: %TASK_NAME%
exit /b 0
