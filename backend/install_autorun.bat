@echo off
setlocal
set TASK_NAME=CmuxHermesLauncher
set SCRIPT=%~dp0run_launcher_hidden.ps1

if not exist "%SCRIPT%" (
  echo ERROR: Missing script: %SCRIPT%
  exit /b 1
)

schtasks /Create /TN "%TASK_NAME%" /SC ONLOGON /RL LIMITED /F /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File \"%SCRIPT%\"" >nul
if %ERRORLEVEL% NEQ 0 (
  echo ERROR: Failed to create scheduled task.
  exit /b 1
)

echo SUCCESS: Autorun task created: %TASK_NAME%
echo It will start launcher on every logon.
exit /b 0
