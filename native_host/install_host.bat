@echo off
:: Run this script ONCE as Administrator to register the Native Messaging Host
:: After running, the extension can start/stop the backend automatically

setlocal

set HOST_NAME=com.cmux_hermes.host
set HOST_DIR=%~dp0
set MANIFEST_PATH=%HOST_DIR%com.cmux_hermes.host.json
set REG_KEY=HKCU\Software\Google\Chrome\NativeMessagingHosts\%HOST_NAME%

echo Installing Native Messaging Host...
echo Host directory: %HOST_DIR%
echo Manifest: %MANIFEST_PATH%

:: Register in Windows Registry (HKCU = no admin needed)
reg add "%REG_KEY%" /ve /t REG_SZ /d "%MANIFEST_PATH%" /f

if %ERRORLEVEL% EQU 0 (
    echo.
    echo SUCCESS: Native Messaging Host registered.
    echo You can now use the "Start Backend" button in the extension.
) else (
    echo.
    echo ERROR: Failed to register. Try running as Administrator.
)

echo.
pause
