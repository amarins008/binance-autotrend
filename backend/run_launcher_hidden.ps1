param()
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$bat = Join-Path $scriptDir 'start_launcher.bat'
if (!(Test-Path $bat)) {
  Write-Output "start_launcher.bat not found: $bat"
  exit 1
}
Start-Process -FilePath $bat -WorkingDirectory $scriptDir -WindowStyle Hidden
