' ---------------------------------------------------------------------------
'  watchdog_silent.vbs
'  Silent runner for watchdog.bat.
'
'  Task Scheduler (BinanceAutotrendWatchdog) fires this every 5 minutes. It
'  launches watchdog.bat in a hidden window so no console flashes on the
'  desktop, then waits so the task's Last Result reflects the real outcome.
'
'  Install it with install_watchdog.bat (needs Administrator once).
' ---------------------------------------------------------------------------
Option Explicit

Dim shell, fso, here, bat, cmd

Set shell = CreateObject("WScript.Shell")
Set fso   = CreateObject("Scripting.FileSystemObject")

here = fso.GetParentFolderName(WScript.ScriptFullName)
bat  = here & "\watchdog.bat"

If Not fso.FileExists(bat) Then
    WScript.Echo "[watchdog_silent] missing " & bat
    WScript.Quit 1
End If

' 0 = hidden window. Waiting keeps Task Scheduler's Last Result meaningful
' (watchdog.bat can still exit non-zero from a failed restart branch).
cmd = """" & bat & """"
shell.CurrentDirectory = here
shell.Run cmd, 0, True

WScript.Quit 0