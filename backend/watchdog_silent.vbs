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

Dim shell, fso, here, bat, logPath, cmd

Set shell = CreateObject("WScript.Shell")
Set fso   = CreateObject("Scripting.FileSystemObject")

here    = fso.GetParentFolderName(WScript.ScriptFullName)
bat     = here & "\watchdog.bat"
logPath = here & "\watchdog.log"

' The whole supervisor used to fail silently: cscript //B swallows Echo, the
' task just returned 1, and nothing on disk said why. Record every abnormal
' path to watchdog_silent.err.log so the failure is findable on its own.
If Not fso.FileExists(bat) Then
    LogFail "missing watchdog.bat at " & bat
    WScript.Quit 1
End If

If Not fso.FileExists(logPath) Then
    LogFail "watchdog.log absent - supervisor has never run from this folder"
End If

' 0 = hidden window. Waiting keeps Task Scheduler's Last Result
' meaningful (watchdog.bat can still exit non-zero from a failed restart).
cmd = """" & bat & """"
shell.CurrentDirectory = here
shell.Run cmd, 0, True

WScript.Quit 0


' LogFail - append one timestamped reason to watchdog_silent.err.log. Never
' re-raises: losing the log must not turn a warning into a second failure.
Sub LogFail(msg)
    Dim ts, fh
    ts = Replace(Replace(Now(), ":", "-"), " ", "_")
    On Error Resume Next
    Set fh = fso.OpenTextFile(here & "\watchdog_silent.err.log", 8, True)
    If Err.Number = 0 Then
        fh.WriteLine ts & "  " & msg
        fh.Close
    End If
    On Error GoTo 0
End Sub