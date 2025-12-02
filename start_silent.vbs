Set WshShell = CreateObject("WScript.Shell")
' 0 = Hide Window, 1 = Show Window
WshShell.Run chr(34) & "run_bot.bat" & chr(34), 0
Set WshShell = Nothing