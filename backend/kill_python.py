import ctypes
import os
import subprocess

my_pid = os.getpid()
result = subprocess.run(['tasklist'], capture_output=True, text=True)
for line in result.stdout.split('\n')[3:]:
    parts = line.split()
    if len(parts) >= 2 and parts[0].lower() == 'python.exe':
        try:
            pid = int(parts[1])
            if pid != my_pid:
                h = ctypes.windll.kernel32.OpenProcess(1, False, pid)
                if h:
                    ctypes.windll.kernel32.TerminateProcess(h, 1)
                    ctypes.windll.kernel32.CloseHandle(h)
                    print(f'Killed python PID {pid}')
        except Exception:
            pass
print('Done')
