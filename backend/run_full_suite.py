import subprocess, glob
tests = [f for f in glob.glob("test_*.py") if f != "test_pineforge.py"]
cmd = [r".venv\Scripts\python.exe", "-u", "-m", "pytest", "-q", "--tb=line"] + tests
r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
print(r.stdout[-4000:])
print(r.stderr[-1000:])