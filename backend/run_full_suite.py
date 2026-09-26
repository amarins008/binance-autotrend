"""Run the full backend test suite (all tests live in tests/ now).

Usage: python run_full_suite.py   (or run the venv pytest directly)
"""
import subprocess, sys

cmd = [sys.executable, "-u", "-m", "pytest", "tests/", "-q", "--tb=line",
       "--ignore=tests/manual"]
r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
print(r.stdout[-6000:])
print(r.stderr[-1000:])
sys.exit(r.returncode)
