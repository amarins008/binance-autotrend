import subprocess
import sys
import os

log_path = r"E:\My Project\Binance autotrend\backend\uvicorn.log"
with open(log_path, "w") as f:
    # Use run_backend.py so the WindowsSelectorEventLoopPolicy is applied
    # BEFORE uvicorn creates its event loop. Stops the OSError(64) crash
    # loop on Python 3.14.
    process = subprocess.Popen(
        [sys.executable, r"E:\My Project\Binance autotrend\backend\run_backend.py"],
        stdout=f,
        stderr=subprocess.STDOUT,
        cwd=r"E:\My Project\Binance autotrend\backend"
    )
    print(f"Started uvicorn PID {process.pid}")
    print(f"Log: {log_path}")
