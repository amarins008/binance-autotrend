#!/usr/bin/env python3
"""
Native Messaging Host for Binance AI Copilot
Receives messages from Chrome extension and starts/stops the backend server.

Install: run install_host.bat as Administrator once.
"""
import json
import os
import struct
import subprocess
import sys
import time
from pathlib import Path

BACKEND_DIR = Path(__file__).parent.parent / "backend"
VENV_PYTHON = BACKEND_DIR / ".venv" / "Scripts" / "python.exe"
PID_FILE = BACKEND_DIR / "uvicorn.pid"
BACKEND_PORT = os.getenv("BACKEND_PORT", "8020")


def read_message():
    raw_len = sys.stdin.buffer.read(4)
    if not raw_len:
        return None
    msg_len = struct.unpack("=I", raw_len)[0]
    raw_msg = sys.stdin.buffer.read(msg_len)
    return json.loads(raw_msg.decode("utf-8"))


def send_message(msg: dict):
    encoded = json.dumps(msg).encode("utf-8")
    sys.stdout.buffer.write(struct.pack("=I", len(encoded)))
    sys.stdout.buffer.write(encoded)
    sys.stdout.buffer.flush()


def is_backend_running() -> bool:
    try:
        import urllib.request
        urllib.request.urlopen(f"http://127.0.0.1:{BACKEND_PORT}/health", timeout=2)
        return True
    except Exception:
        return False


def start_backend() -> dict:
    if is_backend_running():
        return {"ok": True, "msg": "Already running"}
    try:
        python = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable
        proc = subprocess.Popen(
            [python, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", BACKEND_PORT, "--timeout-keep-alive", "30"],
            cwd=str(BACKEND_DIR),
            creationflags=subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        PID_FILE.write_text(str(proc.pid))
        # Wait up to 8s for backend to come up
        for _ in range(16):
            time.sleep(0.5)
            if is_backend_running():
                return {"ok": True, "msg": f"Backend started (PID {proc.pid})"}
        return {"ok": False, "msg": "Backend started but not responding yet — try again"}
    except Exception as e:
        return {"ok": False, "msg": str(e)}


def stop_backend() -> dict:
    try:
        if PID_FILE.exists():
            pid = int(PID_FILE.read_text().strip())
            subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True)
            PID_FILE.unlink(missing_ok=True)
        return {"ok": True, "msg": "Backend stopped"}
    except Exception as e:
        return {"ok": False, "msg": str(e)}


def main():
    while True:
        msg = read_message()
        if msg is None:
            break
        action = msg.get("action", "")
        if action == "start":
            send_message(start_backend())
        elif action == "stop":
            send_message(stop_backend())
        elif action == "status":
            send_message({"ok": True, "running": is_backend_running()})
        else:
            send_message({"ok": False, "msg": f"Unknown action: {action}"})


if __name__ == "__main__":
    main()
