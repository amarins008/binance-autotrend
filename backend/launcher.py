"""
Lightweight helper on port 8021 — keeps running while main API (8020) may be down.
POST /restart  → start main API or ask running API to restart itself.
GET  /health   → launcher alive
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

BACKEND_PORT = os.getenv("BACKEND_PORT", "8020")
LAUNCHER_PORT = int(os.getenv("LAUNCHER_PORT", "8021"))
BACKEND_URL = f"http://127.0.0.1:{BACKEND_PORT}"
BACKEND_PID_FILE = Path(__file__).resolve().parent / ".standalone" / "backend.pid"


def _port_in_use(port: int) -> bool:
    """Return True if *port* is already bound."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1.0)
        return s.connect_ex(("127.0.0.1", port)) == 0


def _read_backend_pid() -> int | None:
    if not BACKEND_PID_FILE.exists():
        return None
    try:
        return int(BACKEND_PID_FILE.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            proc = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True, text=True, check=False,
            )
            out = (proc.stdout or "").strip()
            return bool(out and "No tasks are running" not in out and f'"{pid}"' in out)
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _kill_pid(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/F", "/T"],
                capture_output=True, text=True, check=False, timeout=5,
            )
            return True
        except Exception:
            return False
    try:
        os.kill(pid, 9)
        return True
    except OSError:
        return False


def _wait_port_free(port: int, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _port_in_use(port):
            return True
        time.sleep(0.2)
    return not _port_in_use(port)


def _spawn_backend() -> tuple[bool, str]:
    # ── Single-instance guard: kill stale backend before spawning ────────────
    existing_pid = _read_backend_pid()
    if existing_pid and _pid_is_running(existing_pid):
        _kill_pid(existing_pid)
        _wait_port_free(int(BACKEND_PORT), timeout=5.0)
    elif _port_in_use(int(BACKEND_PORT)):
        # Port in use but no valid PID file — wait for stale listener to clear
        _wait_port_free(int(BACKEND_PORT), timeout=5.0)

    backend_dir = Path(__file__).parent
    candidates = [
        backend_dir / ".venv" / "Scripts" / "python.exe",  # Windows venv on WSL/Windows
        backend_dir / ".venv" / "bin" / "python",          # POSIX venv
    ]
    py = next((p for p in candidates if p.exists()), Path(sys.executable))
    cmd = [str(py), "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", BACKEND_PORT]
    popen_kwargs = {
        "cwd": str(backend_dir),
        "close_fds": True,
    }
    if os.name == "nt":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
    else:
        popen_kwargs["start_new_session"] = True
    try:
        subprocess.Popen(cmd, **popen_kwargs)
        return True, f"Started backend on port {BACKEND_PORT}"
    except Exception as e:
        return False, str(e)


def _backend_online() -> bool:
    try:
        with urllib.request.urlopen(f"{BACKEND_URL}/health", timeout=2.5) as r:
            return r.status == 200
    except Exception:
        return False


def _request_backend_restart() -> tuple[bool, str]:
    req = urllib.request.Request(
        f"{BACKEND_URL}/system/restart",
        data=b"{}",
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=8.0) as r:
            body = json.loads(r.read().decode())
            return True, body.get("message", "Restart requested")
    except urllib.error.HTTPError as e:
        try:
            detail = json.loads(e.read().decode())
            msg = detail.get("detail", str(e))
        except Exception:
            msg = str(e)
        return False, msg
    except Exception as e:
        return False, str(e)


def restart_backend() -> tuple[bool, str]:
    if _backend_online():
        return _request_backend_restart()
    return _spawn_backend()


class LauncherHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json(self, code: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self._cors()
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            # Client (watchdog / curl / dashboard) dropped the connection mid-response.
            # This is a client-side event, NOT a launcher bug — swallow it so the
            # launcher process survives instead of crashing and taking 8021 down.
            pass

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if self.path.rstrip("/") == "/health":
            self._json(
                200,
                {
                    "ok": True,
                    "role": "launcher",
                    "backendPort": int(BACKEND_PORT),
                    "backendOnline": _backend_online(),
                },
            )
            return
        self._json(404, {"ok": False, "detail": "not found"})

    def do_POST(self):
        if self.path.rstrip("/") == "/restart":
            ok, msg = restart_backend()
            self._json(200 if ok else 500, {"ok": ok, "message": msg})
            return
        self._json(404, {"ok": False, "detail": "not found"})


def main():
    server = HTTPServer(("127.0.0.1", LAUNCHER_PORT), LauncherHandler)
    print(f"Cmux + Hermes launcher http://127.0.0.1:{LAUNCHER_PORT} (backend :{BACKEND_PORT})")
    server.serve_forever()


if __name__ == "__main__":
    main()
