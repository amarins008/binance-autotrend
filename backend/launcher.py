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
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

BACKEND_PORT = os.getenv("BACKEND_PORT", "8020")
LAUNCHER_PORT = int(os.getenv("LAUNCHER_PORT", "8021"))
BACKEND_URL = f"http://127.0.0.1:{BACKEND_PORT}"


def _spawn_backend() -> tuple[bool, str]:
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
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self._cors()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

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
    print(f"Binance Copilot launcher http://127.0.0.1:{LAUNCHER_PORT} (backend :{BACKEND_PORT})")
    server.serve_forever()


if __name__ == "__main__":
    main()
