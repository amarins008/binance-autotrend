from __future__ import annotations

import argparse
import json
import mimetypes
import pathlib
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


ROOT = pathlib.Path(__file__).resolve().parent / "dashboard"
HERMES_BASE = "http://127.0.0.1:8020"


def _status_payload() -> tuple[int, bytes, str]:
    status, body, _ = _proxy_request("/autotrade/status-lite")
    try:
        bot = json.loads(body.decode("utf-8")) if body else {}
    except Exception:
        bot = {"ok": False, "error": body.decode("utf-8", errors="replace")}
    health_status, health_body, _ = _proxy_request("/health")
    try:
        health = json.loads(health_body.decode("utf-8")) if health_body else {}
    except Exception:
        health = {}
    payload = {
        "ok": status < 400 and health_status < 500,
        "hermes": {
            "running": health_status < 500,
            "healthy": bool(health.get("ok")) if isinstance(health, dict) else health_status < 500,
            "port": 8020,
            "health": health,
        },
        "bot": bot,
        "network": {},
    }
    return 200, json.dumps(payload).encode("utf-8"), "application/json; charset=utf-8"


def _alias_path(path: str) -> str:
    parsed = urllib.parse.urlsplit(path)
    aliases = {
        "/bot/start": "/autotrade/start",
        "/bot/stop": "/autotrade/stop",
        "/bot/config": "/autotrade/config",
        "/bot/reset": "/autotrade/reset",
        "/learning/report": "/learning/status",
    }
    mapped = aliases.get(parsed.path, parsed.path)
    return urllib.parse.urlunsplit(("", "", mapped, parsed.query, parsed.fragment))


def _proxy_request(path: str, method: str = "GET", body: bytes | None = None) -> tuple[int, bytes, str]:
    try:
        req = urllib.request.Request(
            f"{HERMES_BASE}{_alias_path(path)}",
            data=body,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=18) as res:
            return res.status, res.read(), res.headers.get("Content-Type", "application/json; charset=utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), exc.headers.get("Content-Type", "application/json; charset=utf-8")
    except Exception as exc:
        return 502, json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"), "application/json; charset=utf-8"


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "HermesDashboard/1.0"

    def log_message(self, fmt: str, *args: object) -> None:
        return

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_static_or_proxy(self, send_body: bool = True) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        rel = "index.html" if parsed.path in {"/", "/dashboard"} else parsed.path.lstrip("/")
        target = (ROOT / rel).resolve()
        if str(target).startswith(str(ROOT.resolve())) and target.exists() and target.is_file():
            body = target.read_bytes() if send_body else b""
            content_type = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
            if target.suffix.lower() in {".html", ".css", ".js"}:
                content_type += "; charset=utf-8"
            self._send(200, body, content_type)
            return

        status, body, content_type = _proxy_request(self.path, method="GET")
        self._send(status, body if send_body else b"", content_type)

    def do_GET(self) -> None:
        if self.path.startswith("/api/health"):
            status, body, content_type = _proxy_request("/health")
            self._send(status, body, content_type)
            return
        if self.path.startswith("/api/status-lite"):
            status, body, content_type = _proxy_request("/autotrade/status-lite")
            self._send(status, body, content_type)
            return
        if urllib.parse.urlsplit(self.path).path in {"/status", "/status/quick"}:
            status, body, content_type = _status_payload()
            self._send(status, body, content_type)
            return
        if urllib.parse.urlsplit(self.path).path == "/bot/precheck-live":
            self._send(200, b'{"ok":true,"precheck":{"ok":true,"dashboardProxy":true}}', "application/json; charset=utf-8")
            return

        self._serve_static_or_proxy()

    def do_HEAD(self) -> None:
        self._serve_static_or_proxy(send_body=False)

    def do_POST(self) -> None:
        parsed_path = urllib.parse.urlsplit(self.path).path
        if parsed_path in {"/service/start", "/service/stop"}:
            self._send(200, b'{"ok":true,"service":{"running":true,"healthy":true,"dashboardProxy":true}}', "application/json; charset=utf-8")
            return
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length > 0 else None
        status, res_body, content_type = _proxy_request(self.path, method="POST", body=body)
        self._send(status, res_body, content_type)


def main() -> None:
    parser = argparse.ArgumentParser(description="Hermes dashboard proxy server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8030)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"Hermes dashboard listening on http://{args.host}:{args.port}/", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
