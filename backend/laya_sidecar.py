"""Laya System-1 shadow decision sidecar (stdlib-only HTTP server).

Loads the `english` Laya checkpoint once (first run downloads ~842MB from
HuggingFace) and answers typed questions over a JSON POST /v1/predict.
The bot calls this on localhost as a *shadow observer* — answers are recorded
into entry telemetry but never gate a trade. If the sidecar is down, clients
simply get a connection error and the observer records nothing.

Run with the venv that has laya installed:
    python laya_sidecar.py --port 8790 --model english
"""
import argparse
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LAYA_AVAILABLE = True
try:
    from laya import Router  # noqa: E402
except Exception:
    LAYA_AVAILABLE = False


class _State:
    router = None
    lock = threading.Lock()


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write("[laya-sidecar] %s\n" % (fmt % args))

    def _json(self, code, payload):
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.rstrip("/").endswith("/health"):
            self._json(200, {
                "status": "ok" if LAYA_AVAILABLE else "laya_missing",
                "loaded": _State.router.loaded if _State.router else [],
                "model": "english",
            })
            return
        self._json(404, {"error": "not_found"})

    def do_POST(self):
        if not self.path.rstrip("/").endswith("/predict"):
            self._json(404, {"error": "not_found"})
            return
        if not LAYA_AVAILABLE or _State.router is None:
            self._json(503, {"error": "router_not_loaded", "message": "laya failed to import at startup"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 2 * 1024 * 1024:
                self._json(413, {"error": "bad_body_size"})
                return
            raw = self.rfile.read(length)
            body = json.loads(raw)
        except Exception as e:
            self._json(400, {"error": "bad_json", "message": str(e)})
            return
        state = body.get("state")
        questions = body.get("questions")
        if not isinstance(questions, dict) or not questions:
            self._json(400, {"error": "missing_questions"})
            return
        try:
            with _State.lock:
                res = _State.router.predict(state, questions, model="english")
            self._json(200, res)
        except Exception as e:
            self._json(500, {"error": "predict_failed", "message": str(e)[:200]})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8790)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--model", default="english")
    ap.add_argument("--no-preload", action="store_true")
    args = ap.parse_args()

    if not LAYA_AVAILABLE:
        sys.stderr.write("[laya-sidecar] laya not importable in this environment — check deps\n")
        sys.exit(1)

    sys.stderr.write("[laya-sidecar] loading Router(model=%s) ...\n" % args.model)
    _State.router = Router()
    if args.no_preload:
        # Lazy load on first predict (still downloads on first use).
        pass
    else:
        _State.router.preload([args.model])
    sys.stderr.write("[laya-sidecar] loaded=%s\n" % _State.router.loaded)

    srv = ThreadingHTTPServer((args.host, args.port), _Handler)
    sys.stderr.write("[laya-sidecar] listening on %s:%s\n" % (args.host, args.port))
    srv.serve_forever()


if __name__ == "__main__":
    main()