from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / ".standalone"
DEFAULT_CONFIG = ROOT / "autotrade.standalone.json"
CMUX_PID_FILE = STATE_DIR / "cmux.pid"
CMUX_OUT_LOG = STATE_DIR / "cmux.out.log"
CMUX_ERR_LOG = STATE_DIR / "cmux.err.log"
CMUX_PORT = int(os.getenv("CMUX_PORT", "8030"))
CMUX_BASE = f"http://127.0.0.1:{CMUX_PORT}"


def _ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def _read_pid() -> int | None:
    if not CMUX_PID_FILE.exists():
        return None
    try:
        return int(CMUX_PID_FILE.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _write_pid(pid: int) -> None:
    _ensure_state_dir()
    CMUX_PID_FILE.write_text(str(pid), encoding="utf-8")


def _clear_pid() -> None:
    CMUX_PID_FILE.unlink(missing_ok=True)


def _is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            proc = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                check=False,
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


def _cmux_health(timeout: float = 1.0) -> bool:
    try:
        with urllib.request.urlopen(f"{CMUX_BASE}/health", timeout=timeout) as r:
            _ = r.read()
            return r.status == 200
    except Exception:
        return False


def _python_executable() -> str:
    candidates = [ROOT / ".venv" / "Scripts" / "python.exe", ROOT / ".venv" / "bin" / "python"]
    for p in candidates:
        if p.exists():
            return str(p)
    return sys.executable


def _start_cmux(timeout_sec: float = 10.0) -> dict[str, Any]:
    if _cmux_health():
        return {"ok": True, "message": "cmux already healthy", "port": CMUX_PORT}

    pid = _read_pid()
    if pid and _is_running(pid):
        return {"ok": True, "message": "cmux process already running", "pid": pid, "port": CMUX_PORT}

    _ensure_state_dir()
    cmd = [_python_executable(), str(ROOT / "cmux_service.py"), "serve"]
    out_f = CMUX_OUT_LOG.open("ab")
    err_f = CMUX_ERR_LOG.open("ab")
    kwargs: dict[str, Any] = {"cwd": str(ROOT), "stdout": out_f, "stderr": err_f, "close_fds": True}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
    else:
        kwargs["start_new_session"] = True

    proc = subprocess.Popen(cmd, **kwargs)
    _write_pid(proc.pid)

    deadline = time.time() + max(2.0, float(timeout_sec))
    while time.time() < deadline:
        if _cmux_health(timeout=0.8):
            return {"ok": True, "message": "cmux started", "pid": proc.pid, "port": CMUX_PORT}
        time.sleep(0.25)
    if _is_running(proc.pid):
        return {"ok": True, "message": "cmux start warm-up", "pid": proc.pid, "port": CMUX_PORT, "warming": True}
    return {"ok": False, "message": "cmux health check timed out", "pid": proc.pid, "stderr": str(CMUX_ERR_LOG)}


def _stop_cmux(timeout_sec: float = 6.0) -> dict[str, Any]:
    pid = _read_pid()
    if not pid:
        return {"ok": True, "message": "cmux not running (no pid file)"}
    if not _is_running(pid):
        _clear_pid()
        return {"ok": True, "message": "cmux already stopped", "pid": pid}
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], check=False, capture_output=True)
        else:
            os.kill(pid, signal.SIGTERM)
    except Exception as exc:
        return {"ok": False, "message": f"Failed to stop cmux: {exc}", "pid": pid}

    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if not _is_running(pid):
            _clear_pid()
            return {"ok": True, "message": "cmux stopped", "pid": pid}
        time.sleep(0.2)
    return {"ok": False, "message": "Timed out waiting cmux stop", "pid": pid}


def _ensure_cmux_running() -> dict[str, Any]:
    if _cmux_health():
        return {"ok": True, "message": "cmux healthy", "port": CMUX_PORT}
    return _start_cmux()


def _req(
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    ensure_cmux: bool = True,
    timeout_sec: float = 20.0,
) -> dict[str, Any]:
    if ensure_cmux:
        cmux = _ensure_cmux_running()
        if not cmux.get("ok"):
            return {"ok": False, "step": "cmux.start", "cmux": cmux}
    data = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        f"{CMUX_BASE}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as r:
            body = r.read().decode("utf-8")
            return json.loads(body) if body else {"ok": True}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"ok": False, "error": f"HTTP {exc.code}: {body}"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def cmd_start(args: argparse.Namespace) -> dict[str, Any]:
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8-sig"))
    if args.symbol:
        cfg["symbol"] = args.symbol.upper()
    if args.mode:
        cfg["executionMode"] = args.mode.upper()
    cfg["autoLearn"] = not bool(args.no_auto_learn)
    return _req("POST", "/bot/start", cfg)


def cmd_stop(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {"force": bool(args.force)}
    if args.session_id:
        payload["sessionId"] = args.session_id
    return _req("POST", "/bot/stop", payload)


def cmd_status(_: argparse.Namespace) -> dict[str, Any]:
    return _req("GET", "/status")


def cmd_service(args: argparse.Namespace) -> dict[str, Any]:
    if args.action == "start":
        return _req("POST", "/service/start", {})
    if args.action == "stop":
        return _req("POST", "/service/stop", {})
    return _req("GET", "/status")


def cmd_cmux(args: argparse.Namespace) -> dict[str, Any]:
    if args.action == "start":
        return _start_cmux()
    if args.action == "stop":
        return _stop_cmux()
    pid = _read_pid()
    healthy = _cmux_health()
    if pid and not _is_running(pid):
        _clear_pid()
        pid = None
    return {
        "ok": True,
        "name": "cmux",
        "running": bool(healthy or (pid and _is_running(pid))),
        "healthy": healthy,
        "pid": pid,
        "port": CMUX_PORT,
        "baseUrl": CMUX_BASE,
        "logs": {"stdout": str(CMUX_OUT_LOG), "stderr": str(CMUX_ERR_LOG)},
    }


def cmd_learning(args: argparse.Namespace) -> dict[str, Any]:
    if args.action == "report":
        return _req("GET", "/learning/report")
    payload: dict[str, Any] = {
        "mode": args.mode,
        "trainSize": args.train_size,
        "testSize": args.test_size,
        "maxSymbols": args.max_symbols,
        "promoteHitRatePct": args.promote_hit_rate_pct,
    }
    if args.symbols:
        payload["symbols"] = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    return _req("POST", "/learning/train-now", payload, timeout_sec=180.0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Codex CLI (control plane)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_start = sub.add_parser("start", help="Start bot via cmux")
    p_start.add_argument("--config", default=str(DEFAULT_CONFIG))
    p_start.add_argument("--symbol")
    p_start.add_argument("--mode", choices=["PAPER", "LIVE"])
    p_start.add_argument("--no-auto-learn", action="store_true", help="Disable learning-based tuning from Obsidian data")
    p_start.set_defaults(func=cmd_start)

    p_stop = sub.add_parser("stop", help="Stop bot via cmux")
    p_stop.add_argument("--session-id")
    p_stop.add_argument("--force", action="store_true")
    p_stop.set_defaults(func=cmd_stop)

    p_status = sub.add_parser("status", help="Status via cmux")
    p_status.set_defaults(func=cmd_status)

    p_service = sub.add_parser("service", help="Manage Hermes service via cmux")
    p_service.add_argument("action", choices=["start", "stop", "status"])
    p_service.set_defaults(func=cmd_service)

    p_cmux = sub.add_parser("cmux", help="Manage cmux control-plane")
    p_cmux.add_argument("action", choices=["start", "stop", "status"])
    p_cmux.set_defaults(func=cmd_cmux)

    p_learning = sub.add_parser("learning", help="Train and report learning loop")
    p_learning.add_argument("action", choices=["train-now", "report"])
    p_learning.add_argument("--symbols", help="Comma-separated symbols (default: auto from learning/status)")
    p_learning.add_argument("--mode", default="ALL", choices=["ALL", "PAPER", "LIVE"])
    p_learning.add_argument("--train-size", type=int, default=30)
    p_learning.add_argument("--test-size", type=int, default=10)
    p_learning.add_argument("--max-symbols", type=int, default=20)
    p_learning.add_argument("--promote-hit-rate-pct", type=float, default=55.0)
    p_learning.set_defaults(func=cmd_learning)

    return parser


def _main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    out = args.func(args)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(_main())
