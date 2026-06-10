from __future__ import annotations

import json
import os
import argparse
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
import urllib.request
import urllib.parse

ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / ".standalone"
PID_FILE = STATE_DIR / "hermes.pid"
OUT_LOG = STATE_DIR / "hermes.out.log"
ERR_LOG = STATE_DIR / "hermes.err.log"
HERMES_PORT = int(os.getenv("BACKEND_PORT", "8020"))
HERMES_BASE = f"http://127.0.0.1:{HERMES_PORT}"
_AUTOTRADE_STATUS_CACHE: dict[str, Any] = {
    "ts": 0.0,
    "data": {"ok": False, "running": False, "source": "init"},
}
_AUTOTRADE_STATUS_FULL_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
_HERMES_HEALTH_CACHE: dict[str, Any] = {"ts": 0.0, "healthy": False}
_HERMES_HEALTH_MISS_SINCE = 0.0
_HERMES_HEALTH_GRACE_SEC = max(0.0, float(os.getenv("HERMES_HEALTH_GRACE_SEC", "60")))
_HERMES_DESIRED_HEALTH_GRACE_SEC = max(0.0, float(os.getenv("HERMES_DESIRED_HEALTH_GRACE_SEC", "180")))
_AUTOTRADE_STATUS_GRACE_SEC = max(0.0, float(os.getenv("AUTOTRADE_STATUS_GRACE_SEC", "60")))
BOT_DESIRED_STATE_PATH = STATE_DIR / "bot_desired.json"


def _autotrade_status_needs_full(data: dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return True
    required = ("openLivePositions", "activePosition", "liveStatsAll", "kpiTodayAllSymbols", "log")
    return any(k not in data for k in required)


def _autotrade_status_get_full(timeout_sec: float) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(f"{HERMES_BASE}/autotrade/status", timeout=timeout_sec) as r:
            full = json.loads(r.read().decode("utf-8"))
        if isinstance(full, dict):
            _AUTOTRADE_STATUS_FULL_CACHE["ts"] = time.time()
            _AUTOTRADE_STATUS_FULL_CACHE["data"] = dict(full)
            return full
    except Exception:
        return None
    return None


def _load_bot_desired_snapshot() -> dict[str, Any]:
    try:
        if not BOT_DESIRED_STATE_PATH.exists():
            return {"running": False}
        data = json.loads(BOT_DESIRED_STATE_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {"running": False}
    except Exception:
        return {"running": False}


def _ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def _pids_listening_on_port(port: int) -> list[int]:
    try:
        proc = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True,
            text=True,
            check=False,
        )
        out = proc.stdout or ""
        hits: set[int] = set()
        needle = f":{int(port)}"
        for ln in out.splitlines():
            line = ln.strip()
            if not line:
                continue
            if needle not in line:
                continue
            if "LISTENING" not in line.upper():
                continue
            parts = line.split()
            if not parts:
                continue
            pid_raw = parts[-1]
            try:
                hits.add(int(pid_raw))
            except Exception:
                continue
        return sorted(hits)
    except Exception:
        return []


def _uvicorn_main_pids() -> list[int]:
    try:
        if os.name != "nt":
            return []
        proc = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*uvicorn main:app*' } | Select-Object -ExpandProperty ProcessId",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        out = (proc.stdout or "").strip().splitlines()
        pids: list[int] = []
        for ln in out:
            t = ln.strip()
            if not t:
                continue
            try:
                pids.append(int(t))
            except Exception:
                continue
        return sorted(set(pids))
    except Exception:
        return []


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


def _read_pid() -> int | None:
    if not PID_FILE.exists():
        return None
    try:
        return int(PID_FILE.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _write_pid(pid: int) -> None:
    _ensure_state_dir()
    PID_FILE.write_text(str(pid), encoding="utf-8")


def _clear_pid() -> None:
    PID_FILE.unlink(missing_ok=True)


def _http_ok(path: str = "/health", timeout: float = 2.5) -> bool:
    try:
        req = urllib.request.Request(
            f"{HERMES_BASE}{path}",
            headers={"Connection": "close"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            # Consume body to ensure underlying socket is fully released on Windows.
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


def start(timeout_sec: float = 12.0) -> dict[str, Any]:
    if _http_ok("/health"):
        return {"ok": True, "message": "Hermes already healthy", "port": HERMES_PORT}

    existing = _pids_listening_on_port(HERMES_PORT)
    if existing:
        _write_pid(existing[0])
        return {
            "ok": True,
            "message": "Hermes listener already present (adopted)",
            "pid": existing[0],
            "port": HERMES_PORT,
            "adopted": True,
        }

    pid = _read_pid()
    if pid and _is_running(pid):
        try:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], check=False, capture_output=True)
            else:
                os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
        _clear_pid()

    _ensure_state_dir()
    cmd = [_python_executable(), "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(HERMES_PORT)]
    out_f = OUT_LOG.open("ab")
    err_f = ERR_LOG.open("ab")

    kwargs: dict[str, Any] = {"cwd": str(ROOT), "stdout": out_f, "stderr": err_f, "close_fds": True}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
    else:
        kwargs["start_new_session"] = True

    proc = subprocess.Popen(cmd, **kwargs)
    _write_pid(proc.pid)

    deadline = time.time() + max(12.0, float(timeout_sec))
    while time.time() < deadline:
        if _http_ok("/health"):
            return {"ok": True, "message": "Hermes started", "pid": proc.pid, "port": HERMES_PORT}
        time.sleep(0.5)
    # On Windows, uvicorn can need extra warm-up and transient socket noise.
    # If process is still alive, don't hard-fail start; let callers continue and poll health.
    if _is_running(proc.pid):
        return {
            "ok": True,
            "message": "Hermes start warm-up (health timeout, process alive)",
            "pid": proc.pid,
            "port": HERMES_PORT,
            "warming": True,
        }
    return {"ok": False, "message": "Hermes health check timed out", "pid": proc.pid, "stderr": str(ERR_LOG)}


def stop(timeout_sec: float = 8.0) -> dict[str, Any]:
    pid = _read_pid()
    if not pid:
        # Fallback: kill any listener on service port even when pid file is stale/missing.
        port_pids = _pids_listening_on_port(HERMES_PORT)
        extra_pids = _uvicorn_main_pids()
        targets = sorted(set(port_pids + extra_pids))
        if not targets:
            return {"ok": True, "message": "Hermes not running (no pid file)"}
        for p in targets:
            try:
                if os.name == "nt":
                    subprocess.run(["taskkill", "/PID", str(p), "/T", "/F"], check=False, capture_output=True)
                else:
                    os.kill(p, signal.SIGTERM)
            except Exception:
                pass
        _clear_pid()
        return {"ok": True, "message": f"Hermes stopped by cleanup ({len(targets)} pid)", "pid": None}

    if not _is_running(pid):
        _clear_pid()
        # PID is stale; still clean listeners on the service port.
        port_pids = _pids_listening_on_port(HERMES_PORT)
        extra_pids = _uvicorn_main_pids()
        targets = sorted(set(port_pids + extra_pids))
        if targets:
            for p in targets:
                try:
                    if os.name == "nt":
                        subprocess.run(["taskkill", "/PID", str(p), "/T", "/F"], check=False, capture_output=True)
                    else:
                        os.kill(p, signal.SIGTERM)
                except Exception:
                    pass
            return {"ok": True, "message": f"Hermes stale pid cleaned + cleanup ({len(targets)} pid)", "pid": pid}
        return {"ok": True, "message": "Hermes already stopped", "pid": pid}

    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], check=False, capture_output=True)
        else:
            os.kill(pid, signal.SIGTERM)
    except Exception as exc:
        return {"ok": False, "message": f"Failed to stop Hermes: {exc}", "pid": pid}

    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if not _is_running(pid):
            _clear_pid()
            return {"ok": True, "message": "Hermes stopped", "pid": pid}
        time.sleep(0.25)

    return {"ok": False, "message": "Timed out waiting Hermes stop", "pid": pid}


_QUICK_HTTP_TIMEOUT = 4.0


def status() -> dict[str, Any]:
    pid = _read_pid()
    running = bool(pid and _is_running(pid))
    healthy = _http_ok("/health")
    if pid and not running:
        _clear_pid()
    if healthy and not running:
        # PID file can go stale on Windows after detached restarts.
        # If health endpoint is alive, treat service as running.
        running = True
        pid = None
    return {
        "ok": True,
        "name": "Hermes",
        "running": running,
        "healthy": healthy,
        "pid": pid if running else None,
        "port": HERMES_PORT,
        "baseUrl": HERMES_BASE,
        "logs": {"stdout": str(OUT_LOG), "stderr": str(ERR_LOG)},
    }


def status_quick() -> dict[str, Any]:
    """Lightweight status for dashboard polling (short HTTP timeout)."""
    global _HERMES_HEALTH_MISS_SINCE
    pid = _read_pid()
    running = bool(pid and _is_running(pid))
    healthy = _http_ok("/health", timeout=_QUICK_HTTP_TIMEOUT)
    health_stale = False
    health_age = 0.0
    health_miss_age = 0.0
    health_reason = ""
    if healthy:
        _HERMES_HEALTH_CACHE["ts"] = time.time()
        _HERMES_HEALTH_CACHE["healthy"] = True
        _HERMES_HEALTH_MISS_SINCE = 0.0
    else:
        now = time.time()
        if _HERMES_HEALTH_MISS_SINCE <= 0:
            _HERMES_HEALTH_MISS_SINCE = now
        health_miss_age = max(0.0, now - _HERMES_HEALTH_MISS_SINCE)
        health_age = now - float(_HERMES_HEALTH_CACHE.get("ts", 0.0) or 0.0)
        if running and bool(_HERMES_HEALTH_CACHE.get("healthy")) and health_age <= _HERMES_HEALTH_GRACE_SEC:
            healthy = True
            health_stale = True
            health_reason = "recent_health_cache"
        elif running and bool(_load_bot_desired_snapshot().get("running")) and health_miss_age <= _HERMES_DESIRED_HEALTH_GRACE_SEC:
            healthy = True
            health_stale = True
            health_reason = "desired_running_grace"
    if pid and not running:
        _clear_pid()
    if healthy and not running:
        running = True
        pid = None
    return {
        "ok": True,
        "name": "Hermes",
        "running": running,
        "healthy": healthy,
        "healthStale": health_stale,
        "healthAgeSec": round(health_age, 2) if health_stale else 0.0,
        "healthMissAgeSec": round(health_miss_age, 2) if health_stale else 0.0,
        "healthStaleReason": health_reason,
        "pid": pid if running else None,
        "port": HERMES_PORT,
        "baseUrl": HERMES_BASE,
    }


def autotrade_start(payload: dict[str, Any]) -> dict[str, Any]:
    import urllib.error

    req = urllib.request.Request(
        f"{HERMES_BASE}/autotrade/start",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20.0) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"ok": False, "error": f"HTTP {exc.code}: {body}"}


def autotrade_stop(payload: dict[str, Any]) -> dict[str, Any]:
    import urllib.error

    req = urllib.request.Request(
        f"{HERMES_BASE}/autotrade/stop",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20.0) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"ok": False, "error": f"HTTP {exc.code}: {body}"}


def autotrade_update_config(payload: dict[str, Any]) -> dict[str, Any]:
    import urllib.error

    req = urllib.request.Request(
        f"{HERMES_BASE}/autotrade/config",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20.0) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        # Backward compatibility:
        # some Hermes builds do not expose /autotrade/config.
        # Fallback by reading current status config and re-applying via /autotrade/start.
        if int(exc.code) == 404:
            st = autotrade_status()
            cur = dict(st.get("config") or {})
            if not cur:
                return {"ok": False, "error": f"HTTP {exc.code}: {body}"}
            cur.update(payload or {})
            rs = autotrade_start(cur)
            if isinstance(rs, dict) and rs.get("ok"):
                return {"ok": True, "updated": True, "fallback": "autotrade/start", "config": cur}
            return {"ok": False, "error": f"fallback start failed: {rs}"}
        return {"ok": False, "error": f"HTTP {exc.code}: {body}"}


def autotrade_status() -> dict[str, Any]:
    # Prefer full payload for dashboard cards (positions/KPI/log),
    # fallback to lite only if full endpoint is unavailable.
    for path in ("/autotrade/status", "/autotrade/status-lite"):
        try:
            with urllib.request.urlopen(f"{HERMES_BASE}{path}", timeout=10.0) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception:
            continue
    return {"ok": False, "error": "autotrade status endpoint unavailable"}


def autotrade_status_quick() -> dict[str, Any]:
    try:
        data = None
        last_exc = None
        for path in ("/autotrade/status-lite",):
            try:
                with urllib.request.urlopen(f"{HERMES_BASE}{path}", timeout=_QUICK_HTTP_TIMEOUT) as r:
                    data = json.loads(r.read().decode("utf-8"))
                break
            except Exception as exc:
                last_exc = exc
                continue
        if data is None:
            raise last_exc or RuntimeError("autotrade status endpoint unavailable")
        if isinstance(data, dict):
            _AUTOTRADE_STATUS_CACHE["ts"] = time.time()
            _AUTOTRADE_STATUS_CACHE["data"] = dict(data)
            _AUTOTRADE_STATUS_CACHE["data"]["source"] = "live"
            _AUTOTRADE_STATUS_CACHE["data"]["stale"] = False
        return data
    except Exception as exc:
        cached = _AUTOTRADE_STATUS_CACHE.get("data")
        age = time.time() - float(_AUTOTRADE_STATUS_CACHE.get("ts", 0.0) or 0.0)
        # Keep the last known bot state for short transient timeouts
        # so dashboard doesn't flap from RUNNING -> CACHED -> RUNNING.
        if isinstance(cached, dict) and age <= _AUTOTRADE_STATUS_GRACE_SEC:
            out = dict(cached)
            out["ok"] = True
            out["stale"] = False
            out["softStale"] = True
            out["staleAgeSec"] = round(age, 2)
            out["warning"] = f"status-lite timeout: {exc}"
            out["source"] = "cache"
            return out
        desired = _load_bot_desired_snapshot()
        if bool(desired.get("running")):
            cfg = desired.get("config") if isinstance(desired.get("config"), dict) else {}
            return {
                "ok": True,
                "running": True,
                "source": "desired_cache",
                "stale": False,
                "softStale": True,
                "statusUnknown": True,
                "desiredRunning": True,
                "staleReason": "status_timeout_desired_running",
                "warning": f"status-lite timeout: {exc}; keeping desired-running state",
                "config": dict(cfg),
            }
        return {"ok": False, "error": str(exc), "running": False, "source": "timeout"}


def autotrade_close_orphan(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    import urllib.error

    query = ""
    if payload and payload.get("symbol"):
        query = f"?symbol={payload['symbol']}"
    req = urllib.request.Request(
        f"{HERMES_BASE}/autotrade/close-orphan{query}",
        data=b"{}",
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=25.0) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"ok": False, "error": f"HTTP {exc.code}: {body}"}


def autotrade_adopt_live(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    import urllib.error

    query = ""
    if payload and payload.get("symbol"):
        query = f"?symbol={payload['symbol']}"
    req = urllib.request.Request(
        f"{HERMES_BASE}/autotrade/adopt-live{query}",
        data=b"{}",
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=25.0) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"ok": False, "error": f"HTTP {exc.code}: {body}"}


def autotrade_precheck_live(symbol: str = "BTCUSDT") -> dict[str, Any]:
    import urllib.error

    query = urllib.parse.urlencode({"symbol": symbol or "BTCUSDT"})
    last_error = None
    for path in ("/autotrade/precheck-live", "/debug/binance-auth-check"):
        try:
            with urllib.request.urlopen(f"{HERMES_BASE}{path}?{query}", timeout=20.0) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            last_error = f"HTTP {exc.code}: {body}"
            if exc.code == 404:
                continue
            return {"ok": False, "error": last_error}
        except Exception as exc:
            last_error = str(exc)
            continue
    return {"ok": False, "error": last_error or "precheck endpoint unavailable"}


def learning_propose_config(symbol: str | None = None) -> dict[str, Any]:
    import urllib.error

    query = ""
    if symbol:
        query = f"?symbol={urllib.parse.quote(symbol)}"
    try:
        with urllib.request.urlopen(f"{HERMES_BASE}/learning/propose-config{query}", timeout=20.0) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"ok": False, "error": f"HTTP {exc.code}: {body}"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def learning_status(symbol: str | None = None) -> dict[str, Any]:
    import urllib.error

    query = ""
    if symbol:
        query = f"?symbol={urllib.parse.quote(symbol)}"
    try:
        with urllib.request.urlopen(f"{HERMES_BASE}/learning/status{query}", timeout=20.0) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"ok": False, "error": f"HTTP {exc.code}: {body}"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def learning_walk_forward(symbol: str, mode: str = "ALL", train_size: int = 30, test_size: int = 10) -> dict[str, Any]:
    import urllib.error

    query = urllib.parse.urlencode(
        {
            "symbol": symbol,
            "mode": mode,
            "train_size": int(train_size),
            "test_size": int(test_size),
        }
    )
    try:
        with urllib.request.urlopen(f"{HERMES_BASE}/learning/walk-forward?{query}", timeout=30.0) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"ok": False, "error": f"HTTP {exc.code}: {body}"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _main() -> int:
    parser = argparse.ArgumentParser(description="Hermes service manager")
    parser.add_argument("command", choices=["start", "stop", "status"])
    args = parser.parse_args()
    if args.command == "start":
        out = start()
    elif args.command == "stop":
        out = stop()
    else:
        out = status()
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(_main())
