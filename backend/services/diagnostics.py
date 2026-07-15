"""Centralized diagnostics tracking for the autotrade bot.

Tracks errors, restarts, and crashes into the AUTO_TRADE state plus a
persistent newline-delimited JSON log so the next debug session has
full context about what the bot saw before it (re)started.

Design goals
------------
* **Never raise** — diagnostics must not crash the bot itself.
* **Best-effort disk persistence** — every event goes to
  ``backend/.standalone/errors.jsonl`` (one JSON record per line) so a
  crash that wipes memory still leaves a paper trail.
* **Lightweight** — short helpers callable from any layer.
* **Backwards compatible** — only adds new keys to ``AUTO_TRADE`` and
  appends to a log file; existing code paths are untouched.

Usage
-----
.. code-block:: python

    from services.diagnostics import (
        _record_autotrade_error,
        _record_autotrade_restart,
        _record_autotrade_crash,
    )

    try:
        do_risky_thing()
    except Exception as e:
        _record_autotrade_error(e, "module.func", {"symbol": "BTCUSDT"})

    # When the bot (re)starts for any reason:
    _record_autotrade_restart("auto-resume", "snapshot-restore", {"symbol": "X"})

    # When the main loop dies unexpectedly:
    _record_autotrade_crash(exc, "main-loop")
"""

from __future__ import annotations

import json
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services import app_state
from services.config_paths import BACKEND_ROOT


# Errors log lives next to other standalone logs so operators find it
# without grepping the whole tree.
_ERROR_LOG_PATH: Path = BACKEND_ROOT / ".standalone" / "errors.jsonl"


def _now_unix() -> int:
    return int(time.time())


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _append_jsonl(path: Path, record: dict) -> None:
    """Append a single JSON record and enforce a basic log rotation limit of 2000 lines. Best-effort, never raises."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, ensure_ascii=False, default=str)
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        
        # Simple line-count based log rotation (keep it lightweight, max ~2000 lines)
        if path.exists() and path.stat().st_size > 500 * 1024:  # roughly 500KB trigger
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
                if len(lines) > 2000:
                    path.write_text("\n".join(lines[-1000:]) + "\n", encoding="utf-8")
            except Exception:
                pass
    except Exception:
        # Last-resort: never let diagnostics crash the bot.
        try:
            print(f"[diagnostics] failed to append to {path}", flush=True)
        except Exception:
            pass


def _format_error_message(err: BaseException | None) -> str:
    """Return a short, single-line error message."""
    if err is None:
        return "Unknown"
    try:
        msg = str(err) or ""
    except Exception:
        msg = ""
    name = type(err).__name__
    if msg:
        return f"{name}: {msg}"[:300]
    return name[:300]


def _format_traceback(err: BaseException | None) -> str:
    """Return the last ~1.2 KB of the traceback as a single string."""
    if err is None:
        return ""
    try:
        return "".join(
            traceback.format_exception(type(err), err, err.__traceback__)
        )[-1200:]
    except Exception:
        return ""


def _record_autotrade_error(
    err: BaseException,
    source: str,
    context: dict | None = None,
) -> None:
    """Record a non-fatal error in ``AUTO_TRADE`` state and the JSONL log.

    Parameters
    ----------
    err:
        The exception that was caught (any ``BaseException`` subclass).
    source:
        Short tag identifying where the error happened
        (e.g. ``"auto-resume"``, ``"snapshot-write"``, ``"main-loop"``).
    context:
        Optional dict of extra fields (symbol, mode, attempt, etc.).
        Never replaces ``source`` — it is merged in.
    """
    try:
        at = app_state.AUTO_TRADE
        if not isinstance(at, dict):
            return
        msg = _format_error_message(err)
        tb = _format_traceback(err)
        ctx: dict = dict(context or {})
        ctx.setdefault("source", source)
        at["lastError"] = msg
        at["lastErrorAt"] = _now_unix()
        at["lastErrorType"] = type(err).__name__ if err else "Unknown"
        at["lastErrorSource"] = str(source or "")[:80]
        at["lastErrorContext"] = ctx
        at["lastErrorTraceback"] = tb
        # Bump consecutiveErrors so operators can spot recurring failures.
        at["consecutiveErrors"] = int(at.get("consecutiveErrors", 0) or 0) + 1
        _append_jsonl(_ERROR_LOG_PATH, {
            "ts": at["lastErrorAt"],
            "tsIso": _now_iso(),
            "kind": "error",
            "error": msg,
            "type": at["lastErrorType"],
            "source": at["lastErrorSource"],
            "context": ctx,
            "traceback": tb,
            "running": bool(at.get("running")),
            "sessionId": at.get("sessionId"),
        })
    except Exception:
        # Diagnostics itself must never crash the bot.
        try:
            print("[diagnostics] _record_autotrade_error failed", flush=True)
        except Exception:
            pass


def _record_autotrade_restart(
    reason: str,
    trigger: str,
    details: dict | None = None,
) -> None:
    """Record a (re)start event in ``AUTO_TRADE`` state and the JSONL log.

    Parameters
    ----------
    reason:
        Short human label such as ``"initial"``, ``"auto-resume"``,
        ``"watchdog"``, ``"manual"``.
    trigger:
        What caused the restart — typically one of ``"startup"``,
        ``"snapshot-restore"``, ``"watchdog-detect"``, ``"manual"``.
    details:
        Optional dict of extra context (symbol, mode, leverage, ...).
    """
    try:
        at = app_state.AUTO_TRADE
        if not isinstance(at, dict):
            return
        now = _now_unix()
        at["lastRestartAt"] = now
        at["lastRestartReason"] = str(reason or "")[:80]
        at["lastRestartTrigger"] = str(trigger or "")[:40]
        at["restartCount"] = int(at.get("restartCount", 0) or 0) + 1
        # Reset the uptime marker so callers can compute "time since last
        # restart" cheaply.
        at["uptimeStartAt"] = now
        # Reset consecutiveErrors — a fresh session shouldn't inherit the
        # previous session's failure count.
        at["consecutiveErrors"] = 0
        det: dict = dict(details or {})
        det.setdefault("reason", at["lastRestartReason"])
        det.setdefault("trigger", at["lastRestartTrigger"])
        _append_jsonl(_ERROR_LOG_PATH, {
            "ts": now,
            "tsIso": _now_iso(),
            "kind": "restart",
            "reason": at["lastRestartReason"],
            "trigger": at["lastRestartTrigger"],
            "sessionId": at.get("sessionId"),
            "details": det,
            "running": bool(at.get("running")),
            "config": (at.get("config") or {}).get("symbol") if isinstance(at.get("config"), dict) else None,
        })
    except Exception:
        try:
            print("[diagnostics] _record_autotrade_restart failed", flush=True)
        except Exception:
            pass


def _record_autotrade_crash(
    err: BaseException,
    source: str,
    context: dict | None = None,
) -> None:
    """Record a crash (task died, exception escaped the main loop).

    Increments ``crashCount`` and emits both an ``error`` and a ``crash``
    record so the operator has the full picture in one place.
    """
    try:
        at = app_state.AUTO_TRADE
        if isinstance(at, dict):
            at["crashCount"] = int(at.get("crashCount", 0) or 0) + 1
        ctx: dict = dict(context or {})
        ctx["crash"] = True
        _record_autotrade_error(err, source, ctx)
        if isinstance(at, dict):
            _append_jsonl(_ERROR_LOG_PATH, {
                "ts": _now_unix(),
                "tsIso": _now_iso(),
                "kind": "crash",
                "error": _format_error_message(err),
                "type": type(err).__name__ if err else "Unknown",
                "source": str(source or "")[:80],
                "context": ctx,
                "crashCount": at.get("crashCount", 0),
                "sessionId": at.get("sessionId"),
            })
    except Exception:
        try:
            print("[diagnostics] _record_autotrade_crash failed", flush=True)
        except Exception:
            pass


def _record_autotrade_startup(version: str | None = None) -> None:
    """Convenience wrapper for the very first (cold) startup.

    Use this from ``_lifespan`` so each cold start is logged even when
    no snapshot is restored.
    """
    details: dict[str, Any] = {}
    if version:
        details["version"] = version
    _record_autotrade_restart("initial", "startup", details)


def snapshot_diagnostics() -> dict:
    """Return a diagnostics snapshot suitable for an HTTP endpoint."""
    try:
        at = app_state.AUTO_TRADE
        if not isinstance(at, dict):
            return {}
        now = _now_unix()
        uptime_start = int(at.get("uptimeStartAt") or 0)
        return {
            "running": bool(at.get("running")),
            "sessionId": at.get("sessionId"),
            "startedAt": int(at.get("startedAt") or 0),
            "uptimeStartAt": uptime_start,
            "uptimeSec": (now - uptime_start) if uptime_start > 0 else 0,
            "lastError": at.get("lastError"),
            "lastErrorAt": at.get("lastErrorAt"),
            "lastErrorType": at.get("lastErrorType"),
            "lastErrorSource": at.get("lastErrorSource"),
            "lastErrorContext": at.get("lastErrorContext"),
            "lastErrorTraceback": (at.get("lastErrorTraceback") or "")[-600:],
            "lastRestartAt": at.get("lastRestartAt"),
            "lastRestartReason": at.get("lastRestartReason"),
            "lastRestartTrigger": at.get("lastRestartTrigger"),
            "restartCount": int(at.get("restartCount", 0) or 0),
            "crashCount": int(at.get("crashCount", 0) or 0),
            "consecutiveErrors": int(at.get("consecutiveErrors", 0) or 0),
        }
    except Exception:
        return {}


__all__ = [
    "_record_autotrade_error",
    "_record_autotrade_restart",
    "_record_autotrade_crash",
    "_record_autotrade_startup",
    "snapshot_diagnostics",
]