"""In-process memory monitoring with optional auto-actions.

Uses ``psutil`` to track the current process's RSS and emits a warning
log line when crossing a configurable threshold.  Also exposes an
``estimate_bytes`` helper that samples the size of a Python object so
callers can spot oversized in-memory structures (e.g. the intel/klines
caches) that could OOM the process.
"""

from __future__ import annotations

import os
import sys
from typing import Any

from logger import get_logger, log_exception

_log = get_logger("memory_monitor")

_DEFAULT_WARN_MB = int(os.getenv("MEMORY_WARN_MB", "1200"))
_DEFAULT_CRIT_MB = int(os.getenv("MEMORY_CRIT_MB", "1800"))


def current_rss_mb() -> float:
    """Current process resident set size in MB (0.0 if psutil unavailable)."""
    try:
        import psutil
        return psutil.Process().memory_info().rss / (1024 * 1024)
    except Exception as exc:
        # psutil may be missing in some minimal venvs — degrade gracefully.
        log_exception(_log, exc, {"stage": "current_rss_mb"})
        return 0.0


def check_memory(
    *,
    warn_mb: int = _DEFAULT_WARN_MB,
    crit_mb: int = _DEFAULT_CRIT_MB,
) -> dict:
    """Check current RSS and emit a warning/critical log if over threshold.

    Returns a dict summary (rssMb, level, ok).
    """
    try:
        import psutil
    except Exception:
        # No psutil — degrade to a cheap estimate via sys.getsizeof on known globals is not
        # reliable, so just report 0 / unknown.
        return {"rssMb": 0.0, "level": "unknown", "ok": True}

    rss_mb = current_rss_mb()
    level = "ok"
    if rss_mb >= crit_mb:
        level = "critical"
        _log.critical("Memory critical: %.0f MB (>= %d MB)", rss_mb, crit_mb)
    elif rss_mb >= warn_mb:
        level = "warning"
        _log.warning("Memory high: %.0f MB (>= %d MB)", rss_mb, warn_mb)
    return {"rssMb": round(rss_mb, 1), "level": level, "ok": level == "ok"}


def estimate_bytes(obj: Any, _depth: int = 0, _seen: set | None = None) -> int:
    """Approximate number of bytes a Python object (and its graph) occupies.

    Bounded to depth 3 and 200 elements per container to keep cost low.
    Only counts container headers + primitive scalars.
    """
    if obj is None or _depth > 3:
        return 0
    if _seen is None:
        _seen = set()
    if id(obj) in _seen:
        return 0
    _seen.add(id(obj))

    try:
        size = sys.getsizeof(obj)
    except Exception:
        return 0
    if _depth >= 3:
        return size

    try:
        if isinstance(obj, dict):
            for k, v in list(obj.items())[:200]:
                size += estimate_bytes(k, _depth + 1, _seen)
                size += estimate_bytes(v, _depth + 1, _seen)
        elif isinstance(obj, (list, tuple, set, frozenset)):
            for x in list(obj)[:200]:
                size += estimate_bytes(x, _depth + 1, _seen)
        elif isinstance(obj, bytes) and len(obj) > 1000:
            # Text blobs dominated by the scalar already measured by sys.getsizeof.
            pass
    except Exception:
        pass
    return size


# Lightweight periodic monitor the dashboard can poll.
_last_check_ms = [0.0]
_last_check_result: dict = {"rssMb": 0.0, "level": "unknown", "ok": True}


def periodic_memory_snapshot(max_age_sec: float = 10.0) -> dict:
    """Return a cached memory snapshot, refreshing at most once per period."""
    import time
    now = time.time()
    if now - _last_check_ms[0] >= max_age_sec:
        _last_check_result.update(check_memory())
        _last_check_result["measuredAt"] = now
        _last_check_ms[0] = now
    return dict(_last_check_result)
