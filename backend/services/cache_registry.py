"""Shared in-memory caches with explicit TTL and size bounds."""

from __future__ import annotations

import asyncio
import os
import sys
import time

# L1 — per-symbol derived intel (long_score/short_score/regime/etc)
_INTEL_CACHE: dict[str, tuple[float, dict]] = {}
_INTEL_CACHE_TTL = 15
_INTEL_CACHE_MAX = 10

# L2 — raw klines (largest single object in the process)
_KLINES_CACHE: dict[tuple, tuple[float, list]] = {}
_KLINES_CACHE_TTL = 20
_KLINES_CACHE_MAX = 30
_KLINES_INFLIGHT: dict[tuple, asyncio.Task] = {}

DATA_GET_TIMEOUT_SEC = float(os.getenv("DATA_GET_TIMEOUT_SEC", "6.0"))
DATA_GET_CONNECT_TIMEOUT_SEC = float(os.getenv("DATA_GET_CONNECT_TIMEOUT_SEC", "1.6"))
DATA_GET_MAX_ATTEMPTS = max(1, int(os.getenv("DATA_GET_MAX_ATTEMPTS", "2")))
SCAN_ANALYZE_CONCURRENCY = max(1, min(8, int(os.getenv("SCAN_ANALYZE_CONCURRENCY", "4"))))

_DATA_PROVIDER_HEALTH: dict[str, object] = {
    "streak": 0,
    "cooldownUntil": 0,
    "lastErrorAt": 0,
    "lastError": "",
}

_LIVE_STATS_CACHE: dict[tuple, tuple[float, dict]] = {}
_LIVE_STATS_VERSION = 0

_SESSION_BIAS_CACHE: dict[str, object] = {
    "builtAt": 0.0,
    "liveVersion": -1,
    "mtime": -1.0,
    "hours": {},
}

_EXCHANGE_FILTERS_CACHE: dict[str, tuple[float, dict]] = {}
_EXCHANGE_FILTERS_CACHE_TTL = 60
_EXCHANGE_FILTERS_LOCK = asyncio.Lock()

_TRADE_LOG_CACHE: dict[tuple, tuple[float, int, list]] = {}
_SYMBOL_SAMPLE_COUNT_CACHE: dict[str, tuple[float, int]] = {}

def _limit_cache_size(cache_dict: dict, max_size: int = 150) -> None:
    """Evict oldest items if cache size exceeds max_size to prevent memory leaks."""
    if len(cache_dict) > max_size:
        try:
            # Sort by timestamp (which is first element of values tuple (ts, payload))
            # Some caches have tuples: cached = (now_ts, payload)
            sorted_keys = sorted(cache_dict.keys(), key=lambda k: cache_dict[k][0] if isinstance(cache_dict[k], tuple) and len(cache_dict[k]) > 0 else 0)
            for k in sorted_keys[:len(cache_dict) - max_size]:
                cache_dict.pop(k, None)
        except Exception:
            # Fallback pop if sorting fails
            while len(cache_dict) > max_size:
                cache_dict.pop(next(iter(cache_dict)), None)


def _safe_sizeof(obj, _depth: int = 0) -> int:
    """Bounded deep sizeof — sample only. Safe to call on hot paths."""
    if obj is None or _depth > 2:
        return 0
    try:
        size = sys.getsizeof(obj)
    except Exception:
        return 0
    try:
        if _depth >= 2:
            return size
        if isinstance(obj, dict):
            for k, v in list(obj.items())[:30]:
                size += _safe_sizeof(k, _depth + 1) + _safe_sizeof(v, _depth + 1)
        elif isinstance(obj, (list, tuple)):
            for x in list(obj)[:30]:
                size += _safe_sizeof(x, _depth + 1)
    except Exception:
        pass
    return size


def _est_cache(cache: dict, sample: int = 25) -> dict:
    """Approximate MB used by ``cache``. Sampler-based, O(sample) — cheap."""
    n = len(cache)
    if n == 0:
        return {"entries": 0, "estimatedMb": 0.0}
    sample_n = min(n, sample)
    keys_sample = list(cache.keys())[:sample_n]
    sample_bytes = 0
    for k in keys_sample:
        try:
            sample_bytes += _safe_sizeof(k) + _safe_sizeof(cache.get(k))
        except Exception:
            pass
    scaled = int(sample_bytes * (n / sample_n))
    return {"entries": n, "estimatedMb": round(scaled / (1024 * 1024), 3)}


def cache_size_estimates() -> dict:
    """Memory footprint snapshot for /autotrade/diagnostics.
    
    Designed to be cheap enough to expose on every diagnostics poll without
    material cost. Sampler-based estimate, not exact RSS — use the *trend*
    and not the absolute number.
    """
    return {
        "asOf": int(time.time()),
        "klines": _est_cache(_KLINES_CACHE, sample=15),
        "intel": _est_cache(_INTEL_CACHE),
        "exchangeFilters": _est_cache(_EXCHANGE_FILTERS_CACHE),
        "liveStats": _est_cache(_LIVE_STATS_CACHE),
        "tradeLog": _est_cache(_TRADE_LOG_CACHE),
        "symbolSampleCount": _est_cache(_SYMBOL_SAMPLE_COUNT_CACHE),
        "sessionBias": _est_cache(_SESSION_BIAS_CACHE) if isinstance(_SESSION_BIAS_CACHE, dict) else {"entries": 0, "estimatedMb": 0.0},
        "klinesMax": _KLINES_CACHE_MAX,
        "intelMax": _INTEL_CACHE_MAX,
    }

_USER_OVERRIDEN_PROFILE_KEYS: set[str] = {
    "minConfidence",
    "tpPct",
    "slPct",
    "profitLockTriggerUsdt",
    "notionalCapUsdt",
    "cooldownMinutes",
    "positionSizeMult",
    "leverageMult",
    "entryOffsetBps",
    "group",
}

_UMFUTURES_CLASS = None
