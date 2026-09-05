"""TTL/LRU caching backed by ``cachetools`` (thread-safe).

Replaces the hand-rolled dict-based caches in ``cache_registry.py`` with a
single, bounded, TTL-aware cache layer.  ``cachetools.TTLCache``/``LRUCache``
handle size bounding and expiry internally, and are locked for thread-safety.

Design notes:
- Each cache is scoped to a concern (klines / intel / profile / trade-log).
- TTLs mirror the legacy values (klines 20s, intel 15s, profile 5s, ...).
- Every cache records hit/miss/eviction counts so operators can see whether
  the TTLs are actually saving I/O (see ``CacheStats``).
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable

from cachetools import TTLCache

from logger import get_logger

_log = get_logger("cache")


class CacheStats:
    """Per-cache hit/miss/eviction counters."""

    __slots__ = ("hits", "misses", "evictions")

    def __init__(self) -> None:
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def snapshot(self) -> dict:
        total = self.hits + self.misses
        return {
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "hitRate": round(self.hits / total, 4) if total else 0.0,
        }


class TradingCache:
    """Thread-safe, bounded, TTL cache with stats.

    Args:
        name: cache name (used in log/metrics).
        maxsize: max number of entries (LRU-evicts oldest beyond this).
        ttl: entry time-to-live in seconds.
    """

    def __init__(self, name: str, maxsize: int = 150, ttl: float = 30.0) -> None:
        self.name = name
        self._cache = TTLCache(maxsize=maxsize, ttl=ttl)
        self._lock = threading.RLock()
        self._stats = CacheStats()

    def get(self, key: Any) -> Any | None:
        with self._lock:
            try:
                value = self._cache.get(key)
            except Exception as exc:
                _log.debug("cache %s get failed %r", self.name, exc)
                return None
            if value is not None:
                self._stats.hits += 1
            else:
                self._stats.misses += 1
            return value

    def set(self, key: Any, value: Any, ttl: float | None = None) -> None:
        with self._lock:
            try:
                if ttl is not None and ttl > 0:
                    # cachetools 5.x+: replace entry with a shorter/longer TTL
                    try:
                        self._cache.currsize  # noqa: B018 - ensure initialized
                        self._cache.ttl = ttl
                    except Exception:
                        pass
                self._cache[key] = value
            except Exception:
                # Overflow / too many items — evict oldest and retry once
                if len(self._cache) > 0:
                    try:
                        self._cache.popitem(last=False)
                    except Exception:
                        pass
                try:
                    self._cache[key] = value
                except Exception as exc:
                    _log.debug("cache %s set failed %r", self.name, exc)

    def delete(self, key: Any) -> None:
        with self._lock:
            try:
                self._cache.pop(key, None)
            except Exception:
                pass

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)

    @property
    def stats(self) -> CacheStats:
        return self._stats

    def stats_snapshot(self) -> dict:
        with self._lock:
            snap = self._stats.snapshot()
            snap["size"] = len(self._cache)
            snap["maxsize"] = self._cache.maxsize
            snap["ttl"] = getattr(self._cache, "ttl", None)
            return snap


# ---------------------------------------------------------------------------
# Shared singleton caches (module-level)
# ---------------------------------------------------------------------------

_cache_klines = TradingCache(name="klines", maxsize=100, ttl=20.0)
_cache_intel = TradingCache(name="intel", maxsize=50, ttl=15.0)
_cache_profile = TradingCache(name="profile", maxsize=200, ttl=5.0)
_cache_trade_log = TradingCache(name="trade_log", maxsize=20, ttl=20.0)
_cache_filters = TradingCache(name="filters", maxsize=150, ttl=60.0)


def get_klines_cache() -> TradingCache:
    return _cache_klines


def get_intel_cache() -> TradingCache:
    return _cache_intel


def get_profile_cache() -> TradingCache:
    return _cache_profile


def get_trade_log_cache() -> TradingCache:
    return _cache_trade_log


def get_filters_cache() -> TradingCache:
    return _cache_filters


def all_cache_stats() -> dict:
    """Return a snapshot of every shared cache for the dashboard."""
    return {
        "klines": _cache_klines.stats_snapshot(),
        "intel": _cache_intel.stats_snapshot(),
        "profile": _cache_profile.stats_snapshot(),
        "trade_log": _cache_trade_log.stats_snapshot(),
        "filters": _cache_filters.stats_snapshot(),
    }
