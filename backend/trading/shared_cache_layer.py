"""Shared in-memory cache layer for per-symbol autotune.

Provides a single cache that avoids redundant disk I/O when multiple
auto-tune functions in the same supervisor cycle need the same data.

Caches:
- learning profiles (all symbols, 5 s TTL)
- per-symbol rolling windows (20 s TTL)
- per-symbol risk-tune results (300 s TTL)
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from trading.per_symbol_storage import PerSymbolStorage
from trading.shared_storage import SharedStorage


_GLOBAL_CACHE: SharedCacheLayer | None = None


def get_shared_cache(vault_dir: Path | None = None) -> SharedCacheLayer:
    """Return a module-level singleton SharedCacheLayer.

    If *vault_dir* is explicitly given and the existing singleton is bound
    to a different directory, create a fresh cache for the requested dir so
    callers that rebuild state under a new VAULT_DIR (e.g. tests) are not
    served stale storage from an unrelated path.
    """
    global _GLOBAL_CACHE
    if _GLOBAL_CACHE is not None and vault_dir is not None and _GLOBAL_CACHE._vault_dir != vault_dir:
        _GLOBAL_CACHE = SharedCacheLayer(vault_dir)
        return _GLOBAL_CACHE
    if _GLOBAL_CACHE is None:
        _GLOBAL_CACHE = SharedCacheLayer(vault_dir)
    return _GLOBAL_CACHE


class SharedCacheLayer:
    """In-memory cache that sits above PerSymbolStorage / SharedStorage.

    Lifetime: one instance per supervisor review cycle.
    Created at the start of ``_hermes_supervisor_review()``, used by every
    ``PerSymbolContext``, then discarded.
    """

    def __init__(self, vault_dir: Path | None = None):
        self._vault_dir = vault_dir
        self._shared = SharedStorage(vault_dir)

        # symbol -> PerSymbolStorage (reused within a cycle)
        self._storages: dict[str, PerSymbolStorage] = {}

        # symbol -> profile dict (in-memory, short-lived)
        self._profile_cache: dict[str, tuple[float, dict]] = {}
        self._profile_ttl = 5.0

        # symbol -> windows dict
        self._window_cache: dict[str, tuple[float, dict]] = {}
        self._window_ttl = 20.0

        # symbol -> risk_tune dict
        self._risk_cache: dict[str, tuple[float, dict]] = {}
        self._risk_ttl = 300.0

        # symbol -> guardian lock dict (liveProfitLocks entry)
        self._guardian_cache: dict[str, tuple[float, dict]] = {}
        self._guardian_ttl = 300.0

        # symbol -> TradingView signal dict
        self._tv_cache: dict[str, tuple[float, dict]] = {}
        self._tv_ttl = 20.0

        # symbol -> runtime state dict (cooldowns, scan board, etc.)
        self._runtime_cache: dict[str, tuple[float, dict]] = {}
        self._runtime_ttl = 30.0

        # symbol -> 3-tier symbol profile dict (autotuned params, volatility tier, etc.)
        self._sym_profile_cache: dict[str, tuple[float, dict]] = {}
        self._sym_profile_ttl = 60.0

    # ------------------------------------------------------------------
    # Storage accessors
    # ------------------------------------------------------------------

    def get_storage(self, symbol: str) -> PerSymbolStorage:
        """Return (and cache) a PerSymbolStorage for *symbol*."""
        sym = symbol.upper().strip()
        if sym not in self._storages:
            self._storages[sym] = PerSymbolStorage(self._vault_dir, sym)
        return self._storages[sym]

    @property
    def shared(self) -> SharedStorage:
        return self._shared

    # ------------------------------------------------------------------
    # Profile cache
    # ------------------------------------------------------------------

    def get_profile(self, symbol: str) -> dict:
        """Return the learning profile for *symbol*, cached for a short TTL."""
        sym = symbol.upper().strip()
        now = time.time()
        cached = self._profile_cache.get(sym)
        if cached and (now - cached[0]) < self._profile_ttl:
            return dict(cached[1])
        storage = self.get_storage(sym)
        profile = storage.load_profile()
        self._profile_cache[sym] = (now, dict(profile))
        return profile

    def set_profile(self, symbol: str, profile: dict) -> None:
        """Update the in-memory cache after a write."""
        self._profile_cache[symbol.upper().strip()] = (time.time(), dict(profile))

    # ------------------------------------------------------------------
    # Windows cache
    # ------------------------------------------------------------------

    def get_windows(self, symbol: str) -> dict:
        """Return cached rolling windows for *symbol* if fresh."""
        sym = symbol.upper().strip()
        now = time.time()
        cached = self._window_cache.get(sym)
        if cached and (now - cached[0]) < self._window_ttl:
            return dict(cached[1])
        storage = self.get_storage(sym)
        windows = storage.load_windows(max_age_sec=int(self._window_ttl))
        if windows:
            self._window_cache[sym] = (now, dict(windows))
        return windows

    def set_windows(self, symbol: str, windows: dict) -> None:
        """Update the in-memory cache after a compute + save."""
        self._window_cache[symbol.upper().strip()] = (time.time(), dict(windows))

    # ------------------------------------------------------------------
    # Risk-tune cache
    # ------------------------------------------------------------------

    def get_risk_tune(self, symbol: str) -> dict:
        """Return cached risk-tune for *symbol* if fresh."""
        sym = symbol.upper().strip()
        now = time.time()
        cached = self._risk_cache.get(sym)
        if cached and (now - cached[0]) < self._risk_ttl:
            return dict(cached[1])
        storage = self.get_storage(sym)
        risk = storage.load_risk_tune(max_age_sec=int(self._risk_ttl))
        if risk:
            self._risk_cache[sym] = (now, dict(risk))
        return risk

    def set_risk_tune(self, symbol: str, risk: dict) -> None:
        """Update the in-memory cache after a compute + save."""
        self._risk_cache[symbol.upper().strip()] = (time.time(), dict(risk))

    # ------------------------------------------------------------------
    # Guardian lock cache
    # ------------------------------------------------------------------

    def get_guardian_lock(self, symbol: str) -> dict:
        """Return cached guardian lock for *symbol* if fresh."""
        sym = symbol.upper().strip()
        now = time.time()
        cached = self._guardian_cache.get(sym)
        if cached and (now - cached[0]) < self._guardian_ttl:
            return dict(cached[1])
        storage = self.get_storage(sym)
        lock = storage.load_guardian_lock()
        if lock:
            self._guardian_cache[sym] = (now, dict(lock))
        return lock

    def set_guardian_lock(self, symbol: str, lock: dict) -> None:
        """Update the in-memory cache after a save."""
        self._guardian_cache[symbol.upper().strip()] = (time.time(), dict(lock))

    # ------------------------------------------------------------------
    # TradingView signal cache
    # ------------------------------------------------------------------

    def get_tv_signal(self, symbol: str) -> dict:
        """Return cached TradingView signal for *symbol* if fresh."""
        sym = symbol.upper().strip()
        now = time.time()
        cached = self._tv_cache.get(sym)
        if cached and (now - cached[0]) < self._tv_ttl:
            return dict(cached[1])
        storage = self.get_storage(sym)
        signal = storage.load_tv_signal()
        if signal:
            self._tv_cache[sym] = (now, dict(signal))
        return signal

    def set_tv_signal(self, symbol: str, signal: dict) -> None:
        """Update the in-memory cache after a save."""
        self._tv_cache[symbol.upper().strip()] = (time.time(), dict(signal))

    # ------------------------------------------------------------------
    # Runtime state cache
    # ------------------------------------------------------------------

    def get_runtime(self, symbol: str) -> dict:
        """Return cached runtime state for *symbol* if fresh."""
        sym = symbol.upper().strip()
        now = time.time()
        cached = self._runtime_cache.get(sym)
        if cached and (now - cached[0]) < self._runtime_ttl:
            return dict(cached[1])
        storage = self.get_storage(sym)
        rt = storage.load_runtime()
        if rt:
            self._runtime_cache[sym] = (now, dict(rt))
        return rt

    def set_runtime(self, symbol: str, runtime: dict) -> None:
        """Update the in-memory cache after a save."""
        self._runtime_cache[symbol.upper().strip()] = (time.time(), dict(runtime))

    # ------------------------------------------------------------------
    # Symbol profile cache (3-tier)
    # ------------------------------------------------------------------

    def set_symbol_profile(self, symbol: str, profile: dict) -> None:
        """Update the in-memory cache for 3-tier symbol profile after a write."""
        self._sym_profile_cache[symbol.upper().strip()] = (time.time(), dict(profile))

    # ------------------------------------------------------------------
    # Invalidation
    # ------------------------------------------------------------------

    def invalidate_symbol(self, symbol: str) -> None:
        """Drop all cached data for *symbol* (e.g. after a trade close)."""
        sym = symbol.upper().strip()
        self._profile_cache.pop(sym, None)
        self._window_cache.pop(sym, None)
        self._risk_cache.pop(sym, None)
        self._guardian_cache.pop(sym, None)
        self._tv_cache.pop(sym, None)
        self._runtime_cache.pop(sym, None)
        self._sym_profile_cache.pop(sym, None)

    def invalidate_all(self) -> None:
        """Drop all caches (e.g. after a config change)."""
        self._profile_cache.clear()
        self._window_cache.clear()
        self._risk_cache.clear()
        self._guardian_cache.clear()
        self._tv_cache.clear()
        self._runtime_cache.clear()
        self._sym_profile_cache.clear()
        self._storages.clear()
