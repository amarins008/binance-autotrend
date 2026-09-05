"""Dependency-injection contracts (Protocols) for the trading subsystem.

These are the seams that let modules depend on *interfaces* instead of on
module globals.  Each Protocol describes the minimum surface a collaborating
module needs.  Existing implementations (``cache.TradingCache``,
``http_client.HTTPClientManager``, ``memory_monitor``) conform implicitly
(structural typing) — no subclassing required.

The goal is *gradual* adoption: new code depends on these Protocols and gets
its collaborators from ``container``; legacy monolith code is free to keep
using module globals until it is migrated.
"""

from __future__ import annotations

import sys
from typing import Any, Protocol, runtime_checkable


if sys.version_info >= (3, 8):
    from typing import Protocol as _Protocol  # noqa: F401  (re-export alias)


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------

@runtime_checkable
class Cache(Protocol):
    """Thread-safe bounded key/value cache with TTL and stats."""

    def get(self, key: Any) -> Any | None: ...
    def set(self, key: Any, value: Any, ttl: float | None = None) -> None: ...
    def delete(self, key: Any) -> None: ...
    def clear(self) -> None: ...
    def __len__(self) -> int: ...

    @property
    def stats(self) -> Any: ...
    def stats_snapshot(self) -> dict: ...


class CacheRegistry(Protocol):
    """Provider of named named caches for each concern."""

    def get(self, name: str) -> Cache | None: ...
    def all_stats(self) -> dict: ...


# ---------------------------------------------------------------------------
# HTTP / exchange clients
# ---------------------------------------------------------------------------

@runtime_checkable
class HTTPClientProvider(Protocol):
    """Something that can yield pooled httpx clients for signed/data calls.

    Matches ``http_client.HTTPClientManager`` (async context managers).
    """

    def signed(self) -> Any: ...
    def data(self) -> Any: ...
    async def aclose(self) -> None: ...


# ---------------------------------------------------------------------------
# Memory / health monitors
# ---------------------------------------------------------------------------

@runtime_checkable
class MemoryMonitor(Protocol):
    """Read current process memory state.

    Matches the module-level functions in ``memory_monitor`` so the existing
    implementation conforms without adaptation.
    """

    def current_rss_mb(self) -> float: ...
    def periodic_memory_snapshot(self, max_age_sec: float = 10.0) -> dict: ...
    def check_memory(self) -> dict: ...