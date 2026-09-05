"""Minimal service container for gradual DI adoption.

Exposes the already-extracted singletons (logger, cache, memory, http)
behind the interfaces in ``interfaces.py``.  Legacy ``main.py`` still uses
module globals; this container is the *forward* seam that new and refactored
modules use instead of importing globals directly.

Usage::

    from container import services
    services["cache"].get("klines").set(key, value)
    services["memory"].snapshot()

Add new services here as modules are extracted.  Keep the container
dependency-free (imports only stdlib + the service modules).
"""

from __future__ import annotations

from typing import Any

from interfaces import Cache, HTTPClientProvider, MemoryMonitor

# The implementations conform structurally to the Protocols above.  Importing
# the modules here also side-effects nothing (module-level singletons are lazy).


def _build() -> dict[str, Any]:
    services: dict[str, Any] = {}

    # Cache layer: `cache` = TradingCache singletons + stats (top-level cache.py);
    # `cache_registry` = the legacy shared-state registry (services.cache_registry),
    # which holds the cross-module dicts (_KLINES_CACHE/_INTEL_CACHE/_DATA_PROVIDER_HEALTH).
    try:
        import cache as _cache_mod
        services["cache"] = _cache_mod
    except Exception:
        services.setdefault("cache", None)
    try:
        from services import cache_registry as _registry_mod
        services["cache_registry"] = _registry_mod
    except Exception:
        services.setdefault("cache_registry", None)

    # Memory monitor
    try:
        import memory_monitor as _mem_mod
        services["memory"] = _mem_mod
    except Exception:
        services.setdefault("memory", None)

    # HTTP pooled clients (http_client.py)
    try:
        import http_client as _http_mod
        services["http"] = _http_mod.get_http_manager()
    except Exception:
        services.setdefault("http", None)

    # Logging
    try:
        import logger as _log_mod
        services["logger"] = _log_mod
    except Exception:
        services.setdefault("logger", None)

    return services


services: dict[str, Any] = _build()


def get(name: str, default: Any = None) -> Any:
    """Return a registered service or ``default`` if missing/unavailable."""
    try:
        return services.get(name, default)
    except Exception:
        return default


def has(name: str) -> bool:
    return name in services and services[name] is not None


def register(name: str, service: Any) -> None:
    services[name] = service