"""Connection-pooled HTTP clients for the trading system.

Maintains long-lived ``httpx.AsyncClient`` instances (with keep-alive
connection pooling) instead of creating a new client per request.  The
old code spawned a fresh ``httpx.AsyncClient`` inside every ``_data_get``
call, which paid TCP/TLS handshake + connection overhead on each request.

Two clients are provided, matching the existing split in main.py:

- ``signed``   — account/order endpoints (respects ``BINANCE_TESTNET``)
- ``data``     — public market-data endpoints (always mainnet, faster)

Both are singletons, lazily created, and safe to share across the whole
asyncio loop.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import httpx

from exceptions import ExchangeError
from logger import get_logger, log_exception

_log = get_logger("http_client")

_DEFAULT_LIMITS = httpx.Limits(
    max_connections=100,
    max_keepalive_connections=20,
    keepalive_expiry=30.0,
)

_TIMEOUT = httpx.Timeout(
    connect=float(os.getenv("DATA_GET_CONNECT_TIMEOUT_SEC", "1.6")),
    read=float(os.getenv("DATA_GET_TIMEOUT_SEC", "6.0")),
    write=10.0,
    pool=10.0,
)


class HTTPClientManager:
    """Holds and lazily constructs pooled async clients."""

    _instance: "HTTPClientManager | None" = None

    def __init__(self) -> None:
        self._signed: httpx.AsyncClient | None = None
        self._data: httpx.AsyncClient | None = None

    @classmethod
    def get_instance(cls) -> "HTTPClientManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _ensure_signed(self) -> httpx.AsyncClient:
        if self._signed is None or self._signed.is_closed:
            self._signed = httpx.AsyncClient(limits=_DEFAULT_LIMITS, timeout=_TIMEOUT)
        return self._signed

    def _ensure_data(self) -> httpx.AsyncClient:
        if self._data is None or self._data.is_closed:
            self._data = httpx.AsyncClient(limits=_DEFAULT_LIMITS, timeout=_TIMEOUT)
        return self._data

    @asynccontextmanager
    async def signed(self):
        """Async context manager yielding the pooled signed client."""
        client = self._ensure_signed()
        try:
            yield client
        except httpx.HTTPError as exc:
            log_exception(_log, exc, {"client": "signed"})
            raise ExchangeError(f"exchange HTTP failure: {exc}") from exc

    @asynccontextmanager
    async def data(self):
        """Async context manager yielding the pooled market-data client."""
        client = self._ensure_data()
        try:
            yield client
        except httpx.HTTPError as exc:
            log_exception(_log, exc, {"client": "data"})
            raise ExchangeError(f"market-data HTTP failure: {exc}") from exc

    async def aclose(self) -> None:
        """Close both pooled clients (call during app shutdown)."""
        for client in (self._signed, self._data):
            if client is not None and not client.is_closed:
                await client.aclose()
        self._signed = None
        self._data = None


def get_http_manager() -> HTTPClientManager:
    """Return the module-level singleton ``HTTPClientManager``."""
    return HTTPClientManager.get_instance()
