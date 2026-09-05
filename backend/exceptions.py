"""Custom exceptions for the trading system.

Provides a typed exception hierarchy so consumers can handle failures
specifically instead of using bare ``except:`` blocks.

Hierarchy::

    TradingError (base)
    ├── ExchangeError      — Binance / exchange API failures
    ├── DataError          — market data fetch / parse failures
    ├── PipelineError      — entry pipeline gate failures
    ├── ConfigError        — configuration validation / load failures
    ├── SignalError        — signal generation / confluence failures
    ├── OrderError         — order placement / management failures
    └── StateError         — runtime state corruption / race conditions
"""

from __future__ import annotations


class TradingError(Exception):
    """Base exception for all trading-related errors."""


class ExchangeError(TradingError):
    """Raised when an exchange (Binance) API call fails.

    Attributes:
        status_code: optional HTTP status from the exchange.
        path:        the request path that failed.
        symbol:      symbol involved, if any.
    """

    def __init__(
        self,
        message: str = "",
        *,
        status_code: int | None = None,
        path: str = "",
        symbol: str = "",
    ) -> None:
        self.status_code = status_code
        self.path = path
        self.symbol = symbol
        prefix = path or symbol
        full = f"[{prefix}] {message}" if prefix else message
        super().__init__(full.strip())


class DataError(TradingError):
    """Raised when market data is unavailable, stale, or malformed."""


class PipelineError(TradingError):
    """Raised when the entry pipeline encounters an internal failure."""


class ConfigError(TradingError):
    """Raised when configuration is missing, invalid, or cannot be applied."""


class SignalError(TradingError):
    """Raised when signal generation or confluence scoring fails."""


class OrderError(TradingError):
    """Raised when order placement or position management fails.

    Attributes:
        exchange_code:  raw code returned by the exchange, if any.
        order_id:       client/exchange order id, if known.
    """

    def __init__(
        self,
        message: str = "",
        *,
        exchange_code: str = "",
        order_id: str = "",
    ) -> None:
        self.exchange_code = exchange_code
        self.order_id = order_id
        super().__init__(message)


class StateError(TradingError):
    """Raised when in-memory runtime state is corrupted or raced."""
