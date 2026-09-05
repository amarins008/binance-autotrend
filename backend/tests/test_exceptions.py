"""Tests for the typed exception hierarchy in exceptions.py."""
import pytest

from exceptions import (
    TradingError,
    ExchangeError,
    DataError,
    PipelineError,
    ConfigError,
    SignalError,
    OrderError,
    StateError,
)


def test_exchange_error_has_metadata():
    err = ExchangeError("boom", status_code=400, path="/fapi/v1/test", symbol="BTCUSDT")
    assert err.status_code == 400
    assert err.path == "/fapi/v1/test"
    assert err.symbol == "BTCUSDT"
    assert "/fapi/v1/test" in str(err)  # prefix (path) appears in message


def test_exchange_error_uses_symbol_when_no_path():
    err = ExchangeError("boom", symbol="ETHUSDT")
    assert "ETHUSDT" in str(err)


def test_exchange_error_empty_message():
    err = ExchangeError()
    assert err.status_code is None
    assert err.path == ""
    assert err.symbol == ""


def test_order_error_metadata():
    err = OrderError("rejected", exchange_code="-2010", order_id="abc")
    assert err.exchange_code == "-2010"
    assert err.order_id == "abc"


def test_all_are_trading_error_subclasses():
    for cls in (ExchangeError, DataError, PipelineError, ConfigError, SignalError, OrderError, StateError):
        assert issubclass(cls, TradingError)


def test_hierarchy_distinct():
    # Each should be distinct (not accidentally sharing a base subclass chain)
    assert ExchangeError is not DataError
    assert ConfigError is not StateError
