"""Tests for the DI container + interface contracts (interfaces.py, container.py)."""
import interfaces
import container
from interfaces import Cache, HTTPClientProvider, MemoryMonitor
from cache import TradingCache
import memory_monitor
from http_client import HTTPClientManager


def test_container_has_expected_services():
    for name in ("cache", "cache_registry", "memory", "http", "logger"):
        assert container.has(name), f"{name} missing from container"


def test_container_get_unknown_returns_default():
    assert container.get("does_not_exist") is None
    assert container.get("does_not_exist", "fallback") == "fallback"


def test_container_register_override():
    marker = object()
    container.register("_test_sentinel", marker)
    assert container.get("_test_sentinel") is marker


def test_trading_cache_conforms_to_cache_protocol():
    c = TradingCache(name="test", maxsize=10, ttl=30)
    assert isinstance(c, Cache)


def test_memory_monitor_conforms_to_memory_protocol():
    assert isinstance(memory_monitor, MemoryMonitor)


def test_http_manager_conforms_to_http_provider_protocol():
    assert isinstance(HTTPClientManager.get_instance(), HTTPClientProvider)


def test_interfaces_module_has_protocols():
    for name in ("Cache", "CacheRegistry", "HTTPClientProvider", "MemoryMonitor"):
        assert hasattr(interfaces, name)