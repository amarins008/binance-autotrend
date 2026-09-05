"""Tests for the TTL/LRU cache layer (cache.py)."""
import time

from cache import TradingCache, get_klines_cache, all_cache_stats


def test_basic_set_get():
    c = TradingCache(name="test", maxsize=10, ttl=30)
    c.set("k", "v")
    assert c.get("k") == "v"


def test_miss_returns_none():
    c = TradingCache(name="test", maxsize=10, ttl=30)
    assert c.get("missing") is None


def test_eviction_beyond_maxsize():
    c = TradingCache(name="test", maxsize=3, ttl=30)
    for i in range(5):
        c.set(f"k{i}", i)
    assert len(c) <= 3
    assert c.get("k0") is None  # oldest evicted


def test_ttl_expiry():
    c = TradingCache(name="test", maxsize=10, ttl=0.1)
    c.set("k", "v")
    assert c.get("k") == "v"
    time.sleep(0.2)
    assert c.get("k") is None


def test_delete_removes():
    c = TradingCache(name="test", maxsize=10, ttl=30)
    c.set("k", "v")
    c.delete("k")
    assert c.get("k") is None


def test_clear():
    c = TradingCache(name="test", maxsize=10, ttl=30)
    c.set("a", 1)
    c.set("b", 2)
    c.clear()
    assert len(c) == 0


def test_stats_counts():
    c = TradingCache(name="test", maxsize=10, ttl=30)
    c.set("k", "v")
    c.get("k")  # hit
    c.get("missing")  # miss
    snap = c.stats_snapshot()
    assert snap["hits"] == 1
    assert snap["misses"] == 1
    assert snap["hitRate"] == 0.5


def test_singletons_exist():
    assert get_klines_cache() is get_klines_cache()
    stats = all_cache_stats()
    assert "klines" in stats
    assert "intel" in stats
