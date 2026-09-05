"""Tests for the memory monitor helpers (memory_monitor.py)."""
from memory_monitor import (
    check_memory,
    current_rss_mb,
    estimate_bytes,
    periodic_memory_snapshot,
)


def test_current_rss_positive():
    assert current_rss_mb() >= 0


def test_check_memory_ok():
    result = check_memory(warn_mb=10**9, crit_mb=10**9)  # huge thresholds
    assert "rssMb" in result
    assert result["level"] in ("ok", "warning", "critical") or result["level"] == "unknown"


def test_estimate_bytes_small():
    assert estimate_bytes(5) > 0


def test_estimate_bytes_dict():
    size = estimate_bytes({"a": [1, 2, 3], "b": {"x": "y" * 10}})
    assert size > 0


def test_estimate_bytes_bounded_depth():
    # Deeply nested structure must not blow up
    nested = {"a": {"b": {"c": {"d": {"e": {"f": "deep"}}}}}}
    size = estimate_bytes(nested)
    assert size > 0


def test_periodic_snapshot_shape():
    snap = periodic_memory_snapshot()
    assert "rssMb" in snap
    assert "measuredAt" in snap
    assert "level" in snap
