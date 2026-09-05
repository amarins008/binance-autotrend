"""Aggregated infra-health snapshot, shared across agents.

A single dict published to ``app_state.AUTO_TRADE["infraHealth"]``. Read by
supervisor reviews, market analyst decisions, and the data quality agent
without each one having to reach into binance_client / cache_registry.

Everything in this module is **read-only** — no mutation of external state.
Call ``collect()`` to get a fresh snapshot. Designed to be cheap enough to
call on every autotrade cycle and on every diagnostics hit.
"""

from __future__ import annotations

import time
from typing import Any

# Coarse thresholds (sec) for time-sync freshness. Conservative — orders
# can be rejected with timestamp_recv_window if our clock drifts.
_TIME_SYNC_OK_SEC = 600.0          # 10 min  → all good
_TIME_SYNC_DEGRADED_SEC = 1800.0   # 30 min  → warn
# Anything beyond 30 min or never → critical

# Coarse thresholds for cache fill.
_CACHE_HIGH_FILL = 0.80
_CACHE_SATURATED = 1.00

# When in-flight HTTP tasks pile up, network or binance is unhappy.
_INFLIGHT_PILEUP = 8


def _round1(v: float | None) -> float | int:
    """Round for stable dashboard reads; -1 sentinel for 'never'."""
    if v is None or v < 0:
        return -1
    return round(v, 1)


def collect() -> dict:
    """Snapshot of infra subsystems. Cheap to call (no IO, no deep scans)."""
    # Resolve cache/memory collaborators through the DI container so this module
    # depends on interfaces instead of importing module globals directly.
    from container import get as _svc
    cache_registry = _svc("cache_registry")
    memory_monitor_mod = _svc("memory")
    cache_mod = _svc("cache")
    from exchange.binance_client import (
        get_server_time_offset_ms,
        get_server_time_sync_age_sec,
    )

    now = int(time.time())

    # --- Binance server-time sync (critical for signed requests) ---
    try:
        offset_ms = int(get_server_time_offset_ms() or 0)
    except Exception:
        offset_ms = 0
    try:
        raw_age = float(get_server_time_sync_age_sec())
    except Exception:
        raw_age = float("inf")
    age = raw_age if raw_age != float("inf") else -1.0
    if age >= 0:
        last_sync_at = int(now - age)
        sync_healthy = age < _TIME_SYNC_OK_SEC
        sync_degraded = (not sync_healthy) and age < _TIME_SYNC_DEGRADED_SEC
        sync_stale = age >= _TIME_SYNC_DEGRADED_SEC
    else:
        last_sync_at = 0
        sync_healthy = False
        sync_degraded = False
        sync_stale = True

    binance_block: dict[str, Any] = {
        "timeOffsetMs": offset_ms,
        "lastSyncAt": last_sync_at,
        "ageSinceSyncSec": _round1(age),
        "syncHealthy": sync_healthy,
        "syncDegraded": sync_degraded,
        "syncStale": sync_stale,
    }

    # --- In-memory caches ---
    klines_n = len(cache_registry._KLINES_CACHE)
    intel_n = len(cache_registry._INTEL_CACHE)
    klines_max = int(cache_registry._KLINES_CACHE_MAX or 1)
    intel_max = int(cache_registry._INTEL_CACHE_MAX or 1)
    in_flight = len(getattr(cache_registry, "_KLINES_INFLIGHT", {}) or {})

    klines_block: dict[str, Any] = {
        "entries": klines_n,
        "maxEntries": klines_max,
        "fillRatio": round(klines_n / klines_max, 3),
        "inFlight": in_flight,
        "ttlSec": int(cache_registry._KLINES_CACHE_TTL),
    }
    intel_block: dict[str, Any] = {
        "entries": intel_n,
        "maxEntries": intel_max,
        "fillRatio": round(intel_n / intel_max, 3),
        "ttlSec": int(cache_registry._INTEL_CACHE_TTL),
    }

    # --- Data provider cooldown (already tracked in cache_registry) ---
    dph = getattr(cache_registry, "_DATA_PROVIDER_HEALTH", {}) or {}
    cooldown_until = int(dph.get("cooldownUntil", 0) or 0)
    cooldown_remaining = max(0, cooldown_until - now)
    data_block: dict[str, Any] = {
        "streak": int(dph.get("streak", 0) or 0),
        "cooldownUntil": cooldown_until,
        "cooldownRemainingSec": cooldown_remaining,
        "cooldownActive": cooldown_remaining > 0,
        "lastError": str(dph.get("lastError", "") or "")[:200],
    }

    # --- Process memory ---
    try:
        mem = memory_monitor_mod.periodic_memory_snapshot() or {}
        memory_block: dict[str, Any] = {
            "rssMb": float(mem.get("rssMb", 0.0) or 0.0),
            "level": str(mem.get("level", "unknown")),
            "ok": bool(mem.get("ok", True)),
        }
    except Exception:
        memory_block = {"rssMb": 0.0, "level": "unknown", "ok": True}

    # --- New TradingCache layer (hit/miss/eviction stats) ---
    try:
        cache_stats = cache_mod.all_cache_stats()
    except Exception:
        cache_stats = {}

    out: dict[str, Any] = {
        "asOf": now,
        "binance": binance_block,
        "klinesCache": klines_block,
        "intelCache": intel_block,
        "dataProvider": data_block,
        "memory": memory_block,
        "cacheStats": cache_stats,
        "score": 100,
        "issues": [],
        "warnings": 0,
        "errors": 0,
    }

    # --- Score (start 100, deduct; floor 0) ---
    score = 100
    issues: list[str] = []
    warnings = 0
    errors = 0

    if binance_block["syncStale"]:
        score -= 60
        issues.append("binance_time_sync_stale")
        errors += 1
    elif binance_block["syncDegraded"]:
        score -= 25
        issues.append("binance_time_sync_aging")
        warnings += 1

    if data_block["cooldownActive"]:
        # Long cooldowns are worse — cap on >= 60s remaining.
        penalty = 35 if data_block["cooldownRemainingSec"] >= 60 else 20
        score -= penalty
        issues.append(
            "data_provider_cooldown_active"
            f"({data_block['cooldownRemainingSec']}s)"
        )
        warnings += 1

    kr = klines_block["fillRatio"]
    if kr >= _CACHE_SATURATED:
        score -= 10
        issues.append("klines_cache_saturated")
        warnings += 1
    elif kr >= _CACHE_HIGH_FILL:
        score -= 3
        issues.append("klines_cache_high_fill")
        warnings += 1

    ir = intel_block["fillRatio"]
    if ir >= _CACHE_SATURATED:
        score -= 10
        issues.append("intel_cache_saturated")
        warnings += 1
    elif ir >= _CACHE_HIGH_FILL:
        score -= 3
        issues.append("intel_cache_high_fill")
        warnings += 1

    if klines_block["inFlight"] >= _INFLIGHT_PILEUP:
        score -= 15
        issues.append(f"klines_inflight_pileup({klines_block['inFlight']})")
        warnings += 1

    # --- Process memory check ---
    mem_level = memory_block.get("level")
    if mem_level == "critical":
        score -= 40
        issues.append("memory_critical")
        errors += 1
    elif mem_level == "warning":
        score -= 15
        issues.append("memory_high")
        warnings += 1

    out["score"] = max(0, min(100, score))
    out["issues"] = issues
    out["warnings"] = warnings
    out["errors"] = errors
    return out


def grade(score: int) -> str:
    """Coarse letter-grade for dashboards."""
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def is_actionable(health: dict) -> bool:
    """True when an agent should consider pausing / reducing risk."""
    return int(health.get("score", 0)) < 60 or int(health.get("errors", 0)) > 0


# Score floor for an agent to *proceed* with a new trade.
# Below this we mark the agent blocked and skip the cycle (post-DQ
# check). Tuned conservatively — only fully degraded infra blocks.
_GATE_MIN_SCORE = 40


def gate(*, require_binance_healthy: bool = True, min_score: int = _GATE_MIN_SCORE) -> str | None:
    """Return skip-reason string if infra is too degraded, else None.

    Reads ``app_state.AUTO_TRADE["infraHealth"]`` (the shared snapshot
    refreshed each cycle) — no IO, no locking, microsecond cost.

    Best-effort by design: any error returns None so the caller never
    gets wedged by infra sampling itself.
    """
    try:
        from services import app_state
        trade = getattr(app_state, "AUTO_TRADE", None) or {}
        infra = trade.get("infraHealth") or {}
    except Exception:
        return None
    if not isinstance(infra, dict) or not infra:
        # Not populated yet — first cycle hasn't run. Don't block.
        return None
    try:
        score = int(infra.get("score", 100) or 100)
    except Exception:
        score = 100
    if score < min_score:
        return f"infra_score_{score}_below_{min_score}"
    if require_binance_healthy:
        try:
            bn = infra.get("binance") or {}
            if bool(bn.get("syncStale")):
                return "binance_time_sync_stale"
        except Exception:
            pass
    return None
