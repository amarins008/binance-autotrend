"""Supervisor auto-tune state helpers — extracted from main.py.

Previously these lived in main.py, causing every consumer (supervisor_tuning,
supervisor_review, symbol_profiles …) to lazy-import main at runtime via a
``_main()`` shim just to reach these functions. That pattern made the modules
untestable in isolation and created hidden runtime coupling.

These functions own:
- Cooldown tracking  (_supervisor_delegation_cooldown)
- Tuning history     (_tuning_history_append, _tuning_rollback_last)
- Metric snapshots   (_tuning_pre_metrics)
- Rollback check     (_tuning_should_rollback)  ← includes cache
- Config commit      (_commit_supervisor_config_tune)
- Signature hashing  (_tuning_signature)

All state is stored in ``app_state.AUTO_TRADE`` so it survives across module
reloads within the same process.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import time

from services import app_state

AUTO_TRADE = app_state.AUTO_TRADE

# ---------------------------------------------------------------------------
# Tuning mode lock (prevents oscillation between loosener and tightener)
# ---------------------------------------------------------------------------
_TUNING_MODE_LOCK_STATE: dict[str, object] = {
    "mode": "neutral",  # "neutral" | "loosening" | "tightening"
    "expiresAt": 0,
    "reason": "",
}

# Config key for the lock duration (minutes)
_TUNING_MODE_LOCK_MINUTES_CFG_KEY = "supervisorTuningModeLockMinutes"
_DEFAULT_TUNING_MODE_LOCK_MINUTES = 90  # 90 minutes default


def _tuning_mode_lock_acquire(mode: str, reason: str, cfg: dict) -> bool:
    """Try to acquire the tuning mode lock.

    Returns True if the lock was acquired (i.e., we can proceed with this tune).
    Returns False if the lock is held by the opposite mode (i.e., must skip).
    """
    now = time.time()
    lock = _TUNING_MODE_LOCK_STATE
    lock_duration_min = max(
        30,
        int(cfg.get(_TUNING_MODE_LOCK_MINUTES_CFG_KEY, _DEFAULT_TUNING_MODE_LOCK_MINUTES)
            or _DEFAULT_TUNING_MODE_LOCK_MINUTES),
    )
    lock_duration_sec = lock_duration_min * 60

    current_mode = lock.get("mode", "neutral")
    expires_at = float(lock.get("expiresAt", 0) or 0)

    # If lock expired, it's neutral — acquire freely
    if current_mode == "neutral" or now >= expires_at:
        lock["mode"] = mode
        lock["expiresAt"] = now + lock_duration_sec
        lock["reason"] = reason
        return True

    # If same mode, allow (we can reinforce the same direction)
    if current_mode == mode:
        return True

    # Opposite mode still active — block this tune
    return False


def _tuning_mode_lock_release() -> None:
    """Release the tuning mode lock (set to neutral)."""
    _TUNING_MODE_LOCK_STATE["mode"] = "neutral"
    _TUNING_MODE_LOCK_STATE["expiresAt"] = 0
    _TUNING_MODE_LOCK_STATE["reason"] = ""


def _tuning_mode_lock_status() -> dict:
    """Return current lock status for debugging/status API."""
    now = time.time()
    lock = _TUNING_MODE_LOCK_STATE
    expires_at = float(lock.get("expiresAt", 0) or 0)
    return {
        "mode": lock.get("mode", "neutral"),
        "expiresAt": expires_at,
        "remainingSec": max(0, int(expires_at - now)),
        "reason": lock.get("reason", ""),
    }


# ---------------------------------------------------------------------------
# Cache for rollback metric checks (avoids re-parsing trades_log on every
# supervisor cycle). Invalidated by _LIVE_STATS_VERSION bump or TTL.
# ---------------------------------------------------------------------------
_ROLLBACK_METRICS_CACHE: dict[str, object] = {"version": -1, "ts": 0.0, "stats": {}}
_ROLLBACK_METRICS_TTL = 30  # seconds


# ---------------------------------------------------------------------------
# Cooldown
# ---------------------------------------------------------------------------

_COOLDOWN_CFG_KEYS: dict[str, str] = {
    "low_entry_activity": "supervisorLowEntryTuneCooldownMinutes",
    "bad_utc_hour": "supervisorBadUtcTuneCooldownMinutes",
    "negative_expectancy": "supervisorNegativeExpectancyTuneCooldownMinutes",
    "daily_entry_regression": "supervisorDailyRegressionCooldownMinutes",
    "small_profit_capture": "supervisorSmallProfitCooldownMinutes",
    "weak_payoff": "supervisorPayoffTuneCooldownMinutes",
    "size_streak": "supervisorSizeStreakCooldownMinutes",
    "scan_timeout": "supervisorScanTimeoutCooldownMinutes",
}


def _supervisor_delegation_cooldown(
    key: str, cfg: dict, default_minutes: int
) -> tuple[dict, dict, bool, int]:
    """Per-tuning-type cooldown with independent tracking.

    Returns:
        (state, delegations, active, cooldown_sec)
    """
    now = int(time.time())
    state = AUTO_TRADE.get("supervisorAutoTune")
    if not isinstance(state, dict):
        state = {}
    delegations = state.get("delegations")
    if not isinstance(delegations, dict):
        delegations = {}
    rec = delegations.get(key) if isinstance(delegations.get(key), dict) else {}
    cfg_key = _COOLDOWN_CFG_KEYS.get(key, "supervisorDelegationCooldownMinutes")
    cooldown_sec = max(300, int(cfg.get(cfg_key, default_minutes) or default_minutes) * 60)
    active = now - int(rec.get("at", 0) or 0) < cooldown_sec
    state["delegations"] = delegations
    return state, delegations, active, cooldown_sec


# ---------------------------------------------------------------------------
# Tuning history
# ---------------------------------------------------------------------------

def _tuning_history_append(
    key: str, changes: dict, pre_metrics: dict | None = None
) -> None:
    """Record a tuning action for impact tracking and rollback."""
    entry = {
        "at": int(time.time()),
        "key": key,
        "changes": dict(changes) if changes else {},
        "preMetrics": dict(pre_metrics) if pre_metrics else {},
        "reverted": False,
    }
    history = AUTO_TRADE.setdefault("tuningHistory", [])
    if not isinstance(history, list):
        history = []
        AUTO_TRADE["tuningHistory"] = history
    history.append(entry)
    if len(history) > 50:
        AUTO_TRADE["tuningHistory"] = history[-50:]


def _tuning_rollback_last(key: str) -> dict:
    """Rollback the most recent un-reverted tuning of *key*.

    Returns:
        {"reverted": True, "preMetrics": {...}, "changes": {...}}
        or {"reverted": False, "reason": "..."}
    """
    history = AUTO_TRADE.get("tuningHistory")
    if not isinstance(history, list):
        return {"reverted": False, "reason": "no_history"}
    for entry in reversed(history):
        if entry.get("key") == key and not entry.get("reverted"):
            entry["reverted"] = True
            pre = entry.get("preMetrics", {}) or {}
            return {"reverted": True, "preMetrics": pre, "changes": entry.get("changes", {})}
    return {"reverted": False, "reason": "no_matching_entry"}


# ---------------------------------------------------------------------------
# Metric snapshots
# ---------------------------------------------------------------------------

def _tuning_pre_metrics() -> dict:
    """Capture current performance metrics before applying a tune.

    Avoids importing main.py at module level; uses a lazy import so this
    module stays importable in isolation during tests.
    """
    try:
        from main import _aggregate_live_trade_stats_from_log  # type: ignore[import]
        stats = _aggregate_live_trade_stats_from_log(None) or {}
    except Exception:
        stats = {}
    return {
        "winRatePct": float(stats.get("winRatePct", 0.0) or 0.0),
        "avgPnl": float(stats.get("avgPnl", 0.0) or 0.0),
        "payoffRatio": float(stats.get("payoffRatio", 0.0) or 0.0),
        "realizedPnl": float(stats.get("realizedPnl", 0.0) or 0.0),
        "trades": int(stats.get("trades", 0) or 0),
    }


# ---------------------------------------------------------------------------
# Rollback check (with metrics cache)
# ---------------------------------------------------------------------------

def _tuning_should_rollback(key: str, post_window_trades: int = 3) -> bool:
    """Return True if the most recent tune of *key* has since worsened metrics.

    Uses _ROLLBACK_METRICS_CACHE to avoid re-parsing the full trade log on
    every supervisor cycle. The cache is invalidated when _LIVE_STATS_VERSION
    changes (meaning a new trade was closed) or after _ROLLBACK_METRICS_TTL
    seconds, whichever comes first.
    """
    history = AUTO_TRADE.get("tuningHistory")
    if not isinstance(history, list):
        return False
    for entry in reversed(history):
        if entry.get("key") != key or entry.get("reverted"):
            continue
        age = time.time() - int(entry.get("at", 0) or 0)
        if age < 60 or age > 3600 * 4:
            continue
        pre = entry.get("preMetrics", {}) or {}
        if not pre:
            continue
        # Try cache first.
        now = time.time()
        cache = _ROLLBACK_METRICS_CACHE
        try:
            from main import _LIVE_STATS_VERSION  # type: ignore[import]
            live_ver = _LIVE_STATS_VERSION
        except Exception:
            live_ver = None
        if (
            live_ver is not None
            and cache.get("version") == live_ver
            and (now - float(cache.get("ts", 0.0) or 0.0)) < _ROLLBACK_METRICS_TTL
            and isinstance(cache.get("stats"), dict)
        ):
            current = cache["stats"]
        else:
            try:
                from main import _aggregate_live_trade_stats_from_log  # type: ignore[import]
                current = _aggregate_live_trade_stats_from_log(None) or {}
            except Exception:
                current = {}
            _ROLLBACK_METRICS_CACHE.update({
                "version": live_ver,
                "ts": now,
                "stats": dict(current),
            })
        pre_wr = float(pre.get("winRatePct", 0.0) or 0.0)
        cur_wr = float(current.get("winRatePct", 0.0) or 0.0)
        pre_pnl = float(pre.get("avgPnl", 0.0) or 0.0)
        cur_pnl = float(current.get("avgPnl", 0.0) or 0.0)
        if pre_wr > 0 and cur_wr < pre_wr - 12.0 and cur_pnl < pre_pnl - 0.05:
            return True
        if pre_pnl > 0 and cur_pnl < pre_pnl - 0.08:
            return True
    return False


# ---------------------------------------------------------------------------
# Signature
# ---------------------------------------------------------------------------

def _tuning_signature(key: str, **parts) -> str:
    """Stable short hash of a tuning event for deduplication."""
    payload = json.dumps({"key": key, **parts}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Config commit
# ---------------------------------------------------------------------------

def _commit_supervisor_config_tune(
    state: dict,
    delegations: dict,
    key: str,
    cfg: dict,
    changes: dict,
    reason: str,
) -> dict:
    """Apply tuning changes to live config, record history, and persist snapshot."""
    now = int(time.time())
    delegations[key] = {
        "at": now,
        "reason": reason,
        "changes": changes,
    }
    state["delegations"] = delegations
    AUTO_TRADE["supervisorAutoTune"] = state
    AUTO_TRADE["config"] = copy.deepcopy(cfg)
    _tuning_history_append(key, changes, _tuning_pre_metrics())
    try:
        from main import _persist_autotrade_snapshot, _autotrade_log  # type: ignore[import]
        _persist_autotrade_snapshot()
        _autotrade_log(f"Supervisor delegated {key}: {changes}")
    except Exception:
        pass
    return {"applied": True, "changes": changes, "reason": reason}
