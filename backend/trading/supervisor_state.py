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
# Per-domain locks: each tuning domain (entry/profit/scan/size) tracks the last
# applied direction independently. A tune is only blocked when ANY of the
# domains it touches is currently held by the OPPOSITE direction — so opposite
# tuners that write the same knobs (e.g. scan workload, profit-lock params, size
# multiplier) no longer fight each other across the lock window.
_TUNING_LOCK_DOMAINS: dict[str, dict] = {}
_DEFAULT_TUNING_LOCK_DOMAIN = "entry"  # strategy strictness (legacy single-lock)

# Config key for the lock duration (minutes)
_TUNING_MODE_LOCK_MINUTES_CFG_KEY = "supervisorTuningModeLockMinutes"
_DEFAULT_TUNING_MODE_LOCK_MINUTES = 90  # 90 minutes default


# ---------------------------------------------------------------------------
# Mode switches (fail-safe defaults)
# ---------------------------------------------------------------------------
# supervisorAutoTuneEnabled defaults to DISABLED: the tuners repeatedly
# thrashed live config (stopLossPct 0.28→0.196, TP ratchet → DEAD_ZONE_TIMEOUT
# 41%, minConfidence 0.72<->0.83 swing). A missing/dropped config key must
# never silently reactivate them — re-enabling requires explicit operator
# opt-in via config.
def _supervisor_tuning_enabled(cfg: dict | None = None) -> bool:
    source = cfg if isinstance(cfg, dict) else (AUTO_TRADE.get("config") or {})
    return bool(source.get("supervisorAutoTuneEnabled", False))


# Ops-healing (TradingView health recovery) is independent of the auto-tune
# kill switch: it only writes tradingviewEnabled / resets the TV client and
# fixes real incidents (entries opened blind against strong TV), so it
# defaults ON. Disable with config["supervisorHealingEnabled"]=False.
def _supervisor_healing_enabled(cfg: dict | None = None) -> bool:
    source = cfg if isinstance(cfg, dict) else (AUTO_TRADE.get("config") or {})
    return bool(source.get("supervisorHealingEnabled", True))


# Advisory mode: when auto-tune is disabled, tuners still run read-only on
# scratch copies and LOG what they would have changed — never writing config.
# Silence suggestions entirely with config["supervisorAdvisoryEnabled"]=False.
def _supervisor_advisory_enabled(cfg: dict | None = None) -> bool:
    source = cfg if isinstance(cfg, dict) else (AUTO_TRADE.get("config") or {})
    return bool(source.get("supervisorAdvisoryEnabled", True))


def _tuning_mode_lock_acquire(
    mode: str,
    reason: str,
    cfg: dict,
    *,
    domain: str | None = None,
    domains: tuple[str, ...] | list[str] | None = None,
) -> bool:
    """Try to acquire the tuning mode lock for one or more tuning domains.

    Returns True if the lock was acquired (i.e., we can proceed with this tune).
    Returns False if any requested domain is currently held by the opposite mode
    (i.e., must skip). `domain` is the single-domain shorthand; `domains` allows
    a tune that writes several knob groups (e.g. profit + size) to be gated by
    all of them at once.
    """
    # Advisory runs (auto-tune disabled) compute suggestions on scratch copies
    # and commit nothing — they must neither consult nor stamp mode locks.
    if not _supervisor_tuning_enabled(cfg):
        return True
    now = time.time()
    lock_duration_min = max(
        30,
        int(cfg.get(_TUNING_MODE_LOCK_MINUTES_CFG_KEY, _DEFAULT_TUNING_MODE_LOCK_MINUTES)
            or _DEFAULT_TUNING_MODE_LOCK_MINUTES),
    )
    lock_duration_sec = lock_duration_min * 60

    if isinstance(domains, (list, tuple)) and domains:
        target_domains = tuple(str(d) for d in domains)
    else:
        target_domains = (str(domain or _DEFAULT_TUNING_LOCK_DOMAIN),)

    to_stamp: list[str] = []
    for d in target_domains:
        lock = _TUNING_LOCK_DOMAINS.get(d)
        current_mode = str((lock or {}).get("mode", "neutral"))
        expires_at = float((lock or {}).get("expiresAt", 0) or 0)
        # Opposite mode still active — block this tune
        if current_mode != "neutral" and now < expires_at and current_mode != mode:
            return False
        # Lock expired or neutral — acquire freely (and stamp it)
        if current_mode == "neutral" or now >= expires_at:
            to_stamp.append(d)
    for d in to_stamp:
        lock = _TUNING_LOCK_DOMAINS.setdefault(d, {})
        lock["mode"] = mode
        lock["expiresAt"] = now + lock_duration_sec
        lock["reason"] = reason
    return True


def _tuning_mode_lock_release() -> None:
    """Release all tuning mode locks (set to neutral)."""
    _TUNING_LOCK_DOMAINS.clear()


def _tuning_mode_lock_status() -> dict:
    """Return current lock status for debugging/status API."""
    now = time.time()
    active: list[dict] = []
    for d, lock in sorted(_TUNING_LOCK_DOMAINS.items()):
        expires_at = float(lock.get("expiresAt", 0) or 0)
        if str(lock.get("mode", "neutral")) != "neutral" and now < expires_at:
            active.append({
                "domain": d,
                "mode": lock.get("mode"),
                "expiresAt": expires_at,
                "remainingSec": max(0, int(expires_at - now)),
                "reason": str(lock.get("reason", "") or ""),
            })
    entry = _TUNING_LOCK_DOMAINS.get(_DEFAULT_TUNING_LOCK_DOMAIN, {})
    return {
        "mode": entry.get("mode", "neutral"),
        "expiresAt": float(entry.get("expiresAt", 0) or 0),
        "remainingSec": max(0, int(float(entry.get("expiresAt", 0) or 0) - now)),
        "reason": entry.get("reason", ""),
        "activeDomains": active,
        "domains": {d: dict(lock) for d, lock in _TUNING_LOCK_DOMAINS.items()},
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
    "tradingview_health": "supervisorTradingViewHealthCooldownMinutes",
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


def _apply_rollback_old_values(cfg: dict, rollback: dict) -> dict:
    """Restore cfg keys to their pre-tune values recorded in a rollback entry.

    Tuning history stores the applied per-key {"old", "new"} under "changes";
    reverting means writing info["old"] back. (preMetrics holds performance
    stats, never config keys, so restoring from it was a no-op.) Returns the
    inversion {key: {"old": new, "new": old}} for accurate history logging.
    """
    reverted: dict[str, dict] = {}
    if not isinstance(cfg, dict):
        return reverted
    prev_changes = rollback.get("changes", {})
    if not isinstance(prev_changes, dict):
        return reverted
    for k, info in prev_changes.items():
        if isinstance(info, dict) and "old" in info and info["old"] is not None and k in cfg:
            cfg[k] = info["old"]
            reverted[k] = {"old": info.get("new"), "new": info.get("old")}
    return reverted


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
# Advisory suggestions (tuners run read-only while auto-tune is disabled)
# ---------------------------------------------------------------------------

# Per-key dedupe so advisory runs cannot spam the autotrade log every cycle.
_ADVISORY_LAST: dict[str, tuple[str, float]] = {}
_ADVISORY_DEDUPE_SEC = 1800  # 30 min per (key, identical suggestion)
_ADVISORY_HISTORY_CAP = 50


def record_advisory_suggestion(key: str, changes: dict, reason: str) -> dict:
    """Record what a tuner WOULD have changed, log-only. Never writes config."""
    now = time.time()
    signature = _tuning_signature(key, changes=changes)
    last = _ADVISORY_LAST.get(key)
    if last and last[0] == signature and now - last[1] < _ADVISORY_DEDUPE_SEC:
        return {"applied": False, "advisory": True, "key": key, "deduped": True}
    _ADVISORY_LAST[key] = (signature, now)
    entry = {
        "at": int(now),
        "key": key,
        "reason": str(reason or ""),
        "changes": dict(changes) if changes else {},
        "signature": signature,
    }
    history = AUTO_TRADE.setdefault("tuningSuggestions", [])
    if not isinstance(history, list):
        history = []
        AUTO_TRADE["tuningSuggestions"] = history
    history.append(entry)
    if len(history) > _ADVISORY_HISTORY_CAP:
        AUTO_TRADE["tuningSuggestions"] = history[-_ADVISORY_HISTORY_CAP:]
    try:
        from main import _autotrade_log  # type: ignore[import]
        text = json.dumps(entry["changes"], sort_keys=True, default=str)
        _autotrade_log(f"[Supervisor advisory] {key}: {text[:300]} ({reason})")
    except Exception:
        pass
    return {"applied": False, "advisory": True, "key": key, "reason": reason}


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
    if not _supervisor_tuning_enabled():
        return {"applied": False, "reason": "supervisor_autotune_disabled", "key": key}
    now = int(time.time())
    delegations[key] = {
        "at": now,
        "reason": reason,
        "changes": changes,
    }
    state["delegations"] = delegations
    AUTO_TRADE["supervisorAutoTune"] = state
    # Merge ONLY the tuner's changes onto the CURRENT live config. The old
    # code replaced the whole config with a deepcopy of the tuner's cfg — a
    # review holding a stale cfg reference clobbered operator keys applied
    # moments earlier via /bot/config (mirrors the main.py commit fix from
    # the 2026-08-01 full-audit).
    live = AUTO_TRADE.get("config")
    if not isinstance(live, dict):
        live = cfg if isinstance(cfg, dict) else {}
    merged = copy.deepcopy(live)
    for _k, _v in (changes or {}).items():
        if isinstance(_v, dict) and "new" in _v:
            merged[_k] = _v["new"]
        elif isinstance(_v, dict) and "reverted" in _v:
            merged[_k] = _v["reverted"]
        elif isinstance(_v, dict) and "set" in _v:
            merged[_k] = _v["set"]
        else:
            merged[_k] = _v
    try:
        from main import _enforce_entry_confidence_floor  # type: ignore[import]
        _enforce_entry_confidence_floor(merged)
    except Exception:
        pass
    AUTO_TRADE["config"] = merged
    _tuning_history_append(key, changes, _tuning_pre_metrics())
    try:
        from main import _persist_autotrade_snapshot, _autotrade_log  # type: ignore[import]
        _persist_autotrade_snapshot(force=True)  # config change must survive restart (throttle would lose it)
        _autotrade_log(f"Supervisor delegated {key}: {changes}")
    except Exception:
        pass
    return {"applied": True, "changes": changes, "reason": reason}
