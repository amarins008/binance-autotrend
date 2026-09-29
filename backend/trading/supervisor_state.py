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
# Cache for rollback metric checks — dropped (2026-09-27): the rollback check
# now reads via _live_closed_trades_from_log which self-caches by mtime/size.
# ---------------------------------------------------------------------------


def _recent_closed_trades() -> list[dict]:
    """Closed LIVE trades (newest last) from the shared trade log."""
    try:
        from trading.trade_log import _live_closed_trades_from_log  # type: ignore[import]
        return _live_closed_trades_from_log(symbol=None, mode="ALL") or []
    except Exception:
        return []


def _windowed_stats_from_trades(
    rows: list[dict],
    since_ts: int | None = None,
    limit: int = 20,
) -> dict:
    """Windowed LIVE stats over the most recent closed trades.

    Cleaning mirrors _supervisor_trade_period_reviews (non-finite / outlier
    pnl dropped). ``since_ts`` keeps only trades closed AFTER that unix
    second — the post-tune impact window. Returns {} when nothing qualifies.
    """
    cleaned: list[tuple[int, float]] = []
    for trade in rows or []:
        if not isinstance(trade, dict):
            continue
        try:
            pnl = float(trade.get("_pnl", trade.get("pnl", 0.0)) or 0.0)
        except Exception:
            continue
        if not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        try:
            ts = int(float(trade.get("_ts", trade.get("closedAt", trade.get("ts", 0))) or 0))
        except Exception:
            ts = 0
        if since_ts is not None and (ts <= int(since_ts) or ts <= 0):
            continue
        cleaned.append((ts, pnl))
    cleaned.sort(key=lambda x: x[0])
    recent = cleaned[-max(1, int(limit)):]
    if not recent:
        return {}
    pnls = [p for _, p in recent]
    wins = [p for p in pnls if p >= 0.0]
    losses = [p for p in pnls if p < 0.0]
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = sum(losses) / len(losses) if losses else 0.0
    return {
        "winRatePct": (len(wins) / len(pnls)) * 100.0,
        "avgPnl": sum(pnls) / len(pnls),
        "payoffRatio": (avg_win / abs(avg_loss)) if avg_win > 0 and avg_loss < 0 else 0.0,
        "realizedPnl": sum(pnls),
        "trades": len(pnls),
    }


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


def _tuning_rollback_last(key: str, *, mark: bool = True) -> dict:
    """Get the most recent un-reverted tuning of *key*, optionally mark it.

    ``mark=False`` lets operator rollback preview whether values can safely be
    restored before consuming the history entry. Automatic rollback uses the
    default mark=True: an operator change that superseded a tuned value should
    not be retried indefinitely.
    """
    history = AUTO_TRADE.get("tuningHistory")
    if not isinstance(history, list):
        return {"reverted": False, "reason": "no_history"}
    for entry in reversed(history):
        if entry.get("key") == key and not entry.get("reverted"):
            if mark:
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
        if not isinstance(info, dict) or "old" not in info or info["old"] is None or k not in cfg:
            continue
        # Do not overwrite an operator's later change. A rollback only owns a
        # key while its live value still matches the value this tune wrote.
        if "new" in info and cfg.get(k) != info.get("new"):
            continue
        cfg[k] = info["old"]
        reverted[k] = {"old": info.get("new"), "new": info.get("old")}
    return reverted


# ---------------------------------------------------------------------------
# Metric snapshots
# ---------------------------------------------------------------------------

def _tuning_pre_metrics(limit: int = 20) -> dict:
    """Capture recent-window performance metrics before applying a tune.

    Uses the last ``limit`` closed LIVE trades — the same horizon the trade
    reviews act on. (The old all-time aggregate barely moved after a handful
    of new trades, which made _tuning_should_rollback unable to detect a real
    post-tune regression.)
    """
    stats = _windowed_stats_from_trades(_recent_closed_trades(), limit=limit)
    return {
        "winRatePct": float(stats.get("winRatePct", 0.0) or 0.0),
        "avgPnl": float(stats.get("avgPnl", 0.0) or 0.0),
        "payoffRatio": float(stats.get("payoffRatio", 0.0) or 0.0),
        "realizedPnl": float(stats.get("realizedPnl", 0.0) or 0.0),
        "trades": int(stats.get("trades", 0) or 0),
    }


# ---------------------------------------------------------------------------
# Rollback check (post-tune impact window)
# ---------------------------------------------------------------------------

def _tuning_should_rollback(key: str, post_window_trades: int = 3) -> bool:
    """Return True if the most recent tune of *key* has since worsened metrics.

    Compares the tune's windowed pre-metrics against the trades CLOSED SINCE
    the tune (its actual impact window). The old version compared all-time
    aggregates, which barely move after a handful of trades — rollback
    effectively never fired. Requires at least ``max(3, post_window_trades)``
    post-tune trades as evidence; fewer means not enough data to judge yet.
    """
    history = AUTO_TRADE.get("tuningHistory")
    if not isinstance(history, list):
        return False
    min_post = max(3, int(post_window_trades or 3))
    for entry in reversed(history):
        if entry.get("key") != key or entry.get("reverted"):
            continue
        at = int(entry.get("at", 0) or 0)
        age = time.time() - at
        if age < 60 or age > 3600 * 4:
            continue
        pre = entry.get("preMetrics", {}) or {}
        if not pre:
            continue
        post = _windowed_stats_from_trades(_recent_closed_trades(), since_ts=at)
        if not post or int(post.get("trades", 0) or 0) < min_post:
            continue
        pre_wr = float(pre.get("winRatePct", 0.0) or 0.0)
        post_wr = float(post.get("winRatePct", 0.0) or 0.0)
        pre_pnl = float(pre.get("avgPnl", 0.0) or 0.0)
        post_pnl = float(post.get("avgPnl", 0.0) or 0.0)
        if pre_wr > 0 and post_wr < pre_wr - 15.0 and post_pnl < pre_pnl - 0.05:
            return True
        if pre_pnl > 0 and post_pnl < pre_pnl - 0.08:
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
    *,
    advisory: bool = False,
    allow_when_tuning_disabled: bool = False,
    record_history: bool = True,
    only_if_live_matches_old: bool = False,
) -> dict:
    """Apply config changes, persist snapshot, and optionally record a tune.

    ``allow_when_tuning_disabled`` is reserved for ops-healing (TradingView
    recovery), never risk tuning. ``record_history=False`` is used for an
    already-recorded tune's rollback and for healing. A rollback also sets
    ``only_if_live_matches_old``: it cannot overwrite an operator value that
    changed after the original tune.
    """
    if advisory:
        return record_advisory_suggestion(key, changes, reason)
    if not _supervisor_tuning_enabled() and not allow_when_tuning_disabled:
        return {"applied": False, "reason": "supervisor_autotune_disabled", "key": key}

    # Merge ONLY the tuner's changes onto CURRENT live config. The old code
    # full-replaced config from a stale review cfg, clobbering /bot/config.
    live = AUTO_TRADE.get("config")
    if not isinstance(live, dict):
        live = cfg if isinstance(cfg, dict) else {}
    merged = copy.deepcopy(live)
    effective_changes: dict = {}
    skipped: list[str] = []
    for _k, _v in (changes or {}).items():
        if only_if_live_matches_old and isinstance(_v, dict) and "old" in _v and merged.get(_k) != _v["old"]:
            skipped.append(str(_k))
            continue
        if isinstance(_v, dict) and "new" in _v:
            merged[_k] = _v["new"]
        elif isinstance(_v, dict) and "reverted" in _v:
            merged[_k] = _v["reverted"]
        elif isinstance(_v, dict) and "set" in _v:
            merged[_k] = _v["set"]
        else:
            merged[_k] = _v
        effective_changes[_k] = _v
    if not effective_changes:
        return {
            "applied": False,
            "reason": "operator_override" if skipped else "no_safe_delta",
            "key": key,
            "skipped": skipped,
        }

    now = int(time.time())
    delegations[key] = {
        "at": now,
        "reason": reason,
        "changes": effective_changes,
    }
    state["delegations"] = delegations
    AUTO_TRADE["supervisorAutoTune"] = state
    try:
        from main import _enforce_entry_confidence_floor  # type: ignore[import]
        _enforce_entry_confidence_floor(merged)
    except Exception:
        pass
    AUTO_TRADE["config"] = merged
    if record_history:
        _tuning_history_append(key, effective_changes, _tuning_pre_metrics())
    try:
        from main import _persist_autotrade_snapshot, _autotrade_log  # type: ignore[import]
        _persist_autotrade_snapshot(force=True)  # config change must survive restart (throttle would lose it)
        _autotrade_log(f"Supervisor delegated {key}: {effective_changes}")
    except Exception:
        pass
    return {"applied": True, "changes": effective_changes, "reason": reason, "skipped": skipped}


def rollback_supervisor_config_tune(
    state: dict,
    delegations: dict,
    key: str,
    *,
    reason: str = "rollback_worsened",
    allow_when_tuning_disabled: bool = False,
    advisory: bool = False,
) -> dict:
    """Safely restore latest tune's old values without clobbering operator cfg.

    Finds the un-reverted entry without consuming it, builds inverse changes
    against a copy of current live config, commits only keys still owned by the
    tune, then marks history reverted after successful persistence.
    """
    if advisory:
        return {"applied": False, "rollback": False, "reason": "advisory_mode"}
    rollback = _tuning_rollback_last(key, mark=False)
    if not rollback.get("reverted"):
        return {"applied": False, "rollback": False, "reason": str(rollback.get("reason") or "no_matching_entry")}
    live = AUTO_TRADE.get("config")
    if not isinstance(live, dict):
        return {"applied": False, "rollback": False, "reason": "no_live_config"}
    # Build inverse changes on a detached copy. _apply_rollback_old_values
    # verifies every live value still equals the tune's recorded new value.
    candidate = copy.deepcopy(live)
    reverted = _apply_rollback_old_values(candidate, rollback)
    if not reverted:
        # Operator changed every key this tune owned. Retire the history record
        # so each supervisor review does not attempt the same rollback forever.
        _tuning_rollback_last(key, mark=True)
        return {"applied": False, "rollback": False, "reason": "operator_override"}
    out = _commit_supervisor_config_tune(
        state,
        delegations,
        key,
        live,
        reverted,
        reason,
        allow_when_tuning_disabled=allow_when_tuning_disabled,
        record_history=False,
        only_if_live_matches_old=True,
    )
    if not out.get("applied"):
        return {"applied": False, "rollback": False, "reason": out.get("reason", "rollback_not_applied"), "skipped": out.get("skipped", [])}
    _tuning_rollback_last(key, mark=True)
    out["rollback"] = True
    return out


# ---------------------------------------------------------------------------
# Observability + operator rollback (single source; main.py imports these)
# ---------------------------------------------------------------------------

def tuning_status_snapshot() -> dict:
    """Read-only observability snapshot of the supervisor tuning subsystem.

    Exposes the switches, active domain locks, delegation records (cooldowns),
    tuning history and advisory suggestions in one payload.
    """
    cfg = AUTO_TRADE.get("config") if isinstance(AUTO_TRADE.get("config"), dict) else {}
    state = AUTO_TRADE.get("supervisorAutoTune") if isinstance(AUTO_TRADE.get("supervisorAutoTune"), dict) else {}
    history = AUTO_TRADE.get("tuningHistory") if isinstance(AUTO_TRADE.get("tuningHistory"), list) else []
    suggestions = AUTO_TRADE.get("tuningSuggestions") if isinstance(AUTO_TRADE.get("tuningSuggestions"), list) else []
    delegations = state.get("delegations") if isinstance(state.get("delegations"), dict) else {}
    return {
        "switches": {
            "supervisorAutoTuneEnabled": _supervisor_tuning_enabled(cfg),
            "supervisorHealingEnabled": _supervisor_healing_enabled(cfg),
            "supervisorAdvisoryEnabled": _supervisor_advisory_enabled(cfg),
        },
        "lockStatus": _tuning_mode_lock_status(),
        "delegations": delegations,
        "tuningHistory": history[-20:],
        "tuningSuggestions": suggestions[-20:],
        "ts": int(time.time()),
    }


def manual_rollback_tune(key: str) -> dict:
    """Operator-initiated rollback of latest tune; bypasses tuning kill switch."""
    key = str(key or "").strip()
    if not key:
        return {"ok": False, "reason": "missing_key"}
    state = AUTO_TRADE.get("supervisorAutoTune")
    if not isinstance(state, dict):
        state = {}
    delegations = state.get("delegations")
    if not isinstance(delegations, dict):
        delegations = {}
    out = rollback_supervisor_config_tune(
        state,
        delegations,
        key,
        reason="manual_rollback",
        allow_when_tuning_disabled=True,
    )
    if out.get("applied"):
        try:
            from main import _autotrade_log  # type: ignore[import]
            _autotrade_log(f"[Supervisor] manual rollback {key}: {out.get('changes', {})}")
        except Exception:
            pass
    return {"ok": bool(out.get("applied")), "key": key, **out}
