"""Learning from trades: memory windows, session bias, propose/walk-forward."""

from __future__ import annotations

import asyncio
import json
import math
import time
from functools import wraps
from pathlib import Path

from obsidian_memory import append_trade_memory
from services import app_state
from services import cache_registry as _cache_registry
from services.config_paths import TRADES_LOG_PATH, VAULT_DIR
from trading.per_symbol_storage import per_symbol_lock
from trading.per_symbol_context import PerSymbolContext
from trading.shared_cache_layer import get_shared_cache
from trading.trade_stats import (
    _aggregate_live_trade_stats_from_log,
    _today_entry_performance_guard,
    _append_trade_log,
)
from trading.trade_log import _live_closed_trades_from_log, _live_closed_trades_from_symbol
from trading.risk import (
    AUTOTRADE_EXTRA_COST_BPS,
    AUTOTRADE_TAKER_FEE_BPS_PER_SIDE,
    _autotrade_leverage_bounds,
    _autotrade_leverage_cap,
    fee_edge_min_net_usdt as _fee_edge_min_net_usdt,
)
from trading.state_ops import last_decision_intel as _last_decision_intel
from trading.state_ops import agent_mark as _agent_mark
from services.learning_profiles import _load_single_profile
from trading.symbol_profiles import _symbol_effective_profile

# Module-level shared mutable state
_SESSION_BIAS_CACHE: dict = _cache_registry._SESSION_BIAS_CACHE
# _LIVE_STATS_VERSION is an int that gets incremented in cache_registry;
# reading it via attribute keeps us in sync without a copy.
SCAN_EVENTS_PATH = VAULT_DIR / "scan_events.jsonl"

AUTO_TRADE = app_state.AUTO_TRADE


def _recent_payoff_loss_guard(cfg: dict | None, symbol: str | None = None) -> dict:
    if not isinstance(cfg, dict) or not bool(cfg.get("payoffLossGuardEnabled", True)):
        return {"active": False, "reason": "disabled"}
    window = max(4, int(cfg.get("payoffLossGuardWindowTrades", 8) or 8))
    min_trades = max(4, int(cfg.get("payoffLossGuardMinTrades", 6) or 6))
    max_payoff = max(0.10, float(cfg.get("payoffLossGuardMaxPayoffRatio", 0.75) or 0.75))
    loss_to_win_cap = max(0.60, float(cfg.get("payoffLossGuardLossToWinCap", 1.05) or 1.05))
    min_loss = max(0.05, float(cfg.get("payoffLossGuardMinLossUsdt", 0.22) or 0.22))
    max_loss = max(min_loss, float(cfg.get("payoffLossGuardMaxLossUsdt", 0.9) or 0.9))

    def summarize(rows: list[dict], label: str) -> dict:
        recent = rows[-window:]
        pnls = []
        for trade in recent:
            try:
                pnl = float(trade.get("_pnl", trade.get("pnl", 0.0)) or 0.0)
            except Exception:
                continue
            if math.isfinite(pnl):
                pnls.append(pnl)
        wins = [p for p in pnls if p > 0.0]
        losses = [p for p in pnls if p < 0.0]
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        payoff = avg_win / abs(avg_loss) if avg_win > 0.0 and avg_loss < 0.0 else 0.0
        return {
            "label": label,
            "trades": len(pnls),
            "wins": len(wins),
            "losses": len(losses),
            "avgWin": avg_win,
            "avgLoss": avg_loss,
            "payoffRatio": payoff,
            "pnl": sum(pnls),
        }

    symbol_norm = str(symbol or "").upper().strip()
    symbol_rows = _live_closed_trades_from_log(symbol_norm, "ALL") if symbol_norm else []
    stats = summarize(symbol_rows, f"{symbol_norm}:last_{window}") if len(symbol_rows) >= min_trades else {}
    if not stats or int(stats.get("trades", 0) or 0) < min_trades:
        stats = summarize(_live_closed_trades_from_log(None, "ALL"), f"all:last_{window}")
    trades_n = int(stats.get("trades", 0) or 0)
    losses_n = int(stats.get("losses", 0) or 0)
    avg_win = float(stats.get("avgWin", 0.0) or 0.0)
    avg_loss = float(stats.get("avgLoss", 0.0) or 0.0)
    payoff = float(stats.get("payoffRatio", 0.0) or 0.0)
    if trades_n < min_trades or losses_n < 2 or avg_win <= 0.0 or avg_loss >= 0.0:
        return {"active": False, "reason": "insufficient_window", **stats}
    if payoff <= 0.0 or payoff >= max_payoff:
        return {"active": False, "reason": "payoff_ok", **stats}
    loss_cap = max(min_loss, min(max_loss, avg_win * loss_to_win_cap))
    return {
        "active": True,
        "reason": "weak_payoff_loss_cap",
        "maxLossUsdt": round(float(loss_cap), 6),
        "payoffRatio": round(payoff, 4),
        "avgWin": round(avg_win, 6),
        "avgLoss": round(avg_loss, 6),
        "trades": trades_n,
        "label": stats.get("label", f"last_{window}"),
    }


def _memory_windows_from_trades(trades: list[dict], *, now_ts: int | None = None) -> dict[str, dict]:
    now = int(now_ts or time.time())
    cleaned: list[dict] = []
    for trade in trades or []:
        if not isinstance(trade, dict):
            continue
        try:
            pnl = float(trade.get("_pnl", trade.get("pnl", 0.0)) or 0.0)
            ts = int(float(trade.get("_ts", trade.get("closedAt", trade.get("ts", 0))) or 0))
        except Exception:
            continue
        if ts <= 0 or not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        item = dict(trade)
        item["_pnl"] = pnl
        item["_ts"] = ts
        cleaned.append(item)
    cleaned.sort(key=lambda x: int(x.get("_ts", 0) or 0))

    def build(rows: list[dict], label: str) -> dict:
        pnls = [float(t.get("_pnl", 0.0) or 0.0) for t in rows]
        wins = [p for p in pnls if p >= 0.0]
        losses = [p for p in pnls if p < 0.0]
        gross_win = sum(wins)
        gross_loss = abs(sum(losses))
        avg_win = sum(wins) / max(len(wins), 1) if wins else 0.0
        avg_loss = sum(losses) / max(len(losses), 1) if losses else 0.0
        return {
            "label": label,
            "trades": len(rows),
            "wins": len(wins),
            "losses": len(losses),
            "winRatePct": round((len(wins) / max(len(rows), 1)) * 100.0, 2) if rows else 0.0,
            "pnl": round(sum(pnls), 6),
            "avgPnl": round(sum(pnls) / max(len(rows), 1), 6) if rows else 0.0,
            "avgWin": round(avg_win, 6),
            "avgLoss": round(avg_loss, 6),
            "payoffRatio": round(avg_win / abs(avg_loss), 4) if avg_win > 0.0 and avg_loss < 0.0 else 0.0,
            "profitFactor": round(min(gross_win / gross_loss if gross_loss > 0 else (999.0 if gross_win > 0 else 0.0), 999.0), 4),
        }

    out = {
        "7d": build([t for t in cleaned if now - int(t.get("_ts", 0) or 0) <= 7 * 86400], "7d"),
        "15d": build([t for t in cleaned if now - int(t.get("_ts", 0) or 0) <= 15 * 86400], "15d"),
        "30d": build([t for t in cleaned if now - int(t.get("_ts", 0) or 0) <= 30 * 86400], "30d"),
        "archive": build([t for t in cleaned if now - int(t.get("_ts", 0) or 0) > 30 * 86400], "archive"),
        "all": build(cleaned, "all"),
    }
    return out


def _weighted_recent_memory_score(windows: dict[str, dict]) -> dict:
    weights = [("7d", 0.60), ("15d", 0.25), ("30d", 0.15)]
    total_weight = 0.0
    score = 0.0
    pnl = 0.0
    trades = 0
    for key, weight in weights:
        st = windows.get(key) if isinstance(windows.get(key), dict) else {}
        n = int(st.get("trades", 0) or 0)
        if n <= 0:
            continue
        wr_component = (float(st.get("winRatePct", 50.0) or 50.0) - 50.0) / 50.0
        avg_component = max(-1.0, min(1.0, float(st.get("avgPnl", 0.0) or 0.0) / 0.35))
        score += weight * ((0.65 * wr_component) + (0.35 * avg_component))
        pnl += weight * float(st.get("pnl", 0.0) or 0.0)
        trades += n
        total_weight += weight
    if total_weight <= 0:
        return {"score": 0.0, "pnl": 0.0, "trades": 0, "source": "none"}
    return {
        "score": round(max(-1.0, min(1.0, score / total_weight)), 6),
        "pnl": round(pnl / total_weight, 6),
        "trades": trades,
        "source": "7/15/30d",
    }


def _bkk_hour(ts: float | None = None) -> int:
    """Bangkok (UTC+7) hour, timezone-independent.

    Uses UTC (gmtime) + 7 so it is correct regardless of the machine's local
    timezone. Do NOT use time.localtime() for any hour/day bucketing that feeds
    trading decisions or reports — a machine set to UTC or US time would shift
    every bucket by several hours (e.g. the evening 16-23 BKK guard window).
    """
    if ts is None:
        ts = time.time()
    return (time.gmtime(ts).tm_hour + 7) % 24


def _bkk_day_start_ts(ts: float | None = None) -> int:
    """Epoch seconds of the Bangkok (UTC+7) midnight that starts day of `ts`."""
    if ts is None:
        ts = time.time()
    bkk = time.gmtime(ts)
    # Compute Bangkok wall-clock, then find the UTC epoch of its 00:00 BKK.
    bkk_hour = (bkk.tm_hour + 7) % 24
    bkk_min = bkk.tm_min
    bkk_sec = bkk.tm_sec
    bkk_yday = bkk.tm_yday
    # Seconds already elapsed in the Bangkok day (BKK wall clock).
    elapsed = bkk_hour * 3600 + bkk_min * 60 + bkk_sec
    # UTC epoch of the Bangkok 00:00 of the same UTC calendar day, then adjust
    # for the +7 offset so we land on the correct BKK day boundary.
    utc_day_start = ts - (bkk.tm_hour * 3600 + bkk.tm_min * 60 + bkk.tm_sec)
    # utc_day_start is 00:00 UTC of the UTC day. Bangkok day starts 7h earlier.
    bkk_day_start = utc_day_start - 7 * 3600
    # If we are in the 00:00-07:00 UTC window, the BKK day actually started the
    # previous UTC day -> subtract another 24h.
    if bkk_hour < 7:
        bkk_day_start -= 24 * 3600
    return int(bkk_day_start)


def _entry_session_hours_from_log(max_trades: int = 700) -> dict[int, dict]:
    now = time.time()
    try:
        mtime_trades = TRADES_LOG_PATH.stat().st_mtime if TRADES_LOG_PATH.exists() else -1.0
        mtime_scan = SCAN_EVENTS_PATH.stat().st_mtime if SCAN_EVENTS_PATH.exists() else -1.0
    except Exception:
        mtime_trades = mtime_scan = -1.0
    mtime = (mtime_trades, mtime_scan)
    cached_hours = _SESSION_BIAS_CACHE.get("hours")
    if (
        isinstance(cached_hours, dict)
        and float(_SESSION_BIAS_CACHE.get("builtAt", 0.0) or 0.0) > 0
        and int(_SESSION_BIAS_CACHE.get("liveVersion", -1) or -1) == int(_cache_registry._LIVE_STATS_VERSION)
        and _SESSION_BIAS_CACHE.get("mtime") == mtime
        and now - float(_SESSION_BIAS_CACHE.get("builtAt", 0.0) or 0.0) < 300.0
    ):
        return cached_hours
    if not TRADES_LOG_PATH.exists():
        _SESSION_BIAS_CACHE.update({"builtAt": now, "liveVersion": _cache_registry._LIVE_STATS_VERSION, "mtime": mtime, "hours": {}})
        return {}
    lookback_days = 30
    cutoff_ts = int(now) - (lookback_days * 86400)
    picked: dict[tuple[str, str], list[int]] = {}
    # Scan events live in their own file (split from trades log); LIVE rows
    # stay in trades_log.jsonl. Both are tiny after the split, so a full read
    # is cheap and the 5-min cache keeps it amortized.
    try:
        scan_lines = SCAN_EVENTS_PATH.read_text(encoding="utf-8").splitlines() if SCAN_EVENTS_PATH.exists() else []
    except Exception:
        scan_lines = []
    for line in scan_lines:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if bool(obj.get("picked")):
            sig = str(obj.get("signal", "")).upper()
            sym = str(obj.get("symbol", "")).upper().strip()
            if sig in ("LONG", "SHORT") and sym:
                try:
                    ts = int(float(obj.get("ts", 0) or 0))
                except Exception:
                    ts = 0
                if ts >= cutoff_ts:
                    picked.setdefault((sym, sig), []).append(ts)
    closed: list[dict] = []
    try:
        lines = TRADES_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        _SESSION_BIAS_CACHE.update({"builtAt": now, "liveVersion": _cache_registry._LIVE_STATS_VERSION, "mtime": mtime, "hours": {}})
        return {}
    for line in lines:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if "pnl" not in obj:
            continue
        try:
            pnl = float(obj.get("pnl", 0.0) or 0.0)
            entry = float(obj.get("entry", 0.0) or 0.0)
            exit_px = float(obj.get("exit", 0.0) or 0.0)
            ts = int(float(obj.get("closedAt", obj.get("ts", 0)) or 0))
        except Exception:
            continue
        if ts < cutoff_ts:
            continue
        if ts <= 0 or not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        closed.append({
            "symbol": str(obj.get("symbol", "")).upper().strip(),
            "side": str(obj.get("side", "")).upper(),
            "pnl": pnl,
            "entry": entry,
            "exit": exit_px,
            "ts": ts,
        })
    closed = closed[-max(50, int(max_trades or 700)) :]
    buckets: dict[int, list[dict]] = {}
    for item in closed:
        entry_ts = int(item["ts"])
        scans = picked.get((item["symbol"], item["side"]), [])
        if scans:
            recent = [ts for ts in scans if 0 <= entry_ts - ts <= 10800]
            if recent:
                entry_ts = max(recent)
        hour = _bkk_hour(entry_ts)
        entry = float(item.get("entry", 0.0) or 0.0)
        exit_px = float(item.get("exit", 0.0) or 0.0)
        move = abs((exit_px - entry) / entry * 100.0) if entry > 0 and exit_px > 0 else None
        buckets.setdefault(hour, []).append({"pnl": float(item["pnl"]), "move": move})
    hours: dict[int, dict] = {}
    for hour, items in buckets.items():
        pnls = [float(x["pnl"]) for x in items]
        moves = [float(x["move"]) for x in items if x.get("move") is not None and math.isfinite(float(x["move"]))]
        wins = sum(1 for p in pnls if p >= 0.0)
        hours[int(hour)] = {
            "hour": int(hour),
            "trades": len(items),
            "winRatePct": round((wins / max(len(items), 1)) * 100.0, 2),
            "pnl": round(sum(pnls), 6),
            "avgPnl": round(sum(pnls) / max(len(pnls), 1), 6),
            "avgAbsMovePct": round(sum(moves) / max(len(moves), 1), 4) if moves else 0.0,
        }
    _SESSION_BIAS_CACHE.update({"builtAt": now, "liveVersion": _cache_registry._LIVE_STATS_VERSION, "mtime": mtime, "hours": hours})
    return hours


def _entry_session_bias(cfg: dict, now_ts: int | None = None) -> dict:
    # Use Bangkok time (UTC+7) for session labeling, not server local time.
    # The bot runs on a UTC host, so localtime() == UTC; Boss analyzes sessions
    # in Thailand time. Evening US-overlap (16-23 BKK) is the high-volatility
    # window that bleeds (payoff 0.58, avgLoss -0.30 vs avgWin +0.17).
    # Bangkok time (UTC+7) for session labeling. The bot runs on a UTC host,
    # so server localtime == UTC; Boss analyzes sessions in Thailand time.
    # Evening US-overlap (16-23 BKK) is the high-volatility window that bleeds
    # (payoff 0.58, avgLoss -0.30 vs avgWin +0.17).
    _now = int(now_ts or time.time())
    hour = (time.gmtime(_now).tm_hour + 7) % 24
    neutral = {
        "enabled": bool(cfg.get("sessionBiasEnabled", True)),
        "hour": hour,
        "label": f"{hour:02d}:00-{(hour + 1) % 24:02d}:00",
        "trades": 0,
        "winRatePct": 0.0,
        "pnl": 0.0,
        "avgAbsMovePct": 0.0,
        "confidenceShift": 0.0,
        "scoreShift": 0.0,
        "sizeMult": 1.0,
        "reason": "disabled" if not bool(cfg.get("sessionBiasEnabled", True)) else "insufficient_session_data",
    }
    if not neutral["enabled"]:
        return neutral
    hours = _entry_session_hours_from_log(int(cfg.get("sessionBiasLookbackTrades", 700) or 700))
    st = hours.get(hour)
    if not isinstance(st, dict):
        return neutral
    min_samples = int(cfg.get("sessionBiasMinSamples", 10) or 10)
    trades = int(st.get("trades", 0) or 0)
    neutral.update(st)
    if trades < min_samples:
        neutral["reason"] = "insufficient_session_data"
        return neutral
    wr = float(st.get("winRatePct", 0.0) or 0.0)
    pnl = float(st.get("pnl", 0.0) or 0.0)
    avg_move = float(st.get("avgAbsMovePct", 0.0) or 0.0)
    good_wr = float(cfg.get("sessionBiasGoodWinRatePct", 50.0) or 50.0)
    bad_wr = float(cfg.get("sessionBiasBadWinRatePct", 42.0) or 42.0)
    low_vol = float(cfg.get("sessionBiasLowVolMovePct", 0.45) or 0.45)
    high_vol = float(cfg.get("sessionBiasHighVolMovePct", 0.90) or 0.90)
    max_conf = max(0.0, min(0.20, float(cfg.get("sessionBiasMaxConfShift", 0.05) or 0.05)))
    max_size_pct = max(0.0, min(100.0, float(cfg.get("sessionBiasMaxSizeShiftPct", 25.0) or 25.0)))
    today_guard = _today_entry_performance_guard(cfg)
    if bool(today_guard.get("active")):
        loss_ratio = float(today_guard.get("losses", 0) or 0) / max(float(today_guard.get("trades", 1) or 1), 1.0)
        weakness = max(0.35, min(1.0, loss_ratio + min(0.35, abs(float(today_guard.get("pnl", 0.0) or 0.0)) / 3.0)))
        conf_shift = max_conf * weakness
        size_pct = -max_size_pct * weakness
        neutral.update({
            "todayGuard": today_guard,
            "confidenceShift": round(max(0.0, min(max_conf, conf_shift)), 4),
            "scoreShift": round(max(-0.15, min(0.0, -conf_shift * 0.8)), 4),
            "sizeMult": round(max(0.50, min(1.0, 1.0 + (size_pct / 100.0))), 4),
            "reason": "today_performance_guard",
        })
        return neutral
    conf_shift = 0.0
    size_pct = 0.0
    score_shift = 0.0
    reason = "neutral_session"
    if pnl > 0 and wr >= good_wr:
        strength = min(1.0, ((wr - good_wr) / 20.0) + min(0.5, pnl / max(trades * 0.12, 1e-9)))
        conf_shift = -max_conf * max(0.35, strength)
        size_pct = max_size_pct * max(0.35, strength)
        score_shift = abs(conf_shift) * 0.8
        reason = "boost_good_session"
    elif pnl < 0 and wr <= bad_wr:
        weakness = min(1.0, ((bad_wr - wr) / 20.0) + min(0.5, abs(pnl) / max(trades * 0.12, 1e-9)))
        conf_shift = max_conf * max(0.35, weakness)
        size_pct = -max_size_pct * max(0.35, weakness)
        score_shift = -conf_shift * 0.8
        reason = "reduce_bad_session"
    if avg_move <= low_vol and pnl >= 0:
        conf_shift = max(-max_conf, conf_shift - min(0.01, max_conf * 0.25))
        size_pct = min(max_size_pct, size_pct + min(5.0, max_size_pct * 0.25))
        score_shift += min(0.01, max_conf * 0.25)
        reason = "boost_low_vol_session" if reason == "neutral_session" else reason
    elif avg_move >= high_vol and pnl < 0:
        conf_shift = min(max_conf, conf_shift + min(0.01, max_conf * 0.25))
        size_pct = max(-max_size_pct, size_pct - min(5.0, max_size_pct * 0.25))
        score_shift -= min(0.01, max_conf * 0.25)
        reason = "reduce_high_vol_session" if reason == "neutral_session" else reason
    neutral.update({
        "confidenceShift": round(max(-max_conf, min(max_conf, conf_shift)), 4),
        "scoreShift": round(max(-0.15, min(0.15, score_shift)), 4),
        "sizeMult": round(max(0.50, min(1.75, 1.0 + (size_pct / 100.0))), 4),
        "reason": reason,
    })
    # Explicit evening-volatility override (Bangkok 16-23). Applied LAST so it
    # wins over learned history. This does NOT wait for min_samples; it
    # hard-reduces size and tightens the entry gate during the known-loss
    # US-overlap window regardless of session stats. OWNERSHIP: static risk cap,
    # not a tuner/supervisor write, so it cannot be fought by the other agents.
    evening_lo = int(cfg.get("eveningSessionHourStart", 16) or 16)
    evening_hi = int(cfg.get("eveningSessionHourEnd", 23) or 23)
    if bool(cfg.get("eveningVolatilityGuardEnabled", True)) and (evening_lo <= hour <= evening_hi):
        size_mult = float(cfg.get("eveningSessionSizeMult", 0.70) or 0.70)
        conf_shift = float(cfg.get("eveningSessionConfShift", 0.04) or 0.04)
        neutral["sizeMult"] = round(max(0.40, min(1.0, size_mult)), 4)
        neutral["confidenceShift"] = round(max(0.0, min(0.20, conf_shift)), 4)
        neutral["scoreShift"] = round(-max(0.0, min(0.15, conf_shift * 0.8)), 4)
        neutral["reason"] = "evening_volatility_guard"
        neutral["eveningGuard"] = True
    return neutral


def _market_regime_sizing(cfg: dict, intel: dict | None, trades: list | None = None) -> dict:
    """Dynamic size/threshold modifier based on market regime + result streak.

    Design goals (2026-08-15):
    - Trade is ALWAYS allowed when a symbol passes its normal gates. We never
      hard-block on volatility (that was the old _risk_cooldown_resume_ok
      behavior) — instead we scale exposure.
    - CALM market + a winning streak -> raise cap + multiplier so gains compound.
    - VOLATILE market -> raise the entry bar (confFloor) and cut cap + multiplier
      to contain risk, but keep trading.

    Returns a neutral dict the entry pipeline multiplies into trade sizing and
    adds to the confidence floor. OWNERSHIP: static sizing cap, not
    tuner/supervisor-owned (it must not be fought by the other agents).
    """
    cfg = cfg if isinstance(cfg, dict) else {}
    out = {
        "sizeMult": 1.0,
        "capMult": 1.0,
        "confFloor": 0.0,
        "reason": "regime_neutral",
        "regime": "UNKNOWN",
    }
    if not bool(cfg.get("regimeSizingEnabled", True)):
        out["reason"] = "regime_sizing_disabled"
        return out

    # ── Market regime from live intel ──
    try:
        from trading.regime import detect_market_regime
        regime = detect_market_regime(intel if isinstance(intel, dict) else None)
    except Exception:
        regime = {"name": "UNKNOWN", "confidenceBoost": 0.0, "sizeMultiplier": 1.0, "strictness": "normal"}
    regime_name = str(regime.get("name", "UNKNOWN")).upper()
    out["regime"] = regime_name
    # Clamp the regime's own suggested multiplier to a sane band so it can never
    # blow up or zero-out sizing.
    regime_mult = float(regime.get("sizeMultiplier", 1.0) or 1.0)
    regime_mult = max(0.4, min(1.3, regime_mult))
    out["sizeMult"] *= regime_mult

    # ── Volatile -> stricter bar, smaller cap ──
    if regime_name == "VOLATILE":
        vol_size = float(cfg.get("regimeVolSizeMult", 0.60) or 0.60)
        vol_cap = float(cfg.get("regimeVolCapMult", 0.60) or 0.60)
        vol_floor = float(cfg.get("regimeVolConfFloor", 0.05) or 0.05)
        out["sizeMult"] = round(max(0.30, min(1.0, out["sizeMult"] * vol_size)), 4)
        out["capMult"] = round(max(0.30, min(1.0, vol_cap)), 4)
        out["confFloor"] = round(max(0.0, min(0.15, vol_floor)), 4)
        out["reason"] = "regime_volatile_reduced"
        return out

    # ── Calm / Range + winning streak -> scale UP ──
    if regime_name in ("CALM", "RANGE", "UNKNOWN"):
        try:
            state = _recent_live_result_streak_state(
                trades if isinstance(trades, list) else [],
                int(cfg.get("supervisorSizeLookbackTrades", 12) or 12),
            )
            streak = int(state.get("streak", 0) or 0)
            kind = str(state.get("kind", "") or "")
        except Exception:
            streak, kind = 0, ""
        if kind == "win" and streak >= int(cfg.get("regimeCalmWinStreakMin", 3) or 3):
            calm_size = float(cfg.get("regimeCalmSizeMult", 1.25) or 1.25)
            calm_cap = float(cfg.get("regimeCalmCapMult", 1.20) or 1.20)
            # Diminishing returns: each extra win adds less; cap the boost.
            boost = min(streak - int(cfg.get("regimeCalmWinStreakMin", 3) or 3), 5)
            size_boost = 1.0 + (calm_size - 1.0) * (1.0 + 0.1 * boost)
            cap_boost = 1.0 + (calm_cap - 1.0) * (1.0 + 0.1 * boost)
            out["sizeMult"] = round(min(1.8, max(0.4, out["sizeMult"] * size_boost)), 4)
            out["capMult"] = round(min(1.8, max(0.4, cap_boost)), 4)
            out["reason"] = f"regime_calm_win_streak_{streak}"
    return out


def _early_entry_pullback_reset_ok(side: str, precision: dict | None, cfg: dict | None = None) -> tuple[bool, str]:
    cfg = cfg if isinstance(cfg, dict) else {}
    if not bool(cfg.get("earlyEntryPullbackResetEnabled", True)):
        return True, "disabled"
    px = precision if isinstance(precision, dict) else {}
    side = str(side or "").upper()
    try:
        bb = float(px.get("bbPctB", 0.5) or 0.5)
    except Exception:
        bb = 0.5
    try:
        vwap_dist = abs(float(px.get("vwapDistancePct", 0.0) or 0.0))
    except Exception:
        vwap_dist = 0.0
    late_bb = float(cfg.get("lateEntryMaxBbPctB", 0.95) or 0.95)
    late_vwap = float(cfg.get("lateEntryMaxVwapDistancePct", 0.40) or 0.40)
    long_max_bb = min(late_bb - 0.02, float(cfg.get("earlyEntryMaxBbPctB", 0.82) or 0.82))
    short_min_bb = max(1.0 - late_bb + 0.02, float(cfg.get("earlyEntryMinBbPctBShort", 0.18) or 0.18))
    max_vwap = min(late_vwap, float(cfg.get("earlyEntryMaxVwapDistancePct", 0.24) or 0.24))
    today_guard = _today_entry_performance_guard(cfg)
    if bool(today_guard.get("active")):
        long_max_bb = min(long_max_bb, max(0.50, late_bb - 0.08))
        short_min_bb = max(short_min_bb, min(0.50, 1.0 - late_bb + 0.08))
        max_vwap = min(max_vwap, max(0.05, late_vwap * 0.75))
    if side == "LONG":
        if bb > long_max_bb:
            return False, f"wait_pullback_reset_long bb={bb:.2f}>{long_max_bb:.2f}"
        if vwap_dist > max_vwap:
            return False, f"wait_vwap_reset_long vwapDist={vwap_dist:.3f}%>{max_vwap:.3f}%"
    elif side == "SHORT":
        if bb < short_min_bb:
            return False, f"wait_pullback_reset_short bb={bb:.2f}<{short_min_bb:.2f}"
        if vwap_dist > max_vwap:
            return False, f"wait_vwap_reset_short vwapDist={vwap_dist:.3f}%>{max_vwap:.3f}%"
    return True, "ok"


def _learning_propose_from_trades(symbol: str, trades: list[dict]) -> dict:
    if not trades:
        return {
            "symbol": symbol,
            "trades": 0,
            "winRatePct": 0.0,
            "proposed": {
                "minConfidence": 0.66,
                "hybridMinScore": 0.72,
                "hybridMinEdge": 0.06,
                "adaptiveSizeBoostMaxPct": 20.0,
                "maxOpenPositions": 4,
                "takeProfitPct": 1.2,
                "stopLossPct": 0.75,
            },
            "reasons": ["insufficient_live_data"],
        }
    wins = sum(1 for t in trades if float(t.get("_pnl", 0.0)) >= 0.0)
    losses = len(trades) - wins
    wr = (wins / max(len(trades), 1)) * 100.0
    pnl_sum = round(sum(float(t.get("_pnl", 0.0)) for t in trades), 6)
    avg = pnl_sum / max(len(trades), 1)
    # Stable bounded mapping for live-tuning knobs.
    min_conf = 0.68
    if wr >= 60:
        min_conf = 0.62
    elif wr <= 40:
        min_conf = 0.74
    tp = 1.2
    sl = 0.9
    if avg > 0.12:
        tp, sl = 1.4, 0.95
    elif avg < -0.08:
        tp, sl = 1.0, 0.8
    proposed = {
        "minConfidence": round(min_conf, 3),
        "hybridMinScore": round(0.70 + max(-0.05, min(0.06, (wr - 50.0) / 300.0)), 3),
        "hybridMinEdge": round(0.05 + max(-0.015, min(0.02, (50.0 - wr) / 900.0)), 3),
        "adaptiveSizeBoostMaxPct": round(18.0 + max(-6.0, min(12.0, avg * 40.0)), 2),
        "maxOpenPositions": 5 if wr >= 52 else 4,
        "takeProfitPct": round(tp, 3),
        "stopLossPct": round(sl, 3),
    }
    reasons = [
        f"wins={wins} losses={losses} wr={wr:.2f}%",
        f"netPnl={pnl_sum:.4f} avgPnl={avg:.4f}",
    ]
    return {
        "symbol": symbol,
        "trades": len(trades),
        "winRatePct": round(wr, 2),
        "realizedPnl": pnl_sum,
        "proposed": proposed,
        "reasons": reasons,
    }


def _symbol_risk_tune_from_recent_trades(symbol: str, trades: list[dict], cfg: dict | None = None) -> dict:
    sym = str(symbol or "").upper().strip()
    cfg = cfg if isinstance(cfg, dict) else {}
    cleaned: list[dict] = []
    for trade in trades or []:
        if not isinstance(trade, dict):
            continue
        try:
            pnl = float(trade.get("_pnl", trade.get("pnl", 0.0)) or 0.0)
        except Exception:
            continue
        if not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        ts = 0
        try:
            ts = int(float(trade.get("_ts", trade.get("closedAt", trade.get("ts", 0))) or 0))
        except Exception:
            ts = 0
        item = dict(trade)
        item["_pnl"] = pnl
        item["_ts"] = ts
        cleaned.append(item)
    cleaned.sort(key=lambda row: int(row.get("_ts", 0) or 0))
    if len(cleaned) < 4:
        return {
            "active": False,
            "symbol": sym,
            "trades": len(cleaned),
            "reason": "need_4_closed_trades",
            "sizeMult": 1.0,
            "leverageMult": 1.0,
        }

    window = cleaned[-min(8, len(cleaned)):]
    pnls = [float(t.get("_pnl", 0.0) or 0.0) for t in window]
    wins = [p for p in pnls if p >= 0.0]
    losses = [p for p in pnls if p < 0.0]
    trades_n = len(window)
    win_rate = (len(wins) / max(trades_n, 1)) * 100.0
    pnl_sum = sum(pnls)
    avg_pnl = pnl_sum / max(trades_n, 1)
    avg_win = sum(wins) / max(len(wins), 1) if wins else 0.0
    avg_loss = abs(sum(losses) / max(len(losses), 1)) if losses else 0.0
    profit_factor = (sum(wins) / abs(sum(losses))) if losses and abs(sum(losses)) > 0 else (999.0 if wins else 0.0)
    quick_losses = 0
    for trade in window:
        opened = int(float(trade.get("openedAt", 0) or 0))
        closed = int(float(trade.get("_ts", trade.get("closedAt", 0)) or 0))
        if float(trade.get("_pnl", 0.0) or 0.0) < 0.0 and opened > 0 and closed >= opened and (closed - opened) <= 8 * 60:
            quick_losses += 1

    size_mult = 1.0
    leverage_mult = 1.0
    confidence_shift = 0.0
    reason = "neutral_4_8"
    # ── Target-profit-aware sizing (Boss: aim 0.5-1.0 USDT profit/trade per symbol) ──
    # Compute the avg profit of winning trades and derive a multiplier that
    # scales size so a typical win lands in [targetMin, targetMax] USDT.
    _tgt_min = max(0.1, float(cfg.get("perSymbolTargetProfitMinUsdt", 0.5) or 0.5))
    _tgt_max = max(_tgt_min + 0.1, float(cfg.get("perSymbolTargetProfitMaxUsdt", 1.0) or 1.0))
    _win_pnls = [p for p in pnls if p > 0.0]
    _avg_win = (sum(_win_pnls) / len(_win_pnls)) if _win_pnls else 0.0
    _target_profit_mult = 1.0
    if _avg_win > 0.01:
        # desired avg win = midpoint of target band
        _desired = (_tgt_min + _tgt_max) / 2.0
        _target_profit_mult = max(0.3, min(5.0, _desired / _avg_win))
    else:
        # no wins yet -> modest boost so small winners can reach the floor
        _target_profit_mult = 1.15
    # Risk-based branch ONLY reduces size (drawdown/negative payoff). The
    # target-profit mult (applied later) is the sole source of upside sizing,
    # so a symbol already hitting 0.5-1.0 USDT wins is never over-boosted.
    if pnl_sum < 0.0 or avg_pnl < -0.03 or win_rate < 45.0:
        weakness = 0.0
        weakness += min(0.35, abs(min(pnl_sum, 0.0)) / max(1.2, trades_n * 0.22))
        weakness += min(0.25, max(0.0, 50.0 - win_rate) / 60.0)
        weakness += min(0.20, quick_losses / max(trades_n, 1))
        size_mult = max(0.45, 1.0 - (0.65 * max(0.25, weakness)))
        leverage_mult = max(0.55, 1.0 - (0.55 * max(0.25, weakness)))
        confidence_shift = min(0.06, 0.015 + (0.05 * weakness))
        reason = "reduce_after_symbol_drawdown"
    elif pnl_sum > 0.0 and win_rate >= 58.0 and avg_pnl > 0.03 and profit_factor >= 1.25:
        # Strong edge: keep size_mult at 1.0 (no extra boost); target-profit
        # mult handles upside. Only nudge confidence/leverage slightly.
        size_mult = 1.0
        leverage_mult = min(1.18, 1.0 + (0.30 * 0.20))
        confidence_shift = -min(0.035, 0.008 + (0.025 * 0.20))
        reason = "boost_after_symbol_edge"
    elif win_rate >= 55.0 and pnl_sum <= 0.0:
        size_mult = 0.82
        leverage_mult = 0.82
        confidence_shift = 0.025
        reason = "high_winrate_negative_payoff"

    # Combine risk-based size_mult with target-profit-aware mult (geometric mean
    # so both constraints pull toward the safe zone; target-profit dominates when
    # wins are systematically too small/large for the 0.5-1.0 USDT goal).
    size_mult = float(size_mult) * float(_target_profit_mult)
    size_mult = max(0.30, min(5.0, size_mult))
    leverage_mult = max(0.55, min(1.18, float(leverage_mult)))
    lev_min, lev_max = _autotrade_leverage_bounds(cfg) if cfg else (1, _autotrade_leverage_cap())
    recommended_max = max(lev_min, min(25, int(round(float(lev_max) * float(leverage_mult)))))
    return {
        "active": True,
        "symbol": sym,
        "window": trades_n,
        "trades": len(cleaned),
        "winRatePct": round(win_rate, 2),
        "pnl": round(pnl_sum, 6),
        "avgPnl": round(avg_pnl, 6),
        "avgWin": round(avg_win, 6),
        "avgLoss": round(avg_loss, 6),
        "profitFactor": round(min(profit_factor, 999.0), 4),
        "quickLosses": quick_losses,
        "sizeMult": round(float(size_mult), 4),
        "targetProfitMult": round(float(_target_profit_mult), 4),
        "leverageMult": round(float(leverage_mult), 4),
        "confidenceShift": round(float(confidence_shift), 4),
        "recommendedLeverageMax": int(recommended_max),
        "updatedAt": int(time.time()),
        "reason": reason,
    }


def _walk_forward_from_trades(symbol: str, train_size: int, test_size: int, mode: str) -> dict:
    mode_up = str(mode or "ALL").upper()
    seq = _live_closed_trades_from_log(symbol=symbol, mode=mode_up)
    if not seq:
        return {"ok": False, "symbol": symbol, "mode": mode_up, "detail": "no trades for walk-forward"}
    trn = max(5, int(train_size))
    tst = max(3, int(test_size))
    if len(seq) < (trn + tst):
        return {
            "ok": False,
            "symbol": symbol,
            "mode": mode_up,
            "detail": f"not enough trades ({len(seq)} < {trn+tst})",
            "availableTrades": len(seq),
        }
    train = seq[-(trn + tst):-tst]
    test = seq[-tst:]
    train_wr = (sum(1 for t in train if float(t.get("_pnl", 0.0)) >= 0.0) / max(len(train), 1)) * 100.0
    test_wr = (sum(1 for t in test if float(t.get("_pnl", 0.0)) >= 0.0) / max(len(test), 1)) * 100.0
    train_avg = sum(float(t.get("_pnl", 0.0)) for t in train) / max(len(train), 1)
    test_avg = sum(float(t.get("_pnl", 0.0)) for t in test) / max(len(test), 1)
    rec = {
        "tighten": bool(test_wr < train_wr - 12 or test_avg < -0.04),
        "loosen": bool(test_wr > train_wr + 10 and test_avg > 0.03),
        "trainWinRatePct": round(train_wr, 2),
        "testWinRatePct": round(test_wr, 2),
        "trainAvgPnl": round(train_avg, 6),
        "testAvgPnl": round(test_avg, 6),
    }
    return {
        "ok": True,
        "symbol": symbol,
        "mode": mode_up,
        "trainSize": len(train),
        "testSize": len(test),
        "walkForwardHitRatePct": round(test_wr, 2),
        "recommendation": rec,
    }


# ── Streak analysis functions (extracted from main.py) ─────────────────


def _recent_live_result_streak_state(trades: list[dict], limit: int = 12) -> dict:
    """Detect the most recent consecutive win/loss streak from trade list.

    Pure function — no side effects, no global state reads.
    """
    rows = [t for t in (trades or []) if isinstance(t, dict)]
    if not rows:
        return {"kind": "", "streak": 0, "signature": "", "lastClosedAt": 0, "pnl": 0.0}
    rows = sorted(rows, key=lambda t: int(t.get("_ts", t.get("closedAt", t.get("ts", 0))) or 0))
    streak_rows: list[dict] = []
    kind = ""
    for trade in reversed(rows[-max(3, int(limit)) :]):
        try:
            pnl = float(trade.get("_pnl", trade.get("pnl", 0.0)) or 0.0)
        except Exception:
            continue
        trade_kind = "win" if pnl >= 0.0 else "loss"
        if not kind:
            kind = trade_kind
        if trade_kind != kind:
            break
        item = dict(trade)
        item["_pnl"] = pnl
        streak_rows.append(item)
    if not streak_rows:
        return {"kind": "", "streak": 0, "signature": "", "lastClosedAt": 0, "pnl": 0.0}
    parts = []
    for item in reversed(streak_rows):
        sym = str(item.get("symbol", "") or "").upper()
        side = str(item.get("side", "") or "").upper()
        ts = int(item.get("_ts", item.get("closedAt", item.get("ts", 0))) or 0)
        pnl = float(item.get("_pnl", 0.0) or 0.0)
        parts.append(f"{sym}:{side}:{ts}:{pnl:.8f}")
    return {
        "kind": kind,
        "streak": len(streak_rows),
        "signature": "|".join(parts),
        "lastClosedAt": int(streak_rows[0].get("_ts", streak_rows[0].get("closedAt", streak_rows[0].get("ts", 0))) or 0),
        "pnl": round(sum(float(t.get("_pnl", 0.0) or 0.0) for t in streak_rows), 6),
    }


def _per_symbol_streak_size_mult(symbol: str, cfg: dict | None = None) -> float:
    """Per-symbol streak-based size multiplier.

    Mirrors the streak logic in ``_maybe_tune_size_multiplier_from_streak`` but
    is computed purely from the symbol's own recent trades (via PerSymbolContext)
    and never writes to the global config. Returns 1.0 when there is no symbol
    data or streak, so the global ``supervisorSizeMultiplier`` remains the
    fallback in the entry flow.
    """
    cfg = cfg if isinstance(cfg, dict) else {}
    if not bool(cfg.get("supervisorSizeStreakEnabled", True)):
        return 1.0
    sym = str(symbol or "").upper().strip()
    if not sym:
        return 1.0
    try:
        from trading.per_symbol_context import PerSymbolContext
        from trading.shared_cache_layer import get_shared_cache
        cache = get_shared_cache(VAULT_DIR)
        ctx = PerSymbolContext(sym, cache, cfg)
        trades = ctx.get_trades("LIVE")
        if not trades:
            return 1.0
        state = _recent_live_result_streak_state(trades, int(cfg.get("supervisorSizeLookbackTrades", 12) or 12))
        kind = str(state.get("kind", "") or "")
        streak = int(state.get("streak", 0) or 0)
        win_min = max(2, int(cfg.get("supervisorSizeWinStreakMin", 3) or 3))
        loss_min = max(1, int(cfg.get("supervisorSizeLossStreakMin", 2) or 2))
        min_mult = max(0.1, min(1.0, float(cfg.get("supervisorSizeMinMultiplier", 0.50) or 0.50)))
        if bool(cfg.get("marketScan")) or str(cfg.get("symbol", "")).upper() in {"AUTO", "SCAN"}:
            diversified_floor = max(0.1, min(1.0, float(cfg.get("supervisorSizeDiversifiedMinMultiplier", 0.65) or 0.65)))
            min_mult = max(min_mult, diversified_floor)
        max_mult = max(1.0, min(3.0, float(cfg.get("supervisorSizeMaxMultiplier", 1.35) or 1.35)))
        if max_mult < min_mult:
            max_mult = min_mult
        target = 1.0
        if kind == "win" and streak >= win_min:
            step = max(0.0, float(cfg.get("supervisorSizeWinStepPct", 10.0) or 10.0)) / 100.0
            severity = max(0.0, min(1.0, (streak - win_min) / 10.0))
            scaled_step = step * (1.0 + severity * 2.0)
            target = min(max_mult, 1.0 + scaled_step * (streak - win_min + 1))
        elif kind == "loss" and streak >= loss_min:
            step = max(0.0, float(cfg.get("supervisorSizeLossStepPct", 15.0) or 15.0)) / 100.0
            severity = max(0.0, min(1.0, (streak - loss_min) / 10.0))
            scaled_step = step * (1.0 + severity * 2.0)
            target = max(min_mult, 1.0 - scaled_step * (streak - loss_min + 1))
        return round(float(target), 3)
    except Exception:
        return 1.0


def _recent_live_loss_streak_state(limit: int = 8, symbol: str | None = None) -> dict:
    """Detect the most recent consecutive loss streak from the trade log.

    Reads from the on-disk trade log via ``_live_closed_trades_from_log``.
    """
    sym_filter = str(symbol or "").upper().strip() or None
    seq = _live_closed_trades_from_log(symbol=sym_filter, mode="ALL")
    if not seq:
        return {"symbol": sym_filter, "streak": 0, "signature": "", "lastClosedAt": 0}
    losses = []
    for t in reversed(seq[-max(3, int(limit)) :]):
        pnl = float(t.get("_pnl", 0.0) or 0.0)
        if pnl < 0:
            losses.append(t)
        else:
            break
    if not losses:
        return {"symbol": sym_filter, "streak": 0, "signature": "", "lastClosedAt": int(seq[-1].get("_ts", 0) or 0)}
    parts = []
    for item in reversed(losses):
        sym = str(item.get("symbol", "")).upper()
        side = str(item.get("side", "")).upper()
        ts = int(item.get("_ts", 0) or 0)
        pnl = float(item.get("_pnl", 0.0) or 0.0)
        parts.append(f"{sym}:{side}:{ts}:{pnl:.8f}")
    return {
        "symbol": sym_filter or str(losses[0].get("symbol", "")).upper().strip(),
        "streak": len(losses),
        "signature": "|".join(parts),
        "lastClosedAt": int(losses[0].get("_ts", 0) or 0),
    }


def _recent_live_loss_streak_states_by_symbol(limit: int = 8) -> dict[str, dict]:
    """Per-symbol loss streak detection in a single pass over the trade log."""
    seq = _live_closed_trades_from_log(symbol=None, mode="ALL")
    by_symbol: dict[str, list[dict]] = {}
    for t in seq:
        sym = str(t.get("symbol", "")).upper().strip()
        if not sym:
            continue
        by_symbol.setdefault(sym, []).append(t)
    out: dict[str, dict] = {}
    lookback = max(3, int(limit))
    for sym, rows in by_symbol.items():
        losses = []
        for t in reversed(rows[-lookback:]):
            pnl = float(t.get("_pnl", 0.0) or 0.0)
            if pnl < 0:
                losses.append(t)
            else:
                break
        if not losses:
            continue
        parts = []
        for item in reversed(losses):
            side = str(item.get("side", "")).upper()
            ts = int(item.get("_ts", 0) or 0)
            pnl = float(item.get("_pnl", 0.0) or 0.0)
            parts.append(f"{sym}:{side}:{ts}:{pnl:.8f}")
        out[sym] = {
            "symbol": sym,
            "streak": len(losses),
            "signature": "|".join(parts),
            "lastClosedAt": int(losses[0].get("_ts", 0) or 0),
        }
    return out


def _clamp_float(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


def _estimate_trade_edge_usdt(usdt_amount: float, tp_pct: float, max_slippage_bps: float) -> tuple[float, float, float]:
    # Cost model = taker fee entry+exit + micro cost buffer + half of slippage budget.
    gross_profit = float(usdt_amount) * (float(tp_pct) / 100.0)
    cost_bps = (2.0 * AUTOTRADE_TAKER_FEE_BPS_PER_SIDE) + AUTOTRADE_EXTRA_COST_BPS + max(0.0, float(max_slippage_bps) * 0.5)
    est_cost = float(usdt_amount) * (cost_bps / 10000.0)
    net_profit = gross_profit - est_cost
    return gross_profit, est_cost, net_profit


def _last_decision_entry_metrics(symbol: str | None = None) -> dict:
    intel = _last_decision_intel(symbol)
    if not isinstance(intel, dict):
        return {}
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    return {
        "confidence": float(intel.get("confidence", 0.0) or 0.0),
        "score": float(intel.get("score", intel.get("weightedScore", 0.0)) or 0.0),
        "spreadBps": float(ex.get("spreadBps", 0.0) or 0.0),
        "momentumPct": float(ex.get("momentumPct", 0.0) or 0.0),
    }


def _trade_reward_components(trade: dict, cfg: dict) -> dict:
    pnl = float((trade or {}).get("pnl", 0.0) or 0.0)
    reason = str((trade or {}).get("reason", "") or "").upper()
    side = str((trade or {}).get("side", "") or "").upper()
    sign = 1.0 if side == "LONG" else (-1.0 if side == "SHORT" else 0.0)
    last_metrics = _last_decision_entry_metrics()
    conf = float((trade or {}).get("entryConfidence", last_metrics.get("confidence", 0.0)) or 0.0)
    min_conf = float(cfg.get("minConfidence", 0.65) or 0.65)
    spread_bps = float((trade or {}).get("entrySpreadBps", last_metrics.get("spreadBps", 0.0)) or 0.0)
    max_spread = max(1.0, float(cfg.get("maxSpreadBps", 20.0) or 20.0))
    pattern_bias = float((trade or {}).get("patternBias", 0.0) or 0.0)
    pattern_score = float((trade or {}).get("patternScore", 0.0) or 0.0)
    entry_delta = _clamp_float((conf - min_conf) / 0.25, -1.0, 1.0) * float(cfg.get("learningRewardEntryTiming", 0.25) or 0.25)
    spread_delta = _clamp_float((max_spread - spread_bps) / max_spread, -1.0, 1.0) * 0.12
    pattern_alignment = _clamp_float(sign * pattern_bias, -1.0, 1.0)
    pattern_delta = _clamp_float(pattern_alignment * 0.18 + (pattern_score / 500.0), -0.28, 0.28)
    tp_pct = float(cfg.get("takeProfitPct", 1.2) or 1.2)
    sl_pct = max(0.01, float(cfg.get("stopLossPct", 0.8) or 0.8))
    min_rr = float(cfg.get("minRiskRewardRatio", 1.35) or 1.35)
    rr = tp_pct / sl_pct
    rr_delta = 0.12 if rr >= min_rr else -0.22
    edge_delta = 0.0
    try:
        qty = abs(float((trade or {}).get("qty", 0.0) or 0.0))
        entry = abs(float((trade or {}).get("entry", 0.0) or 0.0))
        notional = qty * entry
        if notional > 0:
            _gross, cost, net = _estimate_trade_edge_usdt(notional, tp_pct, float(cfg.get("maxSlippageBps", 20.0) or 20.0))
            min_net = _fee_edge_min_net_usdt(cfg, cost, notional)
            edge_delta = 0.10 if net > min_net else -0.16
    except Exception:
        edge_delta = 0.0
    drawdown_delta = 0.0
    try:
        max_adverse = abs(float((trade or {}).get("maxAdversePct", 0.0) or 0.0))
        if max_adverse > 0:
            drawdown_delta = -_clamp_float(max_adverse / max(sl_pct, 0.01), 0.0, 1.0) * 0.22
    except Exception:
        drawdown_delta = 0.0
    time_delta = 0.0
    opened = int(float((trade or {}).get("openedAt", 0) or 0))
    closed = int(float((trade or {}).get("closedAt", 0) or 0))
    if opened > 0 and closed >= opened:
        minutes = (closed - opened) / 60.0
        if pnl > 0 and minutes <= float(cfg.get("learningFastWinMinutes", 45) or 45):
            time_delta += 0.12
        elif pnl < 0 and minutes <= 8:
            time_delta -= 0.14
        elif pnl < 0 and minutes >= 90:
            time_delta -= 0.08
    guard_delta = 0.0
    if pnl > 0 and ("TP" in reason or "TARGET_MAX" in reason):
        guard_delta += float(cfg.get("learningRewardTpHitBase", 0.2) or 0.2)
        guard_delta += float(cfg.get("learningRewardTpScalePerUsdt", 0.35) or 0.35) * min(abs(pnl), 3.0) / 3.0
    if pnl > 0 and ("PROFIT" in reason or "GUARD" in reason or "TRAIL" in reason):
        guard_delta += float(cfg.get("learningRewardHoldWinner", 0.2) or 0.2)
    if pnl > 0 and "FLIP" in reason:
        guard_delta += float(cfg.get("learningRewardFlipGood", 0.1) or 0.1)
    if pnl < 0 and ("SL" in reason or "STOP" in reason):
        guard_delta -= float(cfg.get("learningPenaltyEarlySl", 0.35) or 0.35)
    if pnl < 0 and ("SIGNAL_WAIT" in reason or "LOW_CONF" in reason):
        guard_delta -= float(cfg.get("learningPenaltyMemoryMiss", 0.15) or 0.15)
    if pnl < 0 and "LIVE_CUT_LOSING_SIDE" in reason:
        guard_delta -= 0.20
    total = entry_delta + spread_delta + pattern_delta + rr_delta + edge_delta + drawdown_delta + time_delta + guard_delta
    cap = max(0.1, float(cfg.get("learningBehaviorDeltaCap", 2.5) or 2.5))
    return {
        "entryQuality": round(entry_delta, 6),
        "spreadQuality": round(spread_delta, 6),
        "patternAlignment": round(pattern_delta, 6),
        "riskReward": round(rr_delta, 6),
        "feeEdge": round(edge_delta, 6),
        "drawdown": round(drawdown_delta, 6),
        "timeToProfit": round(time_delta, 6),
        "guardDiscipline": round(guard_delta, 6),
        "total": round(_clamp_float(total, -cap, cap), 6),
    }


def _mark_trade_learning_agents(symbol: str, trade: dict, mode: str):
    try:
        pnl = float((trade or {}).get("pnl", 0.0) or 0.0)
        reason = str((trade or {}).get("reason", "") or "")
        side = str((trade or {}).get("side", "") or "")
        label = f"{str(mode).upper()} {str(symbol).upper()} {side} pnl={pnl:.4f}"
        # Only memory_agent actually persists the outcome on this path.
        # reflection_agent / backtest_agent do real work elsewhere (loss-streak tune,
        # walk-forward endpoint) and are marked there — marking them here would be
        # misleading telemetry.
        _agent_mark("memory_agent", "done", "trade outcome stored", label)
    except Exception as exc:
        print(f"[Trade Log] ERROR writing {TRADES_LOG_PATH}: {exc}")


def _serialize_per_symbol_update(fn):
    """Decorator for per-symbol updates."""
    @wraps(fn)
    def wrapped(symbol: str, *args, **kwargs):
        with per_symbol_lock(VAULT_DIR, symbol):
            return fn(symbol, *args, **kwargs)
    return wrapped


def _auto_update_symbol_profile(symbol: str, cfg: dict | None = None) -> dict:
    """Derive a conservative self-learning symbol profile from learning stats.

    This is intentionally cautious: it only nudges policy when the symbol has
    enough evidence in the learning store, and it keeps the resulting profile
    bounded so the bot can learn continuously without overfitting.
    """
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {}
    cfg = cfg if isinstance(cfg, dict) else {}
    pr = _load_single_profile(sym)
    if not isinstance(pr, dict):
        return {}

    windows = pr.get("memoryWindows") if isinstance(pr.get("memoryWindows"), dict) else {}
    w7 = windows.get("7d") if isinstance(windows.get("7d"), dict) else {}
    w14 = windows.get("14d") if isinstance(windows.get("14d"), dict) else {}
    w30 = windows.get("30d") if isinstance(windows.get("30d"), dict) else {}
    recent_score = float((pr.get("weightedRecentScore") or {}).get("score", 0.0) or 0.0)
    reward_score = float(pr.get("rewardScore", 0.0) or 0.0)
    reward_delta = float(pr.get("rewardDelta", 0.0) or 0.0)
    behavior_delta = float(pr.get("rewardBehaviorDelta", 0.0) or 0.0)

    # Choose the longest available window with enough samples, then blend the
    # shorter windows for recency. This makes the bot react to changes but not
    # too quickly.
    def _pick_window() -> dict:
        for win in (w7, w14, w30):
            if int(win.get("trades", 0) or 0) >= 6:
                return win
        return w30 or w14 or w7 or {}

    win = _pick_window()
    trades = int(win.get("trades", 0) or 0)
    if trades < 6:
        return {}

    wr = float(win.get("winRatePct", 0.0) or 0.0)
    pnl = float(win.get("pnl", 0.0) or 0.0)
    # Compute profit factor from the window's win/loss breakdown so we don't
    # depend on a missing profitFactor field on the learning profile.
    gross_win = float(win.get("grossWin", 0.0) or 0.0)
    gross_loss = float(win.get("grossLoss", 0.0) or 0.0)
    if gross_loss > 0:
        pf = gross_win / gross_loss
    elif gross_win > 0:
        pf = 999.0
    else:
        pf = float(pr.get("profitFactor", 0.0) or 0.0)
    pf = min(pf, 999.0)
    ql = int(pr.get("quickLosses", 0) or 0)
    obs = int(pr.get("observations", 0) or 0)
    picks = int(pr.get("pickedCount", 0) or 0)
    pick_rate = (picks / max(obs, 1)) * 100.0 if obs > 0 else 0.0

    # Start from the current effective group profile so user overrides and the
    # 3-tier fallback stay intact.
    base = _symbol_effective_profile(sym, cfg)
    out = dict(base)

    # Confidence floor: good symbols can relax a bit, weak symbols get stricter.
    if wr >= 60.0 and pnl > 0.0:
        out["min_conf_floor"] = max(0.45, float(base.get("min_conf_floor", 0.5)) - 0.03)
    elif wr <= 45.0 or pnl < -0.5:
        out["min_conf_floor"] = min(0.85, float(base.get("min_conf_floor", 0.5)) + 0.04)

    # Scan bias: if the symbol repeatedly performs better on one side,
    # nudge the scan bias slightly. Keep it bounded so the symbol cannot
    # become permanently one-sided from a few trades.
    long_bias = float(base.get("scan_long_bias", 0.5))
    if wr >= 58.0 and pnl > 0.0:
        long_bias += 0.02
    elif wr < 48.0 and pnl < 0.0:
        long_bias -= 0.02
    out["scan_long_bias"] = round(max(0.42, min(0.58, long_bias)), 4)

    # Chase speed: strong symbols can enter faster; noisy symbols should wait.
    chase = str(base.get("scan_chase_speed", "normal"))
    if ql >= 2 or wr < 48.0:
        chase = "slow"
    elif wr >= 60.0 and pnl > 0.0:
        chase = "fast"
    out["scan_chase_speed"] = chase

    # TP/SL / lock: make the learning react more strongly to expectancy,
    # not just win rate. Positive expectancy should widen profit capture a bit;
    # negative expectancy should cut risk faster.
    tp_mult = float(base.get("tpsl_mult", 1.0))
    sl_mult = float(base.get("sl_mult", 1.0))
    lock_mult = float(base.get("lock_trigger_mult", 1.0))
    cap_mult = float(base.get("max_trade_notional_mult", 1.0))
    if wr >= 58.0 and pnl > 0.0 and pf >= 1.10:
        edge = min(0.30, (wr - 55.0) / 100.0 + min(0.10, pnl / 20.0) + min(0.10, (pf - 1.0) / 5.0))
        tp_mult = min(2.0, tp_mult + 0.05 + edge)
        lock_mult = min(2.0, lock_mult + 0.04 + edge * 0.5)
        cap_mult = min(1.45, cap_mult + 0.04 + edge * 0.35)
    elif wr <= 47.0 or pnl < -0.4 or pf < 0.95:
        weakness = min(0.35, max(0.0, 50.0 - wr) / 80.0 + abs(min(pnl, 0.0)) / 8.0 + max(0.0, 1.0 - pf) * 0.12)
        tp_mult = max(0.60, tp_mult - (0.06 + weakness))
        sl_mult = min(1.65, sl_mult + (0.06 + weakness * 0.9))
        lock_mult = max(0.40, lock_mult - (0.06 + weakness * 0.5))
        cap_mult = max(0.30, cap_mult - (0.06 + weakness * 0.6))
    out["tpsl_mult"] = round(tp_mult, 4)
    out["sl_mult"] = round(sl_mult, 4)
    out["lock_trigger_mult"] = round(lock_mult, 4)
    out["max_trade_notional_mult"] = round(cap_mult, 4)

    # Reward / tuning bias: store bounded hint values so existing adaptive
    # functions can incorporate them, but never let them dominate the signal.
    out["rewardDelta"] = round(max(-0.12, min(0.12, reward_delta + (0.02 if pnl > 0 else -0.02))), 4)
    out["rewardBehaviorDelta"] = round(max(-0.12, min(0.12, behavior_delta + (0.015 if wr >= 58.0 else -0.015))), 4)
    out["rewardScore"] = round(max(-5.0, min(5.0, reward_score + (0.05 if pnl > 0 else -0.05))), 4)

    # Learning meta fields for the dashboard.
    out["learnedAt"] = int(time.time())
    out["learnedFromWindow"] = str(win.get("window", "30d") or "30d")
    out["learnedTrades"] = trades
    out["learnedWinRatePct"] = round(wr, 2)
    out["learnedPnl"] = round(pnl, 6)
    out["learnedProfitFactor"] = round(pf, 4)
    out["learnedPickRatePct"] = round(pick_rate, 2)
    out["learnedRecentScore"] = round(recent_score, 4)
    out["learnedQuickLosses"] = ql

    # Position sizing: use symbolRiskTune if available and active,
    # otherwise fall back to the group base multiplier. Learned behavior
    # shifts size based on win rate and pnl trend.
    pos_mult = float(base.get("positionSizeMult") or base.get("position_size_mult", 1.0) or 1.0)
    sm = 1.0
    lv_mult = 1.0
    lv_max_override = 0
    tune = pr.get("symbolRiskTune") if isinstance(pr.get("symbolRiskTune"), dict) else {}
    if bool(tune.get("active")) and int(tune.get("window", 0) or 0) >= 4:
        sm = float(tune.get("sizeMult", 1.0) or 1.0)
        lv_mult = float(tune.get("leverageMult", 1.0) or 1.0)
        lv_max_override = int(tune.get("recommendedLeverageMax", 0) or 0)
    elif wr >= 60.0 and pnl > 0.0:
        sm = min(1.30, pos_mult + 0.05)
    elif wr <= 45.0 or pnl < -0.5:
        sm = max(0.40, pos_mult - 0.08)
    # Loss-streak guard: a bleeding symbol never gets an oversized learned
    # size multiplier (ESPUSDT learned positionSizeMult 1.3 while losing).
    if pnl < 0.0:
        sm = min(sm, 1.10)
    out["positionSizeMult"] = round(sm, 4)
    out["position_size_mult"] = round(sm, 4)

    # Entry offset: use group default (group handles the behavioral class).
    out["entry_offset_bps"] = float(base.get("entry_offset_bps", 0.0) or 0.0)

    # Learned position size and leverage metadata for the dashboard.
    learned_pos_mult = round(sm, 4)
    learned_lev_mult = round(lv_mult, 4)
    learned_lev_max = int(lv_max_override) if lv_max_override else 0

    # Keep symbol profile lean: persist only override-like fields.
    return {
        "group": out.get("group"),
        "minConfidence": round(float(out.get("min_conf_floor", 0.5)), 4),
        "rewardDelta": float(out.get("rewardDelta", 0.0) or 0.0),
        "rewardBehaviorDelta": float(out.get("rewardBehaviorDelta", 0.0) or 0.0),
        "rewardScore": float(out.get("rewardScore", 0.0) or 0.0),
        "longBias": round(float(out.get("scan_long_bias", 0.5)), 4),
        "chaseSpeed": str(out.get("scan_chase_speed", "normal")),
        "tpPct": round(float(cfg.get("takeProfitPct", 1.8) or 1.8) * float(out.get("tpsl_mult", 1.0)), 4),
        "slPct": round(max(float(cfg.get("supervisorStopLossFloor", 0.80) or 0.80),
                           float(cfg.get("stopLossPct", 0.9) or 0.9) * float(out.get("sl_mult", 1.0))), 4),
        "profitLockTriggerUsdt": round(float(cfg.get("profitLockTriggerUsdt", 0.35) or 0.35) * float(out.get("lock_trigger_mult", 1.0)), 4),
        "notionalCapUsdt": round(float(cfg.get("tradeNotionalCapUsdt", 80.0) or 80.0) * float(out.get("max_trade_notional_mult", 1.0)), 4),
        "cooldownMinutes": int(max(0, round(float(cfg.get("symbolCooldownMins", 15) or 15)))) if wr >= 60.0 else int(max(0, round(float(cfg.get("symbolCooldownMins", 15) or 15) * 1.2))),
        # Position sizing learned fields
        "positionSizeMult": learned_pos_mult,
        "leverageMult": learned_lev_mult,
        "leverageMax": learned_lev_max,
        "entryOffsetBps": round(float(out.get("entry_offset_bps", 0.0)), 2),
        # Meta
        "learnedAt": int(out.get("learnedAt", time.time())),
        "learnedFromWindow": out.get("learnedFromWindow", "30d"),
        "learnedTrades": int(out.get("learnedTrades", trades)),
        "learnedWinRatePct": float(out.get("learnedWinRatePct", wr)),
        "learnedPnl": float(out.get("learnedPnl", pnl)),
        "learnedProfitFactor": float(out.get("learnedProfitFactor", pf)),
        "learnedPickRatePct": float(out.get("learnedPickRatePct", pick_rate)),
        "learnedRecentScore": float(out.get("learnedRecentScore", recent_score)),
        "learnedQuickLosses": int(out.get("learnedQuickLosses", ql)),
    }


@_serialize_per_symbol_update
def _record_learning_trade(symbol: str, trade: dict, mode: str):
    sym = str(symbol or "").upper()
    if not sym:
        return
    print(f"[Record Trade] _record_learning_trade called: symbol={sym}, mode={mode}, pnl={trade.get('pnl')}, reason={trade.get('reason')}")
    cfg = AUTO_TRADE.get("config") if isinstance(AUTO_TRADE.get("config"), dict) else {}
    ctx = PerSymbolContext(sym, get_shared_cache(VAULT_DIR), cfg)
    pr = ctx.profile
    pnl = float(trade.get("pnl", 0.0) or 0.0)
    pr["wins"] = int(pr.get("wins", 0)) + (1 if pnl >= 0 else 0)
    pr["losses"] = int(pr.get("losses", 0)) + (1 if pnl < 0 else 0)
    pr["realizedPnl"] = round(float(pr.get("realizedPnl", 0.0)) + pnl, 6)
    pr["trades"] = int(pr.get("trades", 0)) + 1
    pr["lastMode"] = str(mode).upper()
    pr["lastTradeSide"] = str(trade.get("side", ""))
    pr["lastTradeReason"] = str(trade.get("reason", ""))
    pr["sumPnl"] = round(float(pr.get("sumPnl", 0.0)) + pnl, 6)
    pr["avgPnlPerTrade"] = round(float(pr["sumPnl"]) / max(int(pr["trades"]), 1), 6)
    pr["maxWinPnl"] = max(float(pr.get("maxWinPnl", pnl)), pnl)
    pr["maxLossPnl"] = min(float(pr.get("maxLossPnl", pnl)), pnl)
    reward_enabled = bool(cfg.get("learningRewardEnabled", True))
    behavior_enabled = bool(cfg.get("learningBehaviorRewardEnabled", True))
    # Live guardian feedback loop: gently adapt per-symbol TP/SL/lock thresholds.
    if str(mode).upper() == "LIVE":
        ts = int(trade.get("closedAt", trade.get("ts", time.time())) or time.time())
        tloc = time.localtime(ts)
        day_key = (tloc.tm_year, tloc.tm_mon, tloc.tm_mday)
        if day_key != app_state._DAILY_PNL_DATE_KEY:
            app_state.DAILY_REALIZED_PNL = 0.0
            app_state._DAILY_PNL_DATE_KEY = day_key
        app_state.DAILY_REALIZED_PNL += pnl
        print(f"[Record Trade] Processing LIVE trade for {sym}: pnl={pnl}, reason={trade.get('reason')}, dailyPnl={app_state.DAILY_REALIZED_PNL}")
        guard_tp = float(pr.get("tpPct", float(cfg.get("takeProfitPct", 1.8) or 1.8)) or float(cfg.get("takeProfitPct", 1.8) or 1.8))
        guard_sl = float(pr.get("slPct", float(cfg.get("stopLossPct", 0.9) or 0.9)) or float(cfg.get("stopLossPct", 0.9) or 0.9))
        guard_lock = float(pr.get("profitLockTriggerUsdt", float(cfg.get("profitLockTriggerUsdt", 0.35) or 0.35)) or float(cfg.get("profitLockTriggerUsdt", 0.35) or 0.35))
        reason_up = str(trade.get("reason", "") or "").upper()
        win_like = pnl >= 0.0 or reason_up in {"LOCAL_TP_HIT", "TARGET_MAX", "PEAK_CAPTURE", "LIVE_CLOSE"}
        loss_like = pnl < 0.0 or reason_up in {"LOCAL_SL_HIT", "BREAKEVEN_GUARD", "PAYOFF_LOSS_GUARD", "STRONG_REVERSAL_EXIT", "LIVE_CUT_LOSING_SIDE", "RETRACE_BUDGET", "WEAK_SIGNAL"}
        if win_like:
            guard_tp *= 1.012
            guard_sl *= 1.006
            guard_lock *= 1.010
        elif loss_like:
            guard_tp *= 0.988
            guard_sl *= 0.986
            guard_lock *= 0.965
        guard_tp = max(0.35, min(6.0, guard_tp))
        # V11: per-trade SL ratchet must respect the global SL floor — the
        # old hardcoded 0.20 floor let losing streaks tighten per-symbol SL
        # to noise level (0.28-0.47% → instant whipsaw SL hits).
        _sl_floor = float(cfg.get("supervisorStopLossFloor", 0.80) or 0.80)
        guard_sl = max(_sl_floor, min(3.5, guard_sl))
        guard_lock = max(0.08, min(1.50, guard_lock))
        pr["tpPct"] = round(guard_tp, 4)
        pr["slPct"] = round(guard_sl, 4)
        pr["profitLockTriggerUsdt"] = round(guard_lock, 4)
        # Guardian autotune: adapt holdTrailPct and holdMinConfidence per symbol.
        guard_trail = float(pr.get("holdTrailPct", float(cfg.get("holdTrailPct", 0.25) or 0.25)) or 0.25)
        guard_conf = float(pr.get("holdMinConfidence", float(cfg.get("holdMinConfidence", 0.72) or 0.72)) or 0.72)
        if win_like:
            guard_trail *= 1.01
            guard_conf *= 0.99
        elif loss_like:
            guard_trail *= 0.98
            guard_conf *= 1.01
        pr["holdTrailPct"] = round(max(0.10, min(0.50, guard_trail)), 4)
        pr["holdMinConfidence"] = round(max(0.60, min(0.85, guard_conf)), 4)
        pr["tpslFeedback"] = {
            "updatedAt": int(time.time()),
            "mode": "LIVE",
            "reason": reason_up,
            "pnl": round(float(pnl), 6),
        }


    reward_cap = max(1.0, float(cfg.get("learningRewardCap", 50.0) or 50.0))
    reward_decay = float(cfg.get("learningRewardDecay", 0.985) or 0.985)
    reward_decay = min(1.0, max(0.9, reward_decay))
    reward_win = max(0.0, float(cfg.get("learningRewardWin", 1.0) or 1.0))
    penalty_loss = max(0.0, float(cfg.get("learningPenaltyLoss", 0.8) or 0.8))
    pnl_clip = max(1.0, float(cfg.get("learningPnlClipAbsUsdt", 25.0) or 25.0))
    reason = str(trade.get("reason", "") or "").upper()
    base_delta = 0.0
    if reward_enabled:
        # pnl_scale keeps updates smooth while still rewarding larger clean wins.
        pnl_scale = min(1.0, abs(float(pnl)) / pnl_clip)
        if pnl >= 0:
            base_delta = reward_win * (0.75 + 0.25 * pnl_scale)
        else:
            base_delta = -penalty_loss * (0.75 + 0.25 * pnl_scale)
    behavior_delta = 0.0
    if behavior_enabled:
        reward_components = _trade_reward_components(trade, cfg)
        behavior_delta += float(reward_components.get("total", 0.0) or 0.0)
        cap = max(0.1, float(cfg.get("learningBehaviorDeltaCap", 2.5) or 2.5))
        behavior_delta = max(-cap, min(cap, behavior_delta))
    else:
        reward_components = {}
    prev_score = float(pr.get("rewardScore", 0.0) or 0.0)
    new_score = (prev_score * reward_decay) + base_delta + behavior_delta
    pr["rewardScore"] = round(max(-reward_cap, min(reward_cap, new_score)), 6)
    pr["rewardDelta"] = round(base_delta, 6)
    pr["rewardBehaviorDelta"] = round(behavior_delta, 6)
    pr["rewardComponents"] = reward_components
    if pnl >= 0:
        pr["rewardWinStreak"] = int(pr.get("rewardWinStreak", 0)) + 1
        pr["rewardLossStreak"] = 0
    else:
        pr["rewardLossStreak"] = int(pr.get("rewardLossStreak", 0)) + 1
        pr["rewardWinStreak"] = 0
    pr["updatedAt"] = int(time.time())
    try:
        recent_rows = _live_closed_trades_from_symbol(sym, mode="ALL", vault_dir=VAULT_DIR)
        current_trade = {"_pnl": pnl, "_ts": int(time.time()), **trade, "symbol": sym}
        recent_with_current = [*recent_rows, current_trade]
        windows = _memory_windows_from_trades(recent_with_current)
        pr["memoryWindows"] = windows
        pr["weightedRecentScore"] = _weighted_recent_memory_score(windows)
        pr["symbolRiskTune"] = _symbol_risk_tune_from_recent_trades(sym, recent_with_current, cfg)
    except Exception:
        pass
    # Auto-calibrate the symbol profile from the updated learning state.
    try:
        auto_profile = _auto_update_symbol_profile(sym, cfg)
        if isinstance(auto_profile, dict) and auto_profile:
            try:
                existing = ctx._sym_profile if isinstance(ctx._sym_profile, dict) else {}
                merged = dict(existing)
                _AUTOTUNER_MANAGED = {"tpPct", "slPct", "holdTrailPct", "holdMinConfidence"}
                _has_autotune = bool(existing.get("autotuneLastAt"))
                for k, v in auto_profile.items():
                    if _has_autotune and k in _AUTOTUNER_MANAGED:
                        continue
                    merged[k] = v
                merged["updatedAt"] = int(time.time())
                merged["lastAutoLearnedAt"] = int(time.time())
                merged["lastAutoLearnedFrom"] = "close_trade"
                merged["learnedTrades"] = int(pr.get("trades", 0) or 0)
                merged["learnedWinRatePct"] = float(pr.get("memoryWindows", {}).get("7d", {}).get("winRatePct", 0.0) or 0.0) if isinstance(pr.get("memoryWindows"), dict) else 0.0
                merged["learnedPnl"] = float(pr.get("realizedPnl", 0.0) or 0.0)
                ctx._sym_profile = merged
                ctx._dirty_sym_profile = True
            except Exception:
                pass  # per-symbol write failed, skip fallback to global
    except Exception:
        pass

    # Write to per-symbol trade log and vault
    try:
        trade_log_entry = {"ts": int(time.time()), **trade, "symbol": sym}
        if str(mode).upper() == "LIVE" and "pnl" in trade:
            trade_log_entry["mode"] = "LIVE"
            trade_log_entry.setdefault("closedAt", int(trade.get("closedAt") or trade.get("ts") or time.time()))
            trade_log_entry.setdefault("reason", str(trade.get("reason", "LIVE_CLOSE") or "LIVE_CLOSE"))
        else:
            trade_log_entry["mode"] = str(mode).upper()
        
        # Attach TV data from disk (tv_signal.json) — snapshot at time of close (both LIVE and PAPER)
        try:
            _tv_path = VAULT_DIR / "symbols" / sym / "tv_signal.json"
            if _tv_path.exists():
                _tv = json.loads(_tv_path.read_text(encoding="utf-8"))
                if isinstance(_tv, dict) and _tv:
                    trade_log_entry["tvSignal"] = _tv.get("signal", "")
                    trade_log_entry["tvConfidence"] = _tv.get("confidence", 0.0)
                    trade_log_entry["tvStrength"] = _tv.get("strength", 0.0)
                    trade_log_entry["tvAge"] = int(time.time()) - int(_tv.get("ts", 0) or 0)
                    print(f"[Record Trade] {sym}: TV data from disk - signal={_tv.get('signal')}, conf={_tv.get('confidence')}, age={trade_log_entry['tvAge']}s")
                else:
                    print(f"[Record Trade] {sym}: TV file exists but data is invalid")
            else:
                print(f"[Record Trade] {sym}: TV signal file not found at {_tv_path}")
        except Exception as e:
            print(f"[Record Trade] {sym}: Error reading TV signal file: {e}")
        
        # Attach params_at_entry and guardian_stats from per-symbol storage
        # (lock may already be popped from in-memory dict, so read from disk)
        try:
            from trading.per_symbol_storage import PerSymbolStorage
            _ps = PerSymbolStorage(VAULT_DIR, sym)
            _gl = _ps.load_guardian_lock()
            if isinstance(_gl, dict) and _gl:
                _snap = _gl.get("entrySnapshot", {})
                if isinstance(_snap, dict):
                    if _snap.get("params_at_entry"):
                        trade_log_entry["params_at_entry"] = _snap["params_at_entry"]
                        print(f"[Record Trade] {sym}: Found params_at_entry: {_snap['params_at_entry']}")
                    else:
                        print(f"[Record Trade] {sym}: No params_at_entry in entrySnapshot")
                    if _snap.get("tvSignal"):
                        trade_log_entry["tvAtEntry"] = _snap["tvSignal"]
                        print(f"[Record Trade] {sym}: TV at entry - signal={_snap['tvSignal']}")
                    else:
                        print(f"[Record Trade] {sym}: No tvSignal in entrySnapshot")
                    if _snap.get("tvConfidence") is not None:
                        trade_log_entry["tvAtEntryConfidence"] = _snap["tvConfidence"]
                        print(f"[Record Trade] {sym}: TV at entry confidence={_snap['tvConfidence']}")
                _gs = _gl.get("guardianStats", {})
                if isinstance(_gs, dict) and _gs:
                    trade_log_entry["guardian_stats"] = {
                        "peakProfitUsdt": _gs.get("peakProfitUsdt", 0.0),
                        "holdWinnerActivated": _gs.get("holdWinnerActivated", 0),
                        "tpExtensionCount": _gs.get("tpExtensionCount", 0),
                        "notionalUsdt": _gs.get("notionalUsdt", 0.0),
                        "timeInPositionSec": int(time.time()) - int(_gs.get("openedAt", time.time())),
                    }
                    print(f"[Record Trade] {sym}: Found guardian_stats: {trade_log_entry['guardian_stats']}")
                else:
                    print(f"[Record Trade] {sym}: No guardian_stats in guardian lock")
            else:
                print(f"[Record Trade] {sym}: No guardian lock found")
        except Exception as e:
            print(f"[Record Trade] {sym}: Error reading guardian lock: {e}")
        
        _append_trade_log(trade_log_entry)
        ctx.record_trade(trade_log_entry)
        append_trade_memory(VAULT_DIR, trade_log_entry, mode)
    except Exception as e:
        # NEVER swallow silently — a write failure here (e.g. cloud-sync lock
        # on E:) must be visible so we know trades are being lost.
        print(f"[Record Trade] {sym}: FAILED to record trade: {e}")
    ctx._dirty_profile = True
    ctx.commit()
    ctx.update_symbol_note(trade)
    _mark_trade_learning_agents(sym, trade, mode)
    # Per-symbol autotuner: trigger evaluation after LIVE trades
    # Delay 1s to avoid file I/O race with ctx.commit() (symbol_profile.json)
    if str(mode).upper() == "LIVE":
        def _deferred_autotune():
            import time as _t; _t.sleep(1.0)
            try:
                from trading.symbol_autotuner import record_trade_outcome
                print(f"[Autotune] Triggering record_trade_outcome for {sym}")
                record_trade_outcome(sym, trade)
                print(f"[Autotune] Completed record_trade_outcome for {sym}")
            except Exception as e:
                print(f"[Autotune] Error in record_trade_outcome for {sym}: {e}")
        try:
            import threading as _th
            print(f"[Autotune] Starting deferred autotune thread for {sym}")
            _th.Thread(target=_deferred_autotune, daemon=True).start()
        except Exception as e:
            print(f"[Autotune] Failed to start autotune thread for {sym}: {e}")


async def _record_learning_trade_async(symbol: str, trade: dict, mode: str):
    """Async wrapper for _record_learning_trade — delegates to a thread to avoid blocking the event loop."""
    return await asyncio.to_thread(_record_learning_trade, symbol, trade, mode)
