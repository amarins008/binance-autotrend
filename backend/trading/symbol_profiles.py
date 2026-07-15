"""Per-symbol 3-tier profile helpers."""

from __future__ import annotations

import json
import time

from fastapi import Body

from services.cache_registry import _SYMBOL_SAMPLE_COUNT_CACHE
from services.config_paths import TRADES_LOG_PATH, VAULT_DIR

SYMBOL_PROFILES_PATH = VAULT_DIR / "symbol_profiles.json"


# _main() proxy removed, direct imports used instead


from services.learning_profiles import _ensure_vault, _load_single_profile


from trading.trade_log import _live_closed_trades_from_log

def _rolling_symbol_perf(*args, **kwargs):
    from main import _rolling_symbol_perf as rolling
    return rolling(*args, **kwargs)


def _symbol_volatility_score(symbol: str, intel: dict | None = None) -> dict:
    """Compute a per-symbol volatility profile used to scale TP/SL/cap/profit-lock.

    Each symbol gets a tier ("low" / "med" / "high") plus a multiplier (0.65 - 1.4)
    that scales the global TP%, SL%, notional cap and profit-lock trigger. The
    profile is computed from in-flight intel (atrPct, momentum, spread, vwap
    distance, BB position) so volatile symbols get wider TP/SL (giving trades
    room to breathe) and tighter caps (smaller position size for riskier coins),
    while stable majors keep the original tight targets.
    """
    sym = str(symbol or "").upper().strip()
    intel = intel if isinstance(intel, dict) else {}
    precision = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    execution = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}

    atr_pct = max(0.0, float(precision.get("atrPct", 0.0) or 0.0))
    momentum = abs(float(execution.get("momentumPct", 0.0) or 0.0))
    spread_bps = max(0.0, float(execution.get("spreadBps", 0.0) or 0.0))
    vwap_dist = abs(float(precision.get("vwapDistancePct", 0.0) or 0.0))
    bb = float(precision.get("bbPctB", 0.5) or 0.5)
    long_score = float(precision.get("longScore", 0.0) or 0.0)
    short_score = float(precision.get("shortScore", 0.0) or 0.0)
    directional = abs(long_score - short_score) + momentum

    # Volatility score 0.0 (rock solid) → 1.0 (chaotic). Each component is
    # weighted by how much it actually predicts tradable noise.
    score = 0.0
    score += min(0.30, atr_pct / 1.5)            # ATR is the dominant driver
    score += min(0.25, momentum / 1.0)            # absolute momentum
    score += min(0.15, spread_bps / 80.0)         # execution cost / slippage risk
    score += min(0.15, vwap_dist / 0.5)           # stretched-from-fair risk
    score += min(0.10, abs(bb - 0.5) / 0.5)       # BB position extremes
    score += min(0.10, directional / 6.0)         # one-sided directional energy
    score = max(0.0, min(1.0, score))

    if score < 0.30:
        tier = "low"
        tp_mult = 0.85       # tighter TP for stable majors (BTC/ETH)
        sl_mult = 0.85       # tighter SL — trend holds, less whipsaw
        cap_mult = 1.20      # larger cap — high confidence in size
        lock_mult = 0.85     # lock profits earlier (smaller moves)
    elif score < 0.55:
        tier = "med"
        tp_mult = 1.00       # use the configured TP as-is
        sl_mult = 1.00
        cap_mult = 1.00
        lock_mult = 1.00
    else:
        tier = "high"
        tp_mult = 1.30       # wider TP — need room for swings
        sl_mult = 1.20       # slightly wider SL — avoid premature stops
        cap_mult = 0.85      # was 0.65 — raised to avoid orders falling below Binance minimum notional
        lock_mult = 1.40     # lock profits later (give trades room)

    return {
        "symbol": sym,
        "tier": tier,
        "score": round(score, 4),
        "atrPct": round(atr_pct, 4),
        "momentumPct": round(momentum, 4),
        "spreadBps": round(spread_bps, 4),
        "tpMult": round(tp_mult, 4),
        "slMult": round(sl_mult, 4),
        "capMult": round(cap_mult, 4),
        "lockMult": round(lock_mult, 4),
    }


# --- 3-tier policy: System (risk) -> Group (behavioral class) -> Symbol (per-coin) ---

# Group definitions: each group captures a behavioral class so coins that behave
# similarly can share a single profile instead of every coin needing its own.
# The fallback chain is: symbol profile -> group profile -> system policy.
SYMBOL_GROUP_DEFS: dict[str, dict] = {
    # Coins that trend hard, low mean-reversion. Mostly BTC/ETH and the
    # majors. Hold winners longer, tighter SL because trend holds.
    "trend-friendly": {
        "tpsl_mult": 1.10,
        "sl_mult": 0.90,
        "lock_trigger_mult": 0.95,
        "min_conf_floor": 0.50,
        "max_trade_notional_mult": 1.15,
        "scan_long_bias": 0.55,
        "scan_chase_speed": "fast",
        "position_size_mult": 1.00,   # base size (1.0 = full budget)
        "entry_offset_bps": 0,        # bps offset from mid to enter (0 = vwap)
    },
    "mean-reversion-friendly": {
        "tpsl_mult": 0.85,
        "sl_mult": 0.85,
        "lock_trigger_mult": 0.75,
        "min_conf_floor": 0.62,
        "max_trade_notional_mult": 0.95,
        "scan_long_bias": 0.50,
        "scan_chase_speed": "slow",
        "position_size_mult": 0.85,   # smaller size — mean-reversion is lower reward/risk
        "entry_offset_bps": 15,       # enter slightly above mid to avoid chasing a reversal
    },
    "high-volatility": {
        "tpsl_mult": 1.35,
        "sl_mult": 1.20,
        "lock_trigger_mult": 1.50,
        "min_conf_floor": 0.65,
        "max_trade_notional_mult": 0.60,
        "scan_long_bias": 0.50,
        "scan_chase_speed": "slow",
        "position_size_mult": 0.65,   # smaller size — high vol tokens are riskier
        "entry_offset_bps": 10,       # enter slightly above mid for momentum confirm
    },
    "low-liquidity-noisy": {
        "tpsl_mult": 0.95,
        "sl_mult": 0.80,
        "lock_trigger_mult": 0.50,
        "min_conf_floor": 0.70,
        "max_trade_notional_mult": 0.40,
        "scan_long_bias": 0.50,
        "scan_chase_speed": "slow",
        "position_size_mult": 0.40,   # tiny — slippage kills these
        "entry_offset_bps": 25,       # enter well above mid to avoid fake pumps
    },
}

# Hard-coded symbol → group mapping for well-known assets. New symbols
# default to "trend-friendly" until the bot learns better.
SYMBOL_GROUP_DEFAULT: dict[str, str] = {
    # trend-friendly: BTC, ETH and the other majors with deep liquidity
    "BTCUSDT": "trend-friendly", "ETHUSDT": "trend-friendly", "SOLUSDT": "trend-friendly",
    "BNBUSDT": "trend-friendly", "XRPUSDT": "trend-friendly", "ADAUSDT": "trend-friendly",
    "AVAXUSDT": "trend-friendly", "LINKUSDT": "trend-friendly", "DOTUSDT": "trend-friendly",
    "MATICUSDT": "trend-friendly", "NEARUSDT": "trend-friendly", "ATOMUSDT": "trend-friendly",
    "LTCUSDT": "trend-friendly", "ETCUSDT": "trend-friendly",
    # mean-reversion-friendly: range-bound or stable pairings
    "XLMUSDT": "mean-reversion-friendly", "XRPUSDT": "mean-reversion-friendly",
    "EOSUSDT": "mean-reversion-friendly", "BCHUSDT": "mean-reversion-friendly",
    # high-volatility: leveraged tokens, momentum names
    "DOGEUSDT": "high-volatility", "SHIBUSDT": "high-volatility", "PEPEUSDT": "high-volatility",
    "WIFUSDT": "high-volatility", "FLOKIUSDT": "high-volatility",
    "AAVEUSDT": "high-volatility", "INJUSDT": "high-volatility",
    "RNDRUSDT": "high-volatility", "FETUSDT": "high-volatility",
    "SUIUSDT": "high-volatility", "APTUSDT": "high-volatility", "ARBUSDT": "high-volatility",
    "OPUSDT": "high-volatility", "ARKMUSDT": "high-volatility",
    "PUMPUSDT": "high-volatility", "PENGUUSDT": "high-volatility",
    # low-liquidity-noisy: small caps, meme names
    "SPXUSDT": "low-liquidity-noisy", "STGUSDT": "low-liquidity-noisy",
    "HOMEUSDT": "low-liquidity-noisy", "ZROUSDT": "low-liquidity-noisy",
}


def _symbol_group(symbol: str) -> str:
    """Return the behavioral group for ``symbol``.

    Order of resolution: explicit default map -> learned override
    (from symbolProfiles) -> "trend-friendly" (safe default). The group
    drives the per-symbol policy unless a richer symbol profile has been
    promoted for that coin.
    """
    sym = str(symbol or "").upper().strip()
    if not sym:
        return "trend-friendly"
    # Explicit hard-coded list wins first (curated, well-known assets).
    if sym in SYMBOL_GROUP_DEFAULT:
        return SYMBOL_GROUP_DEFAULT[sym]
    # Otherwise consult the learned profile (in case an autotune decided
    # this coin belongs to a different group based on observed behavior).
    profiles = _load_symbol_profiles() or {}
    pr = profiles.get(sym) if isinstance(profiles, dict) else None
    if isinstance(pr, dict):
        grp = pr.get("group")
        if isinstance(grp, str) and grp in SYMBOL_GROUP_DEFS:
            return grp
    return "trend-friendly"


# Sample-count guard: number of closed trades needed before a symbol profile
# is allowed to override the group profile. Below this, we always fall back
# to the group (insufficient evidence for a per-coin policy).
SYMBOL_PROFILE_MIN_TRADES = 8


def _symbol_sample_count(symbol: str) -> int:
    """Number of closed LIVE trades recorded for ``symbol`` (capped to last 30d).
    Cached in memory with a short TTL to avoid repeated file reads during scans.
    """
    global _SYMBOL_SAMPLE_COUNT_CACHE
    sym = str(symbol or "").upper().strip()
    if not sym:
        return 0
    now = time.time()
    cached = _SYMBOL_SAMPLE_COUNT_CACHE.get(sym)
    if cached:
        cached_ts, cached_count = cached
        if now - cached_ts < 5.0:
            return cached_count
    try:
        rows = _live_closed_trades_from_log(symbol=sym, mode="LIVE")
    except Exception:
        return 0
    if not isinstance(rows, list):
        return 0
    count = len(rows)
    _SYMBOL_SAMPLE_COUNT_CACHE[sym] = (now, count)
    # Limit cache size
    if len(_SYMBOL_SAMPLE_COUNT_CACHE) > 100:
        oldest = min(_SYMBOL_SAMPLE_COUNT_CACHE, key=lambda k: _SYMBOL_SAMPLE_COUNT_CACHE[k][0])
        del _SYMBOL_SAMPLE_COUNT_CACHE[oldest]
    return count


def _load_symbol_profiles() -> dict:
    """Load per-symbol override profiles from disk.

    The file stores a flat dict of ``symbol -> profile``. Each profile can
    contain any subset of these fields (missing → fall through to group):
        group: str (one of SYMBOL_GROUP_DEFS)
        minConfidence: float
        rewardDelta: float          # additive bias to reward score
        longBias: float             # 0.0-1.0, bias toward LONG in scan
        chaseSpeed: str             # "fast" / "slow" / "normal"
        tpPct: float                # explicit TP%, overrides volatility tier
        slPct: float                # explicit SL%
        profitLockTriggerUsdt: float
        cooldownMinutes: int        # explicit cooldown after close
        note: str                   # user note
    """
    try:
        _ensure_vault()
        if SYMBOL_PROFILES_PATH.exists():
            data = json.loads(SYMBOL_PROFILES_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def _save_symbol_profiles(profiles: dict) -> None:
    """Persist the per-symbol profiles dict to disk."""
    try:
        _ensure_vault()
        SYMBOL_PROFILES_PATH.write_text(
            json.dumps(profiles, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        print(f"[Trade Log] ERROR writing {TRADES_LOG_PATH}: {exc}")


# Fields the dashboard "Symbol Profile (3-tier)" table reads from each
# openLivePositions row. Only these are copied from the profile; everything
# else stays in the profile store. Keep this list in sync with the frontend
# renderSymbolProfileSummary() in backend/dashboard/index.html.
_SYMBOL_PROFILE_DASHBOARD_FIELDS = (
    "group",
    "minConfidence",
    "tpPct",
    "slPct",
    "profitLockTriggerUsdt",
    "learnedProfitFactor",
    "learnedRecentScore",
    "learnedTrades",
    "learnedWinRatePct",
    "learnedPnl",
    "notionalCapUsdt",
    "leverageMax",
    "positionSizeMult",
    "chaseSpeed",
    "longBias",
)


def _attach_symbol_profile(position: dict, profiles: dict | None) -> None:
    """Merge per-symbol profile fields onto an openLivePositions row in place.

    Without this, the dashboard falls back to the empty `bot.symbolProfiles`
    list only when openLivePositions is empty — so any open live position
    shows `—` for PF/Conf/TP/SL/Lock/Rec even though the profile is saved.
    The merge makes the dashboard read work whether or not a position is open.

    Even when no saved profile exists for the symbol we still populate
    `group` from `_symbol_group()` (default map → learned override → safe
    default) so the dashboard does not render `—` for newly-traded symbols
    that have not yet earned enough samples for a learned profile.
    """
    if not isinstance(position, dict):
        return
    sym = str(position.get("symbol", "") or "").upper().strip()
    if not sym:
        return
    prof = profiles.get(sym) if isinstance(profiles, dict) else None
    if isinstance(prof, dict):
        for key in _SYMBOL_PROFILE_DASHBOARD_FIELDS:
            if key in prof and position.get(key) is None:
                position[key] = prof.get(key)
    # Always resolve `group` so the dashboard shows the tier even without a
    # saved profile (PUMPUSDT, ARBUSDT, etc. that have only the default map).
    if position.get("group") is None:
        try:
            position["group"] = _symbol_group(sym)
        except Exception:
            pass
    # Mark source so the frontend can tell "position with profile" vs raw.
    if position.get("source") is None:
        position["source"] = "live+profile" if isinstance(prof, dict) else "live"


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
    pos_mult = float(base.get("position_size_mult", 1.0))
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
        "slPct": round(float(cfg.get("stopLossPct", 0.9) or 0.9) * float(out.get("sl_mult", 1.0)), 4),
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


def _symbol_effective_profile(symbol: str, cfg: dict | None = None) -> dict:
    """Return the effective per-symbol profile (merged with group defaults).

    Returns a flat dict with every policy field resolved. Callers do not need
    to know which tier the value came from. If the symbol has fewer than
    SYMBOL_PROFILE_MIN_TRADES closed trades, the symbol-level overrides
    (minConfidence, tpPct, slPct, etc.) are ignored and only the group
    defaults apply — prevents overfitting on a handful of trades.
    """
    sym = str(symbol or "").upper().strip()
    cfg = cfg if isinstance(cfg, dict) else {}
    group_name = _symbol_group(sym)
    group_def = SYMBOL_GROUP_DEFS.get(group_name, SYMBOL_GROUP_DEFS["trend-friendly"])

    profile = dict(group_def)
    profile["symbol"] = sym
    profile["group"] = group_name
    profile["source"] = "group"

    n_trades = _symbol_sample_count(sym)
    if n_trades >= SYMBOL_PROFILE_MIN_TRADES:
        learned = _load_symbol_profiles() or {}
        sym_profile = learned.get(sym) if isinstance(learned.get(sym), dict) else None
        if isinstance(sym_profile, dict):
            for k, v in sym_profile.items():
                if k in ("group", "source"):
                    continue
                profile[k] = v
            profile["source"] = "symbol+group"
    profile["sampleTrades"] = n_trades
    profile["minPromotedTrades"] = SYMBOL_PROFILE_MIN_TRADES
    return profile


# --- API: read/write per-symbol profiles (used by the dashboard) ---

def hermes_get_symbol_profile(symbol: str):
    """Return the resolved 3-tier policy for ``symbol`` (System -> Group -> Symbol).

    The ``resolved`` object always reflects what the bot would actually use for
    a fresh entry. ``groupDefaults`` shows the group's raw defaults and
    ``overrides`` shows the user-applied per-symbol overrides (only applied
    when sample count >= minPromotedTrades).
    """
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {"ok": False, "error": "missing symbol"}
    profile = _symbol_effective_profile(sym)
    raw = _load_symbol_profiles() or {}
    sym_raw = raw.get(sym) if isinstance(raw.get(sym), dict) else {}
    perf = _rolling_symbol_perf(sym, 30) or {}
    return {
        "ok": True,
        "symbol": sym,
        "sampleTrades": int(profile.get("sampleTrades", 0)),
        "minPromotedTrades": int(SYMBOL_PROFILE_MIN_TRADES),
        "resolved": {
            "group": str(profile.get("group", "trend-friendly")),
            "source": str(profile.get("source", "group")),
            # Group-level multipliers
            "tpslMult": round(float(profile.get("tpsl_mult", 1.0)), 4),
            "slMult": round(float(profile.get("sl_mult", 1.0)), 4),
            "lockTriggerMult": round(float(profile.get("lock_trigger_mult", 1.0)), 4),
            "capMult": round(float(profile.get("max_trade_notional_mult", 1.0)), 4),
            # Per-symbol resolved values (post all tiers)
            "minConfidence": round(float(profile.get("minConfidence", profile.get("min_conf_floor", 0.5))), 4),
            "scanLongBias": round(float(profile.get("scan_long_bias", 0.5)), 4),
            "scanChaseSpeed": str(profile.get("scan_chase_speed", "normal")),
            "positionSizeMult": round(float(profile.get("position_size_mult", 1.0) or 1.0), 4),
            "entryOffsetBps": round(float(profile.get("entry_offset_bps", 0.0) or 0.0), 2),
            "tpPct": round(float(profile.get("tpPct") or 1.8), 4),
            "slPct": round(float(profile.get("slPct") or 0.9), 4),
            "notionalCapUsdt": round(float(profile.get("notionalCapUsdt") or 80.0), 4),
            "profitLockTriggerUsdt": round(float(profile.get("profitLockTriggerUsdt") or 0.35), 4),
            "cooldownMinutes": int(profile.get("cooldownMinutes") or 15),
            "leverageMult": round(float(profile.get("leverageMult", 1.0) or 1.0), 4),
            "leverageMax": int(profile.get("leverageMax") or 0),
        },
        "overrides": sym_raw,
        "perf": {
            "trades": int(perf.get("trades", 0)),
            "winRatePct": float(perf.get("winRatePct", 0.0)),
            "pnl": float(perf.get("pnl", 0.0)),
            "memoryWindow": str(perf.get("memoryWindow", "none")),
        },
    }


def hermes_set_symbol_profile(payload: dict = Body(default_factory=dict)):
    sym = str(payload.get("symbol", "") or "").upper().strip()
    if not sym:
        return {"ok": False, "error": "missing symbol"}
    updates = {k: v for k, v in payload.items() if k != "symbol"}
    allowed_keys = {
        "group", "minConfidence", "rewardDelta", "longBias", "chaseSpeed",
        "tpPct", "slPct", "profitLockTriggerUsdt", "cooldownMinutes",
        "positionSizeMult", "leverageMult", "entryOffsetBps", "note",
    }
    clean = {k: v for k, v in updates.items() if k in allowed_keys}
    if not clean:
        return {"ok": False, "error": "no supported keys", "allowed": sorted(allowed_keys)}
    if "group" in clean and clean["group"] not in SYMBOL_GROUP_DEFS:
        return {"ok": False, "error": f"unknown group: {clean['group']}",
                "allowed_groups": list(SYMBOL_GROUP_DEFS.keys())}
    profiles = _load_symbol_profiles() or {}
    existing = profiles.get(sym) if isinstance(profiles.get(sym), dict) else {}
    merged = dict(existing)
    merged.update(clean)
    profiles[sym] = merged
    _save_symbol_profiles(profiles)
    return {"ok": True, "symbol": sym, "saved": clean,
            "effective": _symbol_effective_profile(sym)}


def hermes_list_symbol_profiles():
    """Return per-symbol effective profiles for all symbols seen in the
    recent trade log, sorted by sample count descending.
    """
    try:
        rows = _live_closed_trades_from_log(symbol=None, mode="LIVE")
    except Exception:
        rows = []
    symbols = set()
    for r in rows or []:
        if isinstance(r, dict):
            s = str(r.get("symbol", "") or "").upper().strip()
            if s:
                symbols.add(s)
    out = []
    for s in symbols:
        p = _symbol_effective_profile(s)
        perf = _rolling_symbol_perf(s, 30) or {}
        out.append({
            "symbol": s,
            "group": p.get("group", "trend-friendly"),
            "source": p.get("source", "group"),
            "sampleTrades": p.get("sampleTrades", 0),
            "tpslMult": p.get("tpsl_mult", 1.0),
            "scanLongBias": p.get("scan_long_bias", 0.5),
            "winRatePct": perf.get("winRatePct", 0.0),
            "pnl": perf.get("pnl", 0.0),
        })
    out.sort(key=lambda r: (r["source"] != "symbol+group", -r["sampleTrades"], r["symbol"]))
    return {"ok": True, "count": len(out), "items": out}

# Backward-compatible aliases used across main/tests
load_symbol_profiles = _load_symbol_profiles
save_symbol_profiles = _save_symbol_profiles
attach_symbol_profile = _attach_symbol_profile
auto_update_symbol_profile = _auto_update_symbol_profile
symbol_effective_profile = _symbol_effective_profile
symbol_group = _symbol_group

