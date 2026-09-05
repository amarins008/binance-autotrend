"""Risk desk: R:R, ATR stops, fee edge, TP/SL targets."""

import os

from services import app_state

AUTOTRADE_TAKER_FEE_BPS_PER_SIDE = float(os.getenv("AUTOTRADE_TAKER_FEE_BPS_PER_SIDE", "4.0"))
AUTOTRADE_MIN_NET_PROFIT_USDT = float(os.getenv("AUTOTRADE_MIN_NET_PROFIT_USDT", "0.05"))
AUTOTRADE_EXTRA_COST_BPS = float(os.getenv("AUTOTRADE_EXTRA_COST_BPS", "2.0"))


def fee_edge_min_net_usdt(
    cfg: dict | None,
    est_cost_usdt: float = 0.0,
    notional_usdt: float = 0.0,
) -> float:
    cfg = cfg if isinstance(cfg, dict) else {}
    configured = float(cfg.get("feeMinNetProfitUSDT", AUTOTRADE_MIN_NET_PROFIT_USDT) or AUTOTRADE_MIN_NET_PROFIT_USDT)
    multiple = max(1.0, float(cfg.get("feeMinEdgeVsCostMultiple", 3.0) or 3.0))
    taker_roundtrip = max(0.0, float(notional_usdt or 0.0)) * ((2.0 * AUTOTRADE_TAKER_FEE_BPS_PER_SIDE) / 10000.0)
    return round(max(configured, float(est_cost_usdt or 0.0) * multiple, taker_roundtrip * multiple), 6)


def estimate_trade_edge_usdt(
    usdt_amount: float,
    tp_pct: float,
    max_slippage_bps: float,
    *,
    taker_fee_bps_per_side: float = 4.0,
    extra_cost_bps: float = 2.0,
) -> tuple[float, float, float]:
    gross_profit = float(usdt_amount) * (float(tp_pct) / 100.0)
    cost_bps = (2.0 * taker_fee_bps_per_side) + extra_cost_bps + max(0.0, float(max_slippage_bps) * 0.5)
    est_cost = float(usdt_amount) * (cost_bps / 10000.0)
    return gross_profit, est_cost, gross_profit - est_cost


def effective_min_net_profit_usdt(
    cfg: dict,
    realized_vol_pct: float | None = None,
    *,
    default_min_net: float = 0.05,
    taker_fee_bps: float = 4.0,
    extra_cost_bps: float = 2.0,
) -> float:
    # When feeMinNetProfitUSDT is set to 0 or below, skip fee gate entirely.
    _raw_fee_floor = cfg.get("feeMinNetProfitUSDT")
    if _raw_fee_floor is not None and float(_raw_fee_floor) <= 0.0:
        return 0.0
    base_floor = float(_raw_fee_floor if _raw_fee_floor is not None else default_min_net)
    mul = max(1.0, float(cfg.get("feeMinEdgeVsCostMultiple", 1.2) or 1.2))
    usdt = float(cfg.get("usdtAmount", 0.0) or 0.0)
    _, est_cost, _ = estimate_trade_edge_usdt(
        usdt,
        float(cfg.get("takeProfitPct", 1.8) or 1.8),
        float(cfg.get("maxSlippageBps", 28.0) or 28.0),
        taker_fee_bps_per_side=taker_fee_bps,
        extra_cost_bps=extra_cost_bps,
    )
    req = max(base_floor, est_cost * mul)
    if bool(cfg.get("feeAdaptiveNetEnabled", True)) and realized_vol_pct is not None:
        try:
            rv = max(0.0, float(realized_vol_pct))
            v_low = max(0.001, float(cfg.get("feeAdaptiveVolLowPct", 0.08) or 0.08))
            v_high = max(v_low + 0.001, float(cfg.get("feeAdaptiveVolHighPct", 0.35) or 0.35))
            f_min = max(0.6, min(1.0, float(cfg.get("feeAdaptiveMinFactor", 0.8) or 0.8)))
            f_max = max(1.0, min(1.6, float(cfg.get("feeAdaptiveMaxFactor", 1.15) or 1.15)))
            norm = max(0.0, min(1.0, (rv - v_low) / max(v_high - v_low, 1e-9)))
            factor = f_max - (f_max - f_min) * norm
            req *= factor
        except Exception:
            pass
    return round(max(0.0, req), 6)


def passes_min_risk_reward(tp_pct: float, sl_pct: float, min_rr: float) -> bool:
    sl = max(1e-6, float(sl_pct))
    return (float(tp_pct) / sl) >= max(1.0, float(min_rr))


def calc_tp_sl_prices(side: str, entry_mark: float, tp_pct: float, sl_pct: float) -> tuple[float, float]:
    if side == "LONG":
        return (
            entry_mark * (1 + tp_pct / 100),
            entry_mark * (1 - sl_pct / 100),
        )
    return (
        entry_mark * (1 - tp_pct / 100),
        entry_mark * (1 + sl_pct / 100),
    )


def _flat_intel_keys(obj: dict) -> set:
    """Helper: collect intel keys from the precision + execution dicts."""
    keys: set = set()
    for sub in (obj.get("precision"), obj.get("execution")):
        if isinstance(sub, dict):
            keys.update(sub.keys())
    return keys


def _current_max_notional() -> float:
    """Resolve the live server max-notional cap (app_state.RISK is mutated by /risk API)."""
    try:
        return float(app_state.RISK.get("max_notional", 200.0))
    except Exception:
        return float(os.getenv("MAX_NOTIONAL_USDT", "200"))


def _autotrade_leverage_cap() -> int:
    return max(1, min(25, int(app_state.RISK.get("max_leverage", 25) or 25)))


def _sync_autotrade_leverage_cap_from_cfg(cfg: dict) -> int:
    desired = _autotrade_leverage_cap()
    for key in ("leverageMax", "adaptiveLeverageMax", "leverage"):
        try:
            desired = max(desired, int(cfg.get(key, 0) or 0))
        except (TypeError, ValueError):
            pass
    desired = max(1, min(25, desired))
    app_state.RISK["max_leverage"] = desired
    return desired


def _autotrade_leverage_bounds(cfg: dict) -> tuple[int, int]:
    hard_cap = _sync_autotrade_leverage_cap_from_cfg(cfg)
    adaptive_cap = max(
        int(cfg.get("adaptiveLeverageMax", hard_cap) or hard_cap),
        int(cfg.get("leverageMax", hard_cap) or hard_cap),
    )
    hard_cap = max(1, min(hard_cap, adaptive_cap, 25))
    base = int(cfg.get("leverage", 5) or 5)
    lev_min = int(cfg.get("leverageMin", base) or base)
    lev_max = int(cfg.get("leverageMax", max(lev_min, base)) or max(lev_min, base))
    lev_min = max(1, min(hard_cap, lev_min))
    lev_max = max(lev_min, min(hard_cap, lev_max))
    return lev_min, lev_max


def _effective_tp_sl(symbol: str, cfg: dict, intel: dict | None = None) -> dict:
    """Return the effective TP/SL/cap/profit-lock values for a specific symbol.

    Combines three layers in order (most specific wins):
        1. Symbol-level overrides (only when sample count >= threshold)
        2. Group profile (trend-friendly / mean-reversion / high-vol / noisy)
        3. Volatility-tier multiplier (low / med / high, derived from intel)
        4. Global cfg fallback (only when intel is empty)
    The returned dict is the single source of truth used by entry, guardian
    and live order placement.
    """
    from trading.symbol_profiles import _symbol_effective_profile, _symbol_volatility_score
    cfg = cfg if isinstance(cfg, dict) else {}
    sym_profile = _symbol_effective_profile(symbol, cfg)
    group_tpsl_mult = float(sym_profile.get("tpsl_mult", 1.0))
    group_sl_mult = float(sym_profile.get("sl_mult", 1.0))
    group_lock_mult = float(sym_profile.get("lock_trigger_mult", 1.0))
    group_cap_mult = float(sym_profile.get("max_trade_notional_mult", 1.0))

    vol = _symbol_volatility_score(symbol, intel)
    # The two multipliers stack: group first, then volatility tier. This
    # means a high-vol group on a high-volatility symbol gets BOTH the
    # group's wider TP/SL AND the volatility tier's wider TP/SL — fully
    # additive, no surprise cap.
    tp_mult = group_tpsl_mult * float(vol.get("tpMult", 1.0))
    sl_mult = group_sl_mult * float(vol.get("slMult", 1.0))
    cap_mult = group_cap_mult * float(vol.get("capMult", 1.0))
    lock_mult = group_lock_mult * float(vol.get("lockMult", 1.0))

    # Skip override if intel was missing entirely (avoid scaling from a zero
    # signal during boot).
    intel_present = isinstance(intel, dict) and bool(_flat_intel_keys(intel))

    base_tp = float(cfg.get("takeProfitPct", 1.2) or 1.2)
    # V11: per-symbol SL must never fall below the global SL floor. The
    # per-trade ratchet (guard_sl *= 0.986 on losses, floor 0.20) and the
    # sl_mult write path were tightening per-symbol slPct to 0.28-0.47%
    # (noise level on 15x → instant whipsaw SL hits in <1 min, 9 SL/-2.97
    # on 08-01). The V10 supervisorStopLossFloor only capped the global
    # weak_payoff ratchet — this floors the effective per-symbol SL too.
    _sl_floor = float(cfg.get("supervisorStopLossFloor", 0.80) or 0.80)
    _sl_min = max(_sl_floor, float(cfg.get("slMinPct", 0.60) or 0.60))
    base_sl = max(_sl_min, float(cfg.get("stopLossPct", 0.8) or 0.8))
    base_cap = max(20.0, float(cfg.get("tradeNotionalCapUsdt", _current_max_notional()) or _current_max_notional()))
    base_lock_trigger = float(cfg.get("profitLockTriggerUsdt", 0.35) or 0.35)
    base_lock_keep = float(cfg.get("profitLockKeepUsdt", 0.15) or 0.15)
    base_lock_giveback = float(cfg.get("profitLockMaxGivebackUsdt", 0.22) or 0.22)
    base_tp_min = float(cfg.get("tpTargetMinUsdt", 0.55) or 0.55)
    base_tp_max = float(cfg.get("tpTargetMaxUsdt", 2.0) or 2.0)

    if not intel_present:
        return {
            "symbol": str(symbol or "").upper().strip(),
            "tier": "unknown",
            "tpMult": 1.0, "slMult": 1.0, "capMult": 1.0, "lockMult": 1.0,
            "tpPct": round(base_tp, 4),
            "slPct": round(base_sl, 4),
            "notionalCapUsdt": round(base_cap, 4),
            "profitLockTriggerUsdt": round(base_lock_trigger, 4),
            "profitLockKeepUsdt": round(base_lock_keep, 4),
            "profitLockMaxGivebackUsdt": round(base_lock_giveback, 4),
            "tpTargetMinUsdt": round(base_tp_min, 4),
            "tpTargetMaxUsdt": round(base_tp_max, 4),
            "holdTrailPct": round(float(sym_profile.get("holdTrail_base", float(cfg.get("holdTrailPct", 0.25) or 0.25))), 4),
            "holdMinConfidence": round(float(sym_profile.get("holdMinConf_base", float(cfg.get("holdMinConfidence", 0.72) or 0.72))), 4),
        }

    # Allow per-symbol explicit overrides to win when sample count is
    # sufficient (>= SYMBOL_PROFILE_MIN_TRADES). Missing keys still fall
    # through to the computed values.
    sym_overrides = sym_profile if sym_profile.get("source") == "symbol+group" else {}
    # P4: SHORT-specific SL multiplier (SHORT positions need tighter SL to avoid
    # large adverse moves; SHORT_vs_SHORT WR was only 35% historically)
    _sl_mult_adj = sl_mult
    if (intel or {}).get("side") == "SHORT" or str(symbol).upper().endswith("SHORT"):
        _sl_mult_adj = sl_mult * float(cfg.get("shortSlMult", 0.80) or 0.80)
    ret = {
        "symbol": str(symbol or "").upper().strip(),
        "tier": vol.get("tier", "med"),
        "group": sym_profile.get("group", "trend-friendly"),
        "source": sym_profile.get("source", "group"),
        "sampleTrades": sym_profile.get("sampleTrades", 0),
        "tpMult": round(tp_mult, 4),
        "slMult": round(_sl_mult_adj, 4),
        "capMult": round(cap_mult, 4),
        "lockMult": round(lock_mult, 4),
        "tpPct": round(float(sym_overrides.get("tpPct", base_tp * tp_mult)), 4),
        "slPct": round(max(_sl_min, float(sym_overrides.get("slPct", base_sl * _sl_mult_adj))), 4),
        "notionalCapUsdt": round(float(sym_overrides.get("notionalCapUsdt", base_cap * cap_mult)), 4),
        "profitLockTriggerUsdt": round(
            float(sym_overrides.get("profitLockTriggerUsdt", base_lock_trigger * lock_mult)), 4),
        "profitLockKeepUsdt": round(
            float(sym_overrides.get("profitLockKeepUsdt", base_lock_keep * lock_mult)), 4),
        "profitLockMaxGivebackUsdt": round(
            float(sym_overrides.get("profitLockMaxGivebackUsdt", base_lock_giveback * lock_mult)), 4),
        "tpTargetMinUsdt": round(float(sym_overrides.get("tpTargetMinUsdt", base_tp_min * lock_mult)), 4),
        "tpTargetMaxUsdt": round(float(sym_overrides.get("tpTargetMaxUsdt", base_tp_max * lock_mult)), 4),
    }
    # Guardian autotune: per-symbol trailing and hold-confidence
    # Fallback chain: symbol_profile → group base → global cfg → hardcoded default
    ret["holdTrailPct"] = round(float(sym_overrides.get("holdTrailPct", sym_profile.get("holdTrail_base", float(cfg.get("holdTrailPct", 0.25) or 0.25)))), 4)
    ret["holdMinConfidence"] = round(float(sym_overrides.get("holdMinConfidence", sym_profile.get("holdMinConf_base", float(cfg.get("holdMinConfidence", 0.72) or 0.72)))), 4)

    # Fee-based TP% floor: ensure gross profit covers round-trip fees + min net edge.
    # Without this, tight TP% (e.g. 0.35%) on small notional loses money to fees.
    try:
        _usdt = max(1.0, float(cfg.get("usdtAmount", 20.0) or 20.0))
        _taker = float(AUTOTRADE_TAKER_FEE_BPS_PER_SIDE)
        _extra = float(AUTOTRADE_EXTRA_COST_BPS)
        _slip = float(cfg.get("maxSlippageBps", 18.0) or 18.0)
        _cost_bps = (2.0 * _taker) + _extra + (_slip * 0.5)
        _min_net_usdt = float(cfg.get("feeMinNetProfitUSDT", AUTOTRADE_MIN_NET_PROFIT_USDT) or AUTOTRADE_MIN_NET_PROFIT_USDT)
        _tp_fee_floor_pct = ((_cost_bps / 10000.0 * _usdt) + _min_net_usdt) / _usdt * 100.0
        _tp_fee_floor_pct = max(_tp_fee_floor_pct, 0.25)  # absolute floor
        cur_tp = float(ret.get("tpPct", base_tp))
        if cur_tp < _tp_fee_floor_pct:
            ret["tpPct"] = round(_tp_fee_floor_pct, 4)
        cur_sl = float(ret.get("slPct", base_sl))
        _sl_fee_floor_pct = _tp_fee_floor_pct * 0.5  # SL floor = half of TP floor (R:R ≥ 2)
        if cur_sl < _sl_fee_floor_pct:
            ret["slPct"] = round(_sl_fee_floor_pct, 4)
        ret["tpFeeFloorPct"] = round(_tp_fee_floor_pct, 4)
    except Exception:
        pass

    return ret


def _profit_lock_policy(cfg: dict, peak_usdt: float, symbol: str | None = None, intel: dict | None = None) -> dict:
    """Compute the per-symbol profit-lock policy.

    When ``symbol`` + ``intel`` are provided the policy is scaled by the
    per-symbol volatility tier (volatile symbols lock later and let winners
    run; stable majors lock earlier to bank small consistent gains).
    """
    peak = max(0.0, float(peak_usdt or 0.0))
    # Per-symbol override via _effective_tp_sl.
    if symbol and intel is not None:
        eff = _effective_tp_sl(symbol, cfg, intel)
        trigger = max(0.05, float(eff.get("profitLockTriggerUsdt", 0.35)))
        keep = max(0.02, float(eff.get("profitLockKeepUsdt", 0.15)))
        max_giveback = max(0.01, float(eff.get("profitLockMaxGivebackUsdt", 0.22)))
    elif "profitLockTriggerUsdt" in cfg:
        trigger = max(0.05, float(cfg.get("profitLockTriggerUsdt", 0.35) or 0.35))
        keep = max(0.02, float(cfg.get("profitLockKeepUsdt", 0.15) or 0.15))
        max_giveback = max(0.01, float(cfg.get("profitLockMaxGivebackUsdt", 0.22) or 0.22))
    else:
        # Fallback: derive trigger from TP target minimum.
        if symbol and intel is not None:
            eff = _effective_tp_sl(symbol, cfg, intel)
            trigger = max(0.20, float(eff.get("tpTargetMinUsdt", 0.55)) * 0.5)
        else:
            trigger = max(0.20, float(cfg.get("tpTargetMinUsdt", 0.55) or 0.55) * 0.5)
        keep = max(0.02, float(cfg.get("profitLockKeepUsdt", 0.15) or 0.15))
        max_giveback = max(0.01, float(cfg.get("profitLockMaxGivebackUsdt", 0.22) or 0.22))
    lock_usdt = 0.0
    if peak >= trigger:
        lock_usdt = max(0.02, keep, peak - max_giveback, peak * 0.35)
        lock_usdt = min(lock_usdt, peak * 0.98)
    return {
        "trigger": round(float(trigger), 6),
        "keep": round(float(keep), 6),
        "maxGiveback": round(float(max_giveback), 6),
        "lockUsdt": round(float(lock_usdt), 6),
    }


def blend_tpsl_with_atr(
    tp_pct: float,
    sl_pct: float,
    precision: dict | None,
    cfg: dict,
) -> tuple[float, float, dict]:
    if not bool(cfg.get("atrTpSlEnabled", True)):
        return tp_pct, sl_pct, {"atrBlend": False}
    p = precision if isinstance(precision, dict) else {}
    atr_pct = float(p.get("atrPct", 0.0) or 0.0)
    if atr_pct <= 0:
        return tp_pct, sl_pct, {"atrBlend": False}
    tp_mult = float(p.get("atrTpMult", 1.5) or 1.5)
    sl_mult = float(p.get("atrSlMult", 1.0) or 1.0)
    atr_tp = max(0.15, atr_pct * tp_mult)
    atr_sl = max(0.12, atr_pct * sl_mult)
    blended_tp = min(6.0, max(float(tp_pct), atr_tp))
    blended_sl = min(4.5, max(float(sl_pct), atr_sl))
    return (
        round(blended_tp, 4),
        round(blended_sl, 4),
        {
            "atrBlend": True,
            "atrPct": round(atr_pct, 4),
            "atrTpFloorPct": round(atr_tp, 4),
            "atrSlFloorPct": round(atr_sl, 4),
        },
    )


def effective_tpsl_pct_for_trade(
    cfg: dict,
    trade_usdt: float,
    realized_vol_pct: float | None = None,
    pullback_allowance_pct: float | None = None,
    precision: dict | None = None,
) -> tuple[float, float, dict]:
    base_tp = float(cfg.get("takeProfitPct", 1.8) or 1.8)
    base_sl = float(cfg.get("stopLossPct", 0.8) or 0.8)
    if not bool(cfg.get("tpSlTargetUsdtEnabled", True)):
        return base_tp, base_sl, {"enabled": False}
    amt = max(1e-9, float(trade_usdt or 0.0))
    tp_min_u = max(0.05, float(cfg.get("tpTargetMinUsdt", 0.5) or 0.5))
    tp_max_u = max(tp_min_u, float(cfg.get("tpTargetMaxUsdt", 2.0) or 2.0))
    rr = max(0.35, min(0.85, float(cfg.get("slToTpRatio", 0.55) or 0.55)))
    target_u = (tp_min_u + tp_max_u) * 0.5
    _mv = 0.0
    # Phase A: per-symbol realized-vol target (movePct5m %) → USDT TP target.
    # When a live volatility estimate is present it wins over the interpolation
    # path below (which only ever had a dead rvPct source). The result is
    # clamped into the [tpTargetMinUsdt, tpTargetMaxUsdt] band so fee floors and
    # capital limits in the rest of the pipeline still apply.
    if bool(cfg.get("volTargetUsdtEnabled", True)):
        _mv = max(0.0, float((precision or {}).get("movePct5m", 0.0) or 0.0))
        if _mv > 0:
            try:
                _notional_u = max(1e-9, float(amt) * max(1, float(cfg.get("leverage", 5) or 5)))
                _vol_target_u = _notional_u * (_mv / 100.0)
                target_u = max(tp_min_u, min(tp_max_u, _vol_target_u))
            except Exception:
                pass
    if realized_vol_pct is not None:
        try:
            rv = max(0.0, float(realized_vol_pct))
            v_low = max(0.001, float(cfg.get("feeAdaptiveVolLowPct", 0.08) or 0.08))
            v_high = max(v_low + 0.001, float(cfg.get("feeAdaptiveVolHighPct", 0.35) or 0.35))
            norm = max(0.0, min(1.0, (rv - v_low) / max(v_high - v_low, 1e-9)))
            interp_u = tp_min_u + (tp_max_u - tp_min_u) * norm
            # The vol-path target already clamped; keep whichever is tighter
            # so a dead rvPct=0 cannot inflate the target past the vol estimate.
            target_u = min(target_u, interp_u) if _mv > 0 else interp_u
        except Exception:
            pass
    tp_u = max(tp_min_u, min(tp_max_u, target_u))
    sl_u = tp_u * rr
    # tp_u is USDT profit target; convert to % of entry price.
    # PnL = entry × pct/100 × notional, so pct = (tp_u / notional) × 100.
    # notional = amt × leverage (default 5x).
    _leverage = max(1, float(cfg.get('leverage', 5) or 5))
    _notional = amt * _leverage
    tp_pct = max(0.1, (tp_u / max(_notional, 1e-9)) * 100.0)
    sl_pct = max(0.1, (sl_u / max(_notional, 1e-9)) * 100.0)
    sl_from_candles = None
    if bool(cfg.get("slCandleAdaptiveEnabled", True)) and pullback_allowance_pct is not None:
        try:
            raw_pb = max(0.0, float(pullback_allowance_pct))
            pb_mult = max(0.8, min(2.2, float(cfg.get("slCandleBufferMult", 1.15) or 1.15)))
            pb_min = max(0.1, float(cfg.get("slCandleMinPct", 0.35) or 0.35))
            pb_max = max(pb_min, float(cfg.get("slCandleMaxPct", 2.8) or 2.8))
            sl_from_candles = max(pb_min, min(pb_max, raw_pb * pb_mult))
            sl_pct = max(sl_pct, sl_from_candles)
        except Exception:
            sl_from_candles = None
    tp_pct = min(6.0, max(0.2, tp_pct))
    sl_pct = min(4.5, max(0.2, sl_pct))
    tp_pct, sl_pct, atr_meta = blend_tpsl_with_atr(tp_pct, sl_pct, precision, cfg)
    meta = {
        "enabled": True,
        "tpTargetUsdt": round(tp_u, 4),
        "slTargetUsdt": round(sl_u, 4),
        "rr": round(tp_pct / max(sl_pct, 1e-9), 4),
        **atr_meta,
    }
    if sl_from_candles is not None:
        meta["slFromCandlesPct"] = round(float(sl_from_candles), 4)
    return round(tp_pct, 4), round(sl_pct, 4), meta
