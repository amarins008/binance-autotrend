"""Risk desk: R:R, ATR stops, fee edge, TP/SL targets."""


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
    base_floor = float(cfg.get("feeMinNetProfitUSDT", default_min_net) or default_min_net)
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
    if realized_vol_pct is not None:
        try:
            rv = max(0.0, float(realized_vol_pct))
            v_low = max(0.001, float(cfg.get("feeAdaptiveVolLowPct", 0.08) or 0.08))
            v_high = max(v_low + 0.001, float(cfg.get("feeAdaptiveVolHighPct", 0.35) or 0.35))
            norm = max(0.0, min(1.0, (rv - v_low) / max(v_high - v_low, 1e-9)))
            target_u = tp_min_u + (tp_max_u - tp_min_u) * norm
        except Exception:
            pass
    tp_u = max(tp_min_u, min(tp_max_u, target_u))
    sl_u = tp_u * rr
    tp_pct = max(0.1, (tp_u / amt) * 100.0)
    sl_pct = max(0.1, (sl_u / amt) * 100.0)
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
