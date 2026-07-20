"""Ordered entry gate pipeline with audit trail."""

from dataclasses import dataclass, field

from trading.risk import (
    effective_min_net_profit_usdt,
    effective_tpsl_pct_for_trade,
    estimate_trade_edge_usdt,
    passes_min_risk_reward,
)


@dataclass
class EntryInputs:
    cfg: dict
    intel: dict
    regime: dict
    signal: str
    confidence: float
    spread_bps: float
    slippage_bps: float
    mark: float
    ex: dict
    htf: dict
    candle_ctx: dict
    adaptive_min_conf: float
    live_loss_streak: int = 0
    vision_ok: bool = True
    trade_usdt: float = 0.0
    rv_pct: float | None = None
    pb_pct: float | None = None
    eff_leverage: int = 5
    max_notional: float = 200.0
    taker_fee_bps: float = 4.0
    extra_cost_bps: float = 2.0
    default_min_net: float = 0.05
    pre_reversal_score: float = 0.0
    pre_reversal_side_at_risk: str = ""
    bb_pct_b: float = 0.5
    vwap_distance_pct: float = 0.0
    long_score: float = 0.0
    short_score: float = 0.0
    near_resistance: bool = False
    near_support: bool = False
    wait_override_imbalance: float = 0.0
    scan_chase_speed: str = "normal"
    scan_long_bias: float = 0.5


@dataclass
class EntryPlan:
    approved: bool
    skip_code: str = ""
    skip_message: str = ""
    signal: str = "WAIT"
    confidence: float = 0.0
    trade_usdt: float = 0.0
    eff_tp_pct: float = 0.0
    eff_sl_pct: float = 0.0
    eff_leverage: int = 5
    tpsl_meta: dict = field(default_factory=dict)
    pipeline: list[dict] = field(default_factory=list)
    extra: dict = field(default_factory=dict)


def _step(pipeline: list, name: str, passed: bool, detail: str = "") -> bool:
    pipeline.append({"gate": name, "passed": passed, "detail": detail})
    return passed


def evaluate_entry_plan(inp: EntryInputs) -> EntryPlan:
    """
    Run synchronous entry gates after async context (intel, htf, candles) is ready.
    Returns audit trail in plan.pipeline for dashboard / logs.
    """
    pipeline: list[dict] = []
    cfg = inp.cfg
    signal = str(inp.signal).upper()
    conf = float(inp.confidence)
    regime = inp.regime or {}

    if not _step(pipeline, "signal", signal in ("LONG", "SHORT"), signal):
        return EntryPlan(False, "signal_wait", "Skip: signal WAIT", signal, conf, pipeline=pipeline)

    max_spread = float(cfg.get("maxSpreadBps", 18))
    if regime.get("name") == "VOLATILE":
        max_spread = min(max_spread, 16.0)
    elif regime.get("name") == "TREND":
        max_spread = min(max_spread + 2.0, 30.0)
    if not _step(pipeline, "spread", inp.spread_bps <= max_spread, f"{inp.spread_bps:.2f} bps (max {max_spread:.1f})"):
        return EntryPlan(
            False,
            "spread",
            f"Skip: spread too wide {inp.spread_bps:.2f} bps",
            signal,
            conf,
            pipeline=pipeline,
        )

    if not _step(
        pipeline,
        "confidence",
        conf >= inp.adaptive_min_conf,
        f"{conf:.3f} vs min {inp.adaptive_min_conf:.3f}",
    ):
        return EntryPlan(
            False,
            "low_confidence",
            f"Skip: low confidence {conf} < {inp.adaptive_min_conf:.2f}",
            signal,
            conf,
            pipeline=pipeline,
        )

    # Momentum confirmation for timing accuracy
    momentum = inp.intel.get("momentum") if isinstance(inp.intel.get("momentum"), dict) else {}
    mom_pct = float(momentum.get("momentumPct", 0.0) or 0.0)
    mom_strength = float(momentum.get("strength", 0.0) or 0.0)
    
    # Require minimum momentum strength for entries
    min_mom_strength = float(cfg.get("minMomentumStrength", 0.08) or 0.08)
    if not _step(
        pipeline,
        "momentum_strength",
        mom_strength >= min_mom_strength,
        f"Momentum strength {mom_strength:.3f} vs min {min_mom_strength:.3f}",
    ):
        return EntryPlan(
            False,
            "weak_momentum",
            f"Skip: weak momentum {mom_strength:.3f} < {min_mom_strength:.3f}",
            signal,
            conf,
            pipeline=pipeline,
        )

    # Momentum direction confirmation
    if signal == "LONG" and mom_pct < 0:
        if not _step(pipeline, "momentum_direction", False, f"LONG signal but momentum {mom_pct:.3f} is negative"):
            return EntryPlan(
                False,
                "momentum_mismatch",
                f"Skip: LONG signal vs negative momentum {mom_pct:.3f}",
                signal,
                conf,
                pipeline=pipeline,
            )
    elif signal == "SHORT" and mom_pct > 0:
        if not _step(pipeline, "momentum_direction", False, f"SHORT signal but momentum {mom_pct:.3f} is positive"):
            return EntryPlan(
                False,
                "momentum_mismatch",
                f"Skip: SHORT signal vs positive momentum {mom_pct:.3f}",
                signal,
                conf,
                pipeline=pipeline,
            )

    if bool(cfg.get("requireVisionConsensus", False)):
        if not _step(pipeline, "vision", inp.vision_ok, "required"):
            return EntryPlan(False, "vision", "Skip: vision consensus required", signal, conf, pipeline=pipeline)

    if bool(cfg.get("htfStrictEnabled", True)):
        htf_dir = str((inp.htf or {}).get("dir", "NEUTRAL")).upper()
        htf_strength = float((inp.htf or {}).get("strength", 0.0) or 0.0)
        min_strength = float(cfg.get("htfMinStrength", 0.28) or 0.28)
        htf_ok = not (htf_dir in ("LONG", "SHORT") and htf_dir != signal and htf_strength >= min_strength)
        if not _step(pipeline, "htf", htf_ok, f"{signal} vs HTF {htf_dir} ({htf_strength:.2f})"):
            return EntryPlan(
                False,
                "htf_conflict",
                f"Skip: HTF conflict {signal} vs {htf_dir}",
                signal,
                conf,
                pipeline=pipeline,
            )
        if regime.get("name") == "TREND" and htf_dir == signal:
            p2 = inp.intel.get("precision") if isinstance(inp.intel.get("precision"), dict) else {}
            bbp = float(p2.get("bbPctB", 0.5) or 0.5)
            chase = (signal == "LONG" and bbp > 0.93) or (signal == "SHORT" and bbp < 0.07)
            if not _step(pipeline, "structure_chase", not chase, f"bbPctB={bbp:.3f}"):
                return EntryPlan(
                    False,
                    "structure_chase",
                    f"Skip: trend chase extreme bbPctB={bbp:.3f}",
                    signal,
                    conf,
                    pipeline=pipeline,
                )

    if bool(cfg.get("ema200StrictEnabled", True)):
        p_macro = inp.intel.get("precision") if isinstance(inp.intel.get("precision"), dict) else {}
        ema_ok = True
        detail = "n/a"
        if p_macro.get("ema200Ready"):
            if signal == "LONG" and not p_macro.get("priceAboveEma200"):
                ema_ok = False
                detail = "LONG below EMA200"
            if signal == "SHORT" and p_macro.get("priceAboveEma200"):
                ema_ok = False
                detail = "SHORT above EMA200"
            else:
                detail = "aligned"
        if not _step(pipeline, "ema200", ema_ok, detail):
            return EntryPlan(False, "ema200", f"Skip: {detail}", signal, conf, pipeline=pipeline)

    thr_fund = float(cfg.get("skipFundingAgainst") or 0)
    fr = inp.ex.get("lastFundingRate") if isinstance(inp.ex, dict) else None
    fund_ok = True
    fund_detail = "disabled"
    if thr_fund > 0 and fr is not None:
        frf = float(fr)
        if signal == "LONG" and frf > thr_fund:
            fund_ok = False
            fund_detail = f"LONG vs funding {frf:.6f}"
        if signal == "SHORT" and frf < -thr_fund:
            fund_ok = False
            fund_detail = f"SHORT vs funding {frf:.6f}"
    else:
        fund_detail = "ok"
    if not _step(pipeline, "funding", fund_ok, fund_detail):
        return EntryPlan(False, "funding", f"Skip: {fund_detail}", signal, conf, pipeline=pipeline)

    max_slip = float(cfg.get("maxSlippageBps", 18))
    if regime.get("name") == "VOLATILE":
        max_slip = min(max_slip, 18.0)
    if not _step(pipeline, "slippage", inp.slippage_bps <= max_slip, f"{inp.slippage_bps:.2f} bps"):
        return EntryPlan(
            False,
            "slippage",
            f"Skip: slippage {inp.slippage_bps:.2f} bps",
            signal,
            conf,
            pipeline=pipeline,
        )

    # Adaptive position sizing based on loss streak
    trade_usdt = min(float(inp.trade_usdt), float(inp.max_notional))
    if bool(cfg.get("adaptiveLossStreakEnabled", True)) and inp.live_loss_streak >= int(cfg.get("adaptiveLossStreakThreshold", 3)):
        threshold = int(cfg.get("adaptiveLossStreakThreshold", 3))
        max_reduction = float(cfg.get("adaptiveLossStreakMaxReduction", 0.50))
        streak_excess = max(0, inp.live_loss_streak - threshold)
        reduction_pct = min(max_reduction, streak_excess * 0.10)  # 10% reduction per excess loss, capped at max_reduction
        trade_usdt *= (1.0 - reduction_pct)
        _step(pipeline, "adaptive_sizing", True, f"Loss streak {inp.live_loss_streak}: size reduced by {reduction_pct*100:.1f}%")

    # Apply regime-based size multiplier
    regime_multiplier = regime.get("sizeMultiplier", 1.0)
    if regime_multiplier != 1.0:
        trade_usdt *= regime_multiplier
        _step(pipeline, "regime_sizing", True, f"Regime {regime.get('name')}: size multiplier {regime_multiplier:.2f}")

    # Apply session-based adjustments if enabled
    if bool(cfg.get("sessionBasedAdjustments", True)):
        import time
        now_local = time.localtime(time.time())
        hour = now_local.tm_hour
        session_mult = 1.0
        session_name = "normal"
        
        if 0 <= hour < 8:
            session_mult = float(cfg.get("sessionAsianMultiplier", 0.85))
            session_name = "Asian"
        elif 8 <= hour < 12:
            session_mult = float(cfg.get("sessionLondonMultiplier", 1.15))
            session_name = "London"
        elif 12 <= hour < 16:
            session_mult = float(cfg.get("sessionUSOverlapMultiplier", 1.2))
            session_name = "US overlap"
        elif 16 <= hour < 20:
            session_mult = float(cfg.get("sessionUSAfternoonMultiplier", 1.05))
            session_name = "US afternoon"
        elif 20 <= hour < 24:
            session_mult = float(cfg.get("sessionAsianEveningMultiplier", 0.9))
            session_name = "Asian evening"
        
        if session_mult != 1.0:
            trade_usdt *= session_mult
            _step(pipeline, "session_sizing", True, f"Session {session_name}: multiplier {session_mult:.2f}")

    # Order flow confirmation for micro-structure analysis
    imbalance = float(inp.intel.get("imbalance", 0.0) or 0.0) if isinstance(inp.intel, dict) else None
    if bool(cfg.get("orderFlowConfirmation", True)):
        min_imbalance = float(cfg.get("minOrderFlowImbalance", 0.03) or 0.03)
        if imbalance is not None:
            if signal == "LONG" and imbalance < min_imbalance:
                if not _step(pipeline, "order_flow", False, f"LONG signal but order flow imbalance {imbalance:.3f} < {min_imbalance:.3f}"):
                    return EntryPlan(
                        False,
                        "order_flow_mismatch",
                        f"Skip: LONG signal vs weak order flow {imbalance:.3f}",
                        signal,
                        conf,
                        pipeline=pipeline,
                    )
            elif signal == "SHORT" and imbalance > -min_imbalance:
                if not _step(pipeline, "order_flow", False, f"SHORT signal but order flow imbalance {imbalance:.3f} > {-min_imbalance:.3f}"):
                    return EntryPlan(
                        False,
                        "order_flow_mismatch",
                        f"Skip: SHORT signal vs weak order flow {imbalance:.3f}",
                        signal,
                        conf,
                        pipeline=pipeline,
                    )
        else:
            _step(pipeline, "order_flow", False, "No order flow data available")
            return EntryPlan(
                False,
                "no_order_flow",
                "Skip: no order flow data for confirmation",
                signal,
                conf,
                pipeline=pipeline,
            )

    p_intel = inp.intel.get("precision") if isinstance(inp.intel.get("precision"), dict) else {}
    eff_tp, eff_sl, tpsl_meta = effective_tpsl_pct_for_trade(
        cfg,
        trade_usdt,
        inp.rv_pct,
        pullback_allowance_pct=inp.pb_pct,
        precision=p_intel,
    )
    min_rr = float(cfg.get("minRiskRewardRatio", 1.5) or 1.5)
    rr_val = eff_tp / max(eff_sl, 1e-9)
    if not _step(pipeline, "risk_reward", passes_min_risk_reward(eff_tp, eff_sl, min_rr), f"R:R {rr_val:.2f} (min {min_rr})"):
        return EntryPlan(
            False,
            "risk_reward",
            f"Skip: R:R {rr_val:.2f} < {min_rr}",
            signal,
            conf,
            trade_usdt,
            eff_tp,
            eff_sl,
            inp.eff_leverage,
            tpsl_meta,
            pipeline=pipeline,
        )

    gross_u, est_cost_u, net_u = estimate_trade_edge_usdt(
        trade_usdt,
        eff_tp,
        float(cfg.get("maxSlippageBps", 18)),
        taker_fee_bps_per_side=inp.taker_fee_bps,
        extra_cost_bps=inp.extra_cost_bps,
    )
    min_net = effective_min_net_profit_usdt(
        cfg,
        inp.rv_pct,
        default_min_net=inp.default_min_net,
        taker_fee_bps=inp.taker_fee_bps,
        extra_cost_bps=inp.extra_cost_bps,
    )
    if inp.live_loss_streak >= 2:
        min_net *= 1.0 + (0.18 * min(3, inp.live_loss_streak - 1))
    if not _step(
        pipeline,
        "fee_edge",
        net_u > min_net,
        f"net {net_u:.4f} USDT (need >{min_net:.4f}, gross {gross_u:.4f} cost {est_cost_u:.4f})",
    ):
        return EntryPlan(
            False,
            "fee_edge",
            f"Skip: net edge {net_u:.4f} <= {min_net:.4f}",
            signal,
            conf,
            trade_usdt,
            eff_tp,
            eff_sl,
            inp.eff_leverage,
            tpsl_meta,
            pipeline=pipeline,
        )

    _step(pipeline, "approved", True, f"{signal} ${trade_usdt:.2f} TP{eff_tp}% SL{eff_sl}% lev{inp.eff_leverage}")
    return EntryPlan(
        True,
        signal=signal,
        confidence=conf,
        trade_usdt=trade_usdt,
        eff_tp_pct=eff_tp,
        eff_sl_pct=eff_sl,
        eff_leverage=inp.eff_leverage,
        tpsl_meta=tpsl_meta,
        pipeline=pipeline,
        extra={"grossUsdt": gross_u, "estCostUsdt": est_cost_u, "netUsdt": net_u},
    )
