"""Position management: hold winners, cut losers, trail levels."""

from typing import Optional, Dict, Any


def intel_momentum_pct(intel: dict | None) -> float:
    if not isinstance(intel, dict):
        return 0.0
    m = intel.get("momentum")
    if isinstance(m, dict):
        return float(m.get("momentumPct", 0.0) or 0.0)
    ex = intel.get("execution")
    if isinstance(ex, dict):
        return float(ex.get("momentumPct", 0.0) or 0.0)
    return 0.0


def should_hold_winner(side: str, intel: dict | None, cfg: dict) -> bool:
    if not cfg.get("holdWinners", True):
        return False
    if not isinstance(intel, dict):
        return False
    sig = str(intel.get("signal", "WAIT")).upper()
    conf = float(intel.get("confidence", 0.0) or 0.0)
    if sig != side or conf < float(cfg.get("holdMinConfidence", 0.72)):
        return False
    mom = intel_momentum_pct(intel)
    p = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    long_score = float(p.get("longScore", 0.0) or 0.0)
    short_score = float(p.get("shortScore", 0.0) or 0.0)
    score_ok = (long_score - short_score) >= 1.0 if side == "LONG" else (short_score - long_score) >= 1.0
    trend_ok = bool(p.get("trendUp") or p.get("trendUpPartial")) if side == "LONG" else bool(
        p.get("trendDown") or p.get("trendDnPartial")
    )
    mom_ok = (side == "LONG" and mom > 0.04) or (side == "SHORT" and mom < -0.04)
    return bool(mom_ok or (trend_ok and score_ok))


def should_cut_loser_early(side: str, intel: dict | None, cfg: dict) -> bool:
    if not isinstance(intel, dict):
        return False
    sig = str(intel.get("signal", "WAIT")).upper()
    conf = float(intel.get("confidence", 0.0) or 0.0)
    opp = "SHORT" if side == "LONG" else "LONG"
    if sig != opp:
        return False
    min_conf = max(0.65, float(cfg.get("holdMinConfidence", 0.72)))
    if conf < min_conf:
        return False
    p = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    m = intel.get("momentum") if isinstance(intel.get("momentum"), dict) else {}
    long_score = float(p.get("longScore", 0.0) or 0.0)
    short_score = float(p.get("shortScore", 0.0) or 0.0)
    edge = (short_score - long_score) if side == "LONG" else (long_score - short_score)
    trend_against = bool(p.get("trendDown") or p.get("trendDnPartial")) if side == "LONG" else bool(
        p.get("trendUp") or p.get("trendUpPartial")
    )
    mom = intel_momentum_pct(intel)
    mom_against = (side == "LONG" and mom < -0.05) or (side == "SHORT" and mom > 0.05)
    div = str(m.get("divergence", "NONE")).upper()
    div_against = (side == "LONG" and div == "BEARISH_DIVERGENCE") or (side == "SHORT" and div == "BULLISH_DIVERGENCE")
    adverse_votes = sum([1 if edge >= 2.0 else 0, 1 if trend_against else 0, 1 if mom_against else 0, 1 if div_against else 0])
    return adverse_votes >= 2


def should_extend_tp_with_tradingview(side: str, tv_guidance: Optional[Dict[str, Any]], cfg: dict, symbol: str = None) -> bool:
    """
    Check if TP should be extended based on TradingView guidance.
    Returns True if TradingView strongly confirms the direction.
    Supports per-symbol override configuration.
    """
    # Check per-symbol override first
    if symbol and cfg.get("tradingviewTpExtensionOverride"):
        override = cfg.get("tradingviewTpExtensionOverride", {})
        if symbol in override:
            return bool(override[symbol])
    
    # Fall back to global config
    if not cfg.get("tradingviewTpExtensionEnabled", False):
        return False
    if not tv_guidance:
        return False
    
    rec = tv_guidance.get("recommendation")
    strength = tv_guidance.get("strength", 0.0)
    min_strength = float(cfg.get("tradingviewTpExtensionMinStrength", 0.7))
    
    # Extend TP if TradingView strongly confirms
    if side == "LONG" and rec in ("STRONG_BUY", "BUY") and strength >= min_strength:
        return True
    if side == "SHORT" and rec in ("STRONG_SELL", "SELL") and strength >= min_strength:
        return True
    
    return False


def should_trail_sl_with_tradingview(side: str, tv_guidance: Optional[Dict[str, Any]], cfg: dict, symbol: str = None) -> bool:
    """
    Check if SL should be trailed based on TradingView guidance.
    Returns True if TradingView still confirms the direction.
    Supports per-symbol override configuration.
    """
    # Check per-symbol override first
    if symbol and cfg.get("tradingviewSlTrailingOverride"):
        override = cfg.get("tradingviewSlTrailingOverride", {})
        if symbol in override:
            return bool(override[symbol])
    
    # Fall back to global config
    if not cfg.get("tradingviewSlTrailingEnabled", False):
        return False
    if not tv_guidance:
        return False
    
    rec = tv_guidance.get("recommendation")
    strength = tv_guidance.get("strength", 0.0)
    min_strength = float(cfg.get("tradingviewSlTrailingMinStrength", 0.6))
    
    # Trail SL if TradingView still confirms the direction
    if side == "LONG" and rec in ("STRONG_BUY", "BUY", "NEUTRAL") and strength >= min_strength:
        return True
    if side == "SHORT" and rec in ("STRONG_SELL", "SELL", "NEUTRAL") and strength >= min_strength:
        return True
    
    return False


def should_exit_early_with_tradingview(side: str, tv_guidance: Optional[Dict[str, Any]], cfg: dict, symbol: str = None) -> bool:
    """
    Check if should exit early based on TradingView reversal.
    Returns True if TradingView strongly reverses.
    Supports per-symbol override configuration.
    """
    # Check per-symbol override first
    if symbol and cfg.get("tradingviewEarlyExitOverride"):
        override = cfg.get("tradingviewEarlyExitOverride", {})
        if symbol in override:
            return bool(override[symbol])
    
    # Fall back to global config
    if not cfg.get("tradingviewEarlyExitEnabled", False):
        return False
    if not tv_guidance:
        return False
    
    rec = tv_guidance.get("recommendation")
    strength = tv_guidance.get("strength", 0.0)
    min_strength = float(cfg.get("tradingviewEarlyExitMinStrength", 0.7))
    opp = "SHORT" if side == "LONG" else "LONG"
    
    # Exit early if TradingView strongly reverses
    if rec in (opp, f"STRONG_{opp}") and strength >= min_strength:
        return True
    
    # Check for divergence in oscillators
    oscillators = tv_guidance.get("oscillators", {})
    if oscillators:
        # Simple divergence check: if RSI is overbought/oversold but price still going
        rsi = oscillators.get("RSI", 50)
        if side == "LONG" and rsi > 70 and strength >= min_strength:
            return True
        if side == "SHORT" and rsi < 30 and strength >= min_strength:
            return True
    
    return False


def get_tradingview_tp_extension_pct(side: str, tv_guidance: Optional[Dict[str, Any]], cfg: dict) -> float:
    """
    Calculate TP extension percentage based on TradingView strength.
    Returns additional TP percentage to add.
    """
    if not tv_guidance:
        return 0.0
    
    strength = tv_guidance.get("strength", 0.0)
    rec = tv_guidance.get("recommendation")
    
    base_extension = float(cfg.get("tradingviewTpExtensionBasePct", 0.2))  # 0.2% base
    max_extension = float(cfg.get("tradingviewTpExtensionMaxPct", 0.5))  # 0.5% max
    
    # Stronger signals get more extension
    if rec in ("STRONG_BUY", "STRONG_SELL"):
        extension = base_extension * (strength / 0.8)  # Scale with strength
    else:
        extension = base_extension * 0.5  # Half for regular signals
    
    return min(extension, max_extension)


def get_tradingview_sl_trailing_pct(side: str, tv_guidance: Optional[Dict[str, Any]], cfg: dict) -> float:
    """
    Calculate SL trailing percentage based on TradingView guidance.
    Returns trailing percentage.
    """
    if not tv_guidance:
        return 0.0
    
    strength = tv_guidance.get("strength", 0.0)
    base_trail = float(cfg.get("tradingviewSlTrailingBasePct", 0.15))  # 0.15% base
    max_trail = float(cfg.get("tradingviewSlTrailingMaxPct", 0.3))  # 0.3% max
    
    # Stronger signals allow more aggressive trailing
    trail = base_trail * (strength / 0.7)
    return min(trail, max_trail)


def trail_winner_levels(side: str, mark: float, old_sl: float, old_tp: float, trail_pct: float) -> tuple[float, float]:
    t = max(0.05, float(trail_pct))
    if side == "LONG":
        new_sl = max(float(old_sl), mark * (1 - t / 100.0))
        new_tp = max(float(old_tp), mark * (1 + t / 100.0))
        return new_sl, new_tp
    new_sl = min(float(old_sl), mark * (1 + t / 100.0))
    new_tp = min(float(old_tp), mark * (1 - t / 100.0))
    return new_sl, new_tp


def max_winner_tp_expands(intel: dict | None, cfg: dict) -> int:
    base_rounds = int(cfg.get("holdTpExpandBaseRounds", 1) or 1)
    max_rounds = int(cfg.get("holdTpExpandMaxRounds", 3) or 3)
    base_rounds = max(0, min(base_rounds, 6))
    max_rounds = max(base_rounds, min(max_rounds, 8))
    if not isinstance(intel, dict):
        return base_rounds
    conf = float(intel.get("confidence", 0.0) or 0.0)
    strong = float(cfg.get("holdTpExpandStrongConf", 0.82) or 0.82)
    very_strong = float(cfg.get("holdTpExpandVeryStrongConf", 0.9) or 0.9)
    bonus = 0
    if conf >= strong:
        bonus += 1
    if conf >= very_strong:
        bonus += 1
    return max(base_rounds, min(max_rounds, base_rounds + bonus))
