"""Position management: hold winners, cut losers, trail levels."""


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
