"""Market regime classification for sizing and gate strictness."""


def detect_market_regime(intel: dict | None) -> dict:
    if not isinstance(intel, dict):
        return {"name": "UNKNOWN", "confidenceBoost": 0.0, "sizeMultiplier": 1.0, "strictness": "normal"}
    p = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    m = intel.get("momentum") if isinstance(intel.get("momentum"), dict) else {}
    atr = float(p.get("atrPct", 0.0) or 0.0)
    bbw = float(p.get("bbBandwidth", 0.0) or 0.0)
    l_score = float(p.get("longScore", 0.0) or 0.0)
    s_score = float(p.get("shortScore", 0.0) or 0.0)
    edge = abs(l_score - s_score)
    trend_bias = bool(p.get("trendUp") or p.get("trendDown"))
    partial_bias = bool(p.get("trendUpPartial") or p.get("trendDnPartial"))
    vol_ratio = float(m.get("volumeRatio", 1.0) or 1.0)
    mom_abs = abs(float(m.get("momentumPct", 0.0) or 0.0))

    if atr >= 0.55 or bbw >= 0.05:
        return {"name": "VOLATILE", "confidenceBoost": 0.06, "sizeMultiplier": 0.72, "strictness": "high"}
    if edge >= 3 and (trend_bias or partial_bias) and atr >= 0.05 and vol_ratio >= 0.95 and mom_abs >= 0.05:
        return {"name": "TREND", "confidenceBoost": -0.02, "sizeMultiplier": 1.1, "strictness": "low"}
    if edge <= 1.5 or mom_abs < 0.05 or atr < 0.04:
        return {"name": "RANGE", "confidenceBoost": 0.04, "sizeMultiplier": 0.86, "strictness": "medium"}
    return {"name": "NORMAL", "confidenceBoost": 0.0, "sizeMultiplier": 1.0, "strictness": "normal"}
