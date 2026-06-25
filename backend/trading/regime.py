"""Market regime classification for sizing and gate strictness."""
import time


def detect_market_regime(intel: dict | None, loss_streak: int = 0) -> dict:
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

    # Dynamic thresholds based on loss streak
    atr_threshold = 0.55 - (min(loss_streak, 4) * 0.05)  # Lower threshold during loss streaks
    bbw_threshold = 0.05 - (min(loss_streak, 3) * 0.01)   # More sensitive to volatility
    edge_threshold = 3.0 - (min(loss_streak, 2) * 0.3)     # Lower edge requirement during losses
    mom_threshold = 0.05 - (min(loss_streak, 2) * 0.01)    # More sensitive to momentum

    # Session-based adjustments
    now_local = time.localtime(time.time())
    hour = now_local.tm_hour
    session_multiplier = 1.0
    session_boost = 0.0

    # Asian session (00:00-08:00 UTC) - typically lower volatility
    if 0 <= hour < 8:
        session_multiplier = 0.85
        session_boost = 0.03
    # London open (08:00-12:00 UTC) - higher volatility expected
    elif 8 <= hour < 12:
        session_multiplier = 1.15
        session_boost = -0.02
    # US session overlap (12:00-16:00 UTC) - highest volatility
    elif 12 <= hour < 16:
        session_multiplier = 1.2
        session_boost = -0.03
    # US afternoon (16:00-20:00 UTC) - moderate volatility
    elif 16 <= hour < 20:
        session_multiplier = 1.05
        session_boost = -0.01
    # Asian evening (20:00-24:00 UTC) - lower volatility
    elif 20 <= hour < 24:
        session_multiplier = 0.9
        session_boost = 0.02

    # Apply session adjustments to thresholds
    atr_threshold *= session_multiplier
    mom_threshold *= session_multiplier

    if atr >= atr_threshold or bbw >= bbw_threshold:
        loss_adj = min(loss_streak * 0.02, 0.08)  # Additional caution during loss streaks
        return {
            "name": "VOLATILE",
            "confidenceBoost": 0.06 + session_boost + loss_adj,
            "sizeMultiplier": 0.72 * session_multiplier * (1.0 - min(loss_streak * 0.05, 0.2)),
            "strictness": "high"
        }
    if edge >= edge_threshold and (trend_bias or partial_bias) and atr >= 0.05 and vol_ratio >= 0.95 and mom_abs >= mom_threshold:
        loss_adj = min(loss_streak * 0.015, 0.06)  # Reduce confidence boost during losses
        return {
            "name": "TREND",
            "confidenceBoost": -0.02 + session_boost + loss_adj,
            "sizeMultiplier": 1.1 * session_multiplier * (1.0 - min(loss_streak * 0.04, 0.15)),
            "strictness": "low"
        }
    if edge <= 1.5 or mom_abs < mom_threshold or atr < 0.04:
        loss_adj = min(loss_streak * 0.02, 0.08)  # More cautious in ranging markets during losses
        return {
            "name": "RANGE",
            "confidenceBoost": 0.04 + session_boost + loss_adj,
            "sizeMultiplier": 0.86 * session_multiplier * (1.0 - min(loss_streak * 0.06, 0.25)),
            "strictness": "medium"
        }
    loss_adj = min(loss_streak * 0.015, 0.05)
    return {
        "name": "NORMAL",
        "confidenceBoost": 0.0 + session_boost + loss_adj,
        "sizeMultiplier": 1.0 * session_multiplier * (1.0 - min(loss_streak * 0.03, 0.12)),
        "strictness": "normal"
    }
