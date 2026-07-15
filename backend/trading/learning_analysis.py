"""Pure computation helpers for learning and trade analysis.

These functions have no side effects and do not depend on app state or file I/O.
"""

from __future__ import annotations

import math


def _clamp_float(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value or 0.0)))


def _learning_propose_from_trades(symbol: str, trades: list[dict]) -> dict:
    _DEFAULT_PROPOSED = {
        "minConfidence": 0.66,
        "hybridMinScore": 0.72,
        "hybridMinEdge": 0.06,
        "adaptiveSizeBoostMaxPct": 20.0,
        "maxOpenPositions": 4,
        "takeProfitPct": 1.2,
        "stopLossPct": 0.9,
    }
    if not trades:
        return {
            "symbol": symbol,
            "trades": 0,
            "winRatePct": 0.0,
            "proposed": dict(_DEFAULT_PROPOSED),
            "reasons": ["insufficient_live_data"],
        }

    pnls = [float(t.get("_pnl", t.get("pnl", 0.0)) or 0.0) for t in trades]
    wins = sum(1 for p in pnls if p >= 0.0)
    losses = len(trades) - wins
    wr = (wins / max(len(trades), 1)) * 100.0
    pnl_sum = round(sum(pnls), 6)
    avg = pnl_sum / max(len(pnls), 1)

    # --- Per-symbol volatility calibration ---
    # Derive realized P&L volatility (std-dev) from trade outcomes.
    # High-volatility symbols need wider TP/SL; low-volatility need tighter ones.
    pnl_mean = avg
    pnl_variance = sum((p - pnl_mean) ** 2 for p in pnls) / max(len(pnls), 1)
    pnl_std = math.sqrt(pnl_variance)
    # Normalize volatility into a 0→1 scale. Reference: std 0.0→0.5 USDT maps to 0→1.
    vol_norm = _clamp_float(pnl_std / 0.5, 0.0, 1.0)

    # Win-rate based confidence tier (soft boundaries, not hard steps).
    # Maps wr [30%, 70%] → minConf [0.74, 0.62] linearly.
    wr_clamped = _clamp_float(wr, 30.0, 70.0)
    min_conf_base = 0.74 - (wr_clamped - 30.0) / (70.0 - 30.0) * (0.74 - 0.62)

    # Adjust confidence up when symbol is volatile (noisy signals → be pickier).
    min_conf = _clamp_float(min_conf_base + vol_norm * 0.04, 0.60, 0.78)

    # Avg P&L based TP/SL — continuous curve, not step function.
    # Anchor: avg=0 → tp=1.2, sl=0.9. Range: avg [-0.15, +0.20].
    avg_clamped = _clamp_float(avg, -0.15, 0.20)
    tp_base = 1.2 + avg_clamped * 1.0   # avg=+0.20 → tp≈1.40, avg=-0.15 → tp≈1.05
    sl_base = 0.9 + avg_clamped * 0.33  # avg=+0.20 → sl≈0.97, avg=-0.15 → sl≈0.85
    # Widen TP/SL for high-volatility symbols.
    tp = _clamp_float(tp_base + vol_norm * 0.25, 0.80, 1.80)
    sl = _clamp_float(sl_base + vol_norm * 0.15, 0.60, 1.40)

    # hybridMinScore: tighter when WR low (need stronger signal), looser when WR high.
    hybrid_score = _clamp_float(0.70 + (50.0 - wr_clamped) / 200.0, 0.62, 0.78)

    # hybridMinEdge: more edge required when win-rate is weak.
    hybrid_edge = _clamp_float(0.05 + (50.0 - wr_clamped) / 1000.0, 0.035, 0.075)

    # Position sizing: scale down for low WR, scale up for high WR + positive avg.
    wr_factor = (wr_clamped - 50.0) / 20.0   # [-1, +1]
    size_boost = _clamp_float(18.0 + wr_factor * 8.0 + avg * 20.0, 8.0, 35.0)

    # maxOpenPositions: conservative until WR is consistently above 52%.
    max_open = 5 if wr >= 52 else 4

    proposed = {
        "minConfidence": round(min_conf, 3),
        "hybridMinScore": round(hybrid_score, 3),
        "hybridMinEdge": round(hybrid_edge, 3),
        "adaptiveSizeBoostMaxPct": round(size_boost, 2),
        "maxOpenPositions": max_open,
        "takeProfitPct": round(tp, 3),
        "stopLossPct": round(sl, 3),
    }

    reasons = [
        f"wins={wins} losses={losses} wr={wr:.2f}%",
        f"netPnl={pnl_sum:.4f} avgPnl={avg:.4f}",
        f"pnlStd={pnl_std:.4f} volNorm={vol_norm:.3f}",
    ]

    # Pattern × P&L correlation (Reflection Agent learning)
    try:
        breakdown = _pattern_pnl_breakdown(trades)
        proposed["avoidPatterns"] = breakdown.get("loserPatterns") or []
        proposed["boostPatterns"] = breakdown.get("winnerPatterns") or []
        proposed["patternBreakdownSampleSize"] = int(breakdown.get("sampleSize", 0) or 0)
        if breakdown.get("loserPatterns"):
            reasons.append(
                f"avoidPatterns={proposed['avoidPatterns'][:5]} (loser pattern correlation)"
            )
        if breakdown.get("winnerPatterns"):
            reasons.append(
                f"boostPatterns={proposed['boostPatterns'][:5]}"
            )
    except Exception:
        pass

    return {
        "symbol": symbol,
        "trades": len(trades),
        "winRatePct": round(wr, 2),
        "realizedPnl": pnl_sum,
        "pnlStd": round(pnl_std, 4),
        "volNorm": round(vol_norm, 3),
        "proposed": proposed,
        "reasons": reasons,
        "patternBreakdown": breakdown if "breakdown" in locals() else None,
    }


def _memory_windows_from_trades(trades: list[dict], *, now_ts: int | None = None) -> dict[str, dict]:
    import time

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

    def _build(rows: list[dict], label: str) -> dict:
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

    return {
        "7d": _build([t for t in cleaned if now - int(t.get("_ts", 0) or 0) <= 7 * 86400], "7d"),
        "15d": _build([t for t in cleaned if now - int(t.get("_ts", 0) or 0) <= 15 * 86400], "15d"),
        "30d": _build([t for t in cleaned if now - int(t.get("_ts", 0) or 0) <= 30 * 86400], "30d"),
        "archive": _build([t for t in cleaned if now - int(t.get("_ts", 0) or 0) > 30 * 86400], "archive"),
        "all": _build(cleaned, "all"),
    }


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


# ---------------------------------------------------------------------------
# Pattern × P&L correlation (Reflection Agent)
# ---------------------------------------------------------------------------
# Each closed trade persists `patternTags` (e.g. ["5m_hammer", "15m_bullish_engulfing"]).
# This breakdown computes per-tag win-rate / P&L and surfaces a list of "lossy
# patterns" that the confluence engine can soft-penalize going forward.


def _pattern_pnl_breakdown(
    trades: list[dict],
    *,
    min_samples: int = 5,
    loser_wr_pct: float = 35.0,
    loser_avg_pnl: float = -0.04,
    now_ts: int | None = None,
    half_life_days: float = 30.0,
) -> dict:
    """Aggregate per-pattern win-rate / P&L with time-decay weighting.

    Args:
        trades: list of closed-trade dicts (each must have `_pnl` and `patternTags`).
        min_samples: minimum occurrences before a pattern is considered for the
            `loserPatterns` list.
        loser_wr_pct: tags with win-rate below this AND avg-pnl below
            `loser_avg_pnl` are flagged as lossy.
        loser_avg_pnl: avg-pnl threshold (USD) for lossy classification.
        now_ts: epoch seconds (defaults to time.time()).
        half_life_days: weight halflife for time-decay.

    Returns:
        {
          "patterns": {tag: {n, wins, losses, winRatePct, pnl, avgPnl, weight}},
          "loserPatterns": [tag, ...],
          "winnerPatterns": [tag, ...],
          "sampleSize": int,
          "analyzedAt": int,
        }
    """
    import math
    import time

    now = int(now_ts or time.time())
    half_life_sec = max(1.0, half_life_days * 86400.0)
    per_tag: dict[str, dict] = {}
    sample_size = 0
    for t in trades or []:
        if not isinstance(t, dict):
            continue
        try:
            pnl = float(t.get("_pnl", t.get("pnl", 0.0)) or 0.0)
            ts = int(float(t.get("_ts", t.get("closedAt", t.get("ts", 0))) or 0))
        except Exception:
            continue
        if ts <= 0:
            continue
        tags = t.get("patternTags")
        if not isinstance(tags, list) or not tags:
            continue
        # Time-decay weight (newer trades count more)
        age_sec = max(0.0, now - ts)
        weight = math.pow(0.5, age_sec / half_life_sec)
        sample_size += 1
        for raw in tags:
            if not isinstance(raw, str) or not raw:
                continue
            tag = raw.strip()
            if not tag:
                continue
            entry = per_tag.setdefault(
                tag,
                {
                    "n": 0,
                    "wins": 0,
                    "losses": 0,
                    "pnl": 0.0,
                    "weightedPnl": 0.0,
                    "weightedN": 0.0,
                    "lastTs": 0,
                },
            )
            entry["n"] += 1
            entry["pnl"] = round(entry["pnl"] + pnl, 6)
            entry["weightedPnl"] = round(entry["weightedPnl"] + pnl * weight, 6)
            entry["weightedN"] = round(entry["weightedN"] + weight, 6)
            if pnl >= 0.0:
                entry["wins"] += 1
            else:
                entry["losses"] += 1
            if ts > int(entry.get("lastTs", 0) or 0):
                entry["lastTs"] = ts

    patterns: dict[str, dict] = {}
    losers: list[str] = []
    winners: list[str] = []
    for tag, e in per_tag.items():
        n = int(e.get("n", 0) or 0)
        w = int(e.get("wins", 0) or 0)
        wr = (w / max(n, 1)) * 100.0
        weighted_n = float(e.get("weightedN", 0.0) or 0.0)
        weighted_pnl = float(e.get("weightedPnl", 0.0) or 0.0)
        avg_pnl = round(float(e.get("pnl", 0.0) or 0.0) / max(n, 1), 6)
        patterns[tag] = {
            "n": n,
            "wins": w,
            "losses": int(e.get("losses", 0) or 0),
            "winRatePct": round(wr, 2),
            "pnl": round(float(e.get("pnl", 0.0) or 0.0), 6),
            "avgPnl": avg_pnl,
            "weightedPnl": round(weighted_pnl, 6),
            "weightedAvgPnl": round(weighted_pnl / max(weighted_n, 1e-9), 6),
            "lastTs": int(e.get("lastTs", 0) or 0),
        }
        if n >= min_samples and wr < loser_wr_pct and avg_pnl < loser_avg_pnl:
            losers.append(tag)
        if n >= min_samples and wr > 60.0 and avg_pnl > 0.0:
            winners.append(tag)

    # Stable ordering: most-frequent first, ties broken by win-rate
    losers.sort(key=lambda x: (-patterns[x]["n"], patterns[x]["winRatePct"]))
    winners.sort(key=lambda x: (-patterns[x]["n"], -patterns[x]["winRatePct"]))
    return {
        "patterns": patterns,
        "loserPatterns": losers,
        "winnerPatterns": winners,
        "sampleSize": sample_size,
        "analyzedAt": now,
    }


def _avoid_pattern_penalty(
    pattern_tags: list[str] | None,
    avoid_patterns: list[str] | None,
    *,
    base_penalty: float = 0.85,
    max_penalty: float = 0.92,
    enabled: bool = True,
) -> dict:
    """Compute a soft confidence penalty when current tags overlap `avoid_patterns`.

    Returns:
        {"matched": [...], "multiplier": float (≤1.0), "applied": bool}
    """
    if not enabled:
        return {"matched": [], "multiplier": 1.0, "applied": False}
    current = {str(t).strip() for t in (pattern_tags or []) if isinstance(t, str) and str(t).strip()}
    avoid = {str(t).strip() for t in (avoid_patterns or []) if isinstance(t, str) and str(t).strip()}
    if not current or not avoid:
        return {"matched": [], "multiplier": 1.0, "applied": False}
    matched = sorted(current & avoid)
    if not matched:
        return {"matched": [], "multiplier": 1.0, "applied": False}
    # 1 match → base_penalty (0.85), 2 matches → 0.85^2, etc., capped at max_penalty.
    mult = max(max_penalty, pow(base_penalty, len(matched)))
    return {"matched": matched, "multiplier": round(mult, 4), "applied": True}
