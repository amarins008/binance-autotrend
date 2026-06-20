def _ema(values: list[float], period: int):
    if not values:
        return 0.0
    k = 2 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e

def _ema_series(values: list[float], period: int) -> list[float]:
    """Return full EMA series (same length as input)."""
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out

def _rsi(values: list[float], period: int = 14):
    if len(values) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def _macd(values: list[float], fast: int = 12, slow: int = 26, signal: int = 9):
    """Return (macd_line, signal_line, histogram) using last value."""
    if len(values) < slow + signal:
        return 0.0, 0.0, 0.0
    ema_fast = _ema_series(values, fast)
    ema_slow = _ema_series(values, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    sig_line = _ema_series(macd_line, signal)
    hist = macd_line[-1] - sig_line[-1]
    return macd_line[-1], sig_line[-1], hist

def _bollinger(values: list[float], period: int = 20, std_mult: float = 2.0):
    """Return (upper, mid, lower, %B, bandwidth)."""
    if len(values) < period:
        mid = values[-1] if values else 0.0
        return mid, mid, mid, 0.5, 0.0
    window = values[-period:]
    mid = sum(window) / period
    variance = sum((x - mid) ** 2 for x in window) / period
    std = variance ** 0.5
    upper = mid + std_mult * std
    lower = mid - std_mult * std
    last = values[-1]
    bw = (upper - lower) / max(mid, 1e-9)
    pct_b = (last - lower) / max(upper - lower, 1e-9)
    return upper, mid, lower, pct_b, bw

def _vwap(highs: list[float], lows: list[float], closes: list[float], volumes: list[float]):
    """Session VWAP using last N candles."""
    if not closes:
        return closes[-1] if closes else 0.0
    tp_vol = sum(((h + l + c) / 3) * v for h, l, c, v in zip(highs, lows, closes, volumes))
    total_vol = sum(volumes)
    return tp_vol / max(total_vol, 1e-9)

def _stochastic_rsi(values: list[float], rsi_period: int = 14, stoch_period: int = 14):
    """StochRSI %K and %D — O(n) incremental RSI series."""
    if len(values) < rsi_period + stoch_period + 1:
        return 50.0, 50.0
    # Build full RSI series incrementally (single pass)
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    if len(gains) < rsi_period:
        return 50.0, 50.0
    avg_g = sum(gains[:rsi_period]) / rsi_period
    avg_l = sum(losses[:rsi_period]) / rsi_period
    rsi_vals = []
    for i in range(rsi_period, len(gains) + 1):
        if avg_l == 0:
            rsi_vals.append(100.0)
        else:
            rsi_vals.append(100 - 100 / (1 + avg_g / avg_l))
        if i < len(gains):
            avg_g = (avg_g * (rsi_period - 1) + gains[i]) / rsi_period
            avg_l = (avg_l * (rsi_period - 1) + losses[i]) / rsi_period
    if len(rsi_vals) < stoch_period:
        return 50.0, 50.0
    window = rsi_vals[-stoch_period:]
    lo, hi = min(window), max(window)
    k = (rsi_vals[-1] - lo) / max(hi - lo, 1e-9) * 100
    d = sum(rsi_vals[-3:]) / min(3, len(rsi_vals))
    return k, d

def _atr_series(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> list[float]:
    """Wilder-smoothed ATR series."""
    if len(closes) < 2:
        return [0.0]
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    if not trs:
        return [0.0]
    atr = sum(trs[:period]) / min(period, len(trs))
    out = [atr]
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
        out.append(atr)
    return out

def _detect_market_session() -> str:
    """Classify current UTC hour into trading session."""
    import datetime
    h = datetime.datetime.utcnow().hour
    if 0 <= h < 8:
        return "ASIA"
    if 8 <= h < 13:
        return "LONDON"
    if 13 <= h < 21:
        return "NEW_YORK"
    return "OVERLAP_OR_CLOSE"

def _cvd_delta(buy_vols: list[float], sell_vols: list[float]) -> float:
    """Cumulative Volume Delta: positive = buy pressure dominates."""
    if not buy_vols or not sell_vols:
        return 0.0
    recent_buy = sum(buy_vols[-10:])
    recent_sell = sum(sell_vols[-10:])
    total = recent_buy + recent_sell
    return (recent_buy - recent_sell) / max(total, 1e-9)


def _detect_pre_reversal(closes: list[float], highs: list[float], lows: list[float],
                         rsi_period: int = 14, lookback: int = 5) -> dict:
    """
    Heuristic pre-reversal detector for short-horizon entries.

    Returns a dict describing how close the current bar is to a probable
    short-term reversal — used to BLOCK entries that are about to reverse
    rather than continue.

    Fields:
      score           : 0.0..1.0 — higher = more likely reversal is imminent
      side_at_risk    : "LONG" | "SHORT" | None — which side would reverse
      reason          : short string explaining the strongest signal
      rsi             : last RSI value
      stoch_k, stoch_d: last stochastic-RSI values
      bb_pct_b        : last Bollinger %b
      atr_pct         : last ATR as % of close
    """
    out = {
        "score": 0.0,
        "side_at_risk": None,
        "reason": "",
        "rsi": 50.0,
        "stoch_k": 50.0,
        "stoch_d": 50.0,
        "bb_pct_b": 0.5,
        "atr_pct": 0.0,
    }
    if not closes or len(closes) < 20:
        return out

    last = float(closes[-1])
    out["atr_pct"] = round((_atr_series(highs, lows, closes, 14)[-1] / max(last, 1e-9)) * 100.0, 4)

    rsi_val = _rsi(closes, rsi_period)
    out["rsi"] = round(rsi_val, 2)
    k, d = _stochastic_rsi(closes, rsi_period, 14)
    out["stoch_k"] = round(k, 2)
    out["stoch_d"] = round(d, 2)

    bb_upper, bb_mid, bb_lower, bb_pct_b, bb_bw = _bollinger(closes, 20, 2.0)
    out["bb_pct_b"] = round(bb_pct_b, 4)

    score = 0.0
    reason_bits = []
    side_at_risk = None

    # 1) RSI overbought/oversold extremes → mean-reversion risk for longs/shorts.
    if rsi_val >= 75:
        score += 0.35
        side_at_risk = "LONG"
        reason_bits.append(f"rsi_overbought={rsi_val:.1f}")
    elif rsi_val <= 25:
        score += 0.35
        side_at_risk = "SHORT"
        reason_bits.append(f"rsi_oversold={rsi_val:.1f}")

    # 2) Bollinger %b stretch — price riding the band is a classic chase signal.
    if bb_pct_b >= 0.95:
        score += 0.30
        side_at_risk = side_at_risk or "LONG"
        reason_bits.append(f"bb_pct_b_high={bb_pct_b:.2f}")
    elif bb_pct_b <= 0.05:
        score += 0.30
        side_at_risk = side_at_risk or "SHORT"
        reason_bits.append(f"bb_pct_b_low={bb_pct_b:.2f}")

    # 3) Bearish/Bullish RSI divergence over the lookback window.
    if len(closes) >= lookback + 2:
        recent_closes = closes[-lookback - 1:]
        recent_lows = lows[-lookback - 1:] if lows else recent_closes
        recent_highs = highs[-lookback - 1:] if highs else recent_closes

        price_hh = recent_highs[-1] > max(recent_highs[:-1])
        rsi_hh = _rsi(recent_closes, rsi_period) < _rsi(recent_closes[:-1], rsi_period)
        if price_hh and rsi_hh:
            score += 0.35
            side_at_risk = side_at_risk or "LONG"
            reason_bits.append("bearish_divergence")

        price_ll = recent_lows[-1] < min(recent_lows[:-1])
        rsi_ll = _rsi(recent_closes, rsi_period) > _rsi(recent_closes[:-1], rsi_period)
        if price_ll and rsi_ll:
            score += 0.35
            side_at_risk = side_at_risk or "SHORT"
            reason_bits.append("bullish_divergence")

    # 4) Stochastic RSI overbought/oversold cross setup.
    if k >= 90 and d >= 90:
        score += 0.15
        side_at_risk = side_at_risk or "LONG"
        reason_bits.append(f"stoch_top={k:.0f}")
    elif k <= 10 and d <= 10:
        score += 0.15
        side_at_risk = side_at_risk or "SHORT"
        reason_bits.append(f"stoch_bottom={k:.0f}")

    # 5) Wick-rejection on the last bar (open vs close near extremes).
    try:
        if highs and lows and len(closes) >= 2:
            last_open = float(closes[-2])
            rng = max(highs[-1] - lows[-1], 1e-9)
            upper_wick = (highs[-1] - max(last, last_open)) / rng
            lower_wick = (min(last, last_open) - lows[-1]) / rng
            if upper_wick > 0.55 and last < last_open:
                score += 0.20
                side_at_risk = side_at_risk or "LONG"
                reason_bits.append(f"upper_wick={upper_wick:.2f}")
            if lower_wick > 0.55 and last > last_open:
                score += 0.20
                side_at_risk = side_at_risk or "SHORT"
                reason_bits.append(f"lower_wick={lower_wick:.2f}")
    except Exception:
        pass

    out["score"] = round(min(score, 1.0), 3)
    out["side_at_risk"] = side_at_risk
    out["reason"] = ",".join(reason_bits)
    return out
