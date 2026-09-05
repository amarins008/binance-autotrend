"""Realized-volatility TP/SL target model (Phase A).

Converts short-horizon price volatility into a per-symbol TP/SL target so
fast movers and slow drifters get matched targets instead of a fixed USDT
number. The input is the 5-minute kline list the autotrade loop already
fetches (no extra API call): we estimate the expected price move over a
5-minute window from the standard deviation of the last 15-30 minutes of
5-minute returns.
"""

from __future__ import annotations

import math

# Tunable knobs (config-overridable via cfg keys where present).
MOVE_WINDOW_5M = 5            # base kline interval in minutes
WINDOW_SHORT_BARS = 3         # 15 minutes
WINDOW_LONG_BARS = 6          # 30 minutes
MOVE_MIN_PCT = 0.02           # sanity floor (percent)
MOVE_MAX_PCT = 1.50           # sanity cap (percent)


def _rms(a: float, b: float) -> float:
    return math.sqrt((a * a + b * b) / 2.0)


def _series_std(values: list[float]) -> float:
    """Sample standard deviation (ddof=1). Returns 0.0 for <2 points."""
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    return math.sqrt(var if var >= 0 else 0.0)


def _pct_returns(closes: list[float]) -> list[float]:
    rets: list[float] = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        cur = closes[i]
        if prev > 0 and cur > 0:
            rets.append((cur - prev) / prev * 100.0)
    return rets


def estimate_5m_move_pct(
    closes: list[float],
    short_bars: int = WINDOW_SHORT_BARS,
    long_bars: int = WINDOW_LONG_BARS,
    min_pct: float = MOVE_MIN_PCT,
    max_pct: float = MOVE_MAX_PCT,
) -> dict:
    """Estimate the expected 5-minute price move (percent) from kline closes.

    RMS-combines the 15-minute (short) and 30-minute (long) sample stds of
    5-minute simple returns so short-term news spikes and longer baseline
    drift both contribute without letting a single bar dominate.
    """
    if not isinstance(closes, list) or len(closes) < 8:
        return {"movePct5m": 0.0, "ok": False, "reason": "insufficient_closes"}
    rets_all = _pct_returns(closes)
    if len(rets_all) < long_bars:
        return {"movePct5m": 0.0, "ok": False, "reason": "insufficient_returns"}
    short = _series_std(rets_all[-short_bars:])
    long = _series_std(rets_all[-long_bars:])
    move_pct = _rms(short, long)
    unbounded = move_pct
    move_pct = max(float(min_pct or 0.0), min(float(max_pct or MOVE_MAX_PCT), move_pct))
    return {
        "movePct5m": round(move_pct, 4),
        "ok": True,
        "stdShort15m": round(short, 4),
        "stdLong30m": round(long, 4),
        "unboundedMovePct5m": round(unbounded, 4),
        "closesUsed": len(closes),
    }


def series_from_klines(klines: list) -> list[float]:
    """Extract close prices from Binance 5m kline rows (index 4)."""
    closes: list[float] = []
    if isinstance(klines, list):
        for k in klines:
            try:
                closes.append(float(k[4]))
            except (TypeError, ValueError, IndexError):
                continue
    return closes