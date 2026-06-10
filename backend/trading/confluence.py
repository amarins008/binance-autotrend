"""Multi-factor confluence engine — professional signal synthesis."""

from trading.types import ConfluenceResult

STRONG_THRESHOLD = 6
SOFT_THRESHOLD = 4


def evaluate_confluence(
    pk: dict,
    mm: dict,
    *,
    pre_signal: str,
    pre_confidence: float,
    imbalance: float | None = None,
    order_book: dict | None = None,
    bias_signal: str | None = None,
) -> ConfluenceResult:
    """
    Score long/short confluence and resolve final signal + confidence.
    pre_signal: order-flow / momentum bias before confluence.
    """
    notes: list[str] = []
    final_signal = pre_signal
    confidence = pre_confidence
    long_score = 0
    short_score = 0

    if pk.get("trendUp"):
        long_score += 3
    elif pk.get("trendUpPartial"):
        long_score += 2
    if pk.get("trendDown"):
        short_score += 3
    elif pk.get("trendDnPartial"):
        short_score += 2

    if pk.get("macdCrossUp"):
        long_score += 2
    elif pk.get("macdBullish") and pk.get("macdBullish5m"):
        long_score += 2
    elif pk.get("macdBullish"):
        long_score += 1
    if pk.get("macdCrossDn"):
        short_score += 2
    elif pk.get("macdBearish") and not pk.get("macdBullish5m"):
        short_score += 2
    elif pk.get("macdBearish"):
        short_score += 1

    rsi = float(pk.get("rsi14", 50))
    rsi5 = float(pk.get("rsi14_5m", 50))
    if 45 <= rsi <= 70 and rsi5 >= 50:
        long_score += 1
    if 30 <= rsi <= 55 and rsi5 <= 50:
        short_score += 1
    if rsi > 78:
        long_score -= 1
        notes.append(f"RSI overbought {rsi:.1f}")
    if rsi < 22:
        short_score -= 1
        notes.append(f"RSI oversold {rsi:.1f}")

    sk, sd = float(pk.get("stochK", 50)), float(pk.get("stochD", 50))
    if sk > sd and sk < 80:
        long_score += 1
    if sk < sd and sk > 20:
        short_score += 1

    if pk.get("priceNearBbLower") and not pk.get("priceAboveBbMid"):
        long_score += 2
    if pk.get("priceNearBbUpper") and pk.get("priceAboveBbMid"):
        short_score += 2
    if pk.get("bbSqueeze"):
        if float(mm.get("momentumPct", 0)) > 0:
            long_score += 1
        elif float(mm.get("momentumPct", 0)) < 0:
            short_score += 1
        notes.append("BB squeeze — breakout imminent")

    if pk.get("priceAboveVwap"):
        long_score += 1
    else:
        short_score += 1

    vol_ratio = float(pk.get("volumeRatio", mm.get("volumeRatio", 1.0)))
    if pk.get("breakoutUp") and vol_ratio > 1.2:
        long_score += 2
    if pk.get("breakoutDown") and vol_ratio > 1.2:
        short_score += 2

    cvd = float(pk.get("cvd", 0.0))
    if cvd > 0.05:
        long_score += 1
    elif cvd < -0.05:
        short_score += 1

    if imbalance is not None and imbalance > 0.04:
        long_score += 1
    if imbalance is not None and imbalance < -0.04:
        short_score += 1

    if float(mm.get("momentumPct", 0)) > 0.1:
        long_score += 1
    if float(mm.get("momentumPct", 0)) < -0.1:
        short_score += 1

    check_sig = bias_signal or final_signal
    if pk.get("nearResistance") and check_sig == "LONG":
        long_score -= 1
        notes.append("Near resistance — caution on LONG")
    if pk.get("nearSupport") and check_sig == "SHORT":
        short_score -= 1
        notes.append("Near support — caution on SHORT")

    if pk.get("highLiquiditySession"):
        if long_score > short_score:
            long_score += 1
        elif short_score > long_score:
            short_score += 1
    else:
        notes.append(f"Session: {pk.get('session', '?')} (low liquidity)")

    if pk.get("ema200Ready"):
        if pk.get("priceAboveEma200"):
            long_score += 2
        else:
            short_score += 2
        if check_sig == "LONG" and not pk.get("priceAboveEma200"):
            long_score -= 1
            notes.append("Below EMA200 — counter-macro LONG")
        if check_sig == "SHORT" and pk.get("priceAboveEma200"):
            short_score -= 1
            notes.append("Above EMA200 — counter-macro SHORT")

    if isinstance(order_book, dict) and order_book.get("icebergRisk"):
        long_score -= 1
        short_score -= 1
        notes.append("Iceberg/spoof risk in book — reduce conviction")

    atr_pct = float(pk.get("atrPct", 0))
    if atr_pct < 0.025:
        long_score -= 2
        short_score -= 2
        notes.append("Volatility too low (chop risk)")
    elif atr_pct > 0.8:
        long_score -= 1
        short_score -= 1
        notes.append("Extreme volatility — reduce size")

    if long_score >= STRONG_THRESHOLD and long_score >= short_score + 2:
        final_signal = "LONG"
        confidence = max(confidence, min(0.95, 0.62 + 0.025 * long_score))
    elif short_score >= STRONG_THRESHOLD and short_score >= long_score + 2:
        final_signal = "SHORT"
        confidence = max(confidence, min(0.95, 0.62 + 0.025 * short_score))
    elif pre_signal == "LONG" and long_score >= SOFT_THRESHOLD and long_score >= short_score + 1:
        final_signal = "LONG"
        confidence = max(pre_confidence, min(0.88, 0.60 + 0.035 * long_score))
        notes.append("Confluence soft-confirm LONG")
    elif pre_signal == "SHORT" and short_score >= SOFT_THRESHOLD and short_score >= long_score + 1:
        final_signal = "SHORT"
        confidence = max(pre_confidence, min(0.88, 0.60 + 0.035 * short_score))
        notes.append("Confluence soft-confirm SHORT")
    elif (
        pre_signal in ("LONG", "SHORT")
        and max(long_score, short_score) >= 3
        and abs(long_score - short_score) >= 1
    ):
        if long_score > short_score:
            final_signal = "LONG"
            confidence = max(0.64, min(0.82, pre_confidence + 0.025 * (long_score - short_score)))
            notes.append("Confluence lite LONG")
        else:
            final_signal = "SHORT"
            confidence = max(0.64, min(0.82, pre_confidence + 0.025 * (short_score - long_score)))
            notes.append("Confluence lite SHORT")
    else:
        final_signal = "WAIT"
        confidence = min(confidence, 0.50)

    if final_signal in ("LONG", "SHORT") and long_score >= 4 and short_score >= 4:
        if abs(long_score - short_score) < 2:
            final_signal = "WAIT"
            confidence = min(confidence, 0.48)
            notes.append("Chop: balanced confluence — stand aside")

    notes.append(
        f"Score L/S={long_score}/{short_score} | MACD={'↑' if pk.get('macdBullish') else '↓'} | "
        f"BB%B={float(pk.get('bbPctB', 0.5)):.2f} | VWAP={'↑' if pk.get('priceAboveVwap') else '↓'}"
    )

    return ConfluenceResult(
        signal=final_signal,
        confidence=round(confidence, 3),
        long_score=long_score,
        short_score=short_score,
        notes=notes,
    )
