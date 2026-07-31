"""Multi-factor confluence engine — professional signal synthesis."""

import time

from trading.types import ConfluenceResult
from trading.tradingview_mcp import get_tv_mcp

STRONG_THRESHOLD = 7  # Increased from 6 to 7 for stronger signal requirements
SOFT_THRESHOLD = 5   # Increased from 4 to 5 for more selective entries


def evaluate_confluence(
    pk: dict,
    mm: dict,
    *,
    pre_signal: str,
    pre_confidence: float,
    imbalance: float | None = None,
    order_book: dict | None = None,
    bias_signal: str | None = None,
    loss_streak: int = 0,
    cfg: dict | None = None,
    symbol: str = "",
) -> ConfluenceResult:
    """
    Score long/short confluence and resolve final signal + confidence.
    pre_signal: order-flow / momentum bias before confluence.
    loss_streak: current consecutive loss count for adaptive scoring.
    cfg: configuration dict for TradingView integration.
    symbol: symbol name for TradingView API calls.
    """
    notes: list[str] = []
    final_signal = pre_signal
    confidence = pre_confidence
    long_score = 0
    short_score = 0
    
    # TradingView secondary confirmation + exhaustion protection
    tv_boost = 0.0
    tv_signal_val = ""
    tv_conf_val = 0.0
    tv_strength_val = 0.0
    if cfg and symbol and bool(cfg.get("tradingviewEnabled", False)):
        try:
            tv_client = get_tv_mcp(cfg)
            tv_result = tv_client.get_signal(symbol, pre_signal, pre_confidence)
            if tv_result:
                tv_signal_val = tv_result.signal.value if tv_result.signal else ""
                tv_conf_val = tv_result.confidence
                tv_strength_val = float(tv_result.metadata.get("strength", 0.0) or 0.0)
                # --- Protection 1: Signal Age Gate ---
                # If TV signal is stale (> 120s old), zero the boost entirely
                tv_age_sec = time.time() - tv_result.timestamp
                stale_limit = float(cfg.get("tvStaleEntrySec", 120) or 120)
                if tv_age_sec > stale_limit:
                    tv_boost = 0.0
                    notes.append(f"TV stale ({tv_age_sec:.0f}s > {stale_limit:.0f}s) — boost zeroed")
                else:
                    tv_boost = tv_client.confirm_signal(tv_result, pre_signal)
                    
                    # --- Protection 2: Oscillator Exhaustion Detection ---
                    # If TV says LONG but oscillators show SELL > BUY, signal is likely exhausted
                    tv_sig_val = tv_result.signal.value.upper()
                    osc = tv_result.metadata.get("oscillators", {})
                    osc_buy = int(osc.get("BUY", 0))
                    osc_sell = int(osc.get("SELL", 0))
                    # RSI/STOCH/CCI live under oscillators["COMPUTE"] as string signals (BUY/SELL/NEUTRAL)
                    osc_compute = osc.get("COMPUTE", {}) if isinstance(osc, dict) else {}
                    rsi_val = str(osc_compute.get("RSI", "NEUTRAL")).upper()
                    stoch_val = str(osc_compute.get("STOCH.K", "NEUTRAL")).upper()
                    cci_val = str(osc_compute.get("CCI", "NEUTRAL")).upper()
                    
                    ep_base = float(cfg.get("tvExhaustionPenalty", 0.03) or 0.03)
                    exhaustion_penalty = 0.0
                    exhaustion_reasons = []
                    
                    if tv_sig_val == "LONG":
                        if rsi_val == "SELL":
                            exhaustion_penalty += ep_base
                            exhaustion_reasons.append(f"RSI={rsi_val}")
                        if stoch_val == "SELL":
                            exhaustion_penalty += ep_base * 0.67
                            exhaustion_reasons.append(f"STOCH={stoch_val}")
                        if cci_val == "SELL":
                            exhaustion_penalty += ep_base * 0.67
                            exhaustion_reasons.append(f"CCI={cci_val}")
                        if osc_sell > osc_buy:
                            exhaustion_penalty += ep_base * 0.67
                            exhaustion_reasons.append(f"osc_sell({osc_sell})>osc_buy({osc_buy})")
                    elif tv_sig_val == "SHORT":
                        if rsi_val == "BUY":
                            exhaustion_penalty += ep_base
                            exhaustion_reasons.append(f"RSI={rsi_val}")
                        if stoch_val == "BUY":
                            exhaustion_penalty += ep_base * 0.67
                            exhaustion_reasons.append(f"STOCH={stoch_val}")
                        if cci_val == "BUY":
                            exhaustion_penalty += ep_base * 0.67
                            exhaustion_reasons.append(f"CCI={cci_val}")
                        if osc_buy > osc_sell:
                            exhaustion_penalty += ep_base * 0.67
                            exhaustion_reasons.append(f"osc_buy({osc_buy})>osc_sell({osc_sell})")
                    
                    if exhaustion_penalty > 0:
                        tv_boost -= exhaustion_penalty
                        notes.append(f"TV exhaustion: {', '.join(exhaustion_reasons)} -{exhaustion_penalty:.3f}")
                    
                    # --- Protection 3: Signal Strength Weakening ---
                    # Check MA confirmation vs oscillators — weak alignment = weaker signal
                    ma = tv_result.metadata.get("moving_averages", {})
                    ma_buy = int(ma.get("BUY", 0))
                    ma_sell = int(ma.get("SELL", 0))
                    total_ma = ma_buy + ma_sell
                    if total_ma > 0:
                        if tv_sig_val == "LONG" and ma_buy < total_ma * 0.6:
                            weakness = 0.02
                            tv_boost -= weakness
                            notes.append(f"TV weak MA confirm LONG {ma_buy}/{total_ma} -{weakness:.3f}")
                        elif tv_sig_val == "SHORT" and ma_sell < total_ma * 0.6:
                            weakness = 0.02
                            tv_boost -= weakness
                            notes.append(f"TV weak MA confirm SHORT {ma_sell}/{total_ma} -{weakness:.3f}")
                    
                    # --- Protection 4: Momentum Weakening Detection ---
                    # Compare current signal strength vs previous — if weakening, apply penalty
                    current_strength = tv_result.metadata.get("strength", 0.5)
                    momentum = tv_client.get_signal_momentum(symbol, current_strength)
                    if momentum.get("weakening"):
                        weaken_penalty = min(0.04, abs(momentum["delta"]) * 0.5)
                        tv_boost -= weaken_penalty
                        notes.append(
                            f"TV momentum weakening: {momentum['prev_strength']:.2f}->{current_strength:.2f} "
                            f"(delta={momentum['delta']:.3f}) -{weaken_penalty:.3f}"
                        )
                    elif momentum.get("delta", 0) > 0.05:
                        # Signal strengthened — small bonus
                        boost_bonus = min(0.02, momentum["delta"] * 0.3)
                        tv_boost += boost_bonus
                        notes.append(f"TV momentum strengthened +{boost_bonus:.3f}")
                    
                    # Store current signal for future comparison
                    tv_client._track_signal_history(symbol, tv_result)
                    
                    notes.append(f"TradingView confirmation: {tv_result.signal.value} (+{tv_boost:.3f})")
        except Exception:
            # Fallback to internal only on any error
            pass

    # Adaptive thresholds based on loss streak
    adaptive_threshold_adjust = min(loss_streak * 0.5, 2.0)  # Up to 2 point increase during loss streaks
    strong_threshold = STRONG_THRESHOLD + adaptive_threshold_adjust
    soft_threshold = SOFT_THRESHOLD + adaptive_threshold_adjust

    if pk.get("trendUp"):
        long_score += 3
    elif pk.get("trendUpPartial"):
        long_score += 2
    if pk.get("trendDown"):
        short_score += 3
    elif pk.get("trendDnPartial"):
        short_score += 2

    # Reduced MACD weight to prevent over-reliance on single indicator
    if pk.get("macdCrossUp"):
        long_score += 1  # Reduced from 2
    elif pk.get("macdBullish") and pk.get("macdBullish5m"):
        long_score += 1  # Reduced from 2
    elif pk.get("macdBullish"):
        long_score += 1  # Kept at 1
    if pk.get("macdCrossDn"):
        short_score += 1  # Reduced from 2
    elif pk.get("macdBearish") and not pk.get("macdBullish5m"):
        short_score += 1  # Reduced from 2
    elif pk.get("macdBearish"):
        short_score += 1  # Kept at 1

    rsi = float(pk.get("rsi14", 50))
    rsi5 = float(pk.get("rsi14_5m", 50))
    # Tightened RSI ranges for more precise signals
    if 48 <= rsi <= 65 and rsi5 >= 52:  # Narrowed from 45-70 to 48-65
        long_score += 1
    if 35 <= rsi <= 52 and rsi5 <= 48:  # Narrowed from 30-55 to 35-52
        short_score += 1
    if rsi > 75:  # Lowered from 78
        long_score -= 2  # Increased penalty from 1 to 2
        notes.append(f"RSI overbought {rsi:.1f}")
    if rsi < 25:  # Raised from 22
        short_score -= 2  # Increased penalty from 1 to 2
        notes.append(f"RSI oversold {rsi:.1f}")

    sk, sd = float(pk.get("stochK", 50)), float(pk.get("stochD", 50))
    if sk > sd and sk < 75:  # Lowered upper bound from 80 to 75
        long_score += 1
    if sk < sd and sk > 25:  # Raised lower bound from 20 to 25
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
    # Increased volume requirement for breakout confirmation
    if pk.get("breakoutUp") and vol_ratio > 1.4:  # Raised from 1.2 to 1.4
        long_score += 2
    if pk.get("breakoutDown") and vol_ratio > 1.4:  # Raised from 1.2 to 1.4
        short_score += 2

    cvd = float(pk.get("cvd", 0.0))
    # Increased CVD threshold for stronger conviction
    if cvd > 0.08:  # Raised from 0.05
        long_score += 1
    elif cvd < -0.08:  # Lowered from -0.05
        short_score += 1

    if imbalance is not None and imbalance > 0.05:  # Raised from 0.04
        long_score += 1
    if imbalance is not None and imbalance < -0.05:  # Lowered from -0.04
        short_score += 1

    # Increased momentum threshold
    if float(mm.get("momentumPct", 0)) > 0.12:  # Raised from 0.1
        long_score += 1
    if float(mm.get("momentumPct", 0)) < -0.12:  # Lowered from -0.1
        short_score += 1

    check_sig = bias_signal or final_signal
    if pk.get("nearResistance") and check_sig == "LONG":
        long_score -= 2  # Increased penalty from 1 to 2
        notes.append("Near resistance — caution on LONG")
    if pk.get("nearSupport") and check_sig == "SHORT":
        short_score -= 2  # Increased penalty from 1 to 2
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
            long_score -= 2  # Increased penalty from 1 to 2
            notes.append("Below EMA200 — counter-macro LONG")
        if check_sig == "SHORT" and pk.get("priceAboveEma200"):
            short_score -= 2  # Increased penalty from 1 to 2
            notes.append("Above EMA200 — counter-macro SHORT")

    if isinstance(order_book, dict) and order_book.get("icebergRisk"):
        long_score -= 2  # Increased penalty from 1 to 2
        short_score -= 2  # Increased penalty from 1 to 2
        notes.append("Iceberg/spoof risk in book — reduce conviction")

    atr_pct = float(pk.get("atrPct", 0))
    # Tightened ATR bounds for better regime detection
    if atr_pct < 0.03:  # Raised from 0.025
        long_score -= 3  # Increased penalty from 2 to 3
        short_score -= 3  # Increased penalty from 2 to 3
        notes.append("Volatility too low (chop risk)")
    elif atr_pct > 0.75:  # Lowered from 0.8
        long_score -= 2  # Increased penalty from 1 to 2
        short_score -= 2  # Increased penalty from 1 to 2
        notes.append("Extreme volatility — reduce size")

    # Additional loss streak penalty
    if loss_streak >= 3:
        long_score -= min(loss_streak, 3)
        short_score -= min(loss_streak, 3)
        notes.append(f"Loss streak penalty: -{min(loss_streak, 3)}")

    # Reversal detection - avoid entries at potential reversal points
    divergence = str(pk.get("divergence", "NONE")).upper()
    if divergence == "BULLISH_DIVERGENCE" and check_sig == "LONG":
        long_score -= 3
        notes.append("Bullish divergence at LONG entry — potential reversal")
    elif divergence == "BEARISH_DIVERGENCE" and check_sig == "SHORT":
        short_score -= 3
        notes.append("Bearish divergence at SHORT entry — potential reversal")

    # Check for recent momentum shift (avoid entries right after momentum flip)
    mom_change = float(pk.get("momentumChange", 0.0) or 0.0)
    if abs(mom_change) > 0.15:  # Significant momentum change
        if (mom_change > 0 and check_sig == "SHORT") or (mom_change < 0 and check_sig == "LONG"):
            long_score -= 2
            short_score -= 2
            notes.append(f"Recent momentum shift {mom_change:.3f} — avoid counter-momentum entry")

    # Volume spike detection (avoid entries on unsustainable volume spikes)
    vol_spike = float(pk.get("volumeSpike", 1.0) or 1.0)
    if vol_spike > 3.0:  # Volume > 3x average
        if check_sig == "LONG":
            long_score -= 1
            notes.append(f"Volume spike {vol_spike:.1f}x — potential exhaustion")
        elif check_sig == "SHORT":
            short_score -= 1
            notes.append(f"Volume spike {vol_spike:.1f}x — potential exhaustion")

    # Price action at key levels (avoid entries right at resistance/support)
    bbpct = float(pk.get("bbPctB", 0.5) or 0.5)
    if bbpct > 0.92 and check_sig == "LONG":
        long_score -= 2
        notes.append(f"Price at BB upper {bbpct:.3f} — resistance zone")
    elif bbpct < 0.08 and check_sig == "SHORT":
        short_score -= 2
        notes.append(f"Price at BB lower {bbpct:.3f} — support zone")

    # Wick analysis (avoid entries after large rejection wicks)
    upper_wick = float(pk.get("upperWickPct", 0.0) or 0.0)
    lower_wick = float(pk.get("lowerWickPct", 0.0) or 0.0)
    if upper_wick > 0.4 and check_sig == "LONG":
        long_score -= 1
        notes.append(f"Large upper wick {upper_wick:.2f} — rejection above")
    if lower_wick > 0.4 and check_sig == "SHORT":
        short_score -= 1
        notes.append(f"Large lower wick {lower_wick:.2f} — rejection below")

    # Volume profile analysis (avoid entries in low volume zones)
    vol_profile_zone = str(pk.get("volumeProfileZone", "NEUTRAL")).upper()
    if vol_profile_zone == "LOW_VOLUME_ZONE":
        long_score -= 2
        short_score -= 2
        notes.append("Low volume zone — poor liquidity")
    elif vol_profile_zone == "HIGH_VOLUME_NODE":
        # High volume nodes are good for entries (support/resistance)
        if check_sig == "LONG":
            long_score += 1
            notes.append("High volume node — support zone")
        elif check_sig == "SHORT":
            short_score += 1
            notes.append("High volume node — resistance zone")

    # Volume delta analysis (buying/selling pressure)
    vol_delta = float(pk.get("volumeDelta", 0.0) or 0.0)
    if vol_delta > 0.1 and check_sig == "LONG":
        long_score += 1  # Strong buying pressure supports LONG
        notes.append(f"Positive volume delta {vol_delta:.3f}")
    elif vol_delta < -0.1 and check_sig == "SHORT":
        short_score += 1  # Strong selling pressure supports SHORT
        notes.append(f"Negative volume delta {vol_delta:.3f}")
    elif (vol_delta > 0.1 and check_sig == "SHORT") or (vol_delta < -0.1 and check_sig == "LONG"):
        # Conflicting volume delta
        long_score -= 1
        short_score -= 1
        notes.append(f"Conflicting volume delta {vol_delta:.3f}")

    # Time-based candle patterns (avoid entries during uncertain candle formations)
    candle_pattern = str(pk.get("candlePattern", "NONE")).upper()
    if candle_pattern in ("DOJI", "INVERTED_HAMMER", "HANGING_MAN"):
        # Indecisive candles - reduce confidence
        long_score -= 1
        short_score -= 1
        notes.append(f"Indecisive candle {candle_pattern}")
    elif candle_pattern == "HAMMER":
        # Hammer: 33.3% WR historically — strong penalty
        long_score -= 2
        short_score -= 2
        notes.append(f"Weak hammer pattern (33% WR)")
    elif candle_pattern == "ENGULFING_BULLISH" and check_sig == "LONG":
        long_score += 2  # Bullish engulfing supports LONG
        notes.append("Bullish engulfing pattern")
    elif candle_pattern == "ENGULFING_BEARISH" and check_sig == "SHORT":
        short_score += 1  # Bearish engulfing: 29% WR — reduced from +2
        notes.append("Bearish engulfing pattern (weak 29% WR)")

    if long_score >= strong_threshold and long_score >= short_score + 2:
        final_signal = "LONG"
        confidence = max(confidence, min(0.95, 0.62 + 0.025 * long_score))
    elif short_score >= strong_threshold and short_score >= long_score + 2:
        final_signal = "SHORT"
        confidence = max(confidence, min(0.95, 0.62 + 0.025 * short_score))
    elif pre_signal == "LONG" and long_score >= soft_threshold and long_score >= short_score + 1:
        final_signal = "LONG"
        confidence = max(pre_confidence, min(0.88, 0.60 + 0.035 * long_score))
        notes.append("Confluence soft-confirm LONG")
    elif pre_signal == "SHORT" and short_score >= soft_threshold and short_score >= long_score + 1:
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

    # Apply TradingView confidence boost/penalty if available
    if tv_boost > 0:
        confidence = min(0.95, confidence + tv_boost)
        notes.append(f"TV confirm boost: +{tv_boost:.3f}")
    elif tv_boost < 0:
        confidence = max(0.40, confidence + tv_boost)
        notes.append(f"TV conflict penalty: {tv_boost:.3f}")

    return ConfluenceResult(
        signal=final_signal,
        confidence=round(confidence, 3),
        long_score=long_score,
        short_score=short_score,
        notes=notes,
        tv_signal=tv_signal_val,
        tv_confidence=tv_conf_val,
        tv_strength=tv_strength_val,
    )
