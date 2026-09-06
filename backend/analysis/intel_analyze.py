"""intel_analyze — order-flow + multi-TF confluence signal engine.

Migrated verbatim from main.py (leaf module). Data-layer calls
(_cached_klines/_data_get/_INTEL_CACHE/_INTEL_CACHE_TTL/AUTO_TRADE) route
through lazy `import main as _main` so timeouts, retries, cache objects and
config are EXACTLY main's runtime instances — zero behavioral delta.
"""

import asyncio
import copy
import time

from fastapi import HTTPException

from exchange.futures_orders import _normalize_symbol
from indicators import (
    _atr_series,
    _bollinger,
    _cvd_delta,
    _detect_market_session,
    _ema,
    _ema_series,
    _macd,
    _rsi,
    _stochastic_rsi,
    _vwap,
)
from schemas import IntelAnalyzeRequest
from trading.config import apply_autotrade_defaults


# Lazy imports (no import cycle): avoid pulling analysis.direction_bias at
# module import time since it imports main lazily on call. directionBias is
# observational only — it never gates the entry pipeline.
def _direction_bias(symbol: str) -> dict:
    from analysis.direction_bias import detect_direction_bias
    return detect_direction_bias(symbol)


def _main():
    import main as m
    return m


def _decision_data_layers(
    *,
    symbol: str,
    signal: str,
    confidence: float,
    setup: str,
    long_score: float,
    short_score: float,
    momentum: dict,
    precision: dict,
    execution: dict,
    order_book: dict | None,
    candle_ctx: dict | None,
    notes: list[str],
) -> dict:
    spread_bps = execution.get("spreadBps") if isinstance(execution, dict) else None
    funding_rate = execution.get("lastFundingRate") if isinstance(execution, dict) else None
    atr_pct = precision.get("atrPct") if isinstance(precision, dict) else None
    imbalance = order_book.get("imbalance") if isinstance(order_book, dict) else None
    score_gap = float(long_score or 0) - float(short_score or 0)
    candle_ok = bool(isinstance(candle_ctx, dict) and candle_ctx.get("ok"))
    candle_bias = float(candle_ctx.get("bias", 0.0) or 0.0) if isinstance(candle_ctx, dict) else 0.0

    guards = []
    if spread_bps is not None:
        guards.append({
            "name": "spread",
            "state": "caution" if float(spread_bps) > 12 else "ok",
            "value": round(float(spread_bps), 4),
            "unit": "bps",
        })
    if atr_pct is not None:
        atr_value = float(atr_pct)
        guards.append({
            "name": "volatility",
            "state": "block_bias" if atr_value > 0.8 or atr_value < 0.025 else "ok",
            "value": round(atr_value, 4),
            "unit": "pct",
        })
    if funding_rate is not None:
        guards.append({
            "name": "funding",
            "state": "caution" if abs(float(funding_rate)) > 0.003 else "ok",
            "value": round(float(funding_rate), 6),
        })

    return {
        "schema": "hermes-decision-data-v1",
        "symbol": symbol,
        "policy": {
            "marketCore": "primary_signal_source",
            "riskGuards": "can_block_or_reduce_only",
            "newsSentiment": "guard_only_never_opens_trade",
            "learning": "feature_quality_weighting",
        },
        "marketCore": {
            "signal": signal,
            "confidence": round(float(confidence or 0.0), 3),
            "setup": setup,
            "scoreGap": round(score_gap, 4),
            "longScore": round(float(long_score or 0.0), 4),
            "shortScore": round(float(short_score or 0.0), 4),
            "momentumPct": round(float(momentum.get("momentumPct", 0.0) or 0.0), 4),
            "volumeRatio": round(float(momentum.get("volumeRatio", 1.0) or 1.0), 4),
            "trend": "UP" if precision.get("trendUp") else "DOWN" if precision.get("trendDown") else "MIXED",
        },
        "riskGuards": {
            "guards": guards,
            "orderBookImbalance": round(float(imbalance), 6) if imbalance is not None else None,
            "decisionImpact": "block_or_reduce_only",
        },
        "patternContext": {
            "enabled": candle_ok,
            "decisionImpact": "confidence_tuning_only",
            "bias": round(candle_bias, 6),
            "tags": (candle_ctx.get("tags", []) if isinstance(candle_ctx, dict) else [])[:8],
        },
        "newsSentimentGuard": {
            "enabled": False,
            "status": "not_wired",
            "decisionImpact": "none_until_news_agent_is_connected",
            "rule": "news may pause, reduce size, or raise confidence threshold; it must not open trades",
        },
        "learningQuality": {
            "status": "record_for_reward_scoring",
            "features": ["marketCore", "riskGuards", "patternContext", "newsSentimentGuard"],
            "rule": "features that repeatedly precede losses should lose weight or become guards",
        },
        "summary": notes[:4],
    }

async def intel_analyze(req: IntelAnalyzeRequest):
    symbol = _normalize_symbol(req.symbol)
    req_cfg = req.model_dump() if hasattr(req, "model_dump") else dict(req)
    vision = None

    # Return cached result if fresh enough (saves CPU on repeated calls)
    now_ts = time.time()
    if symbol in _main()._INTEL_CACHE:
        cached_ts, cached_result = _main()._INTEL_CACHE[symbol]
        if now_ts - cached_ts < _main()._INTEL_CACHE_TTL:
            return cached_result

    async def _depth_orderflow():
        try:
            res = await _main()._data_get(f"/fapi/v1/depth?symbol={symbol}&limit=50")
            if res.status_code >= 400:
                return None, None
            d = res.json()
            bids = d.get("bids", [])
            asks = d.get("asks", [])
            bid_notional = sum(float(p) * float(q) for p, q in bids)
            ask_notional = sum(float(p) * float(q) for p, q in asks)
            imbalance = (bid_notional - ask_notional) / max(bid_notional + ask_notional, 1.0)

            # Detect large walls (top 5% by qty)
            bid_qtys = sorted([float(q) for _, q in bids], reverse=True)
            ask_qtys = sorted([float(q) for _, q in asks], reverse=True)
            wall_threshold_bid = bid_qtys[max(0, len(bid_qtys) // 20)] if bid_qtys else 0
            wall_threshold_ask = ask_qtys[max(0, len(ask_qtys) // 20)] if ask_qtys else 0

            # Iceberg detection: many small orders clustered at same price level
            bid_price_clusters = {}
            for p, q in bids:
                px = round(float(p), 2)
                bid_price_clusters[px] = bid_price_clusters.get(px, 0) + float(q)
            ask_price_clusters = {}
            for p, q in asks:
                px = round(float(p), 2)
                ask_price_clusters[px] = ask_price_clusters.get(px, 0) + float(q)

            max_bid_cluster = max(bid_price_clusters.values()) if bid_price_clusters else 0
            max_ask_cluster = max(ask_price_clusters.values()) if ask_price_clusters else 0
            avg_bid_qty = bid_notional / max(len(bids), 1) / max(float(bids[0][0]) if bids else 1, 1e-9)
            avg_ask_qty = ask_notional / max(len(asks), 1) / max(float(asks[0][0]) if asks else 1, 1e-9)
            iceberg_risk = max_bid_cluster > avg_bid_qty * 8 or max_ask_cluster > avg_ask_qty * 8

            order_book = {
                "bidNotional": bid_notional,
                "askNotional": ask_notional,
                "imbalance": imbalance,
                "icebergRisk": iceberg_risk,
            }
            return order_book, imbalance
        except Exception:
            return None, None

    async def _microstructure():
        try:
            res_b, res_p = await asyncio.gather(
                _main()._data_get(f"/fapi/v1/ticker/bookTicker?symbol={symbol}"),
                _main()._data_get(f"/fapi/v1/premiumIndex?symbol={symbol}"),
            )
            bid = ask = mark = None
            last_funding = None
            spread_bps = None
            if res_b.status_code < 400:
                jb = res_b.json()
                bid = float(jb.get("bidPrice", 0) or 0)
                ask = float(jb.get("askPrice", 0) or 0)
            if res_p.status_code < 400:
                jp = res_p.json()
                mark = float(jp.get("markPrice", 0) or 0)
                if jp.get("lastFundingRate") is not None:
                    last_funding = float(jp["lastFundingRate"])
            if bid and ask and bid > 0 and ask > 0:
                mid = (bid + ask) / 2
                spread_bps = ((ask - bid) / max(mid, 1e-9)) * 10000
            return {
                "bid": bid,
                "ask": ask,
                "mark": mark,
                "spreadBps": spread_bps,
                "lastFundingRate": last_funding,
            }
        except Exception:
            return {
                "bid": None,
                "ask": None,
                "mark": None,
                "spreadBps": None,
                "lastFundingRate": None,
            }

    # Pre-fetch klines once and share across momentum + precision to avoid duplicate requests
    rows_1m = await _main()._cached_klines(symbol, "1m", 150)  # 150 sufficient for EMA200 approx

    async def _dir_bias():
        try:
            # Timeout guard: if M15/M30 klines stall under rate-limit, degrade
            # to NEUTRAL instead of delaying/cancelling the whole gather.
            return await asyncio.wait_for(_direction_bias(symbol), timeout=6.0)
        except Exception:
            return {
                "ok": False,
                "ts": time.time(),
                "bias": "NEUTRAL",
                "strength": 0.0,
                "regime": "MIXED",
                "timeframes": {},
                "entry": {"keyword": "low_data", "price": None, "emaZonePrice": None, "pullbackDistAtr": 0.0, "action": "wait"},
                "notes": ["direction_bias unavailable"],
            }

    mm, pk, depth_out, execution, candle_ctx, dir_bias = await asyncio.gather(
        _market_momentum(symbol, _rows=rows_1m[:60]),
        _precision_signal_pack(symbol, limit=150, _rows_1m=rows_1m),
        _depth_orderflow(),
        _microstructure(),
        _candlestick_pattern_context(symbol),
        _dir_bias(),
    )
    order_book, imbalance = depth_out

    text_signal = "WAIT"
    setup = "No clear setup"
    if imbalance is not None:
        if imbalance > 0.04:
            text_signal = "LONG"
            setup = "Order flow skewed to bids"
        elif imbalance < -0.04:
            text_signal = "SHORT"
            setup = "Order flow skewed to asks"

    final_signal = text_signal
    confidence = 0.5
    notes = [setup, f"Momentum {mm['momentumPct']:.3f}% | VolRatio {mm['volumeRatio']:.2f}"]
    volume_confirm_enabled = bool(req_cfg.get("volumeConfirmEnabled", True))
    volume_min_ratio = max(0.05, float(req_cfg.get("volumeConfirmMinRatio", 0.85) or 0.85))
    volume_strong_ratio = max(volume_min_ratio, float(req_cfg.get("volumeStrongRatio", 1.20) or 1.20))
    volume_low_penalty = max(0.0, min(0.4, float(req_cfg.get("volumeLowPenalty", 0.06) or 0.06)))
    volume_aligned_boost = max(0.0, min(0.4, float(req_cfg.get("volumeAlignedBoost", 0.05) or 0.05)))
    volume_breakout_boost = max(0.0, min(0.4, float(req_cfg.get("volumeBreakoutBoost", 0.04) or 0.04)))
    volume_ratio = float(mm.get("volumeRatio", 1.0) or 1.0)
    if vision and isinstance(vision, dict):
        v_sig = vision.get("recommendation")
        v_conf = float(vision.get("confidence", 0.5))
        v_notes = vision.get("notes") if isinstance(vision.get("notes"), list) else []
        if v_sig in ("LONG", "SHORT", "WAIT"):
            if v_sig == text_signal:
                final_signal = v_sig
                confidence = min(0.95, 0.5 + v_conf * 0.4)
            else:
                final_signal = "WAIT"
                confidence = 0.45
                notes.append("Vision and order-flow disagree")
        notes.extend([str(x) for x in v_notes[:3]])

    # Momentum + volume confirmation layer
    if volume_confirm_enabled and volume_ratio < volume_min_ratio:
        confidence = max(0.0, confidence - volume_low_penalty)
        notes.append(f"Volume weak {volume_ratio:.2f} < {volume_min_ratio:.2f}")

    if mm["momentumPct"] > 0.12 and volume_ratio >= volume_min_ratio and final_signal != "SHORT":
        final_signal = "LONG"
        confidence = min(0.92, confidence + 0.08 + volume_aligned_boost)
    elif mm["momentumPct"] < -0.12 and volume_ratio >= volume_min_ratio and final_signal != "LONG":
        final_signal = "SHORT"
        confidence = min(0.92, confidence + 0.08 + volume_aligned_boost)

    if mm["divergence"] == "BEARISH_DIVERGENCE" and final_signal == "LONG":
        final_signal = "WAIT"
        confidence = min(confidence, 0.48)
        notes.append("Bearish divergence blocks long")
    elif mm["divergence"] == "BULLISH_DIVERGENCE" and final_signal == "SHORT":
        final_signal = "WAIT"
        confidence = min(confidence, 0.48)
        notes.append("Bullish divergence blocks short")
    elif mm["divergence"] != "NONE":
        notes.append(f"Divergence: {mm['divergence']}")

    # Before strict confluence: momentum / imbalance / divergence already set a bias.
    # Previously any score < 5 forced WAIT, which blocked most alt symbols (TRUTH, etc.).
    pre_confluence_signal = final_signal
    pre_confluence_confidence = confidence

    # ── Professional confluence scoring ──────────────────────────────────────
    # Each indicator group contributes weighted points.
    # Max possible: ~18 long or short. Thresholds are tuned for earlier entries before candles fully stretch.
    long_score = 0
    short_score = 0

    # 1. Multi-TF Trend (weight 3 full / 2 partial)
    if pk["trendUp"]:
        long_score += 3
    elif pk["trendUpPartial"]:
        long_score += 2
    if pk["trendDown"]:
        short_score += 3
    elif pk["trendDnPartial"]:
        short_score += 2

    # 2. MACD (weight 2 cross / 1 bias)
    if pk["macdCrossUp"]:
        long_score += 2
    elif pk["macdBullish"] and pk["macdBullish5m"]:
        long_score += 2
    elif pk["macdBullish"]:
        long_score += 1
    if pk["macdCrossDn"]:
        short_score += 2
    elif pk["macdBearish"] and not pk["macdBullish5m"]:
        short_score += 2
    elif pk["macdBearish"]:
        short_score += 1

    # 3. RSI zones (weight 1 each)
    rsi = pk["rsi14"]
    rsi5 = pk.get("rsi14_5m", 50)
    if 45 <= rsi <= 70 and rsi5 >= 50:
        long_score += 1
    if 30 <= rsi <= 55 and rsi5 <= 50:
        short_score += 1
    # Extreme RSI blocks (overbought/oversold)
    if rsi > 78:
        long_score -= 1
        notes.append(f"RSI overbought {rsi:.1f}")
    if rsi < 22:
        short_score -= 1
        notes.append(f"RSI oversold {rsi:.1f}")

    # 4. StochRSI (weight 1)
    sk, sd = pk.get("stochK", 50), pk.get("stochD", 50)
    if sk > sd and sk < 80:
        long_score += 1
    if sk < sd and sk > 20:
        short_score += 1

    # 5. Bollinger Bands (weight 2)
    if pk["priceNearBbLower"] and pk["priceAboveBbMid"] is False:
        long_score += 2   # bounce from lower band
    if pk["priceNearBbUpper"] and pk["priceAboveBbMid"]:
        short_score += 2  # rejection from upper band
    if pk["bbSqueeze"]:
        # Squeeze: direction determined by other signals, add 1 to leading side
        if mm["momentumPct"] > 0:
            long_score += 1
        elif mm["momentumPct"] < 0:
            short_score += 1
        notes.append("BB squeeze — breakout imminent")

    # 6. VWAP (weight 1)
    if pk["priceAboveVwap"]:
        long_score += 1
    else:
        short_score += 1

    # 7. Breakout with volume (weight 2)
    if pk["breakoutUp"] and pk["volumeRatio"] >= volume_strong_ratio:
        long_score += 2
    if pk["breakoutDown"] and pk["volumeRatio"] >= volume_strong_ratio:
        short_score += 2
    if volume_confirm_enabled and volume_ratio < volume_min_ratio:
        if mm["momentumPct"] > 0:
            long_score -= 1
        elif mm["momentumPct"] < 0:
            short_score -= 1
    elif volume_confirm_enabled and volume_ratio >= volume_strong_ratio:
        if mm["momentumPct"] > 0:
            long_score += 1
        elif mm["momentumPct"] < 0:
            short_score += 1

    # 8. CVD (Cumulative Volume Delta) (weight 1)
    cvd = pk.get("cvd", 0.0)
    if cvd > 0.05:
        long_score += 1
    elif cvd < -0.05:
        short_score += 1

    # 9. Order book imbalance (weight 1)
    if imbalance is not None and imbalance > 0.04:
        long_score += 1
    if imbalance is not None and imbalance < -0.04:
        short_score += 1

    # 10. Momentum (weight 1)
    if mm["momentumPct"] > 0.1:
        long_score += 1
    if mm["momentumPct"] < -0.1:
        short_score += 1

    # 11. S/R proximity penalty
    if pk.get("nearResistance") and final_signal == "LONG":
        long_score -= 1
        notes.append("Near resistance — caution on LONG")
    if pk.get("nearSupport") and final_signal == "SHORT":
        short_score -= 1
        notes.append("Near support — caution on SHORT")

    # 12. Session quality bonus
    if pk.get("highLiquiditySession"):
        if long_score > short_score:
            long_score += 1
        elif short_score > long_score:
            short_score += 1
    else:
        # Low liquidity session: require higher bar
        notes.append(f"Session: {pk.get('session', '?')} (low liquidity)")

    # 13. Volatility filter
    if pk["atrPct"] < 0.025:
        long_score -= 2
        short_score -= 2
        notes.append("Volatility too low (chop risk)")
    elif pk["atrPct"] > 0.8:
        # Extreme volatility: reduce confidence
        long_score -= 1
        short_score -= 1
        notes.append("Extreme volatility — reduce size")

    # ── Final signal decision ─────────────────────────────────────────────────
    STRONG_THRESHOLD = 6
    SOFT_THRESHOLD   = 4

    if long_score >= STRONG_THRESHOLD and long_score >= short_score + 2:
        final_signal = "LONG"
        confidence = max(confidence, min(0.95, 0.62 + 0.025 * long_score + (volume_breakout_boost if volume_ratio >= volume_strong_ratio else 0.0)))
    elif short_score >= STRONG_THRESHOLD and short_score >= long_score + 2:
        final_signal = "SHORT"
        confidence = max(confidence, min(0.95, 0.62 + 0.025 * short_score + (volume_breakout_boost if volume_ratio >= volume_strong_ratio else 0.0)))
    elif (
        pre_confluence_signal == "LONG"
        and long_score >= SOFT_THRESHOLD
        and long_score >= short_score + 1
        and (not bool(req_cfg.get("volumeRequireForLiteEntry", True)) or volume_ratio >= volume_min_ratio)
    ):
        final_signal = "LONG"
        confidence = max(pre_confluence_confidence, min(0.88, 0.60 + 0.035 * long_score))
        notes.append("Confluence soft-confirm LONG")
    elif (
        pre_confluence_signal == "SHORT"
        and short_score >= SOFT_THRESHOLD
        and short_score >= long_score + 1
        and (not bool(req_cfg.get("volumeRequireForLiteEntry", True)) or volume_ratio >= volume_min_ratio)
    ):
        final_signal = "SHORT"
        confidence = max(pre_confluence_confidence, min(0.88, 0.60 + 0.035 * short_score))
        notes.append("Confluence soft-confirm SHORT")
    elif (
        pre_confluence_signal in ("LONG", "SHORT")
        and max(long_score, short_score) >= 3
        and abs(long_score - short_score) >= 1
        and (not bool(req_cfg.get("volumeRequireForLiteEntry", True)) or volume_ratio >= volume_min_ratio)
    ):
        # Lite confirm for alt coins with lower liquidity
        if long_score > short_score:
            final_signal = "LONG"
            confidence = max(0.64, min(0.82, pre_confluence_confidence + 0.025 * (long_score - short_score)))
            notes.append("Confluence lite LONG")
        else:
            final_signal = "SHORT"
            confidence = max(0.64, min(0.82, pre_confluence_confidence + 0.025 * (short_score - long_score)))
            notes.append("Confluence lite SHORT")
    else:
        final_signal = "WAIT"
        confidence = min(confidence, 0.50)
    notes.append(f"Score L/S={long_score}/{short_score} | MACD={'↑' if pk['macdBullish'] else '↓'} | BB%B={pk['bbPctB']:.2f} | VWAP={'↑' if pk['priceAboveVwap'] else '↓'}")

    bias = candle_ctx.get("bias", 0.0) if isinstance(candle_ctx, dict) and candle_ctx.get("ok") else 0.0
    if final_signal == "LONG":
        old_conf = confidence
        confidence = max(0.0, min(0.95, confidence + bias))
        if bias != 0:
            notes.append(f"Candles tuned LONG confidence: {old_conf:.3f} -> {confidence:.3f} (bias={bias:.4f})")
    elif final_signal == "SHORT":
        old_conf = confidence
        confidence = max(0.0, min(0.95, confidence - bias))
        if bias != 0:
            notes.append(f"Candles tuned SHORT confidence: {old_conf:.3f} -> {confidence:.3f} (bias={bias:.4f})")

    # ── TV hard-conflict gate (same logic as trading/confluence.py) ────────
    # The inline confluence scoring above has NO TV layer, so entries could
    # open against a fresh strong TV signal (GIGGLEUSDT 09:44 opened SHORT
    # vs TV LONG strength 0.92 -> SL hit in 50s). The evaluate_confluence()
    # path with this gate is only used by intel_pipeline, not by the scan/
    # entry flow that calls intel_analyze — so the gate must live here too.
    # NOTE (V13 audit): req_cfg is the API request payload (symbol only), so
    # req_cfg.get("tradingviewEnabled") was ALWAYS False here -> the gate was
    # silently skipped and intel["tv"] stayed {} -> the V12 pipeline TV gate
    # fail-opened on every entry. Load the live bot config instead.
    try:
        _live_cfg = apply_autotrade_defaults(copy.deepcopy(_main().AUTO_TRADE.get("config") or {}))
    except Exception:
        _live_cfg = req_cfg
    _tv_res = None
    if final_signal in ("LONG", "SHORT") and bool(_live_cfg.get("tradingviewEnabled", False)):
        try:
            from trading.tradingview_mcp import get_tv_mcp

            _tv_client = get_tv_mcp(_live_cfg)
            _tv_res = await asyncio.to_thread(_tv_client.get_signal, symbol, final_signal, confidence)
            if _tv_res is not None and _tv_res.signal is not None and _tv_res.signal.value not in ("WAIT", "ERROR"):
                _tv_age = time.time() - _tv_res.timestamp
                _tv_stale = float(_live_cfg.get("tvStaleEntrySec", 300) or 300)
                if _tv_age > _tv_stale:
                    try:
                        _fresh = await asyncio.to_thread(
                            _tv_client.get_signal, symbol, final_signal, confidence, True
                        )
                    except Exception:
                        _fresh = None
                    if _fresh is not None and (time.time() - _fresh.timestamp) <= _tv_stale:
                        _tv_res = _fresh
                        notes.append(f"TV refreshed on entry (was {_tv_age:.0f}s old)")
                        _tv_age = time.time() - _tv_res.timestamp
                    else:
                        notes.append(f"TV stale ({_tv_age:.0f}s > {_tv_stale:.0f}s) — treat as unavailable")
                        _tv_res = None  # stale + no refresh → treat as unavailable
                _tv_boost = _tv_client.confirm_signal(_tv_res, final_signal) if _tv_res else 0.0
                _tv_strength = float((_tv_res.metadata or {}).get("strength", 0.0) or 0.0)
                _tv_blocked_by_intel = False
                # Redundant directional-conflict guard (2026-08-15): if TV and
                # the internal signal are OPPOSITE sides (LONG vs SHORT) and TV
                # strength clears tvConflictBlockStrength, block regardless of
                # confirm_signal's return. Catches the SHORT-vs-TV=LONG bleed.
                _tv_sig_val = str(getattr(_tv_res, "signal", None) or "")
                if _tv_sig_val.startswith("TVSignal."):
                    _tv_sig_val = _tv_sig_val.split(".", 1)[1]
                _opp = {"LONG": "SHORT", "SHORT": "LONG"}
                _block_str = float(_live_cfg.get("tvConflictBlockStrength", 0.60) or 0.60)
                if (
                    final_signal in _opp
                    and _tv_sig_val == _opp[final_signal]
                    and _tv_strength >= _block_str
                ):
                    notes.append(
                        f"TV directional BLOCK: TV={_tv_sig_val} vs {final_signal} strength={_tv_strength:.2f}"
                    )
                    final_signal = "WAIT"
                    confidence = min(confidence, 0.40)
                    _tv_blocked_by_intel = True
                from trading.tv_constants import TV_SENTINEL_CHECK_THRESHOLD
                if _tv_boost <= TV_SENTINEL_CHECK_THRESHOLD:
                    notes.append(
                        f"TV hard conflict BLOCK: TV={_tv_res.signal.value} vs {final_signal} strength={_tv_strength:.2f}"
                    )
                    final_signal = "WAIT"
                    confidence = min(confidence, 0.40)
                    _tv_blocked_by_intel = True
                elif _tv_boost != 0.0:
                    notes.append(
                        f"TV {'align' if _tv_boost > 0 else 'conflict'} "
                        f"{_tv_res.signal.value} strength={_tv_strength:.2f} ({_tv_age:.0f}s old)"
                    )
            else:
                # TV disabled / ERROR / unavailable: persist an explicit marker so the
                # entry pipeline's TV-conflict gate (V12) can tell "no TV data"
                # apart from "TV aligned". Without this, a failed TV fetch
                # silently bypassed the TV gate entirely.
                # WAIT is preserved (not nulled): it is a deliberate
                # non-confirmation the pipeline must gate with tvWaitMinConf,
                # not a "no data" state that falls back to the weaker
                # tvUnavailableMinConf. (Before: WAIT -> _tv_res=None -> intel
                # tv={} -> pipeline saw "" and used 0.82; tvWaitMinConf never
                # fired -> 20/20 WAIT entries WR 20% net -0.87.)
                if _tv_res is not None and _tv_res.signal is not None and _tv_res.signal.value == "WAIT":
                    pass  # keep WAIT so _tv_snap carries it downstream
                else:
                    _tv_res = None
        except Exception:
            _tv_res = None

    # Expose the TV snapshot on the intel result so downstream layers (entry
    # pipeline TV-conflict gate, entry snapshot, dashboards) read the SAME
    # signal the intel gate evaluated — not a stale disk read.
    _tv_snap = {}
    if bool(_live_cfg.get("tradingviewEnabled", False)):
        if _tv_res is not None:
            _tv_sig_value = str(getattr(_tv_res, "signal", None) or "")
            if _tv_sig_value.startswith("TVSignal."):
                _tv_sig_value = _tv_sig_value.split(".", 1)[1]
            _tv_snap = {
                "signal": _tv_sig_value,
                "confidence": float(getattr(_tv_res, "confidence", 0.0) or 0.0),
                "strength": float((getattr(_tv_res, "metadata", None) or {}).get("strength", 0.0) or 0.0),
                "age": int(max(0.0, time.time() - getattr(_tv_res, "timestamp", time.time()))),
                "blocked": bool(_tv_blocked_by_intel),
                "status": "ok",
            }
            if _tv_snap.get("signal") == "ERROR":
                _tv_snap = {"signal": "ERROR", "strength": 0.0, "age": 0, "status": "error"}
        else:
            # TV enabled but no result (disabled, stale, error, rate-limited)
            # status=unavailable lets pipeline distinguish "no TV" from "TV WAIT"
            _tv_snap = {"signal": "", "confidence": 0.0, "strength": 0.0, "age": 9999, "status": "unavailable"}
    result = {
        "symbol": symbol,
        "signal": final_signal,
        "confidence": round(confidence, 3),
        "setup": setup,
        "notes": notes[:6],
        "vision": vision,
        "orderBook": order_book,
        "momentum": {
            "momentumPct": round(mm["momentumPct"], 4),
            "volumeRatio": round(mm["volumeRatio"], 4),
            "divergence": mm["divergence"],
            "strength": round(mm.get("strength", 0.0), 4),
        },
        "precision": {
            "rsi14": round(pk["rsi14"], 3),
            "rsi14_5m": round(pk.get("rsi14_5m", 50), 3),
            "stochK": round(pk.get("stochK", 50), 2),
            "stochD": round(pk.get("stochD", 50), 2),
            "macdLine": round(pk["macdLine"], 6),
            "macdSignal": round(pk["macdSignal"], 6),
            "macdHist": round(pk["macdHist"], 6),
            "macdBullish": pk["macdBullish"],
            "macdCrossUp": pk["macdCrossUp"],
            "macdCrossDn": pk["macdCrossDn"],
            "bbPctB": round(pk["bbPctB"], 3),
            "bbBandwidth": round(pk["bbBandwidth"], 4),
            "bbSqueeze": pk["bbSqueeze"],
            "vwap": round(pk["vwap"], 6),
            "priceAboveVwap": pk["priceAboveVwap"],
            "vwapDistancePct": round(pk["vwapDistancePct"], 4),
            "atrPct": round(pk["atrPct"], 4),
            "atrTpMult": pk.get("atrTpMult", 1.5),
            "atrSlMult": pk.get("atrSlMult", 1.0),
            "cvd": round(pk.get("cvd", 0.0), 4),
            "trendUp": pk["trendUp"],
            "trendDown": pk["trendDown"],
            "trendUpPartial": pk.get("trendUpPartial", False),
            "trendDnPartial": pk.get("trendDnPartial", False),
            "breakoutUp": pk["breakoutUp"],
            "breakoutDown": pk["breakoutDown"],
            "nearResistance": pk.get("nearResistance", False),
            "nearSupport": pk.get("nearSupport", False),
            "session": pk.get("session", "UNKNOWN"),
            "highLiquiditySession": pk.get("highLiquiditySession", False),
            "longScore": long_score,
            "shortScore": short_score,
        },
        "snapshotMeta": {"disabled": True, "provider": "none"},
        "execution": execution,
        "candles": candle_ctx,
        "tv": _tv_snap,
        "directionBias": dir_bias,
    }
    result["decisionData"] = _decision_data_layers(
        symbol=symbol,
        signal=final_signal,
        confidence=confidence,
        setup=setup,
        long_score=long_score,
        short_score=short_score,
        momentum=result["momentum"],
        precision=result["precision"],
        execution=execution,
        order_book=order_book,
        candle_ctx=candle_ctx,
        notes=notes,
    )
    # Cache result to avoid recomputing on rapid repeated calls
    _main()._INTEL_CACHE[symbol] = (time.time(), result)
    # Limit cache size to 10 symbols max
    if len(_main()._INTEL_CACHE) > 10:
        oldest = min(_main()._INTEL_CACHE, key=lambda k: _main()._INTEL_CACHE[k][0])
        del _main()._INTEL_CACHE[oldest]
    return result

async def _market_momentum(symbol: str, interval: str = "1m", limit: int = 60, _rows: list | None = None):
    symbol = _normalize_symbol(symbol)
    rows = _rows if _rows is not None else await _main()._cached_klines(symbol, interval, limit)
    if not isinstance(rows, list) or len(rows) < 25:
        return {"momentumPct": 0.0, "volumeRatio": 1.0, "divergence": "NONE", "strength": 0.0}

    closes = [float(x[4]) for x in rows]
    vols = [float(x[5]) for x in rows]
    last = closes[-1]
    ref = closes[-10]
    momentum_pct = ((last - ref) / max(ref, 1e-9)) * 100
    recent_vol = sum(vols[-6:]) / 6
    base_vol = sum(vols[-24:-6]) / max(len(vols[-24:-6]), 1)
    volume_ratio = recent_vol / max(base_vol, 1e-9)

    # Divergence heuristic: price up but volume contracting, or reverse.
    prev_price_slope = closes[-7] - closes[-13]
    now_price_slope = closes[-1] - closes[-7]
    prev_vol = sum(vols[-13:-7]) / 6
    now_vol = sum(vols[-7:-1]) / 6
    divergence = "NONE"
    if now_price_slope > 0 and now_vol < prev_vol * 0.92:
        divergence = "BEARISH_DIVERGENCE"
    elif now_price_slope < 0 and now_vol < prev_vol * 0.92:
        divergence = "BULLISH_DIVERGENCE"
    elif abs(now_price_slope) < abs(prev_price_slope) * 0.6 and now_vol > prev_vol * 1.15:
        divergence = "POSSIBLE_REVERSAL_BUILDUP"

    # Momentum strength: combined price move magnitude + volume confirmation.
    # 0.0 = dead market, 1.0 = strong trend with volume backing.
    mom_abs = abs(momentum_pct)
    strength = min(1.0, (mom_abs / 5.0) * min(volume_ratio, 2.0))

    return {
        "momentumPct": momentum_pct,
        "volumeRatio": volume_ratio,
        "divergence": divergence,
        "strength": round(strength, 4),
    }

async def _precision_signal_pack(symbol: str, limit: int = 200, _rows_1m: list | None = None, _rows_5m: list | None = None, _rows_15m: list | None = None):
    """
    Professional multi-timeframe signal pack.
    Indicators: EMA 9/21/50/200, RSI-14, StochRSI, MACD(12,26,9),
    Bollinger Bands(20,2), VWAP, ATR-14, Volume profile.
    Timeframes: 1m (scalp), 5m (swing), 15m (trend confirmation).
    """
    symbol = _normalize_symbol(symbol)
    b = _main()._fapi_public_data_base()
    # Use pre-fetched 1m rows if provided, otherwise fetch all three in parallel
    if _rows_1m is not None:
        r1 = _rows_1m
        r5, r15 = await asyncio.gather(
            _main()._cached_klines(symbol, "5m", 100),
            _main()._cached_klines(symbol, "15m", 60),
        )
    else:
        r1, r5, r15 = await asyncio.gather(
            _main()._cached_klines(symbol, "1m", limit),
            _main()._cached_klines(symbol, "5m", 100),
            _main()._cached_klines(symbol, "15m", 60),
        )
    if not r1 or not r5:
        raise HTTPException(status_code=502, detail="Failed to fetch klines")

    # ── 1m data ──────────────────────────────────────────────────────────────
    c1 = [float(x[4]) for x in r1]
    h1 = [float(x[2]) for x in r1]
    l1 = [float(x[3]) for x in r1]
    v1 = [float(x[5]) for x in r1]
    # taker buy base volume (index 9) for CVD approximation
    buy_v1 = [float(x[9]) for x in r1] if r1 and len(r1[0]) > 9 else v1
    sell_v1 = [max(v - bv, 0.0) for v, bv in zip(v1, buy_v1)]

    # ── 5m data ──────────────────────────────────────────────────────────────
    c5 = [float(x[4]) for x in r5]
    h5 = [float(x[2]) for x in r5]
    l5 = [float(x[3]) for x in r5]
    v5 = [float(x[5]) for x in r5]

    # ── 15m data ─────────────────────────────────────────────────────────────
    c15 = [float(x[4]) for x in r15] if r15 else []
    h15 = [float(x[2]) for x in r15] if r15 else []
    l15 = [float(x[3]) for x in r15] if r15 else []

    last = c1[-1]

    # ── EMAs ─────────────────────────────────────────────────────────────────
    ema9_1m   = _ema(c1, 9)
    ema21_1m  = _ema(c1, 21)
    ema50_1m  = _ema(c1, 50)
    ema200_1m = _ema(c1, 200)
    ema21_5m  = _ema(c5, 21)
    ema50_5m  = _ema(c5, 50)
    ema21_15m = _ema(c15, 21) if c15 else ema50_5m
    ema50_15m = _ema(c15, 50) if c15 else ema50_5m

    # ── RSI ──────────────────────────────────────────────────────────────────
    rsi14_1m = _rsi(c1, 14)
    rsi14_5m = _rsi(c5, 14)
    stoch_k, stoch_d = _stochastic_rsi(c1, 14, 14)

    # ── MACD ─────────────────────────────────────────────────────────────────
    macd_line, macd_sig, macd_hist = _macd(c1, 12, 26, 9)
    macd_line_5m, macd_sig_5m, macd_hist_5m = _macd(c5, 12, 26, 9)
    macd_bullish = macd_hist > 0 and macd_line > macd_sig
    macd_bearish = macd_hist < 0 and macd_line < macd_sig
    # Crossover: compare current vs previous histogram — reuse series instead of recomputing
    if len(c1) > 2:
        ema_f = _ema_series(c1, 12)
        ema_s = _ema_series(c1, 26)
        macd_series = [f - s for f, s in zip(ema_f, ema_s)]
        sig_series = _ema_series(macd_series, 9)
        hist_series = [m - s for m, s in zip(macd_series, sig_series)]
        prev_hist = hist_series[-2] if len(hist_series) >= 2 else 0.0
    else:
        prev_hist = 0.0
    macd_cross_up = macd_hist > 0 and prev_hist <= 0
    macd_cross_dn = macd_hist < 0 and prev_hist >= 0

    # ── Bollinger Bands ───────────────────────────────────────────────────────
    bb_upper, bb_mid, bb_lower, bb_pct_b, bb_bw = _bollinger(c1, 20, 2.0)
    bb_squeeze = bb_bw < 0.02          # volatility compression → breakout imminent
    bb_expansion = bb_bw > 0.05        # trending / high volatility
    price_above_bb_mid = last > bb_mid
    price_near_bb_lower = bb_pct_b < 0.15   # oversold zone
    price_near_bb_upper = bb_pct_b > 0.85   # overbought zone

    # ── VWAP ─────────────────────────────────────────────────────────────────
    vwap_1m = _vwap(h1[-60:], l1[-60:], c1[-60:], v1[-60:])
    price_above_vwap = last > vwap_1m
    vwap_distance_pct = ((last - vwap_1m) / max(vwap_1m, 1e-9)) * 100

    # ── ATR ───────────────────────────────────────────────────────────────────
    atr_series_1m = _atr_series(h1, l1, c1, 14)
    atr14 = atr_series_1m[-1] if atr_series_1m else 0.0
    atr_pct = (atr14 / max(last, 1e-9)) * 100
    # Dynamic TP/SL multipliers based on ATR
    atr_tp_mult = 2.0 if atr_pct > 0.15 else 1.5   # wider TP in volatile markets
    atr_sl_mult = 1.2 if atr_pct > 0.15 else 1.0

    # ── Volume analysis ───────────────────────────────────────────────────────
    vol_recent = sum(v1[-6:]) / 6
    vol_base   = sum(v1[-60:-6]) / max(len(v1[-60:-6]), 1)
    vol_ratio  = vol_recent / max(vol_base, 1e-9)
    cvd = _cvd_delta(buy_v1, sell_v1)   # positive = buyers dominating

    # ── Breakout detection (ATR-adjusted) ────────────────────────────────────
    lookback = 30
    breakout_high = max(h1[-lookback:-1])
    breakout_low  = min(l1[-lookback:-1])
    # Require close above/below (not just wick) for quality breakout
    breakout_up   = last > breakout_high and c1[-1] > c1[-2]
    breakout_down = last < breakout_low  and c1[-1] < c1[-2]

    # ── Trend alignment (multi-TF) ────────────────────────────────────────────
    trend_up_1m  = ema9_1m > ema21_1m > ema50_1m
    trend_dn_1m  = ema9_1m < ema21_1m < ema50_1m
    trend_up_5m  = ema21_5m > ema50_5m
    trend_dn_5m  = ema21_5m < ema50_5m
    trend_up_15m = ema21_15m > ema50_15m if c15 else trend_up_5m
    trend_dn_15m = ema21_15m < ema50_15m if c15 else trend_dn_5m

    # Full alignment = all 3 TF agree
    trend_up  = trend_up_1m  and trend_up_5m  and trend_up_15m
    trend_down = trend_dn_1m and trend_dn_5m  and trend_dn_15m
    # Partial alignment (2/3 TF)
    trend_up_partial  = (int(trend_up_1m) + int(trend_up_5m) + int(trend_up_15m)) >= 2
    trend_dn_partial  = (int(trend_dn_1m) + int(trend_dn_5m) + int(trend_dn_15m)) >= 2

    # ── Support / Resistance proximity ───────────────────────────────────────
    recent_highs = sorted(h1[-50:], reverse=True)[:5]
    recent_lows  = sorted(l1[-50:])[:5]
    near_resistance = any(abs(last - rh) / max(last, 1e-9) < 0.003 for rh in recent_highs)
    near_support     = any(abs(last - rl) / max(last, 1e-9) < 0.003 for rl in recent_lows)

    # ── Market session ────────────────────────────────────────────────────────
    session = _detect_market_session()
    high_liquidity_session = session in ("LONDON", "NEW_YORK")

    return {
        "last": last,
        # EMAs
        "ema9_1m": ema9_1m,
        "ema21_1m": ema21_1m,
        "ema50_1m": ema50_1m,
        "ema200_1m": ema200_1m,
        "ema21_5m": ema21_5m,
        "ema50_5m": ema50_5m,
        # RSI / StochRSI
        "rsi14": rsi14_1m,
        "rsi14_5m": rsi14_5m,
        "stochK": stoch_k,
        "stochD": stoch_d,
        # MACD
        "macdLine": macd_line,
        "macdSignal": macd_sig,
        "macdHist": macd_hist,
        "macdBullish": macd_bullish,
        "macdBearish": macd_bearish,
        "macdCrossUp": macd_cross_up,
        "macdCrossDn": macd_cross_dn,
        "macdBullish5m": macd_hist_5m > 0,
        # Bollinger
        "bbUpper": bb_upper,
        "bbMid": bb_mid,
        "bbLower": bb_lower,
        "bbPctB": bb_pct_b,
        "bbBandwidth": bb_bw,
        "bbSqueeze": bb_squeeze,
        "bbExpansion": bb_expansion,
        "priceAboveBbMid": price_above_bb_mid,
        "priceNearBbLower": price_near_bb_lower,
        "priceNearBbUpper": price_near_bb_upper,
        # VWAP
        "vwap": vwap_1m,
        "priceAboveVwap": price_above_vwap,
        "vwapDistancePct": vwap_distance_pct,
        # ATR
        "atrPct": atr_pct,
        "atrTpMult": atr_tp_mult,
        "atrSlMult": atr_sl_mult,
        # Volume / CVD
        "volumeRatio": vol_ratio,
        "cvd": cvd,
        # Breakout
        "breakoutUp": breakout_up,
        "breakoutDown": breakout_down,
        # Trend
        "trendUp": trend_up,
        "trendDown": trend_down,
        "trendUpPartial": trend_up_partial,
        "trendDnPartial": trend_dn_partial,
        # S/R
        "nearResistance": near_resistance,
        "nearSupport": near_support,
        # Session
        "session": session,
        "highLiquiditySession": high_liquidity_session,
    }

def _detect_timeframe_patterns(klines: list) -> tuple[list[str], float]:
    if len(klines) < 3:
        return [], 0.0
    c_prev = klines[-3]
    c_curr = klines[-2]
    
    o_prev = float(c_prev[1])
    h_prev = float(c_prev[2])
    l_prev = float(c_prev[3])
    c_prev_val = float(c_prev[4])
    
    o_curr = float(c_curr[1])
    h_curr = float(c_curr[2])
    l_curr = float(c_curr[3])
    c_curr_val = float(c_curr[4])
    
    body_prev = abs(c_prev_val - o_prev)
    range_prev = h_prev - l_prev
    
    body_curr = abs(c_curr_val - o_curr)
    range_curr = h_curr - l_curr
    upper_wick_curr = h_curr - max(o_curr, c_curr_val)
    lower_wick_curr = min(o_curr, c_curr_val) - l_curr
    
    tags = []
    bias = 0.0
    
    if range_curr > 0 and body_curr <= range_curr * 0.1:
        tags.append("doji")
        
    if body_curr > 0 and range_curr > 0:
        if lower_wick_curr >= body_curr * 2.0 and upper_wick_curr <= body_curr * 0.5:
            tags.append("hammer")
            bias += 0.02
            
    if body_curr > 0 and range_curr > 0:
        if upper_wick_curr >= body_curr * 2.0 and lower_wick_curr <= body_curr * 0.5:
            tags.append("shooting_star")
            bias += -0.02
            
    if c_prev_val < o_prev and c_curr_val > o_curr:
        if c_curr_val >= o_prev and o_curr <= c_prev_val and body_curr > body_prev:
            tags.append("bullish_engulfing")
            bias += 0.04
            
    if c_prev_val > o_prev and c_curr_val < o_curr:
        if c_curr_val <= o_prev and o_curr >= c_prev_val and body_curr > body_prev:
            tags.append("bearish_engulfing")
            bias += -0.04
            
    return tags, bias

async def _candlestick_pattern_context(symbol: str) -> dict:
    try:
        r5, r15 = await asyncio.gather(
            _main()._cached_klines(symbol, "5m", 10),
            _main()._cached_klines(symbol, "15m", 10),
        )
        if not r5 or len(r5) < 3 or not r15 or len(r15) < 3:
            return {"ok": False, "tags": [], "bias": 0.0, "score": 0.0}
            
        tags_5m, bias_5m = _detect_timeframe_patterns(r5)
        tags_15m, bias_15m = _detect_timeframe_patterns(r15)
        
        combined_tags = []
        for tag in tags_5m:
            combined_tags.append(f"5m_{tag}")
            if tag not in combined_tags:
                combined_tags.append(tag)
        for tag in tags_15m:
            combined_tags.append(f"15m_{tag}")
            if tag not in combined_tags:
                combined_tags.append(tag)
                
        combined_bias = (bias_5m * 0.6) + (bias_15m * 0.4)
        combined_bias = max(-0.05, min(0.05, combined_bias))
        pattern_score = round(combined_bias * 60.0, 4)
        
        return {
            "ok": True,
            "tags": combined_tags,
            "bias": round(combined_bias, 6),
            "score": pattern_score
        }
    except Exception:
        return {"ok": False, "tags": [], "bias": 0.0, "score": 0.0}
