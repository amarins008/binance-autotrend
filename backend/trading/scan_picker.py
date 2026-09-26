"""scan_picker - market-scan candidate picker (extracted verbatim from main.py).

Every main-module global is reached through the lazy `_main()` bridge
(same pattern as analysis/intel_analyze) so behaviour is byte-identical.
"""
import asyncio
import copy
import json
import os
import time


def _main():
    import main as m
    return m


async def _pick_best_symbol_from_scan(cfg: dict, exclude_symbols: set[str] | None = None) -> tuple[str | None, dict | None, list[dict]]:
    candidates = await _main()._scan_market_candidates(int(cfg.get("scanTopLiquid", 30)))
    blocked_symbols = {str(s).upper().strip() for s in (exclude_symbols or set()) if str(s).strip()}
    # Final-gate rejects are short-lived runtime exclusions. The old profile
    # cooldown was persisted but not consulted here, so a symbol such as BCH
    # could be picked again every cycle after direction_bias blocked it.
    _final_rejects = _main().AUTO_TRADE.setdefault("scanFinalRejects", {})
    _now_scan = int(_main().time.time())
    for _sym, _until in list(_final_rejects.items()):
        try:
            if int(_until or 0) > _now_scan:
                blocked_symbols.add(str(_sym).upper().strip())
            else:
                _final_rejects.pop(_sym, None)
        except Exception:
            _final_rejects.pop(_sym, None)
    blocked_symbols.update(_main()._parse_symbol_whitelist(cfg.get("scanDenySymbols")))
    # 2026-08-22: also apply the capital-preservation deny list here. The
    # resume gate (_risk_cooldown_resume_ok) covers single-symbol entries, but
    # AUTO/scan mode picks its symbol via this picker, which previously only
    # consulted scanDenySymbols — so denySymbols low-caps (PUMP/LINK/ALICE/
    # BOME/WLD/RED) leaked through as fresh scan picks (17 trades post-fix).
    blocked_symbols.update(_main()._parse_symbol_whitelist(cfg.get("denySymbols")))
    # Skip symbols TradingView's scanner does not know (stock tokens, delisted
    # names): their TV signal is always empty → blind entries, and batches
    # that include them burn rate limit / used to trip the failure-disable
    # circuit. The miss list is learned + persisted by batch_fetch_signals.
    try:
        from trading.tradingview_mcp import get_tv_mcp
        tv_client = get_tv_mcp(cfg)
        blocked_symbols.update(str(s).upper().strip() for s in (tv_client._tv_missing or {}).keys())
        # Proactive skip: symbols TradingView's scanner does not know are
        # dropped before analysis entirely (no per-symbol fetch → no timeout,
        # no rate-limit burn, no fail_count). is_tv_known falls back to True
        # when the universe cache is not loaded yet, so this only ever
        # removes, never blocks everything.
        try:
            if tv_client.get_tv_universe():
                candidates = [s for s in candidates if tv_client.is_tv_known(s)]
        except Exception:
            pass
    except Exception:
        pass
    live_scan = str(cfg.get("executionMode", "") or "").upper() == "LIVE"
    if live_scan:
        blocked_symbols.update(_main()._fapi_agreement_locked_symbols())
    if blocked_symbols:
        candidates = [s for s in candidates if s not in blocked_symbols]
    if live_scan:
        day_cap = int(cfg.get("maxDailyTradesPerSymbol", _main()._DEFAULT_MAX_DAILY_TRADES_PER_SYMBOL) or _main()._DEFAULT_MAX_DAILY_TRADES_PER_SYMBOL)
        if day_cap > 0:
            candidates = [s for s in candidates if _main()._live_trades_count_today_symbol(s) < day_cap]
    wl = _main()._parse_symbol_whitelist(cfg.get("whitelistSymbols"))
    if wl:
        candidates = [s for s in candidates if s in wl]
    # Prevent the same repeatedly failing symbol from pinning the scan head.
    candidates = sorted(enumerate(candidates), key=lambda x: (_main()._scan_error_penalty(x[1]), x[0]))
    candidates = [s for _, s in candidates]
    analyze_top = max(3, int(cfg.get("scanAnalyzeTop", 8)))
    all_candidates = list(candidates)
    candidates = all_candidates[:analyze_top]
    if not candidates:
        return None, None, []
    reqs = [_main().IntelAnalyzeRequest(symbol=s) for s in candidates]
    per_symbol_timeout = float(cfg.get("scanPerSymbolTimeoutSec", 7.5) or 7.5)
    per_symbol_timeout = max(2.0, min(20.0, per_symbol_timeout))

    async def _analyze_one(req: _main().IntelAnalyzeRequest):
        try:
            return await _main().asyncio.wait_for(_main().intel_analyze(req), timeout=per_symbol_timeout)
        except _main().asyncio.TimeoutError:
            return TimeoutError(f"analyze timeout>{per_symbol_timeout:.1f}s")
        except Exception as e:
            return e

    scan_sem = _main().asyncio.Semaphore(_main().SCAN_ANALYZE_CONCURRENCY)

    async def _analyze_one_limited(req: _main().IntelAnalyzeRequest):
        async with scan_sem:
            return await _analyze_one(req)

    results = await _main().asyncio.gather(*[_analyze_one_limited(r) for r in reqs], return_exceptions=False)
    if bool(cfg.get("tradingviewEnabled", False)):
        try:
            from trading.tradingview_mcp import get_tv_mcp

            tv_client = get_tv_mcp(cfg)

            # Batch prefetch: all analyzed candidates in ONE scanner request
            # (avoids the per-symbol 429 rate limit that the per-symbol gather
            # used to hit). Coverage includes WAIT candidates too — a symbol
            # near the entry gate can flip LONG/SHORT next cycle and the
            # guardian also reads TV for open positions, so fetching only
            # current LONG/SHORT signals leaves the rest blind (this is why
            # overnight trades had 2-13h-old TV data).
            batch_symbols = [symbol for symbol, _ in zip(candidates, results)]
            try:
                open_rows = _main().AUTO_TRADE.get("openLivePositions") or []
                open_syms = _main()._open_symbols_from_positions(open_rows)
                if open_syms:
                    batch_symbols = list(dict.fromkeys(batch_symbols + sorted(open_syms)))
            except Exception:
                pass
            if batch_symbols:
                await _main().asyncio.to_thread(tv_client.batch_fetch_signals, batch_symbols)
        except Exception:
            pass
    best_sym = None
    best_intel = None
    best_score = -999.0
    long_candidates: list[tuple[float, str, dict]] = []
    short_candidates: list[tuple[float, str, dict]] = []
    near_long_candidates: list[tuple[float, str, dict]] = []
    near_short_candidates: list[tuple[float, str, dict]] = []
    soft_perf_candidates: list[tuple[float, str, dict, str]] = []
    guarded_low_conf_candidates: list[tuple[float, str, dict]] = []
    board = []
    base_min_conf = float(cfg.get("minConfidence", 0.62) or 0.62)
    session_bias = _main()._entry_session_bias(cfg)
    max_spread_bps = float(cfg.get("maxSpreadBps", 22.0) or 22.0)
    near_enabled = bool(cfg.get("scanFallbackNearEnabled", True))
    near_conf_relax = float(cfg.get("scanFallbackNearConfRelax", 0.04) or 0.04)
    near_conf_relax = max(0.0, min(0.15, near_conf_relax))
    soft_perf_enabled = bool(cfg.get("scanPerfSoftFallbackEnabled", True))
    soft_perf_reasons = {
        str(x).strip()
        for x in cfg.get("scanPerfSoftFallbackReasons", ["perf_lock_new", "perf_lock_payoff"])
        if str(x).strip()
    }
    soft_perf_conf_lift = max(0.02, min(0.12, float(cfg.get("scanPerfSoftFallbackConfLift", 0.05) or 0.05)))
    soft_perf_score_penalty = max(0.05, min(0.35, float(cfg.get("scanPerfSoftFallbackScorePenalty", 0.18) or 0.18)))

    def _canonical_perf_lock_reason(reason: str, perf: dict) -> str:
        raw = str((perf or {}).get("lockReason", "") or "").strip()
        if raw == "sustained":
            return "perf_lock_new"
        if raw in ("payoff", "early", "reward"):
            return f"perf_lock_{raw}"
        if raw:
            return f"perf_lock_{raw}"
        return str(reason or "")

    def _soft_perf_fallback_reason(reason: str, perf: dict, confidence: float = 0.0) -> str:
        canonical = _canonical_perf_lock_reason(reason, perf)
        active_lock = str(reason or "").startswith("perf_lock(")
        trades = int((perf or {}).get("trades", 0) or 0)
        win_rate = float((perf or {}).get("winRatePct", 0.0) or 0.0)
        pnl = float((perf or {}).get("pnl", 0.0) or 0.0)
        # High-confidence bypass (>=0.90) for ANY symbol — prevents missing
        # genuinely good entries while a weak symbol is in a perf cooldown.
        if confidence >= 0.90:
            return "perf_lock_high_conf"
        if not active_lock and canonical in soft_perf_reasons:
            return canonical
        if active_lock and trades >= 8 and (win_rate >= 40.0 or pnl >= -0.35):
            return "perf_lock_recovered"
        return ""

    def _soft_perf_fallback_ok(reason: str, signal: str, confidence: float, min_conf: float, spread: float, perf: dict) -> tuple[bool, str]:
        fallback_reason = _soft_perf_fallback_reason(reason, perf, confidence)
        ok = (
            soft_perf_enabled
            and bool(fallback_reason)
            and fallback_reason not in ("perf_lock_early", "perf_lock_reward")
            and signal in ("LONG", "SHORT")
            and confidence >= min(0.90, min_conf + soft_perf_conf_lift)
            and spread <= max_spread_bps
        )
        return ok, fallback_reason

    def _mark_soft_perf_pick(symbol: str, reason: str):
        for row in board:
            if str(row.get("symbol", "")).upper().strip() != symbol:
                continue
            row["qualified"] = True
            row["rejectReason"] = f"perf_soft_fallback:{reason}"
            row["softFallbackPicked"] = True
            break

    def _mark_guarded_fallback_pick(symbol: str, reason: str):
        for row in board:
            if str(row.get("symbol", "")).upper().strip() != symbol:
                continue
            row["qualified"] = True
            row["rejectReason"] = f"guarded_fallback:{reason}"
            row["guardedFallbackPicked"] = True
            break
    for sym, out in zip(candidates, results):
        if isinstance(out, Exception):
            err_text = _main()._format_loop_error(out)
            _main()._record_scan_health(sym, False, err_text)
            # -4411 during scan analyze → permanently deny this symbol
            if _main()._is_fapi_agreement_error(err_text):
                deny = set(_main()._parse_symbol_whitelist(cfg.get("scanDenySymbols")))
                if sym not in deny:
                    deny.add(sym)
                    cfg["scanDenySymbols"] = sorted(deny)
                    _main().AUTO_TRADE["config"] = _main().copy.deepcopy(cfg)
                    _main()._autotrade_log(f"scan analyze -4411: {sym} permanently denied")
            hs = _main()._scan_health_state(sym)
            board.append({
                "symbol": sym,
                "signal": "WAIT",
                "confidence": 0.0,
                "score": -999.0,
                "momentumPct": 0.0,
                "spreadBps": 0.0,
                "qualified": False,
                "rejectReason": "analyze_error",
                "adaptiveMinConf": round(base_min_conf, 4),
                "error": _main()._format_loop_error(out),
                "scanErrorStreak": int(hs.get("streak", 0)),
                "scanCooldownUntil": int(hs.get("cooldownUntil", 0)),
            })
            continue
        if not isinstance(out, dict):
            _main()._record_scan_health(sym, False, "analyze_invalid")
            hs = _main()._scan_health_state(sym)
            board.append({
                "symbol": sym,
                "signal": "WAIT",
                "confidence": 0.0,
                "score": -999.0,
                "momentumPct": 0.0,
                "spreadBps": 0.0,
                "qualified": False,
                "rejectReason": "analyze_invalid",
                "adaptiveMinConf": round(base_min_conf, 4),
                "scanErrorStreak": int(hs.get("streak", 0)),
                "scanCooldownUntil": int(hs.get("cooldownUntil", 0)),
            })
            continue
        sig = str(out.get("signal", "WAIT")).upper()
        conf = float(out.get("confidence", 0.0) or 0.0)
        ex = out.get("execution") if isinstance(out.get("execution"), dict) else {}
        spread_penalty = min(0.2, max(0.0, float(ex.get("spreadBps", 0.0) or 0.0) / 200.0))
        _mm = out.get("momentum") if isinstance(out.get("momentum"), dict) else {}
        momentum = abs(float(_mm.get("momentumPct", 0.0) or 0.0))
        qual = _main()._symbol_quality_score(sym)
        # Resolve per-symbol 3-tier policy (System -> Group -> Symbol) and
        # apply the group-specific confidence floor + long-bias to the score.
        sym_profile = _main()._symbol_effective_profile(sym, cfg)
        group_long_bias = float(sym_profile.get("scan_long_bias", 0.5))
        group_conf_floor = float(sym_profile.get("min_conf_floor", 0.50))
        score = _main()._intel_score(sym, out)
        spread_bps = float(ex.get("spreadBps", 0.0) or 0.0)
        # Adaptive min confidence: per-symbol learned confidence + session
        # shift, but use the per-group floor as the lower bound instead of
        # the global 0.45. Low-vol groups (trend-friendly) tolerate lower
        # confidence; noisy groups (low-liquidity) need higher evidence.
        # Use the PREVIOUS scan board's median (the in-flight `board` is still
        # empty at this point in the loop) so market-adaptive relaxation sees
        # real market conviction from the last cycle.
        _market_median = _main()._scan_board_median_conf(_main().AUTO_TRADE.get("scanBoard"))
        adaptive_min_conf = float(_main()._learned_min_conf(
            sym, max(base_min_conf, group_conf_floor),
            _market_median,
        ))
        adaptive_min_conf += float(session_bias.get("confidenceShift", 0.0) or 0.0)
        # Hard per-symbol floor: never allow entries below 0.60 even if the
        # group profile or learned window loosens the gate (ESPUSDT-style
        # 0.47 profiles opened too easily and ate oversized SLs).
        # Upper bound = supervisor autotune ceiling (configurable), keeping the
        # scan board consistent with the entry pipeline. Lower bound = hard
        # confidence floor (minConfidenceHardFloor, default 0.72) — the
        # 0.7-0.8 zone lost -29.11 USDT over 1,385 LIVE trades (WR 49%).
        autotune_ceiling = float(cfg.get("supervisorMinConfidenceCeiling", 0.72) or 0.80)
        conf_hard_floor = float(cfg.get("minConfidenceHardFloor", 0.72) or 0.72)
        adaptive_min_conf = max(conf_hard_floor, max(group_conf_floor, min(autotune_ceiling, adaptive_min_conf)))
        # Market-wide relaxation (Boss directive 2026-08-24): if the whole board
        # is low-conviction, ease the gate only down to the hard floor. The old
        # max(0.50, ...) relaxation could bypass minConfidenceHardFloor.
        _mkt_med = _main()._scan_board_median_conf(_main().AUTO_TRADE.get("scanBoard"))
        if _mkt_med is not None and _mkt_med > 0:
            _gap = base_min_conf - _mkt_med
            if _gap > 0.01:
                adaptive_min_conf = max(conf_hard_floor, adaptive_min_conf - min(0.06, _gap * 0.75))
        score = score + float(session_bias.get("scoreShift", 0.0) or 0.0)
        # Per-group long-bias: shift score up when signal matches the
        # group's directional preference (e.g. trend-friendly groups
        # slightly favor LONG, mean-reversion groups stay neutral). Range
        # of shift is small (-0.05 to +0.05) so it never overrides the
        # underlying intel signal — just nudges ties.
        if sig in ("LONG", "SHORT"):
            bias_delta = (group_long_bias - 0.5) * 0.10  # ±0.05 max
            if sig == "LONG":
                score += bias_delta
            else:
                score -= bias_delta
        # Stash the profile source on the board row later for dashboard.
        out.setdefault("_sym_group", sym_profile.get("group", "trend-friendly"))
        out.setdefault("_sym_source", sym_profile.get("source", "group"))
        qualified = True
        reject_reason = ""
        if sig not in ("LONG", "SHORT"):
            qualified = False
            reject_reason = "signal_wait"
        elif conf < adaptive_min_conf:
            qualified = False
            reject_reason = "low_conf"
        elif conf > float(cfg.get("maxEntryConfidence", 0.95) or 0.95):
            # Late-chase cap: only the >=0.95 zone genuinely underperforms
            # (WR 44% lifetime). 0.90-0.95 is now allowed through so high-quality
            # signals (e.g. ETH 0.918) are not needlessly rejected. Mirror the
            # pipeline gate so the board shows the same decision.
            qualified = False
            reject_reason = "conf_too_high_late"
        elif spread_bps > max_spread_bps:
            qualified = False
            reject_reason = "wide_spread"

        # TV confirmation gate (2026-08-22): a FRESH but weak TradingView signal
        # rejects the entry. Telemetry (7d) showed tvConfidence<0.7 -> net -4.78
        # USDT; only conf>=0.7 were net-positive. Only applied when TV is enabled
        # AND we have a FRESH TV snapshot (age<=tvEntryMaxAgeSec) for this symbol.
        # A stale or missing TV snapshot is NOT evidence against the trade, so we
        # don't punish on it (mirrors the comment's original intent: "otherwise
        # we don't punish"). This prevents strong primary signals (e.g. AVAX
        # 0.895, TAO 0.873) from being needlessly rejected on stale TV data.
        elif bool(cfg.get("tradingviewEnabled", False)):
            _tv = out.get("tv") if isinstance(out.get("tv"), dict) else {}
            if _tv:
                _tv_age = int(_tv.get("age", 9999) or 9999)
                _tv_conf = float(_tv.get("confidence", 0.0) or 0.0)
                _max_age = int(cfg.get("tvEntryMaxAgeSec", 30) or 30)
                _min_conf = float(cfg.get("tvEntryMinConfidence", 0.70) or 0.70)
                _tv_fresh = _tv_age <= _max_age
                # Only block on a FRESH TV snapshot that is weak. A stale TV
                # snapshot is not a reason to reject a strong primary signal.
                if _tv_fresh and _tv_conf < _min_conf:
                    qualified = False
                    reject_reason = "tv_weak"
        # LONG-specific TV strength gate (2026-09-25, tiered 2026-09-26):
        # fresh TV with strength >= tvLongMinStrength (0.90) passes at full
        # size; fresh 0.50-0.90 passes but is marked for reduced size at the
        # order path; fresh <0.50 blocks. Stale/missing TV is not evidence.
        if qualified and sig == "LONG":
            _tv = out.get("tv") if isinstance(out.get("tv"), dict) else {}
            _tier, _tier_mult = _main()._tv_long_strength_tier(_tv, cfg)
            if _tier == "blocked":
                qualified = False
                reject_reason = "long_tv_low_strength"
            elif _tier == "mid":
                out["_tvStrengthTier"] = "mid"
        # LONG + negative pattern-bias gate (2026-09-25). 90d replay:
        # patternBias>0 WR 62.8%, patternBias==0 WR 56.5%, patternBias<0 WR 42.6%.
        # A LONG against a strongly negative candle pattern is structurally
        # loss-making; block it before it opens. gate uses configurable floor.
        if qualified and sig == "LONG":
            _patt_candle = out.get("candles") if isinstance(out.get("candles"), dict) else {}
            _patt_bias = float(_patt_candle.get("bias", 0.0) or 0.0)
            _long_pb_min = float(cfg.get("longPatternBiasMin", -0.002) or -0.002)
            if _patt_bias < _long_pb_min:
                qualified = False
                reject_reason = "long_negative_pattern_bias"
        # SHORT-side candle mirror: avoid shorting into strongly bullish candle
        # structure unless the later pipeline has stronger confirmation.
        if qualified and sig == "SHORT":
            _patt_candle_short = out.get("candles") if isinstance(out.get("candles"), dict) else {}
            _patt_bias_short = float(_patt_candle_short.get("bias", 0.0) or 0.0)
            _short_pb_max = float(cfg.get("shortPatternBiasMax", 0.002) or 0.002)
            if _patt_bias_short > _short_pb_max:
                qualified = False
                reject_reason = "short_positive_pattern_bias"
        # SHORT-specific TV gate (2026-08-22): telemetry showed SHORT WR 25% /
        # net -5.07 over 7d while only TV-conf>=0.7 SHORT trades were net-positive
        # (WR 62%) and any SHORT entered while TV signal was LONG lost (WR 20%).
        # Require SHORT entries to (a) have a TV snapshot that agrees (signal in
        # LONG/SHORT/WAIT but NOT conflicting LONG) and (b) meet a higher TV
        # confidence floor than LONG.
        if qualified and sig == "SHORT":
            _tv = out.get("tv") if isinstance(out.get("tv"), dict) else {}
            if _tv:
                _tv_sig = str(_tv.get("signal", "")).upper()
                _tv_c = float(_tv.get("confidence", 0.0) or 0.0)
                _short_min_conf = float(cfg.get("shortTvMinConfidence", 0.70) or 0.70)
                # Block SHORT when TV points LONG with meaningful strength.
                # A weak TV LONG (strength < 0.45) should not block a strong
                # SHORT — TV oscillators can flicker BUY/SELL at low strength.
                _tv_strength = float(_tv.get("strength", 0.0) or 0.0)
                _tv_age = int(_tv.get("age", 9999) or 9999)
                _tv_status = str(_tv.get("status", "") or "").lower()
                _tv_fresh = (
                    not ("age" in _tv and "status" in _tv)
                    or (_tv_status == "ok" and _tv_age <= int(cfg.get("tvEntryMaxAgeSec", 30) or 30))
                )
                _short_tv_block_min_strength = float(cfg.get("shortTvBlockMinStrength", 0.45) or 0.45)
                if _tv_sig == "LONG" and _tv_fresh and _tv_strength >= _short_tv_block_min_strength:
                    qualified = False
                    reject_reason = "short_tv_conflict_long"
                # Require higher TV confidence for SHORT than the generic floor
                # Boss 2026-08-28: if the technical SHORT signal is strong & clear
                # (conf >= shortStrongMinConfidence), relax the TV gate so we still
                # enter SHORT on a decisive down-signal even without TV confirmation.
                elif _tv_sig == "SHORT":
                    # SHORT TV agrees with entry — but weak TV SHORT still loses.
                    # Stats (2026-09-24, 2523 LIVE trades): SHORT/SHORT n=347
                    # WR 42.4% -15.4 USDT overall; only tvStrength>=0.9 was
                    # net-positive (+1.66, WR 61.6%). Block weak TV SHORT signal.
                    _short_min_str = float(cfg.get("shortTvMinStrength", 0.90) or 0.90)
                    if _tv_fresh and _tv_strength < _short_min_str:
                        qualified = False
                        reject_reason = "short_tv_low_strength"
                elif _tv_sig == "WAIT":
                    # SHORT + TV=WAIT: require higher internal confidence.
                    # SHORT with TV=WAIT has WR 28% historically — TV's non-
                    # confirmation is meaningful for SHORT (bearish) signals.
                    _tv_short_wait_min = float(cfg.get("tvShortWaitMinConf", 0.88) or 0.88)
                    if conf < _tv_short_wait_min:
                        qualified = False
                        reject_reason = "short_tv_wait_low_conf"
                elif _tv_c < _short_min_conf:
                    _strong_short_min = float(cfg.get("shortStrongMinConfidence", 0.80) or 0.80)
                    if sig == "SHORT" and conf >= _strong_short_min:
                        # strong clear down-signal -> allow despite low TV conf
                        pass
                    else:
                        qualified = False
                        reject_reason = "short_tv_low_conf"
        perf_ok, perf_reason, perf = _main()._symbol_perf_gate(cfg, sym)
        soft_perf_eligible = False
        soft_perf_reason = ""
        if qualified and not perf_ok:
            soft_perf_eligible, soft_perf_reason = _soft_perf_fallback_ok(perf_reason, sig, conf, adaptive_min_conf, spread_bps, perf)
            if soft_perf_eligible:
                soft_perf_candidates.append((score - soft_perf_score_penalty, sym, out, soft_perf_reason or perf_reason or "perf_lock"))
            qualified = False
            reject_reason = perf_reason or "perf_lock"
        board.append({
            "symbol": sym,
            "signal": sig,
            "confidence": round(conf, 4),
            "score": round(score, 6),
            "learningScore": round(qual, 6),
            "momentumPct": round(momentum, 4),
            "spreadBps": round(spread_bps, 4),
            "qualified": bool(qualified),
            "rejectReason": reject_reason,
            "adaptiveMinConf": round(adaptive_min_conf, 4),
            "perfTrades": int(perf.get("trades", 0)),
            "perfWinRatePct": round(float(perf.get("winRatePct", 0.0) or 0.0), 2),
            "perfPnl": round(float(perf.get("pnl", 0.0) or 0.0), 6),
            "softFallbackEligible": bool(soft_perf_eligible),
            "scanErrorStreak": 0,
            "scanCooldownUntil": 0,
            "sessionBias": {
                "hour": int(session_bias.get("hour", 0) or 0),
                "reason": session_bias.get("reason"),
                "confidenceShift": round(float(session_bias.get("confidenceShift", 0.0) or 0.0), 4),
                "sizeMult": round(float(session_bias.get("sizeMult", 1.0) or 1.0), 4),
                "trades": int(session_bias.get("trades", 0) or 0),
                "winRatePct": round(float(session_bias.get("winRatePct", 0.0) or 0.0), 2),
                "pnl": round(float(session_bias.get("pnl", 0.0) or 0.0), 6),
                "avgAbsMovePct": round(float(session_bias.get("avgAbsMovePct", 0.0) or 0.0), 4),
            },
        })
        _main()._record_scan_health(sym, True)
        _main()._record_symbol_observation(sym, out, False, score)
        if (
            near_enabled
            and sig in ("LONG", "SHORT")
            and spread_bps <= max_spread_bps
            and conf >= max(0.45, adaptive_min_conf - near_conf_relax)
            and reject_reason in ("low_conf", "signal_wait", "")
        ):
            if sig == "LONG":
                near_long_candidates.append((score, sym, out))
            else:
                near_short_candidates.append((score, sym, out))
        if reject_reason == "low_conf" and sig in ("LONG", "SHORT") and spread_bps <= max_spread_bps:
            guarded_low_conf_candidates.append((score, sym, out))
        if not qualified:
            continue
        if sig == "LONG":
            long_candidates.append((score, sym, out))
        elif sig == "SHORT":
            short_candidates.append((score, sym, out))
        if score > best_score:
            best_score = score
            best_sym = sym
            best_intel = out
    guarded_rejects = [
        x for x in board
        if str(x.get("rejectReason", "")) in ("low_conf", "wide_spread")
        or str(x.get("rejectReason", "")).startswith("perf_lock")
    ]
    guarded_ratio = (len(guarded_rejects) / max(len(board), 1)) if board else 0.0
    should_expand_guarded_scan = (
        not (long_candidates or short_candidates)
        and len(guarded_rejects) >= int(cfg.get("scanGuardedFallbackMinLocks", 2) or 2)
        and guarded_ratio >= float(cfg.get("scanGuardedFallbackMinRatio", 0.5) or 0.5)
    )
    if should_expand_guarded_scan:
        expanded_top = max(
            analyze_top + 1,
            min(
                int(cfg.get("scanGuardedFallbackAnalyzeTop", max(analyze_top * 2, analyze_top + 4)) or (analyze_top * 2)),
                int(cfg.get("scanTopLiquid", 30) or 30),
                16,
            ),
        )
        extra_candidates = [s for s in all_candidates[analyze_top:expanded_top] if s not in set(candidates)]
        if extra_candidates:
            extra_reqs = [_main().IntelAnalyzeRequest(symbol=s) for s in extra_candidates]
            extra_results = await _main().asyncio.gather(*[_analyze_one_limited(r) for r in extra_reqs], return_exceptions=False)
            for sym, out in zip(extra_candidates, extra_results):
                if isinstance(out, Exception) or not isinstance(out, dict):
                    reason = "analyze_error" if isinstance(out, Exception) else "analyze_invalid"
                    err = _main()._format_loop_error(out) if isinstance(out, Exception) else "analyze_invalid"
                    _main()._record_scan_health(sym, False, err)
                    hs = _main()._scan_health_state(sym)
                    board.append({
                        "symbol": sym,
                        "signal": "WAIT",
                        "confidence": 0.0,
                        "score": -999.0,
                        "momentumPct": 0.0,
                        "spreadBps": 0.0,
                        "qualified": False,
                        "rejectReason": reason,
                        "adaptiveMinConf": round(base_min_conf, 4),
                        "scanErrorStreak": int(hs.get("streak", 0)),
                        "scanCooldownUntil": int(hs.get("cooldownUntil", 0)),
                        "scanExpanded": True,
                    })
                    continue
                sig = str(out.get("signal", "WAIT")).upper()
                conf = float(out.get("confidence", 0.0) or 0.0)
                ex = out.get("execution") if isinstance(out.get("execution"), dict) else {}
                momentum = abs(float(ex.get("momentumPct", 0.0) or 0.0))
                qual = _main()._symbol_quality_score(sym)
                score = _main()._intel_score(sym, out) + float(session_bias.get("scoreShift", 0.0) or 0.0)
                spread_bps = float(ex.get("spreadBps", 0.0) or 0.0)
                adaptive_min_conf = float(_main()._learned_min_conf(sym, base_min_conf, _main()._scan_board_median_conf(board))) + float(session_bias.get("confidenceShift", 0.0) or 0.0)
                # Hard per-symbol floor: same floor as the main scan board
                # (minConfidenceHardFloor, default 0.72); upper bound follows
                # the supervisor autotune ceiling.
                autotune_ceiling = float(cfg.get("supervisorMinConfidenceCeiling", 0.72) or 0.80)
                conf_hard_floor = float(cfg.get("minConfidenceHardFloor", 0.72) or 0.72)
                adaptive_min_conf = max(conf_hard_floor, min(autotune_ceiling, adaptive_min_conf))
                qualified = True
                reject_reason = ""
                if sig not in ("LONG", "SHORT"):
                    qualified = False
                    reject_reason = "signal_wait"
                elif conf < adaptive_min_conf:
                    qualified = False
                    reject_reason = "low_conf"
                elif conf > float(cfg.get("maxEntryConfidence", 0.95) or 0.95):
                    # Late-chase cap: only the >=0.95 zone genuinely underperforms
                    # (WR 44% lifetime). 0.90-0.95 is now allowed through so high-quality
                    # signals (e.g. ETH 0.918) are not needlessly rejected.
                    qualified = False
                    reject_reason = "conf_too_high_late"
                elif spread_bps > max_spread_bps:
                    qualified = False
                    reject_reason = "wide_spread"
                perf_ok, perf_reason, perf = _main()._symbol_perf_gate(cfg, sym)
                soft_perf_eligible = False
                soft_perf_reason = ""
                if qualified and not perf_ok:
                    soft_perf_eligible, soft_perf_reason = _soft_perf_fallback_ok(perf_reason, sig, conf, adaptive_min_conf, spread_bps, perf)
                    if soft_perf_eligible:
                        soft_perf_candidates.append((score - soft_perf_score_penalty, sym, out, soft_perf_reason or perf_reason or "perf_lock"))
                    qualified = False
                    reject_reason = perf_reason or "perf_lock"
                board.append({
                    "symbol": sym,
                    "signal": sig,
                    "confidence": round(conf, 4),
                    "score": round(score, 6),
                    "learningScore": round(qual, 6),
                    "momentumPct": round(momentum, 4),
                    "spreadBps": round(spread_bps, 4),
                    "qualified": bool(qualified),
                    "rejectReason": reject_reason,
                    "adaptiveMinConf": round(adaptive_min_conf, 4),
                    "perfTrades": int(perf.get("trades", 0)),
                    "perfWinRatePct": round(float(perf.get("winRatePct", 0.0) or 0.0), 2),
                    "perfPnl": round(float(perf.get("pnl", 0.0) or 0.0), 6),
                    "softFallbackEligible": bool(soft_perf_eligible),
                    "scanErrorStreak": 0,
                    "scanCooldownUntil": 0,
                    "scanExpanded": True,
                })
                _main()._record_scan_health(sym, True)
                _main()._record_symbol_observation(sym, out, False, score)
                if reject_reason == "low_conf" and sig in ("LONG", "SHORT") and spread_bps <= max_spread_bps:
                    guarded_low_conf_candidates.append((score, sym, out))
                if not qualified:
                    continue
                if sig == "LONG":
                    long_candidates.append((score, sym, out))
                elif sig == "SHORT":
                    short_candidates.append((score, sym, out))
                if score > best_score:
                    best_score = score
                    best_sym = sym
                    best_intel = out
    side_preference = str(cfg.get("scanSidePreference", "score") or "score").lower()
    if side_preference == "long":
        side_candidates = long_candidates or short_candidates
    elif side_preference == "short":
        side_candidates = short_candidates or long_candidates
    else:
        side_candidates = long_candidates + short_candidates
    if side_candidates:
        side_candidates.sort(key=lambda x: x[0], reverse=True)
        best_score, best_sym, best_intel = side_candidates[0]
    elif soft_perf_candidates:
        soft_perf_candidates.sort(key=lambda x: x[0], reverse=True)
        best_score, best_sym, best_intel, soft_reason = soft_perf_candidates[0]
        _mark_soft_perf_pick(best_sym, soft_reason)
    elif near_enabled:
        if side_preference == "long":
            near_candidates = near_long_candidates or near_short_candidates
        elif side_preference == "short":
            near_candidates = near_short_candidates or near_long_candidates
        else:
            near_candidates = near_long_candidates + near_short_candidates
        if near_candidates:
            near_candidates.sort(key=lambda x: x[0], reverse=True)
            best_score, best_sym, best_intel = near_candidates[0]
            _mark_guarded_fallback_pick(best_sym, "low_conf")
    if (not best_sym or not isinstance(best_intel, dict)) and near_enabled:
        guarded_fallback_enabled = bool(cfg.get("scanGuardedFallbackEnabled", True))
        guarded_conf_relax = float(cfg.get("scanGuardedFallbackConfRelax", max(near_conf_relax, 0.12)) or 0.12)
        guarded_conf_relax = max(0.0, min(0.20, guarded_conf_relax))
        guarded_floor = max(float(cfg.get("minConfidenceHardFloor", 0.72) or 0.72), base_min_conf - guarded_conf_relax)
        low_conf_candidates: list[tuple[float, str, dict]] = []
        if guarded_fallback_enabled and board and not any(bool(row.get("qualified")) for row in board):
            low_conf_by_symbol = {symbol: (score, intel) for score, symbol, intel in guarded_low_conf_candidates}
            for row in board:
                symbol = str(row.get("symbol", "") or "").upper().strip()
                if not symbol or str(row.get("rejectReason", "") or "") != "low_conf":
                    continue
                confidence = float(row.get("confidence", 0.0) or 0.0)
                adaptive_min_conf = float(row.get("adaptiveMinConf", base_min_conf) or base_min_conf)
                spread_bps = float(row.get("spreadBps", 0.0) or 0.0)
                if confidence < max(guarded_floor, adaptive_min_conf - guarded_conf_relax):
                    continue
                if spread_bps > max_spread_bps:
                    continue
                candidate = low_conf_by_symbol.get(symbol)
                candidate_intel = candidate[1] if candidate else None
                if not isinstance(candidate_intel, dict):
                    continue
                low_conf_candidates.append((float(row.get("score", 0.0) or 0.0), symbol, candidate_intel))
        if low_conf_candidates:
            low_conf_candidates.sort(key=lambda x: x[0], reverse=True)
            best_score, best_sym, best_intel = low_conf_candidates[0]
            _mark_guarded_fallback_pick(best_sym, "low_conf")
    if best_sym and best_intel:
        _main()._record_symbol_observation(best_sym, best_intel, True, best_score)
    # Fallback: if scan failed across the board (e.g., all analyze timeout), try the
    # least-recently failing symbol first so BTC does not pin the head forever.
    if (not best_sym or not isinstance(best_intel, dict)) and near_enabled:
        all_err = bool(board) and all(str(x.get("rejectReason", "")) == "analyze_error" for x in board)
        if all_err:
            def _mark_fallback_board(symbol: str, reason: str, intel: dict | None = None, score: float | None = None):
                for row in board:
                    if str(row.get("symbol", "")).upper().strip() != symbol:
                        continue
                    row["rejectReason"] = reason
                    if isinstance(intel, dict):
                        fb_sig2 = str(intel.get("signal", "WAIT")).upper()
                        fb_conf2 = float(intel.get("confidence", 0.0) or 0.0)
                        fb_ex2 = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
                        row["signal"] = fb_sig2
                        row["confidence"] = round(fb_conf2, 4)
                        row["score"] = round(float(score if score is not None else _main()._intel_score(symbol, intel)), 6)
                        row["spreadBps"] = round(float(fb_ex2.get("spreadBps", 0.0) or 0.0), 4)
                        row["momentumPct"] = round(abs(float(fb_ex2.get("momentumPct", 0.0) or 0.0)), 4)
                    break

            fallback_candidates = []
            for row in board:
                cand = str(row.get("symbol", "")).upper().strip()
                if not cand:
                    continue
                hs = _main()._scan_health_state(cand)
                fallback_candidates.append((float(_main()._scan_error_penalty(cand)), cand))
            fallback_candidates.sort(key=lambda x: x[0])
            retry_n = max(1, min(3, int(cfg.get("scanFallbackRetrySymbols", 3) or 3)))
            for _, primary in fallback_candidates[:retry_n]:
                try:
                    fb_timeout = max(4.0, min(8.0, per_symbol_timeout + 1.5))
                    fb_intel = await _main().asyncio.wait_for(
                        _main().intel_analyze(_main().IntelAnalyzeRequest(symbol=primary)),
                        timeout=fb_timeout,
                    )
                    if isinstance(fb_intel, dict):
                        fb_sig = str(fb_intel.get("signal", "WAIT")).upper()
                        fb_conf = float(fb_intel.get("confidence", 0.0) or 0.0)
                        fb_ex = fb_intel.get("execution") if isinstance(fb_intel.get("execution"), dict) else {}
                        fb_spread = float(fb_ex.get("spreadBps", 0.0) or 0.0)
                        fb_min_conf = float(_main()._learned_min_conf(primary, base_min_conf, _main()._scan_board_median_conf(board))) + float(session_bias.get("confidenceShift", 0.0) or 0.0)
                        fb_min_conf = max(0.45, min(0.90, fb_min_conf))
                        if fb_sig in ("LONG", "SHORT") and fb_conf >= max(0.45, fb_min_conf - near_conf_relax) and fb_spread <= max_spread_bps:
                            best_sym = primary
                            best_intel = fb_intel
                            best_score = _main()._intel_score(primary, fb_intel)
                            _mark_fallback_board(primary, "fallback_recovered", fb_intel, best_score)
                            _main()._record_scan_health(primary, True)
                            break
                        _mark_fallback_board(primary, "fallback_not_clear", fb_intel, _main()._intel_score(primary, fb_intel))
                        _main()._record_scan_health(primary, False, "fallback_not_clear")
                except Exception as e:
                    _mark_fallback_board(primary, "fallback_error")
                    _main()._record_scan_health(primary, False, _main()._format_loop_error(e) or "fallback_error")
    board.sort(key=lambda x: x["score"], reverse=True)
    # Per-symbol scan cadence bookkeeping: mark every analyzed symbol as scanned.
    try:
        _main()._record_per_symbol_scan_time(
            [str(r.get("symbol", "")).upper().strip() for r in board if str(r.get("symbol", "")).strip()]
        )
    except Exception:
        pass
    if board and all(str(x.get("rejectReason", "")) == "analyze_error" for x in board):
        board.sort(key=lambda x: (float(x.get("scanErrorStreak", 0) or 0), x.get("symbol", "")))
    return best_sym, best_intel, board[:10]