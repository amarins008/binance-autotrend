"""Intel analysis helpers and klines cache."""

from __future__ import annotations

import asyncio
import math
import time

from fastapi import HTTPException

from schemas import IntelAnalyzeRequest

from exchange.binance_client import _data_get
from services.cache_registry import (
    DATA_GET_TIMEOUT_SEC,
    SCAN_ANALYZE_CONCURRENCY,
    _KLINES_CACHE,
    _KLINES_CACHE_MAX,
    _KLINES_CACHE_TTL,
    _KLINES_INFLIGHT,
    _INTEL_CACHE,
    _INTEL_CACHE_MAX,
    _INTEL_CACHE_TTL,
    _limit_cache_size,
)


def _main():
    import main as m
    return m



# Lazy delegates to main during incremental refactor

def _candlestick_pattern_context(*args, **kwargs):
    return _main()._candlestick_pattern_context(*args, **kwargs)

def _decision_data_layers(*args, **kwargs):
    return _main()._decision_data_layers(*args, **kwargs)

def _entry_session_bias(*args, **kwargs):
    return _main()._entry_session_bias(*args, **kwargs)

def _fapi_agreement_locked_symbols(*args, **kwargs):
    return _main()._fapi_agreement_locked_symbols(*args, **kwargs)

def _learned_min_conf(*args, **kwargs):
    return _main()._learned_min_conf(*args, **kwargs)

def _live_trades_count_today_symbol(*args, **kwargs):
    return _main()._live_trades_count_today_symbol(*args, **kwargs)

def _market_momentum(*args, **kwargs):
    return _main()._market_momentum(*args, **kwargs)

def _normalize_symbol(*args, **kwargs):
    return _main()._normalize_symbol(*args, **kwargs)

def _format_loop_error(*args, **kwargs):
    return _main()._format_loop_error(*args, **kwargs)

def _parse_symbol_whitelist(*args, **kwargs):
    return _main()._parse_symbol_whitelist(*args, **kwargs)

def _precision_signal_pack(*args, **kwargs):
    return _main()._precision_signal_pack(*args, **kwargs)

def _record_scan_health(*args, **kwargs):
    return _main()._record_scan_health(*args, **kwargs)

def _record_symbol_observation(*args, **kwargs):
    return _main()._record_symbol_observation(*args, **kwargs)

async def _record_symbol_observation_async(*args, **kwargs):
    return await _main()._record_symbol_observation_async(*args, **kwargs)

def _scan_error_penalty(*args, **kwargs):
    return _main()._scan_error_penalty(*args, **kwargs)

def _scan_health_state(*args, **kwargs):
    return _main()._scan_health_state(*args, **kwargs)

def _symbol_effective_profile(*args, **kwargs):
    return _main()._symbol_effective_profile(*args, **kwargs)

def _symbol_perf_gate(*args, **kwargs):
    return _main()._symbol_perf_gate(*args, **kwargs)


def _load_single_profile(symbol: str) -> dict:
    import main as _main
    return _main._load_single_profile(symbol)


async def _cached_klines(symbol: str, interval: str, limit: int) -> list:
    key = (symbol, interval, limit)
    now = time.time()
    if key in _KLINES_CACHE:
        fetched_at, data = _KLINES_CACHE[key]
        if now - fetched_at < _KLINES_CACHE_TTL:
            return data
    if key in _KLINES_INFLIGHT:
        return await asyncio.wait_for(
            _KLINES_INFLIGHT[key], timeout=max(1.0, DATA_GET_TIMEOUT_SEC + 1.0)
        )

    async def _fetch() -> list:
        res = await _data_get(f"/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}")
        if res.status_code >= 400:
            raise HTTPException(status_code=res.status_code, detail=f"klines {interval} failed: {res.text}")
        return res.json()

    task = asyncio.create_task(_fetch())
    _KLINES_INFLIGHT[key] = task
    try:
        data = await asyncio.wait_for(task, timeout=max(1.0, DATA_GET_TIMEOUT_SEC + 1.0))
    except Exception:
        if not task.done():
            task.cancel()
        raise
    finally:
        if _KLINES_INFLIGHT.get(key) is task:
            _KLINES_INFLIGHT.pop(key, None)
    if len(_KLINES_CACHE) >= _KLINES_CACHE_MAX:
        _limit_cache_size(_KLINES_CACHE, max_size=_KLINES_CACHE_MAX)
    _KLINES_CACHE[key] = (now, data)
    return data


def _symbol_quality_score(symbol: str) -> float:
    pr = _load_single_profile(symbol)
    if not isinstance(pr, dict):
        return 0.0
    windows = pr.get("memoryWindows") if isinstance(pr.get("memoryWindows"), dict) else {}
    recent = windows.get("7d") if isinstance(windows.get("7d"), dict) else {}
    weighted = pr.get("weightedRecentScore") if isinstance(pr.get("weightedRecentScore"), dict) else {}
    n = int(recent.get("trades", 0) or 0)
    if n >= 3:
        wr = float(recent.get("winRatePct", 0.0) or 0.0) / 100.0
        pnl = float(recent.get("pnl", 0.0) or 0.0)
    else:
        wins = int(pr.get("wins", 0))
        losses = int(pr.get("losses", 0))
        n = wins + losses
        wr = wins / max(n, 1) if n > 0 else 0.0
        pnl = float(pr.get("realizedPnl", 0.0) or 0.0)
    if n < 3:
        return 0.0
    wr_bonus = (wr - 0.5) * 0.12
    pnl_bonus = max(-0.05, min(0.05, pnl / 200.0))
    reward_score = float(pr.get("rewardScore", 0.0) or 0.0)
    recent_score = float(weighted.get("score", 0.0) or 0.0)
    reward_delta = float(pr.get("rewardDelta", 0.0) or 0.0)
    reward_behavior = float(pr.get("rewardBehaviorDelta", 0.0) or 0.0)
    components = pr.get("rewardComponents") if isinstance(pr.get("rewardComponents"), dict) else {}
    entry_quality = float(components.get("entryQuality", 0.0) or 0.0) + float(
        components.get("riskReward", 0.0) or 0.0
    )
    reward_bonus = max(-0.06, min(0.06, reward_score / 250.0))
    memory_bonus = max(-0.06, min(0.06, recent_score * 0.06))
    recent_bonus = max(-0.02, min(0.02, reward_delta / 25.0))
    behavior_bonus = max(-0.035, min(0.035, reward_behavior / 40.0))
    entry_bonus = max(-0.025, min(0.025, entry_quality / 20.0))
    win_streak = int(pr.get("rewardWinStreak", 0) or 0)
    loss_streak = int(pr.get("rewardLossStreak", 0) or 0)
    streak_bonus = 0.0
    if win_streak >= 2:
        streak_bonus += min(0.07, 0.018 * (win_streak - 1))
    if loss_streak >= 2:
        streak_bonus -= min(0.09, 0.024 * (loss_streak - 1))
    return (
        wr_bonus
        + pnl_bonus
        + reward_bonus
        + memory_bonus
        + recent_bonus
        + behavior_bonus
        + entry_bonus
        + streak_bonus
    )


def _intel_score(symbol: str, intel: dict) -> float:
    sig = str((intel or {}).get("signal", "WAIT")).upper()
    conf = float((intel or {}).get("confidence", 0.0) or 0.0)
    ex = (intel or {}).get("execution") if isinstance((intel or {}).get("execution"), dict) else {}
    spread_penalty = min(0.2, max(0.0, float(ex.get("spreadBps", 0.0) or 0.0) / 200.0))
    momentum = abs(float(ex.get("momentumPct", 0.0) or 0.0))
    qual = _symbol_quality_score(symbol)
    score = conf + min(0.12, momentum / 5.0) + qual - spread_penalty
    if sig not in ("LONG", "SHORT"):
        score -= 0.25
    # Recent symbol session performance nudge: if this symbol has been
    # losing in the current UTC hour (last 4 hours window, min 4 trades),
    # apply a small negative adjustment so the scan de-prioritises it
    # in favour of symbols with cleaner recent history.
    try:
        import time as _time
        sym_recent = _load_single_profile(str(symbol or "").upper())
        if isinstance(sym_recent, dict):
            windows = sym_recent.get("memoryWindows") if isinstance(sym_recent.get("memoryWindows"), dict) else {}
            w7d = windows.get("7d") if isinstance(windows.get("7d"), dict) else {}
            n7 = int(w7d.get("trades", 0) or 0)
            if n7 >= 4:
                wr7 = float(w7d.get("winRatePct", 50.0) or 50.0)
                avg7 = float(w7d.get("avgPnl", 0.0) or 0.0)
                # Penalise symbols with recent negative drift in 7d window
                if wr7 < 38.0 and avg7 < -0.04:
                    score -= 0.06
                elif wr7 < 44.0 and avg7 < 0.0:
                    score -= 0.03
                # Bonus for consistent recent winners
                elif wr7 >= 58.0 and avg7 > 0.04:
                    score += 0.04
    except Exception:
        pass
    return score


async def _scan_market_candidates(limit_liquid: int = 30) -> list[str]:
    fallback = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT", "ADAUSDT", "LINKUSDT"]
    try:
        res = await asyncio.wait_for(_data_get("/fapi/v1/ticker/24hr"), timeout=12.0)
    except Exception:
        return fallback[: max(5, min(int(limit_liquid or 30), len(fallback)))]
    if res.status_code >= 400:
        return fallback[: max(5, min(int(limit_liquid or 30), len(fallback)))]
    rows = res.json()
    out = []
    for r in rows if isinstance(rows, list) else []:
        sym = str(r.get("symbol", "")).upper()
        if not sym.endswith("USDT"):
            continue
        if "_" in sym or "1000" in sym:
            continue
        try:
            qv = float(r.get("quoteVolume", 0) or 0)
            chg = abs(float(r.get("priceChangePercent", 0) or 0))
        except Exception:
            continue
        moderate_move_bonus = 1.0 + min(0.35, chg / 80.0)
        extreme_move_penalty = 1.0 / (1.0 + max(0.0, chg - 8.0) / 8.0)
        rank = qv * moderate_move_bonus * extreme_move_penalty
        out.append((rank, sym))
    out.sort(key=lambda x: x[0], reverse=True)
    return [s for _, s in out[: max(5, int(limit_liquid))]]

async def _pick_best_symbol_from_scan(cfg: dict, exclude_symbols: set[str] | None = None) -> tuple[str | None, dict | None, list[dict]]:
    candidates = await _scan_market_candidates(int(cfg.get("scanTopLiquid", 30)))
    blocked_symbols = {str(s).upper().strip() for s in (exclude_symbols or set()) if str(s).strip()}
    blocked_symbols.update(_parse_symbol_whitelist(cfg.get("scanDenySymbols")))
    live_scan = str(cfg.get("executionMode", "") or "").upper() == "LIVE"
    if live_scan:
        blocked_symbols.update(_fapi_agreement_locked_symbols())
    if blocked_symbols:
        candidates = [s for s in candidates if s not in blocked_symbols]
    if live_scan:
        day_cap = int(cfg.get("maxDailyTradesPerSymbol", 14) or 14)
        if day_cap > 0:
            candidates = [s for s in candidates if _live_trades_count_today_symbol(s) < day_cap]
    wl = _parse_symbol_whitelist(cfg.get("whitelistSymbols"))
    if wl:
        candidates = [s for s in candidates if s in wl]
    # Prevent the same repeatedly failing symbol from pinning the scan head.
    candidates = sorted(enumerate(candidates), key=lambda x: (_scan_error_penalty(x[1]), x[0]))
    candidates = [s for _, s in candidates]
    analyze_top = max(3, int(cfg.get("scanAnalyzeTop", 8)))
    all_candidates = list(candidates)
    candidates = all_candidates[:analyze_top]
    if not candidates:
        return None, None, []
    reqs = [IntelAnalyzeRequest(symbol=s) for s in candidates]
    per_symbol_timeout = float(cfg.get("scanPerSymbolTimeoutSec", 7.5) or 7.5)
    per_symbol_timeout = max(2.0, min(20.0, per_symbol_timeout))

    async def _analyze_one(req: IntelAnalyzeRequest):
        try:
            return await asyncio.wait_for(intel_analyze(req), timeout=per_symbol_timeout)
        except asyncio.TimeoutError:
            return TimeoutError(f"analyze timeout>{per_symbol_timeout:.1f}s")
        except Exception as e:
            return e

    scan_sem = asyncio.Semaphore(SCAN_ANALYZE_CONCURRENCY)
    scan_t0 = time.time()

    async def _analyze_one_limited(req: IntelAnalyzeRequest):
        async with scan_sem:
            t0 = time.time()
            res: object = None
            try:
                res = await _analyze_one(req)
            except Exception as exc:
                dt = time.time() - t0
                # Per-symbol timing — even on failure, surface what blew up so we
                # can pinpoint which symbol is eating the budget. Without this,
                # `res` is undefined and `type(res)` raises UnboundLocalError in
                # the finally clause, which masks the original exception and
                # makes asyncio.gather fail the entire scan with a confusing
                # "local variable 'res' where it is not associated" error.
                print(
                    f"[scan-timing] sym={req.symbol} dt={dt:.2f}s kind=Exception err={type(exc).__name__}: {str(exc)[:60]}",
                    flush=True,
                )
                return exc
            dt = time.time() - t0
            # Per-symbol timing — appears in uvicorn logs as "[scan-timing]"
            # so we can pinpoint which symbol (if any) is eating the budget.
            kind = type(res).__name__
            print(
                f"[scan-timing] sym={req.symbol} dt={dt:.2f}s kind={kind}",
                flush=True,
            )
            return res

    results = await asyncio.gather(*[_analyze_one_limited(r) for r in reqs], return_exceptions=False)
    scan_dt = time.time() - scan_t0
    print(
        f"[scan-timing] analyze_total={scan_dt:.2f}s symbols={len(reqs)} concurrency={SCAN_ANALYZE_CONCURRENCY} per_symbol_budget={per_symbol_timeout:.1f}s",
        flush=True,
    )
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
    session_bias = _entry_session_bias(cfg)
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

    def _soft_perf_fallback_reason(reason: str, perf: dict) -> str:
        canonical = _canonical_perf_lock_reason(reason, perf)
        active_lock = str(reason or "").startswith("perf_lock(")
        trades = int((perf or {}).get("trades", 0) or 0)
        win_rate = float((perf or {}).get("winRatePct", 0.0) or 0.0)
        pnl = float((perf or {}).get("pnl", 0.0) or 0.0)
        if not active_lock and canonical in soft_perf_reasons:
            return canonical
        if active_lock and trades >= 8 and (win_rate >= 40.0 or pnl >= -0.35):
            return "perf_lock_recovered"
        return ""

    def _soft_perf_fallback_ok(reason: str, signal: str, confidence: float, min_conf: float, spread: float, perf: dict) -> tuple[bool, str]:
        fallback_reason = _soft_perf_fallback_reason(reason, perf)
        ok = (
            soft_perf_enabled
            and bool(fallback_reason)
            and fallback_reason not in ("perf_lock_early", "perf_lock_reward")
            and signal in ("LONG", "SHORT")
            and confidence >= min(0.92, min_conf + soft_perf_conf_lift)
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
            _record_scan_health(sym, False, _format_loop_error(out))
            hs = _scan_health_state(sym)
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
                "error": _format_loop_error(out),
                "scanErrorStreak": int(hs.get("streak", 0)),
                "scanCooldownUntil": int(hs.get("cooldownUntil", 0)),
            })
            continue
        if not isinstance(out, dict):
            _record_scan_health(sym, False, "analyze_invalid")
            hs = _scan_health_state(sym)
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
        momentum = abs(float(ex.get("momentumPct", 0.0) or 0.0))
        qual = _symbol_quality_score(sym)
        # Resolve per-symbol 3-tier policy (System -> Group -> Symbol) and
        # apply the group-specific confidence floor + long-bias to the score.
        sym_profile = _symbol_effective_profile(sym, cfg)
        group_long_bias = float(sym_profile.get("scan_long_bias", 0.5))
        group_conf_floor = float(sym_profile.get("min_conf_floor", 0.50))
        score = _intel_score(sym, out)
        spread_bps = float(ex.get("spreadBps", 0.0) or 0.0)
        # Adaptive min confidence: per-symbol learned confidence + session
        # shift, but use the per-group floor as the lower bound instead of
        # the global 0.45. Low-vol groups (trend-friendly) tolerate lower
        # confidence; noisy groups (low-liquidity) need higher evidence.
        adaptive_min_conf = float(_learned_min_conf(sym, max(base_min_conf, group_conf_floor)))
        adaptive_min_conf += float(session_bias.get("confidenceShift", 0.0) or 0.0)
        adaptive_min_conf = max(group_conf_floor, min(0.92, adaptive_min_conf))
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
        elif spread_bps > max_spread_bps:
            qualified = False
            reject_reason = "wide_spread"
        perf_ok, perf_reason, perf = _symbol_perf_gate(cfg, sym)
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
        _record_scan_health(sym, True)
        await _record_symbol_observation_async(sym, out, False, score)
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
            extra_reqs = [IntelAnalyzeRequest(symbol=s) for s in extra_candidates]
            extra_results = await asyncio.gather(*[_analyze_one_limited(r) for r in extra_reqs], return_exceptions=False)
            for sym, out in zip(extra_candidates, extra_results):
                if isinstance(out, Exception) or not isinstance(out, dict):
                    reason = "analyze_error" if isinstance(out, Exception) else "analyze_invalid"
                    err = _format_loop_error(out) if isinstance(out, Exception) else "analyze_invalid"
                    _record_scan_health(sym, False, err)
                    hs = _scan_health_state(sym)
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
                qual = _symbol_quality_score(sym)
                score = _intel_score(sym, out) + float(session_bias.get("scoreShift", 0.0) or 0.0)
                spread_bps = float(ex.get("spreadBps", 0.0) or 0.0)
                adaptive_min_conf = float(_learned_min_conf(sym, base_min_conf)) + float(session_bias.get("confidenceShift", 0.0) or 0.0)
                adaptive_min_conf = max(0.45, min(0.90, adaptive_min_conf))
                qualified = True
                reject_reason = ""
                if sig not in ("LONG", "SHORT"):
                    qualified = False
                    reject_reason = "signal_wait"
                elif conf < adaptive_min_conf:
                    qualified = False
                    reject_reason = "low_conf"
                elif spread_bps > max_spread_bps:
                    qualified = False
                    reject_reason = "wide_spread"
                perf_ok, perf_reason, perf = _symbol_perf_gate(cfg, sym)
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
                _record_scan_health(sym, True)
                await _record_symbol_observation_async(sym, out, False, score)
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
        guarded_floor = max(0.50, base_min_conf - guarded_conf_relax)
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
        await _record_symbol_observation_async(best_sym, best_intel, True, best_score)
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
                        row["score"] = round(float(score if score is not None else _intel_score(symbol, intel)), 6)
                        row["spreadBps"] = round(float(fb_ex2.get("spreadBps", 0.0) or 0.0), 4)
                        row["momentumPct"] = round(abs(float(fb_ex2.get("momentumPct", 0.0) or 0.0)), 4)
                    break

            fallback_candidates = []
            for row in board:
                cand = str(row.get("symbol", "")).upper().strip()
                if not cand:
                    continue
                hs = _scan_health_state(cand)
                fallback_candidates.append((float(_scan_error_penalty(cand)), cand))
            fallback_candidates.sort(key=lambda x: x[0])
            retry_n = max(1, min(3, int(cfg.get("scanFallbackRetrySymbols", 3) or 3)))
            for _, primary in fallback_candidates[:retry_n]:
                try:
                    fb_timeout = max(4.0, min(8.0, per_symbol_timeout + 1.5))
                    fb_intel = await asyncio.wait_for(
                        intel_analyze(IntelAnalyzeRequest(symbol=primary)),
                        timeout=fb_timeout,
                    )
                    if isinstance(fb_intel, dict):
                        fb_sig = str(fb_intel.get("signal", "WAIT")).upper()
                        fb_conf = float(fb_intel.get("confidence", 0.0) or 0.0)
                        fb_ex = fb_intel.get("execution") if isinstance(fb_intel.get("execution"), dict) else {}
                        fb_spread = float(fb_ex.get("spreadBps", 0.0) or 0.0)
                        fb_min_conf = float(_learned_min_conf(primary, base_min_conf)) + float(session_bias.get("confidenceShift", 0.0) or 0.0)
                        fb_min_conf = max(0.45, min(0.90, fb_min_conf))
                        if fb_sig in ("LONG", "SHORT") and fb_conf >= max(0.45, fb_min_conf - near_conf_relax) and fb_spread <= max_spread_bps:
                            best_sym = primary
                            best_intel = fb_intel
                            best_score = _intel_score(primary, fb_intel)
                            _mark_fallback_board(primary, "fallback_recovered", fb_intel, best_score)
                            _record_scan_health(primary, True)
                            break
                        _mark_fallback_board(primary, "fallback_not_clear", fb_intel, _intel_score(primary, fb_intel))
                        _record_scan_health(primary, False, "fallback_not_clear")
                except Exception as e:
                    _mark_fallback_board(primary, "fallback_error")
                    _record_scan_health(primary, False, _format_loop_error(e) or "fallback_error")
    board.sort(key=lambda x: x["score"], reverse=True)
    if board and all(str(x.get("rejectReason", "")) == "analyze_error" for x in board):
        board.sort(key=lambda x: (float(x.get("scanErrorStreak", 0) or 0), x.get("symbol", "")))
    return best_sym, best_intel, board[:10]

async def intel_analyze(req: IntelAnalyzeRequest):
    symbol = _normalize_symbol(req.symbol)
    req_cfg = req.model_dump() if hasattr(req, "model_dump") else dict(req)
    vision = None

    # Return cached result if fresh enough (saves CPU on repeated calls)
    now_ts = time.time()
    if symbol in _INTEL_CACHE:
        cached_ts, cached_result = _INTEL_CACHE[symbol]
        if now_ts - cached_ts < _INTEL_CACHE_TTL:
            return cached_result

    async def _depth_orderflow():
        try:
            res = await _data_get(f"/fapi/v1/depth?symbol={symbol}&limit=50")
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
                _data_get(f"/fapi/v1/ticker/bookTicker?symbol={symbol}"),
                _data_get(f"/fapi/v1/premiumIndex?symbol={symbol}"),
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

    # Pre-fetch ALL klines upfront and share across all consumers.
    # This eliminates redundant 5m/15m fetches between _precision_signal_pack
    # and _candlestick_pattern_context (each used to hit the API independently).
    rows_1m, rows_5m, rows_15m = await asyncio.gather(
        _cached_klines(symbol, "1m", 150),
        _cached_klines(symbol, "5m", 100),
        _cached_klines(symbol, "15m", 60),
    )

    mm, pk, depth_out, execution, candle_ctx = await asyncio.gather(
        _market_momentum(symbol, _rows=rows_1m[:60]),
        _precision_signal_pack(symbol, limit=150, _rows_1m=rows_1m, _rows_5m=rows_5m, _rows_15m=rows_15m),
        _depth_orderflow(),
        _microstructure(),
        _candlestick_pattern_context(symbol, _rows_5m=rows_5m[:10] if rows_5m else None, _rows_15m=rows_15m[:10] if rows_15m else None),
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

    # ── Confluence scoring via evaluate_confluence() ─────────────────────────
    # Delegate to the single canonical confluence engine instead of duplicating
    # scoring inline. This ensures STRONG_THRESHOLD=7, stricter RSI/BB/wick
    # filters, loss-streak adaptive thresholds, and volume-profile analysis
    # all take effect — previously this inline path used STRONG_THRESHOLD=6
    # which was weaker than the confluence.py implementation.
    try:
        from trading.confluence import evaluate_confluence
        _loss_streak = 0
        try:
            from services import app_state as _app_state
            _cfg_live = (_app_state.AUTO_TRADE.get("config") or {}) if isinstance(_app_state.AUTO_TRADE, dict) else {}
            _loss_streak = int(_cfg_live.get("_recentLossStreak", 0) or 0)
        except Exception:
            pass
        cr = await evaluate_confluence(
            pk,
            mm,
            pre_signal=pre_confluence_signal,
            pre_confidence=pre_confluence_confidence,
            imbalance=imbalance,
            order_book=order_book,
            bias_signal=pre_confluence_signal,
            loss_streak=_loss_streak,
        )
        final_signal = cr.signal
        confidence = cr.confidence
        long_score = cr.long_score
        short_score = cr.short_score
        notes.extend([n for n in cr.notes if n and n not in notes][:3])
    except Exception:
        # Fallback to inline scoring when confluence import fails
        long_score = 0
        short_score = 0
        if pk["trendUp"]:
            long_score += 3
        elif pk["trendUpPartial"]:
            long_score += 2
        if pk["trendDown"]:
            short_score += 3
        elif pk["trendDnPartial"]:
            short_score += 2
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
        rsi = pk["rsi14"]
        rsi5 = pk.get("rsi14_5m", 50)
        if 45 <= rsi <= 70 and rsi5 >= 50:
            long_score += 1
        if 30 <= rsi <= 55 and rsi5 <= 50:
            short_score += 1
        if rsi > 78:
            long_score -= 1
        if rsi < 22:
            short_score -= 1
        sk, sd = pk.get("stochK", 50), pk.get("stochD", 50)
        if sk > sd and sk < 80:
            long_score += 1
        if sk < sd and sk > 20:
            short_score += 1
        if pk["priceNearBbLower"] and pk["priceAboveBbMid"] is False:
            long_score += 2
        if pk["priceNearBbUpper"] and pk["priceAboveBbMid"]:
            short_score += 2
        if pk["priceAboveVwap"]:
            long_score += 1
        else:
            short_score += 1
        if pk["breakoutUp"] and pk["volumeRatio"] >= volume_strong_ratio:
            long_score += 2
        if pk["breakoutDown"] and pk["volumeRatio"] >= volume_strong_ratio:
            short_score += 2
        cvd = pk.get("cvd", 0.0)
        if cvd > 0.05:
            long_score += 1
        elif cvd < -0.05:
            short_score += 1
        if imbalance is not None and imbalance > 0.04:
            long_score += 1
        if imbalance is not None and imbalance < -0.04:
            short_score += 1
        if mm["momentumPct"] > 0.1:
            long_score += 1
        if mm["momentumPct"] < -0.1:
            short_score += 1
        if pk["atrPct"] < 0.025:
            long_score -= 2
            short_score -= 2
        STRONG_THRESHOLD = 6
        SOFT_THRESHOLD = 4
        if long_score >= STRONG_THRESHOLD and long_score >= short_score + 2:
            final_signal = "LONG"
            confidence = max(confidence, min(0.95, 0.62 + 0.025 * long_score))
        elif short_score >= STRONG_THRESHOLD and short_score >= long_score + 2:
            final_signal = "SHORT"
            confidence = max(confidence, min(0.95, 0.62 + 0.025 * short_score))
        elif pre_confluence_signal == "LONG" and long_score >= SOFT_THRESHOLD and long_score >= short_score + 1:
            final_signal = "LONG"
            confidence = max(pre_confluence_confidence, min(0.88, 0.60 + 0.035 * long_score))
        elif pre_confluence_signal == "SHORT" and short_score >= SOFT_THRESHOLD and short_score >= long_score + 1:
            final_signal = "SHORT"
            confidence = max(pre_confluence_confidence, min(0.88, 0.60 + 0.035 * short_score))
        elif pre_confluence_signal in ("LONG", "SHORT") and max(long_score, short_score) >= 3 and abs(long_score - short_score) >= 1:
            if long_score > short_score:
                final_signal = "LONG"
                confidence = max(0.64, min(0.82, pre_confluence_confidence + 0.025 * (long_score - short_score)))
            else:
                final_signal = "SHORT"
                confidence = max(0.64, min(0.82, pre_confluence_confidence + 0.025 * (short_score - long_score)))
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
    # Build shared market context (single source for regime, realized vol,
    # vol-target ratio, dominant pattern, vol-scaled late-entry bound).
    # Read live cfg from app_state so volatility-aware knobs reflect the
    # running bot config; fall back to {} if unavailable (router paths).
    try:
        from services import app_state as _app_state
        _mc_cfg = (_app_state.AUTO_TRADE.get("config") or {}) if isinstance(_app_state.AUTO_TRADE, dict) else {}
    except Exception:
        _mc_cfg = {}
    try:
        from analysis.market_context import build as _build_market_context
        result["marketContext"] = _build_market_context(result, _mc_cfg)
    except Exception as _e:
        # Never let context-build failure break intel — return empty slot.
        result["marketContext"] = {"error": f"{type(_e).__name__}: {_e}"}
    # Cache result to avoid recomputing on rapid repeated calls
    _INTEL_CACHE[symbol] = (time.time(), result)
    # Limit cache size to enforce _INTEL_CACHE_MAX bound
    if len(_INTEL_CACHE) > _INTEL_CACHE_MAX:
        _limit_cache_size(_INTEL_CACHE, max_size=_INTEL_CACHE_MAX)
    return result
