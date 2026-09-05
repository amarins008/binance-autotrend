"""Intel analysis helpers and klines cache."""

from __future__ import annotations

import asyncio
import math
import time

from exceptions import DataError
from logger import get_logger, log_exception

from exchange.binance_client import _data_get
from services.cache_registry import (
    DATA_GET_TIMEOUT_SEC,
    _KLINES_CACHE,
    _KLINES_CACHE_MAX,
    _KLINES_CACHE_TTL,
    _KLINES_INFLIGHT,
    _limit_cache_size,
)

_log = get_logger("analysis.intel_pipeline")


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
        try:
            return await asyncio.wait_for(
                _KLINES_INFLIGHT[key], timeout=max(1.0, DATA_GET_TIMEOUT_SEC + 1.0)
            )
        except asyncio.TimeoutError:
            raise DataError(f"klines {symbol} {interval} inflight timeout")

    async def _fetch() -> list:
        res = await _data_get(f"/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}")
        if res.status_code >= 400:
            raise DataError(
                f"klines {interval} failed: {res.text[:200]}",
                symbol=symbol,
            )
        try:
            return res.json()
        except ValueError as exc:
            raise DataError(f"klines {symbol} {interval} invalid JSON: {exc}")

    task = asyncio.create_task(_fetch())
    _KLINES_INFLIGHT[key] = task
    try:
        data = await asyncio.wait_for(task, timeout=max(1.0, DATA_GET_TIMEOUT_SEC + 1.0))
    except DataError:
        raise
    except asyncio.TimeoutError:
        if not task.done():
            task.cancel()
        log_exception(_log, TimeoutError(f"klines {symbol} {interval} timeout"), {"symbol": symbol})
        raise DataError(f"klines {symbol} {interval} fetch timeout")
    except Exception as exc:
        if not task.done():
            task.cancel()
        log_exception(_log, exc, {"symbol": symbol, "interval": interval})
        raise DataError(f"klines {symbol} {interval} fetch failed: {exc}") from exc
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
    except (TypeError, ValueError) as exc:
        log_exception(_log, exc, {"symbol": symbol, "stage": "intel_score_quality"})
    return score
