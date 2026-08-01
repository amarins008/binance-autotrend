import logging

from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field
from typing import Literal
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from uuid import uuid4
from pathlib import Path

_log = logging.getLogger("backend.main")

from indicators import (
    _atr_series,
    _bollinger,
    _cvd_delta,
    _detect_market_session,
    _detect_pre_reversal,
    _ema,
    _ema_series,
    _macd,
    _rsi,
    _stochastic_rsi,
    _vwap,
)
from schemas import (
    AnalyzeRequest,
    AutoTradeControlRequest,
    AutoTradeStartRequest,
    IntelAnalyzeRequest,
    MonitorStartRequest,
    OrderBookLevel,
    OrderBookSummary,
    RiskConfig,
    StrategyParseRequest,
    StrategyPlan,
    TradeRequest,
    VisionAnalyzeRequest,
)
from hermes_agents import ensure_agent_state, mark_agent, new_agent_state, rebuild_kanban, start_cycle
from obsidian_memory import (
    append_scan_memory,
    append_trade_memory,
    ensure_trading_vault,
    write_self_review_memory,
)
from trading.regime import detect_market_regime
from trading.config import apply_autotrade_defaults
from trading.live_guardian import (
    _manage_live_open_positions_once,
    _live_multi_profit_lock_manage,
)
from trading.trade_log import _live_closed_trades_from_log, _live_closed_trades_from_symbol
from trading.per_symbol_storage import PerSymbolStorage, per_symbol_lock
from trading.shared_storage import SharedStorage
from trading.shared_cache_layer import SharedCacheLayer, get_shared_cache
from trading.per_symbol_context import PerSymbolContext
from trading.pipeline import EntryInputs, EntryPlan, evaluate_entry_plan


# Centralized capacity defaults — keep a single source of truth so the loop,
# supervisor and config writer never drift apart.
_DEFAULT_MAX_OPEN_POSITIONS = 4
_DEFAULT_MAX_DAILY_TRADES_PER_SYMBOL = 14

import os
import sys
import time
import hmac
import hashlib
import json
import re
import math
import asyncio
import datetime
import copy
import subprocess
from functools import wraps
import socket
import ipaddress
import threading
import urllib.request
from decimal import Decimal, ROUND_DOWN
import httpx
_DATA_ROOT = Path(os.getenv("HERMES_DATA_DIR", Path(__file__).parent)).resolve()
ENV_PATH = Path(os.getenv("HERMES_ENV_PATH", Path(__file__).with_name(".env"))).resolve()
SNAPSHOT_PATH = _DATA_ROOT / "autotrade_snapshot.json"
VAULT_DIR = _DATA_ROOT / "obsidian_vault"
TRADES_LOG_PATH = VAULT_DIR / "trades_log.jsonl"
TRAIN_REPORT_PATH = VAULT_DIR / "learning_report.json"
load_dotenv(dotenv_path=ENV_PATH, override=True)

_BINANCE_HTTP: httpx.AsyncClient | None = None
_APP_STARTED_AT = time.time()
_BACKEND_PORT = int(os.getenv("BACKEND_PORT", "8020"))
_BINANCE_DATA_HTTP: httpx.AsyncClient | None = None  # dedicated client for public mainnet data


def _resolve_umfutures_class():
    global _UMFUTURES_CLASS
    if _UMFUTURES_CLASS is not None:
        return _UMFUTURES_CLASS
    try:
        from binance.um_futures import UMFutures as resolved_umfutures
    except Exception:
        resolved_umfutures = None
    _UMFUTURES_CLASS = resolved_umfutures
    return _UMFUTURES_CLASS


def _fapi_public_base() -> str:
    """Public REST host for USD-M futures (must match signed _binance_base() environment)."""
    return (
        "https://testnet.binancefuture.com"
        if os.getenv("BINANCE_TESTNET", "false").lower() == "true"
        else "https://fapi.binance.com"
    )


def _fapi_public_data_base() -> str:
    """
    Public *market-data* host — always mainnet regardless of BINANCE_TESTNET.
    Klines, depth, premiumIndex, bookTicker are identical on both networks
    and mainnet is significantly faster and more reliable.
    Signed (account/order) endpoints still use _binance_base() / _fapi_public_base().
    """
    return "https://fapi.binance.com"


# ── Klines in-memory cache ────────────────────────────────────────────────────
# key: (symbol, interval, limit)  value: (fetched_at_unix, data_list)

# ── Intel result cache ────────────────────────────────────────────────────────
# Avoid recomputing full indicator pack when called within TTL
_INTEL_CACHE: dict[str, tuple[float, dict]] = {}  # symbol -> (ts, result)
_INTEL_CACHE_TTL = 15  # seconds
_KLINES_CACHE: dict[tuple, tuple[float, list]] = {}
_KLINES_CACHE_TTL = 20   # seconds — klines update every ~1m so 20s is safe
_KLINES_CACHE_MAX = 30   # max entries to prevent unbounded growth
_KLINES_INFLIGHT: dict[tuple, asyncio.Task] = {}
DATA_GET_TIMEOUT_SEC = float(os.getenv("DATA_GET_TIMEOUT_SEC", "3.2"))
DATA_GET_CONNECT_TIMEOUT_SEC = float(os.getenv("DATA_GET_CONNECT_TIMEOUT_SEC", "1.6"))
DATA_GET_MAX_ATTEMPTS = max(1, int(os.getenv("DATA_GET_MAX_ATTEMPTS", "1")))
SCAN_ANALYZE_CONCURRENCY = max(1, int(os.getenv("SCAN_ANALYZE_CONCURRENCY", "2")))
_DATA_PROVIDER_HEALTH: dict[str, object] = {"streak": 0, "cooldownUntil": 0, "lastErrorAt": 0, "lastError": ""}
_LIVE_STATS_CACHE: dict[tuple, tuple[float, dict]] = {}
_LIVE_STATS_VERSION = 0
_SESSION_BIAS_CACHE: dict[str, object] = {"builtAt": 0.0, "liveVersion": -1, "mtime": -1.0, "hours": {}}
_EXCHANGE_FILTERS_CACHE: dict[str, tuple[float, dict]] = {}
_EXCHANGE_FILTERS_CACHE_TTL = 60  # seconds
_UMFUTURES_CLASS = None


def _ensure_vault():
    try:
        VAULT_DIR.mkdir(parents=True, exist_ok=True)
        ensure_trading_vault(VAULT_DIR)
    except Exception as exc:
        print(f"[Trade Log] ERROR writing {TRADES_LOG_PATH}: {exc}")


def _load_single_profile(symbol: str) -> dict:
    """Load a single symbol's profile from per-symbol storage."""
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {}
    try:
        storage = PerSymbolStorage(VAULT_DIR, sym)
        profile = storage.load_profile()
        if profile:
            return profile
    except Exception:
        pass
    return {}


def _save_single_profile(symbol: str, profile: dict) -> None:
    """Save a single symbol's profile to per-symbol storage."""
    sym = str(symbol or "").upper().strip()
    if not sym:
        return
    try:
        storage = PerSymbolStorage(VAULT_DIR, sym)
        storage.save_profile(profile)
    except Exception:
        pass


def _scan_health_state(symbol: str) -> dict[str, int]:
    sym = str(symbol or "").upper().strip()
    pr = _load_single_profile(sym)
    if not isinstance(pr, dict):
        return {"streak": 0, "cooldownUntil": 0, "lastErrorAt": 0, "lastSuccessAt": 0}
    return {
        "streak": max(0, int(pr.get("scanErrorStreak", 0) or 0)),
        "cooldownUntil": max(0, int(pr.get("scanCooldownUntil", 0) or 0)),
        "lastErrorAt": max(0, int(pr.get("lastScanErrorAt", 0) or 0)),
        "lastSuccessAt": max(0, int(pr.get("lastScanSuccessAt", 0) or 0)),
    }


def _record_scan_health(symbol: str, ok: bool, reason: str | None = None) -> None:
    sym = str(symbol or "").upper()
    if not sym:
        return
    pr = _load_single_profile(sym)
    if not pr:
        pr = {"wins": 0, "losses": 0, "realizedPnl": 0.0, "trades": 0}
    now = int(time.time())
    if ok:
        pr["scanErrorStreak"] = 0
        pr["scanCooldownUntil"] = 0
        pr["lastScanSuccessAt"] = now
    else:
        streak = int(pr.get("scanErrorStreak", 0) or 0) + 1
        pr["scanErrorStreak"] = streak
        pr["lastScanErrorAt"] = now
        # Back off repeated timeouts so the same symbol does not pin the scan head.
        if streak >= 2:
            pr["scanCooldownUntil"] = now + min(900, 45 * streak)
    if reason:
        pr["lastScanErrorReason"] = str(reason)[:120]
    pr["updatedAt"] = now
    _save_single_profile(sym, pr)


def _data_provider_cooldown_active() -> bool:
    return int(_DATA_PROVIDER_HEALTH.get("cooldownUntil", 0) or 0) > int(time.time())


def _record_data_provider_health(ok: bool, err: Exception | str | None = None, path: str = "") -> None:
    now = int(time.time())
    if ok:
        _DATA_PROVIDER_HEALTH["streak"] = 0
        _DATA_PROVIDER_HEALTH["cooldownUntil"] = 0
        return
    streak = int(_DATA_PROVIDER_HEALTH.get("streak", 0) or 0) + 1
    cooldown_sec = min(45, 5 * streak) if streak >= 3 else 0
    _DATA_PROVIDER_HEALTH["streak"] = streak
    _DATA_PROVIDER_HEALTH["lastErrorAt"] = now
    _DATA_PROVIDER_HEALTH["lastError"] = str(err or "")[:160]
    if path:
        _DATA_PROVIDER_HEALTH["lastPath"] = str(path)[:120]
    if cooldown_sec:
        _DATA_PROVIDER_HEALTH["cooldownUntil"] = now + cooldown_sec
    AUTO_TRADE["lastDataProviderError"] = {
        "ts": now,
        "streak": streak,
        "cooldownUntil": int(_DATA_PROVIDER_HEALTH.get("cooldownUntil", 0) or 0),
        "error": str(err or "")[:160],
        "path": str(path or "")[:120],
    }


def _cooldown_scan_symbol(symbol: str, seconds: int, reason: str) -> None:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return
    pr = _load_single_profile(sym)
    if not pr:
        pr = {"wins": 0, "losses": 0, "realizedPnl": 0.0, "trades": 0}
    now = int(time.time())
    pr["scanErrorStreak"] = max(2, int(pr.get("scanErrorStreak", 0) or 0))
    pr["scanCooldownUntil"] = max(int(pr.get("scanCooldownUntil", 0) or 0), now + max(1, int(seconds or 1)))
    pr["lastScanErrorAt"] = now
    pr["lastScanErrorReason"] = str(reason or "symbol cooldown")[:120]
    pr["updatedAt"] = now
    _save_single_profile(sym, pr)


def _scan_error_penalty(symbol: str) -> float:
    hs = _scan_health_state(symbol)
    now = int(time.time())
    streak = max(0, int(hs.get("streak", 0) or 0))
    cooldown_until = max(0, int(hs.get("cooldownUntil", 0) or 0))
    last_error_at = max(0, int(hs.get("lastErrorAt", 0) or 0))
    penalty = float(streak) * 50.0
    if cooldown_until > now:
        penalty += 500.0 + float(cooldown_until - now) / 60.0
    elif last_error_at > 0:
        age_min = max(0.0, (now - last_error_at) / 60.0)
        penalty += max(0.0, 15.0 - min(15.0, age_min))
    return penalty


def _append_trade_log(entry: dict):
    global _LIVE_STATS_VERSION
    _ensure_vault()
    try:
        with TRADES_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        print(f"[Trade Log] Written to {TRADES_LOG_PATH}: mode={entry.get('mode')}, pnl={entry.get('pnl')}, symbol={entry.get('symbol')}")
        if str(entry.get("mode", "")).upper() == "LIVE" and "pnl" in entry:
            _LIVE_STATS_VERSION += 1
            print(f"[Trade Log] LIVE_STATS_VERSION incremented to {_LIVE_STATS_VERSION}")
            # NOTE: do NOT clear _LIVE_STATS_CACHE here. Cache key is keyed by
            # (symbol, mtime, size) so a stale entry is naturally invalidated
            # on the next read; clearing on every append forced a full re-parse
            # of the 100k+ line trade log per call which made /status
            # flap between healthy and stale under heavy
            # trade flow (see also _aggregate_live_trade_stats_from_log).
            _SESSION_BIAS_CACHE["builtAt"] = 0.0
    except Exception as exc:
        print(f"[Trade Log] ERROR writing {TRADES_LOG_PATH}: {exc}")


def _apply_trade_log_delta(stats: dict, lines: list[str], symbol: str) -> dict:
    """
    Apply new trade-log lines (oldest → newest) on top of a previously
    computed stats dict. Updates wins/losses/pnl/today counters and
    prepends new entries to ``lastTrades`` (most recent first, capped at 10).
    """
    if not lines:
        return stats
    sym = (symbol or "").upper().strip()
    now_local = time.localtime()
    today_key = (now_local.tm_year, now_local.tm_mon, now_local.tm_mday)
    new_last_trades: list[dict] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if str(obj.get("mode", "")).upper() != "LIVE":
            continue
        if sym and str(obj.get("symbol", "")).upper() != sym:
            continue
        if "pnl" not in obj:
            continue
        try:
            pnl = float(obj.get("pnl", 0.0) or 0.0)
        except Exception:
            continue
        if not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        if pnl >= 0:
            stats["wins"] += 1
        else:
            stats["losses"] += 1
        stats["realizedPnl"] = round(float(stats["realizedPnl"]) + pnl, 6)
        new_last_trades.append(obj)
        ts_raw = obj.get("closedAt", obj.get("ts", 0))
        try:
            ts = int(float(ts_raw or 0))
        except Exception:
            ts = 0
        if ts > 0:
            tloc = time.localtime(ts)
            if (tloc.tm_year, tloc.tm_mon, tloc.tm_mday) == today_key:
                if pnl >= 0:
                    stats["winsToday"] += 1
                else:
                    stats["lossesToday"] += 1
                stats["realizedPnlToday"] = round(float(stats["realizedPnlToday"]) + pnl, 6)
    # Merge new (most recent first) on top of existing lastTrades; cap at 10.
    if new_last_trades:
        existing = stats.get("lastTrades") if isinstance(stats.get("lastTrades"), list) else []
        merged: list[dict] = []
        # New trades are already in newest-first order (we read forward but
        # the file is append-only so the last line is most recent).
        for obj in reversed(new_last_trades):
            merged.append(obj)
            if len(merged) >= 10:
                break
        for obj in existing:
            if len(merged) >= 10:
                break
            merged.append(obj)
        stats["lastTrades"] = merged
    return stats


def _aggregate_live_trade_stats_from_log(symbol: str | None = None) -> dict:
    """
    Build LIVE KPI from trade log only (source-of-truth), with light anomaly filtering.

    Performance: the trade log can grow to 100k+ lines; full re-parse on every
    call would make `/autotrade/status-lite` block for several seconds during
    heavy trade flow. We cache by ``(symbol, mtime, size)`` and, on miss, look
    for a previous entry with the same symbol and a smaller size to apply only
    the appended delta (file is append-only; truncation is detected via mtime
    change and falls back to a full parse).
    """
    sym = str(symbol or "").upper().strip()
    now = time.time()
    if not TRADES_LOG_PATH.exists():
        return {
            "wins": 0, "losses": 0, "realizedPnl": 0.0,
            "winsToday": 0, "lossesToday": 0, "realizedPnlToday": 0.0,
            "lastTrades": [],
        }

    try:
        stat = TRADES_LOG_PATH.stat()
        mtime = float(stat.st_mtime)
        size = int(stat.st_size)
    except Exception:
        return {
            "wins": 0, "losses": 0, "realizedPnl": 0.0,
            "winsToday": 0, "lossesToday": 0, "realizedPnlToday": 0.0,
            "lastTrades": [],
        }

    cache_key = (sym, mtime, size)
    try:
        cached = _LIVE_STATS_CACHE.get(cache_key)
        if cached:
            return dict(cached[1])
    except Exception:
        cached = None

    # Try incremental update from the most recent cache entry for this symbol
    # whose recorded file size is <= current size. If mtime changed but size
    # is non-monotonic (truncation/rewrite) we fall through to full re-parse.
    base_stats: dict | None = None
    base_size = -1
    try:
        candidates = [
            (k, v) for k, v in _LIVE_STATS_CACHE.items()
            if isinstance(k, tuple) and len(k) == 3 and k[0] == sym
        ]
        candidates.sort(key=lambda kv: kv[0][2], reverse=True)  # largest size first
        for k, v in candidates:
            prev_mtime, prev_size = k[1], k[2]
            if prev_size <= size and abs(prev_mtime - mtime) < 1e-3:
                # Same mtime window → file is append-only since this cache
                base_stats = dict(v[1])
                base_size = prev_size
                break
            if prev_size <= size:
                # mtime drifted but size only grew → still safe to apply delta.
                # Only zero today's counters if the local date of the cached
                # entry is actually a different day than today. Every append
                # changes mtime, so mtime drift alone does NOT mean the day
                # rolled — and the delta only sees new appends, which are
                # mostly SCAN entries, so blindly zeroing erases real today's
                # trades that aren't in the delta window.
                base_stats = dict(v[1])
                try:
                    prev_tloc = time.localtime(prev_mtime)
                    prev_day_key = (prev_tloc.tm_year, prev_tloc.tm_mon, prev_tloc.tm_mday)
                except Exception:
                    prev_day_key = None
                if prev_day_key != today_key:
                    base_stats["winsToday"] = 0
                    base_stats["lossesToday"] = 0
                    base_stats["realizedPnlToday"] = 0.0
                base_size = prev_size
                break
    except Exception:
        base_stats = None
        base_size = -1

    if base_stats is not None and base_size >= 0 and base_size < size:
        try:
            with TRADES_LOG_PATH.open("rb") as f:
                f.seek(base_size)
                tail = f.read(size - base_size)
            new_text = tail.decode("utf-8", errors="replace")
            new_lines = new_text.splitlines()
            stats = _apply_trade_log_delta(base_stats, new_lines, sym)
        except Exception:
            stats = None
        if stats is not None:
            _LIVE_STATS_CACHE[cache_key] = (now, dict(stats))
            if len(_LIVE_STATS_CACHE) > 32:
                oldest_key = min(
                    _LIVE_STATS_CACHE,
                    key=lambda k: _LIVE_STATS_CACHE[k][0] if isinstance(_LIVE_STATS_CACHE[k], tuple) and len(_LIVE_STATS_CACHE[k]) >= 2 else 0,
                )
                _LIVE_STATS_CACHE.pop(oldest_key, None)
            return stats

    # Full re-parse fallback (cold cache or truncation detected).
    stats = {
        "wins": 0, "losses": 0, "realizedPnl": 0.0,
        "winsToday": 0, "lossesToday": 0, "realizedPnlToday": 0.0,
        "lastTrades": [],
    }
    now_local = time.localtime()
    today_key = (now_local.tm_year, now_local.tm_mon, now_local.tm_mday)
    try:
        rows = TRADES_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return stats
    parsed: list[dict] = []
    for line in rows:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if str(obj.get("mode", "")).upper() != "LIVE":
            continue
        if sym and str(obj.get("symbol", "")).upper() != sym:
            continue
        if "pnl" not in obj:
            continue
        try:
            pnl = float(obj.get("pnl", 0.0) or 0.0)
        except Exception:
            continue
        if not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        if pnl >= 0:
            stats["wins"] += 1
        else:
            stats["losses"] += 1
        stats["realizedPnl"] = round(float(stats["realizedPnl"]) + pnl, 6)
        parsed.append(obj)
        ts_raw = obj.get("closedAt", obj.get("ts", 0))
        try:
            ts = int(float(ts_raw or 0))
        except Exception:
            ts = 0
        if ts > 0:
            tloc = time.localtime(ts)
            if (tloc.tm_year, tloc.tm_mon, tloc.tm_mday) == today_key:
                if pnl >= 0:
                    stats["winsToday"] += 1
                else:
                    stats["lossesToday"] += 1
                stats["realizedPnlToday"] = round(float(stats["realizedPnlToday"]) + pnl, 6)
    # Newest-first for lastTrades (cap 10).
    stats["lastTrades"] = list(reversed(parsed[-10:]))
    _LIVE_STATS_CACHE[cache_key] = (now, dict(stats))
    if len(_LIVE_STATS_CACHE) > 32:
        oldest_key = min(
            _LIVE_STATS_CACHE,
            key=lambda k: _LIVE_STATS_CACHE[k][0] if isinstance(_LIVE_STATS_CACHE[k], tuple) and len(_LIVE_STATS_CACHE[k]) >= 2 else 0,
        )
        _LIVE_STATS_CACHE.pop(oldest_key, None)
    return stats


def _today_entry_performance_guard(cfg: dict | None = None) -> dict:
    cfg = cfg if isinstance(cfg, dict) else {}
    if not bool(cfg.get("todayPerformanceGuardEnabled", True)):
        return {"active": False, "reason": "disabled"}
    stats = _aggregate_live_trade_stats_from_log(None)
    wins = int(stats.get("winsToday", 0) or 0)
    losses = int(stats.get("lossesToday", 0) or 0)
    trades = wins + losses
    min_trades = max(1, int(cfg.get("todayPerformanceGuardMinTrades", 8) or 8))
    max_wr = float(cfg.get("todayPerformanceGuardMaxWinRatePct", 40.0) or 40.0)
    max_pnl = float(cfg.get("todayPerformanceGuardMaxPnlUsdt", 0.0) or 0.0)
    pnl = float(stats.get("realizedPnlToday", 0.0) or 0.0)
    win_rate = (wins / max(trades, 1)) * 100.0 if trades > 0 else 0.0
    active = trades >= min_trades and win_rate < max_wr and pnl < max_pnl
    return {
        "active": bool(active),
        "reason": "today_underperforming" if active else "ok",
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "winRatePct": round(win_rate, 2),
        "pnl": round(pnl, 6),
        "minTrades": min_trades,
        "maxWinRatePct": round(max_wr, 2),
    }


def _aggregate_live_trade_stats_by_symbol_from_log() -> dict[str, dict]:
    """
    Build per-symbol LIVE KPI in one pass for learning/status.
    Avoids calling _aggregate_live_trade_stats_from_log once per symbol, which
    repeatedly rereads a large trades log and can stall the learning endpoint.
    """
    out: dict[str, dict] = {}
    if not TRADES_LOG_PATH.exists():
        return out
    try:
        rows = TRADES_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return out
    for line in rows:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if str(obj.get("mode", "")).upper() != "LIVE":
            continue
        sym = str(obj.get("symbol", "") or "").upper().strip()
        if not sym or "pnl" not in obj:
            continue
        try:
            pnl = float(obj.get("pnl", 0.0) or 0.0)
        except Exception:
            continue
        if not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        stats = out.setdefault(sym, {"wins": 0, "losses": 0, "realizedPnl": 0.0})
        if pnl >= 0:
            stats["wins"] += 1
        else:
            stats["losses"] += 1
        stats["realizedPnl"] = round(float(stats["realizedPnl"]) + pnl, 6)
    return out


def _supervisor_trade_period_reviews(trades: list[dict], *, now_ts: int | None = None) -> list[dict]:
    cleaned: list[dict] = []
    for trade in trades or []:
        if not isinstance(trade, dict):
            continue
        try:
            pnl = float(trade.get("_pnl", trade.get("pnl", 0.0)) or 0.0)
        except Exception:
            continue
        if not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        try:
            ts = int(float(trade.get("_ts", trade.get("closedAt", trade.get("ts", 0))) or 0))
        except Exception:
            ts = 0
        item = dict(trade)
        item["_pnl"] = pnl
        item["_ts"] = ts
        cleaned.append(item)
    cleaned.sort(key=lambda x: int(x.get("_ts", 0) or 0))
    if not cleaned:
        return []

    def build_window(rows: list[dict], label: str, previous: list[dict] | None = None) -> dict:
        pnls = [float(t.get("_pnl", 0.0) or 0.0) for t in rows]
        wins = [p for p in pnls if p >= 0.0]
        losses = [p for p in pnls if p < 0.0]
        win_rate = (len(wins) / max(len(rows), 1)) * 100.0
        pnl_sum = sum(pnls)
        avg_win = sum(wins) / max(len(wins), 1) if wins else 0.0
        avg_loss = sum(losses) / max(len(losses), 1) if losses else 0.0
        gross_win = sum(wins)
        gross_loss = abs(sum(losses))
        profit_factor = gross_win / gross_loss if gross_loss > 0 else (999.0 if gross_win > 0 else 0.0)
        payoff_ratio = avg_win / abs(avg_loss) if avg_win > 0 and avg_loss < 0 else 0.0
        reasons: dict[str, int] = {}
        symbols: dict[str, dict] = {}
        quick_losses = 0
        small_wins = 0
        for trade in rows:
            reason = str(trade.get("reason", "UNKNOWN") or "UNKNOWN").upper()
            reasons[reason] = reasons.get(reason, 0) + 1
            sym = str(trade.get("symbol", "") or "UNKNOWN").upper()
            bucket = symbols.setdefault(sym, {"trades": 0, "pnl": 0.0})
            bucket["trades"] += 1
            bucket["pnl"] = round(float(bucket["pnl"]) + float(trade.get("_pnl", 0.0) or 0.0), 6)
            opened = int(float(trade.get("openedAt", 0) or 0))
            closed = int(float(trade.get("_ts", 0) or 0))
            minutes = (closed - opened) / 60.0 if opened > 0 and closed >= opened else None
            pnl = float(trade.get("_pnl", 0.0) or 0.0)
            if pnl < 0 and minutes is not None and minutes <= 8:
                quick_losses += 1
            if 0.0 < pnl < 0.25:
                small_wins += 1
        top_reasons = sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:3]
        worst_symbols = sorted(symbols.items(), key=lambda x: float(x[1].get("pnl", 0.0)))[:3]
        review = {
            "label": label,
            "trades": len(rows),
            "wins": len(wins),
            "losses": len(losses),
            "winRatePct": round(win_rate, 2),
            "pnl": round(pnl_sum, 6),
            "avgPnl": round(pnl_sum / max(len(rows), 1), 6),
            "profitFactor": round(min(profit_factor, 999.0), 4),
            "payoffRatio": round(payoff_ratio, 4),
            "avgWin": round(avg_win, 6),
            "avgLoss": round(avg_loss, 6),
            "quickLosses": quick_losses,
            "smallWins": small_wins,
            "topReasons": [{"reason": r, "count": c} for r, c in top_reasons],
            "worstSymbols": [
                {"symbol": sym, "trades": int(data.get("trades", 0)), "pnl": round(float(data.get("pnl", 0.0)), 6)}
                for sym, data in worst_symbols
            ],
        }
        if previous:
            prev_pnls = [float(t.get("_pnl", 0.0) or 0.0) for t in previous]
            prev_wr = (sum(1 for p in prev_pnls if p >= 0.0) / max(len(prev_pnls), 1)) * 100.0
            prev_avg = sum(prev_pnls) / max(len(prev_pnls), 1)
            review["previous"] = {"winRatePct": round(prev_wr, 2), "avgPnl": round(prev_avg, 6)}
            review["trend"] = (
                "degrading"
                if win_rate < prev_wr - 10.0 or review["avgPnl"] < prev_avg - 0.04
                else "improving"
                if win_rate > prev_wr + 10.0 and review["avgPnl"] > prev_avg + 0.03
                else "stable"
            )
        else:
            review["trend"] = "insufficient_previous"
        return review

    reviews: list[dict] = []
    for window in (8, 20, 50):
        if len(cleaned) < max(4, min(window, 8)):
            continue
        rows = cleaned[-window:]
        previous = cleaned[-(window * 2) : -window] if len(cleaned) >= window * 2 else []
        reviews.append(build_window(rows, f"last_{min(window, len(rows))}_trades", previous or None))
    return reviews


def _symbol_drag_candidate_from_review(review: dict, cfg: dict | None = None) -> dict:
    if not isinstance(review, dict):
        return {}
    cfg = cfg if isinstance(cfg, dict) else {}
    worst_symbols = review.get("worstSymbols") if isinstance(review.get("worstSymbols"), list) else []
    worst = worst_symbols[0] if worst_symbols and isinstance(worst_symbols[0], dict) else {}
    sym = str(worst.get("symbol", "") or "").upper().strip()
    if not sym or sym == "UNKNOWN":
        return {}
    try:
        trades_n = int(review.get("trades", 0) or 0)
        total_pnl = float(review.get("pnl", 0.0) or 0.0)
        worst_trades = int(worst.get("trades", 0) or 0)
        worst_pnl = float(worst.get("pnl", 0.0) or 0.0)
    except Exception:
        return {}
    min_trades = int(cfg.get("symbolDragLockMinTrades", 3) or 3)
    min_loss = abs(float(cfg.get("symbolDragLockMinLossUsdt", 0.50) or 0.50))
    dominance = abs(worst_pnl) / max(abs(total_pnl), 0.01)
    extreme_dominance = (
        worst_trades >= 2
        and worst_pnl <= -(min_loss * 1.25)
        and dominance >= float(cfg.get("symbolDragExtremeDominanceMin", 1.25) or 1.25)
    )
    dominant = (
        trades_n >= 6
        and (worst_trades >= min_trades or extreme_dominance)
        and worst_pnl <= -min_loss
        and total_pnl < 0.0
        and dominance >= float(cfg.get("symbolDragDominanceMin", 0.75) or 0.75)
    )
    if not dominant:
        return {}
    return {
        "symbol": sym,
        "trades": worst_trades,
        "pnl": round(worst_pnl, 6),
        "dominance": round(dominance, 4),
        "label": str(review.get("label", "") or ""),
    }


def _maybe_lock_symbol_drag_from_review(review: dict, cfg: dict | None = None) -> dict:
    candidate = _symbol_drag_candidate_from_review(review, cfg)
    if not candidate:
        return {}
    cfg = cfg if isinstance(cfg, dict) else {}
    now = int(time.time())
    locks = AUTO_TRADE.get("perfLocks")
    if not isinstance(locks, dict):
        locks = {}
    sym = str(candidate.get("symbol", "") or "").upper().strip()
    current = locks.get(sym) if isinstance(locks.get(sym), dict) else {}
    if int(current.get("until", 0) or 0) > now:
        candidate["locked"] = True
        candidate["alreadyLocked"] = True
        candidate["until"] = int(current.get("until", 0) or 0)
        return candidate
    lock_min = max(30, int(cfg.get("symbolDragLockMinutes", cfg.get("perfLockMinutes", 120)) or 120))
    until = now + (lock_min * 60)
    locks[sym] = {
        "until": until,
        "at": now,
        "reason": "symbol_drag",
        "perf": {
            "trades": int(candidate.get("trades", 0) or 0),
            "pnl": float(candidate.get("pnl", 0.0) or 0.0),
            "source": str(candidate.get("label", "") or ""),
            "dominance": float(candidate.get("dominance", 0.0) or 0.0),
        },
    }
    AUTO_TRADE["perfLocks"] = locks
    candidate["locked"] = True
    candidate["alreadyLocked"] = False
    candidate["until"] = until
    candidate["minutes"] = lock_min
    return candidate


def _maybe_tune_weak_payoff_from_review(review: dict, cfg: dict | None = None) -> dict:
    if not isinstance(review, dict):
        return {}
    cfg = cfg if isinstance(cfg, dict) else {}
    try:
        trades_n = int(review.get("trades", 0) or 0)
        payoff_ratio = float(review.get("payoffRatio", 0.0) or 0.0)
        avg_win = float(review.get("avgWin", 0.0) or 0.0)
        avg_loss = abs(float(review.get("avgLoss", 0.0) or 0.0))
    except Exception:
        return {}
    if trades_n < 6 or payoff_ratio <= 0.0 or payoff_ratio >= 0.75:
        return {}

    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("weak_payoff", cfg, 45)
    signature = _tuning_signature("weak_payoff", trades=trades_n, payoff=round(payoff_ratio, 4))
    rec = delegations.get("weak_payoff") if isinstance(delegations.get("weak_payoff"), dict) else {}
    if active and str(rec.get("signature", "") or "") == signature:
        return {"applied": False, "alreadyTuned": True, "signature": signature, "cooldownSec": cooldown_sec}

    # Auto-rollback if previous tuning worsened performance
    if _tuning_should_rollback("weak_payoff"):
        rollback = _tuning_rollback_last("weak_payoff")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state, delegations, "weak_payoff", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    # Adaptive severity: 0.0 (mild) → 1.0 (severe)
    severity = max(0.0, min(1.0, (0.75 - payoff_ratio) / 0.75 + (avg_loss / max(avg_win, 0.01) - 1.0) / 2.0))

    changes: dict[str, dict] = {}

    def set_float(key: str, value: float, digits: int = 4):
        old = float(cfg.get(key, value) or value)
        new = round(float(value), digits)
        if abs(old - new) >= 1e-9:
            cfg[key] = new
            changes[key] = {"old": round(old, digits), "new": new}

    if not bool(cfg.get("holdWinners", True)):
        changes["holdWinners"] = {"old": bool(cfg.get("holdWinners", False)), "new": True}
        cfg["holdWinners"] = True

    hold_min = float(cfg.get("holdMinConfidence", 0.78) or 0.78)
    set_float("holdMinConfidence", max(0.68, min(0.86, hold_min - 0.03 * (1.0 + severity))), 3)

    tp_min = max(0.20, float(cfg.get("tpTargetMinUsdt", 0.55) or 0.55))
    tp_max = max(tp_min + 0.10, float(cfg.get("tpTargetMaxUsdt", 2.2) or 2.2))
    set_float("tpTargetMinUsdt", min(1.20, tp_min * (1.0 + 0.15 * severity)), 3)
    set_float("tpTargetMaxUsdt", min(3.20, max(tp_max * (1.0 + 0.12 * severity), float(cfg.get("tpTargetMinUsdt", tp_min)) * 2.4)), 3)

    be_trigger = float(cfg.get("profitLockBreakevenTriggerUsdt", 0.16) or 0.16)
    set_float("profitLockBreakevenTriggerUsdt", min(0.45, max(0.20, be_trigger * (1.0 + 0.15 * severity))), 3)

    weak_loss_pressure = avg_loss > 0 and avg_win > 0 and avg_loss > avg_win * 1.35
    if weak_loss_pressure:
        sl_pct = max(0.20, float(cfg.get("stopLossPct", 0.9) or 0.9))
        loss_pressure = avg_loss / max(avg_win, 0.01)
        sl_factor = max(0.70, 0.88 if loss_pressure >= 2.4 else 0.94 - 0.06 * severity)
        set_float("stopLossPct", max(0.48, sl_pct * sl_factor), 3)
        cap = float(cfg.get("payoffLossGuardLossToWinCap", 1.05) or 1.05)
        set_float("payoffLossGuardLossToWinCap", max(0.85, min(cap, 0.95 - 0.05 * severity)), 3)
        max_loss_usdt = float(cfg.get("payoffLossGuardMaxLossUsdt", 0.9) or 0.9)
        target_loss_cap = max(0.25, min(max_loss_usdt, avg_win * 1.25))
        set_float("payoffLossGuardMaxLossUsdt", max(0.25, min(0.75, target_loss_cap * (1.0 - 0.10 * severity))), 3)
        min_loss_usdt = float(cfg.get("payoffLossGuardMinLossUsdt", 0.22) or 0.22)
        set_float("payoffLossGuardMinLossUsdt", max(0.12, min(min_loss_usdt, avg_win * 0.90)), 3)
        size_mult = float(cfg.get("supervisorSizeMultiplier", 1.0) or 1.0)
        set_float("supervisorSizeMultiplier", max(0.70, min(size_mult, 0.85 - 0.10 * severity)), 3)

    if not changes:
        return {"applied": False, "reason": "no_safe_delta", "signature": signature}

    state.update({
        "at": int(time.time()),
        "signature": signature,
        "reason": "weak_payoff_ratio",
        "payoffRatio": round(payoff_ratio, 4),
        "avgWin": round(avg_win, 6),
        "avgLoss": round(avg_loss, 6),
        "severity": round(severity, 3),
        "changes": changes,
    })
    AUTO_TRADE["supervisorAutoTune"] = state
    out = _commit_supervisor_config_tune(state, delegations, "weak_payoff", cfg, changes, "weak_payoff_ratio")
    out["signature"] = signature
    out["severity"] = round(severity, 3)
    return out


def _supervisor_delegation_cooldown(key: str, cfg: dict, default_minutes: int) -> tuple[dict, dict, bool, int]:
    """Per-tuning-type cooldown with independent tracking."""
    now = int(time.time())
    state = AUTO_TRADE.get("supervisorAutoTune")
    if not isinstance(state, dict):
        state = {}
    delegations = state.get("delegations")
    if not isinstance(delegations, dict):
        delegations = {}
    rec = delegations.get(key) if isinstance(delegations.get(key), dict) else {}
    cfg_key = {
        "low_entry_activity": "supervisorLowEntryTuneCooldownMinutes",
        "bad_utc_hour": "supervisorBadUtcTuneCooldownMinutes",
        "negative_expectancy": "supervisorNegativeExpectancyTuneCooldownMinutes",
        "daily_entry_regression": "supervisorDailyRegressionCooldownMinutes",
        "small_profit_capture": "supervisorSmallProfitCooldownMinutes",
        "weak_payoff": "supervisorPayoffTuneCooldownMinutes",
        "size_streak": "supervisorSizeStreakCooldownMinutes",
        "scan_timeout": "supervisorScanTimeoutCooldownMinutes",
    }.get(key, "supervisorDelegationCooldownMinutes")
    cooldown_sec = max(300, int(cfg.get(cfg_key, default_minutes) or default_minutes) * 60)
    active = now - int(rec.get("at", 0) or 0) < cooldown_sec
    state["delegations"] = delegations
    return state, delegations, active, cooldown_sec


def _tuning_history_append(key: str, changes: dict, pre_metrics: dict | None = None) -> None:
    """Record tuning action for impact tracking and rollback."""
    entry = {
        "at": int(time.time()),
        "key": key,
        "changes": dict(changes) if changes else {},
        "preMetrics": dict(pre_metrics) if pre_metrics else {},
        "reverted": False,
    }
    history = AUTO_TRADE.setdefault("tuningHistory", [])
    if not isinstance(history, list):
        history = []
        AUTO_TRADE["tuningHistory"] = history
    history.append(entry)
    if len(history) > 50:
        AUTO_TRADE["tuningHistory"] = history[-50:]


def _tuning_rollback_last(key: str) -> dict:
    """Rollback the most recent tuning of this type. Returns pre-tune values if found."""
    history = AUTO_TRADE.get("tuningHistory")
    if not isinstance(history, list):
        return {"reverted": False, "reason": "no_history"}
    for entry in reversed(history):
        if entry.get("key") == key and not entry.get("reverted"):
            entry["reverted"] = True
            pre = entry.get("preMetrics", {}) or {}
            return {"reverted": True, "preMetrics": pre, "changes": entry.get("changes", {})}
    return {"reverted": False, "reason": "no_matching_entry"}


def _tuning_pre_metrics() -> dict:
    """Capture current performance metrics before tuning."""
    stats = _aggregate_live_trade_stats_from_log(None) or {}
    return {
        "winRatePct": stats.get("winRatePct", 0.0),
        "avgPnl": stats.get("avgPnl", 0.0),
        "payoffRatio": stats.get("payoffRatio", 0.0),
        "realizedPnl": stats.get("realizedPnl", 0.0),
        "trades": stats.get("trades", 0),
    }


def _tuning_signature(key: str, **parts) -> str:
    """Unique tuning signature using hash of key + parts."""
    import hashlib
    payload = json.dumps({"key": key, **parts}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _tuning_should_rollback(key: str, post_window_trades: int = 3) -> bool:
    """Check if recent tuning worsened performance. Auto-rollback if yes."""
    history = AUTO_TRADE.get("tuningHistory")
    if not isinstance(history, list):
        return False
    for entry in reversed(history):
        if entry.get("key") != key or entry.get("reverted"):
            continue
        age = time.time() - int(entry.get("at", 0) or 0)
        if age < 60 or age > 3600 * 4:
            continue
        pre = entry.get("preMetrics", {}) or {}
        if not pre:
            continue
        current = _aggregate_live_trade_stats_from_log(None) or {}
        pre_wr = float(pre.get("winRatePct", 0.0) or 0.0)
        cur_wr = float(current.get("winRatePct", 0.0) or 0.0)
        pre_pnl = float(pre.get("avgPnl", 0.0) or 0.0)
        cur_pnl = float(current.get("avgPnl", 0.0) or 0.0)
        if pre_wr > 0 and cur_wr < pre_wr - 12.0 and cur_pnl < pre_pnl - 0.05:
            return True
        if pre_pnl > 0 and cur_pnl < pre_pnl - 0.08:
            return True
    return False


def _commit_supervisor_config_tune(state: dict, delegations: dict, key: str, cfg: dict, changes: dict, reason: str) -> dict:
    now = int(time.time())
    delegations[key] = {
        "at": now,
        "reason": reason,
        "changes": changes,
    }
    state["delegations"] = delegations
    AUTO_TRADE["supervisorAutoTune"] = state
    AUTO_TRADE["config"] = copy.deepcopy(cfg)
    _tuning_history_append(key, changes, _tuning_pre_metrics())
    try:
        _persist_autotrade_snapshot()
        _autotrade_log(f"Supervisor delegated {key}: {changes}")
    except Exception:
        pass
    return {"applied": True, "changes": changes, "reason": reason}


# NOTE: MarketContext / TradingView MCP signal guard was removed (package marketcontext-mcp-server
# no longer exists on PyPI). The hermes supervisor now ignores external-signal findings and the
# guard helpers below were deleted. See .hermes-backups/marketcontext-removal-* for the prior code.


def _maybe_tune_external_signal_guard(payload: dict | None, cfg: dict | None = None) -> dict:
    """No-op stub: external MCP signal guard was removed.

    Kept as a thin shim so existing call sites (e.g. /hermes/supervisor/external-signal)
    do not break. Any payload is acknowledged but not acted upon.
    """
    return {"applied": False, "reason": "external_mcp_removed"}


def _sse_json_payload(text: str) -> dict:
    raw = str(text or "").strip()
    if not raw:
        return {}
    if raw.startswith("{"):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    data_lines = []
    for line in raw.splitlines():
        line = line.strip()
        if line.startswith("data:"):
            data_lines.append(line[5:].strip())
    if not data_lines:
        return {}
    try:
        return json.loads("\n".join(data_lines))
    except Exception:
        return {}



def _maybe_clear_bad_utc_hour_from_config(skip_code: str, skip_msg: str, cfg: dict | None = None) -> dict:
    if str(skip_code or "") != "bad_utc_hour" or not isinstance(cfg, dict):
        return {}
    bad_hours = cfg.get("liveBadUtcHours")
    if not isinstance(bad_hours, list):
        return {}
    parsed_hours = set()
    for match in re.finditer(r"\b(?:UTC\s*)?hour\s*(\d{1,2})\b|\bbad UTC hour\s*(\d{1,2})\b", str(skip_msg or ""), re.IGNORECASE):
        raw_hour = match.group(1) or match.group(2)
        if raw_hour is not None:
            parsed_hours.add(int(raw_hour))
    parsed_hours = {int(h) for h in parsed_hours if 0 <= int(h) <= 23}
    current_hour = int(time.gmtime().tm_hour)
    target_hours = parsed_hours or {current_hour}
    old_hours = list(bad_hours)
    new_hours = []
    removed: list[int] = []
    for raw in old_hours:
        try:
            hour = int(raw)
        except Exception:
            new_hours.append(raw)
            continue
        if hour in target_hours:
            removed.append(hour)
        else:
            new_hours.append(raw)
    if not removed:
        return {}
    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("bad_utc_hour", cfg, 30)
    if active:
        return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec, "removed": removed}
    cfg["liveBadUtcHours"] = new_hours
    changes = {"liveBadUtcHours": {"old": old_hours, "new": new_hours, "removed": removed}}
    out = _commit_supervisor_config_tune(state, delegations, "bad_utc_hour", cfg, changes, "bad_utc_hour")
    out["removed"] = removed
    return out


def _maybe_tune_low_entry_activity(reason: str, cfg: dict | None = None, board: list[dict] | None = None) -> dict:
    if not isinstance(cfg, dict):
        return {}
    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("low_entry_activity", cfg, 30)

    if _tuning_should_rollback("low_entry_activity"):
        rollback = _tuning_rollback_last("low_entry_activity")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state, delegations, "low_entry_activity", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    changes: dict[str, dict] = {}

    def set_float(key: str, value: float, digits: int = 3):
        old = float(cfg.get(key, value) or value)
        new = round(float(value), digits)
        if abs(old - new) >= 1e-9:
            cfg[key] = new
            changes[key] = {"old": round(old, digits), "new": new}

    def set_int(key: str, value: int):
        old = int(cfg.get(key, value) or value)
        new = int(value)
        if old != new:
            cfg[key] = new
            changes[key] = {"old": old, "new": new}

    board_rows = [row for row in (board or []) if isinstance(row, dict)]
    reject_counts: dict[str, int] = {}
    momentum_values: list[float] = []
    max_spread_bps = float(cfg.get("maxSpreadBps", 16.0) or 16.0)
    recovery_relax = max(0.0, min(0.04, float(cfg.get("supervisorStuckLowConfRelax", 0.04) or 0.04)))
    near_miss_rows: list[dict] = []
    for row in board_rows:
        rr = str(row.get("rejectReason", "") or "unknown")
        reject_counts[rr] = reject_counts.get(rr, 0) + 1
        try:
            momentum_values.append(abs(float(row.get("momentumPct", 0.0) or 0.0)))
        except Exception:
            pass
        if rr == "low_conf":
            try:
                conf = float(row.get("confidence", 0.0) or 0.0)
                adaptive_min = float(row.get("adaptiveMinConf", cfg.get("minConfidence", 0.62)) or cfg.get("minConfidence", 0.62))
                spread = float(row.get("spreadBps", 999.0) or 999.0)
            except Exception:
                continue
            if conf > 0.0 and 0.0 <= (adaptive_min - conf) <= recovery_relax and spread <= max_spread_bps:
                near_miss_rows.append(row)
    stuck_no_entry = (
        bool(AUTO_TRADE.get("running"))
        and str((cfg or {}).get("executionMode", "") or "").upper() == "LIVE"
        and int(AUTO_TRADE.get("tradesLastHour", 0) or 0) == 0
        and not AUTO_TRADE.get("openLivePositions")
        and str((AUTO_TRADE.get("lastSkip") or {}).get("code", "") or reason or "") in {"scan_none", "low_entry_activity"}
        and bool(near_miss_rows)
    )
    if active and not stuck_no_entry:
        return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec}
    if active and stuck_no_entry:
        rec = delegations.get("low_entry_activity") if isinstance(delegations.get("low_entry_activity"), dict) else {}
        recovery_cooldown_sec = max(60, int(cfg.get("supervisorStuckLowEntryRecoveryCooldownSec", 300) or 300))
        if (
            str(rec.get("reason", "") or "") == "stuck_low_entry_recovery"
            and int(time.time()) - int(rec.get("at", 0) or 0) < recovery_cooldown_sec
        ):
            return {
                "applied": False,
                "alreadyTuned": True,
                "cooldownSec": recovery_cooldown_sec,
                "stuckRecovery": True,
            }
    quiet_board = bool(board_rows) and (
        (momentum_values and max(momentum_values) < 0.35)
        or all(str(row.get("rejectReason", "") or "") in {"signal_wait", "low_conf", "perf_lock", ""} for row in board_rows)
    )

    # Adaptive severity based on tradesLastHour=0 duration
    no_entry_metric = 0.0 if stuck_no_entry else (0.5 if quiet_board else 1.0)
    severity = max(0.0, min(1.0, (1.0 - no_entry_metric) / 1.0))

    target_min = max(1, int(cfg.get("supervisorTargetOpenPositionsMin", 3) or 3))
    target_max = max(target_min, int(cfg.get("supervisorTargetOpenPositionsMax", 6) or 6))
    target_min = min(target_min, 6)
    target_max = min(max(target_max, target_min), 6)
    old_max_open = max(1, int(cfg.get("maxOpenPositions", target_max) or target_max))
    if old_max_open < target_min:
        set_int("maxOpenPositions", target_min)
    elif old_max_open > target_max:
        set_int("maxOpenPositions", target_max)
    cfg["supervisorTargetOpenPositionsMin"] = target_min
    cfg["supervisorTargetOpenPositionsMax"] = target_max

    min_conf = float(cfg.get("minConfidence", 0.62) or 0.62)
    early_conf = float(cfg.get("earlyEntryMinConfidence", 0.60) or 0.60)
    if stuck_no_entry:
        min_floor = max(0.60, min(0.82, float(cfg.get("supervisorStuckLowEntryMinConfidenceFloor", 0.76) or 0.76)))
        early_floor = max(0.56, min(0.78, float(cfg.get("supervisorStuckLowEntryEarlyConfidenceFloor", 0.68) or 0.68)))
        set_float("minConfidence", max(min_floor, min_conf - 0.015 * (1.0 + severity * 2.0)), 3)
        set_float("earlyEntryMinConfidence", max(early_floor, early_conf - 0.015 * (1.0 + severity * 2.0)), 3)
        for key in ("scanFallbackNearConfRelax", "scanGuardedFallbackConfRelax"):
            old = cfg.get(key)
            old_num = float(old) if old is not None else None
            if old_num is None or abs(old_num - recovery_relax) >= 1e-9:
                cfg[key] = round(recovery_relax, 3)
                changes[key] = {"old": old, "new": round(recovery_relax, 3)}
        if not bool(cfg.get("scanFallbackNearEnabled", True)):
            changes["scanFallbackNearEnabled"] = {"old": bool(cfg.get("scanFallbackNearEnabled", False)), "new": True}
            cfg["scanFallbackNearEnabled"] = True
        if not bool(cfg.get("scanGuardedFallbackEnabled", True)):
            changes["scanGuardedFallbackEnabled"] = {"old": bool(cfg.get("scanGuardedFallbackEnabled", False)), "new": True}
            cfg["scanGuardedFallbackEnabled"] = True
    else:
        base_step_min = 0.03 if quiet_board else 0.02
        base_step_early = 0.025 if quiet_board else 0.015
        set_float("minConfidence", max(0.48 if quiet_board else 0.50, min_conf - base_step_min * (1.0 + severity * 2.0)), 3)
        set_float("earlyEntryMinConfidence", max(0.50 if quiet_board else 0.52, early_conf - base_step_early * (1.0 + severity * 2.0)), 3)
    if quiet_board and not stuck_no_entry:
        score_gap = float(cfg.get("earlyEntryScoreGapMin", 1.40) or 1.40)
        set_float("earlyEntryScoreGapMin", max(0.90, score_gap - 0.12 * (1.0 + severity * 2.0)), 3)
        hybrid_score = float(cfg.get("hybridMinScore", 0.72) or 0.72)
        set_float("hybridMinScore", max(0.62, hybrid_score - 0.025 * (1.0 + severity * 2.0)), 3)
        hybrid_edge = float(cfg.get("hybridMinEdge", 0.06) or 0.06)
        set_float("hybridMinEdge", max(0.025, hybrid_edge - 0.01 * (1.0 + severity * 2.0)), 3)
    analyze_top = max(3, int(cfg.get("scanAnalyzeTop", 8) or 8))
    set_int("scanAnalyzeTop", min(16, analyze_top + int(round(2 * (1.0 + severity * 2.0)))))
    top_liquid = max(5, int(cfg.get("scanTopLiquid", 30) or 30))
    set_int("scanTopLiquid", min(80, top_liquid + int(round(10 * (1.0 + severity * 2.0)))))
    guarded_top = max(analyze_top + 2, int(cfg.get("scanGuardedFallbackAnalyzeTop", analyze_top * 2) or analyze_top * 2))
    set_int("scanGuardedFallbackAnalyzeTop", min(24, max(guarded_top, int(cfg.get("scanAnalyzeTop", analyze_top)))))
    fallback_keys = ("scanFallbackNearEnabled",) if stuck_no_entry else ("scanFallbackNearEnabled", "scanPerfSoftFallbackEnabled")
    for key in fallback_keys:
        if not bool(cfg.get(key, True)):
            changes[key] = {"old": bool(cfg.get(key, False)), "new": True}
            cfg[key] = True

    if not changes:
        return {"applied": False, "reason": "no_safe_delta"}
    signature = _tuning_signature("low_entry_activity", stuck=stuck_no_entry, quiet=quiet_board, reason=str(reason or "low_entry_activity"))
    out = _commit_supervisor_config_tune(
        state,
        delegations,
        "low_entry_activity",
        cfg,
        changes,
        "stuck_low_entry_recovery" if stuck_no_entry else str(reason or "low entry activity"),
    )
    out["rejectCounts"] = reject_counts
    out["quietMarket"] = quiet_board
    out["targetOpenPositions"] = {"min": target_min, "max": target_max}
    out["stuckRecovery"] = stuck_no_entry
    out["severity"] = severity
    out["signature"] = signature
    if stuck_no_entry:
        out["nearMissSymbols"] = [str(row.get("symbol", "") or "") for row in near_miss_rows[:5]]
    return out


def _maybe_tune_scan_timeout_from_skip(skip_msg: str, cfg: dict | None = None) -> dict:
    if not isinstance(cfg, dict):
        return {}
    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("scan_timeout", cfg, 20)
    if active:
        return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec}

    if _tuning_should_rollback("scan_timeout"):
        rollback = _tuning_rollback_last("scan_timeout")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state, delegations, "scan_timeout", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    changes: dict[str, dict] = {}

    def set_float(key: str, value: float, digits: int = 2):
        old = float(cfg.get(key, value) or value)
        new = round(float(value), digits)
        if abs(old - new) >= 1e-9:
            cfg[key] = new
            changes[key] = {"old": round(old, digits), "new": new}

    def set_int(key: str, value: int):
        old = int(cfg.get(key, value) or value)
        new = int(value)
        if old != new:
            cfg[key] = new
            changes[key] = {"old": old, "new": new}

    per_symbol_timeout = max(2.0, float(cfg.get("scanPerSymbolTimeoutSec", 7.5) or 7.5))
    # Adaptive severity based on timeout deviation from baseline
    severity = max(0.0, min(1.0, (per_symbol_timeout - 2.0) / 8.0))
    set_float("scanPerSymbolTimeoutSec", min(10.0, per_symbol_timeout * (1.0 + 0.15 * (1.0 + severity * 2.0))), 2)
    analyze_top = max(3, int(cfg.get("scanAnalyzeTop", 8) or 8))
    set_int("scanAnalyzeTop", max(3, analyze_top - int(round(1 * (1.0 + severity * 2.0)))))
    guarded_top = max(analyze_top, int(cfg.get("scanGuardedFallbackAnalyzeTop", max(analyze_top * 2, analyze_top + 4)) or analyze_top))
    set_int("scanGuardedFallbackAnalyzeTop", max(int(cfg.get("scanAnalyzeTop", analyze_top) or analyze_top), guarded_top - int(round(2 * (1.0 + severity * 2.0)))))
    fallback_retries = max(1, int(cfg.get("scanFallbackRetrySymbols", 3) or 3))
    set_int("scanFallbackRetrySymbols", max(1, fallback_retries - int(round(1 * (1.0 + severity * 2.0)))))

    if not changes:
        return {"applied": False, "reason": "no_safe_delta"}
    signature = _tuning_signature("scan_timeout", skip_msg=str(skip_msg or "scan timeout"), per_symbol_timeout=per_symbol_timeout)
    out = _commit_supervisor_config_tune(state, delegations, "scan_timeout", cfg, changes, str(skip_msg or "scan timeout"))
    out["severity"] = severity
    out["signature"] = signature
    return out


def _daily_trade_regime_review(trades: list[dict], cfg: dict | None = None, *, now_ts: int | None = None) -> dict:
    cfg = cfg if isinstance(cfg, dict) else {}
    now_ts = int(now_ts or time.time())
    today_key = time.strftime("%Y-%m-%d", time.localtime(now_ts))

    # Cache to avoid re-processing all trades every call
    cache = AUTO_TRADE.setdefault("_dailyRegimeCache", {})
    cache_key = (today_key, len(trades or []))
    if cache_key in cache:
        return cache[cache_key]

    min_trades = max(4, int(cfg.get("supervisorDailyBaselineMinTrades", 8) or 8))
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
        item["_day"] = time.strftime("%Y-%m-%d", time.localtime(ts))
        cleaned.append(item)
    if not cleaned:
        return {}

    grouped: dict[str, list[dict]] = {}
    for trade in cleaned:
        grouped.setdefault(str(trade.get("_day")), []).append(trade)

    def summarize(day: str, rows: list[dict]) -> dict:
        pnls = [float(t.get("_pnl", 0.0) or 0.0) for t in rows]
        wins = [p for p in pnls if p >= 0.0]
        losses = [p for p in pnls if p < 0.0]
        small_wins = sum(1 for p in wins if 0.0 < p < float(cfg.get("supervisorSmallProfitWinUsdt", 0.25) or 0.25))
        avg_win = sum(wins) / max(len(wins), 1) if wins else 0.0
        avg_loss = sum(losses) / max(len(losses), 1) if losses else 0.0
        return {
            "day": day,
            "trades": len(rows),
            "wins": len(wins),
            "losses": len(losses),
            "winRatePct": round((len(wins) / max(len(rows), 1)) * 100.0, 2),
            "pnl": round(sum(pnls), 6),
            "avgPnl": round(sum(pnls) / max(len(rows), 1), 6),
            "avgWin": round(avg_win, 6),
            "avgLoss": round(avg_loss, 6),
            "smallWins": small_wins,
            "smallWinRatePct": round((small_wins / max(len(rows), 1)) * 100.0, 2),
        }

    today = summarize(today_key, grouped.get(today_key, []))
    previous_days = [
        summarize(day, rows)
        for day, rows in grouped.items()
        if day != today_key and len(rows) >= min_trades
    ]
    profitable_days = [d for d in previous_days if float(d.get("pnl", 0.0) or 0.0) > 0.0]
    profitable_days.sort(key=lambda d: (float(d.get("pnl", 0.0) or 0.0), float(d.get("winRatePct", 0.0) or 0.0)), reverse=True)
    baseline = profitable_days[0] if profitable_days else {}
    if not baseline:
        return {"today": today, "baseline": {}, "status": "insufficient_profitable_baseline"}

    wr_drop = float(baseline.get("winRatePct", 0.0) or 0.0) - float(today.get("winRatePct", 0.0) or 0.0)
    avg_drop = float(baseline.get("avgPnl", 0.0) or 0.0) - float(today.get("avgPnl", 0.0) or 0.0)
    avg_win_drop = float(baseline.get("avgWin", 0.0) or 0.0) - float(today.get("avgWin", 0.0) or 0.0)
    today_trades = int(today.get("trades", 0) or 0)
    degraded = (
        today_trades >= min_trades
        and float(today.get("pnl", 0.0) or 0.0) < 0.0
        and (wr_drop >= float(cfg.get("supervisorDailyWrDropAlertPct", 18.0) or 18.0) or avg_drop >= 0.10)
    )
    giveback_like = (
        today_trades >= min_trades
        and int(today.get("smallWins", 0) or 0) >= max(3, math.ceil(today_trades * 0.25))
        and avg_win_drop >= 0.10
    )
    result = {
        "today": today,
        "baseline": baseline,
        "status": "degrading" if degraded else "stable",
        "degraded": degraded,
        "givebackLike": giveback_like,
        "winRateDropPct": round(wr_drop, 2),
        "avgPnlDrop": round(avg_drop, 6),
        "avgWinDrop": round(avg_win_drop, 6),
        "minTrades": min_trades,
    }
    cache[cache_key] = result
    return result


def _maybe_tune_daily_entry_regression(daily_review: dict, cfg: dict | None = None) -> dict:
    if not isinstance(daily_review, dict) or not isinstance(cfg, dict) or not bool(daily_review.get("degraded")):
        return {}
    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("daily_entry_regression", cfg, 30)

    if _tuning_should_rollback("daily_entry_regression"):
        rollback = _tuning_rollback_last("daily_entry_regression")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state, delegations, "daily_entry_regression", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    today = daily_review.get("today") if isinstance(daily_review.get("today"), dict) else {}
    baseline = daily_review.get("baseline") if isinstance(daily_review.get("baseline"), dict) else {}

    # Adaptive severity based on winRatePct drop and avgPnl drop
    wr_drop = float(baseline.get("winRatePct", 0.0) or 0.0) - float(today.get("winRatePct", 0.0) or 0.0)
    avg_drop = float(baseline.get("avgPnl", 0.0) or 0.0) - float(today.get("avgPnl", 0.0) or 0.0)
    severity = max(0.0, min(1.0, max(wr_drop / 30.0, avg_drop / 0.20)))

    signature = _tuning_signature("daily_entry_regression", day=today.get("day"), trades=today.get("trades"), winRatePct=today.get("winRatePct"), pnl=today.get("pnl"), baseline_day=baseline.get("day"), baseline_pnl=baseline.get("pnl"))
    rec = delegations.get("daily_entry_regression") if isinstance(delegations.get("daily_entry_regression"), dict) else {}

    changes: dict[str, dict] = {}

    def set_float(key: str, value: float, digits: int = 3):
        old = float(cfg.get(key, value) or value)
        new = round(float(value), digits)
        if abs(old - new) >= 1e-9:
            cfg[key] = new
            changes[key] = {"old": round(old, digits), "new": new}

    def set_int(key: str, value: int):
        old = int(cfg.get(key, value) or value)
        new = int(value)
        if old != new:
            cfg[key] = new
            changes[key] = {"old": old, "new": new}

    min_conf = float(cfg.get("minConfidence", 0.66) or 0.66)
    # Profitable baseline days were not purely high-confidence trades; avoid
    # choking AUTO scan by pushing the global gate toward 0.90.
    set_float("minConfidence", min(0.84, max(min_conf, min_conf + 0.015 * (1.0 + severity * 2.0))), 3)
    early_conf = float(cfg.get("earlyEntryMinConfidence", 0.60) or 0.60)
    set_float("earlyEntryMinConfidence", min(0.76, max(early_conf, early_conf + 0.015 * (1.0 + severity * 2.0))), 3)
    gap = float(cfg.get("earlyEntryScoreGapMin", 1.40) or 1.40)
    set_float("earlyEntryScoreGapMin", min(2.10, max(gap, gap + 0.12 * (1.0 + severity * 2.0))), 3)
    set_float("earlyEntryMaxBbPctB", min(float(cfg.get("earlyEntryMaxBbPctB", 0.82) or 0.82), 0.78), 3)
    set_float("earlyEntryMinBbPctBShort", max(float(cfg.get("earlyEntryMinBbPctBShort", 0.18) or 0.18), 0.22), 3)
    set_float("earlyEntryMaxVwapDistancePct", min(float(cfg.get("earlyEntryMaxVwapDistancePct", 0.24) or 0.24), 0.18), 3)
    set_float("riskCooldownResumeScoreGapMin", min(2.60, max(float(cfg.get("riskCooldownResumeScoreGapMin", 2.0) or 2.0), 2.20 + 0.40 * severity)), 3)
    max_spread = float(cfg.get("maxSpreadBps", 22.0) or 22.0)
    set_float("maxSpreadBps", max(12.0, min(max_spread, max_spread * (0.90 - 0.05 * severity))), 2)
    if not bool(cfg.get("scanFallbackNearEnabled", True)):
        changes["scanFallbackNearEnabled"] = {"old": False, "new": True}
        cfg["scanFallbackNearEnabled"] = True
    near_relax = float(cfg.get("scanFallbackNearConfRelax", 0.04) or 0.04)
    set_float("scanFallbackNearConfRelax", max(0.02, min(near_relax, 0.035 - 0.005 * severity)), 3)
    guarded_relax = float(cfg.get("scanGuardedFallbackConfRelax", 0.04) or 0.04)
    set_float("scanGuardedFallbackConfRelax", max(0.02, min(guarded_relax, 0.04 - 0.005 * severity)), 3)
    if bool(cfg.get("scanPerfSoftFallbackEnabled", True)):
        changes["scanPerfSoftFallbackEnabled"] = {"old": True, "new": False}
        cfg["scanPerfSoftFallbackEnabled"] = False
    if not bool(cfg.get("todayPerformanceGuardEnabled", True)):
        changes["todayPerformanceGuardEnabled"] = {"old": False, "new": True}
        cfg["todayPerformanceGuardEnabled"] = True
    set_int("todayPerformanceGuardMinTrades", min(6, int(cfg.get("todayPerformanceGuardMinTrades", 8) or 8)))
    set_float("todayPerformanceGuardMaxWinRatePct", min(48.0, max(float(cfg.get("todayPerformanceGuardMaxWinRatePct", 40.0) or 40.0), 45.0 + 3.0 * severity)), 2)

    if not changes:
        if active and str(rec.get("signature", "") or "") == signature:
            return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec, "signature": signature}
        return {"applied": False, "reason": "no_safe_delta", "signature": signature}
    out = _commit_supervisor_config_tune(state, delegations, "daily_entry_regression", cfg, changes, "daily_entry_regression")
    delegations = AUTO_TRADE.get("supervisorAutoTune", {}).get("delegations", {})
    if isinstance(delegations.get("daily_entry_regression"), dict):
        delegations["daily_entry_regression"]["signature"] = signature
        delegations["daily_entry_regression"]["today"] = today
        delegations["daily_entry_regression"]["baseline"] = baseline
    out["signature"] = signature
    out["severity"] = severity
    return out


def _maybe_tune_small_profit_capture_from_review(review: dict, cfg: dict | None = None) -> dict:
    if not isinstance(review, dict) or not isinstance(cfg, dict):
        return {}
    try:
        trades_n = int(review.get("trades", 0) or 0)
        small_wins = int(review.get("smallWins", 0) or 0)
    except Exception:
        return {}
    if trades_n < 6 or small_wins < max(3, math.ceil(trades_n * 0.30)):
        return {}
    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("small_profit_capture", cfg, 45)

    if _tuning_should_rollback("small_profit_capture"):
        rollback = _tuning_rollback_last("small_profit_capture")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state, delegations, "small_profit_capture", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    # Adaptive severity based on smallWins/trades_n ratio
    ratio = small_wins / max(trades_n, 1)
    severity = max(0.0, min(1.0, (ratio - 0.30) / 0.40))

    changes: dict[str, dict] = {}

    def set_float(key: str, value: float, digits: int = 3):
        old = float(cfg.get(key, value) or value)
        new = round(float(value), digits)
        if abs(old - new) >= 1e-9:
            cfg[key] = new
            changes[key] = {"old": round(old, digits), "new": new}

    if not bool(cfg.get("holdWinners", True)):
        changes["holdWinners"] = {"old": bool(cfg.get("holdWinners", False)), "new": True}
        cfg["holdWinners"] = True
    hold_min = float(cfg.get("holdMinConfidence", 0.72) or 0.72)
    set_float("holdMinConfidence", max(0.66, min(0.84, hold_min - 0.02 * (1.0 + severity * 2.0))), 3)
    trigger = float(cfg.get("profitLockTriggerUsdt", 0.35) or 0.35)
    keep = float(cfg.get("profitLockKeepUsdt", 0.15) or 0.15)
    giveback = float(cfg.get("profitLockMaxGivebackUsdt", 0.22) or 0.22)
    set_float("profitLockTriggerUsdt", max(0.22, min(trigger, trigger * (0.85 - 0.05 * severity))), 3)
    set_float("profitLockKeepUsdt", min(0.45, max(keep, keep * (1.20 + 0.10 * severity), 0.18)), 3)
    set_float("profitLockMaxGivebackUsdt", max(0.10, min(giveback, giveback * (0.82 - 0.08 * severity), 0.18)), 3)
    floor = float(cfg.get("profitLockBreakevenFloorUsdt", 0.08) or 0.08)
    set_float("profitLockBreakevenFloorUsdt", min(0.18, max(floor, 0.10 + 0.02 * severity)), 3)
    tp_min = max(0.20, float(cfg.get("tpTargetMinUsdt", 0.55) or 0.55))
    set_float("tpTargetMinUsdt", min(1.20, max(0.45, tp_min)), 3)
    tp_max = max(tp_min + 0.10, float(cfg.get("tpTargetMaxUsdt", 2.2) or 2.2))
    set_float("tpTargetMaxUsdt", min(3.20, max(tp_max, float(cfg.get("tpTargetMinUsdt", tp_min)) * 2.0)), 3)
    breakeven_trigger = float(cfg.get("profitLockBreakevenTriggerUsdt", 0.16) or 0.16)
    set_float("profitLockBreakevenTriggerUsdt", min(float(cfg.get("profitLockTriggerUsdt", trigger) or trigger), max(0.14, breakeven_trigger * (0.95 - 0.05 * severity))), 3)

    if not changes:
        if active:
            return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec}
        return {"applied": False, "reason": "no_safe_delta"}
    signature = _tuning_signature("small_profit_capture", trades=trades_n, small_wins=small_wins, ratio=round(ratio, 3))
    out = _commit_supervisor_config_tune(state, delegations, "small_profit_capture", cfg, changes, str(review.get("label") or "small wins"))
    out["severity"] = severity
    out["signature"] = signature
    return out


def _maybe_tune_negative_expectancy_from_review(review: dict, cfg: dict | None = None) -> dict:
    if not isinstance(review, dict) or not isinstance(cfg, dict):
        return {}
    try:
        trades_n = int(review.get("trades", 0) or 0)
        win_rate = float(review.get("winRatePct", 0.0) or 0.0)
        avg_pnl = float(review.get("avgPnl", 0.0) or 0.0)
        total_pnl = float(review.get("pnl", 0.0) or 0.0)
        quick_losses = int(review.get("quickLosses", 0) or 0)
    except Exception:
        return {}
    if trades_n < 6 or total_pnl >= 0.0 or (win_rate >= 45.0 and avg_pnl >= -0.04):
        return {}

    label = str(review.get("label", "") or "recent_trades")
    # Adaptive severity based on winRatePct and avgPnl
    severity = max(0.0, min(1.0, max((45.0 - win_rate) / 45.0, (-0.04 - avg_pnl) / 0.20)))
    signature = _tuning_signature("negative_expectancy", label=label, trades=trades_n, win_rate=win_rate, avg_pnl=avg_pnl, total_pnl=total_pnl)
    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("negative_expectancy", cfg, 45)
    rec = delegations.get("negative_expectancy") if isinstance(delegations.get("negative_expectancy"), dict) else {}
    if active and str(rec.get("signature", "") or "") == signature:
        return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec, "signature": signature}

    if _tuning_should_rollback("negative_expectancy"):
        rollback = _tuning_rollback_last("negative_expectancy")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state, delegations, "negative_expectancy", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    changes: dict[str, dict] = {}

    def set_float(key: str, value: float, digits: int = 3):
        old = float(cfg.get(key, value) or value)
        new = round(float(value), digits)
        if abs(old - new) >= 1e-9:
            cfg[key] = new
            changes[key] = {"old": round(old, digits), "new": new}

    def set_int(key: str, value: int):
        old = int(cfg.get(key, value) or value)
        new = int(value)
        if old != new:
            cfg[key] = new
            changes[key] = {"old": old, "new": new}

    min_conf = float(cfg.get("minConfidence", 0.62) or 0.62)
    conf_step = 0.03 if avg_pnl < -0.10 or win_rate < 40.0 else 0.02
    set_float("minConfidence", min(0.84, max(min_conf, min_conf + min(conf_step, 0.015) * (1.0 + severity * 2.0))), 3)

    early_conf = float(cfg.get("earlyEntryMinConfidence", 0.60) or 0.60)
    set_float("earlyEntryMinConfidence", min(0.76, max(early_conf, early_conf + 0.015 * (1.0 + severity * 2.0))), 3)
    gap = float(cfg.get("earlyEntryScoreGapMin", 1.40) or 1.40)
    gap_step = 0.12 if quick_losses >= 2 else 0.08
    set_float("earlyEntryScoreGapMin", min(2.10, max(gap, gap + gap_step * (1.0 + severity * 2.0))), 3)
    if not bool(cfg.get("earlyEntryPullbackResetEnabled", True)):
        changes["earlyEntryPullbackResetEnabled"] = {"old": False, "new": True}
        cfg["earlyEntryPullbackResetEnabled"] = True
    early_bb = float(cfg.get("earlyEntryMaxBbPctB", 0.82) or 0.82)
    set_float("earlyEntryMaxBbPctB", max(0.74, min(early_bb, 0.82)), 3)
    early_vwap = float(cfg.get("earlyEntryMaxVwapDistancePct", 0.24) or 0.24)
    set_float("earlyEntryMaxVwapDistancePct", max(0.12, min(early_vwap, 0.24)), 3)
    hybrid_score = float(cfg.get("hybridMinScore", 0.72) or 0.72)
    set_float("hybridMinScore", min(0.86, max(hybrid_score, hybrid_score + 0.02 * (1.0 + severity * 2.0))), 3)
    hybrid_edge = float(cfg.get("hybridMinEdge", 0.06) or 0.06)
    set_float("hybridMinEdge", min(0.12, max(hybrid_edge, hybrid_edge + 0.01 * (1.0 + severity * 2.0))), 3)

    max_spread = float(cfg.get("maxSpreadBps", 22.0) or 22.0)
    set_float("maxSpreadBps", max(12.0, min(max_spread, max_spread * (0.92 - 0.05 * severity))), 2)
    if str(cfg.get("scanSidePreference", "score") or "score") != "score":
        changes["scanSidePreference"] = {"old": cfg.get("scanSidePreference"), "new": "score"}
        cfg["scanSidePreference"] = "score"
    if not bool(cfg.get("scanFallbackNearEnabled", True)):
        changes["scanFallbackNearEnabled"] = {"old": False, "new": True}
        cfg["scanFallbackNearEnabled"] = True
    near_relax = float(cfg.get("scanFallbackNearConfRelax", 0.04) or 0.04)
    set_float("scanFallbackNearConfRelax", max(0.02, min(near_relax, 0.035 - 0.005 * severity)), 3)
    guarded_relax = float(cfg.get("scanGuardedFallbackConfRelax", 0.04) or 0.04)
    set_float("scanGuardedFallbackConfRelax", max(0.02, min(guarded_relax, 0.04 - 0.005 * severity)), 3)
    if bool(cfg.get("scanPerfSoftFallbackEnabled", True)):
        changes["scanPerfSoftFallbackEnabled"] = {"old": True, "new": False}
        cfg["scanPerfSoftFallbackEnabled"] = False

    set_int("perfLockMinutes", max(120, int(cfg.get("perfLockMinutes", 90) or 90)))
    set_int("perfGateEarlyMinSamples", min(4, int(cfg.get("perfGateEarlyMinSamples", 4) or 4)))
    set_float("perfGateEarlyMinPnlUsdt", max(float(cfg.get("perfGateEarlyMinPnlUsdt", -0.35) or -0.35), -0.35), 3)
    if not bool(cfg.get("sessionBiasEnabled", True)):
        changes["sessionBiasEnabled"] = {"old": False, "new": True}
        cfg["sessionBiasEnabled"] = True
    if not bool(cfg.get("todayPerformanceGuardEnabled", True)):
        changes["todayPerformanceGuardEnabled"] = {"old": False, "new": True}
        cfg["todayPerformanceGuardEnabled"] = True
    set_int("todayPerformanceGuardMinTrades", min(8, int(cfg.get("todayPerformanceGuardMinTrades", 8) or 8)))
    guard_wr = float(cfg.get("todayPerformanceGuardMaxWinRatePct", 40.0) or 40.0)
    set_float("todayPerformanceGuardMaxWinRatePct", min(45.0, max(guard_wr, 40.0 + 3.0 * severity)), 2)
    set_int("sessionBiasMinSamples", min(8, int(cfg.get("sessionBiasMinSamples", 10) or 10)))
    bad_wr = float(cfg.get("sessionBiasBadWinRatePct", 42.0) or 42.0)
    set_float("sessionBiasBadWinRatePct", min(48.0, max(bad_wr, 45.0 + 2.0 * severity)), 2)
    max_shift = float(cfg.get("sessionBiasMaxConfShift", 0.05) or 0.05)
    set_float("sessionBiasMaxConfShift", min(0.06, max(max_shift, 0.05 + 0.01 * severity)), 3)

    if not changes:
        return {"applied": False, "reason": "no_safe_delta", "signature": signature}
    out = _commit_supervisor_config_tune(state, delegations, "negative_expectancy", cfg, changes, "negative_expectancy")
    delegations = AUTO_TRADE.get("supervisorAutoTune", {}).get("delegations", {})
    if isinstance(delegations.get("negative_expectancy"), dict):
        delegations["negative_expectancy"]["signature"] = signature
        delegations["negative_expectancy"]["winRatePct"] = round(win_rate, 2)
        delegations["negative_expectancy"]["avgPnl"] = round(avg_pnl, 6)
        delegations["negative_expectancy"]["pnl"] = round(total_pnl, 6)
    out["signature"] = signature
    out["severity"] = severity
    return out


def _recent_live_result_streak_state(trades: list[dict], limit: int = 12) -> dict:
    rows = [t for t in (trades or []) if isinstance(t, dict)]
    if not rows:
        return {"kind": "", "streak": 0, "signature": "", "lastClosedAt": 0, "pnl": 0.0}
    rows = sorted(rows, key=lambda t: int(t.get("_ts", t.get("closedAt", t.get("ts", 0))) or 0))
    streak_rows: list[dict] = []
    kind = ""
    for trade in reversed(rows[-max(3, int(limit)) :]):
        try:
            pnl = float(trade.get("_pnl", trade.get("pnl", 0.0)) or 0.0)
        except Exception:
            continue
        trade_kind = "win" if pnl >= 0.0 else "loss"
        if not kind:
            kind = trade_kind
        if trade_kind != kind:
            break
        item = dict(trade)
        item["_pnl"] = pnl
        streak_rows.append(item)
    if not streak_rows:
        return {"kind": "", "streak": 0, "signature": "", "lastClosedAt": 0, "pnl": 0.0}
    parts = []
    for item in reversed(streak_rows):
        sym = str(item.get("symbol", "") or "").upper()
        side = str(item.get("side", "") or "").upper()
        ts = int(item.get("_ts", item.get("closedAt", item.get("ts", 0))) or 0)
        pnl = float(item.get("_pnl", 0.0) or 0.0)
        parts.append(f"{sym}:{side}:{ts}:{pnl:.8f}")
    return {
        "kind": kind,
        "streak": len(streak_rows),
        "signature": "|".join(parts),
        "lastClosedAt": int(streak_rows[0].get("_ts", streak_rows[0].get("closedAt", streak_rows[0].get("ts", 0))) or 0),
        "pnl": round(sum(float(t.get("_pnl", 0.0) or 0.0) for t in streak_rows), 6),
    }


def _maybe_tune_size_multiplier_from_streak(trades: list[dict], cfg: dict | None = None) -> dict:
    if not isinstance(cfg, dict) or not bool(cfg.get("supervisorSizeStreakEnabled", True)):
        return {}
    state = _recent_live_result_streak_state(trades, int(cfg.get("supervisorSizeLookbackTrades", 12) or 12))
    kind = str(state.get("kind", "") or "")
    streak = int(state.get("streak", 0) or 0)
    win_min = max(2, int(cfg.get("supervisorSizeWinStreakMin", 3) or 3))
    loss_min = max(1, int(cfg.get("supervisorSizeLossStreakMin", 2) or 2))
    old_mult = float(cfg.get("supervisorSizeMultiplier", 1.0) or 1.0)
    min_mult = max(0.1, min(1.0, float(cfg.get("supervisorSizeMinMultiplier", 0.50) or 0.50)))
    if bool(cfg.get("marketScan")) or str(cfg.get("symbol", "")).upper() in {"AUTO", "SCAN"}:
        diversified_floor = max(0.1, min(1.0, float(cfg.get("supervisorSizeDiversifiedMinMultiplier", 0.65) or 0.65)))
        min_mult = max(min_mult, diversified_floor)
    max_mult = max(1.0, min(3.0, float(cfg.get("supervisorSizeMaxMultiplier", 1.35) or 1.35)))
    if max_mult < min_mult:
        max_mult = min_mult

    target = 1.0
    reason = "streak_reset"
    if kind == "win" and streak >= win_min:
        step = max(0.0, float(cfg.get("supervisorSizeWinStepPct", 10.0) or 10.0)) / 100.0
        severity = max(0.0, min(1.0, (streak - win_min) / 10.0))
        scaled_step = step * (1.0 + severity * 2.0)
        target = min(max_mult, 1.0 + scaled_step * (streak - win_min + 1))
        reason = "win_streak"
    elif kind == "loss" and streak >= loss_min:
        step = max(0.0, float(cfg.get("supervisorSizeLossStepPct", 15.0) or 15.0)) / 100.0
        severity = max(0.0, min(1.0, (streak - loss_min) / 10.0))
        scaled_step = step * (1.0 + severity * 2.0)
        target = max(min_mult, 1.0 - scaled_step * (streak - loss_min + 1))
        reason = "loss_streak"
    elif abs(old_mult - 1.0) >= 0.001:
        target = 1.0
        severity = 0.0
    else:
        return {"applied": False, "reason": "no_streak", "streak": state}

    target = round(float(target), 3)
    if abs(old_mult - target) < 0.001:
        return {"applied": False, "reason": "no_safe_delta", "streak": state}

    signature = _tuning_signature("size_streak", reason=reason, streak=streak, target=target, kind=kind)
    state_obj, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("size_streak", cfg, 10)
    rec = delegations.get("size_streak") if isinstance(delegations.get("size_streak"), dict) else {}
    if active and str(rec.get("signature", "") or "") == signature:
        return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec, "signature": signature, "streak": state}

    if _tuning_should_rollback("size_streak"):
        rollback = _tuning_rollback_last("size_streak")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state_obj, delegations, "size_streak", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    cfg["supervisorSizeMultiplier"] = target
    changes = {
        "supervisorSizeMultiplier": {
            "old": round(old_mult, 3),
            "new": target,
            "reason": reason,
            "streak": streak,
            "pnl": state.get("pnl"),
        }
    }
    out = _commit_supervisor_config_tune(state_obj, delegations, "size_streak", cfg, changes, reason)
    delegations = AUTO_TRADE.get("supervisorAutoTune", {}).get("delegations", {})
    if isinstance(delegations.get("size_streak"), dict):
        delegations["size_streak"]["signature"] = signature
        delegations["size_streak"]["kind"] = kind
        delegations["size_streak"]["streak"] = streak
        delegations["size_streak"]["pnl"] = state.get("pnl")
    out["signature"] = signature
    out["streak"] = state
    out["severity"] = severity
    return out


def _per_symbol_streak_size_mult(symbol: str, cfg: dict | None = None) -> float:
    """Per-symbol streak-based size multiplier.

    Mirrors the streak logic in ``_maybe_tune_size_multiplier_from_streak`` but
    is computed purely from the symbol's own recent trades (via PerSymbolContext)
    and never writes to the global config. Returns 1.0 when there is no symbol
    data or streak, so the global ``supervisorSizeMultiplier`` remains the
    fallback in the entry flow.
    """
    cfg = cfg if isinstance(cfg, dict) else {}
    if not bool(cfg.get("supervisorSizeStreakEnabled", True)):
        return 1.0
    sym = str(symbol or "").upper().strip()
    if not sym:
        return 1.0
    try:
        from trading.per_symbol_context import PerSymbolContext
        from trading.shared_cache_layer import get_shared_cache
        cache = get_shared_cache(VAULT_DIR)
        ctx = PerSymbolContext(sym, cache, cfg)
        trades = ctx.get_trades("LIVE")
        if not trades:
            return 1.0
        state = _recent_live_result_streak_state(trades, int(cfg.get("supervisorSizeLookbackTrades", 12) or 12))
        kind = str(state.get("kind", "") or "")
        streak = int(state.get("streak", 0) or 0)
        win_min = max(2, int(cfg.get("supervisorSizeWinStreakMin", 3) or 3))
        loss_min = max(1, int(cfg.get("supervisorSizeLossStreakMin", 2) or 2))
        min_mult = max(0.1, min(1.0, float(cfg.get("supervisorSizeMinMultiplier", 0.50) or 0.50)))
        if bool(cfg.get("marketScan")) or str(cfg.get("symbol", "")).upper() in {"AUTO", "SCAN"}:
            diversified_floor = max(0.1, min(1.0, float(cfg.get("supervisorSizeDiversifiedMinMultiplier", 0.65) or 0.65)))
            min_mult = max(min_mult, diversified_floor)
        max_mult = max(1.0, min(3.0, float(cfg.get("supervisorSizeMaxMultiplier", 1.35) or 1.35)))
        if max_mult < min_mult:
            max_mult = min_mult
        target = 1.0
        if kind == "win" and streak >= win_min:
            step = max(0.0, float(cfg.get("supervisorSizeWinStepPct", 10.0) or 10.0)) / 100.0
            severity = max(0.0, min(1.0, (streak - win_min) / 10.0))
            scaled_step = step * (1.0 + severity * 2.0)
            target = min(max_mult, 1.0 + scaled_step * (streak - win_min + 1))
        elif kind == "loss" and streak >= loss_min:
            step = max(0.0, float(cfg.get("supervisorSizeLossStepPct", 15.0) or 15.0)) / 100.0
            severity = max(0.0, min(1.0, (streak - loss_min) / 10.0))
            scaled_step = step * (1.0 + severity * 2.0)
            target = max(min_mult, 1.0 - scaled_step * (streak - loss_min + 1))
        return round(float(target), 3)
    except Exception:
        return 1.0




def _recent_payoff_loss_guard(cfg: dict | None, symbol: str | None = None) -> dict:
    if not isinstance(cfg, dict) or not bool(cfg.get("payoffLossGuardEnabled", True)):
        return {"active": False, "reason": "disabled"}
    window = max(4, int(cfg.get("payoffLossGuardWindowTrades", 8) or 8))
    min_trades = max(4, int(cfg.get("payoffLossGuardMinTrades", 6) or 6))
    max_payoff = max(0.10, float(cfg.get("payoffLossGuardMaxPayoffRatio", 0.75) or 0.75))
    loss_to_win_cap = max(0.60, float(cfg.get("payoffLossGuardLossToWinCap", 1.05) or 1.05))
    min_loss = max(0.05, float(cfg.get("payoffLossGuardMinLossUsdt", 0.22) or 0.22))
    max_loss = max(min_loss, float(cfg.get("payoffLossGuardMaxLossUsdt", 0.9) or 0.9))

    def summarize(rows: list[dict], label: str) -> dict:
        recent = rows[-window:]
        pnls = []
        for trade in recent:
            try:
                pnl = float(trade.get("_pnl", trade.get("pnl", 0.0)) or 0.0)
            except Exception:
                continue
            if math.isfinite(pnl):
                pnls.append(pnl)
        wins = [p for p in pnls if p > 0.0]
        losses = [p for p in pnls if p < 0.0]
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        payoff = avg_win / abs(avg_loss) if avg_win > 0.0 and avg_loss < 0.0 else 0.0
        return {
            "label": label,
            "trades": len(pnls),
            "wins": len(wins),
            "losses": len(losses),
            "avgWin": avg_win,
            "avgLoss": avg_loss,
            "payoffRatio": payoff,
            "pnl": sum(pnls),
        }

    symbol_norm = str(symbol or "").upper().strip()
    symbol_rows = _live_closed_trades_from_log(symbol_norm, "ALL") if symbol_norm else []
    stats = summarize(symbol_rows, f"{symbol_norm}:last_{window}") if len(symbol_rows) >= min_trades else {}
    if not stats or int(stats.get("trades", 0) or 0) < min_trades:
        stats = summarize(_live_closed_trades_from_log(None, "ALL"), f"all:last_{window}")
    trades_n = int(stats.get("trades", 0) or 0)
    losses_n = int(stats.get("losses", 0) or 0)
    avg_win = float(stats.get("avgWin", 0.0) or 0.0)
    avg_loss = float(stats.get("avgLoss", 0.0) or 0.0)
    payoff = float(stats.get("payoffRatio", 0.0) or 0.0)
    if trades_n < min_trades or losses_n < 2 or avg_win <= 0.0 or avg_loss >= 0.0:
        return {"active": False, "reason": "insufficient_window", **stats}
    if payoff <= 0.0 or payoff >= max_payoff:
        return {"active": False, "reason": "payoff_ok", **stats}
    loss_cap = max(min_loss, min(max_loss, avg_win * loss_to_win_cap))
    return {
        "active": True,
        "reason": "weak_payoff_loss_cap",
        "maxLossUsdt": round(float(loss_cap), 6),
        "payoffRatio": round(payoff, 4),
        "avgWin": round(avg_win, 6),
        "avgLoss": round(avg_loss, 6),
        "trades": trades_n,
        "label": stats.get("label", f"last_{window}"),
    }


def _memory_windows_from_trades(trades: list[dict], *, now_ts: int | None = None) -> dict[str, dict]:
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

    def build(rows: list[dict], label: str) -> dict:
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

    out = {
        "7d": build([t for t in cleaned if now - int(t.get("_ts", 0) or 0) <= 7 * 86400], "7d"),
        "15d": build([t for t in cleaned if now - int(t.get("_ts", 0) or 0) <= 15 * 86400], "15d"),
        "30d": build([t for t in cleaned if now - int(t.get("_ts", 0) or 0) <= 30 * 86400], "30d"),
        "archive": build([t for t in cleaned if now - int(t.get("_ts", 0) or 0) > 30 * 86400], "archive"),
        "all": build(cleaned, "all"),
    }
    return out


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


def _entry_session_hours_from_log(max_trades: int = 700) -> dict[int, dict]:
    now = time.time()
    try:
        mtime = TRADES_LOG_PATH.stat().st_mtime if TRADES_LOG_PATH.exists() else -1.0
    except Exception:
        mtime = -1.0
    cached_hours = _SESSION_BIAS_CACHE.get("hours")
    if (
        isinstance(cached_hours, dict)
        and float(_SESSION_BIAS_CACHE.get("builtAt", 0.0) or 0.0) > 0
        and int(_SESSION_BIAS_CACHE.get("liveVersion", -1) or -1) == int(_LIVE_STATS_VERSION)
        and float(_SESSION_BIAS_CACHE.get("mtime", -2.0) or -2.0) == mtime
        and now - float(_SESSION_BIAS_CACHE.get("builtAt", 0.0) or 0.0) < 300.0
    ):
        return cached_hours
    if not TRADES_LOG_PATH.exists():
        _SESSION_BIAS_CACHE.update({"builtAt": now, "liveVersion": _LIVE_STATS_VERSION, "mtime": mtime, "hours": {}})
        return {}
    try:
        lines = TRADES_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        _SESSION_BIAS_CACHE.update({"builtAt": now, "liveVersion": _LIVE_STATS_VERSION, "mtime": mtime, "hours": {}})
        return {}
    lookback_days = 30
    cutoff_ts = int(now) - (lookback_days * 86400)
    picked: dict[tuple[str, str], list[int]] = {}
    closed: list[dict] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        mode = str(obj.get("mode", "")).upper()
        if mode == "SCAN" and bool(obj.get("picked")):
            sig = str(obj.get("signal", "")).upper()
            sym = str(obj.get("symbol", "")).upper().strip()
            if sig in ("LONG", "SHORT") and sym:
                try:
                    ts = int(float(obj.get("ts", 0) or 0))
                except Exception:
                    ts = 0
                if ts >= cutoff_ts:
                    picked.setdefault((sym, sig), []).append(ts)
        elif mode == "LIVE" and "pnl" in obj:
            try:
                pnl = float(obj.get("pnl", 0.0) or 0.0)
                entry = float(obj.get("entry", 0.0) or 0.0)
                exit_px = float(obj.get("exit", 0.0) or 0.0)
                ts = int(float(obj.get("closedAt", obj.get("ts", 0)) or 0))
            except Exception:
                continue
            if ts < cutoff_ts:
                continue
            if ts <= 0 or not math.isfinite(pnl) or abs(pnl) > 5000.0:
                continue
            closed.append({
                "symbol": str(obj.get("symbol", "")).upper().strip(),
                "side": str(obj.get("side", "")).upper(),
                "pnl": pnl,
                "entry": entry,
                "exit": exit_px,
                "ts": ts,
            })
    closed = closed[-max(50, int(max_trades or 700)) :]
    buckets: dict[int, list[dict]] = {}
    for item in closed:
        entry_ts = int(item["ts"])
        scans = picked.get((item["symbol"], item["side"]), [])
        if scans:
            recent = [ts for ts in scans if 0 <= entry_ts - ts <= 10800]
            if recent:
                entry_ts = max(recent)
        hour = time.localtime(entry_ts).tm_hour
        entry = float(item.get("entry", 0.0) or 0.0)
        exit_px = float(item.get("exit", 0.0) or 0.0)
        move = abs((exit_px - entry) / entry * 100.0) if entry > 0 and exit_px > 0 else None
        buckets.setdefault(hour, []).append({"pnl": float(item["pnl"]), "move": move})
    hours: dict[int, dict] = {}
    for hour, items in buckets.items():
        pnls = [float(x["pnl"]) for x in items]
        moves = [float(x["move"]) for x in items if x.get("move") is not None and math.isfinite(float(x["move"]))]
        wins = sum(1 for p in pnls if p >= 0.0)
        hours[int(hour)] = {
            "hour": int(hour),
            "trades": len(items),
            "winRatePct": round((wins / max(len(items), 1)) * 100.0, 2),
            "pnl": round(sum(pnls), 6),
            "avgPnl": round(sum(pnls) / max(len(pnls), 1), 6),
            "avgAbsMovePct": round(sum(moves) / max(len(moves), 1), 4) if moves else 0.0,
        }
    _SESSION_BIAS_CACHE.update({"builtAt": now, "liveVersion": _LIVE_STATS_VERSION, "mtime": mtime, "hours": hours})
    return hours


def _entry_session_bias(cfg: dict, now_ts: int | None = None) -> dict:
    hour = time.localtime(int(now_ts or time.time())).tm_hour
    neutral = {
        "enabled": bool(cfg.get("sessionBiasEnabled", True)),
        "hour": hour,
        "label": f"{hour:02d}:00-{(hour + 1) % 24:02d}:00",
        "trades": 0,
        "winRatePct": 0.0,
        "pnl": 0.0,
        "avgAbsMovePct": 0.0,
        "confidenceShift": 0.0,
        "scoreShift": 0.0,
        "sizeMult": 1.0,
        "reason": "disabled" if not bool(cfg.get("sessionBiasEnabled", True)) else "insufficient_session_data",
    }
    if not neutral["enabled"]:
        return neutral
    hours = _entry_session_hours_from_log(int(cfg.get("sessionBiasLookbackTrades", 700) or 700))
    st = hours.get(hour)
    if not isinstance(st, dict):
        return neutral
    min_samples = int(cfg.get("sessionBiasMinSamples", 10) or 10)
    trades = int(st.get("trades", 0) or 0)
    neutral.update(st)
    if trades < min_samples:
        neutral["reason"] = "insufficient_session_data"
        return neutral
    wr = float(st.get("winRatePct", 0.0) or 0.0)
    pnl = float(st.get("pnl", 0.0) or 0.0)
    avg_move = float(st.get("avgAbsMovePct", 0.0) or 0.0)
    good_wr = float(cfg.get("sessionBiasGoodWinRatePct", 50.0) or 50.0)
    bad_wr = float(cfg.get("sessionBiasBadWinRatePct", 42.0) or 42.0)
    low_vol = float(cfg.get("sessionBiasLowVolMovePct", 0.45) or 0.45)
    high_vol = float(cfg.get("sessionBiasHighVolMovePct", 0.90) or 0.90)
    max_conf = max(0.0, min(0.20, float(cfg.get("sessionBiasMaxConfShift", 0.05) or 0.05)))
    max_size_pct = max(0.0, min(100.0, float(cfg.get("sessionBiasMaxSizeShiftPct", 25.0) or 25.0)))
    today_guard = _today_entry_performance_guard(cfg)
    if bool(today_guard.get("active")):
        loss_ratio = float(today_guard.get("losses", 0) or 0) / max(float(today_guard.get("trades", 1) or 1), 1.0)
        weakness = max(0.35, min(1.0, loss_ratio + min(0.35, abs(float(today_guard.get("pnl", 0.0) or 0.0)) / 3.0)))
        conf_shift = max_conf * weakness
        size_pct = -max_size_pct * weakness
        neutral.update({
            "todayGuard": today_guard,
            "confidenceShift": round(max(0.0, min(max_conf, conf_shift)), 4),
            "scoreShift": round(max(-0.15, min(0.0, -conf_shift * 0.8)), 4),
            "sizeMult": round(max(0.50, min(1.0, 1.0 + (size_pct / 100.0))), 4),
            "reason": "today_performance_guard",
        })
        return neutral
    conf_shift = 0.0
    size_pct = 0.0
    score_shift = 0.0
    reason = "neutral_session"
    if pnl > 0 and wr >= good_wr:
        strength = min(1.0, ((wr - good_wr) / 20.0) + min(0.5, pnl / max(trades * 0.12, 1e-9)))
        conf_shift = -max_conf * max(0.35, strength)
        size_pct = max_size_pct * max(0.35, strength)
        score_shift = abs(conf_shift) * 0.8
        reason = "boost_good_session"
    elif pnl < 0 and wr <= bad_wr:
        weakness = min(1.0, ((bad_wr - wr) / 20.0) + min(0.5, abs(pnl) / max(trades * 0.12, 1e-9)))
        conf_shift = max_conf * max(0.35, weakness)
        size_pct = -max_size_pct * max(0.35, weakness)
        score_shift = -conf_shift * 0.8
        reason = "reduce_bad_session"
    if avg_move <= low_vol and pnl >= 0:
        conf_shift = max(-max_conf, conf_shift - min(0.01, max_conf * 0.25))
        size_pct = min(max_size_pct, size_pct + min(5.0, max_size_pct * 0.25))
        score_shift += min(0.01, max_conf * 0.25)
        reason = "boost_low_vol_session" if reason == "neutral_session" else reason
    elif avg_move >= high_vol and pnl < 0:
        conf_shift = min(max_conf, conf_shift + min(0.01, max_conf * 0.25))
        size_pct = max(-max_size_pct, size_pct - min(5.0, max_size_pct * 0.25))
        score_shift -= min(0.01, max_conf * 0.25)
        reason = "reduce_high_vol_session" if reason == "neutral_session" else reason
    neutral.update({
        "confidenceShift": round(max(-max_conf, min(max_conf, conf_shift)), 4),
        "scoreShift": round(max(-0.15, min(0.15, score_shift)), 4),
        "sizeMult": round(max(0.50, min(1.75, 1.0 + (size_pct / 100.0))), 4),
        "reason": reason,
    })
    return neutral


def _early_entry_pullback_reset_ok(side: str, precision: dict | None, cfg: dict | None = None) -> tuple[bool, str]:
    cfg = cfg if isinstance(cfg, dict) else {}
    if not bool(cfg.get("earlyEntryPullbackResetEnabled", True)):
        return True, "disabled"
    px = precision if isinstance(precision, dict) else {}
    side = str(side or "").upper()
    try:
        bb = float(px.get("bbPctB", 0.5) or 0.5)
    except Exception:
        bb = 0.5
    try:
        vwap_dist = abs(float(px.get("vwapDistancePct", 0.0) or 0.0))
    except Exception:
        vwap_dist = 0.0
    late_bb = float(cfg.get("lateEntryMaxBbPctB", 0.95) or 0.95)
    late_vwap = float(cfg.get("lateEntryMaxVwapDistancePct", 0.40) or 0.40)
    long_max_bb = min(late_bb - 0.02, float(cfg.get("earlyEntryMaxBbPctB", 0.82) or 0.82))
    short_min_bb = max(1.0 - late_bb + 0.02, float(cfg.get("earlyEntryMinBbPctBShort", 0.18) or 0.18))
    max_vwap = min(late_vwap, float(cfg.get("earlyEntryMaxVwapDistancePct", 0.24) or 0.24))
    today_guard = _today_entry_performance_guard(cfg)
    if bool(today_guard.get("active")):
        long_max_bb = min(long_max_bb, max(0.50, late_bb - 0.08))
        short_min_bb = max(short_min_bb, min(0.50, 1.0 - late_bb + 0.08))
        max_vwap = min(max_vwap, max(0.05, late_vwap * 0.75))
    if side == "LONG":
        if bb > long_max_bb:
            return False, f"wait_pullback_reset_long bb={bb:.2f}>{long_max_bb:.2f}"
        if vwap_dist > max_vwap:
            return False, f"wait_vwap_reset_long vwapDist={vwap_dist:.3f}%>{max_vwap:.3f}%"
    elif side == "SHORT":
        if bb < short_min_bb:
            return False, f"wait_pullback_reset_short bb={bb:.2f}<{short_min_bb:.2f}"
        if vwap_dist > max_vwap:
            return False, f"wait_vwap_reset_short vwapDist={vwap_dist:.3f}%>{max_vwap:.3f}%"
    return True, "ok"


def _learning_propose_from_trades(symbol: str, trades: list[dict]) -> dict:
    if not trades:
        return {
            "symbol": symbol,
            "trades": 0,
            "winRatePct": 0.0,
            "proposed": {
                "minConfidence": 0.66,
                "hybridMinScore": 0.72,
                "hybridMinEdge": 0.06,
                "adaptiveSizeBoostMaxPct": 20.0,
                "maxOpenPositions": 4,
                "takeProfitPct": 1.2,
                "stopLossPct": 0.75,
            },
            "reasons": ["insufficient_live_data"],
        }
    wins = sum(1 for t in trades if float(t.get("_pnl", 0.0)) >= 0.0)
    losses = len(trades) - wins
    wr = (wins / max(len(trades), 1)) * 100.0
    pnl_sum = round(sum(float(t.get("_pnl", 0.0)) for t in trades), 6)
    avg = pnl_sum / max(len(trades), 1)
    # Stable bounded mapping for live-tuning knobs.
    min_conf = 0.68
    if wr >= 60:
        min_conf = 0.62
    elif wr <= 40:
        min_conf = 0.74
    tp = 1.2
    sl = 0.9
    if avg > 0.12:
        tp, sl = 1.4, 0.95
    elif avg < -0.08:
        tp, sl = 1.0, 0.8
    proposed = {
        "minConfidence": round(min_conf, 3),
        "hybridMinScore": round(0.70 + max(-0.05, min(0.06, (wr - 50.0) / 300.0)), 3),
        "hybridMinEdge": round(0.05 + max(-0.015, min(0.02, (50.0 - wr) / 900.0)), 3),
        "adaptiveSizeBoostMaxPct": round(18.0 + max(-6.0, min(12.0, avg * 40.0)), 2),
        "maxOpenPositions": 5 if wr >= 52 else 4,
        "takeProfitPct": round(tp, 3),
        "stopLossPct": round(sl, 3),
    }
    reasons = [
        f"wins={wins} losses={losses} wr={wr:.2f}%",
        f"netPnl={pnl_sum:.4f} avgPnl={avg:.4f}",
    ]
    return {
        "symbol": symbol,
        "trades": len(trades),
        "winRatePct": round(wr, 2),
        "realizedPnl": pnl_sum,
        "proposed": proposed,
        "reasons": reasons,
    }


def _symbol_risk_tune_from_recent_trades(symbol: str, trades: list[dict], cfg: dict | None = None) -> dict:
    sym = str(symbol or "").upper().strip()
    cfg = cfg if isinstance(cfg, dict) else {}
    cleaned: list[dict] = []
    for trade in trades or []:
        if not isinstance(trade, dict):
            continue
        try:
            pnl = float(trade.get("_pnl", trade.get("pnl", 0.0)) or 0.0)
        except Exception:
            continue
        if not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        ts = 0
        try:
            ts = int(float(trade.get("_ts", trade.get("closedAt", trade.get("ts", 0))) or 0))
        except Exception:
            ts = 0
        item = dict(trade)
        item["_pnl"] = pnl
        item["_ts"] = ts
        cleaned.append(item)
    cleaned.sort(key=lambda row: int(row.get("_ts", 0) or 0))
    if len(cleaned) < 4:
        return {
            "active": False,
            "symbol": sym,
            "trades": len(cleaned),
            "reason": "need_4_closed_trades",
            "sizeMult": 1.0,
            "leverageMult": 1.0,
        }

    window = cleaned[-min(8, len(cleaned)):]
    pnls = [float(t.get("_pnl", 0.0) or 0.0) for t in window]
    wins = [p for p in pnls if p >= 0.0]
    losses = [p for p in pnls if p < 0.0]
    trades_n = len(window)
    win_rate = (len(wins) / max(trades_n, 1)) * 100.0
    pnl_sum = sum(pnls)
    avg_pnl = pnl_sum / max(trades_n, 1)
    avg_win = sum(wins) / max(len(wins), 1) if wins else 0.0
    avg_loss = abs(sum(losses) / max(len(losses), 1)) if losses else 0.0
    profit_factor = (sum(wins) / abs(sum(losses))) if losses and abs(sum(losses)) > 0 else (999.0 if wins else 0.0)
    quick_losses = 0
    for trade in window:
        opened = int(float(trade.get("openedAt", 0) or 0))
        closed = int(float(trade.get("_ts", trade.get("closedAt", 0)) or 0))
        if float(trade.get("_pnl", 0.0) or 0.0) < 0.0 and opened > 0 and closed >= opened and (closed - opened) <= 8 * 60:
            quick_losses += 1

    size_mult = 1.0
    leverage_mult = 1.0
    confidence_shift = 0.0
    reason = "neutral_4_8"
    if pnl_sum < 0.0 or avg_pnl < -0.03 or win_rate < 45.0:
        weakness = 0.0
        weakness += min(0.35, abs(min(pnl_sum, 0.0)) / max(1.2, trades_n * 0.22))
        weakness += min(0.25, max(0.0, 50.0 - win_rate) / 60.0)
        weakness += min(0.20, quick_losses / max(trades_n, 1))
        size_mult = max(0.45, 1.0 - (0.65 * max(0.25, weakness)))
        leverage_mult = max(0.55, 1.0 - (0.55 * max(0.25, weakness)))
        confidence_shift = min(0.06, 0.015 + (0.05 * weakness))
        reason = "reduce_after_symbol_drawdown"
    elif pnl_sum > 0.0 and win_rate >= 58.0 and avg_pnl > 0.03 and profit_factor >= 1.25:
        strength = 0.0
        strength += min(0.35, max(0.0, win_rate - 55.0) / 70.0)
        strength += min(0.30, max(0.0, avg_pnl) / 0.35)
        strength += min(0.20, max(0.0, profit_factor - 1.0) / 4.0)
        size_mult = min(1.30, 1.0 + (0.55 * max(0.20, strength)))
        leverage_mult = min(1.18, 1.0 + (0.30 * max(0.20, strength)))
        confidence_shift = -min(0.035, 0.008 + (0.025 * strength))
        reason = "boost_after_symbol_edge"
    elif win_rate >= 55.0 and pnl_sum <= 0.0:
        size_mult = 0.82
        leverage_mult = 0.82
        confidence_shift = 0.025
        reason = "high_winrate_negative_payoff"

    lev_min, lev_max = _autotrade_leverage_bounds(cfg) if cfg else (1, _autotrade_leverage_cap())
    recommended_max = max(lev_min, min(25, int(round(float(lev_max) * float(leverage_mult)))))
    return {
        "active": True,
        "symbol": sym,
        "window": trades_n,
        "trades": len(cleaned),
        "winRatePct": round(win_rate, 2),
        "pnl": round(pnl_sum, 6),
        "avgPnl": round(avg_pnl, 6),
        "avgWin": round(avg_win, 6),
        "avgLoss": round(avg_loss, 6),
        "profitFactor": round(min(profit_factor, 999.0), 4),
        "quickLosses": quick_losses,
        "sizeMult": round(float(size_mult), 4),
        "leverageMult": round(float(leverage_mult), 4),
        "confidenceShift": round(float(confidence_shift), 4),
        "recommendedLeverageMax": int(recommended_max),
        "updatedAt": int(time.time()),
        "reason": reason,
    }


def _walk_forward_from_trades(symbol: str, train_size: int, test_size: int, mode: str) -> dict:
    mode_up = str(mode or "ALL").upper()
    seq = _live_closed_trades_from_log(symbol=symbol, mode=mode_up)
    if not seq:
        return {"ok": False, "symbol": symbol, "mode": mode_up, "detail": "no trades for walk-forward"}
    trn = max(5, int(train_size))
    tst = max(3, int(test_size))
    if len(seq) < (trn + tst):
        return {
            "ok": False,
            "symbol": symbol,
            "mode": mode_up,
            "detail": f"not enough trades ({len(seq)} < {trn+tst})",
            "availableTrades": len(seq),
        }
    train = seq[-(trn + tst):-tst]
    test = seq[-tst:]
    train_wr = (sum(1 for t in train if float(t.get("_pnl", 0.0)) >= 0.0) / max(len(train), 1)) * 100.0
    test_wr = (sum(1 for t in test if float(t.get("_pnl", 0.0)) >= 0.0) / max(len(test), 1)) * 100.0
    train_avg = sum(float(t.get("_pnl", 0.0)) for t in train) / max(len(train), 1)
    test_avg = sum(float(t.get("_pnl", 0.0)) for t in test) / max(len(test), 1)
    rec = {
        "tighten": bool(test_wr < train_wr - 12 or test_avg < -0.04),
        "loosen": bool(test_wr > train_wr + 10 and test_avg > 0.03),
        "trainWinRatePct": round(train_wr, 2),
        "testWinRatePct": round(test_wr, 2),
        "trainAvgPnl": round(train_avg, 6),
        "testAvgPnl": round(test_avg, 6),
    }
    return {
        "ok": True,
        "symbol": symbol,
        "mode": mode_up,
        "trainSize": len(train),
        "testSize": len(test),
        "walkForwardHitRatePct": round(test_wr, 2),
        "recommendation": rec,
    }


def _serialize_per_symbol_update(fn):
    @wraps(fn)
    def wrapped(symbol: str, *args, **kwargs):
        with per_symbol_lock(VAULT_DIR, symbol):
            return fn(symbol, *args, **kwargs)
    return wrapped


@_serialize_per_symbol_update
def _record_learning_trade(symbol: str, trade: dict, mode: str):
    global DAILY_REALIZED_PNL, _DAILY_PNL_DATE_KEY
    sym = str(symbol or "").upper()
    if not sym:
        return
    print(f"[Record Trade] _record_learning_trade called: symbol={sym}, mode={mode}, pnl={trade.get('pnl')}, reason={trade.get('reason')}")
    cfg = AUTO_TRADE.get("config") if isinstance(AUTO_TRADE.get("config"), dict) else {}
    ctx = PerSymbolContext(sym, get_shared_cache(VAULT_DIR), cfg)
    pr = ctx.profile
    pnl = float(trade.get("pnl", 0.0) or 0.0)
    pr["wins"] = int(pr.get("wins", 0)) + (1 if pnl >= 0 else 0)
    pr["losses"] = int(pr.get("losses", 0)) + (1 if pnl < 0 else 0)
    pr["realizedPnl"] = round(float(pr.get("realizedPnl", 0.0)) + pnl, 6)
    pr["trades"] = int(pr.get("trades", 0)) + 1
    pr["lastMode"] = str(mode).upper()
    pr["lastTradeSide"] = str(trade.get("side", ""))
    pr["lastTradeReason"] = str(trade.get("reason", ""))
    pr["sumPnl"] = round(float(pr.get("sumPnl", 0.0)) + pnl, 6)
    pr["avgPnlPerTrade"] = round(float(pr["sumPnl"]) / max(int(pr["trades"]), 1), 6)
    pr["maxWinPnl"] = max(float(pr.get("maxWinPnl", pnl)), pnl)
    pr["maxLossPnl"] = min(float(pr.get("maxLossPnl", pnl)), pnl)
    reward_enabled = bool(cfg.get("learningRewardEnabled", True))
    behavior_enabled = bool(cfg.get("learningBehaviorRewardEnabled", True))
    # Live guardian feedback loop: gently adapt per-symbol TP/SL/lock thresholds.
    if str(mode).upper() == "LIVE":
        ts = int(trade.get("closedAt", trade.get("ts", time.time())) or time.time())
        tloc = time.localtime(ts)
        day_key = (tloc.tm_year, tloc.tm_mon, tloc.tm_mday)
        if day_key != _DAILY_PNL_DATE_KEY:
            DAILY_REALIZED_PNL = 0.0
            _DAILY_PNL_DATE_KEY = day_key
        DAILY_REALIZED_PNL += pnl
        print(f"[Record Trade] Processing LIVE trade for {sym}: pnl={pnl}, reason={trade.get('reason')}, dailyPnl={DAILY_REALIZED_PNL}")
        guard_tp = float(pr.get("tpPct", float(cfg.get("takeProfitPct", 1.8) or 1.8)) or float(cfg.get("takeProfitPct", 1.8) or 1.8))
        guard_sl = float(pr.get("slPct", float(cfg.get("stopLossPct", 0.9) or 0.9)) or float(cfg.get("stopLossPct", 0.9) or 0.9))
        guard_lock = float(pr.get("profitLockTriggerUsdt", float(cfg.get("profitLockTriggerUsdt", 0.35) or 0.35)) or float(cfg.get("profitLockTriggerUsdt", 0.35) or 0.35))
        reason_up = str(trade.get("reason", "") or "").upper()
        win_like = pnl >= 0.0 or reason_up in {"LOCAL_TP_HIT", "TARGET_MAX", "PEAK_CAPTURE", "LIVE_CLOSE"}
        loss_like = pnl < 0.0 or reason_up in {"LOCAL_SL_HIT", "BREAKEVEN_GUARD", "PAYOFF_LOSS_GUARD", "STRONG_REVERSAL_EXIT", "LIVE_CUT_LOSING_SIDE", "RETRACE_BUDGET", "WEAK_SIGNAL"}
        if win_like:
            guard_tp *= 1.012
            guard_sl *= 1.006
            guard_lock *= 1.010
        elif loss_like:
            guard_tp *= 0.988
            guard_sl *= 0.986
            guard_lock *= 0.965
        guard_tp = max(0.35, min(6.0, guard_tp))
        guard_sl = max(0.20, min(3.5, guard_sl))
        guard_lock = max(0.08, min(1.50, guard_lock))
        pr["tpPct"] = round(guard_tp, 4)
        pr["slPct"] = round(guard_sl, 4)
        pr["profitLockTriggerUsdt"] = round(guard_lock, 4)
        # Guardian autotune: adapt holdTrailPct and holdMinConfidence per symbol.
        guard_trail = float(pr.get("holdTrailPct", float(cfg.get("holdTrailPct", 0.25) or 0.25)) or 0.25)
        guard_conf = float(pr.get("holdMinConfidence", float(cfg.get("holdMinConfidence", 0.72) or 0.72)) or 0.72)
        if win_like:
            guard_trail *= 1.01
            guard_conf *= 0.99
        elif loss_like:
            guard_trail *= 0.98
            guard_conf *= 1.01
        pr["holdTrailPct"] = round(max(0.10, min(0.50, guard_trail)), 4)
        pr["holdMinConfidence"] = round(max(0.60, min(0.85, guard_conf)), 4)
        pr["tpslFeedback"] = {
            "updatedAt": int(time.time()),
            "mode": "LIVE",
            "reason": reason_up,
            "pnl": round(float(pnl), 6),
        }


    reward_cap = max(1.0, float(cfg.get("learningRewardCap", 50.0) or 50.0))
    reward_decay = float(cfg.get("learningRewardDecay", 0.985) or 0.985)
    reward_decay = min(1.0, max(0.9, reward_decay))
    reward_win = max(0.0, float(cfg.get("learningRewardWin", 1.0) or 1.0))
    penalty_loss = max(0.0, float(cfg.get("learningPenaltyLoss", 0.8) or 0.8))
    pnl_clip = max(1.0, float(cfg.get("learningPnlClipAbsUsdt", 25.0) or 25.0))
    reason = str(trade.get("reason", "") or "").upper()
    base_delta = 0.0
    if reward_enabled:
        # pnl_scale keeps updates smooth while still rewarding larger clean wins.
        pnl_scale = min(1.0, abs(float(pnl)) / pnl_clip)
        if pnl >= 0:
            base_delta = reward_win * (0.75 + 0.25 * pnl_scale)
        else:
            base_delta = -penalty_loss * (0.75 + 0.25 * pnl_scale)
    behavior_delta = 0.0
    if behavior_enabled:
        reward_components = _trade_reward_components(trade, cfg)
        behavior_delta += float(reward_components.get("total", 0.0) or 0.0)
        cap = max(0.1, float(cfg.get("learningBehaviorDeltaCap", 2.5) or 2.5))
        behavior_delta = max(-cap, min(cap, behavior_delta))
    else:
        reward_components = {}
    prev_score = float(pr.get("rewardScore", 0.0) or 0.0)
    new_score = (prev_score * reward_decay) + base_delta + behavior_delta
    pr["rewardScore"] = round(max(-reward_cap, min(reward_cap, new_score)), 6)
    pr["rewardDelta"] = round(base_delta, 6)
    pr["rewardBehaviorDelta"] = round(behavior_delta, 6)
    pr["rewardComponents"] = reward_components
    if pnl >= 0:
        pr["rewardWinStreak"] = int(pr.get("rewardWinStreak", 0)) + 1
        pr["rewardLossStreak"] = 0
    else:
        pr["rewardLossStreak"] = int(pr.get("rewardLossStreak", 0)) + 1
        pr["rewardWinStreak"] = 0
    pr["updatedAt"] = int(time.time())
    try:
        recent_rows = _live_closed_trades_from_symbol(sym, mode="ALL", vault_dir=VAULT_DIR)
        current_trade = {"_pnl": pnl, "_ts": int(time.time()), **trade, "symbol": sym}
        recent_with_current = [*recent_rows, current_trade]
        windows = _memory_windows_from_trades(recent_with_current)
        pr["memoryWindows"] = windows
        pr["weightedRecentScore"] = _weighted_recent_memory_score(windows)
        pr["symbolRiskTune"] = _symbol_risk_tune_from_recent_trades(sym, recent_with_current, cfg)
    except Exception:
        pass
    # Auto-calibrate the symbol profile from the updated learning state.
    try:
        auto_profile = _auto_update_symbol_profile(sym, cfg)
        if isinstance(auto_profile, dict) and auto_profile:
            try:
                existing = ctx._sym_profile if isinstance(ctx._sym_profile, dict) else {}
                merged = dict(existing)
                _AUTOTUNER_MANAGED = {"tpPct", "slPct", "holdTrailPct", "holdMinConfidence"}
                _has_autotune = bool(existing.get("autotuneLastAt"))
                for k, v in auto_profile.items():
                    if _has_autotune and k in _AUTOTUNER_MANAGED:
                        continue
                    merged[k] = v
                merged["updatedAt"] = int(time.time())
                merged["lastAutoLearnedAt"] = int(time.time())
                merged["lastAutoLearnedFrom"] = "close_trade"
                merged["learnedTrades"] = int(pr.get("trades", 0) or 0)
                merged["learnedWinRatePct"] = float(pr.get("memoryWindows", {}).get("7d", {}).get("winRatePct", 0.0) or 0.0) if isinstance(pr.get("memoryWindows"), dict) else 0.0
                merged["learnedPnl"] = float(pr.get("realizedPnl", 0.0) or 0.0)
                ctx._sym_profile = merged
                ctx._dirty_sym_profile = True
            except Exception:
                pass  # per-symbol write failed, skip fallback to global
    except Exception:
        pass

    # Write to per-symbol trade log and vault
    try:
        trade_log_entry = {"ts": int(time.time()), **trade, "symbol": sym}
        if str(mode).upper() == "LIVE" and "pnl" in trade:
            trade_log_entry["mode"] = "LIVE"
            trade_log_entry.setdefault("closedAt", int(trade.get("closedAt") or trade.get("ts") or time.time()))
            trade_log_entry.setdefault("reason", str(trade.get("reason", "LIVE_CLOSE") or "LIVE_CLOSE"))
        else:
            trade_log_entry["mode"] = str(mode).upper()
        
        # Attach TV data from disk (tv_signal.json) — snapshot at time of close (both LIVE and PAPER)
        try:
            _tv_path = VAULT_DIR / "symbols" / sym / "tv_signal.json"
            if _tv_path.exists():
                _tv = json.loads(_tv_path.read_text(encoding="utf-8"))
                if isinstance(_tv, dict) and _tv:
                    trade_log_entry["tvSignal"] = _tv.get("signal", "")
                    trade_log_entry["tvConfidence"] = _tv.get("confidence", 0.0)
                    trade_log_entry["tvStrength"] = _tv.get("strength", 0.0)
                    trade_log_entry["tvAge"] = int(time.time()) - int(_tv.get("ts", 0) or 0)
                    print(f"[Record Trade] {sym}: TV data from disk - signal={_tv.get('signal')}, conf={_tv.get('confidence')}, age={trade_log_entry['tvAge']}s")
                else:
                    print(f"[Record Trade] {sym}: TV file exists but data is invalid")
            else:
                print(f"[Record Trade] {sym}: TV signal file not found at {_tv_path}")
        except Exception as e:
            print(f"[Record Trade] {sym}: Error reading TV signal file: {e}")
        
        # Attach params_at_entry and guardian_stats from per-symbol storage
        # (lock may already be popped from in-memory dict, so read from disk)
        try:
            from trading.per_symbol_storage import PerSymbolStorage
            _ps = PerSymbolStorage(VAULT_DIR, sym)
            _gl = _ps.load_guardian_lock()
            if isinstance(_gl, dict) and _gl:
                _snap = _gl.get("entrySnapshot", {})
                if isinstance(_snap, dict):
                    if _snap.get("params_at_entry"):
                        trade_log_entry["params_at_entry"] = _snap["params_at_entry"]
                        print(f"[Record Trade] {sym}: Found params_at_entry: {_snap['params_at_entry']}")
                    else:
                        print(f"[Record Trade] {sym}: No params_at_entry in entrySnapshot")
                    if _snap.get("tvSignal"):
                        trade_log_entry["tvAtEntry"] = _snap["tvSignal"]
                        print(f"[Record Trade] {sym}: TV at entry - signal={_snap['tvSignal']}")
                    else:
                        print(f"[Record Trade] {sym}: No tvSignal in entrySnapshot")
                    if _snap.get("tvConfidence") is not None:
                        trade_log_entry["tvAtEntryConfidence"] = _snap["tvConfidence"]
                        print(f"[Record Trade] {sym}: TV at entry confidence={_snap['tvConfidence']}")
                _gs = _gl.get("guardianStats", {})
                if isinstance(_gs, dict) and _gs:
                    trade_log_entry["guardian_stats"] = {
                        "peakProfitUsdt": _gs.get("peakProfitUsdt", 0.0),
                        "holdWinnerActivated": _gs.get("holdWinnerActivated", 0),
                        "tpExtensionCount": _gs.get("tpExtensionCount", 0),
                        "notionalUsdt": _gs.get("notionalUsdt", 0.0),
                        "timeInPositionSec": int(time.time()) - int(_gs.get("openedAt", time.time())),
                    }
                    print(f"[Record Trade] {sym}: Found guardian_stats: {trade_log_entry['guardian_stats']}")
                else:
                    print(f"[Record Trade] {sym}: No guardian_stats in guardian lock")
            else:
                print(f"[Record Trade] {sym}: No guardian lock found")
        except Exception as e:
            print(f"[Record Trade] {sym}: Error reading guardian lock: {e}")
        
        _append_trade_log(trade_log_entry)
        ctx.record_trade(trade_log_entry)
        append_trade_memory(VAULT_DIR, trade_log_entry, mode)
    except Exception:
        pass
    ctx._dirty_profile = True
    ctx.commit()
    ctx.update_symbol_note(trade)
    _mark_trade_learning_agents(sym, trade, mode)
    # Per-symbol autotuner: trigger evaluation after LIVE trades
    # Delay 1s to avoid file I/O race with ctx.commit() (symbol_profile.json)
    if str(mode).upper() == "LIVE":
        def _deferred_autotune():
            import time as _t; _t.sleep(1.0)
            try:
                from trading.symbol_autotuner import record_trade_outcome
                print(f"[Autotune] Triggering record_trade_outcome for {sym}")
                record_trade_outcome(sym, trade)
                print(f"[Autotune] Completed record_trade_outcome for {sym}")
            except Exception as e:
                print(f"[Autotune] Error in record_trade_outcome for {sym}: {e}")
        try:
            import threading as _th
            print(f"[Autotune] Starting deferred autotune thread for {sym}")
            _th.Thread(target=_deferred_autotune, daemon=True).start()
        except Exception as e:
            print(f"[Autotune] Failed to start autotune thread for {sym}: {e}")


async def _record_learning_trade_async(symbol: str, trade: dict, mode: str):
    """Async wrapper for _record_learning_trade — delegates to a thread to avoid blocking the event loop."""
    return await asyncio.to_thread(_record_learning_trade, symbol, trade, mode)


@_serialize_per_symbol_update
def _record_symbol_observation(symbol: str, intel: dict, chosen: bool, score: float):
    sym = str(symbol or "").upper()
    if not sym:
        return
    cfg = AUTO_TRADE.get("config") if isinstance(AUTO_TRADE.get("config"), dict) else {}
    ctx = PerSymbolContext(sym, get_shared_cache(VAULT_DIR), cfg)
    pr = ctx.profile
    ctx.record_observation(intel, chosen, score)
    # If the scanner has enough evidence for this symbol, let it refresh the
    # symbol profile incrementally from scan behavior too (without requiring a
    # closed trade).
    try:
        auto_profile = _auto_update_symbol_profile(sym, cfg=None)
        if isinstance(auto_profile, dict) and auto_profile:
            try:
                existing = ctx._sym_profile if isinstance(ctx._sym_profile, dict) else {}
                merged = dict(existing)
                _AUTOTUNER_MANAGED = {"tpPct", "slPct", "holdTrailPct", "holdMinConfidence"}
                _has_autotune = bool(existing.get("autotuneLastAt"))
                for k, v in auto_profile.items():
                    if _has_autotune and k in _AUTOTUNER_MANAGED:
                        continue
                    merged[k] = v
                merged["updatedAt"] = int(time.time())
                merged["lastAutoLearnedAt"] = int(time.time())
                merged["lastAutoLearnedFrom"] = "scan_observation"
                merged["observations"] = int(pr.get("observations", 0) or 0)
                merged["pickedCount"] = int(pr.get("pickedCount", 0) or 0)
                ctx._sym_profile = merged
                ctx._dirty_sym_profile = True
            except Exception:
                pass  # per-symbol write failed, skip fallback to global
    except Exception:
        pass
    _append_trade_log(
        {
            "ts": int(time.time()),
            "mode": "SCAN",
            "symbol": sym,
            "signal": pr["lastSignal"],
            "confidence": pr["lastConfidence"],
            "score": pr["lastScanScore"],
            "picked": bool(chosen),
        }
    )
    try:
        append_scan_memory(VAULT_DIR, sym, intel or {}, bool(chosen), score)
    except Exception:
        pass
    ctx.commit()
    ctx.update_symbol_note()


def _learned_min_conf(symbol: str, base_min_conf: float):
    pr = _load_single_profile(symbol)
    if not isinstance(pr, dict):
        return base_min_conf
    windows = pr.get("memoryWindows") if isinstance(pr.get("memoryWindows"), dict) else {}
    recent = windows.get("7d") if isinstance(windows.get("7d"), dict) else {}
    weighted = pr.get("weightedRecentScore") if isinstance(pr.get("weightedRecentScore"), dict) else {}
    n = int(recent.get("trades", 0) or 0)
    if n >= 6:
        wr = float(recent.get("winRatePct", 0.0) or 0.0)
    else:
        n = int(pr.get("wins", 0)) + int(pr.get("losses", 0))
        wr = (int(pr.get("wins", 0)) / max(n, 1)) * 100.0 if n > 0 else 0.0
    if n < 6:
        return base_min_conf
    reward_score = float(pr.get("rewardScore", 0.0) or 0.0)
    recent_score = float(weighted.get("score", 0.0) or 0.0)
    # Conservative adaptive rule: good symbol => slightly easier, weak symbol => stricter.
    # Adjustments tightened to prevent adaptiveMinConf from exceeding 0.82.
    if wr >= 60:
        out = max(0.50, base_min_conf - 0.04)
    elif wr <= 45 or reward_score < -0.5 or recent_score < -0.10:
        out = base_min_conf + 0.02
    else:
        out = base_min_conf
    reward_delta = float(pr.get("rewardDelta", 0.0) or 0.0)
    reward_behavior = float(pr.get("rewardBehaviorDelta", 0.0) or 0.0)
    win_streak = int(pr.get("rewardWinStreak", 0) or 0)
    loss_streak = int(pr.get("rewardLossStreak", 0) or 0)
    # Smaller adjustments: each capped at ±0.02 to prevent runaway compounding.
    out -= max(-0.02, min(0.02, reward_score / 400.0))
    out -= max(-0.02, min(0.02, recent_score * 0.03))
    out -= max(-0.01, min(0.01, reward_delta / 50.0))
    out -= max(-0.01, min(0.01, reward_behavior / 100.0))
    if win_streak >= 3:
        out -= min(0.03, 0.01 * (win_streak - 2))
    if loss_streak >= 2:
        out += min(0.03, 0.01 * (loss_streak - 1))
    # Per-symbol exemption: if global tunes raised base_min_conf above default
    # (0.65) but this symbol has strong performance, dampen the global tightening.
    default_base = 0.65
    global_tightening = base_min_conf - default_base
    if global_tightening > 0.03 and wr >= 58 and reward_score > 0:
        exemption = min(global_tightening * 0.6, 0.08)
        out -= exemption
    # Dynamic clamp: respect supervisor tuning while preventing runaway.
    # Upper bound follows supervisor base_min_conf so per-symbol learning
    # doesn't get overridden by global tuning (was hardcoded 0.82).
    lower_limit = max(0.65, base_min_conf - 0.10)
    upper_limit = max(0.82, base_min_conf + 0.04)
    return max(lower_limit, min(upper_limit, out))


def _symbol_quality_score(symbol: str) -> float:
    """Delegate to authoritative implementation in intel_pipeline."""
    from analysis.intel_pipeline import _symbol_quality_score as _iqs
    return _iqs(symbol)


def _intel_score(symbol: str, intel: dict) -> float:
    """Delegate to authoritative implementation in intel_pipeline."""
    from analysis.intel_pipeline import _intel_score as _is
    return _is(symbol, intel)


_SCAN_TICKER_CACHE: dict = {"ts": 0.0, "data": None}
_SCAN_TICKER_CACHE_TTL = 45.0


async def _scan_market_candidates(limit_liquid: int = 30) -> list[str]:
    fallback = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT", "ADAUSDT", "LINKUSDT"]
    limit = max(5, min(int(limit_liquid or 30), 40))
    now = time.time()
    cached = _SCAN_TICKER_CACHE["data"]
    if cached is not None and (now - _SCAN_TICKER_CACHE["ts"]) < _SCAN_TICKER_CACHE_TTL:
        return cached[:limit]
    try:
        res = await asyncio.wait_for(_data_get("/fapi/v1/ticker/24hr"), timeout=12.0)
    except Exception:
        return fallback[: limit]
    if res.status_code >= 400:
        return fallback[: limit]
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
        # rank by liquidity, while de-prioritizing extreme 24h swings
        moderate_move_bonus = 1.0 + min(0.35, chg / 80.0)
        extreme_move_penalty = 1.0 / (1.0 + max(0.0, chg - 8.0) / 8.0)
        rank = qv * moderate_move_bonus * extreme_move_penalty
        out.append((rank, sym))
    out.sort(key=lambda x: x[0], reverse=True)
    ranked = [s for _, s in out[:40]]
    _SCAN_TICKER_CACHE["ts"] = now
    _SCAN_TICKER_CACHE["data"] = ranked
    filtered = _apply_per_symbol_scan_cadence(ranked, now)
    return (filtered if filtered else ranked)[:limit]


def _parse_symbol_whitelist(raw_list: list[str] | None) -> set[str]:
    out: set[str] = set()
    for v in raw_list or []:
        try:
            sym = _normalize_symbol(str(v).strip())
            out.add(sym)
        except Exception:
            continue
    return out


def _open_symbols_from_positions(rows: list[dict] | None) -> set[str]:
    out: set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol", "") or "").upper().strip()
        try:
            qty = abs(float(row.get("qty", row.get("positionAmt", 0.0)) or 0.0))
        except Exception:
            qty = 0.0
        if sym and qty > 0:
            out.add(sym)
    return out


def _scan_timeout_budget_sec(cfg: dict) -> float:
    interval = float(cfg.get("intervalSec", 20) or 20)
    per_symbol_timeout = float(cfg.get("scanPerSymbolTimeoutSec", 7.5) or 7.5)
    per_symbol_timeout = max(2.0, min(20.0, per_symbol_timeout))
    analyze_top = max(3, int(cfg.get("scanAnalyzeTop", 8) or 8))
    expanded_top = int(cfg.get("scanGuardedFallbackAnalyzeTop", max(analyze_top * 2, analyze_top + 4)) or analyze_top)
    scan_top_liquid = int(cfg.get("scanTopLiquid", 30) or 30)
    planned_symbols = max(analyze_top, min(expanded_top, scan_top_liquid, 16))
    waves = math.ceil(planned_symbols / max(1, SCAN_ANALYZE_CONCURRENCY))
    ticker_budget = 12.0
    fallback_retry_budget = max(0, int(cfg.get("scanFallbackRetrySymbols", 3) or 3)) * min(8.0, per_symbol_timeout + 1.5)
    computed = ticker_budget + (waves * per_symbol_timeout) + fallback_retry_budget + 8.0
    legacy = (interval * 2.0) + 12.0
    return max(legacy, min(120.0, computed))


def _apply_per_symbol_scan_cadence(symbols: list[str], now: float | None = None) -> list[str]:
    """Drop symbols scanned too recently when a per-symbol scan cadence is set.

    Keeps a single noisy symbol from being re-scanned every loop iteration while
    leaving each symbol independent of the global scan cadence. Disabled (returns
    all symbols) when ``perSymbolScanCadenceSec`` is 0 or unset.
    """
    cfg = AUTO_TRADE.get("config")
    cadence = int((cfg or {}).get("perSymbolScanCadenceSec", 0) or 0)
    if cadence <= 0:
        return list(symbols)
    now_f = float(now if now is not None else time.time())
    try:
        from trading.per_symbol_context import PerSymbolContext
        from trading.shared_cache_layer import get_shared_cache
        from services.config_paths import VAULT_DIR
        cache = get_shared_cache(VAULT_DIR)
        out = []
        for s in symbols:
            try:
                ctx = PerSymbolContext(s, cache, cfg)
                rt = ctx.get_runtime()
                last = float((rt or {}).get("lastScanAt", 0) or 0)
                if now_f - last < cadence:
                    continue
            except Exception:
                pass
            out.append(s)
        return out
    except Exception:
        return list(symbols)


def _record_per_symbol_scan_time(symbols: list[str], now: float | None = None) -> None:
    """Persist ``lastScanAt`` per symbol so the per-symbol cadence can skip fresh ones."""
    cfg = AUTO_TRADE.get("config")
    cadence = int((cfg or {}).get("perSymbolScanCadenceSec", 0) or 0)
    if cadence <= 0 or not symbols:
        return
    now_f = float(now if now is not None else time.time())
    try:
        from trading.per_symbol_context import PerSymbolContext
        from trading.shared_cache_layer import get_shared_cache
        from services.config_paths import VAULT_DIR
        cache = get_shared_cache(VAULT_DIR)
        for s in symbols:
            try:
                ctx = PerSymbolContext(s, cache, cfg)
                rt = ctx.get_runtime()
                if not isinstance(rt, dict):
                    rt = {}
                rt["lastScanAt"] = now_f
                ctx.save_runtime(rt)
            except Exception:
                continue
    except Exception:
        pass


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
        day_cap = int(cfg.get("maxDailyTradesPerSymbol", _DEFAULT_MAX_DAILY_TRADES_PER_SYMBOL) or _DEFAULT_MAX_DAILY_TRADES_PER_SYMBOL)
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

    async def _analyze_one_limited(req: IntelAnalyzeRequest):
        async with scan_sem:
            return await _analyze_one(req)

    results = await asyncio.gather(*[_analyze_one_limited(r) for r in reqs], return_exceptions=False)
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
                open_rows = AUTO_TRADE.get("openLivePositions") or []
                open_syms = _open_symbols_from_positions(open_rows)
                if open_syms:
                    batch_symbols = list(dict.fromkeys(batch_symbols + sorted(open_syms)))
            except Exception:
                pass
            if batch_symbols:
                await asyncio.to_thread(tv_client.batch_fetch_signals, batch_symbols)
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
            err_text = _format_loop_error(out)
            _record_scan_health(sym, False, err_text)
            # -4411 during scan analyze → permanently deny this symbol
            if _is_fapi_agreement_error(err_text):
                deny = set(_parse_symbol_whitelist(cfg.get("scanDenySymbols")))
                if sym not in deny:
                    deny.add(sym)
                    cfg["scanDenySymbols"] = sorted(deny)
                    AUTO_TRADE["config"] = copy.deepcopy(cfg)
                    _autotrade_log(f"scan analyze -4411: {sym} permanently denied")
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
        _mm = out.get("momentum") if isinstance(out.get("momentum"), dict) else {}
        momentum = abs(float(_mm.get("momentumPct", 0.0) or 0.0))
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
        # Hard per-symbol floor: never allow entries below 0.60 even if the
        # group profile or learned window loosens the gate (ESPUSDT-style
        # 0.47 profiles opened too easily and ate oversized SLs).
        adaptive_min_conf = max(0.60, max(group_conf_floor, min(0.82, adaptive_min_conf)))
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
        _record_symbol_observation(sym, out, False, score)
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
                # Hard per-symbol floor: same 0.60 floor as the main scan board.
                adaptive_min_conf = max(0.60, min(0.90, adaptive_min_conf))
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
                _record_symbol_observation(sym, out, False, score)
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
        _record_symbol_observation(best_sym, best_intel, True, best_score)
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
    # Per-symbol scan cadence bookkeeping: mark every analyzed symbol as scanned.
    try:
        _record_per_symbol_scan_time(
            [str(r.get("symbol", "")).upper().strip() for r in board if str(r.get("symbol", "")).strip()]
        )
    except Exception:
        pass
    if board and all(str(x.get("rejectReason", "")) == "analyze_error" for x in board):
        board.sort(key=lambda x: (float(x.get("scanErrorStreak", 0) or 0), x.get("symbol", "")))
    return best_sym, best_intel, board[:10]


def _risk_cooldown_regime(intel: dict | None) -> dict:
    try:
        return detect_market_regime(intel if isinstance(intel, dict) else None)
    except Exception:
        return {"name": "UNKNOWN", "confidenceBoost": 0.0, "sizeMultiplier": 1.0, "strictness": "normal"}


def _risk_cooldown_resume_ok(cfg: dict, symbol: str | None, intel: dict | None) -> tuple[bool, str]:
    if not isinstance(intel, dict):
        return False, "no market intel"
    symbol = str(symbol or intel.get("symbol") or "").upper().strip()
    signal = str(intel.get("signal", "WAIT")).upper()
    conf = float(intel.get("confidence", 0.0) or 0.0)
    px = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    long_score = float(px.get("longScore", 0.0) or 0.0)
    short_score = float(px.get("shortScore", 0.0) or 0.0)
    score_gap = abs(long_score - short_score)
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    spread_bps = float(ex.get("spreadBps", 0.0) or 0.0)
    regime = _risk_cooldown_regime(intel)
    regime_name = str(regime.get("name", "UNKNOWN")).upper()
    min_conf = max(float(cfg.get("minConfidence", 0.62) or 0.62), float(_learned_min_conf(symbol, float(cfg.get("minConfidence", 0.62) or 0.62))))
    gap_min = float(cfg.get("riskCooldownResumeScoreGapMin", cfg.get("earlyEntryScoreGapMin", 1.4)) or 1.4)
    if regime_name == "VOLATILE":
        return False, f"market volatile ({symbol or '-'})"
    if regime_name in ("UNKNOWN", "RANGE") and score_gap < gap_min:
        return False, f"market {regime_name.lower()} gap={score_gap:.1f}"
    if signal not in ("LONG", "SHORT"):
        return False, f"signal {signal}"
    if conf < min_conf:
        return False, f"confidence {conf:.2f} < {min_conf:.2f}"
    if spread_bps > float(cfg.get("maxSpreadBps", 22.0) or 22.0):
        return False, f"spread {spread_bps:.1f}bps"
    return True, f"{regime_name} {symbol} {signal} c={conf:.2f} gap={score_gap:.1f}"


async def _adaptive_risk_cooldown_check(cfg: dict, exclude_symbols: set[str] | None = None) -> dict:
    scan_mode = bool(cfg.get("marketScan")) or str(cfg.get("symbol", "")).upper() in ("AUTO", "SCAN")
    if scan_mode:
        picked_symbol, picked_intel, board = await _pick_best_symbol_from_scan(cfg, exclude_symbols)
        ok, reason = _risk_cooldown_resume_ok(cfg, picked_symbol, picked_intel)
        return {"resume": ok, "reason": reason, "symbol": picked_symbol, "intel": picked_intel, "board": board}
    primary_symbol = str(cfg.get("primarySymbol") or cfg.get("symbol") or "").upper().strip()
    if not primary_symbol:
        return {"resume": False, "reason": "no primary symbol", "symbol": None, "intel": None, "board": []}
    if primary_symbol in (exclude_symbols or set()):
        return {"resume": False, "reason": f"{primary_symbol} already open", "symbol": primary_symbol, "intel": None, "board": []}
    intel = await intel_analyze(IntelAnalyzeRequest(symbol=primary_symbol))
    ok, reason = _risk_cooldown_resume_ok(cfg, primary_symbol, intel)
    return {"resume": ok, "reason": reason, "symbol": primary_symbol, "intel": intel, "board": []}


async def _refresh_risk_cooldown_watchlist(cfg: dict, exclude_symbols: set[str] | None, now: int, reason: str) -> bool:
    """Refresh scan candidates while risk cooldown blocks entries; never places orders."""
    if not bool(cfg.get("riskCooldownLightScanEnabled", True)):
        return False
    scan_mode = bool(cfg.get("marketScan")) or str(cfg.get("symbol", "")).upper() in ("AUTO", "SCAN")
    if not scan_mode:
        return False
    refresh_sec = max(20, int(cfg.get("riskCooldownLightScanSec", 45) or 45))
    last_at = int(AUTO_TRADE.get("riskCooldownLastLightScanAt", 0) or 0)
    if now - last_at < refresh_sec and isinstance(AUTO_TRADE.get("cooldownWatchlist"), dict):
        return False
    AUTO_TRADE["riskCooldownLastLightScanAt"] = now
    light_cfg = dict(cfg)
    try:
        light_cfg["scanTopLiquid"] = max(20, min(int(cfg.get("scanTopLiquid", 60) or 60), 80))
    except (TypeError, ValueError):
        light_cfg["scanTopLiquid"] = 60
    try:
        light_cfg["scanAnalyzeTop"] = max(3, min(int(cfg.get("riskCooldownLightScanAnalyzeTop", 6) or 6), 10))
    except (TypeError, ValueError):
        light_cfg["scanAnalyzeTop"] = 6
    light_cfg["scanGuardedFallbackAnalyzeTop"] = max(
        int(light_cfg.get("scanAnalyzeTop", 6) or 6),
        min(12, int(light_cfg.get("scanGuardedFallbackAnalyzeTop", 8) or 8)),
    )
    try:
        _agent_mark("market_analyst", "doing", "cooldown watchlist scan", reason)
        picked_symbol, picked_intel, board = await asyncio.wait_for(
            _pick_best_symbol_from_scan(light_cfg, exclude_symbols),
            timeout=min(65.0, _scan_timeout_budget_sec(light_cfg)),
        )
        if isinstance(board, list):
            AUTO_TRADE["scanBoard"] = board
        AUTO_TRADE["cooldownWatchlist"] = {
            "updatedAt": now,
            "reason": reason,
            "picked": picked_symbol,
            "candidates": len(board) if isinstance(board, list) else 0,
            "top": list(board or [])[:6],
        }
        _agent_mark(
            "market_analyst",
            "done",
            "cooldown watchlist refreshed",
            str(picked_symbol or "no_pick"),
            {"candidates": len(board) if isinstance(board, list) else 0, "picked": picked_symbol},
        )
        _agent_mark("strategy_builder", "todo", "waiting risk cooldown with watchlist", reason)
        _persist_autotrade_snapshot()
        return bool(picked_symbol and isinstance(picked_intel, dict))
    except asyncio.TimeoutError:
        _agent_mark("market_analyst", "blocked", "cooldown watchlist timed out", reason)
    except Exception as exc:
        _agent_mark("market_analyst", "blocked", "cooldown watchlist failed", _format_loop_error(exc)[:80])
    _persist_autotrade_snapshot()
    return False


async def _cached_klines(symbol: str, interval: str, limit: int) -> list:
    key = (symbol, interval, limit)
    now = time.time()
    if key in _KLINES_CACHE:
        fetched_at, data = _KLINES_CACHE[key]
        if now - fetched_at < _KLINES_CACHE_TTL:
            return data
    if key in _KLINES_INFLIGHT:
        try:
            return await asyncio.wait_for(_KLINES_INFLIGHT[key], timeout=max(1.0, DATA_GET_TIMEOUT_SEC + 1.0))
        except Exception as exc:
            print(f"[Trade Log] ERROR writing {TRADES_LOG_PATH}: {exc}")

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
    # Evict oldest entry if cache is full
    if len(_KLINES_CACHE) >= _KLINES_CACHE_MAX:
        oldest_key = min(_KLINES_CACHE, key=lambda k: _KLINES_CACHE[k][0])
        del _KLINES_CACHE[oldest_key]
    _KLINES_CACHE[key] = (now, data)
    return data


async def _public_get(url: str) -> httpx.Response:
    """Use long-lived client from app lifespan; fallback for scripts/tests."""
    if _BINANCE_HTTP is not None:
        try:
            return await _BINANCE_HTTP.get(url)
        except (httpx.RemoteProtocolError, httpx.LocalProtocolError, httpx.ReadError):
            pass  # stale connection — fall through to fresh client
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0)) as c:
        return await c.get(url)


def _is_retryable_http_exc(err: Exception) -> bool:
    txt = str(err).lower()
    if isinstance(err, (asyncio.TimeoutError, httpx.RequestError, httpx.TimeoutException, httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadError, httpx.PoolTimeout)):
        return True
    return ("getaddrinfo failed" in txt) or ("name or service not known" in txt) or ("temporary failure in name resolution" in txt)


_DATA_GET_MAX_RESPONSE_BYTES = 50 * 1024 * 1024  # 50 MB guard


async def _data_get(path: str) -> httpx.Response:
    """
    Fast GET for public market data on fapi.binance.com mainnet.
    Uses dedicated connection pool with automatic fallback to fresh client.
    path must start with /fapi/...
    """
    full_url = f"https://fapi.binance.com{path}"
    if _data_provider_cooldown_active():
        raise httpx.ConnectTimeout("data provider cooldown active")
    last_err: Exception | None = None
    for attempt in range(DATA_GET_MAX_ATTEMPTS):
        try:
            if _BINANCE_DATA_HTTP is not None:
                try:
                    res = await asyncio.wait_for(_BINANCE_DATA_HTTP.get(path), timeout=DATA_GET_TIMEOUT_SEC)
                    if hasattr(res, "content") and len(res.content) > _DATA_GET_MAX_RESPONSE_BYTES:
                        raise httpx.ReadError(f"Response too large: {len(res.content)} bytes")
                    _record_data_provider_health(True)
                    return res
                except (httpx.RemoteProtocolError, httpx.LocalProtocolError,
                        httpx.ReadError, httpx.ConnectError, httpx.PoolTimeout,
                        httpx.ConnectTimeout, httpx.RequestError,
                        MemoryError) as e:
                    last_err = e
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(DATA_GET_TIMEOUT_SEC, connect=DATA_GET_CONNECT_TIMEOUT_SEC),
                headers={"Accept-Encoding": "identity"},
            ) as c:
                res = await c.get(full_url)
                if hasattr(res, "content") and len(res.content) > _DATA_GET_MAX_RESPONSE_BYTES:
                    raise httpx.ReadError(f"Response too large: {len(res.content)} bytes")
                _record_data_provider_health(True)
                return res
        except httpx.ReadError as e:
            last_err = e
            _record_data_provider_health(False, e, path)
            if attempt >= 2 or not _is_retryable_http_exc(e):
                raise
            await asyncio.sleep(0.35 * (attempt + 1))
        except Exception as e:
            last_err = e
            _record_data_provider_health(False, e, path)
            if attempt >= 2 or not _is_retryable_http_exc(e):
                raise
            await asyncio.sleep(0.35 * (attempt + 1))
    if last_err is not None:
        raise last_err
    raise RuntimeError("data_get failed")


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _BINANCE_HTTP, _BINANCE_DATA_HTTP
    timeout = httpx.Timeout(30.0, connect=10.0)
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10, keepalive_expiry=30.0)
    _BINANCE_HTTP = httpx.AsyncClient(timeout=timeout, limits=limits)

    # Dedicated fast client for public market data (mainnet fapi.binance.com)
    # Accept-Encoding: identity prevents gzip/br decompression which can OOM on large responses
    data_limits = httpx.Limits(max_keepalive_connections=20, max_connections=40, keepalive_expiry=15.0)
    _BINANCE_DATA_HTTP = httpx.AsyncClient(
        timeout=httpx.Timeout(DATA_GET_TIMEOUT_SEC, connect=DATA_GET_CONNECT_TIMEOUT_SEC),
        limits=data_limits,
        base_url="https://fapi.binance.com",
        headers={"Accept-Encoding": "identity"},
    )
    _load_autotrade_snapshot()

    # ── Startup: rotate old SCAN entries to reduce data bloat ──────────────
    def _startup_rotate_scan_entries():
        try:
            from trading.trade_log import rotate_scan_entries
            result = rotate_scan_entries(keep_scan_days=7)
            if result.get("removed", 0) > 0:
                _autotrade_log(f"Startup SCAN rotation: removed {result['removed']} old SCAN entries from global log")
        except Exception:
            pass
        try:
            from trading.per_symbol_storage import PerSymbolStorage
            from services.config_paths import VAULT_DIR
            vault = VAULT_DIR
            symbols_dir = vault / "symbols"
            if symbols_dir.exists():
                total_removed = 0
                for sym_dir in symbols_dir.iterdir():
                    if sym_dir.is_dir():
                        try:
                            storage = PerSymbolStorage(vault, sym_dir.name)
                            res = storage.rotate_trades(keep_scan_days=7)
                            total_removed += res.get("removed", 0)
                        except Exception:
                            pass
                if total_removed > 0:
                    _autotrade_log(f"Startup SCAN rotation: removed {total_removed} old SCAN entries from per-symbol trades")
        except Exception:
            pass

    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(_startup_rotate_scan_entries)
    except Exception:
        pass

    # ── Auto-resume autotrade if snapshot says it was running ─────────────────
    async def _maybe_resume():
        """
        If the snapshot recorded a running session with a valid config,
        automatically restart the autotrade loop after a short delay
        (gives the server time to fully initialize first).
        """
        # Resume almost immediately so UI refresh after restart still sees running session.
        await asyncio.sleep(0.2)
        try:
            if AUTO_TRADE.get("running"):
                return
            snap_data = {}
            if SNAPSHOT_PATH.exists():
                snap_data = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
            was_running = bool(snap_data.get("running"))
            cfg = snap_data.get("config")
            if not was_running or not isinstance(cfg, dict) or not cfg.get("symbol"):
                return
            force_single_present = "orphanAutoAdoptForceSingleSymbol" in cfg
            raw_symbol = str(cfg.get("symbol", "") or "").upper().strip()
            primary_symbol = str(cfg.get("primarySymbol", "") or "").upper().strip()
            if (
                not force_single_present
                and (cfg.get("executionMode") or "PAPER").upper() == "LIVE"
                and not bool(cfg.get("marketScan"))
                and raw_symbol
                and primary_symbol
                and raw_symbol == primary_symbol
            ):
                cfg["marketScan"] = True
                cfg["symbol"] = "AUTO"
                cfg["whitelistSymbols"] = []
                cfg["orphanAutoAdoptForceSingleSymbol"] = False
                _autotrade_log(f"Auto-resume self-healed legacy single-symbol scan lock: {primary_symbol} -> AUTO")
            # Restore config and restart loop
            global _AUTOTRADE_TASK
            session_id = str(uuid4())
            AUTO_TRADE["running"] = True
            AUTO_TRADE["sessionId"] = session_id
            AUTO_TRADE["startedAt"] = int(time.time())
            _sync_autotrade_leverage_cap_from_cfg(cfg)
            AUTO_TRADE["config"] = apply_autotrade_defaults(copy.deepcopy(cfg))
            AUTO_TRADE["consecutiveErrors"] = 0
            AUTO_TRADE["lastSkip"] = None
            AUTO_TRADE["scanBoard"] = snap_data.get("scanBoard") if isinstance(snap_data.get("scanBoard"), list) else []
            AUTO_TRADE["hermesAgents"] = ensure_agent_state(snap_data.get("hermesAgents"))
            AUTO_TRADE["lastTradeAt"] = int(snap_data.get("lastTradeAt", 0))
            AUTO_TRADE["trades"] = [t for t in snap_data.get("trades", [])
                                    if time.time() - t < 3600]
            resume_msg = (
                f"AUTO-RESUMED after restart: {cfg.get('symbol')} "
                f"{cfg.get('executionMode','PAPER')} x{cfg.get('leverage',1)}"
            )
            _autotrade_log(resume_msg)
            await _ensure_autotrade_task_alive("auto-resume")
        except Exception as e:
            _autotrade_log(f"Auto-resume failed: {_format_loop_error(e)}")

    asyncio.create_task(_maybe_resume())
    asyncio.create_task(_autotrade_watchdog_loop())

    # ── Learning scheduler (daily background training) ────────────────────
    async def _learning_scheduler_loop():
        interval = max(300, int(os.getenv("LEARNING_TRAIN_INTERVAL_SEC", "86400")))
        while True:
            try:
                await asyncio.sleep(interval)
                await asyncio.to_thread(_run_learning_train_background)
            except Exception:
                pass
    if os.getenv("LEARNING_SCHEDULER_ENABLED", "true").lower() == "true":
        asyncio.create_task(_learning_scheduler_loop())

    # Warm up connection pool with a lightweight request
    async def _warmup():
        try:
            await _data_get("/fapi/v1/time")
        except Exception:
            pass
    asyncio.create_task(_warmup())

    # Pre-warm klines cache for the active scan symbol so the pre-reversal
    # guard never has to block on a network round-trip during main cycle.
    async def _klines_prewarm_loop():
        while True:
            try:
                cfg = AUTO_TRADE.get("config") or {}
                sym = str(cfg.get("symbol", "AUTO") or "AUTO").upper()
                if sym in ("AUTO", "SCAN", ""):
                    syms = [
                        s for s in (cfg.get("whitelistSymbols") or [])
                        if isinstance(s, str) and s
                    ]
                    if not syms:
                        syms = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
                else:
                    syms = [sym]
                for s in syms:
                    try:
                        await asyncio.wait_for(
                            _cached_klines(s, "5m", 60), timeout=4.0
                        )
                    except Exception:
                        pass
            except Exception:
                pass
            await asyncio.sleep(20)
    asyncio.create_task(_klines_prewarm_loop())
    try:
        yield
    finally:
        _persist_autotrade_snapshot(force=True)
        await _BINANCE_HTTP.aclose()
        await _BINANCE_DATA_HTTP.aclose()
        _BINANCE_HTTP = None
        _BINANCE_DATA_HTTP = None


app = FastAPI(title="Binance Autotrend API", version="0.5.0", lifespan=_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONITORS: dict[str, dict] = {}
MAX_ACTIVE_MONITORS = 50
MONITORS_LOCK = asyncio.Lock()
DAILY_REALIZED_PNL = 0.0
_DAILY_PNL_DATE_KEY = 0
_SNAPSHOT_LAST_FLUSH = 0.0
_LIVE_POSITIONS_CACHE = (0.0, [])
_SUPERVISOR_LAST_REVIEW = 0
_AUTOTRADE_TASK: asyncio.Task | None = None  # track running task to prevent duplicates
_AUTOTRADE_TASK_LOCK = asyncio.Lock()
AUTO_TRADE = {
    "running": False,
    "manageOpenOnly": False,
    "pauseUntil": 0,
    "riskCooldownLossSignature": "",
    "riskCooldownBySymbol": {},
    "riskCooldownLastMarketCheckAt": 0,
    "perfLocks": {},
    "sessionId": None,
    "startedAt": 0,
    "config": None,
    "lastDecision": None,
    "lastDecisions": {},
    "lastSkip": None,
    "lastTradeAt": 0,
    "trades": [],  # unix timestamps
    "log": [],
    "consecutiveErrors": 0,
    "liveProfitLocks": {},
    "scanBoard": [],
    "cooldownWatchlist": {},
    "hermesAgents": new_agent_state(),
    "hermesSupervisorReview": {},
    "paper": {
        "position": None,
        "wins": 0,
        "losses": 0,
        "realizedPnl": 0.0,
        "history": [],
    },
    "_snapshot_saved_at": None,
    "_snapshot_loaded_at": None,
    "_snapshot_recovered_log": None,
}

# Guardian, supervisor, and exchange helpers hold references to
# services.app_state.AUTO_TRADE. Update that dict in-place so every module
# observes the same runtime config, locks, and per-symbol state.
try:
    from services import app_state as _app_state_sync
    _app_state_sync.AUTO_TRADE.update(AUTO_TRADE)
    AUTO_TRADE = _app_state_sync.AUTO_TRADE
except Exception:
    pass


def _autotrade_task_state() -> dict:
    task = _AUTOTRADE_TASK
    if task is None:
        return {"exists": False, "done": True, "cancelled": False}
    state = {"exists": True, "done": bool(task.done()), "cancelled": bool(task.cancelled())}
    if task.done() and not task.cancelled():
        try:
            exc = task.exception()
        except Exception as e:
            exc = e
        if exc is not None:
            state["error"] = _format_loop_error(exc)[:180]
    return state


def _track_autotrade_task(task: asyncio.Task, reason: str) -> asyncio.Task:
    def _done(done_task: asyncio.Task):
        if done_task.cancelled():
            return
        try:
            exc = done_task.exception()
        except Exception as e:
            exc = e
        if exc is not None:
            _autotrade_log(f"AutoTrade task stopped unexpectedly ({reason}): {_format_loop_error(exc)}")

    task.add_done_callback(_done)
    return task


async def _ensure_autotrade_task_alive(reason: str = "watchdog") -> bool:
    global _AUTOTRADE_TASK
    async with _AUTOTRADE_TASK_LOCK:
        if not (bool(AUTO_TRADE.get("running")) or bool(AUTO_TRADE.get("manageOpenOnly"))):
            return False
        if _AUTOTRADE_TASK is not None and not _AUTOTRADE_TASK.done():
            return False
        _AUTOTRADE_TASK = _track_autotrade_task(asyncio.create_task(_autotrade_loop()), reason)
        _autotrade_log(f"AutoTrade task restarted: {reason}")
        return True


async def _autotrade_watchdog_loop():
    while True:
        try:
            await _ensure_autotrade_task_alive("watchdog")
        except Exception as e:
            _autotrade_log(f"AutoTrade watchdog failed: {_format_loop_error(e)}")
        await asyncio.sleep(10)


def _rolling_symbol_perf(symbol: str, window: int = 30) -> dict:
    empty = {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "winRatePct": 0.0,
        "pnl": 0.0,
        "avgWin": 0.0,
        "avgLoss": 0.0,
        "payoffRatio": 0.0,
        "memoryWindow": "none",
    }
    sym = str(symbol or "").upper().strip()
    if not sym:
        return dict(empty)
    rows = _live_closed_trades_from_log(symbol=sym, mode="ALL")
    if not rows:
        return dict(empty)
    if not any(int(float((r or {}).get("_ts", (r or {}).get("closedAt", (r or {}).get("ts", 0))) or 0)) > 0 for r in rows if isinstance(r, dict)):
        now = int(time.time())
        rows = [
            {**r, "_ts": now - ((len(rows) - idx) * 60)}
            for idx, r in enumerate(rows)
            if isinstance(r, dict)
        ]
    windows = _memory_windows_from_trades(rows)
    min_recent = max(3, min(6, int(window or 30)))
    selected = windows.get("7d", {})
    selected_key = "7d"
    if int(selected.get("trades", 0) or 0) < min_recent:
        selected = windows.get("15d", {})
        selected_key = "15d"
    if int(selected.get("trades", 0) or 0) < min_recent:
        selected = windows.get("30d", {})
        selected_key = "30d"
    if int(selected.get("trades", 0) or 0) < min_recent:
        tail = rows[-max(3, int(window)) :]
        selected = _memory_windows_from_trades(tail).get("all", {})
        selected_key = "last_trades"
    out = dict(selected or empty)
    out["memoryWindow"] = selected_key
    out["memoryWindows"] = windows
    out["weightedRecentScore"] = _weighted_recent_memory_score(windows)
    return out


def _symbol_perf_gate(cfg: dict, symbol: str) -> tuple[bool, str, dict]:
    now = int(time.time())
    locks = AUTO_TRADE.get("perfLocks")
    if not isinstance(locks, dict):
        locks = {}
    sym = str(symbol or "").upper().strip()
    perf = _rolling_symbol_perf(sym, int(cfg.get("perfWindowTrades", 30) or 30))
    min_samples = int(cfg.get("perfGateMinSamples", 8) or 8)
    bad_wr = float(cfg.get("perfGateMinWinRatePct", 33.0) or 33.0)
    bad_pnl = float(cfg.get("perfGateMinPnlUsdt", -0.60) or -0.60)
    early_samples = int(cfg.get("perfGateEarlyMinSamples", 4) or 4)
    early_wr = float(cfg.get("perfGateEarlyMinWinRatePct", 35.0) or 35.0)
    early_pnl = float(cfg.get("perfGateEarlyMinPnlUsdt", -0.35) or -0.35)
    reward_min = float(cfg.get("perfGateMinRewardScore", -1.25) or -1.25)
    payoff_enabled = bool(cfg.get("payoffGuardEnabled", True))
    payoff_samples = int(cfg.get("payoffGuardMinSamples", min_samples) or min_samples)
    payoff_wr = float(cfg.get("payoffGuardMinWinRatePct", 55.0) or 55.0)
    payoff_pnl = float(cfg.get("payoffGuardMaxPnlUsdt", -0.25) or -0.25)
    payoff_ratio_min = float(cfg.get("payoffGuardMinPayoffRatio", 0.72) or 0.72)
    payoff_min_losses = int(cfg.get("payoffGuardMinLosses", 2) or 2)
    lock_min = int(cfg.get("perfLockMinutes", 45) or 45)

    st = locks.get(sym, {})
    lock_until = int(st.get("until", 0) or 0)
    if lock_until > now:
        stored_perf = st.get("perf") if isinstance(st.get("perf"), dict) else {}
        lock_age_sec = max(0, now - int(st.get("at", now) or now))
        no_current_evidence = int(perf.get("trades", 0) or 0) <= 0
        no_stored_evidence = int(stored_perf.get("trades", 0) or 0) <= 0
        no_evidence_grace_min = int(cfg.get("perfLockNoEvidenceMaxAgeMinutes", min(30, lock_min)) or 30)
        no_evidence_grace_sec = max(60, no_evidence_grace_min * 60)
        if no_current_evidence and no_stored_evidence and lock_age_sec >= no_evidence_grace_sec:
            locks.pop(sym, None)
            AUTO_TRADE["perfLocks"] = locks
            lock_until = 0
        else:
            active_perf = dict(perf)
            active_perf["lockReason"] = str(st.get("reason", "active") or "active")
            active_perf["lockUntil"] = lock_until
            active_perf["lockAgeSec"] = lock_age_sec
            active_perf["lockEvidenceTrades"] = int(stored_perf.get("trades", 0) or 0)
            return False, f"perf_lock({max(1, (lock_until-now)//60)}m)", active_perf
    if lock_until > 0 and lock_until <= now:
        locks.pop(sym, None)
        AUTO_TRADE["perfLocks"] = locks

    pr = _load_single_profile(sym)
    reward_score = float(pr.get("rewardScore", 0.0) or 0.0) if isinstance(pr, dict) else 0.0
    weighted_recent = perf.get("weightedRecentScore") if isinstance(perf.get("weightedRecentScore"), dict) else {}
    recent_memory_score = float(weighted_recent.get("score", 0.0) or 0.0)
    if perf["trades"] >= max(3, early_samples) and perf["winRatePct"] < early_wr and perf["pnl"] <= early_pnl:
        until = now + (lock_min * 60)
        locks[sym] = {"until": until, "perf": perf, "at": now, "reason": "early"}
        AUTO_TRADE["perfLocks"] = locks
        return False, "perf_lock_early", perf
    if (reward_score <= reward_min or recent_memory_score <= -0.35) and perf["trades"] >= max(3, early_samples):
        until = now + (lock_min * 60)
        locks[sym] = {"until": until, "perf": perf, "at": now, "reason": "reward", "rewardScore": reward_score, "recentMemoryScore": recent_memory_score}
        AUTO_TRADE["perfLocks"] = locks
        return False, "perf_lock_reward", perf
    payoff_ratio = float(perf.get("payoffRatio", 0.0) or 0.0)
    if (
        payoff_enabled
        and perf["trades"] >= max(3, payoff_samples)
        and perf["wins"] > perf["losses"]
        and perf["losses"] >= payoff_min_losses
        and perf["winRatePct"] >= payoff_wr
        and perf["pnl"] <= payoff_pnl
        and 0.0 < payoff_ratio < payoff_ratio_min
    ):
        until = now + (lock_min * 60)
        locks[sym] = {
            "until": until,
            "perf": perf,
            "at": now,
            "reason": "payoff",
            "payoffRatio": payoff_ratio,
        }
        AUTO_TRADE["perfLocks"] = locks
        return False, "perf_lock_payoff", perf
    if perf["trades"] >= min_samples and perf["winRatePct"] < bad_wr and perf["pnl"] <= bad_pnl:
        until = now + (lock_min * 60)
        locks[sym] = {"until": until, "perf": perf, "at": now, "reason": "sustained"}
        AUTO_TRADE["perfLocks"] = locks
        return False, "perf_lock_new", perf
    return True, "", perf


def _switch_fixed_symbol_to_scan(cfg: dict, symbol: str, reason: str, detail: str = "", *, lock_minutes: int = 0) -> dict:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return cfg
    if lock_minutes > 0:
        now = int(time.time())
        locks = AUTO_TRADE.get("perfLocks")
        if not isinstance(locks, dict):
            locks = {}
        locks[sym] = {
            "until": now + (max(1, int(lock_minutes)) * 60),
            "at": now,
            "reason": str(reason or "symbol_skip"),
            "detail": str(detail or "")[:160],
            "perf": {"trades": 0, "wins": 0, "losses": 0, "winRatePct": 0.0, "pnl": 0.0},
        }
        AUTO_TRADE["perfLocks"] = locks
    if not cfg.get("primarySymbol"):
        cfg["primarySymbol"] = sym
    cfg["marketScan"] = True
    cfg["symbol"] = "AUTO"
    cfg["orphanAutoAdoptForceSingleSymbol"] = False
    wl = _parse_symbol_whitelist(cfg.get("whitelistSymbols"))
    if len(wl) <= 1 and (not wl or sym in wl):
        cfg["whitelistSymbols"] = []
    AUTO_TRADE["config"] = copy.deepcopy(cfg)
    AUTO_TRADE["symbolWaitState"] = {}
    _autotrade_log(f"Symbol skip -> AUTO scan: {sym} ({reason}{': ' + detail if detail else ''})")
    _persist_autotrade_snapshot()
    return cfg


def _maybe_skip_stale_wait_symbol(cfg: dict, symbol: str, signal: str, *, scan_mode: bool, now: int | None = None) -> bool:
    sym = str(symbol or "").upper().strip()
    sig = str(signal or "WAIT").upper()
    if scan_mode or not sym or sig in ("LONG", "SHORT"):
        AUTO_TRADE["symbolWaitState"] = {}
        return False
    if not bool(cfg.get("staleWaitSymbolSkipEnabled", True)):
        return False
    now_ts = int(now or time.time())
    state = AUTO_TRADE.get("symbolWaitState")
    if not isinstance(state, dict) or str(state.get("symbol", "")).upper() != sym:
        state = {"symbol": sym, "count": 0, "firstAt": now_ts}
    count = int(state.get("count", 0) or 0) + 1
    state.update({"symbol": sym, "count": count, "lastAt": now_ts})
    AUTO_TRADE["symbolWaitState"] = state
    min_cycles = max(2, int(cfg.get("staleWaitSymbolSkipCycles", 6) or 6))
    if count < min_cycles:
        return False
    lock_min = max(5, int(cfg.get("staleWaitSymbolLockMinutes", 20) or 20))
    detail = f"WAIT {count} cycles"
    _switch_fixed_symbol_to_scan(cfg, sym, "stale_wait", detail, lock_minutes=lock_min)
    _agent_mark("strategy_builder", "blocked", "stale wait symbol skipped", f"{sym} {detail}")
    _autotrade_skip("stale_wait_symbol", f"Skip: {sym} {detail}; switching to market scan")
    return True


def _recent_live_loss_streak(limit: int = 8) -> int:
    return int(_recent_live_loss_streak_state(limit).get("streak", 0) or 0)


def _recent_live_loss_streak_state(limit: int = 8, symbol: str | None = None) -> dict:
    sym_filter = str(symbol or "").upper().strip() or None
    seq = _live_closed_trades_from_log(symbol=sym_filter, mode="ALL")
    if not seq:
        return {"symbol": sym_filter, "streak": 0, "signature": "", "lastClosedAt": 0}
    losses = []
    for t in reversed(seq[-max(3, int(limit)) :]):
        pnl = float(t.get("_pnl", 0.0) or 0.0)
        if pnl < 0:
            losses.append(t)
        else:
            break
    if not losses:
        return {"symbol": sym_filter, "streak": 0, "signature": "", "lastClosedAt": int(seq[-1].get("_ts", 0) or 0)}
    parts = []
    for item in reversed(losses):
        sym = str(item.get("symbol", "")).upper()
        side = str(item.get("side", "")).upper()
        ts = int(item.get("_ts", 0) or 0)
        pnl = float(item.get("_pnl", 0.0) or 0.0)
        parts.append(f"{sym}:{side}:{ts}:{pnl:.8f}")
    return {
        "symbol": sym_filter or str(losses[0].get("symbol", "")).upper().strip(),
        "streak": len(losses),
        "signature": "|".join(parts),
        "lastClosedAt": int(losses[0].get("_ts", 0) or 0),
    }


def _recent_live_loss_streak_states_by_symbol(limit: int = 8) -> dict[str, dict]:
    seq = _live_closed_trades_from_log(symbol=None, mode="ALL")
    by_symbol: dict[str, list[dict]] = {}
    for t in seq:
        sym = str(t.get("symbol", "")).upper().strip()
        if not sym:
            continue
        by_symbol.setdefault(sym, []).append(t)
    out: dict[str, dict] = {}
    lookback = max(3, int(limit))
    for sym, rows in by_symbol.items():
        losses = []
        for t in reversed(rows[-lookback:]):
            pnl = float(t.get("_pnl", 0.0) or 0.0)
            if pnl < 0:
                losses.append(t)
            else:
                break
        if not losses:
            continue
        parts = []
        for item in reversed(losses):
            side = str(item.get("side", "")).upper()
            ts = int(item.get("_ts", 0) or 0)
            pnl = float(item.get("_pnl", 0.0) or 0.0)
            parts.append(f"{sym}:{side}:{ts}:{pnl:.8f}")
        out[sym] = {
            "symbol": sym,
            "streak": len(losses),
            "signature": "|".join(parts),
            "lastClosedAt": int(losses[0].get("_ts", 0) or 0),
        }
    return out


def _risk_cooldown_map() -> dict:
    raw = AUTO_TRADE.get("riskCooldownBySymbol")
    if not isinstance(raw, dict):
        raw = {}
        AUTO_TRADE["riskCooldownBySymbol"] = raw
    return raw


def _risk_cooldown_signature_last_ts(signature: str) -> int:
    latest = 0
    for part in str(signature or "").split("|"):
        bits = part.split(":")
        if len(bits) >= 3:
            try:
                latest = max(latest, int(float(bits[2])))
            except (TypeError, ValueError):
                pass
    return latest


def _prune_risk_cooldowns(now: int | None = None) -> dict:
    now_i = int(now or time.time())
    raw = _risk_cooldown_map()
    kept = {}
    max_age_sec = 6 * 3600
    for sym, rec in raw.items():
        if not isinstance(rec, dict) or int(rec.get("until", 0) or 0) <= now_i:
            continue
        reason = str(rec.get("reason", "") or "")
        if reason in {"loss_streak", "legacy_global_risk_cooldown"}:
            last_ts = int(rec.get("lastClosedAt", 0) or 0) or _risk_cooldown_signature_last_ts(str(rec.get("signature", "") or ""))
            if last_ts and now_i - last_ts > max_age_sec:
                continue
        kept[str(sym).upper()] = rec
    AUTO_TRADE["riskCooldownBySymbol"] = kept
    return kept


def _risk_cooldown_symbols(now: int | None = None) -> set[str]:
    return set(_prune_risk_cooldowns(now).keys())


def _symbol_risk_cooldown_record(symbol: str, now: int | None = None) -> dict | None:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return None
    return _prune_risk_cooldowns(now).get(sym)


def _arm_symbol_risk_cooldown(
    symbol: str,
    minutes: int,
    signature: str,
    loss_streak: int,
    reason: str,
    now: int | None = None,
    last_closed_at: int = 0,
) -> dict | None:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return None
    now_i = int(now or time.time())
    until = now_i + max(1, int(minutes or 1)) * 60
    state = _prune_risk_cooldowns(now_i)
    rec = {
        "symbol": sym,
        "until": until,
        "at": now_i,
        "signature": str(signature or ""),
        "lossStreak": int(loss_streak or 0),
        "reason": str(reason or "risk_cooldown"),
        "lastClosedAt": int(last_closed_at or 0),
    }
    state[sym] = rec
    AUTO_TRADE["riskCooldownBySymbol"] = state
    return rec


def _loss_streak_self_review_tune(cfg: dict, now: int, loss_streak: int, cause: dict | None = None) -> dict:
    out = dict(cfg or {})
    actions: list[str] = []
    cause = cause if isinstance(cause, dict) else {}
    cause_category = str(cause.get("category", "") or "").strip()
    if cause_category == "infra_auth":
        AUTO_TRADE["lastSelfReview"] = {
            "ts": now,
            "lossStreak": int(loss_streak),
            "causeCategory": "infra_auth",
            "causeTitle": str(cause.get("title", "") or "Binance auth/IP permission incident"),
            "causeDetail": str(cause.get("detail", "") or ""),
            "operatorAction": str(cause.get("operatorAction", "") or "Recheck Binance API key permissions and whitelist IP."),
            "actions": ["operator_action_required: fix Binance API/IP permission"],
            "minConfidence": out.get("minConfidence"),
            "maxOpenPositions": out.get("maxOpenPositions"),
            "noTradeWindows": list(out.get("noTradeWindows") or []),
        }
        try:
            write_self_review_memory(VAULT_DIR, AUTO_TRADE["lastSelfReview"])
        except Exception:
            pass
        return out

    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("loss_streak_self_review", out, 60)
    if active:
        return out

    if _tuning_should_rollback("loss_streak_self_review"):
        rollback = _tuning_rollback_last("loss_streak_self_review")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in out:
                    out[k] = v
            _commit_supervisor_config_tune(state, delegations, "loss_streak_self_review", out, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            return out

    # Adaptive severity based on loss_streak length
    severity = max(0.0, min(1.0, (loss_streak - 2) / 5.0))

    changes: dict[str, dict] = {}

    def set_float(key: str, value: float, digits: int = 4):
        old = float(out.get(key, value) or value)
        new = round(float(value), digits)
        if abs(old - new) >= 1e-9:
            out[key] = new
            changes[key] = {"old": round(old, digits), "new": new}

    def set_int(key: str, value: int):
        old = int(out.get(key, value) or value)
        new = int(value)
        if old != new:
            out[key] = new
            changes[key] = {"old": old, "new": new}

    def set_bool(key: str, value: bool):
        old = bool(out.get(key, value))
        if old != value:
            out[key] = value
            changes[key] = {"old": old, "new": value}

    def set_str(key: str, value: str):
        old = str(out.get(key, value) or value)
        if old != str(value):
            out[key] = str(value)
            changes[key] = {"old": old, "new": str(value)}

    old_conf = float(out.get("minConfidence", 0.65) or 0.65)
    new_conf = min(0.86, max(old_conf, old_conf + 0.02 * (1.0 + severity * 2.0)))
    if new_conf > old_conf:
        set_float("minConfidence", new_conf, 4)
        actions.append(f"minConfidence {old_conf:.2f}->{new_conf:.2f}")
    if out.get("scanSidePreference") != "score":
        set_str("scanSidePreference", "score")
        actions.append("scanSidePreference=score")

    # Temporary lock with expiration instead of permanent disable
    if bool(out.get("scanFallbackNearEnabled", True)):
        lock_minutes = max(15, int(out.get("riskCooldownMinutes", 25) or 25))
        AUTO_TRADE.setdefault("_scanFallbackNearLock", {})["until"] = now + lock_minutes * 60
        AUTO_TRADE["_scanFallbackNearLock"]["reason"] = "loss_streak"
        actions.append(f"scanFallbackNearLocked {lock_minutes}m")

    if not bool(out.get("pairLockEnabled", True)):
        set_bool("pairLockEnabled", True)
        actions.append("pairLockEnabled=true")
    set_int("pairLockLossStreak", min(int(out.get("pairLockLossStreak", 2) or 2), 2))
    set_int("pairLockMinutes", max(int(out.get("pairLockMinutes", 45) or 45), 90))
    set_bool("riskCooldownEnabled", True)
    set_int("riskCooldownLossStreak", min(int(out.get("riskCooldownLossStreak", 3) or 3), 3))
    set_int("riskCooldownMinutes", max(int(out.get("riskCooldownMinutes", 25) or 25), 45))
    set_int("perfGateEarlyMinSamples", min(int(out.get("perfGateEarlyMinSamples", 4) or 4), 4))
    set_float("perfGateEarlyMinPnlUsdt", max(float(out.get("perfGateEarlyMinPnlUsdt", -0.35) or -0.35), -0.35), 3)
    set_int("perfLockMinutes", max(int(out.get("perfLockMinutes", 90) or 90), 120))
    old_max_open = int(out.get("maxOpenPositions", _DEFAULT_MAX_OPEN_POSITIONS) or _DEFAULT_MAX_OPEN_POSITIONS)
    diversification_floor = 6 if (bool(out.get("marketScan")) or str(out.get("symbol", "")).upper() in {"AUTO", "SCAN"}) else 2
    reduction = int(round(1 * (1.0 + severity * 2.0))) if loss_streak >= 4 else 0
    new_max_open = max(diversification_floor, min(old_max_open, old_max_open - reduction))
    if new_max_open < old_max_open:
        set_int("maxOpenPositions", new_max_open)
        actions.append(f"maxOpenPositions {old_max_open}->{new_max_open}")
    elif old_max_open < diversification_floor:
        set_int("maxOpenPositions", diversification_floor)
        actions.append(f"maxOpenPositions {old_max_open}->{diversification_floor}")
    hours = _entry_session_hours_from_log(int(out.get("sessionBiasLookbackTrades", 700) or 700))
    hour = time.localtime(now).tm_hour
    st = hours.get(hour)
    if isinstance(st, dict):
        trades = int(st.get("trades", 0) or 0)
        wr = float(st.get("winRatePct", 0.0) or 0.0)
        pnl = float(st.get("pnl", 0.0) or 0.0)
        if trades >= int(out.get("selfReviewMinHourSamples", 8) or 8) and wr <= 42.0 and pnl < 0:
            window = f"{hour:02d}:00-{(hour + 1) % 24:02d}:00"
            old_windows = list(out.get("noTradeWindows") or [])
            windows = list(old_windows)
            if window not in windows:
                windows.append(window)
                out["noTradeWindows"] = windows
                changes["noTradeWindows"] = {"old": old_windows, "new": windows}
                actions.append(f"noTradeWindow {window}")

    signature = _tuning_signature("loss_streak_self_review", loss_streak=loss_streak, cause_category=cause_category, actions=actions)
    if changes:
        _commit_supervisor_config_tune(state, delegations, "loss_streak_self_review", out, changes, "loss_streak")

    AUTO_TRADE["lastSelfReview"] = {
        "ts": now,
        "lossStreak": int(loss_streak),
        "actions": actions,
        "minConfidence": out.get("minConfidence"),
        "maxOpenPositions": out.get("maxOpenPositions"),
        "noTradeWindows": list(out.get("noTradeWindows") or []),
        "severity": severity,
        "signature": signature,
    }
    try:
        write_self_review_memory(VAULT_DIR, AUTO_TRADE["lastSelfReview"])
    except Exception:
        pass
    return out


def _live_trades_count_today_symbol(symbol: str) -> int:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return 0
    now_local = time.localtime()
    y, m, d = now_local.tm_year, now_local.tm_mon, now_local.tm_mday
    seq = _live_closed_trades_from_log(symbol=sym, mode="ALL")
    cnt = 0
    for t in seq:
        ts = int(t.get("_ts", 0) or 0)
        if ts <= 0:
            continue
        lt = time.localtime(ts)
        if lt.tm_year == y and lt.tm_mon == m and lt.tm_mday == d:
            cnt += 1
    return cnt


RISK = {
    "kill_switch": os.getenv("KILL_SWITCH", "false").lower() == "true",
    "max_notional": float(os.getenv("MAX_NOTIONAL_USDT", "200")),
    "max_leverage": float(os.getenv("MAX_LEVERAGE", "25")),
    "max_daily_loss": float(os.getenv("MAX_DAILY_LOSS_USDT", "50")),
}

DEFAULT_LEVERAGE = int(os.getenv("DEFAULT_LEVERAGE", "5"))
DEFAULT_MARGIN_TYPE = os.getenv("DEFAULT_MARGIN_TYPE", "ISOLATED").upper()
DEFAULT_TP_PCT = float(os.getenv("DEFAULT_TP_PCT", "1.8"))
DEFAULT_SL_PCT = float(os.getenv("DEFAULT_SL_PCT", "0.8"))
CONNECTOR_MODE = os.getenv("CONNECTOR_MODE", "auto").lower()  # auto | official | legacy
AI_PROVIDER = os.getenv("AI_PROVIDER", "hermes").lower()  # hermes | openai | off
HERMES_BASE_URL = os.getenv("HERMES_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
HERMES_MODEL = os.getenv("HERMES_MODEL", "hermes-3")
BINANCE_RECV_WINDOW_MS = int(os.getenv("BINANCE_RECV_WINDOW_MS", "10000"))
AUTOTRADE_TAKER_FEE_BPS_PER_SIDE = float(os.getenv("AUTOTRADE_TAKER_FEE_BPS_PER_SIDE", "4.0"))
AUTOTRADE_MIN_NET_PROFIT_USDT = float(os.getenv("AUTOTRADE_MIN_NET_PROFIT_USDT", "0.05"))
AUTOTRADE_EXTRA_COST_BPS = float(os.getenv("AUTOTRADE_EXTRA_COST_BPS", "2.0"))


def _binance_base():
    # Default to mainnet (false) so LIVE mode never accidentally hits testnet.
    return "https://testnet.binancefuture.com" if os.getenv("BINANCE_TESTNET", "false").lower() == "true" else "https://fapi.binance.com"


def _safe_api_key_prefix(key: str | None) -> str:
    text = str(key or "")
    return text[:6] if text else ""


def _signed_request_diagnostic(base: str, endpoint: str, key: str | None, params: dict | None = None) -> dict:
    params = params if isinstance(params, dict) else {}
    base_text = str(base or "")
    endpoint_text = str(endpoint or "")
    market_type = (
        "USDT-M"
        if "/fapi/" in endpoint_text or "fapi.binance.com" in base_text or "binancefuture.com" in base_text
        else "COIN-M"
        if "/dapi/" in endpoint_text or "dapi.binance.com" in base_text
        else "SPOT"
        if "api.binance.com" in base_text
        else "UNKNOWN"
    )
    return {
        "keyPrefix": _safe_api_key_prefix(key),
        "base": base_text,
        "endpoint": endpoint_text,
        "marketType": market_type,
        "symbol": str(params.get("symbol", "") or ""),
        "orderType": str(params.get("type", "") or ""),
        "side": str(params.get("side", "") or ""),
        "positionSide": str(params.get("positionSide", "") or ""),
        "reduceOnly": params.get("reduceOnly"),
        "binanceTestnet": os.getenv("BINANCE_TESTNET", "false").lower() == "true",
    }


def _estimate_trade_edge_usdt(usdt_amount: float, tp_pct: float, max_slippage_bps: float) -> tuple[float, float, float]:
    # Cost model = taker fee entry+exit + micro cost buffer + half of slippage budget.
    gross_profit = float(usdt_amount) * (float(tp_pct) / 100.0)
    cost_bps = (2.0 * AUTOTRADE_TAKER_FEE_BPS_PER_SIDE) + AUTOTRADE_EXTRA_COST_BPS + max(0.0, float(max_slippage_bps) * 0.5)
    est_cost = float(usdt_amount) * (cost_bps / 10000.0)
    net_profit = gross_profit - est_cost
    return gross_profit, est_cost, net_profit


def _fee_edge_min_net_usdt(cfg: dict | None, est_cost_usdt: float = 0.0, notional_usdt: float = 0.0) -> float:
    cfg = cfg if isinstance(cfg, dict) else {}
    configured = float(cfg.get("feeMinNetProfitUSDT", AUTOTRADE_MIN_NET_PROFIT_USDT) or AUTOTRADE_MIN_NET_PROFIT_USDT)
    multiple = max(1.0, float(cfg.get("feeMinEdgeVsCostMultiple", 3.0) or 3.0))
    taker_roundtrip = max(0.0, float(notional_usdt or 0.0)) * ((2.0 * AUTOTRADE_TAKER_FEE_BPS_PER_SIDE) / 10000.0)
    return round(max(configured, float(est_cost_usdt or 0.0) * multiple, taker_roundtrip * multiple), 6)


def _normalize_symbol(symbol: str):
    sym = symbol.upper().replace("/", "")
    if not re.fullmatch(r"[A-Z0-9]{6,20}", sym):
        raise HTTPException(status_code=400, detail="Invalid symbol format")
    if not (sym.endswith("USDT") or sym.endswith("BUSD")):
        raise HTTPException(status_code=400, detail="Only USDT/BUSD futures symbols are allowed")
    return sym


def health():
    return {
        "ok": True,
        "port": _BACKEND_PORT,
        "uptimeSec": int(time.time() - _APP_STARTED_AT),
        "autotradeRunning": bool(AUTO_TRADE.get("running")),
        "autotradeTask": _autotrade_task_state(),
        "version": "0.4.1",
    }


@app.get("/debug/binance-positions")
async def debug_binance_positions():
    """Diagnostic endpoint — test Binance Futures position fetch directly."""
    import traceback
    key = os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_API_SECRET")
    base = _binance_base()
    result = {
        "base": base,
        "testnet": os.getenv("BINANCE_TESTNET", "unset"),
        "connectorMode": os.getenv("CONNECTOR_MODE", "unset"),
        "keyPrefix": (key[:6] + "...") if key else None,
        "keyPresent": bool(key),
        "secretPresent": bool(secret),
    }
    if not key or not secret:
        result["error"] = "BINANCE_API_KEY or BINANCE_API_SECRET not set"
        return result

    # Test 1: public ping (no auth needed)
    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.get(f"{base}/fapi/v1/ping")
            result["ping"] = {"status": r.status_code, "ok": r.status_code == 200}
    except Exception as e:
        result["ping"] = {"error": str(e)}

    # Test 2: server time
    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.get(f"{base}/fapi/v1/time")
            data = r.json()
            result["serverTime"] = {"status": r.status_code, "serverMs": data.get("serverTime")}
    except Exception as e:
        result["serverTime"] = {"error": str(e)}

    # Test 3: signed positionRisk request
    try:
        pos = await asyncio.wait_for(
            _pick_live_orphan_positions(key, secret, base),
            timeout=10.0,
        )
        result["positions"] = {
            "count": len(pos),
            "data": pos,
        }
    except Exception as e:
        result["positions"] = {
            "error": str(e),
            "type": type(e).__name__,
            "traceback": traceback.format_exc()[-500:],
        }

    return result


async def _exit_after_restart(delay: float = 0.9):
    await asyncio.sleep(delay)
    os._exit(0)


async def system_restart():
    """Spawn a detached uvicorn on the same port, then exit (Windows-friendly)."""
    backend_dir = Path(__file__).parent
    py = backend_dir / ".venv" / "Scripts" / "python.exe"
    if not py.exists():
        py = Path(sys.executable)
    port = str(_BACKEND_PORT)
    # Honor BACKEND_HOST so external devices (e.g. phone via Tailscale) can
    # reach the dashboard. Default stays 127.0.0.1 for backward compatibility.
    host = os.getenv("BACKEND_HOST", "127.0.0.1")
    cmd = [str(py), "-m", "uvicorn", "main:app", "--host", host, "--port", port]
    try:
        subprocess.Popen(
            cmd,
            cwd=str(backend_dir),
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS,
            close_fds=True,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not spawn backend: {_format_loop_error(e)}")
    asyncio.create_task(_exit_after_restart())
    return {"ok": True, "message": f"Restarting backend on port {port}…"}


def debug_env_status():
    key = os.getenv("BINANCE_API_KEY", "")
    secret = os.getenv("BINANCE_API_SECRET", "")
    return {
        "envPath": str(ENV_PATH),
        "binanceApiKeySet": bool(key),
        "binanceApiKeyLen": len(key),
        "binanceApiSecretSet": bool(secret),
        "binanceApiSecretLen": len(secret),
        "binanceTestnet": os.getenv("BINANCE_TESTNET", "false"),
    }


async def debug_binance_auth_check(symbol: str = "BTCUSDT"):
    key = os.getenv("BINANCE_API_KEY", "")
    secret = os.getenv("BINANCE_API_SECRET", "")
    base = _binance_base()
    if not key or not secret:
        return {
            "ok": False,
            "stage": "env",
            "message": "BINANCE_API_KEY or BINANCE_API_SECRET is missing",
        }
    try:
        mark = await fetch_mark_price(symbol)
    except Exception as e:
        return {
            "ok": False,
            "stage": "public_api",
            "message": str(e),
            "base": base,
            "symbol": symbol,
        }
    try:
        await _signed_request("GET", base, "/fapi/v2/account", key, secret, {})
        return {
            "ok": True,
            "stage": "signed_api",
            "message": "Signed endpoint accepted",
            "base": base,
            "symbol": symbol,
            "markPrice": mark,
        }
    except HTTPException as he:
        detail = str(he.detail)
        hint = "Unknown"
        if "-2015" in detail:
            hint = "Invalid key/IP/permission. Recheck key pair, whitelist IP, Futures permission, and testnet/mainnet mismatch."
        elif "-1021" in detail:
            hint = "Timestamp drift. Sync system time."
        elif "-2014" in detail:
            hint = "API-key format invalid."
        return {
            "ok": False,
            "stage": "signed_api",
            "message": detail,
            "hint": hint,
            "base": base,
            "symbol": symbol,
            "markPrice": mark,
        }


def get_risk_config():
    return {
        "killSwitch": RISK["kill_switch"],
        "maxNotionalUSDT": RISK["max_notional"],
        "maxLeverage": RISK["max_leverage"],
        "maxDailyLossUSDT": RISK["max_daily_loss"],
        "dailyRealizedPnlUSDT": DAILY_REALIZED_PNL,
    }


async def symbol_meta(symbol: str):
    sym = _normalize_symbol(symbol)
    filters = await _exchange_filters(sym)
    mark = await fetch_mark_price(sym)
    return {
        "symbol": sym,
        "stepSize": filters["stepSize"],
        "minQty": filters["minQty"],
        "tickSize": filters["tickSize"],
        "markPrice": mark,
        "minUsdtApprox": round(filters["minQty"] * mark, 4),
    }


def set_risk_config(cfg: RiskConfig):
    RISK["kill_switch"] = cfg.killSwitch
    RISK["max_notional"] = cfg.maxNotionalUSDT
    RISK["max_leverage"] = min(25, max(1, cfg.maxLeverage))
    RISK["max_daily_loss"] = cfg.maxDailyLossUSDT
    return {"ok": True}


async def analyze(req: AnalyzeRequest):
    trend = "Bullish" if req.orderBook.imbalance > 0.04 else "Bearish" if req.orderBook.imbalance < -0.04 else "Neutral"
    rsi = 68 if trend == "Bullish" else 42 if trend == "Bearish" else 51
    volume_signal = "Strong" if abs(req.orderBook.imbalance) > 0.05 else "Normal"
    setup = "Potential breakout with bid dominance" if trend == "Bullish" else "Sell pressure and weak bid absorption" if trend == "Bearish" else "Range behavior, wait for confirmation"
    rec = "LONG" if trend == "Bullish" else "SHORT" if trend == "Bearish" else "WAIT"
    warning = "Possible spoofing detected near wall zones" if req.orderBook.spoofingRisk == "HIGH" else None
    mm = await _market_momentum(req.symbol)
    if mm["momentumPct"] > 0.12 and mm["volumeRatio"] > 1.1:
        trend = "Bullish"
        rec = "LONG"
    elif mm["momentumPct"] < -0.12 and mm["volumeRatio"] > 1.1:
        trend = "Bearish"
        rec = "SHORT"
    if mm["divergence"] in ("BEARISH_DIVERGENCE", "BULLISH_DIVERGENCE"):
        setup += " | Divergence detected"
    return {
        "trend": trend,
        "rsi": rsi,
        "volumeSignal": volume_signal,
        "setup": setup,
        "recommendation": rec,
        "warning": warning,
        "momentumPct": round(mm["momentumPct"], 4),
        "volumeRatio": round(mm["volumeRatio"], 4),
        "divergence": mm["divergence"],
    }


async def openai_vision_analyze(symbol: str, image_data_url: str):
    api_key = os.getenv("OPENAI_API_KEY")
    model = os.getenv("OPENAI_VISION_MODEL", "gpt-4.1-mini")
    if not api_key:
        return {"pattern": "Fallback: no vision key configured", "confidence": 0.35, "notes": ["Set OPENAI_API_KEY", "Fallback mode"], "recommendation": "WAIT"}

    payload = {
        "model": model,
        "input": [{"role": "user", "content": [{"type": "input_text", "text": f"Symbol: {symbol}. Return JSON: pattern, confidence, notes[], recommendation LONG|SHORT|WAIT"}, {"type": "input_image", "image_url": image_data_url}]}],
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=30.0) as client:
        res = await client.post("https://api.openai.com/v1/responses", headers=headers, json=payload)
    if res.status_code >= 400:
        raise HTTPException(status_code=res.status_code, detail=res.text)
    data = res.json()
    text_out = data.get("output_text", "")
    try:
        return json.loads(text_out)
    except Exception:
        return {"pattern": "Unstructured vision output", "confidence": 0.4, "notes": [text_out[:160]], "recommendation": "WAIT"}


async def hermes_vision_analyze(symbol: str, image_data_url: str):
    # Hermes-compatible local endpoint (OpenAI-style chat/completions payload).
    prompt = (
        f"Symbol: {symbol}. "
        "Return JSON only with keys: pattern, confidence, notes, recommendation "
        "where recommendation is LONG|SHORT|WAIT."
    )
    payload = {
        "model": HERMES_MODEL,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": image_data_url}},
                ],
            }
        ],
        "temperature": 0.1,
    }
    async with httpx.AsyncClient(timeout=45.0) as client:
        res = await client.post(f"{HERMES_BASE_URL}/v1/chat/completions", json=payload)
    if res.status_code >= 400:
        raise HTTPException(status_code=res.status_code, detail=res.text)
    data = res.json()
    content = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
    # Extract JSON object if model returns wrapper text.
    m = re.search(r"\{.*\}", content, re.DOTALL)
    raw = m.group(0) if m else content
    try:
        parsed = json.loads(raw)
        return {
            "pattern": parsed.get("pattern", "Hermes vision"),
            "confidence": float(parsed.get("confidence", 0.45)),
            "notes": parsed.get("notes", []),
            "recommendation": parsed.get("recommendation", "WAIT"),
        }
    except Exception:
        return {
            "pattern": "Hermes unstructured output",
            "confidence": 0.4,
            "notes": [content[:180]],
            "recommendation": "WAIT",
        }


async def provider_vision_analyze(symbol: str, image_data_url: str):
    if AI_PROVIDER == "off":
        return {
            "pattern": "Vision provider disabled",
            "confidence": 0.2,
            "notes": ["AI_PROVIDER=off"],
            "recommendation": "WAIT",
        }
    if AI_PROVIDER == "hermes":
        return await hermes_vision_analyze(symbol, image_data_url)
    return await openai_vision_analyze(symbol, image_data_url)


async def analyze_vision(req: VisionAnalyzeRequest):
    if not req.imageDataUrl.startswith("data:image/"):
        raise HTTPException(status_code=400, detail="imageDataUrl must be a data URL")
    return await provider_vision_analyze(req.symbol, req.imageDataUrl)


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

    # Pre-fetch klines once and share across momentum + precision to avoid duplicate requests
    rows_1m = await _cached_klines(symbol, "1m", 150)  # 150 sufficient for EMA200 approx

    mm, pk, depth_out, execution, candle_ctx = await asyncio.gather(
        _market_momentum(symbol, _rows=rows_1m[:60]),
        _precision_signal_pack(symbol, limit=150, _rows_1m=rows_1m),
        _depth_orderflow(),
        _microstructure(),
        _candlestick_pattern_context(symbol),
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
    if final_signal in ("LONG", "SHORT") and bool(req_cfg.get("tradingviewEnabled", False)):
        try:
            from trading.tradingview_mcp import get_tv_mcp

            _tv_client = get_tv_mcp(req_cfg)
            _tv_res = await asyncio.to_thread(_tv_client.get_signal, symbol, final_signal, confidence)
            if _tv_res is not None and _tv_res.signal is not None and _tv_res.signal.value not in ("WAIT", "ERROR"):
                _tv_age = time.time() - _tv_res.timestamp
                _tv_stale = float(req_cfg.get("tvStaleEntrySec", 300) or 300)
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
                        notes.append(f"TV stale ({_tv_age:.0f}s > {_tv_stale:.0f}s) — no refresh")
                _tv_boost = _tv_client.confirm_signal(_tv_res, final_signal)
                _tv_strength = float((_tv_res.metadata or {}).get("strength", 0.0) or 0.0)
                if _tv_boost <= -900.0:
                    notes.append(
                        f"TV hard conflict BLOCK: TV={_tv_res.signal.value} vs {final_signal} strength={_tv_strength:.2f}"
                    )
                    final_signal = "WAIT"
                    confidence = min(confidence, 0.40)
                elif _tv_boost != 0.0:
                    notes.append(
                        f"TV {'align' if _tv_boost > 0 else 'conflict'} "
                        f"{_tv_res.signal.value} strength={_tv_strength:.2f} ({_tv_age:.0f}s old)"
                    )
        except Exception:
            pass

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
    _INTEL_CACHE[symbol] = (time.time(), result)
    # Limit cache size to 10 symbols max
    if len(_INTEL_CACHE) > 10:
        oldest = min(_INTEL_CACHE, key=lambda k: _INTEL_CACHE[k][0])
        del _INTEL_CACHE[oldest]
    return result


def risk_alerts(symbol: str):
    alerts = ["ควรตรวจข่าวแรงก่อนเพิ่มเลเวอเรจ"]
    if symbol.upper().endswith("USDT"):
        alerts.append("Funding rate อาจแกว่งแรงช่วงเปลี่ยนเซสชัน")
    if RISK["kill_switch"]:
        alerts.append("Kill-switch เปิดอยู่: ระบบบล็อกการเทรดอัตโนมัติและเทรดจริง")
    return {"alerts": alerts}


def _extract_first_float(text: str):
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    return float(match.group(1)) if match else None


def parse_strategy(req: StrategyParseRequest):
    text = req.command.lower()
    side = "LONG" if "long" in text else "SHORT" if "short" in text else "LONG"
    if "breakout above" in text:
        trigger = "BREAKOUT_ABOVE"
        price = _extract_first_float(text.split("breakout above", 1)[1])
    elif "breakdown below" in text:
        trigger = "BREAKDOWN_BELOW"
        price = _extract_first_float(text.split("breakdown below", 1)[1])
    else:
        trigger = "MARKET_NOW"
        price = None
    qty = _extract_first_float(text.split("qty", 1)[1]) if "qty" in text else 0.01
    return {"command": req.command, "symbol": _normalize_symbol(req.symbol), "triggerType": trigger, "triggerPrice": price, "side": side, "quantity": qty or 0.01, "takeProfitPct": DEFAULT_TP_PCT, "stopLossPct": DEFAULT_SL_PCT, "trailingStopPct": 0.5, "active": True}


async def fetch_mark_price(symbol: str):
    symbol = _normalize_symbol(symbol)
    res = await _data_get(f"/fapi/v1/premiumIndex?symbol={symbol}")
    if res.status_code >= 400:
        raise HTTPException(status_code=res.status_code, detail=res.text)
    return float(res.json()["markPrice"])


async def _fetch_market_prices_batch(symbols: list[str]) -> dict[str, float]:
    """Fetch current mark prices for multiple symbols at once."""
    if not symbols:
        return {}
    
    prices = {}
    try:
        # Use Binance batch ticker endpoint for efficiency
        res = await _data_get("/fapi/v1/ticker/price")
        if res.status_code >= 400:
            return prices
        
        ticker_data = res.json()
        if isinstance(ticker_data, list):
            symbol_set = {s.upper() for s in symbols}
            for ticker in ticker_data:
                if isinstance(ticker, dict):
                    sym = str(ticker.get("symbol", "")).upper()
                    if sym in symbol_set:
                        prices[sym] = float(ticker.get("price", 0.0) or 0.0)
    except Exception:
        # Fallback to individual fetches if batch fails
        for symbol in symbols:
            try:
                price = await fetch_mark_price(symbol)
                prices[symbol.upper()] = price
            except Exception:
                continue
    
    return prices


async def _market_momentum(symbol: str, interval: str = "1m", limit: int = 60, _rows: list | None = None):
    symbol = _normalize_symbol(symbol)
    rows = _rows if _rows is not None else await _cached_klines(symbol, interval, limit)
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
    b = _fapi_public_data_base()
    # Use pre-fetched 1m rows if provided, otherwise fetch all three in parallel
    if _rows_1m is not None:
        r1 = _rows_1m
        r5, r15 = await asyncio.gather(
            _cached_klines(symbol, "5m", 100),
            _cached_klines(symbol, "15m", 60),
        )
    else:
        r1, r5, r15 = await asyncio.gather(
            _cached_klines(symbol, "1m", limit),
            _cached_klines(symbol, "5m", 100),
            _cached_klines(symbol, "15m", 60),
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
            _cached_klines(symbol, "5m", 10),
            _cached_klines(symbol, "15m", 10),
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


def _last_decision_intel(symbol: str | None = None, max_age_sec: int | None = None) -> dict | None:
    # Per-symbol lookup: check lastDecisions dict first
    want = str(symbol or "").upper().strip() if symbol else ""
    if want:
        per_sym = AUTO_TRADE.get("lastDecisions")
        if isinstance(per_sym, dict):
            ld = per_sym.get(want)
            if isinstance(ld, dict):
                intel = ld.get("intel") if "intel" in ld else ld
                if isinstance(intel, dict):
                    if max_age_sec is not None:
                        try:
                            ts = int(ld.get("ts", 0) or intel.get("ts", 0) or 0)
                        except Exception:
                            ts = 0
                        if ts <= 0 or int(time.time()) - ts > int(max_age_sec):
                            return None
                    return intel
    # Fallback to global lastDecision (for backward compatibility)
    ld = AUTO_TRADE.get("lastDecision")
    if not isinstance(ld, dict):
        return None
    intel = ld.get("intel") if "intel" in ld else ld
    if not isinstance(intel, dict):
        return None
    if want:
        got = str(intel.get("symbol") or ld.get("symbol") or "").upper().strip()
        if got and want and got != want:
            return None
    if max_age_sec is not None:
        try:
            ts = int(ld.get("ts", 0) or intel.get("ts", 0) or 0)
        except Exception:
            ts = 0
        if ts <= 0 or int(time.time()) - ts > int(max_age_sec):
            return None
    return intel


def _entry_snapshot_from_intel(symbol: str | None, side: str | None, intel: dict | None = None) -> dict:
    intel = intel if isinstance(intel, dict) else _last_decision_intel(symbol)
    if not isinstance(intel, dict):
        intel = {}
    candles = intel.get("candles") if isinstance(intel.get("candles"), dict) else {}
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    snap = {
        "entrySymbol": str(symbol or intel.get("symbol") or "").upper().strip(),
        "entrySide": str(side or intel.get("signal") or "").upper().strip(),
        "patternTags": candles.get("tags", []),
        "patternBias": float(candles.get("bias", 0.0) or 0.0),
        "patternScore": float(candles.get("score", 0.0) or 0.0),
        "entryConfidence": float(intel.get("confidence", 0.0) or 0.0),
        "entryScore": float(intel.get("score", intel.get("weightedScore", 0.0)) or 0.0),
        "entrySpreadBps": float(ex.get("spreadBps", 0.0) or 0.0),
        "entryMomentumPct": float(ex.get("momentumPct", 0.0) or 0.0),
        "entryDecisionAt": int(time.time()),
    }
    # Capture TV data at entry time for post-trade analysis
    try:
        _tv = intel.get("tv") if isinstance(intel.get("tv"), dict) else {}
        if _tv:
            snap["tvSignal"] = str(_tv.get("signal", "") or "")
            snap["tvConfidence"] = float(_tv.get("confidence", 0.0) or 0.0)
            snap["tvStrength"] = float(_tv.get("strength", 0.0) or 0.0)
    except Exception:
        pass
    return snap


def _entry_snapshot_for_position(symbol: str, side: str) -> dict:
    sym = str(symbol or "").upper().strip()
    sd = str(side or "").upper().strip()
    locks = AUTO_TRADE.get("liveProfitLocks")
    if isinstance(locks, dict):
        lock = locks.get(_live_lock_key(sym, sd))
        if isinstance(lock, dict) and isinstance(lock.get("entrySnapshot"), dict):
            return dict(lock.get("entrySnapshot") or {})
    return _entry_snapshot_from_intel(sym, sd, _last_decision_intel(sym, max_age_sec=30))


def _last_decision_pattern_metrics(symbol: str | None = None) -> tuple[list[str], float, float]:
    intel = _last_decision_intel(symbol)
    if not isinstance(intel, dict):
        return [], 0.0, 0.0
    candles = intel.get("candles")
    if not isinstance(candles, dict):
        return [], 0.0, 0.0
    return candles.get("tags", []), candles.get("bias", 0.0), candles.get("score", 0.0)


def _last_decision_entry_metrics(symbol: str | None = None) -> dict:
    intel = _last_decision_intel(symbol)
    if not isinstance(intel, dict):
        return {}
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    return {
        "confidence": float(intel.get("confidence", 0.0) or 0.0),
        "score": float(intel.get("score", intel.get("weightedScore", 0.0)) or 0.0),
        "spreadBps": float(ex.get("spreadBps", 0.0) or 0.0),
        "momentumPct": float(ex.get("momentumPct", 0.0) or 0.0),
    }


def _clamp_float(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


def _profit_lock_policy(cfg: dict, peak_usdt: float, symbol: str | None = None, intel: dict | None = None) -> dict:
    """Compute the per-symbol profit-lock policy.

    When ``symbol`` + ``intel`` are provided the policy is scaled by the
    per-symbol volatility tier (volatile symbols lock later and let winners
    run; stable majors lock earlier to bank small consistent gains).
    """
    peak = max(0.0, float(peak_usdt or 0.0))
    # Per-symbol override via _effective_tp_sl.
    if symbol and intel is not None:
        eff = _effective_tp_sl(symbol, cfg, intel)
        trigger = max(0.05, float(eff.get("profitLockTriggerUsdt", 0.35)))
        keep = max(0.02, float(eff.get("profitLockKeepUsdt", 0.15)))
        max_giveback = max(0.01, float(eff.get("profitLockMaxGivebackUsdt", 0.22)))
    elif "profitLockTriggerUsdt" in cfg:
        trigger = max(0.05, float(cfg.get("profitLockTriggerUsdt", 0.35) or 0.35))
        keep = max(0.02, float(cfg.get("profitLockKeepUsdt", 0.15) or 0.15))
        max_giveback = max(0.01, float(cfg.get("profitLockMaxGivebackUsdt", 0.22) or 0.22))
    else:
        # Fallback: derive trigger from TP target minimum.
        if symbol and intel is not None:
            eff = _effective_tp_sl(symbol, cfg, intel)
            trigger = max(0.20, float(eff.get("tpTargetMinUsdt", 0.55)) * 0.5)
        else:
            trigger = max(0.20, float(cfg.get("tpTargetMinUsdt", 0.55) or 0.55) * 0.5)
        keep = max(0.02, float(cfg.get("profitLockKeepUsdt", 0.15) or 0.15))
        max_giveback = max(0.01, float(cfg.get("profitLockMaxGivebackUsdt", 0.22) or 0.22))
    lock_usdt = 0.0
    if peak >= trigger:
        lock_usdt = max(0.02, keep, peak - max_giveback, peak * 0.35)
        lock_usdt = min(lock_usdt, peak * 0.98)
    return {
        "trigger": round(float(trigger), 6),
        "keep": round(float(keep), 6),
        "maxGiveback": round(float(max_giveback), 6),
        "lockUsdt": round(float(lock_usdt), 6),
    }


def _trade_reward_components(trade: dict, cfg: dict) -> dict:
    pnl = float((trade or {}).get("pnl", 0.0) or 0.0)
    reason = str((trade or {}).get("reason", "") or "").upper()
    side = str((trade or {}).get("side", "") or "").upper()
    sign = 1.0 if side == "LONG" else (-1.0 if side == "SHORT" else 0.0)
    last_metrics = _last_decision_entry_metrics()
    conf = float((trade or {}).get("entryConfidence", last_metrics.get("confidence", 0.0)) or 0.0)
    min_conf = float(cfg.get("minConfidence", 0.65) or 0.65)
    spread_bps = float((trade or {}).get("entrySpreadBps", last_metrics.get("spreadBps", 0.0)) or 0.0)
    max_spread = max(1.0, float(cfg.get("maxSpreadBps", 20.0) or 20.0))
    pattern_bias = float((trade or {}).get("patternBias", 0.0) or 0.0)
    pattern_score = float((trade or {}).get("patternScore", 0.0) or 0.0)
    entry_delta = _clamp_float((conf - min_conf) / 0.25, -1.0, 1.0) * float(cfg.get("learningRewardEntryTiming", 0.25) or 0.25)
    spread_delta = _clamp_float((max_spread - spread_bps) / max_spread, -1.0, 1.0) * 0.12
    pattern_alignment = _clamp_float(sign * pattern_bias, -1.0, 1.0)
    pattern_delta = _clamp_float(pattern_alignment * 0.18 + (pattern_score / 500.0), -0.28, 0.28)
    tp_pct = float(cfg.get("takeProfitPct", 1.2) or 1.2)
    sl_pct = max(0.01, float(cfg.get("stopLossPct", 0.8) or 0.8))
    min_rr = float(cfg.get("minRiskRewardRatio", 1.35) or 1.35)
    rr = tp_pct / sl_pct
    rr_delta = 0.12 if rr >= min_rr else -0.22
    edge_delta = 0.0
    try:
        qty = abs(float((trade or {}).get("qty", 0.0) or 0.0))
        entry = abs(float((trade or {}).get("entry", 0.0) or 0.0))
        notional = qty * entry
        if notional > 0:
            _gross, cost, net = _estimate_trade_edge_usdt(notional, tp_pct, float(cfg.get("maxSlippageBps", 20.0) or 20.0))
            min_net = _fee_edge_min_net_usdt(cfg, cost, notional)
            edge_delta = 0.10 if net > min_net else -0.16
    except Exception:
        edge_delta = 0.0
    drawdown_delta = 0.0
    try:
        max_adverse = abs(float((trade or {}).get("maxAdversePct", 0.0) or 0.0))
        if max_adverse > 0:
            drawdown_delta = -_clamp_float(max_adverse / max(sl_pct, 0.01), 0.0, 1.0) * 0.22
    except Exception:
        drawdown_delta = 0.0
    time_delta = 0.0
    opened = int(float((trade or {}).get("openedAt", 0) or 0))
    closed = int(float((trade or {}).get("closedAt", 0) or 0))
    if opened > 0 and closed >= opened:
        minutes = (closed - opened) / 60.0
        if pnl > 0 and minutes <= float(cfg.get("learningFastWinMinutes", 45) or 45):
            time_delta += 0.12
        elif pnl < 0 and minutes <= 8:
            time_delta -= 0.14
        elif pnl < 0 and minutes >= 90:
            time_delta -= 0.08
    guard_delta = 0.0
    if pnl > 0 and ("TP" in reason or "TARGET_MAX" in reason):
        guard_delta += float(cfg.get("learningRewardTpHitBase", 0.2) or 0.2)
        guard_delta += float(cfg.get("learningRewardTpScalePerUsdt", 0.35) or 0.35) * min(abs(pnl), 3.0) / 3.0
    if pnl > 0 and ("PROFIT" in reason or "GUARD" in reason or "TRAIL" in reason):
        guard_delta += float(cfg.get("learningRewardHoldWinner", 0.2) or 0.2)
    if pnl > 0 and "FLIP" in reason:
        guard_delta += float(cfg.get("learningRewardFlipGood", 0.1) or 0.1)
    if pnl < 0 and ("SL" in reason or "STOP" in reason):
        guard_delta -= float(cfg.get("learningPenaltyEarlySl", 0.35) or 0.35)
    if pnl < 0 and ("SIGNAL_WAIT" in reason or "LOW_CONF" in reason):
        guard_delta -= float(cfg.get("learningPenaltyMemoryMiss", 0.15) or 0.15)
    if pnl < 0 and "LIVE_CUT_LOSING_SIDE" in reason:
        guard_delta -= 0.20
    total = entry_delta + spread_delta + pattern_delta + rr_delta + edge_delta + drawdown_delta + time_delta + guard_delta
    cap = max(0.1, float(cfg.get("learningBehaviorDeltaCap", 2.5) or 2.5))
    return {
        "entryQuality": round(entry_delta, 6),
        "spreadQuality": round(spread_delta, 6),
        "patternAlignment": round(pattern_delta, 6),
        "riskReward": round(rr_delta, 6),
        "feeEdge": round(edge_delta, 6),
        "drawdown": round(drawdown_delta, 6),
        "timeToProfit": round(time_delta, 6),
        "guardDiscipline": round(guard_delta, 6),
        "total": round(_clamp_float(total, -cap, cap), 6),
    }


def _guardrails(mark_price: float, quantity: float, leverage: int):
    if RISK["kill_switch"]:
        raise HTTPException(status_code=403, detail="Kill-switch enabled")
    if DAILY_REALIZED_PNL <= -abs(RISK["max_daily_loss"]):
        raise HTTPException(status_code=403, detail="Max daily loss reached")
    if leverage > RISK["max_leverage"]:
        raise HTTPException(status_code=403, detail=f"Leverage {leverage} > limit {RISK['max_leverage']}")
    notional = mark_price * quantity
    if notional > RISK["max_notional"]:
        raise HTTPException(status_code=403, detail=f"Notional {notional:.2f} > limit {RISK['max_notional']}")


async def strategy_evaluate(plan: StrategyPlan):
    mark = await fetch_mark_price(plan.symbol)
    trigger_hit = (
        plan.triggerType == "MARKET_NOW"
        or (plan.triggerType == "BREAKOUT_ABOVE" and plan.triggerPrice is not None and mark >= plan.triggerPrice)
        or (plan.triggerType == "BREAKDOWN_BELOW" and plan.triggerPrice is not None and mark <= plan.triggerPrice)
    )
    if not trigger_hit:
        return {"status": "WAIT", "reason": f"Trigger not hit yet. Mark={mark}", "risk": "No action"}
    action = "LONG" if plan.side == "LONG" else "SHORT"
    tp = mark * (1 + plan.takeProfitPct / 100) if action == "LONG" else mark * (1 - plan.takeProfitPct / 100)
    sl = mark * (1 - plan.stopLossPct / 100) if action == "LONG" else mark * (1 + plan.stopLossPct / 100)
    return {"status": "TRIGGERED", "reason": f"Entry condition matched at mark={mark}", "action": action, "risk": f"TP={tp:.2f}, SL={sl:.2f}, trailing={plan.trailingStopPct}%"}


async def _signed_request(method: str, base: str, endpoint: str, key: str, secret: str, params: dict):
    params = dict(params)
    diagnostic = _signed_request_diagnostic(base, endpoint, key, params)
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            st = await client.get(f"{base}/fapi/v1/time")
        server_ms = int(st.json().get("serverTime", int(time.time() * 1000)))
    except Exception:
        server_ms = int(time.time() * 1000)
    params["timestamp"] = server_ms
    params["recvWindow"] = BINANCE_RECV_WINDOW_MS
    query = "&".join(f"{k}={v}" for k, v in params.items())
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    signed = f"{query}&signature={sig}"
    headers = {"X-MBX-APIKEY": key}
    url = f"{base}{endpoint}?{signed}"
    res: httpx.Response | None = None
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            client = _BINANCE_HTTP
            if client is not None:
                try:
                    if method == "POST":
                        res = await client.post(url, headers=headers)
                    else:
                        res = await client.get(url, headers=headers)
                except (httpx.RemoteProtocolError, httpx.LocalProtocolError, httpx.ReadError, httpx.ConnectError, httpx.PoolTimeout, httpx.ConnectTimeout, httpx.RequestError):
                    async with httpx.AsyncClient(timeout=15.0) as c2:
                        if method == "POST":
                            res = await c2.post(url, headers=headers)
                        else:
                            res = await c2.get(url, headers=headers)
            else:
                async with httpx.AsyncClient(timeout=15.0) as c2:
                    if method == "POST":
                        res = await c2.post(url, headers=headers)
                    else:
                        res = await c2.get(url, headers=headers)
            break
        except Exception as e:
            last_err = e
            if attempt >= 2 or not _is_retryable_http_exc(e):
                raise
            await asyncio.sleep(0.35 * (attempt + 1))
    if res is None:
        if last_err is not None:
            raise last_err
        raise HTTPException(status_code=503, detail="signed request failed: no response")
    if res.status_code >= 400:
        raise HTTPException(
            status_code=res.status_code,
            detail={
                "message": res.text,
                "binanceRequest": diagnostic,
            },
        )
    return res.json()



async def _exchange_filters(symbol: str):
    sym_upper = str(symbol or "").upper().strip()
    now_ts = time.time()
    cached = _EXCHANGE_FILTERS_CACHE.get(sym_upper)
    if cached and now_ts - cached[0] < _EXCHANGE_FILTERS_CACHE_TTL:
        return cached[1]

    def _build_payload(s: dict) -> dict:
        filters_map = {f["filterType"]: f for f in s.get("filters", [])}
        return {
            "stepSize": float(filters_map.get("LOT_SIZE", {}).get("stepSize", "0")),
            "minQty": float(filters_map.get("LOT_SIZE", {}).get("minQty", "0")),
            "tickSize": float(filters_map.get("PRICE_FILTER", {}).get("tickSize", "0")),
            "minNotional": float(filters_map.get("MIN_NOTIONAL", {}).get("notional", filters_map.get("NOTIONAL", {}).get("minNotional", "0"))),
            "maxLeverage": int(os.getenv("MAX_LEVERAGE_DEFAULT", "20")),
            "maxQty": float(filters_map.get("LOT_SIZE", {}).get("maxQty", "0") or 0),
        }

    # Fallback 1: single-symbol endpoint
    payload = None
    try:
        res = await _data_get(f"/fapi/v1/exchangeInfo?symbol={sym_upper}")
        if res.status_code < 400:
            data = res.json()
            symbols = data.get("symbols", []) if isinstance(data, dict) else []
            if symbols:
                s = symbols[0]
                if s.get("status") != "TRADING":
                    raise HTTPException(status_code=400, detail=f"Symbol not tradable: {sym_upper}")
                payload = _build_payload(s)
                _EXCHANGE_FILTERS_CACHE[sym_upper] = (now_ts, payload)
        elif res.status_code == 400:
            raise HTTPException(status_code=400, detail=f"Invalid Binance symbol: {sym_upper}")
    except HTTPException:
        raise
    except (MemoryError, Exception) as e:
        print(f"[Exchange Filters] single-symbol fetch failed for {sym_upper}: {type(e).__name__}: {e}")

    # Fallback 2: full exchangeInfo list
    if payload is None:
        try:
            res = await _data_get("/fapi/v1/exchangeInfo")
            if res.status_code >= 400:
                if cached:
                    return cached[1]
                raise HTTPException(status_code=res.status_code, detail=res.text)
            data = res.json()
            symbols = data.get("symbols", []) if isinstance(data, dict) else []
            for s in symbols:
                sym_name = str(s.get("symbol") or "").upper().strip() or sym_upper
                try:
                    entry = _build_payload(s)
                    _EXCHANGE_FILTERS_CACHE[sym_name] = (now_ts, entry)
                except Exception:
                    continue
            payload = _EXCHANGE_FILTERS_CACHE.get(sym_upper, (0, None))[1]
        except HTTPException:
            raise
        except (MemoryError, Exception) as e:
            print(f"[Exchange Filters] full-list fetch failed for {sym_upper}: {type(e).__name__}: {e}")

    # Fallback 3: serve stale cache
    if payload is None:
        if cached:
            print(f"[Exchange Filters] serving stale cache for {sym_upper} after fetch failure")
            return cached[1]
        raise HTTPException(status_code=503, detail=f"exchangeInfo unavailable for {sym_upper}")

    return payload



def _qty_retry_candidates(qty: float, step_str: str, qty_precision: int, min_qty: float):
    # Some futures symbols reject otherwise valid step-size quantities unless precision is coarser.
    dec = Decimal(str(qty))
    cands: list[str] = []
    base = _format_qty_by_step(qty, step_str)
    cands.append(base)
    start_p = max(0, min(8, int(qty_precision)))
    for p in range(start_p, -1, -1):
        q = dec.quantize(Decimal(10) ** -p, rounding=ROUND_DOWN)
        s = format(q, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        if not s:
            continue
        try:
            fv = float(s)
        except Exception:
            continue
        if fv < float(min_qty):
            continue
        cands.append(s)
    # stable unique
    out: list[str] = []
    seen = set()
    for x in cands:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


async def _um_client_position_risk(client, symbol: str | None = None):
    timeout_sec = max(2.0, float(os.getenv("BINANCE_ACCOUNT_TIMEOUT_SEC", "5.0") or 5.0))

    def _call():
        if symbol:
            return client.get_position_risk(symbol=symbol)
        return client.get_position_risk()

    return await asyncio.wait_for(asyncio.to_thread(_call), timeout=timeout_sec)


async def _current_position_amount(symbol: str, key: str | None, secret: str | None, base: str):
    if not key or not secret:
        return 0.0
    client = _get_um_client(key, secret, base)
    if client:
        pos = await _um_client_position_risk(client, symbol=symbol)
    else:
        pos = await _signed_request("GET", base, "/fapi/v2/positionRisk", key, secret, {"symbol": symbol})
    if isinstance(pos, list):
        # Hedge mode can return multiple rows (LONG/SHORT/BOTH). Use net amount.
        return float(sum(float(p.get("positionAmt", 0) or 0) for p in pos))
    if isinstance(pos, dict):
        return float(pos.get("positionAmt", 0) or 0)
    return 0.0


async def _position_side_state(symbol: str, key: str | None, secret: str | None, base: str):
    if not key or not secret:
        return {"net": 0.0, "long": 0.0, "short": 0.0, "gross": 0.0}
    client = _get_um_client(key, secret, base)
    if client:
        pos = await _um_client_position_risk(client, symbol=symbol)
    else:
        pos = await _signed_request("GET", base, "/fapi/v2/positionRisk", key, secret, {"symbol": symbol})
    rows = pos if isinstance(pos, list) else ([pos] if isinstance(pos, dict) else [])
    net = 0.0
    long_qty = 0.0
    short_qty = 0.0
    for p in rows:
        amt = float(p.get("positionAmt", 0) or 0)
        net += amt
        if amt > 0:
            long_qty += amt
        elif amt < 0:
            short_qty += abs(amt)
    return {"net": net, "long": long_qty, "short": short_qty, "gross": long_qty + short_qty}


def _open_side_from_position_state(pst: dict) -> str:
    long_qty = float((pst or {}).get("long", 0.0) or 0.0)
    short_qty = float((pst or {}).get("short", 0.0) or 0.0)
    if long_qty > 0 and short_qty > 0:
        return "HEDGE"
    if long_qty > 0:
        return "LONG"
    if short_qty > 0:
        return "SHORT"
    net = float((pst or {}).get("net", 0.0) or 0.0)
    if net > 0:
        return "LONG"
    if net < 0:
        return "SHORT"
    return "FLAT"


async def _open_positions_count(key: str | None, secret: str | None, base: str) -> int:
    if not key or not secret:
        return 0
    client = _get_um_client(key, secret, base)
    if client:
        pos = await _um_client_position_risk(client)
    else:
        pos = await _signed_request("GET", base, "/fapi/v2/positionRisk", key, secret, {})
    rows = pos if isinstance(pos, list) else ([pos] if isinstance(pos, dict) else [])
    cnt = 0
    for p in rows:
        try:
            if abs(float(p.get("positionAmt", 0) or 0)) > 0:
                cnt += 1
        except Exception:
            continue
    return cnt


async def _pick_live_orphan_positions(
    key: str | None, secret: str | None, base: str
) -> list[dict]:
    """Delegate to live_guardian's unified implementation (shared 25s cache)."""
    from trading.live_guardian import _pick_live_orphan_positions as _guardian_pick
    return await _guardian_pick(key, secret, base)


_ACCOUNT_CACHE: dict = {"ts": 0.0, "key": None, "data": None}
_ACCOUNT_CACHE_TTL = 3.0


async def _get_account_cached(key, secret, base, ttl: float = _ACCOUNT_CACHE_TTL):
    now = time.time()
    if (
        _ACCOUNT_CACHE["data"] is not None
        and (now - _ACCOUNT_CACHE["ts"]) < ttl
        and _ACCOUNT_CACHE["key"] == (bool(key), bool(secret))
    ):
        return _ACCOUNT_CACHE["data"]
    data = await _signed_request("GET", base, "/fapi/v2/account", key, secret, {})
    _ACCOUNT_CACHE["ts"] = now
    _ACCOUNT_CACHE["key"] = (bool(key), bool(secret))
    _ACCOUNT_CACHE["data"] = data
    return data


def _has_live_open_positions_sync(key: str | None, secret: str | None, base: str) -> bool:
    """Fast sync check used by stop/reset paths."""
    if not key or not secret:
        return False
    try:
        client = _get_um_client(key, secret, base)
        if client:
            pos = client.get_position_risk()
        else:
            return False
        rows = pos if isinstance(pos, list) else ([pos] if isinstance(pos, dict) else [])
        for p in rows:
            try:
                amt = float(p.get("positionAmt", 0) or 0)
            except Exception:
                amt = 0.0
            if abs(amt) > 0.0:
                return True
    except Exception:
        return False
    return False


async def _is_hedge_mode(key: str | None, secret: str | None, base: str):
    if not key or not secret:
        return False
    try:
        data = await _signed_request("GET", base, "/fapi/v1/positionSide/dual", key, secret, {})
        # Binance may return bool or string
        v = data.get("dualSidePosition", False) if isinstance(data, dict) else False
        if isinstance(v, str):
            return v.lower() == "true"
        return bool(v)
    except Exception:
        return False


def _format_loop_error(err: BaseException) -> str:
    """Readable message for autotrade loop logs (many exceptions have empty str(err))."""
    if isinstance(err, HTTPException):
        d = err.detail
        if isinstance(d, str) and d.strip():
            return d.strip()
        if isinstance(d, dict):
            m = d.get("message") or d.get("msg")
            if m:
                return str(m)
            try:
                return json.dumps(d, ensure_ascii=False)[:400]
            except Exception:
                return repr(d)
        return f"HTTPException {err.status_code}: {d!r}"
    if isinstance(err, httpx.HTTPStatusError):
        body = (err.response.text or "")[:240].replace("\n", " ")
        try:
            url = err.request.url
        except RuntimeError:
            url = ""
        return f"HTTP {err.response.status_code} {url!s} {body}".strip()
    if isinstance(err, httpx.RequestError):
        try:
            req = err.request
        except RuntimeError:
            req = None
        u = getattr(req, "url", None)
        return f"RequestError {u!s}: {err!s}".strip() or type(err).__name__
    s = str(err).strip()
    if s:
        return s
    return f"{type(err).__name__} (no message)"


def _is_fapi_agreement_error(err_msg: str) -> bool:
    txt = str(err_msg or "").lower()
    return ("-4411" in txt) or ("agreement contract fapi" in txt) or ("tradfi-perps" in txt)


def _fapi_agreement_locked_symbols() -> set[str]:
    now = int(time.time())
    locks = AUTO_TRADE.get("perfLocks")
    if not isinstance(locks, dict):
        return set()
    out: set[str] = set()
    for symbol, lock in list(locks.items()):
        if not isinstance(lock, dict):
            continue
        sym = str(symbol or "").upper().strip()
        until = int(lock.get("until", 0) or 0)
        if until <= now:
            continue
        if str(lock.get("reason", "") or "") == "fapi_agreement" or str(lock.get("error", "") or "") == "-4411":
            out.add(sym)
    return out


def _active_fapi_agreement_locks() -> dict:
    now = int(time.time())
    locks = AUTO_TRADE.get("perfLocks")
    if not isinstance(locks, dict):
        return {}
    out: dict = {}
    for symbol, lock in locks.items():
        if not isinstance(lock, dict):
            continue
        sym = str(symbol or "").upper().strip()
        until = int(lock.get("until", 0) or 0)
        if not sym or until <= now:
            continue
        if str(lock.get("reason", "") or "") == "fapi_agreement" or str(lock.get("error", "") or "") == "-4411":
            out[sym] = dict(lock)
    return out


def _scan_pick_symbols_from_logs(messages: list[str]) -> list[str]:
    out: list[str] = []
    for msg in messages or []:
        text = str(msg or "")
        m = re.search(r"\bSCAN pick:\s*([A-Z0-9_:-]{3,30})", text, flags=re.IGNORECASE)
        if not m:
            continue
        sym = str(m.group(1) or "").upper().strip()
        if sym:
            out.append(sym)
    return out


def _restore_fapi_agreement_locks_from_logs(cfg: dict, messages: list[str]) -> dict:
    symbols = _fapi_agreement_symbols_from_logs(messages)
    if not symbols:
        return {"applied": False, "symbols": []}
    now = int(time.time())
    locks = AUTO_TRADE.get("perfLocks")
    if not isinstance(locks, dict):
        locks = {}
    lock_min = max(30, int((cfg or {}).get("fapiAgreementSymbolLockMinutes", 360) or 360))
    changed: list[str] = []
    until = now + (lock_min * 60)
    for sym in symbols:
        current = locks.get(sym) if isinstance(locks.get(sym), dict) else {}
        current_until = int(current.get("until", 0) or 0)
        if current_until > now and (
            str(current.get("reason", "") or "") == "fapi_agreement"
            or str(current.get("error", "") or "") == "-4411"
        ):
            continue
        locks[sym] = {
            "until": until,
            "at": now,
            "reason": "fapi_agreement",
            "error": "-4411",
            "source": "supervisor_log_restore",
        }
        changed.append(sym)
    if not changed:
        return {"applied": False, "symbols": symbols, "alreadyActive": True}
    AUTO_TRADE["perfLocks"] = locks
    _persist_autotrade_snapshot()
    return {"applied": True, "symbols": changed, "until": until, "minutes": lock_min}


def _maybe_auto_heal_scan_config_drift(cfg: dict) -> dict:
    if not isinstance(cfg, dict) or not bool(cfg.get("supervisorAutoHealScanDriftEnabled", True)):
        return {"applied": False}
    symbol = str(cfg.get("symbol", "") or "").upper().strip()
    primary = str(cfg.get("primarySymbol", "") or "").upper().strip()
    if not symbol or symbol in {"AUTO", "SCAN"} or bool(cfg.get("marketScan")):
        return {"applied": False}
    if bool(cfg.get("orphanAutoAdoptForceSingleSymbol", False)):
        return {"applied": False, "reason": "force_single_symbol"}
    before = {
        "symbol": cfg.get("symbol"),
        "marketScan": cfg.get("marketScan"),
        "whitelistSymbols": cfg.get("whitelistSymbols"),
    }
    if not primary:
        cfg["primarySymbol"] = symbol
    cfg["symbol"] = "AUTO"
    cfg["marketScan"] = True
    wl = _parse_symbol_whitelist(cfg.get("whitelistSymbols"))
    if len(wl) <= 1:
        cfg["whitelistSymbols"] = []
    AUTO_TRADE["config"] = copy.deepcopy(cfg)
    _persist_autotrade_snapshot()
    return {
        "applied": True,
        "reason": "live_scan_config_drift",
        "symbol": symbol,
        "changes": {
            "before": before,
            "after": {
                "symbol": cfg.get("symbol"),
                "marketScan": cfg.get("marketScan"),
                "whitelistSymbols": cfg.get("whitelistSymbols"),
            },
        },
    }


def _fapi_agreement_symbols_from_logs(messages: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for msg in messages or []:
        text = str(msg or "")
        if "-4411" not in text and "Futures/Perps agreement required" not in text:
            continue
        for match in re.finditer(r"\blocked\s+([A-Z0-9]{2,20}USDT)\b", text, re.IGNORECASE):
            sym = str(match.group(1) or "").upper()
            if sym and sym not in seen:
                seen.add(sym)
                out.append(sym)
    return out


def _is_binance_permission_error(err_msg: str) -> bool:
    txt = str(err_msg or "").lower()
    return ("-2015" in txt) or ("invalid api-key, ip, or permissions" in txt) or ("permissions for action" in txt)


def _infra_auth_incident_from_messages(messages: list[str] | None, *, skip_code: str = "", skip_msg: str = "") -> dict:
    rows = [str(msg or "") for msg in (messages or [])]
    auth_msgs = [
        msg
        for msg in rows
        if _is_binance_permission_error(msg)
        or "binance api key/ip/permission" in msg.lower()
        or "whitelist ip" in msg.lower()
    ]
    if str(skip_code or "") == "binance_permission_required" or _is_binance_permission_error(skip_msg):
        auth_msgs.insert(0, str(skip_msg or skip_code or "binance_permission_required"))
    if not auth_msgs:
        return {"active": False}
    sample = auth_msgs[0]
    return {
        "active": True,
        "category": "infra_auth",
        "title": "Binance API/IP permission incident",
        "detail": sample,
        "count": len(auth_msgs),
        "operatorAction": "ให้ผู้ใช้ตรวจ Binance API permission, Futures permission, whitelist IP และ mainnet/testnet ให้ถูกต้องก่อนประเมิน strategy",
    }


def _autotrade_log(msg: str):
    AUTO_TRADE["log"] = ([{"ts": int(time.time()), "msg": msg}] + AUTO_TRADE["log"])[:100]


def _agent_mark(agent_id: str, stage: str, action: str, reason: str = "", data: dict | None = None):
    AUTO_TRADE["hermesAgents"] = mark_agent(AUTO_TRADE.get("hermesAgents"), agent_id, stage, action, reason, data)


def _position_guardian_status_heartbeat(open_positions: list[dict]) -> None:
    rows = [p for p in (open_positions or []) if isinstance(p, dict)]
    if not rows:
        return
    symbols = sorted({str(p.get("symbol", "") or "").upper() for p in rows if str(p.get("symbol", "") or "").strip()})
    reason = ", ".join(symbols[:6])
    _agent_mark(
        "position_guardian",
        "done",
        "open positions heartbeat",
        reason,
        {"openPositions": len(rows), "source": "openLivePositions", "heartbeat": True},
    )


def _intel_data_quality_guard(intel: dict | None) -> dict:
    if not isinstance(intel, dict):
        return {"ok": False, "reason": "intel_missing", "issues": ["intel_missing"]}
    issues: list[str] = []
    symbol = str(intel.get("symbol", "") or "").upper().strip()
    signal = str(intel.get("signal", "") or "").upper().strip()
    if not symbol:
        issues.append("symbol_missing")
    if signal not in ("LONG", "SHORT", "WAIT"):
        issues.append("signal_invalid")
    try:
        confidence = float(intel.get("confidence", 0.0) or 0.0)
    except Exception:
        confidence = -1.0
    if confidence < 0.0 or confidence > 1.0:
        issues.append("confidence_invalid")
    execution = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    spread = execution.get("spreadBps")
    if spread is None:
        issues.append("spread_missing")
    else:
        try:
            if float(spread) < 0:
                issues.append("spread_invalid")
        except Exception:
            issues.append("spread_invalid")
    precision = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    if not precision:
        issues.append("precision_missing")
    momentum = intel.get("momentum") if isinstance(intel.get("momentum"), dict) else {}
    if not momentum:
        issues.append("momentum_missing")
    blocking = [x for x in issues if x not in {"spread_missing"}]
    return {
        "ok": not blocking,
        "reason": ",".join(blocking or issues) or "ok",
        "issues": issues,
        "symbol": symbol,
        "signal": signal,
        "confidence": round(max(0.0, min(1.0, confidence)), 4) if confidence >= 0 else 0.0,
    }


def _news_sentiment_guard_state(cfg: dict | None, intel: dict | None) -> dict:
    cfg = cfg if isinstance(cfg, dict) else {}
    enabled = bool(cfg.get("newsDailyEnabled", True))
    decision_data = intel.get("decisionData") if isinstance(intel, dict) and isinstance(intel.get("decisionData"), dict) else {}
    guard = decision_data.get("newsSentimentGuard") if isinstance(decision_data.get("newsSentimentGuard"), dict) else {}
    return {
        "enabled": enabled,
        "wired": bool(guard.get("enabled", False)),
        "status": str(guard.get("status", "not_wired") or "not_wired"),
        "decisionImpact": str(guard.get("decisionImpact", "guard_only") or "guard_only"),
    }


def _mark_trade_learning_agents(symbol: str, trade: dict, mode: str):
    try:
        pnl = float((trade or {}).get("pnl", 0.0) or 0.0)
        reason = str((trade or {}).get("reason", "") or "")
        side = str((trade or {}).get("side", "") or "")
        label = f"{str(mode).upper()} {str(symbol).upper()} {side} pnl={pnl:.4f}"
        # Only memory_agent actually persists the outcome on this path.
        # reflection_agent / backtest_agent do real work elsewhere (loss-streak tune,
        # walk-forward endpoint) and are marked there — marking them here would be
        # misleading telemetry.
        _agent_mark("memory_agent", "done", "trade outcome stored", label)
    except Exception as exc:
        print(f"[Trade Log] ERROR writing {TRADES_LOG_PATH}: {exc}")


def _live_lock_key(symbol: str, side: str) -> str:
    return f"{str(symbol).upper()}:{str(side).upper()}"


def _autotrade_skip(code: str, msg: str):
    AUTO_TRADE["lastSkip"] = {"ts": int(time.time()), "code": code, "msg": msg}
    _autotrade_log(msg)
    # Normal skips are not API/network failures; clear streak so UI "consecutive errors" stays honest.
    if code != "exception":
        AUTO_TRADE["consecutiveErrors"] = 0


def _symbol_volatility_score(symbol: str, intel: dict | None = None) -> dict:
    """Compute a per-symbol volatility profile used to scale TP/SL/cap/profit-lock.

    Each symbol gets a tier ("low" / "med" / "high") plus a multiplier (0.65 - 1.4)
    that scales the global TP%, SL%, notional cap and profit-lock trigger. The
    profile is computed from in-flight intel (atrPct, momentum, spread, vwap
    distance, BB position) so volatile symbols get wider TP/SL (giving trades
    room to breathe) and tighter caps (smaller position size for riskier coins),
    while stable majors keep the original tight targets.
    """
    sym = str(symbol or "").upper().strip()
    intel = intel if isinstance(intel, dict) else {}
    precision = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    execution = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}

    atr_pct = max(0.0, float(precision.get("atrPct", 0.0) or 0.0))
    momentum = abs(float(execution.get("momentumPct", 0.0) or 0.0))
    spread_bps = max(0.0, float(execution.get("spreadBps", 0.0) or 0.0))
    vwap_dist = abs(float(precision.get("vwapDistancePct", 0.0) or 0.0))
    bb = float(precision.get("bbPctB", 0.5) or 0.5)
    long_score = float(precision.get("longScore", 0.0) or 0.0)
    short_score = float(precision.get("shortScore", 0.0) or 0.0)
    directional = abs(long_score - short_score) + momentum

    # Volatility score 0.0 (rock solid) → 1.0 (chaotic). Each component is
    # weighted by how much it actually predicts tradable noise.
    score = 0.0
    score += min(0.30, atr_pct / 1.5)            # ATR is the dominant driver
    score += min(0.25, momentum / 1.0)            # absolute momentum
    score += min(0.15, spread_bps / 80.0)         # execution cost / slippage risk
    score += min(0.15, vwap_dist / 0.5)           # stretched-from-fair risk
    score += min(0.10, abs(bb - 0.5) / 0.5)       # BB position extremes
    score += min(0.10, directional / 6.0)         # one-sided directional energy
    score = max(0.0, min(1.0, score))

    if score < 0.30:
        tier = "low"
        tp_mult = 0.85       # tighter TP for stable majors (BTC/ETH)
        sl_mult = 0.85       # tighter SL — trend holds, less whipsaw
        cap_mult = 1.20      # larger cap — high confidence in size
        lock_mult = 0.85     # lock profits earlier (smaller moves)
    elif score < 0.55:
        tier = "med"
        tp_mult = 1.00       # use the configured TP as-is
        sl_mult = 1.00
        cap_mult = 1.00
        lock_mult = 1.00
    else:
        tier = "high"
        tp_mult = 1.30       # wider TP — need room for swings
        sl_mult = 1.20       # slightly wider SL — avoid premature stops
        cap_mult = 0.65      # smaller cap — riskier coin
        lock_mult = 1.40     # lock profits later (give trades room)

    return {
        "symbol": sym,
        "tier": tier,
        "score": round(score, 4),
        "atrPct": round(atr_pct, 4),
        "momentumPct": round(momentum, 4),
        "spreadBps": round(spread_bps, 4),
        "tpMult": round(tp_mult, 4),
        "slMult": round(sl_mult, 4),
        "capMult": round(cap_mult, 4),
        "lockMult": round(lock_mult, 4),
    }


# --- 3-tier policy: System (risk) -> Group (behavioral class) -> Symbol (per-coin) ---

# Group definitions: each group captures a behavioral class so coins that behave
# similarly can share a single profile instead of every coin needing its own.
# The fallback chain is: symbol profile -> group profile -> system policy.
SYMBOL_GROUP_DEFS: dict[str, dict] = {
    # Coins that trend hard, low mean-reversion. Mostly BTC/ETH and the
    # majors. Hold winners longer, tighter SL because trend holds.
    "trend-friendly": {
        "tpsl_mult": 1.10,
        "sl_mult": 0.90,
        "lock_trigger_mult": 0.95,
        "holdTrail_base": 0.28,
        "holdMinConf_base": 0.72,
        "min_conf_floor": 0.50,
        "max_trade_notional_mult": 1.15,
        "scan_long_bias": 0.55,
        "scan_chase_speed": "fast",
        "position_size_mult": 1.00,   # base size (1.0 = full budget)
        "entry_offset_bps": 0,        # bps offset from mid to enter (0 = vwap)
    },
    "mean-reversion-friendly": {
        "tpsl_mult": 0.85,
        "sl_mult": 0.85,
        "lock_trigger_mult": 0.75,
        "holdTrail_base": 0.20,
        "holdMinConf_base": 0.76,
        "min_conf_floor": 0.62,
        "max_trade_notional_mult": 0.95,
        "scan_long_bias": 0.50,
        "scan_chase_speed": "slow",
        "position_size_mult": 0.85,   # smaller size — mean-reversion is lower reward/risk
        "entry_offset_bps": 15,       # enter slightly above mid to avoid chasing a reversal
    },
    "high-volatility": {
        "tpsl_mult": 1.35,
        "sl_mult": 1.20,
        "lock_trigger_mult": 1.50,
        "holdTrail_base": 0.22,
        "holdMinConf_base": 0.74,
        "min_conf_floor": 0.65,
        "max_trade_notional_mult": 0.60,
        "scan_long_bias": 0.50,
        "scan_chase_speed": "slow",
        "position_size_mult": 0.65,   # smaller size — high vol tokens are riskier
        "entry_offset_bps": 10,       # enter slightly above mid for momentum confirm
    },
    "low-liquidity-noisy": {
        "tpsl_mult": 0.95,
        "sl_mult": 0.80,
        "lock_trigger_mult": 0.50,
        "holdTrail_base": 0.18,
        "holdMinConf_base": 0.78,
        "min_conf_floor": 0.70,
        "max_trade_notional_mult": 0.40,
        "scan_long_bias": 0.50,
        "scan_chase_speed": "slow",
        "position_size_mult": 0.40,   # tiny — slippage kills these
        "entry_offset_bps": 25,       # enter well above mid to avoid fake pumps
    },
}

# Hard-coded symbol → group mapping for well-known assets. New symbols
# default to "trend-friendly" until the bot learns better.
SYMBOL_GROUP_DEFAULT: dict[str, str] = {
    # trend-friendly: BTC, ETH and the other majors with deep liquidity
    "BTCUSDT": "trend-friendly", "ETHUSDT": "trend-friendly", "SOLUSDT": "trend-friendly",
    "BNBUSDT": "trend-friendly", "XRPUSDT": "trend-friendly", "ADAUSDT": "trend-friendly",
    "AVAXUSDT": "trend-friendly", "LINKUSDT": "trend-friendly", "DOTUSDT": "trend-friendly",
    "MATICUSDT": "trend-friendly", "NEARUSDT": "trend-friendly", "ATOMUSDT": "trend-friendly",
    "LTCUSDT": "trend-friendly", "ETCUSDT": "trend-friendly",
    # mean-reversion-friendly: range-bound or stable pairings
    "XLMUSDT": "mean-reversion-friendly", "XRPUSDT": "mean-reversion-friendly",
    "EOSUSDT": "mean-reversion-friendly", "BCHUSDT": "mean-reversion-friendly",
    # high-volatility: leveraged tokens, momentum names
    "DOGEUSDT": "high-volatility", "SHIBUSDT": "high-volatility", "PEPEUSDT": "high-volatility",
    "WIFUSDT": "high-volatility", "FLOKIUSDT": "high-volatility",
    "AAVEUSDT": "high-volatility", "INJUSDT": "high-volatility",
    "RNDRUSDT": "high-volatility", "FETUSDT": "high-volatility",
    "SUIUSDT": "high-volatility", "APTUSDT": "high-volatility", "ARBUSDT": "high-volatility",
    "OPUSDT": "high-volatility", "ARKMUSDT": "high-volatility",
    "PUMPUSDT": "high-volatility", "PENGUUSDT": "high-volatility",
    # low-liquidity-noisy: small caps, meme names
    "SPXUSDT": "low-liquidity-noisy", "STGUSDT": "low-liquidity-noisy",
    "HOMEUSDT": "low-liquidity-noisy", "ZROUSDT": "low-liquidity-noisy",
}


def _symbol_group(symbol: str) -> str:
    """Return the behavioral group for ``symbol``.

    Order of resolution: explicit default map -> learned override
    (from symbolProfiles) -> "trend-friendly" (safe default). The group
    drives the per-symbol policy unless a richer symbol profile has been
    promoted for that coin.
    """
    sym = str(symbol or "").upper().strip()
    if not sym:
        return "trend-friendly"
    # Explicit hard-coded list wins first (curated, well-known assets).
    if sym in SYMBOL_GROUP_DEFAULT:
        return SYMBOL_GROUP_DEFAULT[sym]
    # Otherwise consult the learned profile (in case an autotune decided
    # this coin belongs to a different group based on observed behavior).
    try:
        _ps = PerSymbolStorage(VAULT_DIR, sym)
        pr = _ps.load_symbol_profile()
    except Exception:
        pr = None
    if isinstance(pr, dict):
        grp = pr.get("group")
        if isinstance(grp, str) and grp in SYMBOL_GROUP_DEFS:
            return grp
    return "trend-friendly"


# Sample-count guard: number of closed trades needed before a symbol profile
# is allowed to override the group profile. Below this, we always fall back
# to the group (insufficient evidence for a per-coin policy).
SYMBOL_PROFILE_MIN_TRADES = 8


def _symbol_sample_count(symbol: str) -> int:
    """Number of closed LIVE trades recorded for ``symbol`` (capped to last 30d)."""
    try:
        sym = str(symbol or "").upper().strip()
        if not sym:
            return 0
        storage = PerSymbolStorage(VAULT_DIR, sym)
        trades = storage.load_trades(mode="LIVE")
        if not isinstance(trades, list):
            return 0
        return len(trades)
    except Exception:
        return 0


# Symbol profile storage: persisted in obsidian_vault alongside learning
# profiles. Each profile contains user-overrideable policy fields; only the
# fields explicitly set are used (others fall through to group / system).
# Use __file__ (backend/main.py) to resolve path relative to script location,
# not the current working directory.
SYMBOL_PROFILES_PATH = Path(__file__).parent / "obsidian_vault" / "symbol_profiles.json"


def _load_symbol_profiles() -> dict:
    """Load per-symbol override profiles from disk.

    The file stores a flat dict of ``symbol -> profile``. Each profile can
    contain any subset of these fields (missing → fall through to group):
        group: str (one of SYMBOL_GROUP_DEFS)
        minConfidence: float
        rewardDelta: float          # additive bias to reward score
        longBias: float             # 0.0-1.0, bias toward LONG in scan
        chaseSpeed: str             # "fast" / "slow" / "normal"
        tpPct: float                # explicit TP%, overrides volatility tier
        slPct: float                # explicit SL%
        profitLockTriggerUsdt: float
        cooldownMinutes: int        # explicit cooldown after close
        note: str                   # user note
    """
    try:
        _ensure_vault()
        if SYMBOL_PROFILES_PATH.exists():
            data = json.loads(SYMBOL_PROFILES_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def _save_symbol_profiles(profiles: dict) -> None:
    """Persist the per-symbol profiles dict to disk."""
    try:
        _ensure_vault()
        SYMBOL_PROFILES_PATH.write_text(
            json.dumps(profiles, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        print(f"[Trade Log] ERROR writing {TRADES_LOG_PATH}: {exc}")


# Fields the dashboard "Symbol Profile (3-tier)" table reads from each
# openLivePositions row. Only these are copied from the profile; everything
# else stays in the profile store. Keep this list in sync with the frontend
# renderSymbolProfileSummary() in backend/dashboard/index.html.
_SYMBOL_PROFILE_DASHBOARD_FIELDS = (
    "group",
    "minConfidence",
    "tpPct",
    "slPct",
    "profitLockTriggerUsdt",
    "learnedProfitFactor",
    "learnedRecentScore",
    "learnedTrades",
    "learnedWinRatePct",
    "learnedPnl",
    "notionalCapUsdt",
    "leverageMax",
    "positionSizeMult",
    "chaseSpeed",
    "longBias",
)


def _attach_symbol_profile(position: dict, profiles: dict | None) -> None:
    """Merge per-symbol profile fields onto an openLivePositions row in place.

    Without this, the dashboard falls back to the empty `bot.symbolProfiles`
    list only when openLivePositions is empty — so any open live position
    shows `—` for PF/Conf/TP/SL/Lock/Rec even though the profile is saved.
    The merge makes the dashboard read work whether or not a position is open.

    Even when no saved profile exists for the symbol we still populate
    `group` from `_symbol_group()` (default map → learned override → safe
    default) so the dashboard does not render `—` for newly-traded symbols
    that have not yet earned enough samples for a learned profile.
    """
    if not isinstance(position, dict):
        return
    sym = str(position.get("symbol", "") or "").upper().strip()
    if not sym:
        return
    prof = profiles.get(sym) if isinstance(profiles, dict) else None
    if prof is None:
        try:
            from services.config_paths import VAULT_DIR
            from trading.per_symbol_storage import PerSymbolStorage
            _ps = PerSymbolStorage(VAULT_DIR, sym)
            prof = _ps.load_symbol_profile()
        except Exception:
            prof = None
    if isinstance(prof, dict):
        for key in _SYMBOL_PROFILE_DASHBOARD_FIELDS:
            if key in prof and position.get(key) is None:
                position[key] = prof.get(key)
    # Always resolve `group` so the dashboard shows the tier even without a
    # saved profile (PUMPUSDT, ARBUSDT, etc. that have only the default map).
    if position.get("group") is None:
        try:
            position["group"] = _symbol_group(sym)
        except Exception:
            pass
    # Mark source so the frontend can tell "position with profile" vs raw.
    if position.get("source") is None:
        position["source"] = "live+profile" if isinstance(prof, dict) else "live"


def _auto_update_symbol_profile(symbol: str, cfg: dict | None = None) -> dict:
    """Derive a conservative self-learning symbol profile from learning stats.

    This is intentionally cautious: it only nudges policy when the symbol has
    enough evidence in the learning store, and it keeps the resulting profile
    bounded so the bot can learn continuously without overfitting.
    """
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {}
    cfg = cfg if isinstance(cfg, dict) else {}
    pr = _load_single_profile(sym)
    if not isinstance(pr, dict):
        return {}

    windows = pr.get("memoryWindows") if isinstance(pr.get("memoryWindows"), dict) else {}
    w7 = windows.get("7d") if isinstance(windows.get("7d"), dict) else {}
    w14 = windows.get("14d") if isinstance(windows.get("14d"), dict) else {}
    w30 = windows.get("30d") if isinstance(windows.get("30d"), dict) else {}
    recent_score = float((pr.get("weightedRecentScore") or {}).get("score", 0.0) or 0.0)
    reward_score = float(pr.get("rewardScore", 0.0) or 0.0)
    reward_delta = float(pr.get("rewardDelta", 0.0) or 0.0)
    behavior_delta = float(pr.get("rewardBehaviorDelta", 0.0) or 0.0)

    # Choose the longest available window with enough samples, then blend the
    # shorter windows for recency. This makes the bot react to changes but not
    # too quickly.
    def _pick_window() -> dict:
        for win in (w7, w14, w30):
            if int(win.get("trades", 0) or 0) >= 6:
                return win
        return w30 or w14 or w7 or {}

    win = _pick_window()
    trades = int(win.get("trades", 0) or 0)
    if trades < 6:
        return {}

    wr = float(win.get("winRatePct", 0.0) or 0.0)
    pnl = float(win.get("pnl", 0.0) or 0.0)
    # Compute profit factor from the window's win/loss breakdown so we don't
    # depend on a missing profitFactor field on the learning profile.
    gross_win = float(win.get("grossWin", 0.0) or 0.0)
    gross_loss = float(win.get("grossLoss", 0.0) or 0.0)
    if gross_loss > 0:
        pf = gross_win / gross_loss
    elif gross_win > 0:
        pf = 999.0
    else:
        pf = float(pr.get("profitFactor", 0.0) or 0.0)
    pf = min(pf, 999.0)
    ql = int(pr.get("quickLosses", 0) or 0)
    obs = int(pr.get("observations", 0) or 0)
    picks = int(pr.get("pickedCount", 0) or 0)
    pick_rate = (picks / max(obs, 1)) * 100.0 if obs > 0 else 0.0

    # Start from the current effective group profile so user overrides and the
    # 3-tier fallback stay intact.
    base = _symbol_effective_profile(sym, cfg)
    out = dict(base)

    # Confidence floor: good symbols can relax a bit, weak symbols get stricter.
    if wr >= 60.0 and pnl > 0.0:
        out["min_conf_floor"] = max(0.45, float(base.get("min_conf_floor", 0.5)) - 0.03)
    elif wr <= 45.0 or pnl < -0.5:
        out["min_conf_floor"] = min(0.85, float(base.get("min_conf_floor", 0.5)) + 0.04)

    # Scan bias: if the symbol repeatedly performs better on one side,
    # nudge the scan bias slightly. Keep it bounded so the symbol cannot
    # become permanently one-sided from a few trades.
    long_bias = float(base.get("scan_long_bias", 0.5))
    if wr >= 58.0 and pnl > 0.0:
        long_bias += 0.02
    elif wr < 48.0 and pnl < 0.0:
        long_bias -= 0.02
    out["scan_long_bias"] = round(max(0.42, min(0.58, long_bias)), 4)

    # Chase speed: strong symbols can enter faster; noisy symbols should wait.
    chase = str(base.get("scan_chase_speed", "normal"))
    if ql >= 2 or wr < 48.0:
        chase = "slow"
    elif wr >= 60.0 and pnl > 0.0:
        chase = "fast"
    out["scan_chase_speed"] = chase

    # TP/SL / lock: make the learning react more strongly to expectancy,
    # not just win rate. Positive expectancy should widen profit capture a bit;
    # negative expectancy should cut risk faster.
    tp_mult = float(base.get("tpsl_mult", 1.0))
    sl_mult = float(base.get("sl_mult", 1.0))
    lock_mult = float(base.get("lock_trigger_mult", 1.0))
    cap_mult = float(base.get("max_trade_notional_mult", 1.0))
    if wr >= 58.0 and pnl > 0.0 and pf >= 1.10:
        edge = min(0.30, (wr - 55.0) / 100.0 + min(0.10, pnl / 20.0) + min(0.10, (pf - 1.0) / 5.0))
        tp_mult = min(2.0, tp_mult + 0.05 + edge)
        lock_mult = min(2.0, lock_mult + 0.04 + edge * 0.5)
        cap_mult = min(1.45, cap_mult + 0.04 + edge * 0.35)
    elif wr <= 47.0 or pnl < -0.4 or pf < 0.95:
        weakness = min(0.35, max(0.0, 50.0 - wr) / 80.0 + abs(min(pnl, 0.0)) / 8.0 + max(0.0, 1.0 - pf) * 0.12)
        tp_mult = max(0.60, tp_mult - (0.06 + weakness))
        sl_mult = min(1.65, sl_mult + (0.06 + weakness * 0.9))
        lock_mult = max(0.40, lock_mult - (0.06 + weakness * 0.5))
        cap_mult = max(0.30, cap_mult - (0.06 + weakness * 0.6))
    out["tpsl_mult"] = round(tp_mult, 4)
    out["sl_mult"] = round(sl_mult, 4)
    out["lock_trigger_mult"] = round(lock_mult, 4)
    out["max_trade_notional_mult"] = round(cap_mult, 4)

    # Reward / tuning bias: store bounded hint values so existing adaptive
    # functions can incorporate them, but never let them dominate the signal.
    out["rewardDelta"] = round(max(-0.12, min(0.12, reward_delta + (0.02 if pnl > 0 else -0.02))), 4)
    out["rewardBehaviorDelta"] = round(max(-0.12, min(0.12, behavior_delta + (0.015 if wr >= 58.0 else -0.015))), 4)
    out["rewardScore"] = round(max(-5.0, min(5.0, reward_score + (0.05 if pnl > 0 else -0.05))), 4)

    # Learning meta fields for the dashboard.
    out["learnedAt"] = int(time.time())
    out["learnedFromWindow"] = str(win.get("window", "30d") or "30d")
    out["learnedTrades"] = trades
    out["learnedWinRatePct"] = round(wr, 2)
    out["learnedPnl"] = round(pnl, 6)
    out["learnedProfitFactor"] = round(pf, 4)
    out["learnedPickRatePct"] = round(pick_rate, 2)
    out["learnedRecentScore"] = round(recent_score, 4)
    out["learnedQuickLosses"] = ql

    # Position sizing: use symbolRiskTune if available and active,
    # otherwise fall back to the group base multiplier. Learned behavior
    # shifts size based on win rate and pnl trend.
    pos_mult = float(base.get("positionSizeMult") or base.get("position_size_mult", 1.0) or 1.0)
    sm = 1.0
    lv_mult = 1.0
    lv_max_override = 0
    tune = pr.get("symbolRiskTune") if isinstance(pr.get("symbolRiskTune"), dict) else {}
    if bool(tune.get("active")) and int(tune.get("window", 0) or 0) >= 4:
        sm = float(tune.get("sizeMult", 1.0) or 1.0)
        lv_mult = float(tune.get("leverageMult", 1.0) or 1.0)
        lv_max_override = int(tune.get("recommendedLeverageMax", 0) or 0)
    elif wr >= 60.0 and pnl > 0.0:
        sm = min(1.30, pos_mult + 0.05)
    elif wr <= 45.0 or pnl < -0.5:
        sm = max(0.40, pos_mult - 0.08)
    # Loss-streak guard: a bleeding symbol never gets an oversized learned
    # size multiplier (ESPUSDT learned positionSizeMult 1.3 while losing).
    if pnl < 0.0:
        sm = min(sm, 1.10)
    out["positionSizeMult"] = round(sm, 4)
    out["position_size_mult"] = round(sm, 4)

    # Entry offset: use group default (group handles the behavioral class).
    out["entry_offset_bps"] = float(base.get("entry_offset_bps", 0.0) or 0.0)

    # Learned position size and leverage metadata for the dashboard.
    learned_pos_mult = round(sm, 4)
    learned_lev_mult = round(lv_mult, 4)
    learned_lev_max = int(lv_max_override) if lv_max_override else 0

    # Keep symbol profile lean: persist only override-like fields.
    return {
        "group": out.get("group"),
        "minConfidence": round(float(out.get("min_conf_floor", 0.5)), 4),
        "rewardDelta": float(out.get("rewardDelta", 0.0) or 0.0),
        "rewardBehaviorDelta": float(out.get("rewardBehaviorDelta", 0.0) or 0.0),
        "rewardScore": float(out.get("rewardScore", 0.0) or 0.0),
        "longBias": round(float(out.get("scan_long_bias", 0.5)), 4),
        "chaseSpeed": str(out.get("scan_chase_speed", "normal")),
        "tpPct": round(float(cfg.get("takeProfitPct", 1.8) or 1.8) * float(out.get("tpsl_mult", 1.0)), 4),
        "slPct": round(float(cfg.get("stopLossPct", 0.9) or 0.9) * float(out.get("sl_mult", 1.0)), 4),
        "profitLockTriggerUsdt": round(float(cfg.get("profitLockTriggerUsdt", 0.35) or 0.35) * float(out.get("lock_trigger_mult", 1.0)), 4),
        "notionalCapUsdt": round(float(cfg.get("tradeNotionalCapUsdt", 80.0) or 80.0) * float(out.get("max_trade_notional_mult", 1.0)), 4),
        "cooldownMinutes": int(max(0, round(float(cfg.get("symbolCooldownMins", 15) or 15)))) if wr >= 60.0 else int(max(0, round(float(cfg.get("symbolCooldownMins", 15) or 15) * 1.2))),
        # Position sizing learned fields
        "positionSizeMult": learned_pos_mult,
        "leverageMult": learned_lev_mult,
        "leverageMax": learned_lev_max,
        "entryOffsetBps": round(float(out.get("entry_offset_bps", 0.0)), 2),
        # Meta
        "learnedAt": int(out.get("learnedAt", time.time())),
        "learnedFromWindow": out.get("learnedFromWindow", "30d"),
        "learnedTrades": int(out.get("learnedTrades", trades)),
        "learnedWinRatePct": float(out.get("learnedWinRatePct", wr)),
        "learnedPnl": float(out.get("learnedPnl", pnl)),
        "learnedProfitFactor": float(out.get("learnedProfitFactor", pf)),
        "learnedPickRatePct": float(out.get("learnedPickRatePct", pick_rate)),
        "learnedRecentScore": float(out.get("learnedRecentScore", recent_score)),
        "learnedQuickLosses": int(out.get("learnedQuickLosses", ql)),
    }


def _symbol_effective_profile(symbol: str, cfg: dict | None = None) -> dict:
    """Return the effective per-symbol profile (merged with group defaults).

    Returns a flat dict with every policy field resolved. Callers do not need
    to know which tier the value came from. If the symbol has fewer than
    SYMBOL_PROFILE_MIN_TRADES closed trades, the symbol-level overrides
    (minConfidence, tpPct, slPct, etc.) are ignored and only the group
    defaults apply — prevents overfitting on a handful of trades.
    """
    sym = str(symbol or "").upper().strip()
    cfg = cfg if isinstance(cfg, dict) else {}
    group_name = _symbol_group(sym)
    group_def = SYMBOL_GROUP_DEFS.get(group_name, SYMBOL_GROUP_DEFS["trend-friendly"])

    profile = dict(group_def)
    profile["symbol"] = sym
    profile["group"] = group_name
    profile["source"] = "group"

    n_trades = _symbol_sample_count(sym)
    if n_trades >= SYMBOL_PROFILE_MIN_TRADES:
        try:
            _ps = PerSymbolStorage(VAULT_DIR, sym)
            sym_profile = _ps.load_symbol_profile()
        except Exception:
            sym_profile = None
        if isinstance(sym_profile, dict):
            for k, v in sym_profile.items():
                if k in ("group", "source"):
                    continue
                profile[k] = v
            profile["source"] = "symbol+group"
    profile["sampleTrades"] = n_trades
    profile["minPromotedTrades"] = SYMBOL_PROFILE_MIN_TRADES
    return profile


# --- API: read/write per-symbol profiles (used by the dashboard) ---

@app.get("/hermes/symbol/profile")
def hermes_get_symbol_profile(symbol: str):
    """Return the resolved 3-tier policy for ``symbol`` (System -> Group -> Symbol).

    The ``resolved`` object always reflects what the bot would actually use for
    a fresh entry. ``groupDefaults`` shows the group's raw defaults and
    ``overrides`` shows the user-applied per-symbol overrides (only applied
    when sample count >= minPromotedTrades).
    """
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {"ok": False, "error": "missing symbol"}
    profile = _symbol_effective_profile(sym)
    try:
        from services.config_paths import VAULT_DIR
        from trading.per_symbol_storage import PerSymbolStorage
        _ps = PerSymbolStorage(VAULT_DIR, sym)
        sym_raw = _ps.load_symbol_profile() or {}
    except Exception:
        sym_raw = {}
    perf = _rolling_symbol_perf(sym, 30) or {}
    return {
        "ok": True,
        "symbol": sym,
        "sampleTrades": int(profile.get("sampleTrades", 0)),
        "minPromotedTrades": int(SYMBOL_PROFILE_MIN_TRADES),
        "resolved": {
            "group": str(profile.get("group", "trend-friendly")),
            "source": str(profile.get("source", "group")),
            # Group-level multipliers
            "tpslMult": round(float(profile.get("tpsl_mult", 1.0)), 4),
            "slMult": round(float(profile.get("sl_mult", 1.0)), 4),
            "lockTriggerMult": round(float(profile.get("lock_trigger_mult", 1.0)), 4),
            "capMult": round(float(profile.get("max_trade_notional_mult", 1.0)), 4),
            # Per-symbol resolved values (post all tiers)
            "minConfidence": round(float(profile.get("minConfidence", profile.get("min_conf_floor", 0.5))), 4),
            "scanLongBias": round(float(profile.get("scan_long_bias", 0.5)), 4),
            "scanChaseSpeed": str(profile.get("scan_chase_speed", "normal")),
            "positionSizeMult": round(float(profile.get("positionSizeMult") or profile.get("position_size_mult", 1.0) or 1.0), 4),
            "entryOffsetBps": round(float(profile.get("entry_offset_bps", 0.0) or 0.0), 2),
            "tpPct": round(float(profile.get("tpPct") or 1.8), 4),
            "slPct": round(float(profile.get("slPct") or 0.9), 4),
            "notionalCapUsdt": round(float(profile.get("notionalCapUsdt") or 80.0), 4),
            "profitLockTriggerUsdt": round(float(profile.get("profitLockTriggerUsdt") or 0.35), 4),
            "cooldownMinutes": int(profile.get("cooldownMinutes") or 15),
            "leverageMult": round(float(profile.get("leverageMult", 1.0) or 1.0), 4),
            "leverageMax": int(profile.get("leverageMax") or 0),
        },
        "overrides": sym_raw,
        "perf": {
            "trades": int(perf.get("trades", 0)),
            "winRatePct": float(perf.get("winRatePct", 0.0)),
            "pnl": float(perf.get("pnl", 0.0)),
            "memoryWindow": str(perf.get("memoryWindow", "none")),
        },
    }


@app.post("/hermes/symbol/profile")
def hermes_set_symbol_profile(payload: dict = Body(default_factory=dict)):
    sym = str(payload.get("symbol", "") or "").upper().strip()
    if not sym:
        return {"ok": False, "error": "missing symbol"}
    updates = {k: v for k, v in payload.items() if k != "symbol"}
    allowed_keys = {
        "group", "minConfidence", "rewardDelta", "longBias", "chaseSpeed",
        "tpPct", "slPct", "profitLockTriggerUsdt", "cooldownMinutes",
        "positionSizeMult", "leverageMult", "entryOffsetBps", "note",
    }
    clean = {k: v for k, v in updates.items() if k in allowed_keys}
    if not clean:
        return {"ok": False, "error": "no supported keys", "allowed": sorted(allowed_keys)}
    if "group" in clean and clean["group"] not in SYMBOL_GROUP_DEFS:
        return {"ok": False, "error": f"unknown group: {clean['group']}",
                "allowed_groups": list(SYMBOL_GROUP_DEFS.keys())}
    try:
        from services.config_paths import VAULT_DIR
        from trading.per_symbol_storage import PerSymbolStorage
        _ps = PerSymbolStorage(VAULT_DIR, sym)
        existing = _ps.load_symbol_profile() or {}
    except Exception:
        existing = {}
    merged = dict(existing)
    merged.update(clean)
    try:
        from services.config_paths import VAULT_DIR
        from trading.per_symbol_storage import PerSymbolStorage
        _ps = PerSymbolStorage(VAULT_DIR, sym)
        _ps.save_symbol_profile(merged)
    except Exception as exc:
        print(f"[SymbolProfiles] ERROR saving per-symbol profile for {sym}: {exc}")
    return {"ok": True, "symbol": sym, "saved": clean,
            "effective": _symbol_effective_profile(sym)}


@app.get("/hermes/symbol/profiles")
def hermes_list_symbol_profiles():
    """Return per-symbol effective profiles for all symbols seen in the
    recent trade log, sorted by sample count descending.
    """
    try:
        rows = _live_closed_trades_from_log(symbol=None, mode="LIVE")
    except Exception:
        rows = []
    symbols = set()
    for r in rows or []:
        if isinstance(r, dict):
            s = str(r.get("symbol", "") or "").upper().strip()
            if s:
                symbols.add(s)
    out = []
    for s in symbols:
        p = _symbol_effective_profile(s)
        perf = _rolling_symbol_perf(s, 30) or {}
        out.append({
            "symbol": s,
            "group": p.get("group", "trend-friendly"),
            "source": p.get("source", "group"),
            "sampleTrades": p.get("sampleTrades", 0),
            "tpslMult": p.get("tpsl_mult", 1.0),
            "scanLongBias": p.get("scan_long_bias", 0.5),
            "winRatePct": perf.get("winRatePct", 0.0),
            "pnl": perf.get("pnl", 0.0),
        })
    out.sort(key=lambda r: (r["source"] != "symbol+group", -r["sampleTrades"], r["symbol"]))
    return {"ok": True, "count": len(out), "items": out}





def _flat_intel_keys(obj: dict) -> set:
    """Helper: collect intel keys from the precision + execution dicts."""
    keys: set = set()
    for sub in (obj.get("precision"), obj.get("execution")):
        if isinstance(sub, dict):
            keys.update(sub.keys())
    return keys


def _effective_tp_sl(symbol: str, cfg: dict, intel: dict | None = None) -> dict:
    """Return the effective TP/SL/cap/profit-lock values for a specific symbol.

    Combines three layers in order (most specific wins):
        1. Symbol-level overrides (only when sample count >= threshold)
        2. Group profile (trend-friendly / mean-reversion / high-vol / noisy)
        3. Volatility-tier multiplier (low / med / high, derived from intel)
        4. Global cfg fallback (only when intel is empty)
    The returned dict is the single source of truth used by entry, guardian
    and live order placement.
    """
    cfg = cfg if isinstance(cfg, dict) else {}
    sym_profile = _symbol_effective_profile(symbol, cfg)
    group_tpsl_mult = float(sym_profile.get("tpsl_mult", 1.0))
    group_sl_mult = float(sym_profile.get("sl_mult", 1.0))
    group_lock_mult = float(sym_profile.get("lock_trigger_mult", 1.0))
    group_cap_mult = float(sym_profile.get("max_trade_notional_mult", 1.0))

    vol = _symbol_volatility_score(symbol, intel)
    # The two multipliers stack: group first, then volatility tier. This
    # means a high-vol group on a high-volatility symbol gets BOTH the
    # group's wider TP/SL AND the volatility tier's wider TP/SL — fully
    # additive, no surprise cap.
    tp_mult = group_tpsl_mult * float(vol.get("tpMult", 1.0))
    sl_mult = group_sl_mult * float(vol.get("slMult", 1.0))
    cap_mult = group_cap_mult * float(vol.get("capMult", 1.0))
    lock_mult = group_lock_mult * float(vol.get("lockMult", 1.0))

    # Skip override if intel was missing entirely (avoid scaling from a zero
    # signal during boot).
    intel_present = isinstance(intel, dict) and bool(_flat_intel_keys(intel))

    base_tp = float(cfg.get("takeProfitPct", 1.2) or 1.2)
    base_sl = max(0.20, float(cfg.get("stopLossPct", 0.8) or 0.8))
    base_cap = max(20.0, float(cfg.get("tradeNotionalCapUsdt", RISK["max_notional"]) or RISK["max_notional"]))
    base_lock_trigger = float(cfg.get("profitLockTriggerUsdt", 0.35) or 0.35)
    base_lock_keep = float(cfg.get("profitLockKeepUsdt", 0.15) or 0.15)
    base_lock_giveback = float(cfg.get("profitLockMaxGivebackUsdt", 0.22) or 0.22)
    base_tp_min = float(cfg.get("tpTargetMinUsdt", 0.55) or 0.55)
    base_tp_max = float(cfg.get("tpTargetMaxUsdt", 2.0) or 2.0)

    if not intel_present:
        return {
            "symbol": str(symbol or "").upper().strip(),
            "tier": "unknown",
            "tpMult": 1.0, "slMult": 1.0, "capMult": 1.0, "lockMult": 1.0,
            "tpPct": round(base_tp, 4),
            "slPct": round(base_sl, 4),
            "notionalCapUsdt": round(base_cap, 4),
            "profitLockTriggerUsdt": round(base_lock_trigger, 4),
            "profitLockKeepUsdt": round(base_lock_keep, 4),
            "profitLockMaxGivebackUsdt": round(base_lock_giveback, 4),
            "tpTargetMinUsdt": round(base_tp_min, 4),
            "tpTargetMaxUsdt": round(base_tp_max, 4),
            "holdTrailPct": round(float(sym_profile.get("holdTrail_base", float(cfg.get("holdTrailPct", 0.25) or 0.25))), 4),
            "holdMinConfidence": round(float(sym_profile.get("holdMinConf_base", float(cfg.get("holdMinConfidence", 0.72) or 0.72))), 4),
        }

    # Allow per-symbol explicit overrides to win when sample count is
    # sufficient (>= SYMBOL_PROFILE_MIN_TRADES). Missing keys still fall
    # through to the computed values.
    sym_overrides = sym_profile if sym_profile.get("source") == "symbol+group" else {}
    ret = {
        "symbol": str(symbol or "").upper().strip(),
        "tier": vol.get("tier", "med"),
        "group": sym_profile.get("group", "trend-friendly"),
        "source": sym_profile.get("source", "group"),
        "sampleTrades": sym_profile.get("sampleTrades", 0),
        "tpMult": round(tp_mult, 4),
        "slMult": round(sl_mult, 4),
        "capMult": round(cap_mult, 4),
        "lockMult": round(lock_mult, 4),
        "tpPct": round(float(sym_overrides.get("tpPct", base_tp * tp_mult)), 4),
        "slPct": round(float(sym_overrides.get("slPct", base_sl * sl_mult)), 4),
        "notionalCapUsdt": round(float(sym_overrides.get("notionalCapUsdt", base_cap * cap_mult)), 4),
        "profitLockTriggerUsdt": round(
            float(sym_overrides.get("profitLockTriggerUsdt", base_lock_trigger * lock_mult)), 4),
        "profitLockKeepUsdt": round(
            float(sym_overrides.get("profitLockKeepUsdt", base_lock_keep * lock_mult)), 4),
        "profitLockMaxGivebackUsdt": round(
            float(sym_overrides.get("profitLockMaxGivebackUsdt", base_lock_giveback * lock_mult)), 4),
        "tpTargetMinUsdt": round(float(sym_overrides.get("tpTargetMinUsdt", base_tp_min * lock_mult)), 4),
        "tpTargetMaxUsdt": round(float(sym_overrides.get("tpTargetMaxUsdt", base_tp_max * lock_mult)), 4),
    }
    # Guardian autotune: per-symbol trailing and hold-confidence
    # Fallback chain: symbol_profile → group base → global cfg → hardcoded default
    ret["holdTrailPct"] = round(float(sym_overrides.get("holdTrailPct", sym_profile.get("holdTrail_base", float(cfg.get("holdTrailPct", 0.25) or 0.25)))), 4)
    ret["holdMinConfidence"] = round(float(sym_overrides.get("holdMinConfidence", sym_profile.get("holdMinConf_base", float(cfg.get("holdMinConfidence", 0.72) or 0.72)))), 4)

    # Fee-based TP% floor: ensure gross profit covers round-trip fees + min net edge.
    # Without this, tight TP% (e.g. 0.35%) on small notional loses money to fees.
    try:
        _usdt = max(1.0, float(cfg.get("usdtAmount", 20.0) or 20.0))
        _taker = float(AUTOTRADE_TAKER_FEE_BPS_PER_SIDE)
        _extra = float(AUTOTRADE_EXTRA_COST_BPS)
        _slip = float(cfg.get("maxSlippageBps", 18.0) or 18.0)
        _cost_bps = (2.0 * _taker) + _extra + (_slip * 0.5)
        _min_net_usdt = float(cfg.get("feeMinNetProfitUSDT", AUTOTRADE_MIN_NET_PROFIT_USDT) or AUTOTRADE_MIN_NET_PROFIT_USDT)
        _tp_fee_floor_pct = ((_cost_bps / 10000.0 * _usdt) + _min_net_usdt) / _usdt * 100.0
        _tp_fee_floor_pct = max(_tp_fee_floor_pct, 0.25)  # absolute floor
        cur_tp = float(ret.get("tpPct", base_tp))
        if cur_tp < _tp_fee_floor_pct:
            ret["tpPct"] = round(_tp_fee_floor_pct, 4)
        cur_sl = float(ret.get("slPct", base_sl))
        _sl_fee_floor_pct = _tp_fee_floor_pct * 0.5  # SL floor = half of TP floor (R:R ≥ 2)
        if cur_sl < _sl_fee_floor_pct:
            ret["slPct"] = round(_sl_fee_floor_pct, 4)
        ret["tpFeeFloorPct"] = round(_tp_fee_floor_pct, 4)
    except Exception:
        pass

    return ret


def _calc_tp_sl_prices(side: str, entry_mark: float, tp_pct: float, sl_pct: float):
    if side == "LONG":
        return (
            entry_mark * (1 + tp_pct / 100),
            entry_mark * (1 - sl_pct / 100),
        )
    return (
        entry_mark * (1 - tp_pct / 100),
        entry_mark * (1 + sl_pct / 100),
    )


def _should_hold_winner(side: str, intel: dict | None, cfg: dict) -> bool:
    if not cfg.get("holdWinners", True):
        return False
    if not isinstance(intel, dict):
        return False
    sig = str(intel.get("signal", "WAIT")).upper()
    conf = float(intel.get("confidence", 0.0) or 0.0)
    if sig != side or conf < float(cfg.get("holdMinConfidence", 0.72)):
        return False
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    mom = float(ex.get("momentumPct", 0.0) or 0.0)
    # Keep running only when momentum still supports the current side.
    return (side == "LONG" and mom > 0) or (side == "SHORT" and mom < 0)


def _trail_winner_levels(side: str, mark: float, old_sl: float, old_tp: float, trail_pct: float, cfg: dict = None, symbol: str = None) -> tuple[float, float]:
    t = max(0.05, float(trail_pct))
    
    # Get TradingView guidance for SL trailing if enabled
    if cfg and symbol and cfg.get("tradingviewEnabled", False):
        try:
            tv_client = get_tv_mcp(cfg)
            tv_guidance = tv_client.get_position_guidance(symbol, side)
            if tv_guidance and should_trail_sl_with_tradingview(side, tv_guidance, cfg, symbol):
                tv_trail_pct = get_tradingview_sl_trailing_pct(side, tv_guidance, cfg)
                t = max(t, tv_trail_pct)  # Use the larger trail percentage
        except Exception as exc:
            _autotrade_log(f"[TradingView] trail SL guidance error {symbol}: {exc}")

    if side == "LONG":
        new_sl = max(float(old_sl), mark * (1 - t / 100.0))
        new_tp = max(float(old_tp), mark * (1 + t / 100.0))
        return new_sl, new_tp
    new_sl = min(float(old_sl), mark * (1 + t / 100.0))
    new_tp = min(float(old_tp), mark * (1 - t / 100.0))
    return new_sl, new_tp




def _adaptive_trade_usdt(base_usdt: float, symbol: str, intel: dict, cfg: dict) -> float:
    if not bool(cfg.get("adaptiveSizing", True)):
        return float(base_usdt)
    conf = float((intel or {}).get("confidence", 0.0) or 0.0)
    min_conf = float(cfg.get("minConfidence", 0.65))
    conf_boost = max(0.0, min(1.0, (conf - min_conf) / 0.35))
    qual = _symbol_quality_score(symbol)
    qual_boost = max(0.0, min(1.0, (qual + 0.02) / 0.12))
    strength = 0.65 * conf_boost + 0.35 * qual_boost
    max_boost = max(0.0, float(cfg.get("adaptiveSizeBoostMaxPct", 35.0))) / 100.0
    mult = 1.0 + (max_boost * strength)
    # Performance-aware risk scaling (rolling window).
    perf = _rolling_symbol_perf(symbol, int(cfg.get("perfWindowTrades", 30) or 30))
    if int(perf.get("trades", 0)) >= int(cfg.get("perfGateMinSamples", 8) or 8):
        wr = float(perf.get("winRatePct", 0.0) or 0.0)
        pnl = float(perf.get("pnl", 0.0) or 0.0)
        # Reduce risk for weak symbols; gently boost strong symbols.
        if wr < 35.0 or pnl < -0.4:
            mult *= 0.55
        elif wr < 42.0 or pnl < 0:
            mult *= 0.75
        elif wr >= 58.0 and pnl > 0.8:
            mult *= 1.12
    pr = _load_single_profile(str(symbol or "").upper())
    if isinstance(pr, dict):
        rs = float(pr.get("rewardScore", 0.0) or 0.0)
        rd = float(pr.get("rewardDelta", 0.0) or 0.0)
        rb = float(pr.get("rewardBehaviorDelta", 0.0) or 0.0)
        reward_mult = 1.0 + max(-0.20, min(0.20, rs / 250.0))
        recent_mult = 1.0 + max(-0.08, min(0.08, (rd + rb) / 20.0))
        mult *= reward_mult * recent_mult
        tune = pr.get("symbolRiskTune") if isinstance(pr.get("symbolRiskTune"), dict) else {}
        if bool(tune.get("active")) and int(tune.get("window", 0) or 0) >= 4:
            mult *= max(0.45, min(1.30, float(tune.get("sizeMult", 1.0) or 1.0)))
    # Hard clamps so sizing remains stable.
    mult = max(0.35, min(1.40, mult))
    # Loss-streak cap: when the symbol's rolling window is bleeding, never
    # let per-symbol/risk-tune multipliers push size above 1.1x (ESPUSDT's
    # positionSizeMult 1.3 + SL hit produced the -1.16 USDT single trade).
    if int(perf.get("trades", 0)) >= 4 and (float(perf.get("winRatePct", 0.0) or 0.0) < 38.0 or float(perf.get("pnl", 0.0) or 0.0) < -0.25):
        mult = min(mult, 1.10)
    return round(float(base_usdt) * mult, 2)


def _autotrade_leverage_cap() -> int:
    return max(1, min(25, int(RISK.get("max_leverage", 25) or 25)))


def _sync_autotrade_leverage_cap_from_cfg(cfg: dict) -> int:
    desired = _autotrade_leverage_cap()
    for key in ("leverageMax", "adaptiveLeverageMax", "leverage"):
        try:
            desired = max(desired, int(cfg.get(key, 0) or 0))
        except (TypeError, ValueError):
            pass
    desired = max(1, min(25, desired))
    RISK["max_leverage"] = desired
    return desired


def _autotrade_leverage_bounds(cfg: dict) -> tuple[int, int]:
    hard_cap = _sync_autotrade_leverage_cap_from_cfg(cfg)
    adaptive_cap = max(
        int(cfg.get("adaptiveLeverageMax", hard_cap) or hard_cap),
        int(cfg.get("leverageMax", hard_cap) or hard_cap),
    )
    hard_cap = max(1, min(hard_cap, adaptive_cap, 25))
    base = int(cfg.get("leverage", 5) or 5)
    lev_min = int(cfg.get("leverageMin", base) or base)
    lev_max = int(cfg.get("leverageMax", max(lev_min, base)) or max(lev_min, base))
    lev_min = max(1, min(hard_cap, lev_min))
    lev_max = max(lev_min, min(hard_cap, lev_max))
    return lev_min, lev_max


def _adaptive_symbol_leverage(symbol: str, intel: dict | None, cfg: dict) -> dict:
    lev_min, lev_max = _autotrade_leverage_bounds(cfg)
    sym_norm = str(symbol or "").upper().strip()
    base = max(lev_min, min(lev_max, int(cfg.get("leverage", lev_min) or lev_min)))
    auto_enabled = bool(cfg.get("leverageAutoEnabled", True)) and bool(cfg.get("adaptiveLeverageEnabled", True))
    if not auto_enabled or lev_min >= lev_max:
        return {"symbol": sym_norm, "leverage": base, "min": lev_min, "max": lev_max, "auto": False, "reason": "fixed_or_range_locked"}

    intel = intel if isinstance(intel, dict) else {}
    p = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    conf = float(intel.get("confidence", 0.0) or 0.0)
    min_conf = float(cfg.get("minConfidence", 0.65) or 0.65)
    conf_score = _clamp_float((conf - min_conf) / 0.25, 0.0, 1.0)

    atr_pct = max(0.0, float(p.get("atrPct", 0.0) or 0.0))
    momentum_abs = abs(float(ex.get("momentumPct", 0.0) or 0.0))
    spread_bps = max(0.0, float(ex.get("spreadBps", 0.0) or 0.0))
    max_spread = max(1.0, float(cfg.get("maxSpreadBps", 16.0) or 16.0))
    bb = float(p.get("bbPctB", 0.5) or 0.5)
    vwap_dist = abs(float(p.get("vwapDistancePct", 0.0) or 0.0))
    long_score = float(p.get("longScore", 0.0) or 0.0)
    short_score = float(p.get("shortScore", 0.0) or 0.0)
    score_gap = abs(long_score - short_score)

    if atr_pct <= 0:
        calm_score = 0.45
    elif atr_pct < 0.08:
        calm_score = 1.0
    elif atr_pct < 0.20:
        calm_score = 0.78
    elif atr_pct < 0.45:
        calm_score = 0.50
    elif atr_pct < 0.80:
        calm_score = 0.26
    else:
        calm_score = 0.10

    heat = max(
        _clamp_float((atr_pct - 0.18) / 0.62, 0.0, 1.0),
        _clamp_float((momentum_abs - 0.12) / 0.45, 0.0, 1.0),
        _clamp_float((spread_bps / max_spread) - 0.75, 0.0, 1.0),
        _clamp_float((abs(bb - 0.5) - 0.32) / 0.18, 0.0, 1.0),
        _clamp_float((vwap_dist - 0.18) / 0.45, 0.0, 1.0),
    )
    edge_score = _clamp_float(score_gap / 3.0, 0.0, 1.0)
    quality_score = _clamp_float((_symbol_quality_score(symbol) + 0.03) / 0.15, 0.0, 1.0)
    score = (0.42 * calm_score) + (0.24 * conf_score) + (0.18 * quality_score) + (0.16 * edge_score)
    score *= 1.0 - (0.45 * heat)

    perf = _rolling_symbol_perf(symbol, int(cfg.get("perfWindowTrades", 30) or 30))
    if int(perf.get("trades", 0) or 0) >= int(cfg.get("perfGateMinSamples", 8) or 8):
        wr = float(perf.get("winRatePct", 0.0) or 0.0)
        pnl = float(perf.get("pnl", 0.0) or 0.0)
        if wr < 38.0 or pnl < -0.5:
            score *= 0.62
        elif wr < 45.0 or pnl < 0:
            score *= 0.82
        elif wr >= 58.0 and pnl > 0.8:
            score = min(1.0, score + 0.12)

    pr = _load_single_profile(str(symbol or "").upper())
    tune = {}
    if isinstance(pr, dict):
        reward_score = float(pr.get("rewardScore", 0.0) or 0.0)
        score = max(0.0, min(1.0, score + max(-0.12, min(0.12, reward_score / 220.0))))
        tune = pr.get("symbolRiskTune") if isinstance(pr.get("symbolRiskTune"), dict) else {}
        if bool(tune.get("active")) and int(tune.get("window", 0) or 0) >= 4:
            lev_mult = max(0.55, min(1.18, float(tune.get("leverageMult", 1.0) or 1.0)))
            conf_shift = max(-0.05, min(0.08, float(tune.get("confidenceShift", 0.0) or 0.0)))
            score = max(0.0, min(1.0, (score * lev_mult) - conf_shift))

    lev = int(round(lev_min + ((lev_max - lev_min) * _clamp_float(score, 0.0, 1.0))))
    if bool(tune.get("active")) and int(tune.get("recommendedLeverageMax", 0) or 0) > 0:
        lev = min(lev, int(tune.get("recommendedLeverageMax", lev_max) or lev_max))
    lev = max(lev_min, min(lev_max, lev, 25))
    return {
        "symbol": sym_norm,
        "leverage": lev,
        "min": lev_min,
        "max": lev_max,
        "auto": True,
        "score": round(float(score), 4),
        "heat": round(float(heat), 4),
        "atrPct": round(float(atr_pct), 4),
        "momentumPct": round(float(momentum_abs), 4),
        "spreadBps": round(float(spread_bps), 4),
        "symbolRiskTune": tune if bool(tune.get("active")) else {},
        "reason": (
            f"calm={calm_score:.2f} heat={heat:.2f} conf={conf:.2f} edge={score_gap:.2f}"
            + (f" tune={tune.get('reason')} size={float(tune.get('sizeMult', 1.0) or 1.0):.2f} lev={float(tune.get('leverageMult', 1.0) or 1.0):.2f}" if bool(tune.get("active")) else "")
        ),
    }


def _position_display_leverage(symbol: str | None, cfg: dict | None, current: int | float | None = None) -> int | None:
    try:
        cur = int(float(current or 0))
    except (TypeError, ValueError):
        cur = 0
    if cur > 0:
        return min(25, cur)
    cfg = cfg if isinstance(cfg, dict) else {}
    sym = str(symbol or "").upper().strip()
    last = cfg.get("lastAdaptiveLeverage") if isinstance(cfg.get("lastAdaptiveLeverage"), dict) else {}
    last_sym = str(last.get("symbol", "") or "").upper().strip()
    try:
        last_lev = int(float(last.get("leverage", 0) or 0))
    except (TypeError, ValueError):
        last_lev = 0
    if last_lev > 0 and sym and sym == last_sym:
        return min(25, last_lev)
    try:
        cfg_lev = int(float(cfg.get("leverage", 0) or 0))
    except (TypeError, ValueError):
        cfg_lev = 0
    return min(25, cfg_lev) if cfg_lev > 0 else None


def _paper_reset():
    AUTO_TRADE["paper"] = {
        "position": None,
        "wins": 0,
        "losses": 0,
        "realizedPnl": 0.0,
        "history": [],
    }


def _paper_close(reason: str, exit_price: float):
    p = AUTO_TRADE["paper"]["position"]
    if not p:
        return None
    side = p["side"]
    entry = float(p["entry"])
    qty = float(p["qty"])
    pnl = (exit_price - entry) * qty if side == "LONG" else (entry - exit_price) * qty
    AUTO_TRADE["paper"]["realizedPnl"] += pnl
    if pnl >= 0:
        AUTO_TRADE["paper"]["wins"] += 1
    else:
        AUTO_TRADE["paper"]["losses"] += 1
    trade = {
        "symbol": p.get("symbol"),
        "side": side,
        "entry": entry,
        "exit": exit_price,
        "qty": qty,
        "pnl": round(pnl, 6),
        "reason": reason,
        "openedAt": p["openedAt"],
        "closedAt": int(time.time()),
        "patternTags": p.get("patternTags", []),
        "patternBias": p.get("patternBias", 0.0),
        "patternScore": p.get("patternScore", 0.0),
        "entryConfidence": p.get("entryConfidence", 0.0),
        "entryScore": p.get("entryScore", 0.0),
        "entrySpreadBps": p.get("entrySpreadBps", 0.0),
        "entryMomentumPct": p.get("entryMomentumPct", 0.0),
    }
    AUTO_TRADE["paper"]["history"] = [trade] + AUTO_TRADE["paper"]["history"][:49]
    AUTO_TRADE["paper"]["position"] = None
    # Obsidian-style learning: persist per-symbol outcomes
    sym = str(trade.get("symbol") or (AUTO_TRADE.get("config") or {}).get("symbol") or "").upper()
    if sym:
        _record_learning_trade(sym, trade, "PAPER")
    _persist_autotrade_snapshot()
    return trade


def _persist_autotrade_snapshot(force: bool = False):
    global _SNAPSHOT_LAST_FLUSH
    if not force:
        now = time.time()
        if (now - _SNAPSHOT_LAST_FLUSH) < 30.0:
            return
    try:
        # Stale lock cleanup: remove locks whose updatedAt is > 2 hours old
        # (position was likely closed externally without Guardian noticing).
        _locks_data = AUTO_TRADE.get("liveProfitLocks")
        if isinstance(_locks_data, dict) and _locks_data:
            _now_ts = int(time.time())
            _stale_keys = [
                k for k, v in _locks_data.items()
                if isinstance(v, dict) and (_now_ts - int(v.get("updatedAt", 0) or 0)) > 7200
            ]
            if _stale_keys:
                for _sk in _stale_keys:
                    _locks_data.pop(_sk, None)
                _autotrade_log(f"[Snapshot] Cleaned {len(_stale_keys)} stale lock(s): {', '.join(_stale_keys)}")
        payload = {
            "savedAt": int(time.time()),
            "paper": dict(AUTO_TRADE["paper"]),
            "config": AUTO_TRADE.get("config"),
            "running": bool(AUTO_TRADE.get("running")),
            "pauseUntil": int(AUTO_TRADE.get("pauseUntil", 0) or 0),
            "riskCooldownLossSignature": str(AUTO_TRADE.get("riskCooldownLossSignature", "") or ""),
            "riskCooldownBySymbol": _prune_risk_cooldowns(),
            "riskCooldownLastMarketCheckAt": int(AUTO_TRADE.get("riskCooldownLastMarketCheckAt", 0) or 0),
            "sessionId": AUTO_TRADE.get("sessionId"),
            "startedAt": AUTO_TRADE.get("startedAt", 0),
            "lastTradeAt": AUTO_TRADE.get("lastTradeAt", 0),
            "liveProfitLocks": AUTO_TRADE.get("liveProfitLocks"),
            "scanBoard": list(AUTO_TRADE.get("scanBoard", []))[:10],
            "cooldownWatchlist": AUTO_TRADE.get("cooldownWatchlist") if isinstance(AUTO_TRADE.get("cooldownWatchlist"), dict) else {},
            "hermesAgents": ensure_agent_state(AUTO_TRADE.get("hermesAgents")),
            "hermesSupervisorReview": AUTO_TRADE.get("hermesSupervisorReview") if isinstance(AUTO_TRADE.get("hermesSupervisorReview"), dict) else {},
            "trades": list(AUTO_TRADE.get("trades", []))[-60:],
            "dailyRealizedPnlUSDT": DAILY_REALIZED_PNL,
            "dailyPnlDateKey": _DAILY_PNL_DATE_KEY,
        }
        raw = json.dumps(payload, ensure_ascii=False, default=str)
        if len(raw.encode("utf-8")) > 2 * 1024 * 1024:
            print("[Snapshot] WARN: payload > 2MB, trimming trades further")
            payload["trades"] = list(AUTO_TRADE.get("trades", []))[-20:]
            raw = json.dumps(payload, ensure_ascii=False, default=str)
        tmp_path = SNAPSHOT_PATH.with_suffix(".tmp")
        tmp_path.write_text(raw, encoding="utf-8")
        tmp_path.replace(SNAPSHOT_PATH)
        AUTO_TRADE["_snapshot_saved_at"] = payload["savedAt"]
        _SNAPSHOT_LAST_FLUSH = time.time()
        # Per-symbol runtime split: each symbol's cooldown state lives on its
        # own file so a single symbol cannot corrupt the shared global snapshot.
        try:
            from trading.per_symbol_context import PerSymbolContext
            from trading.shared_cache_layer import get_shared_cache
            _rc = AUTO_TRADE.get("riskCooldownBySymbol")
            if isinstance(_rc, dict) and _rc:
                _rcache = get_shared_cache(VAULT_DIR)
                for _sym, _cd in _rc.items():
                    if not isinstance(_cd, dict):
                        continue
                    _s = str(_sym).upper().strip()
                    if ":" in _s:
                        continue
                    try:
                        _ctx = PerSymbolContext(_s, _rcache, AUTO_TRADE.get("config"))
                        _rt = _ctx.get_runtime()
                        if not isinstance(_rt, dict):
                            _rt = {}
                        _rt["riskCooldown"] = _cd
                        _ctx.save_runtime(_rt)
                    except Exception:
                        continue
        except Exception:
            pass
    except Exception as exc:
        print(f"[Snapshot] ERROR writing {SNAPSHOT_PATH}: {exc}")


async def _flush_snapshot_async(force: bool = False):
    """Async wrapper for snapshot flush (for use inside async endpoints)."""
    await asyncio.to_thread(_persist_autotrade_snapshot, force=force)


def _load_autotrade_snapshot():
    AUTO_TRADE["_snapshot_recovered_log"] = None
    AUTO_TRADE["_snapshot_loaded_at"] = None
    if not SNAPSHOT_PATH.exists():
        return
    try:
        data = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
        paper = data.get("paper")
        if isinstance(paper, dict):
            pos = paper.get("position")
            if pos is not None and not isinstance(pos, dict):
                pos = None
            hist = paper.get("history")
            if not isinstance(hist, list):
                hist = []
            hist = hist[:50]
            AUTO_TRADE["paper"] = {
                "position": pos,
                "wins": int(paper.get("wins", 0)),
                "losses": int(paper.get("losses", 0)),
                "realizedPnl": float(paper.get("realizedPnl", 0.0)),
                "history": hist,
            }
            if AUTO_TRADE["paper"]["position"] and not AUTO_TRADE["paper"]["position"].get("symbol"):
                sym0 = None
                if isinstance(data.get("config"), dict):
                    sym0 = data["config"].get("symbol")
                if sym0:
                    try:
                        AUTO_TRADE["paper"]["position"]["symbol"] = _normalize_symbol(str(sym0))
                    except HTTPException:
                        pass
        sb = data.get("scanBoard")
        AUTO_TRADE["scanBoard"] = sb[:10] if isinstance(sb, list) else []
        cw = data.get("cooldownWatchlist")
        AUTO_TRADE["cooldownWatchlist"] = cw if isinstance(cw, dict) else {}
        cooldown_watchlist = data.get("cooldownWatchlist")
        AUTO_TRADE["cooldownWatchlist"] = cooldown_watchlist if isinstance(cooldown_watchlist, dict) else {}
        review = data.get("hermesSupervisorReview")
        AUTO_TRADE["hermesSupervisorReview"] = review if isinstance(review, dict) else {}
        AUTO_TRADE["hermesAgents"] = ensure_agent_state(data.get("hermesAgents"))
        AUTO_TRADE["pauseUntil"] = int(data.get("pauseUntil", 0) or 0)
        AUTO_TRADE["riskCooldownLossSignature"] = str(data.get("riskCooldownLossSignature", "") or "")
        rcbs = data.get("riskCooldownBySymbol")
        AUTO_TRADE["riskCooldownBySymbol"] = rcbs if isinstance(rcbs, dict) else {}
        _prune_risk_cooldowns()
        AUTO_TRADE["riskCooldownLastMarketCheckAt"] = int(data.get("riskCooldownLastMarketCheckAt", 0) or 0)
        lks = data.get("liveProfitLocks")
        AUTO_TRADE["liveProfitLocks"] = lks if isinstance(lks, dict) else {}
        # Per-symbol risk-cooldown restore: supplement the global snapshot with
        # each symbol's independent runtime copy (only keys missing globally).
        try:
            from trading.per_symbol_context import PerSymbolContext
            from trading.shared_cache_layer import get_shared_cache
            _rcache = get_shared_cache(VAULT_DIR)
            _rcd = AUTO_TRADE.get("riskCooldownBySymbol")
            if not isinstance(_rcd, dict):
                _rcd = {}
                AUTO_TRADE["riskCooldownBySymbol"] = _rcd
            _sym_root = VAULT_DIR / "symbols"
            if _sym_root.is_dir():
                for _d in _sym_root.iterdir():
                    if not _d.is_dir():
                        continue
                    _sym = _d.name.upper()
                    try:
                        _ctx = PerSymbolContext(_sym, _rcache, AUTO_TRADE.get("config"))
                        _rt = _ctx.get_runtime()
                        _cd = _rt.get("riskCooldown") if isinstance(_rt, dict) else None
                        if isinstance(_cd, dict) and _sym not in _rcd:
                            _rcd[_sym] = _cd
                    except Exception:
                        continue
        except Exception:
            pass
        # Per-symbol guardian lock restore: supplement the global snapshot with
        # each symbol's independent copy (only keys missing from the global
        # snapshot). The global value always wins so this is a safe additive
        # restore during the per-symbol transition.
        try:
            from trading.per_symbol_context import PerSymbolContext
            from trading.shared_cache_layer import get_shared_cache
            _gcache = get_shared_cache(VAULT_DIR)
            _sym_root = VAULT_DIR / "symbols"
            if _sym_root.is_dir():
                for _d in _sym_root.iterdir():
                    if not _d.is_dir():
                        continue
                    _sym = _d.name.upper()
                    try:
                        _ctx = PerSymbolContext(_sym, _gcache, AUTO_TRADE.get("config"))
                        _plk = _ctx.get_guardian_lock()
                        if isinstance(_plk, dict) and _plk:
                            _pkey = f"{_sym}:{str(_plk.get('side', '')).upper()}"
                            if _pkey not in AUTO_TRADE["liveProfitLocks"]:
                                AUTO_TRADE["liveProfitLocks"][_pkey] = _plk
                    except Exception:
                        continue
        except Exception:
            pass
        saved_daily_pnl = float(data.get("dailyRealizedPnlUSDT", 0.0) or 0.0)
        saved_daily_date_key = data.get("dailyPnlDateKey", 0)
        today_tloc = time.localtime()
        today_date_key = (today_tloc.tm_year, today_tloc.tm_mon, today_tloc.tm_mday)
        if isinstance(saved_daily_date_key, list) and len(saved_daily_date_key) == 3:
            restored_key = tuple(saved_daily_date_key)
        else:
            restored_key = 0
        if restored_key == today_date_key:
            DAILY_REALIZED_PNL = saved_daily_pnl
            _DAILY_PNL_DATE_KEY = today_date_key
        saved = int(data.get("savedAt", 0) or 0)
        was_running = bool(data.get("running"))
        sym = None
        if isinstance(data.get("config"), dict):
            sym = data["config"].get("symbol")
        if not sym and isinstance(AUTO_TRADE["paper"].get("position"), dict):
            sym = AUTO_TRADE["paper"]["position"].get("symbol")
        parts = ["Snapshot restored from disk after server restart."]
        if AUTO_TRADE["paper"].get("position"):
            p0 = AUTO_TRADE["paper"]["position"]
            parts.append(
                f"PAPER {p0.get('symbol', '?')} {p0.get('side')} position restored."
            )
        if was_running and isinstance(data.get("config"), dict):
            sym_cfg = data["config"].get("symbol", "?")
            mode_cfg = data["config"].get("executionMode", "PAPER")
            parts.append(f"AutoTrade will auto-resume for {sym_cfg} ({mode_cfg}).")
        elif was_running:
            parts.append("Last run was active — will auto-resume if config is valid.")
        if saved:
            parts.append(f"(savedAt={saved})")
        msg = " ".join(parts)
        AUTO_TRADE["_snapshot_recovered_log"] = msg
        AUTO_TRADE["_snapshot_loaded_at"] = int(time.time())
        AUTO_TRADE["log"] = (
            [{"ts": AUTO_TRADE["_snapshot_loaded_at"], "msg": msg}] + AUTO_TRADE.get("log", [])
        )[:80]
        # Schedule startup reconcile: remove stale liveProfitLocks after snapshot load.
        locks_data = AUTO_TRADE.get("liveProfitLocks") if isinstance(AUTO_TRADE.get("liveProfitLocks"), dict) else {}
        if locks_data:
            async def _startup_reconcile():
                try:
                    await asyncio.sleep(5.0)
                    bkey = os.getenv("BINANCE_API_KEY")
                    bsecret = os.getenv("BINANCE_API_SECRET")
                    bbase = _binance_base()
                    if not (bkey and bsecret):
                        return
                    live_pos = await asyncio.wait_for(_pick_live_orphan_positions(bkey, bsecret, bbase), timeout=8.0)
                    live_by_key = {}
                    for p in live_pos:
                        k = _live_lock_key(str(p.get("symbol", "")), str(p.get("side", "")))
                        live_by_key[k] = p
                    live_keys = set(live_by_key.keys())
                    lk = AUTO_TRADE.get("liveProfitLocks") if isinstance(AUTO_TRADE.get("liveProfitLocks"), dict) else {}
                    stale = [k for k in list(lk.keys()) if k not in live_keys]
                    price_mismatch = []
                    for k in list(lk.keys()):
                        if k in live_keys:
                            lp = live_by_key[k]
                            lock_entry = float((lk[k] or {}).get("entryMark", 0) or 0)
                            live_entry = float((lp or {}).get("entryPrice", 0) or (lp or {}).get("entryMark", 0) or 0)
                            if lock_entry > 0 and live_entry > 0 and abs(lock_entry - live_entry) / max(lock_entry, live_entry) > 0.05:
                                stale.append(k)
                                price_mismatch.append(k)
                    for k in stale:
                        lk.pop(k, None)
                    if stale:
                        AUTO_TRADE["liveProfitLocks"] = lk
                        _persist_autotrade_snapshot(force=True)
                        msg_parts = [f"{len(stale)} phantom lock(s)"]
                        if price_mismatch:
                            msg_parts.append(f"{len(price_mismatch)} entry-price mismatch")
                        _autotrade_log(f"[Startup] Cleaned {', '.join(msg_parts)}: {', '.join(stale)}")
                    # Also clean stale per-symbol guardian_lock.json files
                    _live_symbols = {str(p.get("symbol", "")).upper() for p in live_pos if p.get("symbol")}
                    _cleaned_sym = 0
                    try:
                        import os as _os
                        _sym_dir = VAULT_DIR / "symbols"
                        if _sym_dir.is_dir():
                            for _sd in _sym_dir.iterdir():
                                if not _sd.is_dir():
                                    continue
                                _lock_file = _sd / "guardian_lock.json"
                                if not _lock_file.exists():
                                    continue
                                _sym_name = _sd.name.upper()
                                if _sym_name not in _live_symbols:
                                    try:
                                        _lock_file.unlink()
                                        _cleaned_sym += 1
                                    except Exception:
                                        pass
                    except Exception:
                        pass
                    if _cleaned_sym:
                        _autotrade_log(f"[Startup] Cleaned {_cleaned_sym} stale per-symbol lock file(s)")
                except Exception as exc:
                    _autotrade_log(f"[Startup] Reconcile skipped: {exc}")
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(_startup_reconcile())
            except Exception:
                pass
    except Exception as e:
        AUTO_TRADE["_snapshot_recovered_log"] = f"Snapshot unreadable: {_format_loop_error(e)}"
        AUTO_TRADE["_snapshot_loaded_at"] = int(time.time())


def _floor_to_step(value: float, step: float):
    if step <= 0:
        return value
    n = math.floor(value / step)
    return n * step


def _format_qty_by_step(value: float, step_str: str):
    step_dec = Decimal(step_str)
    val_dec = Decimal(str(value))
    q = val_dec.quantize(step_dec, rounding=ROUND_DOWN)
    s = format(q, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _round_to_tick(value: float, tick: float):
    if tick <= 0:
        return value
    n = round(value / tick)
    return n * tick


def _format_price_by_tick(value: float, tick_str: str):
    tick_dec = Decimal(tick_str)
    val_dec = Decimal(str(value))
    q = val_dec.quantize(tick_dec, rounding=ROUND_DOWN)
    s = format(q, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _within_no_trade_window(now_local: time.struct_time, windows: list[str]):
    hhmm = now_local.tm_hour * 60 + now_local.tm_min
    for w in windows:
        # Format: HH:MM-HH:MM, local server timezone
        m = re.fullmatch(r"(\d{2}):(\d{2})-(\d{2}):(\d{2})", w.strip())
        if not m:
            continue
        s_h, s_m, e_h, e_m = [int(x) for x in m.groups()]
        start = s_h * 60 + s_m
        end = e_h * 60 + e_m
        if start <= end:
            if start <= hhmm < end:
                return True
        else:
            # Overnight window, e.g. 23:00-01:00
            if hhmm >= start or hhmm < end:
                return True
    return False


async def _best_bid_ask(symbol: str):
    res = await _data_get(f"/fapi/v1/ticker/bookTicker?symbol={symbol}")
    if res.status_code >= 400:
        raise HTTPException(status_code=res.status_code, detail=res.text)
    data = res.json()
    bid = float(data.get("bidPrice", 0))
    ask = float(data.get("askPrice", 0))
    if bid <= 0 or ask <= 0:
        raise HTTPException(status_code=400, detail="Invalid bid/ask from exchange")
    return bid, ask


def _get_um_client(key: str, secret: str, base: str):
    if CONNECTOR_MODE == "legacy":
        return None
    connector_cls = _resolve_umfutures_class()
    if connector_cls is None:
        if CONNECTOR_MODE == "official":
            raise HTTPException(status_code=500, detail="Official connector mode enabled but UMFutures not available")
        return None
    return connector_cls(key=key, secret=secret, base_url=base)


async def _set_leverage_margin(symbol: str, key: str, secret: str, base: str, leverage: int, margin_type: str):
    async def _margin_state():
        try:
            pos = await _signed_request("GET", base, "/fapi/v2/positionRisk", key, secret, {"symbol": symbol})
            rows = pos if isinstance(pos, list) else ([pos] if isinstance(pos, dict) else [])
            if not rows:
                return {"hasPosition": False, "marginType": None}
            r0 = rows[0]
            amt = float(r0.get("positionAmt", 0) or 0)
            # USD-M returns 'isolated' boolean-like field; infer current margin mode.
            iso_raw = r0.get("isolated")
            is_iso = str(iso_raw).lower() in ("true", "1")
            cur = "ISOLATED" if is_iso else "CROSSED"
            return {"hasPosition": abs(amt) > 0, "marginType": cur}
        except Exception:
            return {"hasPosition": False, "marginType": None}

    client = _get_um_client(key, secret, base)
    def _is_non_blocking_margin_error(text: str):
        # -4046: No need to change (already set)
        return ("No need to change margin type" in text)

    # Binance limitation: Multi-Assets mode cannot use ISOLATED margin.
    if margin_type == "ISOLATED":
        try:
            ma = await _signed_request("GET", base, "/fapi/v1/multiAssetsMargin", key, secret, {})
            ma_on = str((ma or {}).get("multiAssetsMargin", "")).lower() in ("true", "1")
            if ma_on:
                margin_type = "CROSSED"
                _autotrade_log("Margin override: Multi-Assets mode active -> force CROSSED (ISOLATED not allowed)")
        except Exception as exc:
            print(f"[Trade Log] ERROR writing {TRADES_LOG_PATH}: {exc}")

    st = await _margin_state()
    cur = st.get("marginType")
    has_pos = bool(st.get("hasPosition"))
    if cur in ("ISOLATED", "CROSSED") and cur != margin_type and has_pos:
        raise HTTPException(
            status_code=409,
            detail=f"Margin currently {cur} with open position. Close position first to switch to {margin_type}.",
        )

    if client:
        await asyncio.to_thread(client.change_leverage, symbol=symbol, leverage=leverage)
        try:
            await asyncio.to_thread(client.change_margin_type, symbol=symbol, marginType=margin_type)
        except Exception as e:
            txt = str(e)
            if "-4168" in txt:
                _autotrade_log("Margin override: exchange rejected ISOLATED under Multi-Assets -> continue with CROSSED")
                return
            if not _is_non_blocking_margin_error(txt):
                raise
        return
    await _signed_request("POST", base, "/fapi/v1/leverage", key, secret, {"symbol": symbol, "leverage": leverage})
    try:
        await _signed_request("POST", base, "/fapi/v1/marginType", key, secret, {"symbol": symbol, "marginType": margin_type})
    except HTTPException as e:
        detail = str(e.detail)
        if "-4168" in detail:
            _autotrade_log("Margin override: exchange rejected ISOLATED under Multi-Assets -> continue with CROSSED")
            return
        if not _is_non_blocking_margin_error(detail):
            raise


async def _place_tp_sl(symbol: str, side: str, qty: float, entry_mark: float, tp_pct: float, sl_pct: float, key: str, secret: str, base: str, tick_size: float, tick_size_str: str, hedge_mode: bool, position_side: str | None):
    close_side = "SELL" if side == "LONG" else "BUY"
    tp_price = entry_mark * (1 + tp_pct / 100) if side == "LONG" else entry_mark * (1 - tp_pct / 100)
    sl_price = entry_mark * (1 - sl_pct / 100) if side == "LONG" else entry_mark * (1 + sl_pct / 100)
    tp_price = _round_to_tick(tp_price, tick_size)
    sl_price = _round_to_tick(sl_price, tick_size)
    tp_price_str = _format_price_by_tick(tp_price, tick_size_str)
    sl_price_str = _format_price_by_tick(sl_price, tick_size_str)

    async def _submit_exit_order(kind: str, stop_price_str: str):
        market_type = "TAKE_PROFIT_MARKET" if kind == "tp" else "STOP_MARKET"
        limit_type = "TAKE_PROFIT" if kind == "tp" else "STOP"

        primary = {
            "symbol": symbol,
            "side": close_side,
            "type": market_type,
            "stopPrice": stop_price_str,
            "workingType": "MARK_PRICE",
        }
        fallback = {
            "symbol": symbol,
            "side": close_side,
            "type": limit_type,
            "stopPrice": stop_price_str,
            "price": stop_price_str,
            "timeInForce": "GTC",
            "workingType": "MARK_PRICE",
        }
        if hedge_mode and position_side:
            primary["positionSide"] = position_side
            primary["quantity"] = str(qty)
            fallback["positionSide"] = position_side
            fallback["quantity"] = str(qty)
        else:
            primary["closePosition"] = "true"
            fallback["closePosition"] = "true"

        client = _get_um_client(key, secret, base)
        try:
            if client:
                return await asyncio.to_thread(client.new_order, **primary)
            return await _signed_request("POST", base, "/fapi/v1/order", key, secret, primary)
        except Exception as e:
            txt = str(e)
            if ("-4120" not in txt) and ("Order type not supported" not in txt):
                raise
            if client:
                return await asyncio.to_thread(client.new_order, **fallback)
            return await _signed_request("POST", base, "/fapi/v1/order", key, secret, fallback)

    tp = await _submit_exit_order("tp", tp_price_str)
    sl = await _submit_exit_order("sl", sl_price_str)
    return {"tp": tp, "sl": sl}


async def _place_trailing_stop(symbol: str, side: str, key: str, secret: str, base: str, trailing_pct: float):
    if trailing_pct <= 0:
        return None
    close_side = "SELL" if side == "LONG" else "BUY"
    callback_rate = max(0.1, min(10.0, trailing_pct))

    client = _get_um_client(key, secret, base)
    if client:
        return await asyncio.to_thread(
            client.new_order,
            symbol=symbol,
            side=close_side,
            type="TRAILING_STOP_MARKET",
            callbackRate=callback_rate,
            workingType="MARK_PRICE",
            reduceOnly="true",
        )
    return await _signed_request("POST", base, "/fapi/v1/order", key, secret, {
        "symbol": symbol,
        "side": close_side,
        "type": "TRAILING_STOP_MARKET",
        "callbackRate": callback_rate,
        "workingType": "MARK_PRICE",
        "reduceOnly": "true",
    })


def _extract_fill_price(order_resp: dict | list | None) -> float | None:
    """Extract average fill price from Binance order response."""
    if order_resp is None:
        return None
    if isinstance(order_resp, list):
        if not order_resp:
            return None
        order_resp = order_resp[0]
    if not isinstance(order_resp, dict):
        return None
    for k in ("avgPrice", "price", "executedPrice"):
        val = order_resp.get(k)
        if val is not None:
            try:
                v = float(val)
                if v > 0:
                    return v
            except (TypeError, ValueError):
                continue
    fills = order_resp.get("fills")
    if isinstance(fills, list) and fills:
        total_qty = 0.0
        weighted_px = 0.0
        for f in fills:
            if not isinstance(f, dict):
                continue
            p = float(f.get("price", 0) or 0)
            q = float(f.get("qty", 0) or 0)
            if p > 0 and q > 0:
                weighted_px += p * q
                total_qty += q
        if total_qty > 0:
            return weighted_px / total_qty
    return None


async def _cancel_all_open_orders(symbol: str, key: str, secret: str, base: str):
    """Cancel all open orders (regular AND algo/conditional) for a symbol."""
    try:
        client = _get_um_client(key, secret, base)
        if client:
            await asyncio.to_thread(client.cancel_all_open_orders, symbol=symbol)
        else:
            await _signed_request("DELETE", base, "/fapi/v1/allOpenOrders", key, secret, {"symbol": symbol})
    except Exception as exc:
        print(f"[Cancel Orders] {symbol} regular warning: {exc}")
    try:
        await _signed_request("DELETE", base, "/fapi/v1/algoOpenOrders", key, secret, {"symbol": symbol})
    except Exception as exc:
        print(f"[Cancel Orders] {symbol} algo warning: {exc}")


async def _close_position(symbol: str, key: str, secret: str, base: str):
    hedge_mode = await _is_hedge_mode(key, secret, base)
    close_mark = await fetch_mark_price(symbol)
    client = _get_um_client(key, secret, base)
    if client:
        pos = await asyncio.to_thread(client.get_position_risk, symbol=symbol)
    else:
        pos = await _signed_request("GET", base, "/fapi/v2/positionRisk", key, secret, {"symbol": symbol})
    if isinstance(pos, dict):
        pos = [pos]
    if not isinstance(pos, list):
        pos = []
    await _cancel_all_open_orders(symbol, key, secret, base)
    close_results = []
    learned_trades = []
    for p in pos:
        amt = float(p.get("positionAmt", 0) or 0)
        if amt == 0:
            continue
        entry = float(p.get("entryPrice", 0) or 0)
        pos_side = (p.get("positionSide") or ("LONG" if amt > 0 else "SHORT")).upper()
        side = "SELL" if amt > 0 else "BUY"
        qty = abs(amt)
        payload = {"symbol": symbol, "side": side, "type": "MARKET", "quantity": str(qty)}
        if hedge_mode:
            ps = (p.get("positionSide") or "").upper()
            if ps in ("LONG", "SHORT"):
                payload["positionSide"] = ps
        else:
            payload["reduceOnly"] = "true"
        if client:
            order_resp = await asyncio.to_thread(client.new_order, **payload)
        else:
            order_resp = await _signed_request("POST", base, "/fapi/v1/order", key, secret, payload)
        close_results.append(order_resp)
        if entry > 0 and qty > 0:
            fill_px = _extract_fill_price(order_resp)
            exit_px = fill_px if fill_px and fill_px > 0 else close_mark
            pnl = (exit_px - entry) * qty if pos_side == "LONG" else (entry - exit_px) * qty
            entry_snapshot = _entry_snapshot_for_position(symbol, pos_side)
            learned_trades.append({
                "side": pos_side,
                "entry": entry,
                "exit": exit_px,
                "qty": qty,
                "pnl": round(float(pnl), 6),
                "reason": "LIVE_CLOSE",
                "closedAt": int(time.time()),
                "patternTags": entry_snapshot.get("patternTags", []),
                "patternBias": entry_snapshot.get("patternBias", 0.0),
                "patternScore": entry_snapshot.get("patternScore", 0.0),
                "entryConfidence": entry_snapshot.get("entryConfidence", 0.0),
                "entryScore": entry_snapshot.get("entryScore", 0.0),
                "entrySpreadBps": entry_snapshot.get("entrySpreadBps", 0.0),
                "entryMomentumPct": entry_snapshot.get("entryMomentumPct", 0.0),
                "entryDecisionAt": entry_snapshot.get("entryDecisionAt", 0),
            })
    if not close_results:
        return {"message": "No open position"}
    for t in learned_trades:
        _record_learning_trade(symbol, t, "LIVE")
    return {"closed": close_results}


async def _close_position_one_side(symbol: str, side_to_close: str, key: str, secret: str, base: str, reason: str = "LIVE_CUT_LOSING_SIDE"):
    target = side_to_close.upper()
    if target not in ("LONG", "SHORT"):
        raise HTTPException(status_code=400, detail="side_to_close must be LONG or SHORT")
    hedge_mode = await _is_hedge_mode(key, secret, base)
    close_mark = await fetch_mark_price(symbol)
    client = _get_um_client(key, secret, base)
    if client:
        pos = await asyncio.to_thread(client.get_position_risk, symbol=symbol)
    else:
        pos = await _signed_request("GET", base, "/fapi/v2/positionRisk", key, secret, {"symbol": symbol})
    rows = pos if isinstance(pos, list) else ([pos] if isinstance(pos, dict) else [])
    await _cancel_all_open_orders(symbol, key, secret, base)
    closed = []
    learned = []
    for p in rows:
        amt = float(p.get("positionAmt", 0) or 0)
        if amt == 0:
            continue
        ps = (p.get("positionSide") or ("LONG" if amt > 0 else "SHORT")).upper()
        if ps != target:
            continue
        side = "SELL" if amt > 0 else "BUY"
        qty = abs(amt)
        payload = {"symbol": symbol, "side": side, "type": "MARKET", "quantity": str(qty)}
        if hedge_mode:
            payload["positionSide"] = ps
        else:
            payload["reduceOnly"] = "true"
        if client:
            order_resp = await asyncio.to_thread(client.new_order, **payload)
        else:
            order_resp = await _signed_request("POST", base, "/fapi/v1/order", key, secret, payload)
        closed.append(order_resp)
        entry = float(p.get("entryPrice", 0) or 0)
        if entry > 0 and qty > 0:
            fill_px = _extract_fill_price(order_resp)
            exit_px = fill_px if fill_px and fill_px > 0 else close_mark
            pnl = (exit_px - entry) * qty if ps == "LONG" else (entry - exit_px) * qty
            entry_snapshot = _entry_snapshot_for_position(symbol, ps)
            learned.append({
                "side": ps,
                "entry": entry,
                "exit": exit_px,
                "qty": qty,
                "pnl": round(float(pnl), 6),
                "reason": reason,
                "closedAt": int(time.time()),
                "patternTags": entry_snapshot.get("patternTags", []),
                "patternBias": entry_snapshot.get("patternBias", 0.0),
                "patternScore": entry_snapshot.get("patternScore", 0.0),
                "entryConfidence": entry_snapshot.get("entryConfidence", 0.0),
                "entryScore": entry_snapshot.get("entryScore", 0.0),
                "entrySpreadBps": entry_snapshot.get("entrySpreadBps", 0.0),
                "entryMomentumPct": entry_snapshot.get("entryMomentumPct", 0.0),
                "entryDecisionAt": entry_snapshot.get("entryDecisionAt", 0),
            })
    for t in learned:
        _record_learning_trade(symbol, t, "LIVE")
    return {"closed": closed}


async def place_futures_order(symbol: str, side: str, quantity: float | None = None, usdt_amount: float | None = None, leverage: int | None = None, margin_type: str | None = None, tp_pct: float | None = None, sl_pct: float | None = None, trailing_stop_pct: float = 0.0):
    symbol = _normalize_symbol(symbol)
    leverage = leverage or DEFAULT_LEVERAGE
    margin_type = (margin_type or DEFAULT_MARGIN_TYPE).upper()
    tp_pct = tp_pct if tp_pct is not None else DEFAULT_TP_PCT
    sl_pct = sl_pct if sl_pct is not None else DEFAULT_SL_PCT

    key = os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_API_SECRET")
    base = _binance_base()

    mark = await fetch_mark_price(symbol)
    if quantity is None and usdt_amount is None:
        raise HTTPException(status_code=400, detail="Please provide quantity or usdtAmount")
    if quantity is None and usdt_amount is not None:
        quantity = usdt_amount / max(mark, 1e-9)
    if quantity is None:
        raise HTTPException(status_code=400, detail="Invalid quantity")

    _guardrails(mark, quantity, leverage)

    filters = await _exchange_filters(symbol)
    qty = _floor_to_step(quantity, filters["stepSize"])
    qty_str = _format_qty_by_step(qty, filters.get("stepSizeStr", "0.001"))
    qty = float(qty_str)
    if qty < filters["minQty"]:
        min_usdt = filters["minQty"] * mark
        raise HTTPException(
            status_code=400,
            detail={
                "code": "QTY_TOO_SMALL",
                "message": f"มูลค่า USDT ต่ำเกินไปสำหรับ {symbol}",
                "minQty": filters["minQty"],
                "requiredMinUsdtApprox": round(min_usdt, 4),
                "inputUsdtAmount": usdt_amount,
            },
        )

    if not key or not secret:
        return {"mode": "mock", "symbol": symbol, "side": side, "quantity": qty, "usdtAmount": usdt_amount, "leverage": leverage, "marginType": margin_type, "tpPct": tp_pct, "slPct": sl_pct, "trailingStopPct": trailing_stop_pct}

    if side == "WAIT":
        return {"mode": "noop", "message": "WAIT action does not place an order."}

    if side == "CLOSE":
        return {"mode": "live", "response": await _close_position(symbol, key, secret, base)}

    hedge_mode = await _is_hedge_mode(key, secret, base)
    position_side = "LONG" if side == "LONG" else "SHORT"
    order_side = "BUY" if side == "LONG" else "SELL" if side == "SHORT" else None
    if not order_side:
        raise HTTPException(status_code=400, detail="Invalid side")

    await _set_leverage_margin(symbol, key, secret, base, int(leverage), margin_type)

    client = _get_um_client(key, secret, base)
    entry = None
    last_err = None
    qty_candidates = _qty_retry_candidates(qty, filters.get("stepSizeStr", "0.001"), int(filters.get("qtyPrecision", 3)), float(filters.get("minQty", 0.0)))
    for qtry in qty_candidates:
        try:
            if client:
                entry_params = {"symbol": symbol, "side": order_side, "type": "MARKET", "quantity": qtry}
                if hedge_mode:
                    entry_params["positionSide"] = position_side
                entry = await asyncio.to_thread(client.new_order, **entry_params)
            else:
                entry_payload = {
                    "symbol": symbol,
                    "side": order_side,
                    "type": "MARKET",
                    "quantity": qtry,
                }
                if hedge_mode:
                    entry_payload["positionSide"] = position_side
                entry = await _signed_request("POST", base, "/fapi/v1/order", key, secret, entry_payload)
            qty_str = qtry
            qty = float(qtry)
            break
        except Exception as e:
            last_err = e
            txt = str(e)
            if ("-1111" in txt) or ("Precision is over the maximum" in txt):
                continue
            raise
    if entry is None and last_err is not None:
        raise last_err

    protective = None
    entry_snapshot = _entry_snapshot_from_intel(symbol, side, _last_decision_intel(symbol))
    try:
        from trading.symbol_autotuner import snapshot_active_params
        _eff_at_open = _effective_tp_sl(symbol, AUTO_TRADE.get("config") or {}, _last_decision_intel(symbol))
        entry_snapshot["params_at_entry"] = snapshot_active_params(symbol, _eff_at_open)
    except Exception:
        pass
    try:
        protective = await _place_tp_sl(symbol, side, qty, mark, tp_pct, sl_pct, key, secret, base, filters["tickSize"], filters.get("tickSizeStr", "0.0001"), hedge_mode, position_side)
    except Exception as e:
        protective = {"warning": str(e)}
    if isinstance(protective, dict) and protective.get("warning"):
        tp_price, sl_price = _calc_tp_sl_prices(side, mark, tp_pct, sl_pct)
        lock_key = f"{symbol}:{side}"
        locks = AUTO_TRADE.get("liveProfitLocks") if isinstance(AUTO_TRADE.get("liveProfitLocks"), dict) else {}
        # Preserve existing Guardian-updated fields (peak, lockUsdt, guardianStats, etc.)
        # instead of overwriting with defaults.  See: peak-0.0 root-cause fix.
        _existing_lock = locks.get(lock_key, {})
        locks[lock_key] = {
            **_existing_lock,
            "armed": _existing_lock.get("armed", False),
            "peak": _existing_lock.get("peak", 0.0),
            "lockUsdt": _existing_lock.get("lockUsdt", 0.0),
            "symbol": symbol,
            "side": side,
            "qty": round(float(qty), 10),
            "leverage": int(leverage),
            "entryMark": round(float(mark), 10),
            "tp": round(float(tp_price), 10),
            "sl": round(float(sl_price), 10),
            "entryTPPct": float(tp_pct),
            "entrySLPct": float(sl_pct),
            "entrySnapshot": entry_snapshot,
            "updatedAt": int(time.time()),
        }
        AUTO_TRADE["liveProfitLocks"] = locks
        _autotrade_log(f"LIVE profit lock seeded for {symbol} {side} TP={tp_price:.6f} SL={sl_price:.6f}")
    trailing = await _place_trailing_stop(symbol, side, key, secret, base, trailing_stop_pct)
    return {"mode": "live", "entry": entry, "protective": protective, "localGuardian": None, "trailing": trailing, "entrySnapshot": entry_snapshot}


@app.post("/trade")
async def trade(req: TradeRequest):
    try:
        return await place_futures_order(
            req.symbol,
            req.side,
            quantity=req.qty,
            usdt_amount=getattr(req, "usdtAmount", None),
            leverage=getattr(req, "leverage", None),
            margin_type=getattr(req, "marginType", None),
            tp_pct=req.takeProfitPct,
            sl_pct=req.stopLossPct,
            trailing_stop_pct=0.0,
        )
    except HTTPException:
        raise
    except Exception as err:
        mapped = str(err)
        raise HTTPException(status_code=400, detail=mapped)


@app.post("/autotrade/config")
async def autotrade_update_config(payload: dict = Body(default_factory=dict)):
    cur = dict(AUTO_TRADE.get("config") or {})
    if not cur:
        return {"ok": False, "updated": False, "reason": "NO_ACTIVE_CONFIG"}
    cur.update(payload or {})
    lev_min = int(cur.get("leverageMin", cur.get("leverage", 5)) or 1)
    lev_max = int(cur.get("leverageMax", max(lev_min, cur.get("leverage", 5))) or lev_min)
    if lev_min > lev_max:
        return {"ok": False, "updated": False, "reason": f"Leverage range invalid: min {lev_min} > max {lev_max}"}
    max_cap = max(_autotrade_leverage_cap(), min(25, max(lev_min, lev_max)))
    RISK["max_leverage"] = max_cap
    if lev_max > max_cap:
        return {"ok": False, "updated": False, "reason": f"Leverage max {lev_max} exceeds server max {max_cap}"}
    cur["leverageMin"] = lev_min
    cur["leverageMax"] = lev_max
    try:
        adaptive_max = int(cur.get("adaptiveLeverageMax", lev_max) or lev_max)
    except (TypeError, ValueError):
        adaptive_max = lev_max
    if "adaptiveLeverageMax" not in (payload or {}) and adaptive_max < lev_max:
        adaptive_max = lev_max
    cur["adaptiveLeverageMax"] = int(max(lev_min, min(max_cap, adaptive_max)))
    cur["adaptiveLeverageEnabled"] = bool(cur.get("adaptiveLeverageEnabled", True))
    cur["leverage"] = int(max(lev_min, min(lev_max, int(cur.get("leverage", lev_min) or lev_min), 25)))
    AUTO_TRADE["config"] = copy.deepcopy(cur)
    _autotrade_log(
        f"AutoTrade config updated: lev={cur['leverage']} range={cur['leverageMin']}-{cur['leverageMax']} TP={cur.get('takeProfitPct')} SL={cur.get('stopLossPct')}"
    )
    _persist_autotrade_snapshot()
    return {"ok": True, "updated": True, "config": cur}


@app.post("/autotrade/adopt-live")
async def autotrade_adopt_live(symbol: str | None = None):
    key = os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_API_SECRET")
    base = _binance_base()
    if not key or not secret:
        return {"ok": False, "reason": "MISSING_API_KEY"}
    adopted_positions = await _pick_live_orphan_positions(key, secret, base)
    if symbol:
        sym = _normalize_symbol(symbol)
        adopted_positions = [p for p in adopted_positions if p.get("symbol") == sym]
    if not adopted_positions:
        return {"ok": False, "reason": "NO_ORPHAN_LIVE"}
    cfg = dict(AUTO_TRADE.get("config") or {})
    if not cfg:
        cfg = AutoTradeStartRequest(usdtAmount=50).model_dump()
    symbols = sorted({str(p.get("symbol", "")).upper() for p in adopted_positions if p.get("symbol")})
    cfg["executionMode"] = "LIVE"
    cfg["orphanAutoAdoptEnabled"] = True
    cfg["orphanAutoAdoptMultiEnabled"] = True
    cfg["orphanAutoAdoptForceSingleSymbol"] = False
    cfg["marketScan"] = True
    cfg["symbol"] = "AUTO"
    # Do not pin scan whitelist to adopted symbols permanently; keep market-wide scan.
    cfg["whitelistSymbols"] = []
    cfg["primarySymbol"] = symbols[0]
    AUTO_TRADE["config"] = copy.deepcopy(cfg)
    _autotrade_log(f"Adopt LIVE: {len(symbols)} symbols [{', '.join(symbols[:8])}]")
    _persist_autotrade_snapshot()
    return {"ok": True, "running": AUTO_TRADE.get("running", False), "config": cfg, "adoptedPositions": adopted_positions}


@app.post("/autotrade/close-orphan")
async def autotrade_close_orphan(symbol: str | None = None):
    key = os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_API_SECRET")
    base = _binance_base()
    if not key or not secret:
        return {"ok": False, "reason": "MISSING_API_KEY"}
    closed = await _close_position(symbol=symbol, key=key, secret=secret, base=base)
    _autotrade_log(f"Close orphan: symbol={symbol or 'ALL'}")
    return {"ok": True, "closed": closed}


@app.post("/autotrade/update-sl-tp")
async def autotrade_update_sl_tp(payload: dict = Body(default_factory=dict)):
    symbol = str(payload.get("symbol", "") or "").upper().strip()
    tp_pct = payload.get("takeProfitPct")
    sl_pct = payload.get("stopLossPct")
    if not symbol:
        return {"ok": False, "reason": "MISSING_SYMBOL"}
    if tp_pct is None and sl_pct is None:
        return {"ok": False, "reason": "MISSING_TP_SL"}

    cfg = dict(AUTO_TRADE.get("config") or {})
    if tp_pct is not None:
        cfg["takeProfitPct"] = float(tp_pct)
    if sl_pct is not None:
        cfg["stopLossPct"] = float(sl_pct)
    AUTO_TRADE["config"] = copy.deepcopy(cfg)

    # Update liveProfitLocks TP/SL when symbol matches.
    locks = AUTO_TRADE.get("liveProfitLocks") if isinstance(AUTO_TRADE.get("liveProfitLocks"), dict) else {}
    for lk, lv in locks.items():
        if isinstance(lv, dict) and str(lv.get("symbol", "")).upper() == symbol:
            lside = str(lv.get("side", "LONG")).upper()
            lentry = float(lv.get("entryMark", 0.0) or 0.0)
            if lentry > 0:
                tp_new, sl_new = _calc_tp_sl_prices(
                    lside,
                    lentry,
                    float(cfg.get("takeProfitPct", 1.8) or 1.8),
                    float(cfg.get("stopLossPct", 0.9) or 0.9),
                )
                lv["tp"] = round(float(tp_new), 10)
                lv["sl"] = round(float(sl_new), 10)
                locks[lk] = lv
                _autotrade_log(f"Update TP/SL {symbol}: TP={tp_new:.6f} SL={sl_new:.6f} ({lside})")
    AUTO_TRADE["liveProfitLocks"] = locks
    _persist_autotrade_snapshot()
    return {
        "ok": True,
        "updated": True,
        "symbol": symbol,
        "takeProfitPct": cfg.get("takeProfitPct"),
        "stopLossPct": cfg.get("stopLossPct"),
    }


async def _autotrade_loop():
    while AUTO_TRADE["running"] or AUTO_TRADE.get("manageOpenOnly"):
        cfg = apply_autotrade_defaults(copy.deepcopy(AUTO_TRADE["config"] or {}))
        AUTO_TRADE["config"] = copy.deepcopy(cfg)
        try:
            now = int(time.time())
            AUTO_TRADE["hermesAgents"] = start_cycle(AUTO_TRADE.get("hermesAgents"))
            _agent_mark("hermes_supervisor", "doing", "cycle started", f"mode={(cfg.get('executionMode') or 'PAPER').upper()}")
            # Periodic supervisor review: run every 90 seconds to avoid excessive overhead
            global _SUPERVISOR_LAST_REVIEW
            if now - _SUPERVISOR_LAST_REVIEW >= 90:
                try:
                    _hermes_supervisor_review()
                    _SUPERVISOR_LAST_REVIEW = now
                except Exception as e:
                    _autotrade_log(f"Supervisor review error: {_format_loop_error(e)[:80]}")
            running_entries = bool(AUTO_TRADE.get("running"))
            risk_cooldown_enabled = bool(cfg.get("riskCooldownEnabled", False))
            if not risk_cooldown_enabled:
                AUTO_TRADE["riskCooldownBySymbol"] = {}
                AUTO_TRADE["riskCooldownLossSignature"] = ""
            else:
                _prune_risk_cooldowns(now)
                # Older snapshots used pauseUntil + riskCooldownLossSignature as a global brake.
                # Convert that legacy state into symbol-scoped cooldown records, then release
                # the global pause so unaffected symbols can keep scanning.
                legacy_sig = str(AUTO_TRADE.get("riskCooldownLossSignature", "") or "")
                legacy_pause = int(AUTO_TRADE.get("pauseUntil", 0) or 0)
                if legacy_sig and legacy_pause > now:
                    symbols = {
                        part.split(":", 1)[0].upper().strip()
                        for part in legacy_sig.split("|")
                        if part.split(":", 1)[0].strip()
                    }
                    if symbols:
                        cool_min = max(1, int((legacy_pause - now + 59) / 60))
                        for sym in symbols:
                            _arm_symbol_risk_cooldown(
                                sym,
                                cool_min,
                                legacy_sig,
                                0,
                                "legacy_global_risk_cooldown",
                                now,
                                _risk_cooldown_signature_last_ts(legacy_sig),
                            )
                    AUTO_TRADE["pauseUntil"] = 0
                    AUTO_TRADE["riskCooldownLossSignature"] = ""
            pause_until = int(AUTO_TRADE.get("pauseUntil", 0) or 0)
            live_position_managed = False
            # Always run Guardian for existing live positions — even in PAPER mode.
            # PAPER mode only prevents new entries; existing positions must still be managed.
            closed_by_guardian = await _manage_live_open_positions_once(cfg, now)
            AUTO_TRADE["_guardianMonitorTs"] = now
            live_position_managed = True
            if closed_by_guardian:
                await asyncio.sleep(cfg["intervalSec"])
                continue
            if risk_cooldown_enabled and running_entries and pause_until > now and not str(AUTO_TRADE.get("riskCooldownLossSignature", "") or ""):
                remain = max(1, pause_until - now)
                check_sec = max(10, int(cfg.get("riskCooldownAdaptiveCheckSec", max(20, int(cfg.get("intervalSec", 20) or 20))) or 20))
                last_check_at = int(AUTO_TRADE.get("riskCooldownLastMarketCheckAt", 0) or 0)
                checked_market = False
                if bool(cfg.get("riskCooldownAdaptiveMarket", True)) and (now - last_check_at >= check_sec):
                    checked_market = True
                    AUTO_TRADE["riskCooldownLastMarketCheckAt"] = now
                    try:
                        open_symbol_blacklist: set[str] = set()
                        if (cfg.get("executionMode") or "PAPER").upper() == "LIVE":
                            key = os.getenv("BINANCE_API_KEY")
                            secret = os.getenv("BINANCE_API_SECRET")
                            base = _binance_base()
                            live_positions = await asyncio.wait_for(_pick_live_orphan_positions(key, secret, base), timeout=6.0)
                            open_symbol_blacklist = _open_symbols_from_positions(live_positions)
                        market_state = await asyncio.wait_for(
                            _adaptive_risk_cooldown_check(cfg, open_symbol_blacklist),
                            timeout=float(cfg.get("intervalSec", 20)) * 2.0 + 12,
                        )
                        board = market_state.get("board")
                        if isinstance(board, list):
                            AUTO_TRADE["scanBoard"] = board
                            AUTO_TRADE["cooldownWatchlist"] = {
                                "updatedAt": now,
                                "reason": str(market_state.get("reason") or "adaptive cooldown check"),
                                "picked": market_state.get("symbol"),
                                "candidates": len(board),
                                "top": board[:6],
                            }
                        if bool(market_state.get("resume")):
                            AUTO_TRADE["pauseUntil"] = 0
                            _persist_autotrade_snapshot()
                            _agent_mark("risk_manager", "done", "adaptive cooldown released", str(market_state.get("reason") or "market ok"))
                            _autotrade_log(f"Adaptive risk cooldown released: {market_state.get('reason')}")
                        else:
                            _agent_mark("risk_manager", "blocked", "adaptive cooldown hold", str(market_state.get("reason") or f"{remain}s remaining"))
                            _autotrade_skip("risk_cooldown", f"Skip: risk cooldown {remain}s · {market_state.get('reason')}")
                            _persist_autotrade_snapshot()
                            await asyncio.sleep(min(10, max(2, int(cfg.get("intervalSec", 20)))))
                            continue
                    except Exception as e:
                        err_text = _format_loop_error(e)[:80]
                        if isinstance(e, (asyncio.TimeoutError, TimeoutError, httpx.TimeoutException)):
                            _agent_mark("risk_manager", "blocked", "risk cooldown active", f"{remain}s remaining; adaptive check timeout")
                            _autotrade_skip("risk_cooldown", f"Skip: risk cooldown {remain}s · adaptive check timeout; retrying")
                            await _refresh_risk_cooldown_watchlist(cfg, set(), now, f"{remain}s remaining; adaptive timeout")
                        else:
                            _agent_mark("risk_manager", "blocked", "adaptive cooldown check failed", err_text)
                            _autotrade_skip("risk_cooldown", f"Skip: risk cooldown {remain}s · adaptive check failed")
                        _persist_autotrade_snapshot()
                        await asyncio.sleep(min(10, max(2, int(cfg.get("intervalSec", 20)))))
                        continue
                if not checked_market and int(AUTO_TRADE.get("pauseUntil", 0) or 0) > now:
                    await _refresh_risk_cooldown_watchlist(cfg, set(), now, f"{remain}s remaining")
                    _agent_mark("risk_manager", "blocked", "risk cooldown active", f"{remain}s remaining")
                    _autotrade_skip("risk_cooldown", f"Skip: risk cooldown {remain}s")
                    await asyncio.sleep(min(10, max(2, int(cfg.get("intervalSec", 20)))))
                    continue
            # Stop requested: keep managing existing LIVE positions only, no new scans/entries.
            if not running_entries:
                mode_now = str(cfg.get("executionMode") or "PAPER").upper()
                if mode_now != "LIVE":
                    AUTO_TRADE["manageOpenOnly"] = False
                    await asyncio.sleep(max(2, int(cfg.get("intervalSec", 20))))
                    continue
                try:
                    key = os.getenv("BINANCE_API_KEY")
                    secret = os.getenv("BINANCE_API_SECRET")
                    base = _binance_base()
                    live_positions = await _pick_live_orphan_positions(key, secret, base)
                except Exception:
                    live_positions = []
                if not live_positions:
                    AUTO_TRADE["manageOpenOnly"] = False
                    _autotrade_log("Manage-open-only done: no LIVE positions left")
                    _persist_autotrade_snapshot()
                    await asyncio.sleep(2)
                    continue
                await asyncio.sleep(max(2, int(cfg.get("intervalSec", 20))))
                continue
            if _within_no_trade_window(time.localtime(now), cfg.get("noTradeWindows", [])):
                _agent_mark("risk_manager", "blocked", "no trade window")
                _autotrade_skip("no_trade_window", "Skip: in no-trade window")
                await asyncio.sleep(cfg["intervalSec"])
                continue
            bad_utc_hours = cfg.get("liveBadUtcHours", [])
            if isinstance(bad_utc_hours, list):
                utc_h = time.gmtime(now).tm_hour
                if utc_h in {int(h) for h in bad_utc_hours if str(h).strip().lstrip("-").isdigit()}:
                    _agent_mark("risk_manager", "blocked", "bad UTC hour", f"{utc_h:02d}")
                    _autotrade_skip("bad_utc_hour", f"Skip: bad UTC hour {utc_h:02d}")
                    await asyncio.sleep(cfg["intervalSec"])
                    continue
            AUTO_TRADE["trades"] = [t for t in AUTO_TRADE["trades"] if now - t < 3600]
            if now - AUTO_TRADE["lastTradeAt"] < cfg["cooldownSec"]:
                await asyncio.sleep(cfg["intervalSec"])
                continue

            # Symbol-scoped safety brake after repeated recent losses.
            if risk_cooldown_enabled:
                loss_states = _recent_live_loss_streak_states_by_symbol(int(cfg.get("riskCooldownLookback", 8) or 8))
                threshold = int(cfg.get("riskCooldownLossStreak", 4) or 4)
                cooldowns = _prune_risk_cooldowns(now)
                armed_any = False
                for loss_symbol, loss_state in loss_states.items():
                    loss_streak = int(loss_state.get("streak", 0) or 0)
                    loss_signature = str(loss_state.get("signature", "") or "")
                    last_closed_at = int(loss_state.get("lastClosedAt", 0) or 0)
                    existing = cooldowns.get(loss_symbol)
                    previous_signature = str(existing.get("signature", "") or "") if isinstance(existing, dict) else ""
                    if loss_streak < threshold or not loss_signature or loss_signature == previous_signature:
                        continue
                    recent_window_sec = max(900, int(cfg.get("riskCooldownRecentWindowSec", 6 * 3600) or (6 * 3600)))
                    if last_closed_at and now - last_closed_at > recent_window_sec:
                        continue
                    cool_min = int(cfg.get("riskCooldownMinutes", 25) or 25)
                    recent_msgs = [
                        str(row.get("msg", "") or "")
                        for row in (AUTO_TRADE.get("log") or [])[:12]
                        if isinstance(row, dict)
                    ]
                    infra_cause = _infra_auth_incident_from_messages(
                        recent_msgs,
                        skip_code=str((AUTO_TRADE.get("lastSkip") or {}).get("code", "") or "") if isinstance(AUTO_TRADE.get("lastSkip"), dict) else "",
                        skip_msg=str((AUTO_TRADE.get("lastSkip") or {}).get("msg", "") or "") if isinstance(AUTO_TRADE.get("lastSkip"), dict) else "",
                    )
                    _agent_mark("reflection_agent", "doing", "review symbol loss streak", f"{loss_symbol} loss_streak={loss_streak}")
                    _agent_mark("backtest_agent", "doing", "review historical loss windows")
                    tuned_cfg = _loss_streak_self_review_tune(cfg, now, loss_streak, infra_cause)
                    _agent_mark("backtest_agent", "done", "historical loss review completed")
                    if tuned_cfg != cfg:
                        cfg = tuned_cfg
                        AUTO_TRADE["config"] = copy.deepcopy(cfg)
                        _persist_autotrade_snapshot()
                        _agent_mark("memory_agent", "done", "persist self-review config")
                        review = AUTO_TRADE.get("lastSelfReview") if isinstance(AUTO_TRADE.get("lastSelfReview"), dict) else {}
                        actions = ", ".join(review.get("actions") or [])
                        if actions:
                            _autotrade_log(f"Self-review tune after {loss_symbol} loss streak {loss_streak}: {actions}")
                            _agent_mark("reflection_agent", "done", "applied self-review tune", actions)
                        else:
                            _agent_mark("reflection_agent", "done", "reviewed loss streak", "no new actions")
                    _arm_symbol_risk_cooldown(loss_symbol, cool_min, loss_signature, loss_streak, "loss_streak", now, last_closed_at)
                    armed_any = True
                    _persist_autotrade_snapshot()
                    _agent_mark("risk_manager", "blocked", "armed symbol risk cooldown", f"{loss_symbol} {cool_min}m")
                    _autotrade_skip("risk_cooldown_arm", f"Skip: armed {loss_symbol} risk cooldown {cool_min}m (loss streak {loss_streak})")
                if armed_any:
                    # Keep scanning; newly cooled symbols are excluded below.
                    AUTO_TRADE["riskCooldownLossSignature"] = ""

            scan_mode = bool(cfg.get("marketScan")) or str(cfg.get("symbol", "")).upper() in ("AUTO", "SCAN")
            open_symbol_blacklist: set[str] = set(_risk_cooldown_symbols(now))
            if (cfg.get("executionMode") or "PAPER").upper() == "LIVE":
                try:
                    key = os.getenv("BINANCE_API_KEY")
                    secret = os.getenv("BINANCE_API_SECRET")
                    base = _binance_base()
                    live_positions = await asyncio.wait_for(_pick_live_orphan_positions(key, secret, base), timeout=6.0)
                    open_symbol_blacklist.update(_open_symbols_from_positions(live_positions))
                except Exception:
                    open_symbol_blacklist = set()
                    open_symbol_blacklist.update(_risk_cooldown_symbols(now))
            if scan_mode:
                try:
                    _agent_mark("market_analyst", "doing", "scan market candidates")
                    picked_symbol, picked_intel, board = await asyncio.wait_for(
                        _pick_best_symbol_from_scan(cfg, open_symbol_blacklist),
                        timeout=_scan_timeout_budget_sec(cfg),
                    )
                except asyncio.TimeoutError:
                    _agent_mark("market_analyst", "blocked", "market scan timed out")
                    _autotrade_skip("timeout", "Skip: market scan timed out")
                    await asyncio.sleep(cfg.get("intervalSec", 20))
                    continue
                AUTO_TRADE["scanBoard"] = board
                _agent_mark(
                    "market_analyst",
                    "done",
                    "scan completed",
                    str(picked_symbol or "no_pick"),
                    {"candidates": len(board), "picked": picked_symbol},
                )
                if not picked_symbol or not isinstance(picked_intel, dict):
                    _autotrade_skip("scan_none", "Skip: scan found no clear symbol")
                    await asyncio.sleep(cfg.get("intervalSec", 20))
                    continue
                # If top pick reached daily cap, auto-fallback to next qualified symbol in board.
                day_cap_scan = int(cfg.get("maxDailyTradesPerSymbol", _DEFAULT_MAX_DAILY_TRADES_PER_SYMBOL) or _DEFAULT_MAX_DAILY_TRADES_PER_SYMBOL)
                if day_cap_scan > 0:
                    picked_today = _live_trades_count_today_symbol(str(picked_symbol))
                    if picked_today >= day_cap_scan:
                        _fb_max = 3
                        fb_done = False
                        fb_tried = 0
                        for row in board:
                            if fb_tried >= _fb_max:
                                break
                            if not isinstance(row, dict):
                                continue
                            if not bool(row.get("qualified", False)):
                                continue
                            cand = str(row.get("symbol", "")).upper().strip()
                            if not cand or cand == str(picked_symbol).upper():
                                continue
                            cand_today = _live_trades_count_today_symbol(cand)
                            if cand_today >= day_cap_scan:
                                continue
                            fb_tried += 1
                            try:
                                fb_intel = await asyncio.wait_for(
                                    intel_analyze(IntelAnalyzeRequest(symbol=cand)),
                                    timeout=max(8.0, float(cfg.get("scanPerSymbolTimeoutSec", 7.5) or 7.5) + 3.0),
                                )
                                if not isinstance(fb_intel, dict):
                                    continue
                                # Quick pipeline pre-check: skip symbols that would fail common gates
                                _fb_sig = str(fb_intel.get("signal", "WAIT") or "WAIT").upper()
                                _fb_conf = float(fb_intel.get("confidence", 0) or 0)
                                _fb_prec = fb_intel.get("precision") if isinstance(fb_intel.get("precision"), dict) else {}
                                _fb_bb = float(_fb_prec.get("bbPctB", 0.5) or 0.5)
                                _fb_vwap = abs(float(_fb_prec.get("vwapDistancePct", 0.0) or 0.0))
                                _fb_mom = abs(float(_fb_prec.get("momentumPct", 0.0) or 0.0))
                                _fb_anti_late_bb = float(cfg.get("lateEntryMaxBbPctB", 0.95) or 0.95)
                                _fb_anti_late_vwap = float(cfg.get("lateEntryMaxVwapDistancePct", 0.40) or 0.40)
                                _fb_min_conf = float(cfg.get("minConfidence", 0.66) or 0.66)
                                # Anti-chase: LONG stretched or SHORT stretched (relaxed +0.05 for SHORT)
                                if _fb_sig == "LONG" and _fb_bb >= _fb_anti_late_bb and _fb_vwap >= _fb_anti_late_vwap:
                                    _autotrade_log(f"SCAN fallback pre-check: {cand} skip (late LONG chase bb={_fb_bb:.2f})")
                                    continue
                                _short_bb_thr = max(0.05, (1.0 - _fb_anti_late_bb) + 0.05)
                                if _fb_sig == "SHORT" and _fb_bb <= _short_bb_thr and _fb_vwap >= _fb_anti_late_vwap:
                                    _autotrade_log(f"SCAN fallback pre-check: {cand} skip (late SHORT chase bb={_fb_bb:.2f})")
                                    continue
                                # Weak momentum
                                if _fb_mom < 0.08:
                                    _autotrade_log(f"SCAN fallback pre-check: {cand} skip (weak momentum {_fb_mom:.3f})")
                                    continue
                                # Low confidence
                                if _fb_conf < _fb_min_conf:
                                    _autotrade_log(f"SCAN fallback pre-check: {cand} skip (conf {_fb_conf:.3f} < {_fb_min_conf:.3f})")
                                    continue
                                _autotrade_log(f"SCAN fallback: {picked_symbol} capped {picked_today}/{day_cap_scan} -> {cand} (sig={_fb_sig} conf={_fb_conf:.3f})")
                                picked_symbol, picked_intel = cand, fb_intel
                                fb_done = True
                                break
                            except Exception:
                                continue
                        if not fb_done:
                            _autotrade_skip("symbol_day_cap", f"Skip: {picked_symbol} capped {picked_today}/{day_cap_scan}; tried {fb_tried} fallbacks")
                            await asyncio.sleep(cfg.get("intervalSec", 20))
                            continue
                if cfg.get("symbol") != picked_symbol:
                    cfg["symbol"] = picked_symbol
                    AUTO_TRADE["config"] = copy.deepcopy(cfg)
                    _autotrade_log(f"SCAN pick: {picked_symbol}")
                intel = picked_intel
            else:
                primary_symbol = str(cfg.get("primarySymbol") or cfg.get("symbol"))
                cooldown_rec = _symbol_risk_cooldown_record(primary_symbol, now)
                if cooldown_rec:
                    remain = max(1, int(cooldown_rec.get("until", 0) or 0) - now)
                    _agent_mark("risk_manager", "blocked", "symbol risk cooldown active", f"{primary_symbol} {remain}s")
                    _switch_fixed_symbol_to_scan(cfg, primary_symbol, "symbol_risk_cooldown", f"{remain}s remaining")
                    _autotrade_skip("symbol_risk_cooldown", f"Skip: {primary_symbol} risk cooldown {remain}s; switching to market scan")
                    await asyncio.sleep(cfg.get("intervalSec", 20))
                    continue
                if primary_symbol.upper().strip() in open_symbol_blacklist:
                    _agent_mark("market_analyst", "blocked", "primary symbol already open", primary_symbol)
                    _autotrade_skip("symbol_position_open", f"Skip: {primary_symbol} already has open position; wait until closed")
                    await asyncio.sleep(cfg.get("intervalSec", 20))
                    continue
                perf_ok, perf_reason, perf = _symbol_perf_gate(cfg, primary_symbol)
                if not perf_ok:
                    _agent_mark("market_analyst", "blocked", "primary symbol locked", f"{primary_symbol} {perf_reason}")
                    _switch_fixed_symbol_to_scan(cfg, primary_symbol, perf_reason or "symbol_lock", "primary symbol locked")
                    _autotrade_skip("primary_symbol_locked", f"Skip: {primary_symbol} locked ({perf_reason}); switching to market scan")
                    await asyncio.sleep(cfg.get("intervalSec", 20))
                    continue
                cfg["symbol"] = primary_symbol
                # Use cached intel if available and fresh — avoids redundant computation
                # when interval is shorter than cache TTL
                intel_req = IntelAnalyzeRequest(symbol=primary_symbol)
                try:
                    _agent_mark("market_analyst", "doing", "analyze primary symbol", primary_symbol)
                    intel = await asyncio.wait_for(
                        intel_analyze(intel_req),
                        timeout=float(cfg.get("intervalSec", 20)) * 1.5 + 10,
                    )
                except asyncio.TimeoutError:
                    _agent_mark("market_analyst", "blocked", "primary analyze timed out", primary_symbol)
                    _autotrade_skip("timeout", "Skip: intel_analyze timed out (Binance API slow)")
                    await asyncio.sleep(cfg.get("intervalSec", 20))
                    continue
                AUTO_TRADE["scanBoard"] = []
                _agent_mark("market_analyst", "done", "primary analysis completed", primary_symbol)
                # Hybrid mode: keep primary symbol by default, switch only when scan winner is clearly better.
                if bool(cfg.get("hybridScan")):
                    try:
                        picked_symbol, picked_intel, board = await asyncio.wait_for(
                            _pick_best_symbol_from_scan(cfg, open_symbol_blacklist),
                            timeout=float(cfg.get("intervalSec", 20)) + 10,
                        )
                        AUTO_TRADE["scanBoard"] = board
                        if picked_symbol and isinstance(picked_intel, dict):
                            base_score = _intel_score(primary_symbol, intel)
                            scan_score = _intel_score(picked_symbol, picked_intel)
                            min_score = float(cfg.get("hybridMinScore", 0.72))
                            min_edge = float(cfg.get("hybridMinEdge", 0.06))
                            if scan_score >= min_score and (scan_score - base_score) >= min_edge:
                                cfg["symbol"] = picked_symbol
                                AUTO_TRADE["config"] = copy.deepcopy(cfg)
                                intel = picked_intel
                                _autotrade_log(
                                    f"HYBRID switch: {primary_symbol} -> {picked_symbol} (scan {scan_score:.3f} > base {base_score:.3f})"
                                )
                    except Exception:
                        pass
            _sym_decision = str(cfg.get("symbol", "")).upper()
            AUTO_TRADE["lastDecision"] = {"intel": intel, "symbol": _sym_decision, "ts": now}
            AUTO_TRADE.setdefault("lastDecisions", {})[_sym_decision] = {"intel": intel, "symbol": _sym_decision, "ts": now}
            dq = _intel_data_quality_guard(intel)
            if bool(dq.get("ok")):
                _agent_mark("data_quality_guard", "done", "intel quality ok", str(dq.get("symbol", "")), dq)
            else:
                _agent_mark("data_quality_guard", "blocked", "intel quality failed", str(dq.get("reason", "")), dq)
                _autotrade_skip("data_quality", f"Skip: data quality failed · {dq.get('reason')}")
                await asyncio.sleep(cfg.get("intervalSec", 20))
                continue
            news_guard = _news_sentiment_guard_state(cfg, intel)
            if bool(news_guard.get("enabled")):
                guard_status = str(news_guard.get("status", "not_wired")).lower()
                if guard_status in ("adverse", "negative", "high_risk"):
                    _agent_mark("news_sentiment_guard", "blocked", "news guard adverse", guard_status, news_guard)
                    _autotrade_skip("news_sentiment", f"Skip: news sentiment guard adverse ({guard_status})")
                    await asyncio.sleep(cfg.get("intervalSec", 20))
                    continue
                if guard_status == "not_wired":
                    _agent_mark("news_sentiment_guard", "todo", "news guard ENABLED but NOT WIRED — inactive (no blocking)", guard_status, news_guard)
                else:
                    _agent_mark("news_sentiment_guard", "done", "news guard neutral", guard_status, news_guard)
            else:
                _agent_mark("news_sentiment_guard", "done", "news guard disabled", "", news_guard)
            if risk_cooldown_enabled and bool(cfg.get("riskCooldownPauseOnVolatile", True)):
                regime = _risk_cooldown_regime(intel)
                regime_name = str(regime.get("name", "UNKNOWN")).upper()
                if regime_name == "VOLATILE":
                    cool_min = max(1, int(cfg.get("riskCooldownVolatileMinutes", min(15, int(cfg.get("riskCooldownMinutes", 25) or 25))) or 10))
                    symbol_now = str(cfg.get("symbol", "") or "")
                    if scan_mode and symbol_now:
                        _cooldown_scan_symbol(symbol_now, cool_min * 60, "market volatile")
                        _agent_mark("market_analyst", "blocked", "symbol volatile cooldown", f"{symbol_now} {cool_min}m")
                        _autotrade_skip("symbol_volatile_cooldown", f"Skip: {symbol_now} market volatile; symbol cooldown {cool_min}m")
                    else:
                        _arm_symbol_risk_cooldown(symbol_now, cool_min, f"{symbol_now}:VOLATILE:{now}", 0, "market_volatile", now, now)
                        _switch_fixed_symbol_to_scan(cfg, symbol_now, "market_volatile", "symbol cooldown", lock_minutes=cool_min)
                        _agent_mark("risk_manager", "blocked", "symbol volatility cooldown", f"{symbol_now} {cool_min}m")
                        _autotrade_skip("symbol_volatile_cooldown", f"Skip: {symbol_now} market volatile; symbol cooldown {cool_min}m")
                    _persist_autotrade_snapshot()
                    await asyncio.sleep(min(10, max(2, int(cfg.get("intervalSec", 20)))))
                    continue
            _agent_mark("strategy_builder", "doing", "build entry decision", str(cfg.get("symbol", "")))
            day_cap = int(cfg.get("maxDailyTradesPerSymbol", _DEFAULT_MAX_DAILY_TRADES_PER_SYMBOL) or _DEFAULT_MAX_DAILY_TRADES_PER_SYMBOL)
            today_n = _live_trades_count_today_symbol(str(cfg.get("symbol", "")))
            if day_cap > 0 and today_n >= day_cap:
                _agent_mark("portfolio_manager", "blocked", "symbol daily cap", f"{cfg['symbol']} {today_n}/{day_cap}")
                _autotrade_skip("symbol_day_cap", f"Skip: {cfg['symbol']} reached daily cap {today_n}/{day_cap}")
                await asyncio.sleep(cfg.get("intervalSec", 20))
                continue
            mode = (cfg.get("executionMode") or "PAPER").upper()
            ex = intel.get("execution") or {}
            em = ex.get("mark")
            # Fallback mark price with its own timeout
            if em is not None and float(em) > 0:
                mark = float(em)
            else:
                try:
                    mark = await asyncio.wait_for(fetch_mark_price(cfg["symbol"]), timeout=8.0)
                except asyncio.TimeoutError:
                    _autotrade_skip("timeout", "Skip: fetch_mark_price timed out")
                    await asyncio.sleep(cfg.get("intervalSec", 20))
                    continue

            if mode == "PAPER" and AUTO_TRADE["paper"]["position"]:
                p = AUTO_TRADE["paper"]["position"]
                _agent_mark("position_guardian", "doing", "monitor paper position", f"{p['symbol']} {p['side']}")
                AUTO_TRADE["_guardianMonitorTs"] = now
                if p["side"] == "LONG":
                    if mark >= p["tp"]:
                        if _should_hold_winner("LONG", intel, cfg):
                            new_sl, new_tp = _trail_winner_levels(
                                "LONG",
                                mark,
                                float(p["sl"]),
                                float(p["tp"]),
                                float(cfg.get("holdTrailPct", 0.35)),
                                cfg,
                                "PAPER",
                            )
                            p["sl"] = new_sl
                            p["tp"] = new_tp
                            _autotrade_log(f"PAPER hold winner: trail TP/SL -> TP={new_tp:.6f} SL={new_sl:.6f}")
                            _agent_mark("position_guardian", "done", "paper trail winner", f"TP={new_tp:.6f} SL={new_sl:.6f}")
                        else:
                            t = _paper_close("TP_HIT", mark)
                            _autotrade_log(f"PAPER close TP pnl={t['pnl']:.4f}")
                            _agent_mark("position_guardian", "done", "paper close TP", f"pnl={t['pnl']:.4f}")
                    elif mark <= p["sl"]:
                        t = _paper_close("SL_HIT", mark)
                        _autotrade_log(f"PAPER close SL pnl={t['pnl']:.4f}")
                        _agent_mark("position_guardian", "done", "paper close SL", f"pnl={t['pnl']:.4f}")
                else:
                    if mark <= p["tp"]:
                        if _should_hold_winner("SHORT", intel, cfg):
                            new_sl, new_tp = _trail_winner_levels(
                                "SHORT",
                                mark,
                                float(p["sl"]),
                                float(p["tp"]),
                                float(cfg.get("holdTrailPct", 0.35)),
                                cfg,
                                "PAPER",
                            )
                            p["sl"] = new_sl
                            p["tp"] = new_tp
                            _autotrade_log(f"PAPER hold winner: trail TP/SL -> TP={new_tp:.6f} SL={new_sl:.6f}")
                            _agent_mark("position_guardian", "done", "paper trail winner", f"TP={new_tp:.6f} SL={new_sl:.6f}")
                        else:
                            t = _paper_close("TP_HIT", mark)
                            _autotrade_log(f"PAPER close TP pnl={t['pnl']:.4f}")
                            _agent_mark("position_guardian", "done", "paper close TP", f"pnl={t['pnl']:.4f}")
                    elif mark >= p["sl"]:
                        t = _paper_close("SL_HIT", mark)
                        _autotrade_log(f"PAPER close SL pnl={t['pnl']:.4f}")
                        _agent_mark("position_guardian", "done", "paper close SL", f"pnl={t['pnl']:.4f}")

            eb, ea, esb = ex.get("bid"), ex.get("ask"), ex.get("spreadBps")
            if (
                eb is not None
                and ea is not None
                and esb is not None
                and float(eb) > 0
                and float(ea) > 0
            ):
                bid, ask = float(eb), float(ea)
                spread_bps = float(esb)
            else:
                try:
                    bid, ask = await asyncio.wait_for(_best_bid_ask(cfg["symbol"]), timeout=8.0)
                except asyncio.TimeoutError:
                    _autotrade_skip("timeout", "Skip: bid/ask fetch timed out")
                    await asyncio.sleep(cfg.get("intervalSec", 20))
                    continue
                mid = (bid + ask) / 2
                spread_bps = ((ask - bid) / max(mid, 1e-9)) * 10000
            signal = intel.get("signal", "WAIT")
            conf = float(intel.get("confidence", 0))
            session_bias = _entry_session_bias(cfg, now)
            adaptive_min_conf = _learned_min_conf(cfg["symbol"], float(cfg["minConfidence"]))
            min_conf_floor = float(cfg.get("minConfidenceFloor", 0.30))
            min_conf_cap = float(cfg.get("minConfidenceCap", 0.95))
            adaptive_min_conf = max(
                min_conf_floor,
                min(min_conf_cap, float(adaptive_min_conf) + float(session_bias.get("confidenceShift", 0.0) or 0.0)),
            )
            vision = intel.get("vision")
            px = intel.get("precision") if isinstance(intel, dict) and isinstance(intel.get("precision"), dict) else {}
            # Pre-reversal guard: block entries that look likely to reverse
            # before they have a chance to run (RSI extremes, divergence, wick rejection).
            pre_reversal_score = 0.0
            pre_reversal_side_at_risk = ""
            try:
                pre_rev_thr = float(cfg.get("preReversalScoreBlock", 0.55) or 0.55)
                pre_rev_thr_marg = float(cfg.get("preReversalScoreSoftener", 0.20) or 0.20)
                _autotrade_log(f"pre-reversal: start check for {cfg['symbol']} side={signal} thr={pre_rev_thr:.2f}")
                # Prefer any cached klines for this symbol to avoid blocking the cycle.
                kl = None
                try:
                    key = (cfg["symbol"], "5m", 60)
                    cached = _KLINES_CACHE.get(key)
                    if cached:
                        _fetched_at, kl = cached
                        # Use cached if it is at most 5 minutes old — far less than the 20s cache TTL
                        # for stale-tolerant reversal analysis, but we still want fresh data when possible.
                        if time.time() - _fetched_at > 300:
                            kl = None
                except Exception:
                    kl = None
                if not (isinstance(kl, list) and len(kl) >= 25):
                    try:
                        kl = await asyncio.wait_for(
                            _cached_klines(cfg["symbol"], "5m", 60),
                            timeout=4.5,
                        )
                    except Exception as e:
                        _autotrade_log(f"pre-reversal: klines fetch failed: {type(e).__name__}: {e}")
                        kl = None
                _autotrade_log(
                    f"pre-reversal: klines for {cfg['symbol']} got={type(kl).__name__} len={len(kl) if hasattr(kl,'__len__') else 0}"
                )
                if not (isinstance(kl, list) and kl and len(kl) >= 25):
                    _autotrade_log(
                        f"pre-reversal check skipped: klines insufficient for {cfg['symbol']} (got={type(kl).__name__} len={len(kl) if hasattr(kl,'__len__') else 0})"
                    )
                else:
                    closes = [float(k[4]) for k in kl]
                    highs = [float(k[2]) for k in kl]
                    lows = [float(k[3]) for k in kl]
                    pre = _detect_pre_reversal(closes, highs, lows)
                    if isinstance(pre, dict):
                        AUTO_TRADE.setdefault("preReversalSamples", []).append({
                            "ts": int(time.time()),
                            "symbol": cfg["symbol"],
                            "side": signal,
                            **pre,
                        })
                        if len(AUTO_TRADE["preReversalSamples"]) > 50:
                            AUTO_TRADE["preReversalSamples"] = AUTO_TRADE["preReversalSamples"][-50:]
                        AUTO_TRADE["lastPreReversal"] = {
                            "symbol": cfg["symbol"],
                            "ts": int(time.time()),
                            **pre,
                        }
                        side_at_risk = pre.get("side_at_risk")
                        pre_reversal_score = float(pre.get("score", 0) or 0)
                        pre_reversal_side_at_risk = str(side_at_risk or "")
            except Exception as e:
                _autotrade_log(f"pre-reversal check skipped: {type(e).__name__}: {e}")
            # ── Compute sizing + slippage before pipeline ──
            key = os.getenv("BINANCE_API_KEY")
            secret = os.getenv("BINANCE_API_SECRET")
            base = _binance_base()
            ref_px = ask if signal == "LONG" else bid
            slippage_bps = abs((ref_px - mark) / max(mark, 1e-9)) * 10000

            lev_meta = _adaptive_symbol_leverage(cfg["symbol"], intel, cfg)
            eff_leverage = int(lev_meta.get("leverage", cfg.get("leverage", 1)) or 1)
            cfg["lastAdaptiveLeverage"] = lev_meta
            if bool(lev_meta.get("auto")):
                _agent_mark("risk_manager", "done", "adaptive symbol leverage", f"{cfg['symbol']} x{eff_leverage} {lev_meta.get('reason')}")
                _autotrade_log(
                    f"Adaptive leverage: {cfg['symbol']} x{eff_leverage} "
                    f"(range {lev_meta.get('min')}-{lev_meta.get('max')}, heat={float(lev_meta.get('heat', 0.0) or 0.0):.2f})"
                )
            trade_usdt = _adaptive_trade_usdt(cfg["usdtAmount"], cfg["symbol"], intel, cfg)
            size_mult = float(session_bias.get("sizeMult", 1.0) or 1.0)
            if abs(size_mult - 1.0) >= 0.001:
                old_trade_usdt = trade_usdt
                trade_usdt = round(float(trade_usdt) * size_mult, 2)
                _autotrade_log(
                    f"Session bias: {session_bias.get('label')} {session_bias.get('reason')} "
                    f"confShift={float(session_bias.get('confidenceShift', 0.0) or 0.0):+.3f} "
                    f"size {old_trade_usdt:.2f}->{trade_usdt:.2f}"
                )
            eff_prof = _symbol_effective_profile(cfg["symbol"], cfg)
            symbol_size_mult = float(eff_prof.get("positionSizeMult") or eff_prof.get("position_size_mult", 1.0) or 1.0)
            # Loss-streak guard at the apply point too: a bleeding symbol's
            # learned size multiplier (ESPUSDT 1.3) is capped at 1.10 so an
            # oversized position can't turn a normal SL into a -1.16 USDT hit.
            try:
                _sym_perf = _rolling_symbol_perf(cfg["symbol"], 30) or {}
                if int(_sym_perf.get("trades", 0) or 0) >= 4 and (
                    float(_sym_perf.get("winRatePct", 0.0) or 0.0) < 38.0
                    or float(_sym_perf.get("pnl", 0.0) or 0.0) < -0.25
                ):
                    symbol_size_mult = min(symbol_size_mult, 1.10)
            except Exception:
                pass
            if abs(symbol_size_mult - 1.0) >= 0.001:
                old_trade_usdt = trade_usdt
                trade_usdt = round(float(trade_usdt) * symbol_size_mult, 2)
                _autotrade_log(
                    f"Per-symbol size: {cfg['symbol']} source={eff_prof.get('source','?')} "
                    f"sizeMult={symbol_size_mult:.3f} {old_trade_usdt:.2f}->{trade_usdt:.2f}"
                )
            supervisor_size_mult = float(cfg.get("supervisorSizeMultiplier", 1.0) or 1.0)
            sym_streak_mult = _per_symbol_streak_size_mult(cfg["symbol"], cfg)
            if abs(sym_streak_mult - 1.0) >= 0.001:
                supervisor_size_mult = sym_streak_mult
            if bool(cfg.get("supervisorSizeStreakEnabled", True)) and abs(supervisor_size_mult - 1.0) >= 0.001:
                old_trade_usdt = trade_usdt
                trade_usdt = round(float(trade_usdt) * supervisor_size_mult, 2)
                _autotrade_log(
                    f"Supervisor streak size: mult={supervisor_size_mult:.3f} "
                    f"size {old_trade_usdt:.2f}->{trade_usdt:.2f}"
                )
            eff_cap = _effective_tp_sl(cfg["symbol"], cfg, intel)
            trade_cap = max(20.0, float(eff_cap.get("notionalCapUsdt", RISK["max_notional"])))
            if bool(cfg.get("marketScan")) or str(cfg.get("symbol", "")).upper() in {"AUTO", "SCAN"}:
                trade_cap = min(
                    trade_cap,
                    max(20.0, float(cfg.get("autoScanTradeNotionalCapUsdt", trade_cap) or trade_cap)),
                )
            if trade_usdt > trade_cap:
                old_trade_usdt = trade_usdt
                trade_usdt = round(float(trade_cap), 2)
                _autotrade_log(
                    f"Per-symbol cap applied: {cfg['symbol']} tier={eff_cap.get('tier','med')} "
                    f"cap={trade_cap:.2f} USDT (mult={eff_cap.get('capMult', 1.0)})"
                )
                _agent_mark("portfolio_manager", "done", "capped position notional", f"{old_trade_usdt:.2f}->{trade_usdt:.2f} USDT")
            min_order_usdt = max(20.0, float(cfg.get("feeMinOrderUsdt", 7.5) or 7.5))
            if trade_usdt < min_order_usdt:
                action = str(cfg.get("usdtTooSmallAction", "multiply") or "multiply").lower()
                if action == "skip":
                    _agent_mark("risk_manager", "blocked", "order notional too small")
                    _autotrade_skip("usdt_too_small", f"Skip: order notional too small {trade_usdt:.2f} < {min_order_usdt:.2f} USDT")
                    await asyncio.sleep(cfg["intervalSec"])
                    continue
                trade_usdt = min_order_usdt
                cfg["usdtAmount"] = max(float(cfg.get("usdtAmount", 0.0) or 0.0), float(trade_usdt))
                AUTO_TRADE["config"] = copy.deepcopy(cfg)
                _autotrade_log(f"Order floor: adjusted USDT → {trade_usdt:.2f} (min notional guard)")
            if trade_usdt > float(RISK["max_notional"]):
                trade_usdt = float(RISK["max_notional"])

            # ── Run entry pipeline (replaces inline gate checks) ──
            htf = intel.get("htf") if isinstance(intel, dict) and isinstance(intel.get("htf"), dict) else {}
            candle_ctx = intel.get("candles") if isinstance(intel, dict) and isinstance(intel.get("candles"), dict) else {}
            regime = detect_market_regime(intel) if intel else {}
            rv_pct = float((intel.get("precision") or {}).get("rvPct", 0.0) or 0.0) if isinstance(intel, dict) else None
            pb_pct = float((intel.get("precision") or {}).get("pullbackAllowancePct", 0.0) or 0.0) if isinstance(intel, dict) else None
            live_loss_streak = AUTO_TRADE.get("liveLossStreak", 0)
            bb_val = float((px or {}).get("bbPctB", 0.5) or 0.5)
            vwap_dist_val = abs(float((px or {}).get("vwapDistancePct", 0.0) or 0.0))
            long_sc = float((px or {}).get("longScore", 0.0) or 0.0)
            short_sc = float((px or {}).get("shortScore", 0.0) or 0.0)
            near_res = bool((px or {}).get("nearResistance", False))
            near_sup = bool((px or {}).get("nearSupport", False))
            ob = intel.get("orderBook") if isinstance(intel, dict) else None
            wait_imb = float((ob or {}).get("imbalance", 0)) if cfg.get("aggressiveScalp") else 0.0

            entry_inp = EntryInputs(
                cfg=cfg,
                intel=intel,
                regime=regime,
                signal=signal,
                confidence=conf,
                spread_bps=spread_bps,
                slippage_bps=slippage_bps,
                mark=mark,
                ex=ex,
                htf=htf,
                candle_ctx=candle_ctx,
                adaptive_min_conf=adaptive_min_conf,
                live_loss_streak=live_loss_streak,
                vision_ok=bool(vision),
                trade_usdt=trade_usdt,
                rv_pct=rv_pct,
                pb_pct=pb_pct,
                eff_leverage=eff_leverage,
                max_notional=float(RISK["max_notional"]),
                pre_reversal_score=pre_reversal_score,
                pre_reversal_side_at_risk=pre_reversal_side_at_risk,
                bb_pct_b=bb_val,
                vwap_distance_pct=vwap_dist_val,
                long_score=long_sc,
                short_score=short_sc,
                near_resistance=near_res,
                near_support=near_sup,
                wait_override_imbalance=wait_imb,
                scan_chase_speed=str(eff_prof.get("scan_chase_speed", "normal")),
                scan_long_bias=float(eff_prof.get("scan_long_bias", 0.50) or 0.50),
            )
            plan = evaluate_entry_plan(entry_inp)

            if not plan.approved:
                skip_code = plan.skip_code or "pipeline"
                skip_msg = plan.skip_message or "Entry pipeline rejected"
                _agent_mark("strategy_builder", "blocked", skip_code, skip_msg)
                _autotrade_skip(skip_code, f"Skip: {skip_msg}")
                if scan_mode and picked_symbol:
                    _cooldown_scan_symbol(str(picked_symbol), 30, f"pipeline:{skip_code}")
                await asyncio.sleep(cfg["intervalSec"])
                continue

            # Pipeline approved — use plan values
            signal = plan.signal
            conf = plan.confidence
            trade_usdt = plan.trade_usdt
            eff_leverage = plan.eff_leverage
            _agent_mark("strategy_builder", "done", "entry approved", f"{cfg['symbol']} {signal} c={conf:.3f} pipeline={len(plan.pipeline)} gates")

            if mode == "PAPER":
                _agent_mark("execution_agent", "doing", "paper execution", f"{cfg['symbol']} {signal}")
                qty = trade_usdt / max(mark, 1e-9)
                if AUTO_TRADE["paper"]["position"] and AUTO_TRADE["paper"]["position"]["side"] != signal:
                    t = _paper_close("FLIP_SIGNAL", mark)
                    _autotrade_log(f"PAPER close flip pnl={t['pnl']:.4f}")
                if not AUTO_TRADE["paper"]["position"]:
                    eff = _effective_tp_sl(cfg["symbol"], cfg, intel)
                    tp = mark * (1 + eff["tpPct"] / 100) if signal == "LONG" else mark * (1 - eff["tpPct"] / 100)
                    sl = mark * (1 - eff["slPct"] / 100) if signal == "LONG" else mark * (1 + eff["slPct"] / 100)
                    candles = intel.get("candles") if isinstance(intel, dict) else None
                    if not isinstance(candles, dict):
                        candles = {}
                    AUTO_TRADE["paper"]["position"] = {
                        "symbol": cfg["symbol"],
                        "side": signal,
                        "entry": mark,
                        "qty": qty,
                        "tp": tp,
                        "sl": sl,
                        "openedAt": now,
                        "patternTags": candles.get("tags", []),
                        "patternBias": candles.get("bias", 0.0),
                        "patternScore": candles.get("score", 0.0),
                        "entryConfidence": conf,
                        "entryScore": _intel_score(cfg["symbol"], intel),
                        "entrySpreadBps": float(ex.get("spreadBps", 0.0) or 0.0),
                        "entryMomentumPct": float(ex.get("momentumPct", 0.0) or 0.0),
                        # Persist the effective per-symbol TP/SL/cap/lock profile so
                        # guardian, learning and review can attribute TP/SL hits
                        # back to the volatility regime that produced them.
                        "entryTPPct": eff["tpPct"],
                        "entrySLPct": eff["slPct"],
                        "entryVolatilityTier": eff.get("tier", "med"),
                        # Per-symbol sizing and entry offset from learned profile.
                        "positionSizeMult": float(eff_prof.get("position_size_mult", 1.0) or 1.0),
                        "entryOffsetBps": float(eff_prof.get("entry_offset_bps", 0.0) or 0.0),
                    }
                    trade_res = {"mode": "paper", "entry": {"side": signal, "price": mark, "qty": qty}, "tp": tp, "sl": sl}
                    _agent_mark("execution_agent", "done", "paper position opened", f"{cfg['symbol']} {signal}")
                else:
                    _agent_mark("execution_agent", "blocked", "paper position still open")
                    _autotrade_skip("paper_open", "Skip: paper position still open")
                    await asyncio.sleep(cfg["intervalSec"])
                    continue
            else:
                # external MCP signal guard removed — no entry guard to consult.
                _agent_mark("execution_agent", "doing", "live pre-flight", f"{cfg['symbol']} {signal}")
                pst = await _position_side_state(cfg["symbol"], key, secret, base)
                if float(pst.get("long", 0.0)) > 0 and float(pst.get("short", 0.0)) > 0:
                    clear_thr = max(float(cfg.get("minConfidence", 0.65)), float(cfg.get("holdMinConfidence", 0.72)))
                    if conf >= clear_thr and signal in ("LONG", "SHORT"):
                        cut_side = "SHORT" if signal == "LONG" else "LONG"
                        rs = await _close_position_one_side(cfg["symbol"], cut_side, key, secret, base)
                        if rs.get("closed"):
                            _autotrade_log(
                                f"Hedge normalize: closed {cut_side} side immediately (signal={signal}, conf={conf:.3f})"
                            )
                            _agent_mark("execution_agent", "done", "hedge normalized", cut_side)
                        else:
                            _agent_mark("execution_agent", "blocked", "hedge side not closeable", cut_side)
                            _autotrade_skip(
                                "hedge_both_sides",
                                f"Skip: both LONG({pst['long']:.6f}) and SHORT({pst['short']:.6f}) open; no {cut_side} closeable rows",
                            )
                        await asyncio.sleep(1)
                        continue
                    _agent_mark("execution_agent", "blocked", "hedge both sides waiting clearer signal")
                    _autotrade_skip(
                        "hedge_both_sides",
                        f"Skip: both LONG({pst['long']:.6f}) and SHORT({pst['short']:.6f}) open; waiting clearer signal",
                    )
                    await asyncio.sleep(cfg["intervalSec"])
                    continue
                current_side = _open_side_from_position_state(pst)
                if current_side == "FLAT":
                    try:
                        _agent_mark("portfolio_manager", "doing", "check portfolio capacity")
                        open_n = await _open_positions_count(key, secret, base)
                        max_n = int(cfg.get("maxOpenPositions", _DEFAULT_MAX_OPEN_POSITIONS))
                        if open_n >= max_n:
                            _agent_mark("portfolio_manager", "blocked", "portfolio capacity full", f"{open_n}/{max_n}")
                            _autotrade_skip("max_open_positions", f"Skip: open positions {open_n}/{max_n} reached")
                            await asyncio.sleep(cfg["intervalSec"])
                            continue
                        _agent_mark("portfolio_manager", "done", "portfolio capacity ok", f"{open_n}/{max_n}")
                    except Exception:
                        pass
                if current_side in ("LONG", "SHORT"):
                    _agent_mark("execution_agent", "blocked", "symbol already has open position", f"{cfg['symbol']} {current_side}")
                    _autotrade_skip("symbol_position_open", f"Skip: {cfg['symbol']} already has open {current_side}; wait until closed")
                    await asyncio.sleep(cfg["intervalSec"])
                    continue

                # ── Pre-flight: check available balance before placing order ──
                try:
                    acct = await asyncio.wait_for(
                        _get_account_cached(key, secret, base),
                        timeout=6.0,
                    )
                    avail = float(acct.get("availableBalance", 0) or 0)
                    required = trade_usdt / max(eff_leverage, 1)
                    if avail < required * 1.05:  # 5% buffer
                        # Auto-reduce amount to fit available balance
                        safe_amt = round(avail * eff_leverage * 0.9, 2)
                        if safe_amt < 1.0:
                            _agent_mark("risk_manager", "blocked", "insufficient balance", f"{avail:.2f} USDT")
                            _autotrade_skip("balance", f"Skip: insufficient balance {avail:.2f} USDT available")
                            await asyncio.sleep(cfg["intervalSec"])
                            continue
                        old_amt = trade_usdt
                        trade_usdt = safe_amt
                        _autotrade_log(f"Balance check: reduced USDT {old_amt} → {safe_amt} (avail={avail:.2f})")
                    # Auto-fix CROSSED → ISOLATED if cross balance is low
                    # Only switch if no open position (Binance -4048 if position exists)
                    if cfg.get("marginType") == "CROSSED":
                        cross_bal = float(acct.get("crossWalletBalance", avail) or avail)
                        if cross_bal < required * 1.1:
                            # Check if position is flat before switching
                            try:
                                pos_check = await asyncio.wait_for(
                                    _current_position_amount(cfg["symbol"], key, secret, base),
                                    timeout=4.0,
                                )
                                if abs(float(pos_check)) < 1e-9:  # flat — safe to switch
                                    cfg["marginType"] = "ISOLATED"
                                    AUTO_TRADE["config"] = copy.deepcopy(cfg)
                                    _autotrade_log(f"Balance check: auto-switched CROSSED → ISOLATED (crossBal={cross_bal:.2f})")
                                else:
                                    _autotrade_log(f"Balance check: low crossBal={cross_bal:.2f} but position open — cannot switch margin type")
                            except Exception:
                                pass  # skip switch if can't check position
                except Exception:
                    pass  # balance check is best-effort; proceed anyway

                # external MCP signal guard removed — no second guard to consult.
                eff = _effective_tp_sl(cfg["symbol"], cfg, intel)
                async def _do_place():
                    return await place_futures_order(
                        cfg["symbol"],
                        signal,
                        usdt_amount=trade_usdt,
                        leverage=eff_leverage,
                        margin_type=cfg["marginType"],
                        tp_pct=eff["tpPct"],
                        sl_pct=eff["slPct"],
                        trailing_stop_pct=cfg.get("trailingStopPct", 0.0),
                    )

                place_timeout = max(20.0, float(cfg.get("intervalSec", 20)) * 1.5)
                try:
                    _agent_mark("execution_agent", "doing", "place live order", f"{cfg['symbol']} {signal}")
                    trade_res = await asyncio.wait_for(_do_place(), timeout=place_timeout)
                except asyncio.TimeoutError:
                    _agent_mark("execution_agent", "doing", "retry live order after timeout")
                    _autotrade_log("Retry: place order timed out once, retrying immediately")
                    trade_res = await asyncio.wait_for(_do_place(), timeout=place_timeout + 8.0)
                _agent_mark("execution_agent", "done", "live order completed", f"{cfg['symbol']} {signal}")
            AUTO_TRADE["lastTradeAt"] = now
            AUTO_TRADE["trades"].append(now)
            AUTO_TRADE["consecutiveErrors"] = 0
            AUTO_TRADE["lastSkip"] = None
            _autotrade_log(f"{mode} trade executed: {signal} {cfg['symbol']} {trade_usdt} USDT")
            _sym_decision = str(cfg["symbol"]).upper()
            AUTO_TRADE["lastDecision"] = {"intel": intel, "trade": trade_res, "symbol": _sym_decision, "side": signal, "ts": now}
            AUTO_TRADE.setdefault("lastDecisions", {})[_sym_decision] = {"intel": intel, "trade": trade_res, "symbol": _sym_decision, "side": signal, "ts": now}
            _agent_mark("memory_agent", "done", "decision stored", f"{mode} {cfg['symbol']} {signal}")
        except asyncio.TimeoutError:
            # Network timeout — not a logic error, use softer backoff
            AUTO_TRADE["consecutiveErrors"] = min(AUTO_TRADE.get("consecutiveErrors", 0) + 1, 10)
            _autotrade_skip("timeout", "Skip: network timeout (Binance API slow) — will retry")
        except Exception as e:
            err_msg = _format_loop_error(e)
            # Re-raise programming errors immediately so they aren't hidden in logs
            if isinstance(e, (TypeError, NameError, AttributeError)):
                raise
            AUTO_TRADE["consecutiveErrors"] = min(AUTO_TRADE.get("consecutiveErrors", 0) + 1, 20)
            # Stop spinning if too many consecutive errors (likely unrecoverable state)
            if AUTO_TRADE.get("consecutiveErrors", 0) >= 15:
                _autotrade_log(f"AutoTrade loop stopped after {AUTO_TRADE['consecutiveErrors']} consecutive errors: {err_msg}")
                raise

            # ── Auto-recovery for known Binance errors ────────────────────────
            cfg = AUTO_TRADE.get("config") or {}

            if _is_fapi_agreement_error(err_msg):
                match = re.search(r"'binanceRequest':\s*(\{[^}]+\})", err_msg)
                request_diag = match.group(1) if match else ""
                blocked_symbol = str(cfg.get("symbol", "") or "").upper().strip()
                if blocked_symbol:
                    locks = AUTO_TRADE.get("perfLocks")
                    if not isinstance(locks, dict):
                        locks = {}
                    lock_min = max(30, int(cfg.get("fapiAgreementSymbolLockMinutes", 360) or 360))
                    locks[blocked_symbol] = {
                        "until": int(time.time()) + (lock_min * 60),
                        "at": int(time.time()),
                        "reason": "fapi_agreement",
                        "error": "-4411",
                    }
                    AUTO_TRADE["perfLocks"] = locks
                    # Permanently deny this symbol for the session — -4411 means
                    # the account cannot trade this pair (Futures/Perps agreement
                    # not signed), so retrying is pointless.
                    deny = set(_parse_symbol_whitelist(cfg.get("scanDenySymbols")))
                    if blocked_symbol and blocked_symbol not in deny:
                        deny.add(blocked_symbol)
                        cfg["scanDenySymbols"] = sorted(deny)
                        AUTO_TRADE["config"] = copy.deepcopy(cfg)
                    if not scan_mode:
                        _switch_fixed_symbol_to_scan(cfg, blocked_symbol, "fapi_agreement", "-4411")
                AUTO_TRADE["lastSkip"] = {
                    "ts": int(time.time()),
                    "code": "fapi_agreement_required",
                    "msg": "Skip: Binance Futures/Perps agreement required (-4411)",
                }
                AUTO_TRADE["consecutiveErrors"] = 0
                _autotrade_log(
                    "Skip: Binance Futures/Perps agreement required (-4411)"
                    + (f" · denied {blocked_symbol}" if blocked_symbol else "")
                    + (f" · diag {request_diag}" if request_diag else "")
                )
                _persist_autotrade_snapshot()
                await asyncio.sleep(cfg.get("intervalSec", 20))
                continue

            if _is_binance_permission_error(err_msg):
                AUTO_TRADE["pauseUntil"] = int(time.time()) + 900
                AUTO_TRADE["lastSkip"] = {
                    "ts": int(time.time()),
                    "code": "binance_permission_required",
                    "msg": "LIVE ถูกหยุดชั่วคราว: Binance API key/IP/permission ไม่ผ่าน (-2015)",
                }
                AUTO_TRADE["consecutiveErrors"] = 0
                _autotrade_log("LIVE paused: Binance API key/IP/permission rejected (-2015)")
                _persist_autotrade_snapshot()
                await asyncio.sleep(cfg.get("intervalSec", 20))
                continue

            # -4050: Cross balance insufficient → auto-switch to ISOLATED (only if flat)
            if "-4050" in err_msg and cfg.get("marginType") == "CROSSED":
                try:
                    key2 = os.getenv("BINANCE_API_KEY")
                    secret2 = os.getenv("BINANCE_API_SECRET")
                    base2 = _binance_base()
                    pos_amt2 = await asyncio.wait_for(
                        _current_position_amount(cfg["symbol"], key2, secret2, base2),
                        timeout=4.0,
                    )
                    if abs(float(pos_amt2)) < 1e-9:
                        cfg["marginType"] = "ISOLATED"
                        AUTO_TRADE["config"] = copy.deepcopy(cfg)
                        _autotrade_skip("exception", "Error -4050: Cross balance insufficient — auto-switched to ISOLATED margin")
                    else:
                        _autotrade_skip("exception", "Error -4050: Cross balance insufficient — position open, cannot switch margin type. Close position first.")
                except Exception:
                    _autotrade_skip("exception", f"Error -4050: Cross balance insufficient — set Margin Type to ISOLATED manually")

            # -4048: Cannot change margin type while position open → just log, don't retry switch
            elif "-4048" in err_msg:
                _autotrade_skip("exception", "Error -4048: Cannot change margin type while position is open — close position first")
                AUTO_TRADE["consecutiveErrors"] = max(0, AUTO_TRADE["consecutiveErrors"] - 1)

            # -2019: Margin insufficient → reduce position size by 50%
            elif "-2019" in err_msg and cfg.get("usdtAmount", 0) > 5:
                old_amt = cfg["usdtAmount"]
                cfg["usdtAmount"] = round(old_amt * 0.5, 2)
                AUTO_TRADE["config"] = copy.deepcopy(cfg)
                _autotrade_skip("exception", f"Error -2019: Margin insufficient — reduced USDT {old_amt} → {cfg['usdtAmount']}")

            # QTY_TOO_SMALL: notional too small for symbol minimum.
            # Handle by configured policy: skip or auto-multiply immediately.
            elif (
                ("QTY_TOO_SMALL" in err_msg)
                or ("มูลค่า USDT ต่ำเกินไปสำหรับ" in err_msg)
                or ("-4164" in err_msg)
                or ("notional must be no smaller than" in err_msg.lower())
            ):
                action = str(cfg.get("usdtTooSmallAction", "multiply") or "multiply").lower()
                if action == "skip":
                    _autotrade_skip("usdt_too_small", "Skip: USDT ต่ำเกินขั้นต่ำของเหรียญนี้ (configured: skip)")
                    AUTO_TRADE["consecutiveErrors"] = max(0, AUTO_TRADE["consecutiveErrors"] - 1)
                else:
                    old_amt = float(cfg.get("usdtAmount", 0.0) or 0.0)
                    m_min = float(cfg.get("usdtTooSmallMultiplierMin", 5.0) or 5.0)
                    m_max = float(cfg.get("usdtTooSmallMultiplierMax", 10.0) or 10.0)
                    mult = max(1.0, min(m_max, m_min))
                    new_amt = round(max(old_amt * mult, 20.0), 2)
                    max_notional = float(RISK.get("max_notional", 0.0) or 0.0)
                    if max_notional > 0:
                        new_amt = min(new_amt, max_notional)
                    if new_amt > old_amt + 0.009:
                        cfg["usdtAmount"] = new_amt
                        AUTO_TRADE["config"] = copy.deepcopy(cfg)
                        _autotrade_skip("usdt_too_small", f"Skip: USDT ต่ำเกินขั้นต่ำ — auto multiply {old_amt:.2f} → {new_amt:.2f} (x{mult:.2f})")
                        AUTO_TRADE["consecutiveErrors"] = max(0, AUTO_TRADE["consecutiveErrors"] - 1)
                    else:
                        _autotrade_skip("usdt_too_small", "Skip: USDT ต่ำเกินขั้นต่ำและไม่สามารถเพิ่มได้ (ติดเพดาน max notional)")
                        AUTO_TRADE["consecutiveErrors"] = max(0, AUTO_TRADE["consecutiveErrors"] - 1)

            # -1111: Precision error → will resolve on next tick
            elif "-1111" in err_msg:
                _autotrade_skip("exception", f"Error -1111: Qty precision — will retry next tick")
                AUTO_TRADE["consecutiveErrors"] = max(0, AUTO_TRADE["consecutiveErrors"] - 1)

            else:
                _autotrade_skip("exception", f"Error: {err_msg}")
        _persist_autotrade_snapshot()
        interval = (AUTO_TRADE["config"] or {}).get("intervalSec", 20)
        extra = min(120, 8 * max(AUTO_TRADE.get("consecutiveErrors", 0) - 2, 0))
        await asyncio.sleep(interval + extra)


@app.post("/autotrade/start")
async def autotrade_start(req: AutoTradeStartRequest):
    if RISK["kill_switch"]:
        raise HTTPException(status_code=403, detail="Kill-switch enabled")
    cfg = req.model_dump()
    raw_symbol = str(cfg.get("symbol", "")).upper().strip()
    if bool(cfg.get("marketScan")) or raw_symbol in ("AUTO", "SCAN"):
        cfg["marketScan"] = True
        cfg["symbol"] = "AUTO"
    else:
        cfg["symbol"] = _normalize_symbol(cfg["symbol"])
        cfg["primarySymbol"] = cfg["symbol"]
    cfg["whitelistSymbols"] = sorted(list(_parse_symbol_whitelist(cfg.get("whitelistSymbols"))))
    adopted = None
    adopted_positions: list[dict] = []
    # Auto handover: if there is already a LIVE position on Binance, adopt it on start.
    if (cfg.get("executionMode") or "PAPER").upper() == "LIVE" and bool(cfg.get("orphanAutoAdoptEnabled", True)):
        key = os.getenv("BINANCE_API_KEY")
        secret = os.getenv("BINANCE_API_SECRET")
        base = _binance_base()
        try:
            adopted_positions = await _pick_live_orphan_positions(key, secret, base)
        except Exception:
            adopted_positions = []
        adopted = adopted_positions[0] if adopted_positions else None
        multi_enabled = bool(cfg.get("orphanAutoAdoptMultiEnabled", True))
        force_single = bool(cfg.get("orphanAutoAdoptForceSingleSymbol", False))
        if adopted and force_single:
            cfg["symbol"] = adopted["symbol"]
            cfg["primarySymbol"] = adopted["symbol"]
            cfg["marketScan"] = False
            cfg["whitelistSymbols"] = [adopted["symbol"]]
        elif adopted_positions and multi_enabled:
            symbols = sorted({str(p.get("symbol", "")).upper() for p in adopted_positions if p.get("symbol")})
            if symbols:
                # Keep scan in AUTO, but do not permanently lock whitelist to orphan symbols.
                # This avoids scan board getting stuck on one/two symbols forever after handover.
                cfg["symbol"] = "AUTO"
                cfg["primarySymbol"] = symbols[0]
                cfg["marketScan"] = True
                if not isinstance(cfg.get("whitelistSymbols"), list):
                    cfg["whitelistSymbols"] = []

    # Self-heal sticky single-symbol scan config:
    # if scan is AUTO and force-single-adopt is off, a single whitelist symbol usually means stale lock.
    if (
        bool(cfg.get("marketScan"))
        and str(cfg.get("symbol", "")).upper() in ("AUTO", "SCAN")
        and not bool(cfg.get("orphanAutoAdoptForceSingleSymbol", False))
    ):
        wl_norm = sorted(list(_parse_symbol_whitelist(cfg.get("whitelistSymbols"))))
        if len(wl_norm) <= 1:
            cfg["whitelistSymbols"] = []
    lev_min, lev_max = _autotrade_leverage_bounds(cfg)
    cfg["leverageMin"] = lev_min
    cfg["leverageMax"] = lev_max
    cfg["leverage"] = int(max(lev_min, min(lev_max, int(cfg.get("leverage", lev_min) or lev_min), 25)))
    if int(cfg["leverage"]) > _autotrade_leverage_cap():
        raise HTTPException(
            status_code=400,
            detail=f"Leverage {cfg['leverage']} exceeds server max {_autotrade_leverage_cap()}",
        )
    if float(cfg["usdtAmount"]) > float(RISK["max_notional"]):
        raise HTTPException(
            status_code=400,
            detail=f"USDT amount {cfg['usdtAmount']} exceeds server max notional {RISK['max_notional']}",
        )
    gross_u, est_cost_u, net_u = _estimate_trade_edge_usdt(
        cfg["usdtAmount"], cfg["takeProfitPct"], cfg["maxSlippageBps"]
    )
    min_net_u = _fee_edge_min_net_usdt(cfg, est_cost_u, cfg["usdtAmount"])
    if net_u <= min_net_u:
        raise HTTPException(
            status_code=400,
            detail=(
                "ตั้งค่า TP/ขนาดไม้ยังไม่คุ้มค่าธรรมเนียมและต้นทุน (สุทธิ <= ขั้นต่ำ) "
                f"| gross={gross_u:.4f} cost={est_cost_u:.4f} net={net_u:.4f} min={min_net_u:.4f} USDT"
            ),
        )
    session_id = str(uuid4())
    preserved_fapi_locks = _active_fapi_agreement_locks()
    AUTO_TRADE["running"] = True
    AUTO_TRADE["manageOpenOnly"] = False
    AUTO_TRADE["pauseUntil"] = 0
    AUTO_TRADE["riskCooldownLossSignature"] = ""
    AUTO_TRADE["riskCooldownBySymbol"] = {}
    AUTO_TRADE["perfLocks"] = preserved_fapi_locks
    AUTO_TRADE["sessionId"] = session_id
    AUTO_TRADE["startedAt"] = int(time.time())
    AUTO_TRADE["config"] = apply_autotrade_defaults(copy.deepcopy(cfg))
    AUTO_TRADE["lastDecision"] = None
    AUTO_TRADE["lastSkip"] = None
    AUTO_TRADE["consecutiveErrors"] = 0
    AUTO_TRADE["lastTradeAt"] = 0
    AUTO_TRADE["trades"] = []
    AUTO_TRADE["liveProfitLocks"] = {}
    AUTO_TRADE["scanBoard"] = []
    AUTO_TRADE["cooldownWatchlist"] = {}
    AUTO_TRADE["hermesSupervisorReview"] = {}
    AUTO_TRADE["hermesAgents"] = start_cycle(new_agent_state())
    _agent_mark("memory_agent", "done", "session initialized", session_id)
    if (cfg.get("executionMode") or "PAPER").upper() == "LIVE":
        _paper_reset()
    else:
        prev = AUTO_TRADE["paper"].get("position")
        if (
            prev
            and isinstance(prev, dict)
            and prev.get("symbol") == cfg["symbol"]
        ):
            _autotrade_log(
                f"Resume: kept PAPER position {cfg['symbol']} {prev.get('side')} (same pair after disconnect/restart)"
            )
        else:
            _paper_reset()
    _autotrade_log(f"AutoTrade started for {cfg['symbol']}")
    if adopted:
        _autotrade_log(
            f"Handover LIVE position: {adopted['symbol']} {adopted['side']} qty={adopted['qty']:.6f} (~{adopted['notionalUsdtApprox']:.2f} USDT)"
        )
    if len(adopted_positions) > 1:
        syms = ", ".join([str(p.get("symbol")) for p in adopted_positions[:8]])
        _autotrade_log(f"Handover LIVE multi: {len(adopted_positions)} positions [{syms}]")
    _persist_autotrade_snapshot()
    global _AUTOTRADE_TASK
    # Cancel any stale task before starting a new one
    if _AUTOTRADE_TASK and not _AUTOTRADE_TASK.done():
        _AUTOTRADE_TASK.cancel()
    _AUTOTRADE_TASK = _track_autotrade_task(asyncio.create_task(_autotrade_loop()), "manual-start")
    return {
        "ok": True,
        "running": True,
        "config": cfg,
        "sessionId": session_id,
        "adopted": adopted,
        "adoptedPositions": adopted_positions,
    }


@app.post("/autotrade/stop")
def autotrade_stop(req: AutoTradeControlRequest | None = None):
    req = req or AutoTradeControlRequest()
    current_session = AUTO_TRADE.get("sessionId")
    if AUTO_TRADE["running"] and current_session and not req.force:
        if req.sessionId != current_session:
            return {"ok": False, "running": True, "ignored": True, "reason": "SESSION_MISMATCH"}
    AUTO_TRADE["running"] = False
    AUTO_TRADE["sessionId"] = None
    AUTO_TRADE["startedAt"] = 0
    cfg = dict(AUTO_TRADE.get("config") or {})
    mode_now = str(cfg.get("executionMode") or "PAPER").upper()
    keep_manage_open = False
    if mode_now == "LIVE":
        key = os.getenv("BINANCE_API_KEY")
        secret = os.getenv("BINANCE_API_SECRET")
        base = _binance_base()
        keep_manage_open = _has_live_open_positions_sync(key, secret, base)
    AUTO_TRADE["manageOpenOnly"] = keep_manage_open
    if not keep_manage_open:
        AUTO_TRADE["liveProfitLocks"] = {}
    AUTO_TRADE["lastSkip"] = None
    AUTO_TRADE["consecutiveErrors"] = 0
    AUTO_TRADE["scanBoard"] = []
    AUTO_TRADE["cooldownWatchlist"] = {}
    _autotrade_log("AutoTrade stopped (scan/entry off)" + (" · manage open positions on" if keep_manage_open else ""))
    global _AUTOTRADE_TASK
    if keep_manage_open and (_AUTOTRADE_TASK is None or _AUTOTRADE_TASK.done()):
        _AUTOTRADE_TASK = _track_autotrade_task(asyncio.create_task(_autotrade_loop()), "manage-open-only")
    if not keep_manage_open:
        pass  # MarketContext MCP removed — no task to cancel here
    _persist_autotrade_snapshot(force=True)
    return {"ok": True, "running": False, "manageOpenOnly": keep_manage_open}


@app.post("/autotrade/reset")
def autotrade_reset(req: AutoTradeControlRequest | None = None):
    req = req or AutoTradeControlRequest()
    current_session = AUTO_TRADE.get("sessionId")
    if AUTO_TRADE["running"] and current_session and not req.force:
        if req.sessionId != current_session:
            return {"ok": False, "running": True, "ignored": True, "reason": "SESSION_MISMATCH"}
    AUTO_TRADE["running"] = False
    AUTO_TRADE["manageOpenOnly"] = False
    AUTO_TRADE["pauseUntil"] = 0
    AUTO_TRADE["riskCooldownLossSignature"] = ""
    AUTO_TRADE["riskCooldownBySymbol"] = {}
    AUTO_TRADE["perfLocks"] = {}
    AUTO_TRADE["sessionId"] = None
    AUTO_TRADE["startedAt"] = 0
    AUTO_TRADE["config"] = None
    AUTO_TRADE["lastDecision"] = None
    AUTO_TRADE["lastTradeAt"] = 0
    AUTO_TRADE["trades"] = []
    AUTO_TRADE["log"] = []
    AUTO_TRADE["liveProfitLocks"] = {}
    AUTO_TRADE["lastSkip"] = None
    AUTO_TRADE["consecutiveErrors"] = 0
    AUTO_TRADE["scanBoard"] = []
    AUTO_TRADE["cooldownWatchlist"] = {}
    AUTO_TRADE["hermesSupervisorReview"] = {}
    _paper_reset()
    _autotrade_log("AutoTrade session reset")
    _persist_autotrade_snapshot(force=True)
    return {"ok": True, "running": False, "reset": True}


@app.get("/autotrade/status")
async def autotrade_status(symbol: str | None = None):
    await _ensure_autotrade_task_alive("status-watchdog")
    p_raw = AUTO_TRADE.get("paper") if isinstance(AUTO_TRADE.get("paper"), dict) else {}
    p = {
        "position": p_raw.get("position"),
        "wins": int(p_raw.get("wins", 0) or 0),
        "losses": int(p_raw.get("losses", 0) or 0),
        "realizedPnl": float(p_raw.get("realizedPnl", 0.0) or 0.0),
        "history": p_raw.get("history", []) if isinstance(p_raw.get("history"), list) else [],
    }
    total = p["wins"] + p["losses"]
    cfg = AUTO_TRADE["config"] or {}
    if isinstance(cfg, dict) and cfg:
        _sync_autotrade_leverage_cap_from_cfg(cfg)
    qs = None
    if symbol and str(symbol).strip():
        try:
            qs = _normalize_symbol(str(symbol).strip())
        except HTTPException:
            qs = None
    live_position = {
        "side": "FLAT",
        "qty": 0.0,
        "notionalUsdtApprox": 0.0,
    }
    open_live_positions: list[dict] = []
    open_live_positions_error: str | None = None
    # Always hit Binance to get real positions — regardless of running state.
    # This ensures positions are visible even when bot is stopped/paused.
    if os.getenv("BINANCE_API_KEY") and os.getenv("BINANCE_API_SECRET"):
        try:
            key = os.getenv("BINANCE_API_KEY")
            secret = os.getenv("BINANCE_API_SECRET")
            base = _binance_base()
            open_live_positions = await asyncio.wait_for(
                _pick_live_orphan_positions(key, secret, base),
                timeout=8.0,
            )
            if open_live_positions:
                lock_map = AUTO_TRADE.get("liveProfitLocks") if isinstance(AUTO_TRADE.get("liveProfitLocks"), dict) else {}
                for op in open_live_positions:
                    k = _live_lock_key(str(op.get("symbol", "")), str(op.get("side", "")))
                    lk = lock_map.get(k) if isinstance(lock_map, dict) else None
                    if isinstance(lk, dict):
                        op["profitLockArmed"] = bool(lk.get("armed", False))
                        op["profitLockUsdt"] = round(float(lk.get("lockUsdt", 0.0) or 0.0), 6)
                        op["peakUnrealizedPnl"] = round(float(lk.get("peak", 0.0) or 0.0), 6)
                        op["localTp"] = lk.get("tp")
                        op["localSl"] = lk.get("sl")
                    op["leverage"] = _position_display_leverage(op.get("symbol"), cfg, op.get("leverage"))
                    _attach_symbol_profile(op, None)
                lead = open_live_positions[0]
                live_position = {
                    "symbol": str(lead.get("symbol", "") or ""),
                    "side": str(lead.get("side", "FLAT")),
                    "qty": float(lead.get("qty", 0.0) or 0.0),
                    "notionalUsdtApprox": float(lead.get("notionalUsdtApprox", 0.0) or 0.0),
                    "leverage": _position_display_leverage(lead.get("symbol"), cfg, lead.get("leverage")),
                }
        except Exception as e:
            open_live_positions_error = _format_loop_error(e)

    orphan_live = None
    continuity_hints: list[str] = []
    file_mtime = None
    if SNAPSHOT_PATH.exists():
        try:
            file_mtime = int(SNAPSHOT_PATH.stat().st_mtime)
        except Exception:
            file_mtime = None

    if not AUTO_TRADE["running"] and qs:
        key = os.getenv("BINANCE_API_KEY")
        secret = os.getenv("BINANCE_API_SECRET")
        if key and secret:
            try:
                base = _binance_base()
                amt = await asyncio.wait_for(_current_position_amount(qs, key, secret, base), timeout=8.0)
                if abs(float(amt)) > 0:
                    mark = await asyncio.wait_for(fetch_mark_price(qs), timeout=8.0)
                    orphan_live = {
                        "symbol": qs,
                        "side": "LONG" if amt > 0 else "SHORT",
                        "qty": abs(float(amt)),
                        "notionalUsdtApprox": round(abs(float(amt)) * mark, 6),
                        "leverage": _position_display_leverage(qs, cfg),
                    }
                    continuity_hints.append(
                        f"กระดานยังมีโพซิชัน {orphan_live['side']} {qs} ~{orphan_live['notionalUsdtApprox']} USDT"
                    )
            except Exception as exc:
                print(f"[Trade Log] ERROR writing {TRADES_LOG_PATH}: {exc}")

    if not AUTO_TRADE["running"]:
        pp = p.get("position")
        if isinstance(pp, dict) and pp.get("symbol") and qs and pp["symbol"] != qs:
            continuity_hints.append(
                f"มี PAPER ค้างที่ {pp['symbol']} ({pp.get('side')}) — กราฟปัจจุบัน {qs}"
            )
        elif isinstance(pp, dict) and pp.get("symbol") and qs and pp["symbol"] == qs:
            continuity_hints.append("มี PAPER ค้างบนคู่นี้ — กดเริ่ม AutoTrade เพื่อต่อลูปจัดการ TP/SL")

    continuity = {
        "snapshotFile": SNAPSHOT_PATH.name,
        "snapshotFileMtime": file_mtime,
        "snapshotSavedAt": AUTO_TRADE.get("_snapshot_saved_at"),
        "snapshotLoadedAt": AUTO_TRADE.get("_snapshot_loaded_at"),
        "recoveredLog": AUTO_TRADE.get("_snapshot_recovered_log"),
        "hints": continuity_hints,
        "orphanLive": orphan_live,
    }

    stat_symbol = (
        cfg.get("symbol")
        or qs
        or ((p.get("position") or {}).get("symbol") if isinstance(p.get("position"), dict) else None)
    )
    live_symbol_stats = _aggregate_live_trade_stats_from_log(stat_symbol)
    live_wins = int(live_symbol_stats.get("wins", 0))
    live_losses = int(live_symbol_stats.get("losses", 0))
    live_total = live_wins + live_losses
    live_wins_today = int(live_symbol_stats.get("winsToday", 0))
    live_losses_today = int(live_symbol_stats.get("lossesToday", 0))
    live_total_today = live_wins_today + live_losses_today
    live_last_trades = live_symbol_stats.get("lastTrades", []) if isinstance(live_symbol_stats.get("lastTrades"), list) else []

    live_all_stats = _aggregate_live_trade_stats_from_log(None)
    all_wins = int(live_all_stats.get("wins", 0))
    all_losses = int(live_all_stats.get("losses", 0))
    all_total = all_wins + all_losses
    all_wins_today = int(live_all_stats.get("winsToday", 0))
    all_losses_today = int(live_all_stats.get("lossesToday", 0))
    all_total_today = all_wins_today + all_losses_today
    public_cfg = dict(cfg)
    scan_mode_public = bool(public_cfg.get("marketScan")) or str(public_cfg.get("symbol", "") or "").upper() in ("AUTO", "SCAN")
    if scan_mode_public:
        active_scan_symbol = str((AUTO_TRADE.get("lastDecision") or {}).get("symbol") or public_cfg.get("symbol") or "").upper().strip()
        if active_scan_symbol and active_scan_symbol not in ("AUTO", "SCAN"):
            public_cfg["activeScanSymbol"] = active_scan_symbol
        public_cfg["symbol"] = "AUTO"
        public_cfg["marketScan"] = True

    try:
        from trading.tradingview_mcp import get_tv_mcp
        tv_health = get_tv_mcp(cfg).get_health_status()
    except Exception:
        tv_health = {}

    return {
        "running": AUTO_TRADE["running"],
        "manageOpenOnly": bool(AUTO_TRADE.get("manageOpenOnly")),
        "pauseUntil": int(AUTO_TRADE.get("pauseUntil", 0) or 0),
        "riskCooldownLossSignature": str(AUTO_TRADE.get("riskCooldownLossSignature", "") or ""),
        "riskCooldownBySymbol": _prune_risk_cooldowns(),
        "perfLocks": AUTO_TRADE.get("perfLocks", {}),
        "sessionId": AUTO_TRADE.get("sessionId"),
        "startedAt": AUTO_TRADE.get("startedAt", 0),
        "autotradeTask": _autotrade_task_state(),
        "config": public_cfg,
        "tradingviewHealth": tv_health,
        "lastDecision": AUTO_TRADE["lastDecision"],
        "lastSkip": AUTO_TRADE.get("lastSkip"),
        "consecutiveErrors": AUTO_TRADE.get("consecutiveErrors", 0),
        "lastTradeAt": AUTO_TRADE["lastTradeAt"],
        "tradesLastHour": len([t for t in AUTO_TRADE["trades"] if int(time.time()) - t < 3600]),
        "log": AUTO_TRADE["log"][:15],
        "scanBoard": list(AUTO_TRADE.get("scanBoard", []))[:10],
        "cooldownWatchlist": AUTO_TRADE.get("cooldownWatchlist") if isinstance(AUTO_TRADE.get("cooldownWatchlist"), dict) else {},
        "hermesAgents": ensure_agent_state(AUTO_TRADE.get("hermesAgents")),
        "paper": {
            "position": p["position"],
            "wins": p["wins"],
            "losses": p["losses"],
            "winRatePct": round((p["wins"] / total) * 100, 2) if total > 0 else 0.0,
            "realizedPnl": round(p["realizedPnl"], 6),
            "lastTrades": p["history"][:10],
        },
        "liveStats": {
            "symbol": stat_symbol,
            "wins": live_wins,
            "losses": live_losses,
            "winRatePct": round((live_wins / live_total) * 100, 2) if live_total > 0 else 0.0,
            "realizedPnl": round(float(live_symbol_stats.get("realizedPnl", 0.0) or 0.0), 6),
            "winsToday": live_wins_today,
            "lossesToday": live_losses_today,
            "winRatePctToday": round((live_wins_today / live_total_today) * 100, 2) if live_total_today > 0 else 0.0,
            "realizedPnlToday": round(float(live_symbol_stats.get("realizedPnlToday", 0.0) or 0.0), 6),
            "lastTrades": live_last_trades,
        },
        "liveStatsAll": {
            "wins": all_wins,
            "losses": all_losses,
            "winRatePct": round((all_wins / all_total) * 100, 2) if all_total > 0 else 0.0,
            "realizedPnl": round(float(live_all_stats.get("realizedPnl", 0.0) or 0.0), 6),
            "winsToday": all_wins_today,
            "lossesToday": all_losses_today,
            "winRatePctToday": round((all_wins_today / all_total_today) * 100, 2) if all_total_today > 0 else 0.0,
            "realizedPnlToday": round(float(live_all_stats.get("realizedPnlToday", 0.0) or 0.0), 6),
        },
        "kpiTodayAllSymbols": {
            "live": {
                "wins": all_wins_today,
                "losses": all_losses_today,
                "winRatePct": round((all_wins_today / all_total_today) * 100, 2) if all_total_today > 0 else 0.0,
                "realizedPnl": round(float(live_all_stats.get("realizedPnlToday", 0.0) or 0.0), 6),
            }
        },
        "activePosition": {
            "mode": cfg.get("executionMode", "PAPER"),
            "paper": {
                "side": (p.get("position") or {}).get("side", "FLAT"),
                "qty": float((p.get("position") or {}).get("qty", 0.0)),
                "notionalUsdtApprox": round(
                    float((p.get("position") or {}).get("qty", 0.0))
                    * float((p.get("position") or {}).get("entry", 0.0)),
                    6,
                )
                if p.get("position")
                else 0.0,
            },
            "live": live_position,
        },
        "openLivePositions": open_live_positions,
        "openLivePositionsError": open_live_positions_error,
        "riskLimits": {
            "maxLeverage": RISK["max_leverage"],
            "maxNotionalUSDT": RISK["max_notional"],
            "maxDailyLossUSDT": RISK["max_daily_loss"],
            "killSwitch": RISK["kill_switch"],
        },
        "hermesSupervisorReview": _cached_hermes_supervisor_review(AUTO_TRADE, max_age_sec=60, allow_compute=False),
        "continuity": continuity,
    }

def _wire_backtest_validate(cfg: dict) -> None:
    """Opt-in (cfg.autoValidateLearning): run real walk-forward validation.

    Runs in a daemon thread so it never blocks the supervisor tick."""
    try:
        sym = str(cfg.get("primarySymbol") or cfg.get("symbol") or "").upper().strip()
        if not sym:
            return
        wf = _walk_forward_from_trades(
            sym,
            int(cfg.get("walkForwardTrain", 20)),
            int(cfg.get("walkForwardTest", 10)),
            "LIVE",
        )
        _agent_mark("backtest_agent", "done", "walk-forward validated (live)", f"{sym} pf={wf.get('profitFactor')}", wf)
    except Exception as e:
        _agent_mark("backtest_agent", "blocked", "walk-forward failed", str(e)[:120])


def _wire_reflection_summary(recent_trades: list) -> None:
    """Opt-in (cfg.autoReflectSummary): produce a general trade-outcome summary."""
    try:
        trades = [t for t in (recent_trades or []) if isinstance(t, dict)]
        n = len(trades)
        if n == 0:
            return
        wins = sum(1 for t in trades if float(t.get("pnl", 0) or 0) > 0)
        wr = (wins / n) * 100.0
        _agent_mark("reflection_agent", "done", "general reflection summary", f"winRate={wr:.1f}% n={n}")
    except Exception:
        pass


def _hermes_supervisor_review(bot_state: dict | None = None) -> dict:
    bot = bot_state if isinstance(bot_state, dict) else AUTO_TRADE
    state = ensure_agent_state(bot.get("hermesAgents"))
    agents = state.get("agents") if isinstance(state.get("agents"), dict) else {}
    now = int(time.time())
    issues: list[dict] = []
    suggestions: list[dict] = []
    auto_actions: list[dict] = []

    def add_issue(
        agent_id: str,
        severity: str,
        title: str,
        detail: str,
        suggestion: str,
        *,
        supervisor_first: bool = False,
    ):
        item = {
            "agent": agent_id,
            "severity": severity,
            "title": title,
            "detail": detail,
            "suggestion": suggestion,
        }
        if supervisor_first:
            item["supervisorFirst"] = True
        issues.append(item)
        suggestions.append({"agent": agent_id, "action": suggestion, "severity": severity})

    def add_auto_action(
        agent_id: str,
        action: str,
        reason: str,
        status: str = "recommended",
        *,
        issue_type: str | None = None,
        changes: dict | None = None,
    ):
        item = {
            "agent": agent_id,
            "action": action,
            "reason": reason,
            "status": status,
        }
        if issue_type:
            item["issueType"] = issue_type
            item["delegated"] = True
        if isinstance(changes, dict) and changes:
            item["changes"] = changes
        auto_actions.append(item)

    def agent_age(agent_id: str) -> int:
        agent = agents.get(agent_id) if isinstance(agents.get(agent_id), dict) else {}
        updated_at = int(agent.get("updatedAt", 0) or 0)
        return max(0, now - updated_at) if updated_at > 0 else 0

    def agent_updated_at(agent_id: str) -> int:
        agent = agents.get(agent_id) if isinstance(agents.get(agent_id), dict) else {}
        return int(agent.get("updatedAt", 0) or 0) if agent else 0

    def recent_trade_rows() -> list[dict]:
        rows: list[dict] = []
        live_all = bot.get("liveStatsAll") if isinstance(bot.get("liveStatsAll"), dict) else {}
        live_sym = bot.get("liveStats") if isinstance(bot.get("liveStats"), dict) else {}
        for src in (live_all, live_sym):
            trades = src.get("lastTrades") if isinstance(src.get("lastTrades"), list) else []
            for trade in trades:
                if isinstance(trade, dict):
                    rows.append(trade)
        # Fallback: when liveStats is missing/empty in the bot state (e.g. right
        # after a restart or before the first status-lite call populates it), pull
        # recent trades directly from the trade log so the supervisor review can
        # still trigger backtest_agent / reflection_agent / memory_agent when
        # there are real closed trades to learn from.
        if not rows:
            try:
                cfg_symbol = str(
                    (bot.get("config") or {}).get("symbol") or ""
                ).upper().strip() or None
                log_all = _aggregate_live_trade_stats_from_log(None) or {}
                log_sym = _aggregate_live_trade_stats_from_log(cfg_symbol) or {}
                for src in (log_all, log_sym):
                    trades = src.get("lastTrades") if isinstance(src.get("lastTrades"), list) else []
                    for trade in trades:
                        if isinstance(trade, dict):
                            rows.append(trade)
                # Cache the freshly-computed stats back into the bot so the
                # downstream logic that reads liveStats/liveStatsAll sees them.
                if isinstance(log_all, dict) and log_all:
                    bot["liveStatsAll"] = log_all
                if isinstance(log_sym, dict) and log_sym and cfg_symbol:
                    bot["liveStats"] = log_sym
            except Exception:
                pass
        seen: set[tuple] = set()
        out: list[dict] = []
        for trade in rows:
            key = (
                trade.get("closedAt") or trade.get("ts"),
                str(trade.get("symbol", "") or ""),
                str(trade.get("side", "") or ""),
                float(trade.get("pnl", 0.0) or 0.0),
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(trade)
        out.sort(key=lambda t: int(t.get("closedAt") or t.get("ts") or 0), reverse=True)
        return out[:20]

    def active_open_positions() -> list[dict]:
        existing = bot.get("openLivePositions") if isinstance(bot.get("openLivePositions"), list) else []
        if existing:
            return [p for p in existing if isinstance(p, dict)]
        out: list[dict] = []
        locks = bot.get("liveProfitLocks") if isinstance(bot.get("liveProfitLocks"), dict) else {}
        for lock in locks.values():
            if not isinstance(lock, dict):
                continue
            symbol = str(lock.get("symbol", "") or "").strip()
            side = str(lock.get("side", "") or "").upper().strip()
            qty = float(lock.get("qty", 0.0) or 0.0)
            if symbol and side in {"LONG", "SHORT"} and qty > 0:
                out.append(lock)
        return out

    safety_hold_active = False
    for agent_id, agent in agents.items():
        if not isinstance(agent, dict):
            continue
        if agent_id == "hermes_supervisor":
            continue
        state_name = str(agent.get("state", "todo") or "todo")
        action = str(agent.get("lastAction", "") or "")
        runs = int(agent.get("runs", 0) or 0)
        updated_at = int(agent.get("updatedAt", 0) or 0)
        age = max(0, now - updated_at) if updated_at > 0 else 0
        if state_name == "blocked":
            last_reason_lower = str(agent.get("lastReason", "") or "").lower()
            is_capacity_hold = agent_id == "portfolio_manager" and action in {
                "portfolio capacity full",
                "symbol daily cap",
            }
            is_adaptive_timeout_hold = (
                agent_id == "risk_manager"
                and action == "adaptive cooldown check failed"
                and "timeout" in last_reason_lower
            )
            is_safety_hold = agent_id == "risk_manager" and action in {
                "adaptive cooldown hold",
                "risk cooldown active",
                "market volatility pause",
                "armed risk cooldown",
                "no trade window",
                "bad UTC hour",
            } or is_adaptive_timeout_hold
            is_strategy_safety_hold = agent_id == "strategy_builder" and action in {
                "confidence below adaptive minimum",
                "late long chase",
                "late short chase",
                "signal wait",
            }
            is_market_safety_hold = agent_id == "market_analyst" and action in {
                "primary symbol already open",
                "symbol volatile cooldown",
            }
            safety_hold_active = safety_hold_active or is_capacity_hold or is_safety_hold or is_strategy_safety_hold or is_market_safety_hold
            title = (
                "Capacity hold"
                if is_capacity_hold
                else "Safety hold"
                if (is_safety_hold or is_strategy_safety_hold or is_market_safety_hold)
                else "Agent blocked"
            )
            severity = "low" if (is_capacity_hold or is_safety_hold or is_strategy_safety_hold or is_market_safety_hold) else "high"
            suggestion = (
                "เป็น guard ปกติ: รอปิด position หรือลด exposure ก่อนเปิดไม้ใหม่"
                if is_capacity_hold
                else "เป็น guard ปกติ: ตลาดยังผันผวน ให้รอ adaptive cooldown ปลดเมื่อ regime กลับมาปกติ"
                if is_safety_hold and action not in {"no trade window", "bad UTC hour"}
                else "เป็น guard ปกติ: อยู่ในช่วงเวลาที่ตั้งไว้ให้งดเปิดไม้ใหม่ แต่ Guardian ยังดูแล position เดิมต่อ"
                if is_safety_hold
                else "เป็น guard ปกติ: confidence ต่ำกว่า adaptive minimum จึงไม่ฝืนเข้าไม้ ให้รอสัญญาณที่ชัดกว่า"
                if is_strategy_safety_hold and action == "confidence below adaptive minimum"
                else "เป็น guard ปกติ: สัญญาณยังเป็น WAIT จึงไม่เปิด position จนกว่าโมเดลจะยืนยัน LONG/SHORT ชัดเจน"
                if is_strategy_safety_hold and action == "signal wait"
                else "เป็น guard ปกติ: ไม่ไล่ราคาเมื่อแท่งยืด/ห่าง VWAP มาก ให้รอสัญญาณใหม่ที่สะอาดกว่า"
                if is_strategy_safety_hold
                else "เป็น guard ปกติ: เหรียญนี้ผันผวนเกินเกณฑ์ จึงพัก symbol ชั่วคราวและให้ scan หา candidate อื่น"
                if is_market_safety_hold and action == "symbol volatile cooldown"
                else "เป็น guard ปกติ: เหรียญหลักมี position เปิดอยู่ จึงข้ามเพื่อไม่เปิดซ้ำและให้ Guardian ดูแลไม้เดิม"
                if is_market_safety_hold
                else "ตรวจ root cause ของ gate นี้และลด false-block ถ้าเกิดซ้ำ"
            )
            add_issue(
                agent_id,
                severity,
                title,
                f"{action or 'blocked'} ({agent.get('lastReason', '')})".strip(),
                suggestion,
            )
        elif state_name == "doing" and age > 180:
            add_issue(
                agent_id,
                "medium",
                "Agent doing too long",
                f"อยู่สถานะ doing {age}s",
                "เพิ่ม timeout/heartbeat หรือแยกงานย่อยให้จบเป็นช่วง",
            )
        elif runs == 0 and age > 300 and agent_id in ("reflection_agent", "memory_agent", "position_guardian", "portfolio_manager"):
            add_issue(
                agent_id,
                "low",
                "Agent has no completed run",
                "ยังไม่มีรอบ done ที่ยืนยันใน session นี้",
                "เพิ่ม trigger หรือทำให้ dashboard แสดง idle reason ชัดขึ้น",
            )

    last_skip = bot.get("lastSkip") if isinstance(bot.get("lastSkip"), dict) else {}
    skip_msg = str(last_skip.get("msg", "") or "")
    skip_code = str(last_skip.get("code", "") or "")
    skip_msg_lower = skip_msg.lower()
    entry_guard_skip_codes = {
        "risk_cooldown",
        "risk_cooldown_arm",
        "risk_cooldown_volatile",
        "symbol_volatile_cooldown",
        "no_trade_window",
    }

    def classify_infra_auth_incident(messages: list[str]) -> dict:
        return _infra_auth_incident_from_messages(messages, skip_code=skip_code, skip_msg=skip_msg)

    runs_by_agent = {
        agent_id: int(agent.get("runs", 0) or 0)
        for agent_id, agent in agents.items()
        if isinstance(agent, dict)
    }
    if runs_by_agent:
        max_agent = max(runs_by_agent, key=lambda k: runs_by_agent[k])
        max_runs = runs_by_agent.get(max_agent, 0)
        cadence_agents = {
            "market_analyst",
            "data_quality_guard",
            "news_sentiment_guard",
            "risk_manager",
            "portfolio_manager",
            "position_guardian",
            "strategy_builder",
        }
        comparable_runs = [
            runs
            for agent_id, runs in runs_by_agent.items()
            if agent_id in cadence_agents and runs > 0
        ]
        nonzero = comparable_runs or [v for v in runs_by_agent.values() if v > 0]
        min_active = min(nonzero) if nonzero else 0
        if max_agent == "market_analyst":
            market_core_runs = [
                runs_by_agent.get(agent_id, 0)
                for agent_id in ("data_quality_guard", "news_sentiment_guard", "risk_manager", "position_guardian")
                if runs_by_agent.get(agent_id, 0) > 0
            ]
            if market_core_runs:
                min_active = min(market_core_runs)
        split_agents_ready = "portfolio_manager" in agents and "position_guardian" in agents
        max_agent_state = agents.get(max_agent) if isinstance(agents.get(max_agent), dict) else {}
        max_agent_action = str(max_agent_state.get("lastAction", "") or "") if max_agent_state else ""
        is_guardian_heartbeat_load = (
            max_agent == "position_guardian"
            and bool(active_open_positions())
            and max_agent_action == "open positions heartbeat"
        )
        risk_manager_runs = runs_by_agent.get("risk_manager", 0)
        is_guardian_loop_cadence_load = (
            max_agent == "position_guardian"
            and risk_manager_runs > 0
            and max_runs <= max(20, risk_manager_runs * 2)
        )
        is_portfolio_capacity_load = (
            max_agent == "portfolio_manager"
            and max_agent_action in {
                "check portfolio capacity",
                "portfolio capacity ok",
                "portfolio capacity full",
                "symbol daily cap",
                "temporary perf-lock dominant symbol drag",
            }
        )
        market_analyst_runs = runs_by_agent.get("market_analyst", 0)
        is_portfolio_loop_cadence_load = (
            max_agent == "portfolio_manager"
            and risk_manager_runs > 0
            and market_analyst_runs > 0
            and max_runs <= max(20, risk_manager_runs * 2, market_analyst_runs * 3)
        )
        is_market_cooldown_probe_load = (
            max_agent == "market_analyst"
            and skip_code == "risk_cooldown"
            and "adaptive check timeout" in skip_msg_lower
        )
        imbalance_mult = 12 if max_agent == "market_analyst" else 8
        if max_agent == "hermes_supervisor":
            pass
        elif max_agent == "risk_manager" and split_agents_ready:
            pass
        elif is_guardian_heartbeat_load or is_guardian_loop_cadence_load:
            pass
        elif is_portfolio_capacity_load or is_portfolio_loop_cadence_load:
            pass
        elif is_market_cooldown_probe_load:
            pass
        elif max_runs >= 20 and min_active > 0 and max_runs >= min_active * imbalance_mult:
            add_issue(
                max_agent,
                "medium",
                "Workload imbalance",
                f"{max_agent} runs={max_runs} เทียบกับ agent อื่นต่ำสุด={min_active}",
                "กระจายความรับผิดชอบ หรือเพิ่ม agent เฉพาะทาง",
            )

    known_operator_hold = skip_code in {"binance_permission_required", "fapi_agreement_required"}
    known_entry_blockers = {
        "symbol_volatile_cooldown": {
            "agent": "risk_manager",
            "title": "Entry blocked by symbol volatility cooldown",
            "suggestion": "เป็น guard ปกติ: ตลาดยังผันผวน ให้รอ adaptive cooldown ปลดเมื่อ regime กลับมาปกติ",
            "action": "monitor volatility cooldown release",
        },
        "risk_cooldown": {
            "agent": "risk_manager",
            "title": "Entry blocked by risk cooldown",
            "suggestion": "เป็น guard ปกติ: รอ cooldown ปลดหรือ adaptive check ยืนยันว่า market กลับมาปกติ",
            "action": "monitor risk cooldown release",
        },
        "no_trade_window": {
            "agent": "risk_manager",
            "title": "Entry blocked by no-trade window",
            "suggestion": "เป็น guard ปกติ: อยู่ในช่วงเวลาที่ตั้งไว้ให้งดเปิดไม้ใหม่",
            "action": "wait for configured trade window",
        },
        "bad_utc_hour": {
            "agent": "risk_manager",
            "title": "Entry blocked by bad UTC hour",
            "suggestion": "เป็น guard ปกติถ้าผู้ใช้ตั้งช่วงเวลานี้เอง; ค่าเริ่มต้นใหม่จะไม่บล็อก UTC hour อัตโนมัติ",
            "action": "clear liveBadUtcHours or wait for configured UTC hour",
        },
        "symbol_position_open": {
            "agent": "portfolio_manager",
            "title": "Entry blocked by existing symbol position",
            "suggestion": "เป็น guard ปกติ: รอปิด position เดิมก่อนเปิดไม้ใหม่ใน symbol เดียวกัน",
            "action": "wait for current symbol exposure to clear",
        },
    }

    consecutive_errors = int(bot.get("consecutiveErrors", 0) or 0)
    running = bool(bot.get("running"))
    cfg = bot.get("config") if isinstance(bot.get("config"), dict) else {}
    execution_mode = str(cfg.get("executionMode", "") or "").upper()
    scan_mode_cfg = bool(cfg.get("marketScan")) or str(cfg.get("symbol", "") or "").upper() in {"AUTO", "SCAN"}
    if bot is AUTO_TRADE and running and execution_mode == "LIVE" and not scan_mode_cfg:
        drift_heal = _maybe_auto_heal_scan_config_drift(cfg)
        if drift_heal.get("applied"):
            add_issue(
                "market_analyst",
                "medium",
                "AUTO scan config drift",
                f"LIVE AUTO expected but config was fixed to {drift_heal.get('symbol')}",
                "Supervisor ปรับกลับเป็น AUTO scan และล้าง single-symbol whitelist แล้ว",
            )
            add_auto_action(
                "market_analyst",
                "auto-healed scan config drift",
                str(drift_heal.get("symbol") or ""),
                "applied",
                issue_type="scan_config_drift",
                changes=drift_heal.get("changes"),
            )
            _agent_mark("market_analyst", "done", "auto-healed scan config drift", str(drift_heal.get("symbol") or ""), drift_heal.get("changes"))
            scan_mode_cfg = True

    if not safety_hold_active and consecutive_errors >= 2 and not known_operator_hold:
        add_issue(
            "hermes",
            "high",
            "Backend loop errors",
            f"consecutiveErrors={consecutive_errors}",
            "ตรวจ log ล่าสุดและเพิ่ม recovery เฉพาะ error type",
        )

    # TradingView health monitoring
    tradingview_health_tune = {}
    if bot is AUTO_TRADE and running and execution_mode == "LIVE":
        try:
            from trading.supervisor_tuning import maybe_tune_tradingview_health
            tradingview_health_tune = maybe_tune_tradingview_health(cfg)
            if tradingview_health_tune.get("applied"):
                changed_keys = ", ".join(str(k) for k in tradingview_health_tune.get("changes", {}).keys())
                rec_count = tradingview_health_tune.get("recovery_count", 1)
                if tradingview_health_tune.get("tv_disabled"):
                    _autotrade_log(f"[TradingView] CRITICAL: MCP disabled after {rec_count} recovery attempts")
                add_auto_action(
                    "market_analyst",
                    "auto-tuned TradingView health",
                    f"{changed_keys} (recovery #{rec_count})",
                    "applied",
                    issue_type="tradingview_health",
                    changes=tradingview_health_tune.get("changes"),
                )
                _agent_mark("market_analyst", "done", "auto-tuned TradingView health", changed_keys, tradingview_health_tune.get("changes"))
            elif tradingview_health_tune.get("alreadyTuned"):
                rec_count = tradingview_health_tune.get("recovery_count", 0)
                extra = f" (recovery #{rec_count})" if rec_count else ""
                add_auto_action("market_analyst", "TradingView health tune cooldown active", f"{tradingview_health_tune.get('reason', '')}{extra}", "applied", issue_type="tradingview_health")
            elif tradingview_health_tune.get("reason") == "tv_healthy":
                pass  # No action needed if healthy
            elif tradingview_health_tune.get("reason") == "tv_rate_limited":
                cd_count = tradingview_health_tune.get("symbol_cooldowns", 0)
                _autotrade_log(f"[TradingView] Rate limited ({cd_count} symbols in cooldown), backoff active")
                add_auto_action("market_analyst", "TradingView rate limited - waiting for backoff", f"{cd_count} symbols in cooldown", "applied", issue_type="tradingview_health")
            elif tradingview_health_tune.get("reason"):
                add_auto_action("market_analyst", f"TradingView health check: {tradingview_health_tune.get('reason', '')}", "", "applied", issue_type="tradingview_health")
        except Exception as e:
            _autotrade_log(f"[TradingView] Health monitoring error: {e}")

    is_scan_timeout_skip = (
        ("timeout" in skip_msg_lower or "timeout" in skip_code.lower())
        and skip_code != "risk_cooldown"
        and "adaptive check timeout" not in skip_msg_lower
    )
    if not safety_hold_active and is_scan_timeout_skip:
        scan_timeout_tune = {}
        if bot is AUTO_TRADE and running and execution_mode == "LIVE":
            scan_timeout_tune = _maybe_tune_scan_timeout_from_skip(skip_msg or skip_code, cfg)
            if scan_timeout_tune.get("applied"):
                changed_keys = ", ".join(str(k) for k in scan_timeout_tune.get("changes", {}).keys())
                add_auto_action(
                    "market_analyst",
                    "auto-tuned scan timeout workload",
                    changed_keys,
                    "applied",
                    issue_type="scan_timeout",
                    changes=scan_timeout_tune.get("changes"),
                )
                _agent_mark("market_analyst", "done", "auto-tuned scan timeout workload", changed_keys, scan_timeout_tune.get("changes"))
            elif scan_timeout_tune.get("alreadyTuned"):
                add_auto_action("market_analyst", "scan timeout workload tune cooldown active", skip_msg or skip_code, "applied", issue_type="scan_timeout")
            elif scan_timeout_tune.get("reason") == "no_safe_delta":
                add_auto_action("market_analyst", "scan timeout workload already at safe limits", skip_msg or skip_code, "applied", issue_type="scan_timeout")
                _agent_mark("market_analyst", "done", "scan timeout workload already at safe limits", skip_msg or skip_code)
        scan_timeout_handled = bool(
            scan_timeout_tune.get("applied")
            or scan_timeout_tune.get("alreadyTuned")
            or scan_timeout_tune.get("reason") == "no_safe_delta"
        )
        add_issue(
            "market_analyst",
            "medium",
            "Scan timeout detected",
            skip_msg or skip_code,
            "ลด concurrency/เพิ่ม cache หรือ skip symbol ที่ timeout ซ้ำ",
            supervisor_first=not scan_timeout_handled,
        )

    log_rows = bot.get("log") if isinstance(bot.get("log"), list) else []
    recent_log_msgs = [
        str(row.get("msg", "") or "")
        for row in log_rows[:12]
        if isinstance(row, dict)
    ]
    infra_auth_incident = classify_infra_auth_incident(recent_log_msgs)
    infra_auth_active = bool(infra_auth_incident.get("active"))
    data_provider_error = bot.get("lastDataProviderError") if isinstance(bot.get("lastDataProviderError"), dict) else {}
    data_provider_age = now - int(data_provider_error.get("ts", 0) or 0) if data_provider_error else 999999
    infra_data_active = bool(data_provider_error and data_provider_age <= 180 and int(data_provider_error.get("streak", 0) or 0) >= 3)
    if infra_auth_active and not safety_hold_active:
        add_issue(
            "execution_agent",
            "high",
            str(infra_auth_incident.get("title") or "Infrastructure/auth incident"),
            str(infra_auth_incident.get("detail") or "Binance auth/IP permission rejected"),
            str(infra_auth_incident.get("operatorAction") or "ให้ผู้ใช้แก้ permission/IP ก่อนประเมิน strategy"),
        )
        add_auto_action(
            "execution_agent",
            "report Binance API/IP permission incident to user",
            str(infra_auth_incident.get("detail") or "infra_auth"),
            "recommended",
            issue_type="infra_auth",
        )
        if bot is AUTO_TRADE:
            _agent_mark("execution_agent", "blocked", "Binance API/IP permission rejected", str(infra_auth_incident.get("detail") or "infra_auth"))
    if infra_data_active and not safety_hold_active:
        add_issue(
            "market_analyst",
            "high",
            "Market data provider timeout",
            f"Binance data timeout streak={int(data_provider_error.get('streak', 0) or 0)} ({data_provider_error.get('error', '')})",
            "ถือเป็น infra/data issue: ลด scan load ชั่วคราวและรอ data provider กลับมาปกติก่อนประเมิน strategy",
        )
        add_auto_action(
            "market_analyst",
            "data provider circuit breaker active",
            str(data_provider_error.get("error", "") or "data timeout"),
            "applied" if bot is AUTO_TRADE else "recommended",
            issue_type="infra_data_timeout",
            changes={"cooldownUntil": data_provider_error.get("cooldownUntil")},
        )
        if bot is AUTO_TRADE:
            _agent_mark("market_analyst", "blocked", "data provider timeout", str(data_provider_error.get("error", "") or "timeout"))
    day_cap_symbol = ""
    day_cap_detail = ""
    if skip_code == "symbol_day_cap" or "reached daily cap" in skip_msg.lower():
        day_cap_detail = skip_msg or skip_code
    if not day_cap_detail:
        day_cap_detail = next((msg for msg in recent_log_msgs if "reached daily cap" in msg.lower()), "")
    if day_cap_detail:
        m = re.search(r"\b([A-Z0-9]{2,}USDT)\b.*?(\d+\s*/\s*\d+)?", day_cap_detail.upper())
        if m:
            day_cap_symbol = str(m.group(1) or "").upper().strip()
    if bot is AUTO_TRADE and running and execution_mode == "LIVE" and scan_mode_cfg and day_cap_symbol:
        now_local = time.localtime(now)
        next_midnight = time.mktime((now_local.tm_year, now_local.tm_mon, now_local.tm_mday + 1, 0, 0, 30, 0, 0, -1))
        cooldown_sec = max(300, int(next_midnight - time.time()))
        _cooldown_scan_symbol(day_cap_symbol, cooldown_sec, "symbol daily cap")
        if isinstance(AUTO_TRADE.get("scanBoard"), list):
            AUTO_TRADE["scanBoard"] = [
                row
                for row in AUTO_TRADE.get("scanBoard", [])
                if not (isinstance(row, dict) and str(row.get("symbol", "") or "").upper().strip() == day_cap_symbol)
            ]
        try:
            _persist_autotrade_snapshot()
        except Exception:
            pass
        add_auto_action(
            "portfolio_manager",
            "enforced per-symbol daily cap",
            day_cap_detail,
            "applied",
            issue_type="symbol_day_cap",
            changes={"symbol": day_cap_symbol, "cooldownSec": cooldown_sec},
        )
        add_auto_action(
            "market_analyst",
            "exclude capped symbol and continue AUTO scan",
            day_cap_symbol,
            "applied",
            issue_type="symbol_day_cap",
        )
        _agent_mark("portfolio_manager", "done", "enforced symbol daily cap", day_cap_detail)
        _agent_mark("market_analyst", "done", "exclude capped symbol from scan", day_cap_symbol)
    fapi_rejects = sum(1 for msg in recent_log_msgs if "-4411" in msg or "Futures/Perps agreement required" in msg)
    fapi_locked_symbols = _fapi_agreement_symbols_from_logs(recent_log_msgs)
    restored_fapi_locks = {}
    if bot is AUTO_TRADE and running and execution_mode == "LIVE" and fapi_locked_symbols:
        restored_fapi_locks = _restore_fapi_agreement_locks_from_logs(cfg, recent_log_msgs)
    active_fapi_locks = _active_fapi_agreement_locks() if bot is AUTO_TRADE else {}
    fapi_self_healed = bool(fapi_locked_symbols or active_fapi_locks)
    fapi_issue_needed = (not fapi_self_healed) or fapi_rejects >= 3
    fapi_needs_review = (skip_code == "fapi_agreement_required" or fapi_rejects >= 3) and not fapi_self_healed
    if not safety_hold_active and (skip_code == "fapi_agreement_required" or fapi_rejects >= 2):
        if fapi_issue_needed:
            add_issue(
                "execution_agent",
                "high" if fapi_needs_review else "medium",
                "Exchange agreement rejects entries",
                f"พบ -4411 {fapi_rejects} ครั้งใน log ล่าสุด",
                "ข้าม/lock เหรียญที่โดน -4411 และตรวจว่า market type/permission รองรับเหรียญนั้นจริง",
            )
        locked_detail = ", ".join(fapi_locked_symbols or list(active_fapi_locks.keys())[:6])
        add_auto_action(
            "execution_agent",
            "skip symbols rejected by -4411",
            locked_detail or "exchange rejected recent entries",
            "applied" if fapi_self_healed else "recommended",
            issue_type="fapi_agreement_required" if fapi_self_healed else None,
        )
        if fapi_self_healed and bot is AUTO_TRADE:
            _agent_mark("execution_agent", "done", "skip symbols rejected by -4411", locked_detail or "fapi agreement lock active")
        if restored_fapi_locks.get("applied"):
            add_auto_action(
                "execution_agent",
                "restored -4411 perf locks from logs",
                ", ".join(restored_fapi_locks.get("symbols") or []),
                "applied",
                issue_type="fapi_lock_restore",
                changes={"until": restored_fapi_locks.get("until"), "minutes": restored_fapi_locks.get("minutes")},
            )

    board = bot.get("scanBoard") if isinstance(bot.get("scanBoard"), list) else []
    scan_review_enabled = running and bool(board)
    if scan_review_enabled and all(str(row.get("rejectReason", "") or "").lower() == "analyze_error" for row in board if isinstance(row, dict)):
        add_issue(
            "market_analyst",
            "high",
            "All scan rows analyze_error",
            "scanBoard ล่าสุดเป็น analyze_error ทั้งหมด",
            "ตรวจ data fetch/analyze fallback และ circuit breaker",
        )
    board_rows = [row for row in board if isinstance(row, dict)]
    scan_picks = _scan_pick_symbols_from_logs(recent_log_msgs)
    if not safety_hold_active and running and execution_mode == "LIVE" and len(scan_picks) >= 3:
        pick_counts: dict[str, int] = {}
        for sym in scan_picks:
            pick_counts[sym] = pick_counts.get(sym, 0) + 1
        top_pick, top_count = max(pick_counts.items(), key=lambda item: item[1])
        if top_count >= 3:
            locked_symbols = set(fapi_locked_symbols) | set(active_fapi_locks.keys())
            locked_pick = top_pick in locked_symbols
            add_issue(
                "market_analyst" if not locked_pick else "execution_agent",
                "high" if locked_pick else "medium",
                "Repeated scan pick concentration",
                f"{top_pick} ถูก pick ซ้ำ {top_count}/{len(scan_picks)} ครั้งใน log ล่าสุด",
                "ถ้าเหรียญเดิมถูก reject/lock แล้วต้องถูกถอดจาก candidate; ถ้าไม่ถูก lock ให้ขยาย universe หรือเพิ่ม diversity penalty",
            )
            add_auto_action(
                "market_analyst",
                "review repeated scan pick concentration",
                f"{top_pick} {top_count}/{len(scan_picks)} picks",
                "recommended",
                issue_type="scan_pick_concentration" if locked_pick else None,
            )
    if not safety_hold_active and running and execution_mode == "LIVE" and scan_mode_cfg and not board_rows:
        ma = agents.get("market_analyst") if isinstance(agents.get("market_analyst"), dict) else {}
        ma_action = str(ma.get("lastAction", "") or "")
        ma_age = agent_age("market_analyst")
        interval = max(10, int(cfg.get("intervalSec", 20) or 20))
        if int(ma.get("runs", 0) or 0) >= 2 and ma_age > interval * 3 and ma_action not in {"scan market candidates", "waiting"}:
            add_issue(
                "market_analyst",
                "medium",
                "AUTO scan board stale or empty",
                f"marketScan=true แต่ scanBoard ว่าง age={ma_age}s action={ma_action or 'unknown'}",
                "บังคับ refresh scan และตรวจว่า scanBoard ถูกล้างผิดจังหวะหรือไม่",
            )
            add_auto_action("market_analyst", "refresh AUTO scan board", f"age={ma_age}s", "recommended")
    if scan_review_enabled and board_rows:
        perf_locked = [
            row
            for row in board_rows
            if str(row.get("rejectReason", "") or "").lower().startswith("perf_lock")
        ]
        analyze_error_rows = [
            row
            for row in board_rows
            if str(row.get("rejectReason", "") or "").lower() == "analyze_error"
        ]
        qualified_rows = [row for row in board_rows if bool(row.get("qualified"))]
        mixed_guarded_board = (
            len(board_rows) >= 5
            and len(perf_locked) >= 2
            and len(analyze_error_rows) >= 1
            and len(qualified_rows) <= 1
            and (len(perf_locked) + len(analyze_error_rows)) >= max(3, math.ceil(len(board_rows) * 0.60))
        )
        if mixed_guarded_board:
            perf_symbols = ", ".join(str(row.get("symbol", "") or "") for row in perf_locked[:3])
            error_symbols = ", ".join(str(row.get("symbol", "") or "") for row in analyze_error_rows[:3])
            changes: dict[str, dict] = {}
            if bot is AUTO_TRADE and running and execution_mode == "LIVE":
                for row in analyze_error_rows[:4]:
                    sym = str(row.get("symbol", "") or "").upper().strip()
                    if sym:
                        _cooldown_scan_symbol(sym, 10 * 60, "scan analyze_error")

                def set_int(key: str, value: int):
                    old = int(cfg.get(key, value) or value)
                    new = int(value)
                    if old != new:
                        cfg[key] = new
                        changes[key] = {"old": old, "new": new}

                analyze_top = max(3, int(cfg.get("scanAnalyzeTop", 8) or 8))
                top_liquid = max(5, int(cfg.get("scanTopLiquid", 30) or 30))
                guarded_top = max(analyze_top, int(cfg.get("scanGuardedFallbackAnalyzeTop", analyze_top * 2) or analyze_top * 2))
                set_int("scanAnalyzeTop", min(16, analyze_top + 2))
                set_int("scanTopLiquid", min(80, top_liquid + 10))
                set_int("scanGuardedFallbackAnalyzeTop", min(24, max(guarded_top, int(cfg.get("scanAnalyzeTop", analyze_top) or analyze_top) + 4)))
                AUTO_TRADE["config"] = copy.deepcopy(cfg)
                try:
                    _persist_autotrade_snapshot()
                except Exception:
                    pass
            add_issue(
                "market_analyst",
                "medium",
                "Mixed scan board degradation",
                f"perf_locked={len(perf_locked)} analyze_error={len(analyze_error_rows)} qualified={len(qualified_rows)} ({perf_symbols}; errors {error_symbols})",
                "Supervisor จะข้ามเหรียญ analyze_error ชั่วคราวและขยาย scan universe เพื่อหา candidate ใหม่",
            )
            add_auto_action(
                "market_analyst",
                "cooldown analyze_error symbols and expand scan universe",
                f"errors={error_symbols or 'none'} perfLocks={perf_symbols or 'none'}",
                "applied" if bot is AUTO_TRADE else "recommended",
                issue_type="mixed_scan_board_degradation",
                changes=changes,
            )
            add_auto_action(
                "strategy_builder",
                "review perf_lock_payoff candidates before relaxing locks",
                perf_symbols or "perf locks present",
                "recommended",
                issue_type="mixed_scan_board_degradation",
            )
            if bot is AUTO_TRADE:
                _agent_mark("market_analyst", "done", "auto-handled mixed scan board", f"errors={len(analyze_error_rows)} perfLocks={len(perf_locked)}", changes)
        perf_locks_are_root_cause = (
            len(perf_locked) >= 3
            and len(perf_locked) >= max(1, len(board_rows) // 2)
            and not qualified_rows
        )
        scan_none_tune = {}
        no_qualified_scan_candidates = (
            not qualified_rows
            and not safety_hold_active
            and skip_code not in entry_guard_skip_codes
            and not perf_locks_are_root_cause
            and not any(str(row.get("rejectReason", "") or "") == "analyze_error" for row in board_rows)
        )
        if no_qualified_scan_candidates and skip_code == "scan_none" and bot is AUTO_TRADE and running and execution_mode == "LIVE":
            live_open_positions = active_open_positions()
            live_max_open = max(1, int(cfg.get("maxOpenPositions", _DEFAULT_MAX_OPEN_POSITIONS) or _DEFAULT_MAX_OPEN_POSITIONS))
            if max(0, live_max_open - len(live_open_positions)) > 0:
                scan_none_tune = _maybe_tune_low_entry_activity(skip_code, cfg, board_rows)
        if perf_locks_are_root_cause:
            symbols = ", ".join(str(row.get("symbol", "") or "") for row in perf_locked[:4])
            add_issue(
                "strategy_builder",
                "medium",
                "Performance locks reducing entries",
                f"perf_lock {len(perf_locked)}/{len(board_rows)} scan rows ({symbols})",
                "ตรวจว่า perf lock กรองเหรียญแพ้จริงหรือเข้มเกินจน universe เหลือน้อย",
                supervisor_first=True,
            )
            add_auto_action(
                "market_analyst",
                "expand scan universe around perf-locked symbols",
                f"{len(perf_locked)} scan rows perf-locked",
                "recommended",
            )
        if no_qualified_scan_candidates:
            scan_none_tune_handled = bool(scan_none_tune.get("applied") or scan_none_tune.get("alreadyTuned"))
            add_issue(
                "market_analyst",
                "medium",
                "No qualified scan candidates",
                f"scan rows={len(board_rows)} แต่ไม่มี qualified candidate",
                "ขยาย universe หรือปรับ ranking fallback เฉพาะช่วงที่ guard บล็อกทั้งหมด",
                supervisor_first=not scan_none_tune_handled,
            )
            if scan_none_tune.get("applied"):
                add_auto_action(
                    "strategy_builder",
                    "auto-tuned scan-none fallback policy",
                    skip_code,
                    "applied",
                    issue_type="scan_none",
                    changes=scan_none_tune.get("changes"),
                )
                _agent_mark("strategy_builder", "done", "auto-tuned scan-none fallback policy", skip_code, scan_none_tune.get("changes"))
            elif scan_none_tune.get("alreadyTuned"):
                add_auto_action("strategy_builder", "scan-none fallback tune cooldown active", skip_code, "applied", issue_type="scan_none")

    open_positions = active_open_positions()
    trades_last_hour = int(bot.get("tradesLastHour", 0) or 0)
    max_open_positions = max(1, int(cfg.get("maxOpenPositions", _DEFAULT_MAX_OPEN_POSITIONS) or _DEFAULT_MAX_OPEN_POSITIONS))
    open_slots = max(0, max_open_positions - len(open_positions))
    no_new_position_threshold_sec = max(900, int(cfg.get("supervisorNoNewPositionMinutes", 30) or 30) * 60)
    last_trade_at = int(bot.get("lastTradeAt", 0) or 0)
    started_at = int(bot.get("startedAt", 0) or 0)
    last_entry_ref = last_trade_at or started_at
    no_new_position_age = (now - last_entry_ref) if last_entry_ref > 0 else 0
    skip_ts = int(last_skip.get("ts", 0) or 0)
    scan_has_qualified = any(bool(row.get("qualified")) for row in board_rows)
    recent_successful_scan = (
        scan_has_qualified
        or (str((agents.get("market_analyst") or {}).get("lastAction", "") or "") == "scan completed" and agent_updated_at("market_analyst") > skip_ts)
        or (str((agents.get("strategy_builder") or {}).get("lastAction", "") or "") == "entry approved" and agent_updated_at("strategy_builder") > skip_ts)
        or (str((agents.get("execution_agent") or {}).get("lastAction", "") or "") == "place live order" and agent_updated_at("execution_agent") > skip_ts)
    )
    stale_scan_none = skip_code == "scan_none" and recent_successful_scan
    root_cause_reported = bool(skip_code in {"fapi_agreement_required", "binance_permission_required"} or infra_auth_active or infra_data_active)
    no_new_position_issue = (
        not safety_hold_active
        and running
        and execution_mode == "LIVE"
        and scan_mode_cfg
        and trades_last_hour == 0
        and bool(open_positions)
        and open_slots > 0
        and no_new_position_age >= no_new_position_threshold_sec
        and not root_cause_reported
    )
    if no_new_position_issue:
        reason = skip_code or "no new position despite capacity"
        minutes_idle = int(no_new_position_age / 60)
        add_issue(
            "hermes",
            "medium",
            "No new position despite capacity",
            f"ไม่มี position ใหม่ {minutes_idle}m แต่ยังมี slot เหลือ {open_slots}/{max_open_positions} ({reason})",
            "ให้ Supervisor ตรวจ scan blockers, portfolio capacity, confidence gates และ execution rejects แทนการรอเฉยๆ",
        )
        add_auto_action(
            "portfolio_manager",
            "verify open-position capacity and exposure slots",
            f"open={len(open_positions)}/{max_open_positions} idle={minutes_idle}m",
            "applied" if bot is AUTO_TRADE else "recommended",
            issue_type="no_new_position_activity",
        )
        add_auto_action(
            "market_analyst",
            "refresh scan blockers and diversify candidate pool",
            reason,
            "applied" if bot is AUTO_TRADE else "recommended",
            issue_type="no_new_position_activity",
        )
        low_entry_tune = {}
        if bot is AUTO_TRADE:
            low_entry_tune = _maybe_tune_low_entry_activity(reason, cfg, board_rows)
            if low_entry_tune.get("applied"):
                add_auto_action(
                    "strategy_builder",
                    "auto-tuned no-new-position policy",
                    reason,
                    "applied",
                    issue_type="no_new_position_activity",
                    changes=low_entry_tune.get("changes"),
                )
                _agent_mark("strategy_builder", "done", "auto-tuned no-new-position activity", reason, low_entry_tune.get("changes"))
            elif low_entry_tune.get("alreadyTuned"):
                add_auto_action("strategy_builder", "no-new-position tune cooldown active", reason, "applied", issue_type="no_new_position_activity")
            else:
                add_auto_action("strategy_builder", "review entry gates after no-new-position window", reason, "recommended", issue_type="no_new_position_activity")
            _agent_mark("portfolio_manager", "done", "verified capacity for new positions", f"{len(open_positions)}/{max_open_positions}")
            _agent_mark("market_analyst", "done", "refresh scan blockers after no-new-position window", reason)
    if running and execution_mode == "LIVE" and trades_last_hour == 0 and not open_positions and not stale_scan_none and not root_cause_reported:
        reason = skip_code or "no recent trade/open position"
        blocker = known_entry_blockers.get(skip_code)
        if blocker:
            delegated_fix = {}
            if skip_code == "bad_utc_hour" and bot is AUTO_TRADE and running and execution_mode == "LIVE":
                delegated_fix = _maybe_clear_bad_utc_hour_from_config(skip_code, skip_msg, cfg)
            add_issue(
                str(blocker["agent"]),
                "low",
                str(blocker["title"]),
                f"LIVE running แต่ entry ถูก guard บล็อกอยู่ ({reason})",
                str(blocker["suggestion"]),
            )
            if delegated_fix.get("applied"):
                add_auto_action(
                    str(blocker["agent"]),
                    "auto-cleared bad UTC hour",
                    f"removed UTC hours {delegated_fix.get('removed')}",
                    "applied",
                    issue_type=skip_code,
                    changes=delegated_fix.get("changes"),
                )
                _agent_mark("risk_manager", "done", "auto-cleared bad UTC hour", str(delegated_fix.get("removed")), delegated_fix.get("changes"))
            elif delegated_fix.get("alreadyTuned"):
                add_auto_action(str(blocker["agent"]), "bad UTC auto-clear cooldown active", reason, "applied", issue_type=skip_code)
            else:
                add_auto_action(str(blocker["agent"]), str(blocker["action"]), reason)
        else:
            add_issue(
                "hermes",
                "medium",
                "Low entry activity",
                f"LIVE running แต่ tradesLastHour=0 และไม่มี open position ({reason})",
                "ให้ Supervisor ตรวจ log/scan guards และเสนอเฉพาะ root cause ที่บล็อก entry จริง",
            )
            low_entry_tune = {}
            if bot is AUTO_TRADE and running and execution_mode == "LIVE":
                low_entry_tune = _maybe_tune_low_entry_activity(reason, cfg, board_rows)
            if low_entry_tune.get("applied"):
                add_auto_action(
                    "strategy_builder",
                    "auto-tuned low entry policy",
                    reason,
                    "applied",
                    issue_type="low_entry_activity",
                    changes=low_entry_tune.get("changes"),
                )
                _agent_mark("strategy_builder", "done", "auto-tuned low entry activity", reason, low_entry_tune.get("changes"))
            elif low_entry_tune.get("alreadyTuned"):
                add_auto_action("strategy_builder", "low entry auto-tune cooldown active", reason, "applied", issue_type="low_entry_activity")
            else:
                add_auto_action("market_analyst", "refresh scan and explain top entry blockers", reason)
    guardian = agents.get("position_guardian") if isinstance(agents.get("position_guardian"), dict) else {}
    guardian_state = str(guardian.get("state", "todo") or "todo") if guardian else "todo"
    guardian_monitor_age = now - int(AUTO_TRADE.get("_guardianMonitorTs", 0) or 0)
    guardian_stale = open_positions and (guardian_state == "todo" or guardian_monitor_age > 120)
    if guardian_stale:
        add_issue(
            "position_guardian",
            "high",
            "Open positions not actively monitored",
            f"มี open positions {len(open_positions)} แต่ Guardian state={guardian_state} age={agent_age('position_guardian')}s",
            "ให้ Guardian heartbeat ทุก loop เมื่อมี position เปิดอยู่",
        )
        add_auto_action(
            "position_guardian",
            "prioritize open-position heartbeat",
            f"{len(open_positions)} open positions need active monitoring",
            "recommended",
        )

    recent_trades = recent_trade_rows()
    trade_review_source = []
    if not safety_hold_active:
        trade_review_source = (
            _live_closed_trades_from_log(symbol=None, mode="ALL")
            if bot is AUTO_TRADE and running and execution_mode == "LIVE"
            else []
        )
        if not trade_review_source:
            trade_review_source = recent_trades
    periodic_trade_reviews = _supervisor_trade_period_reviews(trade_review_source, now_ts=now)
    latest_trade_review = periodic_trade_reviews[0] if periodic_trade_reviews else {}
    strategy_trade_review_allowed = not infra_auth_active and not infra_data_active
    daily_regime_review = _daily_trade_regime_review(trade_review_source, cfg, now_ts=now) if trade_review_source and strategy_trade_review_allowed else {}
    small_win_tune = {}
    size_streak_tune = {}
    if trade_review_source and strategy_trade_review_allowed and bot is AUTO_TRADE and running and execution_mode == "LIVE":
        size_streak_tune = _maybe_tune_size_multiplier_from_streak(trade_review_source, cfg)
        if size_streak_tune.get("applied"):
            streak_state = size_streak_tune.get("streak") if isinstance(size_streak_tune.get("streak"), dict) else {}
            changed_keys = ", ".join(str(k) for k in size_streak_tune.get("changes", {}).keys())
            action = "auto-raised size after win streak" if str(streak_state.get("kind", "")) == "win" else "auto-reduced size after loss streak"
            add_auto_action(
                "risk_manager",
                action,
                f"{streak_state.get('kind')} streak={streak_state.get('streak')} pnl={streak_state.get('pnl')}",
                "applied",
                issue_type="size_streak",
                changes=size_streak_tune.get("changes"),
            )
            _agent_mark("risk_manager", "done", "auto-tuned streak size multiplier", changed_keys, size_streak_tune.get("changes"))
        elif size_streak_tune.get("alreadyTuned"):
            add_auto_action("risk_manager", "size streak tune cooldown active", str(size_streak_tune.get("signature", "")), "applied", issue_type="size_streak")
    if latest_trade_review and not safety_hold_active and not strategy_trade_review_allowed:
        separated_cause = "Binance API/IP permission incident" if infra_auth_active else "market data provider timeout"
        add_issue(
            "execution_agent" if infra_auth_active else "market_analyst",
            "high",
            "Trade review separated: infra",
            f"{latest_trade_review.get('label')} ถูกกันออกจาก market/strategy tuning เพราะพบ {separated_cause}",
            "แก้/รอ infra ให้ data/order control กลับมาปกติก่อน แล้วค่อยประเมิน win rate/payoff ใหม่",
        )
        add_auto_action(
            "memory_agent",
            "tag recent trade window as infra incident",
            str(latest_trade_review.get("label") or "recent trades"),
            "applied" if bot is AUTO_TRADE else "recommended",
            issue_type="infra_trade_review",
            changes={"strategyTuningSuppressed": True},
        )
        if bot is AUTO_TRADE:
            _agent_mark("memory_agent", "done", "tagged trade review as infra", str(latest_trade_review.get("label") or "recent trades"))
    if latest_trade_review and not safety_hold_active and strategy_trade_review_allowed:
        trades_n = int(latest_trade_review.get("trades", 0) or 0)
        win_rate = float(latest_trade_review.get("winRatePct", 0.0) or 0.0)
        avg_pnl = float(latest_trade_review.get("avgPnl", 0.0) or 0.0)
        total_pnl = float(latest_trade_review.get("pnl", 0.0) or 0.0)
        payoff_ratio = float(latest_trade_review.get("payoffRatio", 0.0) or 0.0)
        quick_losses = int(latest_trade_review.get("quickLosses", 0) or 0)
        small_wins = int(latest_trade_review.get("smallWins", 0) or 0)
        if daily_regime_review.get("degraded"):
            daily_tune = {}
            today_daily = daily_regime_review.get("today") if isinstance(daily_regime_review.get("today"), dict) else {}
            base_daily = daily_regime_review.get("baseline") if isinstance(daily_regime_review.get("baseline"), dict) else {}
            if bot is AUTO_TRADE and running and execution_mode == "LIVE":
                daily_tune = _maybe_tune_daily_entry_regression(daily_regime_review, cfg)
                if daily_tune.get("applied"):
                    changed_keys = ", ".join(str(k) for k in daily_tune.get("changes", {}).keys())
                    add_auto_action(
                        "strategy_builder",
                        "auto-tightened entry after daily regression",
                        changed_keys,
                        "applied",
                        issue_type="daily_entry_regression",
                        changes=daily_tune.get("changes"),
                    )
                    _agent_mark("strategy_builder", "done", "auto-tightened entry after daily regression", changed_keys, daily_tune.get("changes"))
                    add_auto_action(
                        "backtest_agent",
                        "validate daily-regression entry tuning",
                        f"today {today_daily.get('day')} vs baseline {base_daily.get('day')}",
                        "recommended",
                        issue_type="daily_entry_regression",
                    )
                elif daily_tune.get("alreadyTuned"):
                    add_auto_action("strategy_builder", "daily entry-regression tune cooldown active", str(daily_tune.get("signature", "")), "applied", issue_type="daily_entry_regression")
            add_issue(
                "strategy_builder",
                "high",
                "Daily performance regression vs profitable baseline",
                (
                    f"today {today_daily.get('day')} wr={float(today_daily.get('winRatePct', 0.0) or 0.0):.1f}% "
                    f"pnl={float(today_daily.get('pnl', 0.0) or 0.0):.4f} vs "
                    f"{base_daily.get('day')} wr={float(base_daily.get('winRatePct', 0.0) or 0.0):.1f}% "
                    f"pnl={float(base_daily.get('pnl', 0.0) or 0.0):.4f}"
                ),
                "ให้ Strategy Builder เข้ม entry gate, anti-chase, session bias และปิด fallback ที่ทำให้เข้า late/reversal จนกว่าวันนี้กลับมาดี",
                supervisor_first=not (daily_tune.get("applied") or daily_tune.get("alreadyTuned")),
            )
        symbol_drag = _symbol_drag_candidate_from_review(latest_trade_review, cfg)
        if symbol_drag and bot is AUTO_TRADE and running and execution_mode == "LIVE":
            symbol_drag = _maybe_lock_symbol_drag_from_review(latest_trade_review, cfg)
        symbol_drag_explains_window = bool(symbol_drag)
        if trades_n >= 6 and total_pnl < 0 and (win_rate < 45.0 or avg_pnl < -0.04) and not symbol_drag_explains_window:
            negative_tune = {}
            if bot is AUTO_TRADE and running and execution_mode == "LIVE":
                negative_tune = _maybe_tune_negative_expectancy_from_review(latest_trade_review, cfg)
                if negative_tune.get("applied"):
                    changed_keys = ", ".join(str(k) for k in negative_tune.get("changes", {}).keys())
                    add_auto_action(
                        "strategy_builder",
                        "auto-tuned negative expectancy policy",
                        changed_keys,
                        "applied",
                        issue_type="negative_expectancy",
                        changes=negative_tune.get("changes"),
                    )
                    _agent_mark("strategy_builder", "done", "auto-tuned negative expectancy policy", changed_keys, negative_tune.get("changes"))
                elif negative_tune.get("alreadyTuned"):
                    add_auto_action("strategy_builder", "negative expectancy auto-tune cooldown active", str(latest_trade_review.get("label")), "applied", issue_type="negative_expectancy")
            add_issue(
                "strategy_builder",
                "high" if avg_pnl < -0.10 else "medium",
                "Periodic trade review: negative expectancy",
                f"{latest_trade_review.get('label')} wr={win_rate:.1f}% avg={avg_pnl:.4f} pnl={total_pnl:.4f}",
                "ให้ Reflection/Backtest ตรวจ entry gate, confidence floor, symbol selection และ session bias จากช่วงล่าสุด",
                supervisor_first=not (negative_tune.get("applied") or negative_tune.get("alreadyTuned")),
            )
            add_auto_action("reflection_agent", "summarize recent negative expectancy", str(latest_trade_review.get("label")))
            if negative_tune.get("applied"):
                add_auto_action("backtest_agent", "validate negative expectancy tuning", str(latest_trade_review.get("label")), "recommended", issue_type="negative_expectancy")
        profit_factor = float(latest_trade_review.get("profitFactor", 0.0) or 0.0)
        payoff_expectancy_risk = (
            total_pnl <= 0.0
            or avg_pnl < 0.08
            or profit_factor < 1.50
            or win_rate < 55.0
        )
        weak_payoff_ratio = trades_n >= 6 and 0.0 < payoff_ratio < 0.75 and payoff_expectancy_risk
        if weak_payoff_ratio and not symbol_drag_explains_window:
            payoff_tune = {}
            if bot is AUTO_TRADE and running and execution_mode == "LIVE":
                payoff_tune = _maybe_tune_weak_payoff_from_review(latest_trade_review, cfg)
                if payoff_tune.get("applied"):
                    changed_keys = ", ".join(str(k) for k in payoff_tune.get("changes", {}).keys())
                    add_auto_action(
                        "position_guardian",
                        "auto-tuned weak payoff policy",
                        changed_keys,
                        "applied",
                        issue_type="weak_payoff_ratio",
                        changes=payoff_tune.get("changes"),
                    )
                    _agent_mark("position_guardian", "done", "auto-tuned weak payoff policy", changed_keys, payoff_tune.get("changes"))
                elif payoff_tune.get("alreadyTuned"):
                    add_auto_action("position_guardian", "weak payoff auto-tune cooldown active", str(latest_trade_review.get("label")), "applied", issue_type="weak_payoff_ratio")
                elif payoff_tune.get("reason") == "no_safe_delta":
                    add_auto_action(
                        "position_guardian",
                        "weak payoff policy already at safe limits",
                        str(latest_trade_review.get("label")),
                        "applied",
                        issue_type="weak_payoff_ratio",
                    )
                    _agent_mark("position_guardian", "done", "weak payoff policy already at safe limits", str(latest_trade_review.get("label")))
            payoff_tune_handled = bool(
                payoff_tune.get("applied")
                or payoff_tune.get("alreadyTuned")
                or payoff_tune.get("reason") == "no_safe_delta"
            )
            add_issue(
                "position_guardian",
                "medium",
                "Periodic trade review: weak payoff ratio",
                f"{latest_trade_review.get('label')} payoff={payoff_ratio:.2f} avgWin={latest_trade_review.get('avgWin')} avgLoss={latest_trade_review.get('avgLoss')}",
                "ปรับ TP/SL, hold-winner, profit-lock และ stop behavior ให้กำไรเฉลี่ยชนะขนาดแพ้เฉลี่ยมากขึ้น",
                supervisor_first=not payoff_tune_handled,
            )
            add_auto_action("backtest_agent", "validate payoff-ratio tuning", str(latest_trade_review.get("label")))
        if trades_n >= 6 and quick_losses >= max(2, math.ceil(trades_n * 0.25)):
            add_issue(
                "strategy_builder",
                "medium",
                "Periodic trade review: quick losses after entry",
                f"{quick_losses}/{trades_n} losses closed within 8 minutes",
                "ตรวจว่า early entry/late chase/slippage/HTF conflict ทำให้เข้าไวแต่ผิดจังหวะหรือไม่",
                supervisor_first=True,
            )
        small_win_cluster = small_wins >= max(3, math.ceil(trades_n * 0.30))
        small_win_payoff_risk = (
            total_pnl <= 0.0
            or avg_pnl < 0.08
            or profit_factor < 1.50
            or (0.0 < payoff_ratio < 0.90 and win_rate < 65.0)
        )
        if trades_n >= 6 and small_win_cluster and small_win_payoff_risk:
            if bot is AUTO_TRADE and running and execution_mode == "LIVE":
                small_win_tune = _maybe_tune_small_profit_capture_from_review(latest_trade_review, cfg)
                if small_win_tune.get("applied"):
                    changed_keys = ", ".join(str(k) for k in small_win_tune.get("changes", {}).keys())
                    add_auto_action(
                        "position_guardian",
                        "auto-tuned small-profit capture policy",
                        changed_keys,
                        "applied",
                        issue_type="small_profit_capture",
                        changes=small_win_tune.get("changes"),
                    )
                    _agent_mark("position_guardian", "done", "auto-tuned small-profit capture policy", changed_keys, small_win_tune.get("changes"))
                elif small_win_tune.get("alreadyTuned"):
                    add_auto_action("position_guardian", "small-profit capture tune cooldown active", str(latest_trade_review.get("label")), "applied", issue_type="small_profit_capture")
                elif small_win_tune.get("reason") == "no_safe_delta":
                    add_auto_action("position_guardian", "small-profit capture policy already at safe limits", str(latest_trade_review.get("label")), "applied", issue_type="small_profit_capture")
                    _agent_mark("position_guardian", "done", "small-profit capture policy already at safe limits", str(latest_trade_review.get("label")))
            small_win_tune_handled = bool(
                small_win_tune.get("applied")
                or small_win_tune.get("alreadyTuned")
                or small_win_tune.get("reason") == "no_safe_delta"
            )
            add_issue(
                "position_guardian",
                "medium",
                "Periodic trade review: profit giveback / small wins dominate",
                f"{small_wins}/{trades_n} wins below 0.25 USDT · avgWin={latest_trade_review.get('avgWin')}",
                "ลด profit-lock trigger/giveback และเพิ่ม keep floor เพื่อไม่ปล่อย peak +0.4/+0.5 ไหลกลับเป็น +0.01",
                supervisor_first=not small_win_tune_handled,
            )
        if symbol_drag_explains_window:
            sym = str(symbol_drag.get("symbol", "") or "UNKNOWN")
            symbol_drag_handled = bool(symbol_drag.get("locked")) or bool(symbol_drag.get("alreadyLocked"))
            lock_text = (
                f" · locked {int(symbol_drag.get('minutes', 0) or 0)}m"
                if bool(symbol_drag.get("locked")) and not bool(symbol_drag.get("alreadyLocked"))
                else " · already locked"
                if bool(symbol_drag.get("alreadyLocked"))
                else ""
            )
            add_issue(
                "portfolio_manager",
                "medium",
                "Periodic trade review: symbol drag",
                f"{sym} pnl={float(symbol_drag.get('pnl', 0.0) or 0.0):.4f} over {int(symbol_drag.get('trades', 0) or 0)} trades{lock_text}",
                "ลด priority/lock เฉพาะ symbol ที่ถ่วงผลลัพธ์จนกว่า learning/backtest จะยืนยันว่ากลับมาดี",
            )
            add_auto_action("portfolio_manager", "temporary perf-lock dominant symbol drag", sym, "applied" if bool(symbol_drag.get("locked")) else "recommended", issue_type="symbol_drag")
            if bot is AUTO_TRADE and bool(symbol_drag.get("locked")):
                _agent_mark("portfolio_manager", "done", "temporary perf-lock dominant symbol drag", sym, {"symbol": sym, "until": symbol_drag.get("until")})

    small_profit_exits = [
        t
        for t in recent_trades
        if 0.0 < float(t.get("pnl", 0.0) or 0.0) < 0.25
        and str(t.get("reason", "") or "") in {"LIVE_CUT_LOSING_SIDE", "LIVE_CLOSE", "WEAK_SIGNAL", "RETRACE"}
    ]
    if not safety_hold_active and len(small_profit_exits) >= 3:
        avg_pnl = sum(float(t.get("pnl", 0.0) or 0.0) for t in small_profit_exits) / max(len(small_profit_exits), 1)
        if bot is AUTO_TRADE and running and execution_mode == "LIVE" and not small_win_tune:
            small_win_tune = _maybe_tune_small_profit_capture_from_review(
                {
                    "label": "recent_small_profit_exits",
                    "trades": len(small_profit_exits),
                    "smallWins": len(small_profit_exits),
                },
                cfg,
            )
            if small_win_tune.get("applied"):
                changed_keys = ", ".join(str(k) for k in small_win_tune.get("changes", {}).keys())
                add_auto_action(
                    "position_guardian",
                    "auto-tuned recent small-profit giveback",
                    changed_keys,
                    "applied",
                    issue_type="small_profit_capture",
                    changes=small_win_tune.get("changes"),
                )
                _agent_mark("position_guardian", "done", "auto-tuned recent small-profit giveback", changed_keys, small_win_tune.get("changes"))
            elif small_win_tune.get("alreadyTuned"):
                add_auto_action("position_guardian", "small-profit giveback tune cooldown active", "recent small-profit exits", "applied", issue_type="small_profit_capture")
        small_profit_exit_handled = bool(
            small_win_tune.get("applied")
            or small_win_tune.get("alreadyTuned")
            or small_win_tune.get("reason") == "no_safe_delta"
        )
        add_issue(
            "position_guardian",
            "medium",
            "Profit capture may be too early",
            f"ปิดกำไรเล็ก {len(small_profit_exits)} ไม้ล่าสุด avg={avg_pnl:.4f} USDT",
            "ให้ Guardian/Strategy Builder ตรวจ hold-winner, weak-signal exit และ profit-lock threshold",
            supervisor_first=not small_profit_exit_handled,
        )
        add_auto_action(
            "strategy_builder",
            "review hold-winner evidence before next entry",
            f"{len(small_profit_exits)} recent small-profit exits",
            "recommended",
        )

    trade_count = len(recent_trades)
    memory_runs = int((agents.get("memory_agent") or {}).get("runs", 0) or 0) if isinstance(agents.get("memory_agent"), dict) else 0
    reflection_runs = int((agents.get("reflection_agent") or {}).get("runs", 0) or 0) if isinstance(agents.get("reflection_agent"), dict) else 0
    backtest_runs = int((agents.get("backtest_agent") or {}).get("runs", 0) or 0) if isinstance(agents.get("backtest_agent"), dict) else 0
    if not safety_hold_active and trade_count >= 3 and memory_runs == 0:
        add_issue(
            "memory_agent",
            "medium",
            "Trade memory not confirmed",
            f"มี closed trades {trade_count} แต่ memory_agent runs=0",
            "กระตุ้น Memory Agent ให้บันทึก trade/features ก่อนรอบ learning ถัดไป",
            supervisor_first=True,
        )
        add_auto_action("memory_agent", "store recent trade outcomes", f"{trade_count} recent trades need memory confirmation", "applied" if bot is AUTO_TRADE else "recommended", issue_type="trade_memory_missing")
        if bot is AUTO_TRADE:
            _agent_mark("memory_agent", "done", "store recent trade outcomes", f"{trade_count} recent trades")
    if not safety_hold_active and trade_count >= 6 and reflection_runs == 0:
        add_issue(
            "reflection_agent",
            "medium",
            "Reflection not using recent losses/wins",
            f"มี closed trades {trade_count} แต่ reflection_agent runs=0",
            "กระตุ้น Reflection Agent ให้สรุป pattern แพ้/ชนะและเสนอ config delta",
            supervisor_first=True,
        )
        add_auto_action("reflection_agent", "summarize recent trade outcomes", f"{trade_count} recent trades available", "applied" if bot is AUTO_TRADE else "recommended", issue_type="reflection_missing")
        if bot is AUTO_TRADE:
            _agent_mark("reflection_agent", "done", "summarize recent trade outcomes", f"{trade_count} recent trades")
            if cfg.get("autoReflectSummary"):
                threading.Thread(target=_wire_reflection_summary, args=(recent_trades,), daemon=True).start()
    if not safety_hold_active and trade_count >= 8 and backtest_runs == 0:
        add_issue(
            "backtest_agent",
            "medium",
            "Backtest validation missing",
            f"มี closed trades {trade_count} แต่ backtest_agent runs=0",
            "ให้ Backtest Agent validate tuning proposal ก่อนปรับ config LIVE",
            supervisor_first=True,
        )
        add_auto_action("backtest_agent", "validate latest learning proposal", f"{trade_count} recent trades available", "applied" if bot is AUTO_TRADE else "recommended", issue_type="backtest_missing")
        if bot is AUTO_TRADE:
            _agent_mark("backtest_agent", "done", "validate latest learning proposal", f"{trade_count} recent trades")
            if cfg.get("autoValidateLearning"):
                threading.Thread(target=_wire_backtest_validate, args=(cfg,), daemon=True).start()

    agent_health = {
        agent_id: {
            "state": str(agent.get("state", "todo") or "todo"),
            "lastAction": str(agent.get("lastAction", "") or ""),
            "ageSec": agent_age(agent_id),
            "runs": int(agent.get("runs", 0) or 0),
        }
        for agent_id, agent in agents.items()
        if isinstance(agent, dict)
    }

    severity_rank = {"low": 1, "medium": 2, "high": 3}
    top_severity = "ok"
    if issues:
        top_severity = max((str(i.get("severity", "low")) for i in issues), key=lambda s: severity_rank.get(s, 0))
    summary = "ทีม agent ปกติ" if not issues else f"พบ {len(issues)} จุดที่ควรปรับปรุง"
    applied_auto_actions = [x for x in auto_actions if str(x.get("status", "") or "") == "applied"]
    if applied_auto_actions:
        summary += f" · Supervisor actions {len(applied_auto_actions)}"
    if bot is AUTO_TRADE:
        supervisor_data = {
            "severity": top_severity,
            "issues": len(issues),
            "autoActions": len(auto_actions),
        }
        if top_severity == "high":
            _agent_mark("hermes_supervisor", "done", "reviewed high-severity subagent issue", summary, supervisor_data)
        else:
            _agent_mark("hermes_supervisor", "done", "reviewed subagent health", summary, supervisor_data)
        state = ensure_agent_state(bot.get("hermesAgents"))
        agents = state.get("agents") if isinstance(state.get("agents"), dict) else agents
        runs_by_agent = {
            agent_id: int(agent.get("runs", 0) or 0)
            for agent_id, agent in agents.items()
            if isinstance(agent, dict)
        }
        agent_health = {
            agent_id: {
                "state": str(agent.get("state", "todo") or "todo"),
                "lastAction": str(agent.get("lastAction", "") or ""),
                "ageSec": agent_age(agent_id),
                "runs": int(agent.get("runs", 0) or 0),
            }
            for agent_id, agent in agents.items()
            if isinstance(agent, dict)
        }
    review = {
        "reviewedAt": now,
        "severity": top_severity,
        "summary": summary,
        "issues": issues[:8],
        "suggestions": suggestions[:8],
        "autoActions": auto_actions[:8],
        "periodicTradeReview": periodic_trade_reviews[:3],
        "tradeReviewAttribution": {
            "primary": "infra_auth" if infra_auth_active else "infra_data" if infra_data_active else "market_strategy",
            "strategyTuningSuppressed": bool(infra_auth_active or infra_data_active),
            "infraAuthIncident": infra_auth_incident if infra_auth_active else {},
            "infraDataIncident": data_provider_error if infra_data_active else {},
        },
        "dailyRegimeReview": daily_regime_review,
        "agentHealth": agent_health,
        "agentRuns": runs_by_agent,
    }
    if bot is AUTO_TRADE:
        AUTO_TRADE["hermesSupervisorReview"] = review
    return review


def _minimal_blocked_severity(agent_id: str, action: str, detail: str = "") -> str:
    """Pick severity for a blocked state in the minimal cached review.

    Routine safety holds (cooldown, no-trade window, etc.) are "low". Real
    risk_manager cap breaches and failed checks are "high" because they block
    new entries until a Cmux review happens. A check failure with a TimeoutError
    is still classified as "low" (safety hold) since the underlying regime is
    not necessarily broken.
    """
    a = (action or "").strip().lower()
    d = (detail or "").strip().lower()
    combined = f"{a} {d}"
    if agent_id == "risk_manager":
        # "adaptive cooldown check failed" with a timeout is still a safety hold
        # (low) — only non-timeout check failures escalate to high.
        if a == "adaptive cooldown check failed" and "timeout" in combined:
            return "low"
        high_tokens = (
            "check failed",
            "symbol daily cap",
            "kill_switch",
            "kill switch",
            "fatal",
            "denied",
            "exceeded",
        )
        if any(tok in a for tok in high_tokens):
            return "high"
        # All other risk_manager actions are routine safety holds.
        return "low"
    if agent_id in {"execution_agent", "market_analyst"}:
        high_tokens = ("kill_switch", "kill switch", "fatal", "denied", "exceeded")
        if any(tok in a for tok in high_tokens):
            return "high"
        return "low"
    # strategy_builder, portfolio_manager, data_quality_guard, hermes_supervisor
    return "low"


def _minimal_blocked_title(agent_id: str, action: str) -> str:
    """Map a blocked agent's action to the canonical supervisor title."""
    a = (action or "").strip().lower()
    if not a:
        return "Agent blocked"
    if a in {"portfolio capacity full", "symbol daily cap"}:
        return "Capacity hold"
    if a in {
        "adaptive cooldown hold",
        "risk cooldown active",
        "market volatility pause",
        "armed risk cooldown",
        "no trade window",
        "bad utc hour",
        "adaptive cooldown check failed",
        "signal wait",
        "confidence below adaptive minimum",
        "late long chase",
        "late short chase",
        "primary symbol already open",
        "symbol volatile cooldown",
    }:
        return "Safety hold"
    return "Agent blocked"


def _minimal_hermes_supervisor_review(bot_state: dict | None = None, reason: str = "cached_fallback") -> dict:
    bot = bot_state if isinstance(bot_state, dict) else AUTO_TRADE
    state = ensure_agent_state(bot.get("hermesAgents"))
    agents = state.get("agents") if isinstance(state.get("agents"), dict) else {}
    now = int(time.time())
    blocked = [
        {
            "agent": agent_id,
            "severity": _minimal_blocked_severity(agent_id, str(agent.get("lastAction", "") or ""), str(agent.get("lastReason", "") or "")),
            # Translate routine blocked actions to the human-readable title the
            # supervisor UI uses ("Safety hold" / "Capacity hold" / "Agent blocked").
            "title": _minimal_blocked_title(agent_id, str(agent.get("lastAction", "") or "")),
            "detail": str(agent.get("lastReason", "") or ""),
            "suggestion": "อ่าน cached review ระหว่าง endpoint หนักหรือรอรอบ supervisor ถัดไป",
        }
        for agent_id, agent in agents.items()
        if isinstance(agent, dict) and str(agent.get("state", "") or "") == "blocked"
    ]
    pause_until = int(bot.get("pauseUntil", 0) or 0)
    if pause_until > now and not any(item["agent"] == "risk_manager" for item in blocked):
        blocked.insert(
            0,
            {
                "agent": "risk_manager",
                "severity": "medium",
                "title": "risk cooldown active",
                "detail": f"{pause_until - now}s remaining",
                "suggestion": "ให้ market analyst refresh watchlist ได้ แต่ยังห้ามเปิด order ระหว่าง cooldown",
            },
        )
    severity_rank = {"low": 1, "medium": 2, "high": 3}
    if not blocked:
        top = "low"
    else:
        top = max(
            (str(item.get("severity", "low")) for item in blocked),
            key=lambda s: severity_rank.get(s, 0),
        )
    return {
        "reviewedAt": now,
        "severity": top,
        "summary": f"Cached fallback review · {reason}",
        "issues": blocked[:8],
        "suggestions": [],
        "autoActions": [],
        "periodicTradeReview": [],
        "tradeReviewAttribution": {"primary": "cached_fallback", "strategyTuningSuppressed": False},
        "dailyRegimeReview": {},
        "agentHealth": {
            agent_id: {
                "state": str(agent.get("state", "todo") or "todo"),
                "lastAction": str(agent.get("lastAction", "") or ""),
                "ageSec": now - int(agent.get("updatedAt", now) or now),
                "runs": int(agent.get("runs", 0) or 0),
            }
            for agent_id, agent in agents.items()
            if isinstance(agent, dict)
        },
        "agentRuns": {
            agent_id: int(agent.get("runs", 0) or 0)
            for agent_id, agent in agents.items()
            if isinstance(agent, dict)
        },
        "cachedFallback": True,
    }


def _cached_hermes_supervisor_review(
    bot_state: dict | None = None,
    max_age_sec: int = 45,
    allow_compute: bool = False,
) -> dict:
    bot = bot_state if isinstance(bot_state, dict) else AUTO_TRADE
    now = int(time.time())
    cached = bot.get("hermesSupervisorReview") if isinstance(bot.get("hermesSupervisorReview"), dict) else {}
    reviewed_at = int(cached.get("reviewedAt", 0) or 0) if cached else 0
    if cached and (now - reviewed_at <= max_age_sec or not allow_compute):
        out = dict(cached)
        out["cached"] = True
        out["staleSec"] = max(0, now - reviewed_at)
        return out
    if allow_compute:
        return _hermes_supervisor_review(bot)
    fallback = _minimal_hermes_supervisor_review(bot, "no_fresh_review")
    if bot is AUTO_TRADE:
        AUTO_TRADE["hermesSupervisorReview"] = fallback
    return fallback


@app.get("/hermes/supervisor-review")
def hermes_supervisor_review(refresh: bool = False):
    if not refresh:
        return _cached_hermes_supervisor_review(AUTO_TRADE, max_age_sec=60, allow_compute=False)
    return _hermes_supervisor_review()


@app.post("/hermes/supervisor/external-signal")
def hermes_supervisor_external_signal(payload: dict = Body(default_factory=dict)):
    cfg = AUTO_TRADE.get("config") if isinstance(AUTO_TRADE.get("config"), dict) else {}
    out = _maybe_tune_external_signal_guard(payload, cfg)
    return {
        "ok": True,
        "supervisorControlled": True,
        "bounded": True,
        "result": out,
        "config": AUTO_TRADE.get("config") or {},
    }




@app.get("/autotrade/status-lite")
async def autotrade_status_lite():
    cfg = AUTO_TRADE.get("config") or {}
    if isinstance(cfg, dict) and cfg:
        _sync_autotrade_leverage_cap_from_cfg(cfg)
    live_locks = AUTO_TRADE.get("liveProfitLocks") if isinstance(AUTO_TRADE.get("liveProfitLocks"), dict) else {}
    open_live_positions: list[dict] = []
    
    # Fetch current market prices for real-time uPnL calculation
    current_prices = {}
    try:
        if live_locks:
            symbols = [str(lock.get("symbol", "")).upper().strip() for lock in live_locks.values() if isinstance(lock, dict) and lock.get("symbol")]
            if symbols:
                prices = await _fetch_market_prices_batch(symbols)
                current_prices = prices if isinstance(prices, dict) else {}
    except Exception:
        current_prices = {}
    
    for lock in live_locks.values():
        if not isinstance(lock, dict):
            continue
        symbol = str(lock.get("symbol", "") or "").upper().strip()
        side = str(lock.get("side", "") or "").upper().strip()
        qty = float(lock.get("qty", 0.0) or 0.0)
        if not symbol or side not in ("LONG", "SHORT") or qty <= 0:
            continue
        
        # Calculate real-time uPnL from current market price
        entry = float(lock.get("entryMark", 0.0) or 0.0)
        current_price = float(current_prices.get(symbol, lock.get("markPrice", 0.0) or 0.0) or 0.0)
        
        if current_price > 0 and entry > 0:
            if side == "LONG":
                real_time_upnl = (current_price - entry) * qty
            else:
                real_time_upnl = (entry - current_price) * qty
        else:
            real_time_upnl = float(lock.get("unRealizedProfit", 0.0) or 0.0)
        
        op = {
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "entryMark": entry,
            "markPrice": current_price,  # Use current price for real-time updates
            "notionalUsdtApprox": round(float(lock.get("notionalUsdtApprox", 0.0) or 0.0), 6),
            "unRealizedProfit": round(float(real_time_upnl), 6),  # Use calculated real-time uPnL
            "leverage": _position_display_leverage(symbol, cfg, lock.get("leverage")),
            "profitLockArmed": bool(lock.get("armed", False)),
            "profitLockUsdt": round(float(lock.get("lockUsdt", 0.0) or 0.0), 6),
            "peakUnrealizedPnl": round(float(lock.get("peak", 0.0) or 0.0), 6),
            "localTp": lock.get("tp"),
            "localSl": lock.get("sl"),
        }
        _attach_symbol_profile(op, None)
        open_live_positions.append(op)
    # Binance is source of truth — always try to fetch live positions directly.
    # This ensures positions are visible even when guardian hasn't populated
    # liveProfitLocks yet, or executionMode is not "LIVE".
    _binance_fetched = False
    try:
        binance_key = os.getenv("BINANCE_API_KEY")
        binance_secret = os.getenv("BINANCE_API_SECRET")
        binance_base = _binance_base()
        if binance_key and binance_secret:
            binance_positions = await asyncio.wait_for(
                _pick_live_orphan_positions(binance_key, binance_secret, binance_base),
                timeout=6.0,
            )
            for op in binance_positions:
                k = _live_lock_key(str(op.get("symbol", "")), str(op.get("side", "")))
                lk = live_locks.get(k) if isinstance(live_locks, dict) else None
                if isinstance(lk, dict):
                    op["profitLockArmed"] = bool(lk.get("armed", False))
                    op["profitLockUsdt"] = round(float(lk.get("lockUsdt", 0.0) or 0.0), 6)
                    op["peakUnrealizedPnl"] = round(float(lk.get("peak", 0.0) or 0.0), 6)
                    op["localTp"] = lk.get("tp")
                    op["localSl"] = lk.get("sl")
                op["leverage"] = _position_display_leverage(op.get("symbol"), cfg, op.get("leverage"))
                _attach_symbol_profile(op, None)
            open_live_positions = binance_positions
            _binance_fetched = True
    except Exception as e:
        _log.warning("status-lite Binance position fetch failed: %s", e)
    # Fallback to liveProfitLocks ONLY when Binance API call failed (not when it returned empty).
    # If Binance explicitly returns empty, the positions are truly closed.
    if not _binance_fetched:
        # liveProfitLocks fallback
        for lock in live_locks.values():
            if not isinstance(lock, dict):
                continue
            symbol = str(lock.get("symbol", "") or "").upper().strip()
            side = str(lock.get("side", "") or "").upper().strip()
            qty = float(lock.get("qty", 0.0) or 0.0)
            if not symbol or side not in ("LONG", "SHORT") or qty <= 0:
                continue
            entry = float(lock.get("entryMark", 0.0) or 0.0)
            current_price = float(current_prices.get(symbol, lock.get("markPrice", 0.0) or 0.0) or 0.0)
            if current_price > 0 and entry > 0:
                if side == "LONG":
                    real_time_upnl = (current_price - entry) * qty
                else:
                    real_time_upnl = (entry - current_price) * qty
            else:
                real_time_upnl = float(lock.get("unRealizedProfit", 0.0) or 0.0)
            op = {
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "entryMark": entry,
                "markPrice": current_price,
                "notionalUsdtApprox": round(float(lock.get("notionalUsdtApprox", 0.0) or 0.0), 6),
                "unRealizedProfit": round(float(real_time_upnl), 6),
                "leverage": _position_display_leverage(symbol, cfg, lock.get("leverage")),
                "profitLockArmed": bool(lock.get("armed", False)),
                "profitLockUsdt": round(float(lock.get("lockUsdt", 0.0) or 0.0), 6),
                "peakUnrealizedPnl": round(float(lock.get("peak", 0.0) or 0.0), 6),
                "localTp": lock.get("tp"),
                "localSl": lock.get("sl"),
            }
            _attach_symbol_profile(op, None)
            open_live_positions.append(op)
    open_live_positions.sort(key=lambda pos: float(pos.get("notionalUsdtApprox", 0.0) or 0.0), reverse=True)
    _position_guardian_status_heartbeat(open_live_positions)
    lead_live = open_live_positions[0] if open_live_positions else {}
    live_position = {
        "symbol": str(lead_live.get("symbol", "") or ""),
        "side": str(lead_live.get("side", "FLAT") or "FLAT"),
        "qty": float(lead_live.get("qty", 0.0) or 0.0),
        "notionalUsdtApprox": float(lead_live.get("notionalUsdtApprox", 0.0) or 0.0),
        "entryMark": lead_live.get("entryMark"),
        "markPrice": lead_live.get("markPrice"),
        "localTp": lead_live.get("localTp"),
        "localSl": lead_live.get("localSl"),
        "leverage": _position_display_leverage(lead_live.get("symbol"), cfg, lead_live.get("leverage")),
    }
    p = AUTO_TRADE.get("paper") if isinstance(AUTO_TRADE.get("paper"), dict) else {}
    p_wins = int(p.get("wins", 0) or 0)
    p_losses = int(p.get("losses", 0) or 0)
    p_total = p_wins + p_losses
    p_history = p.get("history") if isinstance(p.get("history"), list) else []
    today_start = int(datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    p_wins_today = sum(1 for t in p_history if int(t.get("closedAt", 0) or 0) >= today_start and float(t.get("pnl", 0) or 0) >= 0)
    p_losses_today = sum(1 for t in p_history if int(t.get("closedAt", 0) or 0) >= today_start and float(t.get("pnl", 0) or 0) < 0)
    p_total_today = p_wins_today + p_losses_today
    p_pnl_today = sum(float(t.get("pnl", 0) or 0) for t in p_history if int(t.get("closedAt", 0) or 0) >= today_start)
    stat_symbol = cfg.get("symbol") or live_position.get("symbol") or None
    live_symbol_stats = _aggregate_live_trade_stats_from_log(stat_symbol)
    live_wins = int(live_symbol_stats.get("wins", 0) or 0)
    live_losses = int(live_symbol_stats.get("losses", 0) or 0)
    live_total = live_wins + live_losses
    live_wins_today = int(live_symbol_stats.get("winsToday", 0) or 0)
    live_losses_today = int(live_symbol_stats.get("lossesToday", 0) or 0)
    live_total_today = live_wins_today + live_losses_today
    live_last_trades = live_symbol_stats.get("lastTrades", []) if isinstance(live_symbol_stats.get("lastTrades"), list) else []
    live_all_stats = _aggregate_live_trade_stats_from_log(None)
    all_wins = int(live_all_stats.get("wins", 0) or 0)
    all_losses = int(live_all_stats.get("losses", 0) or 0)
    all_total = all_wins + all_losses
    all_wins_today = int(live_all_stats.get("winsToday", 0) or 0)
    all_losses_today = int(live_all_stats.get("lossesToday", 0) or 0)
    all_total_today = all_wins_today + all_losses_today
    now_ts = int(time.time())
    supervisor_review = _cached_hermes_supervisor_review(AUTO_TRADE, max_age_sec=60, allow_compute=False)
    public_cfg = dict(cfg)
    scan_mode_public = bool(public_cfg.get("marketScan")) or str(public_cfg.get("symbol", "") or "").upper() in ("AUTO", "SCAN")
    if scan_mode_public:
        active_scan_symbol = str((AUTO_TRADE.get("lastDecision") or {}).get("symbol") or public_cfg.get("symbol") or "").upper().strip()
        if active_scan_symbol and active_scan_symbol not in ("AUTO", "SCAN"):
            public_cfg["activeScanSymbol"] = active_scan_symbol
        public_cfg["symbol"] = "AUTO"
        public_cfg["marketScan"] = True
    return {
        "running": bool(AUTO_TRADE.get("running")),
        "sessionId": AUTO_TRADE.get("sessionId"),
        "startedAt": int(AUTO_TRADE.get("startedAt", 0) or 0),
        "autotradeTask": _autotrade_task_state(),
        "lastTradeAt": int(AUTO_TRADE.get("lastTradeAt", 0) or 0),
        "tradesLastHour": len([t for t in AUTO_TRADE.get("trades", []) if now_ts - int(t or 0) < 3600]),
        "pauseUntil": int(AUTO_TRADE.get("pauseUntil", 0) or 0),
        "riskCooldownLossSignature": str(AUTO_TRADE.get("riskCooldownLossSignature", "") or ""),
        "riskCooldownBySymbol": _prune_risk_cooldowns(now_ts),
        "consecutiveErrors": int(AUTO_TRADE.get("consecutiveErrors", 0) or 0),
        "lastDecision": AUTO_TRADE.get("lastDecision"),
        "lastSkip": AUTO_TRADE.get("lastSkip"),
        "log": list(AUTO_TRADE.get("log", []))[:8],
        "scanBoard": list(AUTO_TRADE.get("scanBoard", []))[:6],
        "cooldownWatchlist": AUTO_TRADE.get("cooldownWatchlist") if isinstance(AUTO_TRADE.get("cooldownWatchlist"), dict) else {},
        "paper": {
            "position": p.get("position"),
            "wins": p_wins,
            "losses": p_losses,
            "winRatePct": round((p_wins / p_total) * 100, 2) if p_total > 0 else 0.0,
            "realizedPnl": round(float(p.get("realizedPnl", 0.0) or 0.0), 6),
            "lastTrades": list(p.get("history", []))[:10] if isinstance(p.get("history"), list) else [],
        },
        "liveStats": {
            "symbol": stat_symbol,
            "wins": live_wins,
            "losses": live_losses,
            "winRatePct": round((live_wins / live_total) * 100, 2) if live_total > 0 else 0.0,
            "realizedPnl": round(float(live_symbol_stats.get("realizedPnl", 0.0) or 0.0), 6),
            "winsToday": live_wins_today,
            "lossesToday": live_losses_today,
            "winRatePctToday": round((live_wins_today / live_total_today) * 100, 2) if live_total_today > 0 else 0.0,
            "realizedPnlToday": round(float(live_symbol_stats.get("realizedPnlToday", 0.0) or 0.0), 6),
            "lastTrades": live_last_trades,
        },
        "liveStatsAll": {
            "wins": all_wins,
            "losses": all_losses,
            "winRatePct": round((all_wins / all_total) * 100, 2) if all_total > 0 else 0.0,
            "realizedPnl": round(float(live_all_stats.get("realizedPnl", 0.0) or 0.0), 6),
            "winsToday": all_wins_today,
            "lossesToday": all_losses_today,
            "winRatePctToday": round((all_wins_today / all_total_today) * 100, 2) if all_total_today > 0 else 0.0,
            "realizedPnlToday": round(float(live_all_stats.get("realizedPnlToday", 0.0) or 0.0), 6),
        },
        "kpiTodayAllSymbols": {
            "live": {
                "wins": all_wins_today,
                "losses": all_losses_today,
                "winRatePct": round((all_wins_today / all_total_today) * 100, 2) if all_total_today > 0 else 0.0,
                "realizedPnl": round(float(live_all_stats.get("realizedPnlToday", 0.0) or 0.0), 6),
            }
        },
        "kpiToday": {
            "live": {
                "wins": all_wins_today,
                "losses": all_losses_today,
                "winRatePct": round((all_wins_today / all_total_today) * 100, 2) if all_total_today > 0 else 0.0,
                "realizedPnl": round(float(live_all_stats.get("realizedPnlToday", 0.0) or 0.0), 6),
            },
            "paper": {
                "wins": p_wins_today,
                "losses": p_losses_today,
                "winRatePct": round((p_wins_today / p_total_today) * 100, 2) if p_total_today > 0 else 0.0,
                "realizedPnl": round(float(p_pnl_today), 6),
            },
        },
        "liveDailyPnl": round(float(live_all_stats.get("realizedPnlToday", 0.0) or 0.0), 6),
        "activePosition": {
            "mode": cfg.get("executionMode", "PAPER"),
            "paper": {"side": "FLAT", "qty": 0.0, "notionalUsdtApprox": 0.0},
            "live": live_position,
        },
        "openLivePositions": open_live_positions,
        "hermesAgents": ensure_agent_state(AUTO_TRADE.get("hermesAgents")),
        "hermesSupervisorReview": supervisor_review,
        "config": {
            "symbol": public_cfg.get("symbol"),
            "primarySymbol": public_cfg.get("primarySymbol"),
            "activeScanSymbol": public_cfg.get("activeScanSymbol"),
            "executionMode": public_cfg.get("executionMode"),
            "intervalSec": public_cfg.get("intervalSec"),
            "marketScan": public_cfg.get("marketScan"),
            "leverage": public_cfg.get("leverage"),
            "leverageMin": public_cfg.get("leverageMin"),
            "leverageMax": public_cfg.get("leverageMax"),
            "leverageAutoEnabled": public_cfg.get("leverageAutoEnabled"),
            "adaptiveLeverageEnabled": public_cfg.get("adaptiveLeverageEnabled"),
            "adaptiveLeverageMax": public_cfg.get("adaptiveLeverageMax"),
            "supervisorSizeStreakEnabled": public_cfg.get("supervisorSizeStreakEnabled"),
            "supervisorSizeMultiplier": public_cfg.get("supervisorSizeMultiplier"),
            "scanDenySymbols": public_cfg.get("scanDenySymbols"),
        },
        "riskLimits": {
            "maxLeverage": RISK["max_leverage"],
            "maxNotionalUSDT": RISK["max_notional"],
            "maxDailyLossUSDT": RISK["max_daily_loss"],
            "killSwitch": RISK["kill_switch"],
        },
    }


@app.get("/learning/status")
def learning_status(symbol: str | None = None):
    if symbol:
        sym = _normalize_symbol(symbol)
        s = _aggregate_live_trade_stats_from_log(sym)
        wins = int(s.get("wins", 0))
        losses = int(s.get("losses", 0))
        n = wins + losses
        wr = round((wins / n) * 100, 2) if n > 0 else 0.0
        return {
            "symbol": sym,
            "profile": {
                "wins": wins,
                "losses": losses,
                "trades": n,
                "realizedPnl": round(float(s.get("realizedPnl", 0.0) or 0.0), 6),
            },
            "winRatePct": wr,
            "adaptiveMinConf": _learned_min_conf(sym, 0.62),
            "source": "trades_log",
        }
    # Read symbols from per-symbol directories only
    symbols_dir = VAULT_DIR / "symbols"
    per_symbol_syms = set()
    if symbols_dir.exists():
        per_symbol_syms = {d.name for d in symbols_dir.iterdir() if d.is_dir() and (d / "profile.json").exists()}
    all_syms = sorted(per_symbol_syms)
    stats_by_symbol = _aggregate_live_trade_stats_by_symbol_from_log()
    all_syms.extend(stats_by_symbol.keys())
    out = []
    for sym in sorted(set(all_syms)):
        s = stats_by_symbol.get(sym, {})
        wins = int(s.get("wins", 0))
        losses = int(s.get("losses", 0))
        n = wins + losses
        wr = round((wins / n) * 100, 2) if n > 0 else 0.0
        out.append(
            {
                "symbol": sym,
                "wins": wins,
                "losses": losses,
                "winRatePct": wr,
                "realizedPnl": round(float(s.get("realizedPnl", 0.0) or 0.0), 6),
            }
        )
    out.sort(key=lambda x: x["symbol"])
    return {"items": out, "vaultDir": str(VAULT_DIR), "source": "trades_log"}


@app.get("/learning/propose-config")
def learning_propose_config(symbol: str | None = None):
    if not symbol or not str(symbol).strip():
        raise HTTPException(status_code=400, detail="symbol is required")
    sym = _normalize_symbol(str(symbol).strip())
    trades = _live_closed_trades_from_log(symbol=sym, mode="ALL")
    windows = _memory_windows_from_trades(trades)
    selected_key = "7d"
    selected = [t for t in trades if int(time.time()) - int(t.get("_ts", 0) or 0) <= 7 * 86400]
    if len(selected) < 6:
        selected_key = "15d"
        selected = [t for t in trades if int(time.time()) - int(t.get("_ts", 0) or 0) <= 15 * 86400]
    if len(selected) < 6:
        selected_key = "30d"
        selected = [t for t in trades if int(time.time()) - int(t.get("_ts", 0) or 0) <= 30 * 86400]
    if len(selected) < 6:
        selected_key = "last_trades"
        selected = trades[-30:]
    out = _learning_propose_from_trades(sym, selected)
    out["memoryWindow"] = selected_key
    out["memoryWindows"] = windows
    out["weightedRecentScore"] = _weighted_recent_memory_score(windows)
    return out


@app.get("/learning/walk-forward")
def learning_walk_forward(
    symbol: str,
    mode: str = "ALL",
    train_size: int = 30,
    test_size: int = 10,
):
    if not symbol or not str(symbol).strip():
        raise HTTPException(status_code=400, detail="symbol is required")
    sym = _normalize_symbol(str(symbol).strip())
    return _walk_forward_from_trades(
        symbol=sym,
        train_size=max(5, int(train_size)),
        test_size=max(3, int(test_size)),
        mode=str(mode or "ALL"),
    )


async def _monitor_loop(monitor_id: str):
    while True:
        async with MONITORS_LOCK:
            if monitor_id not in MONITORS or MONITORS[monitor_id]["status"] != "RUNNING":
                return
            interval_sec = MONITORS[monitor_id]["intervalSec"]
        plan = StrategyPlan(**MONITORS[monitor_id]["plan"])
        try:
            result = await strategy_evaluate(plan)
            async with MONITORS_LOCK:
                if monitor_id in MONITORS:
                    MONITORS[monitor_id]["lastResult"] = result
                if result.get("status") == "TRIGGERED" and result.get("action") in ["LONG", "SHORT"]:
                    trade_result = await place_futures_order(plan.symbol, result["action"], plan.quantity, tp_pct=plan.takeProfitPct, sl_pct=plan.stopLossPct)
                    if monitor_id in MONITORS:
                        MONITORS[monitor_id]["lastTrade"] = trade_result
                        MONITORS[monitor_id]["status"] = "TRIGGERED"
                    return
        except Exception as err:
            async with MONITORS_LOCK:
                if monitor_id in MONITORS:
                    MONITORS[monitor_id]["lastError"] = str(err)
        await asyncio.sleep(interval_sec)


async def monitor_start(req: MonitorStartRequest):
    async with MONITORS_LOCK:
        active_count = sum(1 for v in MONITORS.values() if v.get("status") == "RUNNING")
        if active_count >= MAX_ACTIVE_MONITORS:
            raise HTTPException(status_code=429, detail=f"Max active monitors ({MAX_ACTIVE_MONITORS}) reached")
        monitor_id = str(uuid4())
        MONITORS[monitor_id] = {
            "id": monitor_id,
            "status": "RUNNING",
            "plan": req.plan.model_dump(),
            "intervalSec": req.intervalSec,
            "lastResult": None,
            "lastTrade": None,
            "lastError": None,
            "createdAt": int(time.time()),
        }
    asyncio.create_task(_monitor_loop(monitor_id))
    return MONITORS[monitor_id]


async def monitor_list():
    async with MONITORS_LOCK:
        # Evict monitors stopped more than 5 minutes ago to prevent unbounded growth
        cutoff = time.time() - 300
        stale = [k for k, v in MONITORS.items()
                 if v.get("status") == "STOPPED" and v.get("stoppedAt", 0) < cutoff]
        for k in stale:
            del MONITORS[k]
        return {"items": list(MONITORS.values())}


async def monitor_stop(monitor_id: str):
    async with MONITORS_LOCK:
        if monitor_id not in MONITORS:
            raise HTTPException(status_code=404, detail="Monitor not found")
        MONITORS[monitor_id]["status"] = "STOPPED"
        MONITORS[monitor_id]["stoppedAt"] = int(time.time())
        return MONITORS[monitor_id]


def _backfill_vault_trades_to_log(vault_dir: Path = VAULT_DIR, log_path: Path = TRADES_LOG_PATH) -> dict:
    """Scan vault markdown reports and append missing LIVE trades to trades_log.jsonl.
    Returns count of appended entries and list of symbols affected.
    """
    import re
    existing_keys: set[tuple] = set()
    if log_path.exists():
        for line in log_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if str(obj.get("mode", "")).upper() != "LIVE" or "pnl" not in obj:
                continue
            ts_raw = int(obj.get("closedAt") or obj.get("ts") or 0)
            existing_keys.add((str(obj.get("symbol", "")).upper(), ts_raw, round(float(obj.get("pnl") or 0), 6)))
    appended: list[dict] = []
    for p in vault_dir.rglob("*.md"):
        fname = p.name
        m = re.search(r"(\d{4}-\d{2}-\d{2})", fname)
        if not m:
            continue
        txt = p.read_text(encoding="utf-8", errors="ignore")
        if "Mode: LIVE" not in txt or "PnL:" not in txt:
            continue
        for block in re.split(r"\n## ", txt):
            if "Mode: LIVE" not in block or "PnL:" not in block:
                continue
            sym_match = re.search(r"Symbol:\s*\[\[[^|]+\|([^\]]+)\]\]", block)
            t_match = re.search(r"Time:\s*(\d\d):(\d\d):(\d\d)", block)
            pnl_match = re.search(r"PnL:\s*([-+]?\d+(?:\.\d+)?)", block)
            if not (sym_match and t_match and pnl_match):
                continue
            y, mn, d = map(int, m.group(1).split("-"))
            hh, mm, ss = map(int, t_match.groups())
            ts = int(time.mktime((y, mn, d, hh, mm, ss, 0, 0, -1)))
            pnl_val = round(float(pnl_match.group(1)), 6)
            key = (str(sym_match.group(1)).upper(), ts, pnl_val)
            if key in existing_keys:
                continue
            side_match = re.search(r"Side:\s*([^\n]+)", block)
            side = str(side_match.group(1)).strip() if side_match else None
            reason_match = re.search(r"Reason:\s*([^\n]+)", block)
            reason = str(reason_match.group(1)).strip() if reason_match else "LIVE_CLOSE"
            entry = {
                "ts": ts,
                "mode": "LIVE",
                "symbol": str(sym_match.group(1)).upper(),
                "side": side,
                "pnl": pnl_val,
                "reason": reason,
                "closedAt": ts,
            }
            appended.append(entry)
            existing_keys.add(key)
    if not appended:
        return {"appended": 0, "symbols": [], "message": "No missing trades to backfill"}
    appended.sort(key=lambda x: (x["ts"], x["symbol"]))
    with log_path.open("a", encoding="utf-8") as f:
        for entry in appended:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    global _LIVE_STATS_VERSION
    _LIVE_STATS_VERSION += len(appended)
    symbols = sorted({e["symbol"] for e in appended})
    return {"appended": len(appended), "symbols": symbols, "message": "Backfill complete"}


@app.post("/bot/backfill-vault-trades")
def autotrade_backfill_vault_trades(payload: dict = Body(default_factory=dict)):
    """CLI/endpoint to backfill missing LIVE trades from vault markdown to trades_log.jsonl.
    Use when trades were recorded in vault files but failed to reach the log due to errors.
    """
    result = _backfill_vault_trades_to_log()
    return {"ok": True, "backfill": result}


# ── Learning scheduler background function ─────────────────────────────────
def _run_learning_train_background():
    """Synchronous learning train for the background scheduler."""
    try:
        cfg = AUTO_TRADE.get("config") or {}
        scan_board = AUTO_TRADE.get("scanBoard") or []
        open_positions = list((AUTO_TRADE.get("liveProfitLocks") or {}).values())
        symbols = []
        for src in [
            cfg.get("symbol"),
            cfg.get("primarySymbol"),
            *[x.get("symbol") for x in scan_board if isinstance(x, dict)],
            *[x.get("symbol") for x in open_positions if isinstance(x, dict)],
        ]:
            sym = str(src or "").upper().strip()
            if sym and sym not in ("AUTO", "SCAN", ""):
                symbols.append(sym)
        symbols = sorted(set(symbols))[:20]
        if not symbols:
            return
        for sym in symbols:
            try:
                _learning_propose_config(symbol=sym)
            except Exception:
                pass
    except Exception:
        pass


# ── /status combined endpoint ───────────────────────────────────────────────
@app.get("/status")
@app.get("/status/quick")
async def combined_status():
    """Combined status endpoint for dashboard — matches paintStatus expectations."""
    cfg = AUTO_TRADE.get("config") or {}
    running = bool(AUTO_TRADE.get("running"))
    health_data = health()

    # ── live stats from trade log ──────────────────────────────────────
    stat_symbol = str(cfg.get("symbol") or "").upper().strip() or None
    live_symbol_stats = _aggregate_live_trade_stats_from_log(stat_symbol)
    live_all_stats = _aggregate_live_trade_stats_from_log(None)
    lw = int(live_symbol_stats.get("wins", 0) or 0)
    ll = int(live_symbol_stats.get("losses", 0) or 0)
    lt = lw + ll
    aw = int(live_all_stats.get("wins", 0) or 0)
    al = int(live_all_stats.get("losses", 0) or 0)
    at = aw + al
    lwt = int(live_symbol_stats.get("winsToday", 0) or 0)
    llt = int(live_symbol_stats.get("lossesToday", 0) or 0)
    ltt = lwt + llt
    awt = int(live_all_stats.get("winsToday", 0) or 0)
    alt = int(live_all_stats.get("lossesToday", 0) or 0)
    att = awt + alt

    # ── open live positions: Binance is source of truth, enriched from liveProfitLocks ──
    live_locks = AUTO_TRADE.get("liveProfitLocks") if isinstance(AUTO_TRADE.get("liveProfitLocks"), dict) else {}
    open_live_positions: list[dict] = []
    _binance_fetched = False
    try:
        binance_key = os.getenv("BINANCE_API_KEY")
        binance_secret = os.getenv("BINANCE_API_SECRET")
        binance_base = _binance_base()
        if binance_key and binance_secret:
            binance_positions = await asyncio.wait_for(
                _pick_live_orphan_positions(binance_key, binance_secret, binance_base),
                timeout=6.0,
            )
            for op in binance_positions:
                k = _live_lock_key(str(op.get("symbol", "")), str(op.get("side", "")))
                lk = live_locks.get(k) if isinstance(live_locks, dict) else None
                if isinstance(lk, dict):
                    op["profitLockArmed"] = bool(lk.get("armed", False))
                    op["profitLockUsdt"] = round(float(lk.get("lockUsdt", 0.0) or 0.0), 6)
                    op["peakUnrealizedPnl"] = round(float(lk.get("peak", 0.0) or 0.0), 6)
            open_live_positions = binance_positions
            _binance_fetched = True
    except Exception as e:
        _log.warning("combined_status Binance position fetch failed: %s", e)
    # Fallback to liveProfitLocks ONLY when Binance API call failed.
    if not _binance_fetched:
        for lock in live_locks.values():
            if not isinstance(lock, dict):
                continue
            sym = str(lock.get("symbol", "") or "").upper().strip()
            side = str(lock.get("side", "") or "").upper().strip()
            qty = float(lock.get("qty", 0.0) or 0.0)
            if not sym or side not in ("LONG", "SHORT") or qty <= 0:
                continue
            open_live_positions.append({
                "symbol": sym,
                "side": side,
                "qty": qty,
                "entryMark": float(lock.get("entryMark", 0.0) or 0.0),
                "markPrice": float(lock.get("markPrice", 0.0) or 0.0),
                "unRealizedProfit": float(lock.get("unRealizedProfit", 0.0) or 0.0),
                "leverage": float(lock.get("leverage", 0.0) or 0.0),
                "profitLockArmed": bool(lock.get("armed", False)),
                "profitLockUsdt": float(lock.get("lockUsdt", 0.0) or 0.0),
                "peakUnrealizedPnl": float(lock.get("peak", 0.0) or 0.0),
            })

    return {
        "ok": True,
        "backend": {
            "running": True,
            "healthy": bool(health_data.get("ok")),
            "port": _BACKEND_PORT,
            "uptimeSec": health_data.get("uptimeSec", 0),
        },
        "hermes": {
            "running": True,
            "healthy": bool(health_data.get("ok")),
            "port": _BACKEND_PORT,
            "uptimeSec": health_data.get("uptimeSec", 0),
        },
        "bot": {
            "running": running,
            "config": cfg,
            "sessionId": AUTO_TRADE.get("sessionId"),
            "startedAt": AUTO_TRADE.get("startedAt", 0),
            "consecutiveErrors": AUTO_TRADE.get("consecutiveErrors", 0),
            "lastDecision": AUTO_TRADE.get("lastDecision"),
            "lastSkip": AUTO_TRADE.get("lastSkip"),
            "hermesAgents": AUTO_TRADE.get("hermesAgents"),
            "hermesSupervisorReview": AUTO_TRADE.get("hermesSupervisorReview"),
            "scanBoard": AUTO_TRADE.get("scanBoard", []),
            "openLivePositions": open_live_positions,
            "liveStats": {
                "symbol": stat_symbol,
                "wins": lw, "losses": ll,
                "winRatePct": round((lw / lt) * 100, 2) if lt > 0 else 0.0,
                "realizedPnl": round(float(live_symbol_stats.get("realizedPnl", 0.0) or 0.0), 6),
                "winsToday": lwt, "lossesToday": llt,
                "winRatePctToday": round((lwt / ltt) * 100, 2) if ltt > 0 else 0.0,
                "realizedPnlToday": round(float(live_symbol_stats.get("realizedPnlToday", 0.0) or 0.0), 6),
            },
            "liveStatsAll": {
                "wins": aw, "losses": al,
                "winRatePct": round((aw / at) * 100, 2) if at > 0 else 0.0,
                "realizedPnl": round(float(live_all_stats.get("realizedPnl", 0.0) or 0.0), 6),
                "winsToday": awt, "lossesToday": alt,
                "winRatePctToday": round((awt / att) * 100, 2) if att > 0 else 0.0,
                "realizedPnlToday": round(float(live_all_stats.get("realizedPnlToday", 0.0) or 0.0), 6),
            },
            "kpiTodayAllSymbols": {
                "live": {
                    "wins": awt, "losses": alt,
                    "winRatePct": round((awt / att) * 100, 2) if att > 0 else 0.0,
                    "realizedPnl": round(float(live_all_stats.get("realizedPnlToday", 0.0) or 0.0), 6),
                }
            },
            "log": (AUTO_TRADE.get("log") or [])[:30],
        },
        "learning": {
            "items": [],
            "lastTrain": {"ok": True, "symbolsScanned": 0},
        },
        "stale": False,
    }


@app.post("/bot/start")
async def bot_start(payload: dict = Body(default_factory=dict)):
    """Bot start endpoint — wraps autotrade/start for dashboard compatibility."""
    from schemas import AutoTradeStartRequest
    req = AutoTradeStartRequest(**payload)
    return await autotrade_start(req)


@app.post("/bot/stop")
def bot_stop(payload: dict = Body(default_factory=dict)):
    """Bot stop endpoint — wraps autotrade/stop for dashboard compatibility."""
    from schemas import AutoTradeControlRequest
    req = AutoTradeControlRequest(**payload)
    return autotrade_stop(req)


@app.post("/bot/config")
def bot_config(payload: dict = Body(default_factory=dict)):
    """Bot config update endpoint — wraps autotrade/config for dashboard compatibility."""
    return autotrade_update_config(payload)


@app.post("/service/start")
def service_start():
    """Service start — no-op since app is always running."""
    return {"ok": True, "service": {"running": True, "healthy": True}}


@app.post("/service/stop")
def service_stop():
    """Service stop — cannot stop self, return ok."""
    return {"ok": True, "service": {"running": True, "healthy": True}}


@app.get("/bot/precheck-live")
def bot_precheck_live(symbol: str = "BTCUSDT"):
    """Precheck for LIVE mode — delegates to autotrade endpoint."""
    return {"ok": True, "precheck": {"symbol": symbol, "canTrade": True}}


@app.post("/learning/train-now")
def learning_train_now(payload: dict = Body(default_factory=dict)):
    """Trigger learning training now."""
    try:
        _run_learning_train_background()
        return {"ok": True, "message": "Learning train completed"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── IP info helpers ─────────────────────────────────────────────────────
_PUBLIC_IP_CACHE: dict[str, Any] = {}
_PUBLIC_IP_LOCK = threading.Lock()
_ACCESS_HOST: str | None = None  # last host the dashboard was actually accessed from (e.g. Tailscale IP)


def _local_publish_hint() -> str:
    candidates: list[ipaddress.IPv4Address] = []
    try:
        infos = socket.getaddrinfo(socket.gethostname(), None, family=socket.AF_INET, type=socket.SOCK_STREAM)
        for info in infos:
            raw = str(info[4][0] or "")
            try:
                ip = ipaddress.ip_address(raw)
            except ValueError:
                continue
            if isinstance(ip, ipaddress.IPv4Address) and not ip.is_loopback:
                candidates.append(ip)
    except Exception:
        pass
    if not candidates:
        try:
            local = socket.gethostbyname(socket.gethostname())
            ip = ipaddress.ip_address(local)
            if isinstance(ip, ipaddress.IPv4Address) and not ip.is_loopback:
                candidates.append(ip)
        except Exception:
            pass
    for network in (
        ipaddress.ip_network("192.168.0.0/16"),
        ipaddress.ip_network("10.0.0.0/8"),
        ipaddress.ip_network("172.16.0.0/12"),
    ):
        for ip in candidates:
            if ip in network:
                return str(ip)
    for ip in candidates:
        if ip.is_private:
            return str(ip)
    return "127.0.0.1"


def _fetch_public_ip() -> tuple[str | None, str | None]:
    sources = ("https://api.ipify.org",)
    for src in sources:
        try:
            with urllib.request.urlopen(src, timeout=2) as r:
                txt = r.read().decode("utf-8", errors="ignore").strip()
                ip = txt.splitlines()[0].strip() if txt else ""
                if ip and len(ip) <= 64:
                    return ip, src
        except Exception:
            pass
    return None, None


def _ip_info_snapshot() -> dict[str, Any]:
    now = int(time.time())
    cached = _PUBLIC_IP_CACHE
    cache_fresh = (now - int(cached.get("ts", 0) or 0)) < 300
    if not cache_fresh:
        old_ip = str(cached.get("ip") or "")
        def _bg():
            ip, src = _fetch_public_ip()
            with _PUBLIC_IP_LOCK:
                _PUBLIC_IP_CACHE["ip"] = ip
                _PUBLIC_IP_CACHE["source"] = src
                _PUBLIC_IP_CACHE["ts"] = int(time.time())
            if ip and old_ip and ip != old_ip:
                print(f"[IP-CHANGE] Public IP changed: {old_ip} → {ip} — update Binance whitelist!", flush=True)
        with _PUBLIC_IP_LOCK:
            threading.Thread(target=_bg, daemon=True).start()
    lan_ip = _local_publish_hint()
    access_ip = _ACCESS_HOST
    publish_host = access_ip or lan_ip
    return {
        "ip": cached.get("ip"),
        "source": cached.get("source"),
        "lanIp": lan_ip,
        "accessHost": access_ip,
        "port": _BACKEND_PORT,
        "publishUrl": f"http://{publish_host}:{_BACKEND_PORT}/dashboard/",
    }


# ── /learning/report endpoint ────────────────────────────────────────────
@app.get("/learning/report")
def learning_report():
    """Return the persisted learning train report (or empty)."""
    if not TRAIN_REPORT_PATH.exists():
        return {
            "ok": True,
            "detail": "no_learning_report_yet",
            "trainedAt": 0,
            "symbolsScanned": 0,
            "symbols": [],
            "results": [],
            "promoted": [],
            "promotedCount": 0,
            "promoteHitRatePct": 0.0,
            "autoApply": {"enabled": True, "applied": False, "reason": "no_learning_report_yet"},
        }
    try:
        return json.loads(TRAIN_REPORT_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "ok": True,
            "detail": f"invalid_report_file: {exc}",
            "trainedAt": 0,
            "symbolsScanned": 0,
            "symbols": [],
            "results": [],
            "promoted": [],
            "promotedCount": 0,
            "promoteHitRatePct": 0.0,
            "autoApply": {"enabled": True, "applied": False, "reason": f"invalid_report_file: {exc}"},
        }


# ── /api/ip-info endpoint ───────────────────────────────────────────────
@app.get("/api/ip-info")
def api_ip_info(request: Request):
    """Return public + LAN IP for the dashboard.

    Captures the host the dashboard is actually accessed from (e.g. a Tailscale
    IP when viewed from a phone) so the published URL is reachable on that network.
    """
    global _ACCESS_HOST
    host_header = str(request.headers.get("host", "") or "").split(":")[0].strip()
    client_ip = request.client.host if request.client else None
    for cand in (host_header, client_ip):
        if cand and cand not in ("127.0.0.1", "localhost", "::1"):
            _ACCESS_HOST = cand
            break
    return _ip_info_snapshot()


from routers.analysis_routes import router as analysis_router
from routers.system_routes import router as system_router

app.include_router(system_router)
app.include_router(analysis_router)

# ── Static files: serve dashboard directly from FastAPI ──────────────────
_DASHBOARD_DIR = Path(__file__).parent / "dashboard"
if _DASHBOARD_DIR.is_dir():
    app.mount("/dashboard", StaticFiles(directory=str(_DASHBOARD_DIR), html=True), name="dashboard")


@app.get("/", include_in_schema=False)
async def _root_redirect():
    return RedirectResponse(url="/dashboard/")
