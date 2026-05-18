from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Literal
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from uuid import uuid4
from pathlib import Path

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

import os
import sys
import time
import hmac
import hashlib
import json
import re
import math
import asyncio
import subprocess
from decimal import Decimal, ROUND_DOWN
import httpx
ENV_PATH = Path(__file__).with_name(".env")
SNAPSHOT_PATH = Path(__file__).with_name("autotrade_snapshot.json")
VAULT_DIR = Path(__file__).with_name("obsidian_vault")
LEARN_PATH = VAULT_DIR / "learning_profiles.json"
TRADES_LOG_PATH = VAULT_DIR / "trades_log.jsonl"
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
        if os.getenv("BINANCE_TESTNET", "true").lower() == "true"
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
_EXCHANGE_FILTERS_CACHE: dict[str, tuple[float, dict]] = {}
_EXCHANGE_FILTERS_CACHE_TTL = 60  # seconds
_UMFUTURES_CLASS = None


def _ensure_vault():
    try:
        VAULT_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def _load_learning_profiles() -> dict:
    _ensure_vault()
    try:
        if LEARN_PATH.exists():
            data = json.loads(LEARN_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def _save_learning_profiles(profiles: dict):
    _ensure_vault()
    try:
        LEARN_PATH.write_text(json.dumps(profiles, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _append_trade_log(entry: dict):
    _ensure_vault()
    try:
        with TRADES_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _update_symbol_note(symbol: str, profile: dict, last_trade: dict | None = None):
    _ensure_vault()
    p = VAULT_DIR / f"{symbol}.md"
    wins = int(profile.get("wins", 0))
    losses = int(profile.get("losses", 0))
    total = wins + losses
    wr = round((wins / total) * 100, 2) if total > 0 else 0.0
    pnl = round(float(profile.get("realizedPnl", 0.0)), 6)
    trades = int(profile.get("trades", 0))
    obs = int(profile.get("observations", 0))
    picks = int(profile.get("pickedCount", 0))
    pick_rate = round((picks / obs) * 100, 2) if obs > 0 else 0.0
    avg_pnl = round((pnl / trades), 6) if trades > 0 else 0.0
    last_sig = str(profile.get("lastSignal", "WAIT"))
    last_conf = profile.get("lastConfidence", 0.0)
    last_score = profile.get("lastScanScore", 0.0)
    lines = [
        f"# {symbol}",
        "",
        "## Performance",
        f"- Wins: {wins}",
        f"- Losses: {losses}",
        f"- Trades: {trades}",
        f"- WinRate: {wr}%",
        f"- RealizedPnL: {pnl}",
        f"- AvgPnL/Trade: {avg_pnl}",
        "",
        "## Scan Behavior",
        f"- Observations: {obs}",
        f"- PickedCount: {picks}",
        f"- PickRate: {pick_rate}%",
        f"- LastSignal: {last_sig}",
        f"- LastConfidence: {last_conf}",
        f"- LastScanScore: {last_score}",
        f"- UpdatedAt: {int(time.time())}",
        "",
    ]
    if last_trade:
        lines += [
            "## Last Trade",
            f"- Side: {last_trade.get('side')}",
            f"- Entry: {last_trade.get('entry')}",
            f"- Exit: {last_trade.get('exit')}",
            f"- PnL: {last_trade.get('pnl')}",
            f"- Reason: {last_trade.get('reason')}",
            f"- ClosedAt: {last_trade.get('closedAt')}",
            "",
        ]
    try:
        p.write_text("\n".join(lines), encoding="utf-8")
    except Exception:
        pass


def _record_learning_trade(symbol: str, trade: dict, mode: str):
    sym = str(symbol or "").upper()
    if not sym:
        return
    profiles = _load_learning_profiles()
    pr = profiles.get(sym, {"wins": 0, "losses": 0, "realizedPnl": 0.0, "trades": 0})
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
    pr["updatedAt"] = int(time.time())
    profiles[sym] = pr
    _save_learning_profiles(profiles)
    _append_trade_log({"ts": int(time.time()), "mode": mode, **trade, "symbol": sym})
    _update_symbol_note(sym, pr, trade)


def _record_symbol_observation(symbol: str, intel: dict, chosen: bool, score: float):
    sym = str(symbol or "").upper()
    if not sym:
        return
    profiles = _load_learning_profiles()
    pr = profiles.get(sym, {"wins": 0, "losses": 0, "realizedPnl": 0.0, "trades": 0})
    pr["observations"] = int(pr.get("observations", 0)) + 1
    pr["lastSignal"] = str((intel or {}).get("signal", "WAIT"))
    pr["lastConfidence"] = round(float((intel or {}).get("confidence", 0.0) or 0.0), 4)
    pr["lastScanScore"] = round(float(score or 0.0), 6)
    ex = (intel or {}).get("execution") if isinstance((intel or {}).get("execution"), dict) else {}
    pr["lastSpreadBps"] = round(float(ex.get("spreadBps", 0.0) or 0.0), 4)
    pr["lastMomentumPct"] = round(float(ex.get("momentumPct", 0.0) or 0.0), 6)
    if chosen:
        pr["pickedCount"] = int(pr.get("pickedCount", 0)) + 1
    pr["updatedAt"] = int(time.time())
    profiles[sym] = pr
    _save_learning_profiles(profiles)
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
    _update_symbol_note(sym, pr, None)


def _learned_min_conf(symbol: str, base_min_conf: float):
    profiles = _load_learning_profiles()
    pr = profiles.get(symbol) if isinstance(profiles, dict) else None
    if not isinstance(pr, dict):
        return base_min_conf
    n = int(pr.get("wins", 0)) + int(pr.get("losses", 0))
    if n < 6:
        return base_min_conf
    wr = (int(pr.get("wins", 0)) / max(n, 1)) * 100.0
    # Conservative adaptive rule: good symbol => slightly easier, weak symbol => stricter.
    if wr >= 60:
        return max(0.45, base_min_conf - 0.05)
    if wr <= 45:
        return min(0.85, base_min_conf + 0.05)
    return base_min_conf


def _symbol_quality_score(symbol: str) -> float:
    profiles = _load_learning_profiles()
    pr = profiles.get(symbol) if isinstance(profiles, dict) else None
    if not isinstance(pr, dict):
        return 0.0
    wins = int(pr.get("wins", 0))
    losses = int(pr.get("losses", 0))
    n = wins + losses
    if n < 4:
        return 0.0
    wr = wins / max(n, 1)
    pnl = float(pr.get("realizedPnl", 0.0) or 0.0)
    # bounded bonus/penalty from historical behaviour of each symbol
    wr_bonus = (wr - 0.5) * 0.12
    pnl_bonus = max(-0.05, min(0.05, pnl / 200.0))
    return wr_bonus + pnl_bonus


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
    return score


async def _scan_market_candidates(limit_liquid: int = 30) -> list[str]:
    res = await _data_get("/fapi/v1/ticker/24hr")
    if res.status_code >= 400:
        raise HTTPException(status_code=res.status_code, detail=f"ticker/24hr failed: {res.text}")
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
        # rank by liquidity and movement to find clearer opportunities
        rank = qv * (1.0 + min(2.0, chg / 100.0))
        out.append((rank, sym))
    out.sort(key=lambda x: x[0], reverse=True)
    return [s for _, s in out[: max(5, int(limit_liquid))]]


def _parse_symbol_whitelist(raw_list: list[str] | None) -> set[str]:
    out: set[str] = set()
    for v in raw_list or []:
        try:
            sym = _normalize_symbol(str(v).strip())
            out.add(sym)
        except Exception:
            continue
    return out


async def _pick_best_symbol_from_scan(cfg: dict) -> tuple[str | None, dict | None, list[dict]]:
    candidates = await _scan_market_candidates(int(cfg.get("scanTopLiquid", 30)))
    wl = _parse_symbol_whitelist(cfg.get("whitelistSymbols"))
    if wl:
        candidates = [s for s in candidates if s in wl]
    analyze_top = max(3, int(cfg.get("scanAnalyzeTop", 8)))
    candidates = candidates[:analyze_top]
    if not candidates:
        return None, None, []
    reqs = [IntelAnalyzeRequest(symbol=s) for s in candidates]
    results = await asyncio.gather(*[intel_analyze(r) for r in reqs], return_exceptions=True)
    best_sym = None
    best_intel = None
    best_score = -999.0
    board = []
    for sym, out in zip(candidates, results):
        if isinstance(out, Exception) or not isinstance(out, dict):
            continue
        sig = str(out.get("signal", "WAIT")).upper()
        conf = float(out.get("confidence", 0.0) or 0.0)
        ex = out.get("execution") if isinstance(out.get("execution"), dict) else {}
        spread_penalty = min(0.2, max(0.0, float(ex.get("spreadBps", 0.0) or 0.0) / 200.0))
        momentum = abs(float(ex.get("momentumPct", 0.0) or 0.0))
        qual = _symbol_quality_score(sym)
        score = _intel_score(sym, out)
        board.append({
            "symbol": sym,
            "signal": sig,
            "confidence": round(conf, 4),
            "score": round(score, 6),
            "momentumPct": round(momentum, 4),
            "spreadBps": round(float(ex.get("spreadBps", 0.0) or 0.0), 4),
        })
        _record_symbol_observation(sym, out, False, score)
        if sig not in ("LONG", "SHORT"):
            continue
        if score > best_score:
            best_score = score
            best_sym = sym
            best_intel = out
    if best_sym and best_intel:
        _record_symbol_observation(best_sym, best_intel, True, best_score)
    board.sort(key=lambda x: x["score"], reverse=True)
    return best_sym, best_intel, board[:10]


async def _cached_klines(symbol: str, interval: str, limit: int) -> list:
    key = (symbol, interval, limit)
    now = time.time()
    if key in _KLINES_CACHE:
        fetched_at, data = _KLINES_CACHE[key]
        if now - fetched_at < _KLINES_CACHE_TTL:
            return data
    res = await _data_get(f"/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}")
    if res.status_code >= 400:
        raise HTTPException(status_code=res.status_code, detail=f"klines {interval} failed: {res.text}")
    data = res.json()
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


async def _data_get(path: str) -> httpx.Response:
    """
    Fast GET for public market data on fapi.binance.com mainnet.
    Uses dedicated connection pool with automatic fallback to fresh client.
    path must start with /fapi/...
    """
    full_url = f"https://fapi.binance.com{path}"
    # Try dedicated pool first
    if _BINANCE_DATA_HTTP is not None:
        try:
            return await _BINANCE_DATA_HTTP.get(path)
        except (httpx.RemoteProtocolError, httpx.LocalProtocolError,
                httpx.ReadError, httpx.ConnectError, httpx.PoolTimeout,
                httpx.ConnectTimeout):
            pass  # fall through to fresh client
    # Fallback: fresh client — always works, slightly slower
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(25.0, connect=10.0),
        headers={"Accept-Encoding": "identity"},
    ) as c:
        return await c.get(full_url)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _BINANCE_HTTP, _BINANCE_DATA_HTTP
    timeout = httpx.Timeout(30.0, connect=10.0)
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10, keepalive_expiry=30.0)
    _BINANCE_HTTP = httpx.AsyncClient(timeout=timeout, limits=limits)

    # Dedicated fast client for public market data (mainnet fapi.binance.com)
    # Accept-Encoding: identity prevents gzip/br decompression which can OOM on large responses
    data_limits = httpx.Limits(max_keepalive_connections=5, max_connections=10, keepalive_expiry=15.0)
    _BINANCE_DATA_HTTP = httpx.AsyncClient(
        timeout=httpx.Timeout(20.0, connect=8.0),
        limits=data_limits,
        base_url="https://fapi.binance.com",
        headers={"Accept-Encoding": "identity"},
    )
    _load_autotrade_snapshot()

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
            # Restore config and restart loop
            global _AUTOTRADE_TASK
            session_id = str(uuid4())
            AUTO_TRADE["running"] = True
            AUTO_TRADE["sessionId"] = session_id
            AUTO_TRADE["startedAt"] = int(time.time())
            AUTO_TRADE["config"] = cfg
            AUTO_TRADE["consecutiveErrors"] = 0
            AUTO_TRADE["lastSkip"] = None
            AUTO_TRADE["liveGuardian"] = snap_data.get("liveGuardian")
            AUTO_TRADE["scanBoard"] = snap_data.get("scanBoard") if isinstance(snap_data.get("scanBoard"), list) else []
            AUTO_TRADE["lastTradeAt"] = int(snap_data.get("lastTradeAt", 0))
            AUTO_TRADE["trades"] = [t for t in snap_data.get("trades", [])
                                    if time.time() - t < 3600]
            resume_msg = (
                f"AUTO-RESUMED after restart: {cfg.get('symbol')} "
                f"{cfg.get('executionMode','PAPER')} x{cfg.get('leverage',1)}"
            )
            _autotrade_log(resume_msg)
            _AUTOTRADE_TASK = asyncio.create_task(_autotrade_loop())
        except Exception as e:
            _autotrade_log(f"Auto-resume failed: {_format_loop_error(e)}")

    asyncio.create_task(_maybe_resume())

    # Warm up connection pool with a lightweight request
    async def _warmup():
        try:
            await _data_get("/fapi/v1/time")
        except Exception:
            pass
    asyncio.create_task(_warmup())
    try:
        yield
    finally:
        _persist_autotrade_snapshot()
        await _BINANCE_HTTP.aclose()
        await _BINANCE_DATA_HTTP.aclose()
        _BINANCE_HTTP = None
        _BINANCE_DATA_HTTP = None


app = FastAPI(title="Binance AI Copilot API", version="0.4.0", lifespan=_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONITORS: dict[str, dict] = {}
DAILY_REALIZED_PNL = 0.0
_AUTOTRADE_TASK: asyncio.Task | None = None  # track running task to prevent duplicates
AUTO_TRADE = {
    "running": False,
    "sessionId": None,
    "startedAt": 0,
    "config": None,
    "lastDecision": None,
    "lastSkip": None,
    "lastTradeAt": 0,
    "trades": [],  # unix timestamps
    "log": [],
    "consecutiveErrors": 0,
    "liveGuardian": None,
    "scanBoard": [],
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


RISK = {
    "kill_switch": os.getenv("KILL_SWITCH", "false").lower() == "true",
    "max_notional": float(os.getenv("MAX_NOTIONAL_USDT", "200")),
    "max_leverage": float(os.getenv("MAX_LEVERAGE", "5")),
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
BINANCE_RECV_WINDOW_MS = int(os.getenv("BINANCE_RECV_WINDOW_MS", "60000"))
AUTOTRADE_TAKER_FEE_BPS_PER_SIDE = float(os.getenv("AUTOTRADE_TAKER_FEE_BPS_PER_SIDE", "4.0"))
AUTOTRADE_MIN_NET_PROFIT_USDT = float(os.getenv("AUTOTRADE_MIN_NET_PROFIT_USDT", "0.05"))
AUTOTRADE_EXTRA_COST_BPS = float(os.getenv("AUTOTRADE_EXTRA_COST_BPS", "2.0"))


def _binance_base():
    return "https://testnet.binancefuture.com" if os.getenv("BINANCE_TESTNET", "true").lower() == "true" else "https://fapi.binance.com"


def _estimate_trade_edge_usdt(usdt_amount: float, tp_pct: float, max_slippage_bps: float) -> tuple[float, float, float]:
    # Cost model = taker fee entry+exit + micro cost buffer + half of slippage budget.
    gross_profit = float(usdt_amount) * (float(tp_pct) / 100.0)
    cost_bps = (2.0 * AUTOTRADE_TAKER_FEE_BPS_PER_SIDE) + AUTOTRADE_EXTRA_COST_BPS + max(0.0, float(max_slippage_bps) * 0.5)
    est_cost = float(usdt_amount) * (cost_bps / 10000.0)
    net_profit = gross_profit - est_cost
    return gross_profit, est_cost, net_profit


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
        "version": "0.4.1",
    }


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
    cmd = [str(py), "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", port]
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
        "binanceTestnet": os.getenv("BINANCE_TESTNET", "true"),
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
    RISK["max_leverage"] = cfg.maxLeverage
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


async def intel_analyze(req: IntelAnalyzeRequest):
    symbol = _normalize_symbol(req.symbol)
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

    mm, pk, depth_out, execution = await asyncio.gather(
        _market_momentum(symbol, _rows=rows_1m[:60]),
        _precision_signal_pack(symbol, limit=150, _rows_1m=rows_1m),
        _depth_orderflow(),
        _microstructure(),
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
    if mm["momentumPct"] > 0.12 and mm["volumeRatio"] > 1.05 and final_signal != "SHORT":
        final_signal = "LONG"
        confidence = min(0.92, confidence + 0.12)
    elif mm["momentumPct"] < -0.12 and mm["volumeRatio"] > 1.05 and final_signal != "LONG":
        final_signal = "SHORT"
        confidence = min(0.92, confidence + 0.12)

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
    # Max possible: ~18 long or short. Threshold: 7 for strong, 5 for soft.
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
    if pk["breakoutUp"] and pk["volumeRatio"] > 1.2:
        long_score += 2
    if pk["breakoutDown"] and pk["volumeRatio"] > 1.2:
        short_score += 2

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
    STRONG_THRESHOLD = 7
    SOFT_THRESHOLD   = 5

    if long_score >= STRONG_THRESHOLD and long_score >= short_score + 2:
        final_signal = "LONG"
        confidence = max(confidence, min(0.95, 0.60 + 0.025 * long_score))
    elif short_score >= STRONG_THRESHOLD and short_score >= long_score + 2:
        final_signal = "SHORT"
        confidence = max(confidence, min(0.95, 0.60 + 0.025 * short_score))
    elif (
        pre_confluence_signal == "LONG"
        and long_score >= SOFT_THRESHOLD
        and long_score >= short_score + 1
    ):
        final_signal = "LONG"
        confidence = max(pre_confluence_confidence, min(0.88, 0.58 + 0.03 * long_score))
        notes.append("Confluence soft-confirm LONG")
    elif (
        pre_confluence_signal == "SHORT"
        and short_score >= SOFT_THRESHOLD
        and short_score >= long_score + 1
    ):
        final_signal = "SHORT"
        confidence = max(pre_confluence_confidence, min(0.88, 0.58 + 0.03 * short_score))
        notes.append("Confluence soft-confirm SHORT")
    elif (
        pre_confluence_signal in ("LONG", "SHORT")
        and max(long_score, short_score) >= 4
        and abs(long_score - short_score) >= 1
    ):
        # Lite confirm for alt coins with lower liquidity
        if long_score > short_score:
            final_signal = "LONG"
            confidence = max(0.62, min(0.80, pre_confluence_confidence + 0.015 * (long_score - short_score)))
            notes.append("Confluence lite LONG")
        else:
            final_signal = "SHORT"
            confidence = max(0.62, min(0.80, pre_confluence_confidence + 0.015 * (short_score - long_score)))
            notes.append("Confluence lite SHORT")
    else:
        final_signal = "WAIT"
        confidence = min(confidence, 0.50)
    notes.append(f"Score L/S={long_score}/{short_score} | MACD={'↑' if pk['macdBullish'] else '↓'} | BB%B={pk['bbPctB']:.2f} | VWAP={'↑' if pk['priceAboveVwap'] else '↓'}")

    return {
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
    }
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


async def _market_momentum(symbol: str, interval: str = "1m", limit: int = 60, _rows: list | None = None):
    symbol = _normalize_symbol(symbol)
    rows = _rows if _rows is not None else await _cached_klines(symbol, interval, limit)
    if not isinstance(rows, list) or len(rows) < 25:
        return {"momentumPct": 0.0, "volumeRatio": 1.0, "divergence": "NONE"}

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

    return {
        "momentumPct": momentum_pct,
        "volumeRatio": volume_ratio,
        "divergence": divergence,
    }


async def _precision_signal_pack(symbol: str, limit: int = 200, _rows_1m: list | None = None):
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
    client = _BINANCE_HTTP
    if client is not None:
        try:
            if method == "POST":
                res = await client.post(url, headers=headers)
            else:
                res = await client.get(url, headers=headers)
        except (httpx.RemoteProtocolError, httpx.LocalProtocolError, httpx.ReadError, httpx.ConnectError, httpx.PoolTimeout, httpx.ConnectTimeout):
            # fallback to fresh client on stale/closed pooled connection
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
    if res.status_code >= 400:
        raise HTTPException(status_code=res.status_code, detail=res.text)
    return res.json()



async def _exchange_filters(symbol: str):
    now_ts = time.time()
    cached = _EXCHANGE_FILTERS_CACHE.get(symbol)
    if cached and now_ts - cached[0] < _EXCHANGE_FILTERS_CACHE_TTL:
        return cached[1]

    res = await _data_get(f"/fapi/v1/exchangeInfo?symbol={symbol}")
    if res.status_code >= 400:
        raise HTTPException(status_code=res.status_code, detail=res.text)
    data = res.json()
    symbols = data.get("symbols", [])
    if not symbols:
        raise HTTPException(status_code=400, detail=f"Symbol not found on futures: {symbol}")
    s = symbols[0]
    if s.get("status") != "TRADING":
        raise HTTPException(status_code=400, detail=f"Symbol not tradable: {symbol}")
    filters = {f["filterType"]: f for f in s.get("filters", [])}
    payload = {
        "stepSize": float(filters.get("LOT_SIZE", {}).get("stepSize", "0")),
        "minQty": float(filters.get("LOT_SIZE", {}).get("minQty", "0")),
        "tickSize": float(filters.get("PRICE_FILTER", {}).get("tickSize", "0")),
        "minNotional": float(filters.get("MIN_NOTIONAL", {}).get("notional", filters.get("NOTIONAL", {}).get("minNotional", "0"))),
        "maxLeverage": int(os.getenv("MAX_LEVERAGE_DEFAULT", "20")),
        "maxQty": float(filters.get("LOT_SIZE", {}).get("maxQty", "0") or 0),
    }
    _EXCHANGE_FILTERS_CACHE[symbol] = (now_ts, payload)
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


async def _current_position_amount(symbol: str, key: str | None, secret: str | None, base: str):
    if not key or not secret:
        return 0.0
    client = _get_um_client(key, secret, base)
    if client:
        pos = client.get_position_risk(symbol=symbol)
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
        pos = client.get_position_risk(symbol=symbol)
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


async def _open_positions_count(key: str | None, secret: str | None, base: str) -> int:
    if not key or not secret:
        return 0
    client = _get_um_client(key, secret, base)
    if client:
        pos = client.get_position_risk()
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
        return f"HTTP {err.response.status_code} {err.request.url!s} {body}".strip()
    if isinstance(err, httpx.RequestError):
        u = getattr(err.request, "url", None)
        return f"RequestError {u!s}: {err!s}".strip() or type(err).__name__
    s = str(err).strip()
    if s:
        return s
    return f"{type(err).__name__} (no message)"


def _autotrade_log(msg: str):
    AUTO_TRADE["log"] = ([{"ts": int(time.time()), "msg": msg}] + AUTO_TRADE["log"])[:100]


def _autotrade_skip(code: str, msg: str):
    AUTO_TRADE["lastSkip"] = {"ts": int(time.time()), "code": code, "msg": msg}
    _autotrade_log(msg)
    # Normal skips are not API/network failures; clear streak so UI "consecutive errors" stays honest.
    if code != "exception":
        AUTO_TRADE["consecutiveErrors"] = 0


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


def _trail_winner_levels(side: str, mark: float, old_sl: float, old_tp: float, trail_pct: float) -> tuple[float, float]:
    t = max(0.05, float(trail_pct))
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
    return round(float(base_usdt) * mult, 2)


async def _live_guardian_maybe_close(cfg: dict):
    g = AUTO_TRADE.get("liveGuardian")
    if not g or not g.get("active"):
        return False
    symbol = g.get("symbol")
    side = g.get("side")
    if not symbol or side not in ("LONG", "SHORT"):
        return False
    key = os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_API_SECRET")
    base = _binance_base()
    amt = await _current_position_amount(symbol, key, secret, base)
    if abs(float(amt)) <= 0:
        g["active"] = False
        g["closedBy"] = "NO_POSITION"
        return False
    mark = await fetch_mark_price(symbol)
    tp = float(g.get("tp", 0))
    sl = float(g.get("sl", 0))
    hit_tp = (side == "LONG" and mark >= tp) or (side == "SHORT" and mark <= tp)
    hit_sl = (side == "LONG" and mark <= sl) or (side == "SHORT" and mark >= sl)
    if hit_tp and _should_hold_winner(side, AUTO_TRADE.get("lastDecision"), cfg):
        new_sl, new_tp = _trail_winner_levels(
            side,
            mark,
            sl,
            tp,
            float(cfg.get("holdTrailPct", 0.35)),
        )
        g["sl"] = new_sl
        g["tp"] = new_tp
        g["lastTrailAt"] = int(time.time())
        _autotrade_log(f"LIVE hold winner: trail TP/SL -> TP={new_tp:.6f} SL={new_sl:.6f}")
        return False
    if not (hit_tp or hit_sl):
        return False
    reason = "LOCAL_TP_HIT" if hit_tp else "LOCAL_SL_HIT"
    await place_futures_order(
        symbol,
        "CLOSE",
        usdt_amount=cfg.get("usdtAmount"),
        leverage=cfg.get("leverage"),
        margin_type=cfg.get("marginType"),
        tp_pct=cfg.get("takeProfitPct"),
        sl_pct=cfg.get("stopLossPct"),
        trailing_stop_pct=cfg.get("trailingStopPct", 0.0),
    )
    g["active"] = False
    g["closedBy"] = reason
    g["closedMark"] = mark
    g["closedAt"] = int(time.time())
    _autotrade_log(f"LIVE guardian close: {reason} at {mark:.6f}")
    return True


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
    }
    AUTO_TRADE["paper"]["history"] = [trade] + AUTO_TRADE["paper"]["history"][:49]
    AUTO_TRADE["paper"]["position"] = None
    # Obsidian-style learning: persist per-symbol outcomes
    sym = str(trade.get("symbol") or (AUTO_TRADE.get("config") or {}).get("symbol") or "").upper()
    if sym:
        _record_learning_trade(sym, trade, "PAPER")
    _persist_autotrade_snapshot()
    return trade


def _persist_autotrade_snapshot():
    try:
        payload = {
            "savedAt": int(time.time()),
            "paper": dict(AUTO_TRADE["paper"]),
            "config": AUTO_TRADE.get("config"),
            "running": bool(AUTO_TRADE.get("running")),
            "sessionId": AUTO_TRADE.get("sessionId"),
            "startedAt": AUTO_TRADE.get("startedAt", 0),
            "lastTradeAt": AUTO_TRADE.get("lastTradeAt", 0),
            "liveGuardian": AUTO_TRADE.get("liveGuardian"),
            "scanBoard": list(AUTO_TRADE.get("scanBoard", []))[:10],
            "trades": list(AUTO_TRADE.get("trades", []))[-60:],
        }
        SNAPSHOT_PATH.write_text(json.dumps(payload, ensure_ascii=False, default=str), encoding="utf-8")
        AUTO_TRADE["_snapshot_saved_at"] = payload["savedAt"]
    except Exception:
        pass


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
            if start <= hhmm <= end:
                return True
        else:
            # Overnight window, e.g. 23:00-01:00
            if hhmm >= start or hhmm <= end:
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

def _get_um_client(key: str, secret: str, base: str):
    if CONNECTOR_MODE == "legacy":
        return None
    connector_cls = _resolve_umfutures_class()
    if connector_cls is None:
        if CONNECTOR_MODE == "official":
            raise HTTPException(status_code=500, detail="Official connector mode enabled but UMFutures not available")
        return None
    return connector_cls(key=key, secret=secret, base_url=base)
    text = str(err)
    if "No need to change margin type" in text:
        return {"code": "MARGIN_UNCHANGED", "message": "Margin type is already set"}
    if "-2019" in text or "Margin is insufficient" in text:
        return {"code": "INSUFFICIENT_MARGIN", "message": "Insufficient margin for this order"}
    if "-2021" in text:
        return {"code": "ORDER_REJECTED", "message": "Order rejected by exchange filters/constraints"}
    if "Quantity below minQty" in text:
        return {"code": "QTY_TOO_SMALL", "message": text}
    if "Notional" in text and "limit" in text:
        return {"code": "RISK_NOTIONAL_LIMIT", "message": text}
    if "Leverage" in text and "limit" in text:
        return {"code": "RISK_LEVERAGE_LIMIT", "message": text}
    if "Kill-switch enabled" in text:
        return {"code": "RISK_KILL_SWITCH", "message": text}
    return {"code": "TRADE_ERROR", "message": text}


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
        except Exception:
            pass

    st = await _margin_state()
    cur = st.get("marginType")
    has_pos = bool(st.get("hasPosition"))
    if cur in ("ISOLATED", "CROSSED") and cur != margin_type and has_pos:
        raise HTTPException(
            status_code=409,
            detail=f"Margin currently {cur} with open position. Close position first to switch to {margin_type}.",
        )

    if client:
        client.change_leverage(symbol=symbol, leverage=leverage)
        try:
            client.change_margin_type(symbol=symbol, marginType=margin_type)
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
                return client.new_order(**primary)
            return await _signed_request("POST", base, "/fapi/v1/order", key, secret, primary)
        except Exception as e:
            txt = str(e)
            if ("-4120" not in txt) and ("Order type not supported" not in txt):
                raise
            if client:
                return client.new_order(**fallback)
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
        return client.new_order(
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


async def _close_position(symbol: str, key: str, secret: str, base: str):
    hedge_mode = await _is_hedge_mode(key, secret, base)
    close_mark = await fetch_mark_price(symbol)
    client = _get_um_client(key, secret, base)
    if client:
        pos = client.get_position_risk(symbol=symbol)
    else:
        pos = await _signed_request("GET", base, "/fapi/v2/positionRisk", key, secret, {"symbol": symbol})
    if isinstance(pos, dict):
        pos = [pos]
    if not isinstance(pos, list):
        pos = []
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
            close_results.append(client.new_order(**payload))
        else:
            close_results.append(await _signed_request("POST", base, "/fapi/v1/order", key, secret, payload))
        if entry > 0 and qty > 0:
            pnl = (close_mark - entry) * qty if pos_side == "LONG" else (entry - close_mark) * qty
            learned_trades.append({
                "side": pos_side,
                "entry": entry,
                "exit": close_mark,
                "qty": qty,
                "pnl": round(float(pnl), 6),
                "reason": "LIVE_CLOSE",
                "closedAt": int(time.time()),
            })
    if not close_results:
        return {"message": "No open position"}
    for t in learned_trades:
        _record_learning_trade(symbol, t, "LIVE")
    return {"closed": close_results}


async def _close_position_one_side(symbol: str, side_to_close: str, key: str, secret: str, base: str):
    target = side_to_close.upper()
    if target not in ("LONG", "SHORT"):
        raise HTTPException(status_code=400, detail="side_to_close must be LONG or SHORT")
    hedge_mode = await _is_hedge_mode(key, secret, base)
    close_mark = await fetch_mark_price(symbol)
    client = _get_um_client(key, secret, base)
    if client:
        pos = client.get_position_risk(symbol=symbol)
    else:
        pos = await _signed_request("GET", base, "/fapi/v2/positionRisk", key, secret, {"symbol": symbol})
    rows = pos if isinstance(pos, list) else ([pos] if isinstance(pos, dict) else [])
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
            closed.append(client.new_order(**payload))
        else:
            closed.append(await _signed_request("POST", base, "/fapi/v1/order", key, secret, payload))
        entry = float(p.get("entryPrice", 0) or 0)
        if entry > 0 and qty > 0:
            pnl = (close_mark - entry) * qty if ps == "LONG" else (entry - close_mark) * qty
            learned.append({
                "side": ps,
                "entry": entry,
                "exit": close_mark,
                "qty": qty,
                "pnl": round(float(pnl), 6),
                "reason": "LIVE_CUT_LOSING_SIDE",
                "closedAt": int(time.time()),
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
        AUTO_TRADE["liveGuardian"] = None
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
                entry = client.new_order(**entry_params)
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
    local_guardian = None
    try:
        protective = await _place_tp_sl(symbol, side, qty, mark, tp_pct, sl_pct, key, secret, base, filters["tickSize"], filters.get("tickSizeStr", "0.0001"), hedge_mode, position_side)
    except Exception as e:
        # Keep entry execution successful even if exchange rejects TP/SL endpoint type.
        protective = {"warning": str(e)}
    if isinstance(protective, dict) and protective.get("warning"):
        tp_price, sl_price = _calc_tp_sl_prices(side, mark, tp_pct, sl_pct)
        local_guardian = {
            "active": True,
            "symbol": symbol,
            "side": side,
            "entryMark": mark,
            "tp": tp_price,
            "sl": sl_price,
            "qty": qty,
            "armedAt": int(time.time()),
            "reason": "EXCHANGE_TPSL_UNAVAILABLE",
        }
        AUTO_TRADE["liveGuardian"] = local_guardian
        _autotrade_log(f"LIVE guardian armed for {symbol} {side} TP={tp_price:.6f} SL={sl_price:.6f}")
    trailing = await _place_trailing_stop(symbol, side, key, secret, base, trailing_stop_pct)
    return {"mode": "live", "entry": entry, "protective": protective, "localGuardian": local_guardian, "trailing": trailing}


async def trade(req: TradeRequest):
    try:
        return await place_futures_order(
            req.symbol,
            req.side,
            quantity=req.quantity,
            usdt_amount=req.usdtAmount,
            leverage=req.leverage,
            margin_type=req.marginType,
            tp_pct=req.takeProfitPct,
            sl_pct=req.stopLossPct,
            trailing_stop_pct=0.0,
        )
    except HTTPException:
        raise
    except Exception as err:
        mapped = _humanize_trade_error(err)
        raise HTTPException(status_code=400, detail=mapped)


async def _autotrade_loop():
    while AUTO_TRADE["running"]:
        cfg = AUTO_TRADE["config"] or {}
        try:
            now = int(time.time())
            if (cfg.get("executionMode") or "PAPER").upper() == "LIVE":
                closed = await _live_guardian_maybe_close(cfg)
                if closed:
                    AUTO_TRADE["lastTradeAt"] = now
                    AUTO_TRADE["trades"].append(now)
                    await asyncio.sleep(cfg["intervalSec"])
                    continue
            if _within_no_trade_window(time.localtime(now), cfg.get("noTradeWindows", [])):
                _autotrade_skip("no_trade_window", "Skip: in no-trade window")
                await asyncio.sleep(cfg["intervalSec"])
                continue
            AUTO_TRADE["trades"] = [t for t in AUTO_TRADE["trades"] if now - t < 3600]
            if len(AUTO_TRADE["trades"]) >= cfg["maxTradesPerHour"]:
                _autotrade_skip("max_trades", "Skip: max trades per hour reached")
                await asyncio.sleep(cfg["intervalSec"])
                continue
            if now - AUTO_TRADE["lastTradeAt"] < cfg["cooldownSec"]:
                await asyncio.sleep(cfg["intervalSec"])
                continue

            scan_mode = bool(cfg.get("marketScan")) or str(cfg.get("symbol", "")).upper() in ("AUTO", "SCAN")
            if scan_mode:
                try:
                    picked_symbol, picked_intel, board = await asyncio.wait_for(
                        _pick_best_symbol_from_scan(cfg),
                        timeout=float(cfg.get("intervalSec", 20)) * 2.0 + 12,
                    )
                except asyncio.TimeoutError:
                    _autotrade_skip("timeout", "Skip: market scan timed out")
                    await asyncio.sleep(cfg.get("intervalSec", 20))
                    continue
                AUTO_TRADE["scanBoard"] = board
                if not picked_symbol or not isinstance(picked_intel, dict):
                    _autotrade_skip("scan_none", "Skip: scan found no clear symbol")
                    await asyncio.sleep(cfg.get("intervalSec", 20))
                    continue
                if cfg.get("symbol") != picked_symbol:
                    cfg["symbol"] = picked_symbol
                    AUTO_TRADE["config"] = cfg
                    _autotrade_log(f"SCAN pick: {picked_symbol}")
                intel = picked_intel
            else:
                primary_symbol = str(cfg.get("primarySymbol") or cfg.get("symbol"))
                cfg["symbol"] = primary_symbol
                # Use cached intel if available and fresh — avoids redundant computation
                # when interval is shorter than cache TTL
                intel_req = IntelAnalyzeRequest(symbol=primary_symbol)
                try:
                    intel = await asyncio.wait_for(
                        intel_analyze(intel_req),
                        timeout=float(cfg.get("intervalSec", 20)) * 1.5 + 10,
                    )
                except asyncio.TimeoutError:
                    _autotrade_skip("timeout", "Skip: intel_analyze timed out (Binance API slow)")
                    await asyncio.sleep(cfg.get("intervalSec", 20))
                    continue
                AUTO_TRADE["scanBoard"] = []
                # Hybrid mode: keep primary symbol by default, switch only when scan winner is clearly better.
                if bool(cfg.get("hybridScan")):
                    try:
                        picked_symbol, picked_intel, board = await asyncio.wait_for(
                            _pick_best_symbol_from_scan(cfg),
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
                                AUTO_TRADE["config"] = cfg
                                intel = picked_intel
                                _autotrade_log(
                                    f"HYBRID switch: {primary_symbol} -> {picked_symbol} (scan {scan_score:.3f} > base {base_score:.3f})"
                                )
                    except Exception:
                        pass
            AUTO_TRADE["lastDecision"] = intel
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
                if p["side"] == "LONG":
                    if mark >= p["tp"]:
                        if _should_hold_winner("LONG", intel, cfg):
                            new_sl, new_tp = _trail_winner_levels(
                                "LONG",
                                mark,
                                float(p["sl"]),
                                float(p["tp"]),
                                float(cfg.get("holdTrailPct", 0.35)),
                            )
                            p["sl"] = new_sl
                            p["tp"] = new_tp
                            _autotrade_log(f"PAPER hold winner: trail TP/SL -> TP={new_tp:.6f} SL={new_sl:.6f}")
                        else:
                            t = _paper_close("TP_HIT", mark)
                            _autotrade_log(f"PAPER close TP pnl={t['pnl']:.4f}")
                    elif mark <= p["sl"]:
                        t = _paper_close("SL_HIT", mark)
                        _autotrade_log(f"PAPER close SL pnl={t['pnl']:.4f}")
                else:
                    if mark <= p["tp"]:
                        if _should_hold_winner("SHORT", intel, cfg):
                            new_sl, new_tp = _trail_winner_levels(
                                "SHORT",
                                mark,
                                float(p["sl"]),
                                float(p["tp"]),
                                float(cfg.get("holdTrailPct", 0.35)),
                            )
                            p["sl"] = new_sl
                            p["tp"] = new_tp
                            _autotrade_log(f"PAPER hold winner: trail TP/SL -> TP={new_tp:.6f} SL={new_sl:.6f}")
                        else:
                            t = _paper_close("TP_HIT", mark)
                            _autotrade_log(f"PAPER close TP pnl={t['pnl']:.4f}")
                    elif mark >= p["sl"]:
                        t = _paper_close("SL_HIT", mark)
                        _autotrade_log(f"PAPER close SL pnl={t['pnl']:.4f}")

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
            if spread_bps > cfg["maxSpreadBps"]:
                _autotrade_skip("spread", f"Skip: spread too wide {spread_bps:.2f} bps")
                await asyncio.sleep(cfg["intervalSec"])
                continue

            signal = intel.get("signal", "WAIT")
            conf = float(intel.get("confidence", 0))
            adaptive_min_conf = _learned_min_conf(cfg["symbol"], float(cfg["minConfidence"]))
            vision = intel.get("vision")
            if signal == "WAIT" and cfg.get("aggressiveScalp"):
                ob = intel.get("orderBook") if isinstance(intel, dict) else None
                imb = float((ob or {}).get("imbalance", 0))
                thr = float(cfg.get("waitOverrideImbalance", 0.08))
                if imb >= thr:
                    signal = "LONG"
                    conf = max(conf, 0.5)
                    _autotrade_log(f"Aggressive override: WAIT->LONG (imbalance={imb:.4f})")
                elif imb <= -thr:
                    signal = "SHORT"
                    conf = max(conf, 0.5)
                    _autotrade_log(f"Aggressive override: WAIT->SHORT (imbalance={imb:.4f})")
            if signal not in ("LONG", "SHORT"):
                _autotrade_skip("signal_wait", "Skip: signal WAIT")
                await asyncio.sleep(cfg["intervalSec"])
                continue
            if conf < adaptive_min_conf:
                _autotrade_skip("low_confidence", f"Skip: low confidence {conf} < adaptive {adaptive_min_conf:.2f}")
                await asyncio.sleep(cfg["intervalSec"])
                continue
            if cfg["requireVisionConsensus"] and not vision:
                _autotrade_skip("vision", "Skip: vision consensus required")
                await asyncio.sleep(cfg["intervalSec"])
                continue

            thr_fund = float(cfg.get("skipFundingAgainst") or 0)
            if thr_fund > 0 and ex.get("lastFundingRate") is not None:
                fr = float(ex["lastFundingRate"])
                if signal == "LONG" and fr > thr_fund:
                    _autotrade_skip("funding", f"Skip: funding adverse for LONG ({fr:.6f} > {thr_fund})")
                    await asyncio.sleep(cfg["intervalSec"])
                    continue
                if signal == "SHORT" and fr < -thr_fund:
                    _autotrade_skip("funding", f"Skip: funding adverse for SHORT ({fr:.6f} < {-thr_fund})")
                    await asyncio.sleep(cfg["intervalSec"])
                    continue

            key = os.getenv("BINANCE_API_KEY")
            secret = os.getenv("BINANCE_API_SECRET")
            base = _binance_base()
            ref_px = ask if signal == "LONG" else bid
            slippage_bps = abs((ref_px - mark) / max(mark, 1e-9)) * 10000
            if slippage_bps > cfg["maxSlippageBps"]:
                _autotrade_skip("slippage", f"Skip: slippage estimate too high {slippage_bps:.2f} bps")
                await asyncio.sleep(cfg["intervalSec"])
                continue
            trade_usdt = _adaptive_trade_usdt(cfg["usdtAmount"], cfg["symbol"], intel, cfg)
            if trade_usdt > float(RISK["max_notional"]):
                trade_usdt = float(RISK["max_notional"])
            gross_u, est_cost_u, net_u = _estimate_trade_edge_usdt(
                trade_usdt, cfg["takeProfitPct"], cfg["maxSlippageBps"]
            )
            if net_u <= AUTOTRADE_MIN_NET_PROFIT_USDT:
                _autotrade_skip(
                    "fee_edge",
                    f"Skip: net edge too low (gross {gross_u:.4f} - cost {est_cost_u:.4f} = {net_u:.4f} USDT)",
                )
                await asyncio.sleep(cfg["intervalSec"])
                continue

            if mode == "PAPER":
                qty = trade_usdt / max(mark, 1e-9)
                if AUTO_TRADE["paper"]["position"] and AUTO_TRADE["paper"]["position"]["side"] != signal:
                    t = _paper_close("FLIP_SIGNAL", mark)
                    _autotrade_log(f"PAPER close flip pnl={t['pnl']:.4f}")
                if not AUTO_TRADE["paper"]["position"]:
                    tp = mark * (1 + cfg["takeProfitPct"] / 100) if signal == "LONG" else mark * (1 - cfg["takeProfitPct"] / 100)
                    sl = mark * (1 - cfg["stopLossPct"] / 100) if signal == "LONG" else mark * (1 + cfg["stopLossPct"] / 100)
                    AUTO_TRADE["paper"]["position"] = {
                        "symbol": cfg["symbol"],
                        "side": signal,
                        "entry": mark,
                        "qty": qty,
                        "tp": tp,
                        "sl": sl,
                        "openedAt": now,
                    }
                    trade_res = {"mode": "paper", "entry": {"side": signal, "price": mark, "qty": qty}, "tp": tp, "sl": sl}
                else:
                    _autotrade_skip("paper_open", "Skip: paper position still open")
                    await asyncio.sleep(cfg["intervalSec"])
                    continue
            else:
                pst = await _position_side_state(cfg["symbol"], key, secret, base)
                pos_amt = float(pst.get("net", 0.0))
                if float(pst.get("long", 0.0)) > 0 and float(pst.get("short", 0.0)) > 0:
                    clear_thr = max(float(cfg.get("minConfidence", 0.65)), float(cfg.get("holdMinConfidence", 0.72)))
                    if conf >= clear_thr and signal in ("LONG", "SHORT"):
                        cut_side = "SHORT" if signal == "LONG" else "LONG"
                        rs = await _close_position_one_side(cfg["symbol"], cut_side, key, secret, base)
                        if rs.get("closed"):
                            _autotrade_log(
                                f"Hedge normalize: closed {cut_side} side immediately (signal={signal}, conf={conf:.3f})"
                            )
                        else:
                            _autotrade_skip(
                                "hedge_both_sides",
                                f"Skip: both LONG({pst['long']:.6f}) and SHORT({pst['short']:.6f}) open; no {cut_side} closeable rows",
                            )
                        await asyncio.sleep(1)
                        continue
                    _autotrade_skip(
                        "hedge_both_sides",
                        f"Skip: both LONG({pst['long']:.6f}) and SHORT({pst['short']:.6f}) open; waiting clearer signal",
                    )
                    await asyncio.sleep(cfg["intervalSec"])
                    continue
                current_side = "LONG" if pos_amt > 0 else "SHORT" if pos_amt < 0 else "FLAT"
                if current_side == "FLAT":
                    try:
                        open_n = await _open_positions_count(key, secret, base)
                        max_n = int(cfg.get("maxOpenPositions", 6))
                        if open_n >= max_n:
                            _autotrade_skip("max_open_positions", f"Skip: open positions {open_n}/{max_n} reached")
                            await asyncio.sleep(cfg["intervalSec"])
                            continue
                    except Exception:
                        pass
                if current_side == signal:
                    _autotrade_skip("same_side", f"Skip: already in {signal}")
                    await asyncio.sleep(cfg["intervalSec"])
                    continue
                if current_side in ("LONG", "SHORT") and signal != current_side:
                    if not cfg["allowFlip"]:
                        _autotrade_skip("flip_disabled", "Skip: opposite signal but flip disabled")
                        await asyncio.sleep(cfg["intervalSec"])
                        continue
                    await place_futures_order(cfg["symbol"], "CLOSE", usdt_amount=trade_usdt, leverage=cfg["leverage"], margin_type=cfg["marginType"], tp_pct=cfg["takeProfitPct"], sl_pct=cfg["stopLossPct"], trailing_stop_pct=cfg.get("trailingStopPct", 0.0))
                    _autotrade_log("Closed old position for flip")

                # ── Pre-flight: check available balance before placing order ──
                try:
                    acct = await asyncio.wait_for(
                        _signed_request("GET", base, "/fapi/v2/account", key, secret, {}),
                        timeout=6.0,
                    )
                    avail = float(acct.get("availableBalance", 0) or 0)
                    required = trade_usdt / max(cfg["leverage"], 1)
                    if avail < required * 1.05:  # 5% buffer
                        # Auto-reduce amount to fit available balance
                        safe_amt = round(avail * cfg["leverage"] * 0.9, 2)
                        if safe_amt < 1.0:
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
                                    AUTO_TRADE["config"] = cfg
                                    _autotrade_log(f"Balance check: auto-switched CROSSED → ISOLATED (crossBal={cross_bal:.2f})")
                                else:
                                    _autotrade_log(f"Balance check: low crossBal={cross_bal:.2f} but position open — cannot switch margin type")
                            except Exception:
                                pass  # skip switch if can't check position
                except Exception:
                    pass  # balance check is best-effort; proceed anyway

                async def _do_place():
                    return await place_futures_order(
                        cfg["symbol"],
                        signal,
                        usdt_amount=trade_usdt,
                        leverage=cfg["leverage"],
                        margin_type=cfg["marginType"],
                        tp_pct=cfg["takeProfitPct"],
                        sl_pct=cfg["stopLossPct"],
                        trailing_stop_pct=cfg.get("trailingStopPct", 0.0),
                    )

                place_timeout = max(20.0, float(cfg.get("intervalSec", 20)) * 1.5)
                try:
                    trade_res = await asyncio.wait_for(_do_place(), timeout=place_timeout)
                except asyncio.TimeoutError:
                    _autotrade_log("Retry: place order timed out once, retrying immediately")
                    trade_res = await asyncio.wait_for(_do_place(), timeout=place_timeout + 8.0)
            AUTO_TRADE["lastTradeAt"] = now
            AUTO_TRADE["trades"].append(now)
            AUTO_TRADE["consecutiveErrors"] = 0
            AUTO_TRADE["lastSkip"] = None
            _autotrade_log(f"{mode} trade executed: {signal} {cfg['symbol']} {trade_usdt} USDT")
            AUTO_TRADE["lastDecision"] = {"intel": intel, "trade": trade_res}
        except asyncio.TimeoutError:
            # Network timeout — not a logic error, use softer backoff
            AUTO_TRADE["consecutiveErrors"] = min(AUTO_TRADE.get("consecutiveErrors", 0) + 1, 10)
            _autotrade_skip("timeout", "Skip: network timeout (Binance API slow) — will retry")
        except Exception as e:
            err_msg = _format_loop_error(e)
            AUTO_TRADE["consecutiveErrors"] = min(AUTO_TRADE.get("consecutiveErrors", 0) + 1, 20)

            # ── Auto-recovery for known Binance errors ────────────────────────
            cfg = AUTO_TRADE.get("config") or {}

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
                        AUTO_TRADE["config"] = cfg
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
                AUTO_TRADE["config"] = cfg
                _autotrade_skip("exception", f"Error -2019: Margin insufficient — reduced USDT {old_amt} → {cfg['usdtAmount']}")

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
    if int(cfg["leverage"]) > int(RISK["max_leverage"]):
        raise HTTPException(
            status_code=400,
            detail=f"Leverage {cfg['leverage']} exceeds server max {int(RISK['max_leverage'])}",
        )
    if float(cfg["usdtAmount"]) > float(RISK["max_notional"]):
        raise HTTPException(
            status_code=400,
            detail=f"USDT amount {cfg['usdtAmount']} exceeds server max notional {RISK['max_notional']}",
        )
    gross_u, est_cost_u, net_u = _estimate_trade_edge_usdt(
        cfg["usdtAmount"], cfg["takeProfitPct"], cfg["maxSlippageBps"]
    )
    if net_u <= AUTOTRADE_MIN_NET_PROFIT_USDT:
        raise HTTPException(
            status_code=400,
            detail=(
                "ตั้งค่า TP/ขนาดไม้ยังไม่คุ้มค่าธรรมเนียมและต้นทุน (สุทธิ <= ขั้นต่ำ) "
                f"| gross={gross_u:.4f} cost={est_cost_u:.4f} net={net_u:.4f} USDT"
            ),
        )
    session_id = str(uuid4())
    AUTO_TRADE["running"] = True
    AUTO_TRADE["sessionId"] = session_id
    AUTO_TRADE["startedAt"] = int(time.time())
    AUTO_TRADE["config"] = cfg
    AUTO_TRADE["lastDecision"] = None
    AUTO_TRADE["lastSkip"] = None
    AUTO_TRADE["consecutiveErrors"] = 0
    AUTO_TRADE["lastTradeAt"] = 0
    AUTO_TRADE["trades"] = []
    AUTO_TRADE["liveGuardian"] = None
    AUTO_TRADE["scanBoard"] = []
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
    _persist_autotrade_snapshot()
    global _AUTOTRADE_TASK
    # Cancel any stale task before starting a new one
    if _AUTOTRADE_TASK and not _AUTOTRADE_TASK.done():
        _AUTOTRADE_TASK.cancel()
    _AUTOTRADE_TASK = asyncio.create_task(_autotrade_loop())
    return {"ok": True, "running": True, "config": cfg, "sessionId": session_id}


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
    AUTO_TRADE["liveGuardian"] = None
    AUTO_TRADE["lastSkip"] = None
    AUTO_TRADE["consecutiveErrors"] = 0
    AUTO_TRADE["scanBoard"] = []
    _autotrade_log("AutoTrade stopped")
    _persist_autotrade_snapshot()
    return {"ok": True, "running": False}


@app.post("/autotrade/reset")
def autotrade_reset(req: AutoTradeControlRequest | None = None):
    req = req or AutoTradeControlRequest()
    current_session = AUTO_TRADE.get("sessionId")
    if AUTO_TRADE["running"] and current_session and not req.force:
        if req.sessionId != current_session:
            return {"ok": False, "running": True, "ignored": True, "reason": "SESSION_MISMATCH"}
    AUTO_TRADE["running"] = False
    AUTO_TRADE["sessionId"] = None
    AUTO_TRADE["startedAt"] = 0
    AUTO_TRADE["config"] = None
    AUTO_TRADE["lastDecision"] = None
    AUTO_TRADE["lastTradeAt"] = 0
    AUTO_TRADE["trades"] = []
    AUTO_TRADE["log"] = []
    AUTO_TRADE["liveGuardian"] = None
    AUTO_TRADE["lastSkip"] = None
    AUTO_TRADE["consecutiveErrors"] = 0
    AUTO_TRADE["scanBoard"] = []
    _paper_reset()
    _autotrade_log("AutoTrade session reset")
    _persist_autotrade_snapshot()
    return {"ok": True, "running": False, "reset": True}


@app.get("/autotrade/status")
async def autotrade_status(symbol: str | None = None):
    p = AUTO_TRADE["paper"]
    total = p["wins"] + p["losses"]
    cfg = AUTO_TRADE["config"] or {}
    profiles = _load_learning_profiles()
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
    # Only hit Binance signed API when actually running LIVE — saves 2 API calls/5s when stopped
    if AUTO_TRADE["running"] and cfg.get("executionMode") == "LIVE":
        try:
            sym = cfg.get("symbol")
            if sym:
                key = os.getenv("BINANCE_API_KEY")
                secret = os.getenv("BINANCE_API_SECRET")
                base = _binance_base()
                amt = await asyncio.wait_for(_current_position_amount(sym, key, secret, base), timeout=8.0)
                mark = await asyncio.wait_for(fetch_mark_price(sym), timeout=8.0)
                live_position = {
                    "side": "LONG" if amt > 0 else "SHORT" if amt < 0 else "FLAT",
                    "qty": abs(float(amt)),
                    "notionalUsdtApprox": round(abs(float(amt)) * mark, 6),
                }
        except Exception:
            pass

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
                    }
                    continuity_hints.append(
                        f"กระดานยังมีโพซิชัน {orphan_live['side']} {qs} ~{orphan_live['notionalUsdtApprox']} USDT"
                    )
            except Exception:
                pass

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
    live_profile = profiles.get(stat_symbol, {}) if stat_symbol else {}
    live_wins = int(live_profile.get("wins", 0))
    live_losses = int(live_profile.get("losses", 0))
    live_total = live_wins + live_losses
    live_last_trades = []
    if stat_symbol and TRADES_LOG_PATH.exists():
        try:
            rows = TRADES_LOG_PATH.read_text(encoding="utf-8").splitlines()
            for line in reversed(rows):
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if str(obj.get("mode", "")).upper() != "LIVE":
                    continue
                if str(obj.get("symbol", "")).upper() != str(stat_symbol).upper():
                    continue
                live_last_trades.append(obj)
                if len(live_last_trades) >= 10:
                    break
        except Exception:
            live_last_trades = []

    return {
        "running": AUTO_TRADE["running"],
        "sessionId": AUTO_TRADE.get("sessionId"),
        "startedAt": AUTO_TRADE.get("startedAt", 0),
        "config": cfg,
        "lastDecision": AUTO_TRADE["lastDecision"],
        "lastSkip": AUTO_TRADE.get("lastSkip"),
        "consecutiveErrors": AUTO_TRADE.get("consecutiveErrors", 0),
        "lastTradeAt": AUTO_TRADE["lastTradeAt"],
        "tradesLastHour": len([t for t in AUTO_TRADE["trades"] if int(time.time()) - t < 3600]),
        "log": AUTO_TRADE["log"][:15],
        "liveGuardian": AUTO_TRADE.get("liveGuardian"),
        "scanBoard": list(AUTO_TRADE.get("scanBoard", []))[:10],
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
            "realizedPnl": round(float(live_profile.get("realizedPnl", 0.0)), 6),
            "lastTrades": live_last_trades,
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
        "continuity": continuity,
    }


@app.get("/learning/status")
def learning_status(symbol: str | None = None):
    profiles = _load_learning_profiles()
    if symbol:
        sym = _normalize_symbol(symbol)
        p = profiles.get(sym, {})
        wins = int(p.get("wins", 0))
        losses = int(p.get("losses", 0))
        n = wins + losses
        wr = round((wins / n) * 100, 2) if n > 0 else 0.0
        return {"symbol": sym, "profile": p, "winRatePct": wr, "adaptiveMinConf": _learned_min_conf(sym, 0.62)}
    out = []
    for sym, p in profiles.items():
        wins = int(p.get("wins", 0))
        losses = int(p.get("losses", 0))
        n = wins + losses
        wr = round((wins / n) * 100, 2) if n > 0 else 0.0
        out.append({"symbol": sym, "wins": wins, "losses": losses, "winRatePct": wr, "realizedPnl": p.get("realizedPnl", 0.0)})
    out.sort(key=lambda x: x["symbol"])
    return {"items": out, "vaultDir": str(VAULT_DIR)}


async def _monitor_loop(monitor_id: str):
    while monitor_id in MONITORS and MONITORS[monitor_id]["status"] == "RUNNING":
        plan = StrategyPlan(**MONITORS[monitor_id]["plan"])
        try:
            result = await strategy_evaluate(plan)
            MONITORS[monitor_id]["lastResult"] = result
            if result.get("status") == "TRIGGERED" and result.get("action") in ["LONG", "SHORT"]:
                trade_result = await place_futures_order(plan.symbol, result["action"], plan.quantity, tp_pct=plan.takeProfitPct, sl_pct=plan.stopLossPct)
                MONITORS[monitor_id]["lastTrade"] = trade_result
                MONITORS[monitor_id]["status"] = "TRIGGERED"
                return
        except Exception as err:
            MONITORS[monitor_id]["lastError"] = str(err)
        await asyncio.sleep(MONITORS[monitor_id]["intervalSec"])


async def monitor_start(req: MonitorStartRequest):
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


def monitor_list():
    # Evict monitors stopped more than 5 minutes ago to prevent unbounded growth
    cutoff = time.time() - 300
    stale = [k for k, v in MONITORS.items()
             if v.get("status") == "STOPPED" and v.get("stoppedAt", 0) < cutoff]
    for k in stale:
        del MONITORS[k]
    return {"items": list(MONITORS.values())}


def monitor_stop(monitor_id: str):
    if monitor_id not in MONITORS:
        raise HTTPException(status_code=404, detail="Monitor not found")
    MONITORS[monitor_id]["status"] = "STOPPED"
    MONITORS[monitor_id]["stoppedAt"] = int(time.time())
    return MONITORS[monitor_id]

from routers.analysis_routes import router as analysis_router
from routers.system_routes import router as system_router

app.include_router(system_router)
app.include_router(analysis_router)
