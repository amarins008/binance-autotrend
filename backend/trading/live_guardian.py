"""Live position guardian orchestration and profit-lock management."""

from __future__ import annotations

import asyncio
import os
import time

from exchange.binance_client import _binance_base
from schemas import IntelAnalyzeRequest
from services import app_state
from services.config_paths import TRADES_LOG_PATH
from trading.position import (
    get_tradingview_sl_trailing_pct,
    get_tradingview_tp_extension_pct,
    should_exit_early_with_tradingview,
    should_extend_tp_with_tradingview,
    should_trail_sl_with_tradingview,
)
from trading.tradingview_mcp import get_tv_mcp, async_get_position_guidance

AUTO_TRADE = app_state.AUTO_TRADE


def _main():
    import main as m
    return m


# Lazy delegates to main during incremental refactor

def _agent_mark(*args, **kwargs):
    return _main()._agent_mark(*args, **kwargs)

def _autotrade_log(*args, **kwargs):
    return _main()._autotrade_log(*args, **kwargs)

def _fee_edge_min_net_usdt(*args, **kwargs):
    return _main()._fee_edge_min_net_usdt(*args, **kwargs)

def _profit_lock_policy(*args, **kwargs):
    return _main()._profit_lock_policy(*args, **kwargs)

def _recent_payoff_loss_guard(*args, **kwargs):
    return _main()._recent_payoff_loss_guard(*args, **kwargs)

def _last_decision_intel(*args, **kwargs):
    return _main()._last_decision_intel(*args, **kwargs)

def _entry_snapshot_from_intel(*args, **kwargs):
    return _main()._entry_snapshot_from_intel(*args, **kwargs)

def _effective_tp_sl(*args, **kwargs):
    return _main()._effective_tp_sl(*args, **kwargs)

def _symbol_effective_profile(*args, **kwargs):
    return _main()._symbol_effective_profile(*args, **kwargs)

def _calc_tp_sl_prices(*args, **kwargs):
    return _main()._calc_tp_sl_prices(*args, **kwargs)

def _get_um_client(*args, **kwargs):
    return _main()._get_um_client(*args, **kwargs)

async def _signed_request(*args, **kwargs):
    return await _main()._signed_request(*args, **kwargs)

async def _um_client_position_risk(*args, **kwargs):
    return await _main()._um_client_position_risk(*args, **kwargs)

async def fetch_mark_price(*args, **kwargs):
    return await _main().fetch_mark_price(*args, **kwargs)

async def _current_position_amount(*args, **kwargs):
    return await _main()._current_position_amount(*args, **kwargs)

async def _close_position_one_side(*args, **kwargs):
    return await _main()._close_position_one_side(*args, **kwargs)

async def place_futures_order(*args, **kwargs):
    return await _main().place_futures_order(*args, **kwargs)

async def intel_analyze(*args, **kwargs):
    return await _main().intel_analyze(*args, **kwargs)


def _persist_autotrade_snapshot(*args, **kwargs):
    return _main()._persist_autotrade_snapshot(*args, **kwargs)


async def _manage_live_open_positions_once(cfg: dict, now: int) -> bool:
    _orch_start = time.monotonic()
    _main()._agent_mark("risk_manager", "done", "risk policy active")
    _main()._agent_mark("position_guardian", "doing", "monitor open live positions")
    closed_by_lock = await _live_multi_profit_lock_manage(cfg)
    if closed_by_lock:
        _main()._agent_mark("position_guardian", "done", "position closed by profit guard")
        AUTO_TRADE["lastTradeAt"] = now
        AUTO_TRADE["trades"].append(now)
        _persist_autotrade_snapshot(force=True)
        return True
    # Heartbeat — use live positions cache populated by System B.
    _, live_rows = app_state._LIVE_POSITIONS_CACHE
    heartbeat_positions = [
        p for p in (live_rows or [])
        if isinstance(p, dict)
        and str(p.get("side", "")).upper() in ("LONG", "SHORT")
        and float(p.get("qty", 0.0) or 0.0) > 0
    ]
    if heartbeat_positions:
        _position_guardian_status_heartbeat(heartbeat_positions)
        _main()._agent_mark("position_guardian", "done", "open positions checked")
        _orch_ms = int((time.monotonic() - _orch_start) * 1000)
        _autotrade_log(f"[Guardian] cycle={_orch_ms}ms positions={len(heartbeat_positions)} no-close")
    else:
        _main()._agent_mark("position_guardian", "todo", "no open positions")
        _orch_ms = int((time.monotonic() - _orch_start) * 1000)
        _autotrade_log(f"[Guardian] cycle={_orch_ms}ms positions=0")
    return False


async def _pick_live_orphan_positions(
    key: str | None, secret: str | None, base: str
) -> list[dict]:
    """Return all open live positions from Binance Futures, sorted by notional desc.

    Cached for 25 seconds (req 5) — long enough to cover a full ~20 s loop cycle so
    System A can reuse the same data fetched by System B without a second API call.
    """
    if not key or not secret:
        return []
    now = time.time()
    cached_at, cached = app_state._LIVE_POSITIONS_CACHE
    # Req 5: raised TTL from 5 s → 25 s
    if (now - cached_at) < 25.0:
        return list(cached)
    client = _main()._get_um_client(key, secret, base)
    if client:
        pos = await _main()._um_client_position_risk(client)
    else:
        pos = await _main()._signed_request("GET", base, "/fapi/v2/positionRisk", key, secret, {})
    rows = pos if isinstance(pos, list) else ([pos] if isinstance(pos, dict) else [])
    out: list[dict] = []
    for p in rows:
        try:
            amt = float(p.get("positionAmt", 0) or 0)
            if abs(amt) <= 0:
                continue
            sym = str(p.get("symbol", "") or "").upper().strip()
            if not sym or not sym.endswith("USDT"):
                continue
            mark = float(p.get("markPrice", 0) or 0)
            entry = float(p.get("entryPrice", 0) or 0)
            notional = abs(amt) * max(mark, 0.0)
            lev_raw = float(p.get("leverage", 0) or 0)
            leverage = int(lev_raw) if lev_raw > 0 else 0
            pos_init_margin = float(p.get("positionInitialMargin", 0) or 0)
            iso_wallet = float(p.get("isolatedWallet", 0) or 0)
            margin_used = pos_init_margin if pos_init_margin > 0 else (
                (notional / max(leverage, 1)) if leverage > 0 else 0.0
            )
            out.append(
                {
                    "symbol": sym,
                    "side": "LONG" if amt > 0 else "SHORT",
                    "qty": abs(amt),
                    "entryMark": round(float(entry), 10),
                    "markPrice": round(float(mark), 10),
                    "notionalUsdtApprox": round(notional, 6),
                    "unRealizedProfit": round(float(p.get("unRealizedProfit", 0) or 0), 6),
                    "leverage": leverage,
                    "marginUsedUsdt": round(float(margin_used), 6),
                    "isolatedWalletUsdt": round(float(iso_wallet), 6),
                }
            )
        except Exception as exc:
            _autotrade_log(f"[Guardian] position parse error for {p.get('symbol','?')}: {exc}")
            continue
    out.sort(key=lambda x: float(x.get("notionalUsdtApprox", 0.0) or 0.0), reverse=True)
    app_state._LIVE_POSITIONS_CACHE = (now, out)
    return out

def _position_guardian_status_heartbeat(open_positions: list[dict]) -> None:
    rows = [p for p in (open_positions or []) if isinstance(p, dict)]
    if not rows:
        return
    symbols = sorted({str(p.get("symbol", "") or "").upper() for p in rows if str(p.get("symbol", "") or "").strip()})
    reason = ", ".join(symbols[:6])
    _main()._agent_mark(
        "position_guardian",
        "done",
        "open positions heartbeat",
        reason,
        {"openPositions": len(rows), "source": "openLivePositions", "heartbeat": True},
    )

def _live_lock_key(symbol: str, side: str) -> str:
    return f"{str(symbol).upper()}:{str(side).upper()}"

def _should_hold_winner(side: str, intel: dict | None, cfg: dict, hold_min_conf: float | None = None) -> bool:
    if not cfg.get("holdWinners", True):
        return False
    if not isinstance(intel, dict):
        return False
    sig = str(intel.get("signal", "WAIT")).upper()
    conf = float(intel.get("confidence", 0.0) or 0.0)
    min_conf = float(hold_min_conf) if hold_min_conf is not None else float(cfg.get("holdMinConfidence", 0.72))
    if sig != side or conf < min_conf:
        return False
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    mom = float(ex.get("momentumPct", 0.0) or 0.0)
    # Keep running only when momentum is clearly supporting the current side.
    # Use a minimum momentum threshold to avoid holding on weak momentum.
    min_momentum = float(cfg.get("holdMinMomentumPct", 0.30) or 0.30)
    return (side == "LONG" and mom >= min_momentum) or (side == "SHORT" and mom <= -min_momentum)

async def _trail_winner_levels(side: str, mark: float, old_sl: float, old_tp: float, trail_pct: float, cfg: dict = None, symbol: str = None) -> tuple[float, float]:
    t = max(0.05, float(trail_pct))
    
    # Get TradingView guidance for SL trailing if enabled
    if cfg and symbol and cfg.get("tradingviewEnabled", False):
        try:
            tv_client = get_tv_mcp(cfg)
            tv_guidance = await async_get_position_guidance(tv_client, symbol, side)
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

def _strong_reversal_exit(side: str, intel: dict | None, cfg: dict) -> tuple[bool, str]:
    if not bool(cfg.get("strongFlipEnabled", True)):
        return False, ""
    if not isinstance(intel, dict):
        return False, ""
    if isinstance(intel.get("intel"), dict):
        intel = intel.get("intel")
    current_side = str(side or "").upper()
    if current_side not in ("LONG", "SHORT"):
        return False, ""
    opposite = "SHORT" if current_side == "LONG" else "LONG"
    sig = str(intel.get("signal", "WAIT")).upper()
    if sig != opposite:
        return False, ""
    conf = float(intel.get("confidence", 0.0) or 0.0)
    min_conf = float(cfg.get("strongFlipMinConfidence", 0.82) or 0.82)
    if conf < min_conf:
        return False, ""
    px = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    long_score = float(px.get("longScore", 0.0) or 0.0)
    short_score = float(px.get("shortScore", 0.0) or 0.0)
    score_gap = (short_score - long_score) if opposite == "SHORT" else (long_score - short_score)
    min_gap = float(cfg.get("strongFlipMinScoreGap", 1.5) or 1.5)
    ultra_gap = float(cfg.get("strongFlipUltraScoreGap", 2.2) or 2.2)
    relax = float(cfg.get("strongFlipUltraConfRelax", 0.08) or 0.08)
    if score_gap < min_gap and not (score_gap >= ultra_gap and conf >= max(0.5, min_conf - relax)):
        return False, ""
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    mom = float(ex.get("momentumPct", 0.0) or 0.0)
    momentum_against = (current_side == "LONG" and mom < 0) or (current_side == "SHORT" and mom > 0)
    if not momentum_against:
        return False, ""
    structure_ok, structure_reason = _strong_reversal_structure_confirmed(current_side, opposite, px, cfg)
    if not structure_ok:
        # Req 4: log suppressed reversal so operators can diagnose false negatives.
        _autotrade_log(f"[Guard] strong-reversal suppressed: {current_side} → {opposite} c={conf:.3f} gap={score_gap:.1f} · {structure_reason}")
        return False, ""
    return True, f"{opposite} c={conf:.3f} gap={score_gap:.1f} mom={mom:.3f}% {structure_reason}".strip()

def _strong_reversal_structure_confirmed(current_side: str, opposite: str, precision: dict | None, cfg: dict) -> tuple[bool, str]:
    if not bool(cfg.get("strongFlipStructureConfirmEnabled", True)):
        return True, "structure=disabled"
    px = precision if isinstance(precision, dict) else {}
    # Req 4: empty / missing precision → FAIL (was incorrectly passing through as True).
    # A reversal exit with zero structural data is indistinguishable from a noise signal.
    if not px:
        return False, "structure=unavailable"
    required = max(1, int(cfg.get("strongFlipConfirmationsRequired", 2) or 2))
    vwap_min = max(0.0, float(cfg.get("strongFlipVwapConfirmPct", 0.06) or 0.06))
    bb_band = max(0.05, min(0.49, float(cfg.get("strongFlipBbConfirmPctB", 0.42) or 0.42)))
    confirms: list[str] = []
    observed = 0

    if any(k in px for k in ("trendUp", "trendDown", "trendUpPartial", "trendDnPartial")):
        observed += 1
        if opposite == "LONG" and (bool(px.get("trendUp")) or bool(px.get("trendUpPartial"))):
            confirms.append("trend")
        if opposite == "SHORT" and (bool(px.get("trendDown")) or bool(px.get("trendDnPartial"))):
            confirms.append("trend")

    if any(k in px for k in ("macdBullish", "macdBearish", "macdCrossUp", "macdCrossDn")):
        observed += 1
        if opposite == "LONG" and (bool(px.get("macdBullish")) or bool(px.get("macdCrossUp"))):
            confirms.append("macd")
        if opposite == "SHORT" and (bool(px.get("macdBearish")) or bool(px.get("macdCrossDn"))):
            confirms.append("macd")

    if "vwapDistancePct" in px:
        observed += 1
        try:
            vwap = float(px.get("vwapDistancePct", 0.0) or 0.0)
        except Exception:
            vwap = 0.0
            _autotrade_log(f"[Guard] vwap parse error: {px.get('vwapDistancePct')}")
        if opposite == "LONG" and vwap >= vwap_min:
            confirms.append("vwap")
        if opposite == "SHORT" and vwap <= -vwap_min:
            confirms.append("vwap")

    if "bbPctB" in px:
        observed += 1
        try:
            bb = float(px.get("bbPctB", 0.5) or 0.5)
        except Exception:
            bb = 0.5
            _autotrade_log(f"[Guard] bbPctB parse error: {px.get('bbPctB')}")
        if opposite == "LONG" and bb >= (1.0 - bb_band):
            confirms.append("bb")
        if opposite == "SHORT" and bb <= bb_band:
            confirms.append("bb")

    if "rsi14" in px or "rsi14_5m" in px:
        observed += 1
        try:
            rsi = float(px.get("rsi14", 50.0) or 50.0)
            rsi5 = float(px.get("rsi14_5m", rsi) or rsi)
        except Exception:
            rsi = 50.0
            rsi5 = 50.0
            _autotrade_log(f"[Guard] RSI parse error: rsi14={px.get('rsi14')} rsi14_5m={px.get('rsi14_5m')}")
        if opposite == "LONG" and (rsi >= 52.0 or rsi5 >= 52.0):
            confirms.append("rsi")
        if opposite == "SHORT" and (rsi <= 48.0 or rsi5 <= 48.0):
            confirms.append("rsi")

    # Req 4: observed == 0 means none of the five indicator keys were present → FAIL.
    if observed == 0:
        return False, "structure=no-indicators"
    if len(confirms) < min(required, observed):
        return False, f"structure={len(confirms)}/{min(required, observed)}"
    return True, f"structure={','.join(confirms[:4])}"

def _strong_follow_tp_extension(side: str, intel: dict | None, cfg: dict, hold_min_conf: float | None = None) -> tuple[bool, str]:
    if not bool(cfg.get("holdWinners", True)):
        return False, ""
    if not isinstance(intel, dict):
        return False, ""
    if isinstance(intel.get("intel"), dict):
        intel = intel.get("intel")
    current_side = str(side or "").upper()
    if current_side not in ("LONG", "SHORT"):
        return False, ""
    sig = str(intel.get("signal", "WAIT")).upper()
    if sig != current_side:
        return False, ""
    conf = float(intel.get("confidence", 0.0) or 0.0)
    base_min_conf = float(hold_min_conf) if hold_min_conf is not None else float(cfg.get("holdMinConfidence", 0.72) or 0.72)
    min_conf = max(base_min_conf, float(cfg.get("tpExtendMinConfidence", 0.78) or 0.78))
    if conf < min_conf:
        return False, ""
    px = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    long_score = float(px.get("longScore", 0.0) or 0.0)
    short_score = float(px.get("shortScore", 0.0) or 0.0)
    score_gap = (long_score - short_score) if current_side == "LONG" else (short_score - long_score)
    min_gap = float(cfg.get("tpExtendMinScoreGap", 1.2) or 1.2)
    if score_gap < min_gap:
        return False, ""
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    mom = float(ex.get("momentumPct", 0.0) or 0.0)
    aligned = (current_side == "LONG" and mom > 0) or (current_side == "SHORT" and mom < 0)
    if not aligned:
        return False, ""
    return True, f"{current_side} c={conf:.3f} gap={score_gap:.1f} mom={mom:.3f}%"

async def _extend_tp_sl_levels(side: str, mark: float, entry: float, old_tp: float, old_sl: float, cfg: dict, symbol: str = None, hold_trail_pct: float | None = None) -> tuple[float, float, bool]:
    current_side = str(side or "").upper()
    mark = float(mark or 0.0)
    entry = float(entry or 0.0)
    old_tp = float(old_tp or 0.0)
    old_sl = float(old_sl or 0.0)
    if current_side not in ("LONG", "SHORT") or mark <= 0 or entry <= 0 or old_tp <= 0 or old_sl <= 0:
        return old_tp, old_sl, False
    
    # Get TradingView guidance if enabled
    tv_guidance = None
    if symbol and cfg.get("tradingviewEnabled", False):
        try:
            tv_client = get_tv_mcp(cfg)
            tv_guidance = await async_get_position_guidance(tv_client, symbol, current_side)
        except Exception as exc:
            _autotrade_log(f"[TradingView] TP extend guidance error {symbol}: {exc}")

    # Check if TradingView suggests TP extension
    tv_extend = False
    if tv_guidance and should_extend_tp_with_tradingview(current_side, tv_guidance, cfg, symbol):
        tv_extend = True
        tv_extension_pct = get_tradingview_tp_extension_pct(current_side, tv_guidance, cfg)
    
    _trail_base = float(hold_trail_pct) if hold_trail_pct is not None else float(cfg.get("holdTrailPct", 0.25) or 0.25)
    tp_step_pct = max(0.05, float(cfg.get("tpExtendStepPct", _trail_base)) or _trail_base)
    sl_trail_pct = max(0.05, _trail_base)
    base_tp_pct = max(0.05, float(cfg.get("takeProfitPct", 1.8) or 1.8))
    max_tp_pct = max(base_tp_pct, float(cfg.get("tpExtendMaxPctFromEntry", max(base_tp_pct * 2.5, base_tp_pct + 1.2)) or base_tp_pct))
    
    # Apply TradingView TP extension if enabled
    if tv_extend:
        tp_step_pct += tv_extension_pct
    
    if current_side == "LONG":
        cap_tp = entry * (1 + max_tp_pct / 100.0)
        candidate_tp = min(max(old_tp, mark * (1 + tp_step_pct / 100.0)), cap_tp)
        candidate_sl = max(old_sl, mark * (1 - sl_trail_pct / 100.0), entry)
        return candidate_tp, candidate_sl, candidate_tp > old_tp + 1e-12 or candidate_sl > old_sl + 1e-12
    cap_tp = entry * (1 - max_tp_pct / 100.0)
    candidate_tp = max(min(old_tp, mark * (1 - tp_step_pct / 100.0)), cap_tp)
    candidate_sl = min(old_sl, mark * (1 + sl_trail_pct / 100.0), entry)
    return candidate_tp, candidate_sl, candidate_tp < old_tp - 1e-12 or candidate_sl < old_sl - 1e-12

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

async def _live_multi_profit_lock_manage(cfg: dict) -> bool:
    """Manage ALL open positions via liveProfitLocks (single source of truth).

    Phase 0: entry validation after restart (idempotent per lock).
    Phase 1: metadata update (no intel needed).
    Phase 2: fast price-level checks (SL/breakeven).
    Phase 3: concurrent intel dispatch.
    Phase 4: intel-dependent decisions (reversal, TV exit, TP extension, profit lock, close).
    """
    _cycle_start = time.monotonic()
    key = os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_API_SECRET")
    base = _binance_base()
    if not key or not secret:
        return False
    rows = await _main()._pick_live_orphan_positions(key, secret, base)
    locks = AUTO_TRADE.get("liveProfitLocks")
    if not isinstance(locks, dict):
        locks = {}
    changed = False
    now = int(time.time())
    live_keys = set()
    tp_max = max(0.15, float(cfg.get("tpTargetMaxUsdt", 2.0) or 2.0))
    hold_min_conf = float(cfg.get("holdMinConfidence", 0.72) or 0.72)
    close_decisions: list[str] = []  # Req 9
    _closed_symbols: set[str] = set()  # Double-close guard: track already-closed positions this cycle

    # ── Phase 0: entry validation after restart (merged from System A) ────────
    # Runs once per process lifetime per lock (idempotent after session_validated=True).
    if AUTO_TRADE.get("_snapshot_loaded_at"):
        _row_lookup: dict[str, dict] = {}
        for p in rows:
            _rk = _live_lock_key(str(p.get("symbol", "")).upper(), str(p.get("side", "")).upper())
            _row_lookup.setdefault(_rk, p)
        for p in rows:
            sym = str(p.get("symbol", "")).upper()
            side = str(p.get("side", "")).upper()
            k = _live_lock_key(sym, side)
            st = locks.get(k)
            if not isinstance(st, dict) or st.get("session_validated"):
                continue
            try:
                live_entry: float | None = None
                matched_row = _row_lookup.get(k)
                if matched_row is not None:
                    live_entry = float(matched_row.get("entryMark", 0.0) or 0.0) or None
                if live_entry is None:
                    live_rows = await _main()._pick_live_orphan_positions(key, secret, base)
                    for row in (live_rows or []):
                        if (
                            str(row.get("symbol", "") or "").upper() == sym
                            and str(row.get("side", "") or "").upper() == side
                        ):
                            live_entry = float(row.get("entryMark", 0.0) or 0.0) or None
                            break
                stored_entry = float(st.get("entryMark", 0.0) or 0.0)
                if live_entry is None or live_entry <= 0:
                    st["session_validated"] = True
                    locks[k] = st
                    _autotrade_log(f"[Guardian] entry validation: {sym} {side} no live position — retained lock")
                    continue
                if stored_entry > 0:
                    drift = abs(live_entry - stored_entry) / stored_entry
                    if drift > 0.0001:
                        tp_pct = float(st.get("entryTPPct", cfg.get("takeProfitPct", 1.8)))
                        sl_pct = float(st.get("entrySLPct", cfg.get("stopLossPct", 0.9)))
                        new_tp, new_sl = _calc_tp_sl_prices(side, live_entry, tp_pct, sl_pct)
                        _autotrade_log(
                            f"[Guardian] entry refresh after restart: {sym} {side} "
                            f"entry {stored_entry:.6f}→{live_entry:.6f} "
                            f"TP {float(st.get('tp',0)):.6f}→{new_tp:.6f} "
                            f"SL {float(st.get('sl',0)):.6f}→{new_sl:.6f}"
                        )
                        st["entryMark"] = round(live_entry, 10)
                        st["tp"] = round(new_tp, 10)
                        st["sl"] = round(new_sl, 10)
                st["session_validated"] = True
                locks[k] = st
            except Exception as exc:
                _autotrade_log(f"[Guardian] entry validation failed (will retry): {exc}")

    # ── Phase 1: metadata update (no intel needed) ───────────────────────────
    for p in rows:
        sym = str(p.get("symbol", "")).upper()
        side = str(p.get("side", "")).upper()
        upnl = float(p.get("unRealizedProfit", 0.0) or 0.0)
        mark = float(p.get("markPrice", 0.0) or 0.0)
        entry = float(p.get("entryMark", 0.0) or 0.0) or mark
        notional = abs(float(p.get("notionalUsdtApprox", 0.0) or 0.0))
        fee_min_capture = _fee_edge_min_net_usdt(cfg, 0.0, notional)
        k = _live_lock_key(sym, side)
        live_keys.add(k)
        st = locks.get(k, {"armed": False, "peak": upnl, "lockUsdt": 0.0, "updatedAt": now})
        st["symbol"] = sym
        st["side"] = side
        st["qty"] = round(float(p.get("qty", 0.0) or 0.0), 10)
        st["leverage"] = _position_display_leverage(sym, cfg, p.get("leverage"))
        st["markPrice"] = round(float(mark), 10)
        st["notionalUsdtApprox"] = round(float(notional), 6)
        st["unRealizedProfit"] = round(float(upnl), 6)
        if entry > 0 and not st.get("entryMark"):
            st["entryMark"] = round(float(entry), 10)
        if not isinstance(st.get("entrySnapshot"), dict):
            st["entrySnapshot"] = _entry_snapshot_from_intel(sym, side, _last_decision_intel(sym, max_age_sec=30))
        # Add per-symbol effective profile to the entry snapshot (idempotent).
        if isinstance(st.get("entrySnapshot"), dict):
            _eff_snap = _effective_tp_sl(sym, cfg, _last_decision_intel(sym, max_age_sec=30))
            _eff_prof = _symbol_effective_profile(sym, cfg)
            st["entrySnapshot"].setdefault("effectiveTP", _eff_snap["tpPct"])
            st["entrySnapshot"].setdefault("effectiveSL", _eff_snap["slPct"])
            st["entrySnapshot"].setdefault("effectiveCap", _eff_snap["notionalCapUsdt"])
            st["entrySnapshot"].setdefault("profitLockTrigger", _eff_snap["profitLockTriggerUsdt"])
            st["entrySnapshot"].setdefault("volatilityTier", _eff_snap.get("tier", "med"))
            st["entrySnapshot"].setdefault("volatilityScore", _eff_snap.get("volatilityScore", 0.0))
            st["entrySnapshot"].setdefault("positionSizeMult", float(_eff_prof.get("position_size_mult", 1.0) or 1.0))
            st["entrySnapshot"].setdefault("entryOffsetBps", float(_eff_prof.get("entry_offset_bps", 0.0) or 0.0))
            # Autotuner: snapshot active params at position open (idempotent).
            if "params_at_entry" not in st["entrySnapshot"]:
                try:
                    from trading.symbol_autotuner import snapshot_active_params
                    st["entrySnapshot"]["params_at_entry"] = snapshot_active_params(sym, _eff_snap)
                except Exception:
                    pass
        guard_entry = float(st.get("entryMark", entry) or entry)

        if guard_entry > 0 and (not st.get("tp") or not st.get("sl")):
            # Use the per-symbol effective TP/SL (scales with volatility tier).
            eff_g = _effective_tp_sl(sym, cfg, _last_decision_intel(sym, max_age_sec=30))
            tp, sl = _calc_tp_sl_prices(
                side,
                guard_entry,
                float(eff_g["tpPct"]),
                float(eff_g["slPct"]),
            )
            st["tp"] = round(float(tp), 10)
            st["sl"] = round(float(sl), 10)
            st["entryTPPct"] = float(eff_g["tpPct"])
            st["entrySLPct"] = float(eff_g["slPct"])
            st["entryVolatilityTier"] = eff_g.get("tier", "med")

        # Cache per-row computed values as temp keys; cleaned in Phase 4.
        lock_policy_ph1 = _profit_lock_policy(cfg, max(float(st.get("peak", upnl) or upnl), upnl), sym, _last_decision_intel(sym, max_age_sec=30))
        lk_trigger_ph1 = max(float(lock_policy_ph1.get("trigger", 0.0) or 0.0), fee_min_capture * 1.35)
        bk_floor_ph1 = max(0.03, float(cfg.get("profitLockBreakevenFloorUsdt", 0.08) or 0.08), notional * float(cfg.get("profitLockFeeBufferRate", 0.0015) or 0.0015), fee_min_capture)
        bk_trigger_ph1 = max(bk_floor_ph1 * 1.5, min(lk_trigger_ph1, float(cfg.get("profitLockBreakevenTriggerUsdt", 0.16) or 0.16)))
        payoff_guard_ph1 = _recent_payoff_loss_guard(cfg, sym)
        st["peak"] = max(float(st.get("peak", upnl) or upnl), upnl)
        st["updatedAt"] = now
        # Guardian stats for autotuner outcome tracking
        if "guardianStats" not in st:
            st["guardianStats"] = {
                "openedAt": now,
                "peakProfitUsdt": round(float(st.get("peak", 0.0) or 0.0), 6),
                "holdWinnerActivated": 0,
                "tpExtensionCount": 0,
                "notionalUsdt": round(float(notional), 6),
            }
        gs = st.get("guardianStats", {})
        if isinstance(gs, dict):
            gs["peakProfitUsdt"] = round(max(float(gs.get("peakProfitUsdt", 0.0) or 0.0), float(st.get("peak", 0.0) or 0.0)), 6)
            gs["notionalUsdt"] = round(float(notional), 6)
            gs["updatedAt"] = now
        if st["peak"] >= bk_trigger_ph1:
            st["breakevenGuardArmed"] = True
            st["breakevenFloorUsdt"] = round(float(bk_floor_ph1), 6)
        if bool(payoff_guard_ph1.get("active")) and upnl <= -float(payoff_guard_ph1.get("maxLossUsdt", 0.0) or 0.0):
            st["payoffLossGuard"] = payoff_guard_ph1
        st["_lk_trigger"] = lk_trigger_ph1
        st["_bk_floor"] = bk_floor_ph1
        st["_fee_min"] = fee_min_capture
        st["_lock_policy_lockUsdt"] = float(lock_policy_ph1.get("lockUsdt", 0.0) or 0.0)
        live_keys.add(k)
        locks[k] = st

    # ── Phase 2: fast price-level checks (no intel needed) ───────────────────
    _pending_closes: list[tuple[str, str, str, str, float]] = []  # (sym, side, reason, mark_str, mark_val)
    for p in list(rows):
        sym = str(p.get("symbol", "")).upper()
        side = str(p.get("side", "")).upper()
        k = _live_lock_key(sym, side)
        st = locks.get(k)
        if not isinstance(st, dict):
            continue
        mark = float(p.get("markPrice", 0.0) or 0.0)
        upnl = float(p.get("unRealizedProfit", 0.0) or 0.0)
        tp = float(st.get("tp", 0.0) or 0.0)
        sl = float(st.get("sl", 0.0) or 0.0)
        bk_floor = float(st.get("_bk_floor", 0.03))
        if mark > 0 and tp > 0 and sl > 0:
            hit_sl = (side == "LONG" and mark <= sl) or (side == "SHORT" and mark >= sl)
            hit_be = bool(st.get("breakevenGuardArmed")) and 0 < upnl <= bk_floor
            if hit_be or hit_sl:
                if f"{sym}:{side}" in _closed_symbols:
                    continue
                reason = "BREAKEVEN_GUARD" if hit_be else "LOCAL_SL_HIT"
                _pending_closes.append((sym, side, reason, f"mark={mark:.6f} TP={tp:.6f} SL={sl:.6f}", mark))
                _closed_symbols.add(f"{sym}:{side}")
                _persist_single_lock_before_close(st, cfg)
                locks.pop(k, None)
                changed = True
    if _pending_closes:
        async def _do_close(sym_: str, side_: str):
            await _main()._close_position_one_side(sym_, side_, key, secret, base)
        await asyncio.gather(*[_do_close(s, sd) for s, sd, _, _, _ in _pending_closes])
        for sym, side, reason, mark_str, mark_val in _pending_closes:
            _autotrade_log(f"LIVE multi guard close: {sym} {side} {reason} {mark_str}")
            close_decisions.append(f"{sym}:{side}:{reason}:system=B")
        app_state._LIVE_POSITIONS_CACHE = (0, [])

    # ── Phase 3: concurrent intel dispatch (Req 1) ───────────────────────────
    active_rows = [p for p in rows if _live_lock_key(str(p.get("symbol","")).upper(), str(p.get("side","")).upper()) in locks]
    _n = len(active_rows)
    intel_results: list[dict | None] = [None] * _n
    if _n > 0:
        # Reduced timeout: 4s per symbol, 15s batch max (was 6s/30s)
        batch_timeout = min(15.0, max(4.0, float(_n * 1.5)))
        async def _safe_intel(sym_: str) -> dict | None:
            try:
                return await asyncio.wait_for(intel_analyze(IntelAnalyzeRequest(symbol=sym_)), timeout=4.0)
            except Exception:
                return None
        try:
            raw = await asyncio.wait_for(
                asyncio.gather(*[_safe_intel(str(p.get("symbol","")).upper()) for p in active_rows]),
                timeout=batch_timeout,
            )
            intel_results = [r if isinstance(r, dict) else None for r in raw]
        except Exception:
            intel_results = [None] * _n
        # Stale-but-available: use previous cycle's intel if current failed
        prev_intel = app_state._GUARDIAN_PREV_INTEL if isinstance(app_state._GUARDIAN_PREV_INTEL, dict) else {}
        for idx, p in enumerate(active_rows):
            if intel_results[idx] is None:
                sym = str(p.get("symbol", "")).upper()
                if sym in prev_intel:
                    intel_results[idx] = prev_intel[sym]
        # Save current intel for next cycle
        new_prev = {}
        for idx, p in enumerate(active_rows):
            if isinstance(intel_results[idx], dict):
                new_prev[str(p.get("symbol", "")).upper()] = intel_results[idx]
        app_state._GUARDIAN_PREV_INTEL = new_prev
    intel_ok = sum(1 for r in intel_results if r is not None)

    # ── Phase 3.5: concurrent TradingView dispatch (if enabled) ──────────────
    _tv_results: dict[str, dict | None] = {}
    if cfg.get("tradingviewEnabled", False):
        try:
            tv_client = get_tv_mcp(cfg)
            async def _safe_tv(sym_: str, side_: str) -> tuple[str, str, dict | None]:
                try:
                    guidance = await async_get_position_guidance(tv_client, sym_, side_)
                    return (sym_, side_, guidance)
                except Exception:
                    return (sym_, side_, None)
            tv_tasks = []
            for p in active_rows:
                _ts = str(p.get("symbol", "")).upper()
                _td = str(p.get("side", "")).upper()
                tv_tasks.append(_safe_tv(_ts, _td))
            if tv_tasks:
                tv_timeout = min(10.0, max(3.0, float(len(tv_tasks) * 2.0)))
                tv_raw = await asyncio.wait_for(
                    asyncio.gather(*tv_tasks, return_exceptions=True),
                    timeout=tv_timeout,
                )
                for r in tv_raw:
                    if isinstance(r, tuple) and len(r) == 3:
                        _tv_results[f"{r[0]}:{r[1]}"] = r[2]
        except Exception:
            pass

    # ── Phase 4: intel-dependent decisions ───────────────────────────────────
    for idx, p in enumerate(active_rows):
        sym = str(p.get("symbol", "")).upper()
        side = str(p.get("side", "")).upper()
        k = _live_lock_key(sym, side)
        st = locks.get(k)
        if not isinstance(st, dict):
            continue
        mark = float(p.get("markPrice", 0.0) or 0.0)
        upnl = float(p.get("unRealizedProfit", 0.0) or 0.0)
        notional = abs(float(p.get("notionalUsdtApprox", 0.0) or 0.0))
        fee_min_capture = float(st.pop("_fee_min", _fee_edge_min_net_usdt(cfg, 0.0, notional)))
        lock_trigger = float(st.pop("_lk_trigger", fee_min_capture * 1.35))
        bk_floor = float(st.pop("_bk_floor", 0.03))
        lock_policy_lock_usdt = float(st.pop("_lock_policy_lockUsdt", 0.0))
        tp = float(st.get("tp", 0.0) or 0.0)
        sl = float(st.get("sl", 0.0) or 0.0)
        guard_entry = float(st.get("entryMark", 0.0) or 0.0)

        intel = intel_results[idx] if idx < len(intel_results) else None
        # Per-symbol Guardian autotune: compute effective holdTrailPct/holdMinConfidence
        _per_sym_eff = None
        try:
            _eff = _effective_tp_sl(sym, cfg, intel)
            if isinstance(_eff, dict) and _eff:
                _per_sym_eff = _eff
        except Exception:
            pass
        strong_follow = False
        weak_now = False
        follow_reason = ""
        reversal_exit = False
        reversal_reason = ""
        tv_early_exit = False
        tv_early_exit_reason = ""
        hit_payoff_loss_guard = False
        if isinstance(intel, dict):
            sig = str(intel.get("signal", "WAIT")).upper()
            conf = float(intel.get("confidence", 0.0) or 0.0)
            ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
            mom = float(ex.get("momentumPct", 0.0) or 0.0)
            aligned = (side == "LONG" and mom > 0) or (side == "SHORT" and mom < 0)
            against = (side == "LONG" and mom <= 0) or (side == "SHORT" and mom >= 0)
            strong_follow, follow_reason = _strong_follow_tp_extension(side, intel, cfg, _per_sym_eff.get("holdMinConfidence") if _per_sym_eff else None)
            if not strong_follow:
                _effective_min_conf = float(_per_sym_eff.get("holdMinConfidence", hold_min_conf)) if _per_sym_eff else hold_min_conf
                strong_follow = (sig == side and conf >= _effective_min_conf and aligned)
                follow_reason = f"{side} c={conf:.3f} mom={mom:.3f}%"
            _effective_min_conf_for_weak = float(_per_sym_eff.get("holdMinConfidence", hold_min_conf)) if _per_sym_eff else hold_min_conf
            weak_now = (sig != side) or (conf < max(0.55, _effective_min_conf_for_weak - 0.08)) or against
            reversal_exit, reversal_reason = _strong_reversal_exit(side, intel, cfg)
            st["lastSignal"] = sig
            st["lastConfidence"] = round(conf, 4)
            st["lastMomentumPct"] = round(mom, 6)
        payoff_guard = _recent_payoff_loss_guard(cfg, sym)
        if guard_entry > 0 and float(st.get("qty", 0.0) or 0.0) > 0:
            qty_abs = abs(float(st.get("qty", 0.0) or 0.0))
            upnl_calc = ((mark - guard_entry) * qty_abs) if side == "LONG" else ((guard_entry - mark) * qty_abs)
            if bool(payoff_guard.get("active")) and upnl_calc <= -float(payoff_guard.get("maxLossUsdt", 0.0) or 0.0):
                hit_payoff_loss_guard = True
                st["payoffLossGuard"] = payoff_guard
        if cfg.get("tradingviewEnabled", False):
            tv_guidance = _tv_results.get(f"{sym}:{side}")
            try:
                if tv_guidance and should_exit_early_with_tradingview(side, tv_guidance, cfg, sym):
                    tv_early_exit = True
                    tv_early_exit_reason = f"TradingView reversal: {tv_guidance.get('recommendation')}"
            except Exception as exc:
                _autotrade_log(f"[TradingView] early exit guidance error {sym}: {exc}")

        if mark > 0 and tp > 0 and sl > 0:
            hit_tp = (side == "LONG" and mark >= tp) or (side == "SHORT" and mark <= tp)
            hit_sl = (side == "LONG" and mark <= sl) or (side == "SHORT" and mark >= sl)
            hit_be = bool(st.get("breakevenGuardArmed")) and 0 < upnl <= bk_floor
            if tv_early_exit:
                if f"{sym}:{side}" not in _closed_symbols:
                    await _main()._close_position_one_side(sym, side, key, secret, base)
                    _closed_symbols.add(f"{sym}:{side}")
                _autotrade_log(f"LIVE multi guard close: {sym} {side} TRADINGVIEW_EARLY_EXIT {tv_early_exit_reason}")
                close_decisions.append(f"{sym}:{side}:TRADINGVIEW_EARLY_EXIT:system=B")
                _persist_single_lock_before_close(st, cfg)
                locks.pop(k, None)
                app_state._LIVE_POSITIONS_CACHE = (0, [])
                changed = True
                continue
            if hit_tp and (strong_follow or _should_hold_winner(side, intel, cfg, _per_sym_eff.get("holdMinConfidence") if _per_sym_eff else None)):
                new_tp, new_sl, extended = await _extend_tp_sl_levels(side, mark, guard_entry, tp, sl, cfg, sym, _per_sym_eff.get("holdTrailPct") if _per_sym_eff else None)
                if not extended:
                    new_sl, new_tp = await _trail_winner_levels(side, mark, sl, tp, float(_per_sym_eff.get("holdTrailPct", cfg.get("holdTrailPct", 0.25)) if _per_sym_eff else cfg.get("holdTrailPct", 0.25)), cfg, sym)
                st["tp"] = round(float(new_tp), 10)
                st["sl"] = round(float(new_sl), 10)
                st["tpExtendedAt"] = now
                st["tpExtensionCount"] = int(st.get("tpExtensionCount", 0) or 0) + 1
                _gs_h = st.get("guardianStats")
                if isinstance(_gs_h, dict):
                    _gs_h["holdWinnerActivated"] = int(_gs_h.get("holdWinnerActivated", 0) or 0) + 1
                    _gs_h["tpExtensionCount"] = int(st.get("tpExtensionCount", 0) or 0)
                    _gs_h["updatedAt"] = now
                _autotrade_log(f"LIVE multi guard extend TP: {sym} {side} TP={new_tp:.6f} SL={new_sl:.6f} · {follow_reason}")
                locks[k] = st
                changed = True
                continue
            if reversal_exit:
                if f"{sym}:{side}" not in _closed_symbols:
                    await _main()._close_position_one_side(sym, side, key, secret, base)
                    _closed_symbols.add(f"{sym}:{side}")
                _autotrade_log(f"LIVE multi guard close: {sym} {side} STRONG_REVERSAL_EXIT {reversal_reason}")
                close_decisions.append(f"{sym}:{side}:STRONG_REVERSAL_EXIT:system=B")
                _persist_single_lock_before_close(st, cfg)
                locks.pop(k, None)
                app_state._LIVE_POSITIONS_CACHE = (0, [])
                changed = True
                continue
            if hit_payoff_loss_guard:
                if f"{sym}:{side}" not in _closed_symbols:
                    await _main()._close_position_one_side(sym, side, key, secret, base)
                    _closed_symbols.add(f"{sym}:{side}")
                _autotrade_log(f"LIVE multi guard close: {sym} {side} PAYOFF_LOSS_GUARD payoff={float(payoff_guard.get('payoffRatio', 0.0) or 0.0):.2f}")
                close_decisions.append(f"{sym}:{side}:PAYOFF_LOSS_GUARD:system=B")
                _persist_single_lock_before_close(st, cfg)
                locks.pop(k, None)
                app_state._LIVE_POSITIONS_CACHE = (0, [])
                changed = True
                continue
            if hit_tp or hit_be or hit_sl:
                if f"{sym}:{side}" not in _closed_symbols:
                    await _main()._close_position_one_side(sym, side, key, secret, base)
                    _closed_symbols.add(f"{sym}:{side}")
                reason = "LOCAL_TP_HIT" if hit_tp else ("BREAKEVEN_GUARD" if hit_be else "LOCAL_SL_HIT")
                _autotrade_log(f"LIVE multi guard close: {sym} {side} {reason} mark={mark:.6f} TP={tp:.6f} SL={sl:.6f}")
                close_decisions.append(f"{sym}:{side}:{reason}:system=B")
                _persist_single_lock_before_close(st, cfg)
                locks.pop(k, None)
                app_state._LIVE_POSITIONS_CACHE = (0, [])
                changed = True
                continue

        if st["peak"] >= lock_trigger and not st.get("armed", False):
            st["armed"] = True
            st["lockUsdt"] = round(max(fee_min_capture, lock_policy_lock_usdt), 6)
            _autotrade_log(f"Profit lock armed: {sym} {side} lock={st['lockUsdt']:.3f} peak={st['peak']:.3f}")

        if st.get("armed") and st["peak"] >= max(fee_min_capture, 0.12):
            retrace_abs_floor = notional * float(cfg.get("profitLockRetraceFloorRatePct", 0.70) or 0.70) / 100.0
            retrace_budget = max(fee_min_capture, float(st.get("lockUsdt",0.0) or 0.0) * 0.55, float(st["peak"]) * 0.55, float(st["peak"]) - max(retrace_abs_floor, 0.03))
            if upnl <= retrace_budget:
                if f"{sym}:{side}" not in _closed_symbols:
                    await _main()._close_position_one_side(sym, side, key, secret, base)
                    _closed_symbols.add(f"{sym}:{side}")
                _autotrade_log(f"LIVE lock close: {sym} {side} RETRACE_BUDGET upnl={upnl:.3f} peak={float(st['peak']):.3f} budget={retrace_budget:.3f}")
                close_decisions.append(f"{sym}:{side}:RETRACE_BUDGET:system=B")
                _persist_single_lock_before_close(st, cfg)
                locks.pop(k, None)
                app_state._LIVE_POSITIONS_CACHE = (0, [])
                changed = True
                continue

        if upnl >= tp_max:
            if f"{sym}:{side}" not in _closed_symbols:
                await _main()._close_position_one_side(sym, side, key, secret, base)
                _closed_symbols.add(f"{sym}:{side}")
            _autotrade_log(f"LIVE lock close: {sym} {side} TARGET_MAX {upnl:.3f} USDT")
            close_decisions.append(f"{sym}:{side}:TARGET_MAX:system=B")
            _persist_single_lock_before_close(st, cfg)
            locks.pop(k, None)
            app_state._LIVE_POSITIONS_CACHE = (0, [])
            changed = True
            continue

        # Req 8: notional-scaled min_profit_lock (replaces hard 0.08 floor).
        weak_signal_rate = max(0.0, float(cfg.get("profitLockWeakSignalRatePct", 0.04) or 0.04))
        min_profit_lock = max(fee_min_capture * 2.0, float(cfg.get("profitLockMinUsdt", 0.10) or 0.10), notional * weak_signal_rate / 100.0)
        if upnl >= min_profit_lock and weak_now and st["peak"] >= lock_trigger:
            if f"{sym}:{side}" not in _closed_symbols:
                await _main()._close_position_one_side(sym, side, key, secret, base)
                _closed_symbols.add(f"{sym}:{side}")
            _autotrade_log(f"LIVE lock close: {sym} {side} WEAK_SIGNAL {upnl:.3f} USDT (min_lock={min_profit_lock:.4f} fee={fee_min_capture:.4f} notional={notional:.2f} rate={weak_signal_rate}%)")
            close_decisions.append(f"{sym}:{side}:WEAK_SIGNAL:system=B")
            _persist_single_lock_before_close(st, cfg)
            locks.pop(k, None)
            app_state._LIVE_POSITIONS_CACHE = (0, [])
            changed = True
            continue
        locks[k] = st

    # Cleanup stale lock states when position disappeared.
    for k in list(locks.keys()):
        if k not in live_keys:
            locks.pop(k, None)
    AUTO_TRADE["liveProfitLocks"] = locks
    _persist_per_symbol_guardian_locks(locks, AUTO_TRADE.get("config"))

    # Req 9: structured cycle timing summary.
    cycle_ms = int((time.monotonic() - _cycle_start) * 1000)
    n_pos = len(rows)
    if n_pos > 0:
        intel_fail = _n - intel_ok
        summary = f"[Guardian/B] cycle={cycle_ms}ms positions={n_pos} intel=ok:{intel_ok}/fail:{intel_fail}"
        if close_decisions:
            summary += f" closes=[{', '.join(close_decisions)}]"
        _autotrade_log(summary)

    return changed


def _persist_per_symbol_guardian_locks(locks: dict, cfg: dict | None = None) -> None:
    """Mirror each ``liveProfitLocks`` entry into its per-symbol storage.

    The global ``AUTO_TRADE["liveProfitLocks"]`` remains the source of truth
    during the transition; this only adds an independent per-symbol copy so
    each symbol's guardian state survives on its own file and a single symbol
    cannot corrupt the shared global snapshot.
    """
    if not isinstance(locks, dict):
        return
    cfg = cfg if isinstance(cfg, dict) else {}
    try:
        from services.config_paths import VAULT_DIR
        from trading.per_symbol_context import PerSymbolContext
        from trading.shared_cache_layer import get_shared_cache

        cache = get_shared_cache(VAULT_DIR)
        for key, lock in locks.items():
            if not isinstance(lock, dict):
                continue
            sym = str(key).split(":")[0].upper().strip()
            if not sym:
                continue
            try:
                ctx = PerSymbolContext(sym, cache, cfg)
                ctx.save_guardian_lock(lock)
            except Exception:
                continue
    except Exception:
        return


def _persist_single_lock_before_close(lock: dict, cfg: dict | None = None) -> None:
    """Persist a single guardian lock to disk BEFORE it's popped from in-memory.

    This ensures _record_learning_trade can read params_at_entry and
    guardianStats from per-symbol storage after the lock is removed.
    """
    if not isinstance(lock, dict):
        return
    cfg = cfg if isinstance(cfg, dict) else {}
    sym = str(lock.get("symbol", "")).upper().strip()
    if not sym:
        return
    try:
        from services.config_paths import VAULT_DIR
        from trading.per_symbol_context import PerSymbolContext
        from trading.shared_cache_layer import get_shared_cache
        cache = get_shared_cache(VAULT_DIR)
        ctx = PerSymbolContext(sym, cache, cfg)
        ctx.save_guardian_lock(lock)
    except Exception:
        pass
