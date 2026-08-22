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
from trading.per_symbol_storage import PerSymbolStorage

AUTO_TRADE = app_state.AUTO_TRADE


def _persist_tv_signal(symbol: str, guidance: dict) -> None:
    """Persist TradingView signal to per-symbol disk so restarts don't lose TV data."""
    sym = str(symbol).upper().strip()
    if not sym:
        return
    try:
        from services.config_paths import VAULT_DIR
        storage = PerSymbolStorage(str(VAULT_DIR), sym)
        signal = {
            "signal": guidance.get("signal", "WAIT"),
            "confidence": guidance.get("confidence", 0.0),
            "strength": guidance.get("strength", 0.0),
            "source": "tradingview-ta",
            "metadata": {
                "recommendation": guidance.get("recommendation", ""),
                "oscillators": guidance.get("oscillators", {}),
                "moving_averages": guidance.get("moving_averages", {}),
                "strength": guidance.get("strength", 0.0),
            },
            "timestamp": guidance.get("timestamp", time.time()),
        }
        storage.save_tv_signal(signal)
    except Exception as exc:
        _autotrade_log(f"[TradingView] persist error {sym}: {exc}")


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

async def _close_position_one_side(*args, reason: str = "LIVE_CUT_LOSING_SIDE", **kwargs):
    return await _main()._close_position_one_side(*args, reason=reason, **kwargs)

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
    min_conf = float(hold_min_conf) if hold_min_conf is not None else float(cfg.get("holdMinConfidence", 0.55))
    if sig != side or conf < min_conf:
        return False
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    mom = float(ex.get("momentumPct", 0.0) or 0.0)
    min_momentum = float(cfg.get("holdMinMomentumPct", 0.15) or 0.15)
    return (side == "LONG" and mom >= min_momentum) or (side == "SHORT" and mom <= -min_momentum)

async def _trail_winner_levels(side: str, mark: float, old_sl: float, old_tp: float, trail_pct: float, cfg: dict = None, symbol: str = None, _tv_guidance: dict | None = None) -> tuple[float, float]:
    t = max(0.05, float(trail_pct))
    
    # Get TradingView guidance for SL trailing if enabled
    if cfg and symbol and cfg.get("tradingviewEnabled", False):
        try:
            # Use pre-fetched guidance from Phase 3.5 if available
            tv_guidance = _tv_guidance
            if tv_guidance is None:
                tv_client = get_tv_mcp(cfg)
                tv_guidance = await async_get_position_guidance(tv_client, symbol, side)
            if tv_guidance and should_trail_sl_with_tradingview(side, tv_guidance, cfg, symbol):
                tv_trail_pct = get_tradingview_sl_trailing_pct(side, tv_guidance, cfg)
                t = max(t, tv_trail_pct)  # Use the larger trail percentage
                _autotrade_log(f"[TradingView] SL trail {symbol}: side={side} trail_pct={tv_trail_pct:.4f} strength={tv_guidance.get('strength', 0)}")
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
    min_conf = float(cfg.get("strongFlipMinConfidence", 0.90) or 0.90)
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


def _tv_conflict_structure_reversal(side: str, intel: dict | None, cfg: dict) -> tuple[bool, str]:
    """Distinguish a true reversal from a pullback when TV conflicts with the
    open position.

    TV early-exit used to fire on any opposing TV signal (strength >= 0.45)
    with no structural check — so a normal pullback inside a still-valid trend
    was closed at the worst moment, right before the bounce. This gate only
    approves the early exit when the *internal* structure also confirms the
    flip (>= tvConflictConfirmationsRequired of trend/MACD/VWAP/BB/RSI agree
    with the opposite side). Otherwise the position is a pullback hold: keep it
    and tighten the stop instead.
    """
    if not isinstance(intel, dict):
        return False, "no-intel"
    if isinstance(intel.get("intel"), dict):
        intel = intel.get("intel")
    current_side = str(side or "").upper()
    if current_side not in ("LONG", "SHORT"):
        return False, "bad-side"
    opposite = "SHORT" if current_side == "LONG" else "LONG"
    px = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    if not px:
        return False, "no-precision"
    required = max(1, int(cfg.get("tvConflictConfirmationsRequired", 2) or 2))
    vwap_min = max(0.0, float(cfg.get("strongFlipVwapConfirmPct", 0.06) or 0.06))
    bb_band = max(0.05, min(0.49, float(cfg.get("strongFlipBbConfirmPctB", 0.42) or 0.42)))
    confirms: list[str] = []

    if opposite == "SHORT":  # reversal down
        if bool(px.get("trendDown")) or bool(px.get("trendDnPartial")):
            confirms.append("trend")
        if bool(px.get("macdBearish")) or bool(px.get("macdCrossDn")):
            confirms.append("macd")
        if "vwapDistancePct" in px:
            try:
                vwap = float(px.get("vwapDistancePct", 0.0) or 0.0)
                if vwap >= vwap_min:
                    confirms.append("vwap")
            except Exception:
                pass
        if "bbPctB" in px:
            try:
                bb = float(px.get("bbPctB", 0.5) or 0.5)
                if bb >= (1.0 - bb_band):
                    confirms.append("bb")
            except Exception:
                pass
        if "rsi14" in px or "rsi14_5m" in px:
            try:
                rsi = float(px.get("rsi14", 50.0) or 50.0)
                rsi5 = float(px.get("rsi14_5m", rsi) or rsi)
                if rsi <= 48.0 or rsi5 <= 48.0:
                    confirms.append("rsi")
            except Exception:
                pass
    else:  # opposite == LONG → reversal up
        if bool(px.get("trendUp")) or bool(px.get("trendUpPartial")):
            confirms.append("trend")
        if bool(px.get("macdBullish")) or bool(px.get("macdCrossUp")):
            confirms.append("macd")
        if "vwapDistancePct" in px:
            try:
                vwap = float(px.get("vwapDistancePct", 0.0) or 0.0)
                if vwap <= -vwap_min:
                    confirms.append("vwap")
            except Exception:
                pass
        if "bbPctB" in px:
            try:
                bb = float(px.get("bbPctB", 0.5) or 0.5)
                if bb <= bb_band:
                    confirms.append("bb")
            except Exception:
                pass
        if "rsi14" in px or "rsi14_5m" in px:
            try:
                rsi = float(px.get("rsi14", 50.0) or 50.0)
                rsi5 = float(px.get("rsi14_5m", rsi) or rsi)
                if rsi >= 52.0 or rsi5 >= 52.0:
                    confirms.append("rsi")
            except Exception:
                pass

    ok = len(confirms) >= required
    if ok:
        return True, f"reversal={','.join(confirms[:4])}"
    return False, (f"pullback-intact({','.join(confirms[:3]) or 'no-confirm'})" if confirms else "pullback-intact(none)")


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
    base_min_conf = float(hold_min_conf) if hold_min_conf is not None else float(cfg.get("holdMinConfidence", 0.55) or 0.55)
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


def _momentum_deceleration_detected(side: str, intel: dict | None, cfg: dict, st: dict) -> tuple[bool, str]:
    """Detect momentum deceleration across guardian cycles."""
    if not bool(cfg.get("swingDecelerationEnabled", True)):
        return False, ""
    if not isinstance(intel, dict):
        return False, ""

    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    mom = float(ex.get("momentumPct", 0.0) or 0.0)
    conf = float(intel.get("confidence", 0.0) or 0.0)

    prev_conf = float(st.get("lastConfidence", 0.0) or 0.0)
    prev_mom = float(st.get("lastMomentumPct", 0.0) or 0.0)

    if prev_conf == 0.0 and prev_mom == 0.0:
        return False, ""

    conf_drop = prev_conf - conf
    significant_conf_drop = conf_drop >= 0.08

    mom_reversed = False
    if side == "LONG":
        mom_reversed = prev_mom > 0.05 and mom < -0.05
    else:
        mom_reversed = prev_mom < -0.05 and mom > 0.05

    mom_decel = False
    if side == "LONG":
        mom_decel = prev_mom > 0.1 and mom < prev_mom * 0.3
    else:
        mom_decel = prev_mom < -0.1 and mom > prev_mom * 0.3

    if not (significant_conf_drop or mom_reversed or mom_decel):
        return False, ""

    parts = []
    if significant_conf_drop:
        parts.append(f"conf_drop={conf_drop:.3f}")
    if mom_reversed:
        parts.append(f"mom_reversed prev={prev_mom:.3f} now={mom:.3f}")
    if mom_decel:
        parts.append(f"mom_decel prev={prev_mom:.3f} now={mom:.3f}")
    return True, " ".join(parts)


def _preemptive_loss_exit(side: str, intel: dict | None, cfg: dict, upnl: float, mark: float, entry: float, sl: float, notional: float, st: dict, decel_reason: str = "") -> tuple[bool, str]:
    """Exit early from a losing position when signal weakens before full SL hit."""
    if not bool(cfg.get("preemptiveLossExitEnabled", True)):
        return False, ""
    if upnl >= 0:
        return False, ""
    if not isinstance(intel, dict):
        return False, ""

    sig = str(intel.get("signal", "WAIT")).upper()
    conf = float(intel.get("confidence", 0.0) or 0.0)
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    mom = float(ex.get("momentumPct", 0.0) or 0.0)

    sig_against = (side == "LONG" and sig in ("SHORT", "WAIT")) or (side == "SHORT" and sig in ("LONG", "WAIT"))
    if not sig_against:
        return False, ""

    min_conf = float(cfg.get("preemptiveLossExitMinConfidence", 0.55) or 0.55)
    if conf < min_conf:
        return False, ""

    sl_dist = abs(entry - sl) if entry > 0 and sl > 0 else 0
    if sl_dist <= 0:
        return False, ""
    qty_abs = abs(float(st.get("qty", 1.0) or 1.0))
    total_sl_loss = sl_dist * qty_abs
    if total_sl_loss <= 0:
        return False, ""
    loss_vs_sl = abs(upnl) / total_sl_loss
    min_entry = float(cfg.get("preemptiveLossExitMinEntryPct", 0.50) or 0.50)
    max_entry = float(cfg.get("preemptiveLossExitMaxEntryPct", 0.85) or 0.85)
    # Evening-volatility guard (Bangkok 16-23): tighten the preemptive exit so
    # a losing position is cut sooner during the high-vol US-overlap window,
    # where avgLoss (~-0.30) has been ~2x avgWin. Lower max_entry => exit before
    # the full SL distance; higher min_confidence => only exit on a real signal
    # flip. Ownership: static risk cap, not a tuner/supervisor write.
    if bool(cfg.get("eveningPreemptiveTighten", False)):
        try:
            import time as _tg
            _bkk_h = (int(_tg.gmtime(int(_tg.time())).tm_hour) + 7) % 24
        except Exception:
            _bkk_h = -1
        _ev_lo = int(cfg.get("eveningSessionHourStart", 16) or 16)
        _ev_hi = int(cfg.get("eveningSessionHourEnd", 23) or 23)
        if _ev_lo <= _bkk_h <= _ev_hi:
            _ev_max = float(cfg.get("eveningPreemptiveMaxEntryPct", 0.75) or 0.75)
            _ev_minconf = float(cfg.get("eveningPreemptiveMinConfidence", 0.60) or 0.60)
            max_entry = min(max_entry, _ev_max)
            min_conf = max(min_conf, _ev_minconf)
    if loss_vs_sl < min_entry or loss_vs_sl > max_entry:
        return False, ""

    px = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    confirms = 0
    if px:
        if side == "LONG":
            if px.get("macdBearish") or px.get("macdCrossDn"):
                confirms += 1
            if px.get("trendDown") or px.get("trendDnPartial"):
                confirms += 1
            try:
                if float(px.get("bbPctB", 0.5)) < 0.35:
                    confirms += 1
            except (TypeError, ValueError):
                pass
            try:
                if float(px.get("rsi14", 50)) < 45:
                    confirms += 1
            except (TypeError, ValueError):
                pass
        else:
            if px.get("macdBullish") or px.get("macdCrossUp"):
                confirms += 1
            if px.get("trendUp") or px.get("trendUpPartial"):
                confirms += 1
            try:
                if float(px.get("bbPctB", 0.5)) > 0.65:
                    confirms += 1
            except (TypeError, ValueError):
                pass
            try:
                if float(px.get("rsi14", 50)) > 55:
                    confirms += 1
            except (TypeError, ValueError):
                pass

    min_confirms = int(cfg.get("preemptiveLossExitMinConfirmations", 2) or 2)
    if confirms < min_confirms:
        return False, ""

    reason = f"sig={sig} c={conf:.3f} mom={mom:.3f}% sl_hit={loss_vs_sl:.0%} confirms={confirms}"
    if decel_reason:
        reason += f" decel=[{decel_reason}]"
    return True, reason


def _try_green_exit(side: str, upnl: float, st: dict, fee_min: float, cfg: dict) -> tuple[bool, str]:
    """Detect when a position bounced from loss to small profit — exit to lock green."""
    if not bool(cfg.get("tryGreenExitEnabled", True)):
        return False, ""

    min_profit = float(cfg.get("tryGreenExitMinProfitUsdt", 0.06) or 0.06)
    max_profit = float(cfg.get("tryGreenExitMaxProfitUsdt", 0.15) or 0.15)
    if upnl < min_profit or upnl > max_profit:
        return False, ""

    lowest = float(st.get("lowestLoss", 0.0) or 0.0)
    min_prior_loss = float(cfg.get("tryGreenExitMinPriorLossUsdt", 0.20) or 0.20)
    if lowest >= -min_prior_loss:
        return False, ""

    loss_range = abs(lowest)
    if loss_range <= 0:
        return False, ""
    recovery_pct = upnl / loss_range
    min_recovery = float(cfg.get("tryGreenExitMinRecoveryPct", 0.70) or 0.70)
    if recovery_pct < min_recovery:
        return False, ""

    last_sig = str(st.get("lastSignal", "WAIT")).upper()
    last_mom = float(st.get("lastMomentumPct", 0.0) or 0.0)
    sig_against = (side == "LONG" and last_sig in ("SHORT", "WAIT")) or (side == "SHORT" and last_sig in ("LONG", "WAIT"))
    mom_against = (side == "LONG" and last_mom < 0) or (side == "SHORT" and last_mom > 0)
    # Require BOTH signal and momentum against — not just one
    if not (sig_against and mom_against):
        return False, ""

    if upnl <= fee_min * 1.5:
        return False, ""

    return True, f"lowest={lowest:.3f} recovery={recovery_pct:.0%} pnl={upnl:.3f} sig={last_sig} mom={last_mom:.3f}%"


# ═══════════════════════════════════════════════════════════════════════
# Improvement 1: Proactive Trail — trail SL + extend TP when in profit
# ═══════════════════════════════════════════════════════════════════════

def _proactive_trail_in_profit(
    side: str, mark: float, entry: float, upnl: float,
    old_tp: float, old_sl: float, notional: float,
    cfg: dict, intel: dict | None, st: dict,
) -> tuple[bool, float, float, str]:
    """Trail SL up and optionally extend TP when signal is strong + position is in profit.

    Unlike the reactive extend (which only fires on hit_tp), this proactively
    adjusts levels every cycle while the position is profitable and the signal
    remains strong.  Returns (changed, new_tp, new_sl, reason).
    """
    if not bool(cfg.get("proactiveTrailEnabled", True)):
        return False, old_tp, old_sl, ""

    side = str(side or "").upper()
    if side not in ("LONG", "SHORT") or mark <= 0 or entry <= 0:
        return False, old_tp, old_sl, ""

    # Must be in profit above the minimum threshold
    min_profit_pct = float(cfg.get("proactiveTrailMinProfitPct", 0.25) or 0.25)
    profit_pct = ((mark - entry) / entry * 100.0) if side == "LONG" else ((entry - mark) / entry * 100.0)
    if profit_pct < min_profit_pct:
        return False, old_tp, old_sl, ""

    # Signal must be strongly aligned
    if not isinstance(intel, dict):
        return False, old_tp, old_sl, ""
    sig = str(intel.get("signal", "WAIT")).upper()
    conf = float(intel.get("confidence", 0.0) or 0.0)
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    mom = float(ex.get("momentumPct", 0.0) or 0.0)

    aligned = (side == "LONG" and sig == "LONG" and mom > 0) or (side == "SHORT" and sig == "SHORT" and mom < 0)
    min_conf = float(cfg.get("holdMinConfidence", 0.55) or 0.55)
    if not (aligned and conf >= min_conf):
        return False, old_tp, old_sl, ""

    # Compute trail step — scale with profit (more profit = tighter trail)
    step_pct = float(cfg.get("proactiveTrailStepPct", 0.12) or 0.12)
    # Dynamic step: tighten as profit grows
    if profit_pct > 1.0:
        step_pct *= 0.8
    if profit_pct > 2.0:
        step_pct *= 0.7

    # Cap SL distance from entry
    max_sl_from_entry = float(cfg.get("proactiveTrailMaxSlFromEntryPct", 0.40) or 0.40)

    new_tp = old_tp
    new_sl = old_sl
    changed = False

    if side == "LONG":
        # Trail SL up: max of current SL, mark - step, entry * (1 - max_sl)
        candidate_sl = max(
            old_sl,
            mark * (1 - step_pct / 100.0),
            entry * (1 - max_sl_from_entry / 100.0),
        )
        # Never trail SL above entry until profit is substantial (> 0.5%)
        if profit_pct < 0.5:
            candidate_sl = min(candidate_sl, entry)
        if candidate_sl > old_sl + 1e-12:
            new_sl = candidate_sl
            changed = True
        # Extend TP slightly to capture more upside
        tp_extend = float(cfg.get("proactiveTrailTpExtendPct", 0.08) or 0.08)
        cap_tp = entry * (1 + max(1.8, profit_pct * 2.5) / 100.0)
        candidate_tp = min(max(old_tp, mark * (1 + tp_extend / 100.0)), cap_tp)
        if candidate_tp > old_tp + 1e-12:
            new_tp = candidate_tp
            changed = True
    else:
        # SHORT — mirror logic
        candidate_sl = min(
            old_sl,
            mark * (1 + step_pct / 100.0),
            entry * (1 + max_sl_from_entry / 100.0),
        )
        if profit_pct < 0.5:
            candidate_sl = max(candidate_sl, entry)
        if candidate_sl < old_sl - 1e-12:
            new_sl = candidate_sl
            changed = True
        tp_extend = float(cfg.get("proactiveTrailTpExtendPct", 0.08) or 0.08)
        cap_tp = entry * (1 - max(1.8, profit_pct * 2.5) / 100.0)
        candidate_tp = max(min(old_tp, mark * (1 - tp_extend / 100.0)), cap_tp)
        if candidate_tp < old_tp - 1e-12:
            new_tp = candidate_tp
            changed = True

    if changed:
        reason = f"profit={profit_pct:.2f}% c={conf:.3f} mom={mom:.3f}% sl={new_sl:.6f} tp={new_tp:.6f}"
        return True, new_tp, new_sl, reason
    return False, old_tp, old_sl, ""


# ═══════════════════════════════════════════════════════════════════════
# Improvement 2: Adaptive Preemptive Exit — smarter dip vs reversal
# ═══════════════════════════════════════════════════════════════════════

def _adaptive_preemptive_exit(
    side: str, intel: dict | None, cfg: dict,
    upnl: float, mark: float, entry: float, sl: float,
    notional: float, st: dict, decel_reason: str = "",
) -> tuple[bool, str]:
    """Improved preemptive exit with adaptive thresholds and dip vs reversal detection.

    Key improvements over _preemptive_loss_exit:
    1. Adaptive min_entry: high-confidence signals can exit earlier (15% of SL)
    2. Dip detection: checks if price is recovering (momentum turning) → skip exit
    3. Volume/candle confirmation: requires more evidence for small losses
    4. Recovery penalty: if UPnL is improving, raise the bar for exit
    """
    if not bool(cfg.get("preemptiveExitAdaptiveEnabled", True)):
        # Fall back to original function
        return _preemptive_loss_exit(side, intel, cfg, upnl, mark, entry, sl, notional, st, decel_reason)

    if upnl >= 0:
        return False, ""
    if not isinstance(intel, dict):
        return False, ""

    sig = str(intel.get("signal", "WAIT")).upper()
    conf = float(intel.get("confidence", 0.0) or 0.0)
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    mom = float(ex.get("momentumPct", 0.0) or 0.0)

    sig_against = (side == "LONG" and sig in ("SHORT", "WAIT")) or (side == "SHORT" and sig in ("LONG", "WAIT"))
    if not sig_against:
        return False, ""

    # ── Adaptive threshold: high confidence → lower min_entry ──
    high_conf_threshold = float(cfg.get("preemptiveExitHighConfThreshold", 0.75) or 0.75)
    high_conf_min_entry = float(cfg.get("preemptiveExitHighConfMinEntryPct", 0.15) or 0.15)
    default_min_entry = float(cfg.get("preemptiveLossExitMinEntryPct", 0.50) or 0.50)
    max_entry = float(cfg.get("preemptiveLossExitMaxEntryPct", 0.85) or 0.85)

    min_entry = high_conf_min_entry if conf >= high_conf_threshold else default_min_entry

    min_conf = float(cfg.get("preemptiveLossExitMinConfidence", 0.55) or 0.55)
    if conf < min_conf:
        return False, ""

    sl_dist = abs(entry - sl) if entry > 0 and sl > 0 else 0
    if sl_dist <= 0:
        return False, ""
    qty_abs = abs(float(st.get("qty", 1.0) or 1.0))
    total_sl_loss = sl_dist * qty_abs
    if total_sl_loss <= 0:
        return False, ""
    loss_vs_sl = abs(upnl) / total_sl_loss
    if loss_vs_sl < min_entry or loss_vs_sl > max_entry:
        return False, ""

    # ── Dip detection: if momentum is recovering, this might be a dip ──
    prev_mom = float(st.get("lastMomentumPct", 0.0) or 0.0)
    mom_turning = False
    if side == "LONG":
        # Was negative, now less negative or positive → recovering
        mom_turning = prev_mom < -0.05 and mom > prev_mom * 0.5
    else:
        mom_turning = prev_mom > 0.05 and mom < prev_mom * 0.5

    if mom_turning:
        # Momentum is recovering — this looks like a dip, not a reversal
        # Require higher evidence to exit
        recovery_penalty = float(cfg.get("preemptiveExitRecoveryPenaltyPct", 0.12) or 0.12)
        min_entry = min_entry + recovery_penalty
        if loss_vs_sl < min_entry:
            return False, ""

    # ── Indicator confirmations ──
    px = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    confirms = 0
    if px:
        if side == "LONG":
            if px.get("macdBearish") or px.get("macdCrossDn"):
                confirms += 1
            if px.get("trendDown") or px.get("trendDnPartial"):
                confirms += 1
            try:
                if float(px.get("bbPctB", 0.5)) < 0.35:
                    confirms += 1
            except (TypeError, ValueError):
                pass
            try:
                if float(px.get("rsi14", 50)) < 45:
                    confirms += 1
            except (TypeError, ValueError):
                pass
        else:
            if px.get("macdBullish") or px.get("macdCrossUp"):
                confirms += 1
            if px.get("trendUp") or px.get("trendUpPartial"):
                confirms += 1
            try:
                if float(px.get("bbPctB", 0.5)) > 0.65:
                    confirms += 1
            except (TypeError, ValueError):
                pass
            try:
                if float(px.get("rsi14", 50)) > 55:
                    confirms += 1
            except (TypeError, ValueError):
                pass

    # Adaptive min_confirmations: small loss needs more evidence
    base_min_confirms = int(cfg.get("preemptiveLossExitMinConfirmations", 2) or 2)
    if loss_vs_sl < 0.40:
        # Small loss — require extra confirmation to avoid premature exit
        min_confirms = base_min_confirms + 1
    else:
        min_confirms = base_min_confirms

    if confirms < min_confirms:
        return False, ""

    reason = f"sig={sig} c={conf:.3f} mom={mom:.3f}% sl_hit={loss_vs_sl:.0%} confirms={confirms} adaptive_min={min_entry:.2f}"
    if mom_turning:
        reason += " [detection: dip-confirmed]"
    if decel_reason:
        reason += f" decel=[{decel_reason}]"
    return True, reason


# ═══════════════════════════════════════════════════════════════════════
# Improvement 3: Swing Peak Detection — close at detected local tops
# ═══════════════════════════════════════════════════════════════════════

def _swing_peak_detection(
    side: str, upnl: float, peak: float, mark: float, entry: float,
    notional: float, cfg: dict, intel: dict | None, st: dict,
) -> tuple[bool, str]:
    """Detect when position is near a local swing peak and should close.

    Uses multi-factor confluence:
    1. UPnL near peak (within 85% of highest seen)
    2. Momentum decelerating or reversed
    3. RSI overbought/oversold from intel
    4. BB near upper/lower band from intel
    5. Signal confidence dropping from previous cycle

    Returns (should_close, reason).
    """
    if not bool(cfg.get("swingPeakDetectionEnabled", True)):
        return False, ""

    side = str(side or "").upper()
    if side not in ("LONG", "SHORT"):
        return False, ""

    # Must be in profit
    min_profit = float(cfg.get("swingPeakMinProfitUsdt", 0.08) or 0.08)
    if upnl < min_profit:
        return False, ""

    # Must have a meaningful peak to compare against
    if peak <= 0 or peak < min_profit:
        return False, ""

    # UPnL should be near the peak (within 85%)
    near_peak_ratio = upnl / peak if peak > 0 else 0
    if near_peak_ratio < 0.80:
        return False, ""

    if not isinstance(intel, dict):
        return False, ""

    px = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    conf = float(intel.get("confidence", 0.0) or 0.0)
    mom = float(ex.get("momentumPct", 0.0) or 0.0)
    prev_conf = float(st.get("lastConfidence", 0.0) or 0.0)
    prev_mom = float(st.get("lastMomentumPct", 0.0) or 0.0)

    # ── Factor 1: Momentum deceleration ──
    mom_decel = False
    decel_threshold = float(cfg.get("swingPeakMomDecelThreshold", 0.30) or 0.30)
    if side == "LONG":
        mom_decel = prev_mom > 0.05 and mom < prev_mom * decel_threshold
    else:
        mom_decel = prev_mom < -0.05 and mom > prev_mom * decel_threshold

    # ── Factor 2: RSI extreme ──
    rsi_extreme = False
    rsi_overbought = float(cfg.get("swingPeakRsiOverbought", 72) or 72)
    rsi_oversold = float(cfg.get("swingPeakRsiOversold", 28) or 28)
    try:
        rsi = float(px.get("rsi14", 50.0) or 50.0)
        if side == "LONG" and rsi >= rsi_overbought:
            rsi_extreme = True
        if side == "SHORT" and rsi <= rsi_oversold:
            rsi_extreme = True
    except (TypeError, ValueError):
        pass

    # ── Factor 3: BB extreme ──
    bb_extreme = False
    bb_upper = float(cfg.get("swingPeakBbUpperPct", 0.88) or 0.88)
    bb_lower = float(cfg.get("swingPeakBbLowerPct", 0.12) or 0.12)
    try:
        bb = float(px.get("bbPctB", 0.5) or 0.5)
        if side == "LONG" and bb >= bb_upper:
            bb_extreme = True
        if side == "SHORT" and bb <= bb_lower:
            bb_extreme = True
    except (TypeError, ValueError):
        pass

    # ── Factor 4: Confidence dropping ──
    conf_dropping = False
    if prev_conf > 0:
        conf_drop = prev_conf - conf
        conf_dropping = conf_drop >= 0.05

    # ── Factor 5: Momentum direction against position ──
    mom_against = False
    if side == "LONG":
        mom_against = mom < -0.02
    else:
        mom_against = mom > 0.02

    # ── Scoring: need at least 2 of 5 factors ──
    factors = [mom_decel, rsi_extreme, bb_extreme, conf_dropping, mom_against]
    score = sum(1 for f in factors if f)

    # Req 10 (SWING_PEAK conservative): momentum deceleration alone is not a
    # swing top — it is usually a mid-trend pause and the price continues.
    # Require the momentum to actually turn AGAINST the position (swing broke),
    # or an extreme RSI/BB reading on top of a strong confidence drop. This
    # stops the guardian selling the exact pause before the next leg.
    require_against = bool(cfg.get("swingPeakRequireMomentumAgainst", True))
    if require_against and not mom_against:
        return False, ""

    # Require more factors if profit is small
    min_score = 2
    if upnl < float(cfg.get("swingPeakMinProfitUsdt", 0.08) or 0.08) * 2:
        min_score = 3

    if score < min_score:
        return False, ""

    parts = []
    if mom_decel:
        parts.append(f"mom_decel prev={prev_mom:.3f} now={mom:.3f}")
    if rsi_extreme:
        parts.append(f"rsi_extreme={rsi:.1f}")
    if bb_extreme:
        parts.append(f"bb_extreme={bb:.2f}")
    if conf_dropping:
        parts.append(f"conf_drop={prev_conf:.3f}→{conf:.3f}")
    if mom_against:
        parts.append(f"mom_against={mom:.3f}%")

    reason = f"swing_peak score={score}/{len(factors)} near_peak={near_peak_ratio:.0%} pnl={upnl:.3f} peak={peak:.3f} [{', '.join(parts)}]"
    return True, reason


async def _extend_tp_sl_levels(side: str, mark: float, entry: float, old_tp: float, old_sl: float, cfg: dict, symbol: str = None, hold_trail_pct: float | None = None, _tv_guidance: dict | None = None) -> tuple[float, float, bool]:
    current_side = str(side or "").upper()
    mark = float(mark or 0.0)
    entry = float(entry or 0.0)
    old_tp = float(old_tp or 0.0)
    old_sl = float(old_sl or 0.0)
    if current_side not in ("LONG", "SHORT") or mark <= 0 or entry <= 0 or old_tp <= 0 or old_sl <= 0:
        return old_tp, old_sl, False
    
    # Get TradingView guidance if enabled
    tv_guidance = _tv_guidance
    if tv_guidance is None and symbol and cfg.get("tradingviewEnabled", False):
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
        _autotrade_log(f"[TradingView] TP extend {symbol}: side={current_side} extension={tv_extension_pct:.4f} strength={tv_guidance.get('strength', 0) if tv_guidance else 0}")
    
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
    hold_min_conf = float(cfg.get("holdMinConfidence", 0.55) or 0.55)
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
        lock_policy_ph1 = _profit_lock_policy(cfg, st["peak"], sym, _last_decision_intel(sym, max_age_sec=30))
        lk_trigger_ph1 = max(float(lock_policy_ph1.get("trigger", 0.0) or 0.0), fee_min_capture * 1.35)
        bk_floor_ph1 = max(0.03, float(cfg.get("profitLockBreakevenFloorUsdt", 0.08) or 0.08), notional * float(cfg.get("profitLockFeeBufferRate", 0.0015) or 0.0015), fee_min_capture)
        bk_trigger_ph1 = max(bk_floor_ph1 * 1.5, min(lk_trigger_ph1, float(cfg.get("profitLockBreakevenTriggerUsdt", 0.16) or 0.16)))
        payoff_guard_ph1 = _recent_payoff_loss_guard(cfg, sym)
        _prev_peak_raw = st.get("peak")
        _prev_peak_f = float(_prev_peak_raw) if _prev_peak_raw is not None else 0.0
        st["peak"] = max(_prev_peak_f, upnl)
        if _prev_peak_f == 0.0 and upnl > 0.0:
            _autotrade_log(f"[Guardian] {sym} {side} peak initialized: {_prev_peak_f:.6f}→{upnl:.6f} (upnl={upnl:.6f})")
        if upnl < 0:
            prev_lowest = float(st.get("lowestLoss", 0.0) or 0.0)
            if prev_lowest == 0.0 or upnl < prev_lowest:
                st["lowestLoss"] = round(upnl, 6)
        elif upnl > float(cfg.get("tryGreenExitMaxProfitUsdt", 0.20) or 0.20):
            st["lowestLoss"] = 0.0
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
        fee_min_local = float(st.get("_fee_min", 0.0) or 0.0)
        if mark > 0 and tp > 0 and sl > 0:
            hit_sl = (side == "LONG" and mark <= sl) or (side == "SHORT" and mark >= sl)
            # Fee-aware breakeven: don't close at a profit smaller than round-trip
            # fee (net loss after fees). Only lock when upnl >= fee floor.
            hit_be = bool(st.get("breakevenGuardArmed")) and fee_min_local <= upnl <= bk_floor
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
        async def _do_close(sym_: str, side_: str, reason_: str):
            await _main()._close_position_one_side(sym_, side_, key, secret, base, reason=reason_)
        await asyncio.gather(*[_do_close(s, sd, r) for s, sd, r, _, _ in _pending_closes])
        for sym, side, reason, mark_str, mark_val in _pending_closes:
            _delete_guardian_lock_file(_live_lock_key(sym, side), cfg)
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
            _tv_timeout_per = min(10.0, max(3.0, float(len(active_rows) * 2.0)))
            async def _safe_tv(sym_: str, side_: str) -> tuple[str, str, dict | None]:
                try:
                    guidance = await asyncio.wait_for(
                        async_get_position_guidance(tv_client, sym_, side_),
                        timeout=_tv_timeout_per,
                    )
                    if guidance is None:
                        _autotrade_log(f"[TradingView] guidance None for {sym_}:{side_}")
                    return (sym_, side_, guidance)
                except asyncio.TimeoutError:
                    _autotrade_log(f"[TradingView] timeout for {sym_}:{side_} after {_tv_timeout_per}s")
                    return (sym_, side_, None)
                except Exception as _tv_exc:
                    _autotrade_log(f"[TradingView] error for {sym_}:{side_}: {_tv_exc}")
                    return (sym_, side_, None)
            tv_tasks = []
            for p in active_rows:
                _ts = str(p.get("symbol", "")).upper()
                _td = str(p.get("side", "")).upper()
                tv_tasks.append(_safe_tv(_ts, _td))
            if tv_tasks:
                tv_raw = await asyncio.gather(*tv_tasks, return_exceptions=True)
                for r in tv_raw:
                    if isinstance(r, tuple) and len(r) == 3:
                        _tv_results[f"{r[0]}:{r[1]}"] = r[2]
                        # Persist TV signal to disk so restarts don't lose TV data
                        try:
                            _sym_u, _side_u, _guid = r[0], r[1], r[2]
                            if _guid and _sym_u:
                                _persist_tv_signal(_sym_u, _guid)
                        except Exception as _tv_persist_err:
                            _autotrade_log(f"[TradingView] persist error {_sym_u}: {_tv_persist_err}")
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

        # ── Minimum holding period guard: skip exit decisions for first 3 min ──
        min_hold_sec = float(cfg.get("guardianMinHoldSec", 180) or 180)
        opened_at = float(st.get("guardianStats", {}).get("openedAt", 0) or 0)
        held_sec = now - opened_at if opened_at > 0 else 9999
        too_new = held_sec < min_hold_sec

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
        mom_decel = False
        mom_decel_reason = ""
        preempt_exit = False
        preempt_reason = ""
        tv_early_exit = False
        tv_early_exit_reason = ""
        tv_pullback_hold = False
        tv_pullback_reason = ""
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
            mom_decel, mom_decel_reason = _momentum_deceleration_detected(side, intel, cfg, st)
            preempt_exit, preempt_reason = _adaptive_preemptive_exit(side, intel, cfg, upnl, mark, guard_entry, sl, notional, st, mom_decel_reason)
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
                # Staleness gate: skip TV data older than tvStaleEntrySec
                if tv_guidance and isinstance(tv_guidance, dict):
                    _tv_ts = float(tv_guidance.get("timestamp", 0) or 0)
                    _tv_age = now - _tv_ts if _tv_ts > 0 else 9999
                    _tv_stale_limit = float(cfg.get("tvStaleEntrySec", 300) or 300)
                    if _tv_age > _tv_stale_limit:
                        tv_guidance = None
                if tv_guidance and should_exit_early_with_tradingview(side, tv_guidance, cfg, sym):
                    # Strong-signal conflict: only close when the internal
                    # structure confirms a TRUE reversal. A pullback inside a
                    # still-valid trend (structure intact) must NOT be closed
                    # at the bottom right before the bounce — hold and tighten
                    # the stop instead.
                    tv_conflict_reversal, tv_conflict_reason = False, "no-check"
                    if bool(cfg.get("tvEarlyExitStructureConfirm", True)):
                        tv_conflict_reversal, tv_conflict_reason = _tv_conflict_structure_reversal(side, intel, cfg)
                    if tv_conflict_reversal or not bool(cfg.get("tvEarlyExitStructureConfirm", True)):
                        tv_early_exit = True
                        tv_early_exit_reason = f"TradingView reversal: {tv_guidance.get('recommendation')} · {tv_conflict_reason}"
                    else:
                        tv_pullback_hold = True
                        tv_pullback_reason = f"TV conflict but structure intact ({tv_conflict_reason}) — pullback hold, tightening SL"
            except Exception as exc:
                _autotrade_log(f"[TradingView] early exit guidance error {sym}: {exc}")

        try_green, try_green_reason = _try_green_exit(side, upnl, st, fee_min_capture, cfg)

        # ── Skip aggressive exit decisions if position is too new (< min_hold) ──
        if too_new:
            # Only allow SL hit and TP hit — skip all signal-based exits
            # Early whipsaw cut: a position held < min_hold that already lost
            # >= earlyWhipsawCutPct of the full SL distance AND never reached a
            # meaningful peak (fee-scale) was entered at bad timing. Waiting for
            # the full SL costs ~2x (whipsaw SL avg -0.94% notional vs cut at
            # 50%). Peak filter keeps genuine recoverable dips (they print green
            # early) and dip-detection stays untouched.
            _ewc_hit = False
            # 2026-08-22: WHIPSAW GRACE PERIOD. Telemetry showed EARLY_WHIPSAW_CUT
            # closed 23 trades at avg 83s with net -3.35 (WR 19%) — the guardian
            # cut at the exact bottom before the bounce. Now we refuse to cut
            # during the first whipGraceSec (default 180s) of a position's life,
            # giving the price room to recover. Only after the grace window may
            # the whipsaw cut fire (and only if it still meets the loss/peak
            # criteria, i.e. it was genuinely bad timing, not a normal dip).
            _whip_grace = float(cfg.get("whipGraceSec", 180) or 180)
            _within_grace = held_sec < _whip_grace
            if bool(cfg.get("earlyWhipsawCutEnabled", True)) and not _within_grace:
                try:
                    _ewc_pct = float(cfg.get("earlyWhipsawCutPct", 0.50) or 0.50)
                    _ewc_peak_min = float(cfg.get("earlyWhipsawCutPeakMinUsdt", 0.05) or 0.05)
                    _ewc_peak = float(st.get("peak", 0.0) or 0.0)
                    _ewc_qty = abs(float(st.get("qty", 0.0) or 0.0))
                    if upnl < 0 and _ewc_peak < _ewc_peak_min and mark > 0 and sl > 0 and guard_entry > 0 and _ewc_qty > 0:
                        _ewc_sl_dist = abs(guard_entry - sl)
                        _ewc_full_loss = _ewc_sl_dist * _ewc_qty
                        if _ewc_full_loss > 0 and abs(upnl) >= _ewc_full_loss * _ewc_pct:
                            _ewc_hit = True
                except Exception:
                    _ewc_hit = False
            if mark > 0 and tp > 0 and sl > 0:
                hit_sl = (side == "LONG" and mark <= sl) or (side == "SHORT" and mark >= sl)
                hit_tp = (side == "LONG" and mark >= tp) or (side == "SHORT" and mark <= tp)
                if hit_sl or hit_tp:
                    if f"{sym}:{side}" not in _closed_symbols:
                        _close_reason = "LOCAL_TP_HIT" if hit_tp else "LOCAL_SL_HIT"
                        _persist_single_lock_before_close(st, cfg)
                        await _main()._close_position_one_side(sym, side, key, secret, base, reason=_close_reason)
                        _closed_symbols.add(f"{sym}:{side}")
                    reason = "LOCAL_TP_HIT" if hit_tp else "LOCAL_SL_HIT"
                    _autotrade_log(f"LIVE multi guard close: {sym} {side} {reason} (held {held_sec:.0f}s < {min_hold_sec:.0f}s) mark={mark:.6f}")
                    close_decisions.append(f"{sym}:{side}:{reason}:system=B")
                    _delete_guardian_lock_file(k, cfg)
                    locks.pop(k, None)
                    app_state._LIVE_POSITIONS_CACHE = (0, [])
                    changed = True
                    continue
                elif _ewc_hit:
                    if f"{sym}:{side}" not in _closed_symbols:
                        _persist_single_lock_before_close(st, cfg)
                        await _main()._close_position_one_side(sym, side, key, secret, base, reason="EARLY_WHIPSAW_CUT")
                        _closed_symbols.add(f"{sym}:{side}")
                    _autotrade_log(f"LIVE multi guard close: {sym} {side} EARLY_WHIPSAW_CUT (held {held_sec:.0f}s < {min_hold_sec:.0f}s) loss={upnl:.4f} peak={float(st.get('peak',0.0)):.6f}")
                    close_decisions.append(f"{sym}:{side}:EARLY_WHIPSAW_CUT:system=B")
                    _delete_guardian_lock_file(k, cfg)
                    locks.pop(k, None)
                    app_state._LIVE_POSITIONS_CACHE = (0, [])
                    changed = True
                    continue
                else:
                    _autotrade_log(f"[Guardian] {sym} {side} held {held_sec:.0f}s < {min_hold_sec:.0f}s — skipping signal exits")
            locks[k] = st
            continue

        # ── Dead zone exit: held too long in stagnant profit + weak signal ──
        # Fee-aware: only exit when profit >= round-trip fee. Closing a stale
        # position at upnl < fee floor converts it into a guaranteed net loss.
        dead_zone_sec = float(cfg.get("deadZoneExitSec", 600) or 600)
        if held_sec >= dead_zone_sec and weak_now and upnl >= fee_min_capture and upnl < lock_trigger:
            if f"{sym}:{side}" not in _closed_symbols:
                _persist_single_lock_before_close(st, cfg)
                await _main()._close_position_one_side(sym, side, key, secret, base, reason="DEAD_ZONE_TIMEOUT")
                _closed_symbols.add(f"{sym}:{side}")
            _autotrade_log(f"LIVE multi guard close: {sym} {side} DEAD_ZONE_TIMEOUT held={held_sec:.0f}s upnl={upnl:.4f} peak={float(st.get('peak',0.0)):.4f} lock_trigger={lock_trigger:.4f}")
            close_decisions.append(f"{sym}:{side}:DEAD_ZONE_TIMEOUT:system=B")
            _delete_guardian_lock_file(k, cfg)
            locks.pop(k, None)
            app_state._LIVE_POSITIONS_CACHE = (0, [])
            changed = True
            continue

        if mark > 0 and tp > 0 and sl > 0:
            hit_tp = (side == "LONG" and mark >= tp) or (side == "SHORT" and mark <= tp)
            hit_sl = (side == "LONG" and mark <= sl) or (side == "SHORT" and mark >= sl)
            # Fee-aware breakeven guard: never lock at a profit below the fee floor.
            hit_be = bool(st.get("breakevenGuardArmed")) and fee_min_capture <= upnl <= bk_floor
            if tv_early_exit:
                if f"{sym}:{side}" not in _closed_symbols:
                    _persist_single_lock_before_close(st, cfg)
                    await _main()._close_position_one_side(sym, side, key, secret, base, reason="TRADINGVIEW_EARLY_EXIT")
                    _closed_symbols.add(f"{sym}:{side}")
                _autotrade_log(f"LIVE multi guard close: {sym} {side} TRADINGVIEW_EARLY_EXIT {tv_early_exit_reason}")
                close_decisions.append(f"{sym}:{side}:TRADINGVIEW_EARLY_EXIT:system=B")
                _delete_guardian_lock_file(k, cfg)
                locks.pop(k, None)
                app_state._LIVE_POSITIONS_CACHE = (0, [])
                changed = True
                continue
            # 2026-08-22: activate hold/extend on ANY in-profit position (not only
            # when price literally touches TP). Telemetry showed TP targets were
            # set too far (only 7/195 trades hit TP in 7d) so the old
            # `hit_tp and ...` branch never fired -> holdWinnerActivated=0/195 and
            # ~+0.105 USDT/position of peak profit leaked out. Now we also hold
            # when upnl exceeds a small profit floor AND the signal is still
            # aligned (strong_follow / _should_hold_winner), letting the guardian
            # trail SL to breakeven / extend TP on winners that never tap TP.
            _hold_min_profit = float(cfg.get("holdMinProfitUsdt", 0.03) or 0.03)
            _in_profit_hold = upnl > _hold_min_profit
            if (hit_tp or (isinstance(intel, dict) and _in_profit_hold)) and (strong_follow or _should_hold_winner(side, intel, cfg, _per_sym_eff.get("holdMinConfidence") if _per_sym_eff else None)):
                _tv_prefetched = _tv_results.get(f"{sym}:{side}")
                new_tp, new_sl, extended = await _extend_tp_sl_levels(side, mark, guard_entry, tp, sl, cfg, sym, _per_sym_eff.get("holdTrailPct") if _per_sym_eff else None, _tv_guidance=_tv_prefetched)
                if not extended:
                    new_sl, new_tp = await _trail_winner_levels(side, mark, sl, tp, float(_per_sym_eff.get("holdTrailPct", cfg.get("holdTrailPct", 0.25)) if _per_sym_eff else cfg.get("holdTrailPct", 0.25)), cfg, sym, _tv_guidance=_tv_prefetched)
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
            # ── Improvement 1: Proactive trail when in profit + strong signal ──
            if not hit_tp and upnl > 0 and isinstance(intel, dict):
                pt_changed, pt_tp, pt_sl, pt_reason = _proactive_trail_in_profit(
                    side, mark, guard_entry, upnl, tp, sl, notional, cfg, intel, st,
                )
                if pt_changed:
                    st["tp"] = round(float(pt_tp), 10)
                    st["sl"] = round(float(pt_sl), 10)
                    st["proactiveTrailCount"] = int(st.get("proactiveTrailCount", 0) or 0) + 1
                    _autotrade_log(f"LIVE multi guard proactive trail: {sym} {side} TP={pt_tp:.6f} SL={pt_sl:.6f} · {pt_reason}")
                    locks[k] = st
                    changed = True
                    # Don't continue — let other checks (reversal, swing) still evaluate
            # ── Improvement 2: TV pullback hold — tighten SL, do NOT close ──
            # TV is against us but internal structure is still intact (a normal
            # pullback inside the trend). Closing here would sell the exact low
            # before the bounce. Instead lock in what we have by pulling the
            # stop up (LONG) / down (SHORT) to a tight trail off the mark.
            if tv_pullback_hold and not hit_tp and upnl > 0 and mark > 0 and sl > 0:
                pb_trail_pct = max(0.05, float(cfg.get("tvPullbackTrailPct", 0.12) or 0.12))
                if side == "LONG":
                    pb_sl = max(sl, mark * (1 - pb_trail_pct / 100.0), guard_entry if guard_entry > 0 else 0.0)
                    moved = pb_sl > sl + 1e-12
                else:
                    pb_sl = min(sl, mark * (1 + pb_trail_pct / 100.0), guard_entry if guard_entry > 0 else 1e18)
                    moved = pb_sl < sl - 1e-12
                if moved:
                    st["sl"] = round(float(pb_sl), 10)
                    st["tvPullbackTrailCount"] = int(st.get("tvPullbackTrailCount", 0) or 0) + 1
                    _autotrade_log(f"LIVE multi guard pullback tighten: {sym} {side} SL={pb_sl:.6f} · {tv_pullback_reason}")
                    locks[k] = st
                    changed = True
                else:
                    _autotrade_log(f"LIVE multi guard pullback hold: {sym} {side} SL already tight · {tv_pullback_reason}")
                    locks[k] = st
            # ── Green exits first: try profit before loss ──
            if try_green:
                if f"{sym}:{side}" not in _closed_symbols:
                    _persist_single_lock_before_close(st, cfg)
                    await _main()._close_position_one_side(sym, side, key, secret, base, reason="TRY_GREEN_EXIT")
                    _closed_symbols.add(f"{sym}:{side}")
                _autotrade_log(f"LIVE multi guard close: {sym} {side} TRY_GREEN_EXIT {try_green_reason} pnl={upnl:.3f}")
                close_decisions.append(f"{sym}:{side}:TRY_GREEN_EXIT:system=B")
                _delete_guardian_lock_file(k, cfg)
                locks.pop(k, None)
                app_state._LIVE_POSITIONS_CACHE = (0, [])
                changed = True
                continue
            swing_peak, swing_reason = _swing_peak_detection(
                side, upnl, float(st.get("peak", 0.0) or 0.0), mark, guard_entry,
                notional, cfg, intel, st,
            )
            if swing_peak:
                if f"{sym}:{side}" not in _closed_symbols:
                    _persist_single_lock_before_close(st, cfg)
                    await _main()._close_position_one_side(sym, side, key, secret, base, reason="SWING_PEAK_CLOSE")
                    _closed_symbols.add(f"{sym}:{side}")
                _autotrade_log(f"LIVE multi guard close: {sym} {side} SWING_PEAK_CLOSE {swing_reason}")
                close_decisions.append(f"{sym}:{side}:SWING_PEAK_CLOSE:system=B")
                _delete_guardian_lock_file(k, cfg)
                locks.pop(k, None)
                app_state._LIVE_POSITIONS_CACHE = (0, [])
                changed = True
                continue
            if reversal_exit:
                if f"{sym}:{side}" not in _closed_symbols:
                    _persist_single_lock_before_close(st, cfg)
                    await _main()._close_position_one_side(sym, side, key, secret, base, reason="STRONG_REVERSAL_EXIT")
                    _closed_symbols.add(f"{sym}:{side}")
                _autotrade_log(f"LIVE multi guard close: {sym} {side} STRONG_REVERSAL_EXIT {reversal_reason}")
                close_decisions.append(f"{sym}:{side}:STRONG_REVERSAL_EXIT:system=B")
                _delete_guardian_lock_file(k, cfg)
                locks.pop(k, None)
                app_state._LIVE_POSITIONS_CACHE = (0, [])
                changed = True
                continue
            if preempt_exit:
                if f"{sym}:{side}" not in _closed_symbols:
                    _persist_single_lock_before_close(st, cfg)
                    await _main()._close_position_one_side(sym, side, key, secret, base, reason="PREEMPTIVE_LOSS_EXIT")
                    _closed_symbols.add(f"{sym}:{side}")
                _autotrade_log(f"LIVE multi guard close: {sym} {side} PREEMPTIVE_LOSS_EXIT {preempt_reason} pnl={upnl:.3f}")
                close_decisions.append(f"{sym}:{side}:PREEMPTIVE_LOSS_EXIT:system=B")
                _delete_guardian_lock_file(k, cfg)
                locks.pop(k, None)
                app_state._LIVE_POSITIONS_CACHE = (0, [])
                changed = True
                continue
            if hit_payoff_loss_guard:
                if f"{sym}:{side}" not in _closed_symbols:
                    _persist_single_lock_before_close(st, cfg)
                    await _main()._close_position_one_side(sym, side, key, secret, base, reason="PAYOFF_LOSS_GUARD")
                    _closed_symbols.add(f"{sym}:{side}")
                _autotrade_log(f"LIVE multi guard close: {sym} {side} PAYOFF_LOSS_GUARD payoff={float(payoff_guard.get('payoffRatio', 0.0) or 0.0):.2f}")
                close_decisions.append(f"{sym}:{side}:PAYOFF_LOSS_GUARD:system=B")
                _delete_guardian_lock_file(k, cfg)
                locks.pop(k, None)
                app_state._LIVE_POSITIONS_CACHE = (0, [])
                changed = True
                continue
            if hit_tp or hit_be or hit_sl:
                if f"{sym}:{side}" not in _closed_symbols:
                    _close_reason2 = "LOCAL_TP_HIT" if hit_tp else ("BREAKEVEN_GUARD" if hit_be else "LOCAL_SL_HIT")
                    _persist_single_lock_before_close(st, cfg)
                    await _main()._close_position_one_side(sym, side, key, secret, base, reason=_close_reason2)
                    _closed_symbols.add(f"{sym}:{side}")
                reason = "LOCAL_TP_HIT" if hit_tp else ("BREAKEVEN_GUARD" if hit_be else "LOCAL_SL_HIT")
                _autotrade_log(f"LIVE multi guard close: {sym} {side} {reason} mark={mark:.6f} TP={tp:.6f} SL={sl:.6f}")
                close_decisions.append(f"{sym}:{side}:{reason}:system=B")
                _delete_guardian_lock_file(k, cfg)
                locks.pop(k, None)
                app_state._LIVE_POSITIONS_CACHE = (0, [])
                changed = True
                continue

        if st["peak"] >= lock_trigger and not st.get("armed", False):
            st["armed"] = True
            st["lockUsdt"] = round(max(fee_min_capture, lock_policy_lock_usdt), 6)
            _autotrade_log(f"Profit lock armed: {sym} {side} lock={st['lockUsdt']:.3f} peak={st['peak']:.3f}")

        if st.get("armed") and st["peak"] >= max(fee_min_capture, 0.12):
            max_giveback = max(0.01, float(cfg.get("profitLockMaxGivebackUsdt", 0.22) or 0.22))
            retrace_budget = max(fee_min_capture, float(st["peak"]) - max_giveback, float(st["peak"]) * 0.55)
            if upnl <= retrace_budget:
                if f"{sym}:{side}" not in _closed_symbols:
                    _persist_single_lock_before_close(st, cfg)
                    await _main()._close_position_one_side(sym, side, key, secret, base, reason="RETRACE_BUDGET")
                    _closed_symbols.add(f"{sym}:{side}")
                _autotrade_log(f"LIVE lock close: {sym} {side} RETRACE_BUDGET upnl={upnl:.3f} peak={float(st['peak']):.3f} budget={retrace_budget:.3f}")
                close_decisions.append(f"{sym}:{side}:RETRACE_BUDGET:system=B")
                _delete_guardian_lock_file(k, cfg)
                locks.pop(k, None)
                app_state._LIVE_POSITIONS_CACHE = (0, [])
                changed = True
                continue

        if upnl >= tp_max:
            if f"{sym}:{side}" not in _closed_symbols:
                _persist_single_lock_before_close(st, cfg)
                await _main()._close_position_one_side(sym, side, key, secret, base, reason="TARGET_MAX")
                _closed_symbols.add(f"{sym}:{side}")
            _autotrade_log(f"LIVE lock close: {sym} {side} TARGET_MAX {upnl:.3f} USDT")
            close_decisions.append(f"{sym}:{side}:TARGET_MAX:system=B")
            _delete_guardian_lock_file(k, cfg)
            locks.pop(k, None)
            app_state._LIVE_POSITIONS_CACHE = (0, [])
            changed = True
            continue

        # Req 8: notional-scaled min_profit_lock (replaces hard 0.08 floor).
        weak_signal_rate = max(0.0, float(cfg.get("profitLockWeakSignalRatePct", 0.04) or 0.04))
        min_profit_lock = max(fee_min_capture * 2.0, float(cfg.get("profitLockMinUsdt", 0.10) or 0.10), notional * weak_signal_rate / 100.0)
        # Req 9 (WEAK_SIGNAL conservative): don't close winners the instant the
        # signal softens — the price often just paused and continues. Require
        # (a) the position held past weakSignalMinHoldSec (default = guardian
        # min-hold, so a fresh winner isn't stopped out on noise), and
        # (b) peak well above the lock trigger (weakSignalPeakMultiplier x),
        # so only a position that actually had a real run gets taken early.
        weak_min_hold = max(min_hold_sec, float(cfg.get("weakSignalMinHoldSec", min_hold_sec) or min_hold_sec))
        weak_peak_floor = lock_trigger * max(1.0, float(cfg.get("weakSignalPeakMultiplier", 2.0) or 2.0))
        if upnl >= min_profit_lock and weak_now and st["peak"] >= weak_peak_floor and held_sec >= weak_min_hold:
            if f"{sym}:{side}" not in _closed_symbols:
                _persist_single_lock_before_close(st, cfg)
                await _main()._close_position_one_side(sym, side, key, secret, base, reason="WEAK_SIGNAL")
                _closed_symbols.add(f"{sym}:{side}")
            _autotrade_log(f"LIVE lock close: {sym} {side} WEAK_SIGNAL {upnl:.3f} USDT (min_lock={min_profit_lock:.4f} peak_floor={weak_peak_floor:.4f} held={held_sec:.0f}s>={weak_min_hold:.0f}s)")
            close_decisions.append(f"{sym}:{side}:WEAK_SIGNAL:system=B")
            _delete_guardian_lock_file(k, cfg)
            locks.pop(k, None)
            app_state._LIVE_POSITIONS_CACHE = (0, [])
            changed = True
            continue
        locks[k] = st

    # Cleanup stale lock states when position disappeared.
    for k in list(locks.keys()):
        if k not in live_keys:
            _delete_guardian_lock_file(k, cfg)
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


def _delete_guardian_lock_file(lock_key: str, cfg: dict | None = None) -> None:
    """Delete the per-symbol guardian_lock.json file after position is closed."""
    sym = str(lock_key).split(":")[0].upper().strip()
    if not sym:
        return
    cfg = cfg if isinstance(cfg, dict) else {}
    try:
        from services.config_paths import VAULT_DIR
        from trading.per_symbol_context import PerSymbolContext
        from trading.shared_cache_layer import get_shared_cache
        cache = get_shared_cache(VAULT_DIR)
        ctx = PerSymbolContext(sym, cache, cfg)
        ctx.delete_guardian_lock()
    except Exception:
        pass
