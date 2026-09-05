"""Binance USD-M futures order execution and position queries."""

from __future__ import annotations

import asyncio
import math
import os
import re
import time
from decimal import Decimal, ROUND_DOWN

import httpx
from fastapi import HTTPException

from exchange.binance_client import (
    _binance_base,
    _data_get,
    _exchange_filters,
    _get_um_client,
    _signed_request,
)
from services import app_state
from services.config_paths import TRADES_LOG_PATH
from trading.state_ops import (
    autotrade_log as _autotrade_log,
    entry_snapshot_from_intel as _entry_snapshot_from_intel,
    last_decision_intel as _last_decision_intel,
)
from trading.risk import _effective_tp_sl, calc_tp_sl_prices as _calc_tp_sl_prices
from trading.learning import (
    _record_learning_trade,
    _record_learning_trade_async,
)

AUTO_TRADE = app_state.AUTO_TRADE
DEFAULT_LEVERAGE = int(os.getenv("DEFAULT_LEVERAGE", "5"))
DEFAULT_MARGIN_TYPE = os.getenv("DEFAULT_MARGIN_TYPE", "ISOLATED").upper()
DEFAULT_TP_PCT = float(os.getenv("DEFAULT_TP_PCT", "1.8"))
DEFAULT_SL_PCT = float(os.getenv("DEFAULT_SL_PCT", "0.8"))


def _normalize_symbol(symbol: str):
    sym = symbol.upper().replace("/", "")
    if not re.fullmatch(r"[A-Z0-9]{6,20}", sym):
        raise HTTPException(status_code=400, detail="Invalid symbol format")
    if not (sym.endswith("USDT") or sym.endswith("BUSD")):
        raise HTTPException(status_code=400, detail="Only USDT/BUSD futures symbols are allowed")
    return sym


def _guardrails(mark_price: float, quantity: float, leverage: int):
    if app_state.RISK["kill_switch"]:
        raise HTTPException(status_code=403, detail="Kill-switch enabled")
    if app_state.DAILY_REALIZED_PNL <= -abs(app_state.RISK["max_daily_loss"]):
        raise HTTPException(status_code=403, detail="Max daily loss reached")
    if leverage > app_state.RISK["max_leverage"]:
        raise HTTPException(status_code=403, detail=f"Leverage {leverage} > limit {app_state.RISK['max_leverage']}")
    notional = mark_price * quantity
    if notional > app_state.RISK["max_notional"]:
        raise HTTPException(status_code=403, detail=f"Notional {notional:.2f} > limit {app_state.RISK['max_notional']}")


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


def _live_lock_key(symbol: str, side: str) -> str:
    return f"{str(symbol).upper()}:{str(side).upper()}"


def _entry_snapshot_for_position(symbol: str, side: str) -> dict:
    sym = str(symbol or "").upper().strip()
    sd = str(side or "").upper().strip()
    locks = AUTO_TRADE.get("liveProfitLocks")
    if isinstance(locks, dict):
        lock = locks.get(_live_lock_key(sym, sd))
        if isinstance(lock, dict) and isinstance(lock.get("entrySnapshot"), dict):
            return dict(lock.get("entrySnapshot") or {})
    return _entry_snapshot_from_intel(sym, sd, _last_decision_intel(sym, max_age_sec=30))


async def fetch_mark_price(symbol: str):
    symbol = _normalize_symbol(symbol)
    res = await _data_get(f"/fapi/v1/premiumIndex?symbol={symbol}")
    if res.status_code >= 400:
        raise HTTPException(status_code=res.status_code, detail=res.text)
    return float(res.json()["markPrice"])

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
    """Count open positions, filtering to USDT pairs only (consistent with _pick_live_orphan_positions)."""
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
            sym = str(p.get("symbol", "") or "").upper().strip()
            if not sym.endswith("USDT"):
                continue
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

async def _estimate_market_slippage_bps(symbol: str, notional_usdt: float, side: str, mark: float) -> tuple[float, float]:
    """Estimate real slippage by walking the order book for the target notional.
    Returns (slippage_bps, weighted_avg_fill_price). Falls back to 0/mark on error.
    """
    if notional_usdt <= 0 or mark <= 0:
        return 0.0, mark
    try:
        res = await _data_get(f"/fapi/v1/depth?symbol={symbol}&limit=50")
        if res.status_code >= 400:
            return 0.0, mark
        data = res.json()
        levels = (data.get("asks", []) if side == "LONG" else data.get("bids", []))
        remaining = float(notional_usdt)
        total_qty = 0.0
        weighted_px = 0.0
        for p, q in levels:
            px = float(p)
            qty = float(q)
            if px <= 0 or qty <= 0:
                continue
            notional_at_level = px * qty
            take = min(remaining, notional_at_level)
            if take <= 0:
                continue
            take_qty = take / px
            weighted_px += px * take_qty
            total_qty += take_qty
            remaining -= take
            if remaining <= 1e-12:
                break
        if total_qty <= 0:
            return 0.0, mark
        avg_fill = weighted_px / total_qty
        # LONG: fill usually above mark; SHORT: fill usually below mark
        if side == "LONG":
            slippage_bps = ((avg_fill - mark) / max(mark, 1e-9)) * 10000.0
        else:
            slippage_bps = ((mark - avg_fill) / max(mark, 1e-9)) * 10000.0
        return max(0.0, slippage_bps), avg_fill
    except Exception:
        return 0.0, mark

def _extract_fill_price(entry: dict | list | None) -> float | None:
    """Extract average fill price from Binance order response.
    Handles both official connector (dict) and raw httpx response (list/dict).
    """
    if entry is None:
        return None
    if isinstance(entry, list):
        if not entry:
            return None
        entry = entry[0]
    if not isinstance(entry, dict):
        return None
    # Try avgPrice first (Binance futures returns this for MARKET orders)
    for key in ("avgPrice", "price", "executedPrice"):
        val = entry.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    # Fallback: calculate from fills
    fills = entry.get("fills")
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
            _autotrade_log(f"Margin multi-assets check failed: {exc}")

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
        # Skip marginType when the current mode already matches — Binance
        # rejects a redundant POST with -4067 even when there are no open
        # orders, which keeps blocking new entries until restart.
        if cur != margin_type:
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
    if cur != margin_type:
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

        # --- Strategy 1: Algo Order API (current Binance standard) ---
        algo_params = {
            "symbol": symbol,
            "side": close_side,
            "type": market_type,
            "algoType": "CONDITIONAL",
            "triggerPrice": stop_price_str,
            "workingType": "MARK_PRICE",
        }
        if hedge_mode and position_side:
            algo_params["positionSide"] = position_side
            algo_params["quantity"] = str(qty)
        else:
            algo_params["closePosition"] = "true"

        # --- Strategy 2: Legacy /fapi/v1/order (fallback) ---
        legacy_primary = {
            "symbol": symbol,
            "side": close_side,
            "type": market_type,
            "stopPrice": stop_price_str,
            "workingType": "MARK_PRICE",
        }
        legacy_fallback = {
            "symbol": symbol,
            "side": close_side,
            "type": limit_type,
            "stopPrice": stop_price_str,
            "price": stop_price_str,
            "timeInForce": "GTC",
            "workingType": "MARK_PRICE",
        }
        if hedge_mode and position_side:
            legacy_primary["positionSide"] = position_side
            legacy_primary["quantity"] = str(qty)
            legacy_fallback["positionSide"] = position_side
            legacy_fallback["quantity"] = str(qty)
        else:
            legacy_primary["closePosition"] = "true"
            legacy_fallback["closePosition"] = "true"

        # Try Algo Order API first
        try:
            return await _signed_request("POST", base, "/fapi/v1/algoOrder", key, secret, algo_params)
        except Exception as e:
            algo_err = str(e)
            _autotrade_log(f"Algo order ({kind}) failed: {algo_err[:200]}; trying legacy endpoint")

        # Fallback: legacy /fapi/v1/order
        client = _get_um_client(key, secret, base)
        try:
            if client:
                return await asyncio.to_thread(client.new_order, **legacy_primary)
            return await _signed_request("POST", base, "/fapi/v1/order", key, secret, legacy_primary)
        except Exception as e:
            txt = str(e)
            if ("-4120" not in txt) and ("Order type not supported" not in txt):
                raise
            if client:
                return await asyncio.to_thread(client.new_order, **legacy_fallback)
            return await _signed_request("POST", base, "/fapi/v1/order", key, secret, legacy_fallback)

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

async def _cancel_all_open_orders(symbol: str, key: str, secret: str, base: str):
    """Cancel all open orders (regular AND algo/conditional) for a symbol.

    Since Binance migrated conditional orders (STOP_MARKET, TAKE_PROFIT_MARKET,
    TRAILING_STOP_MARKET) to the Algo Service on 2025-12-09, the legacy
    ``DELETE /fapi/v1/allOpenOrders`` no longer touches them. Lingering algo
    TP/SL orders on the opposite position side cause Binance error -4067
    ("Position side cannot be changed if there exists open orders") when
    entering the opposite side in hedge mode. We therefore also cancel algo
    orders via ``DELETE /fapi/v1/algoOpenOrders``.
    """
    # 1) Regular open orders (LIMIT / MARKET etc.)
    try:
        client = _get_um_client(key, secret, base)
        if client:
            await asyncio.to_thread(client.cancel_all_open_orders, symbol=symbol)
        else:
            await _signed_request("DELETE", base, "/fapi/v1/allOpenOrders", key, secret, {"symbol": symbol})
    except Exception as exc:
        # Log but don't fail; position close should still be attempted
        print(f"[Cancel Orders] {symbol} regular warning: {exc}")

    # 2) Algo / conditional open orders (STOP_MARKET / TP / TRAILING_STOP)
    #    These are the ones that cause -4067 when left over the opposite side.
    try:
        await _signed_request("DELETE", base, "/fapi/v1/algoOpenOrders", key, secret, {"symbol": symbol})
    except Exception as exc:
        # Endpoint may be unavailable on some account tiers; non-fatal.
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
    close_results = []
    learned_trades = []
    # Cancel all open orders first to avoid Binance -4067 when changing position side
    await _cancel_all_open_orders(symbol, key, secret, base)
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
            close_results.append(await asyncio.to_thread(client.new_order, **payload))
        else:
            close_results.append(await _signed_request("POST", base, "/fapi/v1/order", key, secret, payload))
        if entry > 0 and qty > 0:
            pnl = (close_mark - entry) * qty if pos_side == "LONG" else (entry - close_mark) * qty
            entry_snapshot = _entry_snapshot_for_position(symbol, pos_side)
            learned_trades.append({
                "side": pos_side,
                "entry": entry,
                "exit": close_mark,
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
        await _record_learning_trade_async(symbol, t, "LIVE")
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
