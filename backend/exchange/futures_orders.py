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
from trading.close_reconciler import (
    mark_close_intent as _mark_close_intent,
    pop_close_intent as _pop_close_intent,
    reconcile_symbol_closes as _reconcile_symbol_closes,
)
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

_ALGO_ACCEPTED_STATES = {"NEW", "WORKING", "PARTIALLY_FILLED", "PENDING"}


def _raise_if_algo_rejected(resp, kind: str) -> None:
    """Raise when the algo service rejected a protective order it already ACKed.

    ``POST /fapi/v1/algoOrder`` answers HTTP 200 with ``algoStatus: REJECTED``
    (``rejectReason: "Reduce only reject"``) rather than a 4xx error. Callers
    that only catch exceptions therefore treat a rejected TP/SL as placed and
    the position ends up with no exchange-side protection.
    """
    if not isinstance(resp, dict):
        return
    status = str(resp.get("algoStatus") or resp.get("status") or "").upper()
    reason = str(resp.get("rejectReason") or "").strip()
    try:
        code = int(resp.get("code") or 0)
    except (TypeError, ValueError):
        code = 0
    if code < 0:
        reason = reason or str(resp.get("msg") or "").strip() or f"code {code}"
        status = status or "REJECTED"
    if not status and not reason:
        return
    if status in _ALGO_ACCEPTED_STATES and not reason:
        return
    raise RuntimeError(
        f"algo {kind} not accepted: algoStatus={status or 'unknown'}"
        f" rejectReason={reason or 'unknown'}"
    )


async def _verify_protective_orders(
    symbol: str, key: str, secret: str, base: str, expected: list[tuple[str, float]]
) -> None:
    """Confirm the exchange is actually holding a live order at each level.

    ``expected`` is ``[(kind, trigger_price), ...]``. Catches orders the algo
    service accepted and then rejected asynchronously, which the submit
    response cannot show. Raises when an order that should exist is missing so
    the caller falls back to the in-process guardian lock.

    A failure of the query itself is not treated as missing protection — the
    endpoint is unavailable on some account tiers and that must not block
    entries.
    """
    rows = None
    for attempt in range(2):
        try:
            rows = await _signed_request("GET", base, "/fapi/v1/openAlgoOrders", key, secret, {"symbol": symbol})
            break
        except Exception as exc:  # noqa: BLE001 - verification is best-effort
            if attempt:  # noqa: SIM108
                _autotrade_log(f"Protect verify unavailable for {symbol}: {str(exc)[:160]}")
                return
            await asyncio.sleep(0.4)
    if not isinstance(rows, list):
        return
    live = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        state = str(row.get("algoStatus") or row.get("status") or "").upper()
        if state and state not in _ALGO_ACCEPTED_STATES:
            continue
        try:
            live.append(float(row.get("triggerPrice") or 0.0))
        except (TypeError, ValueError):
            continue
    missing = []
    for kind, price in expected:
        try:
            target = float(price)
        except (TypeError, ValueError):
            continue
        # Match on trigger level within one tick; the exchange rounds to tickSize.
        if not any(abs(v - target) <= max(1e-9, abs(target) * 1e-6) for v in live):
            missing.append(f"{kind}@{price:g}")
    if missing:
        raise RuntimeError(
            f"protective order(s) missing on exchange after submit: {', '.join(missing)}"
            f" (live triggers: {sorted(live) or 'none'})"
        )


async def _place_tp_sl(symbol: str, side: str, qty: float, entry_mark: float, tp_pct: float, sl_pct: float, key: str, secret: str, base: str, tick_size: float, tick_size_str: str, hedge_mode: bool, position_side: str | None):
    close_side = "SELL" if side == "LONG" else "BUY"

    placed_levels: list[tuple[str, float]] = []

    async def _submit_exit_order(kind: str, base_pct: float):
        market_type = "TAKE_PROFIT_MARKET" if kind == "tp" else "STOP_MARKET"
        limit_type = "TAKE_PROFIT" if kind == "tp" else "STOP"

        # -2021 "Order would immediately trigger": the trigger price is too close
        # to (or past) current mark, so Binance rejects the STOP/TP outright and
        # the position could open with NO exchange-side SL.  Retry with a widened
        # stop distance (moves the trigger away from mark) up to a few steps so the
        # protective order lands even during a fast move; the caller's local
        # guardian lock remains as a secondary backstop at the original level.
        _widen_bps = 0.25   # added to distance (%) per retry — wider for low-price coins
        _max_tries = 6

        async def _submit(params: dict, use_algo: bool, kind: str = ""):
            if use_algo:
                resp = await _signed_request("POST", base, "/fapi/v1/algoOrder", key, secret, params)
                _raise_if_algo_rejected(resp, kind or str(params.get("type") or "algo"))
                return resp
            client = _get_um_client(key, secret, base)
            if client:
                return await asyncio.to_thread(client.new_order, **params)
            return await _signed_request("POST", base, "/fapi/v1/order", key, secret, params)

        last_err = None
        for attempt in range(_max_tries):
            cur_pct = base_pct + attempt * _widen_bps
            if kind == "tp":
                cur_price = entry_mark * (1 + cur_pct / 100) if side == "LONG" else entry_mark * (1 - cur_pct / 100)
            else:
                cur_price = entry_mark * (1 - cur_pct / 100) if side == "LONG" else entry_mark * (1 + cur_pct / 100)
            cur_price = _round_to_tick(cur_price, tick_size)
            cur_price_str = _format_price_by_tick(cur_price, tick_size_str)

            # --- Strategy 1: Algo Order API (current Binance standard) ---
            algo_params = {
                "symbol": symbol,
                "side": close_side,
                "type": market_type,
                "algoType": "CONDITIONAL",
                "triggerPrice": cur_price_str,
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
                "stopPrice": cur_price_str,
                "workingType": "MARK_PRICE",
            }
            legacy_fallback = {
                "symbol": symbol,
                "side": close_side,
                "type": limit_type,
                "stopPrice": cur_price_str,
                "price": cur_price_str,
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
                resp = await _submit(algo_params, True, kind)
                placed_levels.append((kind, cur_price))
                return resp
            except Exception as e:
                algo_err = str(e)
                _autotrade_log(f"Algo order ({kind}) failed: {algo_err[:200]}; trying legacy endpoint")

            # Fallback: legacy /fapi/v1/order
            try:
                resp = await _submit(legacy_primary, False, kind)
                placed_levels.append((kind, cur_price))
                return resp
            except Exception as e:
                txt = str(e)
                last_err = e
                if ("-2021" in txt) or ("would immediately trigger" in txt.lower()):
                    if attempt < _max_tries - 1:
                        _autotrade_log(f"{kind} -2021 trigger too close; retry {attempt + 1}/{_max_tries} widen +{attempt * _widen_bps:.2f}%")
                        await asyncio.sleep(0.8)
                        continue
                    raise
                if ("-4120" not in txt) and ("Order type not supported" not in txt):
                    raise
                resp = await _submit(legacy_fallback, False, kind)
                placed_levels.append((kind, cur_price))
                return resp
        raise last_err or RuntimeError(f"{kind} protective order failed after {_max_tries} attempts")

    tp = await _submit_exit_order("tp", tp_pct)
    sl = await _submit_exit_order("sl", sl_pct)
    # Algo rejections can land after the submit response returns, so confirm the
    # exchange is actually holding both levels before the caller treats the
    # position as protected.
    await _verify_protective_orders(symbol, key, secret, base, list(placed_levels))
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

    Endpoint names changed when Binance moved conditional orders (STOP_MARKET,
    TAKE_PROFIT_MARKET, TRAILING_STOP_MARKET) to the Algo Service:
    ``/fapi/v1/allOpenOrders`` and ``/fapi/v1/algoOpenOrders`` both answer 404,
    so cancels raised nothing and every protective order outlived its position.
    The live bulk-cancel routes are ``/fapi/v1/openOrders`` and
    ``/fapi/v1/openAlgoOrders``.

    Lingering algo TP/SL orders on the opposite position side also cause Binance
    -4067 ("Position side cannot be changed if there exists open orders") when
    entering the opposite side in hedge mode.
    """
    # 1) Regular open orders (LIMIT / MARKET etc.)
    try:
        client = _get_um_client(key, secret, base)
        if client:
            await asyncio.to_thread(client.cancel_all_open_orders, symbol=symbol)
        else:
            await _signed_request("DELETE", base, "/fapi/v1/openOrders", key, secret, {"symbol": symbol})
    except Exception as exc:
        # Log but don't fail; position close should still be attempted
        _autotrade_log(f"[Cancel Orders] {symbol} regular warning: {str(exc)[:160]}")

    # 2) Algo / conditional open orders (STOP_MARKET / TP / TRAILING_STOP)
    #    These are the ones that cause -4067 when left over the opposite side.
    try:
        await _signed_request("DELETE", base, "/fapi/v1/openAlgoOrders", key, secret, {"symbol": symbol})
    except Exception as exc:
        # Endpoint may be unavailable on some account tiers; non-fatal.
        _autotrade_log(f"[Cancel Orders] {symbol} algo warning: {str(exc)[:160]}")

async def sweep_orphan_protective_orders(key: str, secret: str, base: str) -> dict:
    """Cancel algo TP/SL left resting on symbols that have no open position.

    A protective order that outlives its position protects nothing: it only sits
    there until the symbol is traded again, where it can immediately close the
    new position (and trips Binance -4067 when flipping side in hedge mode).
    Closing goes through ``_cancel_all_open_orders``, but orders the exchange
    fills or auto-cancels leave their sibling TP behind, so they accumulate.

    Fails closed: any error reading positions cancels nothing.
    """
    try:
        open_algo = await _signed_request("GET", base, "/fapi/v1/openAlgoOrders", key, secret, {})
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)[:200], "cancelledSymbols": []}
    if not isinstance(open_algo, list) or not open_algo:
        return {"ok": True, "cancelledSymbols": [], "ordersChecked": 0}

    try:
        positions = await _signed_request("GET", base, "/fapi/v2/positionRisk", key, secret, {})
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"positionRisk: {str(exc)[:160]}", "cancelledSymbols": []}
    if not isinstance(positions, list):
        return {"ok": False, "error": "positionRisk returned non-list", "cancelledSymbols": []}

    held = {
        str(p.get("symbol") or "").upper()
        for p in positions
        if isinstance(p, dict) and abs(float(p.get("positionAmt", 0) or 0)) > 0
    }

    orphan_symbols: dict[str, int] = {}
    for row in open_algo:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "").upper()
        if not sym or sym in held:
            continue
        orphan_symbols[sym] = orphan_symbols.get(sym, 0) + 1

    attempted: list[str] = []
    for sym in sorted(orphan_symbols):
        await _cancel_all_open_orders(sym, key, secret, base)
        attempted.append(sym)

    # Binance answers 200 on algo DELETE without actually cancelling these
    # orders (observed: algoStatus stays NEW, updateTime unchanged, 52 orders
    # from 09-23 onward). Re-read and report what really cleared instead of
    # logging a success the exchange did not perform.
    cleared: list[str] = []
    stuck: list[str] = []
    if attempted:
        try:
            after = await _signed_request("GET", base, "/fapi/v1/openAlgoOrders", key, secret, {})
            still = {
                str(r.get("symbol") or "").upper()
                for r in after if isinstance(r, dict)
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "ok": False,
                "error": f"post-cancel verify: {str(exc)[:160]}",
                "ordersChecked": len(open_algo),
                "attemptedSymbols": attempted,
            }
        cleared = [s for s in attempted if s not in still]
        stuck = [s for s in attempted if s in still]

    if cleared:
        _autotrade_log(f"Swept orphan protective orders on flat symbols: {', '.join(cleared)}")
    if stuck:
        _autotrade_log(
            f"{sum(orphan_symbols[s] for s in stuck)} orphan protective order(s) on flat symbols "
            f"would not cancel (exchange returns success but order stays open): {', '.join(stuck)}"
        )
    return {
        "ok": True,
        "ordersChecked": len(open_algo),
        "orphanSymbols": orphan_symbols,
        "attemptedSymbols": attempted,
        "cancelledSymbols": cleared,
        "unclearedSymbols": stuck,
    }


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
            order_resp = await asyncio.to_thread(client.new_order, **payload)
        else:
            order_resp = await _signed_request("POST", base, "/fapi/v1/order", key, secret, payload)
        close_results.append(order_resp)
        if entry > 0 and qty > 0:
            # A market close fills at a moving price; the mark we read before
            # submitting is already stale by the time the fill lands. Book the
            # real average fill price so the recorded PnL matches the exchange.
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
    order_ids = sorted(
        {int((o or {}).get("orderId", 0) or 0) for o in close_results if isinstance(o, dict)} - {0}
    )
    for t in learned_trades:
        t["orderIds"] = order_ids
        await _record_learning_trade_async(symbol, t, "LIVE")
    return {"closed": close_results, "orderIds": order_ids}

async def _close_position_one_side(symbol: str, side_to_close: str, key: str, secret: str, base: str, reason: str = "LIVE_CUT_LOSING_SIDE"):
    target = side_to_close.upper()
    if target not in ("LONG", "SHORT"):
        raise HTTPException(status_code=400, detail="side_to_close must be LONG or SHORT")
    # Mark the intent BEFORE any exchange call. An exchange-side TP/SL may have
    # filled already, in which case positionRisk below comes back flat, the
    # loop has nothing to close, and without this marker the close vanishes
    # from the trade log entirely (observed: QNTUSDT 2026-10-03 07:48->07:55,
    # -1.084 USDT over two partial fills, no trades.jsonl entry).
    _mark_close_intent(symbol, reason, target)
    close_start_ms = int(time.time() * 1000)
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
    # Exchange truth beats the arithmetic above: a partially filled MARKET
    # close only realises on what actually filled, and an exchange-triggered
    # SL/TP never appears in `rows` at all. reconcile_symbol_closes is
    # idempotent (fill ids already in trades.jsonl are skipped), so it is safe
    # to always run. Returns None when userTrades could not be read.
    #
    # A reducing MARKET order fills milliseconds after it is accepted, so the
    # first userTrades read almost always comes back empty. Settle for a beat
    # before falling back to arithmetic, otherwise the exact numbers are lost
    # to a race and only the estimate survives.
    reconciled = None
    settle_tries = max(1, int(os.getenv("CLOSE_RECONCILE_SETTLE_TRIES", "3") or 3))
    settle_delay = max(0.0, float(os.getenv("CLOSE_RECONCILE_SETTLE_DELAY_SEC", "0.5") or 0.5))
    for attempt in range(settle_tries):
        try:
            reconciled = await _reconcile_symbol_closes(
                symbol, key, secret, base, reason=reason, since_ms=close_start_ms - 5000
            )
        except Exception as exc:
            _autotrade_log(f"[Close] reconcile failed {symbol} {target}: {exc}")
            break
        if reconciled or reconciled is None or not closed:
            break
        if attempt < settle_tries - 1:
            await asyncio.sleep(settle_delay)

    order_ids = sorted(
        {int((o or {}).get("orderId", 0) or 0) for o in closed if isinstance(o, dict)}
        - {0}
    )
    if reconciled:
        _pop_close_intent(symbol)
    else:
        # Fallback to the pre-existing behaviour when the exchange could not be
        # read or confirmed no fills: record what our own order implies. The
        # order ids anchor the row so a later scan that finally sees the fills
        # attaches them to this trade instead of recording it a second time.
        for t in learned:
            t["orderIds"] = order_ids
            _record_learning_trade(symbol, t, "LIVE")
        if not closed:
            # Nothing left the exchange and no fills appeared: the position was
            # already flat (an exchange-side TP/SL won the race). There is
            # nothing to wait for.
            _pop_close_intent(symbol)
        # else: our order is in flight or the fill has not surfaced yet — keep
        # the intent so the background cycle records the real numbers.
    return {"closed": closed, "reconciled": len(reconciled or []), "orderIds": order_ids}

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
        # notional = usdt_amount × leverage → qty = notional / mark.
        # (2026-09-08 sizing redesign: leverage now scales real exposure so
        # TP/SL land on ±2 USDT; margin used = notional/lev = usdt_amount.)
        quantity = (usdt_amount * max(1, leverage)) / max(mark, 1e-9)
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
        # Record the ACTUAL TP/SL sent to the exchange (pipeline ±2 USDT target
        # path) — not the legacy per-symbol pct (_effective_tp_sl().slPct could
        # be 0.96% ≈ 4 USDT on notional 414 while the order carries 2 USDT).
        # The autotuner reads params_at_entry.slPct/tpPct to tune; a legacy
        # snapshot would make it optimize against levels the order never used.
        _eff_at_open = _effective_tp_sl(symbol, AUTO_TRADE.get("config") or {}, _last_decision_intel(symbol))
        _eff_at_open = dict(_eff_at_open)
        if tp_pct:
            _eff_at_open["tpPct"] = float(tp_pct)
        if sl_pct:
            _eff_at_open["slPct"] = float(sl_pct)
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
        _warn = str(protective.get("warning") or "")[:200]
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
        _autotrade_log(f"LIVE profit lock seeded for {symbol} {side} TP={tp_price:.6f} SL={sl_price:.6f}"
                         + (f" | exchange protection NOT confirmed: {_warn}" if _warn else ""))
    trailing = await _place_trailing_stop(symbol, side, key, secret, base, trailing_stop_pct)
    return {"mode": "live", "entry": entry, "protective": protective, "localGuardian": None, "trailing": trailing, "entrySnapshot": entry_snapshot}
