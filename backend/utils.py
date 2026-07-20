"""Low-level text / symbol helpers used across the backend."""

from __future__ import annotations

import re

from fastapi import HTTPException


import math
from decimal import Decimal, ROUND_DOWN
import time


def _normalize_symbol(symbol: str) -> str:
    sym = symbol.upper().replace("/", "")
    if not re.fullmatch(r"[A-Z0-9]{6,20}", sym):
        raise HTTPException(status_code=400, detail="Invalid symbol format")
    if not (sym.endswith("USDT") or sym.endswith("BUSD")):
        raise HTTPException(status_code=400, detail="Only USDT/BUSD futures symbols are allowed")
    return sym


def _floor_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    n = math.floor(value / step)
    return float(n * step)


def _format_qty_by_step(value: float, step_str: str) -> str:
    step_dec = Decimal(step_str)
    val_dec = Decimal(str(value))
    q = val_dec.quantize(step_dec, rounding=ROUND_DOWN)
    s = format(q, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _round_to_tick(value: float, tick: float) -> float:
    if tick <= 0:
        return value
    n = math.floor(value / tick)
    return float(n * tick)


def _format_price_by_tick(value: float, tick_str: str) -> str:
    tick_dec = Decimal(tick_str)
    val_dec = Decimal(str(value))
    q = val_dec.quantize(tick_dec, rounding=ROUND_DOWN)
    s = format(q, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _within_no_trade_window(now_local: time.struct_time, windows: list[str]) -> bool:
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

