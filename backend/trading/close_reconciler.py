"""Exchange-truth reconciliation for LIVE position closes.

A LIVE close used to be recorded only when the bot itself submitted the
reducing order AND the exchange still reported the position at that moment.
Two real paths lose the trade that way:

  * the exchange-side STOP_MARKET TP/SL fills first (the -2021 widen-retry fix
    in ``_place_tp_sl`` means those orders actually land now), so when the
    Guardian's local price check fires the position is already flat —
    positionRisk returns no row, nothing gets closed, nothing gets recorded.
    Observed: QNTUSDT 2026-10-03 07:48:51 → 07:55:29, -1.084 USDT over two
    partial fills, no ``trades.jsonl`` entry, lock already popped;
  * the bot's own MARKET close only partially fills, yet what was written is
    the optimistic ``(exit - entry) * full_qty`` figure.

``/fapi/v1/userTrades`` sees both. This module groups reducing fills into close
events and, for every event not already accounted for, emits the same
learning-trade dict the rest of the pipeline already consumes.
"""

from __future__ import annotations

import asyncio
import os
import time

from exchange.binance_client import _binance_base, _signed_request
from services import app_state
from trading.state_ops import autotrade_log as _autotrade_log

AUTO_TRADE = app_state.AUTO_TRADE

# Fill ids older than this many seconds are never re-considered on the next
# scan: the bot runs every cycle, so anything older is already accounted for
# (recorded) or belongs to a session the ledger never saw.
DEFAULT_LOOKBACK_SEC = 1800
# Reducing fills closer together than this belong to the same close event
# (one MARKET order routinely fills in several partials).
_EVENT_GAP_MS = 120_000
_MAX_FETCH = 100
# A close intent that has produced no anchorable fill for this long is dropped
# rather than retried forever. Without this a close that never produced a fill
# (position already flat, order cancelled) would keep its intent alive for the
# life of the process and re-scan userTrades on every cycle.
INTENT_TTL_SEC = max(300, int(os.getenv("CLOSE_INTENT_TTL_SEC", "1800") or 1800))
# How wide a recorded row may differ from an exchange close event and still be
# considered the same trade (so the fill ids get attached to it instead of the
# close being recorded a second time).
_REPAIR_TIME_TOLER_SEC = 120
_REPAIR_QTY_REL_TOL = 0.02


def _intents() -> dict:
    v = AUTO_TRADE.get("closeIntents")
    if not isinstance(v, dict):
        v = {}
        AUTO_TRADE["closeIntents"] = v
    return v


def mark_close_intent(symbol: str, reason: str, side: str = "") -> None:
    """Record that the bot is about to close ``symbol`` for ``reason``.

    Stored on AUTO_TRADE (so it rides along in the snapshot) rather than in a
    module global: a close that outlives a restart must still be reconcilable.
    """
    sym = str(symbol or "").upper().strip()
    if not sym:
        return
    _intents()[sym] = {
        "reason": str(reason or "LIVE_CLOSE"),
        "side": str(side or "").upper(),
        "sinceMs": int(time.time() * 1000),
    }


def pop_close_intent(symbol: str) -> dict:
    sym = str(symbol or "").upper().strip()
    intents = _intents()
    return intents.pop(sym, {}) if isinstance(intents.get(sym), dict) else {}


def peek_close_intent(symbol: str) -> dict:
    v = _intents().get(str(symbol or "").upper().strip())
    return dict(v) if isinstance(v, dict) else {}


async def fetch_recent_fills(
    symbol: str,
    key: str | None,
    secret: str | None,
    base: str,
    start_time_ms: int | None = None,
    limit: int = _MAX_FETCH,
) -> list[dict] | None:
    """Normalised ``/fapi/v1/userTrades`` rows for one symbol.

    Returns ``None`` when the request failed (unknown), ``[]`` when the
    exchange answered and the window genuinely holds no fills. Callers keep a
    pending close intent alive on ``None`` so a transient Binance error cannot
    make a real close look permanently unreconciled.
    """
    if not key or not secret:
        return None
    sym = str(symbol or "").upper().strip()
    if not sym:
        return None
    params: dict = {"symbol": sym, "limit": int(max(1, min(1000, limit or _MAX_FETCH)))}
    if start_time_ms and int(start_time_ms) > 0:
        params["startTime"] = int(start_time_ms)
    try:
        rows = await _signed_request("GET", base, "/fapi/v1/userTrades", key, secret, params)
    except Exception as exc:
        _autotrade_log(f"[CloseReconcile] userTrades {sym} failed: {exc}")
        return None
    if isinstance(rows, dict):
        rows = [rows]
    out: list[dict] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        try:
            ps = str(r.get("positionSide") or "BOTH").upper()
            side = str(r.get("side") or "").upper()
            qty = abs(float(r.get("qty", 0.0) or 0.0))
            price = float(r.get("price", 0.0) or 0.0)
            out.append(
                {
                    "id": int(r.get("id", 0) or 0),
                    "orderId": int(r.get("orderId", 0) or 0),
                    "timeMs": int(r.get("time", 0) or 0),
                    "side": side,
                    "positionSide": ps,
                    "qty": qty,
                    "price": price,
                    "realizedPnl": float(r.get("realizedPnl", 0.0) or 0.0),
                    "commission": float(r.get("commission", 0.0) or 0.0),
                    "commissionAsset": str(r.get("commissionAsset") or ""),
                    # SELL closes a LONG (one-way BOTH or hedge LONG);
                    # BUY closes a SHORT.
                    "reduces": (side == "SELL" and ps in ("BOTH", "LONG"))
                    or (side == "BUY" and ps in ("BOTH", "SHORT")),
                    "closesSide": "LONG" if side == "SELL" else "SHORT",
                }
            )
        except Exception:
            continue
    out.sort(key=lambda x: x["timeMs"])
    return out


_TRADES_CACHE: dict[str, tuple[float, int, set, set]] = {}


def _trades_log_path(symbol: str):
    from services.config_paths import VAULT_DIR

    return VAULT_DIR / "symbols" / str(symbol or "").upper().strip() / "trades.jsonl"


def _recorded_anchors(symbol: str) -> tuple[set, set]:
    """``(fill_ids, order_ids)`` already present in this symbol's trade log.

    Read straight from trades.jsonl instead of PerSymbolStorage.load_trades:
    that helper drops rows it considers corrupt, and a dropped row would make
    the same close look unrecorded forever.

    Order ids are anchored as well as fill ids because the arithmetic fallback
    in ``_close_position_one_side`` records a trade before userTrades has
    surfaced the fill. That row can only be matched by the order that produced
    it; without the order anchor the real close looks unrecorded later and gets
    counted twice.
    """
    import json

    sym = str(symbol or "").upper().strip()
    if not sym:
        return set(), set()
    try:
        path = _trades_log_path(sym)
        st = path.stat() if path.exists() else None
    except Exception:
        st = None
    prev = _TRADES_CACHE.get(sym)
    if st is None:
        # No log on disk yet: the only ids we know are the ones this process
        # just recorded, so the cache is authoritative rather than stale.
        return (set(prev[2]), set(prev[3])) if prev else (set(), set())
    if prev and (prev[0], prev[1]) == (st.st_mtime, st.st_size):
        return prev[2], prev[3]
    fill_ids: set = set()
    order_ids: set = set()
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            if '"fills"' not in line and '"orderIds"' not in line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if not isinstance(row, dict):
                continue
            for fid in row.get("fills") or []:
                try:
                    fill_ids.add(int(fid))
                except Exception:
                    continue
            for oid in row.get("orderIds") or []:
                try:
                    order_ids.add(int(oid))
                except Exception:
                    continue
    except Exception:
        return set(), set()
    _TRADES_CACHE[sym] = (st.st_mtime, st.st_size, fill_ids, order_ids)
    return fill_ids, order_ids


def _recorded_fill_ids(symbol: str) -> set:
    return _recorded_anchors(symbol)[0]


def _remember_anchors(symbol: str, fill_ids=(), order_ids=()) -> None:
    """Cache anchors as recorded so the next scan skips them."""
    sym = str(symbol or "").upper().strip()
    if not sym:
        return
    prev = _TRADES_CACHE.get(sym, (0.0, 0, frozenset(), frozenset()))
    _TRADES_CACHE[sym] = (
        prev[0],
        prev[1],
        set(prev[2]) | {int(i) for i in fill_ids or ()},
        set(prev[3]) | {int(i) for i in order_ids or ()},
    )


def _remember_fill_ids(symbol: str, fill_ids) -> None:
    _remember_anchors(symbol, fill_ids=fill_ids)


def _find_unanchored_row(lines: list[str], trade: dict) -> int | None:
    """Index of the recorded row this close event belongs to, if any.

    Rows written by the arithmetic fallback carry no anchor, so a later scan
    sees the same close as unrecorded. Rather than record it again (which would
    double-count the PnL) the fill ids are attached to the row that is already
    there. Matching is on side, quantity and close time: the fallback always
    records the full position quantity it tried to close and the time it sent
    the order, which is within a second or two of the fill.
    """
    import json

    ts = int(trade.get("closedAt") or 0)
    qty = float(trade.get("qty") or 0.0)
    side = str(trade.get("side") or "").upper()
    if not ts or qty <= 0 or not side:
        return None
    best: int | None = None
    best_dt: int | None = None
    for i, raw in enumerate(lines):
        s = raw.strip()
        if not s:
            continue
        try:
            row = json.loads(s)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        if row.get("fills") or row.get("orderIds"):
            continue
        if str(row.get("side") or "").upper() != side:
            continue
        try:
            rq = float(row.get("qty") or 0.0)
        except Exception:
            continue
        if abs(rq - qty) > max(1e-9, qty * _REPAIR_QTY_REL_TOL):
            continue
        try:
            rts = int(float(row.get("closedAt") or row.get("time") or 0) or 0)
        except Exception:
            continue
        if not rts:
            continue
        dt = abs(rts - ts)
        if dt > _REPAIR_TIME_TOLER_SEC:
            continue
        if best_dt is None or dt < best_dt:
            best, best_dt = i, dt
    return best


def _repair_anchor(symbol: str, trade: dict) -> bool:
    """Attach this close's fill/order ids to a recorded row that lacks them.

    Only anchor metadata is written — the recorded pnl is left untouched.
    Rewriting historical PnL would also invalidate every profile aggregate that
    was derived from it, which is a separate decision from making the ledger
    idempotent.
    """
    import json

    sym = str(symbol or "").upper().strip()
    if not sym:
        return False
    try:
        path = _trades_log_path(sym)
        if not path.exists():
            return False
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return False
    idx = _find_unanchored_row(lines, trade)
    if idx is None:
        return False
    try:
        row = json.loads(lines[idx])
        row["fills"] = [int(i) for i in trade.get("fills") or []]
        row["orderIds"] = [int(i) for i in trade.get("orderIds") or []]
        row["closeSource"] = "EXCHANGE_FILLS"
        row["anchorRepairedAt"] = int(time.time())
        lines[idx] = json.dumps(row, ensure_ascii=False, default=str)
        tmp = path.with_name(path.name + ".anchor.tmp")
        tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
        os.replace(tmp, path)
    except Exception as exc:
        _autotrade_log(f"[CloseReconcile] anchor repair failed {sym}: {exc}")
        return False
    _TRADES_CACHE.pop(sym, None)
    _remember_anchors(sym, trade.get("fills"), trade.get("orderIds"))
    return True


def _close_events(fills: list[dict], since_ms: int) -> list[list[dict]]:
    """Group contiguous reducing fills into close events."""
    events: list[list[dict]] = []
    current: list[dict] = []
    for f in fills:
        if not f.get("reduces"):
            continue
        if since_ms and int(f.get("timeMs", 0) or 0) < int(since_ms):
            continue
        if current and int(f["timeMs"]) - int(current[-1]["timeMs"]) > _EVENT_GAP_MS:
            events.append(current)
            current = []
        if current and current[0]["closesSide"] != f["closesSide"]:
            events.append(current)
            current = []
        current.append(f)
    if current:
        events.append(current)
    return events


def _event_trade(symbol: str, event: list[dict], reason: str) -> dict:
    """Build the learning-trade dict for one close event, from exchange truth."""
    qty = sum(float(f["qty"]) for f in event)
    gross = sum(float(f["realizedPnl"]) for f in event)
    commission = sum(float(f["commission"]) for f in event)
    notional = sum(float(f["price"]) * float(f["qty"]) for f in event)
    side = event[0]["closesSide"]
    # realizedPnl is (exit - entry) * qty for LONG and (entry - exit) * qty for
    # SHORT, so the entry price falls out of the fills without needing a
    # positionRisk row that no longer exists.
    entry = (notional - gross) / qty if side == "LONG" and qty > 0 else (notional + gross) / qty if qty > 0 else 0.0
    exit_px = notional / qty if qty > 0 else 0.0
    return {
        "side": side,
        "entry": round(float(entry), 10),
        "exit": round(float(exit_px), 10),
        "qty": round(float(qty), 10),
        "pnl": round(float(gross), 6),
        "commission": round(float(commission), 6),
        "netPnl": round(float(gross) - float(commission), 6),
        "reason": str(reason or "EXCHANGE_CLOSE"),
        "closedAt": int(max(int(f["timeMs"]) for f in event) / 1000),
        "fills": [int(f["id"]) for f in event],
        "closeSource": "EXCHANGE_FILLS",
    }


async def reconcile_symbol_closes(
    symbol: str,
    key: str | None,
    secret: str | None,
    base: str,
    *,
    reason: str | None = None,
    since_ms: int | None = None,
    lookback_sec: int = DEFAULT_LOOKBACK_SEC,
    record: bool = True,
) -> list[dict] | None:
    """Record every close fill of ``symbol`` that the trade log is missing.

    Returns the trade dicts that were recorded, ``[]`` when there was nothing
    new, and ``None`` when the exchange could not be read. Idempotent: fills
    are matched against the ids already present in ``trades.jsonl``.
    """
    return (await reconcile_symbol_closes_detailed(
        symbol,
        key,
        secret,
        base,
        reason=reason,
        since_ms=since_ms,
        lookback_sec=lookback_sec,
        record=record,
    ) or {}).get("recorded")


async def reconcile_symbol_closes_detailed(
    symbol: str,
    key: str | None,
    secret: str | None,
    base: str,
    *,
    reason: str | None = None,
    since_ms: int | None = None,
    lookback_sec: int = DEFAULT_LOOKBACK_SEC,
    record: bool = True,
    repair: bool = True,
) -> dict | None:
    """``reconcile_symbol_closes`` plus the rows it only re-anchored.

    ``None`` when the exchange could not be read (unknown, retry later).
    Otherwise ``{"recorded": [...], "repaired": [...]}``. ``repaired`` holds
    closes that were already in the trade log through the arithmetic fallback
    and only needed their exchange ids attached, so they are counted once.
    """
    sym = str(symbol or "").upper().strip()
    if not sym or not key or not secret:
        return None
    intent = peek_close_intent(sym)
    eff_reason = reason or (intent.get("reason") if intent else None)
    if since_ms is None:
        floor_ms = int(intent.get("sinceMs") or 0) - _EVENT_GAP_MS
        if not floor_ms:
            floor_ms = int((time.time() - max(60, int(lookback_sec or DEFAULT_LOOKBACK_SEC))) * 1000)
        since_ms = floor_ms
    fills = await fetch_recent_fills(sym, key, secret, base, start_time_ms=int(since_ms))
    if fills is None:
        return None
    if not fills:
        return {"recorded": [], "repaired": []}
    known_fills, known_orders = _recorded_anchors(sym)
    fresh = [
        f
        for f in fills
        if int(f["id"]) not in known_fills
        and not (int(f.get("orderId") or 0) and int(f["orderId"]) in known_orders)
    ]
    events = _close_events(fresh, int(since_ms))
    if not events:
        return {"recorded": [], "repaired": []}
    recorded: list[dict] = []
    repaired: list[dict] = []
    for ev in events:
        trade = _event_trade(sym, ev, eff_reason or "EXCHANGE_CLOSE")
        trade["orderIds"] = sorted({int(f.get("orderId") or 0) for f in ev if int(f.get("orderId") or 0)})
        if repair and _repair_anchor(sym, trade):
            repaired.append(trade)
            _autotrade_log(
                f"[CloseReconcile] anchored recorded {sym} {trade['side']} qty={trade['qty']} "
                f"fills={len(trade['fills'])} (no duplicate written)"
            )
            continue
        if record:
            try:
                from trading.learning import _record_learning_trade

                _record_learning_trade(sym, trade, "LIVE")
            except Exception as exc:
                _autotrade_log(f"[CloseReconcile] record failed {sym}: {exc}")
                continue
            _remember_anchors(sym, trade["fills"], trade["orderIds"])
        recorded.append(trade)
        _autotrade_log(
            f"[CloseReconcile] {sym} {trade['side']} qty={trade['qty']} pnl={trade['pnl']:+.4f} "
            f"commission={trade['commission']:.4f} reason={trade['reason']} fills={len(trade['fills'])}"
        )
    return {"recorded": recorded, "repaired": repaired}


async def reconcile_pending_closes() -> list[dict] | None:
    """Reconcile every symbol the bot tried to close but has not yet recorded."""
    key = os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_API_SECRET")
    base = _binance_base()
    out: list[dict] = []
    intents = _intents()
    for sym in list(intents.keys()):
        try:
            got = await reconcile_symbol_closes(
                sym,
                key,
                secret,
                base,
                since_ms=int((intents.get(sym) or {}).get("sinceMs") or 0) - _EVENT_GAP_MS,
            )
        except Exception as exc:
            _autotrade_log(f"[CloseReconcile] {sym} failed: {exc}")
            continue
        if got is None:
            # Exchange unreadable this cycle — keep the intent and retry.
            continue
        out.extend(got)
        if got:
            pop_close_intent(sym)
            continue
        # [] means the fill has not surfaced in userTrades yet, not that the
        # close produced nothing. Dropping the intent here is what made real
        # closes unrecoverable: the reducing MARKET order fills milliseconds
        # after it is accepted, so the first scan almost always sees nothing.
        # Observed: QNTUSDT 2026-10-03 lost -1.9631 / -1.1120 / -1.0840 this way.
        since_ms = int((intents.get(sym) or {}).get("sinceMs") or 0)
        age = time.time() - (since_ms / 1000.0 if since_ms else time.time())
        if age > INTENT_TTL_SEC:
            pop_close_intent(sym)
            _autotrade_log(
                f"[CloseReconcile] dropped {sym} intent after {int(age)}s with no fill "
                f"(close likely never filled; reason={(intents.get(sym) or {}).get('reason')})"
            )
    return out


async def _symbols_with_realized_pnl(
    key: str | None, secret: str | None, base: str, since_ms: int
) -> list[str] | None:
    """Symbols that realised PnL in the window — one call instead of N.

    ``None`` when the request failed. Used to discover closes that left no
    close intent behind, which intent-driven reconciliation cannot see.
    """
    if not key or not secret:
        return None
    params = {"incomeType": "REALIZED_PNL", "startTime": int(since_ms), "limit": 1000}
    try:
        rows = await _signed_request("GET", base, "/fapi/v1/income", key, secret, params)
    except Exception as exc:
        _autotrade_log(f"[CloseReconcile] income scan failed: {exc}")
        return None
    if isinstance(rows, dict):
        rows = [rows]
    out: list[str] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        sym = str(r.get("symbol") or "").upper().strip()
        if sym and sym not in out:
            out.append(sym)
    return out


async def discover_unrecorded_closes(
    key: str | None,
    secret: str | None,
    base: str,
    *,
    lookback_sec: int = 21600,
    record: bool = True,
    repair: bool = True,
) -> dict:
    """Backfill closes the trade log never received, without any intent.

    ``reconcile_pending_closes`` can only reconcile what the bot still holds an
    intent for, and ``find_untracked_positions`` can only see positions that
    are still open. A close whose intent was dropped is invisible to both, so
    it is found here instead: one ``/fapi/v1/income`` call names the symbols
    that realised PnL, then each is checked against its trade-log anchors.
    """
    if not key or not secret:
        return {"ok": False, "reason": "MISSING_API_KEY", "recorded": [], "repaired": []}
    since_ms = int((time.time() - max(600, int(lookback_sec or 21600))) * 1000)
    syms = await _symbols_with_realized_pnl(key, secret, base, since_ms)
    if syms is None:
        return {"ok": False, "reason": "INCOME_UNREADABLE", "recorded": [], "repaired": []}
    recorded: list[dict] = []
    repaired: list[dict] = []
    for sym in syms:
        try:
            got = await reconcile_symbol_closes_detailed(
                sym,
                key,
                secret,
                base,
                reason="EXCHANGE_CLOSE",
                since_ms=since_ms,
                record=record,
                repair=repair,
            )
        except Exception as exc:
            _autotrade_log(f"[CloseReconcile] discovery {sym} failed: {exc}")
            continue
        if not got:
            continue
        recorded.extend(got.get("recorded") or [])
        repaired.extend(got.get("repaired") or [])
    if recorded or repaired:
        _autotrade_log(
            f"[CloseReconcile] discovery over {len(syms)} symbol(s): "
            f"{len(repaired)} anchored, {len(recorded)} backfilled"
        )
    return {"ok": True, "recorded": recorded, "repaired": repaired, "scanned": len(syms)}


async def find_untracked_positions(key: str | None, secret: str | None, base: str) -> list[dict]:
    """USDT positions live on the exchange that no guardian lock is managing."""
    if not key or not secret:
        return []
    try:
        from trading.live_guardian import _live_lock_key, _pick_live_orphan_positions

        rows = await _pick_live_orphan_positions(key, secret, base)
    except Exception as exc:
        _autotrade_log(f"[CloseReconcile] position scan failed: {exc}")
        return []
    locks = AUTO_TRADE.get("liveProfitLocks")
    locks = locks if isinstance(locks, dict) else {}
    untracked = []
    for r in rows or []:
        sym = str(r.get("symbol", "")).upper()
        side = str(r.get("side", "")).upper()
        if not sym or _live_lock_key(sym, side) in locks:
            continue
        untracked.append(r)
    return untracked


# NOTE: adoption of an exchange position with no guardian lock is NOT done
# here on purpose. live_guardian Phase 1 already creates the lock
# (locks.get(k, {...})) and derives TP/SL through
# _effective_tpsl_pct_for_trade, the same USDT-target path the entry used.
# Seeding one from here with the legacy _effective_tp_sl would pin the SL to
# the wrong USDT level and Phase 1 would then keep it (tp/sl already set).
# This module reports untracked positions so the gap is visible instead.


async def reconcile_cycle() -> dict:
    """One pass of the background reconciliation task."""
    key = os.getenv("BINANCE_API_KEY")
    secret = os.getenv("BINANCE_API_SECRET")
    base = _binance_base()
    if not key or not secret:
        return {"ok": False, "reason": "MISSING_API_KEY"}
    trades = await reconcile_pending_closes()
    untracked = await find_untracked_positions(key, secret, base)
    if untracked:
        names = ", ".join(
            f"{p.get('symbol')}:{p.get('side')}:{float(p.get('qty', 0.0) or 0.0):.4g}" for p in untracked[:5]
        )
        _autotrade_log(
            f"[CloseReconcile] {len(untracked)} exchange position(s) had no guardian lock "
            f"this cycle (Phase 1 adopts them next cycle): {names}"
        )
    # Intent-driven reconciliation only sees closes the bot still holds an
    # intent for. The discovery pass is what catches the ones it dropped, so it
    # runs on a slower cadence than the 60s intent cycle.
    discovery: dict = {"ok": True, "recorded": [], "repaired": [], "skipped": True}
    if _discovery_due():
        discovery = await discover_unrecorded_closes(key, secret, base)
    return {
        "ok": True,
        "recorded": trades or [],
        "repaired": discovery.get("repaired") or [],
        "discovered": discovery.get("recorded") or [],
        "untracked": untracked,
    }


_DISCOVERY_LAST_SEC = 0.0


def _discovery_due() -> bool:
    """True when the intent-independent discovery pass should run again."""
    global _DISCOVERY_LAST_SEC
    interval = max(120, int(os.getenv("CLOSE_DISCOVERY_INTERVAL_SEC", "600") or 600))
    if (time.time() - _DISCOVERY_LAST_SEC) < interval:
        return False
    _DISCOVERY_LAST_SEC = time.time()
    return True
