"""Analyze Binance income vs trades_log to find truly-missing closes.

Robust matching:
  1. Group income events into closes (same symbol, gap <= 120s = one close).
  2. Match each close to a log trade: same symbol, |closedAt diff| <= 150s,
     |gross pnl diff| <= 0.15 (bot estimates gross from mark price).
  3. Unmatched closes = missing -> derive entry via position replay over
     allOrders (walk orders, track position, entry = avg price + first-order
     time of the position build-up that the close reduces to zero).

Prints the missing list as JSON for the backfill step.
"""
import asyncio
import json
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

from exchange.binance_client import _signed_request

BACKEND = Path(__file__).parent.parent
TRADES_LOG = BACKEND / "obsidian_vault" / "trades_log.jsonl"


def group_closes(income_events):
    """Group REALIZED_PNL income events into individual closes."""
    by_sym = defaultdict(list)
    for r in income_events:
        by_sym[r["symbol"]].append(r)
    closes = []
    for sym, events in by_sym.items():
        events.sort(key=lambda r: int(r["time"]))
        cur = [events[0]]
        for ev in events[1:]:
            if int(ev["time"]) - int(cur[-1]["time"]) <= 120_000:
                cur.append(ev)
            else:
                closes.append(cur)
                cur = [ev]
        closes.append(cur)
    out = []
    for grp in closes:
        out.append({
            "symbol": grp[0]["symbol"],
            "close_ms": max(int(r["time"]) for r in grp),
            "gross": round(sum(float(r["income"]) for r in grp), 6),
            "trade_ids": [str(r.get("tradeId") or r.get("info") or "") for r in grp],
        })
    out.sort(key=lambda c: c["close_ms"])
    return out


async def main():
    key = os.getenv("BINANCE_API_KEY")
    sec = os.getenv("BINANCE_API_SECRET")
    base = os.getenv("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com")

    start = int(datetime(2026, 9, 29, 0, 0).timestamp()) * 1000
    res = await _signed_request("GET", base, "/fapi/v1/income", key, sec, {"startTime": start, "limit": 1000})
    realized = [r for r in res if r.get("incomeType") == "REALIZED_PNL"]
    closes = group_closes(realized)
    print(f"income events={len(realized)} -> grouped closes={len(closes)}")

    with open(TRADES_LOG, "r", encoding="utf-8") as f:
        log_trades = [json.loads(l) for l in f if l.strip()]
    log_by_sym = defaultdict(list)
    for t in log_trades:
        ts = int(t.get("closedAt", t.get("ts", 0)) or 0)
        if ts >= start // 1000:
            log_by_sym[str(t.get("symbol"))].append(t)

    missing = []
    for c in closes:
        sym = c["symbol"]
        best = None
        best_dt = None
        for t in log_by_sym.get(sym, []):
            ts = int(t.get("closedAt", t.get("ts", 0)) or 0)
            dt = abs(ts - c["close_ms"] // 1000)
            if best is None or dt < best_dt:
                best, best_dt = t, dt
        if best is not None and best_dt <= 150 and abs(float(best.get("pnl", 0)) - c["gross"]) <= 0.15:
            continue  # matched
        missing.append(c)

    print(f"missing closes: {len(missing)}")
    for c in missing:
        dt = datetime.fromtimestamp(c["close_ms"] // 1000).strftime("%m-%d %H:%M:%S")
        print(f"  {dt} {c['symbol']}: gross={c['gross']:+.4f}")

    # Position replay per symbol with missing closes: derive entry price/time
    syms = sorted({c["symbol"] for c in missing})
    for sym in syms:
        orders = await _signed_request("GET", base, "/fapi/v1/allOrders", key, sec, {
            "symbol": sym, "startTime": start - 36 * 3600 * 1000, "limit": 200,
        })
        orders = [o for o in orders if o.get("status") == "FILLED" and float(o.get("executedQty", 0) or 0) > 0]
        orders.sort(key=lambda o: int(o["time"]))
        # replay
        pos_qty = 0.0
        pos_px = 0.0
        pos_first_ts = 0
        entries = {}  # close_ms -> (entry_px, entry_ts)
        for o in orders:
            side = o.get("side")
            pside = o.get("positionSide")
            qty = float(o["executedQty"])
            px = float(o["avgPrice"])
            ts = int(o["time"]) // 1000
            opening = (pside == "LONG" and side == "BUY") or (pside == "SHORT" and side == "SELL")
            if opening:
                total = pos_qty + qty
                pos_px = (pos_px * pos_qty + px * qty) / total if total > 0 else px
                pos_qty = total
                if pos_qty <= 0 or abs(pos_qty - qty) < 1e-12:
                    pos_first_ts = ts
                elif pos_first_ts == 0:
                    pos_first_ts = ts
            else:
                pos_qty -= qty
                if abs(pos_qty) < 1e-9:
                    pos_qty = 0.0
                    entries[ts] = (pos_px, pos_first_ts)
                    pos_px = 0.0
                    pos_first_ts = 0
        for c in [m for m in missing if m["symbol"] == sym]:
            cs = c["close_ms"] // 1000
            cand = [t for t in entries if abs(t - cs) <= 150]
            if cand:
                t = min(cand, key=lambda x: abs(x - cs))
                c["entry_px"], c["entry_ts"] = round(entries[t][0], 10), entries[t][1]
            else:
                # fallback: derive from gross
                c["entry_px"], c["entry_ts"] = None, None
        for o in orders:
            pass
        # qty of close: from fills later; store orders list length for info
    # attach close qty + exact fees via userTrades per close
    for c in missing:
        sym = c["symbol"]
        qty = 0.0
        fee = 0.0
        wpx_num = 0.0
        for tid in c["trade_ids"]:
            if not tid:
                continue
            try:
                ut = await _signed_request("GET", base, "/fapi/v1/userTrades", key, sec, {
                    "symbol": sym, "fromId": int(tid), "limit": 5,
                })
            except Exception:
                continue
            for fill in ut if isinstance(ut, list) else []:
                if str(fill.get("id")) == tid:
                    qty += float(fill.get("qty", 0) or 0)
                    fee += float(fill.get("commission", 0) or 0)
                    wpx_num += float(fill.get("price", 0) or 0) * float(fill.get("qty", 0) or 0)
        c["qty"] = round(qty, 10)
        c["exit_fee"] = round(fee, 6)
        c["exit_px"] = round(wpx_num / qty, 10) if qty > 0 else None

    # derive entry price from gross when replay failed
    for c in missing:
        if c.get("entry_px") is None and c.get("exit_px") and c["qty"]:
            side_guess = "LONG" if c["gross"] >= 0 else None
            # sign unknown without side; infer: income>0 means long-profit or short-profit both possible.
            # Use position replay result absent -> leave entry None; backfill will use exit-based derivation.
            c["entry_px"] = None

    out = []
    for c in missing:
        out.append({
            "symbol": c["symbol"],
            "closedAt": c["close_ms"] // 1000,
            "gross": c["gross"],
            "qty": c["qty"],
            "exit_fee": c["exit_fee"],
            "exit_px": c["exit_px"],
            "entry_px": c.get("entry_px"),
            "entry_ts": c.get("entry_ts"),
            "trade_ids": c["trade_ids"][:1],
        })
    print()
    print(json.dumps(out, indent=1))
    with open(BACKEND / "scripts" / "missing_closes.json", "w") as f:
        json.dump(out, f, indent=1)
    print("saved -> backend/scripts/missing_closes.json")


asyncio.run(main())
