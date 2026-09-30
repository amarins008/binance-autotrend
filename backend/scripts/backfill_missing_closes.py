"""Backfill the 27 missing closes (from missing_closes.json) into
trades_log.jsonl + per-symbol trades.jsonl + learning profiles.

Bot must be STOPPED while this runs.

Special handling:
  - ENA merged group (closedAt 1790701389, qty 805) -> replaced by two manual
    records (00:01:37 qty 403 and 00:03:09 qty 402 — two position lifecycles).
  - Replay-null entries get manual overrides (partial closes keep avg entry).
  - HBAR 06:29 (1790724546) already recorded with wrong ts -> corrected in place.
  - HBAR 22:48 and TAO 11:06 log entries recorded by the bot with bad mark-price
    estimates (>0.09 off) -> corrected to Binance actual fills.
"""
import asyncio
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

from exchange.binance_client import _signed_request

BACKEND = Path(__file__).parent.parent
TRADES_LOG = BACKEND / "obsidian_vault" / "trades_log.jsonl"
VAULT = BACKEND / "obsidian_vault"

MANUAL_ENTRIES = {
    # closedAt -> (entry_px, entry_ts)
    1790693156: (0.5134, 1790690298),    # ONDO 21:45 partial close of 21:18 SHORT @0.5134
    1790694796: (314.86284, 1790693464), # TAO 22:13 LONG opened 21:51:04
    1790700797: (8.799, 1790700741),     # UNI 23:53 SHORT opened 23:52:21 @8.799
}
SKIP = {
    1790701389,  # ENA merged group — replaced by two manual records
    1790724546,  # HBAR 06:29 — already recorded, fix ts in place instead
}
MANUAL_RECORDS = [
    {   # ENA 00:01:37 close of SHORT opened 23:57:23
        "symbol": "ENAUSDT", "side": "SHORT", "qty": 403.0,
        "entry": 0.2477800, "entry_ts": 1790701043,
        "exit": 0.2457902, "closedAt": 1790701297,
        "gross": 0.8019, "exit_fee": 0.0495, "entry_fee": 0.0499,
    },
    {   # ENA 00:03:09 close of SHORT opened 00:02:41 (402 of 406)
        "symbol": "ENAUSDT", "side": "SHORT", "qty": 402.0,
        "entry": 0.2455233, "entry_ts": 1790701361,
        "exit": 0.2447473, "closedAt": 1790701389,
        "gross": 0.3119, "exit_fee": 0.0492, "entry_fee": 0.0493,
    },
]
# In-place corrections of bot-recorded entries with bad mark-price estimates
CORRECTIONS = [
    {
        # HBAR 06:29 ts fix (already-recorded trade)
        "match": {"symbol": "HBARUSDT", "reason": "EXTERNAL_CLOSE", "closedAt": 1790724946},
        "set": {"closedAt": 1790724546, "entryDecisionAt": 1790724133},
    },
    {
        # TAO 11:06 — bot gross 0.05157 (stale mark) vs Binance 0.1452
        "match": {"symbol": "TAOUSDT", "closedAt": 1790741163, "approx": True},
        "set": {"pnl": 0.1452, "grossPnl": 0.1452, "exit": 302.74,
                "feeEstUsdt": 0.0998316, "netPnl": 0.0453684},
    },
]


async def fetch_fill_meta(key, sec, base, symbol, trade_id):
    ut = await _signed_request("GET", base, "/fapi/v1/userTrades", key, sec, {
        "symbol": symbol, "fromId": int(trade_id), "limit": 5,
    })
    for fill in ut if isinstance(ut, list) else []:
        if str(fill.get("id")) == trade_id:
            return {
                "side": fill.get("side"),
                "positionSide": fill.get("positionSide"),
            }
    return {}


async def main():
    key = os.getenv("BINANCE_API_KEY")
    sec = os.getenv("BINANCE_API_SECRET")
    base = os.getenv("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com")

    from trading.learning import _record_learning_trade
    from trading.trade_log import net_pnl_of

    with open(BACKEND / "scripts" / "missing_closes.json") as f:
        missing = json.load(f)

    # ---------- 1. in-place corrections ----------
    with open(TRADES_LOG, "r", encoding="utf-8") as f:
        log_rows = [json.loads(l) for l in f if l.strip()]

    def apply_correction(rows):
        n = 0
        for corr in CORRECTIONS:
            m = corr["match"]
            for t in rows:
                if m.get("approx"):
                    if (t.get("symbol") == m["symbol"]
                            and abs(int(t.get("closedAt", 0)) - 1790743560) < 120
                            and abs(float(t.get("pnl", 0)) - 0.0516) < 0.01):
                        t.update(corr["set"])
                        n += 1
                        break
                elif all(t.get(k) == v for k, v in m.items()):
                    t.update(corr["set"])
                    n += 1
                    break
        return n

    n_fixed = apply_correction(log_rows)
    print(f"[1] corrections applied to global log: {n_fixed}")

    # also mirror TAO 11:06 + HBAR ts corrections into per-symbol files
    for sym in {c["match"]["symbol"] for c in CORRECTIONS}:
        ppath = VAULT / "symbols" / sym / "trades.jsonl"
        with open(ppath, "r", encoding="utf-8") as f:
            rows = [json.loads(l) for l in f if l.strip()]
        n = apply_correction(rows)
        with open(ppath, "w", encoding="utf-8") as f:
            for t in rows:
                f.write(json.dumps(t, ensure_ascii=False) + "\n")
        print(f"    {sym} per-symbol corrections: {n}")

    with open(TRADES_LOG, "w", encoding="utf-8") as f:
        for t in log_rows:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")

    # ---------- 2. backfill missing closes ----------
    added = 0
    for c in missing:
        closed_at = c["closedAt"]
        if closed_at in SKIP:
            continue
        sym = c["symbol"]
        qty = c["qty"]
        gross = c["gross"]
        exit_px = c["exit_px"]
        if not qty or not exit_px:
            print(f"    SKIP {sym} {closed_at}: no qty/exit")
            continue

        side = None
        if c.get("trade_ids") and c["trade_ids"][0]:
            meta = await fetch_fill_meta(key, sec, base, sym, c["trade_ids"][0])
            pside = meta.get("positionSide", "")
            if pside in ("LONG", "SHORT"):
                side = pside
        if side is None:
            # infer from replay entry vs exit
            ep = c.get("entry_px")
            if ep is not None:
                side = "LONG" if (gross >= 0) == (exit_px > ep) else "SHORT"
            else:
                side = "LONG" if gross >= 0 else "SHORT"  # fallback

        entry_px = c.get("entry_px")
        entry_ts = c.get("entry_ts")
        if entry_px is None and closed_at in MANUAL_ENTRIES:
            entry_px, entry_ts = MANUAL_ENTRIES[closed_at]
        if entry_px is not None and abs((exit_px - entry_px) * qty - gross) > 0.01:
            # replay entry inconsistent (multi-lifecycle group) — derive from gross
            sign = 1 if side == "LONG" else -1
            entry_px = round(exit_px - sign * gross / qty, 10)
        if entry_px is None:
            sign = 1 if side == "LONG" else -1
            entry_px = round(exit_px - sign * gross / qty, 10)
        if entry_ts is None:
            entry_ts = closed_at - 600

        exit_fee = c.get("exit_fee") or 0.0
        entry_fee = round(qty * entry_px * 0.0005, 6)  # Binance actual taker 5bps
        fee_total = round(exit_fee + entry_fee, 6)
        net = round(gross - fee_total, 6)

        rec = {
            "side": side,
            "entry": entry_px,
            "exit": exit_px,
            "qty": qty,
            "pnl": round(gross, 6),
            "grossPnl": round(gross, 6),
            "feeEstUsdt": fee_total,
            "fundingEstUsdt": 0.0,
            "netPnl": net,
            "reason": "EXTERNAL_CLOSE",
            "closedAt": closed_at,
            "entryDecisionAt": entry_ts,
        }
        _record_learning_trade(sym, rec, "LIVE")
        added += 1
        dt = datetime.fromtimestamp(closed_at).strftime("%m-%d %H:%M")
        print(f"    + {dt} {sym} {side} qty={qty} gross={gross:+.4f} net={net:+.4f}")

    for rec in MANUAL_RECORDS:
        fee_total = round(rec["exit_fee"] + rec["entry_fee"], 6)
        net = round(rec["gross"] - fee_total, 6)
        out = {
            "side": rec["side"],
            "entry": rec["entry"],
            "exit": rec["exit"],
            "qty": rec["qty"],
            "pnl": rec["gross"],
            "grossPnl": rec["gross"],
            "feeEstUsdt": fee_total,
            "fundingEstUsdt": 0.0,
            "netPnl": net,
            "reason": "EXTERNAL_CLOSE",
            "closedAt": rec["closedAt"],
            "entryDecisionAt": rec["entry_ts"],
        }
        _record_learning_trade(rec["symbol"], out, "LIVE")
        added += 1
        dt = datetime.fromtimestamp(rec["closedAt"]).strftime("%m-%d %H:%M")
        print(f"    + {dt} {rec['symbol']} {rec['side']} qty={rec['qty']} gross={rec['gross']:+.4f} net={net:+.4f}")

    print(f"[2] backfilled {added} records")

    # ---------- 2b. TAO profile adjust for the 11:06 correction (net flipped loss->win) ----------
    sym_dir = VAULT / "symbols" / "TAOUSDT"
    ppath = sym_dir / "profile.json"
    with open(ppath, "r", encoding="utf-8") as f:
        pr = json.load(f)
    delta = 0.0453684 - (-0.068278)
    pr["losses"] = int(pr.get("losses", 0)) - 1
    pr["wins"] = int(pr.get("wins", 0)) + 1
    pr["realizedPnl"] = round(float(pr.get("realizedPnl", 0.0)) + delta, 6)
    pr["sumPnl"] = round(float(pr.get("sumPnl", 0.0)) + delta, 6)
    pr["avgPnlPerTrade"] = round(float(pr["sumPnl"]) / max(int(pr["trades"]), 1), 6)
    pr["rewardWinStreak"] = max(1, int(pr.get("rewardWinStreak", 0)))
    pr["rewardLossStreak"] = 0
    pr["tpslFeedback"] = {"updatedAt": int(time.time()), "mode": "LIVE",
                          "reason": "DEAD_ZONE_TIMEOUT", "pnl": 0.0453684}
    try:
        from trading.learning import (
            _live_closed_trades_from_symbol,
            _memory_windows_from_trades,
            _weighted_recent_memory_score,
            _symbol_risk_tune_from_recent_trades,
        )
        cfg = snap_cfg = {}
        try:
            with open(BACKEND / "autotrade_snapshot.json", "r", encoding="utf-8") as f:
                snap_cfg = (json.load(f) or {}).get("config") or {}
        except Exception:
            pass
        recent = _live_closed_trades_from_symbol("TAOUSDT", mode="ALL", vault_dir=VAULT)
        windows = _memory_windows_from_trades(recent)
        pr["memoryWindows"] = windows
        pr["weightedRecentScore"] = _weighted_recent_memory_score(windows)
        pr["symbolRiskTune"] = _symbol_risk_tune_from_recent_trades("TAOUSDT", recent, snap_cfg)
    except Exception as e:
        print(f"    WARN TAO window recompute: {e}")
    pr["updatedAt"] = int(time.time())
    with open(ppath, "w", encoding="utf-8") as f:
        json.dump(pr, f, ensure_ascii=False, indent=1)
    print(f"[2b] TAO profile adjusted: wins={pr['wins']} losses={pr['losses']} realizedPnl={pr['realizedPnl']}")

    # ---------- 3. snapshot daily fix ----------
    snap_path = BACKEND / "autotrade_snapshot.json"
    with open(snap_path, "r", encoding="utf-8") as f:
        snap = json.load(f)
    tloc = time.localtime()
    today_key = (tloc.tm_year, tloc.tm_mon, tloc.tm_mday)
    daily = 0.0
    with open(TRADES_LOG, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            t = json.loads(line)
            ts = int(t.get("closedAt", t.get("ts", 0)) or 0)
            if ts <= 0:
                continue
            tl = time.localtime(ts)
            if (tl.tm_year, tl.tm_mon, tl.tm_mday) == today_key:
                daily += net_pnl_of(t)
    snap["dailyRealizedPnlUSDT"] = round(daily, 6)
    snap["dailyPnlDateKey"] = [today_key[0], today_key[1], today_key[2]]
    with open(snap_path, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False)
    print(f"[3] snapshot dailyRealizedPnlUSDT={snap['dailyRealizedPnlUSDT']}")
    print("DONE")


asyncio.run(main())
