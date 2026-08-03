"""AI Symbol Analyzer — per-symbol trade window analysis for Hermes autotune loop.

Reads obsidian_vault/trades_log.jsonl (or per-symbol trades.jsonl), groups by
symbol, analyzes the last N trades (default 4-8 window), and writes a verdict
JSON that the Hermes cron agent reads to decide safe auto-apply actions.

State file: obsidian_vault/shared/ai_analyzer_state.json (last analyzed ts per symbol)
Verdict file: obsidian_vault/shared/ai_analyzer_verdict.json (stdout also prints it)

Usage: python ai_symbol_analyzer.py [--min-trades 4] [--window 8] [--since-hours 24]
"""
import argparse
import json
import os
import sys
import time
import datetime as dt
from collections import defaultdict

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VAULT = os.path.join(BACKEND, "obsidian_vault")
LOG = os.path.join(VAULT, "trades_log.jsonl")
STATE = os.path.join(VAULT, "shared", "ai_analyzer_state.json")
VERDICT = os.path.join(VAULT, "shared", "ai_analyzer_verdict.json")
FEE_FLOOR = 0.10  # feeMinNetProfitUSDT effective floor

def ts(x):
    try:
        return int(float(x or 0))
    except (TypeError, ValueError):
        return 0

def load_trades():
    rows = []
    if not os.path.exists(LOG):
        return rows
    with open(LOG, encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                rows.append(json.loads(ln))
            except json.JSONDecodeError:
                continue
    return [r for r in rows if r.get("mode") == "LIVE"]

def analyze_symbol(sym, trades, window):
    """Analyze last `window` trades for one symbol."""
    t = sorted(trades, key=lambda r: ts(r.get("closedAt") or r.get("ts")))
    recent = t[-window:]
    if not recent:
        return None
    pnls = [float(r.get("pnl") or 0.0) for r in recent]
    n = len(pnls)
    net = sum(pnls)
    wins = [p for p in pnls if p >= 0]
    wr = len(wins) / max(n, 1) * 100.0
    gross_win = sum(wins)
    gross_loss = abs(sum(p for p in pnls if p < 0))
    payoff = gross_win / gross_loss if gross_loss > 0.01 else (99.0 if gross_win > 0 else 0.0)

    reasons = [str(r.get("reason") or "?") for r in recent]
    sl_count = sum(1 for x in reasons if "SL" in x)
    dz_count = sum(1 for x in reasons if x == "DEAD_ZONE_TIMEOUT")
    fee_bleed = sum(1 for p in pnls if -FEE_FLOOR < p < FEE_FLOOR)  # closed below fee floor

    # TV alignment at entry — distinguish real alignment from data gaps.
    # tvAtEntry may be a plain string ("LONG"/"SHORT"/"WAIT") OR a dict {"signal": ...}.
    # None = intel didn't capture TV (post-restart data gap, NOT a blind entry).
    tv_ok = 0
    tv_wait = 0
    tv_miss = 0
    for r in recent:
        tv = r.get("tvAtEntry")
        if isinstance(tv, dict):
            sig = str(tv.get("signal") or "")
        elif isinstance(tv, str):
            sig = tv.strip().upper()
        else:
            sig = ""
        if sig in ("LONG", "SHORT"):
            tv_ok += 1
        elif sig == "WAIT":
            tv_wait += 1
        else:
            tv_miss += 1  # None / missing → data gap, reported separately below

    # Guardian hold efficiency: actual pnl vs peak (did we give back profit?)
    hold_effs = []
    for r in recent:
        gs = r.get("guardian_stats") or {}
        peak = float(gs.get("peakProfitUsdt") or 0.0)
        p = float(r.get("pnl") or 0.0)
        if peak > 0.01:
            hold_effs.append(min(1.0, max(-0.5, p / peak)))
    hold_eff = sum(hold_effs) / len(hold_effs) if hold_effs else None

    last_closed = ts(recent[-1].get("closedAt") or recent[-1].get("ts"))
    return {
        "symbol": sym,
        "n": n,
        "net": round(net, 4),
        "winRatePct": round(wr, 1),
        "payoff": round(payoff, 2),
        "avgPnl": round(net / max(n, 1), 4),
        "slCount": sl_count,
        "deadZoneCount": dz_count,
        "feeBleedCount": fee_bleed,
        "tvAlignAtEntry": tv_ok,
        "tvWaitAtEntry": tv_wait,
        "tvMissAtEntry": tv_miss,
        "holdEfficiency": round(hold_eff, 3) if hold_eff is not None else None,
        "reasons": dict(sorted(defaultdict(int, {x: reasons.count(x) for x in set(reasons)}).items(), key=lambda kv: -kv[1])),
        "lastClosedAt": last_closed,
        "lastClosed": dt.datetime.fromtimestamp(last_closed).strftime("%m-%d %H:%M") if last_closed else None,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-trades", type=int, default=4)
    ap.add_argument("--window", type=int, default=8)
    ap.add_argument("--since-hours", type=int, default=24)
    args = ap.parse_args()

    os.makedirs(os.path.join(VAULT, "shared"), exist_ok=True)
    state = {}
    if os.path.exists(STATE):
        try:
            state = json.load(open(STATE, encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            state = {}

    now = int(time.time())
    rows = load_trades()
    by_sym = defaultdict(list)
    for r in rows:
        by_sym[r.get("symbol", "")].append(r)

    verdicts = []
    new_state = dict(state)
    for sym, trades in sorted(by_sym.items()):
        if not sym or not trades:
            continue
        last_ts = max(ts(r.get("closedAt") or r.get("ts")) for r in trades)
        prev_ts = int(state.get(sym, {}).get("lastAnalyzedTs") or 0)
        # only symbols with NEW trades since last run
        if prev_ts and last_ts <= prev_ts:
            continue
        if now - last_ts > args.since_hours * 3600:
            continue  # stale symbol, skip
        v = analyze_symbol(sym, trades, args.window)
        if v and v["n"] >= args.min_trades:
            verdicts.append(v)
        new_state[sym] = {"lastAnalyzedTs": last_ts}

    # prune state: drop symbols with no new trades in 7 days
    for sym in list(new_state.keys()):
        last = int(new_state[sym].get("lastAnalyzedTs") or 0)
        if now - last > 7 * 86400:
            del new_state[sym]

    json.dump(new_state, open(STATE, "w", encoding="utf-8"))
    json.dump(verdicts, open(VERDICT, "w", encoding="utf-8"), indent=1)
    print(json.dumps({"symbols": verdicts, "total": len(verdicts), "stateFile": STATE}, indent=1))
    sys.exit(0)

if __name__ == "__main__":
    main()
