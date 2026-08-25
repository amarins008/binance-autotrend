#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
autotrade_entry_watchdog.py
===========================

Watches whether the Binance Autotrend bot (LIVE / real-money mode) has actually
ENTERED a new order after a deadlock fix + resume.

WHY THIS EXISTS
---------------
`trades_log.jsonl` only records a LIVE trade when it *closes* (every entry has a
close-style `reason` such as LIVE_CLOSE / LOCAL_SL_HIT / TP_HIT ...). A trade that
has *entered* but not yet closed does NOT appear there. So to know "did the bot
start trading again?", the authoritative signal is the **exchange open-position
count** -- exactly what the bot itself uses (`_open_positions_count` /
`_has_live_open_positions_sync`). We replicate that check with stdlib only
(urllib + hmac) so the watchdog has no third-party dependencies and can run from
plain `python`.

DETECTION (in priority order)
----------------------------
1. EXCHANGE OPEN POSITIONS (timely, primary): signed GET /fapi/v2/positionRisk.
   At arming (first run) we snapshot the set/count of currently-open symbols.
   A *new* entry = open-position count is greater than the resume-snapshot count
   (resume had open=False, so resume count should be 0). Any open position after
   resume => an order has entered.
2. TRADES_LOG CLOSED-LIVE (confirmation): a LIVE row in trades_log.jsonl whose
   `ts > resume_epoch` and `reason != TEST`. This proves an order entered and
   already closed.

REPORTING / STATE
-----------------
State is persisted to <backend>/.watchdog_entry_state.json so the watchdog is
resume-safe and can be driven by repeated short cron invocations (spread the
~2h watch across many short runs instead of one blocking 2h process).

Per-run exit semantics for the calling cron/report layer:
  * ENTRY_DETECTED  -> a new LIVE order entered (report it)
  * STUCK_TIMEOUT   -> >= --max-min minutes since resume, still no entry (report it)
  * WATCHING        -> nothing to report yet (caller should stay silent)
  * ARMED           -> first run: watchdog just armed with baseline (report once)

Usage:
  python autotrade_entry_watchdog.py [--backend-dir DIR] [--resume-epoch EPOCH]
                                    [--max-min 120] [--interval-sec 60]
                                    [--loop] [--json]
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

STATE_FILE = ".watchdog_entry_state.json"
DEFAULT_MAX_MIN = 120
DEFAULT_INTERVAL_SEC = 60


def fts(ts: float | int) -> str:
    if not ts:
        return "n/a"
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def load_env(path: str) -> dict:
    env = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip('"').strip("'")
    except Exception:
        pass
    return env


def signed_position_risk(api_key: str, api_secret: str, base: str, timeout: float = 10.0):
    """Return list of positionRisk rows (or raise). Stdlib-only signed GET."""
    params = {
        "timestamp": int(time.time() * 1000),
        "recvWindow": 60000,
    }
    q = urllib.parse.urlencode(params)
    sig = hmac.new(api_secret.encode("utf-8"), q.encode("utf-8"), hashlib.sha256).hexdigest()
    url = f"{base}/fapi/v2/positionRisk?{q}&signature={sig}"
    req = urllib.request.Request(url, headers={"X-MBX-APIKEY": api_key})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def open_positions_from_risk(risk_rows) -> list[dict]:
    out = []
    for p in risk_rows or []:
        try:
            amt = float(p.get("positionAmt", 0) or 0)
        except Exception:
            amt = 0.0
        if abs(amt) > 0.0:
            out.append({
                "symbol": p.get("symbol"),
                "positionAmt": amt,
                "entryPrice": p.get("entryPrice"),
                "leverage": p.get("leverage"),
            })
    return out


def latest_live_trade(trades_log_path: str, after_epoch: float):
    """Return (last_real_live_row, new_rows_after_resume_excluding_test)."""
    last_real = None
    new_after = []
    try:
        with open(trades_log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if str(obj.get("mode", "")).upper() != "LIVE":
                    continue
                if "pnl" not in obj:
                    continue
                # TEST rows are not real orders -- excluded from both metrics.
                if obj.get("reason") == "TEST":
                    continue
                ts = float(obj.get("ts") or obj.get("closedAt") or 0)
                if last_real is None or ts >= float(last_real.get("ts") or 0):
                    last_real = obj
                if ts > after_epoch:
                    new_after.append(obj)
    except Exception:
        pass
    return last_real, new_after


def read_snapshot(backend_dir: str):
    snap_path = os.path.join(backend_dir, "autotrade_snapshot.json")
    try:
        with open(snap_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend-dir", default=os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument("--resume-epoch", type=float, default=None,
                    help="Epoch (sec) the bot resumed after the deadlock fix. "
                         "If omitted, uses snapshot.startedAt when fresh (<1h) else now.")
    ap.add_argument("--max-min", type=int, default=DEFAULT_MAX_MIN,
                    help="Watchdog window in minutes before reporting STUCK_TIMEOUT.")
    ap.add_argument("--interval-sec", type=int, default=DEFAULT_INTERVAL_SEC,
                    help="Poll interval when --loop.")
    ap.add_argument("--loop", action="store_true", help="Run a blocking poll loop.")
    ap.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    args = ap.parse_args()

    backend_dir = args.backend_dir
    state_path = os.path.join(backend_dir, STATE_FILE)
    env = load_env(os.path.join(backend_dir, ".env"))
    api_key = env.get("BINANCE_API_KEY", "")
    api_secret = env.get("BINANCE_API_SECRET", "")
    base = env.get("BINANCE_FUTURES_BASE") or env.get("BINANCE_BASE") or "https://fapi.binance.com"

    trades_log_path = os.path.join(backend_dir, "obsidian_vault", "trades_log.jsonl")
    snapshot = read_snapshot(backend_dir)

    now = time.time()

    # ---- resolve resume epoch ----
    resume_epoch = args.resume_epoch
    if resume_epoch is None:
        started = float(snapshot.get("startedAt") or 0)
        if started and (now - started) < 3600:
            resume_epoch = started
        else:
            resume_epoch = now

    # ---- load / init state ----
    state = {}
    if os.path.exists(state_path):
        try:
            with open(state_path, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception:
            state = {}
    armed = "armed_at" in state
    if not armed:
        # Snapshot open positions at arming time as the resume baseline.
        resume_open = []
        if api_key and api_secret:
            try:
                resume_open = open_positions_from_risk(
                    signed_position_risk(api_key, api_secret, base))
            except Exception:
                resume_open = []
        state = {
            "armed_at": now,
            "resume_epoch": resume_epoch,
            "resume_open_symbols": [p["symbol"] for p in resume_open],
            "resume_open_count": len(resume_open),
            "last_entry_reported": False,
            "last_stuck_reported": False,
        }
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        armed = True

    # Keep resume_epoch in sync if caller overrode it on a later run.
    if args.resume_epoch and abs(float(state.get("resume_epoch", 0)) - float(args.resume_epoch)) > 1:
        state["resume_epoch"] = float(args.resume_epoch)
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

    resume_epoch = float(state["resume_epoch"])
    resume_open_count = int(state.get("resume_open_count", 0))
    resume_open_symbols = set(state.get("resume_open_symbols", []))

    def evaluate() -> dict:
        # --- exchange open positions (primary) ---
        open_now = []
        exchange_ok = False
        if api_key and api_secret:
            try:
                open_now = open_positions_from_risk(
                    signed_position_risk(api_key, api_secret, base))
                exchange_ok = True
            except Exception as e:
                open_now = []
                exchange_ok = False
                err = str(e)
        else:
            err = "no_api_credentials"

        new_open = [p for p in open_now if p["symbol"] not in resume_open_symbols]
        open_count_now = len(open_now)

        # --- trades_log closed-live (confirmation) ---
        last_real, new_trades = latest_live_trade(trades_log_path, resume_epoch)

        # --- snapshot derived status ---
        running = snapshot.get("running")
        scan_board = snapshot.get("scanBoard", []) or []
        qualified = [
            {"symbol": b.get("symbol"), "signal": b.get("signal"),
             "confidence": b.get("confidence"), "score": b.get("score"),
             "rejectReason": b.get("rejectReason")}
            for b in scan_board if b.get("qualified")
        ]
        last_decision = snapshot.get("lastDecision") or {}
        ld_symbol = last_decision.get("symbol")
        ld_side = last_decision.get("side")
        ld_ts = last_decision.get("ts")

        minutes_since_resume = (now - resume_epoch) / 60.0

        # --- verdict ---
        entry_detected = (len(new_open) > 0) or (len(new_trades) > 0)
        if entry_detected and not state.get("last_entry_reported"):
            verdict = "ENTRY_DETECTED"
        elif minutes_since_resume >= args.max_min and not entry_detected:
            verdict = "STUCK_TIMEOUT"
        elif not armed_fresh:
            verdict = "WATCHING"
        else:
            verdict = "ARMED"

        return {
            "verdict": verdict,
            "mode": "LIVE",
            "running": running,
            "resume_epoch": resume_epoch,
            "resume_at": fts(resume_epoch),
            "now_epoch": now,
            "now_at": fts(now),
            "minutes_since_resume": round(minutes_since_resume, 1),
            "max_min": args.max_min,
            "exchange_reachable": exchange_ok,
            "open_positions_now": open_now,
            "open_count_now": open_count_now,
            "resume_open_count": resume_open_count,
            "new_open_positions": new_open,
            "new_trades_after_resume": [
                {"symbol": t.get("symbol"), "side": t.get("side"),
                 "reason": t.get("reason"), "ts": t.get("ts"),
                 "at": fts(t.get("ts"))}
                for t in new_trades
            ],
            "last_real_live_trade": (
                {"symbol": last_real.get("symbol"), "side": last_real.get("side"),
                 "reason": last_real.get("reason"), "ts": last_real.get("ts"),
                 "at": fts(last_real.get("ts"))}
                if last_real else None
            ),
            "qualified_scanboard": qualified,
            "last_decision": {"symbol": ld_symbol, "side": ld_side, "ts": ld_ts,
                              "at": fts(ld_ts) if ld_ts else None},
        }

    armed_fresh = (now - float(state.get("armed_at", now))) < 5  # first ~5s = arming run

    result = evaluate()

    # --- persist reporting flags ---
    if result["verdict"] == "ENTRY_DETECTED":
        state["last_entry_reported"] = True
    if result["verdict"] == "STUCK_TIMEOUT":
        state["last_stuck_reported"] = True
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

    # --- write a durable report file for terminal / scheduled-job consumption ---
    # Only the conclusive verdicts (ENTRY_DETECTED / STUCK_TIMEOUT) and the first
    # arming summary are persisted; WATCHING polls only update the state file.
    report_path = os.path.join(backend_dir, ".watchdog_entry_report.json")
    if result["verdict"] in ("ENTRY_DETECTED", "STUCK_TIMEOUT", "ARMED"):
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        v = result["verdict"]
        print(f"[{v}] mode={result['mode']} running={result['running']} "
              f"resume={result['resume_at']} ({result['minutes_since_resume']}m/{result['max_min']}m)")
        print(f"  exchange_reachable={result['exchange_reachable']} "
              f"open_now={result['open_count_now']} (resume_baseline={result['resume_open_count']})")
        if result["new_open_positions"]:
            print("  NEW OPEN POSITIONS (entry confirmed):")
            for p in result["new_open_positions"]:
                print(f"    - {p['symbol']} amt={p['positionAmt']} entry={p['entryPrice']} x{p['leverage']}")
        if result["new_trades_after_resume"]:
            print("  NEW CLOSED LIVE TRADES since resume:")
            for t in result["new_trades_after_resume"]:
                print(f"    - {t['symbol']} {t['side']} reason={t['reason']} {t['at']}")
        print(f"  qualified_scanboard={[q['symbol'] for q in result['qualified_scanboard']]}")
        for q in result["qualified_scanboard"]:
            print(f"    * {q['symbol']} {q['signal']} conf={q['confidence']} score={q['score']} "
                  f"reject={q['rejectReason'] or '-'}")
        ld = result["last_decision"]
        print(f"  last_decision={ld['symbol']} {ld['side']} @ {ld['at']}")
        lr = result["last_real_live_trade"]
        if lr:
            print(f"  last_real_live_trade={lr['symbol']} {lr['side']} reason={lr['reason']} @ {lr['at']}")
        else:
            print("  last_real_live_trade=none")

    # exit code: 0 normally; non-zero only on ENTRY/STUCK so a caller can branch
    if result["verdict"] in ("ENTRY_DETECTED", "STUCK_TIMEOUT"):
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
