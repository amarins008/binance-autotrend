"""Continuous TV + direction-bias + entry monitor.

Re-checks the watch list on a fixed cadence, appends a JSONL record per symbol
cycle (full intel snapshot) and raises a console banner + writes a dedicated
event the moment a symbol transitions to entry keyword = pullback_in_zone
(holding both the pre-entry drift and post-entry observation in one file).

Usage:
  python tests/monitor_tv_entry_loop.py            # default: INTERVAL=120s
  python tests/monitor_tv_entry_loop.py 60          # INTERVAL=60s
  python tests/monitor_tv_entry_loop.py 60 BTCUSDT  # watch only BTCUSDT

Logs (append JSONL):
  <vault>/monitor_tv_entry.jsonl      every cycle, every symbol
  <vault>/monitor_tv_entry_events.jsonl  only pullback_in_zone transitions

Stop with Ctrl+C (loop guard lets the current cycle finish).
"""
import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, r"E:\My Project\Binance autotrend\backend")

# Load .env so network/cache wiring matches the running app.
from dotenv import load_dotenv  # noqa: E402

load_dotenv(r"E:\My Project\Binance autotrend\backend\.env")

from pathlib import Path  # noqa: E402

VAULT_DIR = Path(os.getenv("HERMES_DATA_DIR", Path(r"E:\My Project\Binance autotrend\backend")).resolve()) / "obsidian_vault"
LOG_PATH = VAULT_DIR / "monitor_tv_entry.jsonl"
EVENT_PATH = VAULT_DIR / "monitor_tv_entry_events.jsonl"

DEFAULT_WATCH = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
TRANSITION_TTL = 600  # seconds: only log a pullback_in_zone once per symbol per TTL


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def _num(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def snapshot_row(symbol, r, ok):
    tv = r.get("tv") or {}
    db = r.get("directionBias") or {}
    entry = db.get("entry") or {}
    ex = r.get("execution") or {}
    pk = r.get("precision") or {}
    m = r.get("momentum") or {}
    ob = r.get("orderBook") or {}
    return {
        "ts": utc_now(),
        "t": time.time(),
        "symbol": symbol,
        "ok": ok,
        "intel": {
            "signal": r.get("signal"),
            "confidence": _num(r.get("confidence"), default=None),
            "tv": {
                "signal": tv.get("signal"),
                "confidence": _num(tv.get("confidence"), default=None),
                "strength": _num(tv.get("strength")),
                "age": _num(tv.get("age")),
                "status": tv.get("status"),
            },
            "directionBias": {
                "bias": db.get("bias"),
                "strength": _num(db.get("strength")),
                "regime": db.get("regime"),
                "entryKeyword": entry.get("keyword"),
                "entryAction": entry.get("action"),
                "pullbackDistAtr": _num(entry.get("pullbackDistAtr")),
                "entryPrice": entry.get("price"),
                "emaZonePrice": entry.get("emaZonePrice"),
            },
            "momentum": {
                "momentumPct": _num(m.get("momentumPct")),
                "volumeRatio": _num(m.get("volumeRatio")),
                "divergence": m.get("divergence"),
            },
            "precision": {
                "rsi14": _num(pk.get("rsi14")),
                "macdHist": _num(pk.get("macdHist")),
                "bbPctB": _num(pk.get("bbPctB")),
                "trendUp": bool(pk.get("trendUp")),
                "trendDown": bool(pk.get("trendDown")),
            },
            "execution": {
                "mark": ex.get("mark"),
                "spreadBps": _num(ex.get("spreadBps")),
            },
            "orderBook": {
                "imbalance": _num(ob.get("imbalance")),
                "icebergRisk": bool(ob.get("icebergRisk")),
            },
        },
    }


async def one(symbol):
    """Run one intel analysis with retry-on-flaky-network."""
    from schemas import IntelAnalyzeRequest
    import main

    last_exc = None
    for attempt in range(1, 4):
        try:
            r = await asyncio.wait_for(
                main.intel_analyze(IntelAnalyzeRequest(symbol=symbol)), timeout=30
            )
            return snapshot_row(symbol, r, ok=True), None
        except Exception as exc:
            last_exc = exc
            await asyncio.sleep(2)
    row = snapshot_row(symbol, {}, ok=False)
    row["intel"]["error"] = f"{type(last_exc).__name__}: {last_exc}"
    return row, last_exc


def emit(rows):
    with LOG_PATH.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def banner(symbol, row):
    e = row["intel"]["directionBias"]
    print(" " + "!" * 72)
    print("  PULLBACK IN ZONE")
    print(f"    {symbol}  bias={e['bias']} str={e['strength']} "
          f"kw={e['entryKeyword']} dist={e['pullbackDistAtr']:+.2f}ATR "
          f"price={e['entryPrice']} zone={e['emaZonePrice']}")
    print(" " + "!" * 72)


async def run_cycle(watch, last_zone_ts):
    results = []
    for s in watch:
        row, exc = await one(s)
        e = row["intel"].get("directionBias") or {}
        kw = e.get("entryKeyword")
        if kw == "pullback_in_zone":
            now = time.time()
            if now - last_zone_ts.get(s, 0.0) >= TRANSITION_TTL:
                last_zone_ts[s] = now
                banner(s, row)
                with EVENT_PATH.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
        results.append(row)
    emit(results)
    return results


def pretty(results):
    for r in results:
        i = r["intel"]
        db = i["directionBias"]
        tv = i["tv"]
        mark = f"@{float(i['execution']['mark']):,g}" if i["execution"].get("mark") is not None else "? "
        line = (
            f"{r['ts'][11:19]} {r['symbol']:10s} sig={str(i['signal']):5s} "
            f"conf={i['confidence'] or 0:.3f} tw={str(tv['signal']):5s}/"
            f"{(tv['confidence'] or 0):.2f}/{int(tv['age']):>3d}s "
            f"db={str(db['bias']):7s}/{db['regime']:5s}/{str(db['entryKeyword']):16s} "
            f"dist={db['pullbackDistAtr']:+.2f} mark={mark}"
        )
        if not r["ok"]:
            line += f"  ERROR {i.get('error')}"
        if r["ok"] and i["momentum"]["divergence"] not in ("NONE", None, ""):
            line += f"  !!div={i['momentum']['divergence']}"
        print(f"  {line}")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("interval", nargs="?", type=int, default=120)
    parser.add_argument("symbols", nargs="*", default=DEFAULT_WATCH)
    args = parser.parse_args()
    interval = max(10, args.interval)
    watch = [s.upper() for s in args.symbols]

    print("=" * 76)
    print("  TV + DIRECTION BIAS + ENTRY LOOP MONITOR (live intel)")
    print(f"  interval={interval}s  watch={watch}")
    print(f"  log={LOG_PATH}")
    print(f"  events={EVENT_PATH}")
    print("=" * 76)

    last_zone_ts: dict[str, float] = {}
    cycle = 0
    t_start = time.time()
    while True:
        cycle += 1
        t0 = time.time()
        print(f"── cycle {cycle}  (uptime={int(time.time()-t_start)}s) ──")
        try:
            results = await run_cycle(watch, last_zone_ts)
            pretty(results)
        except Exception as exc:
            print(f"  cycle failed: {type(exc).__name__}: {exc}")
        elapsed = time.time() - t0
        try:
            await asyncio.sleep(max(1, interval - elapsed))
        except KeyboardInterrupt:
            print("\n  stopped.")
            break


if __name__ == "__main__":
    asyncio.run(main())