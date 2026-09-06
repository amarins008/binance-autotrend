"""Real-time monitor: market direction bias detector (direction_bias.py)."""
import asyncio
import os
import sys
import time

sys.path.insert(0, r"E:\My Project\Binance autotrend\backend")

# Load .env so network/cache wiring matches the running app.
from dotenv import load_dotenv  # noqa: E402

load_dotenv(r"E:\My Project\Binance autotrend\backend\.env")

WATCH = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]


def section(title: str):
    print(f"\n{'═'*70}")
    print(f"  {title}")
    print(f"{'═'*70}")


async def one_symbol(symbol):
    from analysis.direction_bias import detect_direction_bias

    t0 = time.time()
    r = await detect_direction_bias(symbol)
    dt = time.time() - t0
    if not r.get("ok"):
        print(f"  {symbol:10s} FAILED  kw={r['entry']['keyword']}  {r['notes']}")
        return r

    tfs = r.get("timeframes", {})
    t15 = tfs.get("15m", {})
    t30 = tfs.get("30m", {})
    e = r.get("entry", {})
    bias_tag = {
        "LONG": "⬆ LONG",
        "SHORT": "⬇ SHORT",
        "NEUTRAL": "➖ NEUTRAL",
    }.get(r.get("bias"), "?")
    print(
        f"  {symbol:10s} {bias_tag:11s} strength={r.get('strength'):.2f} "
        f"struct={r.get('regime'):5s} 15m[ema={(t15.get('emaDir') or '?'):5s} "
        f"gap={(t15.get('emaGapPct') or 0):.3f}% {(t15.get('structure') or '?'):5s}] "
        f"30m[ema={(t30.get('emaDir') or '?'):5s} {(t30.get('structure') or '?'):5s}] "
        f"entry={e.get('keyword')} act={e.get('action')} "
        f"dist={(e.get('pullbackDistAtr') or 0):+.2f}ATR  ({dt*1000:.0f}ms)"
    )
    return r


async def main():
    section("DIRECTION BIAS LIVE MONITOR")
    exec_mode = os.getenv("EXECUTION_MODE", "PAPER")
    print(f"  executionMode : {exec_mode}")

    section(f"LIVE DIRECTION BIAS  ({', '.join(WATCH)})")
    t0 = time.time()
    results = [await one_symbol(s) for s in WATCH]
    total = time.time() - t0

    longs = sum(1 for r in results if r.get("bias") == "LONG")
    shorts = sum(1 for r in results if r.get("bias") == "SHORT")
    neutrals = sum(1 for r in results if r.get("bias") == "NEUTRAL")

    section("SUMMARY")
    print(f"  LONG={longs} SHORT={shorts} NEUTRAL={neutrals}")
    print(f"  total time: {total:.1f}s")

    section("LIVE RE-CHECK after 30s (bias drift)")
    print("  waiting 30s…")
    await asyncio.sleep(30)
    for s in WATCH:
        await one_symbol(s)


if __name__ == "__main__":
    asyncio.run(main())