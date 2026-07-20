"""Real-time monitor: TradingView × Entry Pipeline × Guardian interaction."""
import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, r"E:\My Project\Binance autotrend\backend")


def section(title: str):
    print(f"\n{'═'*70}")
    print(f"  {title}")
    print(f"{'═'*70}")


async def main():
    from trading.tradingview_mcp import get_tv_client, async_get_position_guidance
    from trading.shared_cache_layer import get_shared_cache
    from services.config_paths import VAULT_DIR
    from main import AUTO_TRADE
    from trading.config import apply_autotrade_defaults
    import os

    # ── 1. System State ────────────────────────────────────────────────
    section("1. SYSTEM STATE")
    cfg = AUTO_TRADE.get("config")
    if not isinstance(cfg, dict) or not cfg:
        cfg = apply_autotrade_defaults({})
        AUTO_TRADE["config"] = cfg

    tv_enabled = cfg.get("tradingviewEnabled", False)
    exec_mode = cfg.get("executionMode", "PAPER")
    print(f"  executionMode   : {exec_mode}")
    print(f"  tvEnabled       : {tv_enabled}")
    print(f"  tvEarlyExit     : {cfg.get('tradingviewEarlyExitEnabled', False)}")
    print(f"  tvTpExtension   : {cfg.get('tradingviewTpExtensionEnabled', False)}")
    print(f"  tvSlTrailing    : {cfg.get('tradingviewSlTrailingEnabled', False)}")
    print(f"  tvCacheTtl      : {cfg.get('tradingviewCacheTtl', 60)}s")
    print(f"  tvConfidenceBoost: {cfg.get('tradingviewConfidenceBoost', 0.05)}")

    # ── 2. TradingView Client Health ───────────────────────────────────
    section("2. TRADINGVIEW CLIENT HEALTH")
    tv_client = get_tv_client(cfg)
    health = tv_client.get_health_status()
    for k, v in health.items():
        print(f"  {k:30s}: {v}")

    # ── 3. Cache Layer State ──────────────────────────────────────────
    section("3. CACHE LAYER STATE")
    cache = get_shared_cache(VAULT_DIR)
    print(f"  TV cache entries : {len(cache._tv_cache)}")
    print(f"  TV cache TTL     : {cache._tv_ttl}s")
    for sym, (ts, data) in cache._tv_cache.items():
        age = time.time() - ts
        sig = data.get("signal", "?")
        rec = data.get("recommendation", "?")
        print(f"    {sym:15s} age={age:.0f}s signal={sig} rec={rec}")

    # ── 4. Per-Symbol TV Signals (disk) ───────────────────────────────
    section("4. PER-SYMBOL TV SIGNALS (disk)")
    symbols_dir = Path(r"E:\My Project\Binance autotrend\obsidian_vault\symbols")
    tv_count = 0
    if symbols_dir.exists():
        for sym_dir in sorted(symbols_dir.iterdir()):
            tv_file = sym_dir / "tv_signal.json"
            if tv_file.exists():
                tv_count += 1
                try:
                    data = json.loads(tv_file.read_text(encoding="utf-8"))
                    age = time.time() - float(data.get("ts", 0))
                    sig = data.get("signal", "?")
                    rec = data.get("recommendation", "?")
                    conf = data.get("confidence", 0)
                    if tv_count <= 15:
                        print(f"    {sym_dir.name:15s} age={age:.0f}s signal={sig} rec={rec} conf={conf:.2f}")
                except Exception:
                    pass
    print(f"  Total TV signals on disk: {tv_count}")

    # ── 5. Live Guardian State ────────────────────────────────────────
    section("5. LIVE GUARDIAN STATE")
    locks = AUTO_TRADE.get("liveProfitLocks", {})
    print(f"  Active locks    : {len(locks)}")
    for k, v in locks.items():
        if isinstance(v, dict):
            sym = v.get("symbol", "?")
            side = v.get("side", "?")
            armed = v.get("armed", False)
            peak = v.get("peak", 0)
            tp = v.get("tp", 0)
            sl = v.get("sl", 0)
            ext_count = v.get("tpExtensionCount", 0)
            print(f"    {k:20s} armed={armed} peak={peak:.4f} tp={tp:.6f} sl={sl:.6f} ext={ext_count}")

    # ── 6. Entry Pipeline Config ──────────────────────────────────────
    section("6. ENTRY PIPELINE GATES (key thresholds)")
    gates = [
        ("signal", "WAIT/LONG/SHORT required"),
        ("confidence", f"min={cfg.get('holdMinConfidence', 0.62)}"),
        ("strongFlip", f"enabled={cfg.get('strongFlipEnabled', True)} minConf={cfg.get('strongFlipMinConfidence', 0.82)}"),
        ("payoffLossGuard", f"enabled={cfg.get('payoffLossGuardEnabled', True)} maxRatio={cfg.get('payoffLossGuardMaxPayoffRatio', 0.75)}"),
        ("profitLock", f"trigger={cfg.get('profitLockTriggerUsdt', 0.35)} giveback={cfg.get('profitLockMaxGivebackUsdt', 0.22)}"),
        ("retraceFloor", f"rate={cfg.get('profitLockRetraceFloorRatePct', 0.70)}%"),
        ("breakevenGuard", f"trigger={cfg.get('profitLockBreakevenTriggerUsdt', 0.16)} floor={cfg.get('profitLockBreakevenFloorUsdt', 0.08)}"),
        ("feeEdge", f"minNetUsdt={cfg.get('feeEdgeMinNetUsdt', 0.05)}"),
        ("holdTrailPct", f"{cfg.get('holdTrailPct', 0.25)}%"),
        ("tpExtendStepPct", f"{cfg.get('tpExtendStepPct', 0.25)}%"),
    ]
    for name, detail in gates:
        print(f"    {name:25s} {detail}")

    # ── 7. Guardian Phase Flow ────────────────────────────────────────
    section("7. GUARDIAN PHASE FLOW (per cycle)")
    print("""
    ┌─ Phase 0: Validate entry after restart (idempotent)
    ├─ Phase 1: Update metadata (mark, uPnL, peak, breakeven)
    ├─ Phase 2: Fast price checks (SL/BE hit) → close immediately
    ├─ Phase 3: Fetch intel concurrently (4s/symbol, 15s batch)
    ├─ Phase 3.5: Fetch TV guidance concurrently ──── TV CACHE HIT?
    │   └─ get_position_guidance() → _get_from_cache() → _fetch_from_tradingview()
    └─ Phase 4: Intel-dependent decisions (per position):
        ├─ 1. TV Early Exit (if lock armed + profit → DEFER)
        ├─ 2. TP Hit + Hold Winner → extend TP/SL (uses Phase 3.5 TV)
        ├─ 3. Strong Reversal Exit
        ├─ 4. Payoff Loss Guard
        ├─ 5. Local TP/BE/SL Hit
        ├─ 6. Profit Lock Arming
        ├─ 7. Retrace Budget Close
        ├─ 8. Target Max Close
        └─ 9. Weak Signal Close
    """)

    # ── 8. TV × Guardian Interaction Matrix ────────────────────────────
    section("8. TV × GUARDIAN INTERACTION MATRIX")
    print("""
    ┌─────────────────────┬──────────────┬──────────────────────────────────┐
    │ TV Touchpoint       │ Priority     │ Effect                           │
    ├─────────────────────┼──────────────┼──────────────────────────────────┤
    │ Confluence boost    │ Pre-entry    │ +0.0~0.05 confidence             │
    │ Early exit          │ #1 (highest) │ Close if: losing OR no lock      │
    │                     │              │ Defer if: lock armed + profit    │
    │ TP extension        │ #2           │ Extend TP +0.2~0.5%             │
    │ SL trailing         │ #2           │ Trail SL 0.15~0.3%              │
    └─────────────────────┴──────────────┴──────────────────────────────────┘

    Data Flow:
    Phase 3.5 fetches TV ONCE → stores in _tv_results dict
    → Early exit reads _tv_results[sym:side]
    → _extend_tp_sl_levels() receives tv_guidance param (no re-fetch)
    → _trail_winner_levels() receives tv_guidance param (no re-fetch)
    """)

    # ── 9. Endpoints ──────────────────────────────────────────────────
    section("9. API ENDPOINTS (monitoring)")
    endpoints = [
        ("GET", "/health", "System health"),
        ("GET", "/tradingview/health", "TV client health + config"),
        ("GET", "/learning/status", "Per-symbol learning stats"),
        ("GET", "/learning/status?symbol=BTCUSDT", "Single symbol stats"),
        ("GET", "/debug/binance-positions", "Live Binance positions"),
    ]
    for method, path, desc in endpoints:
        print(f"    {method:6s} {path:45s} {desc}")

    # ── 10. Quick TV Test (if enabled) ────────────────────────────────
    if tv_enabled:
        section("10. LIVE TV TEST (BTCUSDT)")
        try:
            guidance = await async_get_position_guidance(tv_client, "BTCUSDT", "LONG")
            if guidance:
                print(f"  recommendation : {guidance.get('recommendation')}")
                print(f"  signal         : {guidance.get('signal')}")
                print(f"  strength       : {guidance.get('strength'):.2f}")
                osc = guidance.get("oscillators", {})
                if osc:
                    print(f"  RSI            : {osc.get('RSI', 'N/A')}")
                ma = guidance.get("moving_averages", {})
                if ma:
                    print(f"  MA summary     : {ma.get('summary', 'N/A')}")
            else:
                print("  No guidance returned")
        except Exception as e:
            print(f"  Error: {e}")
    else:
        section("10. LIVE TV TEST")
        print("  TradingView disabled — enable via TRADINGVIEW_ENABLED=1 or config")


if __name__ == "__main__":
    asyncio.run(main())
