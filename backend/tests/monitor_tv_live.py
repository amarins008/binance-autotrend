"""Live monitor: TradingView × Entry × Guardian real-time interaction."""
import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, r"E:\My Project\Binance autotrend\backend")

VAULT = Path(r"E:\My Project\Binance autotrend\obsidian_vault")
SYMBOLS_DIR = VAULT / "symbols"

RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
DIM = "\033[2m"


def hr():
    print(f"{DIM}{'─'*72}{RESET}")


def header(title: str):
    print(f"\n{BOLD}{CYAN}{'═'*72}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'═'*72}{RESET}")


def kv(key: str, val, color=""):
    c = color if color else ""
    r = RESET if color else ""
    print(f"  {key:30s} {c}{val}{r}")


async def main():
    from trading.config import apply_autotrade_defaults
    from main import AUTO_TRADE

    cfg = AUTO_TRADE.get("config")
    if not isinstance(cfg, dict) or not cfg:
        cfg = apply_autotrade_defaults({})
        AUTO_TRADE["config"] = cfg

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("SYSTEM STATUS")
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    kv("executionMode", cfg.get("executionMode", "PAPER"), GREEN if cfg.get("executionMode") == "LIVE" else YELLOW)
    kv("tvEnabled", cfg.get("tradingviewEnabled", False), GREEN if cfg.get("tradingviewEnabled") else RED)
    kv("tvEarlyExit", cfg.get("tradingviewEarlyExitEnabled", False))
    kv("tvTpExtension", cfg.get("tradingviewTpExtensionEnabled", False))
    kv("tvSlTrailing", cfg.get("tradingviewSlTrailingEnabled", False))
    kv("tvConfidenceBoost", cfg.get("tradingviewConfidenceBoost", 0.05))
    kv("tvCacheTtl", f"{cfg.get('tradingviewCacheTtl', 60)}s")
    kv("tvRateLimit", f"{cfg.get('tradingviewRateLimit', 30)}/min")
    kv("holdMinConfidence", cfg.get("holdMinConfidence", 0.78))
    kv("holdTrailPct", f"{cfg.get('holdTrailPct', 0.25)}%")
    kv("tpExtendStepPct", f"{cfg.get('tpExtendStepPct', 0.25)}%")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("TRADINGVIEW CLIENT")
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    from trading.tradingview_mcp import get_tv_client
    tv = get_tv_client(cfg)
    h = tv.get_health_status()
    kv("enabled", h.get("enabled"), GREEN if h.get("enabled") else RED)
    kv("healthy", h.get("healthy"), GREEN if h.get("healthy") else RED)
    kv("fail_count", h.get("fail_count", 0), RED if h.get("fail_count", 0) > 0 else GREEN)
    kv("cache_size", h.get("cache_size", 0))
    kv("tv_ta_available", h.get("tradingview_ta_available"), GREEN if h.get("tradingview_ta_available") else RED)
    if h.get("disabled_until", 0) > time.time():
        remaining = int(h["disabled_until"] - time.time())
        kv("DISABLED FOR", f"{remaining}s", RED)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("TV CACHE (in-memory)")
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    if tv._cache:
        for sym, result in tv._cache.items():
            age = time.time() - result.timestamp
            status = f"{GREEN}FRESH{RESET}" if age < tv.cache_ttl else f"{RED}STALE{RESET}"
            print(f"  {sym:15s} signal={result.signal.value:8s} conf={result.confidence:.2f}  age={age:.0f}s  {status}")
    else:
        print(f"  {DIM}(empty — no cached signals){RESET}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("SHARED CACHE LAYER")
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    from trading.shared_cache_layer import get_shared_cache
    cache = get_shared_cache(VAULT)
    kv("tv_cache_entries", len(cache._tv_cache))
    kv("tv_cache_ttl", f"{cache._tv_ttl}s")
    for sym, (ts, data) in cache._tv_cache.items():
        age = time.time() - ts
        sig = data.get("signal", "?")
        rec = data.get("recommendation", "?")
        status = f"{GREEN}FRESH{RESET}" if age < cache._tv_ttl else f"{RED}STALE{RESET}"
        print(f"  {sym:15s} signal={sig:8s} rec={rec:12s} age={age:.0f}s  {status}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("PER-SYMBOL TV SIGNALS (disk)")
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    tv_count = 0
    tv_active = 0
    if SYMBOLS_DIR.exists():
        for sym_dir in sorted(SYMBOLS_DIR.iterdir()):
            tv_file = sym_dir / "tv_signal.json"
            if tv_file.exists():
                tv_count += 1
                try:
                    data = json.loads(tv_file.read_text(encoding="utf-8"))
                    age = time.time() - float(data.get("ts", 0))
                    sig = data.get("signal", "?")
                    rec = data.get("recommendation", "?")
                    conf = data.get("confidence", 0)
                    fresh = age < cfg.get("tradingviewCacheTtl", 60)
                    if fresh:
                        tv_active += 1
                    status = f"{GREEN}FRESH{RESET}" if fresh else f"{DIM}stale{RESET}"
                    print(f"  {sym_dir.name:15s} signal={sig:8s} rec={rec:12s} conf={conf:.2f} age={age:.0f}s  {status}")
                except Exception:
                    pass
    kv("total_on_disk", tv_count)
    kv("fresh_signals", tv_active, GREEN if tv_active > 0 else DIM)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("LIVE GUARDIAN STATE")
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    locks = AUTO_TRADE.get("liveProfitLocks", {})
    kv("active_locks", len(locks))
    if locks:
        for k, v in locks.items():
            if not isinstance(v, dict):
                continue
            sym = v.get("symbol", "?")
            side = v.get("side", "?")
            armed = v.get("armed", False)
            peak = v.get("peak", 0)
            tp = v.get("tp", 0)
            sl = v.get("sl", 0)
            ext = v.get("tpExtensionCount", 0)
            entry = v.get("entryMark", 0)
            mark = v.get("markPrice", 0)
            upnl = v.get("unRealizedProfit", 0)
            be_armed = v.get("breakevenGuardArmed", False)

            side_color = GREEN if side == "LONG" else RED
            print(f"\n  {BOLD}{sym}{RESET} {side_color}{side}{RESET}")
            print(f"    entry={entry:.6f}  mark={mark:.6f}  uPnL={upnl:.4f}")
            print(f"    TP={tp:.6f}  SL={sl:.6f}  peak={peak:.4f}")
            print(f"    armed={armed}  be_armed={be_armed}  ext_count={ext}")
    else:
        print(f"  {DIM}(no open positions){RESET}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("DATA FLOW: TV → Entry → Guardian")
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"""
  {BOLD}① SIGNAL GENERATION (Confluence){RESET}
  ┌─────────────────────────────────────────────────────────────────┐
  │  intel_pipeline.py → evaluate_confluence()                     │
  │  └─ tv_client.get_signal(symbol) ──→ cache check ──→ API call  │
  │  └─ tv_client.confirm_signal() ──→ +0.0~0.05 confidence boost  │
  └─────────────────────────────────┬───────────────────────────────┘
                                    │ signal + confidence
                                    ▼
  {BOLD}② ENTRY PIPELINE (22 Gates){RESET}
  ┌─────────────────────────────────────────────────────────────────┐
  │  pipeline.py → evaluate_entry_plan(EntryInputs)                │
  │  signal→spread→confidence→momentum→HTF→EMA200→funding→         │
  │  slippage→order_flow→risk_reward→fee_edge→approved             │
  │  TV impact: only through confidence boost (max +0.05)          │
  └─────────────────────────────────┬───────────────────────────────┘
                                    │ APPROVED
                                    ▼
  {BOLD}③ GUARDIAN (5 Phases){RESET}
  ┌─────────────────────────────────────────────────────────────────┐
  │  Phase 3.5: async_get_position_guidance() ──→ ONCE per cycle   │
  │  └─ _tv_results dict ──→ shared across Phase 4                 │
  │                                                                 │
  │  Phase 4 decisions:                                             │
  │  ├─ TV Early Exit ──→ defer if lock+profit (strength<0.9)      │
  │  ├─ Hold Winner ──→ _extend_tp_sl_levels(tv_guidance=...)      │
  │  ├─ SL Trailing ──→ _trail_winner_levels(tv_guidance=...)      │
  │  ├─ Reversal Exit ──→ intel-based (no TV)                      │
  │  ├─ Profit Lock ──→ retrace budget close                       │
  │  └─ Weak Signal ──→ signal weakened + profit                   │
  └─────────────────────────────────────────────────────────────────┘
""")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("TV × GUARDIAN INTERACTION MATRIX")
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"""
  ┌───────────────────┬──────────┬───────────────────────────────────────┐
  │ {BOLD}TV Touchpoint{RESET}       │ {BOLD}Priority{RESET}  │ {BOLD}Effect{RESET}                                  │
  ├───────────────────┼──────────┼───────────────────────────────────────┤
  │ Confluence boost  │ Pre-etry │ +0.0~0.05 confidence                  │
  │ Early exit        │ #1       │ Close if: losing OR no lock           │
  │                   │          │ Defer if: lock armed + profit         │
  │ TP extension      │ #2       │ Extend TP +0.2~0.5%                  │
  │ SL trailing       │ #2       │ Trail SL 0.15~0.3%                   │
  └───────────────────┴──────────┴───────────────────────────────────────┘
""")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("LIVE TV TEST (top 5 symbols)")
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    if not cfg.get("tradingviewEnabled"):
        print(f"  {YELLOW}TradingView disabled — skipping live test{RESET}")
    else:
        from trading.tradingview_mcp import async_get_position_guidance
        test_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
        for sym in test_symbols:
            try:
                guidance = await async_get_position_guidance(tv, sym, "LONG")
                if guidance:
                    rec = guidance.get("recommendation", "?")
                    sig = guidance.get("signal", "?")
                    strength = guidance.get("strength", 0)
                    rec_color = GREEN if rec in ("STRONG_BUY", "BUY") else (RED if rec in ("STRONG_SELL", "SELL") else YELLOW)
                    print(f"  {sym:12s} rec={rec_color}{rec:12s}{RESET} signal={sig:8s} strength={strength:.2f}")
                else:
                    print(f"  {sym:12s} {DIM}(no data){RESET}")
            except Exception as e:
                print(f"  {sym:12s} {RED}error: {e}{RESET}")
            await asyncio.sleep(0.1)  # rate limit

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("API ENDPOINTS")
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"  GET  /health                  System health")
    print(f"  GET  /tradingview/health      TV client health + config")
    print(f"  GET  /learning/status         Per-symbol learning stats")
    print(f"  GET  /learning/status?sym=X   Single symbol stats")
    print(f"  GET  /debug/binance-positions Live Binance positions")

    print(f"\n{DIM}{'═'*72}{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
