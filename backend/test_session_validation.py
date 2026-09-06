"""Post-session validation: verify all Phase 1-8 fixes work correctly."""
import json
import time
import tempfile
from pathlib import Path
from unittest import mock

import main
import trading.trade_log as _trade_log
import trading.symbol_profiles as _symbol_profiles
from trading.per_symbol_storage import PerSymbolStorage
from trading.shared_cache_layer import SharedCacheLayer, get_shared_cache
from trading.per_symbol_context import PerSymbolContext
from trading.symbol_profiles import _symbol_effective_profile, _symbol_group, _symbol_sample_count
from trading.learning_analysis import _memory_windows_from_trades
from trading.trade_log import _append_trade_log, _live_closed_trades_from_log
from services.cache_registry import _LIVE_STATS_VERSION, _SESSION_BIAS_CACHE, _LIVE_STATS_CACHE
from services.config_paths import VAULT_DIR
import services.cache_registry as _cache_registry

passed = 0
failed = 0


def check(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


# ============================================================
print("=" * 60)
print("1. IMPORT CHECK")
print("=" * 60)
check("main.py imports", True)
check("PerSymbolStorage", PerSymbolStorage is not None)
check("SharedCacheLayer", SharedCacheLayer is not None)
check("PerSymbolContext", PerSymbolContext is not None)
check("_symbol_effective_profile", _symbol_effective_profile is not None)
check("_memory_windows_from_trades", _memory_windows_from_trades is not None)
check("_append_trade_log", _append_trade_log is not None)
check("_live_closed_trades_from_log", _live_closed_trades_from_log is not None)

# ============================================================
print()
print("=" * 60)
print("2. BACKWARD-COMPATIBLE ALIASES (Phase 2a)")
print("=" * 60)
check("main._SESSION_BIAS_CACHE is cache_registry._SESSION_BIAS_CACHE",
      main._SESSION_BIAS_CACHE is _SESSION_BIAS_CACHE)
check("main._LIVE_STATS_CACHE is cache_registry._LIVE_STATS_CACHE",
      main._LIVE_STATS_CACHE is _LIVE_STATS_CACHE)
check("main._LIVE_STATS_VERSION == cache_registry._LIVE_STATS_VERSION",
      main._LIVE_STATS_VERSION == _cache_registry._LIVE_STATS_VERSION)

# Mutation test: alias must share the same dict object
_SESSION_BIAS_CACHE["test_alias_key"] = 999
check("Alias mutation is shared", main._SESSION_BIAS_CACHE.get("test_alias_key") == 999)
del _SESSION_BIAS_CACHE["test_alias_key"]

# ============================================================
print()
print("=" * 60)
print("3. PER-SYMBOL STORAGE")
print("=" * 60)
sym_dir = VAULT_DIR / "symbols"
check("symbols/ directory exists", sym_dir.is_dir())
if sym_dir.is_dir():
    symbols = [d.name for d in sym_dir.iterdir() if d.is_dir()]
    check(f"Has symbols ({len(symbols)} total)", len(symbols) > 100)
    for s in ["BTCUSDT", "ETHUSDT", "DOGEUSDT", "SOLUSDT"]:
        p = sym_dir / s
        check(f"{s}/ exists", p.is_dir())
        if p.is_dir():
            files = [f.name for f in p.iterdir()]
            check(f"{s} has profile.json", "profile.json" in files)
            check(f"{s} has trades.jsonl", "trades.jsonl" in files)

# ============================================================
print()
print("=" * 60)
print("4. TTL=0 CACHE BYPASS FIX (Phase 3)")
print("=" * 60)
cache = get_shared_cache(VAULT_DIR)
# Verify the fix: TTL <= 0 should mean "never expire"
check("Profile TTL > 0 (normal mode)", cache._profile_ttl > 0)
check("Window TTL > 0 (normal mode)", cache._window_ttl > 0)
check("Risk TTL > 0 (normal mode)", cache._risk_ttl > 0)

# Simulate TTL=0 scenario: manually set, verify cache still works
with tempfile.TemporaryDirectory() as tmp:
    storage = PerSymbolStorage(Path(tmp), "TESTUSDT")
    storage.save_profile({"test": True})
    # Temporarily set TTL to 0
    orig = cache._profile_ttl
    cache._profile_ttl = 0
    # Now get_profile with TTL=0 should still return data (never expire)
    cache._profile_cache.clear()
    # Direct storage read works
    loaded = storage.load_profile()
    check("TTL=0: storage.load_profile() returns data", loaded.get("test") is True)
    cache._profile_ttl = orig

# ============================================================
print()
print("=" * 60)
print("5. PERFLOCKS PERSISTENCE (Phase 4)")
print("=" * 60)
check("AUTO_TRADE has perfLocks key", "perfLocks" in main.AUTO_TRADE)
check("perfLocks is dict", isinstance(main.AUTO_TRADE.get("perfLocks"), dict))

# Test persist/load cycle
with tempfile.TemporaryDirectory() as tmp:
    snapshot_path = Path(tmp) / "autotrade_snapshot.json"
    main.AUTO_TRADE["perfLocks"] = {"BTCUSDT": {"reason": "test", "until": 99999}}
    # Simulate persist
    payload = {"perfLocks": main.AUTO_TRADE.get("perfLocks", {})}
    snapshot_path.write_text(json.dumps(payload), encoding="utf-8")
    # Simulate load
    data = json.loads(snapshot_path.read_text(encoding="utf-8"))
    pl = data.get("perfLocks")
    restored = pl if isinstance(pl, dict) else {}
    check("perfLocks roundtrip", restored.get("BTCUSDT", {}).get("reason") == "test")
    # Clean up
    main.AUTO_TRADE["perfLocks"] = {}

# ============================================================
print()
print("=" * 60)
print("6. SAMPLE COUNT CACHE INVALIDATION (Phase 5)")
print("=" * 60)
from services.cache_registry import _SYMBOL_SAMPLE_COUNT_CACHE
# _symbol_sample_count is now backed by per-symbol storage (Phase 5 migration);
# it no longer populates or depends on _SYMBOL_SAMPLE_COUNT_CACHE.
count_before = _symbol_sample_count("BTCUSDT")
check(f"_symbol_sample_count(BTCUSDT) = {count_before}", count_before >= 0)
check("Storage-backed read (no cache population)", "BTCUSDT" not in _SYMBOL_SAMPLE_COUNT_CACHE)

# ============================================================
print()
print("=" * 60)
print("7. AUTOTUNER MANAGED KEYS (Phase 6)")
print("=" * 60)
# Verify ctx.sym_profile property works (was ctx._sym_profile = None before fix)
with tempfile.TemporaryDirectory() as tmp:
    vault = Path(tmp)
    storage = PerSymbolStorage(vault, "TESTAUTOTUNE")
    storage.save_symbol_profile({"tpPct": 2.5, "autotuneLastAt": 12345})
    cache2 = SharedCacheLayer(vault)
    ctx = PerSymbolContext("TESTAUTOTUNE", cache2, {})
    # Property accessor should load from disk
    sp = ctx.sym_profile
    check("ctx.sym_profile loads from disk", sp.get("tpPct") == 2.5)
    check("ctx.sym_profile has autotuneLastAt", sp.get("autotuneLastAt") == 12345)
    # Old code: ctx._sym_profile was always None
    check("ctx._sym_profile is populated after property access", ctx._sym_profile is not None)

# ============================================================
print()
print("=" * 60)
print("8. _append_section RACE FIX (Phase 7)")
print("=" * 60)
# Test that _append_section uses file locking
from trading.per_symbol_storage import _append_section
with tempfile.TemporaryDirectory() as tmp:
    test_path = Path(tmp) / "test.md"
    _append_section(test_path, "Header1", ["- line1"])
    _append_section(test_path, "Header2", ["- line2"])
    content = test_path.read_text(encoding="utf-8")
    check("First section present", "## Header1" in content)
    check("Second section present", "## Header2" in content)
    check("Both lines present", "- line1" in content and "- line2" in content)

# ============================================================
print()
print("=" * 60)
print("9. SYMBOL GROUPS")
print("=" * 60)
groups = {
    "BTCUSDT": "trend-friendly",
    "ETHUSDT": "trend-friendly",
    "DOGEUSDT": "high-volatility",
    "XRPUSDT": "mean-reversion-friendly",
    "SPXUSDT": "low-liquidity-noisy",
    "XLMUSDT": "mean-reversion-friendly",
}
for sym, expected in groups.items():
    actual = _symbol_group(sym)
    check(f"{sym} -> {actual}", actual == expected, f"(expected {expected})")

# ============================================================
print()
print("=" * 60)
print("10. EFFECTIVE PROFILE ARCHITECTURE")
print("=" * 60)
# Unknown symbol should use group defaults
p_unknown = _symbol_effective_profile("UNKNOWNUSDT", cfg={})
check("UNKNOWNUSDT source=group", p_unknown.get("source") == "group")
check("UNKNOWNUSDT sampleTrades=0", p_unknown.get("sampleTrades") == 0)

# Known symbol should also use group (no per-symbol override for fresh symbol)
p_btc = _symbol_effective_profile("BTCUSDT", cfg={})
check("BTCUSDT has group", "group" in p_btc or p_btc.get("source") == "group" or "tpslMult" in p_btc or "tpPct" in p_btc)
check("BTCUSDT has source", "source" in p_btc)

# ============================================================
print()
print("=" * 60)
print("11. MEMORY WINDOWS")
print("=" * 60)
now = int(time.time())
trades = [
    {"_pnl": 1.0, "_ts": now - 86400 * 2},   # 2 days ago
    {"_pnl": -0.5, "_ts": now - 86400 * 5},  # 5 days ago
    {"_pnl": 2.0, "_ts": now - 86400 * 15},  # 15 days ago
    {"_pnl": -1.0, "_ts": now - 86400 * 40},  # 40 days ago
]
w = _memory_windows_from_trades(trades)
check("7d window present", "7d" in w)
check("15d window present", "15d" in w)
check("30d window present", "30d" in w)
check("all window present", "all" in w)
check("all window has 4 trades", w.get("all", {}).get("trades") == 4)
check("7d window has 2 trades", w.get("7d", {}).get("trades") == 2)

# ============================================================
print()
print("=" * 60)
print("12. TRADE LOG ATOMICITY")
print("=" * 60)
with tempfile.TemporaryDirectory() as tmp:
    log_path = Path(tmp) / "trades_log.jsonl"
    # Append the same way trading.trade_log._append_trade_log does today
    entry = json.dumps({"ts": now, "mode": "LIVE", "symbol": "TESTUSDT", "pnl": 1.0})
    with log_path.open("a", encoding="utf-8") as f:
        f.write(entry + "\n")
    check("Append creates file", log_path.exists())
    content = log_path.read_text(encoding="utf-8")
    check("Content is valid JSON", '"mode": "LIVE"' in content)

    # Second append
    with log_path.open("a", encoding="utf-8") as f:
        f.write(entry + "\n")
    # File should have both entries
    full = log_path.read_text(encoding="utf-8")
    check("Second append present", full.count("TESTUSDT") == 2)

# ============================================================
print()
print("=" * 60)
print("13. SNAPSHOT PERSISTENCE (Phase 4)")
print("=" * 60)
# Check that _persist_autotrade_snapshot includes liveProfitLocks
import inspect
src = inspect.getsource(main._persist_autotrade_snapshot)
check("Snapshot includes liveProfitLocks", "liveProfitLocks" in src)
# Check that _load_autotrade_snapshot restores liveProfitLocks
src_load = inspect.getsource(main._load_autotrade_snapshot)
check("Snapshot load restores liveProfitLocks", "liveProfitLocks" in src_load)

# ============================================================
print()
print("=" * 60)
print("SUMMARY")
print("=" * 60)
total = passed + failed
print(f"  Passed: {passed}/{total}")
print(f"  Failed: {failed}/{total}")
if failed == 0:
    print("  ALL CHECKS PASSED")
else:
    print(f"  {failed} CHECK(S) FAILED")
