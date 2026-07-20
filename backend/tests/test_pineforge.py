"""Comprehensive PineForge Engine integration tests."""
import csv
import math
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, r"E:\My Project\Binance autotrend\backend")

STRATEGY_SO = Path(r"E:\My Project\Binance autotrend\pineforge-engine\tutorial\macd\strategy.so")
CSV_PATH = Path(r"E:\My Project\Binance autotrend\pineforge-engine\tutorial\data\btcusdt_15m.csv")
PASSED = 0
FAILED = 0

def test(name, fn):
    global PASSED, FAILED
    try:
        fn()
        PASSED += 1
        print(f"  PASS  {name}")
    except Exception as e:
        FAILED += 1
        print(f"  FAIL  {name}: {e}")

def gen_synthetic_bars(n=500, seed=42):
    bars = []
    for i in range(n):
        p = 42000 + 500*math.sin(i*0.05 + seed) + 200*math.sin(i*0.13)
        bars.append({
            "open": p - 10, "high": p + 50, "low": p - 50,
            "close": p + 10, "volume": 1000 + i,
            "timestamp": 1700000000000 + i * 900000,
        })
    return bars

# ── 1. Engine version + ABI ──────────────────────────────────────────
print("\n[1] Engine version + ABI check")

def test_version():
    from analysis.pineforge_adapter import PineForgeEngine
    e = PineForgeEngine()
    assert e is not None
test("PineForgeEngine() instantiates", test_version)

def test_abi():
    from analysis.pineforge_adapter import PineForgeEngine, EXPECTED_PF_ABI
    e = PineForgeEngine()
    import ctypes
    lib = ctypes.CDLL(str(STRATEGY_SO))
    lib.pf_abi_version.restype = ctypes.c_int
    assert lib.pf_abi_version() == EXPECTED_PF_ABI
test("ABI version = 2", test_abi)

# ── 2. OHLCV conversion ─────────────────────────────────────────────
print("\n[2] OHLCV conversion")

def test_ohlcv_to_bars():
    from analysis.pineforge_adapter import PineForgeEngine, BarC
    bars_dict = gen_synthetic_bars(10)
    bars, n = PineForgeEngine.ohlcv_to_bars(bars_dict)
    assert n == 10
    assert isinstance(bars[0], BarC)
    assert bars[0].open == bars_dict[0]["open"]
    assert bars[9].timestamp == bars_dict[9]["timestamp"]
test("ohlcv_to_bars (10 bars)", test_ohlcv_to_bars)

def test_ohlcv_empty():
    from analysis.pineforge_adapter import PineForgeEngine
    bars, n = PineForgeEngine.ohlcv_to_bars([])
    assert n == 0
test("ohlcv_to_bars (empty)", test_ohlcv_empty)

def test_ohlcv_large():
    from analysis.pineforge_adapter import PineForgeEngine
    bars_dict = gen_synthetic_bars(5000)
    bars, n = PineForgeEngine.ohlcv_to_bars(bars_dict)
    assert n == 5000
    assert bars[0].open == bars_dict[0]["open"]
    assert bars[4999].close == bars_dict[4999]["close"]
test("ohlcv_to_bars (5000 bars)", test_ohlcv_large)

def test_bars_to_ohlcv():
    from analysis.pineforge_adapter import PineForgeEngine
    bars_dict = gen_synthetic_bars(5)
    bars, n = PineForgeEngine.ohlcv_to_bars(bars_dict)
    result = PineForgeEngine.bars_to_ohlcv(bars, n)
    assert len(result) == 5
    for i in range(5):
        assert result[i]["open"] == bars_dict[i]["open"]
        assert result[i]["high"] == bars_dict[i]["high"]
        assert result[i]["low"] == bars_dict[i]["low"]
        assert result[i]["close"] == bars_dict[i]["close"]
        assert result[i]["volume"] == bars_dict[i]["volume"]
        assert result[i]["timestamp"] == bars_dict[i]["timestamp"]
test("bars_to_ohlcv roundtrip", test_bars_to_ohlcv)

# ── 3. Backtest with tutorial MACD ───────────────────────────────────
print("\n[3] Backtest with tutorial MACD strategy")

def test_basic_backtest():
    from analysis.pineforge_adapter import PineForgeEngine
    e = PineForgeEngine()
    result = e.backtest(
        bars_ohlcv=gen_synthetic_bars(500),
        strategy_so=STRATEGY_SO,
        inputs={"Fast Length": 12, "Slow Length": 26, "Signal Smoothing": 9},
        overrides={"initial_capital": 100000, "commission_value": 0.04, "default_qty_value": 1},
    )
    assert result["num_trades"] > 0, "Expected trades, got 0"
    assert result["net_profit"] != 0, "Expected non-zero PnL"
    assert 0 <= result["percent_profitable"] <= 100
    assert "trades" in result and len(result["trades"]) > 0
    assert result["sharpe"] != 0 or result["num_trades"] < 3
test("basic backtest produces trades", test_basic_backtest)

def test_backtest_result_fields():
    from analysis.pineforge_adapter import PineForgeEngine
    e = PineForgeEngine()
    result = e.backtest(
        bars_ohlcv=gen_synthetic_bars(500),
        strategy_so=STRATEGY_SO,
    )
    required_fields = [
        "net_profit", "num_trades", "percent_profitable", "profit_factor",
        "avg_trade", "avg_win", "avg_loss", "largest_win", "largest_loss",
        "max_equity_drawdown", "sharpe", "sortino", "calmar",
        "trades", "long_trades", "short_trades",
        "input_bars_processed", "script_bars_processed",
    ]
    for field in required_fields:
        assert field in result, f"Missing field: {field}"
test("result has all required fields", test_backtest_result_fields)

def test_backtest_trades_structure():
    from analysis.pineforge_adapter import PineForgeEngine
    e = PineForgeEngine()
    result = e.backtest(
        bars_ohlcv=gen_synthetic_bars(500),
        strategy_so=STRATEGY_SO,
        inputs={"Fast Length": 12, "Slow Length": 26, "Signal Smoothing": 9},
        overrides={"initial_capital": 100000, "commission_value": 0.04},
    )
    for t in result["trades"]:
        assert "entry_time" in t and "exit_time" in t
        assert "entry_price" in t and "exit_price" in t
        assert "pnl" in t and "pnl_pct" in t
        assert "is_long" in t
        assert "qty" in t and t["qty"] > 0
        assert t["entry_time"] > 0
        assert t["exit_time"] > t["entry_time"]
test("trade records have correct structure", test_backtest_trades_structure)

def test_backtest_with_csv_data():
    from analysis.pineforge_adapter import PineForgeEngine
    if not CSV_PATH.exists():
        print("  SKIP  (CSV not found)")
        return
    e = PineForgeEngine()
    with open(CSV_PATH) as f:
        reader = csv.DictReader(f)
        bars = [{"open": float(r["open"]), "high": float(r["high"]),
                 "low": float(r["low"]), "close": float(r["close"]),
                 "volume": float(r["volume"]), "timestamp": int(r["timestamp"])}
                for r in reader]
    result = e.backtest(
        bars_ohlcv=bars,
        strategy_so=STRATEGY_SO,
        inputs={"Fast Length": 12, "Slow Length": 26, "Signal Smoothing": 9},
        overrides={"initial_capital": 100000, "commission_value": 0.04},
    )
    assert result["num_trades"] >= 0
    assert isinstance(result["net_profit"], float)
test("backtest with BTCUSDT 15m CSV", test_backtest_with_csv_data)

# ── 4. Parameter sweep ───────────────────────────────────────────────
print("\n[4] Parameter sweep")

def test_parameter_sweep():
    from analysis.pineforge_adapter import PineForgeEngine
    e = PineForgeEngine()
    bars = gen_synthetic_bars(500)
    configs = [
        {"Fast Length": 8, "Slow Length": 21, "Signal Smoothing": 9},
        {"Fast Length": 12, "Slow Length": 26, "Signal Smoothing": 9},
        {"Fast Length": 19, "Slow Length": 39, "Signal Smoothing": 9},
        {"Fast Length": 26, "Slow Length": 52, "Signal Smoothing": 9},
    ]
    results = []
    for inputs in configs:
        r = e.backtest(
            bars_ohlcv=bars, strategy_so=STRATEGY_SO,
            inputs=inputs,
            overrides={"initial_capital": 100000, "commission_value": 0.04},
        )
        results.append({"inputs": inputs, "net_profit": r["net_profit"],
                        "trades": r["num_trades"], "win_rate": r["percent_profitable"]})
    assert len(results) == 4
    assert all(r["trades"] >= 0 for r in results)
    # At least one config should have different results (proving sweep works)
    profits = [r["net_profit"] for r in results]
    assert len(set(profits)) > 1 or all(p == 0 for p in profits), \
        "Sweep produced identical results — something is wrong"
test("4-config parameter sweep", test_parameter_sweep)

def test_commission_sensitivity():
    from analysis.pineforge_adapter import PineForgeEngine
    e = PineForgeEngine()
    bars = gen_synthetic_bars(500)
    r_low = e.backtest(bars_ohlcv=bars, strategy_so=STRATEGY_SO,
                       overrides={"commission_value": 0.01})
    r_high = e.backtest(bars_ohlcv=bars, strategy_so=STRATEGY_SO,
                        overrides={"commission_value": 0.10})
    # Higher commission should produce lower net profit
    if r_low["num_trades"] > 0 and r_high["num_trades"] > 0:
        assert r_high["net_profit"] <= r_low["net_profit"] + 1.0, \
            "Higher commission should not produce higher profit"
test("commission sensitivity", test_commission_sensitivity)

# ── 5. Error handling ────────────────────────────────────────────────
print("\n[5] Error handling")

def test_missing_strategy():
    from analysis.pineforge_adapter import PineForgeEngine
    e = PineForgeEngine()
    try:
        e.backtest(bars_ohlcv=gen_synthetic_bars(10), strategy_so=Path("nonexistent.so"))
        assert False, "Should have raised"
    except FileNotFoundError:
        pass
test("missing strategy raises FileNotFoundError", test_missing_strategy)

def test_empty_bars():
    from analysis.pineforge_adapter import PineForgeEngine
    e = PineForgeEngine()
    result = e.backtest(bars_ohlcv=[], strategy_so=STRATEGY_SO)
    assert result["num_trades"] == 0
test("empty bars = 0 trades", test_empty_bars)

# ── 6. Streaming API ─────────────────────────────────────────────────
print("\n[6] Streaming API")

def test_stream_begin_end():
    from analysis.pineforge_adapter import PineForgeEngine
    e = PineForgeEngine()
    bars = gen_synthetic_bars(50)
    session = e.stream_begin(
        strategy_so=STRATEGY_SO,
        warmup_bars=bars,
        input_tf="15",
        script_tf="15",
    )
    assert session is not None
    session.end()
    session.close()
test("stream_begin + end + close", test_stream_begin_end)

def test_stream_push_ticks():
    from analysis.pineforge_adapter import PineForgeEngine
    e = PineForgeEngine()
    bars = gen_synthetic_bars(50)
    session = e.stream_begin(strategy_so=STRATEGY_SO, warmup_bars=bars,
                               input_tf="15", script_tf="15")
    ticks = [
        {"timestamp": 1700000000000 + 50*900000, "price": 42100, "quantity": 0.01, "sequence": 1},
        {"timestamp": 1700000000000 + 50*900000 + 1000, "price": 42110, "quantity": 0.02, "sequence": 2},
    ]
    ret = session.push_ticks(ticks)
    assert ret == 0
    ret = session.advance_time(1700000000000 + 51*900000)
    assert ret == 0
    session.end()
    report = session.fill_report()
    assert "num_trades" in report
    session.close()
test("stream push_ticks + advance_time + fill_report", test_stream_push_ticks)

# ── 7. Backend API endpoints ─────────────────────────────────────────
print("\n[7] Backend API endpoints")

def test_backend_imports():
    from main import app
    assert app is not None
test("main app imports", test_backend_imports)

def test_backend_health():
    from fastapi.testclient import TestClient
    from main import app
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
test("/health endpoint", test_backend_health)

# ── 8. Performance ───────────────────────────────────────────────────
print("\n[8] Performance benchmark")

def test_backtest_speed():
    from analysis.pineforge_adapter import PineForgeEngine
    e = PineForgeEngine()
    bars = gen_synthetic_bars(1000)
    t0 = time.perf_counter()
    N = 10
    for _ in range(N):
        e.backtest(
            bars_ohlcv=bars, strategy_so=STRATEGY_SO,
            inputs={"Fast Length": 12, "Slow Length": 26, "Signal Smoothing": 9},
            overrides={"initial_capital": 100000, "commission_value": 0.04},
        )
    elapsed = time.perf_counter() - t0
    avg_ms = elapsed / N * 1000
    print(f"    avg backtest time: {avg_ms:.1f}ms (1000 bars, {N} runs)")
    assert avg_ms < 10000, f"Too slow: {avg_ms:.0f}ms"
test("1000-bar backtest < 10s", test_backtest_speed)

def test_sweep_speed():
    from analysis.pineforge_adapter import PineForgeEngine
    e = PineForgeEngine()
    bars = gen_synthetic_bars(500)
    configs = [{"Fast Length": f, "Slow Length": s, "Signal Smoothing": 9}
               for f, s in [(8,21),(12,26),(19,39),(26,52)]]
    t0 = time.perf_counter()
    for inputs in configs:
        e.backtest(bars_ohlcv=bars, strategy_so=STRATEGY_SO, inputs=inputs,
                   overrides={"initial_capital": 100000, "commission_value": 0.04})
    elapsed = time.perf_counter() - t0
    print(f"    4-config sweep on 500 bars: {elapsed*1000:.1f}ms total")
test("4-config sweep speed", test_sweep_speed)

# ── Summary ──────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"  RESULTS: {PASSED} passed, {FAILED} failed, {PASSED+FAILED} total")
print(f"{'='*60}")
sys.exit(1 if FAILED else 0)
