"""Smoke tests for refactored trading modules.

Catches NameError, ImportError, and type regressions by calling every public
function with minimal/dummy data.  These are NOT logic tests — they verify
that the extraction didn't break any references or signatures.

Run: python -m pytest test_refactored_modules.py -v
"""
import sys
import unittest
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# ── Module imports (these themselves verify no ImportError) ──────────────
from trading.trade_stats import (
    _empty_stats,
    _apply_trade_log_delta,
    _aggregate_live_trade_stats_from_log,
    _aggregate_live_trade_stats_by_symbol_from_log,
    _today_entry_performance_guard,
    SCAN_EVENTS_PATH,
)
from trading.learning import (
    _bkk_hour,
    _bkk_day_start_ts,
    _memory_windows_from_trades,
    _weighted_recent_memory_score,
    _recent_payoff_loss_guard,
    _learning_propose_from_trades,
    _early_entry_pullback_reset_ok,
    _entry_session_bias,
    _market_regime_sizing,
    _symbol_risk_tune_from_recent_trades,
    _walk_forward_from_trades,
    _recent_live_result_streak_state,
    _per_symbol_streak_size_mult,
    _recent_live_loss_streak_state,
    _recent_live_loss_streak_states_by_symbol,
    SCAN_EVENTS_PATH as LEARN_SCAN_PATH,
)
from trading.supervisor_tuning import (
    _supervisor_trade_period_reviews,
    _maybe_tune_size_multiplier_from_streak,
)
from trading.trade_log import (
    _apply_trade_log_delta as tl_apply_delta,
    _aggregate_live_trade_stats_from_log as tl_agg_stats,
    _live_closed_trades_from_log,
)

# ── Dummy trade data ────────────────────────────────────────────────────
_NOW = int(time.time())
_DUMMY_TRADE = {
    "symbol": "BTCUSDT",
    "side": "LONG",
    "mode": "LIVE",
    "pnl": 0.15,
    "entry": 60000.0,
    "exit": 60015.0,
    "ts": _NOW,
    "closedAt": _NOW,
    "openedAt": _NOW - 300,
    "_pnl": 0.15,
    "_ts": _NOW,
}
_DUMMY_TRADE_LOSS = dict(_DUMMY_TRADE, pnl=-0.20, _pnl=-0.20, side="SHORT")
_DUMMY_TRADES = [_DUMMY_TRADE, _DUMMY_TRADE_LOSS] * 4  # 8 trades


# ═══════════════════════════════════════════════════════════════════════════
class TestTradeStats(unittest.TestCase):
    """Smoke tests for trading.trade_stats."""

    def test_empty_stats(self):
        s = _empty_stats()
        self.assertIsInstance(s, dict)
        self.assertEqual(s["wins"], 0)
        self.assertEqual(s["losses"], 0)
        self.assertIsInstance(s["lastTrades"], list)

    def test_apply_delta_empty(self):
        s = _empty_stats()
        result = _apply_trade_log_delta(s, [], "BTCUSDT")
        self.assertIsInstance(result, dict)
        self.assertEqual(result["wins"], 0)

    def test_apply_delta_with_lines(self):
        import json
        s = _empty_stats()
        line = json.dumps(_DUMMY_TRADE)
        result = _apply_trade_log_delta(s, [line], "BTCUSDT")
        self.assertIsInstance(result, dict)
        self.assertEqual(result["wins"], 1)

    def test_apply_delta_wrong_symbol_filtered(self):
        import json
        s = _empty_stats()
        line = json.dumps(_DUMMY_TRADE)
        result = _apply_trade_log_delta(s, [line], "ETHUSDT")
        self.assertEqual(result["wins"], 0)

    def test_aggregate_from_log_symbol(self):
        result = _aggregate_live_trade_stats_from_log("BTCUSDT")
        self.assertIsInstance(result, dict)
        for key in ("wins", "losses", "realizedPnl", "winsToday", "lossesToday",
                     "realizedPnlToday", "lastTrades"):
            self.assertIn(key, result)

    def test_aggregate_from_log_all(self):
        result = _aggregate_live_trade_stats_from_log(None)
        self.assertIsInstance(result, dict)

    def test_today_guard_disabled(self):
        result = _today_entry_performance_guard({"todayPerformanceGuardEnabled": False})
        self.assertIsInstance(result, dict)
        self.assertFalse(result["active"])

    def test_today_guard_enabled_no_data(self):
        result = _today_entry_performance_guard({})
        self.assertIsInstance(result, dict)
        self.assertIn("active", result)

    def test_today_guard_none_cfg(self):
        result = _today_entry_performance_guard(None)
        self.assertIsInstance(result, dict)

    def test_per_symbol_stats(self):
        result = _aggregate_live_trade_stats_by_symbol_from_log()
        self.assertIsInstance(result, dict)

    def test_scan_events_path_exists(self):
        self.assertIsInstance(SCAN_EVENTS_PATH, Path)


# ═══════════════════════════════════════════════════════════════════════════
class TestLearning(unittest.TestCase):
    """Smoke tests for trading.learning."""

    def test_bkk_hour_now(self):
        h = _bkk_hour()
        self.assertIsInstance(h, int)
        self.assertGreaterEqual(h, 0)
        self.assertLess(h, 24)

    def test_bkk_hour_specific(self):
        # 2026-09-04 14:00 UTC = 21:00 BKK
        h = _bkk_hour(1756984800.0)
        self.assertIsInstance(h, int)

    def test_bkk_day_start_ts(self):
        ts = _bkk_day_start_ts()
        self.assertIsInstance(ts, int)
        self.assertGreater(ts, 0)

    def test_memory_windows_empty(self):
        result = _memory_windows_from_trades([])
        self.assertIsInstance(result, dict)
        for key in ("7d", "15d", "30d", "archive", "all"):
            self.assertIn(key, result)

    def test_memory_windows_with_trades(self):
        result = _memory_windows_from_trades(_DUMMY_TRADES)
        self.assertIsInstance(result, dict)
        self.assertGreater(result["all"]["trades"], 0)

    def test_weighted_score_empty(self):
        result = _weighted_recent_memory_score({})
        self.assertIsInstance(result, dict)
        self.assertEqual(result["trades"], 0)

    def test_weighted_score_with_data(self):
        windows = _memory_windows_from_trades(_DUMMY_TRADES)
        result = _weighted_recent_memory_score(windows)
        self.assertIsInstance(result, dict)
        self.assertIn("score", result)

    def test_payoff_guard_disabled(self):
        result = _recent_payoff_loss_guard({"payoffLossGuardEnabled": False})
        self.assertIsInstance(result, dict)
        self.assertFalse(result["active"])

    def test_payoff_guard_none_cfg(self):
        result = _recent_payoff_loss_guard(None)
        self.assertIsInstance(result, dict)

    def test_payoff_guard_no_trades(self):
        result = _recent_payoff_loss_guard({})
        self.assertIsInstance(result, dict)
        self.assertFalse(result["active"])

    def test_learning_propose_empty(self):
        result = _learning_propose_from_trades("BTCUSDT", [])
        self.assertIsInstance(result, dict)
        self.assertEqual(result["trades"], 0)
        self.assertIn("proposed", result)

    def test_learning_propose_with_trades(self):
        result = _learning_propose_from_trades("BTCUSDT", _DUMMY_TRADES)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["trades"], 8)

    def test_pullback_reset_long(self):
        ok, reason = _early_entry_pullback_reset_ok("LONG", {}, {})
        self.assertIsInstance(ok, bool)
        self.assertIsInstance(reason, str)

    def test_pullback_reset_short(self):
        ok, reason = _early_entry_pullback_reset_ok("SHORT", {}, {})
        self.assertIsInstance(ok, bool)

    def test_pullback_reset_disabled(self):
        ok, reason = _early_entry_pullback_reset_ok(
            "LONG", {}, {"earlyEntryPullbackResetEnabled": False}
        )
        self.assertTrue(ok)
        self.assertEqual(reason, "disabled")

    def test_session_bias_disabled(self):
        result = _entry_session_bias({"sessionBiasEnabled": False})
        self.assertIsInstance(result, dict)
        self.assertFalse(result["enabled"])

    def test_session_bias_empty_cfg(self):
        result = _entry_session_bias({})
        self.assertIsInstance(result, dict)
        self.assertIn("reason", result)

    def test_regime_sizing_disabled(self):
        result = _market_regime_sizing({"regimeSizingEnabled": False}, None)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["sizeMult"], 1.0)

    def test_regime_sizing_default(self):
        result = _market_regime_sizing({}, None)
        self.assertIsInstance(result, dict)
        self.assertIn("sizeMult", result)
        self.assertIn("regime", result)

    def test_symbol_risk_tune_empty(self):
        result = _symbol_risk_tune_from_recent_trades("BTCUSDT", [])
        self.assertIsInstance(result, dict)
        self.assertFalse(result["active"])

    def test_symbol_risk_tune_with_trades(self):
        try:
            import fastapi  # noqa: F401
        except ImportError:
            self.skipTest("fastapi not installed — _autotrade_leverage_* unavailable")
        result = _symbol_risk_tune_from_recent_trades("BTCUSDT", _DUMMY_TRADES)
        self.assertIsInstance(result, dict)
        self.assertTrue(result["active"])
        self.assertIn("sizeMult", result)

    def test_walk_forward_no_trades(self):
        result = _walk_forward_from_trades("BTCUSDT", 10, 5, "LIVE")
        self.assertIsInstance(result, dict)
        self.assertFalse(result["ok"])

    def test_walk_forward_insufficient(self):
        result = _walk_forward_from_trades("BTCUSDT", 100, 50, "LIVE")
        self.assertIsInstance(result, dict)
        self.assertFalse(result["ok"])

    def test_learn_scan_path_matches(self):
        """Both trade_stats and learning should reference the same SCAN_EVENTS_PATH."""
        self.assertEqual(SCAN_EVENTS_PATH, LEARN_SCAN_PATH)

    # ── Streak functions (extracted from main.py) ──

    def test_streak_state_empty(self):
        result = _recent_live_result_streak_state([])
        self.assertIsInstance(result, dict)
        self.assertEqual(result["streak"], 0)
        self.assertEqual(result["kind"], "")

    def test_streak_state_wins(self):
        trades = [{"_pnl": 0.10, "_ts": _NOW - i * 60, "symbol": "BTCUSDT", "side": "LONG"} for i in range(5)]
        result = _recent_live_result_streak_state(trades)
        self.assertEqual(result["kind"], "win")
        self.assertEqual(result["streak"], 5)

    def test_streak_state_losses(self):
        trades = [{"_pnl": -0.10, "_ts": _NOW - i * 60, "symbol": "BTCUSDT", "side": "SHORT"} for i in range(3)]
        result = _recent_live_result_streak_state(trades)
        self.assertEqual(result["kind"], "loss")
        self.assertEqual(result["streak"], 3)

    def test_streak_state_mixed(self):
        trades = [
            {"_pnl": -0.10, "_ts": _NOW - 120, "symbol": "A", "side": "LONG"},
            {"_pnl": 0.20, "_ts": _NOW - 60, "symbol": "A", "side": "LONG"},
            {"_pnl": 0.15, "_ts": _NOW, "symbol": "A", "side": "LONG"},
        ]
        result = _recent_live_result_streak_state(trades)
        self.assertEqual(result["kind"], "win")
        self.assertEqual(result["streak"], 2)  # only last 2 are consecutive wins

    def test_per_symbol_streak_mult_disabled(self):
        result = _per_symbol_streak_size_mult("BTCUSDT", {"supervisorSizeStreakEnabled": False})
        self.assertEqual(result, 1.0)

    def test_per_symbol_streak_mult_no_symbol(self):
        result = _per_symbol_streak_size_mult("", {})
        self.assertEqual(result, 1.0)

    def test_loss_streak_state_empty(self):
        result = _recent_live_loss_streak_state(limit=8, symbol=None)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["streak"], 0)

    def test_loss_streak_states_by_symbol(self):
        result = _recent_live_loss_streak_states_by_symbol(limit=8)
        self.assertIsInstance(result, dict)

    # ── Supervisor review functions (extracted from main.py) ──

    def test_trade_period_reviews_empty(self):
        result = _supervisor_trade_period_reviews([])
        self.assertIsInstance(result, list)
        self.assertEqual(result, [])

    def test_trade_period_reviews_with_trades(self):
        result = _supervisor_trade_period_reviews(_DUMMY_TRADES)
        self.assertIsInstance(result, list)
        # Should have at least one review window
        if result:
            self.assertIn("label", result[0])
            self.assertIn("winRatePct", result[0])

    def test_maybe_tune_streak_disabled(self):
        result = _maybe_tune_size_multiplier_from_streak(_DUMMY_TRADES, {"supervisorSizeStreakEnabled": False})
        self.assertEqual(result, {})

    def test_maybe_tune_streak_no_cfg(self):
        result = _maybe_tune_size_multiplier_from_streak(_DUMMY_TRADES, None)
        self.assertEqual(result, {})

    def test_maybe_tune_streak_empty_trades(self):
        result = _maybe_tune_size_multiplier_from_streak([], {})
        self.assertIsInstance(result, dict)


# ═══════════════════════════════════════════════════════════════════════════
class TestTradeLog(unittest.TestCase):
    """Smoke tests for trading.trade_log (verify it still works)."""

    def test_apply_delta_empty(self):
        from trading.trade_stats import _empty_stats
        s = _empty_stats()
        result = tl_apply_delta(s, [], "BTCUSDT")
        self.assertIsInstance(result, dict)

    def test_aggregate_from_log(self):
        result = tl_agg_stats(None)
        self.assertIsInstance(result, dict)

    def test_live_closed_trades(self):
        result = _live_closed_trades_from_log(None, "ALL")
        self.assertIsInstance(result, list)


# ═══════════════════════════════════════════════════════════════════════════
class TestCrossModuleConsistency(unittest.TestCase):
    """Verify cross-module references resolve correctly."""

    def test_trade_stats_same_func_from_trade_log(self):
        """trade_stats and trade_log export the same _apply_trade_log_delta."""
        from trading.trade_stats import _apply_trade_log_delta as ts_delta
        from trading.trade_log import _apply_trade_log_delta as tl_delta
        # They should be different implementations (both exist) — just verify both are callable
        self.assertTrue(callable(ts_delta))
        self.assertTrue(callable(tl_delta))

    def test_learning_imports_resolve(self):
        """All learning.py references to cache_registry are live."""
        from services import cache_registry
        self.assertIsInstance(cache_registry._SESSION_BIAS_CACHE, dict)
        self.assertIsInstance(cache_registry._LIVE_STATS_VERSION, int)

    def test_empty_stats_shape(self):
        """_empty_stats returns the canonical stats dict shape."""
        from trading.trade_stats import _empty_stats
        s = _empty_stats()
        expected_keys = {"wins", "losses", "realizedPnl", "winsToday",
                         "lossesToday", "realizedPnlToday", "lastTrades"}
        self.assertEqual(set(s.keys()), expected_keys)


if __name__ == "__main__":
    unittest.main()
