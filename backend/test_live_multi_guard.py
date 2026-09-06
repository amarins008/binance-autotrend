import unittest
from pathlib import Path
from unittest import mock

import main
import trading.live_guardian as lg
import trading.learning as lrn
import trading.trade_log as tl
import exchange.futures_orders as fo


class TestLiveMultiGuard(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.prev_locks = main.AUTO_TRADE.get("liveProfitLocks")
        main.AUTO_TRADE["liveProfitLocks"] = {}

    async def asyncTearDown(self):
        main.AUTO_TRADE["liveProfitLocks"] = self.prev_locks if isinstance(self.prev_locks, dict) else {}

    async def test_multi_guard_closes_adopted_long_on_local_sl(self):
        cfg = {"takeProfitPct": 1.8, "stopLossPct": 0.9, "tpTargetMinUsdt": 0.55}
        rows = [
            {
                "symbol": "GUAUSDT",
                "side": "LONG",
                "qty": 86.0,
                "entryMark": 0.6200,
                "markPrice": 0.6130,
                "unRealizedProfit": -0.6,
            }
        ]

        with mock.patch.dict(main.os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
            with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(lg, "_close_position_one_side", new=mock.AsyncMock(return_value={"ok": True})) as close_one:
                    with mock.patch.object(lg, "intel_analyze", new=mock.AsyncMock(return_value={"signal": "WAIT", "confidence": 0.5, "execution": {"momentumPct": 0.0}})) as intel:
                        with mock.patch.object(lg, "_autotrade_log"):
                            changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertTrue(changed)
        close_one.assert_awaited_once()
        intel.assert_not_awaited()
        args = close_one.await_args.args
        self.assertEqual(args[:2], ("GUAUSDT", "LONG"))
        self.assertEqual(main.AUTO_TRADE["liveProfitLocks"], {})

    async def test_multi_guard_tracks_local_tp_sl_for_open_position(self):
        cfg = {"takeProfitPct": 1.8, "stopLossPct": 0.9, "tpTargetMinUsdt": 0.55, "holdMinConfidence": 0.78}
        rows = [
            {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "qty": 148.0,
                "entryMark": 0.0980,
                "markPrice": 0.0982,
                "unRealizedProfit": 0.03,
            }
        ]

        with mock.patch.dict(main.os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
            with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(lg, "intel_analyze", new=mock.AsyncMock(return_value={"signal": "WAIT", "confidence": 0.5, "execution": {"momentumPct": 0.0}})):
                    with mock.patch.object(lg, "_close_position_one_side", new=mock.AsyncMock()) as close_one:
                        changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertFalse(changed)
        close_one.assert_not_awaited()
        lock = main.AUTO_TRADE["liveProfitLocks"]["DOGEUSDT:LONG"]
        self.assertAlmostEqual(lock["entryMark"], 0.0980)
        self.assertAlmostEqual(lock["tp"], 0.099764)
        self.assertAlmostEqual(lock["sl"], 0.097118)

    async def test_multi_guard_closes_positive_retrace_before_negative(self):
        cfg = {"takeProfitPct": 1.8, "stopLossPct": 0.9, "tpTargetMinUsdt": 0.55, "profitLockBreakevenFloorUsdt": 0.08, "holdWinners": False, "feeMinEdgeVsCostMultiple": 1.0}
        main.AUTO_TRADE["liveProfitLocks"] = {
            "DOGEUSDT:LONG": {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "entryMark": 1.0,
                "tp": 1.018,
                "sl": 0.991,
                "peak": 0.30,
                "guardianStats": {"openedAt": int(main.time.time()) - 3600},
            }
        }
        rows = [
            {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "qty": 100.0,
                "entryMark": 1.0,
                "markPrice": 1.0015,
                "notionalUsdtApprox": 100.15,
                "unRealizedProfit": 0.15,
            }
        ]

        with mock.patch.dict(main.os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
            with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(lg, "intel_analyze", new=mock.AsyncMock(return_value={"signal": "WAIT", "confidence": 0.5, "execution": {"momentumPct": 0.0}})):
                    with mock.patch.object(lg, "_close_position_one_side", new=mock.AsyncMock(return_value={"ok": True})) as close_one:
                        with mock.patch.object(lg, "_autotrade_log") as log:
                            changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertTrue(changed)
        close_one.assert_awaited_once_with("DOGEUSDT", "LONG", "k", "s", main._binance_base(), reason="BREAKEVEN_GUARD")
        self.assertEqual(main.AUTO_TRADE["liveProfitLocks"], {})
        self.assertTrue(any("BREAKEVEN_GUARD" in call.args[0] for call in log.call_args_list))

    async def test_multi_guard_uses_configured_profit_lock_trigger_and_giveback(self):
        cfg = {
            "takeProfitPct": 1.8,
            "stopLossPct": 0.9,
            "tpTargetMinUsdt": 1.20,
            "profitLockTriggerUsdt": 0.40,
            "profitLockKeepUsdt": 0.18,
            "profitLockMaxGivebackUsdt": 0.22,
            "profitLockBreakevenFloorUsdt": 0.08,
            "holdWinners": False,
            "feeMinEdgeVsCostMultiple": 1.0,
            "deadZoneExitSec": 999999,
        }
        main.AUTO_TRADE["liveProfitLocks"] = {
            "ADAUSDT:LONG": {
                "symbol": "ADAUSDT",
                "side": "LONG",
                "entryMark": 1.0,
                "tp": 1.018,
                "sl": 0.991,
                "peak": 0.50,
                "armed": True,
                "lockUsdt": 0.28,
                "guardianStats": {"openedAt": int(main.time.time()) - 3600},
            }
        }
        rows = [
            {
                "symbol": "ADAUSDT",
                "side": "LONG",
                "qty": 100.0,
                "entryMark": 1.0,
                "markPrice": 1.0025,
                "notionalUsdtApprox": 100.25,
                "unRealizedProfit": 0.25,
            }
        ]

        with mock.patch.dict(main.os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
            with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(lg, "_close_position_one_side", new=mock.AsyncMock(return_value={"ok": True})) as close_one:
                    with mock.patch.object(lg, "intel_analyze", new=mock.AsyncMock(return_value={"signal": "WAIT", "confidence": 0.5, "execution": {"momentumPct": 0.0}})):
                        with mock.patch.object(lg, "_autotrade_log") as log:
                            changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertTrue(changed)
        close_one.assert_awaited_once_with("ADAUSDT", "LONG", "k", "s", main._binance_base(), reason="RETRACE_BUDGET")
        self.assertEqual(main.AUTO_TRADE["liveProfitLocks"], {})
        self.assertTrue(any("RETRACE" in call.args[0] for call in log.call_args_list))

    async def test_multi_guard_arms_at_configured_profit_lock_trigger(self):
        cfg = {
            "takeProfitPct": 1.8,
            "stopLossPct": 0.9,
            "tpTargetMinUsdt": 1.20,
            "profitLockTriggerUsdt": 0.40,
            "profitLockKeepUsdt": 0.18,
            "profitLockMaxGivebackUsdt": 0.22,
            "holdWinners": False,
            "feeMinEdgeVsCostMultiple": 1.0,
        }
        main.AUTO_TRADE["liveProfitLocks"] = {
            "BTCUSDT:LONG": {
                "symbol": "BTCUSDT",
                "side": "LONG",
                "entryMark": 1.0,
                "tp": 1.018,
                "sl": 0.991,
                "peak": 0.40,
                "guardianStats": {"openedAt": int(main.time.time()) - 3600},
            }
        }
        rows = [
            {
                "symbol": "BTCUSDT",
                "side": "LONG",
                "qty": 100.0,
                "entryMark": 1.0,
                "markPrice": 1.004,
                "notionalUsdtApprox": 100.40,
                "unRealizedProfit": 0.40,
            }
        ]

        with mock.patch.dict(main.os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
            with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(lg, "intel_analyze", new=mock.AsyncMock(return_value={"signal": "WAIT", "confidence": 0.5, "execution": {"momentumPct": 0.0}})):
                    with mock.patch.object(lg, "_close_position_one_side", new=mock.AsyncMock()) as close_one:
                        with mock.patch.object(lg, "_autotrade_log"):
                            changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertFalse(changed)
        close_one.assert_not_awaited()
        lock = main.AUTO_TRADE["liveProfitLocks"]["BTCUSDT:LONG"]
        self.assertTrue(lock["armed"])
        self.assertAlmostEqual(lock["lockUsdt"], 0.18)

    async def test_multi_guard_extends_tp_when_signal_stays_strong(self):
        cfg = {
            "takeProfitPct": 1.8,
            "stopLossPct": 0.9,
            "tpTargetMinUsdt": 0.55,
            "holdMinConfidence": 0.72,
            "holdTrailPct": 0.35,
            "tpExtendMinConfidence": 0.78,
            "tpExtendMinScoreGap": 1.2,
            "tpExtendStepPct": 0.45,
        }
        main.AUTO_TRADE["liveProfitLocks"] = {
            "DOGEUSDT:LONG": {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "entryMark": 1.0,
                "tp": 1.018,
                "sl": 0.991,
                "peak": 1.9,
                "guardianStats": {"openedAt": int(main.time.time()) - 3600},
            }
        }
        rows = [
            {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "qty": 100.0,
                "entryMark": 1.0,
                "markPrice": 1.019,
                "notionalUsdtApprox": 101.9,
                "unRealizedProfit": 1.9,
            }
        ]
        follow_intel = {
            "symbol": "DOGEUSDT",
            "signal": "LONG",
            "confidence": 0.84,
            "precision": {"longScore": 5.0, "shortScore": 2.5},
            "execution": {"momentumPct": 0.34},
        }

        with mock.patch.dict(main.os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
            with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(lg, "intel_analyze", new=mock.AsyncMock(return_value=follow_intel)):
                    with mock.patch.object(lg, "_close_position_one_side", new=mock.AsyncMock()) as close_one:
                        with mock.patch.object(lg, "_autotrade_log") as log:
                            changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertTrue(changed)
        close_one.assert_not_awaited()
        lock = main.AUTO_TRADE["liveProfitLocks"]["DOGEUSDT:LONG"]
        self.assertGreater(lock["tp"], 1.018)
        self.assertGreater(lock["sl"], 0.991)
        self.assertEqual(lock["tpExtensionCount"], 1)
        self.assertTrue(any("extend TP" in call.args[0] for call in log.call_args_list))

    async def test_multi_guard_does_not_profit_lock_close_negative_gap_before_sl(self):
        cfg = {"takeProfitPct": 1.8, "stopLossPct": 0.9, "tpTargetMinUsdt": 0.55, "profitLockBreakevenFloorUsdt": 0.08}
        main.AUTO_TRADE["liveProfitLocks"] = {
            "DOGEUSDT:LONG": {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "entryMark": 1.0,
                "tp": 1.018,
                "sl": 0.98,
                "peak": 0.22,
                "armed": True,
                "lockUsdt": 0.10,
            }
        }
        rows = [
            {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "qty": 100.0,
                "entryMark": 1.0,
                "markPrice": 0.999,
                "notionalUsdtApprox": 99.9,
                "unRealizedProfit": -0.10,
            }
        ]

        with mock.patch.dict(main.os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
            with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(lg, "intel_analyze", new=mock.AsyncMock(return_value={"signal": "WAIT", "confidence": 0.5, "execution": {"momentumPct": 0.0}})):
                    with mock.patch.object(lg, "_close_position_one_side", new=mock.AsyncMock()) as close_one:
                        changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertFalse(changed)
        close_one.assert_not_awaited()
        self.assertIn("DOGEUSDT:LONG", main.AUTO_TRADE["liveProfitLocks"])

    async def test_multi_guard_closes_loser_on_strong_reversal_without_reopen(self):
        cfg = {
            "takeProfitPct": 1.8,
            "stopLossPct": 2.5,
            "tpTargetMinUsdt": 0.55,
            "profitLockBreakevenFloorUsdt": 0.08,
            "strongFlipEnabled": True,
            "strongFlipMinConfidence": 0.82,
            "strongFlipMinScoreGap": 1.5,
            "strongFlipUltraScoreGap": 2.2,
            "strongFlipUltraConfRelax": 0.08,
        }
        main.AUTO_TRADE["liveProfitLocks"] = {
            "DOGEUSDT:LONG": {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "entryMark": 1.0,
                "tp": 1.018,
                "sl": 0.975,
                "peak": 0.0,
                "guardianStats": {"openedAt": int(main.time.time()) - 3600},
            }
        }
        rows = [
            {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "qty": 100.0,
                "entryMark": 1.0,
                "markPrice": 0.99,
                "notionalUsdtApprox": 99.0,
                "unRealizedProfit": -1.0,
            }
        ]
        opposite_intel = {
            "symbol": "DOGEUSDT",
            "signal": "SHORT",
            "confidence": 0.86,
            "precision": {
                "longScore": 2.0,
                "shortScore": 5.0,
                "trendDown": True,
                "macdBearish": True,
                "vwapDistancePct": -0.12,
                "bbPctB": 0.35,
                "rsi14": 44.0,
            },
            "execution": {"momentumPct": -0.42},
        }

        with mock.patch.dict(main.os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
            with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(lg, "intel_analyze", new=mock.AsyncMock(return_value=opposite_intel)):
                    with mock.patch.object(lg, "_close_position_one_side", new=mock.AsyncMock(return_value={"closed": [{"ok": True}]})) as close_one:
                        with mock.patch.object(lg, "place_futures_order", new=mock.AsyncMock()) as place_order:
                            with mock.patch.object(lg, "_autotrade_log") as log:
                                changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertTrue(changed)
        close_one.assert_awaited_once_with("DOGEUSDT", "LONG", "k", "s", main._binance_base(), reason="STRONG_REVERSAL_EXIT")
        place_order.assert_not_awaited()
        self.assertNotIn("DOGEUSDT:LONG", main.AUTO_TRADE["liveProfitLocks"])
        self.assertTrue(any("STRONG_REVERSAL_EXIT" in call.args[0] for call in log.call_args_list))

    async def test_multi_guard_does_not_strong_reversal_exit_on_pullback_noise(self):
        cfg = {
            "takeProfitPct": 1.8,
            "stopLossPct": 2.5,
            "tpTargetMinUsdt": 0.55,
            "profitLockTriggerUsdt": 0.35,
            "profitLockBreakevenFloorUsdt": 0.08,
            "strongFlipEnabled": True,
            "strongFlipMinConfidence": 0.82,
            "strongFlipMinScoreGap": 1.5,
            "strongFlipUltraScoreGap": 2.2,
            "strongFlipUltraConfRelax": 0.08,
            "strongFlipConfirmationsRequired": 2,
            "payoffLossGuardEnabled": False,
        }
        rows = [
            {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "qty": 100.0,
                "entryMark": 1.0,
                "markPrice": 0.997,
                "notionalUsdtApprox": 99.7,
                "unRealizedProfit": -0.30,
            }
        ]
        pullback_intel = {
            "symbol": "DOGEUSDT",
            "signal": "SHORT",
            "confidence": 0.88,
            "precision": {
                "longScore": 2.0,
                "shortScore": 5.0,
                "trendUpPartial": True,
                "macdBullish": True,
                "vwapDistancePct": -0.02,
                "bbPctB": 0.49,
                "rsi14": 51.0,
            },
            "execution": {"momentumPct": -0.30},
        }

        with mock.patch.dict(main.os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
            with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(lg, "intel_analyze", new=mock.AsyncMock(return_value=pullback_intel)):
                    with mock.patch.object(lg, "_close_position_one_side", new=mock.AsyncMock()) as close_one:
                        changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertFalse(changed)
        close_one.assert_not_awaited()
        self.assertIn("DOGEUSDT:LONG", main.AUTO_TRADE["liveProfitLocks"])

    async def test_single_guardian_ignores_last_decision_for_other_symbol(self):
        cfg = {
            "takeProfitPct": 1.8,
            "stopLossPct": 2.5,
            "strongFlipEnabled": True,
            "strongFlipMinConfidence": 0.82,
            "strongFlipMinScoreGap": 1.5,
            "guardianDecisionMaxAgeSec": 30,
            "payoffLossGuardEnabled": False,
        }
        prev_decision = main.AUTO_TRADE.get("lastDecision")
        rows = [
            {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "qty": 100.0,
                "entryMark": 1.0,
                "markPrice": 0.99,
                "notionalUsdtApprox": 99.0,
                "unRealizedProfit": -1.0,
                "leverage": 10,
            }
        ]
        main.AUTO_TRADE["liveProfitLocks"] = {
            "DOGEUSDT:LONG": {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "qty": 100.0,
                "entryMark": 1.0,
                "tp": 1.05,
                "sl": 0.95,
                "armed": False,
                "peak": 0.0,
                "lockUsdt": 0.0,
            }
        }
        main.AUTO_TRADE["lastDecision"] = {
            "symbol": "BTCUSDT",
            "ts": int(main.time.time()),
            "intel": {
                "symbol": "BTCUSDT",
                "signal": "SHORT",
                "confidence": 0.95,
                "precision": {
                    "longScore": 1.0,
                    "shortScore": 5.0,
                    "trendDown": True,
                    "macdBearish": True,
                    "vwapDistancePct": -0.2,
                    "bbPctB": 0.2,
                    "rsi14": 42.0,
                },
                "execution": {"momentumPct": -0.5},
            },
        }

        try:
            with mock.patch.dict(main.os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
                with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                    with mock.patch.object(lg, "intel_analyze", new=mock.AsyncMock(return_value=None)):
                        with mock.patch.object(lg, "_close_position_one_side", new=mock.AsyncMock()) as close_one:
                            changed = await main._live_multi_profit_lock_manage(cfg)

            self.assertFalse(changed)
            close_one.assert_not_awaited()
            self.assertIn("DOGEUSDT:LONG", main.AUTO_TRADE["liveProfitLocks"])
        finally:
            main.AUTO_TRADE["lastDecision"] = prev_decision
            main.AUTO_TRADE["liveProfitLocks"] = {}

    async def test_close_one_side_logs_position_entry_snapshot_not_other_symbol_last_decision(self):
        prev_locks = main.AUTO_TRADE.get("liveProfitLocks")
        prev_decision = main.AUTO_TRADE.get("lastDecision")
        main.AUTO_TRADE["liveProfitLocks"] = {
            "DOGEUSDT:LONG": {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "entrySnapshot": {
                    "patternTags": ["entry_tag"],
                    "patternBias": 0.03,
                    "patternScore": 1.8,
                    "entryConfidence": 0.77,
                    "entryScore": 0.66,
                    "entrySpreadBps": 2.2,
                    "entryMomentumPct": 0.11,
                    "entryDecisionAt": 12345,
                },
            }
        }
        main.AUTO_TRADE["lastDecision"] = {
            "symbol": "BTCUSDT",
            "ts": int(main.time.time()),
            "intel": {
                "symbol": "BTCUSDT",
                "confidence": 0.99,
                "candles": {"tags": ["wrong_tag"], "bias": -0.05, "score": -3.0},
                "execution": {"spreadBps": 9.9, "momentumPct": -0.9},
            },
        }
        pos_rows = [{"positionAmt": "100", "positionSide": "LONG", "entryPrice": "1.0"}]
        recorded = []

        async def fake_signed(method, _base, path, _key, _secret, _payload):
            if method == "GET" and path.endswith("/positionRisk"):
                return pos_rows
            return {"ok": True}

        try:
            with mock.patch.object(fo, "_is_hedge_mode", new=mock.AsyncMock(return_value=False)):
                with mock.patch.object(fo, "fetch_mark_price", new=mock.AsyncMock(return_value=1.01)):
                    with mock.patch.object(fo, "_get_um_client", return_value=None):
                        with mock.patch.object(fo, "_signed_request", new=fake_signed):
                            with mock.patch.object(fo, "_record_learning_trade", side_effect=lambda sym, trade, mode: recorded.append((sym, trade, mode))):
                                res = await main._close_position_one_side("DOGEUSDT", "LONG", "k", "s", main._binance_base())

            self.assertTrue(res["closed"])
            self.assertEqual(recorded[0][0], "DOGEUSDT")
            trade = recorded[0][1]
            self.assertEqual(trade["patternTags"], ["entry_tag"])
            self.assertEqual(trade["entryConfidence"], 0.77)
            self.assertEqual(trade["entryDecisionAt"], 12345)
        finally:
            main.AUTO_TRADE["liveProfitLocks"] = prev_locks
            main.AUTO_TRADE["lastDecision"] = prev_decision

    async def test_multi_guard_caps_loss_when_recent_payoff_is_weak(self):
        cfg = {
            "takeProfitPct": 1.8,
            "stopLossPct": 2.5,
            "tpTargetMinUsdt": 0.55,
            "payoffLossGuardEnabled": True,
            "payoffLossGuardMinTrades": 6,
            "payoffLossGuardWindowTrades": 8,
            "payoffLossGuardMaxPayoffRatio": 0.75,
            "payoffLossGuardLossToWinCap": 1.05,
            "payoffLossGuardMinLossUsdt": 0.22,
            "payoffLossGuardMaxLossUsdt": 0.9,
        }
        main.AUTO_TRADE["liveProfitLocks"] = {
            "DOGEUSDT:LONG": {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "entryMark": 1.0,
                "tp": 1.018,
                "sl": 0.975,
                "peak": 0.0,
                "guardianStats": {"openedAt": int(main.time.time()) - 3600},
            }
        }
        rows = [
            {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "qty": 100.0,
                "entryMark": 1.0,
                "markPrice": 0.9938,
                "notionalUsdtApprox": 99.38,
                "unRealizedProfit": -0.62,
            }
        ]
        recent_trades = [
            {"symbol": "DOGEUSDT", "_pnl": -0.98, "pnl": -0.98},
            {"symbol": "DOGEUSDT", "_pnl": 0.56, "pnl": 0.56},
            {"symbol": "DOGEUSDT", "_pnl": -1.02, "pnl": -1.02},
            {"symbol": "DOGEUSDT", "_pnl": -0.95, "pnl": -0.95},
            {"symbol": "DOGEUSDT", "_pnl": 0.57, "pnl": 0.57},
            {"symbol": "DOGEUSDT", "_pnl": -1.01, "pnl": -1.01},
            {"symbol": "DOGEUSDT", "_pnl": 0.56, "pnl": 0.56},
            {"symbol": "DOGEUSDT", "_pnl": -0.99, "pnl": -0.99},
        ]

        with mock.patch.dict(main.os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
            with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(lrn, "_live_closed_trades_from_log", return_value=recent_trades):
                    with mock.patch.object(lg, "intel_analyze", new=mock.AsyncMock(return_value={"signal": "LONG", "confidence": 0.76, "execution": {"momentumPct": 0.08}})):
                        with mock.patch.object(lg, "_close_position_one_side", new=mock.AsyncMock(return_value={"ok": True})) as close_one:
                            with mock.patch.object(lg, "_autotrade_log") as log:
                                changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertTrue(changed)
        close_one.assert_awaited_once_with("DOGEUSDT", "LONG", "k", "s", main._binance_base(), reason="PAYOFF_LOSS_GUARD")
        self.assertEqual(main.AUTO_TRADE["liveProfitLocks"], {})
        self.assertTrue(any("PAYOFF_LOSS_GUARD" in call.args[0] for call in log.call_args_list))

    async def test_live_guardian_runs_before_cooldown_continue(self):
        class StopLoop(BaseException):
            pass

        prev = {
            "running": main.AUTO_TRADE.get("running"),
            "manageOpenOnly": main.AUTO_TRADE.get("manageOpenOnly"),
            "config": main.AUTO_TRADE.get("config"),
            "pauseUntil": main.AUTO_TRADE.get("pauseUntil"),
            "riskCooldownLastMarketCheckAt": main.AUTO_TRADE.get("riskCooldownLastMarketCheckAt"),
            "hermesAgents": main.AUTO_TRADE.get("hermesAgents"),
        }
        now = int(main.time.time())
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["manageOpenOnly"] = False
        main.AUTO_TRADE["pauseUntil"] = now + 600
        main.AUTO_TRADE["riskCooldownLastMarketCheckAt"] = 0
        main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "riskCooldownEnabled": True,
            "riskCooldownAdaptiveMarket": True,
            "riskCooldownAdaptiveCheckSec": 10,
            "intervalSec": 20,
            "symbol": "AUTO",
        }

        async def stop_sleep(_seconds):
            raise StopLoop()

        try:
            with mock.patch.object(main, "_manage_live_open_positions_once", new=mock.AsyncMock(return_value=False)) as manage:
                with mock.patch.object(main, "_adaptive_risk_cooldown_check", new=mock.AsyncMock(return_value={"resume": False, "reason": "market volatile (XLMUSDT)", "board": []})):
                    with mock.patch.object(main.asyncio, "sleep", new=stop_sleep):
                        with self.assertRaises(StopLoop):
                            await main._autotrade_loop()
            manage.assert_awaited()
        finally:
            for key, value in prev.items():
                main.AUTO_TRADE[key] = value

    async def test_adaptive_cooldown_timeout_remains_normal_cooldown_hold(self):
        class StopLoop(BaseException):
            pass

        prev = {
            "running": main.AUTO_TRADE.get("running"),
            "manageOpenOnly": main.AUTO_TRADE.get("manageOpenOnly"),
            "config": main.AUTO_TRADE.get("config"),
            "pauseUntil": main.AUTO_TRADE.get("pauseUntil"),
            "riskCooldownLastMarketCheckAt": main.AUTO_TRADE.get("riskCooldownLastMarketCheckAt"),
            "hermesAgents": main.AUTO_TRADE.get("hermesAgents"),
            "lastSkip": main.AUTO_TRADE.get("lastSkip"),
        }
        now = int(main.time.time())
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["manageOpenOnly"] = False
        main.AUTO_TRADE["pauseUntil"] = now + 600
        main.AUTO_TRADE["riskCooldownLastMarketCheckAt"] = 0
        main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
        main.AUTO_TRADE["lastSkip"] = None
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "riskCooldownEnabled": True,
            "riskCooldownAdaptiveMarket": True,
            "riskCooldownAdaptiveCheckSec": 10,
            "intervalSec": 20,
            "symbol": "AUTO",
        }

        async def stop_sleep(_seconds):
            raise StopLoop()

        try:
            with mock.patch.object(main, "_manage_live_open_positions_once", new=mock.AsyncMock(return_value=False)):
                with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=[])):
                    with mock.patch.object(main, "_adaptive_risk_cooldown_check", new=mock.AsyncMock(side_effect=main.asyncio.TimeoutError())):
                        with mock.patch.object(main, "_persist_autotrade_snapshot"):
                            with mock.patch.object(main.asyncio, "sleep", new=stop_sleep):
                                with self.assertRaises(StopLoop):
                                    await main._autotrade_loop()

            risk_agent = main.AUTO_TRADE["hermesAgents"]["agents"]["risk_manager"]
            self.assertEqual(risk_agent["lastAction"], "risk cooldown active")
            self.assertIn("adaptive check timeout", risk_agent["lastReason"])
            self.assertEqual(main.AUTO_TRADE["lastSkip"]["code"], "risk_cooldown")
            self.assertIn("adaptive check timeout", main.AUTO_TRADE["lastSkip"]["msg"])
        finally:
            for key, value in prev.items():
                main.AUTO_TRADE[key] = value

    async def test_live_guardian_idle_check_does_not_count_as_completed_work(self):
        prev = {
            "hermesAgents": main.AUTO_TRADE.get("hermesAgents"),
            "liveProfitLocks": main.AUTO_TRADE.get("liveProfitLocks"),
            "lastTradeAt": main.AUTO_TRADE.get("lastTradeAt"),
            "trades": list(main.AUTO_TRADE.get("trades", [])),
        }
        main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
        main.AUTO_TRADE["liveProfitLocks"] = {}

        try:
            with mock.patch.object(main, "_live_multi_profit_lock_manage", new=mock.AsyncMock(return_value=False)):
                closed = await main._manage_live_open_positions_once({"executionMode": "LIVE"}, int(main.time.time()))

            guardian = main.AUTO_TRADE["hermesAgents"]["agents"]["position_guardian"]
            self.assertFalse(closed)
            self.assertEqual(guardian["state"], "todo")
            self.assertEqual(guardian["lastAction"], "no open positions")
            self.assertEqual(guardian["runs"], 0)
        finally:
            for key, value in prev.items():
                main.AUTO_TRADE[key] = value

    async def test_scan_volatile_sets_symbol_cooldown_not_global_pause(self):
        class StopLoop(BaseException):
            pass

        prev = {
            "running": main.AUTO_TRADE.get("running"),
            "manageOpenOnly": main.AUTO_TRADE.get("manageOpenOnly"),
            "config": main.AUTO_TRADE.get("config"),
            "pauseUntil": main.AUTO_TRADE.get("pauseUntil"),
            "riskCooldownLastMarketCheckAt": main.AUTO_TRADE.get("riskCooldownLastMarketCheckAt"),
            "riskCooldownLossSignature": main.AUTO_TRADE.get("riskCooldownLossSignature"),
            "hermesAgents": main.AUTO_TRADE.get("hermesAgents"),
            "scanBoard": main.AUTO_TRADE.get("scanBoard"),
            "lastSkip": main.AUTO_TRADE.get("lastSkip"),
        }
        saved_profiles = {}
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["manageOpenOnly"] = False
        main.AUTO_TRADE["pauseUntil"] = 0
        main.AUTO_TRADE["riskCooldownLastMarketCheckAt"] = 0
        main.AUTO_TRADE["riskCooldownLossSignature"] = ""
        main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
        main.AUTO_TRADE["config"] = {
            "executionMode": "PAPER",
            "marketScan": True,
            "riskCooldownEnabled": True,
            "riskCooldownPauseOnVolatile": True,
            "riskCooldownVolatileMinutes": 15,
            "intervalSec": 20,
            "cooldownSec": 0,
            "symbol": "AUTO",
            "scanAnalyzeTop": 3,
        }

        async def stop_sleep(_seconds):
            raise StopLoop()

        try:
            with mock.patch.object(main, "_pick_best_symbol_from_scan", new=mock.AsyncMock(return_value=(
                "XLMUSDT",
                {"symbol": "XLMUSDT", "signal": "LONG", "confidence": 0.82,
                 "execution": {"spreadBps": 2.0, "momentumPct": 0.1},
                 "precision": {"longScore": 2.0, "shortScore": 0.2},
                 "momentum": {"net": 0.5}},
                [{"symbol": "XLMUSDT", "qualified": True}],
            ))):
                with mock.patch.object(main, "_recent_live_loss_streak_state", return_value={"streak": 0, "signature": ""}):
                    with mock.patch.object(main, "_risk_cooldown_regime", return_value={"name": "VOLATILE", "cooldownSec": 60}):
                        with mock.patch.object(main, "_load_single_profile", side_effect=lambda sym: saved_profiles.get(sym, {})):
                            with mock.patch.object(main, "_save_single_profile", side_effect=lambda sym, pr: saved_profiles.update({sym: pr})):
                                with mock.patch.object(main.asyncio, "sleep", new=stop_sleep):
                                    with self.assertRaises(StopLoop):
                                        await main._autotrade_loop()

            self.assertEqual(main.AUTO_TRADE["pauseUntil"], 0)
            self.assertEqual(main.AUTO_TRADE["lastSkip"]["code"], "symbol_volatile_cooldown")
            self.assertGreater(int(saved_profiles["XLMUSDT"]["scanCooldownUntil"]), int(main.time.time()))
        finally:
            for key, value in prev.items():
                main.AUTO_TRADE[key] = value

    async def test_autotrade_loop_applies_defaults_for_legacy_snapshot_config(self):
        class StopLoop(BaseException):
            pass

        prev = {
            "running": main.AUTO_TRADE.get("running"),
            "manageOpenOnly": main.AUTO_TRADE.get("manageOpenOnly"),
            "config": main.AUTO_TRADE.get("config"),
            "pauseUntil": main.AUTO_TRADE.get("pauseUntil"),
            "consecutiveErrors": main.AUTO_TRADE.get("consecutiveErrors"),
            "lastSkip": main.AUTO_TRADE.get("lastSkip"),
            "hermesAgents": main.AUTO_TRADE.get("hermesAgents"),
            "trades": main.AUTO_TRADE.get("trades"),
            "lastTradeAt": main.AUTO_TRADE.get("lastTradeAt"),
            "lastDecision": main.AUTO_TRADE.get("lastDecision"),
            "lastDecisions": main.AUTO_TRADE.get("lastDecisions"),
        }
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["manageOpenOnly"] = False
        main.AUTO_TRADE["pauseUntil"] = 0
        main.AUTO_TRADE["consecutiveErrors"] = 0
        main.AUTO_TRADE["lastSkip"] = None
        main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
        main.AUTO_TRADE["trades"] = []
        main.AUTO_TRADE["lastTradeAt"] = 0
        main.AUTO_TRADE["lastDecision"] = None
        main.AUTO_TRADE["lastDecisions"] = {}
        main.AUTO_TRADE["config"] = {
            "executionMode": "PAPER",
            "symbol": "AUTO",
            "marketScan": True,
            "intervalSec": 20,
            "cooldownSec": 0,
        }

        async def stop_sleep(_seconds):
            raise StopLoop()

        try:
            with mock.patch.object(main, "_pick_best_symbol_from_scan", new=mock.AsyncMock(return_value=(
                "BTCUSDT",
                {
                    "symbol": "BTCUSDT",
                    "signal": "WAIT",
                    "confidence": 0.5,
                    "execution": {"mark": 100.0, "bid": 99.99, "ask": 100.01, "spreadBps": 2.0, "momentumPct": 0.0},
                    "precision": {"longScore": 0.5, "shortScore": 0.3},
                    "momentum": {"net": 0.0},
                },
                [{"symbol": "BTCUSDT", "qualified": False, "rejectReason": "signal_wait"}],
            ))):
                with mock.patch.object(main, "_live_trades_count_today_symbol", return_value=0):
                    with mock.patch.object(main.asyncio, "sleep", new=stop_sleep):
                        with mock.patch.object(main, "_cached_klines", new=mock.AsyncMock(return_value=[])):
                            with mock.patch.object(main, "_exchange_filters", new=mock.AsyncMock(return_value={"minNotional": 0.0})):
                                with self.assertRaises(StopLoop):
                                    await main._autotrade_loop()

            self.assertIn("maxSpreadBps", main.AUTO_TRADE["config"])
            self.assertEqual(main.AUTO_TRADE["lastSkip"]["code"], "signal_wait")
            self.assertEqual(main.AUTO_TRADE["consecutiveErrors"], 0)
        finally:
            for key, value in prev.items():
                main.AUTO_TRADE[key] = value

    async def test_permission_error_recovery_resets_consecutive_errors(self):
        class StopLoop(BaseException):
            pass

        prev = {
            "running": main.AUTO_TRADE.get("running"),
            "manageOpenOnly": main.AUTO_TRADE.get("manageOpenOnly"),
            "config": main.AUTO_TRADE.get("config"),
            "pauseUntil": main.AUTO_TRADE.get("pauseUntil"),
            "consecutiveErrors": main.AUTO_TRADE.get("consecutiveErrors"),
            "lastSkip": main.AUTO_TRADE.get("lastSkip"),
            "hermesAgents": main.AUTO_TRADE.get("hermesAgents"),
            "trades": main.AUTO_TRADE.get("trades"),
            "lastTradeAt": main.AUTO_TRADE.get("lastTradeAt"),
        }
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["manageOpenOnly"] = False
        main.AUTO_TRADE["pauseUntil"] = 0
        main.AUTO_TRADE["consecutiveErrors"] = 2
        main.AUTO_TRADE["lastSkip"] = None
        main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
        main.AUTO_TRADE["trades"] = []
        main.AUTO_TRADE["lastTradeAt"] = 0
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "riskCooldownEnabled": False,
            "intervalSec": 20,
            "cooldownSec": 0,
            "symbol": "BTCUSDT",
            "primarySymbol": "BTCUSDT",
            "marketScan": False,
            "noTradeWindows": [],
            "liveBadUtcHours": [],
        }

        async def stop_sleep(_seconds):
            raise StopLoop()

        try:
            with mock.patch.object(main, "_manage_live_open_positions_once", new=mock.AsyncMock(return_value=False)):
                with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=[])):
                    with mock.patch.object(main, "intel_analyze", new=mock.AsyncMock(side_effect=Exception("-2015 invalid api-key, ip, or permissions"))):
                        with mock.patch.object(main.asyncio, "sleep", new=stop_sleep):
                            with self.assertRaises(StopLoop):
                                await main._autotrade_loop()

            self.assertEqual(main.AUTO_TRADE["consecutiveErrors"], 0)
            self.assertEqual(main.AUTO_TRADE["lastSkip"]["code"], "binance_permission_required")
            self.assertGreater(int(main.AUTO_TRADE["pauseUntil"]), int(main.time.time()))
        finally:
            for key, value in prev.items():
                main.AUTO_TRADE[key] = value

    async def test_fapi_agreement_error_locks_symbol_instead_of_repeating(self):
        class StopLoop(BaseException):
            pass

        prev = {
            "running": main.AUTO_TRADE.get("running"),
            "manageOpenOnly": main.AUTO_TRADE.get("manageOpenOnly"),
            "config": main.AUTO_TRADE.get("config"),
            "pauseUntil": main.AUTO_TRADE.get("pauseUntil"),
            "consecutiveErrors": main.AUTO_TRADE.get("consecutiveErrors"),
            "lastSkip": main.AUTO_TRADE.get("lastSkip"),
            "hermesAgents": main.AUTO_TRADE.get("hermesAgents"),
            "trades": main.AUTO_TRADE.get("trades"),
            "lastTradeAt": main.AUTO_TRADE.get("lastTradeAt"),
            "perfLocks": main.AUTO_TRADE.get("perfLocks"),
        }
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["manageOpenOnly"] = False
        main.AUTO_TRADE["pauseUntil"] = 0
        main.AUTO_TRADE["consecutiveErrors"] = 2
        main.AUTO_TRADE["lastSkip"] = None
        main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
        main.AUTO_TRADE["trades"] = []
        main.AUTO_TRADE["lastTradeAt"] = 0
        main.AUTO_TRADE["perfLocks"] = {}
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "riskCooldownEnabled": False,
            "intervalSec": 20,
            "cooldownSec": 0,
            "symbol": "XAUUSDT",
            "primarySymbol": "XAUUSDT",
            "marketScan": False,
            "noTradeWindows": [],
            "liveBadUtcHours": [],
            "fapiAgreementSymbolLockMinutes": 45,
        }

        async def stop_sleep(_seconds):
            raise StopLoop()

        try:
            with mock.patch.object(main, "_manage_live_open_positions_once", new=mock.AsyncMock(return_value=False)):
                with mock.patch.object(lg, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=[])):
                    with mock.patch.object(main, "intel_analyze", new=mock.AsyncMock(side_effect=Exception("-4411 agreement contract fapi"))):
                        with mock.patch.object(main.asyncio, "sleep", new=stop_sleep):
                            with self.assertRaises(StopLoop):
                                await main._autotrade_loop()

            self.assertEqual(main.AUTO_TRADE["consecutiveErrors"], 0)
            self.assertEqual(main.AUTO_TRADE["lastSkip"]["code"], "fapi_agreement_required")
            self.assertEqual(main.AUTO_TRADE["perfLocks"]["XAUUSDT"]["reason"], "fapi_agreement")
            self.assertGreater(int(main.AUTO_TRADE["perfLocks"]["XAUUSDT"]["until"]), int(main.time.time()))
        finally:
            for key, value in prev.items():
                main.AUTO_TRADE[key] = value

class _NoOpTvMcp:
    """Stub TradingView MCP: empty miss list + empty universe so scan-path
    tests never filter synthetic symbols through the TV whitelist."""

    _tv_missing = {}

    def get_tv_universe(self, force_refresh: bool = False) -> set:
        return set()

    def is_tv_known(self, symbol: str) -> bool:
        return True


class TestMarketScanTimeoutGuard(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tv_patch = mock.patch("trading.tradingview_mcp.get_tv_mcp", return_value=_NoOpTvMcp())
        self._tv_patch.start()

    def tearDown(self):
        self._tv_patch.stop()
    async def test_scan_timeout_budget_accounts_for_expanded_scan_and_retries(self):
        cfg = {
            "intervalSec": 20,
            "scanAnalyzeTop": 6,
            "scanGuardedFallbackAnalyzeTop": 12,
            "scanTopLiquid": 25,
            "scanPerSymbolTimeoutSec": 7.5,
            "scanFallbackRetrySymbols": 3,
        }

        budget = main._scan_timeout_budget_sec(cfg)

        self.assertGreaterEqual(budget, 80.0)
        self.assertLessEqual(budget, 120.0)

    async def test_scan_picks_highest_score_instead_of_long_first(self):
        async def fake_intel(req):
            if req.symbol == "LONGUSDT":
                return {
                    "signal": "LONG",
                    "confidence": 0.76,
                    "execution": {"momentumPct": 0.1, "spreadBps": 1.0},
                }
            return {
                "signal": "SHORT",
                "confidence": 0.84,
                "execution": {"momentumPct": -0.4, "spreadBps": 1.0},
            }

        cfg = {"minConfidence": 0.7, "scanAnalyzeTop": 3, "scanSidePreference": "score"}
        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=["LONGUSDT", "SHORTUSDT"])):
            with mock.patch.object(main, "intel_analyze", new=fake_intel):
                with mock.patch.object(main, "_symbol_perf_gate", return_value=(True, "", {"trades": 0})):
                    picked_symbol, _picked_intel, board = await main._pick_best_symbol_from_scan(cfg)

        self.assertEqual(picked_symbol, "SHORTUSDT")
        self.assertEqual(len(board), 2)

    async def test_scan_excludes_symbols_with_open_live_position(self):
        analyzed = []

        async def fake_intel(req):
            analyzed.append(req.symbol)
            return {
                "signal": "LONG",
                "confidence": 0.84 if req.symbol == "OPENUSDT" else 0.78,
                "execution": {"momentumPct": 0.2, "spreadBps": 1.0},
            }

        cfg = {"executionMode": "LIVE", "minConfidence": 0.7, "scanAnalyzeTop": 3, "scanSidePreference": "score"}
        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=["OPENUSDT", "FREEUSDT"])):
            with mock.patch.object(main, "intel_analyze", new=fake_intel):
                with mock.patch.object(main, "_symbol_perf_gate", return_value=(True, "", {"trades": 0})):
                    picked_symbol, _picked_intel, board = await main._pick_best_symbol_from_scan(cfg, {"OPENUSDT"})

        self.assertEqual(picked_symbol, "FREEUSDT")
        self.assertEqual([row["symbol"] for row in board], ["FREEUSDT"])
        self.assertEqual(analyzed, ["FREEUSDT"])

    async def test_scan_excludes_active_fapi_agreement_locked_symbols(self):
        analyzed = []
        prev_locks = main.AUTO_TRADE.get("perfLocks")
        main.AUTO_TRADE["perfLocks"] = {
            "XAUUSDT": {
                "until": int(main.time.time()) + 3600,
                "at": int(main.time.time()),
                "reason": "fapi_agreement",
                "error": "-4411",
            }
        }

        async def fake_intel(req):
            analyzed.append(req.symbol)
            return {
                "signal": "LONG",
                "confidence": 0.82,
                "execution": {"momentumPct": 0.2, "spreadBps": 1.0},
            }

        cfg = {"executionMode": "LIVE", "minConfidence": 0.7, "scanAnalyzeTop": 3, "scanSidePreference": "score"}
        try:
            with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=["XAUUSDT", "FREEUSDT"])):
                with mock.patch.object(main, "intel_analyze", new=fake_intel):
                    with mock.patch.object(main, "_symbol_perf_gate", return_value=(True, "", {"trades": 0})):
                        picked_symbol, _picked_intel, board = await main._pick_best_symbol_from_scan(cfg)
        finally:
            main.AUTO_TRADE["perfLocks"] = prev_locks if isinstance(prev_locks, dict) else {}

        self.assertEqual(picked_symbol, "FREEUSDT")
        self.assertEqual([row["symbol"] for row in board], ["FREEUSDT"])
        self.assertEqual(analyzed, ["FREEUSDT"])

    async def test_scan_excludes_configured_deny_symbols_before_analyze(self):
        analyzed = []

        async def fake_intel(req):
            analyzed.append(req.symbol)
            return {
                "signal": "LONG",
                "confidence": 0.82,
                "execution": {"momentumPct": 0.2, "spreadBps": 1.0},
            }

        cfg = {
            "executionMode": "LIVE",
            "minConfidence": 0.7,
            "scanAnalyzeTop": 3,
            "scanSidePreference": "score",
            "scanDenySymbols": ["SPCXUSDT", "MRVLUSDT"],
        }
        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=["SPCXUSDT", "MRVLUSDT", "FREEUSDT"])):
            with mock.patch.object(main, "intel_analyze", new=fake_intel):
                with mock.patch.object(main, "_symbol_perf_gate", return_value=(True, "", {"trades": 0})):
                    picked_symbol, _picked_intel, board = await main._pick_best_symbol_from_scan(cfg)

        self.assertEqual(picked_symbol, "FREEUSDT")
        self.assertEqual([row["symbol"] for row in board], ["FREEUSDT"])
        self.assertEqual(analyzed, ["FREEUSDT"])

    async def test_live_scan_excludes_symbols_that_reached_daily_cap(self):
        analyzed = []

        async def fake_intel(req):
            analyzed.append(req.symbol)
            return {
                "signal": "LONG",
                "confidence": 0.82,
                "execution": {"momentumPct": 0.2, "spreadBps": 1.0},
            }

        def fake_today_count(symbol):
            return 14 if symbol == "NEARUSDT" else 0

        cfg = {
            "executionMode": "LIVE",
            "minConfidence": 0.7,
            "scanAnalyzeTop": 3,
            "scanSidePreference": "score",
            "maxDailyTradesPerSymbol": 14,
        }
        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=["NEARUSDT", "FREEUSDT"])):
            with mock.patch.object(main, "_live_trades_count_today_symbol", side_effect=fake_today_count):
                with mock.patch.object(main, "intel_analyze", new=fake_intel):
                    with mock.patch.object(main, "_symbol_perf_gate", return_value=(True, "", {"trades": 0})):
                        picked_symbol, _picked_intel, board = await main._pick_best_symbol_from_scan(cfg)

        self.assertEqual(picked_symbol, "FREEUSDT")
        self.assertEqual([row["symbol"] for row in board], ["FREEUSDT"])
        self.assertEqual(analyzed, ["FREEUSDT"])

    async def test_market_candidates_fall_back_when_ticker_timeout(self):
        async def slow_data_get(_path):
            await main.asyncio.sleep(20)

        # Clear the ticker cache so this test actually exercises the timeout
        # path (a prior test may have seeded it with fresh fake data).
        main._SCAN_TICKER_CACHE["data"] = None
        main._SCAN_TICKER_CACHE["ts"] = 0.0
        with mock.patch.object(main, "_data_get", new=slow_data_get):
            symbols = await main._scan_market_candidates(30)

        self.assertIn("BTCUSDT", symbols)
        self.assertIn("XRPUSDT", symbols)
        self.assertGreaterEqual(len(symbols), 5)

    async def test_market_candidates_deprioritize_extreme_swing_symbols(self):
        class Resp:
            status_code = 200

            def json(self):
                return [
                    {"symbol": "WILDUSDT", "quoteVolume": "1100", "priceChangePercent": "30"},
                    {"symbol": "CALMUSDT", "quoteVolume": "1000", "priceChangePercent": "4"},
                    {"symbol": "MIDUSDT", "quoteVolume": "900", "priceChangePercent": "6"},
                    {"symbol": "BTCUSDT", "quoteVolume": "800", "priceChangePercent": "2"},
                    {"symbol": "ETHUSDT", "quoteVolume": "700", "priceChangePercent": "1"},
                ]

        async def fake_data_get(_path):
            return Resp()

        with mock.patch.object(main, "_data_get", new=fake_data_get):
            symbols = await main._scan_market_candidates(5)

        self.assertLess(symbols.index("CALMUSDT"), symbols.index("WILDUSDT"))

    async def test_scan_circuit_breaker_marks_fallback_when_all_analyze_timeout(self):
        async def slow_intel(_req):
            await main.asyncio.sleep(20)

        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=["BTCUSDT", "ETHUSDT", "XRPUSDT"])):
            with mock.patch.object(main, "intel_analyze", new=slow_intel):
                picked_symbol, picked_intel, board = await main._pick_best_symbol_from_scan(
                    {"scanAnalyzeTop": 3, "scanPerSymbolTimeoutSec": 0.01, "scanFallbackNearEnabled": True}
                )

        self.assertIsNone(picked_symbol)
        self.assertIsNone(picked_intel)
        self.assertEqual(len(board), 3)
        self.assertFalse(all(row["rejectReason"] == "analyze_error" for row in board))
        self.assertTrue(any(row["rejectReason"] == "fallback_error" for row in board))

    async def test_scan_circuit_breaker_recovers_with_second_fallback_symbol(self):
        calls = []

        async def flaky_intel(req):
            calls.append(req.symbol)
            if len(calls) <= 3:
                raise RuntimeError("primary analyze fail")
            if req.symbol == "ETHUSDT":
                return {"signal": "LONG", "confidence": 0.82, "execution": {"momentumPct": 0.3, "spreadBps": 1.0}}
            raise RuntimeError("fallback fail")

        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=["BTCUSDT", "ETHUSDT", "XRPUSDT"])):
            with mock.patch.object(main, "intel_analyze", new=flaky_intel):
                with mock.patch.object(main, "_symbol_perf_gate", return_value=(True, "", {"trades": 0})):
                    with mock.patch.object(main, "_learned_min_conf", return_value=0.7):
                        picked_symbol, picked_intel, board = await main._pick_best_symbol_from_scan(
                            {"minConfidence": 0.7, "scanAnalyzeTop": 3, "scanFallbackNearEnabled": True, "scanFallbackRetrySymbols": 3}
                        )

        self.assertEqual(picked_symbol, "ETHUSDT")
        self.assertIsInstance(picked_intel, dict)
        self.assertTrue(any(row["symbol"] == "ETHUSDT" and row["rejectReason"] == "fallback_recovered" for row in board))

    async def test_scan_expands_universe_when_perf_locks_guard_out_initial_rows(self):
        calls = []

        async def fake_intel(req):
            calls.append(req.symbol)
            return {
                "signal": "LONG",
                "confidence": 0.82,
                "execution": {"momentumPct": 0.25, "spreadBps": 1.0},
            }

        def fake_perf(_cfg, symbol):
            if symbol in {"LOCK1USDT", "LOCK2USDT", "LOCK3USDT"}:
                return False, "perf_lock_reward", {"trades": 8, "winRatePct": 25.0, "pnl": -0.8}
            return True, "", {"trades": 0, "winRatePct": 0.0, "pnl": 0.0}

        cfg = {
            "minConfidence": 0.7,
            "scanAnalyzeTop": 3,
            "scanGuardedFallbackAnalyzeTop": 6,
            "scanSidePreference": "score",
        }
        symbols = ["LOCK1USDT", "LOCK2USDT", "LOCK3USDT", "FREE1USDT", "FREE2USDT", "FREE3USDT"]
        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=symbols)):
            with mock.patch.object(main, "intel_analyze", new=fake_intel):
                with mock.patch.object(main, "_symbol_perf_gate", side_effect=fake_perf):
                    picked_symbol, _picked_intel, board = await main._pick_best_symbol_from_scan(cfg)

        self.assertIn(picked_symbol, {"FREE1USDT", "FREE2USDT", "FREE3USDT"})
        self.assertEqual(calls, symbols)
        self.assertTrue(any(row.get("scanExpanded") for row in board if row["symbol"].startswith("FREE")))
        self.assertTrue(any(row["rejectReason"] == "perf_lock_reward" for row in board))

    async def test_scan_expands_universe_when_confidence_guards_out_initial_rows(self):
        calls = []

        async def fake_intel(req):
            calls.append(req.symbol)
            conf = 0.66 if req.symbol.startswith("LOW") else 0.82
            return {
                "signal": "LONG",
                "confidence": conf,
                "execution": {"momentumPct": 0.25, "spreadBps": 1.0},
            }

        cfg = {
            "minConfidence": 0.7,
            "scanAnalyzeTop": 3,
            "scanGuardedFallbackAnalyzeTop": 6,
            "scanSidePreference": "score",
            "scanFallbackNearEnabled": True,
        }
        symbols = ["LOW1USDT", "LOW2USDT", "LOW3USDT", "FREE1USDT", "FREE2USDT", "FREE3USDT"]
        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=symbols)):
            with mock.patch.object(main, "intel_analyze", new=fake_intel):
                with mock.patch.object(main, "_symbol_perf_gate", return_value=(True, "", {"trades": 0, "winRatePct": 0.0, "pnl": 0.0})):
                    picked_symbol, _picked_intel, board = await main._pick_best_symbol_from_scan(cfg)

        self.assertIn(picked_symbol, {"FREE1USDT", "FREE2USDT", "FREE3USDT"})
        self.assertEqual(calls, symbols)
        self.assertTrue(any(row.get("scanExpanded") for row in board if row["symbol"].startswith("FREE")))
        self.assertTrue(any(row["rejectReason"] == "low_conf" for row in board))

    async def test_scan_uses_guarded_low_conf_fallback_when_all_candidates_blocked(self):
        async def fake_intel(req):
            conf = 0.62 if req.symbol == "LOW1USDT" else 0.66
            return {
                "signal": "SHORT",
                "confidence": conf,
                "execution": {"momentumPct": 0.18, "spreadBps": 1.0},
            }

        cfg = {
            "minConfidence": 0.72,
            "scanAnalyzeTop": 3,
            "scanGuardedFallbackAnalyzeTop": 6,
            "scanGuardedFallbackConfRelax": 0.14,
            "scanSidePreference": "score",
            "scanFallbackNearEnabled": True,
        }
        symbols = ["LOW1USDT", "LOW2USDT", "LOW3USDT", "LOW4USDT", "LOW5USDT", "LOW6USDT"]
        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=symbols)):
            with mock.patch.object(main, "intel_analyze", new=fake_intel):
                with mock.patch.object(main, "_symbol_perf_gate", return_value=(True, "", {"trades": 0, "winRatePct": 0.0, "pnl": 0.0})):
                    picked_symbol, picked_intel, board = await main._pick_best_symbol_from_scan(cfg)

        self.assertIn(picked_symbol, {"LOW2USDT", "LOW3USDT", "LOW4USDT", "LOW5USDT", "LOW6USDT"})
        self.assertIsInstance(picked_intel, dict)
        picked_row = next(row for row in board if row["symbol"] == picked_symbol)
        self.assertTrue(picked_row["qualified"])
        self.assertEqual(picked_row["rejectReason"], "guarded_fallback:low_conf")
        self.assertTrue(picked_row["guardedFallbackPicked"])

    async def test_scan_prefers_soft_perf_fallback_over_low_conf_near_miss(self):
        async def fake_intel(req):
            if req.symbol == "SOFTUSDT":
                return {
                    "signal": "LONG",
                    "confidence": 0.86,
                    "execution": {"momentumPct": 0.2, "spreadBps": 1.0},
                }
            return {
                "signal": "SHORT",
                "confidence": 0.69,
                "execution": {"momentumPct": 0.18, "spreadBps": 1.0},
            }

        def fake_perf(_cfg, symbol):
            if symbol == "SOFTUSDT":
                return False, "perf_lock(42m)", {
                    "trades": 12,
                    "winRatePct": 44.0,
                    "pnl": -0.16,
                    "lockReason": "sustained",
                }
            return True, "", {"trades": 0, "winRatePct": 0.0, "pnl": 0.0}

        cfg = {
            "minConfidence": 0.72,
            "scanAnalyzeTop": 3,
            "scanGuardedFallbackAnalyzeTop": 3,
            "scanSidePreference": "score",
            "scanFallbackNearEnabled": True,
            "scanFallbackNearConfRelax": 0.06,
            "scanPerfSoftFallbackEnabled": True,
        }
        symbols = ["SOFTUSDT", "LOW1USDT", "LOW2USDT"]
        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=symbols)):
            with mock.patch.object(main, "intel_analyze", new=fake_intel):
                with mock.patch.object(main, "_symbol_perf_gate", side_effect=fake_perf):
                    picked_symbol, _picked_intel, board = await main._pick_best_symbol_from_scan(cfg)

        self.assertEqual(picked_symbol, "SOFTUSDT")
        picked_row = next(row for row in board if row["symbol"] == "SOFTUSDT")
        self.assertTrue(picked_row["qualified"])
        self.assertEqual(picked_row["rejectReason"], "perf_soft_fallback:perf_lock_recovered")
        self.assertTrue(picked_row["softFallbackPicked"])

    async def test_scan_uses_soft_perf_fallback_when_all_candidates_are_guarded(self):
        calls = []

        async def fake_intel(req):
            calls.append(req.symbol)
            return {
                "signal": "LONG",
                "confidence": 0.86 if req.symbol == "SOFT2USDT" else 0.82,
                "execution": {"momentumPct": 0.25, "spreadBps": 1.0},
            }

        def fake_perf(_cfg, _symbol):
            return False, "perf_lock_new", {"trades": 12, "winRatePct": 32.0, "pnl": -0.7}

        cfg = {
            "minConfidence": 0.7,
            "scanAnalyzeTop": 3,
            "scanGuardedFallbackAnalyzeTop": 6,
            "scanSidePreference": "score",
            "scanPerfSoftFallbackEnabled": True,
        }
        symbols = ["LOCK1USDT", "LOCK2USDT", "LOCK3USDT", "SOFT1USDT", "SOFT2USDT", "SOFT3USDT"]
        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=symbols)):
            with mock.patch.object(main, "intel_analyze", new=fake_intel):
                with mock.patch.object(main, "_symbol_perf_gate", side_effect=fake_perf):
                    picked_symbol, _picked_intel, board = await main._pick_best_symbol_from_scan(cfg)

        self.assertEqual(picked_symbol, "SOFT2USDT")
        self.assertEqual(calls, symbols)
        picked_row = next(row for row in board if row["symbol"] == "SOFT2USDT")
        self.assertTrue(picked_row["qualified"])
        self.assertEqual(picked_row["rejectReason"], "perf_soft_fallback:perf_lock_new")
        self.assertTrue(picked_row["softFallbackPicked"])

    async def test_scan_uses_soft_perf_fallback_for_recovered_active_lock(self):
        async def fake_intel(req):
            conf = 0.86 if req.symbol == "REC2USDT" else 0.82
            return {
                "signal": "LONG",
                "confidence": conf,
                "execution": {"momentumPct": 0.25, "spreadBps": 1.0},
            }

        def fake_perf(_cfg, _symbol):
            return False, "perf_lock(42m)", {
                "trades": 12,
                "winRatePct": 44.0,
                "pnl": -0.16,
                "lockReason": "sustained",
            }

        cfg = {
            "minConfidence": 0.7,
            "scanAnalyzeTop": 3,
            "scanGuardedFallbackAnalyzeTop": 3,
            "scanSidePreference": "score",
            "scanPerfSoftFallbackEnabled": True,
        }
        symbols = ["REC1USDT", "REC2USDT", "REC3USDT"]
        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=symbols)):
            with mock.patch.object(main, "intel_analyze", new=fake_intel):
                with mock.patch.object(main, "_symbol_perf_gate", side_effect=fake_perf):
                    picked_symbol, _picked_intel, board = await main._pick_best_symbol_from_scan(cfg)

        self.assertEqual(picked_symbol, "REC2USDT")
        picked_row = next(row for row in board if row["symbol"] == "REC2USDT")
        self.assertTrue(picked_row["softFallbackPicked"])
        self.assertEqual(picked_row["rejectReason"], "perf_soft_fallback:perf_lock_recovered")

    async def test_scan_does_not_soft_fallback_unrecovered_active_lock(self):
        async def fake_intel(req):
            return {
                "signal": "LONG",
                "confidence": 0.88,
                "execution": {"momentumPct": 0.25, "spreadBps": 1.0},
            }

        def fake_perf(_cfg, _symbol):
            return False, "perf_lock(42m)", {
                "trades": 30,
                "winRatePct": 26.0,
                "pnl": -1.5,
                "lockReason": "sustained",
            }

        cfg = {
            "minConfidence": 0.7,
            "scanAnalyzeTop": 3,
            "scanGuardedFallbackAnalyzeTop": 3,
            "scanSidePreference": "score",
            "scanPerfSoftFallbackEnabled": True,
        }
        symbols = ["BAD1USDT", "BAD2USDT", "BAD3USDT"]
        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=symbols)):
            with mock.patch.object(main, "intel_analyze", new=fake_intel):
                with mock.patch.object(main, "_symbol_perf_gate", side_effect=fake_perf):
                    picked_symbol, picked_intel, board = await main._pick_best_symbol_from_scan(cfg)

        self.assertIsNone(picked_symbol)
        self.assertIsNone(picked_intel)
        self.assertFalse(any(row.get("softFallbackPicked") for row in board))

    async def test_scan_does_not_soft_fallback_reward_locked_symbols(self):
        async def fake_intel(req):
            return {
                "signal": "LONG",
                "confidence": 0.88,
                "execution": {"momentumPct": 0.25, "spreadBps": 1.0},
            }

        cfg = {
            "minConfidence": 0.7,
            "scanAnalyzeTop": 3,
            "scanGuardedFallbackAnalyzeTop": 3,
            "scanSidePreference": "score",
            "scanPerfSoftFallbackEnabled": True,
        }
        symbols = ["BAD1USDT", "BAD2USDT", "BAD3USDT"]
        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=symbols)):
            with mock.patch.object(main, "intel_analyze", new=fake_intel):
                with mock.patch.object(
                    main,
                    "_symbol_perf_gate",
                    return_value=(False, "perf_lock_reward", {"trades": 8, "winRatePct": 25.0, "pnl": -0.8}),
                ):
                    picked_symbol, picked_intel, board = await main._pick_best_symbol_from_scan(cfg)

        self.assertIsNone(picked_symbol)
        self.assertIsNone(picked_intel)
        self.assertFalse(any(row.get("softFallbackPicked") for row in board))


class TestSymbolPerfGate(unittest.TestCase):
    def setUp(self):
        self.prev_locks = main.AUTO_TRADE.get("perfLocks")
        self.prev_wait_state = main.AUTO_TRADE.get("symbolWaitState")
        self.prev_config = main.AUTO_TRADE.get("config")
        main.AUTO_TRADE["perfLocks"] = {}
        main.AUTO_TRADE["symbolWaitState"] = {}

    def tearDown(self):
        main.AUTO_TRADE["perfLocks"] = self.prev_locks if isinstance(self.prev_locks, dict) else {}
        main.AUTO_TRADE["symbolWaitState"] = self.prev_wait_state if isinstance(self.prev_wait_state, dict) else {}
        main.AUTO_TRADE["config"] = self.prev_config

    def test_perf_gate_locks_early_underperformer(self):
        cfg = {"perfGateEarlyMinSamples": 4, "perfGateEarlyMinWinRatePct": 35, "perfGateEarlyMinPnlUsdt": -0.35}
        perf = {"trades": 4, "wins": 1, "losses": 3, "winRatePct": 25.0, "pnl": -0.5}

        with mock.patch.object(main, "_rolling_symbol_perf", return_value=perf):
            with mock.patch.object(main, "_load_single_profile", return_value={"rewardScore": 0.0}):
                ok, reason, out = main._symbol_perf_gate(cfg, "BADUSDT")

        self.assertFalse(ok)
        self.assertEqual(reason, "perf_lock_early")
        self.assertEqual(out, perf)

    def test_perf_gate_locks_negative_reward_symbol(self):
        cfg = {"perfGateEarlyMinSamples": 4, "perfGateMinRewardScore": -1.25}
        perf = {"trades": 6, "wins": 3, "losses": 3, "winRatePct": 50.0, "pnl": 0.05}

        with mock.patch.object(main, "_rolling_symbol_perf", return_value=perf):
            with mock.patch.object(main, "_load_single_profile", return_value={"rewardScore": -2.0}):
                ok, reason, _out = main._symbol_perf_gate(cfg, "BADUSDT")

        self.assertFalse(ok)
        self.assertEqual(reason, "perf_lock_reward")

    def test_rolling_symbol_perf_prefers_recent_7_day_memory(self):
        now = int(main.time.time())
        recent = [
            {"symbol": "MEMUSDT", "_pnl": 0.2, "_ts": now - (i * 3600)}
            for i in range(6)
        ]
        archive_losses = [
            {"symbol": "MEMUSDT", "_pnl": -1.0, "_ts": now - (45 * 86400) - (i * 3600)}
            for i in range(20)
        ]

        with mock.patch.object(main, "_live_closed_trades_from_log", return_value=[*archive_losses, *recent]):
            out = main._rolling_symbol_perf("MEMUSDT", 30)

        self.assertEqual(out["memoryWindow"], "7d")
        self.assertEqual(out["trades"], 6)
        self.assertEqual(out["wins"], 6)
        self.assertGreater(out["pnl"], 0)

    def test_rolling_symbol_perf_falls_back_to_30_day_memory_when_7_day_sparse(self):
        now = int(main.time.time())
        recent = [{"symbol": "MEMUSDT", "_pnl": 0.2, "_ts": now - 3600}]
        month_rows = [
            {"symbol": "MEMUSDT", "_pnl": -0.2, "_ts": now - (10 * 86400) - (i * 3600)}
            for i in range(6)
        ]

        with mock.patch.object(main, "_live_closed_trades_from_log", return_value=[*month_rows, *recent]):
            out = main._rolling_symbol_perf("MEMUSDT", 30)

        self.assertEqual(out["memoryWindow"], "15d")
        self.assertEqual(out["trades"], 7)
        self.assertLess(out["pnl"], 0)

    def test_perf_gate_releases_stale_active_lock_without_trade_evidence(self):
        now = int(main.time.time())
        main.AUTO_TRADE["perfLocks"] = {
            "NEWUSDT": {
                "until": now + 900,
                "at": now - 3600,
                "reason": "sustained",
                "perf": {"trades": 0, "wins": 0, "losses": 0, "winRatePct": 0.0, "pnl": 0.0},
            }
        }
        cfg = {"perfLockNoEvidenceMaxAgeMinutes": 15, "perfLockMinutes": 240}
        perf = {"trades": 0, "wins": 0, "losses": 0, "winRatePct": 0.0, "pnl": 0.0}

        with mock.patch.object(main, "_rolling_symbol_perf", return_value=perf):
            with mock.patch.object(main, "_load_single_profile", return_value={"rewardScore": 0.0}):
                ok, reason, out = main._symbol_perf_gate(cfg, "NEWUSDT")

        self.assertTrue(ok)
        self.assertEqual(reason, "")
        self.assertEqual(out, perf)
        self.assertNotIn("NEWUSDT", main.AUTO_TRADE["perfLocks"])

    def test_active_fapi_agreement_locks_filters_only_live_4411_locks(self):
        now = int(main.time.time())
        main.AUTO_TRADE["perfLocks"] = {
            "XAUUSDT": {"until": now + 900, "reason": "fapi_agreement", "error": "-4411"},
            "OLDUSDT": {"until": now - 1, "reason": "fapi_agreement", "error": "-4411"},
            "BADUSDT": {"until": now + 900, "reason": "sustained"},
        }

        out = main._active_fapi_agreement_locks()

        self.assertEqual(list(out.keys()), ["XAUUSDT"])
        self.assertEqual(out["XAUUSDT"]["error"], "-4411")

    def test_stale_wait_symbol_switches_fixed_mode_to_auto_scan(self):
        now = int(main.time.time())
        cfg = {
            "symbol": "XAUUSDT",
            "primarySymbol": "XAUUSDT",
            "marketScan": False,
            "whitelistSymbols": ["XAUUSDT"],
            "staleWaitSymbolSkipCycles": 2,
            "staleWaitSymbolLockMinutes": 10,
        }
        main.AUTO_TRADE["config"] = cfg

        first = main._maybe_skip_stale_wait_symbol(cfg, "XAUUSDT", "WAIT", scan_mode=False, now=now)
        second = main._maybe_skip_stale_wait_symbol(cfg, "XAUUSDT", "WAIT", scan_mode=False, now=now + 20)

        self.assertFalse(first)
        self.assertTrue(second)
        self.assertEqual(cfg["symbol"], "AUTO")
        self.assertTrue(cfg["marketScan"])
        self.assertEqual(cfg["whitelistSymbols"], [])
        self.assertEqual(main.AUTO_TRADE["perfLocks"]["XAUUSDT"]["reason"], "stale_wait")

    def test_perf_gate_keeps_active_lock_with_stored_trade_evidence(self):
        now = int(main.time.time())
        main.AUTO_TRADE["perfLocks"] = {
            "BADUSDT": {
                "until": now + 900,
                "at": now - 3600,
                "reason": "sustained",
                "perf": {"trades": 12, "wins": 3, "losses": 9, "winRatePct": 25.0, "pnl": -1.2},
            }
        }
        cfg = {"perfLockNoEvidenceMaxAgeMinutes": 15, "perfLockMinutes": 240}
        perf = {"trades": 0, "wins": 0, "losses": 0, "winRatePct": 0.0, "pnl": 0.0}

        with mock.patch.object(main, "_rolling_symbol_perf", return_value=perf):
            with mock.patch.object(main, "_load_single_profile", return_value={"rewardScore": 0.0}):
                ok, reason, out = main._symbol_perf_gate(cfg, "BADUSDT")

        self.assertFalse(ok)
        self.assertTrue(reason.startswith("perf_lock("))
        self.assertEqual(out["lockEvidenceTrades"], 12)
        self.assertIn("BADUSDT", main.AUTO_TRADE["perfLocks"])

    def test_symbol_quality_prioritizes_win_streak_and_deprioritizes_loss_streak(self):
        profiles = {
            "WINUSDT": {
                "wins": 5,
                "losses": 1,
                "realizedPnl": 1.2,
                "rewardScore": 4.0,
                "rewardDelta": 1.0,
                "rewardBehaviorDelta": 0.6,
                "rewardWinStreak": 4,
                "rewardLossStreak": 0,
            },
            "LOSSUSDT": {
                "wins": 1,
                "losses": 5,
                "realizedPnl": -1.2,
                "rewardScore": -4.0,
                "rewardDelta": -1.0,
                "rewardBehaviorDelta": -0.6,
                "rewardWinStreak": 0,
                "rewardLossStreak": 4,
            },
        }

        with mock.patch.object(main, "_load_single_profile", side_effect=lambda sym: profiles.get(sym, {})):
            win_score = main._symbol_quality_score("WINUSDT")
            loss_score = main._symbol_quality_score("LOSSUSDT")

        self.assertGreater(win_score, 0.10)
        self.assertLess(loss_score, -0.10)
        self.assertGreater(win_score, loss_score)

    def test_learned_min_conf_loosens_winners_and_tightens_losers(self):
        profiles = {
            "WINUSDT": {"wins": 5, "losses": 1, "rewardScore": 4.0, "rewardWinStreak": 4},
            "LOSSUSDT": {"wins": 1, "losses": 5, "rewardScore": -4.0, "rewardLossStreak": 4},
        }

        with mock.patch.object(main, "_load_single_profile", side_effect=lambda sym: profiles.get(sym, {})):
            winner_min = main._learned_min_conf("WINUSDT", 0.62)
            loser_min = main._learned_min_conf("LOSSUSDT", 0.62)

        self.assertLess(winner_min, 0.62)
        self.assertGreater(loser_min, 0.62)

    def test_rolling_symbol_perf_calculates_payoff_ratio(self):
        rows = [
            {"_pnl": 0.12},
            {"_pnl": 0.08},
            {"_pnl": 0.10},
            {"_pnl": -0.50},
            {"_pnl": 0.15},
            {"_pnl": -0.40},
        ]

        with mock.patch.object(main, "_live_closed_trades_from_log", return_value=rows):
            perf = main._rolling_symbol_perf("BADUSDT", 30)

        self.assertEqual(perf["trades"], 6)
        self.assertEqual(perf["wins"], 4)
        self.assertEqual(perf["losses"], 2)
        self.assertEqual(perf["pnl"], -0.45)
        self.assertEqual(perf["avgWin"], 0.1125)
        self.assertEqual(perf["avgLoss"], -0.45)
        self.assertEqual(perf["payoffRatio"], 0.25)

    def test_perf_gate_locks_high_winrate_negative_payoff_symbol(self):
        cfg = {
            "payoffGuardEnabled": True,
            "payoffGuardMinSamples": 8,
            "payoffGuardMinWinRatePct": 55.0,
            "payoffGuardMaxPnlUsdt": -0.25,
            "payoffGuardMinPayoffRatio": 0.72,
            "payoffGuardMinLosses": 2,
            "perfLockMinutes": 30,
        }
        perf = {
            "trades": 10,
            "wins": 7,
            "losses": 3,
            "winRatePct": 70.0,
            "pnl": -0.80,
            "avgWin": 0.10,
            "avgLoss": -0.50,
            "payoffRatio": 0.20,
        }

        with mock.patch.object(main, "_rolling_symbol_perf", return_value=perf):
            with mock.patch.object(main, "_load_single_profile", return_value={"rewardScore": 0.0}):
                ok, reason, out = main._symbol_perf_gate(cfg, "BADUSDT")

        self.assertFalse(ok)
        self.assertEqual(reason, "perf_lock_payoff")
        self.assertEqual(out, perf)
        self.assertEqual(main.AUTO_TRADE["perfLocks"]["BADUSDT"]["reason"], "payoff")


class TestStatusLitePositionCard(unittest.TestCase):
    def setUp(self):
        self.prev_config = main.AUTO_TRADE.get("config")
        self.prev_locks = main.AUTO_TRADE.get("liveProfitLocks")
        self.prev_paper = main.AUTO_TRADE.get("paper")
        self.prev_last_decision = main.AUTO_TRADE.get("lastDecision")
        self.prev_log = main.AUTO_TRADE.get("log")
        self.prev_trades = main.AUTO_TRADE.get("trades")
        self.prev_agents = main.AUTO_TRADE.get("hermesAgents")
        self.prev_running = main.AUTO_TRADE.get("running")
        self.prev_scan_board = main.AUTO_TRADE.get("scanBoard")
        self.prev_perf_locks = main.AUTO_TRADE.get("perfLocks")
        self.prev_supervisor_auto_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        self.prev_supervisor_review = main.AUTO_TRADE.get("hermesSupervisorReview")
        main.AUTO_TRADE.pop("hermesSupervisorReview", None)
        self.prev_risk = dict(main.RISK)
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "symbol": "AUTO",
            "intervalSec": 25,
            "marketScan": True,
            "leverage": 5,
            "leverageMin": 5,
            "leverageMax": 10,
            "leverageAutoEnabled": True,
        }

    def tearDown(self):
        main.AUTO_TRADE["config"] = self.prev_config
        main.AUTO_TRADE["liveProfitLocks"] = self.prev_locks
        main.AUTO_TRADE["paper"] = self.prev_paper
        main.AUTO_TRADE["lastDecision"] = self.prev_last_decision
        main.AUTO_TRADE["log"] = self.prev_log
        main.AUTO_TRADE["trades"] = self.prev_trades
        main.AUTO_TRADE["hermesAgents"] = self.prev_agents
        main.AUTO_TRADE["running"] = self.prev_running
        main.AUTO_TRADE["scanBoard"] = self.prev_scan_board
        main.AUTO_TRADE["perfLocks"] = self.prev_perf_locks
        main.AUTO_TRADE["supervisorAutoTune"] = self.prev_supervisor_auto_tune
        if self.prev_supervisor_review is None:
            main.AUTO_TRADE.pop("hermesSupervisorReview", None)
        else:
            main.AUTO_TRADE["hermesSupervisorReview"] = self.prev_supervisor_review
        main.RISK.clear()
        main.RISK.update(self.prev_risk)

    def test_format_loop_error_handles_request_error_without_request(self):
        err = main.httpx.ConnectTimeout("data provider cooldown active")

        msg = main._format_loop_error(err)

        self.assertIn("RequestError", msg)
        self.assertIn("data provider cooldown active", msg)

    def test_no_trade_window_end_is_exclusive(self):
        noon = main.time.struct_time((2026, 6, 7, 12, 0, 0, 6, 158, -1))
        end = main.time.struct_time((2026, 6, 7, 13, 0, 0, 6, 158, -1))
        overnight_end = main.time.struct_time((2026, 6, 7, 1, 0, 0, 6, 158, -1))

        self.assertTrue(main._within_no_trade_window(noon, ["12:00-13:00"]))
        self.assertFalse(main._within_no_trade_window(end, ["12:00-13:00"]))
        self.assertFalse(main._within_no_trade_window(overnight_end, ["23:00-01:00"]))

    def test_status_lite_exposes_position_from_profit_locks(self):
        main.AUTO_TRADE["liveProfitLocks"] = {
            "XRPUSDT:SHORT": {
                "symbol": "XRPUSDT",
                "side": "SHORT",
                "qty": 21.8,
                "entryMark": 1.2909,
                "tp": 1.2676,
                "sl": 1.3025,
                "armed": False,
                "peak": 0.0,
                "lockUsdt": 0.0,
            }
        }

        out = main.asyncio.run(main.autotrade_status_lite())

        self.assertEqual(out["activePosition"]["live"]["side"], "SHORT")
        self.assertEqual(out["activePosition"]["live"]["symbol"], "XRPUSDT")
        self.assertEqual(out["activePosition"]["live"]["qty"], 21.8)
        self.assertEqual(out["activePosition"]["live"]["localTp"], 1.2676)
        self.assertEqual(out["openLivePositions"][0]["symbol"], "XRPUSDT")
        self.assertEqual(out["openLivePositions"][0]["localTp"], 1.2676)

    def test_status_lite_keeps_auto_symbol_public_when_scan_pick_is_active(self):
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "symbol": "XAUUSDT",
            "primarySymbol": "BTCUSDT",
            "marketScan": True,
            "leverage": 5,
            "leverageMin": 3,
            "leverageMax": 25,
        }
        main.AUTO_TRADE["lastDecision"] = {"symbol": "XAUUSDT", "signal": "SHORT", "confidence": 0.8}

        out = main.asyncio.run(main.autotrade_status_lite())

        self.assertEqual(out["config"]["symbol"], "AUTO")
        self.assertTrue(out["config"]["marketScan"])
        self.assertEqual(out["config"]["activeScanSymbol"], "XAUUSDT")

    def test_status_lite_exposes_position_from_profit_lock(self):
        main.AUTO_TRADE["liveProfitLocks"] = {
            "DOGEUSDT:LONG": {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "qty": 148.0,
                "leverage": 17,
                "entryMark": 0.098,
                "markPrice": 0.099,
                "notionalUsdtApprox": 14.652,
                "unRealizedProfit": 0.12,
                "tp": 0.099764,
                "sl": 0.097118,
            }
        }

        out = main.asyncio.run(main.autotrade_status_lite())

        self.assertEqual(out["activePosition"]["live"]["side"], "LONG")
        self.assertEqual(out["activePosition"]["live"]["symbol"], "DOGEUSDT")
        self.assertEqual(out["activePosition"]["live"]["leverage"], 17)
        self.assertEqual(out["openLivePositions"][0]["symbol"], "DOGEUSDT")
        self.assertEqual(out["openLivePositions"][0]["leverage"], 17)
        self.assertAlmostEqual(out["openLivePositions"][0]["notionalUsdtApprox"], 14.652)

    def test_status_lite_heartbeats_guardian_from_open_positions(self):
        state = main.new_agent_state()
        state["agents"]["position_guardian"]["state"] = "doing"
        state["agents"]["position_guardian"]["lastAction"] = "monitor open live positions"
        state["agents"]["position_guardian"]["updatedAt"] = int(main.time.time()) - 600
        state["agents"]["position_guardian"]["runs"] = 7
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["liveProfitLocks"] = {
            "DOGEUSDT:LONG": {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "qty": 148.0,
                "entryMark": 0.098,
                "markPrice": 0.099,
                "notionalUsdtApprox": 14.652,
                "unRealizedProfit": 0.12,
                "tp": 0.099764,
                "sl": 0.097118,
            }
        }

        out = main.asyncio.run(main.autotrade_status_lite())

        guardian = out["hermesAgents"]["agents"]["position_guardian"]
        self.assertEqual(guardian["state"], "done")
        self.assertEqual(guardian["lastAction"], "open positions heartbeat")
        self.assertEqual(guardian["runs"], 8)
        self.assertTrue(guardian["data"]["heartbeat"])
        self.assertLessEqual(int(main.time.time()) - int(guardian["updatedAt"]), 2)
        self.assertNotIn("cmuxHandoff", out["hermesSupervisorReview"])

    def test_supervisor_ignores_stopped_scan_board_perf_locks(self):
        main.AUTO_TRADE["running"] = False
        main.AUTO_TRADE["scanBoard"] = [
            {"symbol": "AUSDT", "rejectReason": "perf_lock(30m)", "qualified": False},
            {"symbol": "BUSDT", "rejectReason": "perf_lock(30m)", "qualified": False},
            {"symbol": "CUSDT", "rejectReason": "perf_lock(30m)", "qualified": False},
        ]

        out = main.asyncio.run(main.autotrade_status_lite())

        review = out["hermesSupervisorReview"]
        self.assertNotIn("cmuxHandoff", review)

    def test_status_lite_keeps_dashboard_kpi_fields(self):
        main.AUTO_TRADE["paper"] = {"wins": 2, "losses": 1, "realizedPnl": 1.25, "position": None, "history": []}
        main.AUTO_TRADE["liveProfitLocks"] = {}

        def fake_stats(symbol=None):
            if symbol is None:
                return {"wins": 4, "losses": 2, "realizedPnl": 3.4, "winsToday": 1, "lossesToday": 1, "realizedPnlToday": -0.2}
            return {"wins": 3, "losses": 1, "realizedPnl": 2.75, "winsToday": 1, "lossesToday": 0, "realizedPnlToday": 0.5, "lastTrades": []}

        with mock.patch.object(main, "_aggregate_live_trade_stats_from_log", side_effect=fake_stats):
            out = main.asyncio.run(main.autotrade_status_lite())

        self.assertEqual(out["paper"]["winRatePct"], 66.67)
        self.assertEqual(out["paper"]["realizedPnl"], 1.25)
        self.assertEqual(out["liveStats"]["wins"], 3)
        self.assertEqual(out["liveStats"]["winRatePct"], 75.0)
        self.assertEqual(out["liveStats"]["realizedPnl"], 2.75)
        self.assertEqual(out["liveStatsAll"]["wins"], 4)
        self.assertEqual(out["liveStatsAll"]["winRatePct"], 66.67)
        self.assertEqual(out["liveStatsAll"]["realizedPnl"], 3.4)
        self.assertEqual(out["kpiTodayAllSymbols"]["live"]["realizedPnl"], -0.2)
        self.assertEqual(out["kpiToday"]["live"]["wins"], 1)
        self.assertEqual(out["kpiToday"]["live"]["losses"], 1)
        self.assertEqual(out["kpiToday"]["live"]["winRatePct"], 50.0)
        self.assertEqual(out["kpiToday"]["live"]["realizedPnl"], -0.2)
        self.assertEqual(out["liveDailyPnl"], -0.2)

    def test_learning_status_aggregates_all_symbols_in_one_log_pass(self):
        import tempfile
        stats_by_symbol = {
            "BTCUSDT": {"wins": 2, "losses": 1, "realizedPnl": 1.25},
            "ETHUSDT": {"wins": 0, "losses": 1, "realizedPnl": -0.5},
        }

        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)
            ada_dir = vault / "symbols" / "ADAUSDT"
            ada_dir.mkdir(parents=True)
            (ada_dir / "profile.json").write_text("{}")
            with mock.patch.object(main, "VAULT_DIR", vault), \
                 mock.patch.object(main, "_aggregate_live_trade_stats_by_symbol_from_log", return_value=stats_by_symbol) as aggregate_all, \
                 mock.patch.object(main, "_aggregate_live_trade_stats_from_log", side_effect=AssertionError("per-symbol reread")):
                out = main.learning_status()

        aggregate_all.assert_called_once()
        by_symbol = {row["symbol"]: row for row in out["items"]}
        self.assertEqual(by_symbol["BTCUSDT"]["wins"], 2)
        self.assertEqual(by_symbol["BTCUSDT"]["winRatePct"], 66.67)
        self.assertEqual(by_symbol["ETHUSDT"]["realizedPnl"], -0.5)
        self.assertEqual(by_symbol["ADAUSDT"]["wins"], 0)
        self.assertEqual(out["source"], "trades_log")

    def test_signed_request_diagnostic_masks_key_and_identifies_fapi_order(self):
        with mock.patch.dict(main.os.environ, {"BINANCE_TESTNET": "false"}):
            diag = main._signed_request_diagnostic(
                "https://fapi.binance.com",
                "/fapi/v1/order",
                "ABCDEF1234567890",
                {
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "type": "MARKET",
                    "positionSide": "LONG",
                    "reduceOnly": "true",
                },
            )

        self.assertEqual(diag["keyPrefix"], "ABCDEF")
        self.assertNotIn("1234567890", str(diag))
        self.assertEqual(diag["marketType"], "USDT-M")
        self.assertEqual(diag["endpoint"], "/fapi/v1/order")
        self.assertEqual(diag["orderType"], "MARKET")
        self.assertEqual(diag["symbol"], "BTCUSDT")
        self.assertFalse(diag["binanceTestnet"])

    def test_status_lite_keeps_decision_log_fields(self):
        main.AUTO_TRADE["lastDecision"] = {"symbol": "XRPUSDT", "signal": "LONG", "confidence": 0.77}
        main.AUTO_TRADE["log"] = [{"ts": 123, "msg": "Skip: already in LONG"}]
        main.AUTO_TRADE["trades"] = [int(main.time.time())]

        out = main.asyncio.run(main.autotrade_status_lite())

        self.assertEqual(out["lastDecision"]["symbol"], "XRPUSDT")
        self.assertEqual(out["lastDecision"]["signal"], "LONG")
        self.assertEqual(out["log"][0]["msg"], "Skip: already in LONG")
        self.assertEqual(out["tradesLastHour"], 1)

    def test_status_lite_exposes_hermes_agents_kanban(self):
        main.AUTO_TRADE["hermesAgents"] = main.start_cycle(main.new_agent_state())
        main._agent_mark("market_analyst", "done", "scan completed", "BTCUSDT")

        out = main.asyncio.run(main.autotrade_status_lite())

        self.assertIn("hermesAgents", out)
        self.assertIn("market_analyst", out["hermesAgents"]["agents"])
        self.assertIn("market_analyst", out["hermesAgents"]["kanban"]["done"])
        self.assertEqual(out["hermesAgents"]["engine"]["title"], "AI Decision Engine")
        self.assertEqual(
            out["hermesAgents"]["agents"]["market_analyst"]["playbookPath"],
            "docs/hermes_agents/market_analyst.md",
        )

    def test_agent_state_backfills_playbook_paths(self):
        state = main.ensure_agent_state(
            {
                "agents": {
                    "execution_agent": {
                        "id": "execution_agent",
                        "name": "Execution Agent",
                        "role": "Order placement and exchange responses",
                    }
                }
            }
        )

        self.assertEqual(
            state["agents"]["execution_agent"]["playbookPath"],
            "docs/hermes_agents/execution_agent.md",
        )
        self.assertEqual(
            state["agents"]["memory_agent"]["playbookPath"],
            "docs/hermes_agents/memory_agent.md",
        )

    def test_status_lite_keeps_leverage_config_and_risk_cap(self):
        main.RISK["max_leverage"] = 25
        main.AUTO_TRADE["config"].update({"leverage": 10, "leverageMin": 10, "leverageMax": 25})

        out = main.asyncio.run(main.autotrade_status_lite())

        self.assertEqual(out["config"]["leverage"], 10)
        self.assertEqual(out["config"]["leverageMin"], 10)
        self.assertEqual(out["config"]["leverageMax"], 25)
        self.assertEqual(out["riskLimits"]["maxLeverage"], 25)

    def test_autotrade_update_config_accepts_10_to_25_leverage_range(self):
        main.RISK["max_leverage"] = 25
        main.AUTO_TRADE["config"]["adaptiveLeverageMax"] = 10

        with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
            out = main.autotrade_update_config({"leverage": 10, "leverageMin": 10, "leverageMax": 25})

        self.assertTrue(out["ok"])
        self.assertTrue(out["updated"])
        self.assertEqual(out["config"]["leverage"], 10)
        self.assertEqual(out["config"]["leverageMin"], 10)
        self.assertEqual(out["config"]["leverageMax"], 25)
        self.assertEqual(out["config"]["adaptiveLeverageMax"], 25)

    def test_dashboard_uses_single_leverage_max_as_adaptive_cap(self):
        html = Path(__file__).with_name("dashboard").joinpath("index.html").read_text(encoding="utf-8")

        self.assertIn("<label>Lev max</label>", html)
        self.assertIn('id="cfgLevMin" type="hidden" value="1"', html)
        self.assertIn('id="cfgLevMax" type="number" value="25"', html)
        self.assertIn("adaptiveLeverageMax: lev.max", html)
        self.assertNotIn("adaptiveLeverageMax: 25", html)
        self.assertIn("Lev max x${lev.max}", html)

    def test_adaptive_symbol_leverage_boosts_calm_symbols_and_caps_at_25(self):
        main.RISK["max_leverage"] = 125
        cfg = {
            "leverage": 5,
            "leverageMin": 3,
            "leverageMax": 60,
            "adaptiveLeverageMax": 50,
            "leverageAutoEnabled": True,
            "adaptiveLeverageEnabled": True,
            "minConfidence": 0.66,
            "maxSpreadBps": 16,
            "perfWindowTrades": 30,
            "perfGateMinSamples": 8,
        }
        calm_intel = {
            "confidence": 0.92,
            "precision": {"atrPct": 0.045, "bbPctB": 0.52, "vwapDistancePct": 0.03, "longScore": 4.2, "shortScore": 0.7},
            "execution": {"spreadBps": 2.0, "momentumPct": 0.04},
        }
        volatile_intel = {
            "confidence": 0.92,
            "precision": {"atrPct": 0.95, "bbPctB": 0.98, "vwapDistancePct": 0.75, "longScore": 4.2, "shortScore": 0.7},
            "execution": {"spreadBps": 24.0, "momentumPct": 0.72},
        }

        with (
            mock.patch.object(main, "_symbol_quality_score", return_value=0.10),
            mock.patch.object(main, "_rolling_symbol_perf", return_value={"trades": 0}),
            mock.patch.object(main, "_load_single_profile", return_value={}),
        ):
            calm = main._adaptive_symbol_leverage("SLOWUSDT", calm_intel, cfg)
            volatile = main._adaptive_symbol_leverage("FASTUSDT", volatile_intel, cfg)

        self.assertLessEqual(calm["leverage"], 25)
        self.assertLessEqual(volatile["leverage"], 25)
        self.assertEqual(calm["max"], 25)
        self.assertGreater(calm["leverage"], volatile["leverage"])

    def test_adaptive_symbol_leverage_can_be_disabled(self):
        cfg = {"leverage": 9, "leverageMin": 3, "leverageMax": 25, "adaptiveLeverageEnabled": False}

        out = main._adaptive_symbol_leverage("BTCUSDT", {"confidence": 0.99}, cfg)

        self.assertFalse(out["auto"])
        self.assertEqual(out["leverage"], 9)

    def test_status_lite_exposes_supervisor_review(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "risk_manager", "blocked", "symbol daily cap", "XLMUSDT 14/14")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.asyncio.run(main.autotrade_status_lite())

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "high")
        self.assertTrue(any(x["agent"] == "risk_manager" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_treats_portfolio_capacity_as_hold(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "portfolio_manager", "blocked", "portfolio capacity full", "4/4")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.asyncio.run(main.autotrade_status_lite())

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "low")
        self.assertTrue(any(x["title"] == "Capacity hold" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_auto_handles_symbol_day_cap_scan_loop(self):
        prev = {
            "running": main.AUTO_TRADE.get("running"),
            "config": main.AUTO_TRADE.get("config"),
            "lastSkip": main.AUTO_TRADE.get("lastSkip"),
            "log": main.AUTO_TRADE.get("log"),
            "scanBoard": main.AUTO_TRADE.get("scanBoard"),
            "hermesAgents": main.AUTO_TRADE.get("hermesAgents"),
        }
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {"executionMode": "LIVE", "marketScan": True, "symbol": "AUTO"}
        main.AUTO_TRADE["lastSkip"] = {
            "code": "symbol_day_cap",
            "msg": "Skip: NEARUSDT reached daily cap 14/14",
        }
        main.AUTO_TRADE["log"] = [{"msg": "Skip: NEARUSDT reached daily cap 14/14"}]
        main.AUTO_TRADE["scanBoard"] = [
            {"symbol": "NEARUSDT", "qualified": True},
            {"symbol": "FREEUSDT", "qualified": True},
        ]
        main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()

        try:
            with mock.patch.object(main, "_cooldown_scan_symbol") as cooldown:
                with mock.patch.object(main, "_persist_autotrade_snapshot"):
                    review = main._hermes_supervisor_review(main.AUTO_TRADE)
                    scan_board_after = list(main.AUTO_TRADE.get("scanBoard", []))
        finally:
            for key, value in prev.items():
                main.AUTO_TRADE[key] = value

        cooldown.assert_called_once()
        self.assertEqual(cooldown.call_args.args[0], "NEARUSDT")
        self.assertFalse(any(row.get("symbol") == "NEARUSDT" for row in scan_board_after))
        self.assertTrue(
            any(
                action.get("agent") == "portfolio_manager"
                and action.get("status") == "applied"
                and action.get("issueType") == "symbol_day_cap"
                for action in review["autoActions"]
            )
        )
        self.assertTrue(
            any(
                action.get("agent") == "market_analyst"
                and action.get("status") == "applied"
                and action.get("issueType") == "symbol_day_cap"
                for action in review["autoActions"]
            )
        )
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_auto_handles_mixed_perf_lock_and_analyze_error_board(self):
        prev = {
            "running": main.AUTO_TRADE.get("running"),
            "config": main.AUTO_TRADE.get("config"),
            "lastSkip": main.AUTO_TRADE.get("lastSkip"),
            "log": main.AUTO_TRADE.get("log"),
            "scanBoard": main.AUTO_TRADE.get("scanBoard"),
            "hermesAgents": main.AUTO_TRADE.get("hermesAgents"),
        }
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "marketScan": True,
            "symbol": "AUTO",
            "scanAnalyzeTop": 6,
            "scanTopLiquid": 25,
            "scanGuardedFallbackAnalyzeTop": 12,
        }
        main.AUTO_TRADE["lastSkip"] = None
        main.AUTO_TRADE["log"] = []
        main.AUTO_TRADE["scanBoard"] = [
            {"symbol": "BCHUSDT", "signal": "SHORT", "confidence": 0.81, "score": 0.8, "qualified": False, "rejectReason": "perf_lock(88m)"},
            {"symbol": "ZECUSDT", "signal": "SHORT", "confidence": 0.80, "score": 0.8, "qualified": False, "rejectReason": "perf_lock_payoff"},
            {"symbol": "XAUUSDT", "signal": "SHORT", "confidence": 0.78, "score": 0.8, "qualified": True, "rejectReason": ""},
            {"symbol": "INTCUSDT", "signal": "WAIT", "confidence": 0.48, "score": 0.3, "qualified": False, "rejectReason": "signal_wait"},
            {"symbol": "TONUSDT", "signal": "WAIT", "confidence": 0.0, "score": -999.0, "qualified": False, "rejectReason": "analyze_error"},
            {"symbol": "TAOUSDT", "signal": "WAIT", "confidence": 0.0, "score": -999.0, "qualified": False, "rejectReason": "analyze_error"},
        ]
        main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()

        try:
            with mock.patch.object(main, "_cooldown_scan_symbol") as cooldown:
                with mock.patch.object(main, "_persist_autotrade_snapshot"):
                    review = main._hermes_supervisor_review(main.AUTO_TRADE)
                    cfg_after = dict(main.AUTO_TRADE.get("config", {}))
        finally:
            for key, value in prev.items():
                main.AUTO_TRADE[key] = value

        self.assertEqual([call.args[0] for call in cooldown.call_args_list], ["TONUSDT", "TAOUSDT"])
        self.assertGreater(cfg_after["scanAnalyzeTop"], 6)
        self.assertGreater(cfg_after["scanTopLiquid"], 25)
        self.assertTrue(any(x["title"] == "Mixed scan board degradation" for x in review["issues"]))
        self.assertTrue(
            any(
                action.get("agent") == "market_analyst"
                and action.get("status") == "applied"
                and action.get("issueType") == "mixed_scan_board_degradation"
                for action in review["autoActions"]
            )
        )
        self.assertTrue(
            any(
                action.get("agent") == "strategy_builder"
                and action.get("status") == "recommended"
                and action.get("issueType") == "mixed_scan_board_degradation"
                for action in review["autoActions"]
            )
        )
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_treats_adaptive_cooldown_as_safety_hold(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "risk_manager", "blocked", "adaptive cooldown hold", "market volatile (XLMUSDT)")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.asyncio.run(main.autotrade_status_lite())

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "low")
        self.assertTrue(any(x["title"] == "Safety hold" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_treats_no_trade_window_as_safety_hold(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "risk_manager", "blocked", "no trade window", "")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.asyncio.run(main.autotrade_status_lite())

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "low")
        self.assertTrue(any(x["title"] == "Safety hold" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_treats_late_chase_as_strategy_safety_hold(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "strategy_builder", "blocked", "late long chase", "bb=0.94 vwapDist=0.42%")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.asyncio.run(main.autotrade_status_lite())

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "low")
        self.assertTrue(any(x["title"] == "Safety hold" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_treats_signal_wait_as_strategy_safety_hold(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "strategy_builder", "blocked", "signal wait", "")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.asyncio.run(main.autotrade_status_lite())

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "low")
        self.assertTrue(any(x["title"] == "Safety hold" and x["agent"] == "strategy_builder" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_treats_adaptive_confidence_as_strategy_safety_hold(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "strategy_builder", "blocked", "confidence below adaptive minimum", "0.620 < 0.630")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.asyncio.run(main.autotrade_status_lite())

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "low")
        self.assertTrue(any(x["title"] == "Safety hold" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_treats_primary_symbol_open_as_market_safety_hold(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "market_analyst", "blocked", "primary symbol already open", "BTCUSDT")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.asyncio.run(main.autotrade_status_lite())

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "low")
        self.assertTrue(any(x["title"] == "Safety hold" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_treats_symbol_volatile_cooldown_as_market_safety_hold(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "market_analyst", "blocked", "symbol volatile cooldown", "HOMEUSDT 10m")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.asyncio.run(main.autotrade_status_lite())

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "low")
        self.assertTrue(any(x["title"] == "Safety hold" and x["agent"] == "market_analyst" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_keeps_cooldown_check_failed_actionable(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "risk_manager", "blocked", "adaptive cooldown check failed", "ValueError bad state")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.asyncio.run(main.autotrade_status_lite())

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "high")
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_treats_cooldown_check_timeout_as_safety_hold(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "risk_manager", "blocked", "adaptive cooldown check failed", "TimeoutError (no message)")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.asyncio.run(main.autotrade_status_lite())

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "low")
        self.assertTrue(any(x["title"] == "Safety hold" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_reports_stale_guardian_with_open_position(self):
        state = main.new_agent_state()
        state["agents"]["position_guardian"]["state"] = "done"
        state["agents"]["position_guardian"]["updatedAt"] = int(main.time.time()) - 120
        bot = {
            "hermesAgents": state,
            "openLivePositions": [{"symbol": "BTCUSDT", "side": "LONG", "qty": 1.0}],
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertEqual(review["severity"], "high")
        self.assertTrue(any(x["title"] == "Open positions not actively monitored" for x in review["issues"]))
        self.assertTrue(any(x["agent"] == "position_guardian" for x in review["autoActions"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_reports_repeated_fapi_agreement_rejects(self):
        state = main.new_agent_state()
        bot = {
            "hermesAgents": state,
            "running": True,
            "config": {"executionMode": "LIVE"},
            "openLivePositions": [],
            "lastSkip": {"code": "fapi_agreement_required", "msg": "Skip: Binance Futures/Perps agreement required (-4411)"},
            "log": [
                {"msg": "Skip: Binance Futures/Perps agreement required (-4411) · locked XAUUSDT"},
                {"msg": "Skip: Binance Futures/Perps agreement required (-4411) · locked CLUSDT"},
                {"msg": "Skip: Binance Futures/Perps agreement required (-4411) · locked XAUUSDT"},
            ],
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertTrue(any(x["title"] == "Exchange agreement rejects entries" for x in review["issues"]))
        self.assertTrue(
            any(
                x["agent"] == "execution_agent"
                and x.get("action") == "skip symbols rejected by -4411"
                and x.get("status") == "applied"
                for x in review["autoActions"]
            )
        )
        self.assertNotIn("cmuxHandoff", review)
        self.assertFalse(any(x["title"] == "Low entry activity" for x in review["issues"]))

    def test_supervisor_escalates_unhandled_repeated_fapi_agreement_rejects(self):
        state = main.new_agent_state()
        bot = {
            "hermesAgents": state,
            "running": True,
            "config": {"executionMode": "LIVE"},
            "openLivePositions": [],
            "lastSkip": {"code": "fapi_agreement_required", "msg": "Skip: Binance Futures/Perps agreement required (-4411)"},
            "log": [
                {"msg": "Skip: Binance Futures/Perps agreement required (-4411)"},
                {"msg": "Skip: Binance Futures/Perps agreement required (-4411)"},
                {"msg": "Skip: Binance Futures/Perps agreement required (-4411)"},
            ],
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertTrue(any(x["title"] == "Exchange agreement rejects entries" and x["severity"] == "high" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)
        self.assertTrue(
            any(
                x.get("action") == "skip symbols rejected by -4411"
                and x.get("status") == "recommended"
                for x in review["autoActions"]
            )
        )

    def test_supervisor_treats_single_fapi_agreement_lock_as_self_healed(self):
        state = main.new_agent_state()
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {"executionMode": "LIVE"}
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["lastSkip"] = {
            "code": "fapi_agreement_required",
            "msg": "Skip: Binance Futures/Perps agreement required (-4411)",
        }
        main.AUTO_TRADE["log"] = [
            {"msg": "Skip: Binance Futures/Perps agreement required (-4411) · locked XAUUSDT"},
        ]
        main.AUTO_TRADE["consecutiveErrors"] = 0
        main.AUTO_TRADE["perfLocks"] = {
            "XAUUSDT": {
                "until": int(main.time.time()) + 3600,
                "at": int(main.time.time()),
                "reason": "fapi_agreement",
                "error": "-4411",
            }
        }

        review = main._hermes_supervisor_review(main.AUTO_TRADE)

        self.assertFalse(any(x["title"] == "Exchange agreement rejects entries" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)
        self.assertTrue(any(x.get("action") == "skip symbols rejected by -4411" and x.get("status") == "applied" for x in review["autoActions"]))
        self.assertEqual(main.AUTO_TRADE["hermesAgents"]["agents"]["execution_agent"]["lastAction"], "skip symbols rejected by -4411")

    def test_supervisor_suppresses_two_self_healed_fapi_agreement_locks(self):
        state = main.new_agent_state()
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {"executionMode": "LIVE"}
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["lastSkip"] = {"code": "scan_none", "msg": "No qualified scan candidates"}
        main.AUTO_TRADE["log"] = [
            {"msg": "Skip: Binance Futures/Perps agreement required (-4411) · locked XAUUSDT"},
            {"msg": "Skip: Binance Futures/Perps agreement required (-4411) · locked CLUSDT"},
        ]
        main.AUTO_TRADE["consecutiveErrors"] = 0
        main.AUTO_TRADE["perfLocks"] = {
            "XAUUSDT": {"until": int(main.time.time()) + 3600, "reason": "fapi_agreement", "error": "-4411"},
            "CLUSDT": {"until": int(main.time.time()) + 3600, "reason": "fapi_agreement", "error": "-4411"},
        }

        review = main._hermes_supervisor_review(main.AUTO_TRADE)

        self.assertFalse(any(x["title"] == "Exchange agreement rejects entries" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)
        self.assertTrue(any(x.get("action") == "skip symbols rejected by -4411" and x.get("status") == "applied" for x in review["autoActions"]))

    def test_supervisor_auto_heals_live_scan_config_drift(self):
        state = main.new_agent_state()
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "symbol": "XAUUSDT",
            "primarySymbol": "BTCUSDT",
            "marketScan": False,
            "whitelistSymbols": ["XAUUSDT"],
            "orphanAutoAdoptForceSingleSymbol": False,
        }
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["log"] = []

        review = main._hermes_supervisor_review(main.AUTO_TRADE)

        self.assertEqual(main.AUTO_TRADE["config"]["symbol"], "AUTO")
        self.assertTrue(main.AUTO_TRADE["config"]["marketScan"])
        self.assertEqual(main.AUTO_TRADE["config"]["whitelistSymbols"], [])
        self.assertTrue(any(x["title"] == "AUTO scan config drift" for x in review["issues"]))
        self.assertTrue(any(x.get("action") == "auto-healed scan config drift" and x.get("status") == "applied" for x in review["autoActions"]))

    def test_supervisor_restores_fapi_lock_from_recent_logs(self):
        state = main.new_agent_state()
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {"executionMode": "LIVE", "symbol": "AUTO", "marketScan": True, "fapiAgreementSymbolLockMinutes": 45}
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["perfLocks"] = {}
        main.AUTO_TRADE["lastSkip"] = {
            "code": "fapi_agreement_required",
            "msg": "Skip: Binance Futures/Perps agreement required (-4411)",
        }
        main.AUTO_TRADE["log"] = [
            {"msg": "Skip: Binance Futures/Perps agreement required (-4411) · locked XAUUSDT"},
        ]

        review = main._hermes_supervisor_review(main.AUTO_TRADE)

        self.assertEqual(main.AUTO_TRADE["perfLocks"]["XAUUSDT"]["reason"], "fapi_agreement")
        self.assertTrue(any(x.get("action") == "restored -4411 perf locks from logs" and x.get("status") == "applied" for x in review["autoActions"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_escalates_repeated_locked_scan_pick(self):
        state = main.new_agent_state()
        bot = {
            "hermesAgents": state,
            "running": True,
            "config": {"executionMode": "LIVE", "symbol": "AUTO", "marketScan": True},
            "openLivePositions": [],
            "lastSkip": {"code": "fapi_agreement_required", "msg": "Skip: Binance Futures/Perps agreement required (-4411)"},
            "log": [
                {"msg": "SCAN pick: XAUUSDT"},
                {"msg": "Skip: Binance Futures/Perps agreement required (-4411) · locked XAUUSDT"},
                {"msg": "SCAN pick: XAUUSDT"},
                {"msg": "SCAN pick: XAUUSDT"},
            ],
            "consecutiveErrors": 0,
            "perfLocks": {
                "XAUUSDT": {
                    "until": int(main.time.time()) + 3600,
                    "reason": "fapi_agreement",
                    "error": "-4411",
                }
            },
        }

        review = main._hermes_supervisor_review(bot)

        self.assertTrue(any(x["title"] == "Repeated scan pick concentration" and x["severity"] == "high" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_reports_perf_locks_reducing_entries_when_no_qualified_candidates(self):
        state = main.new_agent_state()
        bot = {
            "hermesAgents": state,
            "running": True,
            "config": {"executionMode": "LIVE"},
            "openLivePositions": [],
            "scanBoard": [
                {"symbol": "BTCUSDT", "qualified": False, "rejectReason": "perf_lock(51m)"},
                {"symbol": "DOGEUSDT", "qualified": False, "rejectReason": "perf_lock(101m)"},
                {"symbol": "XRPUSDT", "qualified": False, "rejectReason": "perf_lock_payoff"},
                {"symbol": "XAUUSDT", "qualified": False, "rejectReason": "low_conf"},
            ],
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertTrue(any(x["title"] == "Performance locks reducing entries" for x in review["issues"]))
        self.assertFalse(any(x["title"] == "No qualified scan candidates" for x in review["issues"]))
        self.assertTrue(any(x["agent"] == "market_analyst" for x in review["autoActions"]))
        self.assertTrue(any(x["title"] == "Performance locks reducing entries" and x.get("supervisorFirst") for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_suppresses_no_qualified_scan_candidates_during_risk_cooldown_hold(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "risk_manager", "blocked", "armed risk cooldown", "45m")
        bot = {
            "hermesAgents": state,
            "running": True,
            "config": {"executionMode": "LIVE", "marketScan": True},
            "openLivePositions": [],
            "scanBoard": [
                {"symbol": "BTCUSDT", "qualified": False, "rejectReason": "low_conf"},
                {"symbol": "ETHUSDT", "qualified": False, "rejectReason": "signal_wait"},
                {"symbol": "SOLUSDT", "qualified": False, "rejectReason": "late_chase"},
            ],
            "lastSkip": {"code": "risk_cooldown_arm", "msg": "Skip: armed risk cooldown 45m (loss streak 3)"},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertTrue(any(x["title"] == "Safety hold" and x["agent"] == "risk_manager" for x in review["issues"]))
        self.assertFalse(any(x["title"] == "No qualified scan candidates" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_suppresses_no_qualified_scan_candidates_during_entry_risk_cooldown(self):
        state = main.new_agent_state()
        state["agents"]["risk_manager"]["runs"] = 209
        state["agents"]["risk_manager"]["state"] = "done"
        state["agents"]["risk_manager"]["lastAction"] = "monitor risk cooldown release"
        bot = {
            "hermesAgents": state,
            "running": True,
            "config": {"executionMode": "LIVE", "marketScan": True},
            "openLivePositions": [],
            "scanBoard": [
                {"symbol": "BTCUSDT", "qualified": False, "rejectReason": "low_conf"},
                {"symbol": "ETHUSDT", "qualified": False, "rejectReason": "signal_wait"},
                {"symbol": "SOLUSDT", "qualified": False, "rejectReason": "late_chase"},
            ],
            "lastSkip": {"code": "risk_cooldown", "msg": "Skip: risk cooldown 2700s"},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertTrue(any(x["title"] == "Entry blocked by risk cooldown" for x in review["issues"]))
        self.assertFalse(any(x["title"] == "No qualified scan candidates" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_ignores_perf_locks_when_qualified_candidates_exist(self):
        state = main.new_agent_state()
        bot = {
            "hermesAgents": state,
            "openLivePositions": [],
            "scanBoard": [
                {"symbol": "HYPEUSDT", "qualified": False, "rejectReason": "perf_lock(82m)"},
                {"symbol": "SOLUSDT", "qualified": False, "rejectReason": "perf_lock(82m)"},
                {"symbol": "ZECUSDT", "qualified": False, "rejectReason": "perf_lock(82m)"},
                {"symbol": "XRPUSDT", "qualified": True, "rejectReason": "", "scanExpanded": True},
                {"symbol": "XLMUSDT", "qualified": True, "rejectReason": "", "scanExpanded": True},
                {"symbol": "CLUSDT", "qualified": True, "rejectReason": "", "scanExpanded": True},
            ],
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertFalse(any(x["title"] == "Performance locks reducing entries" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_reports_low_live_entry_activity(self):
        state = main.new_agent_state()
        bot = {
            "running": True,
            "config": {"executionMode": "LIVE"},
            "tradesLastHour": 0,
            "hermesAgents": state,
            "openLivePositions": [],
            "lastSkip": {"code": "scan_none", "msg": "Skip: scan found no clear symbol"},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertTrue(any(x["title"] == "Low entry activity" for x in review["issues"]))
        self.assertTrue(any(x["action"] == "refresh scan and explain top entry blockers" for x in review["autoActions"]))

    def test_supervisor_reports_no_new_position_despite_capacity(self):
        prev = {
            "running": main.AUTO_TRADE.get("running"),
            "config": main.AUTO_TRADE.get("config"),
            "tradesLastHour": main.AUTO_TRADE.get("tradesLastHour"),
            "lastTradeAt": main.AUTO_TRADE.get("lastTradeAt"),
            "startedAt": main.AUTO_TRADE.get("startedAt"),
            "lastSkip": main.AUTO_TRADE.get("lastSkip"),
            "openLivePositions": main.AUTO_TRADE.get("openLivePositions"),
            "scanBoard": main.AUTO_TRADE.get("scanBoard"),
            "hermesAgents": main.AUTO_TRADE.get("hermesAgents"),
        }
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "symbol": "AUTO",
            "marketScan": True,
            "maxOpenPositions": 4,
            "supervisorNoNewPositionMinutes": 30,
        }
        main.AUTO_TRADE["tradesLastHour"] = 0
        main.AUTO_TRADE["lastTradeAt"] = int(main.time.time()) - 3600
        main.AUTO_TRADE["startedAt"] = int(main.time.time()) - 7200
        main.AUTO_TRADE["lastSkip"] = {"code": "scan_none", "msg": "Skip: scan found no clear symbol"}
        main.AUTO_TRADE["openLivePositions"] = [{"symbol": "BTCUSDT", "side": "LONG", "qty": 1.0}]
        main.AUTO_TRADE["scanBoard"] = [{"symbol": "ETHUSDT", "qualified": True, "rejectReason": ""}]
        state = main.new_agent_state()
        state = main.mark_agent(state, "position_guardian", "done", "open positions heartbeat", "BTCUSDT")
        main.AUTO_TRADE["hermesAgents"] = state

        try:
            with mock.patch.object(
                main,
                "_maybe_tune_low_entry_activity",
                return_value={"applied": True, "changes": {"minConfidence": {"old": 0.66, "new": 0.64}}},
            ):
                review = main._hermes_supervisor_review(main.AUTO_TRADE)
        finally:
            for key, value in prev.items():
                main.AUTO_TRADE[key] = value

        self.assertTrue(any(x["title"] == "No new position despite capacity" for x in review["issues"]))
        self.assertTrue(
            any(
                x.get("agent") == "portfolio_manager"
                and x.get("status") == "applied"
                and x.get("issueType") == "no_new_position_activity"
                for x in review["autoActions"]
            )
        )
        self.assertTrue(
            any(
                x.get("agent") == "strategy_builder"
                and x.get("action") == "auto-tuned no-new-position policy"
                and x.get("status") == "applied"
                for x in review["autoActions"]
            )
        )
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_does_not_report_no_new_position_when_capacity_full(self):
        state = main.new_agent_state()
        bot = {
            "running": True,
            "config": {
                "executionMode": "LIVE",
                "symbol": "AUTO",
                "marketScan": True,
                "maxOpenPositions": 2,
                "supervisorNoNewPositionMinutes": 30,
            },
            "tradesLastHour": 0,
            "lastTradeAt": int(main.time.time()) - 3600,
            "startedAt": int(main.time.time()) - 7200,
            "hermesAgents": state,
            "openLivePositions": [
                {"symbol": "BTCUSDT", "side": "LONG", "qty": 1.0},
                {"symbol": "ETHUSDT", "side": "SHORT", "qty": 1.0},
            ],
            "lastSkip": {"code": "max_open_positions", "msg": "Skip: open positions 2/2 reached"},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertFalse(any(x["title"] == "No new position despite capacity" for x in review["issues"]))

    def test_supervisor_delegates_low_entry_tune_to_strategy_builder(self):
        state = main.new_agent_state()
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "minConfidence": 0.62,
            "earlyEntryMinConfidence": 0.60,
            "scanAnalyzeTop": 8,
            "scanTopLiquid": 30,
            "scanFallbackNearEnabled": False,
            "scanPerfSoftFallbackEnabled": False,
        }
        main.AUTO_TRADE["tradesLastHour"] = 0
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["scanBoard"] = [
            {"symbol": "BTCUSDT", "qualified": False, "rejectReason": "low_conf"},
            {"symbol": "ETHUSDT", "qualified": False, "rejectReason": "perf_lock_payoff"},
        ]
        main.AUTO_TRADE["lastSkip"] = {"code": "scan_none", "msg": "Skip: scan found no clear symbol"}
        main.AUTO_TRADE["consecutiveErrors"] = 0
        main.AUTO_TRADE["supervisorAutoTune"] = {}

        with mock.patch.object(main, "_live_closed_trades_from_log", return_value=[]):
            with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                review = main._hermes_supervisor_review(main.AUTO_TRADE)

        cfg = main.AUTO_TRADE["config"]
        self.assertLess(cfg["minConfidence"], 0.62)
        self.assertLess(cfg["earlyEntryMinConfidence"], 0.60)
        self.assertGreater(cfg["scanAnalyzeTop"], 8)
        self.assertGreater(cfg["scanTopLiquid"], 30)
        self.assertTrue(cfg["scanFallbackNearEnabled"])
        self.assertTrue(cfg["scanPerfSoftFallbackEnabled"])
        self.assertTrue(any(x["action"] == "auto-tuned scan-none fallback policy" and x["status"] == "applied" for x in review["autoActions"]))
        self.assertEqual(main.AUTO_TRADE["hermesAgents"]["agents"]["strategy_builder"]["lastAction"], "auto-tuned scan-none fallback policy")

    def test_supervisor_bypasses_low_entry_cooldown_for_stuck_near_miss_scan(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        prev = {
            "running": main.AUTO_TRADE.get("running"),
            "config": main.AUTO_TRADE.get("config"),
            "tradesLastHour": main.AUTO_TRADE.get("tradesLastHour"),
            "lastSkip": main.AUTO_TRADE.get("lastSkip"),
            "openLivePositions": main.AUTO_TRADE.get("openLivePositions"),
            "scanBoard": main.AUTO_TRADE.get("scanBoard"),
            "hermesAgents": main.AUTO_TRADE.get("hermesAgents"),
            "supervisorAutoTune": main.AUTO_TRADE.get("supervisorAutoTune"),
        }
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "symbol": "AUTO",
            "marketScan": True,
            "maxOpenPositions": 5,
            "minConfidence": 0.88,
            "earlyEntryMinConfidence": 0.80,
            "earlyEntryScoreGapMin": 2.40,
            "maxSpreadBps": 10.0,
            "scanAnalyzeTop": 16,
            "scanTopLiquid": 75,
            "scanFallbackNearEnabled": False,
            "scanPerfSoftFallbackEnabled": False,
        }
        main.AUTO_TRADE["tradesLastHour"] = 0
        main.AUTO_TRADE["lastSkip"] = {"code": "scan_none", "msg": "Skip: scan found no clear symbol"}
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["scanBoard"] = [
            {
                "symbol": "HYPEUSDT",
                "qualified": False,
                "rejectReason": "low_conf",
                "confidence": 0.82,
                "adaptiveMinConf": 0.855,
                "spreadBps": 0.2,
            },
            {
                "symbol": "TAOUSDT",
                "qualified": False,
                "rejectReason": "low_conf",
                "confidence": 0.81,
                "adaptiveMinConf": 0.90,
                "spreadBps": 0.5,
            },
        ]
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["supervisorAutoTune"] = {
            "delegations": {
                "low_entry_activity": {"at": now - 60, "reason": "scan_none", "changes": {}}
            }
        }

        try:
            with mock.patch.object(main, "_live_closed_trades_from_log", return_value=[]):
                with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                    review = main._hermes_supervisor_review(main.AUTO_TRADE)
                    cfg_after = dict(main.AUTO_TRADE["config"])
        finally:
            for key, value in prev.items():
                main.AUTO_TRADE[key] = value

        cfg = cfg_after
        self.assertTrue(cfg["scanFallbackNearEnabled"])
        self.assertFalse(cfg["scanPerfSoftFallbackEnabled"])
        self.assertEqual(cfg["scanGuardedFallbackConfRelax"], 0.04)
        self.assertTrue(any(x.get("changes", {}).get("scanGuardedFallbackConfRelax") for x in review["autoActions"]))

    def test_supervisor_auto_tunes_scan_none_fallback_when_capacity_has_room(self):
        prev = {
            "running": main.AUTO_TRADE.get("running"),
            "config": main.AUTO_TRADE.get("config"),
            "tradesLastHour": main.AUTO_TRADE.get("tradesLastHour"),
            "lastTradeAt": main.AUTO_TRADE.get("lastTradeAt"),
            "startedAt": main.AUTO_TRADE.get("startedAt"),
            "lastSkip": main.AUTO_TRADE.get("lastSkip"),
            "openLivePositions": main.AUTO_TRADE.get("openLivePositions"),
            "scanBoard": main.AUTO_TRADE.get("scanBoard"),
            "hermesAgents": main.AUTO_TRADE.get("hermesAgents"),
            "supervisorAutoTune": main.AUTO_TRADE.get("supervisorAutoTune"),
        }
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "symbol": "AUTO",
            "marketScan": True,
            "maxOpenPositions": 5,
            "supervisorNoNewPositionMinutes": 30,
            "minConfidence": 0.62,
            "earlyEntryMinConfidence": 0.60,
            "earlyEntryScoreGapMin": 1.40,
            "hybridMinScore": 0.76,
            "hybridMinEdge": 0.06,
            "scanAnalyzeTop": 8,
            "scanTopLiquid": 30,
            "scanFallbackNearEnabled": False,
            "scanPerfSoftFallbackEnabled": False,
        }
        main.AUTO_TRADE["tradesLastHour"] = 0
        main.AUTO_TRADE["lastTradeAt"] = int(main.time.time()) - 2160
        main.AUTO_TRADE["startedAt"] = int(main.time.time()) - 7200
        main.AUTO_TRADE["lastSkip"] = {"code": "scan_none", "msg": "Skip: scan found no clear symbol"}
        main.AUTO_TRADE["openLivePositions"] = [
            {"symbol": "BTCUSDT", "side": "LONG", "qty": 1.0},
            {"symbol": "ETHUSDT", "side": "SHORT", "qty": 1.0},
            {"symbol": "SOLUSDT", "side": "LONG", "qty": 1.0},
        ]
        main.AUTO_TRADE["scanBoard"] = [
            {"symbol": f"SYM{i}USDT", "qualified": False, "rejectReason": "low_conf"}
            for i in range(10)
        ]
        main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
        main.AUTO_TRADE["supervisorAutoTune"] = {}

        try:
            with mock.patch.object(main, "_live_closed_trades_from_log", return_value=[]):
                with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                    review = main._hermes_supervisor_review(main.AUTO_TRADE)
        finally:
            for key, value in prev.items():
                main.AUTO_TRADE[key] = value

        self.assertTrue(any(x["title"] == "No qualified scan candidates" for x in review["issues"]))
        self.assertTrue(any(x["title"] == "No new position despite capacity" for x in review["issues"]))
        self.assertTrue(any(x["action"] == "auto-tuned scan-none fallback policy" and x["status"] == "applied" for x in review["autoActions"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_low_entry_tune_relaxes_quiet_market_and_targets_min_positions(self):
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        cfg = {
            "minConfidence": 0.62,
            "earlyEntryMinConfidence": 0.60,
            "earlyEntryScoreGapMin": 1.40,
            "hybridMinScore": 0.76,
            "hybridMinEdge": 0.06,
            "maxOpenPositions": 2,
            "scanAnalyzeTop": 8,
            "scanTopLiquid": 30,
            "scanFallbackNearEnabled": False,
            "scanPerfSoftFallbackEnabled": False,
        }
        board = [
            {"symbol": "BTCUSDT", "qualified": False, "rejectReason": "signal_wait", "momentumPct": 0.04},
            {"symbol": "ETHUSDT", "qualified": False, "rejectReason": "low_conf", "momentumPct": 0.12},
            {"symbol": "SOLUSDT", "qualified": False, "rejectReason": "signal_wait", "momentumPct": 0.08},
        ]
        main.AUTO_TRADE["supervisorAutoTune"] = {}

        try:
            with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                out = main._maybe_tune_low_entry_activity("quiet market", cfg, board)
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune

        self.assertTrue(out.get("applied"))
        self.assertTrue(out.get("quietMarket"))
        self.assertEqual(out.get("targetOpenPositions"), {"min": 3, "max": 6})
        self.assertEqual(cfg["maxOpenPositions"], 3)
        self.assertLessEqual(cfg["minConfidence"], 0.59)
        self.assertLessEqual(cfg["earlyEntryMinConfidence"], 0.575)
        self.assertLess(cfg["earlyEntryScoreGapMin"], 1.40)
        self.assertLess(cfg["hybridMinScore"], 0.76)
        self.assertLess(cfg["hybridMinEdge"], 0.06)

    def test_low_entry_tune_caps_target_positions_at_six(self):
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        cfg = {
            "minConfidence": 0.62,
            "earlyEntryMinConfidence": 0.60,
            "maxOpenPositions": 8,
            "supervisorTargetOpenPositionsMin": 3,
            "supervisorTargetOpenPositionsMax": 6,
            "scanAnalyzeTop": 8,
            "scanTopLiquid": 30,
        }
        main.AUTO_TRADE["supervisorAutoTune"] = {}

        try:
            with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                out = main._maybe_tune_low_entry_activity("over target capacity", cfg, [])
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune

        self.assertTrue(out.get("applied"))
        self.assertEqual(cfg["maxOpenPositions"], 6)
        self.assertEqual(out.get("targetOpenPositions"), {"min": 3, "max": 6})

    def test_supervisor_ignores_stale_scan_none_after_successful_scan(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        state["agents"]["market_analyst"]["state"] = "done"
        state["agents"]["market_analyst"]["lastAction"] = "scan completed"
        state["agents"]["market_analyst"]["lastReason"] = "NVDAUSDT"
        state["agents"]["market_analyst"]["updatedAt"] = now
        state["agents"]["strategy_builder"]["state"] = "done"
        state["agents"]["strategy_builder"]["lastAction"] = "entry approved"
        state["agents"]["strategy_builder"]["lastReason"] = "NVDAUSDT LONG c=0.800"
        state["agents"]["strategy_builder"]["updatedAt"] = now
        bot = {
            "running": True,
            "config": {"executionMode": "LIVE"},
            "tradesLastHour": 0,
            "hermesAgents": state,
            "openLivePositions": [],
            "scanBoard": [
                {"symbol": "NVDAUSDT", "qualified": True, "rejectReason": ""},
                {"symbol": "BNBUSDT", "qualified": False, "rejectReason": "low_conf"},
            ],
            "lastSkip": {"ts": now - 60, "code": "scan_none", "msg": "Skip: scan found no clear symbol"},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertFalse(any(x["title"] == "Low entry activity" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_reports_symbol_volatile_cooldown_as_root_entry_blocker(self):
        state = main.new_agent_state()
        bot = {
            "running": True,
            "config": {"executionMode": "LIVE"},
            "tradesLastHour": 0,
            "hermesAgents": state,
            "openLivePositions": [],
            "lastSkip": {"code": "symbol_volatile_cooldown", "msg": "Skip: XLMUSDT market volatile; symbol cooldown 15m"},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertFalse(any(x["title"] == "Low entry activity" for x in review["issues"]))
        self.assertTrue(any(x["title"] == "Entry blocked by symbol volatility cooldown" for x in review["issues"]))
        self.assertTrue(any(x["agent"] == "risk_manager" for x in review["autoActions"]))

    def test_supervisor_reports_bad_utc_hour_as_root_entry_blocker(self):
        state = main.new_agent_state()
        bot = {
            "running": True,
            "config": {"executionMode": "LIVE"},
            "tradesLastHour": 0,
            "hermesAgents": state,
            "openLivePositions": [],
            "lastSkip": {"code": "bad_utc_hour", "msg": "Skip: bad UTC hour 15"},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertFalse(any(x["title"] == "Low entry activity" for x in review["issues"]))
        self.assertTrue(any(x["title"] == "Entry blocked by bad UTC hour" for x in review["issues"]))
        self.assertTrue(any(x["agent"] == "risk_manager" for x in review["autoActions"]))

    def test_supervisor_delegates_bad_utc_unlock_to_risk_manager(self):
        state = main.new_agent_state()
        utc_hour = int(main.time.gmtime(main.time.time()).tm_hour)
        other_hour = (utc_hour + 1) % 24
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {"executionMode": "LIVE", "liveBadUtcHours": [utc_hour, other_hour]}
        main.AUTO_TRADE["tradesLastHour"] = 0
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["lastSkip"] = {"code": "bad_utc_hour", "msg": f"Skip: bad UTC hour {utc_hour:02d}"}
        main.AUTO_TRADE["consecutiveErrors"] = 0
        main.AUTO_TRADE["supervisorAutoTune"] = {}

        with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
            review = main._hermes_supervisor_review(main.AUTO_TRADE)

        self.assertNotIn(utc_hour, main.AUTO_TRADE["config"]["liveBadUtcHours"])
        self.assertIn(other_hour, main.AUTO_TRADE["config"]["liveBadUtcHours"])
        self.assertTrue(any(x["action"] == "auto-cleared bad UTC hour" and x["status"] == "applied" for x in review["autoActions"]))
        self.assertEqual(main.AUTO_TRADE["hermesAgents"]["agents"]["risk_manager"]["lastAction"], "auto-cleared bad UTC hour")

    def test_supervisor_does_not_report_low_activity_when_live_lock_position_exists(self):
        state = main.new_agent_state()
        bot = {
            "running": True,
            "config": {"executionMode": "LIVE"},
            "tradesLastHour": 0,
            "hermesAgents": state,
            "liveProfitLocks": {
                "BTCUSDT:SHORT": {"symbol": "BTCUSDT", "side": "SHORT", "qty": 0.01},
            },
            "lastSkip": {"code": "symbol_position_open", "msg": "Skip: BTCUSDT already has open position"},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertFalse(any(x["title"] == "Low entry activity" for x in review["issues"]))

    def test_supervisor_reports_early_profit_capture(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 1, "symbol": "BTCUSDT", "side": "LONG", "pnl": 0.08, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 2, "symbol": "ETHUSDT", "side": "SHORT", "pnl": 0.12, "reason": "LIVE_CLOSE"},
            {"closedAt": now - 3, "symbol": "SOLUSDT", "side": "LONG", "pnl": 0.05, "reason": "WEAK_SIGNAL"},
        ]
        bot = {
            "hermesAgents": state,
            "openLivePositions": [],
            "liveStatsAll": {"lastTrades": trades},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertTrue(any(x["title"] == "Profit capture may be too early" for x in review["issues"]))
        self.assertTrue(any(x["agent"] == "strategy_builder" for x in review["autoActions"]))
        self.assertTrue(any(x["title"] == "Profit capture may be too early" and x.get("supervisorFirst") for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_auto_tunes_scan_timeout_when_live(self):
        state = main.new_agent_state()
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "scanPerSymbolTimeoutSec": 7.5,
            "scanAnalyzeTop": 8,
            "scanGuardedFallbackAnalyzeTop": 16,
            "scanFallbackRetrySymbols": 3,
        }
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["tradesLastHour"] = 1
        main.AUTO_TRADE["lastSkip"] = {"code": "network_timeout", "msg": "Skip: network timeout (Binance API slow) — will retry"}
        main.AUTO_TRADE["consecutiveErrors"] = 0
        main.AUTO_TRADE["supervisorAutoTune"] = {}

        try:
            with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                review = main._hermes_supervisor_review(main.AUTO_TRADE)
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune

        self.assertTrue(any(x["title"] == "Scan timeout detected" for x in review["issues"]))
        self.assertTrue(any(x.get("action") == "auto-tuned scan timeout workload" and x.get("status") == "applied" for x in review["autoActions"]))
        self.assertNotIn("cmuxHandoff", review)
        self.assertLess(main.AUTO_TRADE["config"]["scanAnalyzeTop"], 8)
        self.assertGreater(main.AUTO_TRADE["config"]["scanPerSymbolTimeoutSec"], 7.5)

    def test_supervisor_does_not_report_risk_cooldown_timeout_as_scan_timeout(self):
        state = main.new_agent_state()
        bot = {
            "running": True,
            "config": {"executionMode": "LIVE"},
            "tradesLastHour": 1,
            "hermesAgents": state,
            "openLivePositions": [],
            "lastSkip": {
                "code": "risk_cooldown",
                "msg": "Skip: risk cooldown 2677s · adaptive check timeout; retrying",
            },
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertFalse(any(x["title"] == "Scan timeout detected" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_periodic_trade_review_keeps_negative_expectancy_supervisor_first(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 8, "openedAt": now - 40, "symbol": "BAD1USDT", "side": "LONG", "pnl": -0.22, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 7, "openedAt": now - 180, "symbol": "BAD2USDT", "side": "LONG", "pnl": -0.18, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 6, "openedAt": now - 120, "symbol": "BAD3USDT", "side": "SHORT", "pnl": -0.20, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 5, "openedAt": now - 70, "symbol": "OKUSDT", "side": "LONG", "pnl": 0.04, "reason": "LIVE_CLOSE"},
            {"closedAt": now - 4, "openedAt": now - 60, "symbol": "OKUSDT", "side": "SHORT", "pnl": -0.16, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 3, "openedAt": now - 50, "symbol": "BAD4USDT", "side": "SHORT", "pnl": -0.24, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 2, "openedAt": now - 45, "symbol": "OKUSDT", "side": "LONG", "pnl": 0.05, "reason": "LIVE_CLOSE"},
            {"closedAt": now - 1, "openedAt": now - 30, "symbol": "BAD5USDT", "side": "LONG", "pnl": -0.19, "reason": "LOCAL_SL_HIT"},
        ]
        bot = {
            "running": True,
            "config": {"executionMode": "LIVE"},
            "tradesLastHour": 8,
            "hermesAgents": state,
            "openLivePositions": [],
            "liveStatsAll": {"lastTrades": trades},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertTrue(review["periodicTradeReview"])
        self.assertTrue(any(x["title"] == "Periodic trade review: negative expectancy" for x in review["issues"]))
        self.assertTrue(any(x["title"] == "Periodic trade review: negative expectancy" and x.get("supervisorFirst") for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_auto_tunes_negative_expectancy_when_live(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 80, "openedAt": now - 3680, "symbol": "AUSDT", "side": "LONG", "pnl": -0.38, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 70, "openedAt": now - 3670, "symbol": "BUSDT", "side": "LONG", "pnl": 0.24, "reason": "LIVE_CLOSE"},
            {"closedAt": now - 60, "openedAt": now - 3660, "symbol": "CUSDT", "side": "SHORT", "pnl": -0.39, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 50, "openedAt": now - 3650, "symbol": "DUSDT", "side": "LONG", "pnl": -0.36, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 40, "openedAt": now - 3640, "symbol": "EUSDT", "side": "SHORT", "pnl": 0.25, "reason": "LIVE_CLOSE"},
            {"closedAt": now - 30, "openedAt": now - 3630, "symbol": "FUSDT", "side": "LONG", "pnl": -0.37, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 20, "openedAt": now - 3620, "symbol": "GUSDT", "side": "SHORT", "pnl": 0.22, "reason": "LIVE_CLOSE"},
            {"closedAt": now - 10, "openedAt": now - 3610, "symbol": "HUSDT", "side": "LONG", "pnl": -0.35, "reason": "LOCAL_SL_HIT"},
        ]
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "minConfidence": 0.62,
            "earlyEntryMinConfidence": 0.60,
            "earlyEntryScoreGapMin": 1.40,
            "hybridMinScore": 0.72,
            "hybridMinEdge": 0.06,
            "maxSpreadBps": 22.0,
            "scanFallbackNearEnabled": True,
            "scanPerfSoftFallbackEnabled": True,
            "holdMinConfidence": 0.78,
            "tpTargetMinUsdt": 0.55,
            "tpTargetMaxUsdt": 2.20,
        }
        main.AUTO_TRADE["tradesLastHour"] = 8
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["liveStatsAll"] = {"lastTrades": trades}
        main.AUTO_TRADE["supervisorAutoTune"] = {}

        with mock.patch.object(main, "_live_closed_trades_from_log", return_value=trades):
            with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                review = main._hermes_supervisor_review(main.AUTO_TRADE)

        cfg = main.AUTO_TRADE["config"]
        self.assertTrue(any(x["title"] == "Periodic trade review: negative expectancy" for x in review["issues"]))
        self.assertTrue(any(x.get("action") == "auto-tuned negative expectancy policy" and x.get("status") == "applied" for x in review["autoActions"]))
        self.assertTrue(any(x.get("action") == "auto-tuned weak payoff policy" and x.get("status") == "applied" for x in review["autoActions"]))
        self.assertNotIn("cmuxHandoff", review)
        self.assertGreater(cfg["minConfidence"], 0.62)
        self.assertGreater(cfg["earlyEntryScoreGapMin"], 1.40)
        self.assertGreater(cfg["hybridMinScore"], 0.72)
        self.assertLess(cfg["maxSpreadBps"], 22.0)
        self.assertFalse(cfg["scanFallbackNearEnabled"])
        self.assertFalse(cfg["scanPerfSoftFallbackEnabled"])
        self.assertEqual(main.AUTO_TRADE["hermesAgents"]["agents"]["strategy_builder"]["lastAction"], "auto-tuned negative expectancy policy")

    def test_supervisor_separates_infra_auth_from_strategy_tuning(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 80, "openedAt": now - 3680, "symbol": "AUSDT", "side": "LONG", "pnl": -0.38, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 70, "openedAt": now - 3670, "symbol": "BUSDT", "side": "LONG", "pnl": 0.24, "reason": "LIVE_CLOSE"},
            {"closedAt": now - 60, "openedAt": now - 3660, "symbol": "CUSDT", "side": "SHORT", "pnl": -0.39, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 50, "openedAt": now - 3650, "symbol": "DUSDT", "side": "LONG", "pnl": -0.36, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 40, "openedAt": now - 3640, "symbol": "EUSDT", "side": "SHORT", "pnl": 0.25, "reason": "LIVE_CLOSE"},
            {"closedAt": now - 30, "openedAt": now - 3630, "symbol": "FUSDT", "side": "LONG", "pnl": -0.37, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 20, "openedAt": now - 3620, "symbol": "GUSDT", "side": "SHORT", "pnl": 0.22, "reason": "LIVE_CLOSE"},
            {"closedAt": now - 10, "openedAt": now - 3610, "symbol": "HUSDT", "side": "LONG", "pnl": -0.35, "reason": "LOCAL_SL_HIT"},
        ]
        prev_auto = dict(main.AUTO_TRADE)
        try:
            main.AUTO_TRADE["running"] = True
            main.AUTO_TRADE["config"] = {
                "executionMode": "LIVE",
                "minConfidence": 0.62,
                "earlyEntryMinConfidence": 0.60,
                "earlyEntryScoreGapMin": 1.40,
                "hybridMinScore": 0.72,
                "hybridMinEdge": 0.06,
                "maxSpreadBps": 22.0,
                "scanFallbackNearEnabled": True,
                "scanPerfSoftFallbackEnabled": True,
                "supervisorSizeStreakEnabled": True,
                "supervisorSizeMultiplier": 1.0,
            }
            main.AUTO_TRADE["tradesLastHour"] = 8
            main.AUTO_TRADE["hermesAgents"] = state
            main.AUTO_TRADE["openLivePositions"] = []
            main.AUTO_TRADE["liveStatsAll"] = {"lastTrades": trades}
            main.AUTO_TRADE["lastSkip"] = {
                "code": "binance_permission_required",
                "msg": "LIVE paused: Binance API key/IP/permission rejected (-2015)",
            }
            main.AUTO_TRADE["log"] = [
                {"ts": now, "msg": "LIVE paused: Binance API key/IP/permission rejected (-2015)"}
            ]
            main.AUTO_TRADE["supervisorAutoTune"] = {}

            with mock.patch.object(main, "_live_closed_trades_from_log", return_value=trades):
                with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                    review = main._hermes_supervisor_review(main.AUTO_TRADE)

            cfg = main.AUTO_TRADE["config"]
            self.assertEqual(review["tradeReviewAttribution"]["primary"], "infra_auth")
            self.assertTrue(review["tradeReviewAttribution"]["strategyTuningSuppressed"])
            self.assertTrue(any(x["title"] == "Binance API/IP permission incident" for x in review["issues"]))
            self.assertTrue(any(x["title"] == "Trade review separated: infra" for x in review["issues"]))
            self.assertFalse(any(x.get("action") == "auto-tuned negative expectancy policy" for x in review["autoActions"]))
            self.assertFalse(any(x.get("action") == "auto-tuned weak payoff policy" for x in review["autoActions"]))
            self.assertFalse(any(x.get("agent") in {"strategy_builder", "position_guardian"} and str(x.get("issueType", "")).startswith(("negative", "weak")) for x in review["autoActions"]))
            self.assertEqual(cfg["minConfidence"], 0.62)
            self.assertEqual(cfg["supervisorSizeMultiplier"], 1.0)
        finally:
            main.AUTO_TRADE.clear()
            main.AUTO_TRADE.update(prev_auto)

    def test_supervisor_separates_data_timeout_from_strategy_tuning(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 80, "openedAt": now - 3680, "symbol": "AUSDT", "side": "LONG", "pnl": -0.38, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 70, "openedAt": now - 3670, "symbol": "BUSDT", "side": "LONG", "pnl": 0.24, "reason": "LIVE_CLOSE"},
            {"closedAt": now - 60, "openedAt": now - 3660, "symbol": "CUSDT", "side": "SHORT", "pnl": -0.39, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 50, "openedAt": now - 3650, "symbol": "DUSDT", "side": "LONG", "pnl": -0.36, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 40, "openedAt": now - 3640, "symbol": "EUSDT", "side": "SHORT", "pnl": 0.25, "reason": "LIVE_CLOSE"},
            {"closedAt": now - 30, "openedAt": now - 3630, "symbol": "FUSDT", "side": "LONG", "pnl": -0.37, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 20, "openedAt": now - 3620, "symbol": "GUSDT", "side": "SHORT", "pnl": 0.22, "reason": "LIVE_CLOSE"},
            {"closedAt": now - 10, "openedAt": now - 3610, "symbol": "HUSDT", "side": "LONG", "pnl": -0.35, "reason": "LOCAL_SL_HIT"},
        ]
        prev_auto = dict(main.AUTO_TRADE)
        try:
            main.AUTO_TRADE["running"] = True
            main.AUTO_TRADE["config"] = {
                "executionMode": "LIVE",
                "minConfidence": 0.62,
                "earlyEntryScoreGapMin": 1.40,
                "hybridMinScore": 0.72,
                "maxSpreadBps": 22.0,
                "supervisorSizeStreakEnabled": True,
                "supervisorSizeMultiplier": 1.0,
            }
            main.AUTO_TRADE["tradesLastHour"] = 8
            main.AUTO_TRADE["hermesAgents"] = state
            main.AUTO_TRADE["openLivePositions"] = []
            main.AUTO_TRADE["liveStatsAll"] = {"lastTrades": trades}
            main.AUTO_TRADE["lastDataProviderError"] = {
                "ts": now,
                "streak": 4,
                "cooldownUntil": now + 20,
                "error": "data provider cooldown active",
                "path": "/fapi/v1/klines",
            }
            main.AUTO_TRADE["supervisorAutoTune"] = {}

            with mock.patch.object(main, "_live_closed_trades_from_log", return_value=trades):
                with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                    review = main._hermes_supervisor_review(main.AUTO_TRADE)

            cfg = main.AUTO_TRADE["config"]
            self.assertEqual(review["tradeReviewAttribution"]["primary"], "infra_data")
            self.assertTrue(review["tradeReviewAttribution"]["strategyTuningSuppressed"])
            self.assertTrue(any(x["title"] == "Market data provider timeout" for x in review["issues"]))
            self.assertFalse(any(x.get("action") == "auto-tuned negative expectancy policy" for x in review["autoActions"]))
            self.assertFalse(any(x.get("action") == "auto-tuned weak payoff policy" for x in review["autoActions"]))
            self.assertEqual(cfg["minConfidence"], 0.62)
            self.assertEqual(cfg["supervisorSizeMultiplier"], 1.0)
        finally:
            main.AUTO_TRADE.clear()
            main.AUTO_TRADE.update(prev_auto)

    def test_supervisor_does_not_self_block_on_subagent_high_issue(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        state["agents"]["hermes_supervisor"]["state"] = "blocked"
        state["agents"]["hermes_supervisor"]["lastAction"] = "high-severity agent issue"
        state["agents"]["hermes_supervisor"]["lastReason"] = "previous review"
        state["agents"]["market_analyst"]["state"] = "blocked"
        state["agents"]["market_analyst"]["lastAction"] = "data provider timeout"
        state["agents"]["market_analyst"]["lastReason"] = "timeout"
        state["agents"]["market_analyst"]["updatedAt"] = now
        prev_auto = dict(main.AUTO_TRADE)
        try:
            main.AUTO_TRADE["running"] = True
            main.AUTO_TRADE["config"] = {"executionMode": "LIVE", "marketScan": True, "symbol": "AUTO"}
            main.AUTO_TRADE["hermesAgents"] = state
            main.AUTO_TRADE["lastDataProviderError"] = {
                "ts": now,
                "streak": 4,
                "cooldownUntil": now + 20,
                "error": "data provider cooldown active",
                "path": "/fapi/v1/klines",
            }

            review = main._hermes_supervisor_review(main.AUTO_TRADE)

            supervisor = main.AUTO_TRADE["hermesAgents"]["agents"]["hermes_supervisor"]
            self.assertEqual(supervisor["state"], "done")
            self.assertEqual(supervisor["lastAction"], "reviewed high-severity subagent issue")
            self.assertFalse(any(x.get("agent") == "hermes_supervisor" for x in review["issues"]))
            self.assertNotIn("cmuxHandoff", review)
        finally:
            main.AUTO_TRADE.clear()
            main.AUTO_TRADE.update(prev_auto)

    def test_loss_streak_self_review_does_not_tune_on_infra_auth(self):
        cfg = {
            "minConfidence": 0.62,
            "scanFallbackNearEnabled": True,
            "scanSidePreference": "confidence",
            "maxOpenPositions": 5,
        }
        prev_review = main.AUTO_TRADE.get("lastSelfReview")
        try:
            with mock.patch.object(main, "write_self_review_memory"):
                out = main._loss_streak_self_review_tune(
                    cfg,
                    int(main.time.time()),
                    4,
                    {
                        "category": "infra_auth",
                        "title": "Binance API/IP permission incident",
                        "detail": "Invalid API-key, IP, or permissions for action (-2015)",
                        "operatorAction": "fix whitelist IP",
                    },
                )

            self.assertEqual(out, cfg)
            self.assertEqual(main.AUTO_TRADE["lastSelfReview"]["causeCategory"], "infra_auth")
            self.assertIn("operator_action_required", main.AUTO_TRADE["lastSelfReview"]["actions"][0])
        finally:
            if prev_review is None:
                main.AUTO_TRADE.pop("lastSelfReview", None)
            else:
                main.AUTO_TRADE["lastSelfReview"] = prev_review

    def test_supervisor_raises_size_multiplier_after_win_streak(self):
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        prev_config = main.AUTO_TRADE.get("config")
        cfg = {
            "supervisorSizeStreakEnabled": True,
            "supervisorSizeMultiplier": 1.0,
            "supervisorSizeWinStreakMin": 3,
            "supervisorSizeWinStepPct": 10.0,
            "supervisorSizeMaxMultiplier": 1.35,
        }
        trades = [
            {"symbol": "AUSDT", "side": "LONG", "closedAt": 10, "pnl": -0.2},
            {"symbol": "BUSDT", "side": "LONG", "closedAt": 11, "pnl": 0.3},
            {"symbol": "CUSDT", "side": "SHORT", "closedAt": 12, "pnl": 0.4},
            {"symbol": "DUSDT", "side": "LONG", "closedAt": 13, "pnl": 0.5},
        ]
        main.AUTO_TRADE["supervisorAutoTune"] = {}
        try:
            with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                out = main._maybe_tune_size_multiplier_from_streak(trades, cfg)
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune
            main.AUTO_TRADE["config"] = prev_config

        self.assertTrue(out.get("applied"))
        self.assertEqual(cfg["supervisorSizeMultiplier"], 1.1)
        self.assertEqual(out["changes"]["supervisorSizeMultiplier"]["reason"], "win_streak")

    def test_supervisor_reduces_size_multiplier_after_loss_streak(self):
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        prev_config = main.AUTO_TRADE.get("config")
        cfg = {
            "supervisorSizeStreakEnabled": True,
            "supervisorSizeMultiplier": 1.0,
            "supervisorSizeLossStreakMin": 2,
            "supervisorSizeLossStepPct": 15.0,
            "supervisorSizeMinMultiplier": 0.50,
        }
        trades = [
            {"symbol": "AUSDT", "side": "LONG", "closedAt": 10, "pnl": 0.2},
            {"symbol": "BUSDT", "side": "LONG", "closedAt": 11, "pnl": -0.3},
            {"symbol": "CUSDT", "side": "SHORT", "closedAt": 12, "pnl": -0.4},
            {"symbol": "DUSDT", "side": "LONG", "closedAt": 13, "pnl": -0.5},
        ]
        main.AUTO_TRADE["supervisorAutoTune"] = {}
        try:
            with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                out = main._maybe_tune_size_multiplier_from_streak(trades, cfg)
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune
            main.AUTO_TRADE["config"] = prev_config

        self.assertTrue(out.get("applied"))
        self.assertEqual(cfg["supervisorSizeMultiplier"], 0.7)
        self.assertEqual(out["changes"]["supervisorSizeMultiplier"]["reason"], "loss_streak")

    def test_supervisor_size_floor_for_auto_scan_diversification(self):
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        prev_config = main.AUTO_TRADE.get("config")
        cfg = {
            "symbol": "AUTO",
            "marketScan": True,
            "supervisorSizeStreakEnabled": True,
            "supervisorSizeMultiplier": 1.0,
            "supervisorSizeLossStreakMin": 2,
            "supervisorSizeLossStepPct": 20.0,
            "supervisorSizeMinMultiplier": 0.50,
            "supervisorSizeDiversifiedMinMultiplier": 0.65,
        }
        trades = [
            {"symbol": "AUSDT", "side": "LONG", "closedAt": 10, "pnl": -0.2},
            {"symbol": "BUSDT", "side": "LONG", "closedAt": 11, "pnl": -0.3},
            {"symbol": "CUSDT", "side": "SHORT", "closedAt": 12, "pnl": -0.4},
            {"symbol": "DUSDT", "side": "LONG", "closedAt": 13, "pnl": -0.5},
        ]
        main.AUTO_TRADE["supervisorAutoTune"] = {}
        try:
            with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                out = main._maybe_tune_size_multiplier_from_streak(trades, cfg)
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune
            main.AUTO_TRADE["config"] = prev_config

        self.assertTrue(out.get("applied"))
        self.assertEqual(cfg["supervisorSizeMultiplier"], 0.65)

    def test_cached_supervisor_review_avoids_compute_for_status(self):
        prev_review = main.AUTO_TRADE.get("hermesSupervisorReview")
        now = int(main.time.time())
        cached = {"reviewedAt": now - 120, "severity": "medium", "summary": "cached"}
        main.AUTO_TRADE["hermesSupervisorReview"] = cached
        try:
            with mock.patch.object(main, "_hermes_supervisor_review") as heavy:
                out = main._cached_hermes_supervisor_review(main.AUTO_TRADE, max_age_sec=1, allow_compute=False)
        finally:
            main.AUTO_TRADE["hermesSupervisorReview"] = prev_review if isinstance(prev_review, dict) else {}

        heavy.assert_not_called()
        self.assertTrue(out.get("cached"))
        self.assertEqual(out["summary"], "cached")

    def test_risk_cooldown_watchlist_refreshes_scan_without_order(self):
        prev_board = main.AUTO_TRADE.get("scanBoard")
        prev_watch = main.AUTO_TRADE.get("cooldownWatchlist")
        prev_last = main.AUTO_TRADE.get("riskCooldownLastLightScanAt")
        board = [{"symbol": "SOLUSDT", "signal": "LONG", "qualified": True}]

        async def fake_pick(_cfg, _exclude):
            return "SOLUSDT", {"symbol": "SOLUSDT", "signal": "LONG", "confidence": 0.82}, board

        cfg = {
            "symbol": "AUTO",
            "marketScan": True,
            "riskCooldownLightScanEnabled": True,
            "riskCooldownLightScanSec": 20,
            "riskCooldownLightScanAnalyzeTop": 4,
            "scanTopLiquid": 100,
            "scanAnalyzeTop": 18,
            "scanGuardedFallbackAnalyzeTop": 18,
            "scanPerSymbolTimeoutSec": 3,
            "intervalSec": 20,
        }
        watch_after = {}
        board_after = []
        try:
            main.AUTO_TRADE["riskCooldownLastLightScanAt"] = 0
            with mock.patch.object(main, "_pick_best_symbol_from_scan", side_effect=fake_pick), mock.patch.object(main, "_persist_autotrade_snapshot"):
                refreshed = main.asyncio.run(main._refresh_risk_cooldown_watchlist(cfg, set(), 1000, "test cooldown"))
            watch_after = dict(main.AUTO_TRADE.get("cooldownWatchlist") or {})
            board_after = list(main.AUTO_TRADE.get("scanBoard") or [])
        finally:
            main.AUTO_TRADE["scanBoard"] = prev_board if isinstance(prev_board, list) else []
            main.AUTO_TRADE["cooldownWatchlist"] = prev_watch if isinstance(prev_watch, dict) else {}
            main.AUTO_TRADE["riskCooldownLastLightScanAt"] = prev_last or 0

        self.assertTrue(refreshed)
        self.assertEqual(watch_after["picked"], "SOLUSDT")
        self.assertEqual(board_after, board)

    def test_supervisor_resets_size_multiplier_when_streak_clears(self):
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        prev_config = main.AUTO_TRADE.get("config")
        cfg = {
            "supervisorSizeStreakEnabled": True,
            "supervisorSizeMultiplier": 0.7,
            "supervisorSizeWinStreakMin": 3,
            "supervisorSizeLossStreakMin": 2,
        }
        trades = [
            {"symbol": "AUSDT", "side": "LONG", "closedAt": 10, "pnl": -0.2},
            {"symbol": "BUSDT", "side": "LONG", "closedAt": 11, "pnl": 0.3},
        ]
        main.AUTO_TRADE["supervisorAutoTune"] = {}
        try:
            with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                out = main._maybe_tune_size_multiplier_from_streak(trades, cfg)
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune
            main.AUTO_TRADE["config"] = prev_config

        self.assertTrue(out.get("applied"))
        self.assertEqual(cfg["supervisorSizeMultiplier"], 1.0)
        self.assertEqual(out["changes"]["supervisorSizeMultiplier"]["reason"], "streak_reset")

    def test_supervisor_locks_dominant_symbol_drag_instead_of_broad_tuning(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 8, "openedAt": now - 900, "symbol": "ZECUSDT", "side": "LONG", "pnl": -0.28, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 7, "openedAt": now - 850, "symbol": "ZECUSDT", "side": "LONG", "pnl": -0.22, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 6, "openedAt": now - 800, "symbol": "ZECUSDT", "side": "SHORT", "pnl": -0.1726, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 5, "openedAt": now - 760, "symbol": "AUSDT", "side": "LONG", "pnl": 0.10, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 4, "openedAt": now - 720, "symbol": "BUSDT", "side": "SHORT", "pnl": 0.09, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 3, "openedAt": now - 680, "symbol": "CUSDT", "side": "LONG", "pnl": 0.08, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 2, "openedAt": now - 640, "symbol": "DUSDT", "side": "SHORT", "pnl": 0.04, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 1, "openedAt": now - 600, "symbol": "EUSDT", "side": "LONG", "pnl": 0.0347, "reason": "LIVE_CUT_LOSING_SIDE"},
        ]
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {"executionMode": "LIVE", "perfLockMinutes": 120}
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["liveStatsAll"] = {"lastTrades": trades}
        main.AUTO_TRADE["perfLocks"] = {}

        with mock.patch.object(main, "_live_closed_trades_from_log", return_value=trades):
            review = main._hermes_supervisor_review(main.AUTO_TRADE)

        self.assertTrue(any(x["title"] == "Periodic trade review: symbol drag" for x in review["issues"]))
        self.assertFalse(any(x["title"] == "Periodic trade review: negative expectancy" for x in review["issues"]))
        self.assertFalse(any(x["title"] == "Periodic trade review: weak payoff ratio" for x in review["issues"]))
        self.assertEqual(main.AUTO_TRADE["perfLocks"]["ZECUSDT"]["reason"], "symbol_drag")
        self.assertTrue(any(x.get("action") == "temporary perf-lock dominant symbol drag" for x in review["autoActions"]))

    def test_supervisor_treats_extreme_two_trade_symbol_drag_as_root_cause(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 8, "openedAt": now - 900, "symbol": "ZECUSDT", "side": "LONG", "pnl": -0.47, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 7, "openedAt": now - 850, "symbol": "ZECUSDT", "side": "LONG", "pnl": -0.465275, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 6, "openedAt": now - 800, "symbol": "TAOUSDT", "side": "SHORT", "pnl": -0.560337, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 5, "openedAt": now - 760, "symbol": "AUSDT", "side": "LONG", "pnl": 0.32, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 4, "openedAt": now - 720, "symbol": "BUSDT", "side": "SHORT", "pnl": 0.28, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 3, "openedAt": now - 680, "symbol": "CUSDT", "side": "LONG", "pnl": 0.24, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 2, "openedAt": now - 640, "symbol": "DUSDT", "side": "SHORT", "pnl": 0.20, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 1, "openedAt": now - 600, "symbol": "EUSDT", "side": "LONG", "pnl": 0.174283, "reason": "LIVE_CUT_LOSING_SIDE"},
        ]
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {"executionMode": "LIVE", "perfLockMinutes": 120}
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["liveStatsAll"] = {"lastTrades": trades}
        main.AUTO_TRADE["perfLocks"] = {}

        with mock.patch.object(main, "_live_closed_trades_from_log", return_value=trades):
            review = main._hermes_supervisor_review(main.AUTO_TRADE)

        self.assertTrue(any(x["title"] == "Periodic trade review: symbol drag" for x in review["issues"]))
        self.assertFalse(any(x["title"] == "Periodic trade review: weak payoff ratio" for x in review["issues"]))
        self.assertEqual(main.AUTO_TRADE["perfLocks"]["ZECUSDT"]["reason"], "symbol_drag")

    def test_supervisor_does_not_handoff_already_locked_symbol_drag(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 8, "openedAt": now - 900, "symbol": "FETUSDT", "side": "LONG", "pnl": -0.51, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 7, "openedAt": now - 850, "symbol": "FETUSDT", "side": "LONG", "pnl": -0.5048, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 6, "openedAt": now - 800, "symbol": "AUSDT", "side": "LONG", "pnl": 0.22, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 5, "openedAt": now - 760, "symbol": "BUSDT", "side": "SHORT", "pnl": 0.18, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 4, "openedAt": now - 720, "symbol": "CUSDT", "side": "LONG", "pnl": 0.14, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 3, "openedAt": now - 680, "symbol": "DUSDT", "side": "SHORT", "pnl": 0.12, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 2, "openedAt": now - 640, "symbol": "EUSDT", "side": "LONG", "pnl": 0.10, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 1, "openedAt": now - 600, "symbol": "GUSDT", "side": "SHORT", "pnl": 0.0648, "reason": "LIVE_CUT_LOSING_SIDE"},
        ]
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {"executionMode": "LIVE", "perfLockMinutes": 120}
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["liveStatsAll"] = {"lastTrades": trades}
        main.AUTO_TRADE["perfLocks"] = {
            "FETUSDT": {"until": now + 3600, "at": now - 60, "reason": "symbol_drag"}
        }

        with mock.patch.object(main, "_live_closed_trades_from_log", return_value=trades):
            review = main._hermes_supervisor_review(main.AUTO_TRADE)

        self.assertTrue(any("already locked" in x.get("detail", "") for x in review["issues"]))
        self.assertTrue(any(x.get("action") == "temporary perf-lock dominant symbol drag" and x.get("status") == "applied" for x in review["autoActions"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_ignores_profitable_small_win_cluster(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 8, "openedAt": now - 900, "symbol": "AUSDT", "side": "LONG", "pnl": 0.35, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 7, "openedAt": now - 850, "symbol": "BUSDT", "side": "LONG", "pnl": 0.31, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 6, "openedAt": now - 800, "symbol": "CUSDT", "side": "SHORT", "pnl": 0.28, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 5, "openedAt": now - 760, "symbol": "DUSDT", "side": "LONG", "pnl": 0.18, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 4, "openedAt": now - 720, "symbol": "EUSDT", "side": "SHORT", "pnl": 0.16, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 3, "openedAt": now - 680, "symbol": "FUSDT", "side": "LONG", "pnl": 0.12, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 2, "openedAt": now - 640, "symbol": "GUSDT", "side": "SHORT", "pnl": -0.12, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 1, "openedAt": now - 600, "symbol": "HUSDT", "side": "LONG", "pnl": -0.10, "reason": "LIVE_CUT_LOSING_SIDE"},
        ]
        bot = {
            "running": True,
            "config": {"executionMode": "LIVE"},
            "tradesLastHour": 8,
            "hermesAgents": state,
            "openLivePositions": [],
            "liveStatsAll": {"lastTrades": trades},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertTrue(review["periodicTradeReview"])
        self.assertFalse(any(x["title"] == "Periodic trade review: small wins dominate" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_ignores_healthy_low_payoff_window(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 8, "openedAt": now - 900, "symbol": "AUSDT", "side": "LONG", "pnl": 0.42, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 7, "openedAt": now - 850, "symbol": "BUSDT", "side": "LONG", "pnl": 0.24, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 6, "openedAt": now - 800, "symbol": "CUSDT", "side": "SHORT", "pnl": 0.22, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 5, "openedAt": now - 760, "symbol": "DUSDT", "side": "LONG", "pnl": 0.18, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 4, "openedAt": now - 720, "symbol": "EUSDT", "side": "SHORT", "pnl": 0.16, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 3, "openedAt": now - 680, "symbol": "FUSDT", "side": "LONG", "pnl": 0.14, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 2, "openedAt": now - 640, "symbol": "GUSDT", "side": "SHORT", "pnl": 0.11, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 1, "openedAt": now - 600, "symbol": "HUSDT", "side": "LONG", "pnl": -0.33, "reason": "LIVE_CUT_LOSING_SIDE"},
        ]
        bot = {
            "running": True,
            "config": {"executionMode": "LIVE"},
            "tradesLastHour": 8,
            "hermesAgents": state,
            "openLivePositions": [],
            "liveStatsAll": {"lastTrades": trades},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertTrue(review["periodicTradeReview"])
        self.assertFalse(any(x["title"] == "Periodic trade review: weak payoff ratio" for x in review["issues"]))
        self.assertFalse(any(x["title"] == "Periodic trade review: small wins dominate" for x in review["issues"]))

    def test_supervisor_prioritizes_weak_payoff_over_small_wins(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 8, "openedAt": now - 900, "symbol": "AUSDT", "side": "LONG", "pnl": 0.08, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 7, "openedAt": now - 850, "symbol": "BUSDT", "side": "LONG", "pnl": 0.09, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 6, "openedAt": now - 800, "symbol": "CUSDT", "side": "SHORT", "pnl": 0.07, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 5, "openedAt": now - 760, "symbol": "DUSDT", "side": "LONG", "pnl": 0.06, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 4, "openedAt": now - 720, "symbol": "EUSDT", "side": "SHORT", "pnl": -0.20, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 3, "openedAt": now - 680, "symbol": "FUSDT", "side": "LONG", "pnl": -0.18, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 2, "openedAt": now - 640, "symbol": "GUSDT", "side": "SHORT", "pnl": 0.05, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 1, "openedAt": now - 600, "symbol": "HUSDT", "side": "LONG", "pnl": -0.16, "reason": "LIVE_CUT_LOSING_SIDE"},
        ]
        bot = {
            "running": True,
            "config": {"executionMode": "LIVE"},
            "tradesLastHour": 8,
            "hermesAgents": state,
            "openLivePositions": [],
            "liveStatsAll": {"lastTrades": trades},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertTrue(any(x["title"] == "Periodic trade review: weak payoff ratio" for x in review["issues"]))
        self.assertFalse(any(x["title"] == "Periodic trade review: small wins dominate" for x in review["issues"]))
        self.assertTrue(any(x["title"] == "Periodic trade review: weak payoff ratio" and x.get("supervisorFirst") for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_auto_tunes_weak_payoff_when_live(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 8, "openedAt": now - 900, "symbol": "AUSDT", "side": "LONG", "pnl": 0.24, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 7, "openedAt": now - 850, "symbol": "BUSDT", "side": "SHORT", "pnl": -0.50, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 6, "openedAt": now - 800, "symbol": "CUSDT", "side": "LONG", "pnl": 0.22, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 5, "openedAt": now - 760, "symbol": "DUSDT", "side": "SHORT", "pnl": -0.49, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 4, "openedAt": now - 720, "symbol": "EUSDT", "side": "LONG", "pnl": 0.26, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 3, "openedAt": now - 680, "symbol": "FUSDT", "side": "SHORT", "pnl": 0.25, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 2, "openedAt": now - 640, "symbol": "GUSDT", "side": "LONG", "pnl": 0.23, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 1, "openedAt": now - 600, "symbol": "HUSDT", "side": "LONG", "pnl": -0.51, "reason": "LOCAL_SL_HIT"},
        ]
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "holdWinners": True,
            "holdMinConfidence": 0.78,
            "tpTargetMinUsdt": 0.55,
            "tpTargetMaxUsdt": 2.2,
            "profitLockBreakevenTriggerUsdt": 0.16,
            "stopLossPct": 0.9,
        }
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["liveStatsAll"] = {"lastTrades": trades}
        main.AUTO_TRADE["supervisorAutoTune"] = {}

        try:
            with mock.patch.object(main, "_live_closed_trades_from_log", return_value=trades):
                with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                    review = main._hermes_supervisor_review(main.AUTO_TRADE)
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune

        self.assertTrue(any(x["title"] == "Periodic trade review: weak payoff ratio" for x in review["issues"]))
        self.assertTrue(any(x.get("action") == "auto-tuned weak payoff policy" and x.get("status") == "applied" for x in review["autoActions"]))
        self.assertLess(main.AUTO_TRADE["config"]["holdMinConfidence"], 0.78)
        self.assertGreater(main.AUTO_TRADE["config"]["tpTargetMinUsdt"], 0.55)
        self.assertGreater(main.AUTO_TRADE["config"]["profitLockBreakevenTriggerUsdt"], 0.16)

    def test_supervisor_handles_weak_payoff_when_policy_at_safe_limits(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 8, "openedAt": now - 900, "symbol": "AUSDT", "side": "LONG", "pnl": 0.24, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 7, "openedAt": now - 850, "symbol": "BUSDT", "side": "SHORT", "pnl": -0.50, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 6, "openedAt": now - 800, "symbol": "CUSDT", "side": "LONG", "pnl": 0.22, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 5, "openedAt": now - 760, "symbol": "DUSDT", "side": "SHORT", "pnl": -0.49, "reason": "LOCAL_SL_HIT"},
            {"closedAt": now - 4, "openedAt": now - 720, "symbol": "EUSDT", "side": "LONG", "pnl": 0.26, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 3, "openedAt": now - 680, "symbol": "FUSDT", "side": "SHORT", "pnl": 0.25, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 2, "openedAt": now - 640, "symbol": "GUSDT", "side": "LONG", "pnl": 0.23, "reason": "LIVE_CUT_LOSING_SIDE"},
            {"closedAt": now - 1, "openedAt": now - 600, "symbol": "HUSDT", "side": "LONG", "pnl": -0.51, "reason": "LOCAL_SL_HIT"},
        ]
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "holdWinners": True,
            "holdMinConfidence": 0.68,
            "tpTargetMinUsdt": 1.20,
            "tpTargetMaxUsdt": 3.20,
            "profitLockBreakevenTriggerUsdt": 0.45,
            "stopLossPct": 0.55,
        }
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["liveStatsAll"] = {"lastTrades": trades}
        main.AUTO_TRADE["supervisorAutoTune"] = {}

        try:
            with mock.patch.object(main, "_live_closed_trades_from_log", return_value=trades):
                with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                    review = main._hermes_supervisor_review(main.AUTO_TRADE)
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune

        self.assertTrue(any(x["title"] == "Periodic trade review: weak payoff ratio" for x in review["issues"]))
        self.assertTrue(any(x.get("action") == "weak payoff policy already at safe limits" and x.get("status") == "applied" for x in review["autoActions"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_reports_small_wins_without_weak_payoff(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 8, "openedAt": now - 900, "symbol": "AUSDT", "side": "LONG", "pnl": 0.08, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 7, "openedAt": now - 850, "symbol": "BUSDT", "side": "LONG", "pnl": 0.09, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 6, "openedAt": now - 800, "symbol": "CUSDT", "side": "SHORT", "pnl": 0.07, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 5, "openedAt": now - 760, "symbol": "DUSDT", "side": "LONG", "pnl": 0.06, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 4, "openedAt": now - 720, "symbol": "EUSDT", "side": "SHORT", "pnl": 0.07, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 3, "openedAt": now - 680, "symbol": "FUSDT", "side": "LONG", "pnl": 0.08, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 2, "openedAt": now - 640, "symbol": "GUSDT", "side": "SHORT", "pnl": 0.09, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 1, "openedAt": now - 600, "symbol": "HUSDT", "side": "LONG", "pnl": 0.07, "reason": "WEAK_SIGNAL"},
        ]
        bot = {
            "running": True,
            "config": {"executionMode": "LIVE"},
            "tradesLastHour": 8,
            "hermesAgents": state,
            "openLivePositions": [],
            "liveStatsAll": {"lastTrades": trades},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertFalse(any(x["title"] == "Periodic trade review: weak payoff ratio" for x in review["issues"]))
        self.assertTrue(any(x["title"] == "Periodic trade review: small wins dominate" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_auto_tunes_small_wins_when_live(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - 8, "openedAt": now - 900, "symbol": "AUSDT", "side": "LONG", "pnl": 0.08, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 7, "openedAt": now - 850, "symbol": "BUSDT", "side": "LONG", "pnl": 0.09, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 6, "openedAt": now - 800, "symbol": "CUSDT", "side": "SHORT", "pnl": 0.07, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 5, "openedAt": now - 760, "symbol": "DUSDT", "side": "LONG", "pnl": 0.06, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 4, "openedAt": now - 720, "symbol": "EUSDT", "side": "SHORT", "pnl": 0.07, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 3, "openedAt": now - 680, "symbol": "FUSDT", "side": "LONG", "pnl": 0.08, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 2, "openedAt": now - 640, "symbol": "GUSDT", "side": "SHORT", "pnl": 0.09, "reason": "WEAK_SIGNAL"},
            {"closedAt": now - 1, "openedAt": now - 600, "symbol": "HUSDT", "side": "LONG", "pnl": 0.07, "reason": "WEAK_SIGNAL"},
        ]
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["config"] = {
            "executionMode": "LIVE",
            "holdWinners": True,
            "holdMinConfidence": 0.72,
            "tpTargetMinUsdt": 0.55,
            "tpTargetMaxUsdt": 2.2,
            "profitLockBreakevenTriggerUsdt": 0.16,
        }
        main.AUTO_TRADE["hermesAgents"] = state
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["liveStatsAll"] = {"lastTrades": trades}
        main.AUTO_TRADE["supervisorAutoTune"] = {}

        try:
            with mock.patch.object(main, "_live_closed_trades_from_log", return_value=trades):
                with mock.patch.object(main, "_persist_autotrade_snapshot"), mock.patch.object(lg, "_autotrade_log"):
                    review = main._hermes_supervisor_review(main.AUTO_TRADE)
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune

        self.assertTrue(any(x["title"] == "Periodic trade review: small wins dominate" for x in review["issues"]))
        self.assertTrue(any(x.get("action") == "auto-tuned small-profit capture policy" and x.get("status") == "applied" for x in review["autoActions"]))
        self.assertNotIn("cmuxHandoff", review)
        self.assertLess(main.AUTO_TRADE["config"]["holdMinConfidence"], 0.72)
        self.assertGreater(main.AUTO_TRADE["config"]["tpTargetMinUsdt"], 0.55)

    def test_supervisor_reports_missing_learning_agents_after_trades(self):
        state = main.new_agent_state()
        now = int(main.time.time())
        trades = [
            {"closedAt": now - i, "symbol": "BTCUSDT", "side": "LONG", "pnl": 0.3, "reason": "LIVE_CLOSE"}
            for i in range(1, 9)
        ]
        bot = {
            "hermesAgents": state,
            "openLivePositions": [],
            "liveStatsAll": {"lastTrades": trades},
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        titles = {x["title"] for x in review["issues"]}
        self.assertIn("Trade memory not confirmed", titles)
        self.assertIn("Reflection not using recent losses/wins", titles)
        self.assertIn("Backtest validation missing", titles)
        self.assertTrue(any(x["agent"] == "memory_agent" for x in review["autoActions"]))
        self.assertTrue(any(x["title"] == "Backtest validation missing" and x.get("supervisorFirst") for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_ignores_known_operator_permission_hold(self):
        prev_errors = main.AUTO_TRADE.get("consecutiveErrors", 0)
        prev_skip = main.AUTO_TRADE.get("lastSkip")
        try:
            main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
            main.AUTO_TRADE["consecutiveErrors"] = 2
            main.AUTO_TRADE["lastSkip"] = {
                "ts": int(main.time.time()),
                "code": "binance_permission_required",
                "msg": "LIVE ถูกหยุดชั่วคราว: Binance API key/IP/permission ไม่ผ่าน (-2015)",
            }

            out = main.asyncio.run(main.autotrade_status_lite())

            review = out["hermesSupervisorReview"]
            self.assertFalse(any(x["title"] == "Backend loop errors" for x in review["issues"]))
            self.assertNotIn("cmuxHandoff", review)
        finally:
            main.AUTO_TRADE["consecutiveErrors"] = prev_errors
            main.AUTO_TRADE["lastSkip"] = prev_skip

    def test_supervisor_ignores_legacy_risk_run_imbalance_after_split(self):
        state = main.new_agent_state()
        state["agents"]["risk_manager"]["runs"] = 230
        state["agents"]["market_analyst"]["runs"] = 150
        state["agents"]["position_guardian"]["runs"] = 40
        state["agents"]["portfolio_manager"]["runs"] = 1
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.asyncio.run(main.autotrade_status_lite())

        review = out["hermesSupervisorReview"]
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_ignores_guardian_heartbeat_run_imbalance(self):
        state = main.new_agent_state()
        state["agents"]["position_guardian"]["runs"] = 119
        state["agents"]["position_guardian"]["state"] = "done"
        state["agents"]["position_guardian"]["lastAction"] = "open positions heartbeat"
        state["agents"]["position_guardian"]["updatedAt"] = int(main.time.time())
        state["agents"]["market_analyst"]["runs"] = 42
        state["agents"]["risk_manager"]["runs"] = 45
        state["agents"]["backtest_agent"]["runs"] = 3
        bot = {
            "running": True,
            "config": {"executionMode": "LIVE"},
            "tradesLastHour": 0,
            "hermesAgents": state,
            "openLivePositions": [{"symbol": "BTCUSDT", "side": "LONG", "qty": 1.0}],
            "consecutiveErrors": 0,
        }

        review = main._hermes_supervisor_review(bot)

        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_ignores_guardian_loop_cadence_when_risk_manager_also_active(self):
        state = main.new_agent_state()
        runs = {
            "market_analyst": 51,
            "data_quality_guard": 39,
            "news_sentiment_guard": 39,
            "risk_manager": 1158,
            "portfolio_manager": 45,
            "position_guardian": 1559,
            "strategy_builder": 47,
            "backtest_agent": 16,
            "execution_agent": 102,
            "reflection_agent": 16,
            "memory_agent": 30,
        }
        for agent_id, run_count in runs.items():
            state["agents"][agent_id]["runs"] = run_count
        state["agents"]["position_guardian"]["state"] = "done"
        state["agents"]["position_guardian"]["lastAction"] = "open positions checked"
        state["agents"]["risk_manager"]["state"] = "done"
        state["agents"]["risk_manager"]["lastAction"] = "monitor risk cooldown release"

        review = main._hermes_supervisor_review(
            {
                "running": True,
                "config": {"executionMode": "LIVE"},
                "hermesAgents": state,
                "openLivePositions": [],
                "lastSkip": {"code": "risk_cooldown", "msg": "Skip: risk cooldown 45m"},
                "consecutiveErrors": 0,
            }
        )

        self.assertFalse(any(x["title"] == "Workload imbalance" and x["agent"] == "position_guardian" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_compares_workload_against_cadence_agents_only(self):
        state = main.new_agent_state()
        runs = {
            "market_analyst": 31,
            "data_quality_guard": 31,
            "news_sentiment_guard": 31,
            "risk_manager": 31,
            "portfolio_manager": 27,
            "position_guardian": 31,
            "strategy_builder": 27,
            "backtest_agent": 0,
            "execution_agent": 0,
            "reflection_agent": 0,
            "memory_agent": 1,
        }
        for agent_id, run_count in runs.items():
            state["agents"][agent_id]["runs"] = run_count

        review = main._hermes_supervisor_review(
            {
                "running": True,
                "config": {"executionMode": "LIVE"},
                "hermesAgents": state,
                "openLivePositions": [{"symbol": "BTCUSDT", "side": "LONG", "qty": 1.0}],
                "consecutiveErrors": 0,
            }
        )

        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_ignores_portfolio_loop_cadence_workload(self):
        state = main.new_agent_state()
        runs = {
            "market_analyst": 22,
            "data_quality_guard": 2,
            "news_sentiment_guard": 2,
            "risk_manager": 28,
            "portfolio_manager": 54,
            "position_guardian": 16,
            "strategy_builder": 17,
            "backtest_agent": 2,
            "execution_agent": 5,
            "reflection_agent": 2,
            "memory_agent": 4,
        }
        for agent_id, run_count in runs.items():
            state["agents"][agent_id]["runs"] = run_count
        state["agents"]["portfolio_manager"]["state"] = "done"
        state["agents"]["portfolio_manager"]["lastAction"] = "verified capacity for new positions"
        state["agents"]["risk_manager"]["state"] = "done"
        state["agents"]["risk_manager"]["lastAction"] = "risk checks complete"

        review = main._hermes_supervisor_review(
            {
                "running": True,
                "config": {"executionMode": "LIVE", "marketScan": True},
                "hermesAgents": state,
                "openLivePositions": [],
                "scanBoard": [
                    {"symbol": "BTCUSDT", "qualified": False, "rejectReason": "low_conf"},
                ],
                "lastSkip": {"code": "scan_none", "msg": "Skip: scan found no clear symbol"},
                "consecutiveErrors": 0,
            }
        )

        self.assertFalse(any(x["title"] == "Workload imbalance" and x["agent"] == "portfolio_manager" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_ignores_market_workload_during_risk_cooldown_probe_timeout(self):
        state = main.new_agent_state()
        runs = {
            "market_analyst": 59,
            "data_quality_guard": 8,
            "news_sentiment_guard": 8,
            "risk_manager": 45,
            "portfolio_manager": 21,
            "position_guardian": 37,
            "strategy_builder": 7,
            "backtest_agent": 5,
            "execution_agent": 16,
            "reflection_agent": 5,
            "memory_agent": 9,
        }
        for agent_id, run_count in runs.items():
            state["agents"][agent_id]["runs"] = run_count

        review = main._hermes_supervisor_review(
            {
                "running": True,
                "config": {"executionMode": "LIVE"},
                "hermesAgents": state,
                "openLivePositions": [],
                "lastSkip": {
                    "code": "risk_cooldown",
                    "msg": "Skip: risk cooldown 2677s · adaptive check timeout; retrying",
                },
                "consecutiveErrors": 0,
            }
        )

        self.assertFalse(any(x["title"] == "Workload imbalance" and x["agent"] == "market_analyst" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_supervisor_uses_wider_market_workload_threshold_for_scan_fanout(self):
        state = main.new_agent_state()
        runs = {
            "market_analyst": 31,
            "data_quality_guard": 3,
            "news_sentiment_guard": 3,
            "risk_manager": 15,
            "portfolio_manager": 1,
            "position_guardian": 14,
            "strategy_builder": 2,
            "backtest_agent": 2,
            "execution_agent": 1,
            "reflection_agent": 2,
            "memory_agent": 4,
        }
        for agent_id, run_count in runs.items():
            state["agents"][agent_id]["runs"] = run_count

        review = main._hermes_supervisor_review(
            {
                "running": True,
                "config": {"executionMode": "LIVE", "marketScan": True},
                "hermesAgents": state,
                "openLivePositions": [{"symbol": "BTCUSDT", "side": "LONG", "qty": 1.0}],
                "consecutiveErrors": 0,
            }
        )

        self.assertFalse(any(x["title"] == "Workload imbalance" and x["agent"] == "market_analyst" for x in review["issues"]))
        self.assertNotIn("cmuxHandoff", review)

    def test_weak_payoff_tune_tightens_loss_guard_when_losses_dwarf_wins(self):
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        prev_config = main.AUTO_TRADE.get("config")
        main.AUTO_TRADE["supervisorAutoTune"] = {}
        cfg = {
            "holdWinners": True,
            "holdMinConfidence": 0.75,
            "tpTargetMinUsdt": 0.632,
            "tpTargetMaxUsdt": 2.464,
            "profitLockBreakevenTriggerUsdt": 0.2,
            "stopLossPct": 0.752,
            "payoffLossGuardLossToWinCap": 1.05,
            "payoffLossGuardMaxLossUsdt": 0.9,
            "payoffLossGuardMinLossUsdt": 0.22,
            "supervisorSizeMultiplier": 1.0,
        }
        review = {
            "label": "last_8_trades",
            "trades": 8,
            "payoffRatio": 0.36,
            "avgWin": 0.279066,
            "avgLoss": -0.780008,
        }
        try:
            out = main._maybe_tune_weak_payoff_from_review(review, cfg)
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune
            main.AUTO_TRADE["config"] = prev_config

        self.assertTrue(out.get("applied"))
        self.assertLess(cfg["stopLossPct"], 0.752)
        self.assertLessEqual(cfg["payoffLossGuardLossToWinCap"], 0.95)
        self.assertLessEqual(cfg["payoffLossGuardMaxLossUsdt"], 0.75)
        self.assertLessEqual(cfg["payoffLossGuardMinLossUsdt"], 0.22)
        self.assertLessEqual(cfg["supervisorSizeMultiplier"], 0.85)

    def test_weak_payoff_tune_reduces_risk_for_june_five_like_payoff(self):
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        prev_config = main.AUTO_TRADE.get("config")
        main.AUTO_TRADE["supervisorAutoTune"] = {}
        cfg = {
            "holdWinners": True,
            "holdMinConfidence": 0.75,
            "tpTargetMinUsdt": 0.632,
            "tpTargetMaxUsdt": 2.464,
            "profitLockBreakevenTriggerUsdt": 0.2,
            "stopLossPct": 0.9,
            "payoffLossGuardLossToWinCap": 1.05,
            "payoffLossGuardMaxLossUsdt": 0.9,
            "payoffLossGuardMinLossUsdt": 0.22,
            "supervisorSizeMultiplier": 1.0,
        }
        review = {
            "label": "last_98_trades",
            "trades": 98,
            "payoffRatio": 0.643,
            "avgWin": 0.35061,
            "avgLoss": -0.545271,
        }
        try:
            out = main._maybe_tune_weak_payoff_from_review(review, cfg)
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune
            main.AUTO_TRADE["config"] = prev_config

        self.assertTrue(out.get("applied"))
        self.assertLess(cfg["stopLossPct"], 0.9)
        self.assertLessEqual(cfg["payoffLossGuardLossToWinCap"], 0.95)
        self.assertLessEqual(cfg["payoffLossGuardMaxLossUsdt"], 0.75)
        self.assertLessEqual(cfg["supervisorSizeMultiplier"], 0.85)

    def test_supervisor_ignores_portfolio_capacity_run_imbalance(self):
        state = main.new_agent_state()
        runs = {
            "market_analyst": 5,
            "data_quality_guard": 5,
            "news_sentiment_guard": 5,
            "risk_manager": 11,
            "portfolio_manager": 30,
            "position_guardian": 1,
            "strategy_builder": 5,
            "backtest_agent": 1,
            "execution_agent": 20,
            "reflection_agent": 1,
            "memory_agent": 2,
        }
        for agent_id, run_count in runs.items():
            state["agents"][agent_id]["runs"] = run_count
        state["agents"]["portfolio_manager"]["lastAction"] = "portfolio capacity ok"

        review = main._hermes_supervisor_review(
            {
                "running": True,
                "config": {"executionMode": "LIVE"},
                "hermesAgents": state,
                "openLivePositions": [],
                "consecutiveErrors": 0,
            }
        )

        self.assertNotIn("cmuxHandoff", review)

    def test_agent_start_cycle_clears_stale_action(self):
        state = main.mark_agent(main.new_agent_state(), "memory_agent", "done", "decision stored", "LIVE XLMUSDT LONG")

        state = main.start_cycle(state)

        agent = state["agents"]["memory_agent"]
        self.assertEqual(agent["state"], "todo")
        self.assertEqual(agent["lastAction"], "waiting")
        self.assertEqual(agent["lastReason"], "")
        self.assertNotIn("data", agent)

    def test_agent_mark_dedupes_repeated_same_event(self):
        state = main.mark_agent(main.new_agent_state(), "market_analyst", "done", "scan completed", "BTCUSDT", {"picked": "BTCUSDT"})
        first_agent = dict(state["agents"]["market_analyst"])

        state = main.mark_agent(state, "market_analyst", "done", "scan completed", "BTCUSDT", {"picked": "BTCUSDT"})
        second_agent = state["agents"]["market_analyst"]

        self.assertEqual(second_agent["runs"], first_agent["runs"])
        self.assertEqual(second_agent["updatedAt"], first_agent["updatedAt"])

    def test_agent_mark_keeps_distinct_events(self):
        state = main.mark_agent(main.new_agent_state(), "market_analyst", "done", "scan completed", "BTCUSDT", {"picked": "BTCUSDT"})
        first_runs = state["agents"]["market_analyst"]["runs"]

        state = main.mark_agent(state, "market_analyst", "done", "scan completed", "ETHUSDT", {"picked": "ETHUSDT"})

        self.assertEqual(state["agents"]["market_analyst"]["runs"], first_runs + 1)
        self.assertEqual(state["agents"]["market_analyst"]["lastReason"], "ETHUSDT")


class TestLiveSymbolDuplicateGuard(unittest.TestCase):
    def test_open_side_from_position_state_prefers_any_open_side(self):
        self.assertEqual(main._open_side_from_position_state({"long": 1.0, "short": 0.0, "net": 1.0}), "LONG")
        self.assertEqual(main._open_side_from_position_state({"long": 0.0, "short": 2.0, "net": -2.0}), "SHORT")
        self.assertEqual(main._open_side_from_position_state({"long": 1.0, "short": 2.0, "net": -1.0}), "HEDGE")
        self.assertEqual(main._open_side_from_position_state({"long": 0.0, "short": 0.0, "net": 0.0}), "FLAT")

    def test_open_symbols_from_positions_returns_only_nonzero_symbols(self):
        rows = [
            {"symbol": "ADAUSDT", "qty": 10},
            {"symbol": "BTCUSDT", "qty": 0},
            {"symbol": "ETHUSDT", "positionAmt": "-0.2"},
        ]

        self.assertEqual(main._open_symbols_from_positions(rows), {"ADAUSDT", "ETHUSDT"})


if __name__ == "__main__":
    unittest.main()
