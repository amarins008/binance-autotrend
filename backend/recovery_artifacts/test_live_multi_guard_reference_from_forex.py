import unittest
from unittest import mock

import main


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
            with mock.patch.object(main, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(main, "_close_position_one_side", new=mock.AsyncMock(return_value={"ok": True})) as close_one:
                    with mock.patch.object(main, "intel_analyze", new=mock.AsyncMock(return_value={"signal": "WAIT", "confidence": 0.5, "execution": {"momentumPct": 0.0}})):
                        with mock.patch.object(main, "_autotrade_log"):
                            changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertTrue(changed)
        close_one.assert_awaited_once()
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
            with mock.patch.object(main, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(main, "intel_analyze", new=mock.AsyncMock(return_value={"signal": "WAIT", "confidence": 0.5, "execution": {"momentumPct": 0.0}})):
                    with mock.patch.object(main, "_close_position_one_side", new=mock.AsyncMock()) as close_one:
                        changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertFalse(changed)
        close_one.assert_not_awaited()
        lock = main.AUTO_TRADE["liveProfitLocks"]["DOGEUSDT:LONG"]
        self.assertAlmostEqual(lock["entryMark"], 0.0980)
        self.assertAlmostEqual(lock["tp"], 0.099764)
        self.assertAlmostEqual(lock["sl"], 0.097118)

    async def test_multi_guard_closes_positive_retrace_before_negative(self):
        cfg = {"takeProfitPct": 1.8, "stopLossPct": 0.9, "tpTargetMinUsdt": 0.55, "profitLockBreakevenFloorUsdt": 0.08}
        main.AUTO_TRADE["liveProfitLocks"] = {
            "DOGEUSDT:LONG": {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "entryMark": 1.0,
                "tp": 1.018,
                "sl": 0.991,
                "peak": 0.30,
            }
        }
        rows = [
            {
                "symbol": "DOGEUSDT",
                "side": "LONG",
                "qty": 100.0,
                "entryMark": 1.0,
                "markPrice": 1.0006,
                "notionalUsdtApprox": 100.06,
                "unRealizedProfit": 0.06,
            }
        ]

        with mock.patch.dict(main.os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
            with mock.patch.object(main, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(main, "_close_position_one_side", new=mock.AsyncMock(return_value={"ok": True})) as close_one:
                    with mock.patch.object(main, "intel_analyze", new=mock.AsyncMock(return_value={"signal": "WAIT", "confidence": 0.5, "execution": {"momentumPct": 0.0}})):
                        with mock.patch.object(main, "_autotrade_log") as log:
                            changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertTrue(changed)
        close_one.assert_awaited_once_with("DOGEUSDT", "LONG", "k", "s", main._binance_base())
        self.assertEqual(main.AUTO_TRADE["liveProfitLocks"], {})
        self.assertTrue(any("BREAKEVEN_GUARD" in call.args[0] for call in log.call_args_list))

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
            with mock.patch.object(main, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(main, "intel_analyze", new=mock.AsyncMock(return_value=follow_intel)):
                    with mock.patch.object(main, "_close_position_one_side", new=mock.AsyncMock()) as close_one:
                        with mock.patch.object(main, "_autotrade_log") as log:
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
            with mock.patch.object(main, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(main, "intel_analyze", new=mock.AsyncMock(return_value={"signal": "WAIT", "confidence": 0.5, "execution": {"momentumPct": 0.0}})):
                    with mock.patch.object(main, "_close_position_one_side", new=mock.AsyncMock()) as close_one:
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
            "precision": {"longScore": 2.0, "shortScore": 5.0},
            "execution": {"momentumPct": -0.42},
        }

        with mock.patch.dict(main.os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
            with mock.patch.object(main, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=rows)):
                with mock.patch.object(main, "intel_analyze", new=mock.AsyncMock(return_value=opposite_intel)):
                    with mock.patch.object(main, "_close_position_one_side", new=mock.AsyncMock(return_value={"closed": [{"ok": True}]})) as close_one:
                        with mock.patch.object(main, "place_futures_order", new=mock.AsyncMock()) as place_order:
                            with mock.patch.object(main, "_autotrade_log") as log:
                                changed = await main._live_multi_profit_lock_manage(cfg)

        self.assertTrue(changed)
        close_one.assert_awaited_once_with("DOGEUSDT", "LONG", "k", "s", main._binance_base())
        place_order.assert_not_awaited()
        self.assertNotIn("DOGEUSDT:LONG", main.AUTO_TRADE["liveProfitLocks"])
        self.assertTrue(any("STRONG_REVERSAL_EXIT" in call.args[0] for call in log.call_args_list))

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
                {"symbol": "XLMUSDT", "signal": "LONG", "confidence": 0.82, "execution": {}},
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
        }
        main.AUTO_TRADE["running"] = True
        main.AUTO_TRADE["manageOpenOnly"] = False
        main.AUTO_TRADE["pauseUntil"] = 0
        main.AUTO_TRADE["consecutiveErrors"] = 0
        main.AUTO_TRADE["lastSkip"] = None
        main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
        main.AUTO_TRADE["trades"] = []
        main.AUTO_TRADE["lastTradeAt"] = 0
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
                    "execution": {"mark": 100.0, "bid": 99.99, "ask": 100.01, "spreadBps": 2.0},
                },
                [{"symbol": "BTCUSDT", "qualified": False, "rejectReason": "signal_wait"}],
            ))):
                with mock.patch.object(main.asyncio, "sleep", new=stop_sleep):
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
                with mock.patch.object(main, "_pick_live_orphan_positions", new=mock.AsyncMock(return_value=[])):
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

class TestMarketScanTimeoutGuard(unittest.IsolatedAsyncioTestCase):
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

        cfg = {"minConfidence": 0.7, "scanAnalyzeTop": 3, "scanSidePreference": "score"}
        with mock.patch.object(main, "_scan_market_candidates", new=mock.AsyncMock(return_value=["OPENUSDT", "FREEUSDT"])):
            with mock.patch.object(main, "intel_analyze", new=fake_intel):
                with mock.patch.object(main, "_symbol_perf_gate", return_value=(True, "", {"trades": 0})):
                    picked_symbol, _picked_intel, board = await main._pick_best_symbol_from_scan(cfg, {"OPENUSDT"})

        self.assertEqual(picked_symbol, "FREEUSDT")
        self.assertEqual([row["symbol"] for row in board], ["FREEUSDT"])
        self.assertEqual(analyzed, ["FREEUSDT"])

    async def test_market_candidates_fall_back_when_ticker_timeout(self):
        async def slow_data_get(_path):
            await main.asyncio.sleep(20)

        with mock.patch.object(main, "_data_get", new=slow_data_get):
            symbols = await main._scan_market_candidates(30)

        self.assertIn("BTCUSDT", symbols)
        self.assertIn("XRPUSDT", symbols)
        self.assertGreaterEqual(len(symbols), 5)

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
                    picked_symbol, picked_intel, board = await main._pick_best_symbol_from_scan(
                        {"minConfidence": 0.7, "scanAnalyzeTop": 3, "scanFallbackNearEnabled": True, "scanFallbackRetrySymbols": 3}
                    )

        self.assertEqual(picked_symbol, "ETHUSDT")
        self.assertIsInstance(picked_intel, dict)
        self.assertTrue(any(row["symbol"] == "ETHUSDT" and row["rejectReason"] == "fallback_recovered" for row in board))


class TestSymbolPerfGate(unittest.TestCase):
    def setUp(self):
        self.prev_locks = main.AUTO_TRADE.get("perfLocks")
        main.AUTO_TRADE["perfLocks"] = {}

    def tearDown(self):
        main.AUTO_TRADE["perfLocks"] = self.prev_locks if isinstance(self.prev_locks, dict) else {}

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


class TestStatusLitePositionCard(unittest.TestCase):
    def setUp(self):
        self.prev_config = main.AUTO_TRADE.get("config")
        self.prev_locks = main.AUTO_TRADE.get("liveProfitLocks")
        self.prev_paper = main.AUTO_TRADE.get("paper")
        self.prev_last_decision = main.AUTO_TRADE.get("lastDecision")
        self.prev_log = main.AUTO_TRADE.get("log")
        self.prev_trades = main.AUTO_TRADE.get("trades")
        self.prev_agents = main.AUTO_TRADE.get("hermesAgents")
        main.AUTO_TRADE["config"] = {"executionMode": "LIVE", "symbol": "AUTO", "intervalSec": 25, "marketScan": True}

    def tearDown(self):
        main.AUTO_TRADE["config"] = self.prev_config
        main.AUTO_TRADE["liveProfitLocks"] = self.prev_locks
        main.AUTO_TRADE["paper"] = self.prev_paper
        main.AUTO_TRADE["lastDecision"] = self.prev_last_decision
        main.AUTO_TRADE["log"] = self.prev_log
        main.AUTO_TRADE["trades"] = self.prev_trades
        main.AUTO_TRADE["hermesAgents"] = self.prev_agents

    def test_status_lite_exposes_position_from_profit_lock(self):
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

        out = main.autotrade_status_lite()

        self.assertEqual(out["activePosition"]["live"]["side"], "SHORT")
        self.assertEqual(out["activePosition"]["live"]["symbol"], "XRPUSDT")
        self.assertEqual(out["activePosition"]["live"]["qty"], 21.8)
        self.assertEqual(out["activePosition"]["live"]["localTp"], 1.2676)
        self.assertEqual(out["openLivePositions"][0]["symbol"], "XRPUSDT")
        self.assertEqual(out["openLivePositions"][0]["localTp"], 1.2676)

    def test_status_lite_exposes_position_from_profit_lock_two(self):
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

        out = main.autotrade_status_lite()

        self.assertEqual(out["activePosition"]["live"]["side"], "LONG")
        self.assertEqual(out["activePosition"]["live"]["symbol"], "DOGEUSDT")
        self.assertEqual(out["openLivePositions"][0]["symbol"], "DOGEUSDT")
        self.assertAlmostEqual(out["openLivePositions"][0]["notionalUsdtApprox"], 14.652)

    def test_status_lite_keeps_dashboard_kpi_fields(self):
        main.AUTO_TRADE["paper"] = {"wins": 2, "losses": 1, "realizedPnl": 1.25, "position": None, "history": []}
        main.AUTO_TRADE["liveProfitLocks"] = {}

        def fake_stats(symbol=None):
            if symbol is None:
                return {"wins": 4, "losses": 2, "realizedPnl": 3.4, "winsToday": 1, "lossesToday": 1, "realizedPnlToday": -0.2}
            return {"wins": 3, "losses": 1, "realizedPnl": 2.75, "winsToday": 1, "lossesToday": 0, "realizedPnlToday": 0.5, "lastTrades": []}

        with mock.patch.object(main, "_aggregate_live_trade_stats_from_log", side_effect=fake_stats):
            out = main.autotrade_status_lite()

        self.assertEqual(out["paper"]["winRatePct"], 66.67)
        self.assertEqual(out["paper"]["realizedPnl"], 1.25)
        self.assertEqual(out["liveStats"]["wins"], 3)
        self.assertEqual(out["liveStats"]["winRatePct"], 75.0)
        self.assertEqual(out["liveStats"]["realizedPnl"], 2.75)
        self.assertEqual(out["liveStatsAll"]["wins"], 4)
        self.assertEqual(out["liveStatsAll"]["winRatePct"], 66.67)
        self.assertEqual(out["liveStatsAll"]["realizedPnl"], 3.4)
        self.assertEqual(out["kpiTodayAllSymbols"]["live"]["realizedPnl"], -0.2)

    def test_status_lite_keeps_decision_log_fields(self):
        main.AUTO_TRADE["lastDecision"] = {"symbol": "XRPUSDT", "signal": "LONG", "confidence": 0.77}
        main.AUTO_TRADE["log"] = [{"ts": 123, "msg": "Skip: already in LONG"}]
        main.AUTO_TRADE["trades"] = [int(main.time.time())]

        out = main.autotrade_status_lite()

        self.assertEqual(out["lastDecision"]["symbol"], "XRPUSDT")
        self.assertEqual(out["lastDecision"]["signal"], "LONG")
        self.assertEqual(out["log"][0]["msg"], "Skip: already in LONG")
        self.assertEqual(out["tradesLastHour"], 1)

    def test_status_lite_exposes_hermes_agents_kanban(self):
        main.AUTO_TRADE["hermesAgents"] = main.start_cycle(main.new_agent_state())
        main._agent_mark("market_analyst", "done", "scan completed", "BTCUSDT")

        out = main.autotrade_status_lite()

        self.assertIn("hermesAgents", out)
        self.assertIn("market_analyst", out["hermesAgents"]["agents"])
        self.assertIn("market_analyst", out["hermesAgents"]["kanban"]["done"])
        self.assertEqual(out["hermesAgents"]["engine"]["title"], "AI Decision Engine")

    def test_status_lite_exposes_supervisor_review(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "risk_manager", "blocked", "symbol daily cap", "XLMUSDT 14/14")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.autotrade_status_lite()

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "high")
        self.assertTrue(any(x["agent"] == "risk_manager" for x in review["issues"]))
        self.assertTrue(review["cmuxHandoff"])

    def test_supervisor_treats_portfolio_capacity_as_hold(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "portfolio_manager", "blocked", "portfolio capacity full", "4/4")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.autotrade_status_lite()

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "low")
        self.assertTrue(any(x["title"] == "Capacity hold" for x in review["issues"]))
        self.assertFalse(review["cmuxHandoff"])

    def test_supervisor_treats_adaptive_cooldown_as_safety_hold(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "risk_manager", "blocked", "adaptive cooldown hold", "market volatile (XLMUSDT)")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.autotrade_status_lite()

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "low")
        self.assertTrue(any(x["title"] == "Safety hold" for x in review["issues"]))
        self.assertFalse(review["cmuxHandoff"])

    def test_supervisor_treats_late_chase_as_strategy_safety_hold(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "strategy_builder", "blocked", "late long chase", "bb=0.94 vwapDist=0.42%")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.autotrade_status_lite()

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "low")
        self.assertTrue(any(x["title"] == "Safety hold" for x in review["issues"]))
        self.assertFalse(any(x.get("task") == "Inspect repeated blocked state for strategy_builder: late long chase" for x in review["cmuxHandoff"]))

    def test_supervisor_keeps_cooldown_check_failed_actionable(self):
        state = main.new_agent_state()
        state = main.mark_agent(state, "risk_manager", "blocked", "adaptive cooldown check failed", "timeout")
        main.AUTO_TRADE["hermesAgents"] = state

        out = main.autotrade_status_lite()

        review = out["hermesSupervisorReview"]
        self.assertEqual(review["severity"], "high")
        self.assertTrue(review["cmuxHandoff"])

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

            out = main.autotrade_status_lite()

            review = out["hermesSupervisorReview"]
            self.assertFalse(any(x["title"] == "Backend loop errors" for x in review["issues"]))
            self.assertFalse(any(x.get("task") == "Trace AUTO_TRADE consecutiveErrors and add targeted recovery" for x in review["cmuxHandoff"]))
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

        out = main.autotrade_status_lite()

        review = out["hermesSupervisorReview"]
        self.assertFalse(any(x.get("task") == "Review workload split around risk_manager" for x in review["cmuxHandoff"]))

    def test_agent_start_cycle_clears_stale_action(self):
        state = main.mark_agent(main.new_agent_state(), "memory_agent", "done", "decision stored", "LIVE XLMUSDT LONG")

        state = main.start_cycle(state)

        agent = state["agents"]["memory_agent"]
        self.assertEqual(agent["state"], "todo")
        self.assertEqual(agent["lastAction"], "waiting")
        self.assertEqual(agent["lastReason"], "")
        self.assertNotIn("data", agent)


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
