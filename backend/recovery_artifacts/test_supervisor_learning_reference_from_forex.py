import unittest
import copy
import time
import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import main_forex
import hermes_agents


class TestForexSupervisorLearning(unittest.TestCase):
    def setUp(self):
        self._state = copy.deepcopy(main_forex._STATE)
        main_forex._STATE["lastSupervisorAutoAction"] = None
        main_forex._STATE["supervisorDelegations"] = {}
        main_forex._STATE["learningProfilesBySymbol"] = {}
        main_forex._STATE["lastSymbolLearningReview"] = None
        main_forex._STATE["lastLearningFeedbackAudit"] = None

    def tearDown(self):
        main_forex._STATE.clear()
        main_forex._STATE.update(self._state)

    def test_agent_daily_runs_reset_for_new_day(self):
        state = hermes_agents.new_agent_state()
        state = hermes_agents.mark_agent(state, "market_analyst", "done", "scan market", "EURUSD")
        state = hermes_agents.mark_agent(state, "market_analyst", "done", "scan market", "GBPUSD")
        agent = state["agents"]["market_analyst"]
        today = time.strftime("%Y-%m-%d", time.localtime())

        self.assertEqual(agent["runs"], 2)
        self.assertEqual(agent["dailyRuns"], 2)
        self.assertEqual(agent["dailyRunDate"], today)

        agent["dailyRunDate"] = "2000-01-01"
        agent["dailyRuns"] = 999
        state = hermes_agents.ensure_agent_state(state)
        agent = state["agents"]["market_analyst"]

        self.assertEqual(agent["runs"], 2)
        self.assertEqual(agent["dailyRuns"], 0)
        self.assertEqual(agent["dailyRunDate"], today)

    def test_agent_daily_runs_reset_when_state_missing_daily_counter_date(self):
        today = time.strftime("%Y-%m-%d", time.localtime())
        state = hermes_agents.new_agent_state()
        state.pop("dailyCounterDate", None)
        state["agents"]["market_analyst"]["runs"] = 36348
        state["agents"]["market_analyst"]["dailyRuns"] = 36348
        state["agents"]["market_analyst"]["dailyRunDate"] = today

        state = hermes_agents.ensure_agent_state(state)
        agent = state["agents"]["market_analyst"]

        self.assertEqual(state["dailyCounterDate"], today)
        self.assertEqual(agent["runs"], 36348)
        self.assertEqual(agent["dailyRuns"], 0)
        self.assertEqual(agent["dailyRunDate"], today)

    def test_guardian_exit_reports_all_positions_even_when_signal_weak(self):
        positions = [
            {"ticket": 1, "type": 0, "profit": -1.2, "volume": 0.01, "symbol": "XAUUSD"},
            {"ticket": 2, "type": 1, "profit": 0.4, "volume": 0.01, "symbol": "XAUUSD"},
        ]
        weak_signal = {"ok": False, "side": "BUY", "confidence": 0.6, "momentumPoints": 4}
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=weak_signal):
            out = main_forex._guardian_close_wrong_way_positions(
                {"guardianEarlyExitEnabled": True, "guardianLossCapAlwaysOn": False},
                "XAUUSD",
                "DEMO",
                positions,
                {"ok": True},
            )

        self.assertEqual(out["closed"], 0)
        self.assertEqual(out["monitoredCount"], 2)
        self.assertEqual([x["ticket"] for x in out["monitored"]], [1, 2])

    def test_marketcontext_findings_block_pending_forex_reversal(self):
        targets = [{"symbol": "EURUSD", "side": "LONG", "target": "pending_entry"}]
        results = [
            {
                "symbol": "EURUSD",
                "name": "multi_timeframe_analysis",
                "ok": True,
                "data": {"alignment": {"status": "LEAN BEARISH", "confidence": "High"}},
            },
            {
                "symbol": "EURUSD",
                "name": "volume_confirmation_analysis",
                "ok": True,
                "data": {"overall_assessment": {"bullish_signals": 0, "bearish_signals": 1}},
            },
        ]

        findings = main_forex._marketcontext_findings_from_results(targets, results, {})

        self.assertEqual(findings[0]["condition"], "pending_entry_mtf_contradiction")
        self.assertEqual(findings[0]["target"], "pending_entry")
        self.assertEqual(findings[0]["symbol"], "EURUSD")

    def test_entry_marketcontext_context_blocks_recent_contradiction(self):
        main_forex._STATE["marketContextWatcher"] = {
            "updatedAt": int(time.time()),
            "findings": [
                {
                    "symbol": "GBPUSD",
                    "side": "SHORT",
                    "target": "pending_entry",
                    "condition": "pending_entry_mtf_contradiction",
                    "marketContextSignal": "LEAN BULLISH",
                }
            ],
        }

        out = main_forex._entry_marketcontext_context("GBPUSD", "SHORT", {"entryMarketContextBlockContradiction": True})

        self.assertTrue(out["active"])
        self.assertTrue(out["block"])
        self.assertTrue(out["contradiction"])

    def test_guardian_marketcontext_context_tightens_forex_sl_and_blocks_tp_extension(self):
        main_forex._STATE["marketContextWatcher"] = {
            "updatedAt": int(time.time()),
            "findings": [
                {
                    "symbol": "XAUUSD",
                    "side": "LONG",
                    "target": "open_position",
                    "condition": "strong_mtf_contradiction",
                    "marketContextSignal": "LEAN BEARISH",
                }
            ],
        }

        out = main_forex._guardian_marketcontext_context(
            "XAUUSD",
            "LONG",
            mark=2000.0,
            entry=1996.0,
            volume=0.01,
            sl=1990.0,
            tp=2010.0,
            cfg={"guardianMarketContextTightenSlPct": 0.25, "guardianMarketContextBlockTpExtension": True},
        )

        self.assertTrue(out["active"])
        self.assertTrue(out["changedSl"])
        self.assertGreater(out["tightenedSl"], 1998.0)
        self.assertTrue(out["blockTpExtension"])

    def test_marketcontext_watcher_uses_forex_targets_and_applies_risk_tune(self):
        def fake_provider(calls, cfg):
            self.assertTrue(any(call["name"] == "multi_timeframe_analysis" for call in calls))
            self.assertEqual(calls[0]["arguments"]["exchange"], "OANDA")
            return [
                {
                    **call,
                    "ok": True,
                    "data": {"alignment": {"status": "LEAN BEARISH", "confidence": "High"}}
                    if call["name"] == "multi_timeframe_analysis"
                    else {"overall_assessment": {"bullish_signals": 0, "bearish_signals": 1}},
                }
                for call in calls
            ]

        main_forex._STATE["agentState"] = main_forex.new_agent_state()
        main_forex._STATE["config"] = {
            "marketContextWatcherEnabled": True,
            "marketContextWatcherMaxSymbols": 2,
            "marketContextWatcherExchange": "OANDA",
            "demoAutoMinConfidence": 0.7,
            "demoQualityMinConfidence": 0.7,
            "guardianMaxLossPerPositionUsd": 0.85,
            "guardianProfitTrailMinPnlUsd": 0.45,
        }
        main_forex._STATE["openLivePositions"] = [
            {"symbol": "EURUSD", "type": 0, "volume": 0.01, "profit": -0.1}
        ]
        main_forex._STATE["lastAutoSignal"] = None
        main_forex._STATE["lastSymbolOpportunity"] = None
        main_forex._STATE["lastMultiSymbolScan"] = {}

        out = main_forex._marketcontext_signal_watch_once(fake_provider)

        self.assertTrue(out["ok"])
        self.assertTrue(out["submitted"])
        self.assertEqual(out["findings"][0]["condition"], "strong_mtf_contradiction")
        self.assertEqual(main_forex._STATE["config"]["demoAutoMinConfidence"], 0.78)
        self.assertEqual(main_forex._STATE["config"]["guardianMaxLossPerPositionUsd"], 0.55)

    def test_supervisor_reports_cmux_and_pauses_entries_when_marketcontext_stale(self):
        now = int(time.time())
        cfg = {
            "executionMode": "DEMO",
            "marketContextWatcherEnabled": True,
            "marketContextWatcherIntervalSec": 60,
            "supervisorMarketContextWatcherMaxAgeSec": 120,
            "supervisorMarketContextWatcherPauseSec": 300,
            "supervisorAgentHealthEnabled": True,
        }
        main_forex._STATE["running"] = True
        main_forex._STATE["agentState"] = main_forex.new_agent_state()
        main_forex._STATE["marketContextWatcher"] = {
            "updatedAt": now - 999,
            "ok": True,
            "targets": [{"symbol": "EURUSD", "target": "open_position"}],
        }

        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        self.assertFalse(review["ok"])
        self.assertTrue(any(item["title"] == "MarketContext MCP watcher is unhealthy" for item in review["issues"]))
        self.assertEqual(main_forex._STATE["externalSignalGuard"]["reason"], "marketcontext_mcp_watcher_stale")
        self.assertGreater(main_forex._STATE["externalSignalGuard"]["until"], now)
        self.assertEqual(main_forex._STATE["lastCmuxReport"]["action"], "pause_new_entries")
        self.assertTrue(main_forex._STATE["lastCmuxReport"]["requiresCmux"])

    def test_marketcontext_watcher_pause_guard_blocks_new_entry(self):
        now = int(time.time())
        cfg = {
            "marketScan": True,
            "executionMode": "DEMO",
            "autoDemoTrading": True,
        }
        main_forex._STATE["running"] = True
        main_forex._STATE["agentState"] = main_forex.new_agent_state()
        main_forex._STATE["externalSignalGuard"] = {
            "reason": "marketcontext_mcp_watcher_error",
            "until": now + 300,
            "active": True,
        }

        main_forex._maybe_open_autonomous_mt5_trade(cfg, "EURUSD", "DEMO", [], {"ok": True}, {"trade_mode": 0})

        self.assertEqual(main_forex._STATE["botNotice"]["code"], "marketcontext_mcp_watcher_error")
        self.assertEqual(main_forex._STATE["agentState"]["agents"]["risk_manager"]["state"], "blocked")

    def test_guardian_loss_cap_closes_even_when_signal_is_weak(self):
        positions = [
            {"ticket": 1, "type": 0, "profit": -0.92, "volume": 0.01, "symbol": "EURUSD"},
            {"ticket": 2, "type": 1, "profit": -0.22, "volume": 0.01, "symbol": "EURUSD"},
        ]
        weak_signal = {"ok": False, "side": "BUY", "confidence": 0.4, "momentumPoints": 1}
        calls = []

        class FakeBroker:
            def close_position(self, position, comment=""):
                calls.append({"ticket": position.get("ticket"), "comment": comment})
                return {"ok": True}

            def shutdown(self):
                return None

        cfg = {
            "guardianEarlyExitEnabled": True,
            "guardianMaxLossPerPositionUsd": 0.75,
            "guardianLossCapAlwaysOn": True,
            "guardianEarlyExitMinConfidence": 0.8,
            "guardianEarlyExitMinMomentumPoints": 18,
        }
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=weak_signal):
            with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
                out = main_forex._guardian_close_wrong_way_positions(cfg, "EURUSD", "DEMO", positions, {"ok": True})

        self.assertTrue(out["ok"])
        self.assertEqual(out["closed"], 1)
        self.assertEqual(calls[0]["ticket"], 1)
        self.assertTrue(out["monitored"][0]["lossCapHit"])
        self.assertEqual(out["monitored"][0]["reason"], "loss_cap_before_sl")
        self.assertFalse(out["monitored"][1]["lossCapHit"])

    def test_guardian_soft_loss_cap_closes_during_loss_asymmetry_refactor(self):
        positions = [
            {"ticket": 3, "type": 0, "profit": -0.24, "volume": 0.01, "symbol": "EURUSD"},
        ]
        weak_signal = {"ok": False, "side": "BUY", "confidence": 0.4, "momentumPoints": 1}
        calls = []

        class FakeBroker:
            def close_position(self, position, comment=""):
                calls.append({"ticket": position.get("ticket"), "comment": comment})
                return {"ok": True}

            def shutdown(self):
                return None

        cfg = {
            "guardianEarlyExitEnabled": True,
            "guardianMaxLossPerPositionUsd": 0.38,
            "guardianSoftLossCapUsd": 0.22,
            "guardianLossCapAlwaysOn": True,
            "supervisorLossAsymmetryRefactorActive": True,
        }
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=weak_signal):
            with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
                out = main_forex._guardian_close_wrong_way_positions(cfg, "EURUSD", "DEMO", positions, {"ok": True})

        self.assertTrue(out["ok"])
        self.assertEqual(out["closed"], 1)
        self.assertEqual(calls[0]["ticket"], 3)
        self.assertTrue(out["monitored"][0]["lossCapHit"])
        self.assertEqual(out["monitored"][0]["effectiveLossCapUsd"], 0.22)

    def test_guardian_closes_wrong_way_position_on_strong_opposite_signal(self):
        positions = [
            {"ticket": 1, "type": 0, "profit": 0.35, "volume": 0.01, "symbol": "GBPUSD"},
            {"ticket": 2, "type": 1, "profit": 0.22, "volume": 0.01, "symbol": "GBPUSD"},
        ]
        strong_sell_signal = {"ok": True, "side": "SELL", "confidence": 0.86, "momentumPoints": 14}
        calls = []

        class FakeBroker:
            def close_position(self, position, comment=""):
                calls.append({"ticket": position.get("ticket"), "comment": comment})
                return {"ok": True}

            def shutdown(self):
                return None

        cfg = {
            "guardianEarlyExitEnabled": True,
            "guardianEarlyExitMinConfidence": 0.76,
            "guardianEarlyExitMinMomentumPoints": 12,
            "guardianEarlyExitMaxPnlUsd": -0.25,
            "guardianStrongOppositeExitEnabled": True,
            "guardianStrongOppositeExitMaxLossUsd": 0.75,
            "guardianMaxLossPerPositionUsd": 0.85,
        }
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=strong_sell_signal):
            with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
                out = main_forex._guardian_close_wrong_way_positions(cfg, "GBPUSD", "DEMO", positions, {"ok": True})

        self.assertTrue(out["ok"])
        self.assertEqual(out["closed"], 1)
        self.assertEqual(calls[0]["ticket"], 1)
        self.assertEqual(out["monitoredCount"], 2)
        self.assertTrue(out["monitored"][0]["strongOppositeExit"])
        self.assertEqual(out["monitored"][0]["reason"], "strong_opposite_signal_exit")
        self.assertFalse(out["monitored"][1]["wrongWay"])

    def test_guardian_strong_opposite_respects_max_loss_before_loss_cap(self):
        positions = [
            {"ticket": 1, "type": 0, "profit": -0.72, "volume": 0.01, "symbol": "GBPUSD"},
        ]
        strong_sell_signal = {"ok": True, "side": "SELL", "confidence": 0.86, "momentumPoints": 14}

        class FakeBroker:
            def __init__(self):
                raise AssertionError("Guardian should wait for loss cap when loss is beyond strong-opposite guard")

        cfg = {
            "guardianEarlyExitEnabled": True,
            "guardianEarlyExitMinConfidence": 0.76,
            "guardianEarlyExitMinMomentumPoints": 12,
            "guardianEarlyExitMaxPnlUsd": -0.9,
            "guardianStrongOppositeExitEnabled": True,
            "guardianStrongOppositeExitMaxLossUsd": 0.5,
            "guardianMaxLossPerPositionUsd": 0.85,
            "guardianLossCapAlwaysOn": True,
        }
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=strong_sell_signal):
            with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
                out = main_forex._guardian_close_wrong_way_positions(cfg, "GBPUSD", "DEMO", positions, {"ok": True})

        self.assertEqual(out["closed"], 0)
        self.assertFalse(out["monitored"][0]["strongOppositeExit"])
        self.assertFalse(out["monitored"][0]["lossCapHit"])
        self.assertEqual(out["monitored"][0]["reason"], "same_side_or_signal_not_strong_enough")

    def test_guardian_rebound_hold_avoids_closing_small_red_range_position(self):
        positions = [
            {"ticket": 1, "type": 0, "profit": -0.18, "volume": 0.01, "symbol": "GBPUSD"},
        ]
        strong_sell_signal = {
            "ok": True,
            "side": "SELL",
            "confidence": 0.86,
            "momentumPoints": 14,
            "regime": "range",
            "scoreGap": 2.0,
            "breakout": False,
        }

        class FakeBroker:
            def __init__(self):
                raise AssertionError("Guardian should wait for a positive rebound instead of closing small red range chop")

        cfg = {
            "guardianEarlyExitEnabled": True,
            "guardianEarlyExitMinConfidence": 0.76,
            "guardianEarlyExitMinMomentumPoints": 12,
            "guardianEarlyExitMaxPnlUsd": -0.1,
            "guardianStrongOppositeExitEnabled": True,
            "guardianStrongOppositeExitMaxLossUsd": 0.75,
            "guardianMaxLossPerPositionUsd": 0.85,
            "guardianLossCapAlwaysOn": True,
            "guardianReboundHoldEnabled": True,
            "guardianReboundHoldMaxLossUsd": 0.35,
            "guardianReboundHoldMaxScoreGap": 4.0,
        }
        market_snapshot = {
            "candles": [{"high": 1.2702, "low": 1.2700} for _ in range(12)],
        }
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=strong_sell_signal):
            with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
                out = main_forex._guardian_close_wrong_way_positions(cfg, "GBPUSD", "DEMO", positions, market_snapshot)

        self.assertEqual(out["closed"], 0)
        self.assertEqual(out["monitoredCount"], 1)
        self.assertTrue(out["monitored"][0]["reboundHoldActive"])
        self.assertFalse(out["monitored"][0]["eligibleToClose"])
        self.assertEqual(out["monitored"][0]["reason"], "rebound_hold_wait_for_positive")

    def test_guardian_rebound_hold_does_not_block_breakout_exit(self):
        positions = [
            {"ticket": 1, "type": 0, "profit": -0.18, "volume": 0.01, "symbol": "GBPUSD"},
        ]
        breakout_sell_signal = {
            "ok": True,
            "side": "SELL",
            "confidence": 0.86,
            "momentumPoints": 14,
            "regime": "breakout",
            "scoreGap": 2.0,
            "breakout": True,
        }
        calls = []

        class FakeBroker:
            def close_position(self, position, comment=""):
                calls.append({"ticket": position.get("ticket"), "comment": comment})
                return {"ok": True}

            def shutdown(self):
                return None

        cfg = {
            "guardianEarlyExitEnabled": True,
            "guardianEarlyExitMinConfidence": 0.76,
            "guardianEarlyExitMinMomentumPoints": 12,
            "guardianEarlyExitMaxPnlUsd": -0.1,
            "guardianStrongOppositeExitEnabled": True,
            "guardianStrongOppositeExitMaxLossUsd": 0.75,
            "guardianMaxLossPerPositionUsd": 0.85,
            "guardianLossCapAlwaysOn": True,
            "guardianReboundHoldEnabled": True,
            "guardianReboundHoldMaxLossUsd": 0.35,
            "guardianReboundHoldMaxScoreGap": 4.0,
        }
        market_snapshot = {
            "candles": [{"high": 1.2702, "low": 1.2700} for _ in range(12)],
        }
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=breakout_sell_signal):
            with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
                out = main_forex._guardian_close_wrong_way_positions(cfg, "GBPUSD", "DEMO", positions, market_snapshot)

        self.assertTrue(out["ok"])
        self.assertEqual(out["closed"], 1)
        self.assertEqual(calls[0]["ticket"], 1)
        self.assertFalse(out["monitored"][0]["reboundHoldActive"])
        self.assertTrue(out["monitored"][0]["strongOppositeExit"])
        self.assertEqual(out["monitored"][0]["reason"], "strong_opposite_signal_exit")

    def test_guardian_trail_reports_all_positions_even_when_signal_weak(self):
        positions = [
            {"ticket": 1, "type": 0, "profit": 1.2, "volume": 0.01, "symbol": "XAUUSD"},
            {"ticket": 2, "type": 1, "profit": -0.4, "volume": 0.01, "symbol": "XAUUSD"},
        ]
        weak_signal = {"ok": False, "side": "BUY", "confidence": 0.6, "momentumPoints": 4}
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=weak_signal):
            out = main_forex._guardian_extend_winning_positions(
                {"guardianProfitTrailEnabled": True},
                "XAUUSD",
                "DEMO",
                positions,
                {"ok": True},
            )

        self.assertEqual(out["modified"], 0)
        self.assertEqual(out["noops"], 2)
        self.assertEqual(out["intentionalSkips"], 2)
        self.assertEqual(out["monitoredCount"], 2)
        self.assertEqual([x["ticket"] for x in out["monitored"]], [1, 2])

    def test_guardian_profit_fallback_trails_when_signal_is_valid_but_below_thresholds(self):
        positions = [
            {
                "ticket": 1,
                "type": 0,
                "profit": 1.2,
                "volume": 0.01,
                "symbol": "GBPUSD",
                "price_open": 1.345,
                "price_current": 1.347,
                "sl": 1.344,
                "tp": 1.348,
            },
        ]
        weak_but_valid_signal = {"ok": True, "side": "SELL", "confidence": 0.7, "momentumPoints": 3}
        calls = []

        class FakeBroker:
            def modify_position_sltp(self, position, stop_loss=None, take_profit=None):
                calls.append({"position": position, "sl": stop_loss, "tp": take_profit})
                return {"ok": True, "noop": False}

            def shutdown(self):
                return None

        cfg = {
            "guardianProfitTrailEnabled": True,
            "guardianProfitTrailFallbackEnabled": True,
            "guardianProfitTrailMinConfidence": 0.8,
            "guardianProfitTrailMinMomentumPoints": 10,
            "guardianProfitTrailMinPnlUsd": 0.5,
            "guardianProfitTpExtendPips": 8,
            "guardianProfitTrailSlPips": 10,
            "guardianProfitLockPips": 2,
            "pipSize": 0.0001,
        }
        market = {
            "ok": True,
            "tick": {"bid": 1.347, "ask": 1.3471},
            "symbolInfo": {"digits": 5, "point": 0.00001, "trade_stops_level": 0, "trade_freeze_level": 0},
        }
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=weak_but_valid_signal):
            with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
                out = main_forex._guardian_extend_winning_positions(cfg, "GBPUSD", "DEMO", positions, market)

        self.assertTrue(out["ok"])
        self.assertEqual(out["modified"], 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(out["monitored"][0]["reason"], "profit_protect_fallback")
        self.assertTrue(out["monitored"][0]["fallbackTrail"])

    def test_guardian_trail_skips_broker_modify_when_levels_are_unchanged(self):
        positions = [
            {
                "ticket": 1,
                "type": 0,
                "profit": 1.2,
                "volume": 0.01,
                "symbol": "GBPUSD",
                "price_open": 1.345,
                "price_current": 1.34795,
                "sl": 1.346,
                "tp": 1.348,
            },
        ]
        strong_signal = {"ok": True, "side": "BUY", "confidence": 0.9, "momentumPoints": 20}
        main_forex._STATE["botNotice"] = {"code": "guardian_trail_failed"}

        class FakeBroker:
            def __init__(self):
                raise AssertionError("Guardian should not call MT5 when rounded TP/SL levels are unchanged")

        cfg = {
            "guardianProfitTrailEnabled": True,
            "guardianProfitTrailMinConfidence": 0.8,
            "guardianProfitTrailMinMomentumPoints": 10,
            "guardianProfitTrailMinPnlUsd": 0.5,
            "guardianProfitTpExtendPips": 0.1,
            "guardianProfitTrailSlPips": 20,
            "guardianProfitLockPips": 2,
            "pipSize": 0.0001,
        }
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=strong_signal):
            with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
                out = main_forex._guardian_extend_winning_positions(cfg, "GBPUSD", "DEMO", positions, {"ok": True})

        self.assertTrue(out["ok"])
        self.assertEqual(out["modified"], 0)
        self.assertEqual(out["monitored"][0]["reason"], "trail_no_change")
        self.assertEqual(main_forex._STATE["botNotice"]["code"], "guardian_profit_trail_noop")

    def test_guardian_trail_skips_modify_when_stops_are_too_close(self):
        positions = [
            {
                "ticket": 1,
                "type": 1,
                "profit": 0.8,
                "volume": 0.01,
                "symbol": "GBPUSD",
                "price_open": 1.3475,
                "price_current": 1.3468,
                "sl": 1.348,
                "tp": 1.346,
            },
        ]
        strong_signal = {"ok": True, "side": "SELL", "confidence": 0.9, "momentumPoints": 20}

        class FakeBroker:
            def __init__(self):
                raise AssertionError("Guardian should not call MT5 when broker stop distance would reject the levels")

        cfg = {
            "guardianProfitTrailEnabled": True,
            "guardianProfitTrailMinConfidence": 0.8,
            "guardianProfitTrailMinMomentumPoints": 10,
            "guardianProfitTrailMinPnlUsd": 0.3,
            "guardianProfitTpExtendPips": 18,
            "guardianProfitTrailSlPips": 10,
            "guardianProfitLockPips": 2,
            "pipSize": 0.0001,
        }
        market = {
            "ok": True,
            "tick": {"bid": 1.3468, "ask": 1.3469},
            "symbolInfo": {"digits": 5, "point": 0.00001, "trade_stops_level": 50, "trade_freeze_level": 0},
        }
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=strong_signal):
            with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
                out = main_forex._guardian_extend_winning_positions(cfg, "GBPUSD", "DEMO", positions, market)

        self.assertTrue(out["ok"])
        self.assertEqual(out["modified"], 0)
        self.assertEqual(out["monitored"][0]["reason"], "broker_stop_distance")

    def test_guardian_profit_giveback_closes_after_peak_pullback(self):
        position = {"ticket": 1, "type": 0, "profit": 3.5, "volume": 0.01, "symbol": "XAUUSD"}
        cfg = {
            "guardianProfitGivebackEnabled": True,
            "guardianProfitGivebackTriggerUsd": 2.5,
            "guardianProfitGivebackPct": 0.45,
            "guardianProfitGivebackMinLockUsd": 1.2,
            "guardianAdaptiveTakeProfitEnabled": False,
        }
        first = main_forex._guardian_close_profit_giveback_positions(
            cfg,
            "XAUUSD",
            "DEMO",
            [position],
        )
        self.assertEqual(first["closed"], 0)
        position["profit"] = 1.5

        class FakeBroker:
            def close_position(self, position, comment=""):
                return {"ok": True, "comment": comment}

            def shutdown(self):
                return None

        with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
            second = main_forex._guardian_close_profit_giveback_positions(
                cfg,
                "XAUUSD",
                "DEMO",
                [position],
            )

        self.assertEqual(second["closed"], 1)
        self.assertEqual(second["monitored"][0]["peakPnl"], 3.5)

    def test_guardian_profit_lock_closes_small_winner_before_negative(self):
        position = {"ticket": 101, "type": 1, "profit": 0.42, "volume": 0.01, "symbol": "USDJPY"}
        cfg = {
            "guardianProfitGivebackEnabled": True,
            "guardianProfitGivebackTriggerUsd": 2.5,
            "guardianProfitGivebackPct": 0.55,
            "guardianProfitGivebackMinLockUsd": 1.0,
            "guardianAdaptiveTakeProfitEnabled": False,
            "profitLockTriggerUsdt": 0.35,
            "profitLockKeepUsdt": 0.15,
            "profitLockMaxGivebackUsdt": 0.22,
            "guardianBreakevenFloorUsd": 0.08,
            "guardianBreakevenTriggerUsd": 0.16,
        }
        first = main_forex._guardian_close_profit_giveback_positions(cfg, "USDJPY", "DEMO", [position])
        self.assertEqual(first["closed"], 0)
        self.assertEqual(first["monitored"][0]["profitLock"]["lockUsd"], 0.2)
        position["profit"] = 0.14

        class FakeBroker:
            def close_position(self, position, comment=""):
                return {"ok": True, "comment": comment}

            def shutdown(self):
                return None

        with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
            second = main_forex._guardian_close_profit_giveback_positions(cfg, "USDJPY", "DEMO", [position])

        self.assertEqual(second["closed"], 1)
        self.assertEqual(second["monitored"][0]["reason"], "profit_lock_retrace_close")
        self.assertEqual(second["monitored"][0]["peakPnl"], 0.42)

    def test_guardian_profit_peak_persists_for_positions_without_ticket(self):
        main_forex._STATE["guardianProfitPeaks"] = {}
        position = {"type": 0, "time": 123456, "profit": 0.16, "volume": 0.01, "symbol": "XAUUSD", "price_open": 2310.0}
        cfg = {
            "guardianProfitGivebackEnabled": True,
            "guardianProfitGivebackTriggerUsd": 2.5,
            "guardianProfitGivebackPct": 0.45,
            "guardianProfitGivebackMinLockUsd": 1.2,
            "guardianAdaptiveTakeProfitEnabled": False,
        }

        first = main_forex._guardian_close_profit_giveback_positions(cfg, "XAUUSD", "DEMO", [position])
        position["profit"] = -0.415
        second = main_forex._guardian_close_profit_giveback_positions(cfg, "XAUUSD", "DEMO", [position])

        self.assertEqual(first["monitored"][0]["peakPnl"], 0.16)
        self.assertEqual(second["monitored"][0]["pnl"], -0.41)
        self.assertEqual(second["monitored"][0]["peakPnl"], 0.16)
        self.assertTrue(second["monitored"][0]["guardianKey"])

    def test_open_positions_are_hydrated_with_guardian_peak(self):
        position = {"ticket": 42, "type": 0, "profit": 0.16, "volume": 0.01, "symbol": "XAUUSD"}
        key = main_forex._position_ticket(position)
        main_forex._STATE["guardianProfitPeaks"] = {key: 0.41}

        hydrated = main_forex._hydrate_guardian_profit_peaks([{**position, "profit": -0.12}])

        self.assertEqual(hydrated[0]["guardianKey"], key)
        self.assertEqual(hydrated[0]["peakUnrealizedPnl"], 0.41)
        self.assertEqual(hydrated[0]["profit"], -0.12)

    def test_guardian_adaptive_take_profit_closes_near_learned_average_peak(self):
        main_forex._STATE["guardianProfitPeakSamples"] = [
            {"peakPnl": 3.0},
            {"peakPnl": 3.2},
            {"peakPnl": 2.8},
        ]
        position = {"ticket": 7, "type": 0, "profit": 2.75, "volume": 0.01, "symbol": "XAUUSD"}

        class FakeBroker:
            def close_position(self, position, comment=""):
                return {"ok": True, "comment": comment}

            def shutdown(self):
                return None

        cfg = {
            "guardianProfitGivebackEnabled": True,
            "guardianAdaptiveTakeProfitEnabled": True,
            "guardianAdaptiveTakeProfitCapturePct": 0.9,
            "guardianAdaptiveTakeProfitMinSamples": 3,
            "guardianAdaptiveTakeProfitDefaultUsd": 2.8,
        }
        with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
            out = main_forex._guardian_close_profit_giveback_positions(cfg, "XAUUSD", "DEMO", [position])

        self.assertEqual(out["closed"], 1)
        self.assertEqual(out["adaptiveTarget"]["avgPeakPnl"], 3.0)
        self.assertEqual(out["adaptiveTarget"]["targetUsd"], 2.7)
        self.assertEqual(out["monitored"][0]["reason"], "adaptive_take_profit_close")

    def test_guardian_breakeven_guard_closes_before_profit_disappears(self):
        position = {"ticket": 9, "type": 0, "profit": 1.4, "volume": 0.01, "symbol": "XAUUSD"}
        cfg = {
            "guardianProfitGivebackEnabled": True,
            "guardianProfitGivebackTriggerUsd": 2.5,
            "guardianProfitGivebackPct": 0.45,
            "guardianProfitGivebackMinLockUsd": 1.2,
            "guardianAdaptiveTakeProfitEnabled": False,
            "guardianMinPositiveCloseUsd": 0.85,
            "guardianBreakevenFloorUsd": 0.85,
            "guardianBreakevenTriggerUsd": 1.0,
        }
        main_forex._guardian_close_profit_giveback_positions(cfg, "XAUUSD", "DEMO", [position])
        position["profit"] = 0.8

        class FakeBroker:
            def close_position(self, position, comment=""):
                return {"ok": True, "comment": comment}

            def shutdown(self):
                return None

        with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
            out = main_forex._guardian_close_profit_giveback_positions(cfg, "XAUUSD", "DEMO", [position])

        self.assertEqual(out["closed"], 1)
        self.assertEqual(out["monitored"][0]["reason"], "breakeven_guard_close")

    def test_guardian_closes_calm_market_profit_above_fee_floor(self):
        position = {"ticket": 11, "type": 0, "profit": 0.32, "volume": 0.01, "symbol": "USDJPY"}
        weak_follow = {"ok": False, "side": "SELL", "confidence": 0.52, "momentumPoints": 0.0}

        class FakeBroker:
            def close_position(self, position, comment=""):
                return {"ok": True, "comment": comment}

            def shutdown(self):
                return None

        cfg = {
            "guardianProfitGivebackEnabled": True,
            "guardianAdaptiveTakeProfitEnabled": True,
            "guardianAdaptiveTakeProfitDefaultUsd": 0.85,
            "guardianCalmProfitCloseEnabled": True,
            "guardianCalmProfitCloseMinUsd": 0.25,
            "guardianCalmProfitFeeMultiple": 3.0,
            "estimatedRoundTurnCommissionUsd": 0.08,
            "baseLot": 0.01,
            "pipSize": 0.01,
            "symbolMultiplierProfiles": {
                "FOREX_JPY": {"volatilityThrottlePoints": 16, "volatilityBoostBelowPoints": 4}
            },
        }
        market = {"candles": [{"high": 160.01, "low": 160.00} for _ in range(12)]}
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=weak_follow):
            with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
                out = main_forex._guardian_close_profit_giveback_positions(cfg, "USDJPY", "DEMO", [position], market)

        self.assertEqual(out["closed"], 1)
        self.assertEqual(out["monitored"][0]["reason"], "calm_fee_profit_close")
        self.assertEqual(out["monitored"][0]["calmProfitClose"]["thresholdUsd"], 0.25)

    def test_guardian_closes_stale_calm_near_flat_position(self):
        now = int(time.time())
        position = {"ticket": 13, "type": 0, "profit": -0.09, "volume": 0.01, "symbol": "GBPUSD", "time": now - 500}
        calls = []

        class FakeBroker:
            def close_position(self, position, comment=""):
                calls.append({"ticket": position.get("ticket"), "comment": comment})
                return {"ok": True, "comment": comment}

            def shutdown(self):
                return None

        cfg = {
            "guardianProfitGivebackEnabled": True,
            "guardianAdaptiveTakeProfitEnabled": True,
            "guardianCalmProfitCloseEnabled": True,
            "guardianCalmStaleExitEnabled": True,
            "guardianCalmStaleExitMaxAgeSec": 240,
            "guardianCalmStaleExitMaxAbsPnlUsd": 0.7,
            "guardianCalmStaleExitMinPnlUsd": -0.35,
            "pipSize": 0.0001,
            "symbolMultiplierProfiles": {
                "FOREX_MAJOR": {"volatilityThrottlePoints": 12, "volatilityBoostBelowPoints": 3}
            },
        }
        market = {"candles": [{"high": 1.2702, "low": 1.2700} for _ in range(12)]}
        with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
            out = main_forex._guardian_close_profit_giveback_positions(cfg, "GBPUSD", "DEMO", [position], market)

        self.assertEqual(out["closed"], 1)
        self.assertEqual(calls[0]["ticket"], 13)
        self.assertEqual(out["monitored"][0]["reason"], "calm_stale_near_flat_close")
        self.assertTrue(out["monitored"][0]["calmStaleExit"]["ok"])

    def test_guardian_does_not_calm_close_when_follow_signal_is_strong(self):
        position = {"ticket": 12, "type": 0, "profit": 0.32, "volume": 0.01, "symbol": "USDJPY"}
        strong_follow = {"ok": True, "side": "BUY", "confidence": 0.88, "momentumPoints": 14}

        class FakeBroker:
            def __init__(self):
                raise AssertionError("Guardian should extend/hold target instead of calm-closing a strong follow")

        cfg = {
            "guardianProfitGivebackEnabled": True,
            "guardianAdaptiveTakeProfitEnabled": True,
            "guardianAdaptiveTakeProfitDefaultUsd": 0.85,
            "guardianCalmProfitCloseEnabled": True,
            "guardianCalmProfitCloseMinUsd": 0.25,
            "guardianCalmProfitFeeMultiple": 3.0,
            "estimatedRoundTurnCommissionUsd": 0.08,
            "baseLot": 0.01,
            "pipSize": 0.01,
            "guardianStrongFollowTargetExtensionEnabled": True,
            "guardianStrongFollowMinConfidence": 0.82,
            "guardianStrongFollowMinMomentumPoints": 12,
            "symbolMultiplierProfiles": {
                "FOREX_JPY": {"volatilityThrottlePoints": 16, "volatilityBoostBelowPoints": 4}
            },
        }
        market = {"candles": [{"high": 160.01, "low": 160.00} for _ in range(12)]}
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=strong_follow):
            with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
                out = main_forex._guardian_close_profit_giveback_positions(cfg, "USDJPY", "DEMO", [position], market)

        self.assertEqual(out["closed"], 0)
        self.assertTrue(out["monitored"][0]["strongFollow"]["ok"])
        self.assertNotEqual(out["monitored"][0]["reason"], "calm_fee_profit_close")

    def test_guardian_extends_adaptive_target_when_signal_stays_strong(self):
        main_forex._STATE["guardianProfitPeakSamples"] = [
            {"peakPnl": 3.0},
            {"peakPnl": 3.2},
            {"peakPnl": 2.8},
        ]
        position = {"ticket": 10, "type": 0, "profit": 2.75, "volume": 0.01, "symbol": "XAUUSD"}
        strong_signal = {"ok": True, "side": "BUY", "confidence": 0.88, "momentumPoints": 20, "breakout": False}

        class FakeBroker:
            def __init__(self):
                raise AssertionError("Guardian should not close when Binance-style strong follow extends target")

        cfg = {
            "guardianProfitGivebackEnabled": True,
            "guardianAdaptiveTakeProfitEnabled": True,
            "guardianAdaptiveTakeProfitCapturePct": 0.9,
            "guardianAdaptiveTakeProfitMinSamples": 3,
            "guardianAdaptiveTakeProfitDefaultUsd": 2.8,
            "guardianStrongFollowTargetExtensionEnabled": True,
            "guardianStrongFollowMinConfidence": 0.82,
            "guardianStrongFollowMinMomentumPoints": 12,
            "guardianStrongFollowTargetExtensionFactor": 1.35,
        }
        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=strong_signal):
            with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
                out = main_forex._guardian_close_profit_giveback_positions(cfg, "XAUUSD", "DEMO", [position], {"ok": True})

        self.assertEqual(out["closed"], 0)
        self.assertEqual(out["monitored"][0]["reason"], "target_extended_on_strong_follow")
        self.assertGreater(out["monitored"][0]["targetPnl"], out["monitored"][0]["baseTargetPnl"])

    def test_learning_proposal_tightens_weak_performance(self):
        stats = {"trades": 20, "winRatePct": 30.0, "realizedPnl": -5.0, "profitFactor": 0.5, "lossStreak": 2}

        proposal, reasons = main_forex._learning_proposal_from_stats(stats)

        self.assertEqual(proposal["demoAutoMinConfidence"], 0.82)
        self.assertTrue(proposal["demoBlockRangeEntries"])
        self.assertEqual(proposal["maxOpenOrders"], 1)
        self.assertTrue(any("Tighten" in reason for reason in reasons))

    def test_mt5_account_from_snapshot_extracts_account_info_for_demo_guard(self):
        wrapped = {"account": {"ok": True, "account": {"trade_mode": 0, "login": 123}, "openLivePositions": []}}
        flat = {"account": {"trade_mode": 0, "login": 123}}

        self.assertTrue(main_forex._is_demo_account(main_forex._mt5_account_from_snapshot(wrapped)))
        self.assertTrue(main_forex._is_demo_account(main_forex._mt5_account_from_snapshot(flat)))

    def test_learning_proposal_mismatches_ignore_stricter_current_values(self):
        cfg = {
            "demoHigherTimeframeConfirm": True,
            "demoHigherTimeframeMinConfirmations": 2,
            "demoHigherTimeframeMinConfidence": 0.72,
            "demoClearSignalOnly": True,
            "maxOpenOrders": 5,
            "maxOpenPositions": 5,
            "maxExposureLots": 0.05,
            "demoLossCooldownAfter": 2,
            "demoLossCooldownSec": 600,
            "demoMaxLossStreakStop": 4,
            "demoAutoMinConfidence": 0.72,
        }
        proposal = {
            "demoHigherTimeframeConfirm": True,
            "demoHigherTimeframeMinConfirmations": 1,
            "demoHigherTimeframeMinConfidence": 0.72,
            "maxOpenOrders": 1,
            "maxOpenPositions": 1,
            "maxExposureLots": 0.01,
            "demoLossCooldownAfter": 3,
            "demoLossCooldownSec": 420,
            "demoMaxLossStreakStop": 6,
            "demoAutoMinConfidence": 0.82,
        }

        mismatches = main_forex._learning_proposal_mismatches(cfg, proposal)

        self.assertNotIn("demoHigherTimeframeMinConfirmations", mismatches)
        self.assertNotIn("demoLossCooldownAfter", mismatches)
        self.assertNotIn("demoLossCooldownSec", mismatches)
        self.assertNotIn("demoMaxLossStreakStop", mismatches)
        self.assertNotIn("maxOpenOrders", mismatches)
        self.assertNotIn("maxOpenPositions", mismatches)
        self.assertNotIn("maxExposureLots", mismatches)
        self.assertIn("demoAutoMinConfidence", mismatches)

    def test_autotrade_start_syncs_closed_deals_before_learning(self):
        def sync_closed_deals(symbol, mode):
            main_forex._STATE["tradeReports"] = [
                {"symbol": symbol, "mode": mode, "ts": i, "pnl": -1.0, "exit": 4500.0, "reason": "mt5_closed_deal"}
                for i in range(12)
            ]
            return {"ok": True, "synced": 12}

        with mock.patch.object(main_forex, "_sync_mt5_closed_deals", side_effect=sync_closed_deals):
            with mock.patch.object(main_forex, "_safe_mt5_snapshot", return_value={"precheck": {"ok": True}, "account": {"openLivePositions": []}}):
                out = main_forex.autotrade_start(main_forex.AutotradePayload(symbol="XAUUSD", executionMode="DEMO", autoLearn=True))

        self.assertTrue(out["config"]["demoHigherTimeframeConfirm"])
        self.assertEqual(out["config"]["demoHigherTimeframes"], ["H1", "M15"])
        self.assertTrue(out["config"]["professionalStrategyEnabled"])
        self.assertEqual(out["config"]["professionalPlaybook"], "mtf_pullback_volume")
        self.assertLessEqual(out["config"]["demoAutoMinConfidence"], 0.55)
        self.assertEqual(out["config"]["portfolioTargetOpenOrders"], 3)
        self.assertEqual(out["config"]["maxOpenOrders"], 5)
        self.assertEqual(out["config"]["maxOpenPositions"], 5)
        self.assertGreaterEqual(out["config"]["maxExposureLots"], out["config"]["maxOpenOrders"] * out["config"]["maxLot"])
        self.assertIn("maxOpenOrders", main_forex._STATE["learningDecision"]["portfolioKeysHeld"])
        self.assertIn("demoReversalCooldownSec", main_forex._STATE["learningDecision"]["portfolioKeysHeld"])
        self.assertIn("demoAutoMinConfidence", main_forex._STATE["learningDecision"]["portfolioKeysHeld"])
        self.assertTrue(main_forex._STATE["learningDecision"]["applied"])
        self.assertEqual(main_forex._STATE["learningDecision"]["sync"]["synced"], 12)

    def test_higher_timeframe_confirmation_requires_matching_side(self):
        cfg = {
            "demoHigherTimeframeConfirm": True,
            "demoHigherTimeframes": ["M5", "M15"],
            "demoHigherTimeframeMinConfirmations": 2,
            "demoHigherTimeframeMinConfidence": 0.7,
        }
        signals = [
            {"ok": True, "side": "BUY", "confidence": 0.8},
            {"ok": True, "side": "SELL", "confidence": 0.9},
        ]
        with mock.patch.object(main_forex, "_safe_mt5_market_snapshot", return_value={"ok": True}):
            with mock.patch.object(main_forex, "_autonomous_signal_from_market", side_effect=signals):
                out = main_forex._higher_timeframe_confirmation(cfg, "XAUUSD", "BUY", "DEMO")

        self.assertFalse(out["ok"])
        self.assertEqual(out["confirmations"], 1)
        self.assertEqual(out["required"], 2)

    def test_professional_strategy_guard_requires_all_bias_frames(self):
        cfg = {
            "professionalStrategyEnabled": True,
            "professionalBiasTimeframes": ["M15", "M5"],
            "professionalMinConfirmations": 2,
            "professionalMinTfConfidence": 0.74,
            "professionalMinTfMomentumPoints": 8,
            "professionalMaxSpreadPoints": 30,
            "pipSize": 0.1,
        }
        signals = [
            {"ok": True, "side": "BUY", "confidence": 0.8, "momentumPoints": 12, "regime": "trend", "breakout": False},
            {"ok": True, "side": "SELL", "confidence": 0.86, "momentumPoints": 18, "regime": "trend", "breakout": False},
        ]
        snapshot = {"tick": {"bid": 4500.0, "ask": 4500.09}}
        with mock.patch.object(main_forex, "_safe_mt5_market_snapshot", return_value=snapshot):
            with mock.patch.object(main_forex, "_autonomous_signal_from_market", side_effect=signals):
                out = main_forex._professional_strategy_guard(cfg, "XAUUSD", "BUY", "DEMO")

        self.assertFalse(out["ok"])
        self.assertEqual(out["confirmations"], 1)
        self.assertEqual(out["required"], 2)

    def test_professional_pullback_playbook_requires_h1_direction_m5_pullback_and_m1_trigger(self):
        def candles(start=4500.0, step=0.25, count=24, volume=100):
            return [
                {
                    "time": 1_700_000_000 + i * 60,
                    "open": start + i * step - 0.05,
                    "high": start + i * step + 0.18,
                    "low": start + i * step - 0.18,
                    "close": start + i * step,
                    "volume": volume,
                    "tick_volume": volume,
                    "real_volume": 0,
                    "spread": 3,
                }
                for i in range(count)
            ]

        h1 = {"candles": candles(4500.0, 0.35)}
        m15 = {"candles": candles(4502.0, 0.22)}
        m5_candles = candles(4503.0, 0.12)
        m5_candles[-3]["low"] = m5_candles[-3]["close"] - 0.8
        m5 = {"candles": m5_candles}
        m1_candles = candles(4504.0, 0.08, volume=100)
        m1_candles[-1]["tick_volume"] = 180
        m1 = {"candles": m1_candles}
        cfg = {
            "professionalStrategyEnabled": True,
            "professionalPlaybook": "mtf_pullback_volume",
            "professionalRequireVolumeSpike": True,
            "professionalVolumeSpikeMultiplier": 1.25,
            "professionalM1MinConfidence": 0.72,
            "professionalM1MinMomentumPoints": 1.0,
            "pipSize": 0.1,
        }
        with mock.patch.object(main_forex, "_safe_mt5_market_snapshot", side_effect=[h1, m15, m5, m1]) as snapshot_mock:
            out = main_forex._professional_pullback_playbook_guard(
                cfg,
                "XAUUSD",
                "BUY",
                "DEMO",
                {"side": "BUY", "confidence": 0.78, "momentumPoints": 2.5},
            )

        self.assertTrue(out["ok"])
        self.assertTrue(out["checks"]["h1Direction"])
        self.assertTrue(out["checks"]["m5Pullback"])
        self.assertTrue(out["volume"]["ok"])
        self.assertEqual(out["lookbacks"]["H1"]["bars"], 168)
        self.assertEqual(out["lookbacks"]["M15"]["bars"], 192)
        self.assertEqual(out["lookbacks"]["M5"]["bars"], 96)
        self.assertEqual(out["lookbacks"]["M1"]["bars"], 60)
        requested_bars = [call.args[1]["marketDataBars"] for call in snapshot_mock.call_args_list]
        self.assertEqual(requested_bars, [168, 192, 96, 60])
        self.assertIn("atr24hPoints", out["memory"])
        self.assertIn("liquidityZone3d", out["memory"])
        self.assertIn("marketStructure", out["memory"])
        self.assertIn("indicators", out["memory"])
        self.assertIn("spread", out["memory"])
        self.assertTrue(out["memory"]["marketStructure"]["M15"]["available"])
        self.assertTrue(out["memory"]["spread"]["M1"]["available"])

    def test_professional_context_is_delegated_to_all_sub_agents(self):
        main_forex._STATE["running"] = True
        main_forex._STATE["agentState"] = main_forex.new_agent_state()
        guard = {
            "ok": True,
            "playbook": "H1 direction + M15 setup + M5 pullback + M1 confirmation",
            "checks": {"h1Direction": True, "m15Setup": True, "m5Pullback": True, "m1Confirmation": True},
            "lookbacks": {"H1": {"hours": 168, "bars": 168}, "M15": {"hours": 48, "bars": 192}, "M5": {"hours": 8, "bars": 96}, "M1": {"hours": 1, "bars": 60}},
            "memory": {
                "atr24hPoints": 12.5,
                "averageVolume8h": 180,
                "liquidityZone3d": {"available": True, "high": 4520, "low": 4480},
                "marketStructure": {
                    "H1": {"available": True, "side": "BUY", "support": 4490, "resistance": 4520, "higherHigh": True, "higherLow": True},
                    "M15": {"available": True, "side": "BUY", "support": 4500, "resistance": 4515},
                },
                "indicators": {"M15": {"ema20": 4508, "ema50": 4501, "rsi14": 58, "atr14Points": 9, "macd": {"side": "BUY"}}},
                "spread": {"M1": {"available": True, "average": 3, "max": 4}},
                "londonSession": {"available": True, "high": 4510, "low": 4495},
                "newYorkSession": {"available": True, "high": 4518, "low": 4502},
            },
            "message": "professional pullback playbook confirmed",
        }

        packet = main_forex._delegate_professional_context_to_agents(
            {"hermesAgentDelegationEnabled": True, "portfolioTargetOpenOrders": 3, "maxOpenOrders": 5, "maxExposureLots": 0.05, "baseLot": 0.01},
            "XAUUSD",
            "BUY",
            {"confidence": 0.78, "momentumPoints": 2.5, "trendPoints": 3.0, "regime": "trend", "breakout": False},
            guard,
            {"ok": True, "probability": 0.82},
            {"ok": True, "confirmations": 2},
            {"ok": True, "code": "clear_signal_ready"},
            {"openOrders": 2, "targetOpenOrders": 3, "maxOpenOrders": 5, "maxExposureLots": 0.05},
        )

        self.assertTrue(packet["ok"])
        for agent_id in ("market_analyst", "strategy_builder", "portfolio_manager", "lot_sizing_agent", "risk_manager", "position_guardian", "memory_agent", "hermes_supervisor"):
            self.assertIn(agent_id, packet["assignments"])
            self.assertIn(agent_id, main_forex._STATE["agentState"]["agents"])
        self.assertEqual(main_forex._STATE["agentState"]["agents"]["hermes_supervisor"]["state"], "done")
        monitor = main_forex._agent_delegation_monitor({"hermesAgentDelegationEnabled": True})
        self.assertTrue(monitor["ok"])
        self.assertEqual(monitor["symbol"], "XAUUSD")

    def test_professional_pullback_playbook_blocks_counter_h1_direction(self):
        def candles(start=4500.0, step=0.25, count=24):
            return [
                {"open": start + i * step - 0.05, "high": start + i * step + 0.18, "low": start + i * step - 0.18, "close": start + i * step, "tick_volume": 100}
                for i in range(count)
            ]

        h1_down = {"candles": candles(4508.0, -0.25)}
        m15 = {"candles": candles(4502.0, 0.22)}
        m5 = {"candles": candles(4503.0, 0.12)}
        m1 = {"candles": candles(4504.0, 0.08)}
        cfg = {"professionalStrategyEnabled": True, "professionalPlaybook": "mtf_pullback_volume", "pipSize": 0.1}
        with mock.patch.object(main_forex, "_safe_mt5_market_snapshot", side_effect=[h1_down, m15, m5, m1]):
            out = main_forex._professional_pullback_playbook_guard(
                cfg,
                "XAUUSD",
                "BUY",
                "DEMO",
                {"side": "BUY", "confidence": 0.8, "momentumPoints": 3.0},
            )

        self.assertFalse(out["ok"])
        self.assertFalse(out["checks"]["h1Direction"])
        self.assertIn("h1Direction", out["message"])

    def test_professional_strategy_guard_allows_one_of_two_confirmations_when_configured(self):
        cfg = {
            "professionalStrategyEnabled": True,
            "professionalBiasTimeframes": ["M15", "M5"],
            "professionalMinConfirmations": 1,
            "professionalMinTfConfidence": 0.74,
            "professionalMinTfMomentumPoints": 6,
            "professionalMaxSpreadPoints": 30,
            "pipSize": 0.1,
        }
        signals = [
            {"ok": False, "side": "SELL", "confidence": 0.799, "momentumPoints": 87.4, "regime": "trend", "breakout": False},
            {"ok": True, "side": "BUY", "confidence": 0.9, "momentumPoints": 7.5, "regime": "trend", "breakout": False},
        ]
        snapshot = {"tick": {"bid": 4500.0, "ask": 4500.09}}
        with mock.patch.object(main_forex, "_safe_mt5_market_snapshot", return_value=snapshot):
            with mock.patch.object(main_forex, "_autonomous_signal_from_market", side_effect=signals):
                out = main_forex._professional_strategy_guard(cfg, "XAUUSD", "SELL", "DEMO")

        self.assertTrue(out["ok"])
        self.assertEqual(out["confirmations"], 1)
        self.assertEqual(out["required"], 1)

    def test_professional_strategy_guard_reports_opposite_bias_side(self):
        cfg = {
            "professionalStrategyEnabled": True,
            "professionalBiasTimeframes": ["M15", "M5"],
            "professionalMinConfirmations": 1,
            "professionalMinTfConfidence": 0.74,
            "professionalMinTfMomentumPoints": 6,
            "professionalMaxSpreadPoints": 30,
            "pipSize": 0.1,
        }
        signals = [
            {"ok": True, "side": "SELL", "confidence": 0.9, "momentumPoints": 80, "regime": "trend", "breakout": False},
            {"ok": True, "side": "SELL", "confidence": 0.9, "momentumPoints": 20, "regime": "trend", "breakout": False},
        ]
        snapshot = {"tick": {"bid": 4500.0, "ask": 4500.09}}
        with mock.patch.object(main_forex, "_safe_mt5_market_snapshot", return_value=snapshot):
            with mock.patch.object(main_forex, "_autonomous_signal_from_market", side_effect=signals):
                out = main_forex._professional_strategy_guard(cfg, "XAUUSD", "BUY", "DEMO")

        self.assertFalse(out["ok"])
        self.assertEqual(out["biasSide"], "SELL")
        self.assertEqual(out["biasConfirmations"], 2)

    def test_professional_strategy_guard_uses_professional_threshold_not_signal_ok(self):
        cfg = {
            "professionalStrategyEnabled": True,
            "professionalBiasTimeframes": ["M15", "M5"],
            "professionalMinTfConfidence": 0.74,
            "professionalMinTfMomentumPoints": 6,
            "professionalMaxSpreadPoints": 30,
            "pipSize": 0.1,
        }
        signals = [
            {"ok": False, "side": "SELL", "confidence": 0.799, "momentumPoints": 87.4, "regime": "trend", "breakout": False},
            {"ok": True, "side": "SELL", "confidence": 0.9, "momentumPoints": 7.5, "regime": "trend", "breakout": False},
        ]
        snapshot = {"tick": {"bid": 4500.0, "ask": 4500.09}}
        with mock.patch.object(main_forex, "_safe_mt5_market_snapshot", return_value=snapshot):
            with mock.patch.object(main_forex, "_autonomous_signal_from_market", side_effect=signals):
                out = main_forex._professional_strategy_guard(cfg, "XAUUSD", "SELL", "DEMO")

        self.assertTrue(out["ok"])
        self.assertEqual(out["confirmations"], 2)

    def test_professional_quality_guard_blocks_stacking(self):
        out = main_forex._strategy_quality_guard(
            {"professionalStrategyEnabled": True, "lotSize": 0.01, "contractSize": 100, "takeProfitPips": 25, "pipSize": 0.1},
            "XAUUSD",
            "BUY",
            {"confidence": 0.9, "momentumPoints": 20, "breakout": False, "regime": "trend"},
            [{"ticket": 1, "side": "LONG", "profit": 0.0}],
            "DEMO",
        )

        self.assertFalse(out["ok"])
        self.assertEqual(out["code"], "professional_single_position")

    def test_professional_quality_guard_allows_multi_symbol_slots(self):
        base_cfg = {
            "professionalStrategyEnabled": True,
            "professionalAllowMultiSymbolPositions": True,
            "demoBlockRangeEntries": False,
            "demoQualityMinConfidence": 0.72,
            "demoQualityMinMomentumPoints": 12,
            "lotSize": 0.01,
            "contractSize": 100,
            "takeProfitPips": 25,
            "pipSize": 0.1,
        }
        signal = {"confidence": 0.9, "momentumPoints": 20, "breakout": False, "regime": "trend"}

        other_symbol = main_forex._strategy_quality_guard(
            base_cfg,
            "EURUSD",
            "BUY",
            signal,
            [{"ticket": 1, "side": "LONG", "profit": 0.0, "symbol": "XAUUSD"}],
            "DEMO",
        )
        same_symbol = main_forex._strategy_quality_guard(
            base_cfg,
            "EURUSD",
            "BUY",
            signal,
            [{"ticket": 2, "side": "LONG", "profit": 0.0, "symbol": "EURUSD"}],
            "DEMO",
        )

        self.assertTrue(other_symbol["ok"])
        self.assertFalse(same_symbol["ok"])
        self.assertEqual(same_symbol["code"], "same_symbol_position")

    def test_multi_symbol_guard_blocks_same_symbol_stack_without_professional_mode(self):
        out = main_forex._strategy_quality_guard(
            {
                "multiSymbolScanEnabled": True,
                "onePositionPerSymbol": True,
                "demoQualityMinConfidence": 0.55,
                "demoQualityMinMomentumPoints": 1,
                "lotSize": 0.01,
                "contractSize": 100,
                "takeProfitPips": 25,
                "pipSize": 0.1,
            },
            "GBPUSD",
            "SELL",
            {"confidence": 0.7, "momentumPoints": 3, "breakout": False, "regime": "trend"},
            [{"ticket": 1, "side": "SHORT", "profit": 0.0, "symbol": "GBPUSD"}],
            "DEMO",
        )

        self.assertFalse(out["ok"])
        self.assertEqual(out["code"], "same_symbol_position")

    def test_demo_portfolio_slots_default_to_symbol_diversification(self):
        cfg = main_forex._enforce_demo_portfolio_slots(
            {
                "executionMode": "DEMO",
                "portfolioFixedSlots": True,
                "portfolioTargetOpenOrders": 3,
                "maxLot": 0.03,
                "maxExposureLots": 0.03,
            }
        )

        self.assertEqual(cfg["maxOpenOrders"], 3)
        self.assertTrue(cfg["allowStacking"])
        self.assertTrue(cfg["portfolioDiversifySymbols"])
        self.assertTrue(cfg["onePositionPerSymbol"])
        self.assertEqual(cfg["maxPositionsPerSymbol"], 1)
        self.assertEqual(cfg["maxExposureLots"], 0.09)

    def test_demo_portfolio_slots_keep_minimum_calm_fill_floor(self):
        cfg = main_forex._enforce_demo_portfolio_slots(
            {
                "executionMode": "DEMO",
                "portfolioFixedSlots": True,
                "portfolioTargetOpenOrders": 1,
                "maxOpenOrders": 1,
                "maxOpenPositions": 1,
                "supervisorCalmFillSlotsEnabled": True,
                "supervisorCalmFillMinOpenOrders": 3,
                "supervisorCalmFillMaxOpenOrders": 5,
                "maxLot": 0.01,
                "maxExposureLots": 0.01,
            }
        )

        self.assertEqual(cfg["portfolioTargetOpenOrders"], 3)
        self.assertEqual(cfg["maxOpenOrders"], 5)
        self.assertEqual(cfg["maxOpenPositions"], 5)
        self.assertEqual(cfg["maxExposureLots"], 0.05)

    def test_symbol_runtime_config_protects_diversification_keys_from_learning(self):
        main_forex._STATE["learningProfilesBySymbol"] = {
            "GBPUSD": {
                "proposal": {
                    "portfolioDiversifySymbols": False,
                    "onePositionPerSymbol": False,
                    "maxPositionsPerSymbol": 3,
                },
                "latestWindow": {"weak": True},
            }
        }
        out = main_forex._symbol_runtime_config(
            {
                "symbol": "XAUUSD",
                "executionMode": "DEMO",
                "symbolLearningEnabled": True,
                "symbolLearningApplyTuning": True,
                "portfolioFixedSlots": True,
                "portfolioDiversifySymbols": True,
                "onePositionPerSymbol": True,
                "maxPositionsPerSymbol": 1,
            },
            "GBPUSD",
        )

        self.assertTrue(out["portfolioDiversifySymbols"])
        self.assertTrue(out["onePositionPerSymbol"])
        self.assertEqual(out["maxPositionsPerSymbol"], 1)

    def test_risk_guard_blocks_duplicate_symbol_when_diversifying(self):
        out = main_forex._risk_guard_for_order(
            {
                "portfolioDiversifySymbols": True,
                "multiSymbolScanEnabled": True,
                "maxOpenOrders": 3,
                "maxExposureLots": 0.09,
                "maxPositionsPerSymbol": 1,
            },
            "GBPUSD",
            0.01,
            [{"ticket": 1, "symbol": "GBPUSD", "volume": 0.01}],
            None,
        )

        self.assertFalse(out["ok"])
        self.assertEqual(out["code"], "max_positions_per_symbol")
        self.assertEqual(out["sameSymbolOpen"], 1)

    def test_risk_guard_allows_different_symbol_slots_when_diversifying(self):
        out = main_forex._risk_guard_for_order(
            {
                "portfolioDiversifySymbols": True,
                "multiSymbolScanEnabled": True,
                "maxOpenOrders": 3,
                "maxExposureLots": 0.09,
                "maxPositionsPerSymbol": 1,
            },
            "EURUSD",
            0.01,
            [{"ticket": 1, "symbol": "GBPUSD", "volume": 0.01}],
            None,
        )

        self.assertTrue(out["ok"])
        self.assertEqual(out["sameSymbolOpen"], 0)

    def test_professional_quality_guard_allows_confirmed_pullback_momentum(self):
        out = main_forex._strategy_quality_guard(
            {
                "professionalStrategyEnabled": True,
                "professionalM1MinMomentumPoints": 2,
                "demoQualityMinConfidence": 0.72,
                "demoQualityMinMomentumPoints": 12,
                "lotSize": 0.01,
                "contractSize": 100,
                "takeProfitPips": 25,
                "pipSize": 0.1,
            },
            "XAUUSD",
            "SELL",
            {"confidence": 0.734, "momentumPoints": 2.3, "breakout": False, "regime": "trend", "professionalConfirmed": True},
            [],
            "DEMO",
        )

        self.assertTrue(out["ok"])

    def test_symbol_runtime_config_uses_forex_pip_profile(self):
        cfg = {"symbol": "XAUUSD", "pipSize": 0.1, "takeProfitPips": 25, "stopLossPips": 14}

        eur = main_forex._symbol_runtime_config(cfg, "EURUSD")
        jpy = main_forex._symbol_runtime_config(cfg, "USDJPY")
        gold = main_forex._symbol_runtime_config(cfg, "XAUUSD")

        self.assertEqual(eur["pipSize"], 0.0001)
        self.assertEqual(jpy["pipSize"], 0.01)
        self.assertEqual(gold["pipSize"], 0.1)
        self.assertEqual(eur["contractSize"], 100000)
        self.assertEqual(gold["contractSize"], 100)
        self.assertEqual(eur["takeProfitPips"], 16)
        self.assertEqual(eur["stopLossPips"], 8)

    def test_adaptive_lot_uses_independent_symbol_multiplier_profiles(self):
        cfg = {
            "executionMode": "DEMO",
            "lotSizingMode": "adaptive_risk",
            "baseLot": 0.01,
            "minLot": 0.001,
            "maxLot": 0.03,
            "lotStep": 0.001,
            "maxExposureLots": 0.03,
            "dailyDrawdownPct": 3.0,
            "symbolMultiplierProfiles": {
                "XAUUSD": {"multiplierMin": 0.65, "multiplierMax": 1.7, "volatilityThrottlePoints": 120, "volatilityBoostBelowPoints": 35, "calmMultiplierFloor": 1.55},
                "FOREX_MAJOR": {"multiplierMin": 0.75, "multiplierMax": 2.0, "volatilityThrottlePoints": 12, "volatilityBoostBelowPoints": 3, "calmMultiplierFloor": 1.55},
            },
        }
        xau_snapshot = {"candles": [{"high": 4501.0, "low": 4499.0} for _ in range(12)]}
        eur_snapshot = {"candles": [{"high": 1.1020, "low": 1.1000} for _ in range(12)]}

        xau = main_forex._adaptive_lot_size({**cfg, "pipSize": 0.1}, {"confidence": 0.9}, "XAUUSD", 0.01, [], xau_snapshot, None)
        eur = main_forex._adaptive_lot_size({**cfg, "pipSize": 0.0001}, {"confidence": 0.9}, "EURUSD", 0.01, [], eur_snapshot, None)

        self.assertEqual(xau["symbolMultiplierProfile"]["group"], "XAUUSD")
        self.assertEqual(eur["symbolMultiplierProfile"]["group"], "FOREX_MAJOR")
        self.assertEqual(xau["multiplierMax"], 1.7)
        self.assertEqual(eur["multiplierMax"], 2.0)
        self.assertGreater(xau["multiplier"], eur["multiplier"])
        self.assertTrue(any("XAUUSD calm volatility acceleration" in reason for reason in xau["reasons"]))
        self.assertTrue(any("EURUSD high volatility throttle" in reason or "EURUSD extreme volatility throttle" in reason for reason in eur["reasons"]))

    def test_calm_volatility_can_round_to_larger_demo_lot(self):
        main_forex._STATE["tradeReports"] = []
        cfg = {
            "executionMode": "DEMO",
            "lotSizingMode": "adaptive_risk",
            "baseLot": 0.01,
            "minLot": 0.01,
            "maxLot": 0.02,
            "lotStep": 0.01,
            "maxExposureLots": 0.06,
            "demoQualityMinConfidence": 0.55,
            "symbolMultiplierProfiles": {
                "FOREX_MAJOR": {"multiplierMin": 0.75, "multiplierMax": 2.0, "volatilityThrottlePoints": 12, "volatilityBoostBelowPoints": 3, "calmMultiplierFloor": 1.55},
            },
        }
        snapshot = {"candles": [{"high": 1.1001, "low": 1.1000} for _ in range(12)]}

        out = main_forex._adaptive_lot_size({**cfg, "pipSize": 0.0001}, {"confidence": 0.56}, "EURUSD", 0.01, [], snapshot, None)

        self.assertEqual(out["recommendedLot"], 0.02)
        self.assertGreaterEqual(out["multiplier"], 1.55)
        self.assertTrue(any("calm volatility acceleration" in reason for reason in out["reasons"]))

    def test_fee_aware_lot_boost_raises_size_when_tp_net_is_too_small(self):
        main_forex._STATE["tradeReports"] = []
        cfg = {
            "executionMode": "DEMO",
            "lotSizingMode": "adaptive_risk",
            "baseLot": 0.01,
            "minLot": 0.01,
            "maxLot": 0.05,
            "lotStep": 0.01,
            "maxExposureLots": 0.15,
            "takeProfitPips": 4,
            "contractSize": 100000,
            "pipSize": 0.0001,
            "estimatedRoundTurnCommissionUsd": 0.08,
            "demoNetProfitMinUsd": 0.65,
            "minGrossProfitUsd": 0.85,
            "autoBoostLotForFees": True,
            "symbolMultiplierProfiles": {
                "FOREX_MAJOR": {"multiplierMin": 0.75, "multiplierMax": 3.2, "volatilityThrottlePoints": 12, "volatilityBoostBelowPoints": 3}
            },
        }
        snapshot = {"candles": [{"high": 1.1002, "low": 1.1000} for _ in range(12)]}

        out = main_forex._adaptive_lot_size(cfg, {"confidence": 0.7}, "EURUSD", 0.01, [], snapshot, None)

        self.assertGreaterEqual(out["recommendedLot"], 0.03)
        self.assertTrue(any("fee-aware lot boost" in reason for reason in out["reasons"]))

    def test_calm_weak_symbol_can_get_bounded_fee_recovery_lot_boost(self):
        main_forex._STATE["tradeReports"] = []
        cfg = {
            "executionMode": "DEMO",
            "lotSizingMode": "adaptive_risk",
            "baseLot": 0.01,
            "minLot": 0.01,
            "maxLot": 0.03,
            "lotStep": 0.01,
            "maxExposureLots": 0.06,
            "demoQualityMinConfidence": 0.55,
            "lotCalmWeakSymbolFeeRecoveryEnabled": True,
            "lotCalmWeakSymbolMaxMultiplier": 1.25,
            "symbolLearningProfile": {
                "learningScore": 30,
                "latestWindow": {"weak": True},
            },
            "symbolMultiplierProfiles": {
                "FOREX_JPY": {"multiplierMin": 0.75, "multiplierMax": 2.6, "volatilityThrottlePoints": 16, "volatilityBoostBelowPoints": 4, "calmMultiplierFloor": 1.8},
            },
        }
        snapshot = {"candles": [{"high": 160.01, "low": 160.00} for _ in range(12)]}

        out = main_forex._adaptive_lot_size({**cfg, "pipSize": 0.01}, {"confidence": 0.76, "opportunityProbability": 0.74}, "USDJPY", 0.01, [], snapshot, None)

        self.assertGreaterEqual(out["multiplier"], 1.25)
        self.assertLess(out["multiplier"], 1.8)
        self.assertTrue(any("calm fee-recovery lot boost" in reason for reason in out["reasons"]))

    def test_fee_aware_order_guard_adjusts_tp_above_commission_floor(self):
        cfg = {
            "symbol": "EURUSD",
            "takeProfitPips": 4,
            "contractSize": 100000,
            "pipSize": 0.0001,
            "baseLot": 0.01,
            "estimatedRoundTurnCommissionUsd": 0.08,
            "demoNetProfitMinUsd": 0.65,
            "minGrossProfitUsd": 0.85,
            "feeCoverageMultiplier": 5.0,
            "autoAdjustTpForFees": True,
            "maxForexTakeProfitPipsForFeeGuard": 18,
        }

        out = main_forex._fee_aware_order_economics_guard(cfg, "EURUSD", 0.01)

        self.assertTrue(out["ok"])
        self.assertGreater(out["adjustedTakeProfitPips"], out["takeProfitPips"])
        self.assertGreaterEqual(out["estimatedGrossTpUsd"], out["minGrossProfitUsd"])
        self.assertGreaterEqual(out["estimatedNetTpUsd"], out["minNetProfitUsd"])

    def test_symbol_volatility_regime_controls_demo_lot_size(self):
        main_forex._STATE["tradeReports"] = []
        cfg = {
            "executionMode": "DEMO",
            "lotSizingMode": "adaptive_risk",
            "baseLot": 0.01,
            "minLot": 0.01,
            "maxLot": 0.03,
            "lotStep": 0.01,
            "maxExposureLots": 0.09,
            "demoQualityMinConfidence": 0.42,
            "symbolMultiplierProfiles": {
                "FOREX_MAJOR": {
                    "multiplierMin": 0.75,
                    "multiplierMax": 2.8,
                    "volatilityThrottlePoints": 12,
                    "volatilityBoostBelowPoints": 3,
                    "calmMultiplierFloor": 1.85,
                    "calmMultiplierCeiling": 2.7,
                    "calmMultiplierBoost": 0.4,
                    "highVolatilityThrottleFactor": 0.65,
                    "extremeVolatilityThrottleFactor": 0.48,
                },
            },
        }
        calm_snapshot = {"candles": [{"high": 1.10001, "low": 1.10000} for _ in range(12)]}
        volatile_snapshot = {"candles": [{"high": 1.1030, "low": 1.1000} for _ in range(12)]}

        calm = main_forex._adaptive_lot_size({**cfg, "pipSize": 0.0001}, {"confidence": 0.9}, "EURUSD", 0.01, [], calm_snapshot, None)
        volatile = main_forex._adaptive_lot_size({**cfg, "pipSize": 0.0001}, {"confidence": 0.9}, "EURUSD", 0.01, [], volatile_snapshot, None)

        self.assertEqual(calm["recommendedLot"], 0.03)
        self.assertGreaterEqual(calm["multiplier"], 2.5)
        self.assertTrue(any("calm volatility acceleration" in reason for reason in calm["reasons"]))
        self.assertEqual(volatile["recommendedLot"], 0.01)
        self.assertLess(volatile["multiplier"], 1.0)
        self.assertTrue(any("extreme volatility throttle" in reason for reason in volatile["reasons"]))

    def test_symbol_entry_opportunity_guard_waits_for_weak_symbol_timing(self):
        cfg = {
            "pipSize": 0.0001,
            "symbolOpportunityMinProbability": 0.62,
            "symbolOpportunityMinScore": 1.0,
            "symbolOpportunityMinMomentumPoints": 0.5,
            "symbolMultiplierProfiles": {
                "FOREX_MAJOR": {"volatilityThrottlePoints": 12, "volatilityBoostBelowPoints": 3}
            },
        }
        snapshot = {"candles": [{"high": 1.1002, "low": 1.1000, "close": 1.1001} for _ in range(12)]}
        signal = {"side": "BUY", "confidence": 0.56, "scoreGap": 0.4, "momentumPoints": 0.1, "trendPoints": 0.2, "breakout": False}

        out = main_forex._symbol_entry_opportunity_guard(cfg, "EURUSD", signal, snapshot)

        self.assertFalse(out["ok"])
        self.assertEqual(out["code"], "symbol_opportunity_wait")
        self.assertTrue(any("probability" in reason for reason in out["reasons"]))

    def test_symbol_entry_opportunity_guard_accepts_strong_symbol_timing(self):
        cfg = {
            "pipSize": 0.0001,
            "symbolOpportunityMinProbability": 0.56,
            "symbolOpportunityMinScore": 0.75,
            "symbolOpportunityMinMomentumPoints": 0.25,
            "symbolMultiplierProfiles": {
                "FOREX_MAJOR": {"volatilityThrottlePoints": 12, "volatilityBoostBelowPoints": 3}
            },
        }
        snapshot = {"candles": [{"high": 1.1003, "low": 1.1000, "close": 1.1002} for _ in range(12)]}
        signal = {"side": "SELL", "confidence": 0.68, "scoreGap": 1.8, "momentumPoints": 1.2, "trendPoints": 1.0, "breakout": False}

        out = main_forex._symbol_entry_opportunity_guard(cfg, "EURUSD", signal, snapshot)

        self.assertTrue(out["ok"])
        self.assertGreaterEqual(out["probability"], 0.56)
        self.assertGreaterEqual(out["score"], 0.75)

    def test_adaptive_lot_uses_opportunity_probability_for_multiplier(self):
        main_forex._STATE["tradeReports"] = []
        cfg = {
            "executionMode": "DEMO",
            "lotSizingMode": "adaptive_risk",
            "baseLot": 0.01,
            "minLot": 0.01,
            "maxLot": 0.03,
            "lotStep": 0.01,
            "maxExposureLots": 0.09,
            "demoQualityMinConfidence": 0.5,
            "symbolMultiplierProfiles": {
                "FOREX_MAJOR": {"multiplierMin": 0.75, "multiplierMax": 2.8, "volatilityThrottlePoints": 12, "volatilityBoostBelowPoints": 3, "calmMultiplierFloor": 1.85}
            },
        }
        snapshot = {"candles": [{"high": 1.10001, "low": 1.10000} for _ in range(12)]}

        out = main_forex._adaptive_lot_size({**cfg, "pipSize": 0.0001}, {"confidence": 0.86, "opportunityProbability": 0.86, "opportunityScore": 1.8}, "EURUSD", 0.01, [], snapshot, None)

        self.assertEqual(out["recommendedLot"], 0.03)
        self.assertGreaterEqual(out["opportunityProbability"], 0.86)
        self.assertTrue(any("high probability opportunity boost" in reason for reason in out["reasons"]))

    def test_learning_profiles_are_separated_by_symbol(self):
        main_forex._STATE["config"] = {
            "symbol": "XAUUSD",
            "watchlist": ["XAUUSD", "EURUSD"],
            "multiSymbolScanEnabled": True,
        }
        main_forex._STATE["tradeReports"] = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 1, "pnl": 2.0, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "EURUSD", "mode": "DEMO", "ts": 2, "pnl": -1.0, "exit": 1.08, "reason": "mt5_closed_deal"},
            {"symbol": "EURUSD", "mode": "DEMO", "ts": 3, "pnl": -2.0, "exit": 1.07, "reason": "mt5_closed_deal"},
        ]

        profiles = main_forex._refresh_learning_profiles(main_forex._STATE["config"], "DEMO")

        self.assertEqual(profiles["XAUUSD"]["realizedPnl"], 2.0)
        self.assertEqual(profiles["EURUSD"]["realizedPnl"], -3.0)
        self.assertEqual(profiles["XAUUSD"]["trades"], 1)
        self.assertEqual(profiles["EURUSD"]["trades"], 2)

    def test_layered_memory_weights_recent_regime_more_than_archive(self):
        now = 2_000_000_000
        reports = []
        for index in range(20):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": now - 40 * 86400 + index, "pnl": 1.0, "exit": 4500.0, "reason": "mt5_closed_deal"})
        for index in range(6):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": now - index * 3600, "pnl": -1.0, "exit": 4500.0, "reason": "mt5_closed_deal"})

        layered = main_forex._time_layered_memory_stats(reports, now_ts=now)

        self.assertEqual(layered["layers"]["archiveOver30d"]["trades"], 20)
        self.assertEqual(layered["layers"]["last7d"]["trades"], 6)
        self.assertLess(layered["weighted"]["realizedPnl"], 0)
        self.assertEqual(layered["weighted"]["lossStreak"], 6)

    def test_learning_profile_uses_recent_weighted_memory_over_old_wins(self):
        now = int(time.time())
        main_forex._STATE["config"] = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 5,
            "supervisorReviewMinTrades": 5,
        }
        reports = []
        for index in range(30):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": now - 45 * 86400 + index, "pnl": 1.0, "exit": 4500.0, "reason": "mt5_closed_deal"})
        for index in range(6):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": now - index * 3600, "pnl": -1.0, "exit": 4500.0, "reason": "mt5_closed_deal"})
        main_forex._STATE["tradeReports"] = reports

        profile = main_forex._learning_profile_for_symbol("XAUUSD", "DEMO")

        self.assertGreater(profile["overall"]["realizedPnl"], 0)
        self.assertLess(profile["realizedPnl"], 0)
        self.assertEqual(profile["layeredMemory"]["layers"]["last7d"]["trades"], 6)
        self.assertLess(profile["learningScore"], 45)

    def test_symbol_learning_ranks_winners_first_and_losers_last(self):
        main_forex._STATE["config"] = {
            "symbol": "XAUUSD",
            "watchlist": ["XAUUSD", "EURUSD", "GBPUSD"],
            "multiSymbolScanEnabled": True,
            "symbolLearningEnabled": True,
        }
        main_forex._STATE["tradeReports"] = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 1, "pnl": 1.0, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 2, "pnl": 1.2, "exit": 4501.0, "reason": "mt5_closed_deal"},
            {"symbol": "EURUSD", "mode": "DEMO", "ts": 3, "pnl": -1.0, "exit": 1.08, "reason": "mt5_closed_deal"},
            {"symbol": "EURUSD", "mode": "DEMO", "ts": 4, "pnl": -1.2, "exit": 1.07, "reason": "mt5_closed_deal"},
        ]

        profiles = main_forex._refresh_learning_profiles(main_forex._STATE["config"], "DEMO")
        ranked = main_forex._rank_symbols_for_scan(["EURUSD", "GBPUSD", "XAUUSD"], main_forex._STATE["config"])
        review = main_forex._symbol_cross_learning_review(profiles)

        self.assertGreater(profiles["XAUUSD"]["learningScore"], profiles["EURUSD"]["learningScore"])
        self.assertEqual(profiles["XAUUSD"]["priority"], "front")
        self.assertEqual(profiles["EURUSD"]["priority"], "back")
        self.assertEqual(ranked[0], "XAUUSD")
        self.assertEqual(ranked[-1], "EURUSD")
        self.assertIn("XAUUSD", review["winners"])
        self.assertIn("EURUSD", review["losers"])
        self.assertTrue(any(x["type"] == "promote_winner_patterns" for x in review["lessons"]))
        self.assertTrue(any(x["type"] == "study_loser_failures" for x in review["lessons"]))

    def test_forex_confluence_adds_score_gap_to_signal(self):
        closes = [100 + i * 0.1 for i in range(20)]
        candles = [{"close": c, "high": c + 0.03, "low": c - 0.03} for c in closes]
        out = main_forex._autonomous_signal_from_market(
            {"pipSize": 0.01, "demoAutoMinConfidence": 0.55, "maxSpreadPoints": 30},
            "EURUSD",
            {"candles": candles, "tick": {"spread": 0.0}},
            "demo",
        )

        self.assertEqual(out["side"], "BUY")
        self.assertGreater(out["scoreGap"], 0)
        self.assertIn("confluence", out)
        self.assertTrue(any(step["gate"] == "confluence" for step in out["pipeline"]))

    def test_early_entry_bias_can_flip_lagging_ma_signal(self):
        closes = [100.0, 99.8, 99.6, 99.4, 99.2, 99.0, 98.8, 98.7, 98.6, 98.55, 98.9, 99.25]
        candles = [{"close": c, "high": c + 0.04, "low": c - 0.04} for c in closes]
        out = main_forex._autonomous_signal_from_market(
            {
                "pipSize": 0.1,
                "demoAutoMinConfidence": 0.5,
                "maxSpreadPoints": 30,
                "earlyEntryEnabled": True,
                "earlyEntryScoreGapMin": 0.8,
                "earlyEntryMinMomentumPoints": 0.5,
                "earlyEntryMinAccelerationPoints": 0.0,
            },
            "XAUUSD",
            {"candles": candles, "tick": {"spread": 0.0}},
            "demo",
        )

        self.assertEqual(out["side"], "BUY")
        self.assertEqual(out["earlyEntry"]["side"], "BUY")
        self.assertIn("early entry bias", " ".join(out["confluence"]["notes"]))

    def test_quality_guard_uses_near_signal_fallback_from_confluence_gap(self):
        out = main_forex._strategy_quality_guard(
            {
                "demoQualityMinConfidence": 0.60,
                "demoQualityMinMomentumPoints": 6,
                "scanFallbackNearEnabled": True,
                "scanFallbackNearConfRelax": 0.06,
                "entryMinScoreGap": 1.0,
                "lotSize": 0.01,
                "contractSize": 100,
                "takeProfitPips": 25,
                "pipSize": 0.1,
            },
            "XAUUSD",
            "BUY",
            {"confidence": 0.56, "momentumPoints": 1.0, "scoreGap": 3.2, "breakout": False, "regime": "trend"},
            [],
            "DEMO",
        )

        self.assertTrue(out["ok"])
        self.assertTrue(out["nearSignalFallback"])

    def test_quality_guard_ignores_future_closed_trade_timestamp_for_loss_cooldown(self):
        now = int(time.time())
        main_forex._STATE["tradeReports"] = [
            {"symbol": "EURUSD", "mode": "DEMO", "ts": now + 10_000, "pnl": -0.4, "exit": 1.1, "reason": "mt5_closed_deal"},
            {"symbol": "EURUSD", "mode": "DEMO", "ts": now + 10_001, "pnl": -0.3, "exit": 1.1, "reason": "mt5_closed_deal"},
        ]

        out = main_forex._strategy_quality_guard(
            {
                "demoQualityMinConfidence": 0.45,
                "demoQualityMinMomentumPoints": 0.1,
                "entryMinScoreGap": 0.5,
                "demoLossCooldownAfter": 2,
                "demoLossCooldownSec": 180,
                "demoMaxLossStreakStop": 12,
                "lotSize": 0.01,
                "contractSize": 100000,
                "takeProfitPips": 12,
                "pipSize": 0.0001,
            },
            "EURUSD",
            "SELL",
            {"confidence": 0.7, "momentumPoints": 2.0, "scoreGap": 4.0, "breakout": False, "regime": "trend"},
            [],
            "DEMO",
        )

        self.assertTrue(out["ok"])

    def test_quality_guard_bypasses_loss_cooldown_while_filling_required_slots(self):
        now = int(time.time())
        main_forex._STATE["tradeReports"] = [
            {"symbol": "EURUSD", "mode": "DEMO", "ts": now - 10, "pnl": -0.4, "exit": 1.1, "reason": "mt5_closed_deal"},
            {"symbol": "GBPUSD", "mode": "DEMO", "ts": now - 9, "pnl": -0.3, "exit": 1.2, "reason": "mt5_closed_deal"},
            {"symbol": "USDJPY", "mode": "DEMO", "ts": now - 8, "pnl": -0.2, "exit": 150.0, "reason": "mt5_closed_deal"},
        ]

        out = main_forex._strategy_quality_guard(
            {
                "executionMode": "DEMO",
                "portfolioFixedSlots": True,
                "supervisorCalmFillSlotsEnabled": True,
                "portfolioTargetOpenOrders": 3,
                "demoAllowRangeForSlotFill": True,
                "demoBypassLossStopsForSlotFill": True,
                "demoBlockRangeEntries": True,
                "demoQualityMinConfidence": 0.45,
                "demoQualityMinMomentumPoints": 0.1,
                "entryMinScoreGap": 0.5,
                "demoLossCooldownAfter": 2,
                "demoLossCooldownSec": 600,
                "demoMaxLossStreakStop": 3,
                "lotSize": 0.01,
                "contractSize": 100000,
                "takeProfitPips": 12,
                "pipSize": 0.0001,
            },
            "AUDUSD",
            "SELL",
            {"confidence": 0.7, "momentumPoints": 2.0, "scoreGap": 5.0, "breakout": False, "regime": "range"},
            [{"ticket": 1, "symbol": "GBPUSD", "side": "LONG", "profit": 0.0}],
            "DEMO",
        )

        self.assertTrue(out["ok"])

    def test_quality_guard_bypasses_range_filter_after_idle_clear_signal(self):
        main_forex._STATE["lastClearSignalGuard"] = {
            "ok": True,
            "htfCalmAllowed": True,
            "professionalCalmAllowed": True,
        }
        out = main_forex._strategy_quality_guard(
            {
                "executionMode": "DEMO",
                "portfolioFixedSlots": True,
                "professionalIdleRelaxationEnabled": True,
                "portfolioBypassEntrySpacingForSlotFill": True,
                "supervisorCalmFillSlotsEnabled": False,
                "portfolioTargetOpenOrders": 3,
                "demoAllowRangeForSlotFill": True,
                "demoBypassLossStopsForSlotFill": True,
                "demoBlockRangeEntries": True,
                "demoQualityMinConfidence": 0.68,
                "demoQualityMinMomentumPoints": 0.8,
                "entryMinScoreGap": 1.1,
                "demoLossCooldownAfter": 2,
                "demoLossCooldownSec": 600,
                "demoMaxLossStreakStop": 3,
                "lotSize": 0.01,
                "contractSize": 100000,
                "takeProfitPips": 16,
                "pipSize": 0.0001,
            },
            "AUDUSD",
            "SELL",
            {"confidence": 0.692, "momentumPoints": 3.3, "scoreGap": 5.0, "breakout": False, "regime": "range"},
            [],
            "DEMO",
        )

        self.assertTrue(out["ok"])

    def test_learning_status_exposes_profiles_by_symbol(self):
        main_forex._STATE["config"] = {
            "symbol": "XAUUSD",
            "watchlist": ["XAUUSD", "EURUSD"],
            "multiSymbolScanEnabled": True,
        }
        main_forex._STATE["tradeReports"] = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 1, "pnl": 1.0, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "EURUSD", "mode": "DEMO", "ts": 2, "pnl": -1.0, "exit": 1.08, "reason": "mt5_closed_deal"},
        ]

        out = main_forex.learning_status()

        self.assertEqual(out["symbolsScanned"], 2)
        self.assertIn("XAUUSD", out["profilesBySymbol"])
        self.assertIn("EURUSD", out["profilesBySymbol"])
        self.assertEqual(out["profilesBySymbol"]["EURUSD"]["realizedPnl"], -1.0)
        self.assertIn("symbolLearningReview", out)

    def test_symbol_runtime_config_applies_symbol_learning_without_portfolio_limits(self):
        main_forex._STATE["learningProfilesBySymbol"] = {
            "EURUSD": {
                "symbol": "EURUSD",
                "trades": 20,
                "winRatePct": 30.0,
                "realizedPnl": -10.0,
                "sampleStatus": "ready",
                "proposal": {
                    "demoQualityMinConfidence": 0.82,
                    "demoQualityMinMomentumPoints": 18,
                    "maxOpenOrders": 1,
                    "maxOpenPositions": 1,
                    "maxExposureLots": 0.01,
                },
            }
        }

        out = main_forex._symbol_runtime_config(
            {
                "symbol": "XAUUSD",
                "executionMode": "DEMO",
                "multiSymbolScanEnabled": True,
                "symbolLearningEnabled": True,
                "symbolLearningApplyTuning": True,
                "demoQualityMinConfidence": 0.55,
                "demoQualityMinMomentumPoints": 1,
                "maxOpenOrders": 3,
                "maxOpenPositions": 3,
                "maxExposureLots": 0.03,
            },
            "EURUSD",
        )

        self.assertEqual(out["demoQualityMinConfidence"], 0.55)
        self.assertEqual(out["demoQualityMinMomentumPoints"], 1)
        self.assertEqual(out["maxOpenOrders"], 3)
        self.assertEqual(out["maxOpenPositions"], 3)
        self.assertEqual(out["maxExposureLots"], 0.03)
        self.assertIn("demoQualityMinConfidence", out["symbolLearningProfile"]["portfolioKeysHeld"])
        self.assertIn("demoQualityMinMomentumPoints", out["symbolLearningProfile"]["portfolioKeysHeld"])
        self.assertIn("maxOpenOrders", out["symbolLearningProfile"]["portfolioKeysHeld"])

    def test_symbol_runtime_config_keeps_learning_score_without_tuning_by_default(self):
        main_forex._STATE["learningProfilesBySymbol"] = {
            "EURUSD": {
                "symbol": "EURUSD",
                "learningScore": 20,
                "priority": "back",
                "proposal": {"demoQualityMinConfidence": 0.82, "demoQualityMinMomentumPoints": 18},
            }
        }

        out = main_forex._symbol_runtime_config(
            {
                "symbol": "XAUUSD",
                "executionMode": "DEMO",
                "multiSymbolScanEnabled": True,
                "symbolLearningEnabled": True,
                "demoQualityMinConfidence": 0.68,
                "demoQualityMinMomentumPoints": 6,
            },
            "EURUSD",
        )

        self.assertEqual(out["demoQualityMinConfidence"], 0.68)
        self.assertEqual(out["demoQualityMinMomentumPoints"], 6)
        self.assertFalse(out["symbolLearningProfile"]["applyTuning"])
        self.assertEqual(out["symbolLearningProfile"]["learningScore"], 20)

    def test_latest_window_drag_pushes_xauusd_to_back_queue(self):
        main_forex._STATE["config"] = {
            "symbol": "XAUUSD",
            "watchlist": ["XAUUSD", "EURUSD"],
            "executionMode": "DEMO",
            "multiSymbolScanEnabled": True,
            "symbolLearningEnabled": True,
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
        }
        reports = []
        for index in range(20):
            reports.append({"symbol": "EURUSD", "mode": "DEMO", "ts": index + 1, "pnl": 0.6, "exit": 1.1, "reason": "mt5_closed_deal"})
        xau_pnls = [0.5] * 8 + [-1.0] * 12
        for index, pnl in enumerate(xau_pnls, start=30):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 4500.0, "reason": "mt5_closed_deal"})
        main_forex._STATE["tradeReports"] = reports

        profiles = main_forex._refresh_learning_profiles(main_forex._STATE["config"], "DEMO")
        ranked = main_forex._rank_symbols_for_scan(["XAUUSD", "EURUSD"], main_forex._STATE["config"])

        self.assertTrue(profiles["XAUUSD"]["latestWindow"]["weak"])
        self.assertEqual(profiles["XAUUSD"]["priority"], "back")
        self.assertLess(profiles["XAUUSD"]["learningScore"], profiles["EURUSD"]["learningScore"])
        self.assertEqual(ranked[-1], "XAUUSD")
        self.assertIn("latest window negative expectancy", profiles["XAUUSD"]["scoreReasons"])

    def test_symbol_runtime_config_applies_latest_window_guard_without_full_tuning(self):
        main_forex._STATE["learningProfilesBySymbol"] = {
            "XAUUSD": {
                "symbol": "XAUUSD",
                "trades": 20,
                "winRatePct": 40.0,
                "realizedPnl": -7.78,
                "learningScore": 20,
                "priority": "back",
                "latestWindow": {"weak": True, "trades": 20, "winRatePct": 40.0, "profitFactor": 0.78, "realizedPnl": -7.78},
                "proposal": {
                    "demoAutoMinConfidence": 0.82,
                    "demoQualityMinConfidence": 0.82,
                    "demoQualityMinMomentumPoints": 18,
                    "demoBlockRangeEntries": True,
                    "demoHigherTimeframeConfirm": True,
                    "demoHigherTimeframeMinConfidence": 0.72,
                    "guardianAdaptiveTakeProfitCapturePct": 0.72,
                    "guardianProfitGivebackTriggerUsd": 1.6,
                    "maxOpenOrders": 1,
                },
                "sampleStatus": "ready",
            }
        }

        out = main_forex._symbol_runtime_config(
            {
                "symbol": "XAUUSD",
                "executionMode": "DEMO",
                "symbolLearningEnabled": True,
                "symbolLearningApplyTuning": False,
                "demoQualityMinConfidence": 0.55,
                "demoQualityMinMomentumPoints": 1,
                "demoBlockRangeEntries": False,
                "takeProfitPips": 25,
                "stopLossPips": 14,
                "maxOpenOrders": 3,
            },
            "XAUUSD",
        )

        self.assertEqual(out["demoQualityMinConfidence"], 0.55)
        self.assertEqual(out["demoQualityMinMomentumPoints"], 1)
        self.assertFalse(out["demoBlockRangeEntries"])
        self.assertFalse(out.get("demoHigherTimeframeConfirm", False))
        self.assertEqual(out["guardianAdaptiveTakeProfitCapturePct"], 0.72)
        self.assertEqual(out["takeProfitPips"], 18.0)
        self.assertEqual(out["stopLossPips"], 10.0)
        self.assertEqual(out["maxOpenOrders"], 3)
        self.assertFalse(out["symbolLearningProfile"]["applyTuning"])
        self.assertTrue(out["symbolLearningProfile"]["applyLatestWindowGuard"])
        self.assertIn("maxOpenOrders", out["symbolLearningProfile"]["portfolioKeysHeld"])
        self.assertIn("demoHigherTimeframeConfirm", out["symbolLearningProfile"]["portfolioKeysHeld"])

    def test_symbol_runtime_config_does_not_reenable_range_block_after_low_activity_override(self):
        main_forex._STATE["learningProfilesBySymbol"] = {
            "USDJPY": {
                "symbol": "USDJPY",
                "trades": 20,
                "learningScore": 20,
                "priority": "back",
                "latestWindow": {"weak": True},
                "proposal": {
                    "demoBlockRangeEntries": True,
                    "guardianProfitTrailMinPnlUsd": 0.25,
                },
            }
        }

        out = main_forex._symbol_runtime_config(
            {
                "symbol": "USDJPY",
                "executionMode": "DEMO",
                "portfolioFixedSlots": False,
                "symbolLearningEnabled": True,
                "symbolLearningApplyTuning": False,
                "demoBlockRangeEntries": False,
            },
            "USDJPY",
        )

        self.assertFalse(out["demoBlockRangeEntries"])
        self.assertEqual(out["guardianProfitTrailMinPnlUsd"], 0.25)
        self.assertNotIn("demoBlockRangeEntries", out["symbolLearningProfile"]["latestGuardKeys"])

    def test_latest_window_drag_throttles_xauusd_lot_without_calm_floor(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "baseLot": 0.01,
            "minLot": 0.01,
            "maxLot": 0.02,
            "maxExposureLots": 0.02,
            "lotStep": 0.01,
            "demoQualityMinConfidence": 0.82,
            "symbolMultiplierProfiles": {
                "XAUUSD": {"multiplierMin": 0.65, "multiplierMax": 1.7, "volatilityThrottlePoints": 120, "volatilityBoostBelowPoints": 35, "calmMultiplierFloor": 1.55},
            },
            "symbolLearningProfile": {
                "learningScore": 25,
                "latestWindow": {"weak": True, "trades": 20, "realizedPnl": -7.78},
            },
        }
        snapshot = {"candles": [{"high": 4500.2, "low": 4500.0} for _ in range(12)]}

        out = main_forex._adaptive_lot_size(cfg, {"confidence": 0.9}, "XAUUSD", 0.01, [], snapshot, None)

        self.assertEqual(out["recommendedLot"], 0.01)
        self.assertLess(out["multiplier"], 1.0)
        self.assertTrue(any("latest-window risk throttle" in reason for reason in out["reasons"]))
        self.assertFalse(any("calm volatility acceleration" in reason for reason in out["reasons"]))

    def test_periodic_review_downgrades_latest_window_when_quarantine_active(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "demoHigherTimeframeConfirm": True,
            "demoHigherTimeframes": ["M5", "M15"],
            "demoQualityMinConfidence": 0.82,
            "demoQualityMinMomentumPoints": 18,
            "demoBlockRangeEntries": True,
            "maxOpenOrders": 1,
            "maxOpenPositions": 1,
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
        }
        reports = []
        for index in range(180):
            pnl = 0.8 if index % 2 == 0 else -1.0
            reports.append({"symbol": "GBPUSD", "mode": "DEMO", "ts": index + 1, "pnl": pnl, "exit": 1.2, "reason": "mt5_closed_deal"})
        latest_pnls = [0.9] * 8 + [-1.0] * 12
        for index, pnl in enumerate(latest_pnls, start=181):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 4500.0, "reason": "mt5_closed_deal"})
        main_forex._STATE["config"] = cfg
        main_forex._STATE["tradeReports"] = reports
        main_forex._STATE["learningProfilesBySymbol"] = {
            "XAUUSD": {"latestWindow": {"weak": True}, "learningScore": 20, "priority": "back"}
        }

        perf = main_forex._periodic_performance_review(cfg, "DEMO")
        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        titles = [item["title"] for item in perf["recommendations"]]
        self.assertIn("Latest trading window under tightened review", titles)
        self.assertIn("Worst symbol under latest-window quarantine", titles)
        self.assertNotIn("Latest trading window has negative expectancy", titles)
        self.assertNotIn("Worst symbol is dragging the latest window", titles)
        self.assertFalse(any(item["severity"] == "high" for item in perf["recommendations"]))
        self.assertFalse(any("latest losing trade window" in item.get("task", "") for item in review["cmuxHandoff"]))

    def test_periodic_review_downgrades_near_breakeven_recovery_under_tightened_review(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "demoHigherTimeframeConfirm": True,
            "demoHigherTimeframes": ["M5", "M15"],
            "demoQualityMinConfidence": 0.82,
            "demoQualityMinMomentumPoints": 18,
            "maxOpenOrders": 2,
            "maxOpenPositions": 2,
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
        }
        reports = []
        previous_pnls = [0.8] * 8 + [-1.185] * 12
        latest_pnls = [0.874] * 7 + [-0.724] * 11 + [0.88, 0.88]
        for index in range(160):
            pnl = 0.9 if index % 2 == 0 else -1.0
            reports.append({"symbol": "GBPUSD", "mode": "DEMO", "ts": index + 1, "pnl": pnl, "exit": 1.2, "reason": "mt5_closed_deal"})
        for index, pnl in enumerate(previous_pnls, start=161):
            reports.append({"symbol": "XAGUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 30.0, "reason": "mt5_closed_deal"})
        for index, pnl in enumerate(latest_pnls, start=181):
            reports.append({"symbol": "GBPUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 1.2, "reason": "mt5_closed_deal"})
        main_forex._STATE["config"] = cfg
        main_forex._STATE["tradeReports"] = reports
        main_forex._STATE["learningProfilesBySymbol"] = {
            "XAUUSD": {"latestWindow": {"weak": True}, "learningScore": 20, "priority": "back"}
        }

        perf = main_forex._periodic_performance_review(cfg, "DEMO")
        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        titles = [item["title"] for item in perf["recommendations"]]
        self.assertIn("Latest trading window recovering under tightened review", titles)
        self.assertNotIn("Latest trading window has negative expectancy", titles)
        self.assertFalse(any(item.get("task") for item in perf["recommendations"] if item["agent"] == "strategy_builder"))
        self.assertFalse(any(item["severity"] in {"warn", "high"} for item in perf["recommendations"] if item["agent"] == "strategy_builder"))
        self.assertFalse(any("latest losing trade window" in item.get("task", "") for item in review["cmuxHandoff"]))

    def test_periodic_review_downgrades_improving_demo_exploration_window(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "demoAutoMinConfidence": 0.55,
            "demoClearSignalOnly": False,
            "demoQualityMinConfidence": 0.55,
            "demoQualityMinMomentumPoints": 1,
            "demoHigherTimeframeConfirm": False,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
        }
        reports = []
        xau_pnls = [-1.40] * 11 + [0.42] * 5 + [1.0, 1.0, 1.0, 1.02]
        for index, pnl in enumerate(xau_pnls, start=1):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 4500.0, "reason": "mt5_closed_deal"})
        for index in range(120):
            pnl = 0.7 if index % 2 == 0 else -0.8
            reports.append({"symbol": "EURUSD", "mode": "DEMO", "ts": index + 30, "pnl": pnl, "exit": 1.1, "reason": "mt5_closed_deal"})
        previous_pnls = [0.776] * 9 + [-1.061] * 11
        latest_pnls = [0.754] * 8 + [-0.847] * 10 + [0.754, 0.754]
        for index, pnl in enumerate(previous_pnls, start=161):
            reports.append({"symbol": "GBPUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 1.2, "reason": "mt5_closed_deal"})
        for index, pnl in enumerate(latest_pnls, start=181):
            reports.append({"symbol": "GBPUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 1.2, "reason": "mt5_closed_deal"})
        main_forex._STATE["config"] = cfg
        main_forex._STATE["tradeReports"] = reports

        perf = main_forex._periodic_performance_review(cfg, "DEMO")
        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        titles = [item["title"] for item in perf["recommendations"]]
        self.assertIn("Latest trading window improving in DEMO exploration", titles)
        self.assertIn("Worst symbol rebounding under DEMO observation", titles)
        self.assertNotIn("Latest trading window has negative expectancy", titles)
        self.assertNotIn("Worst symbol is dragging the latest window", titles)
        self.assertFalse(any(item.get("task") for item in perf["recommendations"] if item["agent"] in {"strategy_builder", "portfolio_manager"}))
        self.assertFalse(any(item["severity"] in {"warn", "high"} for item in perf["recommendations"] if item["agent"] in {"strategy_builder", "portfolio_manager"}))
        self.assertFalse(any("latest losing trade window" in item.get("task", "") for item in review["cmuxHandoff"]))

    def test_periodic_review_downgrades_substantial_demo_exploration_recovery(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "demoAutoMinConfidence": 0.45,
            "demoQualityMinConfidence": 0.58,
            "demoQualityMinMomentumPoints": 0.1,
            "demoHigherTimeframeConfirm": False,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
        }
        reports = []
        previous_pnls = [0.325] * 4 + [-0.6025] * 16
        latest_pnls = [0.5133] * 6 + [-0.5029] * 14
        for index, pnl in enumerate(previous_pnls, start=1):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 4500.0, "reason": "mt5_closed_deal"})
        for index, pnl in enumerate(latest_pnls, start=21):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 4500.0, "reason": "mt5_closed_deal"})
        main_forex._STATE["config"] = cfg
        main_forex._STATE["tradeReports"] = reports

        perf = main_forex._periodic_performance_review(cfg, "DEMO")
        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        titles = [item["title"] for item in perf["recommendations"]]
        self.assertIn("Latest trading window improving in DEMO exploration", titles)
        self.assertIn("Periodic trade rounds under active mitigation", titles)
        self.assertNotIn("Latest trading window has negative expectancy", titles)
        self.assertNotIn("Periodic trade rounds are below profit target", titles)
        self.assertFalse(any("latest losing trade window" in item.get("task", "") for item in review["cmuxHandoff"]))

    def test_deterioration_memory_backqueues_symbol_and_clears_handoff(self):
        cfg = {
            "symbol": "XAUUSD",
            "watchlist": ["XAUUSD", "XAGUSD", "GBPUSD"],
            "executionMode": "DEMO",
            "demoHigherTimeframeConfirm": True,
            "demoQualityMinConfidence": 0.82,
            "demoQualityMinMomentumPoints": 18,
            "maxOpenOrders": 1,
            "maxOpenPositions": 1,
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
        }
        reports = []
        for index in range(120):
            pnl = 0.7 if index % 2 == 0 else -0.8
            reports.append({"symbol": "GBPUSD", "mode": "DEMO", "ts": index + 1, "pnl": pnl, "exit": 1.2, "reason": "mt5_closed_deal"})
        previous_pnls = [0.8] * 11 + [-0.75] * 9
        latest_pnls = [0.75] * 8 + [-0.8] * 12
        for index, pnl in enumerate(previous_pnls, start=141):
            reports.append({"symbol": "XAGUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 30.0, "reason": "mt5_closed_deal"})
        for index, pnl in enumerate(latest_pnls, start=161):
            reports.append({"symbol": "XAGUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 30.0, "reason": "mt5_closed_deal"})
        main_forex._STATE["config"] = cfg
        main_forex._STATE["tradeReports"] = reports

        profiles = main_forex._refresh_learning_profiles(cfg, "DEMO")
        perf = main_forex._periodic_performance_review(cfg, "DEMO")
        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(profiles["XAGUSD"]["deteriorationMemory"]["active"])
        self.assertEqual(profiles["XAGUSD"]["priority"], "back")
        self.assertIn("latest window deteriorated versus previous", profiles["XAGUSD"]["scoreReasons"])
        self.assertTrue(any(item["title"] == "Deterioration memory active for latest window" for item in perf["recommendations"]))
        self.assertFalse(any(item["title"] == "Win rate deteriorated versus previous window" for item in perf["recommendations"]))
        self.assertFalse(any(item.get("task") == "Add/adjust learning memory so deteriorating symbols are deprioritized until recovery." for item in perf["recommendations"]))
        self.assertFalse(any("deteriorating symbols" in item.get("task", "") for item in review["cmuxHandoff"]))

    def test_active_deterioration_memory_clears_relaxed_exploration_handoffs(self):
        cfg = {
            "symbol": "XAUUSD",
            "watchlist": ["XAUUSD", "GBPUSD"],
            "executionMode": "DEMO",
            "demoAutoMinConfidence": 0.5,
            "demoQualityMinConfidence": 0.5,
            "demoQualityMinMomentumPoints": 0.3,
            "demoHigherTimeframeConfirm": False,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
        }
        reports = []
        for index in range(120):
            pnl = 0.7 if index % 2 == 0 else -0.8
            reports.append({"symbol": "GBPUSD", "mode": "DEMO", "ts": index + 1, "pnl": pnl, "exit": 1.2, "reason": "mt5_closed_deal"})
        previous_pnls = [0.734] * 10 + [-0.806] * 10
        latest_pnls = [0.771] * 8 + [-0.767] * 12
        for index, pnl in enumerate(previous_pnls, start=141):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 4500.0, "reason": "mt5_closed_deal"})
        for index, pnl in enumerate(latest_pnls, start=161):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 4500.0, "reason": "mt5_closed_deal"})
        main_forex._STATE["running"] = True
        main_forex._STATE["learningDecision"] = {"enabled": False, "decision": "deferred_for_demo_exploration"}
        main_forex._STATE["config"] = cfg
        main_forex._STATE["tradeReports"] = reports

        perf = main_forex._periodic_performance_review(cfg, "DEMO")
        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        titles = [item["title"] for item in perf["recommendations"]]
        self.assertTrue(perf["deteriorationMemory"]["active"])
        self.assertIn("Latest trading window managed by deterioration memory", titles)
        self.assertIn("Deterioration memory active for latest window", titles)
        self.assertIn("Worst symbol managed by deterioration memory", titles)
        self.assertNotIn("Latest trading window has negative expectancy", titles)
        self.assertNotIn("Win rate deteriorated versus previous window", titles)
        self.assertNotIn("Worst symbol is dragging the latest window", titles)
        self.assertFalse(any(item.get("task") for item in perf["recommendations"] if item["agent"] in {"strategy_builder", "memory_agent", "portfolio_manager"}))
        self.assertFalse(any("latest losing trade window" in item.get("task", "") for item in review["cmuxHandoff"]))
        self.assertFalse(any("deteriorating symbols" in item.get("task", "") for item in review["cmuxHandoff"]))

    def test_deterioration_memory_uses_profit_factor_drop_even_when_win_rate_is_flat(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "demoAutoMinConfidence": 0.5,
            "demoHigherTimeframeConfirm": False,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
        }
        previous_pnls = [0.83] * 5 + [-0.52] * 15
        latest_pnls = [0.18] * 5 + [-0.57] * 15
        main_forex._STATE["running"] = True
        main_forex._STATE["tradeReports"] = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": index + 1, "pnl": pnl, "exit": 4500.0, "reason": "mt5_closed_deal"}
            for index, pnl in enumerate(previous_pnls + latest_pnls)
        ]

        perf = main_forex._periodic_performance_review(cfg, "DEMO")

        self.assertEqual(perf["latest"]["winRatePct"], perf["previous"]["winRatePct"])
        self.assertTrue(perf["deteriorationMemory"]["active"])
        self.assertGreaterEqual(perf["deteriorationMemory"]["profitFactorDrop"], 0.25)
        self.assertTrue(any(item["title"] == "Latest trading window managed by deterioration memory" for item in perf["recommendations"]))

    def test_relaxed_exploration_config_infers_intent_when_decision_is_stale(self):
        cfg = {
            "symbol": "XAUUSD",
            "watchlist": ["XAUUSD", "GBPUSD"],
            "executionMode": "DEMO",
            "demoAutoMinConfidence": 0.5,
            "demoQualityMinConfidence": 0.5,
            "demoQualityMinMomentumPoints": 0.3,
            "demoHigherTimeframeConfirm": False,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
        }
        reports = []
        for index in range(120):
            pnl = 0.7 if index % 2 == 0 else -0.8
            reports.append({"symbol": "GBPUSD", "mode": "DEMO", "ts": index + 1, "pnl": pnl, "exit": 1.2, "reason": "mt5_closed_deal"})
        previous_pnls = [0.734] * 9 + [-0.806] * 11
        latest_pnls = [0.743] * 9 + [-0.68] * 11
        for index, pnl in enumerate(previous_pnls, start=141):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 4500.0, "reason": "mt5_closed_deal"})
        for index, pnl in enumerate(latest_pnls, start=161):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": index, "pnl": pnl, "exit": 4500.0, "reason": "mt5_closed_deal"})
        main_forex._STATE["running"] = True
        main_forex._STATE["learningDecision"] = {"enabled": True, "decision": "stale_auto_learn_state"}
        main_forex._STATE["agentState"] = {
            "agents": {
                "market_analyst": {"runs": 10},
                "strategy_builder": {"runs": 4},
                "lot_sizing_agent": {"runs": 4},
                "risk_manager": {"runs": 4},
                "execution_agent": {"runs": 0},
                "position_guardian": {"runs": 4},
                "memory_agent": {"runs": 4},
            }
        }
        main_forex._STATE["config"] = cfg
        main_forex._STATE["tradeReports"] = reports

        perf = main_forex._periodic_performance_review(cfg, "DEMO")
        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        perf_titles = [item["title"] for item in perf["recommendations"]]
        issue_titles = [item["title"] for item in review["issues"]]
        self.assertIn("Latest trading window improving in DEMO exploration", perf_titles)
        self.assertIn("Worst symbol managed by deterioration memory", perf_titles)
        self.assertIn("Learning proposal intentionally deferred", issue_titles)
        self.assertIn("Strategy in DEMO exploration", issue_titles)
        self.assertNotIn("Agent has not run yet", issue_titles)
        self.assertNotIn("Learning proposal not fully applied", issue_titles)
        self.assertNotIn("Strategy performance is weak", issue_titles)
        self.assertFalse(any("latest losing trade window" in item.get("task", "") for item in review["cmuxHandoff"]))

    def test_supervisor_accepts_nested_multi_symbol_deal_sync(self):
        cfg = {"symbol": "XAUUSD", "executionMode": "DEMO"}
        main_forex._STATE["tradeReports"] = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 1, "pnl": 1.0, "exit": 4500.0, "reason": "mt5_closed_deal"}
        ]
        main_forex._STATE["lastClosedDealSync"] = {
            "ok": True,
            "symbols": ["XAUUSD", "EURUSD"],
            "results": [{"ok": True, "deals": 2}, {"ok": True, "deals": 0}],
        }
        stats = main_forex._closed_trade_stats("DEMO")

        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, stats)

        self.assertNotIn("Closed trades exist but MT5 deal sync is empty", [issue["title"] for issue in review["issues"]])

    def test_aggregate_guardian_reports_counts_all_symbols(self):
        out = main_forex._aggregate_guardian_reports(
            [
                {"ok": True, "closed": 0, "monitored": [{"ticket": 1, "symbol": "XAUUSD"}]},
                {"ok": True, "closed": 1, "monitored": [{"ticket": 2, "symbol": "EURUSD"}]},
            ],
            "closed",
        )

        self.assertTrue(out["ok"])
        self.assertEqual(out["closed"], 1)
        self.assertEqual(out["monitoredCount"], 2)

    def test_portfolio_spacing_allows_required_slot_fill_in_clear_signal_mode(self):
        calls = []

        class FakeBroker:
            def place_market_order(self, symbol, side, volume, stop_loss=None, take_profit=None):
                calls.append({"symbol": symbol, "side": side, "volume": volume, "stopLoss": stop_loss, "takeProfit": take_profit})
                return {"ok": True, "request": {"price": 1.2345}}

            def shutdown(self):
                return None

        cfg = {
            "executionMode": "DEMO",
            "portfolioFixedSlots": True,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 5,
            "maxOpenPositions": 5,
            "maxExposureLots": 0.05,
            "portfolioMinSecondsBetweenEntries": 45,
            "intervalSec": 10,
            "autoDemoTrading": True,
            "marketScan": True,
            "baseLot": 0.01,
            "lotSize": 0.01,
            "pipSize": 0.0001,
            "demoClearSignalOnly": True,
        }
        main_forex._STATE["running"] = True
        main_forex._STATE["lastPortfolioEntryAt"] = int(time.time())
        open_positions = [{"ticket": 1, "symbol": "GBPUSD", "volume": 0.01, "profit": 0.0, "type": 0}]
        signal = {"ok": True, "symbol": "USDCHF", "side": "BUY", "confidence": 0.8, "momentumPoints": 3, "scoreGap": 2.0, "breakout": False, "regime": "trend", "reason": "test"}
        market = {"candles": [{"high": 1.235, "low": 1.234} for _ in range(12)], "tick": {"bid": 1.2344, "ask": 1.2346}}

        with mock.patch.object(main_forex, "_autonomous_signal_from_market", return_value=signal):
            with mock.patch.object(main_forex, "_symbol_entry_opportunity_guard", return_value={"ok": True, "probability": 0.8, "score": 2.0, "scoreGap": 2.0, "volatilityPoints": 1.0}):
                with mock.patch.object(main_forex, "_professional_strategy_guard", return_value={"ok": True, "enabled": False}):
                    with mock.patch.object(main_forex, "_higher_timeframe_confirmation", return_value={"ok": True, "enabled": False}):
                        with mock.patch.object(main_forex, "_strategy_quality_guard", return_value={"ok": True}):
                            with mock.patch.object(main_forex, "_adaptive_lot_size", return_value={"ok": True, "recommendedLot": 0.01, "multiplier": 1.0}):
                                with mock.patch.object(main_forex, "_fee_aware_order_economics_guard", return_value={"ok": True, "adjustedTakeProfitPips": 0.0}):
                                    with mock.patch.object(main_forex, "_tp_sl_from_market", return_value=(1.236, 1.233)):
                                        with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
                                            main_forex._maybe_open_autonomous_mt5_trade(cfg, "USDCHF", "DEMO", open_positions, market, {"trade_mode": 0})

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["symbol"], "USDCHF")
        self.assertNotEqual((main_forex._STATE.get("botNotice") or {}).get("code"), "portfolio_entry_spacing")

    def test_clear_signal_guard_blocks_weak_calm_entry(self):
        out = main_forex._clear_signal_entry_guard(
            {
                "demoClearSignalOnly": True,
                "demoClearSignalMinConfidence": 0.72,
                "demoClearSignalMinProbability": 0.74,
                "demoClearSignalMinOpportunityScore": 1.45,
                "demoClearSignalMinMomentumPoints": 2.0,
                "demoClearSignalMinScoreGap": 1.1,
                "demoClearSignalBlockCalmEntries": True,
            },
            "USDJPY",
            {"confidence": 0.68, "momentumPoints": 0.8, "scoreGap": 0.9, "breakout": False, "regime": "trend"},
            {"probability": 0.7, "score": 1.2, "scoreGap": 0.9, "calmMarket": True},
            {"ok": True, "enabled": True},
        )

        self.assertFalse(out["ok"])
        self.assertIn("calm market requires breakout", out["reasons"])

    def test_clear_signal_guard_allows_strong_calm_after_long_idle(self):
        main_forex._STATE["lastPortfolioEntryAt"] = int(time.time()) - 2400
        out = main_forex._clear_signal_entry_guard(
            {
                "demoClearSignalOnly": True,
                "demoClearSignalMinConfidence": 0.72,
                "demoClearSignalMinProbability": 0.74,
                "demoClearSignalMinOpportunityScore": 1.45,
                "demoClearSignalMinMomentumPoints": 2.0,
                "demoClearSignalMinScoreGap": 1.1,
                "demoClearSignalBlockCalmEntries": True,
                "demoClearSignalAllowStrongCalmAfterIdle": True,
                "demoClearSignalStrongCalmMinProbability": 0.9,
                "demoClearSignalStrongCalmMinOpportunityScore": 4.0,
                "demoClearSignalStrongCalmMinMomentumPoints": 6.0,
                "supervisorEntryContinuityMaxIdleSec": 900,
            },
            "XAUUSD",
            {"confidence": 0.93, "momentumPoints": 8.9, "scoreGap": 7.0, "breakout": False, "regime": "trend"},
            {"probability": 0.95, "score": 5.39, "scoreGap": 7.0, "calmMarket": True},
            {"ok": True, "enabled": True, "confirmations": 2, "required": 2},
        )

        self.assertTrue(out["ok"])
        self.assertTrue(out["strongCalmAllowed"])
        self.assertNotIn("calm market requires breakout", out["reasons"])

    def test_clear_signal_guard_allows_professional_calm_after_long_idle(self):
        main_forex._STATE["lastPortfolioEntryAt"] = int(time.time()) - 2400
        out = main_forex._clear_signal_entry_guard(
            {
                "demoClearSignalOnly": True,
                "demoClearSignalMinConfidence": 0.72,
                "demoClearSignalMinProbability": 0.74,
                "demoClearSignalMinOpportunityScore": 1.45,
                "demoClearSignalMinMomentumPoints": 2.0,
                "demoClearSignalMinScoreGap": 1.1,
                "demoClearSignalBlockCalmEntries": True,
                "demoClearSignalAllowProfessionalCalmAfterIdle": True,
                "demoClearSignalProfessionalCalmMinProbability": 0.82,
                "demoClearSignalProfessionalCalmMinOpportunityScore": 2.4,
                "demoClearSignalProfessionalCalmMinMomentumPoints": 3.0,
                "supervisorEntryContinuityMaxIdleSec": 900,
            },
            "GBPUSD",
            {
                "confidence": 0.78,
                "momentumPoints": 3.8,
                "scoreGap": 2.0,
                "breakout": False,
                "regime": "trend",
                "professionalGuard": {"ok": True, "playbook": "H1/M15/M5/M1"},
            },
            {"probability": 0.84, "score": 2.7, "scoreGap": 2.0, "calmMarket": True},
            {"ok": True, "enabled": True, "confirmations": 1, "required": 2, "fallback": "professional_playbook_after_idle"},
        )

        self.assertTrue(out["ok"])
        self.assertTrue(out["professionalCalmAllowed"])
        self.assertNotIn("calm market requires breakout", out["reasons"])

    def test_clear_signal_guard_allows_htf_calm_after_idle_sampling(self):
        main_forex._STATE["lastPortfolioEntryAt"] = int(time.time()) - 300
        out = main_forex._clear_signal_entry_guard(
            {
                "demoClearSignalOnly": True,
                "demoClearSignalMinConfidence": 0.72,
                "demoClearSignalMinProbability": 0.74,
                "demoClearSignalMinOpportunityScore": 1.45,
                "demoClearSignalMinMomentumPoints": 2.0,
                "demoClearSignalMinScoreGap": 1.1,
                "demoClearSignalBlockCalmEntries": True,
                "demoClearSignalAllowHtfCalmAfterIdle": True,
                "demoClearSignalProfessionalCalmMinProbability": 0.78,
                "demoClearSignalProfessionalCalmMinOpportunityScore": 2.0,
                "demoClearSignalProfessionalCalmMinMomentumPoints": 1.5,
                "supervisorEntryContinuityMaxIdleSec": 0,
            },
            "EURUSD",
            {
                "confidence": 0.76,
                "momentumPoints": 2.1,
                "scoreGap": 2.4,
                "breakout": False,
                "regime": "trend",
            },
            {"probability": 0.88, "score": 2.6, "scoreGap": 2.4, "calmMarket": True},
            {"ok": True, "enabled": True, "confirmations": 2, "required": 2},
        )

        self.assertTrue(out["ok"])
        self.assertTrue(out["htfCalmAllowed"])
        self.assertNotIn("calm market requires breakout", out["reasons"])

    def test_clear_signal_guard_idle_sampling_bypasses_range_breakout_gate(self):
        main_forex._STATE["lastPortfolioEntryAt"] = int(time.time()) - 2400
        out = main_forex._clear_signal_entry_guard(
            {
                "demoClearSignalOnly": True,
                "demoClearSignalMinConfidence": 0.72,
                "demoClearSignalMinProbability": 0.74,
                "demoClearSignalMinOpportunityScore": 1.45,
                "demoClearSignalMinMomentumPoints": 2.0,
                "demoClearSignalMinScoreGap": 1.1,
                "demoClearSignalBlockCalmEntries": True,
                "demoClearSignalBlockRangeEntries": True,
                "demoClearSignalAllowHtfCalmAfterIdle": True,
                "demoClearSignalProfessionalCalmMinConfidence": 0.68,
                "demoClearSignalProfessionalCalmMinProbability": 0.78,
                "demoClearSignalProfessionalCalmMinOpportunityScore": 2.0,
                "demoClearSignalProfessionalCalmMinMomentumPoints": 1.5,
                "supervisorEntryContinuityMaxIdleSec": 0,
            },
            "AUDUSD",
            {
                "confidence": 0.692,
                "momentumPoints": 3.3,
                "scoreGap": 5.0,
                "breakout": False,
                "regime": "range",
            },
            {"probability": 0.815, "score": 3.728, "scoreGap": 5.0, "calmMarket": True},
            {"ok": True, "enabled": True, "confirmations": 1, "required": 2, "fallback": "professional_playbook_after_idle"},
        )

        self.assertTrue(out["ok"])
        self.assertTrue(out["htfCalmAllowed"])
        self.assertNotIn("confidence 0.69 < 0.72", out["reasons"])
        self.assertNotIn("calm market requires breakout", out["reasons"])
        self.assertNotIn("range market requires breakout", out["reasons"])

    def test_symbol_opportunity_guard_blocks_supervisor_quarantined_symbol(self):
        out = main_forex._symbol_entry_opportunity_guard(
            {
                "supervisorSymbolBias": {
                    "XAGUSD": {
                        "action": "lock",
                        "scorePenalty": 999,
                        "until": int(time.time()) + 3600,
                        "reason": "latest window 0 wins",
                    }
                },
                "symbolOpportunityMinProbability": 0.74,
                "symbolOpportunityMinScore": 1.45,
                "symbolOpportunityMinMomentumPoints": 2.0,
            },
            "XAGUSD",
            {"side": "BUY", "confidence": 0.92, "momentumPoints": 12.0, "scoreGap": 4.0, "breakout": True},
            {"candles": [{"high": 73.1, "low": 73.0} for _ in range(12)]},
        )

        self.assertFalse(out["ok"])
        self.assertEqual(out["code"], "symbol_quarantined")

    def test_portfolio_rotation_closes_weak_symbol_when_stronger_replacement_exists(self):
        main_forex._STATE["learningProfilesBySymbol"] = {
            "XAGUSD": {"symbol": "XAGUSD", "learningScore": 10, "priority": "back", "learningTier": "cold", "latestWindow": {"weak": True}},
            "GBPUSD": {"symbol": "GBPUSD", "learningScore": 78, "priority": "front", "learningTier": "hot", "latestWindow": {"weak": False}},
        }
        positions = [
            {"ticket": 1, "symbol": "XAGUSD", "volume": 0.01, "profit": -0.25, "type": 0},
            {"ticket": 2, "symbol": "EURUSD", "volume": 0.01, "profit": 0.1, "type": 0},
            {"ticket": 3, "symbol": "XAUUSD", "volume": 0.01, "profit": 0.2, "type": 0},
        ]

        class FakeBroker:
            def close_position(self, position, comment=""):
                return {"ok": True, "ticket": position.get("ticket"), "comment": comment}

            def shutdown(self):
                return None

        cfg = {
            "executionMode": "DEMO",
            "portfolioRotationEnabled": True,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "portfolioRotationReplacementMinScore": 55,
            "portfolioRotationMinLosingPnlUsd": -0.15,
        }
        with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
            with mock.patch.object(main_forex, "_safe_mt5_market_snapshot", return_value={"candles": [{"high": 76.6, "low": 76.5} for _ in range(12)]}):
                out = main_forex._portfolio_rotate_weak_positions(cfg, "DEMO", positions, ["GBPUSD", "EURUSD", "XAUUSD", "XAGUSD"])

        self.assertTrue(out["ok"])
        self.assertEqual(out["closed"], 1)
        self.assertEqual(out["results"][0]["symbol"], "XAGUSD")
        self.assertIn("GBPUSD", out["replacementQueue"])

    def test_portfolio_rotation_closes_duplicate_symbol_to_diversify_slots(self):
        main_forex._STATE["learningProfilesBySymbol"] = {}
        positions = [
            {"ticket": 1, "symbol": "GBPUSD", "volume": 0.01, "profit": -0.31, "type": 1},
            {"ticket": 2, "symbol": "GBPUSD", "volume": 0.01, "profit": -0.22, "type": 1},
            {"ticket": 3, "symbol": "GBPUSD", "volume": 0.01, "profit": -0.12, "type": 1},
        ]

        class FakeBroker:
            def close_position(self, position, comment=""):
                return {"ok": True, "ticket": position.get("ticket"), "comment": comment}

            def shutdown(self):
                return None

        cfg = {
            "executionMode": "DEMO",
            "portfolioRotationEnabled": True,
            "portfolioDiversifySymbols": True,
            "maxPositionsPerSymbol": 1,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "portfolioRotationReplacementMinScore": 45,
            "portfolioRotationMinLosingPnlUsd": -0.15,
        }
        with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
            with mock.patch.object(main_forex, "_safe_mt5_market_snapshot", return_value={"candles": [{"high": 1.2, "low": 1.1} for _ in range(12)]}):
                out = main_forex._portfolio_rotate_weak_positions(cfg, "DEMO", positions, ["EURUSD", "XAUUSD", "GBPUSD", "XAGUSD"])

        self.assertTrue(out["ok"])
        self.assertEqual(out["closed"], 1)
        self.assertEqual(out["results"][0]["symbol"], "GBPUSD")
        self.assertTrue(out["results"][0]["duplicateSymbol"])
        self.assertIn("EURUSD", out["replacementQueue"])

    def test_portfolio_rotation_uses_any_watchlist_replacement_for_duplicate_symbols(self):
        main_forex._STATE["learningProfilesBySymbol"] = {}
        positions = [
            {"ticket": 1, "symbol": "XAGUSD", "volume": 0.01, "profit": 0.31, "type": 0},
            {"ticket": 2, "symbol": "XAGUSD", "volume": 0.01, "profit": 0.32, "type": 0},
            {"ticket": 3, "symbol": "XAGUSD", "volume": 0.01, "profit": 0.33, "type": 0},
        ]

        class FakeBroker:
            def close_position(self, position, comment=""):
                return {"ok": True, "ticket": position.get("ticket"), "comment": comment}

            def shutdown(self):
                return None

        cfg = {
            "executionMode": "DEMO",
            "portfolioRotationEnabled": True,
            "portfolioDiversifySymbols": True,
            "maxPositionsPerSymbol": 1,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "portfolioRotationReplacementMinScore": 90,
        }
        with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
            with mock.patch.object(main_forex, "_safe_mt5_market_snapshot", return_value={"candles": [{"high": 25.1, "low": 25.0} for _ in range(12)]}):
                out = main_forex._portfolio_rotate_weak_positions(cfg, "DEMO", positions, ["XAGUSD", "EURUSD", "GBPUSD"])

        self.assertTrue(out["ok"])
        self.assertEqual(out["closed"], 1)
        self.assertEqual(out["results"][0]["symbol"], "XAGUSD")
        self.assertIn("EURUSD", out["replacementQueue"])

    def test_supervisor_downgrades_weak_strategy_when_mitigations_applied(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "demoHigherTimeframeConfirm": True,
            "demoHigherTimeframes": ["M5", "M15"],
            "demoQualityMinConfidence": 0.82,
            "demoQualityMinMomentumPoints": 18,
            "maxOpenOrders": 1,
            "maxOpenPositions": 1,
        }
        reports = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": i, "pnl": -1.0, "exit": 4500.0, "reason": "mt5_closed_deal"}
            for i in range(30)
        ]
        main_forex._STATE["tradeReports"] = reports
        stats = main_forex._closed_trade_stats("DEMO")

        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, stats)

        self.assertFalse(any(issue["title"] == "Strategy performance is weak" for issue in review["issues"]))
        self.assertTrue(any(issue["title"] == "Strategy under tightened DEMO review" for issue in review["issues"]))

    def test_supervisor_records_learning_deferral_during_demo_exploration(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "demoAutoMinConfidence": 0.68,
            "demoQualityMinConfidence": 0.68,
            "demoQualityMinMomentumPoints": 4,
            "demoBlockRangeEntries": False,
            "demoHigherTimeframeConfirm": False,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "maxExposureLots": 0.03,
            "supervisorDailyReviewMinTrades": 999,
        }
        reports = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": i, "pnl": -1.0, "exit": 4500.0, "reason": "mt5_closed_deal"}
            for i in range(40)
        ]
        main_forex._STATE["running"] = True
        main_forex._STATE["learningDecision"] = {"enabled": False, "decision": "deferred_for_demo_exploration"}
        main_forex._STATE["lastClosedDealSync"] = {"ok": True, "deals": 80, "synced": 40}
        main_forex._STATE["botNotice"] = {"code": "waiting_interval"}
        main_forex._STATE["tradeReports"] = reports
        stats = main_forex._closed_trade_stats("DEMO")

        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, stats)

        titles = [issue["title"] for issue in review["issues"]]
        self.assertIn("Learning proposal intentionally deferred", titles)
        self.assertIn("Strategy in DEMO exploration", titles)
        self.assertNotIn("Learning proposal not fully applied", titles)
        self.assertNotIn("Strategy performance is weak", titles)
        self.assertFalse(any("latest losing trade window" in item["task"] for item in review["cmuxHandoff"]))
        self.assertTrue(any(item.get("issueType") == "weak_payoff_ratio" for item in review["autoAction"]["actions"]))
        self.assertTrue(review["ok"])
        self.assertIn("performanceReview", review)

    def test_supervisor_records_intentional_multi_symbol_review(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "demoAutoMinConfidence": 0.72,
            "demoQualityMinConfidence": 0.72,
            "demoQualityMinMomentumPoints": 12,
            "demoBlockRangeEntries": True,
            "demoHigherTimeframeConfirm": True,
            "demoHigherTimeframes": ["M5", "M15"],
            "demoHigherTimeframeMinConfirmations": 1,
            "demoHigherTimeframeMinConfidence": 0.72,
            "professionalStrategyEnabled": True,
            "professionalAllowMultiSymbolPositions": True,
            "multiSymbolScanEnabled": True,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "maxExposureLots": 0.03,
            "supervisorDailyReviewMinTrades": 999,
            "demoLossCooldownAfter": 99,
            "demoLossCooldownSec": 60,
            "demoMaxLossStreakStop": 30,
            "demoReversalCooldownSec": 180,
        }
        reports = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": i, "pnl": -1.0, "exit": 4500.0, "reason": "mt5_closed_deal"}
            for i in range(40)
        ]
        main_forex._STATE["running"] = True
        main_forex._STATE["learningDecision"] = {"enabled": False, "decision": "deferred_for_professional_v1"}
        main_forex._STATE["lastClosedDealSync"] = {"ok": True, "deals": 80, "synced": 40}
        main_forex._STATE["botNotice"] = {"code": "waiting_interval"}
        main_forex._STATE["tradeReports"] = reports
        stats = main_forex._closed_trade_stats("DEMO")

        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, stats)

        titles = [issue["title"] for issue in review["issues"]]
        self.assertIn("Learning proposal intentionally adapted", titles)
        self.assertIn("Strategy in DEMO multi-symbol review", titles)
        self.assertNotIn("Learning proposal not fully applied", titles)
        self.assertNotIn("Strategy performance is weak", titles)
        self.assertFalse(any("latest losing trade window" in item["task"] for item in review["cmuxHandoff"]))
        self.assertTrue(any(item.get("issueType") == "weak_payoff_ratio" for item in review["autoAction"]["actions"]))
        self.assertTrue(review["ok"])
        self.assertIn("supervisor note(s)", review["summary"])

    def test_supervisor_records_intentional_clear_signal_review_without_professional_flags(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "demoAutoMinConfidence": 0.72,
            "demoQualityMinConfidence": 0.72,
            "demoQualityMinMomentumPoints": 3.0,
            "demoBlockRangeEntries": True,
            "demoClearSignalOnly": True,
            "demoHigherTimeframeConfirm": True,
            "demoHigherTimeframes": ["M5", "M15"],
            "demoHigherTimeframeMinConfirmations": 2,
            "demoHigherTimeframeMinConfidence": 0.72,
            "maxOpenOrders": 5,
            "maxOpenPositions": 5,
            "maxExposureLots": 0.05,
            "demoLossCooldownAfter": 2,
            "demoLossCooldownSec": 600,
            "demoMaxLossStreakStop": 4,
            "demoReversalCooldownSec": 180,
            "guardianEarlyExitMinConfidence": 0.76,
            "guardianEarlyExitMinMomentumPoints": 12,
        }
        reports = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": i, "pnl": -0.4, "exit": 4500.0, "reason": "mt5_closed_deal"}
            for i in range(40)
        ]
        main_forex._STATE["running"] = True
        main_forex._STATE["learningDecision"] = {"enabled": False, "decision": "user_clear_signal_review"}
        main_forex._STATE["lastClosedDealSync"] = {"ok": True, "deals": 40, "synced": 40}
        main_forex._STATE["botNotice"] = {"code": "clear_signal_required"}
        main_forex._STATE["tradeReports"] = reports

        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        titles = [issue["title"] for issue in review["issues"]]
        self.assertIn("Learning proposal intentionally adapted", titles)
        self.assertIn("Strategy in DEMO multi-symbol review", titles)
        self.assertNotIn("Learning proposal not fully applied", titles)
        self.assertEqual(cfg["maxOpenOrders"], 5)
        self.assertEqual(cfg["maxOpenPositions"], 5)

    def test_periodic_performance_review_finds_worst_symbol_and_handoff(self):
        cfg = {"symbol": "XAUUSD", "executionMode": "DEMO", "supervisorReviewWindowTrades": 10, "supervisorReviewMinTrades": 5}
        reports = []
        for index in range(10):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": index + 1, "pnl": 0.6, "exit": 4500.0, "reason": "mt5_closed_deal"})
        for index in range(10):
            reports.append({"symbol": "EURUSD", "mode": "DEMO", "ts": index + 20, "pnl": -0.7, "exit": 1.1, "reason": "mt5_closed_deal"})
        main_forex._STATE["tradeReports"] = reports

        perf = main_forex._periodic_performance_review(cfg, "DEMO")
        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertEqual(perf["latest"]["trades"], 10)
        self.assertLess(perf["latest"]["realizedPnl"], 0)
        self.assertTrue(any(item["title"] == "Worst symbol is dragging the latest window" for item in perf["recommendations"]))
        self.assertFalse(any("EURUSD" in item["task"] for item in review["cmuxHandoff"]))
        self.assertIn("EURUSD", review["autoAction"]["delta"]["supervisorSymbolBias"])

    def test_supervisor_round_review_delegates_below_target_tuning(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 5,
            "supervisorReviewMinTrades": 5,
            "supervisorTargetWinRatePct": 50.0,
            "supervisorTargetProfitFactor": 1.2,
            "supervisorTargetMinRoundPnlUsd": 0.2,
            "supervisorDelegationMaxDeltaKeys": 20,
            "guardianAdaptiveTakeProfitCapturePct": 0.9,
            "guardianProfitGivebackPct": 0.45,
            "guardianProfitTrailMinPnlUsd": 0.45,
            "demoQualityMinConfidence": 0.5,
            "entryMinScoreGap": 0.5,
        }
        reports = []
        for index in range(10):
            reports.append(
                {
                    "symbol": "XAUUSD",
                    "mode": "DEMO",
                    "ts": index + 1,
                    "pnl": 0.35 if index in {1, 6} else -0.55,
                    "exit": 4500.0,
                    "reason": "mt5_closed_deal",
                }
            )
        main_forex._STATE["tradeReports"] = reports

        perf = main_forex._periodic_performance_review(cfg, "DEMO")
        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertEqual(len(perf["rounds"]), 2)
        self.assertTrue(perf["rounds"][-1]["goalGap"]["belowTarget"])
        self.assertTrue(any(item["title"] == "Periodic trade rounds are below profit target" for item in perf["recommendations"]))
        methods = [item["method"] for item in review["autoAction"]["actions"]]
        self.assertIn("autoTuneRoundPayoff", methods)
        self.assertIn("autoTuneRoundEntryQuality", methods)
        self.assertIn("recordRoundEvaluation", methods)
        self.assertIn("supervisorRoundMemory", cfg)
        self.assertFalse(any(item["issueTitle"] == "Periodic trade rounds are below profit target" for item in review["cmuxHandoff"]))

    def test_supervisor_daily_win_rate_tunes_agents_and_preserves_3_to_5_slots(self):
        now = int(time.time())
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 5,
            "supervisorReviewMinTrades": 5,
            "supervisorDailyReviewMinTrades": 1,
            "supervisorDailyWinRateTargetPct": 51.0,
            "supervisorDelegationMaxDeltaKeys": 40,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "baseLot": 0.01,
            "maxLot": 0.01,
            "maxExposureLots": 0.03,
            "symbolOpportunityMinProbability": 0.6,
            "symbolOpportunityMinScore": 0.95,
            "guardianProfitGivebackPct": 0.55,
        }
        main_forex._STATE["tradeReports"] = [
            {
                "symbol": "XAUUSD" if index % 2 else "GBPUSD",
                "mode": "DEMO",
                "ts": now - index * 60,
                "pnl": 0.4 if index in {1, 4} else -0.45,
                "exit": 4500.0,
                "reason": "mt5_closed_deal",
            }
            for index in range(1)
        ]
        main_forex._STATE["lastClosedDealSync"] = {"ok": True, "deals": len(main_forex._STATE["tradeReports"])}

        perf = main_forex._periodic_performance_review(cfg, "DEMO")
        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(perf["dailyGoal"]["belowTarget"])
        self.assertTrue(any(item["title"] == "Daily win rate is below target" for item in perf["recommendations"]))
        methods = [item["method"] for item in review["autoAction"]["actions"]]
        self.assertIn("tuneDailyWinRateEntryQuality", methods)
        self.assertIn("maintainDiversifiedSlots", methods)
        self.assertIn("tuneDailyExitCapture", methods)
        self.assertIn("keepBaseLotDiversification", methods)
        self.assertIn("recordDailyWinRateRound", methods)
        self.assertEqual(cfg["portfolioTargetOpenOrders"], 3)
        self.assertEqual(cfg["maxOpenOrders"], 5)
        self.assertEqual(cfg["maxOpenPositions"], 5)
        self.assertGreaterEqual(cfg["maxExposureLots"], 0.05)
        self.assertTrue(cfg["demoHtfAllowProfessionalPlaybookFallbackAfterIdle"])
        self.assertTrue(cfg["demoClearSignalAllowProfessionalCalmAfterIdle"])
        self.assertGreaterEqual(cfg["professionalM1MinConfidence"], 0.74)
        self.assertGreaterEqual(cfg["professionalM1MinMomentumPoints"], 3.0)
        self.assertFalse(review["autoAction"]["requiresCmux"])
        self.assertFalse(review["cmuxHandoff"])

    def test_supervisor_does_not_send_repeated_daily_win_rate_to_cmux_while_tuning_active(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorDailyReviewMinTrades": 1,
            "supervisorDailyWinRateTargetPct": 51.0,
            "supervisorReviewWindowTrades": 5,
            "supervisorReviewMinTrades": 5,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 5,
            "maxOpenPositions": 5,
            "baseLot": 0.01,
            "maxLot": 0.01,
            "maxExposureLots": 0.05,
            "supervisorDelegationMaxDeltaKeys": 80,
        }
        main_forex._STATE["lastSupervisorAutoAction"] = {
            "handledIssueTitles": ["Daily win rate is below target"],
            "actions": [{"issueType": "daily_win_rate_below_target", "method": "tuneDailyWinRateEntryQuality"}],
        }
        now = int(time.time())
        main_forex._STATE["tradeReports"] = [
            {
                "symbol": "GBPUSD" if index % 2 else "XAUUSD",
                "mode": "DEMO",
                "ts": now - index * 60,
                "pnl": 0.25 if index in {1, 4, 7} else -0.3,
                "exit": 1.2,
                "reason": "mt5_closed_deal",
            }
            for index in range(12)
        ]
        main_forex._STATE["lastClosedDealSync"] = {"ok": True, "deals": len(main_forex._STATE["tradeReports"])}

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(any(issue["title"] == "Daily win rate is below target" for issue in review["issues"]))
        self.assertFalse(review["autoAction"]["requiresCmux"])
        self.assertFalse(any(item.get("issueTitle") == "Daily win rate is below target" for item in review["cmuxHandoff"]))

    def test_supervisor_severe_latest_window_activates_strict_mitigation(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
            "supervisorDelegationMaxDeltaKeys": 40,
            "portfolioFixedSlots": True,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 5,
            "maxOpenPositions": 5,
            "supervisorCalmFillSlotsEnabled": False,
            "demoClearSignalOnly": True,
            "portfolioMinSecondsBetweenEntries": 45,
            "baseLot": 0.01,
            "maxLot": 0.03,
            "maxExposureLots": 0.09,
            "symbolOpportunityMinProbability": 0.56,
            "symbolOpportunityMinScore": 0.75,
            "entryMinScoreGap": 0.5,
            "guardianMaxLossPerPositionUsd": 0.85,
            "forexStopLossPips": 8,
            "silverStopLossPips": 10,
        }
        reports = []
        for index in range(20):
            reports.append(
                {
                    "symbol": "GBPUSD" if index % 2 else "USDCHF",
                    "mode": "DEMO",
                    "ts": index + 1,
                    "pnl": 0.3 if index in {2, 6, 10, 14, 18} else -0.45,
                    "exit": 1.2,
                    "reason": "mt5_closed_deal",
                }
            )
        for index in range(20):
            symbol = "XAGUSD" if index < 10 else "USDJPY"
            reports.append(
                {
                    "symbol": symbol,
                    "mode": "DEMO",
                    "ts": index + 100,
                    "pnl": 0.18 if index in {1, 7, 13, 19} else -0.62,
                    "exit": 1.2,
                    "reason": "mt5_closed_deal",
                }
            )
        main_forex._STATE["tradeReports"] = reports
        main_forex._STATE["lastClosedDealSync"] = {"ok": True, "deals": len(reports)}
        main_forex._STATE["learningProfilesBySymbol"] = {
            "XAGUSD": {"symbol": "XAGUSD", "learningScore": 25, "priority": "back", "latestWindow": {"weak": True}},
            "USDJPY": {"symbol": "USDJPY", "learningScore": 28, "priority": "back", "latestWindow": {"weak": True}},
            "GBPUSD": {"symbol": "GBPUSD", "learningScore": 55, "priority": "normal", "latestWindow": {"weak": False}},
            "USDCHF": {"symbol": "USDCHF", "learningScore": 52, "priority": "normal", "latestWindow": {"weak": False}},
        }

        perf = main_forex._periodic_performance_review(cfg, "DEMO")
        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(any(item["title"] == "Latest trading window has negative expectancy" for item in perf["recommendations"]))
        self.assertTrue(review["autoAction"]["applied"])
        self.assertFalse(review["autoAction"]["requiresCmux"])
        self.assertTrue(cfg["demoStrictLossControlMode"])
        self.assertTrue(cfg["supervisorDeepExitRefactorActive"])
        self.assertEqual(cfg["portfolioTargetOpenOrders"], 3)
        self.assertEqual(cfg["maxOpenOrders"], 5)
        self.assertEqual(cfg["maxOpenPositions"], 5)
        self.assertFalse(cfg["supervisorCalmFillSlotsEnabled"])
        self.assertEqual(cfg["portfolioMinSecondsBetweenEntries"], 300)
        self.assertEqual(cfg["maxLot"], 0.01)
        self.assertEqual(cfg["maxExposureLots"], 0.05)
        self.assertLessEqual(cfg["guardianMaxLossPerPositionUsd"], 0.55)
        self.assertGreaterEqual(cfg["guardianMaxLossPerPositionUsd"], 0.5)
        self.assertGreaterEqual(cfg["symbolOpportunityMinProbability"], 0.72)
        self.assertIn("XAGUSD", cfg["supervisorSymbolBias"])
        self.assertIn("USDJPY", cfg["supervisorSymbolBias"])
        methods = [item["method"] for item in review["autoAction"]["actions"]]
        self.assertIn("activateSevereWindowEntryFilter", methods)
        self.assertIn("rankSevereLatestWindowSymbols", methods)

    def test_supervisor_applies_m5_m15_confirmation_before_reenabling_symbol_stacks(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorDelegationEnabled": True,
            "supervisorDelegationCooldownSec": 0,
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
            "supervisorDelegationMaxDeltaKeys": 80,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 5,
            "maxOpenPositions": 5,
            "allowStacking": True,
            "portfolioDiversifySymbols": True,
            "onePositionPerSymbol": False,
            "maxPositionsPerSymbol": 3,
            "demoHigherTimeframeConfirm": True,
            "demoHigherTimeframes": ["H1", "M15"],
            "demoHigherTimeframeMinConfirmations": 1,
            "demoHigherTimeframeMinConfidence": 0.68,
            "professionalBiasTimeframes": ["H1", "M15", "M5"],
            "professionalMinTfConfidence": 0.68,
            "demoStackMinConfidence": 0.86,
            "demoStackMinMomentumPoints": 24,
        }
        performance = {
            "latest": {"trades": 20, "wins": 6, "losses": 14, "winRatePct": 30, "realizedPnl": -3.67, "profitFactor": 0.3481, "lossStreak": 2, "winStreak": 0},
            "rounds": [{"round": 70, "trades": 20, "winRatePct": 25, "realizedPnl": -4.4, "profitFactor": 0.18}],
            "symbols": [],
        }

        with mock.patch.object(main_forex, "_save_state"):
            action = main_forex._supervisor_delegation_policy(
                cfg,
                [{"title": "Strategy performance is weak"}, {"title": "Latest trading window has negative expectancy"}],
                performance,
                {},
                {},
            )

        self.assertTrue(action["applied"])
        self.assertEqual(cfg["demoHigherTimeframes"], ["M5", "M15"])
        self.assertEqual(cfg["demoHigherTimeframeMinConfirmations"], 2)
        self.assertGreaterEqual(cfg["demoHigherTimeframeMinConfidence"], 0.72)
        self.assertEqual(cfg["professionalBiasTimeframes"], ["H1", "M15", "M5"])
        self.assertTrue(cfg["onePositionPerSymbol"])
        self.assertEqual(cfg["maxPositionsPerSymbol"], 1)
        self.assertEqual(cfg["portfolioTargetOpenOrders"], 3)
        self.assertEqual(cfg["maxOpenOrders"], 5)
        self.assertTrue(cfg["supervisorM5M15TrendReviewActive"])
        self.assertEqual(cfg["supervisorM5M15TrendReviewBaseline"]["round"]["round"], 70)
        methods = {item["method"] for item in action["actions"]}
        self.assertIn("applyM5M15TrendConfirmation", methods)
        self.assertIn("keepSameSymbolStackingPaused", methods)
        self.assertIn("recordM5M15ReviewBaseline", methods)

    def test_supervisor_skips_collapsed_symbols_and_promotes_positive_reference(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
            "supervisorDelegationMaxDeltaKeys": 40,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 5,
            "maxOpenPositions": 5,
            "demoClearSignalOnly": True,
            "baseLot": 0.01,
            "maxLot": 0.02,
            "maxExposureLots": 0.1,
            "supervisorSymbolBias": {},
            "supervisorLearningFeedbackMonitorEnabled": False,
        }
        reports = []
        for index in range(20):
            reports.append(
                {
                    "symbol": "XAUUSD",
                    "mode": "DEMO",
                    "ts": index + 1,
                    "pnl": 0.45 if index % 2 else -0.2,
                    "exit": 4500.0,
                    "reason": "mt5_closed_deal",
                }
            )
        for index in range(20):
            reports.append(
                {
                    "symbol": "EURUSD",
                    "mode": "DEMO",
                    "ts": index + 100,
                    "pnl": 0.05 if index in {3, 11} else -0.45,
                    "exit": 1.1,
                    "reason": "mt5_closed_deal",
                }
            )
        for index in range(20):
            reports.append(
                {
                    "symbol": "USDJPY",
                    "mode": "DEMO",
                    "ts": index + 200,
                    "pnl": 0.04 if index in {5, 17} else -0.3,
                    "exit": 145.0,
                    "reason": "mt5_closed_deal",
                }
            )
        main_forex._STATE["tradeReports"] = reports
        main_forex._STATE["lastClosedDealSync"] = {"ok": True, "deals": len(reports)}

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 1}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(review["autoAction"]["applied"])
        self.assertIn(cfg["supervisorSymbolBias"]["EURUSD"]["action"], {"skip", "lock", "quarantine"})
        self.assertIn(cfg["supervisorSymbolBias"]["USDJPY"]["action"], {"skip", "lock", "quarantine"})
        self.assertEqual(cfg["supervisorSymbolBias"]["XAUUSD"]["action"], "promote")
        self.assertGreaterEqual(cfg["supervisorSymbolBias"]["XAUUSD"]["scoreBoost"], 40)

    def test_supervisor_promotes_best_survivor_when_no_symbol_is_positive(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
            "supervisorDelegationMaxDeltaKeys": 40,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 5,
            "maxOpenPositions": 5,
            "demoClearSignalOnly": True,
            "baseLot": 0.01,
            "maxLot": 0.02,
            "maxExposureLots": 0.1,
            "supervisorSymbolBias": {
                "XAUUSD": {"action": "reduce", "scorePenalty": 25, "until": int(time.time()) + 1800}
            },
            "supervisorLearningFeedbackMonitorEnabled": False,
        }
        reports = []
        for index in range(20):
            reports.append(
                {
                    "symbol": "XAUUSD",
                    "mode": "DEMO",
                    "ts": index + 1,
                    "pnl": 0.9857 if index in {1, 4, 7, 10, 13, 16, 19} else -0.5854,
                    "exit": 4500.0,
                    "reason": "mt5_closed_deal",
                }
            )
        for index in range(20):
            reports.append(
                {
                    "symbol": "XAGUSD",
                    "mode": "DEMO",
                    "ts": index + 100,
                    "pnl": -0.4385,
                    "exit": 73.0,
                    "reason": "mt5_closed_deal",
                }
            )
        for index in range(20):
            reports.append(
                {
                    "symbol": "USDJPY",
                    "mode": "DEMO",
                    "ts": index + 200,
                    "pnl": 0.05 if index in {5, 17} else -0.148,
                    "exit": 145.0,
                    "reason": "mt5_closed_deal",
                }
            )
        main_forex._STATE["tradeReports"] = reports
        main_forex._STATE["lastClosedDealSync"] = {"ok": True, "deals": len(reports)}

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(review["autoAction"]["applied"])
        self.assertIn(cfg["supervisorSymbolBias"]["XAGUSD"]["action"], {"skip", "lock", "quarantine"})
        self.assertEqual(cfg["supervisorSymbolBias"]["XAUUSD"]["action"], "promote")
        self.assertGreaterEqual(cfg["supervisorSymbolBias"]["XAUUSD"]["scoreBoost"], 22)
        self.assertIn("survivor reference", cfg["supervisorSymbolBias"]["XAUUSD"]["reason"])

    def test_supervisor_boosts_lot_and_multiplier_after_confirmed_win_streak(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorDelegationEnabled": True,
            "supervisorDelegationCooldownSec": 0,
            "supervisorStreakLotTuningEnabled": True,
            "baseLot": 0.01,
            "maxLot": 0.02,
            "maxExposureLots": 0.06,
            "maxOpenOrders": 3,
            "symbolMultiplierProfiles": {
                "XAUUSD": {"multiplierMin": 0.65, "multiplierMax": 2.0, "calmMultiplierFloor": 1.4, "calmMultiplierCeiling": 1.8}
            },
        }
        performance = {
            "latest": {"trades": 20, "wins": 6, "losses": 0, "winStreak": 4, "lossStreak": 0, "realizedPnl": 3.2, "profitFactor": 99.0, "winRatePct": 100},
            "symbols": [],
        }

        with mock.patch.object(main_forex, "_save_state"):
            action = main_forex._supervisor_delegation_policy(cfg, [], performance, {}, {})

        self.assertTrue(action["applied"])
        self.assertEqual(cfg["supervisorStreakLotMode"], "win_boost")
        self.assertGreater(cfg["maxLot"], 0.02)
        self.assertGreater(cfg["maxExposureLots"], 0.06)
        self.assertGreater(cfg["symbolMultiplierProfiles"]["XAUUSD"]["multiplierMax"], 2.0)
        self.assertTrue(any(item["method"] == "boostLotAfterWinStreak" for item in action["actions"]))

    def test_supervisor_throttles_lot_and_multiplier_after_loss_streak(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorDelegationEnabled": True,
            "supervisorDelegationCooldownSec": 0,
            "supervisorStreakLotTuningEnabled": True,
            "baseLot": 0.01,
            "maxLot": 0.04,
            "maxExposureLots": 0.2,
            "maxOpenOrders": 5,
            "symbolMultiplierProfiles": {
                "XAUUSD": {"multiplierMin": 0.65, "multiplierMax": 2.6, "calmMultiplierFloor": 1.8, "calmMultiplierCeiling": 2.4, "highVolatilityThrottleFactor": 0.72}
            },
        }
        performance = {
            "latest": {"trades": 20, "wins": 0, "losses": 5, "winStreak": 0, "lossStreak": 4, "realizedPnl": -2.4, "profitFactor": 0.0, "winRatePct": 0},
            "symbols": [],
        }

        with mock.patch.object(main_forex, "_save_state"):
            action = main_forex._supervisor_delegation_policy(cfg, [], performance, {}, {})

        self.assertTrue(action["applied"])
        self.assertEqual(cfg["supervisorStreakLotMode"], "loss_throttle")
        self.assertLess(cfg["maxLot"], 0.04)
        self.assertLess(cfg["maxExposureLots"], 0.2)
        self.assertLess(cfg["symbolMultiplierProfiles"]["XAUUSD"]["multiplierMax"], 2.6)
        self.assertLessEqual(cfg["symbolMultiplierProfiles"]["XAUUSD"]["highVolatilityThrottleFactor"], 0.58)
        self.assertTrue(any(item["method"] == "throttleLotAfterLossStreak" for item in action["actions"]))

    def test_severe_mitigation_preserves_sampling_loss_stop_after_entry_continuity(self):
        now = int(time.time())
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
            "supervisorDelegationMaxDeltaKeys": 40,
            "portfolioFixedSlots": True,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 5,
            "maxOpenPositions": 5,
            "demoClearSignalOnly": True,
            "demoClearSignalAllowStrongCalmAfterIdle": True,
            "supervisorEntryContinuityLastTuneAt": now - 60,
            "baseLot": 0.01,
            "maxLot": 0.01,
            "maxExposureLots": 0.05,
            "demoLossCooldownAfter": 6,
            "demoLossCooldownSec": 180,
            "demoMaxLossStreakStop": 12,
        }
        main_forex._STATE["tradeReports"] = [
            {
                "symbol": "XAUUSD",
                "mode": "DEMO",
                "ts": index + 1,
                "pnl": 0.1 if index in {3, 11} else -0.35,
                "exit": 4500.0,
                "reason": "mt5_closed_deal",
            }
            for index in range(20)
        ]
        main_forex._STATE["lastClosedDealSync"] = {"ok": True, "deals": 20}

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 1}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(review["autoAction"]["applied"])
        self.assertEqual(cfg["demoLossCooldownAfter"], 6)
        self.assertEqual(cfg["demoLossCooldownSec"], 180)
        self.assertEqual(cfg["demoMaxLossStreakStop"], 12)

    def test_supervisor_profit_capture_review_delegates_agent_tuning_first(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 5,
            "supervisorReviewMinTrades": 5,
            "supervisorTargetWinRatePct": 45.0,
            "supervisorTargetProfitFactor": 1.25,
            "supervisorTargetMinRoundPnlUsd": 0.4,
            "supervisorDelegationMaxDeltaKeys": 20,
            "guardianAdaptiveTakeProfitCapturePct": 0.86,
            "guardianProfitGivebackPct": 0.45,
            "guardianProfitTrailMinPnlUsd": 0.45,
            "baseLot": 0.01,
            "lotSize": 0.01,
            "maxLot": 0.03,
            "maxOpenOrders": 3,
            "maxExposureLots": 0.09,
            "demoQualityMinConfidence": 0.5,
        }
        main_forex._STATE["tradeReports"] = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 1, "pnl": 0.25, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 2, "pnl": 0.25, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 3, "pnl": 0.25, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 4, "pnl": -0.2, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 5, "pnl": -0.2, "exit": 4500.0, "reason": "mt5_closed_deal"},
        ]

        perf = main_forex._periodic_performance_review(cfg, "DEMO")
        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(any(item["title"] == "Profit capture is below target" for item in perf["recommendations"]))
        methods = [item["method"] for item in review["autoAction"]["actions"]]
        self.assertIn("autoTuneProfitCapture", methods)
        self.assertIn("autoTuneCalmVolatilitySize", methods)
        self.assertIn("recordProfitCaptureRound", methods)
        self.assertLessEqual(cfg["guardianAdaptiveTakeProfitCapturePct"], 0.72)
        self.assertLessEqual(cfg["guardianProfitGivebackPct"], 0.32)
        self.assertTrue(cfg["lotCalmVolatilityProfitBoost"])
        self.assertFalse(review["cmuxHandoff"])

    def test_supervisor_profit_capture_escalates_after_failed_auto_tune(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 5,
            "supervisorReviewMinTrades": 5,
            "supervisorTargetWinRatePct": 45.0,
            "supervisorTargetProfitFactor": 1.25,
            "supervisorTargetMinRoundPnlUsd": 0.4,
        }
        main_forex._STATE["lastSupervisorAutoAction"] = {
            "handledIssueTitles": ["Profit capture is below target"],
            "actions": [{"issueType": "profit_capture_below_target"}],
        }
        main_forex._STATE["tradeReports"] = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 1, "pnl": 0.25, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 2, "pnl": 0.25, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 3, "pnl": 0.25, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 4, "pnl": -0.2, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 5, "pnl": -0.2, "exit": 4500.0, "reason": "mt5_closed_deal"},
        ]

        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(any(issue["title"] == "Profit capture still below target after auto-tune" for issue in review["issues"]))
        self.assertTrue(review["autoAction"]["requiresCmux"])
        self.assertTrue(any(item["issueTitle"] == "Profit capture still below target after auto-tune" for item in review["cmuxHandoff"]))

    def test_supervisor_loss_size_review_delegates_agent_tuning_first(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 5,
            "supervisorReviewMinTrades": 5,
            "supervisorDelegationMaxDeltaKeys": 20,
            "guardianMaxLossPerPositionUsd": 0.85,
            "guardianLossCapAlwaysOn": False,
            "guardianEarlyExitMaxPnlUsd": -0.25,
            "guardianEarlyExitMinConfidence": 0.76,
            "guardianEarlyExitMinMomentumPoints": 12,
            "forexStopLossPips": 8,
            "silverStopLossPips": 10,
            "stopLossPips": 14,
            "symbolOpportunityMinProbability": 0.56,
            "symbolOpportunityMinScore": 0.75,
        }
        main_forex._STATE["tradeReports"] = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 1, "pnl": 0.55, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 2, "pnl": 0.45, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 3, "pnl": 0.5, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 4, "pnl": -1.05, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 5, "pnl": -1.0, "exit": 4500.0, "reason": "mt5_closed_deal"},
        ]

        perf = main_forex._periodic_performance_review(cfg, "DEMO")
        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(any(item["title"] == "Loss size exceeds profit capture" for item in perf["recommendations"]))
        methods = [item["method"] for item in review["autoAction"]["actions"]]
        self.assertIn("autoTuneLossCap", methods)
        self.assertIn("autoTuneOpportunityQuality", methods)
        self.assertIn("tightenStopDistance", methods)
        self.assertLessEqual(cfg["guardianMaxLossPerPositionUsd"], 0.65)
        self.assertTrue(cfg["guardianLossCapAlwaysOn"])
        self.assertLessEqual(cfg["forexStopLossPips"], 8)
        self.assertGreaterEqual(cfg["symbolOpportunityMinProbability"], 0.6)
        self.assertFalse(review["cmuxHandoff"])

    def test_supervisor_loss_size_second_stage_delegates_before_cmux(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 5,
            "supervisorReviewMinTrades": 5,
            "supervisorDelegationMaxDeltaKeys": 20,
            "guardianMaxLossPerPositionUsd": 0.85,
            "guardianLossCapAlwaysOn": False,
            "forexStopLossPips": 8,
            "silverStopLossPips": 10,
            "symbolOpportunityMinProbability": 0.56,
            "symbolOpportunityMinScore": 0.75,
        }
        main_forex._STATE["lastSupervisorAutoAction"] = {
            "handledIssueTitles": ["Loss size exceeds profit capture"],
            "actions": [{"issueType": "loss_size_exceeds_profit_capture"}],
        }
        main_forex._STATE["lastClosedDealSync"] = {"ok": True, "deals": 5}
        main_forex._STATE["tradeReports"] = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 1, "pnl": 0.55, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 2, "pnl": 0.45, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 3, "pnl": 0.5, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 4, "pnl": -1.05, "exit": 4500.0, "reason": "mt5_closed_deal"},
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 5, "pnl": -1.0, "exit": 4500.0, "reason": "mt5_closed_deal"},
        ]

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(any(issue["title"] == "Loss size still exceeds profit capture after auto-tune" for issue in review["issues"]))
        methods = [item["method"] for item in review["autoAction"]["actions"]]
        self.assertIn("refactorLossCapAfterFailedTune", methods)
        self.assertIn("refactorEntryQualityAfterLossTune", methods)
        self.assertIn("lockExposureAfterLossTune", methods)
        self.assertLessEqual(cfg["guardianMaxLossPerPositionUsd"], 0.55)
        self.assertGreaterEqual(cfg["guardianMaxLossPerPositionUsd"], 0.5)
        self.assertTrue(cfg["guardianLossCapAlwaysOn"])
        self.assertFalse(review["autoAction"]["requiresCmux"])
        self.assertFalse(review["cmuxHandoff"])

    def test_supervisor_delegation_cooldown_uses_issue_signature_not_global_action_type(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorDelegationCooldownSec": 999,
            "supervisorDelegationMaxDeltaKeys": 20,
            "guardianAdaptiveTakeProfitCapturePct": 0.9,
            "guardianProfitGivebackPct": 0.45,
            "guardianProfitTrailMinPnlUsd": 0.45,
            "demoQualityMinConfidence": 0.5,
            "entryMinScoreGap": 0.5,
        }
        issue = {
            "agent": "hermes_supervisor",
            "severity": "warn",
            "title": "Periodic trade rounds are below profit target",
        }

        def review_with(pf, pnl):
            latest_round = {"trades": 20, "winRatePct": 35.0, "profitFactor": pf, "realizedPnl": pnl}
            return {
                "windowTrades": 20,
                "latest": latest_round,
                "rounds": [latest_round],
                "profitTargets": {"winRatePct": 48.0, "profitFactor": 1.12, "minPnlUsd": 0.0},
                "symbols": [],
            }

        with mock.patch.object(main_forex, "_save_state"):
            first = main_forex._supervisor_delegation_policy(cfg, [issue], review_with(0.8, -1.0), {"openCount": 0}, {})
            second = main_forex._supervisor_delegation_policy(cfg, [issue], review_with(0.6, -2.0), {"openCount": 0}, {})
            third = main_forex._supervisor_delegation_policy(cfg, [issue], review_with(0.6, -2.0), {"openCount": 0}, {})

        self.assertTrue(first["applied"])
        self.assertTrue(second["applied"])
        self.assertNotEqual(second.get("reason"), "issue_cooldown")
        self.assertFalse(third["applied"])
        self.assertEqual(third["reason"], "issue_cooldown")
        self.assertEqual(third["cooldownActions"][0]["issueType"], "periodic_round_below_target")

    def test_supervisor_does_not_cooldown_failed_threshold_actions(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorDelegationCooldownSec": 999,
            "supervisorDelegationMaxDeltaKeys": 1,
            "guardianAdaptiveTakeProfitCapturePct": 0.9,
            "guardianProfitGivebackPct": 0.45,
            "guardianProfitTrailMinPnlUsd": 0.45,
            "demoQualityMinConfidence": 0.5,
            "entryMinScoreGap": 0.5,
        }
        issue = {
            "agent": "hermes_supervisor",
            "severity": "warn",
            "title": "Periodic trade rounds are below profit target",
        }
        latest_round = {"trades": 20, "winRatePct": 35.0, "profitFactor": 0.8, "realizedPnl": -1.0}
        review = {
            "windowTrades": 20,
            "latest": latest_round,
            "rounds": [latest_round],
            "profitTargets": {"winRatePct": 48.0, "profitFactor": 1.12, "minPnlUsd": 0.0},
            "symbols": [],
        }

        with mock.patch.object(main_forex, "_save_state"):
            first = main_forex._supervisor_delegation_policy(cfg, [issue], review, {"openCount": 0}, {})
            cfg["supervisorDelegationMaxDeltaKeys"] = 20
            second = main_forex._supervisor_delegation_policy(cfg, [issue], review, {"openCount": 0}, {})

        self.assertFalse(first["applied"])
        self.assertTrue(first["thresholdExceeded"])
        self.assertTrue(second["applied"])
        self.assertNotEqual(second.get("reason"), "issue_cooldown")

    def test_auto_resume_restores_running_when_not_manually_stopped(self):
        cfg = {"symbol": "XAUUSD", "executionMode": "DEMO", "autoDemoTrading": True}
        main_forex._STATE["running"] = False
        main_forex._STATE["desiredRunning"] = True
        main_forex._STATE["manualStop"] = False

        resumed = main_forex._auto_resume_if_requested(cfg, "DEMO")

        self.assertTrue(resumed)
        self.assertTrue(main_forex._STATE["running"])
        self.assertEqual(main_forex._STATE["botNotice"]["code"], "auto_resumed")

    def test_auto_resume_respects_manual_stop(self):
        cfg = {"symbol": "XAUUSD", "executionMode": "DEMO", "autoDemoTrading": True}
        main_forex._STATE["running"] = False
        main_forex._STATE["desiredRunning"] = True
        main_forex._STATE["manualStop"] = True

        resumed = main_forex._auto_resume_if_requested(cfg, "DEMO")

        self.assertFalse(resumed)
        self.assertFalse(main_forex._STATE["running"])

    def test_supervisor_does_not_warn_zero_run_agents_when_capacity_full(self):
        cfg = {"symbol": "XAUUSD", "executionMode": "DEMO", "maxOpenOrders": 3, "maxOpenPositions": 3}
        main_forex._STATE["running"] = True
        main_forex._STATE["agentState"] = main_forex.new_agent_state()
        main_forex._STATE["botNotice"] = {"code": "order_sent"}
        review = main_forex._hermes_supervisor_review(
            cfg,
            [
                {"ticket": 1, "symbol": "XAUUSD", "volume": 0.01},
                {"ticket": 2, "symbol": "GBPUSD", "volume": 0.01},
                {"ticket": 3, "symbol": "XAGUSD", "volume": 0.01},
            ],
            {"openCount": 3},
            {"trades": 0, "winRatePct": 0, "realizedPnl": 0},
        )

        self.assertNotIn("Agent has not run yet", [issue["title"] for issue in review["issues"]])

    def test_supervisor_delegation_relaxes_entry_when_activity_is_low(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "portfolioFixedSlots": True,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "maxExposureLots": 0.06,
            "demoAutoMinConfidence": 0.55,
            "demoClearSignalOnly": False,
            "demoQualityMinConfidence": 0.55,
            "demoQualityMinMomentumPoints": 0.4,
            "entryMinScoreGap": 0.7,
            "earlyEntryScoreGapMin": 0.7,
            "symbolOpportunityMinProbability": 0.7,
            "symbolOpportunityMinScore": 1.35,
            "symbolOpportunityMinMomentumPoints": 1.0,
            "demoBlockRangeEntries": True,
            "demoLossCooldownAfter": 2,
            "demoLossCooldownSec": 600,
            "demoMaxLossStreakStop": 4,
        }
        main_forex._STATE["running"] = True
        main_forex._STATE["botNotice"] = {"code": "loss_streak_stop"}
        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        action = review["autoAction"]
        self.assertTrue(action["applied"])
        self.assertTrue(any(item["method"] == "relaxEntryGate" for item in action["actions"]))
        self.assertEqual(cfg["maxOpenOrders"], 3)
        self.assertLess(cfg["demoAutoMinConfidence"], 0.55)
        self.assertLess(cfg["symbolOpportunityMinProbability"], 0.7)
        self.assertLess(cfg["symbolOpportunityMinScore"], 1.35)
        self.assertLess(cfg["symbolOpportunityMinMomentumPoints"], 1.0)
        self.assertGreaterEqual(cfg["symbolOpportunityMinScore"], 0.75)
        self.assertGreaterEqual(cfg["demoLossCooldownAfter"], 6)
        self.assertGreaterEqual(cfg["demoMaxLossStreakStop"], 12)
        self.assertLessEqual(cfg["demoLossCooldownSec"], 180)

    def test_supervisor_delegation_activates_idle_sampling_when_strict_gates_block_entries(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "portfolioFixedSlots": True,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 5,
            "maxOpenPositions": 5,
            "maxExposureLots": 0.05,
            "demoClearSignalOnly": True,
            "professionalStrategyEnabled": True,
            "professionalPlaybook": "mtf_pullback_volume",
            "professionalRequireVolumeSpike": True,
            "professionalM1MinConfidence": 0.74,
            "professionalM1MinMomentumPoints": 3.0,
            "professionalVolumeSpikeMultiplier": 1.25,
            "symbolOpportunityMinProbability": 0.74,
            "symbolOpportunityMinScore": 1.45,
            "symbolOpportunityMinMomentumPoints": 2.0,
            "supervisorEntryContinuityMaxIdleSec": 900,
            "supervisorDelegationMaxDeltaKeys": 80,
        }
        main_forex._STATE["running"] = True
        main_forex._STATE["lastPortfolioEntryAt"] = int(time.time()) - 1800
        main_forex._STATE["botNotice"] = {"code": "professional_strategy_not_confirmed"}
        main_forex._STATE["lastSymbolOpportunity"] = {"ok": True, "code": "symbol_opportunity_ready", "symbol": "XAUUSD"}
        main_forex._STATE["lastMultiSymbolScan"] = {"results": []}

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        action = review["autoAction"]
        methods = {item["method"] for item in action["actions"]}
        self.assertTrue(action["applied"])
        self.assertIn("activateIdleSamplingEntryGate", methods)
        self.assertTrue(cfg["professionalIdleRelaxationEnabled"])
        self.assertTrue(cfg["professionalIdleAllowM5TrendWithoutPullback"])
        self.assertTrue(cfg["professionalIdleAllowM1SignalWithoutCandleConfirmation"])
        self.assertTrue(cfg["professionalIdleBypassVolumeSpike"])
        self.assertTrue(cfg["professionalIdleAllowHtfClearFallback"])
        self.assertTrue(cfg["supervisorSkipBlockedSymbols"])
        self.assertLess(cfg["professionalM1MinConfidence"], 0.74)
        self.assertLess(cfg["professionalM1MinMomentumPoints"], 3.0)
        self.assertLess(cfg["professionalVolumeSpikeMultiplier"], 1.25)
        self.assertLess(cfg["symbolOpportunityMinProbability"], 0.74)
        self.assertLess(cfg["symbolOpportunityMinScore"], 1.45)

    def test_supervisor_delegation_unblocks_range_when_no_positions_are_open(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "portfolioFixedSlots": True,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 3,
            "demoAutoMinConfidence": 0.55,
            "demoClearSignalOnly": False,
            "demoBlockRangeEntries": True,
            "symbolOpportunityMinProbability": 0.7,
            "symbolOpportunityMinScore": 1.35,
            "symbolOpportunityMinMomentumPoints": 1.0,
        }
        main_forex._STATE["running"] = True
        main_forex._STATE["botNotice"] = {"code": "range_filter"}
        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        self.assertTrue(review["autoAction"]["applied"])
        self.assertFalse(cfg["demoBlockRangeEntries"])

    def test_supervisor_fills_calm_market_slots_when_activity_is_low(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "portfolioFixedSlots": True,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "maxExposureLots": 0.03,
            "baseLot": 0.01,
            "supervisorCalmFillSlotsEnabled": True,
            "supervisorCalmFillMinOpenOrders": 3,
            "supervisorCalmFillMaxOpenOrders": 5,
            "symbolOpportunityMinProbability": 0.7,
            "symbolOpportunityMinScore": 1.35,
            "symbolOpportunityMinMomentumPoints": 1.0,
        }
        main_forex._STATE["running"] = True
        main_forex._STATE["botNotice"] = {"code": "waiting_interval"}
        main_forex._STATE["lastMultiSymbolScan"] = {
            "results": [
                {"symbol": "EURUSD", "lastOpportunity": {"calmMarket": True, "code": "symbol_opportunity_wait"}},
                {"symbol": "USDJPY", "lastOpportunity": {"calmMarket": True, "code": "symbol_opportunity_ready"}},
            ]
        }

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 2}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        action = review["autoAction"]
        self.assertTrue(action["applied"])
        self.assertEqual(cfg["portfolioTargetOpenOrders"], 5)
        self.assertEqual(cfg["maxOpenOrders"], 5)
        self.assertEqual(cfg["maxOpenPositions"], 5)
        self.assertGreaterEqual(cfg["maxExposureLots"], 0.05)
        self.assertTrue(any(item["method"] == "fillCalmMarketSlots" for item in action["actions"]))

    def test_supervisor_delegation_reduces_dragging_symbol_rank(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 10,
            "supervisorReviewMinTrades": 5,
            "portfolioFixedSlots": True,
            "maxOpenOrders": 3,
            "maxOpenPositions": 3,
            "maxExposureLots": 0.06,
        }
        reports = []
        for index in range(10):
            reports.append({"symbol": "XAUUSD", "mode": "DEMO", "ts": index + 1, "pnl": 0.5, "exit": 4500.0, "reason": "mt5_closed_deal"})
            reports.append({"symbol": "EURUSD", "mode": "DEMO", "ts": index + 20, "pnl": -0.8, "exit": 1.1, "reason": "mt5_closed_deal"})
        main_forex._STATE["tradeReports"] = reports
        main_forex._STATE["learningProfilesBySymbol"] = {
            "XAUUSD": {"learningScore": 50},
            "EURUSD": {"learningScore": 60},
        }
        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(review["autoAction"]["applied"])
        self.assertIn("EURUSD", cfg["supervisorSymbolBias"])
        ranked = main_forex._rank_symbols_for_scan(["EURUSD", "XAUUSD"], cfg)
        self.assertEqual(ranked[0], "XAUUSD")

    def test_supervisor_quarantines_zero_win_dragging_symbol(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 20,
            "supervisorReviewMinTrades": 12,
            "supervisorDelegationMaxDeltaKeys": 20,
            "supervisorSymbolBias": {},
        }
        reports = [
            {"symbol": "XAGUSD", "mode": "DEMO", "ts": index + 1, "pnl": -0.4, "exit": 73.0, "reason": "mt5_closed_deal"}
            for index in range(20)
        ] + [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": index + 40, "pnl": 0.2 if index % 2 else -0.1, "exit": 4500.0, "reason": "mt5_closed_deal"}
            for index in range(20)
        ]
        main_forex._STATE["tradeReports"] = reports
        main_forex._STATE["lastClosedDealSync"] = {"ok": True, "deals": len(reports)}

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(review["autoAction"]["applied"])
        self.assertEqual(cfg["supervisorSymbolBias"]["XAGUSD"]["action"], "lock")
        self.assertEqual(cfg["supervisorSymbolBias"]["XAGUSD"]["scorePenalty"], 999)
        self.assertTrue(any(action["method"] == "quarantineSymbol" for action in review["autoAction"]["actions"]))

    def test_rank_symbols_skips_active_supervisor_locked_symbols(self):
        until = int(time.time()) + 3600
        cfg = {
            "symbolLearningEnabled": True,
            "supervisorSkipBlockedSymbols": True,
            "supervisorSymbolBias": {
                "XAGUSD": {"action": "lock", "scorePenalty": 999, "until": until, "reason": "latest window 0 wins"},
                "EURUSD": {"action": "reduce", "scorePenalty": 25, "until": until, "reason": "weak latest window"},
            },
        }
        main_forex._STATE["learningProfilesBySymbol"] = {
            "XAGUSD": {"learningScore": 99},
            "EURUSD": {"learningScore": 80},
            "XAUUSD": {"learningScore": 50},
        }

        ranked = main_forex._rank_symbols_for_scan(["XAGUSD", "EURUSD", "XAUUSD"], cfg)

        self.assertNotIn("XAGUSD", ranked)
        self.assertIn("EURUSD", ranked)
        self.assertEqual((main_forex._STATE.get("lastSkippedScanSymbols") or {})["symbols"][0]["symbol"], "XAGUSD")

    def test_rank_symbols_boosts_active_supervisor_promoted_symbol(self):
        until = int(time.time()) + 3600
        cfg = {
            "symbolLearningEnabled": True,
            "supervisorSymbolBias": {
                "XAUUSD": {"action": "promote", "scoreBoost": 40, "until": until, "reason": "positive reference"},
            },
        }
        main_forex._STATE["learningProfilesBySymbol"] = {
            "USDJPY": {"learningScore": 80},
            "XAUUSD": {"learningScore": 50},
        }

        ranked = main_forex._rank_symbols_for_scan(["USDJPY", "XAUUSD"], cfg)

        self.assertEqual(ranked[0], "XAUUSD")

    def test_learning_feedback_promotes_latest_positive_reference_instead_of_reduce(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorDelegationEnabled": True,
            "supervisorDelegationCooldownSec": 0,
            "supervisorLearningFeedbackBiasTtlSec": 3600,
            "supervisorSymbolBias": {
                "XAUUSD": {"action": "reduce", "scorePenalty": 25, "until": int(time.time()) + 1800}
            },
        }
        main_forex._STATE["lastSupervisorAutoAction"] = {}
        main_forex._STATE["lastLearningFeedbackAudit"] = {
            "staleProfiles": [{"symbol": "XAUUSD"}, {"symbol": "EURUSD"}],
        }
        performance = {
            "latest": {"trades": 20, "realizedPnl": -1.0, "profitFactor": 0.5, "winRatePct": 35},
            "symbols": [
                {"symbol": "XAUUSD", "trades": 20, "realizedPnl": 1.79, "profitFactor": 1.25, "winRatePct": 45},
                {"symbol": "EURUSD", "trades": 20, "realizedPnl": -3.11, "profitFactor": 0.05, "winRatePct": 25},
            ],
        }

        with mock.patch.object(main_forex, "_save_state"):
            action = main_forex._supervisor_delegation_policy(
                cfg,
                [{"title": "Learning feedback is not applied to runtime"}],
                performance,
                {},
                {},
            )

        self.assertTrue(action["applied"])
        self.assertEqual(cfg["supervisorSymbolBias"]["XAUUSD"]["action"], "promote")
        self.assertIn(cfg["supervisorSymbolBias"]["EURUSD"]["action"], {"reduce", "skip", "lock", "quarantine"})

    def test_learning_feedback_promotes_latest_survivor_reference_instead_of_reduce(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorDelegationEnabled": True,
            "supervisorDelegationCooldownSec": 0,
            "supervisorLearningFeedbackBiasTtlSec": 3600,
            "supervisorSymbolBias": {
                "XAUUSD": {"action": "reduce", "scorePenalty": 25, "until": int(time.time()) + 1800}
            },
        }
        main_forex._STATE["lastSupervisorAutoAction"] = {}
        main_forex._STATE["lastLearningFeedbackAudit"] = {
            "staleProfiles": [{"symbol": "XAUUSD"}, {"symbol": "GBPUSD"}],
        }
        performance = {
            "latest": {"trades": 20, "realizedPnl": -3.89, "profitFactor": 0.15, "winRatePct": 20},
            "symbols": [
                {"symbol": "XAUUSD", "trades": 20, "wins": 7, "losses": 13, "winRatePct": 35, "realizedPnl": -0.71, "avgPnl": -0.0355, "profitFactor": 0.9067},
                {"symbol": "GBPUSD", "trades": 20, "wins": 6, "losses": 14, "winRatePct": 30, "realizedPnl": -1.81, "avgPnl": -0.0905, "profitFactor": 0.3627},
            ],
        }

        with mock.patch.object(main_forex, "_save_state"):
            action = main_forex._supervisor_delegation_policy(
                cfg,
                [{"title": "Learning feedback is not applied to runtime"}],
                performance,
                {},
                {},
            )

        self.assertTrue(action["applied"])
        self.assertEqual(cfg["supervisorSymbolBias"]["XAUUSD"]["action"], "promote")
        self.assertIn("survivor reference", cfg["supervisorSymbolBias"]["XAUUSD"]["reason"])

    def test_rank_symbols_falls_back_when_every_symbol_is_locked(self):
        until = int(time.time()) + 3600
        cfg = {
            "symbolLearningEnabled": True,
            "supervisorSkipBlockedSymbols": True,
            "supervisorSymbolBias": {
                "XAGUSD": {"action": "lock", "until": until},
                "XAUUSD": {"action": "lock", "until": until},
            },
        }

        ranked = main_forex._rank_symbols_for_scan(["XAGUSD", "XAUUSD"], cfg)

        self.assertEqual(set(ranked), {"XAGUSD", "XAUUSD"})
        self.assertEqual(len((main_forex._STATE.get("lastSkippedScanSymbols") or {})["symbols"]), 2)

    def test_supervisor_delegation_tunes_payoff_and_memory_window(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "supervisorReviewWindowTrades": 10,
            "supervisorReviewMinTrades": 5,
            "supervisorDailyReviewMinTrades": 999,
            "learningRecentTradeCount": 60,
            "guardianAdaptiveTakeProfitCapturePct": 0.9,
            "guardianProfitGivebackTriggerUsd": 2.5,
            "guardianProfitGivebackPct": 0.45,
            "guardianProfitTrailMinPnlUsd": 0.45,
        }
        now = int(time.time())
        reports = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": now - 40 * 86400 + i, "pnl": 0.4, "exit": 4500.0, "reason": "mt5_closed_deal"}
            for i in range(6)
        ] + [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": now - i, "pnl": -0.8, "exit": 4500.0, "reason": "mt5_closed_deal"}
            for i in range(6)
        ]
        main_forex._STATE["tradeReports"] = reports
        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        methods = [item["method"] for item in review["autoAction"]["actions"]]
        self.assertIn("autoTunePayoff", methods)
        self.assertIn("trimWindow", methods)
        self.assertLess(cfg["guardianAdaptiveTakeProfitCapturePct"], 0.9)
        self.assertEqual(cfg["learningRecentTradeCount"], 30)

    def test_supervisor_applies_learning_feedback_when_profiles_are_not_reflected(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "demoAutoMinConfidence": 0.5,
            "demoQualityMinConfidence": 0.5,
            "demoQualityMinMomentumPoints": 0.25,
            "demoBlockRangeEntries": False,
            "entryMinScoreGap": 0.55,
            "guardianProfitGivebackTriggerUsd": 2.5,
            "guardianProfitGivebackPct": 0.45,
            "guardianProfitTrailMinPnlUsd": 0.45,
            "supervisorDelegationMaxDeltaKeys": 20,
            "symbolLearningApplyTuning": False,
            "symbolLearningApplyLatestWindowGuard": True,
        }
        reports = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": index + 1, "pnl": -1.0, "exit": 4500.0, "reason": "mt5_closed_deal"}
            for index in range(20)
        ]
        main_forex._STATE["running"] = True
        main_forex._STATE["tradeReports"] = reports
        main_forex._STATE["learningProfilesBySymbol"] = {
            "XAUUSD": {
                "symbol": "XAUUSD",
                "trades": 20,
                "winRatePct": 20.0,
                "profitFactor": 0.3,
                "realizedPnl": -20.0,
                "learningScore": 30,
                "priority": "back",
                "latestWindow": {"weak": True},
                "proposal": {
                    "demoAutoMinConfidence": 0.82,
                    "demoQualityMinConfidence": 0.82,
                    "demoQualityMinMomentumPoints": 18,
                    "demoBlockRangeEntries": True,
                    "guardianProfitGivebackTriggerUsd": 1.6,
                    "guardianProfitGivebackPct": 0.35,
                    "guardianProfitTrailMinPnlUsd": 0.38,
                },
            }
        }

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(any(issue["title"] == "Learning feedback is not applied to runtime" for issue in review["issues"]))
        methods = [item["method"] for item in review["autoAction"]["actions"]]
        self.assertIn("applyLearningFeedbackSnapshot", methods)
        self.assertIn("applyLearnedEntryGate", methods)
        self.assertIn("applyLearnedExitGuard", methods)
        self.assertIn("applyLearnedSymbolBias", methods)
        self.assertGreaterEqual(cfg["demoAutoMinConfidence"], 0.58)
        self.assertGreaterEqual(cfg["demoQualityMinConfidence"], 0.6)
        self.assertTrue(cfg["demoBlockRangeEntries"])
        self.assertIn("XAUUSD", cfg["supervisorSymbolBias"])
        self.assertFalse(review["cmuxHandoff"])

    def test_learning_feedback_audit_waits_after_bounded_auto_tune(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "demoAutoMinConfidence": 0.5,
            "demoQualityMinConfidence": 0.5,
            "demoQualityMinMomentumPoints": 0.25,
            "demoBlockRangeEntries": False,
            "supervisorLearningFeedbackMinTrades": 1,
            "supervisorLearningFeedbackLastAppliedAt": int(time.time()),
            "supervisorLearningFeedbackTuneCooldownSec": 300,
        }
        main_forex._STATE["tradeReports"] = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": 1, "pnl": -1.0, "exit": 4500.0, "reason": "mt5_closed_deal"},
        ]
        main_forex._STATE["learningProfilesBySymbol"] = {
            "XAUUSD": {
                "symbol": "XAUUSD",
                "trades": 20,
                "learningScore": 20,
                "priority": "back",
                "latestWindow": {"weak": True},
                "proposal": {
                    "demoAutoMinConfidence": 0.82,
                    "demoQualityMinConfidence": 0.82,
                    "demoQualityMinMomentumPoints": 18,
                    "demoBlockRangeEntries": True,
                },
            }
        }

        audit = main_forex._learning_feedback_runtime_audit(cfg, "DEMO")

        self.assertFalse(audit["needsTuning"])
        self.assertEqual(audit["reason"], "bounded_tune_recently_applied")

    def test_supervisor_escalates_learning_feedback_after_failed_auto_tune(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "demoAutoMinConfidence": 0.5,
            "demoQualityMinConfidence": 0.5,
            "demoQualityMinMomentumPoints": 0.25,
            "symbolLearningApplyTuning": False,
        }
        main_forex._STATE["lastSupervisorAutoAction"] = {
            "handledIssueTitles": ["Learning feedback is not applied to runtime"],
            "actions": [{"issueType": "learning_feedback_runtime_tune"}],
        }
        main_forex._STATE["tradeReports"] = [
            {"symbol": "XAUUSD", "mode": "DEMO", "ts": index + 1, "pnl": -1.0, "exit": 4500.0, "reason": "mt5_closed_deal"}
            for index in range(20)
        ]
        main_forex._STATE["learningProfilesBySymbol"] = {
            "XAUUSD": {
                "symbol": "XAUUSD",
                "trades": 20,
                "learningScore": 30,
                "priority": "back",
                "latestWindow": {"weak": True},
                "proposal": {"demoAutoMinConfidence": 0.82, "demoQualityMinConfidence": 0.82},
            }
        }

        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, main_forex._closed_trade_stats("DEMO"))

        self.assertTrue(any(issue["title"] == "Learning feedback still not reflected after auto-tune" for issue in review["issues"]))
        self.assertTrue(review["autoAction"]["requiresCmux"])
        self.assertTrue(any(item["issueTitle"] == "Learning feedback still not reflected after auto-tune" for item in review["cmuxHandoff"]))

    def test_supervisor_tunes_calm_position_stall_before_cmux(self):
        now = int(time.time())
        cfg = {
            "symbol": "EURUSD",
            "executionMode": "DEMO",
            "supervisorCalmPositionMaxAgeSec": 60,
            "supervisorCalmPositionMaxAbsPnlUsd": 0.7,
            "supervisorDelegationMaxDeltaKeys": 20,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 3,
            "baseLot": 0.01,
            "maxLot": 0.03,
            "maxExposureLots": 0.09,
            "guardianAdaptiveTakeProfitMinUsd": 1.2,
            "guardianAdaptiveTakeProfitDefaultUsd": 2.8,
            "guardianProfitTrailMinPnlUsd": 0.45,
            "symbolMultiplierProfiles": {
                "FOREX_MAJOR": {
                    "multiplierMax": 2.8,
                    "volatilityThrottlePoints": 12,
                    "volatilityBoostBelowPoints": 3,
                    "calmMultiplierFloor": 1.85,
                    "calmMultiplierCeiling": 2.7,
                    "calmMultiplierBoost": 0.4,
                }
            },
        }
        positions = [{"ticket": 1, "symbol": "EURUSD", "type": 0, "profit": 0.12, "volume": 0.01, "time": now - 300}]
        main_forex._STATE["lastGuardianExit"] = {"monitoredCount": 1}
        main_forex._STATE["lastGuardianTrail"] = {"monitoredCount": 1}
        main_forex._STATE["lastMultiSymbolScan"] = {
            "results": [
                {
                    "symbol": "EURUSD",
                    "lastOpportunity": {"symbol": "EURUSD", "calmMarket": True, "volatilityPoints": 1.0},
                    "lotSizing": {"symbol": "EURUSD", "multiplier": 1.85, "recommendedLot": 0.02, "volatilityPoints": 1.0},
                }
            ]
        }

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, positions, {"openCount": 1}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        self.assertTrue(any(issue["title"] == "Calm volatility positions are staying open too long" for issue in review["issues"]))
        methods = [item["method"] for item in review["autoAction"]["actions"]]
        self.assertIn("autoTuneCalmStallMultiplier", methods)
        self.assertIn("autoTuneCalmStallProfitCapture", methods)
        self.assertGreater(cfg["maxLot"], 0.03)
        self.assertGreaterEqual(cfg["maxExposureLots"], 0.12)
        self.assertIn("EURUSD", cfg["symbolMultiplierProfiles"])
        self.assertGreater(cfg["symbolMultiplierProfiles"]["EURUSD"]["calmMultiplierFloor"], 1.85)
        self.assertGreaterEqual(cfg["guardianAdaptiveTakeProfitMinUsd"], 1.0)
        self.assertGreaterEqual(cfg["guardianMinPositiveCloseUsd"], 1.0)
        self.assertLessEqual(cfg["guardianProfitTrailMinPnlUsd"], 0.38)
        self.assertFalse(review["cmuxHandoff"])

    def test_supervisor_calm_position_stall_second_stage_delegates_before_cmux(self):
        now = int(time.time())
        cfg = {
            "symbol": "EURUSD",
            "executionMode": "DEMO",
            "supervisorCalmPositionMaxAgeSec": 60,
            "supervisorDelegationMaxDeltaKeys": 20,
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 3,
            "baseLot": 0.01,
            "maxLot": 0.03,
            "maxExposureLots": 0.09,
        }
        positions = [{"ticket": 1, "symbol": "EURUSD", "type": 0, "profit": 0.12, "volume": 0.01, "time": now - 300}]
        main_forex._STATE["lastGuardianExit"] = {"monitoredCount": 1}
        main_forex._STATE["lastGuardianTrail"] = {"monitoredCount": 1}
        main_forex._STATE["lastMultiSymbolScan"] = {
            "results": [{"symbol": "EURUSD", "lastOpportunity": {"symbol": "EURUSD", "calmMarket": True, "volatilityPoints": 1.0}}]
        }
        main_forex._STATE["lastSupervisorAutoAction"] = {
            "handledIssueTitles": ["Calm volatility positions are staying open too long"],
            "actions": [{"issueType": "calm_position_stall"}],
        }

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, positions, {"openCount": 1}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        self.assertTrue(any(issue["title"] == "Calm volatility positions still stale after auto-tune" for issue in review["issues"]))
        methods = [item["method"] for item in review["autoAction"]["actions"]]
        self.assertIn("secondStageCalmStallSizingReview", methods)
        self.assertIn("forceCalmStallExitReview", methods)
        self.assertIn("rotateCalmStallSymbol", methods)
        self.assertTrue(cfg["supervisorCalmStallSecondStageActive"])
        self.assertTrue(cfg["portfolioRotationEnabled"])
        self.assertFalse(review["autoAction"]["requiresCmux"])
        self.assertFalse(review["cmuxHandoff"])

    def test_supervisor_records_learning_proposal_decision_before_cmux(self):
        cfg = {"symbol": "XAUUSD", "executionMode": "DEMO"}
        issue = {
            "agent": "memory_agent",
            "severity": "warn",
            "title": "Learning proposal not fully applied",
        }

        with mock.patch.object(main_forex, "_save_state"):
            action = main_forex._supervisor_delegation_policy(cfg, [issue], {}, {"openCount": 0}, {})

        self.assertTrue(action["applied"])
        self.assertFalse(action["requiresCmux"])
        self.assertTrue(any(item["method"] == "recordLearningProposalDecision" for item in action["actions"]))
        self.assertEqual(cfg["supervisorLearningProposalDecision"]["decision"], "defer_full_proposal")
        self.assertEqual(main_forex._STATE["learningDecision"]["decision"], "defer_full_proposal")

    def test_supervisor_refreshes_strategy_heartbeat_before_cmux(self):
        cfg = {"symbol": "XAUUSD", "executionMode": "DEMO"}
        issue = {
            "agent": "hermes_supervisor",
            "severity": "high",
            "title": "Agent run counter stalled",
            "detail": "stalledAgents=['strategy_builder']",
        }
        main_forex._STATE["agentState"] = main_forex.new_agent_state()
        main_forex._STATE["agentHealthMonitor"] = {"agent_stalled_strategy_builder": {"count": 4}}

        with mock.patch.object(main_forex, "_save_state"):
            action = main_forex._supervisor_delegation_policy(cfg, [issue], {}, {"openCount": 0}, {})

        self.assertTrue(action["applied"])
        self.assertFalse(action["requiresCmux"])
        self.assertTrue(any(item["method"] == "refreshRuntimeHeartbeat" for item in action["actions"]))
        self.assertEqual(main_forex._STATE["agentHealthMonitor"]["agent_stalled_strategy_builder"]["count"], 0)

    def test_supervisor_health_rebalances_repeated_symbol_scan(self):
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "multiSymbolScanEnabled": True,
            "multiSymbolScanLimit": 1,
            "watchlist": ["XAUUSD", "EURUSD", "GBPUSD"],
        }
        main_forex._STATE["running"] = True
        main_forex._STATE["lastMultiSymbolScan"] = {"symbols": ["XAUUSD"]}
        main_forex._STATE["agentHealthMonitor"] = {"market_repeated_scan": {"count": 1}}
        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        self.assertTrue(any(issue["title"] == "Market scanner is repeating too few symbols" for issue in review["issues"]))
        self.assertTrue(any(action["method"] == "rebalanceScanQueue" for action in review["autoAction"]["actions"]))
        self.assertGreaterEqual(cfg["multiSymbolScanLimit"], 3)
        self.assertFalse(review["autoAction"]["requiresCmux"])

    def test_supervisor_health_self_tunes_guardian_trail(self):
        cfg = {
            "symbol": "GBPUSD",
            "executionMode": "DEMO",
            "guardianProfitTrailMinConfidence": 0.8,
            "guardianProfitTrailMinMomentumPoints": 12,
            "guardianProfitTrailMinPnlUsd": 0.45,
        }
        positions = [{"ticket": 1, "symbol": "GBPUSD", "type": 1, "profit": 0.8, "volume": 0.01}]
        main_forex._STATE["running"] = True
        main_forex._STATE["lastGuardianTrail"] = {"ok": True, "modified": 0, "noops": 0, "attempted": 0, "monitoredCount": 1}
        main_forex._STATE["agentHealthMonitor"] = {"guardian_not_adjusting_tpsl": {"count": 2}}
        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, positions, {"openCount": 1}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        self.assertTrue(any(issue["title"] == "Guardian is not adjusting TP/SL on profitable positions" for issue in review["issues"]))
        self.assertTrue(any(action["method"] == "selfTuneTrail" for action in review["autoAction"]["actions"]))
        self.assertLess(cfg["guardianProfitTrailMinConfidence"], 0.8)
        self.assertGreaterEqual(cfg["guardianMinStopDistancePips"], 1.5)
        self.assertTrue(cfg["guardianProfitTrailFallbackEnabled"])
        self.assertFalse(review["autoAction"]["requiresCmux"])

    def test_supervisor_health_ignores_guardian_intentional_trail_skips(self):
        cfg = {"symbol": "GBPUSD", "executionMode": "DEMO", "guardianProfitTrailMinPnlUsd": 0.45}
        positions = [{"ticket": 1, "symbol": "GBPUSD", "type": 1, "profit": 0.8, "volume": 0.01}]
        main_forex._STATE["running"] = True
        main_forex._STATE["lastGuardianTrail"] = {
            "ok": True,
            "modified": 0,
            "noops": 1,
            "intentionalSkips": 1,
            "attempted": 0,
            "monitoredCount": 1,
        }
        main_forex._STATE["agentHealthMonitor"] = {"guardian_not_adjusting_tpsl": {"count": 5}}

        review = main_forex._hermes_supervisor_review(cfg, positions, {"openCount": 1}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        self.assertFalse(any(issue["title"] == "Guardian is not adjusting TP/SL on profitable positions" for issue in review["issues"]))

    def test_supervisor_health_uses_agent_telemetry_before_declaring_counter_stalled(self):
        now = int(time.time())
        cfg = {"symbol": "GBPUSD", "executionMode": "DEMO"}
        main_forex._STATE["running"] = True
        main_forex._STATE["agentState"] = main_forex.new_agent_state()
        strategy = main_forex._STATE["agentState"]["agents"]["strategy_builder"]
        strategy.update({"state": "doing", "runs": 10, "lastAction": "scan skipped", "lastReason": "quality_confidence", "updatedAt": now})
        main_forex._STATE["agentHealthMonitor"] = {
            "agentRuns": {"strategy_builder": 10},
            "agentTelemetry": {"strategy_builder": {"updatedAt": now - 5, "lastAction": "scan skipped", "lastReason": "weak_momentum"}},
            "agent_stalled_strategy_builder": {"count": 4},
        }

        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        self.assertFalse(any(issue["title"] == "Agent run counter stalled" for issue in review["issues"]))
        self.assertEqual(main_forex._STATE["agentHealthMonitor"]["agent_stalled_strategy_builder"]["count"], 0)

    def test_supervisor_health_does_not_stall_downstream_agents_waiting_on_entry_gate(self):
        old = int(time.time()) - 500
        cfg = {"symbol": "GBPUSD", "executionMode": "DEMO", "supervisorAgentHeartbeatTtlSec": 60, "maxOpenOrders": 3}
        main_forex._STATE["running"] = True
        main_forex._STATE["botNotice"] = {"code": "signal_below_threshold"}
        main_forex._STATE["agentState"] = main_forex.new_agent_state()
        for agent_id in ("lot_sizing_agent", "risk_manager", "execution_agent"):
            agent = main_forex._STATE["agentState"]["agents"][agent_id]
            agent.update({"state": "done", "runs": 4, "lastAction": "waiting for signal", "lastReason": "entry gate", "updatedAt": old})
        main_forex._STATE["agentHealthMonitor"] = {
            "agentRuns": {"lot_sizing_agent": 4, "risk_manager": 4, "execution_agent": 4},
            "agentTelemetry": {
                "lot_sizing_agent": {"updatedAt": old, "lastAction": "waiting for signal", "lastReason": "entry gate"},
                "risk_manager": {"updatedAt": old, "lastAction": "waiting for signal", "lastReason": "entry gate"},
                "execution_agent": {"updatedAt": old, "lastAction": "waiting for signal", "lastReason": "entry gate"},
            },
            "agent_stalled_lot_sizing_agent": {"count": 4},
            "agent_stalled_risk_manager": {"count": 4},
            "agent_stalled_execution_agent": {"count": 4},
        }

        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        self.assertFalse(any(issue["title"] == "Agent run counter stalled" for issue in review["issues"]))
        self.assertEqual(main_forex._STATE["agentHealthMonitor"]["agent_stalled_lot_sizing_agent"]["count"], 0)
        self.assertEqual(main_forex._STATE["agentHealthMonitor"]["agent_stalled_risk_manager"]["count"], 0)
        self.assertEqual(main_forex._STATE["agentHealthMonitor"]["agent_stalled_execution_agent"]["count"], 0)

    def test_supervisor_health_does_not_stall_downstream_agents_waiting_on_clear_signal(self):
        old = int(time.time()) - 500
        cfg = {"symbol": "GBPUSD", "executionMode": "DEMO", "supervisorAgentHeartbeatTtlSec": 60, "maxOpenOrders": 5}
        main_forex._STATE["running"] = True
        main_forex._STATE["botNotice"] = {"code": "clear_signal_required"}
        main_forex._STATE["agentState"] = main_forex.new_agent_state()
        for agent_id in ("lot_sizing_agent", "risk_manager", "execution_agent"):
            agent = main_forex._STATE["agentState"]["agents"][agent_id]
            agent.update({"state": "done", "runs": 4, "lastAction": "waiting for clear signal", "lastReason": "entry gate", "updatedAt": old})
        main_forex._STATE["agentHealthMonitor"] = {
            "agentRuns": {"lot_sizing_agent": 4, "risk_manager": 4, "execution_agent": 4},
            "agentTelemetry": {
                "lot_sizing_agent": {"updatedAt": old, "lastAction": "waiting for clear signal", "lastReason": "entry gate"},
                "risk_manager": {"updatedAt": old, "lastAction": "waiting for clear signal", "lastReason": "entry gate"},
                "execution_agent": {"updatedAt": old, "lastAction": "waiting for clear signal", "lastReason": "entry gate"},
            },
            "agent_stalled_lot_sizing_agent": {"count": 4},
            "agent_stalled_risk_manager": {"count": 4},
            "agent_stalled_execution_agent": {"count": 4},
        }

        review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        self.assertFalse(any(issue["title"] == "Agent run counter stalled" for issue in review["issues"]))
        self.assertEqual(main_forex._STATE["agentHealthMonitor"]["agent_stalled_lot_sizing_agent"]["count"], 0)
        self.assertEqual(main_forex._STATE["agentHealthMonitor"]["agent_stalled_risk_manager"]["count"], 0)
        self.assertEqual(main_forex._STATE["agentHealthMonitor"]["agent_stalled_execution_agent"]["count"], 0)

    def test_supervisor_monitors_entry_continuity_with_capacity_remaining(self):
        old = int(time.time()) - 2400
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "watchlist": ["XAUUSD", "EURUSD", "GBPUSD", "XAGUSD"],
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 5,
            "maxOpenPositions": 5,
            "portfolioMinSecondsBetweenEntries": 300,
            "supervisorEntryContinuityMaxIdleSec": 900,
            "supervisorDelegationMaxDeltaKeys": 20,
            "multiSymbolScanEnabled": True,
            "multiSymbolScanLimit": 4,
        }
        main_forex._STATE["running"] = True
        main_forex._STATE["lastPortfolioEntryAt"] = old
        main_forex._STATE["botNotice"] = {"code": "symbol_opportunity_wait", "message": "waiting for better entry"}
        main_forex._STATE["lastSymbolOpportunity"] = {
            "ok": False,
            "code": "symbol_quarantined",
            "symbol": "XAGUSD",
            "reasons": ["symbol quarantined by Supervisor"],
        }
        main_forex._STATE["lastHigherTimeframeConfirm"] = {"ok": True, "enabled": True, "confirmations": 2, "required": 2}
        main_forex._STATE["lastClearSignalGuard"] = {
            "ok": False,
            "code": "clear_signal_required",
            "reasons": ["calm market requires breakout"],
        }

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        titles = [issue["title"] for issue in review["issues"]]
        self.assertIn("Entry continuity stalled while capacity remains", titles)
        self.assertTrue(review["autoAction"]["applied"])
        self.assertIn("Entry continuity stalled while capacity remains", review["autoAction"]["handledIssueTitles"])
        methods = {action["method"] for action in review["autoAction"]["actions"]}
        self.assertIn("rebalanceScanQueue", methods)
        self.assertIn("auditClearSignalBlockers", methods)
        self.assertIn("rotatePastQuarantinedSymbol", methods)
        self.assertEqual(cfg["portfolioTargetOpenOrders"], 3)
        self.assertEqual(cfg["maxOpenOrders"], 5)
        self.assertTrue(cfg["supervisorSkipBlockedSymbols"])
        self.assertEqual(cfg["forceScanQueueRefreshAt"], review["autoAction"]["ts"])

    def test_supervisor_entry_continuity_allows_strong_calm_sampling_candidate(self):
        old = int(time.time()) - 2400
        cfg = {
            "symbol": "XAUUSD",
            "executionMode": "DEMO",
            "watchlist": ["XAUUSD", "EURUSD", "GBPUSD", "XAGUSD"],
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 5,
            "maxOpenPositions": 5,
            "portfolioMinSecondsBetweenEntries": 300,
            "supervisorEntryContinuityMaxIdleSec": 900,
            "supervisorDelegationMaxDeltaKeys": 20,
            "multiSymbolScanEnabled": True,
            "multiSymbolScanLimit": 4,
        }
        main_forex._STATE["running"] = True
        main_forex._STATE["lastPortfolioEntryAt"] = old
        main_forex._STATE["botNotice"] = {"code": "clear_signal_required", "message": "calm market requires breakout"}
        main_forex._STATE["lastSymbolOpportunity"] = {
            "ok": True,
            "code": "symbol_opportunity_ready",
            "symbol": "XAUUSD",
            "reasons": [],
        }
        main_forex._STATE["lastHigherTimeframeConfirm"] = {"ok": True, "enabled": True, "confirmations": 2, "required": 2}
        main_forex._STATE["lastClearSignalGuard"] = {
            "ok": False,
            "code": "clear_signal_required",
            "reasons": ["calm market requires breakout"],
        }

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        self.assertTrue(review["autoAction"]["applied"])
        self.assertTrue(cfg["demoClearSignalAllowStrongCalmAfterIdle"])
        self.assertEqual(cfg["demoClearSignalStrongCalmMinProbability"], 0.9)
        self.assertGreaterEqual(cfg["demoLossCooldownAfter"], 6)
        self.assertGreaterEqual(cfg["demoMaxLossStreakStop"], 12)
        self.assertLessEqual(cfg["demoLossCooldownSec"], 180)
        self.assertTrue(any(action["method"] == "allowStrongCalmSampling" for action in review["autoAction"]["actions"]))
        self.assertTrue(any(action["method"] == "relaxLossStreakSamplingStop" for action in review["autoAction"]["actions"]))

    def test_supervisor_entry_continuity_allows_professional_fallback_after_htf_conflict(self):
        old = int(time.time()) - 2400
        cfg = {
            "symbol": "GBPUSD",
            "executionMode": "DEMO",
            "watchlist": ["XAUUSD", "EURUSD", "GBPUSD", "XAGUSD"],
            "portfolioTargetOpenOrders": 3,
            "maxOpenOrders": 5,
            "maxOpenPositions": 5,
            "portfolioMinSecondsBetweenEntries": 300,
            "supervisorEntryContinuityMaxIdleSec": 900,
            "supervisorDelegationMaxDeltaKeys": 30,
            "multiSymbolScanEnabled": True,
            "multiSymbolScanLimit": 4,
        }
        main_forex._STATE["running"] = True
        main_forex._STATE["lastPortfolioEntryAt"] = old
        main_forex._STATE["botNotice"] = {"code": "clear_signal_required", "message": "HTF conflict and calm market"}
        main_forex._STATE["lastSymbolOpportunity"] = {
            "ok": True,
            "code": "symbol_opportunity_ready",
            "symbol": "GBPUSD",
            "reasons": [],
        }
        main_forex._STATE["lastHigherTimeframeConfirm"] = {"ok": False, "enabled": True, "confirmations": 1, "required": 2}
        main_forex._STATE["lastClearSignalGuard"] = {
            "ok": False,
            "code": "clear_signal_required",
            "reasons": ["calm market requires breakout"],
        }

        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, [], {"openCount": 0}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        self.assertTrue(review["autoAction"]["applied"])
        self.assertTrue(cfg["demoHtfAllowProfessionalPlaybookFallbackAfterIdle"])
        self.assertTrue(cfg["demoClearSignalAllowProfessionalCalmAfterIdle"])
        methods = {action["method"] for action in review["autoAction"]["actions"]}
        self.assertIn("allowProfessionalPlaybookIdleFallback", methods)

    def test_supervisor_health_escalates_after_failed_self_heal(self):
        cfg = {
            "symbol": "GBPUSD",
            "executionMode": "DEMO",
            "guardianProfitTrailMinConfidence": 0.8,
            "guardianProfitTrailMinMomentumPoints": 12,
            "guardianProfitTrailMinPnlUsd": 0.45,
        }
        positions = [{"ticket": 1, "symbol": "GBPUSD", "type": 1, "profit": 0.8, "volume": 0.01}]
        main_forex._STATE["running"] = True
        main_forex._STATE["lastGuardianTrail"] = {"ok": True, "modified": 0, "noops": 0, "attempted": 0, "monitoredCount": 1}
        main_forex._STATE["agentHealthMonitor"] = {"guardian_not_adjusting_tpsl": {"count": 5}}
        main_forex._STATE["lastSupervisorAutoAction"] = {
            "ts": int(time.time()) - 10,
            "handledIssueTitles": ["Guardian is not adjusting TP/SL on profitable positions"],
            "actions": [{"issueType": "guardian_not_adjusting_tpsl"}],
        }
        with mock.patch.object(main_forex, "_save_state"):
            review = main_forex._hermes_supervisor_review(cfg, positions, {"openCount": 1}, {"trades": 0, "winRatePct": 0, "realizedPnl": 0})

        self.assertTrue(review["autoAction"]["requiresCmux"])
        self.assertTrue(any(item.get("issueTitle") == "Guardian is not adjusting TP/SL on profitable positions" for item in review["cmuxHandoff"]))

    def test_hermes_playbook_status_loads_stable_docs(self):
        status = main_forex._hermes_playbook_status()

        self.assertTrue(status["ok"])
        self.assertEqual(status["governance"], "stable_playbooks_cmux_or_human_only; supervisor_notes_append_only")
        self.assertIn("market_analyst.md", status["stablePlaybooks"])
        self.assertTrue(status["stablePlaybooks"]["position_guardian.md"]["sha256"])
        self.assertTrue(status["learningNotes"]["appendOnly"])

    def test_session_bias_penalizes_signal_confidence_for_bad_utc_hour(self):
        current_hour = time.gmtime().tm_hour
        cfg = {
            "demoAutoMinConfidence": 0.5,
            "entryMinScoreGap": 0.5,
            "maxSpreadPoints": 30,
            "pipSize": 0.1,
            "sessionBiasOverride": {
                "badUtcHours": [current_hour],
                "confidencePenalty": 0.05,
                "until": int(time.time()) + 60,
            },
        }
        candles = [{"close": 4500 + i * 0.5, "high": 4500 + i * 0.5 + 0.2, "low": 4500 + i * 0.5 - 0.2} for i in range(20)]
        market = {"ok": True, "tick": {"spread": 1}, "candles": candles}

        out = main_forex._autonomous_signal_from_market(cfg, "XAUUSD", market, mode_label="demo")

        self.assertTrue(any(gate["gate"] == "session_bias" for gate in out["pipeline"]))

    def test_mt5_closed_deal_sync_keeps_realized_pnl_stable_when_history_order_changes(self):
        def make_deals(reverse=False):
            positions = range(205, 0, -1) if reverse else range(1, 206)
            deals = []
            for position_id in positions:
                pnl = 1.0 if position_id % 2 else -0.5
                deals.append(
                    {
                        "ticket": position_id * 10,
                        "position_id": position_id,
                        "entry": 0,
                        "type": 0,
                        "time": 1000 + position_id,
                        "symbol": "XAUUSD",
                        "price": 4500.0,
                        "volume": 0.01,
                        "profit": 0.0,
                    }
                )
                deals.append(
                    {
                        "ticket": position_id * 10 + 1,
                        "position_id": position_id,
                        "entry": 1,
                        "type": 1,
                        "time": 2000 + position_id,
                        "symbol": "XAUUSD",
                        "price": 4501.0,
                        "volume": 0.01,
                        "profit": pnl,
                    }
                )
            return deals

        class FakeBroker:
            calls = 0

            def closed_deals(self, symbol=None, days=14):
                FakeBroker.calls += 1
                return {"ok": True, "deals": make_deals(reverse=FakeBroker.calls % 2 == 0)}

            def shutdown(self):
                return None

        with mock.patch.object(main_forex, "MT5Broker", FakeBroker):
            first_sync = main_forex._sync_mt5_closed_deals("XAUUSD", "DEMO")
            first_stats = main_forex._closed_trade_stats("DEMO")
            second_sync = main_forex._sync_mt5_closed_deals("XAUUSD", "DEMO")
            second_stats = main_forex._closed_trade_stats("DEMO")

        self.assertEqual(first_sync["synced"], 205)
        self.assertEqual(second_sync["synced"], 0)
        self.assertEqual(first_stats["trades"], 205)
        self.assertEqual(second_stats["trades"], 205)
        self.assertEqual(first_stats["realizedPnl"], second_stats["realizedPnl"])
        self.assertEqual(first_stats["wins"], second_stats["wins"])
        self.assertEqual(first_stats["losses"], second_stats["losses"])

    def test_stable_trade_reports_keeps_more_than_200_closed_mt5_deals(self):
        reports = [
            {
                "ts": index,
                "mode": "DEMO",
                "symbol": "XAUUSD",
                "pnl": -1.0,
                "exit": 4500.0,
                "reason": "mt5_closed_deal",
                "positionId": str(index),
            }
            for index in range(454)
        ]

        out = main_forex._stable_trade_reports(reports)

        self.assertEqual(len([item for item in out if main_forex._is_closed_trade(item)]), 454)
        main_forex._STATE["tradeReports"] = out
        stats = main_forex._closed_trade_stats("DEMO")
        self.assertEqual(stats["trades"], 454)
        self.assertEqual(stats["realizedPnl"], -454.0)


if __name__ == "__main__":
    unittest.main()
