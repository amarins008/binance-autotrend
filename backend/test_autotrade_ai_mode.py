import unittest
import tempfile
import json
import time
import asyncio
from pathlib import Path
from unittest import mock

import main
from schemas import AutoTradeStartRequest


class TestAutotradeAiMode(unittest.TestCase):
    def test_autotrade_defaults_to_ai_market_scan(self):
        req = AutoTradeStartRequest(usdtAmount=50)
        self.assertEqual(req.symbol, "AUTO")
        self.assertTrue(req.marketScan)
        self.assertFalse(req.orphanAutoAdoptForceSingleSymbol)
        self.assertEqual(req.maxOpenPositions, 6)
        self.assertEqual(req.scanTopLiquid, 60)
        self.assertEqual(req.scanAnalyzeTop, 12)
        self.assertEqual(req.tradeNotionalCapUsdt, 80.0)
        self.assertEqual(req.autoScanTradeNotionalCapUsdt, 80.0)

    def test_session_bias_fields_are_enabled_by_default(self):
        req = AutoTradeStartRequest(usdtAmount=50)
        self.assertTrue(req.sessionBiasEnabled)
        self.assertEqual(req.sessionBiasMinSamples, 10)

    def test_adaptive_trade_size_uses_tamed_boost(self):
        cfg = {
            "adaptiveSizing": True,
            "minConfidence": 0.65,
            "adaptiveSizeBoostMaxPct": 12.0,
            "perfWindowTrades": 30,
            "perfGateMinSamples": 8,
        }
        intel = {"confidence": 0.95}
        with mock.patch.object(main, "_symbol_quality_score", return_value=0.12), \
             mock.patch.object(main, "_rolling_symbol_perf", return_value={"trades": 0}), \
             mock.patch.object(main, "_load_learning_profiles", return_value={}):
            out = main._adaptive_trade_usdt(80.0, "BTCUSDT", intel, cfg)

        self.assertLessEqual(out, 89.6)


class TestEntrySessionBias(unittest.TestCase):
    def setUp(self):
        main._LIVE_STATS_CACHE.clear()
        main._SESSION_BIAS_CACHE.update({"builtAt": 0.0, "liveVersion": -1, "mtime": -1.0, "hours": {}})

    def _stable_now(self) -> int:
        parts = list(time.localtime(time.time()))
        parts[4] = 30
        parts[5] = 0
        return int(time.mktime(tuple(parts)))

    def _write_trade_log(self, path: Path, rows: list[dict]):
        path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")

    def test_session_bias_boosts_good_entry_hour(self):
        now = self._stable_now()
        rows = []
        for i in range(12):
            scan_ts = now - 900 - (i * 60)
            close_ts = scan_ts + 300
            rows.append({"ts": scan_ts, "mode": "SCAN", "symbol": "TESTUSDT", "signal": "LONG", "picked": True})
            rows.append({
                "ts": close_ts,
                "closedAt": close_ts,
                "mode": "LIVE",
                "symbol": "TESTUSDT",
                "side": "LONG",
                "entry": 100.0,
                "exit": 100.3,
                "pnl": 0.3,
            })
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "trades_log.jsonl"
            self._write_trade_log(log_path, rows)
            with mock.patch.object(main, "TRADES_LOG_PATH", log_path):
                main._SESSION_BIAS_CACHE.update({"builtAt": 0.0, "liveVersion": -1, "mtime": -1.0, "hours": {}})
                bias = main._entry_session_bias({"sessionBiasMinSamples": 5, "todayPerformanceGuardEnabled": False}, now_ts=now)
        self.assertEqual(bias["reason"], "boost_good_session")
        self.assertLess(bias["confidenceShift"], 0)
        self.assertGreater(bias["sizeMult"], 1.0)

    def test_session_bias_reduces_bad_high_vol_hour(self):
        now = self._stable_now()
        rows = []
        for i in range(12):
            scan_ts = now - 900 - (i * 60)
            close_ts = scan_ts + 300
            rows.append({"ts": scan_ts, "mode": "SCAN", "symbol": "TESTUSDT", "signal": "SHORT", "picked": True})
            rows.append({
                "ts": close_ts,
                "closedAt": close_ts,
                "mode": "LIVE",
                "symbol": "TESTUSDT",
                "side": "SHORT",
                "entry": 100.0,
                "exit": 102.0,
                "pnl": -0.4,
            })
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "trades_log.jsonl"
            self._write_trade_log(log_path, rows)
            with mock.patch.object(main, "TRADES_LOG_PATH", log_path):
                main._SESSION_BIAS_CACHE.update({"builtAt": 0.0, "liveVersion": -1, "mtime": -1.0, "hours": {}})
                bias = main._entry_session_bias({"sessionBiasMinSamples": 5, "todayPerformanceGuardEnabled": False}, now_ts=now)
        self.assertEqual(bias["reason"], "reduce_bad_session")
        self.assertGreater(bias["confidenceShift"], 0)
        self.assertLess(bias["sizeMult"], 1.0)

    def test_today_guard_suppresses_historical_good_session_boost(self):
        now = self._stable_now()
        rows = []
        for i in range(12):
            scan_ts = now - 86400 - 900 - (i * 60)
            close_ts = scan_ts + 300
            rows.append({"ts": scan_ts, "mode": "SCAN", "symbol": "TESTUSDT", "signal": "LONG", "picked": True})
            rows.append({
                "ts": close_ts,
                "closedAt": close_ts,
                "mode": "LIVE",
                "symbol": "TESTUSDT",
                "side": "LONG",
                "entry": 100.0,
                "exit": 100.4,
                "pnl": 0.4,
            })
        for i in range(8):
            close_ts = now - 600 + (i * 30)
            rows.append({
                "ts": close_ts,
                "closedAt": close_ts,
                "mode": "LIVE",
                "symbol": "LOSSUSDT",
                "side": "LONG",
                "entry": 100.0,
                "exit": 99.7,
                "pnl": -0.3,
            })
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "trades_log.jsonl"
            self._write_trade_log(log_path, rows)
            with mock.patch.object(main, "TRADES_LOG_PATH", log_path):
                main._LIVE_STATS_CACHE.clear()
                main._SESSION_BIAS_CACHE.update({"builtAt": 0.0, "liveVersion": -1, "mtime": -1.0, "hours": {}})
                bias = main._entry_session_bias({"sessionBiasMinSamples": 5}, now_ts=now)
        self.assertEqual(bias["reason"], "today_performance_guard")
        self.assertGreater(bias["confidenceShift"], 0)
        self.assertLess(bias["sizeMult"], 1.0)
        self.assertEqual(bias["todayGuard"]["winRatePct"], 0.0)

    def test_early_entry_pullback_reset_blocks_stretched_long_and_short(self):
        cfg = {
            "todayPerformanceGuardEnabled": False,
            "lateEntryMaxBbPctB": 0.90,
            "lateEntryMaxVwapDistancePct": 0.32,
            "earlyEntryMaxBbPctB": 0.82,
            "earlyEntryMinBbPctBShort": 0.18,
            "earlyEntryMaxVwapDistancePct": 0.24,
        }
        ok_long, reason_long = main._early_entry_pullback_reset_ok(
            "LONG",
            {"bbPctB": 0.96, "vwapDistancePct": 0.62},
            cfg,
        )
        ok_short, reason_short = main._early_entry_pullback_reset_ok(
            "SHORT",
            {"bbPctB": 0.04, "vwapDistancePct": -0.55},
            cfg,
        )
        ok_reset, reason_reset = main._early_entry_pullback_reset_ok(
            "LONG",
            {"bbPctB": 0.68, "vwapDistancePct": 0.12},
            cfg,
        )

        self.assertFalse(ok_long)
        self.assertIn("wait_pullback_reset_long", reason_long)
        self.assertFalse(ok_short)
        self.assertIn("wait_pullback_reset_short", reason_short)
        self.assertTrue(ok_reset)
        self.assertEqual(reason_reset, "ok")


class TestSupervisorRegimeReview(unittest.TestCase):
    def setUp(self):
        main.AUTO_TRADE["supervisorAutoTune"] = {}

    def test_supervisor_agent_exists_in_template(self):
        state = main.new_agent_state()

        self.assertIn("hermes_supervisor", state["agents"])
        self.assertEqual(state["kanban"]["todo"][0], "hermes_supervisor")
        self.assertIn("hermes_supervisor.md", state["agents"]["hermes_supervisor"]["playbookPath"])

    def test_supervisor_review_marks_control_plane_agent(self):
        prev_agents = main.AUTO_TRADE.get("hermesAgents")
        prev_running = main.AUTO_TRADE.get("running")
        prev_config = main.AUTO_TRADE.get("config")
        try:
            main.AUTO_TRADE["running"] = False
            main.AUTO_TRADE["config"] = {"executionMode": "PAPER", "marketScan": False, "symbol": "BTCUSDT"}
            main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()

            review = main._hermes_supervisor_review(main.AUTO_TRADE)

            supervisor = main.AUTO_TRADE["hermesAgents"]["agents"]["hermes_supervisor"]
            self.assertIn("hermes_supervisor", review["agentHealth"])
            self.assertEqual(supervisor["state"], "done")
            self.assertEqual(supervisor["lastAction"], "reviewed subagent health")
            self.assertGreaterEqual(supervisor["runs"], 1)
        finally:
            main.AUTO_TRADE["hermesAgents"] = prev_agents
            main.AUTO_TRADE["running"] = prev_running
            main.AUTO_TRADE["config"] = prev_config

    def test_external_signal_contradiction_applies_bounded_risk_tune(self):
        prev_agents = main.AUTO_TRADE.get("hermesAgents")
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        prev_signals = main.AUTO_TRADE.get("supervisorExternalSignals")
        prev_positions = main.AUTO_TRADE.get("openLivePositions")
        cfg = {
            "leverage": 20,
            "leverageMax": 25,
            "maxOpenPositions": 5,
            "minConfidence": 0.80,
            "scanPerfSoftFallbackEnabled": True,
            "scanGuardedFallbackConfRelax": 0.04,
            "supervisorSizeMultiplier": 1.0,
            "supervisorSizeMinMultiplier": 0.5,
            "profitLockTriggerUsdt": 0.35,
            "profitLockKeepUsdt": 0.10,
            "profitLockMaxGivebackUsdt": 0.22,
        }
        payload = {
            "source": "tradingview_mcp",
            "findings": [{
                "symbol": "ETHUSDT",
                "side": "LONG",
                "target": "open_position",
                "condition": "strong_mtf_contradiction",
                "alignment": "LEAN_BEARISH",
                "severity": "high",
            }],
        }
        try:
            main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
            main.AUTO_TRADE["supervisorAutoTune"] = {}
            main.AUTO_TRADE["openLivePositions"] = [{"symbol": "ETHUSDT", "side": "LONG", "qty": 0.01}]
            with mock.patch.object(main, "_persist_autotrade_snapshot", return_value=None):
                with mock.patch.object(main, "_autotrade_log", return_value=None):
                    out = main._maybe_tune_external_signal_guard(payload, cfg)

            self.assertTrue(out["applied"])
            self.assertEqual(cfg["leverage"], 20)
            self.assertEqual(cfg["leverageMax"], 25)
            self.assertEqual(cfg["supervisorSizeMultiplier"], 0.75)
            self.assertEqual(cfg["maxOpenPositions"], 3)
            self.assertFalse(cfg["scanPerfSoftFallbackEnabled"])
            self.assertEqual(cfg["scanGuardedFallbackConfRelax"], 0.025)
            self.assertLessEqual(cfg["profitLockTriggerUsdt"], 0.25)
            self.assertGreaterEqual(cfg["profitLockKeepUsdt"], 0.12)
            self.assertLessEqual(cfg["profitLockMaxGivebackUsdt"], 0.16)
            supervisor = main.AUTO_TRADE["hermesAgents"]["agents"]["hermes_supervisor"]
            self.assertEqual(supervisor["lastAction"], "bounded external-signal tune approved")
        finally:
            main.AUTO_TRADE["hermesAgents"] = prev_agents
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune
            main.AUTO_TRADE["supervisorExternalSignals"] = prev_signals
            main.AUTO_TRADE["openLivePositions"] = prev_positions

    def test_external_signal_pending_breakout_relaxes_only_scan_fallback(self):
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        prev_signals = main.AUTO_TRADE.get("supervisorExternalSignals")
        cfg = {
            "leverage": 7,
            "maxOpenPositions": 4,
            "minConfidence": 0.84,
            "scanFallbackNearEnabled": False,
            "scanGuardedFallbackEnabled": False,
            "scanGuardedFallbackConfRelax": 0.02,
            "scanAnalyzeTop": 8,
        }
        payload = {
            "source": "tradingview_mcp",
            "finding": {
                "symbol": "ONDOUSDT",
                "side": "LONG",
                "target": "pending_entry",
                "condition": "volume_breakout_aligned_pending_entry",
                "severity": "medium",
            },
        }
        try:
            main.AUTO_TRADE["supervisorAutoTune"] = {}
            with mock.patch.object(main, "_persist_autotrade_snapshot", return_value=None):
                with mock.patch.object(main, "_autotrade_log", return_value=None):
                    out = main._maybe_tune_external_signal_guard(payload, cfg)

            self.assertTrue(out["applied"])
            self.assertEqual(cfg["leverage"], 7)
            self.assertEqual(cfg["maxOpenPositions"], 4)
            self.assertEqual(cfg["minConfidence"], 0.84)
            self.assertTrue(cfg["scanFallbackNearEnabled"])
            self.assertTrue(cfg["scanGuardedFallbackEnabled"])
            self.assertEqual(cfg["scanGuardedFallbackConfRelax"], 0.04)
            self.assertEqual(cfg["scanAnalyzeTop"], 10)
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune
            main.AUTO_TRADE["supervisorExternalSignals"] = prev_signals

    def test_external_signal_data_failure_pauses_entries_and_reports_codex(self):
        prev_agents = main.AUTO_TRADE.get("hermesAgents")
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        prev_signals = main.AUTO_TRADE.get("supervisorExternalSignals")
        prev_pause = main.AUTO_TRADE.get("pauseUntil")
        prev_guard = main.AUTO_TRADE.get("externalSignalGuard")
        cfg = {
            "minConfidence": 0.82,
            "scanGuardedFallbackConfRelax": 0.04,
            "scanPerfSoftFallbackEnabled": True,
            "scanFallbackNearEnabled": True,
            "maxOpenPositions": 5,
        }
        payload = {
            "source": "tradingview_mcp",
            "finding": {
                "symbol": "VVVUSDT",
                "target": "guardian",
                "condition": "mcp_data_failure",
                "guardianActive": True,
                "severity": "high",
            },
        }
        try:
            main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
            main.AUTO_TRADE["supervisorAutoTune"] = {}
            main.AUTO_TRADE["openLivePositions"] = [{"symbol": "VVVUSDT", "side": "LONG", "qty": 1.0}]
            main.AUTO_TRADE["pauseUntil"] = 0
            out = main._maybe_tune_external_signal_guard(payload, cfg)

            self.assertTrue(out["applied"])
            self.assertEqual(cfg["minConfidence"], 0.84)
            self.assertEqual(cfg["maxOpenPositions"], 1)
            self.assertFalse(cfg["scanPerfSoftFallbackEnabled"])
            self.assertFalse(cfg["scanFallbackNearEnabled"])
            self.assertTrue(cfg["riskCooldownEnabled"])
            self.assertTrue(cfg["entryTradingViewBlockWeakSignal"])
            self.assertEqual(cfg["scanGuardedFallbackConfRelax"], 0.04)
            self.assertGreater(main.AUTO_TRADE["pauseUntil"], 0)
            self.assertEqual(out["codexActionReport"]["action"], "pause_new_entries")
            data_guard = main.AUTO_TRADE["hermesAgents"]["agents"]["data_quality_guard"]
            self.assertEqual(data_guard["state"], "blocked")
            self.assertEqual(data_guard["lastAction"], "external MCP data unavailable; entry guard active")
        finally:
            main.AUTO_TRADE["hermesAgents"] = prev_agents
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune
            main.AUTO_TRADE["supervisorExternalSignals"] = prev_signals
            main.AUTO_TRADE["pauseUntil"] = prev_pause
            if prev_guard is None:
                main.AUTO_TRADE.pop("externalSignalGuard", None)
            else:
                main.AUTO_TRADE["externalSignalGuard"] = prev_guard

    def test_tradingview_watcher_recovery_clears_data_failure_pause(self):
        prev_agents = main.AUTO_TRADE.get("hermesAgents")
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        prev_signals = main.AUTO_TRADE.get("supervisorExternalSignals")
        prev_config = main.AUTO_TRADE.get("config")
        prev_positions = main.AUTO_TRADE.get("openLivePositions")
        prev_watcher = main.AUTO_TRADE.get("tradingViewWatcher")
        prev_guardian = main.AUTO_TRADE.get("liveGuardian")
        prev_decision = main.AUTO_TRADE.get("lastDecision")
        prev_scan = main.AUTO_TRADE.get("scanBoard")
        prev_pause = main.AUTO_TRADE.get("pauseUntil")
        prev_guard = main.AUTO_TRADE.get("externalSignalGuard")
        prev_report = main.AUTO_TRADE.get("lastCodexReport")

        async def fake_provider(calls, cfg):
            return [
                {
                    **call,
                    "ok": True,
                    "data": {
                        "alignment": {"status": "LEAN BULLISH", "confidence": "Medium"},
                    } if call["name"] == "multi_timeframe_analysis" else {
                        "market_sentiment": {"buy_sell_signal": "NEUTRAL"},
                    },
                }
                for call in calls
            ]

        try:
            now = int(time.time())
            main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
            main.AUTO_TRADE["supervisorAutoTune"] = {}
            main.AUTO_TRADE["supervisorExternalSignals"] = []
            main.AUTO_TRADE["config"] = {"tradingViewWatcherEnabled": True, "tradingViewWatcherMaxSymbols": 1}
            main.AUTO_TRADE["openLivePositions"] = [{"symbol": "BTCUSDT", "side": "LONG", "qty": 0.002}]
            main.AUTO_TRADE["liveGuardian"] = None
            main.AUTO_TRADE["lastDecision"] = None
            main.AUTO_TRADE["scanBoard"] = []
            main.AUTO_TRADE["pauseUntil"] = now + 600
            main.AUTO_TRADE["externalSignalGuard"] = {
                "updatedAt": now,
                "until": now + 600,
                "reason": "tradingview_mcp_data_failure",
                "symbols": ["BTCUSDT"],
            }
            with mock.patch.object(main, "_persist_autotrade_snapshot", return_value=None):
                out = asyncio.run(main._tradingview_signal_watch_once(fake_provider))

            self.assertTrue(out["ok"])
            self.assertEqual(main.AUTO_TRADE["pauseUntil"], 0)
            self.assertEqual(out["codexActionReport"]["type"], "tradingview_mcp_recovered")
            self.assertEqual(main.AUTO_TRADE["lastCodexReport"]["action"], "clear_data_failure_entry_pause")
        finally:
            main.AUTO_TRADE["hermesAgents"] = prev_agents
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune
            main.AUTO_TRADE["supervisorExternalSignals"] = prev_signals
            main.AUTO_TRADE["config"] = prev_config
            main.AUTO_TRADE["openLivePositions"] = prev_positions
            main.AUTO_TRADE["tradingViewWatcher"] = prev_watcher
            main.AUTO_TRADE["liveGuardian"] = prev_guardian
            main.AUTO_TRADE["lastDecision"] = prev_decision
            main.AUTO_TRADE["scanBoard"] = prev_scan
            main.AUTO_TRADE["pauseUntil"] = prev_pause
            if prev_guard is None:
                main.AUTO_TRADE.pop("externalSignalGuard", None)
            else:
                main.AUTO_TRADE["externalSignalGuard"] = prev_guard
            if prev_report is None:
                main.AUTO_TRADE.pop("lastCodexReport", None)
            else:
                main.AUTO_TRADE["lastCodexReport"] = prev_report

    def test_tradingview_watcher_open_position_submits_supervisor_risk_tune(self):
        prev_agents = main.AUTO_TRADE.get("hermesAgents")
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        prev_signals = main.AUTO_TRADE.get("supervisorExternalSignals")
        prev_config = main.AUTO_TRADE.get("config")
        prev_positions = main.AUTO_TRADE.get("openLivePositions")
        prev_watcher = main.AUTO_TRADE.get("tradingViewWatcher")
        prev_guardian = main.AUTO_TRADE.get("liveGuardian")
        prev_decision = main.AUTO_TRADE.get("lastDecision")
        prev_scan = main.AUTO_TRADE.get("scanBoard")

        async def fake_provider(calls, cfg):
            self.assertTrue(any(call["name"] == "multi_timeframe_analysis" for call in calls))
            return [
                {
                    **call,
                    "ok": True,
                    "data": {
                        "alignment": {"status": "LEAN BEARISH", "confidence": "Medium"},
                    } if call["name"] == "multi_timeframe_analysis" else {
                        "market_sentiment": {"buy_sell_signal": "NEUTRAL"},
                    },
                }
                for call in calls
            ]

        try:
            main.AUTO_TRADE["hermesAgents"] = main.new_agent_state()
            main.AUTO_TRADE["supervisorAutoTune"] = {}
            main.AUTO_TRADE["supervisorExternalSignals"] = []
            main.AUTO_TRADE["config"] = {
                "tradingViewWatcherEnabled": True,
                "tradingViewWatcherMaxSymbols": 3,
                "supervisorSizeMultiplier": 1.0,
                "supervisorSizeMinMultiplier": 0.5,
                "maxOpenPositions": 5,
                "minConfidence": 0.80,
                "scanPerfSoftFallbackEnabled": True,
                "scanGuardedFallbackConfRelax": 0.04,
                "profitLockTriggerUsdt": 0.35,
                "profitLockKeepUsdt": 0.10,
                "profitLockMaxGivebackUsdt": 0.22,
            }
            main.AUTO_TRADE["openLivePositions"] = [{"symbol": "BTCUSDT", "side": "LONG", "qty": 0.002}]
            main.AUTO_TRADE["liveGuardian"] = None
            main.AUTO_TRADE["lastDecision"] = None
            main.AUTO_TRADE["scanBoard"] = []
            with mock.patch.object(main, "_persist_autotrade_snapshot", return_value=None):
                with mock.patch.object(main, "_autotrade_log", return_value=None):
                    out = asyncio.run(main._tradingview_signal_watch_once(fake_provider))

            self.assertTrue(out["ok"])
            self.assertTrue(out["submitted"])
            self.assertEqual(out["findings"][0]["symbol"], "BTCUSDT")
            self.assertEqual(out["findings"][0]["condition"], "strong_mtf_contradiction")
            self.assertEqual(main.AUTO_TRADE["config"]["supervisorSizeMultiplier"], 0.75)
            self.assertEqual(main.AUTO_TRADE["tradingViewWatcher"]["supervisorResult"]["reason"], "tradingview_mcp_internal")
        finally:
            main.AUTO_TRADE["hermesAgents"] = prev_agents
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune
            main.AUTO_TRADE["supervisorExternalSignals"] = prev_signals
            main.AUTO_TRADE["config"] = prev_config
            main.AUTO_TRADE["openLivePositions"] = prev_positions
            main.AUTO_TRADE["tradingViewWatcher"] = prev_watcher
            main.AUTO_TRADE["liveGuardian"] = prev_guardian
            main.AUTO_TRADE["lastDecision"] = prev_decision
            main.AUTO_TRADE["scanBoard"] = prev_scan

    def test_tradingview_watcher_uses_live_profit_locks_as_open_positions(self):
        state = {
            "openLivePositions": [],
            "liveProfitLocks": {
                "ETHUSDT": {
                    "symbol": "ETHUSDT",
                    "side": "LONG",
                    "qty": 0.031,
                }
            },
            "liveGuardian": None,
            "lastDecision": {"symbol": "TONUSDT", "intel": {"signal": "LONG"}},
            "scanBoard": [],
        }

        targets = main._tradingview_target_symbols(state, {"tradingViewWatcherMaxSymbols": 3})

        self.assertEqual(targets[0]["symbol"], "ETHUSDT")
        self.assertEqual(targets[0]["target"], "open_position")
        self.assertEqual(targets[0]["side"], "LONG")

    def test_tradingview_watcher_pending_breakout_relaxes_scan_only(self):
        prev_tune = main.AUTO_TRADE.get("supervisorAutoTune")
        prev_signals = main.AUTO_TRADE.get("supervisorExternalSignals")
        prev_config = main.AUTO_TRADE.get("config")
        prev_watcher = main.AUTO_TRADE.get("tradingViewWatcher")
        prev_positions = main.AUTO_TRADE.get("openLivePositions")
        prev_guardian = main.AUTO_TRADE.get("liveGuardian")
        prev_decision = main.AUTO_TRADE.get("lastDecision")
        prev_scan = main.AUTO_TRADE.get("scanBoard")

        async def fake_provider(calls, cfg):
            out = []
            for call in calls:
                if call["name"] == "multi_timeframe_analysis":
                    data = {"alignment": {"status": "LEAN BULLISH", "confidence": "Medium"}}
                elif call["name"] == "volume_confirmation_analysis":
                    data = {"overall_assessment": {"bullish_signals": 1, "bearish_signals": 0}}
                else:
                    data = {"market_sentiment": {"buy_sell_signal": "BUY"}}
                out.append({**call, "ok": True, "data": data})
            return out

        try:
            main.AUTO_TRADE["supervisorAutoTune"] = {}
            main.AUTO_TRADE["supervisorExternalSignals"] = []
            main.AUTO_TRADE["config"] = {
                "tradingViewWatcherEnabled": True,
                "tradingViewWatcherMaxSymbols": 2,
                "scanFallbackNearEnabled": False,
                "scanGuardedFallbackEnabled": False,
                "scanGuardedFallbackConfRelax": 0.02,
                "scanAnalyzeTop": 8,
                "maxOpenPositions": 4,
                "minConfidence": 0.84,
            }
            main.AUTO_TRADE["openLivePositions"] = []
            main.AUTO_TRADE["liveGuardian"] = None
            main.AUTO_TRADE["lastDecision"] = None
            main.AUTO_TRADE["scanBoard"] = [{"symbol": "ENAUSDT", "signal": "LONG", "qualified": True}]
            with mock.patch.object(main, "_persist_autotrade_snapshot", return_value=None):
                with mock.patch.object(main, "_autotrade_log", return_value=None):
                    out = asyncio.run(main._tradingview_signal_watch_once(fake_provider))

            self.assertTrue(out["submitted"])
            self.assertEqual(out["findings"][0]["condition"], "volume_breakout_aligned_pending_entry")
            self.assertTrue(main.AUTO_TRADE["config"]["scanFallbackNearEnabled"])
            self.assertTrue(main.AUTO_TRADE["config"]["scanGuardedFallbackEnabled"])
            self.assertEqual(main.AUTO_TRADE["config"]["scanAnalyzeTop"], 10)
        finally:
            main.AUTO_TRADE["supervisorAutoTune"] = prev_tune
            main.AUTO_TRADE["supervisorExternalSignals"] = prev_signals
            main.AUTO_TRADE["config"] = prev_config
            main.AUTO_TRADE["tradingViewWatcher"] = prev_watcher
            main.AUTO_TRADE["openLivePositions"] = prev_positions
            main.AUTO_TRADE["liveGuardian"] = prev_guardian
            main.AUTO_TRADE["lastDecision"] = prev_decision
            main.AUTO_TRADE["scanBoard"] = prev_scan

    def test_tradingview_watcher_pending_contradiction_creates_entry_gate_finding(self):
        targets = [{"symbol": "ENAUSDT", "side": "LONG", "target": "pending_entry"}]
        results = [
            {
                "symbol": "ENAUSDT",
                "name": "multi_timeframe_analysis",
                "ok": True,
                "data": {"alignment": {"status": "LEAN BEARISH", "confidence": "High"}},
            },
            {
                "symbol": "ENAUSDT",
                "name": "volume_confirmation_analysis",
                "ok": True,
                "data": {"overall_assessment": {"bullish_signals": 0, "bearish_signals": 1}},
            },
        ]

        findings = main._tradingview_findings_from_results(targets, results, {})

        self.assertEqual(findings[0]["condition"], "pending_entry_mtf_contradiction")
        self.assertEqual(findings[0]["target"], "pending_entry")
        self.assertEqual(findings[0]["severity"], "high")

    def test_entry_tradingview_context_blocks_pending_reversal(self):
        prev_watcher = main.AUTO_TRADE.get("tradingViewWatcher")
        try:
            main.AUTO_TRADE["tradingViewWatcher"] = {
                "updatedAt": int(time.time()),
                "findings": [{
                    "symbol": "ENAUSDT",
                    "side": "LONG",
                    "target": "pending_entry",
                    "condition": "pending_entry_mtf_contradiction",
                    "tradingViewSignal": "LEAN BEARISH",
                }],
            }

            out = main._entry_tradingview_context(
                "ENAUSDT",
                "LONG",
                {"entryTradingViewGateEnabled": True, "entryTradingViewBlockContradiction": True},
            )

            self.assertTrue(out["active"])
            self.assertTrue(out["block"])
            self.assertTrue(out["contradiction"])
        finally:
            main.AUTO_TRADE["tradingViewWatcher"] = prev_watcher

    def test_entry_tradingview_context_boosts_confirmed_pending_breakout(self):
        prev_watcher = main.AUTO_TRADE.get("tradingViewWatcher")
        try:
            main.AUTO_TRADE["tradingViewWatcher"] = {
                "updatedAt": int(time.time()),
                "findings": [{
                    "symbol": "ENAUSDT",
                    "side": "LONG",
                    "target": "pending_entry",
                    "condition": "volume_breakout_aligned_pending_entry",
                    "tradingViewSignal": "LEAN BULLISH",
                }],
            }

            out = main._entry_tradingview_context(
                "ENAUSDT",
                "LONG",
                {"entryTradingViewGateEnabled": True, "entryTradingViewConfirmConfidenceBoost": 0.02},
            )

            self.assertTrue(out["active"])
            self.assertFalse(out["block"])
            self.assertTrue(out["confirmed"])
            self.assertEqual(out["confidenceBoost"], 0.02)
        finally:
            main.AUTO_TRADE["tradingViewWatcher"] = prev_watcher

    def test_guardian_tradingview_context_tightens_long_sl_and_blocks_tp_extension(self):
        prev_watcher = main.AUTO_TRADE.get("tradingViewWatcher")
        try:
            main.AUTO_TRADE["tradingViewWatcher"] = {
                "updatedAt": int(time.time()),
                "findings": [{
                    "symbol": "ETHUSDT",
                    "side": "LONG",
                    "target": "open_position",
                    "condition": "strong_mtf_contradiction",
                    "tradingViewSignal": "LEAN BEARISH",
                }],
            }
            out = main._guardian_tradingview_context(
                "ETHUSDT",
                "LONG",
                mark=100.0,
                entry=98.0,
                qty=1.0,
                sl=95.0,
                tp=104.0,
                cfg={
                    "guardianTradingViewContextEnabled": True,
                    "guardianTradingViewTightenSlPct": 0.2,
                    "guardianTradingViewMinProfitLockUsdt": 0.5,
                    "guardianTradingViewBlockTpExtension": True,
                },
            )

            self.assertTrue(out["active"])
            self.assertTrue(out["changedSl"])
            self.assertGreater(out["tightenedSl"], 99.0)
            self.assertTrue(out["blockTpExtension"])
            self.assertTrue(out["armProfitLock"])
        finally:
            main.AUTO_TRADE["tradingViewWatcher"] = prev_watcher

    def test_guardian_tradingview_context_ignores_stale_signal(self):
        prev_watcher = main.AUTO_TRADE.get("tradingViewWatcher")
        try:
            main.AUTO_TRADE["tradingViewWatcher"] = {
                "updatedAt": int(time.time()) - 9999,
                "findings": [{
                    "symbol": "ETHUSDT",
                    "side": "LONG",
                    "target": "open_position",
                    "condition": "strong_mtf_contradiction",
                    "tradingViewSignal": "LEAN BEARISH",
                }],
            }
            out = main._guardian_tradingview_context(
                "ETHUSDT",
                "LONG",
                mark=100.0,
                entry=98.0,
                qty=1.0,
                sl=95.0,
                tp=104.0,
                cfg={"guardianTradingViewContextMaxAgeSec": 60},
            )

            self.assertFalse(out["active"])
        finally:
            main.AUTO_TRADE["tradingViewWatcher"] = prev_watcher

    def _today_noon(self) -> int:
        parts = list(time.localtime(time.time()))
        parts[3] = 12
        parts[4] = 0
        parts[5] = 0
        return int(time.mktime(tuple(parts)))

    def test_daily_regime_review_flags_today_vs_profitable_baseline(self):
        now = self._today_noon()
        rows = []
        for i in range(10):
            rows.append({
                "closedAt": now - 86400 + i * 600,
                "mode": "LIVE",
                "symbol": "BTCUSDT",
                "pnl": 0.45 if i < 8 else -0.25,
            })
        for i in range(8):
            rows.append({
                "closedAt": now - 3600 + i * 300,
                "mode": "LIVE",
                "symbol": "ADAUSDT",
                "pnl": 0.08 if i < 2 else -0.35,
            })

        review = main._daily_trade_regime_review(rows, {"supervisorDailyBaselineMinTrades": 8}, now_ts=now)

        self.assertTrue(review["degraded"])
        self.assertEqual(review["today"]["trades"], 8)
        self.assertGreater(review["winRateDropPct"], 40)
        self.assertGreater(review["baseline"]["pnl"], 0)

    def test_daily_entry_regression_tightens_entry_knobs(self):
        cfg = {
            "minConfidence": 0.66,
            "earlyEntryMinConfidence": 0.60,
            "earlyEntryScoreGapMin": 1.4,
            "earlyEntryMaxBbPctB": 0.82,
            "earlyEntryMinBbPctBShort": 0.18,
            "earlyEntryMaxVwapDistancePct": 0.24,
            "scanFallbackNearEnabled": True,
            "scanPerfSoftFallbackEnabled": True,
            "todayPerformanceGuardMinTrades": 8,
        }
        review = {
            "degraded": True,
            "today": {"day": "2026-06-06", "trades": 8, "winRatePct": 25.0, "pnl": -2.4},
            "baseline": {"day": "2026-06-03", "trades": 12, "winRatePct": 75.0, "pnl": 4.5},
        }

        with mock.patch.object(main, "_persist_autotrade_snapshot", return_value=None):
            with mock.patch.object(main, "_autotrade_log", return_value=None):
                out = main._maybe_tune_daily_entry_regression(review, cfg)

        self.assertTrue(out["applied"])
        self.assertGreater(cfg["minConfidence"], 0.66)
        self.assertLessEqual(cfg["minConfidence"], 0.84)
        self.assertLessEqual(cfg["earlyEntryMinConfidence"], 0.76)
        self.assertGreater(cfg["earlyEntryScoreGapMin"], 1.4)
        self.assertLessEqual(cfg["earlyEntryMaxBbPctB"], 0.78)
        self.assertTrue(cfg["scanFallbackNearEnabled"])
        self.assertFalse(cfg["scanPerfSoftFallbackEnabled"])
        self.assertEqual(cfg["todayPerformanceGuardMinTrades"], 6)

    def test_daily_entry_regression_reapplies_when_config_drifted_during_cooldown(self):
        cfg = {
            "minConfidence": 0.66,
            "earlyEntryMinConfidence": 0.60,
            "earlyEntryScoreGapMin": 1.4,
            "earlyEntryMaxBbPctB": 0.82,
            "earlyEntryMinBbPctBShort": 0.18,
            "earlyEntryMaxVwapDistancePct": 0.24,
            "scanFallbackNearEnabled": True,
            "scanPerfSoftFallbackEnabled": True,
        }
        review = {
            "degraded": True,
            "today": {"day": "2026-06-06", "trades": 8, "winRatePct": 25.0, "pnl": -2.4},
            "baseline": {"day": "2026-06-03", "trades": 12, "winRatePct": 75.0, "pnl": 4.5},
        }
        signature = "2026-06-06:8:25.0:-2.4:2026-06-03:4.5"
        main.AUTO_TRADE["supervisorAutoTune"] = {
            "delegations": {
                "daily_entry_regression": {
                    "at": int(time.time()),
                    "signature": signature,
                }
            }
        }

        with mock.patch.object(main, "_persist_autotrade_snapshot", return_value=None):
            with mock.patch.object(main, "_autotrade_log", return_value=None):
                out = main._maybe_tune_daily_entry_regression(review, cfg)

        self.assertTrue(out["applied"])
        self.assertGreater(cfg["minConfidence"], 0.66)
        self.assertTrue(cfg["scanFallbackNearEnabled"])

    def test_daily_entry_regression_caps_over_tight_confidence(self):
        cfg = {
            "minConfidence": 0.88,
            "earlyEntryMinConfidence": 0.80,
            "earlyEntryScoreGapMin": 2.4,
            "earlyEntryMaxBbPctB": 0.82,
            "earlyEntryMinBbPctBShort": 0.18,
            "earlyEntryMaxVwapDistancePct": 0.24,
            "scanFallbackNearEnabled": False,
            "scanPerfSoftFallbackEnabled": True,
            "scanFallbackNearConfRelax": 0.04,
        }
        review = {
            "degraded": True,
            "today": {"day": "2026-06-07", "trades": 15, "winRatePct": 20.0, "pnl": -3.6},
            "baseline": {"day": "2026-06-03", "trades": 155, "winRatePct": 66.45, "pnl": 15.3},
        }

        with mock.patch.object(main, "_persist_autotrade_snapshot", return_value=None):
            with mock.patch.object(main, "_autotrade_log", return_value=None):
                out = main._maybe_tune_daily_entry_regression(review, cfg)

        self.assertTrue(out["applied"])
        self.assertEqual(cfg["minConfidence"], 0.84)
        self.assertEqual(cfg["earlyEntryMinConfidence"], 0.76)
        self.assertEqual(cfg["earlyEntryScoreGapMin"], 2.1)
        self.assertTrue(cfg["scanFallbackNearEnabled"])
        self.assertEqual(cfg["scanFallbackNearConfRelax"], 0.035)

    def test_small_profit_capture_reduces_profit_giveback(self):
        cfg = {
            "holdWinners": True,
            "profitLockTriggerUsdt": 0.35,
            "profitLockKeepUsdt": 0.15,
            "profitLockMaxGivebackUsdt": 0.22,
            "profitLockBreakevenFloorUsdt": 0.08,
            "profitLockBreakevenTriggerUsdt": 0.16,
            "tpTargetMinUsdt": 0.55,
            "tpTargetMaxUsdt": 2.2,
        }
        review = {
            "label": "last_8_trades",
            "trades": 8,
            "smallWins": 4,
        }

        with mock.patch.object(main, "_persist_autotrade_snapshot", return_value=None):
            with mock.patch.object(main, "_autotrade_log", return_value=None):
                out = main._maybe_tune_small_profit_capture_from_review(review, cfg)

        self.assertTrue(out["applied"])
        self.assertLess(cfg["profitLockTriggerUsdt"], 0.35)
        self.assertGreaterEqual(cfg["profitLockKeepUsdt"], 0.18)
        self.assertLess(cfg["profitLockMaxGivebackUsdt"], 0.22)
        self.assertGreaterEqual(cfg["profitLockBreakevenFloorUsdt"], 0.10)


class TestRewardSystem(unittest.TestCase):
    def test_reward_components_reward_quality_win(self):
        cfg = {
            "minConfidence": 0.70,
            "maxSpreadBps": 16,
            "takeProfitPct": 1.8,
            "stopLossPct": 0.9,
            "minRiskRewardRatio": 1.5,
            "maxSlippageBps": 18,
        }
        trade = {
            "side": "LONG",
            "entry": 100.0,
            "exit": 101.0,
            "qty": 1.0,
            "pnl": 1.0,
            "reason": "TP_HIT",
            "openedAt": 1000,
            "closedAt": 1900,
            "entryConfidence": 0.82,
            "entrySpreadBps": 4.0,
            "patternBias": 0.8,
            "patternScore": 45.0,
        }

        comp = main._trade_reward_components(trade, cfg)

        self.assertGreater(comp["total"], 0.0)
        self.assertGreater(comp["entryQuality"], 0.0)
        self.assertGreater(comp["guardDiscipline"], 0.0)
        self.assertGreaterEqual(comp["riskReward"], 0.0)

    def test_reward_components_penalize_bad_loss(self):
        cfg = {
            "minConfidence": 0.74,
            "maxSpreadBps": 16,
            "takeProfitPct": 1.0,
            "stopLossPct": 1.0,
            "minRiskRewardRatio": 1.5,
            "maxSlippageBps": 18,
        }
        trade = {
            "side": "SHORT",
            "entry": 100.0,
            "exit": 101.0,
            "qty": 1.0,
            "pnl": -1.0,
            "reason": "SL_HIT_LOW_CONF",
            "openedAt": 1000,
            "closedAt": 1300,
            "entryConfidence": 0.60,
            "entrySpreadBps": 30.0,
            "patternBias": 0.7,
            "patternScore": 35.0,
            "maxAdversePct": 1.2,
        }

        comp = main._trade_reward_components(trade, cfg)

        self.assertLess(comp["total"], 0.0)
        self.assertLess(comp["entryQuality"], 0.0)
        self.assertLess(comp["guardDiscipline"], 0.0)
        self.assertLess(comp["riskReward"], 0.0)

    def test_record_learning_trade_persists_reward_components(self):
        prev_config = main.AUTO_TRADE.get("config")
        try:
            main.AUTO_TRADE["config"] = {"minConfidence": 0.70, "takeProfitPct": 1.8, "stopLossPct": 0.9}
            with tempfile.TemporaryDirectory() as tmp:
                vault = Path(tmp) / "vault"
                learn = vault / "learning_profiles.json"
                log_path = vault / "trades_log.jsonl"
                with mock.patch.object(main, "VAULT_DIR", vault):
                    with mock.patch.object(main, "LEARN_PATH", learn):
                        with mock.patch.object(main, "TRADES_LOG_PATH", log_path):
                            main._record_learning_trade(
                                "GOODUSDT",
                                {
                                    "side": "LONG",
                                    "entry": 100.0,
                                    "exit": 101.0,
                                    "qty": 1.0,
                                    "pnl": 1.0,
                                    "reason": "TP_HIT",
                                    "openedAt": 1000,
                                    "closedAt": 2000,
                                    "entryConfidence": 0.82,
                                    "entrySpreadBps": 3.0,
                                    "patternBias": 0.6,
                                },
                                "LIVE",
                            )
                            profiles = json.loads(learn.read_text(encoding="utf-8"))
        finally:
            main.AUTO_TRADE["config"] = prev_config

        profile = profiles["GOODUSDT"]
        self.assertGreater(profile["rewardScore"], 0.0)
        self.assertGreater(profile["rewardBehaviorDelta"], 0.0)
        self.assertIn("rewardComponents", profile)
        self.assertGreater(profile["rewardComponents"]["total"], 0.0)


class TestLossStreakSelfReview(unittest.TestCase):
    def test_loss_streak_signature_changes_only_after_new_closed_loss(self):
        now = int(time.time())
        rows = [
            {"ts": now - 30, "closedAt": now - 30, "mode": "LIVE", "symbol": "AUSDT", "side": "LONG", "pnl": -0.1},
            {"ts": now - 20, "closedAt": now - 20, "mode": "LIVE", "symbol": "BUSDT", "side": "SHORT", "pnl": -0.2},
            {"ts": now - 10, "closedAt": now - 10, "mode": "LIVE", "symbol": "CUSDT", "side": "LONG", "pnl": -0.3},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "trades_log.jsonl"
            log_path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
            with mock.patch.object(main, "TRADES_LOG_PATH", log_path):
                state = main._recent_live_loss_streak_state(8)
                same_state = main._recent_live_loss_streak_state(8)
                rows.append({"ts": now, "closedAt": now, "mode": "LIVE", "symbol": "DUSDT", "side": "LONG", "pnl": -0.4})
                log_path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
                new_state = main._recent_live_loss_streak_state(8)

        self.assertEqual(state["streak"], 3)
        self.assertEqual(state["signature"], same_state["signature"])
        self.assertEqual(new_state["streak"], 4)
        self.assertNotEqual(state["signature"], new_state["signature"])

    def test_loss_streak_self_review_tightens_config_and_blocks_bad_hour(self):
        now = int(time.time())
        hour = time.localtime(now).tm_hour
        hour_start = now - ((time.localtime(now).tm_min * 60) + time.localtime(now).tm_sec) + (30 * 60)
        rows = []
        for i in range(10):
            rows.append({
                "ts": hour_start - (i * 60),
                "closedAt": hour_start - (i * 60),
                "mode": "LIVE",
                "symbol": "BADUSDT",
                "side": "LONG",
                "entry": 100.0,
                "exit": 99.8,
                "pnl": -0.2,
            })
        cfg = {
            "minConfidence": 0.70,
            "scanFallbackNearEnabled": True,
            "scanSidePreference": "long",
            "pairLockEnabled": False,
            "pairLockMinutes": 45,
            "riskCooldownMinutes": 25,
            "maxOpenPositions": 5,
            "selfReviewMinHourSamples": 8,
        }
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "trades_log.jsonl"
            log_path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
            with mock.patch.object(main, "TRADES_LOG_PATH", log_path):
                main._SESSION_BIAS_CACHE.update({"builtAt": 0.0, "liveVersion": -1, "mtime": -1.0, "hours": {}})
                tuned = main._loss_streak_self_review_tune(cfg, now=now, loss_streak=4)

        self.assertEqual(tuned["scanSidePreference"], "score")
        self.assertFalse(tuned["scanFallbackNearEnabled"])
        self.assertTrue(tuned["pairLockEnabled"])
        self.assertEqual(tuned["pairLockMinutes"], 90)
        self.assertEqual(tuned["riskCooldownMinutes"], 45)
        self.assertEqual(tuned["maxOpenPositions"], 4)
        self.assertGreater(tuned["minConfidence"], cfg["minConfidence"])
        self.assertIn(f"{hour:02d}:00-{(hour + 1) % 24:02d}:00", tuned["noTradeWindows"])
        self.assertEqual(main.AUTO_TRADE["lastSelfReview"]["lossStreak"], 4)

    def test_loss_streak_self_review_preserves_auto_scan_diversification_slots(self):
        cfg = {
            "symbol": "AUTO",
            "marketScan": True,
            "minConfidence": 0.70,
            "scanFallbackNearEnabled": True,
            "maxOpenPositions": 6,
            "riskCooldownMinutes": 25,
        }

        tuned = main._loss_streak_self_review_tune(cfg, now=int(time.time()), loss_streak=4)

        self.assertEqual(tuned["maxOpenPositions"], 6)


if __name__ == "__main__":
    unittest.main()
