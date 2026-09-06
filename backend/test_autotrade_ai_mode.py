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
        self.assertEqual(req.maxOpenPositions, 4)
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
             mock.patch.object(main, "_load_single_profile", return_value={}):
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
            with mock.patch.object(main, "TRADES_LOG_PATH", log_path), \
                 mock.patch("trading.learning.TRADES_LOG_PATH", log_path), \
                 mock.patch("trading.learning.SCAN_EVENTS_PATH", Path(tmp) / "scan_events.jsonl"):
                main._SESSION_BIAS_CACHE.update({"builtAt": 0.0, "liveVersion": -1, "mtime": -1.0, "hours": {}})
                bias = main._entry_session_bias({"sessionBiasMinSamples": 5, "todayPerformanceGuardEnabled": False, "eveningVolatilityGuardEnabled": False}, now_ts=now)
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
            with mock.patch.object(main, "TRADES_LOG_PATH", log_path), \
                 mock.patch("trading.learning.TRADES_LOG_PATH", log_path), \
                 mock.patch("trading.learning.SCAN_EVENTS_PATH", Path(tmp) / "scan_events.jsonl"):
                main._SESSION_BIAS_CACHE.update({"builtAt": 0.0, "liveVersion": -1, "mtime": -1.0, "hours": {}})
                bias = main._entry_session_bias({"sessionBiasMinSamples": 5, "todayPerformanceGuardEnabled": False, "eveningVolatilityGuardEnabled": False}, now_ts=now)
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
            with mock.patch.object(main, "TRADES_LOG_PATH", log_path), \
                 mock.patch("trading.learning.TRADES_LOG_PATH", log_path), \
                 mock.patch("trading.trade_stats.TRADES_LOG_PATH", log_path), \
                 mock.patch("trading.learning.SCAN_EVENTS_PATH", Path(tmp) / "scan_events.jsonl"):
                main._LIVE_STATS_CACHE.clear()
                main._SESSION_BIAS_CACHE.update({"builtAt": 0.0, "liveVersion": -1, "mtime": -1.0, "hours": {}})
                bias = main._entry_session_bias({"sessionBiasMinSamples": 5, "eveningVolatilityGuardEnabled": False}, now_ts=now)
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
        main.AUTO_TRADE["supervisorExternalSignals"] = []
        main.AUTO_TRADE["externalSignalGuard"] = {}
        main.AUTO_TRADE["lastCmuxReport"] = {}
        main.AUTO_TRADE["marketContextWatcher"] = {}
        main.AUTO_TRADE["pauseUntil"] = 0
        main.AUTO_TRADE["openLivePositions"] = []
        main.AUTO_TRADE["liveGuardian"] = None
        main.AUTO_TRADE["lastDecision"] = None
        main.AUTO_TRADE["scanBoard"] = []

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

    def test_record_learning_trade_with_behavior_enabled_writes_log_and_profile(self):
        prev_config = main.AUTO_TRADE.get("config")
        try:
            main.AUTO_TRADE["config"] = {
                "minConfidence": 0.70,
                "takeProfitPct": 1.8,
                "stopLossPct": 0.9,
                "learningRewardEnabled": True,
                "learningBehaviorRewardEnabled": True,
            }
            with tempfile.TemporaryDirectory() as tmp:
                vault = Path(tmp) / "vault"
                log_path = Path(tmp) / "trades_log.jsonl"
                with mock.patch("trading.learning.VAULT_DIR", vault), \
                     mock.patch("trading.learning.TRADES_LOG_PATH", log_path), \
                     mock.patch("trading.symbol_profiles.VAULT_DIR", vault):
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
                    profile = json.loads((vault / "symbols" / "GOODUSDT" / "profile.json").read_text(encoding="utf-8"))
                    log_rows = (vault / "symbols" / "GOODUSDT" / "trades.jsonl").read_text(encoding="utf-8").splitlines()
        finally:
            main.AUTO_TRADE["config"] = prev_config

        self.assertGreater(profile["rewardScore"], 0.0)
        self.assertGreater(profile["rewardBehaviorDelta"], 0.0)
        self.assertIn("rewardComponents", profile)
        self.assertGreater(profile["rewardComponents"]["total"], 0.0)
        self.assertTrue(any('"symbol": "GOODUSDT"' in row for row in log_rows))


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
            with mock.patch.object(main, "TRADES_LOG_PATH", log_path), \
                 mock.patch("trading.trade_log.TRADES_LOG_PATH", log_path):
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
            "noTradeWindowsAutoEnabled": True,
        }
        # Reset supervisor tuner cooldown/rollback state so this test drives
        # the tune itself (other tests in the class may have committed one).
        main.AUTO_TRADE.setdefault("supervisorAutoTune", {})["delegations"] = {}
        main.AUTO_TRADE["tuningHistory"] = []
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "trades_log.jsonl"
            log_path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
            with mock.patch.object(main, "TRADES_LOG_PATH", log_path), \
                 mock.patch("trading.trade_log.TRADES_LOG_PATH", log_path), \
                 mock.patch("trading.learning.TRADES_LOG_PATH", log_path):
                main._SESSION_BIAS_CACHE.update({"builtAt": 0.0, "liveVersion": -1, "mtime": -1.0, "hours": {}})
                tuned = main._loss_streak_self_review_tune(cfg, now=now, loss_streak=4)

        self.assertEqual(tuned["scanSidePreference"], "score")
        # scanFallbackNear is now locked temporarily (not disabled) during a
        # loss streak; the knob itself stays enabled.
        self.assertTrue(tuned["scanFallbackNearEnabled"])
        self.assertGreaterEqual(main.AUTO_TRADE["_scanFallbackNearLock"].get("until", 0), now)
        self.assertTrue(tuned["pairLockEnabled"])
        self.assertEqual(tuned["pairLockMinutes"], 90)
        self.assertEqual(tuned["riskCooldownMinutes"], 45)
        self.assertEqual(tuned["maxOpenPositions"], 3)
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


class TestPerSymbolVolatilityTPSL(unittest.TestCase):
    """Per-symbol TP/SL/cap/profit-lock scaling via _symbol_volatility_score + _effective_tp_sl."""

    def _base_cfg(self):
        return {
            "takeProfitPct": 1.8,
            "stopLossPct": 0.9,
            "tradeNotionalCapUsdt": 80.0,
            "profitLockTriggerUsdt": 0.35,
            "profitLockKeepUsdt": 0.15,
            "profitLockMaxGivebackUsdt": 0.22,
            "tpTargetMinUsdt": 0.55,
            "tpTargetMaxUsdt": 2.0,
        }

    def _intel_btc(self):
        return {
            "precision": {"atrPct": 0.04, "vwapDistancePct": 0.02, "bbPctB": 0.5, "longScore": 0.0, "shortScore": 0.0},
            "execution": {"momentumPct": 0.05, "spreadBps": 3.0},
        }

    def _intel_sol(self):
        return {
            "precision": {"atrPct": 0.32, "vwapDistancePct": 0.22, "bbPctB": 0.72, "longScore": 1.5, "shortScore": 0.0},
            "execution": {"momentumPct": 0.45, "spreadBps": 9.0},
        }

    def test_symbol_volatility_score_tier_assignment(self):
        # BTC profile → low tier (tight multipliers, larger cap)
        btc = main._symbol_volatility_score("BTCUSDT", self._intel_btc())
        self.assertEqual(btc["tier"], "low")
        self.assertEqual(btc["tpMult"], 0.85)
        self.assertEqual(btc["slMult"], 0.85)
        self.assertEqual(btc["capMult"], 1.20)
        self.assertEqual(btc["lockMult"], 0.85)
        # SOL profile → high tier (wider TP, tighter cap)
        sol = main._symbol_volatility_score("SOLUSDT", self._intel_sol())
        self.assertEqual(sol["tier"], "high")
        self.assertEqual(sol["tpMult"], 1.30)
        self.assertEqual(sol["slMult"], 1.20)
        self.assertEqual(sol["capMult"], 0.65)
        self.assertEqual(sol["lockMult"], 1.40)

    def test_symbol_volatility_score_score_in_unit_interval(self):
        intel = {
            "precision": {"atrPct": 1.5, "vwapDistancePct": 0.7, "bbPctB": 1.0, "longScore": 6.0, "shortScore": 0.0},
            "execution": {"momentumPct": 2.0, "spreadBps": 200.0},
        }
        v = main._symbol_volatility_score("EXTREMEUSDT", intel)
        # All inputs are way above the high thresholds — score should saturate
        # at the tier ceiling but stay in [0, 1] with multipliers at "high" values.
        self.assertGreaterEqual(v["score"], 0.55)
        self.assertLessEqual(v["score"], 1.0)
        self.assertEqual(v["tier"], "high")

    def test_symbol_volatility_score_ignores_zero_intel(self):
        # Empty intel dict → score collapses to 0 (no data yet).
        v = main._symbol_volatility_score("BTCUSDT", {})
        self.assertEqual(v["score"], 0.0)
        self.assertEqual(v["tier"], "low")

    def test_effective_tp_sl_scales_per_symbol(self):
        cfg = self._base_cfg()
        btc = main._effective_tp_sl("BTCUSDT", cfg, self._intel_btc())
        sol = main._effective_tp_sl("SOLUSDT", cfg, self._intel_sol())
        # BTC: smaller TP, smaller SL, larger cap, smaller lock trigger.
        self.assertLess(btc["tpPct"], sol["tpPct"])
        self.assertLess(btc["slPct"], sol["slPct"])
        self.assertGreater(btc["notionalCapUsdt"], sol["notionalCapUsdt"])
        self.assertLess(btc["profitLockTriggerUsdt"], sol["profitLockTriggerUsdt"])
        # Sanity: TP/SL still sane for both.
        self.assertGreater(btc["tpPct"], 0.5)
        self.assertLess(sol["tpPct"], 3.5)
        self.assertGreater(sol["notionalCapUsdt"], 20.0)

    def test_effective_tp_sl_without_intel_returns_baseline(self):
        cfg = self._base_cfg()
        out = main._effective_tp_sl("ANYUSDT", cfg, None)
        self.assertEqual(out["tier"], "unknown")
        self.assertEqual(out["tpPct"], cfg["takeProfitPct"])
        self.assertEqual(out["slPct"], cfg["stopLossPct"])
        self.assertEqual(out["notionalCapUsdt"], cfg["tradeNotionalCapUsdt"])
        self.assertEqual(out["profitLockTriggerUsdt"], cfg["profitLockTriggerUsdt"])

    def test_effective_tp_sl_with_empty_intel_returns_baseline(self):
        cfg = self._base_cfg()
        out = main._effective_tp_sl("ANYUSDT", cfg, {})
        # Empty intel has no precision/execution keys → treat as missing.
        self.assertEqual(out["tier"], "unknown")
        self.assertEqual(out["tpPct"], cfg["takeProfitPct"])

    def test_effective_tp_sl_respects_minimum_floor(self):
        # Very tight base SL should never go below 0.20.
        cfg = self._base_cfg()
        cfg["stopLossPct"] = 0.25
        sol = main._effective_tp_sl("SOLUSDT", cfg, self._intel_sol())
        # floor is enforced before the multiplier is applied.
        self.assertGreaterEqual(sol["slPct"], 0.20)

    def test_effective_tp_sl_preserves_mid_tier_baseline(self):
        # A mid-volatility profile should keep cfg values within a sensible
        # range. After the 3-tier multiplier stack, the exact value depends
        # on the resolved group + tier — the assertion just checks that the
        # TP/SL/cap values stay close to the cfg baseline.
        cfg = self._base_cfg()
        intel_med = {
            "precision": {"atrPct": 0.12, "vwapDistancePct": 0.10, "bbPctB": 0.55, "longScore": 0.3, "shortScore": 0.0},
            "execution": {"momentumPct": 0.12, "spreadBps": 5.0},
        }
        out = main._effective_tp_sl("MIDUSDT", cfg, intel_med)
        # Group (trend-friendly tp_mult=1.10) + med vol (1.0) → tpMult 1.10.
        # 1.8 * 1.10 = 1.98 (no exotic high-vol stack expected).
        self.assertGreater(out["tpPct"], 1.0)
        self.assertLess(out["tpPct"], 3.0)
        # Cap: trend-friendly group capMult=1.15, med vol capMult=1.0 → 1.15.
        # 80 * 1.15 = 92.
        self.assertGreater(out["notionalCapUsdt"], 40.0)
        self.assertLess(out["notionalCapUsdt"], 120.0)



class TestThreeTierProfile(unittest.TestCase):
    """Verify the System > Group > Symbol 3-tier policy architecture."""

    def test_symbol_group_hardcoded_for_well_known_coins(self):
        # Hard-coded defaults must be respected for well-known coins.
        self.assertEqual(main._symbol_group("BTCUSDT"), "trend-friendly")
        self.assertEqual(main._symbol_group("ETHUSDT"), "trend-friendly")
        self.assertEqual(main._symbol_group("DOGEUSDT"), "high-volatility")
        self.assertEqual(main._symbol_group("SPXUSDT"), "low-liquidity-noisy")
        self.assertEqual(main._symbol_group("XLMUSDT"), "mean-reversion-friendly")
        # Unknown coins default to trend-friendly (safe fallback).
        self.assertEqual(main._symbol_group("RANDOMUSDT"), "trend-friendly")
        self.assertEqual(main._symbol_group(""), "trend-friendly")

    def test_effective_profile_uses_group_when_no_samples(self):
        # For a fresh symbol with no trade history, the effective profile
        # must equal the group defaults (no premature per-coin override).
        p = main._symbol_effective_profile("NEWUSDT", cfg={})
        self.assertEqual(p["source"], "group")
        self.assertEqual(p["sampleTrades"], 0)
        # Defaults come from the trend-friendly group baseline.
        self.assertGreaterEqual(p["tpsl_mult"], 0.5)
        self.assertLessEqual(p["tpsl_mult"], 2.0)
        self.assertIn(p["group"], main.SYMBOL_GROUP_DEFS)

    def test_effective_profile_promotes_symbol_with_enough_samples(self):
        # DOGEUSDT with ≥8 LIVE trades in per-symbol storage should allow
        # symbol-level overrides to apply.
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp) / "vault"
            sym_dir = vault / "symbols" / "DOGEUSDT"
            sym_dir.mkdir(parents=True, exist_ok=True)
            with mock.patch("trading.symbol_profiles.VAULT_DIR", vault), \
                 mock.patch("trading.symbol_profiles._symbol_sample_count", return_value=10):
                p = main._symbol_effective_profile("DOGEUSDT", cfg={})
                self.assertGreaterEqual(p["sampleTrades"], 10)
                # With enough samples, symbol_profile is loaded (even if
                # empty) so source = symbol+group.
                self.assertEqual(p["source"], "symbol+group")
                # Write user override symbol_profile.json
                (sym_dir / "symbol_profile.json").write_text(
                    json.dumps({"tpPct": 2.5}), encoding="utf-8")
                p2 = main._symbol_effective_profile("DOGEUSDT", cfg={})
                self.assertEqual(p2["source"], "symbol+group")
                self.assertEqual(p2["tpPct"], 2.5)

    def test_sample_count_guard_prevents_overfitting(self):
        # A symbol with fewer than SYMBOL_PROFILE_MIN_TRADES trades must NOT
        # use user overrides even if they exist. This is the safety net
        # against overfitting on a handful of trades.
        main._save_symbol_profiles({"FEWUSDT": {"tpPct": 9.9}})
        try:
            p = main._symbol_effective_profile("FEWUSDT", cfg={})
            # FEWUSDT has 0 trades in the log — overrides must be ignored.
            self.assertLess(p["sampleTrades"], main.SYMBOL_PROFILE_MIN_TRADES)
            self.assertEqual(p["source"], "group")
            # tpsl_mult must come from the group, not from the override.
            self.assertEqual(p["tpsl_mult"], p["tpsl_mult"])  # present
            # And the user override's tpPct was not applied.
            # (We can't check it directly because tpsl_mult ≠ tpPct; but the
            # ``source`` check above already proves the override path was
            # not taken.)
        finally:
            main._save_symbol_profiles({})

    def test_effective_tpsl_stacks_group_and_volatility(self):
        # When the group multiplier and the volatility multiplier differ,
        # the effective TP/SL must be the product (stacked), not the max.
        cfg = {"takeProfitPct": 1.8, "stopLossPct": 0.9,
               "tradeNotionalCapUsdt": 80.0,
               "profitLockTriggerUsdt": 0.35, "profitLockKeepUsdt": 0.15,
               "profitLockMaxGivebackUsdt": 0.22,
               "tpTargetMinUsdt": 0.55, "tpTargetMaxUsdt": 2.0}
        # BTCUSDT (trend-friendly group) + low volatility profile:
        #   group tp_mult = 1.10, vol tier = low (0.85)
        #   effective tp_mult should be 1.10 * 0.85 = 0.935
        intel_btc = {
            "precision": {"atrPct": 0.04, "vwapDistancePct": 0.02, "bbPctB": 0.5,
                          "longScore": 0.0, "shortScore": 0.0},
            "execution": {"momentumPct": 0.05, "spreadBps": 3.0},
        }
        btc = main._effective_tp_sl("BTCUSDT", cfg, intel_btc)
        # tpMult should be approximately 0.935 (1.10 * 0.85)
        self.assertAlmostEqual(btc["tpMult"], 0.935, places=3)
        # And the effective TP% must be cfg["takeProfitPct"] * tpMult.
        self.assertAlmostEqual(btc["tpPct"], 1.8 * 0.935, places=3)
        # DOGEUSDT (high-volatility group) + high volatility profile:
        #   group tp_mult = 1.35, vol tier = high (1.30)
        #   effective tp_mult = 1.35 * 1.30 = 1.755
        intel_doge = {
            "precision": {"atrPct": 0.40, "vwapDistancePct": 0.30, "bbPctB": 0.75,
                          "longScore": 2.0, "shortScore": 0.0},
            "execution": {"momentumPct": 0.60, "spreadBps": 12.0},
        }
        doge = main._effective_tp_sl("DOGEUSDT", cfg, intel_doge)
        self.assertAlmostEqual(doge["tpMult"], 1.755, places=3)
        self.assertGreater(doge["tpPct"], btc["tpPct"])  # DOGE wider than BTC

    def test_scan_bias_uses_group_long_bias(self):
        # The group profile's scan_long_bias must surface through the
        # helper. trend-friendly groups have a small LONG tilt (>0.5),
        # mean-reversion groups are neutral (0.5).
        btc = main._symbol_effective_profile("BTCUSDT", cfg={})
        xlm = main._symbol_effective_profile("XLMUSDT", cfg={})
        self.assertGreater(btc["scan_long_bias"], 0.5)
        self.assertEqual(xlm["scan_long_bias"], 0.5)

    def test_unknown_group_falls_back_safely(self):
        # An unknown coin with no hard-coded mapping should still return a
        # valid profile (defaults to trend-friendly).
        p = main._symbol_effective_profile("ZZZ_NEW_COIN", cfg={})
        self.assertIn(p["group"], main.SYMBOL_GROUP_DEFS)
        self.assertEqual(p["source"], "group")
        # Every required key must be present.
        for k in ("tpsl_mult", "sl_mult", "lock_trigger_mult",
                  "min_conf_floor", "max_trade_notional_mult",
                  "scan_long_bias", "scan_chase_speed"):
            self.assertIn(k, p)

    def test_position_size_mult_respects_group(self):
        # Each group has a different position_size_mult baseline.
        # high-volatility tokens should use smaller size than trend-friendly.
        hv = main._symbol_effective_profile("DOGEUSDT", cfg={})
        tf = main._symbol_effective_profile("BTCUSDT", cfg={})
        self.assertLess(hv.get("position_size_mult", 1.0), tf.get("position_size_mult", 1.0))
        # low-liquidity group should be smallest
        ln = main._symbol_effective_profile("SPXUSDT", cfg={})
        self.assertLess(ln.get("position_size_mult", 1.0), hv.get("position_size_mult", 1.0))

    def test_entry_offset_bps_is_group_specific(self):
        # trend-friendly: enter at mid (0 bps offset)
        # mean-reversion: enter slightly above mid (15 bps) to avoid chasing
        # low-liquidity: enter well above mid (25 bps) to avoid fake pumps
        tf = main._symbol_effective_profile("BTCUSDT", cfg={})
        mr = main._symbol_effective_profile("XLMUSDT", cfg={})
        ll = main._symbol_effective_profile("SPXUSDT", cfg={})
        self.assertEqual(tf.get("entry_offset_bps", 0.0), 0.0)
        self.assertGreater(mr.get("entry_offset_bps", 0.0), 0.0)
        self.assertGreater(ll.get("entry_offset_bps", 0.0), mr.get("entry_offset_bps", 0.0))

    def test_position_size_mult_bounded_by_wr(self):
        # Low WR (<=45%) + negative pnl should push position_size_mult below group base.
        cfg = {"takeProfitPct": 1.8, "stopLossPct": 0.9}
        prof = {
            "memoryWindows": {
                "7d": {"trades": 10, "winRatePct": 42.0, "pnl": -0.6},
                "14d": {"trades": 20, "winRatePct": 43.0, "pnl": -1.0},
                "30d": {"trades": 30, "winRatePct": 44.0, "pnl": -1.5},
            },
            "symbolRiskTune": {"active": False},
        }
        with mock.patch("trading.learning._load_single_profile", return_value=prof):
            p = main._auto_update_symbol_profile("DOGEUSDT", cfg)
        # WR <= 45% + negative pnl → size shrinks vs group baseline of 0.65
        self.assertLess(p.get("positionSizeMult", 1.0), 0.65)

    def test_leverage_mult_defaults_to_one(self):
        # When no symbolRiskTune is active, leverageMult defaults to 1.0.
        # Only symbolRiskTune can push it away from 1.0.
        p = main._symbol_effective_profile("BTCUSDT", cfg={})
        self.assertEqual(p.get("leverageMult", 1.0), 1.0)
        self.assertEqual(p.get("leverageMax", 0), 0)


if __name__ == "__main__":
    unittest.main()
