import time
import unittest
from unittest import mock

import main
import trading.supervisor_state as supervisor_state
import trading.supervisor_tuning as supervisor_tuning
from fastapi.testclient import TestClient


class TestSupervisorState(unittest.TestCase):
    def setUp(self):
        self.prev_config = main.AUTO_TRADE.get("config")
        self.prev_state = main.AUTO_TRADE.get("supervisorAutoTune")
        self.prev_history = main.AUTO_TRADE.get("tuningHistory")
        self.prev_suggestions = main.AUTO_TRADE.get("tuningSuggestions")
        self.prev_log = main.AUTO_TRADE.get("log")
        main.AUTO_TRADE["config"] = {"supervisorAutoTuneEnabled": True, "operatorKey": "keep"}
        main.AUTO_TRADE["supervisorAutoTune"] = {}
        main.AUTO_TRADE["tuningHistory"] = []
        main.AUTO_TRADE["tuningSuggestions"] = []
        main.AUTO_TRADE["log"] = []
        self.persist = mock.patch.object(main, "_persist_autotrade_snapshot")
        self.persist.start()
        self.addCleanup(self.persist.stop)

    def tearDown(self):
        main.AUTO_TRADE["config"] = self.prev_config
        main.AUTO_TRADE["supervisorAutoTune"] = self.prev_state
        main.AUTO_TRADE["tuningHistory"] = self.prev_history
        main.AUTO_TRADE["tuningSuggestions"] = self.prev_suggestions
        main.AUTO_TRADE["log"] = self.prev_log
        supervisor_state._tuning_mode_lock_release()

    def _state_and_delegations(self):
        state = main.AUTO_TRADE["supervisorAutoTune"]
        state.setdefault("delegations", {})
        return state, state["delegations"]

    def test_commit_merges_only_recorded_changes_not_stale_cfg(self):
        state, delegations = self._state_and_delegations()
        stale_cfg = {"supervisorAutoTuneEnabled": True, "operatorKey": "stale", "obsolete": 1}
        out = supervisor_state._commit_supervisor_config_tune(
            state,
            delegations,
            "probe",
            stale_cfg,
            {"tunedKey": {"old": 1, "new": 2}},
            "probe",
        )
        self.assertTrue(out["applied"])
        live = main.AUTO_TRADE["config"]
        self.assertEqual(live["operatorKey"], "keep")
        self.assertEqual(live["tunedKey"], 2)
        self.assertNotIn("obsolete", live)

    def test_rollback_restores_old_value_without_creating_new_history(self):
        state, delegations = self._state_and_delegations()
        main.AUTO_TRADE["config"] = {"supervisorAutoTuneEnabled": True, "stopLossPct": 0.8}
        main.AUTO_TRADE["tuningHistory"] = [{
            "at": int(time.time()),
            "key": "weak_payoff",
            "changes": {"stopLossPct": {"old": 1.0, "new": 0.8}},
            "preMetrics": {"trades": 20},
            "reverted": False,
        }]
        out = supervisor_state.rollback_supervisor_config_tune(state, delegations, "weak_payoff")
        self.assertTrue(out["applied"])
        self.assertTrue(out["rollback"])
        self.assertEqual(main.AUTO_TRADE["config"]["stopLossPct"], 1.0)
        self.assertTrue(main.AUTO_TRADE["tuningHistory"][0]["reverted"])
        self.assertEqual(len(main.AUTO_TRADE["tuningHistory"]), 1)

    def test_rollback_preserves_operator_override(self):
        state, delegations = self._state_and_delegations()
        main.AUTO_TRADE["config"] = {"supervisorAutoTuneEnabled": True, "stopLossPct": 0.9}
        main.AUTO_TRADE["tuningHistory"] = [{
            "at": int(time.time()),
            "key": "weak_payoff",
            "changes": {"stopLossPct": {"old": 1.0, "new": 0.8}},
            "preMetrics": {"trades": 20},
            "reverted": False,
        }]
        out = supervisor_state.rollback_supervisor_config_tune(state, delegations, "weak_payoff")
        self.assertFalse(out["applied"])
        self.assertEqual(out["reason"], "operator_override")
        self.assertEqual(main.AUTO_TRADE["config"]["stopLossPct"], 0.9)
        self.assertTrue(main.AUTO_TRADE["tuningHistory"][0]["reverted"])

    def test_windowed_rollback_uses_only_post_tune_closed_trades(self):
        now = int(time.time())
        rows = [
            {"closedAt": now - 120, "pnl": 0.5},
            {"closedAt": now - 110, "pnl": 0.4},
            {"closedAt": now - 100, "pnl": 0.3},
            {"closedAt": now + 1, "pnl": -0.3},
            {"closedAt": now + 2, "pnl": -0.2},
            {"closedAt": now + 3, "pnl": -0.4},
        ]
        main.AUTO_TRADE["tuningHistory"] = [{
            "at": now,
            "key": "probe",
            "changes": {"x": {"old": 1, "new": 2}},
            "preMetrics": {"winRatePct": 90.0, "avgPnl": 0.3, "trades": 20},
            "reverted": False,
        }]
        with mock.patch.object(supervisor_state, "_recent_closed_trades", return_value=rows), \
             mock.patch.object(supervisor_state.time, "time", return_value=now + 90):
            self.assertTrue(supervisor_state._tuning_should_rollback("probe"))

    def test_weak_payoff_cooldown_blocks_signature_drift(self):
        cfg = {
            "supervisorAutoTuneEnabled": True,
            "supervisorPayoffTuneCooldownMinutes": 45,
            "holdWinners": True,
            "holdMinConfidence": 0.8,
            "tpTargetMinUsdt": 1.0,
            "tpTargetMaxUsdt": 2.0,
        }
        main.AUTO_TRADE["config"] = dict(cfg)
        main.AUTO_TRADE["supervisorAutoTune"] = {"delegations": {"weak_payoff": {"at": int(time.time()), "signature": "old"}}}
        out = main._maybe_tune_weak_payoff_from_review(
            {"trades": 10, "payoffRatio": 0.35, "avgWin": 0.4, "avgLoss": -1.0},
            main.AUTO_TRADE["config"],
        )
        self.assertFalse(out["applied"])
        self.assertTrue(out["alreadyTuned"])

    def test_enabled_tuner_never_mutates_caller_config_before_commit(self):
        cfg = {
            "supervisorAutoTuneEnabled": True,
            "holdWinners": False,
            "holdMinConfidence": 0.8,
            "tpTargetMinUsdt": 1.0,
            "tpTargetMaxUsdt": 2.0,
        }
        original = dict(cfg)
        with mock.patch.object(main, "_commit_supervisor_config_tune", return_value={"applied": False}):
            main._maybe_tune_weak_payoff_from_review(
                {"trades": 10, "payoffRatio": 0.35, "avgWin": 0.4, "avgLoss": -1.0},
                cfg,
            )
        self.assertEqual(cfg, original)

    def test_tv_healthy_recovery_persists_while_tuning_disabled(self):
        class FakeTV:
            def get_health_status(self):
                return {"healthy": True, "fail_count": 0}

            def force_enable(self):
                return None

        main.AUTO_TRADE["config"] = {
            "supervisorAutoTuneEnabled": False,
            "supervisorHealingEnabled": True,
            "tradingviewEnabled": False,
        }
        with mock.patch("trading.tradingview_mcp.get_tv_client", return_value=FakeTV()), \
             mock.patch("trading.tradingview_mcp.reset_tv_client"):
            out = supervisor_tuning._maybe_tune_tradingview_health(main.AUTO_TRADE["config"])
        self.assertTrue(out["applied"])
        self.assertTrue(main.AUTO_TRADE["config"]["tradingviewEnabled"])
        self.assertEqual(len(main.AUTO_TRADE["tuningHistory"]), 0)

    def test_tuning_status_and_manual_rollback_routes(self):
        routes = {getattr(route, "path", None) for route in main.app.routes}
        self.assertIn("/hermes/supervisor/tuning", routes)
        self.assertIn("/hermes/supervisor/tuning/{key}/rollback", routes)
        main.AUTO_TRADE["config"] = {"supervisorAutoTuneEnabled": False, "stopLossPct": 0.8}
        main.AUTO_TRADE["tuningHistory"] = [{
            "at": int(time.time()),
            "key": "manual_probe",
            "changes": {"stopLossPct": {"old": 1.0, "new": 0.8}},
            "preMetrics": {},
            "reverted": False,
        }]
        client = TestClient(main.app)
        status = client.get("/hermes/supervisor/tuning")
        self.assertEqual(status.status_code, 200)
        self.assertIn("switches", status.json())
        rollback = client.post("/hermes/supervisor/tuning/manual_probe/rollback")
        self.assertEqual(rollback.status_code, 200)
        self.assertTrue(rollback.json()["ok"])
        self.assertEqual(main.AUTO_TRADE["config"]["stopLossPct"], 1.0)


if __name__ == "__main__":
    unittest.main()
