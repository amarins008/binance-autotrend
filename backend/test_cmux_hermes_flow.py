import argparse
import json
import unittest
from unittest import mock

import cmux_service
import cmux_cli
import hermes_service


class _FakeHttpResponse:
    status = 200

    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class TestCmuxBootstrap(unittest.TestCase):
    def test_req_starts_cmux_before_sending_command(self):
        calls = []

        def fake_urlopen(req, timeout):
            calls.append(
                {
                    "url": req.full_url,
                    "method": req.get_method(),
                    "payload": json.loads(req.data.decode("utf-8")),
                    "timeout": timeout,
                }
            )
            return _FakeHttpResponse({"ok": True, "from": "cmux"})

        with mock.patch.object(cmux_cli, "_ensure_cmux_running", return_value={"ok": True}) as ensure_cmux:
            with mock.patch.object(cmux_cli.urllib.request, "urlopen", side_effect=fake_urlopen):
                out = cmux_cli._req("POST", "/bot/start", {"symbol": "BTCUSDT"})

        self.assertEqual(out, {"ok": True, "from": "cmux"})
        ensure_cmux.assert_called_once()
        self.assertEqual(calls[0]["url"], f"{cmux_cli.CMUX_BASE}/bot/start")
        self.assertEqual(calls[0]["method"], "POST")
        self.assertEqual(calls[0]["payload"], {"symbol": "BTCUSDT"})

    def test_cmd_service_start_routes_through_cmux(self):
        with mock.patch.object(cmux_cli, "_req", return_value={"ok": True, "service": {"ok": True}}) as req:
            out = cmux_cli.cmd_service(argparse.Namespace(action="start"))

        self.assertTrue(out["ok"])
        req.assert_called_once_with("POST", "/service/start", {})


class _FakeHermes:
    HERMES_BASE = "http://127.0.0.1:8020"

    def __init__(self, start_ok=True):
        self.start_ok = start_ok
        self.calls = []

    def start(self):
        self.calls.append("start")
        return {"ok": self.start_ok, "name": "Hermes"}

    def stop(self):
        self.calls.append("stop")
        return {"ok": True, "stopped": True}

    def status(self):
        self.calls.append("status")
        return {"ok": True, "healthy": True}

    def autotrade_start(self, payload):
        self.calls.append(("autotrade_start", dict(payload)))
        return {"ok": True, "payload": dict(payload)}

    def autotrade_stop(self, payload):
        self.calls.append(("autotrade_stop", dict(payload)))
        return {"ok": True}

    def autotrade_update_config(self, payload):
        self.calls.append(("autotrade_update_config", dict(payload)))
        return {"ok": True}

    def autotrade_close_orphan(self, payload):
        self.calls.append(("autotrade_close_orphan", dict(payload)))
        return {"ok": True}

    def autotrade_adopt_live(self, payload):
        self.calls.append(("autotrade_adopt_live", dict(payload)))
        return {"ok": True}

    def autotrade_precheck_live(self, symbol):
        self.calls.append(("autotrade_precheck_live", symbol))
        return {"ok": True}


class TestCmuxToHermes(unittest.TestCase):
    def setUp(self):
        cmux_service._STATUS_CACHE.update({"ts": 0.0, "data": None})
        cmux_service._STATUS_REFRESHING = False
        cmux_service._RICH_BOT_STATUS_CACHE = {}

    def test_bot_start_starts_hermes_then_autotrade(self):
        fake = _FakeHermes()
        payload = {"symbol": "BTCUSDT", "executionMode": "PAPER", "autoLearn": False}

        with mock.patch.object(cmux_service, "hermes_service", fake):
            with mock.patch.object(cmux_service, "_save_learning_applied", return_value=None):
                with mock.patch.object(cmux_service, "_save_bot_desired_state") as save_desired:
                    code, out = cmux_service.dispatch_post("/bot/start", payload)

        self.assertEqual(code, 200)
        self.assertTrue(out["ok"])
        self.assertEqual(fake.calls[0], "start")
        self.assertEqual(fake.calls[1], "status")
        self.assertEqual(fake.calls[2], ("autotrade_start", {"symbol": "BTCUSDT", "executionMode": "PAPER"}))
        save_desired.assert_called_once_with(True, {"symbol": "BTCUSDT", "executionMode": "PAPER"}, "bot_start")

    def test_bot_stop_clears_desired_running(self):
        fake = _FakeHermes()

        with mock.patch.object(cmux_service, "hermes_service", fake):
            with mock.patch.object(cmux_service, "_save_bot_desired_state") as save_desired:
                code, out = cmux_service.dispatch_post("/bot/stop", {"force": True})

        self.assertEqual(code, 200)
        self.assertTrue(out["ok"])
        self.assertEqual(fake.calls[0], ("autotrade_stop", {"force": True}))
        save_desired.assert_called_once_with(False, None, "bot_stop")

    def test_bot_start_stops_when_hermes_fails(self):
        fake = _FakeHermes(start_ok=False)

        with mock.patch.object(cmux_service, "hermes_service", fake):
            code, out = cmux_service.dispatch_post("/bot/start", {"symbol": "BTCUSDT", "autoLearn": False})

        self.assertEqual(code, 500)
        self.assertFalse(out["ok"])
        self.assertEqual(out["step"], "hermes.start")
        self.assertEqual(fake.calls, ["start"])

    def test_get_health_declares_cmux_role(self):
        code, out = cmux_service.dispatch_get("/health")

        self.assertEqual(code, 200)
        self.assertEqual(out["role"], "cmux")

    def test_local_publish_hint_prefers_wifi_lan_over_tailscale(self):
        infos = [
            (None, None, None, None, ("100.89.42.68", 0)),
            (None, None, None, None, ("192.168.1.8", 0)),
        ]

        with mock.patch.object(cmux_service.socket, "gethostname", return_value="host"):
            with mock.patch.object(cmux_service.socket, "getaddrinfo", return_value=infos):
                self.assertEqual(cmux_service._local_publish_hint(), "192.168.1.8")

    def test_local_publish_hint_falls_back_to_tailscale_when_no_lan(self):
        infos = [(None, None, None, None, ("100.89.42.68", 0))]

        with mock.patch.object(cmux_service.socket, "gethostname", return_value="host"):
            with mock.patch.object(cmux_service.socket, "getaddrinfo", return_value=infos):
                self.assertEqual(cmux_service._local_publish_hint(), "100.89.42.68")

    def test_learning_report_missing_file_returns_empty_ok_report(self):
        with mock.patch.object(cmux_service, "TRAIN_REPORT_PATH") as path:
            path.exists.return_value = False
            code, out = cmux_service.dispatch_get("/learning/report")

        self.assertEqual(code, 200)
        self.assertTrue(out["ok"])
        self.assertEqual(out["trainedAt"], 0)
        self.assertEqual(out["promotedCount"], 0)
        self.assertEqual(out["results"], [])

    def test_status_uses_quick_bot_status(self):
        fake = _FakeHermes()
        fake.status_quick = mock.Mock(return_value={"ok": True, "healthy": True})
        fake.autotrade_status_quick = mock.Mock(return_value={"ok": True, "running": False})
        fake.autotrade_status = mock.Mock(side_effect=AssertionError("full status should not be called"))

        with mock.patch.object(cmux_service, "hermes_service", fake):
            with mock.patch.object(cmux_service, "_public_ip_status", return_value={"ip": "127.0.0.1"}):
                with mock.patch.object(cmux_service, "_load_bot_desired_state", return_value={"running": False}):
                    code, out = cmux_service.dispatch_get("/status/quick")

        self.assertEqual(code, 200)
        self.assertTrue(out["ok"])
        fake.autotrade_status_quick.assert_called_once()
        fake.autotrade_status.assert_not_called()

    def test_status_marks_timeout_stale_when_bot_desired_running(self):
        fake = _FakeHermes()
        fake.status_quick = mock.Mock(return_value={"ok": True, "healthy": True})
        fake.autotrade_status_quick = mock.Mock(return_value={"ok": False, "running": False, "source": "timeout", "error": "timed out"})

        with mock.patch.object(cmux_service, "hermes_service", fake):
            with mock.patch.object(cmux_service, "_public_ip_status", return_value={"ip": "127.0.0.1"}):
                with mock.patch.object(cmux_service, "_load_bot_desired_state", return_value={"running": True, "updatedAt": 123, "reason": "bot_start"}):
                    code, out = cmux_service.dispatch_get("/status/quick")

        self.assertEqual(code, 200)
        self.assertTrue(out["bot"]["ok"])
        self.assertTrue(out["bot"]["desiredRunning"])
        self.assertTrue(out["bot"]["running"])
        self.assertTrue(out["bot"]["stale"])
        self.assertTrue(out["bot"]["statusUnknown"])
        self.assertEqual(out["bot"]["staleReason"], "status_timeout_desired_running")

    def test_status_does_not_mark_soft_stale_cache_as_global_stale(self):
        fake = _FakeHermes()
        fake.status_quick = mock.Mock(return_value={"ok": True, "healthy": True})
        fake.autotrade_status_quick = mock.Mock(
            return_value={
                "ok": True,
                "running": True,
                "source": "desired_cache",
                "softStale": True,
                "stale": False,
                "statusUnknown": True,
            }
        )

        with mock.patch.object(cmux_service, "hermes_service", fake):
            with mock.patch.object(cmux_service, "_public_ip_status", return_value={"ip": "127.0.0.1"}):
                with mock.patch.object(cmux_service, "_load_bot_desired_state", return_value={"running": True, "updatedAt": 123, "reason": "bot_start"}):
                    code, out = cmux_service.dispatch_get("/status/quick")

        self.assertEqual(code, 200)
        self.assertFalse(out["stale"])
        self.assertTrue(out["bot"]["softStale"])

    def test_status_preserves_hermes_agents_from_rich_cache(self):
        fake = _FakeHermes()
        fake.status_quick = mock.Mock(return_value={"ok": True, "healthy": True})
        fake.autotrade_status_quick = mock.Mock(
            side_effect=[
                {
                    "ok": True,
                    "running": True,
                    "source": "live",
                    "hermesAgents": {"agents": {"market_analyst": {"state": "done"}}},
                    "scanBoard": [{"symbol": "BTCUSDT"}],
                    "log": [{"msg": "cycle"}],
                },
                {
                    "ok": True,
                    "running": True,
                    "source": "desired_cache",
                    "softStale": True,
                    "stale": False,
                    "statusUnknown": True,
                },
            ]
        )

        with mock.patch.object(cmux_service, "hermes_service", fake):
            with mock.patch.object(cmux_service, "_public_ip_status", return_value={"ip": "127.0.0.1"}):
                with mock.patch.object(cmux_service, "_load_bot_desired_state", return_value={"running": True, "updatedAt": 123, "reason": "bot_start"}):
                    _code1, _out1 = cmux_service.dispatch_get("/status/quick")
                    cmux_service._STATUS_CACHE.update({"ts": 0.0, "data": None})
                    code2, out2 = cmux_service.dispatch_get("/status/quick")

        self.assertEqual(code2, 200)
        self.assertTrue(out2["bot"]["richStatusCached"])
        self.assertIn("market_analyst", out2["bot"]["hermesAgents"]["agents"])
        self.assertEqual(out2["bot"]["scanBoard"][0]["symbol"], "BTCUSDT")

    def test_status_fills_hermes_agents_from_snapshot_when_cache_empty(self):
        fake = _FakeHermes()
        fake.status_quick = mock.Mock(return_value={"ok": True, "healthy": True})
        fake.autotrade_status_quick = mock.Mock(
            return_value={
                "ok": True,
                "running": True,
                "source": "desired_cache",
                "softStale": True,
                "stale": False,
            }
        )
        snapshot = {
            "hermesAgents": {"agents": {"market_analyst": {"state": "done"}}},
            "scanBoard": [{"symbol": "ETHUSDT"}],
        }
        fake_path = mock.Mock()
        fake_path.read_text.return_value = json.dumps(snapshot)

        with mock.patch.object(cmux_service, "AUTOTRADE_SNAPSHOT_PATH", fake_path):
            with mock.patch.object(cmux_service, "hermes_service", fake):
                with mock.patch.object(cmux_service, "_public_ip_status", return_value={"ip": "127.0.0.1"}):
                    with mock.patch.object(cmux_service, "_load_bot_desired_state", return_value={"running": True, "updatedAt": 123, "reason": "bot_start"}):
                        code, out = cmux_service.dispatch_get("/status/quick")

        self.assertEqual(code, 200)
        self.assertTrue(out["bot"]["richStatusCached"])
        self.assertIn("market_analyst", out["bot"]["hermesAgents"]["agents"])
        self.assertEqual(out["bot"]["scanBoard"][0]["symbol"], "ETHUSDT")

    def test_status_reuses_fresh_cmux_cache_for_dashboard_polling(self):
        fake = _FakeHermes()
        fake.status_quick = mock.Mock(return_value={"ok": True, "healthy": True})
        fake.autotrade_status_quick = mock.Mock(return_value={"ok": True, "running": True, "source": "live"})

        with mock.patch.object(cmux_service, "hermes_service", fake):
            with mock.patch.object(cmux_service, "_public_ip_status", return_value={"ip": "127.0.0.1"}):
                with mock.patch.object(cmux_service, "_load_bot_desired_state", return_value={"running": True, "updatedAt": 123, "reason": "bot_start"}):
                    code1, out1 = cmux_service.dispatch_get("/status/quick")
                    code2, out2 = cmux_service.dispatch_get("/status/quick")

        self.assertEqual(code1, 200)
        self.assertEqual(code2, 200)
        self.assertTrue(out2["cmuxStatusCache"]["hit"])
        fake.status_quick.assert_called_once()
        fake.autotrade_status_quick.assert_called_once()

    def test_status_returns_stale_cache_while_background_refresh_runs(self):
        cmux_service._STATUS_CACHE.update({
            "ts": cmux_service.time.time() - 10.0,
            "data": {
                "ok": True,
                "cmux": {"running": True, "port": 8030},
                "hermes": {"ok": True, "healthy": True, "running": True},
                "bot": {"ok": True, "running": True, "source": "live"},
                "botDesired": {"running": True},
                "learning": {},
                "stale": False,
            },
        })

        with mock.patch.object(cmux_service.threading, "Thread") as thread_cls:
            code, out = cmux_service.dispatch_get("/status/quick")

        self.assertEqual(code, 200)
        self.assertTrue(out["cmuxStatusCache"]["stale"])
        self.assertTrue(out["bot"]["softStale"])
        self.assertFalse(out["stale"])
        thread_cls.assert_called_once()

    def test_status_returns_old_cache_instead_of_blocking_dashboard(self):
        cmux_service._STATUS_CACHE.update({
            "ts": cmux_service.time.time() - (cmux_service.STATUS_CACHE_STALE_SEC + 30.0),
            "data": {
                "ok": True,
                "cmux": {"running": True, "port": 8030},
                "hermes": {"ok": True, "healthy": True, "running": True},
                "bot": {"ok": True, "running": True, "source": "live"},
                "botDesired": {"running": True},
                "learning": {},
                "stale": False,
            },
        })

        with mock.patch.object(cmux_service, "_build_status_payload_live", side_effect=AssertionError("must refresh in background")):
            with mock.patch.object(cmux_service.threading, "Thread") as thread_cls:
                code, out = cmux_service.dispatch_get("/status/quick")

        self.assertEqual(code, 200)
        self.assertTrue(out["cmuxStatusCache"]["tooOld"])
        self.assertTrue(out["bot"]["softStale"])
        self.assertFalse(out["stale"])
        thread_cls.assert_called_once()

    def test_bot_watchdog_skips_resume_on_status_timeout(self):
        fake = _FakeHermes()
        fake.start = mock.Mock(return_value={"ok": True})
        fake.autotrade_start = mock.Mock(return_value={"ok": True, "running": True})

        with mock.patch.object(cmux_service, "hermes_service", fake):
            with mock.patch.object(cmux_service, "_load_bot_desired_state", return_value={"running": True, "config": {"symbol": "AUTO", "executionMode": "LIVE"}}):
                with mock.patch.object(cmux_service.time, "time", return_value=10_000.0):
                    cmux_service._BOT_WATCHDOG_LAST_ATTEMPT = 0.0
                    out = cmux_service._bot_watchdog_maybe_resume({"running": False, "source": "timeout"}, reason="timeout")

        self.assertFalse(out["attempted"])
        self.assertEqual(out["reason"], "transient_status_timeout")
        fake.start.assert_not_called()
        fake.autotrade_start.assert_not_called()

    def test_bot_watchdog_resumes_when_live_status_confirms_stopped(self):
        fake = _FakeHermes()
        fake.start = mock.Mock(return_value={"ok": True})
        fake.autotrade_start = mock.Mock(return_value={"ok": True, "running": True})

        with mock.patch.object(cmux_service, "hermes_service", fake):
            with mock.patch.object(cmux_service, "_load_bot_desired_state", return_value={"running": True, "config": {"symbol": "AUTO", "executionMode": "LIVE"}}):
                with mock.patch.object(cmux_service.time, "time", return_value=10_000.0):
                    cmux_service._BOT_WATCHDOG_LAST_ATTEMPT = 0.0
                    out = cmux_service._bot_watchdog_maybe_resume({"running": False, "source": "live"}, reason="not_running")

        self.assertTrue(out["attempted"])
        self.assertTrue(out["ok"])
        fake.start.assert_called_once()
        fake.autotrade_start.assert_called_once_with({"symbol": "AUTO", "executionMode": "LIVE"})

    def test_learning_train_auto_applies_promoted_live_config(self):
        fake = _FakeHermes()
        fake.learning_propose_config = mock.Mock(
            return_value={
                "proposed": {"minConfidence": 0.62, "takeProfitPct": 1.2, "stopLossPct": 0.9},
                "reasons": ["wins=8 losses=2"],
            }
        )
        fake.learning_walk_forward = mock.Mock(
            return_value={
                "ok": True,
                "walkForwardHitRatePct": 80.0,
                "recommendation": {"loosen": True},
            }
        )
        fake.autotrade_status_quick = mock.Mock(
            return_value={"ok": True, "running": True, "config": {"executionMode": "LIVE", "symbol": "BEATUSDT"}}
        )

        with mock.patch.object(cmux_service, "hermes_service", fake):
            with mock.patch.object(cmux_service, "_save_train_report", return_value=None):
                report = cmux_service._run_learning_train(
                    {"symbols": ["BEATUSDT"], "trainSize": 5, "testSize": 3, "promoteHitRatePct": 55.0}
                )

        self.assertEqual(report["promotedCount"], 1)
        self.assertTrue(report["autoApply"]["applied"])
        self.assertEqual(report["autoApply"]["symbol"], "BEATUSDT")
        self.assertIn(("autotrade_update_config", {"minConfidence": 0.62, "takeProfitPct": 1.2, "stopLossPct": 0.9}), fake.calls)

    def test_learning_train_does_not_apply_when_not_live(self):
        fake = _FakeHermes()
        fake.learning_propose_config = mock.Mock(return_value={"proposed": {"minConfidence": 0.62}, "reasons": []})
        fake.learning_walk_forward = mock.Mock(return_value={"ok": True, "walkForwardHitRatePct": 80.0, "recommendation": {}})
        fake.autotrade_status_quick = mock.Mock(
            return_value={"ok": True, "running": True, "config": {"executionMode": "PAPER", "symbol": "BEATUSDT"}}
        )

        with mock.patch.object(cmux_service, "hermes_service", fake):
            with mock.patch.object(cmux_service, "_save_train_report", return_value=None):
                report = cmux_service._run_learning_train({"symbols": ["BEATUSDT"]})

        self.assertFalse(report["autoApply"]["applied"])
        self.assertEqual(report["autoApply"]["reason"], "not_live_mode")
        self.assertNotIn(("autotrade_update_config", {"minConfidence": 0.62}), fake.calls)

    def test_learning_train_falls_back_to_scan_symbols_when_status_empty(self):
        fake = _FakeHermes()
        fake.learning_status = mock.Mock(return_value={"items": []})
        fake.learning_propose_config = mock.Mock(return_value={"proposed": {"minConfidence": 0.62}, "reasons": []})
        fake.learning_walk_forward = mock.Mock(return_value={"ok": True, "walkForwardHitRatePct": 40.0, "recommendation": {}})
        fake.autotrade_status_quick = mock.Mock(
            return_value={
                "ok": True,
                "running": True,
                "config": {"executionMode": "LIVE", "symbol": "AUTO", "primarySymbol": "BEATUSDT"},
                "scanBoard": [{"symbol": "HYPEUSDT"}, {"symbol": "SOXLUSDT"}],
                "openLivePositions": [{"symbol": "XRPUSDT"}],
            }
        )

        with mock.patch.object(cmux_service, "hermes_service", fake):
            with mock.patch.object(cmux_service, "_save_train_report", return_value=None):
                report = cmux_service._run_learning_train({})

        self.assertEqual(report["symbolsScanned"], 4)
        self.assertEqual(report["symbols"], ["BEATUSDT", "HYPEUSDT", "SOXLUSDT", "XRPUSDT"])
        fake.learning_status.assert_called_once()


class TestHermesWrapper(unittest.TestCase):
    def setUp(self):
        hermes_service._HERMES_HEALTH_CACHE.update({"ts": 0.0, "healthy": False})
        hermes_service._HERMES_HEALTH_MISS_SINCE = 0.0

    def test_quick_status_uses_lite_endpoint_only(self):
        urls = []

        def fake_urlopen(url, timeout):
            urls.append((url, timeout))
            return _FakeHttpResponse({"running": False})

        with mock.patch.object(hermes_service.urllib.request, "urlopen", side_effect=fake_urlopen):
            out = hermes_service.autotrade_status_quick()

        self.assertEqual(out["running"], False)
        self.assertEqual(len(urls), 1)
        self.assertTrue(urls[0][0].endswith("/autotrade/status-lite"))

    def test_autotrade_status_quick_uses_soft_stale_cache_for_transient_timeout(self):
        hermes_service._AUTOTRADE_STATUS_CACHE.update({
            "ts": hermes_service.time.time(),
            "data": {"ok": True, "running": True, "source": "live", "stale": False},
        })

        def timeout_urlopen(_url, timeout):
            raise TimeoutError("timed out")

        with mock.patch.object(hermes_service.urllib.request, "urlopen", side_effect=timeout_urlopen):
            out = hermes_service.autotrade_status_quick()

        self.assertTrue(out["ok"])
        self.assertTrue(out["running"])
        self.assertFalse(out["stale"])
        self.assertTrue(out["softStale"])
        self.assertEqual(out["source"], "cache")

    def test_autotrade_status_quick_uses_desired_running_when_cache_empty(self):
        hermes_service._AUTOTRADE_STATUS_CACHE.update({
            "ts": 0.0,
            "data": {"ok": False, "running": False, "source": "init"},
        })

        def timeout_urlopen(_url, timeout):
            raise TimeoutError("timed out")

        with mock.patch.object(hermes_service.urllib.request, "urlopen", side_effect=timeout_urlopen):
            with mock.patch.object(
                hermes_service,
                "_load_bot_desired_snapshot",
                return_value={"running": True, "config": {"symbol": "AUTO", "executionMode": "LIVE"}},
            ):
                out = hermes_service.autotrade_status_quick()

        self.assertTrue(out["ok"])
        self.assertTrue(out["running"])
        self.assertTrue(out["softStale"])
        self.assertTrue(out["statusUnknown"])
        self.assertEqual(out["source"], "desired_cache")
        self.assertEqual(out["config"]["symbol"], "AUTO")

    def test_status_quick_uses_health_grace_for_transient_timeout(self):
        hermes_service._HERMES_HEALTH_CACHE.update({"ts": hermes_service.time.time(), "healthy": True})

        with mock.patch.object(hermes_service, "_read_pid", return_value=1234):
            with mock.patch.object(hermes_service, "_is_running", return_value=True):
                with mock.patch.object(hermes_service, "_http_ok", return_value=False):
                    out = hermes_service.status_quick()

        self.assertTrue(out["running"])
        self.assertTrue(out["healthy"])
        self.assertTrue(out["healthStale"])

    def test_status_quick_uses_desired_running_grace_when_health_cache_empty(self):
        with mock.patch.object(hermes_service, "_read_pid", return_value=1234):
            with mock.patch.object(hermes_service, "_is_running", return_value=True):
                with mock.patch.object(hermes_service, "_http_ok", return_value=False):
                    with mock.patch.object(hermes_service, "_load_bot_desired_snapshot", return_value={"running": True}):
                        out = hermes_service.status_quick()

        self.assertTrue(out["running"])
        self.assertTrue(out["healthy"])
        self.assertTrue(out["healthStale"])
        self.assertEqual(out["healthStaleReason"], "desired_running_grace")

    def test_start_replaces_stale_running_pid_without_listener(self):
        fake_proc = mock.Mock(pid=5678)
        http_checks = [False, True]

        with mock.patch.object(hermes_service, "_read_pid", return_value=1234):
            with mock.patch.object(hermes_service, "_is_running", return_value=True):
                with mock.patch.object(hermes_service, "_pids_listening_on_port", return_value=[]):
                    with mock.patch.object(hermes_service, "_http_ok", side_effect=lambda *_args, **_kwargs: http_checks.pop(0)):
                        with mock.patch.object(hermes_service.subprocess, "run") as run:
                            with mock.patch.object(hermes_service.subprocess, "Popen", return_value=fake_proc) as popen:
                                with mock.patch.object(hermes_service, "_write_pid") as write_pid:
                                    out = hermes_service.start()

        self.assertTrue(out["ok"])
        self.assertEqual(out["pid"], 5678)
        run.assert_called()
        popen.assert_called_once()
        write_pid.assert_called_with(5678)

    def test_precheck_falls_back_to_auth_check(self):
        urls = []

        def fake_urlopen(url, timeout):
            urls.append(url)
            if "/autotrade/precheck-live" in url:
                raise hermes_service.urllib.error.HTTPError(url, 404, "Not Found", {}, None)
            return _FakeHttpResponse({"ok": True, "stage": "signed_api"})

        with mock.patch.object(hermes_service.urllib.request, "urlopen", side_effect=fake_urlopen):
            out = hermes_service.autotrade_precheck_live("BTCUSDT")

        self.assertTrue(out["ok"])
        self.assertEqual(out["stage"], "signed_api")
        self.assertIn("/debug/binance-auth-check", urls[1])


if __name__ == "__main__":
    unittest.main()
