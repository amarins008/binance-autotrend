"""Regression tests for the PAPER position monitor.

Background (2026-07 incident)
-----------------------------
``_autotrade_loop`` calls a helper that decides whether the open paper
position should be closed at TP / SL, trailed, or left alone. The bug we
hit in production was that the helper used ``mark`` from the *current*
cycle's intel — i.e. the mark of the freshly-scanned symbol — even when
the open paper position belonged to a *different* symbol.

Net effect: when the scan switched from PAXG (~$4,000) to EDGE (~$0.40),
the SL_HIT check compared ``0.40 <= 3990`` and the position was closed
instantly at the wrong price, producing fake PnL of hundreds of USDT per
trade and a win-rate of ~20% over a 10-trade window.

The fix: when ``cfg["symbol"]`` differs from ``position["symbol"]``, the
helper MUST fetch the mark for ``position["symbol"]`` instead of using
``cfg_mark``. These tests pin that contract.
"""

from __future__ import annotations

import asyncio
import unittest
from unittest import mock

import main
import trading.live_guardian as lg


def _run(coro):
    """Helper: drive an async coroutine to completion in a sync test."""
    return asyncio.new_event_loop().run_until_complete(coro)


def _make_position(symbol: str, side: str, entry: float, tp: float, sl: float) -> dict:
    return {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "qty": 1.0,
        "tp": tp,
        "sl": sl,
        "openedAt": 1_700_000_000,
    }


def _make_intel(cfg_symbol: str, mark: float | None = None) -> dict:
    """Intel payload shaped like what ``intel_analyze`` returns."""
    intel: dict = {"symbol": cfg_symbol}
    if mark is not None:
        intel["execution"] = {"mark": mark}
    return intel


class TestPaperPositionMonitorRegression(unittest.IsolatedAsyncioTestCase):
    """Pin the cross-symbol mark behaviour."""

    async def test_cross_symbol_uses_position_mark_not_cfg_mark(self):
        """The regression itself: cfg mark triggers SL, position mark does not.

        Open paper: LONG PAXG @ 4050, TP=4095, SL=3990.
        Cycle scan pick: EDGE @ 0.40 (cfg["symbol"] = "EDGEUSDT", cfg_mark = 0.40).
        Naive buggy behaviour: ``0.40 <= 3990`` -> SL_HIT (instant fake close).
        Correct behaviour: fetch PAXG mark (4050, between SL and TP) -> NONE.
        """
        cfg = {"symbol": "EDGEUSDT", "holdTrailPct": 0.35}
        intel = _make_intel("EDGEUSDT")
        position = _make_position("PAXGUSDT", "LONG", 4050.0, tp=4095.0, sl=3990.0)

        async def fake_fetch(symbol: str):
            # Position mark is well inside the [SL, TP] band.
            if symbol == "PAXGUSDT":
                return 4050.0
            # If we ever ask for the wrong symbol in this code path, fail loudly.
            raise AssertionError(f"unexpected fetch for {symbol}")

        decision = await main._paper_position_monitor_decision(
            cfg, intel, position, cfg_mark=0.40, fetch_mark_fn=fake_fetch,
        )

        self.assertEqual(
            decision,
            {"action": "NONE"},
            "cross-symbol cycle must NOT close PAXG position at EDGE's mark",
        )

    async def test_cross_symbol_still_closes_tp_when_position_mark_hits_tp(self):
        """Same scenario, but the position's actual mark IS above TP -> TP_HIT."""
        cfg = {"symbol": "EDGEUSDT", "holdTrailPct": 0.35}
        intel = _make_intel("EDGEUSDT")
        position = _make_position("PAXGUSDT", "LONG", 4050.0, tp=4095.0, sl=3990.0)

        async def fake_fetch(symbol: str):
            self.assertEqual(symbol, "PAXGUSDT")
            return 4100.0  # above TP

        decision = await main._paper_position_monitor_decision(
            cfg, intel, position, cfg_mark=0.40, fetch_mark_fn=fake_fetch,
        )

        self.assertEqual(decision["action"], "TP_HIT")
        self.assertAlmostEqual(decision["exit_price"], 4100.0)

    async def test_cross_symbol_still_closes_sl_when_position_mark_hits_sl(self):
        cfg = {"symbol": "EDGEUSDT", "holdTrailPct": 0.35}
        intel = _make_intel("EDGEUSDT")
        position = _make_position("PAXGUSDT", "LONG", 4050.0, tp=4095.0, sl=3990.0)

        async def fake_fetch(symbol: str):
            self.assertEqual(symbol, "PAXGUSDT")
            return 3980.0  # below SL

        decision = await main._paper_position_monitor_decision(
            cfg, intel, position, cfg_mark=4100.0, fetch_mark_fn=fake_fetch,
        )

        self.assertEqual(decision["action"], "SL_HIT")
        self.assertAlmostEqual(decision["exit_price"], 3980.0)

    async def test_cross_symbol_short_position_uses_position_mark(self):
        """Mirror case for SHORT — make sure we did not hard-code LONG logic."""
        cfg = {"symbol": "QQQUSDT", "holdTrailPct": 0.35}
        intel = _make_intel("QQQUSDT")
        position = _make_position("EWYUSDT", "SHORT", 175.43, tp=172.0, sl=177.0)

        async def fake_fetch(symbol: str):
            self.assertEqual(symbol, "EWYUSDT")
            return 175.43  # neutral

        decision = await main._paper_position_monitor_decision(
            cfg, intel, position, cfg_mark=713.27, fetch_mark_fn=fake_fetch,
        )

        self.assertEqual(decision, {"action": "NONE"})

        # SL branch (mark >= sl) when cfg has a wildly different price.
        async def fetch_below_tp(_sym: str):
            return 178.0  # SHORT SL hit (>= 177)

        decision_sl = await main._paper_position_monitor_decision(
            cfg, intel, position, cfg_mark=713.27, fetch_mark_fn=fetch_below_tp,
        )
        self.assertEqual(decision_sl["action"], "SL_HIT")

    async def test_cross_symbol_returns_mark_unavailable_when_fetch_returns_none(self):
        """If we can't fetch the position's mark, leave the position alone —
        NEVER fall back to the unrelated cfg_mark."""
        cfg = {"symbol": "EDGEUSDT"}
        intel = _make_intel("EDGEUSDT")
        position = _make_position("PAXGUSDT", "LONG", 4050.0, tp=4095.0, sl=3990.0)

        async def fake_fetch(_sym: str):
            return None

        decision = await main._paper_position_monitor_decision(
            cfg, intel, position, cfg_mark=0.40, fetch_mark_fn=fake_fetch,
        )

        self.assertEqual(decision, {"action": "MARK_UNAVAILABLE"})

    async def test_cross_symbol_returns_mark_unavailable_when_fetch_raises(self):
        cfg = {"symbol": "EDGEUSDT"}
        intel = _make_intel("EDGEUSDT")
        position = _make_position("PAXGUSDT", "LONG", 4050.0, tp=4095.0, sl=3990.0)

        async def fake_fetch(_sym: str):
            raise RuntimeError("binance timeout")

        decision = await main._paper_position_monitor_decision(
            cfg, intel, position, cfg_mark=0.40, fetch_mark_fn=fake_fetch,
        )

        self.assertEqual(decision, {"action": "MARK_UNAVAILABLE"})

    async def test_cross_symbol_without_fetch_fn_never_closes(self):
        """Defence-in-depth: even if a caller forgets to pass fetch_mark_fn,
        the helper must NOT close on cfg_mark when symbols differ."""
        cfg = {"symbol": "EDGEUSDT"}
        intel = _make_intel("EDGEUSDT")
        position = _make_position("PAXGUSDT", "LONG", 4050.0, tp=4095.0, sl=3990.0)

        decision = await main._paper_position_monitor_decision(
            cfg, intel, position, cfg_mark=0.40, fetch_mark_fn=None,
        )

        self.assertEqual(decision, {"action": "MARK_UNAVAILABLE"})


class TestPaperPositionMonitorSameSymbol(unittest.IsolatedAsyncioTestCase):
    """Existing behaviour preserved when position symbol == cfg symbol."""

    async def test_same_symbol_long_tp_hit(self):
        cfg = {"symbol": "BTCUSDT", "holdTrailPct": 0.35}
        intel = _make_intel("BTCUSDT")
        position = _make_position("BTCUSDT", "LONG", 100.0, tp=102.0, sl=99.0)

        decision = await main._paper_position_monitor_decision(
            cfg, intel, position, cfg_mark=103.0, fetch_mark_fn=None,
        )

        self.assertEqual(decision["action"], "TP_HIT")
        self.assertAlmostEqual(decision["exit_price"], 103.0)

    async def test_same_symbol_long_sl_hit(self):
        cfg = {"symbol": "BTCUSDT", "holdTrailPct": 0.35}
        intel = _make_intel("BTCUSDT")
        position = _make_position("BTCUSDT", "LONG", 100.0, tp=102.0, sl=99.0)

        decision = await main._paper_position_monitor_decision(
            cfg, intel, position, cfg_mark=98.0, fetch_mark_fn=None,
        )

        self.assertEqual(decision["action"], "SL_HIT")

    async def test_same_symbol_long_no_hit(self):
        cfg = {"symbol": "BTCUSDT", "holdTrailPct": 0.35}
        intel = _make_intel("BTCUSDT")
        position = _make_position("BTCUSDT", "LONG", 100.0, tp=102.0, sl=99.0)

        decision = await main._paper_position_monitor_decision(
            cfg, intel, position, cfg_mark=100.5, fetch_mark_fn=None,
        )

        self.assertEqual(decision, {"action": "NONE"})

    async def test_same_symbol_short_tp_hit(self):
        cfg = {"symbol": "BTCUSDT", "holdTrailPct": 0.35}
        intel = _make_intel("BTCUSDT")
        position = _make_position("BTCUSDT", "SHORT", 100.0, tp=98.0, sl=101.0)

        decision = await main._paper_position_monitor_decision(
            cfg, intel, position, cfg_mark=97.0, fetch_mark_fn=None,
        )

        self.assertEqual(decision["action"], "TP_HIT")

    async def test_same_symbol_long_trailed_when_winner_should_be_held(self):
        """If the new scan still agrees with the position direction at
        sufficient confidence, the helper should TRAIL rather than close."""
        cfg = {"symbol": "BTCUSDT", "holdTrailPct": 0.35}
        # intel signal matches LONG with high confidence + positive momentum,
        # so _should_hold_winner() returns True.
        intel = {
            "symbol": "BTCUSDT",
            "signal": "LONG",
            "confidence": 0.9,
            "execution": {"momentumPct": 0.4},
        }
        position = _make_position("BTCUSDT", "LONG", 100.0, tp=102.0, sl=99.0)

        with mock.patch.object(main, "_should_hold_winner", return_value=True), \
             mock.patch.object(
                 main,
                 "_trail_winner_levels",
                 return_value=(101.5, 102.5),
             ) as trail:
            decision = await main._paper_position_monitor_decision(
                cfg, intel, position, cfg_mark=103.0, fetch_mark_fn=None,
            )

        self.assertEqual(decision["action"], "TRAIL")
        self.assertAlmostEqual(decision["new_sl"], 101.5)
        self.assertAlmostEqual(decision["new_tp"], 102.5)
        # Make sure the trail helper was passed the position symbol, not cfg.
        trail.assert_called_once()
        args, _kwargs = trail.call_args
        # symbol is the 7th positional arg per _trail_winner_levels() signature.
        self.assertGreaterEqual(len(args), 7)
        self.assertEqual(args[6], "BTCUSDT")
        self.assertEqual(args[0], "LONG")


class TestPaperPositionMonitorEdgeCases(unittest.IsolatedAsyncioTestCase):
    async def test_missing_position_returns_none(self):
        decision = await main._paper_position_monitor_decision(
            {"symbol": "BTCUSDT"}, _make_intel("BTCUSDT"), {}, cfg_mark=100.0,
        )
        self.assertEqual(decision, {"action": "NONE"})

    async def test_missing_tp_or_sl_returns_none(self):
        position = {"symbol": "BTCUSDT", "side": "LONG", "entry": 100.0, "qty": 1.0}
        decision = await main._paper_position_monitor_decision(
            {"symbol": "BTCUSDT"}, _make_intel("BTCUSDT"), position, cfg_mark=100.0,
        )
        self.assertEqual(decision, {"action": "NONE"})

    async def test_unknown_side_returns_none(self):
        position = _make_position("BTCUSDT", "WEIRD", 100.0, tp=102.0, sl=99.0)
        decision = await main._paper_position_monitor_decision(
            {"symbol": "BTCUSDT"}, _make_intel("BTCUSDT"), position, cfg_mark=103.0,
        )
        self.assertEqual(decision, {"action": "NONE"})

    async def test_zero_or_negative_cfg_mark_with_same_symbol_falls_through(self):
        """Sanity: a 0/negative cfg_mark with same symbol must not cause a
        spurious SL_HIT (we already check `mark <= 0` -> MARK_UNAVAILABLE)."""
        cfg = {"symbol": "BTCUSDT"}
        position = _make_position("BTCUSDT", "LONG", 100.0, tp=102.0, sl=99.0)

        decision = await main._paper_position_monitor_decision(
            cfg, _make_intel("BTCUSDT"), position, cfg_mark=0.0,
        )
        self.assertEqual(decision, {"action": "MARK_UNAVAILABLE"})

        decision_neg = await main._paper_position_monitor_decision(
            cfg, _make_intel("BTCUSDT"), position, cfg_mark=-5.0,
        )
        self.assertEqual(decision_neg, {"action": "MARK_UNAVAILABLE"})


class TestPaperPositionMonitorSymbolCaseInsensitive(unittest.IsolatedAsyncioTestCase):
    """Defence against a casing mismatch sneaking the wrong mark through."""

    async def test_lowercase_position_symbol_still_triggers_fetch(self):
        cfg = {"symbol": "edgeusdt"}  # lowercase
        intel = _make_intel("edgeusdt")
        position = _make_position("paxgusdt", "LONG", 4050.0, tp=4095.0, sl=3990.0)

        fetch_calls: list[str] = []

        async def fake_fetch(symbol: str):
            fetch_calls.append(symbol)
            return 4050.0

        decision = await main._paper_position_monitor_decision(
            cfg, intel, position, cfg_mark=0.40, fetch_mark_fn=fake_fetch,
        )

        self.assertEqual(decision, {"action": "NONE"})
        self.assertEqual(fetch_calls, ["PAXGUSDT"])


class TestPaperPositionIntegrationWithPaperClose(unittest.TestCase):
    """End-to-end-ish: drive the helper, then call _paper_close and confirm
    the closed trade records the POSITION's mark, not the cfg_mark."""

    def test_closed_trade_records_position_mark(self):
        async def scenario():
            cfg = {"symbol": "EDGEUSDT", "holdTrailPct": 0.35}
            intel = _make_intel("EDGEUSDT")
            position = _make_position(
                "PAXGUSDT", "LONG", 4050.0, tp=4095.0, sl=3990.0,
            )
            main.AUTO_TRADE["paper"]["position"] = position
            try:
                async def fake_fetch(symbol: str):
                    self.assertEqual(symbol, "PAXGUSDT")
                    return 4100.0  # above TP

                decision = await main._paper_position_monitor_decision(
                    cfg, intel, position, cfg_mark=0.40, fetch_mark_fn=fake_fetch,
                )
                self.assertEqual(decision["action"], "TP_HIT")
                trade = main._paper_close("TP_HIT", decision["exit_price"])
                return trade
            finally:
                main.AUTO_TRADE["paper"]["position"] = None

        trade = _run(scenario())
        # Critical assertion: the closed trade's exit price must be the
        # position-symbol mark (4100), not the cfg_mark (0.40).
        self.assertAlmostEqual(trade["exit"], 4100.0)
        self.assertEqual(trade["symbol"], "PAXGUSDT")
        self.assertEqual(trade["reason"], "TP_HIT")
        # And the trade must be appended to paper.history (the bug was also
        # that bogus PnL rows were landing in the history).
        self.assertTrue(any(
            h.get("symbol") == "PAXGUSDT" and h.get("reason") == "TP_HIT"
            for h in main.AUTO_TRADE["paper"]["history"]
        ))


# ─── Regression tests for Hermes Supervisor review (2026-07-10) ───────────────────────────────

class TestGuardianHeartbeatFromCache(unittest.IsolatedAsyncioTestCase):
    """Issue: Guardian state=todo age=0s even when 2 open positions exist.

    Root cause: _manage_live_open_positions_once built heartbeat_positions from
    locks snapshot rather than from _LIVE_POSITIONS_CACHE.
    Fix: read the cache directly after _live_multi_profit_lock_manage runs.
    """

    async def test_heartbeat_fires_when_cache_has_positions_but_no_locks(self):
        """Heartbeat must fire from cache even when liveProfitLocks
        are both empty — the bug was that guardian stayed at 'todo' age=0s."""
        import services.app_state as app_state

        cfg = {"symbol": "BTCUSDT"}
        now = 1_800_000_000

        # Pre-populate cache with 2 live positions (as _pick_live_orphan_positions would).
        app_state._LIVE_POSITIONS_CACHE = (
            float(now),
            [
                {"symbol": "ARBUSDT", "side": "LONG", "qty": 100.0, "markPrice": 0.095},
                {"symbol": "BTCUSDT", "side": "SHORT", "qty": 0.01, "markPrice": 60000.0},
            ],
        )

        # Ensure locks are empty (the scenario that triggered the bug).
        main.AUTO_TRADE["liveProfitLocks"] = {}

        # Track what _agent_mark is called with.
        marks = []

        async def fake_lock_manage(cfg_arg):
            return False  # no positions closed by lock

        with mock.patch.object(main, "_live_multi_profit_lock_manage", fake_lock_manage), \
             mock.patch.object(main, "_persist_autotrade_snapshot"):
            with mock.patch.object(main, "_agent_mark") as mark_mock:
                mark_mock.side_effect = lambda *a, **kw: marks.append((a, kw))
                result = await lg._manage_live_open_positions_once(cfg, now)

        # Should return False (nothing closed) but heartbeat MUST have fired.
        self.assertFalse(result)
        done_marks = [m for m in marks if m[0][1] == "done" and m[0][0] == "position_guardian"]
        self.assertGreaterEqual(
            len(done_marks), 1,
            "guardian must emit 'done' with heartbeat when positions exist in cache",
        )
        done_args = done_marks[0][0]
        self.assertEqual(done_args[1], "done")
        # The heartbeat metadata (5th positional arg) must reflect 2 open positions.
        extra = done_args[4] if len(done_args) > 4 else {}
        self.assertEqual(extra.get("openPositions"), 2)

    async def test_heartbeat_source_is_cache_not_locks(self):
        """Verify heartbeat is populated from _LIVE_POSITIONS_CACHE, not from locks dict."""
        import services.app_state as app_state

        cfg = {}
        now = 1_800_000_000

        # Cache has 1 position.
        app_state._LIVE_POSITIONS_CACHE = (float(now), [
            {"symbol": "ETHUSDT", "side": "LONG", "qty": 0.5, "markPrice": 3500.0},
        ])
        # But locks dict is empty (no lock entries yet).
        main.AUTO_TRADE["liveProfitLocks"] = {}

        heartbeat_rows = []

        async def fake_lock_manage(c):
            return False

        async def fake_guardian_close(c):
            return False

        with mock.patch.object(main, "_live_multi_profit_lock_manage", fake_lock_manage), \
             mock.patch.object(main, "_persist_autotrade_snapshot"):
            with mock.patch.object(main, "_agent_mark"):
                with mock.patch.object(lg, "_position_guardian_status_heartbeat") as hb_mock:
                    hb_mock.side_effect = lambda rows: heartbeat_rows.extend(rows)
                    await lg._manage_live_open_positions_once(cfg, now)

        self.assertEqual(len(heartbeat_rows), 1)
        self.assertEqual(heartbeat_rows[0]["symbol"], "ETHUSDT")


class TestWeakSignalExitMinProfitThreshold(unittest.IsolatedAsyncioTestCase):
    """Issue: 3 recent trades closed at avg=0.0858 USDT — too cheap.

    Root cause: WEAK_SIGNAL exit fired at upnl >= fee_min_capture (~$0.05).
    Fix: require upnl >= max(0.08, fee_min_capture*2, profitLockMinUsdt) before
    weak-signal exit can trigger.
    """

    async def test_weak_signal_does_not_fire_below_min_profit_threshold(self):
        """upnl=0.06 below the 0.08 floor — WEAK_SIGNAL must NOT fire."""
        import services.app_state as app_state

        # Reset module-level cache to avoid cross-test pollution.
        app_state._LIVE_POSITIONS_CACHE = (0.0, [])
        # Use tp=602 > mark=600 so LOCAL_TP_HIT does NOT fire first,
        # letting WEAK_SIGNAL (or no exit) be the determining factor.
        main.AUTO_TRADE["liveProfitLocks"] = {
            "BNBUSDT:LONG": {
                "armed": True, "peak": 0.09, "lockUsdt": 0.06,
                "tp": 602.0, "sl": 594.0, "entryMark": 595.0,
                "side": "LONG", "qty": 1.0,
            },
        }

        close_sym_side = []

        async def fake_close(sym, side, *_args):
            close_sym_side.append((sym, side))

        _FAKE_POSITIONS = [{
            "symbol": "BNBUSDT", "side": "LONG", "qty": 1.0,
            "markPrice": 600.0, "entryMark": 595.0,
            "notionalUsdtApprox": 600.0, "unRealizedProfit": 0.06,
        }]

        with \
             mock.patch.object(lg, "_binance_base", return_value="https://fapi.binance.com"), \
             mock.patch.object(main, "_pick_live_orphan_positions",
                               new=mock.AsyncMock(return_value=_FAKE_POSITIONS)), \
             mock.patch.object(main, "_close_position_one_side", fake_close), \
             mock.patch.object(main, "intel_analyze", new=mock.AsyncMock(return_value={
                 "symbol": "BNBUSDT", "signal": "SHORT",
                 "confidence": 0.55, "execution": {"momentumPct": -0.1},
             })), \
             mock.patch.object(main, "_last_decision_intel", return_value={
                 "symbol": "BNBUSDT", "signal": "SHORT",
                 "confidence": 0.55, "execution": {"momentumPct": -0.1},
             }), \
             mock.patch.object(main, "_entry_snapshot_from_intel", return_value={}), \
             mock.patch.object(main, "_effective_tp_sl", return_value={
                 "tpPct": 1.0, "slPct": 1.0, "notionalCapUsdt": 10.0,
                 "profitLockTriggerUsdt": 0.10,
             }), \
             mock.patch.object(main, "_symbol_effective_profile", return_value={}), \
             mock.patch.object(main, "_calc_tp_sl_prices", return_value=(602.0, 594.0)), \
             mock.patch.object(main, "_fee_edge_min_net_usdt", return_value=0.030), \
             mock.patch.object(main, "_profit_lock_policy", return_value={
                 "trigger": 0.10, "lockUsdt": 0.08,
             }), \
             mock.patch.object(main, "_recent_payoff_loss_guard", return_value={}), \
             mock.patch.object(main, "_autotrade_log"):

            await lg._live_multi_profit_lock_manage({})

        self.assertEqual(
            close_sym_side, [],
            "WEAK_SIGNAL must NOT fire when upnl (0.06) < min_profit_lock (0.08)",
        )

    async def test_weak_signal_does_fire_above_min_profit_threshold(self):
        """upnl=0.12 above the 0.08 floor — WEAK_SIGNAL SHOULD fire."""
        import services.app_state as app_state

        # Reset module-level cache to avoid cross-test pollution.
        app_state._LIVE_POSITIONS_CACHE = (0.0, [])
        main.AUTO_TRADE["liveProfitLocks"] = {
            "BNBUSDT:LONG": {
                "armed": True, "peak": 0.15, "lockUsdt": 0.08,
                # tp=602 > mark=601 so LOCAL_TP_HIT doesn't fire first,
                # letting the WEAK_SIGNAL branch execute.
                "tp": 602.0, "sl": 594.0, "entryMark": 595.0,
                "side": "LONG", "qty": 1.0,
            },
        }

        close_sym_side = []

        async def fake_close(sym, side, *_args):
            close_sym_side.append((sym, side))

        _FAKE_POSITIONS = [{
            "symbol": "BNBUSDT", "side": "LONG", "qty": 1.0,
            "markPrice": 601.0, "entryMark": 595.0,
            "notionalUsdtApprox": 601.0, "unRealizedProfit": 0.12,
        }]

        with \
             mock.patch.object(lg, "_binance_base", return_value="https://fapi.binance.com"), \
             mock.patch.object(main, "_pick_live_orphan_positions",
                               new=mock.AsyncMock(return_value=_FAKE_POSITIONS)), \
             mock.patch.object(main, "_close_position_one_side", fake_close), \
             mock.patch.object(main, "intel_analyze", new=mock.AsyncMock(return_value={
                 "symbol": "BNBUSDT", "signal": "SHORT",
                 "confidence": 0.55, "execution": {"momentumPct": -0.1},
             })), \
             mock.patch.object(main, "_last_decision_intel", return_value={
                 "symbol": "BNBUSDT", "signal": "SHORT",
                 "confidence": 0.55, "execution": {"momentumPct": -0.1},
             }), \
             mock.patch.object(main, "_entry_snapshot_from_intel", return_value={}), \
             mock.patch.object(main, "_effective_tp_sl", return_value={
                 "tpPct": 1.0, "slPct": 1.0, "notionalCapUsdt": 10.0,
                 "profitLockTriggerUsdt": 0.10,
             }), \
             mock.patch.object(main, "_symbol_effective_profile", return_value={}), \
             mock.patch.object(main, "_calc_tp_sl_prices", return_value=(602.0, 594.0)), \
             mock.patch.object(main, "_fee_edge_min_net_usdt", return_value=0.030), \
             mock.patch.object(main, "_profit_lock_policy", return_value={
                 "trigger": 0.10, "lockUsdt": 0.08,
             }), \
             mock.patch.object(main, "_recent_payoff_loss_guard", return_value={}), \
             mock.patch.object(main, "_autotrade_log"):

            await lg._live_multi_profit_lock_manage({})

        self.assertEqual(
            close_sym_side, [("BNBUSDT", "LONG")],
            "WEAK_SIGNAL should fire when upnl (0.12) >= min_profit_lock (0.08)",
        )


if __name__ == "__main__":
    unittest.main()