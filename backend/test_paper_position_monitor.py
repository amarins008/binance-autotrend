"""Regression tests for live-guardian position heartbeat + weak-signal exit.

NOTE: Paper-trading mode was removed on 2026-08-24 (Boss directive: LIVE-only).
The regression classes for ``_paper_position_monitor_decision`` / ``_paper_close``
were deleted together with the feature. This file now covers:

  * Guardian heartbeat firing from ``_LIVE_POSITIONS_CACHE`` (Hermes review fix 2026-07-10).
  * WEAK_SIGNAL exit min-profit threshold (>= $0.08 floor fix).
"""

from __future__ import annotations

import os
import time
import unittest
from unittest import mock

import main
import trading.live_guardian as lg


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

        with mock.patch.object(lg, "_live_multi_profit_lock_manage", fake_lock_manage), \
             mock.patch.object(lg, "_persist_autotrade_snapshot"):
            with mock.patch.object(lg, "_agent_mark") as mark_mock:
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

        with mock.patch.object(lg, "_live_multi_profit_lock_manage", fake_lock_manage), \
             mock.patch.object(lg, "_persist_autotrade_snapshot"):
            with mock.patch.object(lg, "_agent_mark"):
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

    def setUp(self):
        # conftest.py wipes BINANCE_API_KEY/SECRET to keep tests off the live
        # account; restore dummy values so the credential gate in
        # _live_multi_profit_lock_manage does not early-return. All network
        # calls below are mocked, so no request actually reaches Binance.
        os.environ["BINANCE_API_KEY"] = "test-key"
        os.environ["BINANCE_API_SECRET"] = "test-secret"

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

        async def fake_close(sym, side, *args, **kwargs):
            close_sym_side.append((sym, side))

        _FAKE_POSITIONS = [{
            "symbol": "BNBUSDT", "side": "LONG", "qty": 1.0,
            "markPrice": 600.0, "entryMark": 595.0,
            "notionalUsdtApprox": 600.0, "unRealizedProfit": 0.06,
        }]

        with \
             mock.patch.object(lg, "_binance_base", return_value="https://fapi.binance.com"), \
             mock.patch.object(lg, "_pick_live_orphan_positions",
                               new=mock.AsyncMock(return_value=_FAKE_POSITIONS)), \
             mock.patch.object(lg, "_close_position_one_side", fake_close), \
             mock.patch.object(lg, "intel_analyze", new=mock.AsyncMock(return_value={
                 "symbol": "BNBUSDT", "signal": "SHORT",
                 "confidence": 0.55, "execution": {"momentumPct": -0.1},
             })), \
             mock.patch.object(lg, "_last_decision_intel", return_value={
                 "symbol": "BNBUSDT", "signal": "SHORT",
                 "confidence": 0.55, "execution": {"momentumPct": -0.1},
             }), \
             mock.patch.object(lg, "_entry_snapshot_from_intel", return_value={}), \
             mock.patch.object(lg, "_effective_tp_sl", return_value={
                 "tpPct": 1.0, "slPct": 1.0, "notionalCapUsdt": 10.0,
                 "profitLockTriggerUsdt": 0.10,
             }), \
             mock.patch.object(lg, "_symbol_effective_profile", return_value={}), \
             mock.patch.object(lg, "_calc_tp_sl_prices", return_value=(602.0, 594.0)), \
             mock.patch.object(lg, "_fee_edge_min_net_usdt", return_value=0.030), \
             mock.patch.object(lg, "_profit_lock_policy", return_value={
                 "trigger": 0.10, "lockUsdt": 0.08,
             }), \
             mock.patch.object(lg, "_recent_payoff_loss_guard", return_value={}), \
             mock.patch.object(lg, "_autotrade_log"):

            await lg._live_multi_profit_lock_manage({})

        self.assertEqual(
            close_sym_side, [],
            "WEAK_SIGNAL must NOT fire when upnl (0.06) < min_profit_lock (0.08)",
        )

    async def test_weak_signal_does_fire_above_min_profit_threshold(self):
        """upnl above the notional-scaled min-profit floor — WEAK_SIGNAL SHOULD fire.
        Req 8: min_profit_lock = max(fee_min*2, profitLockMinUsdt, notional*weakRate%);
        for notional=601, rate=0.04% -> floor ~= 0.24, so upnl=0.28 must close."""
        import services.app_state as app_state

        # Reset module-level cache to avoid cross-test pollution.
        app_state._LIVE_POSITIONS_CACHE = (0.0, [])
        main.AUTO_TRADE["liveProfitLocks"] = {
            "BNBUSDT:LONG": {
                "armed": True, "peak": 0.28, "lockUsdt": 0.08,
                # tp=602 > mark=601 so LOCAL_TP_HIT doesn't fire first,
                # letting the WEAK_SIGNAL branch execute.
                "tp": 602.0, "sl": 594.0, "entryMark": 595.0,
                "side": "LONG", "qty": 1.0,
                # Position held 7200s (> guardianMinHoldSec 180), so the
                # min-hold guard (too_new) does NOT skip exit decisions.
                "guardianStats": {"openedAt": time.time() - 7200},
            },
        }

        close_sym_side = []

        async def fake_close(sym, side, *args, **kwargs):
            close_sym_side.append((sym, side))

        _FAKE_POSITIONS = [{
            "symbol": "BNBUSDT", "side": "LONG", "qty": 1.0,
            "markPrice": 601.0, "entryMark": 595.0,
            "notionalUsdtApprox": 601.0, "unRealizedProfit": 0.28,
        }]

        with \
             mock.patch.object(lg, "_binance_base", return_value="https://fapi.binance.com"), \
             mock.patch.object(lg, "_pick_live_orphan_positions",
                               new=mock.AsyncMock(return_value=_FAKE_POSITIONS)), \
             mock.patch.object(lg, "_close_position_one_side", fake_close), \
             mock.patch.object(lg, "intel_analyze", new=mock.AsyncMock(return_value={
                 "symbol": "BNBUSDT", "signal": "SHORT",
                 "confidence": 0.55, "execution": {"momentumPct": -0.1},
             })), \
             mock.patch.object(lg, "_last_decision_intel", return_value={
                 "symbol": "BNBUSDT", "signal": "SHORT",
                 "confidence": 0.55, "execution": {"momentumPct": -0.1},
             }), \
             mock.patch.object(lg, "_entry_snapshot_from_intel", return_value={}), \
             mock.patch.object(lg, "_effective_tp_sl", return_value={
                 "tpPct": 1.0, "slPct": 1.0, "notionalCapUsdt": 10.0,
                 "profitLockTriggerUsdt": 0.10,
             }), \
             mock.patch.object(lg, "_symbol_effective_profile", return_value={}), \
             mock.patch.object(lg, "_calc_tp_sl_prices", return_value=(602.0, 594.0)), \
             mock.patch.object(lg, "_fee_edge_min_net_usdt", return_value=0.030), \
             mock.patch.object(lg, "_profit_lock_policy", return_value={
                 "trigger": 0.10, "lockUsdt": 0.08,
             }), \
             mock.patch.object(lg, "_recent_payoff_loss_guard", return_value={}), \
             mock.patch.object(lg, "_autotrade_log"):

            await lg._live_multi_profit_lock_manage({})

        self.assertEqual(
            close_sym_side, [("BNBUSDT", "LONG")],
            "WEAK_SIGNAL should fire when upnl (0.28) >= notional-scaled floor (~0.24)",
        )


if __name__ == "__main__":
    unittest.main()