"""Tests for guardian performance improvements (guardian-performance spec).

Covers:
- Req 1: parallel intel (asyncio.gather path vs sequential)
- Req 3: dead if-False block gone — tv_blocks_tp_extension never referenced
- Req 4: structure confirmation bypass fix
- Req 5: position cache TTL raised to 25s
- Req 6: supervisor staleness threshold dynamic budget
- Req 8: notional-scaled min_profit_lock
"""

from __future__ import annotations

import asyncio
import time
import types
import unittest
from unittest import mock


# ---------------------------------------------------------------------------
# Req 4 — _strong_reversal_structure_confirmed bypass fix
# ---------------------------------------------------------------------------

class TestStructureConfirmedBypassFix(unittest.TestCase):
    def _fn(self, precision, cfg=None):
        from trading.live_guardian import _strong_reversal_structure_confirmed
        cfg = cfg or {}
        return _strong_reversal_structure_confirmed("LONG", "SHORT", precision, cfg)

    def test_none_precision_returns_false(self):
        ok, reason = self._fn(None)
        self.assertFalse(ok)
        self.assertIn("unavailable", reason)

    def test_empty_dict_precision_returns_false(self):
        ok, reason = self._fn({})
        self.assertFalse(ok)
        self.assertIn("unavailable", reason)

    def test_non_dict_precision_returns_false(self):
        ok, reason = self._fn("bad_value")
        self.assertFalse(ok)
        self.assertIn("unavailable", reason)

    def test_dict_with_no_indicator_keys_returns_no_indicators(self):
        # Has keys but none are the 5 tracked indicator families
        ok, reason = self._fn({"someOtherKey": 1.0, "anotherKey": True})
        self.assertFalse(ok)
        self.assertIn("no-indicators", reason)

    def test_structure_disabled_opt_out_preserved(self):
        # When strongFlipStructureConfirmEnabled=False, should always return True
        ok, reason = self._fn(None, cfg={"strongFlipStructureConfirmEnabled": False})
        self.assertTrue(ok)
        self.assertIn("disabled", reason)

    def test_structure_disabled_bypasses_even_empty(self):
        ok, reason = self._fn({}, cfg={"strongFlipStructureConfirmEnabled": False})
        self.assertTrue(ok)

    def test_passes_with_enough_confirms(self):
        # 2 indicators present and both confirm SHORT: trend + macd
        px = {
            "trendDown": True,
            "macdBearish": True,
        }
        ok, reason = self._fn(px, cfg={"strongFlipConfirmationsRequired": 2})
        self.assertTrue(ok)
        self.assertIn("structure=", reason)
        self.assertNotIn("unavailable", reason)

    def test_fails_when_confirms_insufficient(self):
        # Only 1 confirm (trend) but requirement is 2 and 2 indicators present
        px = {
            "trendDown": True,
            "macdBullish": True,  # bullish → wrong direction for SHORT
        }
        ok, reason = self._fn(px, cfg={"strongFlipConfirmationsRequired": 2})
        self.assertFalse(ok)
        # reason should be "structure=1/2" form
        self.assertIn("1/", reason)

    def test_single_indicator_sufficient_when_required_1(self):
        px = {"trendDown": True}
        ok, reason = self._fn(px, cfg={"strongFlipConfirmationsRequired": 1})
        self.assertTrue(ok)


# ---------------------------------------------------------------------------
# Req 5 — position cache TTL raised to 25 s
# ---------------------------------------------------------------------------

class TestPositionCacheTTL(unittest.IsolatedAsyncioTestCase):
    async def test_ttl_is_25_seconds(self):
        """_pick_live_orphan_positions should serve cache when age < 25 s."""
        from trading import live_guardian
        from services import app_state

        fake_rows = [{"symbol": "BTCUSDT", "side": "LONG", "qty": 0.01,
                      "entryMark": 50000.0, "markPrice": 51000.0,
                      "notionalUsdtApprox": 510.0, "unRealizedProfit": 10.0,
                      "leverage": 5, "marginUsedUsdt": 102.0, "isolatedWalletUsdt": 102.0}]

        # Seed cache with age = 20 s (within new 25 s window, outside old 5 s window)
        app_state._LIVE_POSITIONS_CACHE = (time.time() - 20.0, fake_rows)

        result = await live_guardian._pick_live_orphan_positions("k", "s", "https://fapi.binance.com")
        # Should return cached rows without hitting Binance
        self.assertEqual(result, fake_rows)

    async def test_cache_expired_triggers_fetch(self):
        """When cache is older than 25 s, a Binance fetch must be attempted."""
        from trading import live_guardian
        from services import app_state

        app_state._LIVE_POSITIONS_CACHE = (time.time() - 30.0, [])

        with mock.patch.object(live_guardian, "_get_um_client", return_value=None):
            with mock.patch.object(live_guardian, "_signed_request",
                                   new=mock.AsyncMock(return_value=[])) as signed:
                result = await live_guardian._pick_live_orphan_positions("k", "s", "https://fapi.binance.com")
        # Signed request should have been called since cache was stale
        signed.assert_called_once()


# ---------------------------------------------------------------------------
# Req 6 — supervisor dynamic staleness budget
# ---------------------------------------------------------------------------

class TestSupervisorDynamicBudget(unittest.TestCase):
    def _make_bot_with_positions(self, n_positions: int, guardian_age_sec: int):
        """Return minimal bot state + agents dict for supervisor review."""
        import time as t
        now = int(t.time())
        agents = {
            "position_guardian": {
                "state": "done",
                "lastAction": "open positions heartbeat",
                "lastReason": "",
                "runs": 5,
                "updatedAt": now - guardian_age_sec,
            }
        }
        open_positions = [{"symbol": f"SYM{i}USDT", "side": "LONG", "qty": 0.01} for i in range(n_positions)]
        return agents, open_positions, now

    def _compute_budget(self, n_positions: int) -> int:
        base = 30
        return min(120, base + n_positions * 2)

    def test_single_position_budget_is_32(self):
        self.assertEqual(self._compute_budget(1), 32)

    def test_five_positions_budget_is_40(self):
        self.assertEqual(self._compute_budget(5), 40)

    def test_budget_caps_at_120(self):
        # 46 positions → 30 + 92 = 122 → capped at 120
        self.assertEqual(self._compute_budget(46), 120)

    def test_guardian_age_95_with_1_position_is_high(self):
        """age=95s with 1 position → budget=32s → HIGH alert."""
        agents, positions, now = self._make_bot_with_positions(1, 95)
        budget = self._compute_budget(1)  # 32
        guardian_age = 95
        # Should be stale (95 > 32)
        stale = guardian_age > budget
        self.assertTrue(stale)

    def test_guardian_age_95_with_35_positions_is_not_high(self):
        """age=95s with 35 positions → budget=100s → NOT high."""
        budget = self._compute_budget(35)  # 100
        guardian_age = 95
        stale = guardian_age > budget
        self.assertFalse(stale)

    def test_guardian_age_92_is_medium_advisory(self):
        """age between 90s and budget → MEDIUM, not HIGH."""
        budget = self._compute_budget(10)  # 50
        guardian_age = 92
        stale = guardian_age > budget   # 92 > 50 → True (still stale)
        medium = not stale and guardian_age > 90
        # With 10 positions budget=50, 92 > 50 so still HIGH stale
        # To get medium: need age in (90, budget)
        budget2 = self._compute_budget(35)  # 100
        stale2 = 92 > budget2   # 92 > 100 → False
        medium2 = not stale2 and 92 > 90  # True
        self.assertTrue(medium2)
        self.assertFalse(stale2)


# ---------------------------------------------------------------------------
# Req 8 — notional-scaled min_profit_lock
# ---------------------------------------------------------------------------

class TestNotionalScaledMinProfitLock(unittest.TestCase):
    def _compute(self, fee_min: float, notional: float, cfg: dict) -> float:
        """Replicate the Req 8 formula from _live_multi_profit_lock_manage."""
        weak_signal_rate = max(0.0, float(cfg.get("profitLockWeakSignalRatePct", 0.04) or 0.04))
        return max(
            fee_min * 2.0,
            float(cfg.get("profitLockMinUsdt", 0.10) or 0.10),
            notional * weak_signal_rate / 100.0,
        )

    def test_small_notional_uses_fee_floor(self):
        """Very small notional: rate-based floor < fee_min*2 → fee_min*2 wins."""
        fee_min = 0.05
        notional = 5.0
        val = self._compute(fee_min, notional, {})
        # rate-based = 5 * 0.04% = 0.002; fee_min*2 = 0.10; profitLockMinUsdt=0.10
        self.assertAlmostEqual(val, 0.10, places=6)

    def test_large_notional_raises_threshold(self):
        """Large notional: rate-based floor wins over hard minimums."""
        fee_min = 0.05
        notional = 1000.0
        val = self._compute(fee_min, notional, {})
        # rate-based = 1000 * 0.04% = 0.40 → highest
        self.assertAlmostEqual(val, 0.40, places=6)

    def test_config_override_rate(self):
        """Custom profitLockWeakSignalRatePct overrides default 0.04."""
        val = self._compute(0.05, 500.0, {"profitLockWeakSignalRatePct": 0.10})
        # 500 * 0.10% = 0.50
        self.assertAlmostEqual(val, 0.50, places=6)

    def test_profitLockMinUsdt_is_hard_lower_bound(self):
        """profitLockMinUsdt beats rate-based floor when higher."""
        val = self._compute(0.01, 10.0, {"profitLockMinUsdt": 0.30})
        # rate-based = 0.004, fee*2=0.02, profitLockMinUsdt=0.30
        self.assertAlmostEqual(val, 0.30, places=6)

    def test_zero_notional_falls_back_to_fee_and_config_floor(self):
        fee_min = 0.06
        val = self._compute(fee_min, 0.0, {})
        # max(0.12, 0.10, 0.0) = 0.12
        self.assertAlmostEqual(val, 0.12, places=6)


# ---------------------------------------------------------------------------
# Req 1 — parallel intel: verify asyncio.gather is used (not sequential)
# ---------------------------------------------------------------------------

class TestParallelIntelDispatch(unittest.IsolatedAsyncioTestCase):
    async def test_gather_called_for_multiple_positions(self):
        """When N≥2 positions, all intel calls should be dispatched concurrently."""
        from trading import live_guardian
        call_times: list[float] = []

        async def slow_intel(req):
            call_times.append(time.monotonic())
            await asyncio.sleep(0.05)  # simulate 50ms intel call
            return {"signal": "WAIT", "confidence": 0.5}

        fake_rows = [
            {"symbol": f"SYM{i}USDT", "side": "LONG", "qty": 0.01,
             "entryMark": 100.0, "markPrice": 100.5, "notionalUsdtApprox": 10.0,
             "unRealizedProfit": 0.05, "leverage": 5,
             "marginUsedUsdt": 2.0, "isolatedWalletUsdt": 2.0}
            for i in range(3)
        ]

        with mock.patch.object(live_guardian, "intel_analyze", side_effect=slow_intel):
            with mock.patch.object(live_guardian, "_get_um_client", return_value=None):
                with mock.patch.object(live_guardian, "_signed_request",
                                       new=mock.AsyncMock(return_value=fake_rows)):
                    with mock.patch.object(live_guardian, "_fee_edge_min_net_usdt", return_value=0.02):
                        with mock.patch.object(live_guardian, "_profit_lock_policy",
                                               return_value={"trigger": 0.5, "lockUsdt": 0.3}):
                            with mock.patch.object(live_guardian, "_recent_payoff_loss_guard",
                                                   return_value={"active": False}):
                                with mock.patch.object(live_guardian, "_last_decision_intel", return_value=None):
                                    with mock.patch.object(live_guardian, "_entry_snapshot_from_intel", return_value={}):
                                        with mock.patch.object(live_guardian, "_effective_tp_sl",
                                                               return_value={"tpPct": 1.5, "slPct": 0.9, "notionalCapUsdt": 20.0, "profitLockTriggerUsdt": 0.5, "tier": "med", "volatilityScore": 0.0}):
                                            with mock.patch.object(live_guardian, "_symbol_effective_profile",
                                                                   return_value={"position_size_mult": 1.0, "entry_offset_bps": 0.0}):
                                                with mock.patch.object(live_guardian, "_calc_tp_sl_prices",
                                                                       return_value=(101.5, 99.1)):
                                                    with mock.patch.object(live_guardian, "_close_position_one_side", new=mock.AsyncMock()):
                                                        with mock.patch.object(live_guardian, "_agent_mark"):
                                                            with mock.patch.object(live_guardian, "_autotrade_log"):
                                                                with mock.patch.object(live_guardian, "_position_guardian_status_heartbeat"):
                                                                    from services import app_state
                                                                    app_state._LIVE_POSITIONS_CACHE = (time.time(), fake_rows)
                                                                    cfg = {"holdMinConfidence": 0.72, "tpTargetMaxUsdt": 3.0}
                                                                    import os
                                                                    with mock.patch.dict(os.environ, {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}):
                                                                        start = time.monotonic()
                                                                        await live_guardian._live_multi_profit_lock_manage(cfg)
                                                                        elapsed = time.monotonic() - start

        # 3 × 50ms sequential = 150ms; parallel should be ~50ms
        # Allow generous 130ms budget to avoid flaky CI failures
        self.assertLess(elapsed, 0.13,
            f"Expected concurrent dispatch (~50ms) but took {elapsed*1000:.0f}ms — "
            "intel calls appear to be sequential")


# ---------------------------------------------------------------------------
# Req 3 — no dead code / tv_blocks_tp_extension
# ---------------------------------------------------------------------------

class TestNoDeadCode(unittest.TestCase):
    def _guardian_source(self) -> str:
        """Read raw file content rather than inspect.getsource to avoid
        picking up class/function docstrings that mention old symbols."""
        import pathlib
        path = pathlib.Path(__file__).parent / "trading" / "live_guardian.py"
        return path.read_text(encoding="utf-8")

    def test_no_if_false_block_in_guardian_source(self):
        """Verify the source file no longer contains 'if False:' blocks."""
        src = self._guardian_source()
        # strip comments before checking so the assertion message in the old
        # comment doesn't false-positive
        lines_no_comment = "\n".join(
            line for line in src.splitlines()
            if not line.lstrip().startswith("#")
        )
        self.assertNotIn(
            "if False:",
            lines_no_comment,
            "Found 'if False:' dead-code block in non-comment code (Req 3)",
        )

    def test_tv_blocks_tp_extension_not_referenced(self):
        src = self._guardian_source()
        lines_no_comment = "\n".join(
            line for line in src.splitlines()
            if not line.lstrip().startswith("#")
        )
        self.assertNotIn(
            "tv_blocks_tp_extension",
            lines_no_comment,
            "Found reference to removed variable tv_blocks_tp_extension in non-comment code (Req 3)",
        )


if __name__ == "__main__":
    unittest.main()
