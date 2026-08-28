"""Reproduce the "no entries" incident (2026-08-28).

Root-cause hypothesis from live telemetry:
  - marginBasedSizing (cap + multiple by margin) is NOT the blocker — sizing
    still computes a valid >0 USDT notional (BICOUSDT sized to 50 USDT).
  - The real blocker is the pre-reversal guard inside evaluate_entry_plan():
    when the market detector flags score >= preReversalScoreBlock AND the
    flagged side == the intended entry side, the entry is rejected with
    skip_code "pre_reversal".

These tests pin that behavior so any later "fix" (e.g. lowering the threshold
or disabling the guard) is deliberate, not accidental, and cannot silently
regress the margin-sizing path.

Run: pytest test_entry_block_scenario.py -v
"""
import asyncio
import os
import sys
import unittest
from pathlib import Path

# Make the backend importable both from repo root and from backend/.
_HERE = Path(__file__).resolve().parent
for _cand in (_HERE, _HERE.parent):
    if _cand not in sys.path:
        sys.path.insert(0, str(_cand))

import indicators  # noqa: E402
import main  # noqa: E402
from trading.pipeline import EntryInputs, EntryPlan, evaluate_entry_plan  # noqa: E402


def _base_cfg(**over):
    cfg = {
        "symbol": "BICOUSDT",
        "minConfidence": 0.75,
        "preReversalScoreBlock": 0.65,
        "preReversalScoreSoftener": 0.20,
        "tradeNotionalCapUsdt": 60.0,
        "usdtAmount": 60.0,
        "maxOpenPositions": 3,
        "marginBasedSizing": True,
        "marginRiskFraction": 0.33,
        "marginSizingMinUsdt": 5.0,
        "marginSizingMaxUsdt": 60.0,
    }
    cfg.update(over)
    return cfg


def _entry_inputs(cfg, signal="LONG", conf=0.832, pre_rev_score=0.0, pre_rev_risk="",
                  trade_usdt=50.0, **extra):
    # Live incident: BICOUSDT passed the TV gate because a FRESH TV snapshot
    # aligned LONG (so the tv_unavailable conservate branch was NOT taken).
    # Mirror that here — otherwise the TV-conservative gate (conf>=0.85) would
    # block at 0.832 before we ever reach the pre-reversal guard we're testing.
    intel = {
        "signal": signal, "confidence": conf,
        "execution": {"spreadBps": 5.0},
        "momentum": {"momentumPct": 0.50, "strength": 0.30},
        "imbalance": 0.10,  # LONG order-flow confirmation needs >= 0.03
        "htf": {"dir": "NEUTRAL", "strength": 0.0},  # no HTF conflict
        "precision": {"ema200Ready": False},  # EMA200 guard skipped when not ready
        "tv": {"signal": "LONG", "strength": 0.80, "age": 10},
    }
    kw = dict(
        cfg=cfg, intel=intel, regime={}, signal=signal, confidence=conf,
        spread_bps=5.0, slippage_bps=1.0, mark=0.039, ex={}, htf={},
        candle_ctx={}, adaptive_min_conf=0.69, trade_usdt=trade_usdt,
        pre_reversal_score=pre_rev_score, pre_reversal_side_at_risk=pre_rev_risk,
    )
    kw.update(extra)
    return EntryInputs(**kw)


def _make_klines(rsi_high=True, bb_high=True, n=60):
    """Synthetic 5m klines that push RSI + Bollinger %b into reversal zone.

    A flat series followed by a single explosive close spikes RSI to 100
    (overbought, +0.35) and drives BB %b >> 0.95 (+0.30) => base score 0.65
    (the live block threshold). An upper wick on the final bar adds margin.
    Verified against indicators._detect_pre_reversal output.
    """
    if not bb_high:
        # flat series -> mid-range BB, low reversal score
        closes = [100.0] * n
        highs = [c + 0.1 for c in closes]
        lows = [c - 0.1 for c in closes]
        return closes, highs, lows
    closes = [100.0] * (n - 1)
    # explosive final bar
    closes.append(160.0 if rsi_high else 100.0)
    highs = [c + 1.0 for c in closes]
    lows = [c - 1.0 for c in closes]
    # upper-wick rejection on final bar (open near high, close lower) -> +margin
    closes[-1] = 155.0
    highs[-1] = 162.0
    lows[-1] = 154.0
    return closes, highs, lows


class TestPreReversalBlocksEntry(unittest.IsolatedAsyncioTestCase):
    """SCENARIO 1 (root cause): pre-reversal guard rejects the entry."""

    def test_detector_flags_long_reversal_on_uptrend_klines(self):
        """_detect_pre_reversal must return score>=block and side_at_risk=LONG
        for the synthetic uptrend klines used to reproduce the incident."""
        closes, highs, lows = _make_klines()
        pre = indicators._detect_pre_reversal(closes, highs, lows)
        self.assertIsInstance(pre, dict)
        self.assertGreaterEqual(
            pre["score"], 0.65,
            f"detector score {pre['score']} should clear the 0.65 block threshold",
        )
        self.assertEqual(pre["side_at_risk"], "LONG")

    def test_entry_rejected_when_pre_reversal_score_at_threshold(self):
        """Exact live condition: score 0.65, side at risk LONG, entry LONG.
        Must be rejected with skip_code 'pre_reversal' (this is the blocker)."""
        cfg = _base_cfg()
        inp = _entry_inputs(cfg, signal="LONG", conf=0.832,
                            pre_rev_score=0.65, pre_rev_risk="LONG")
        plan = evaluate_entry_plan(inp)
        self.assertIsInstance(plan, EntryPlan)
        self.assertFalse(plan.approved, "entry must be blocked by pre-reversal guard")
        self.assertEqual(plan.skip_code, "pre_reversal")
        self.assertIn("0.65", plan.skip_message)

    def test_entry_rejected_when_pre_reversal_score_above_threshold(self):
        cfg = _base_cfg()
        inp = _entry_inputs(cfg, signal="LONG", conf=0.832,
                            pre_rev_score=0.80, pre_rev_risk="LONG")
        plan = evaluate_entry_plan(inp)
        self.assertFalse(plan.approved)
        self.assertEqual(plan.skip_code, "pre_reversal")

    def test_entry_allowed_when_reversal_risk_is_short_but_entry_long(self):
        """Guard only blocks when flagged side == entry side. SHORT-at-risk
        must NOT block a LONG entry (and vice-versa)."""
        cfg = _base_cfg()
        inp = _entry_inputs(cfg, signal="LONG", conf=0.832,
                            pre_rev_score=0.90, pre_rev_risk="SHORT")
        plan = evaluate_entry_plan(inp)
        self.assertTrue(plan.approved, "LONG entry should pass when SHORT is at risk")

    def test_entry_allowed_when_score_below_threshold_soft_zone(self):
        """A score just under the block (0.64) is NOT a hard block; it only
        softens confidence. With high conf 0.832 it should still be approved."""
        cfg = _base_cfg()
        inp = _entry_inputs(cfg, signal="LONG", conf=0.832,
                            pre_rev_score=0.64, pre_rev_risk="LONG")
        plan = evaluate_entry_plan(inp)
        self.assertTrue(plan.approved, "score 0.64 is below block; entry should pass")

    def test_entry_allowed_after_fix_threshold_072_with_score_070(self):
        """REGRESSION for the 2026-08-28 fix: after raising preReversalScoreBlock
        to 0.72, a score of 0.70 (which previously blocked at 0.65) must now
        PASS so entries can open in mild-reversal zones without bleeding."""
        cfg = _base_cfg(preReversalScoreBlock=0.72)
        inp = _entry_inputs(cfg, signal="LONG", conf=0.832,
                            pre_rev_score=0.70, pre_rev_risk="LONG")
        plan = evaluate_entry_plan(inp)
        self.assertTrue(plan.approved, "score 0.70 must pass once block raised to 0.72")
        self.assertNotEqual(plan.skip_code, "pre_reversal")

    def test_entry_still_blocked_after_fix_at_high_score(self):
        """The fix only relaxes the gate; a genuinely extreme reversal (>=0.72)
        must still be blocked to honor loss-prevention."""
        cfg = _base_cfg(preReversalScoreBlock=0.72)
        inp = _entry_inputs(cfg, signal="LONG", conf=0.832,
                            pre_rev_score=0.80, pre_rev_risk="LONG")
        plan = evaluate_entry_plan(inp)
        self.assertFalse(plan.approved)
        self.assertEqual(plan.skip_code, "pre_reversal")


class TestMarginSizingNotTheBlocker(unittest.IsolatedAsyncioTestCase):
    """SCENARIO 2: prove marginBasedSizing (cap+multiple by margin) is healthy
    and therefore is NOT why no trades open."""

    async def test_margin_sizing_returns_positive_notional(self):
        """With marginBasedSizing on and a healthy balance, the computed per-trade
        USDT must be > 0 (the incident's BICOUSDT sized to 50 USDT)."""
        cfg = _base_cfg()
        cfg["_liveAvailableBalance"] = 150.0  # healthy free margin
        usdt = await main._margin_aware_trade_usdt(cfg)
        self.assertGreater(usdt, 0.0, "margin-based sizing must not collapse to 0")
        # 150 / 3 * 0.33 = 16.5, clamped to [5, 60] -> 16.5
        self.assertAlmostEqual(usdt, 16.5, delta=0.5)

    async def test_margin_sizing_floor_at_symbol_min_notional(self):
        """Even with a small balance, sizing floors at marginSizingMinUsdt, so it
        never silently produces a sub-minimal (unplaceable) order."""
        cfg = _base_cfg()
        cfg["_liveAvailableBalance"] = 5.0  # tiny balance
        usdt = await main._margin_aware_trade_usdt(cfg)
        self.assertGreaterEqual(usdt, 5.0, "sizing must respect the min floor")

    async def test_margin_sizing_disabled_falls_back_to_usdt_amount(self):
        cfg = _base_cfg(marginBasedSizing=False)
        usdt = await main._margin_aware_trade_usdt(cfg)
        self.assertEqual(usdt, 60.0)


class TestEndToEndBlockRepro(unittest.IsolatedAsyncioTestCase):
    """SCENARIO 3: wire the detector output straight into the pipeline the same
    way _autotrade_loop does, proving the full path blocks on a real signal."""

    def test_detector_output_blocks_pipeline_end_to_end(self):
        closes, highs, lows = _make_klines()
        pre = indicators._detect_pre_reversal(closes, highs, lows)
        cfg = _base_cfg()
        inp = _entry_inputs(
            cfg, signal="LONG", conf=0.832,
            pre_rev_score=float(pre["score"]),
            pre_rev_risk=str(pre.get("side_at_risk") or ""),
        )
        plan = evaluate_entry_plan(inp)
        self.assertFalse(plan.approved)
        self.assertEqual(plan.skip_code, "pre_reversal")


if __name__ == "__main__":
    unittest.main(verbosity=2)
