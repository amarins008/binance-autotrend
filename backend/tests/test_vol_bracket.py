"""Vol-bracket TP/SL tests — per-symbol volatility-scaled brackets.

2026-09-26 operator proposal implemented: TP/SL as % of the realized 30m
swing instead of the fixed +/-2 USDT target (which was 3.7x the swing on
BNB 0.54% and unreachable — LOCAL_TP_HIT 1/79). Replay on 79 LIVE trades:
+26.67 vs +24.59, TP touch 40 vs SL 5. Asymmetric (TP<SL) BY DESIGN — the
edge is the touch rate, so the R:R gate uses volBracketMinRiskReward.
"""
import dataclasses
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
for _cand in (_HERE, _HERE.parent):
    if _cand not in sys.path:
        sys.path.insert(0, str(_cand))

import main  # noqa: E402
from trading.risk import _vol_bracket_pct, effective_tpsl_pct_for_trade  # noqa: E402
from trading.pipeline import EntryInputs, evaluate_entry_plan  # noqa: E402


class TestVolBracketPct:
    def test_mid_vol_symbol(self):
        tp, sl, meta = _vol_bracket_pct(1.0, {})
        assert abs(tp - 0.5) < 1e-9      # 0.5 x swing
        assert abs(sl - 1.2) < 1e-9      # 1.2 x swing
        assert meta["bracket"] is True

    def test_low_vol_hits_fee_floor(self):
        # swing 0.2% -> tp would be 0.1% but the 3x-fee floor (0.36%) holds
        tp, sl, meta = _vol_bracket_pct(0.2, {})
        assert tp >= 0.36
        assert sl >= 0.90

    def test_high_vol_capped(self):
        tp, sl, meta = _vol_bracket_pct(5.0, {})
        assert tp <= 2.0
        assert sl <= 2.0

    def test_asymmetric_by_design(self):
        tp, sl, _ = _vol_bracket_pct(1.0, {})
        assert tp < sl  # touch-rate edge, not ratio edge

    def test_extreme_swing_clamped(self):
        tp, sl, _ = _vol_bracket_pct(20.0, {})
        assert tp <= 2.0 and sl <= 2.0


class TestTpslFunctionBracketBranch:
    def _cfg(self):
        return {
            "tpSlTargetUsdtEnabled": True,
            "volBracketEnabled": True,
            "volBracketTpMult": 0.5,
            "volBracketSlMult": 1.2,
            "volBracketMinTpPct": 0.36,
            "volBracketMaxTpPct": 2.0,
            "volBracketMinSlPct": 0.90,
            "volBracketMaxSlPct": 2.0,
            "takeProfitPct": 1.8,
            "stopLossPct": 0.9,
        }

    def test_bracket_active_with_swing(self):
        precision = {"swing30Pct": 1.0}
        tp, sl, meta = effective_tpsl_pct_for_trade(self._cfg(), 100.0, None, precision=precision, effective_leverage=10)
        assert abs(tp - 0.5) < 1e-6
        assert abs(sl - 1.2) < 1e-6
        assert meta["bracket"] is True

    def test_bracket_off_falls_back(self):
        cfg = self._cfg()
        cfg["volBracketEnabled"] = False
        precision = {"swing30Pct": 1.0}
        tp, sl, meta = effective_tpsl_pct_for_trade(cfg, 100.0, None, precision=precision, effective_leverage=10)
        assert meta.get("bracket") is None

    def test_bracket_without_swing_falls_back(self):
        tp, sl, meta = effective_tpsl_pct_for_trade(self._cfg(), 100.0, None, precision={}, effective_leverage=10)
        assert meta.get("bracket") is None


class TestPipelineBracketMinRr:
    def _inputs(self, cfg, precision):
        intel = {
            "signal": "LONG",
            "confidence": 0.85,
            "momentum": {"momentumPct": 0.5, "strength": 0.3},
            "tv": {"signal": "LONG", "strength": 1.0, "confidence": 0.9, "age": 1, "status": "ok"},
            "precision": precision,
        }
        return EntryInputs(
            cfg=cfg,
            intel=intel,
            regime={},
            signal="LONG",
            confidence=0.85,
            spread_bps=5.0,
            slippage_bps=1.0,
            mark=100.0,
            ex={},
            htf={"dir": "NEUTRAL", "strength": 0.0},
            candle_ctx={},
            adaptive_min_conf=0.72,
            trade_usdt=50.0,
            eff_leverage=10,
        )

    def test_bracket_rr_below_one_passes(self):
        cfg = {
            "minRiskRewardRatio": 1.0,
            "volBracketEnabled": True,
            "volBracketMinRiskReward": 0.40,
            "minMomentumStrength": 0.005,
        }
        precision = {"swing30Pct": 1.0}  # tp 0.5 / sl 1.2 -> rr 0.42
        plan = evaluate_entry_plan(self._inputs(cfg, precision))
        assert plan.skip_code != "risk_reward"

    def test_normal_mode_still_requires_rr_one(self):
        cfg = {"minRiskRewardRatio": 1.0}
        precision = {"swing30Pct": 1.0}
        plan = evaluate_entry_plan(self._inputs(cfg, precision))
        # without the bracket the full R:R gate applies to the same asymmetry
        assert plan.skip_code == "risk_reward" or not plan.approved
