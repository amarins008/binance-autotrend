"""Tests for supervisor self-correction framework.

Covers:
- Deep correction escalation logic
- Agent self-correction handlers
- Dispatch routing
"""

import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from supervisor_self_correction import (
    _is_deep_correction_required,
    _position_guardian_self_correct,
    _strategy_builder_self_correct,
    _market_analyst_self_correct,
)


class TestDeepCorrectionEscalation:
    """Tests for _is_deep_correction_required."""

    def test_daily_entry_regression_is_deep(self):
        assert _is_deep_correction_required("strategy_builder", "daily_entry_regression", "") is True

    def test_negative_expectancy_is_deep(self):
        assert _is_deep_correction_required("strategy_builder", "negative_expectancy", "") is True

    def test_scan_timeout_is_not_deep(self):
        assert _is_deep_correction_required("market_analyst", "scan_timeout", "") is False

    def test_workload_imbalance_is_deep(self):
        assert _is_deep_correction_required("position_guardian", "", "review workload split") is True

    def test_basic_heartbeat_is_not_deep(self):
        assert _is_deep_correction_required("position_guardian", "", "prioritize heartbeat") is False


class TestPositionGuardianSelfCorrect:
    """Tests for _position_guardian_self_correct."""

    def test_heartbeat_returns_true(self):
        corrected, changes = _position_guardian_self_correct("prioritize heartbeat", "", {})
        assert corrected is True
        assert changes == {}

    def test_small_profit_tightens_giveback(self):
        cfg = {"profitLockMaxGivebackUsdt": 0.15}
        corrected, changes = _position_guardian_self_correct("auto-tuned small-profit", "small_profit_capture", cfg)
        assert corrected is True
        assert changes["profitLockMaxGivebackUsdt"] < 0.15

    def test_weak_payoff_tightens_sl(self):
        cfg = {"stopLossPct": 0.8}
        corrected, changes = _position_guardian_self_correct("auto-tuned weak payoff", "weak_payoff_ratio", cfg)
        assert corrected is True
        assert changes["stopLossPct"] < 0.8


class TestStrategyBuilderSelfCorrect:
    """Tests for _strategy_builder_self_correct."""

    def test_entry_gate_raises_confidence(self):
        cfg = {"minConfidence": 0.66}
        corrected, changes = _strategy_builder_self_correct("inspect entry gate blockers", "", cfg)
        assert corrected is True
        assert changes["minConfidence"] > 0.66

    def test_anti_chase_tightens_thresholds(self):
        cfg = {"lateEntryMaxBbPctB": 0.90, "lateEntryMaxVwapDistancePct": 0.32}
        corrected, changes = _strategy_builder_self_correct("tighten anti-chase", "", cfg)
        assert corrected is True
        assert changes["lateEntryMaxBbPctB"] < 0.90
        assert changes["lateEntryMaxVwapDistancePct"] < 0.32

    def test_negative_expectancy_returns_true(self):
        corrected, changes = _strategy_builder_self_correct("", "negative_expectancy", {})
        assert corrected is True
        assert changes == {}


class TestMarketAnalystSelfCorrect:
    """Tests for _market_analyst_self_correct."""

    def test_refresh_expands_universe(self):
        cfg = {"scanTopLiquid": 60, "scanAnalyzeTop": 12}
        corrected, changes = _market_analyst_self_correct("refresh scan blockers", "", cfg)
        assert corrected is True
        assert changes["scanTopLiquid"] > 60
        assert changes["scanAnalyzeTop"] > 12

    def test_stale_board_clears(self):
        corrected, changes = _market_analyst_self_correct("scan board stale", "", {})
        assert corrected is True
        assert changes == {}


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
