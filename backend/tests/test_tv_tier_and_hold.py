"""Tests for the 2026-09-26 TV strength tier + hold-winner fixes.

- _tv_long_strength_tier: fresh-TV LONG strength tiers (full / mid / blocked / stale)
- should_hold_winner: WAIT signals must not be gated by the directional conf
  floor (WAIT conf is capped ~0.50 upstream, which made hold-winner activation
  impossible — 0/124 trades).
"""
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
for _cand in (_HERE, _HERE.parent):
    if _cand not in sys.path:
        sys.path.insert(0, str(_cand))

import main  # noqa: E402
from trading.position import should_hold_winner  # noqa: E402


def _tv(signal="LONG", strength=0.95, age=1, status="ok"):
    return {"signal": signal, "confidence": 0.8, "strength": strength, "age": age, "status": status}


class TestTvLongStrengthTier:
    def test_fresh_strong_is_full(self):
        tier, mult = main._tv_long_strength_tier(_tv(strength=0.95), {})
        assert tier == "full" and mult == 1.0

    def test_fresh_mid_is_reduced(self):
        tier, mult = main._tv_long_strength_tier(_tv(strength=0.60), {})
        assert tier == "mid"
        assert mult == 0.70

    def test_fresh_below_floor_is_blocked(self):
        tier, mult = main._tv_long_strength_tier(_tv(strength=0.40), {})
        assert tier == "blocked" and mult == 0.0

    def test_stale_age_is_not_evidence(self):
        tier, _ = main._tv_long_strength_tier(_tv(strength=0.10, age=90), {})
        assert tier == "stale"

    def test_legacy_snapshot_without_status_is_stale(self):
        tier, _ = main._tv_long_strength_tier({"signal": "LONG", "strength": 0.2}, {})
        assert tier == "stale"

    def test_missing_tv_is_stale(self):
        assert main._tv_long_strength_tier(None, {})[0] == "stale"
        assert main._tv_long_strength_tier({}, {})[0] == "stale"

    def test_non_long_tv_left_to_conflict_gates(self):
        tier, mult = main._tv_long_strength_tier(_tv(signal="SHORT", strength=0.95), {})
        assert tier == "full" and mult == 1.0

    def test_boundary_uses_mid_floor(self):
        assert main._tv_long_strength_tier(_tv(strength=0.50), {})[0] == "mid"
        assert main._tv_long_strength_tier(_tv(strength=0.90), {})[0] == "full"


class TestShouldHoldWinnerWaitPath:
    def _cfg(self, **over):
        cfg = {
            "holdWinners": True,
            "holdAllowWaitSignal": True,
            "holdWaitSignalIgnoreConf": True,
            "holdMinConfidence": 0.78,
            "holdMinMomentumPct": 0.05,
        }
        cfg.update(over)
        return cfg

    def _intel(self, signal="WAIT", conf=0.45, mom=0.20):
        return {
            "signal": signal,
            "confidence": conf,
            "execution": {"momentumPct": mom},
        }

    def test_wait_low_conf_with_aligned_momentum_now_holds(self):
        # The regression: WAIT conf 0.45 < 0.78 floor made this False forever.
        assert should_hold_winner("LONG", self._intel(), self._cfg()) is True

    def test_wait_conf_gate_can_be_restored(self):
        cfg = self._cfg(holdWaitSignalIgnoreConf=False)
        assert should_hold_winner("LONG", self._intel(), cfg) is False

    def test_wait_against_momentum_does_not_hold(self):
        assert should_hold_winner("LONG", self._intel(mom=-0.20), self._cfg()) is False

    def test_opposite_signal_never_holds(self):
        assert should_hold_winner("LONG", self._intel(signal="SHORT", conf=0.9), self._cfg()) is False

    def test_directional_low_conf_still_requires_floor(self):
        assert should_hold_winner("LONG", self._intel(signal="LONG", conf=0.5), self._cfg()) is False

    def test_directional_aligned_holds(self):
        assert should_hold_winner("LONG", self._intel(signal="LONG", conf=0.85), self._cfg()) is True

    def test_disabled_hold_winners(self):
        assert should_hold_winner("LONG", self._intel(), self._cfg(holdWinners=False)) is False
