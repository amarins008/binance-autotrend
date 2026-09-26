"""Exit-context telemetry tests — bias + TV recorded at guardian close time.

Stage 1 of the bias/TV hold matrix: every guardian close carries BOTH decision
systems' state at exit so the Stage-2 policy (bias = structural hold, TV =
fresh timing veto) can be validated on real post-exit continuation data.
Telemetry only — it must never change close behaviour.
"""
import inspect
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
for _cand in (_HERE, _HERE.parent):
    if _cand not in sys.path:
        sys.path.insert(0, str(_cand))

import main  # noqa: E402
import exchange.futures_orders as fo  # noqa: E402
import trading.live_guardian as lg  # noqa: E402
from trading.state_ops import exit_context_from_intel  # noqa: E402


def _intel(bias="LONG", strength=0.75, regime="UP", tv_signal="LONG",
           tv_strength=1.0, tv_age=5, tv_status="ok"):
    return {
        "directionBias": {"bias": bias, "strength": strength, "regime": regime},
        "tv": {"signal": tv_signal, "confidence": 0.8, "strength": tv_strength,
               "age": tv_age, "status": tv_status},
    }


class TestExitContextFromIntel:
    def test_aligned_bias_flagged(self):
        ctx = exit_context_from_intel("ENAUSDT", "LONG", _intel(bias="LONG"))
        assert ctx["exitBias"] == "LONG"
        assert ctx["exitBiasAligned"] is True
        assert ctx["exitBiasStrength"] == 0.75
        assert ctx["exitBiasRegime"] == "UP"

    def test_opposing_bias_not_aligned(self):
        ctx = exit_context_from_intel("ENAUSDT", "LONG", _intel(bias="SHORT"))
        assert ctx["exitBias"] == "SHORT"
        assert ctx["exitBiasAligned"] is False

    def test_tv_fields_carried_raw(self):
        ctx = exit_context_from_intel("ENAUSDT", "LONG", _intel())
        assert ctx["exitTvSignal"] == "LONG"
        assert ctx["exitTvStrength"] == 1.0
        assert ctx["exitTvAge"] == 5
        assert ctx["exitTvStatus"] == "ok"

    def test_neutral_bias_still_recorded(self):
        ctx = exit_context_from_intel("ENAUSDT", "LONG", _intel(bias="NEUTRAL"))
        assert ctx["exitBias"] == "NEUTRAL"
        assert ctx["exitBiasAligned"] is False

    def test_missing_intel_returns_empty(self):
        assert exit_context_from_intel("ENAUSDT", "LONG", None) == {}
        assert exit_context_from_intel("ENAUSDT", "LONG", "garbage") == {}

    def test_partial_intel_no_crash(self):
        ctx = exit_context_from_intel("ENAUSDT", "LONG", {"momentum": {}})
        assert ctx == {}


class TestCloseFunctionsAcceptExitIntel:
    def test_close_position_one_side_signature(self):
        sig = inspect.signature(fo._close_position_one_side)
        assert "exit_intel" in sig.parameters
        assert sig.parameters["exit_intel"].default is None

    def test_close_position_signature(self):
        sig = inspect.signature(fo._close_position)
        assert "exit_intel" in sig.parameters
        assert sig.parameters["exit_intel"].default is None

    def test_guardian_close_sites_pass_exit_intel(self):
        src = Path(lg.__file__).read_text(encoding="utf-8")
        n_pass = src.count("exit_intel=intel")
        assert n_pass >= 13, f"guardian close sites passing intel: {n_pass}"
        # the mechanical SL/BE gatherer has no per-position intel — stays None
        assert "await _close_position_one_side(sym_, side_, key, secret, base, reason=reason_)" in src
