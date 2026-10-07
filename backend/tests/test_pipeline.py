"""Tests for the entry pipeline gates (pipeline.py).

Focuses on the early deterministic gates (signal, spread, confidence) which
do not require a fully-populated intel/TV context.  Later gates (fee edge,
pattern, TV-conflict) are covered loosely with an all-clear input.
"""
import pytest

from trading.pipeline import evaluate_entry_plan, EntryInputs


def _inputs(**overrides) -> EntryInputs:
    base = dict(
        cfg={},
        intel={},
        regime={},
        signal="LONG",
        confidence=0.80,
        spread_bps=5.0,
        slippage_bps=2.0,
        mark=100.0,
        ex={},
        htf={},
        candle_ctx={},
        adaptive_min_conf=0.72,
    )
    base.update(overrides)
    return EntryInputs(**base)


def test_signal_wait_rejected():
    plan = evaluate_entry_plan(_inputs(signal="WAIT"))
    assert not plan.approved
    assert plan.skip_code == "signal_wait"


def test_spread_too_wide_rejected():
    plan = evaluate_entry_plan(_inputs(spread_bps=40.0, cfg={"maxSpreadBps": 18}))
    assert not plan.approved
    assert plan.skip_code == "spread"


def test_low_confidence_rejected():
    plan = evaluate_entry_plan(_inputs(confidence=0.50, adaptive_min_conf=0.72))
    assert not plan.approved
    assert plan.skip_code == "low_confidence"


def test_conf_too_high_late_chase_rejected():
    plan = evaluate_entry_plan(
        _inputs(confidence=0.95, cfg={"maxEntryConfidence": 0.90})
    )
    assert not plan.approved
    assert plan.skip_code == "conf_too_high_late"


def test_audit_trail_recorded():
    plan = evaluate_entry_plan(_inputs(signal="WAIT"))
    gates = plan.pipeline
    assert any(g["gate"] == "signal" and not g["passed"] for g in gates)


def test_pipeline_returns_basic_metadata():
    plan = evaluate_entry_plan(_inputs(signal="LONG", confidence=0.80))
    # Plan object carries signal + confidence even when rejected early
    assert plan.signal == "LONG"
    assert plan.confidence == 0.80


def test_short_entries_disabled_by_default():
    plan = evaluate_entry_plan(_inputs(signal="SHORT"))
    assert not plan.approved
    assert plan.skip_code == "side_blocked"
    assert "SHORT entries disabled" in plan.skip_message
    assert any(g["gate"] == "side" and g["passed"] for g in plan.pipeline)


def test_long_entries_allowed_when_short_disabled():
    plan = evaluate_entry_plan(_inputs(signal="LONG", cfg={"shortEntriesEnabled": False}))
    assert plan.skip_code != "side_blocked"


def test_short_entries_allowed_when_explicitly_enabled():
    plan = evaluate_entry_plan(_inputs(signal="SHORT", cfg={"shortEntriesEnabled": True}))
    assert plan.skip_code != "side_blocked"
