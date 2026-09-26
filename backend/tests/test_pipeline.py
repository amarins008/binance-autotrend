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


def test_stale_tv_snapshot_routes_to_unavailable_gate():
    plan = evaluate_entry_plan(_inputs(
        confidence=0.75,
        cfg={"tvEntryMaxAgeSec": 30, "tvUnavailableMinConf": 0.80},
        intel={"tv": {"signal": "LONG", "strength": 1.0, "confidence": 0.9, "age": 90, "status": "ok"}},
    ))
    assert not plan.approved
    assert plan.skip_code == "tv_unavailable_low_conf"


def test_legacy_tv_snapshot_without_age_keeps_compatibility():
    plan = evaluate_entry_plan(_inputs(
        confidence=0.75,
        cfg={"tvUnavailableMinConf": 0.80},
        intel={"tv": {"signal": "LONG", "strength": 1.0, "confidence": 0.9}},
    ))
    assert plan.skip_code != "tv_unavailable_low_conf"


def test_explicit_htf_conflict_blocks():
    plan = evaluate_entry_plan(_inputs(
        cfg={"htfStrictEnabled": True, "htfMinStrength": 0.22},
        htf={"dir": "SHORT", "strength": 0.8},
        intel={"momentum": {"momentumPct": 0.5, "strength": 0.3}, "tv": {"signal": "LONG", "strength": 1.0, "confidence": 0.9, "age": 1, "status": "ok"}},
    ))
    assert not plan.approved
    assert plan.skip_code == "htf_conflict"


def test_ema200_below_long_blocks_when_ready():
    plan = evaluate_entry_plan(_inputs(
        cfg={"ema200StrictEnabled": True},
        intel={
            "tv": {"signal": "LONG", "strength": 1.0, "confidence": 0.9, "age": 1, "status": "ok"},
            "momentum": {"momentumPct": 0.5, "strength": 0.3},
            "precision": {"ema200Ready": True, "priceAboveEma200": False},
        },
    ))
    assert not plan.approved
    assert plan.skip_code == "ema200"


def test_short_positive_pattern_bias_blocks():
    plan = evaluate_entry_plan(_inputs(
        signal="SHORT",
        cfg={"shortPatternBiasMax": 0.002, "orderFlowConfirmation": False},
        intel={"momentum": {"momentumPct": -0.5, "strength": 0.3}, "candles": {"bias": 0.01, "tags": []}},
        candle_ctx={"bias": 0.01, "tags": []},
    ))
    assert not plan.approved
    assert plan.skip_code == "short_positive_pattern_bias"
