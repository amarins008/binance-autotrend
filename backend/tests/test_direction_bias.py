"""Tests for market direction bias detector (direction_bias.py)."""
import math

from analysis.direction_bias import (
    bias_gate,
    compute_direction_bias,
    detect_direction_bias,
    _structure_state,
    _swing_pivots,
)


def _row(px, i):
    return [i * 900000, round(px - 0.01, 4), round(px + 0.05, 4), round(px - 0.05, 4),
            round(px, 4), 100.0, i * 900000 + 899999, 10000.0, 10, 50.0, 5000.0, 0]


def _synth_wave(up=True, n=70, base=100.0):
    """Sawtooth waves with real pullbacks so swing pivots exist."""
    rows = []
    px = base
    for i in range(n):
        cycle = i % 14
        if up:
            px += 0.12 if cycle < 10 else -0.09
        else:
            px += -0.12 if cycle < 10 else 0.09
        rows.append(_row(px, i))
    return rows


def _synth_sideways(n=70, base=100.0):
    rows = []
    px = base
    for i in range(n):
        px += 0.15 * math.sin(i / 3.0)
        rows.append(_row(px, i))
    return rows


def test_low_data_returns_neutral():
    r = compute_direction_bias([], [])
    assert r["ok"] is False
    assert r["bias"] == "NEUTRAL"
    assert r["entry"]["keyword"] == "low_data"


def test_up_trend_gives_long_bias():
    rows = _synth_wave(up=True)
    r = compute_direction_bias(rows, rows)
    assert r["bias"] == "LONG"
    assert r["regime"] == "UP"
    assert r["strength"] >= 0.5


def test_down_trend_gives_short_bias():
    rows = _synth_wave(up=False)
    r = compute_direction_bias(rows, rows)
    assert r["bias"] == "SHORT"
    assert r["regime"] == "DOWN"


def test_sideways_gives_neutral():
    rows = _synth_sideways()
    r = compute_direction_bias(rows, rows)
    assert r["bias"] == "NEUTRAL"
    assert r["entry"]["keyword"] == "no_bias"


def test_extended_long_waits_for_pullback():
    rows = _synth_wave(up=True)
    r = compute_direction_bias(rows, rows)
    # price is above EMA20 after the up move, not yet at the pullback zone.
    assert r["bias"] == "LONG"
    assert r["entry"]["keyword"] == "wait_pullback"
    assert r["entry"]["action"] == "wait"


def test_pullback_to_ema_zone_triggers_entry():
    rows = _synth_wave(up=True)
    base = compute_direction_bias(rows, rows)
    zone = base["entry"]["emaZonePrice"]
    assert zone is not None
    adj = [list(r) for r in rows]
    adj[-1][4] = round(zone - 0.05, 4)  # pull back below EMA20
    adj[-1][2] = round(zone, 4)
    adj[-1][3] = round(zone - 0.10, 4)
    r = compute_direction_bias(adj, rows)
    assert r["bias"] == "LONG"
    assert r["entry"]["keyword"] == "pullback_in_zone"
    assert r["entry"]["action"] == "long_on_pullback"


def test_swing_pivots_locate_fractals():
    rows = _synth_wave(up=True)
    closes = [x[4] for x in rows]
    highs = [x[2] for x in rows]
    lows = [x[3] for x in rows]
    sh, sl = _swing_pivots(closes, highs, lows)
    assert len(sh) >= 2
    assert len(sl) >= 2


def test_structure_classifies_up_wave():
    rows = _synth_wave(up=True)
    closes = [x[4] for x in rows]
    highs = [x[2] for x in rows]
    lows = [x[3] for x in rows]
    direction, conf = _structure_state(closes, highs, lows)
    assert direction == "UP"
    assert conf >= 1


# ── bias_gate ──────────────────────────────────────────────────────────
def test_bias_gate_allows_matching_side():
    assert bias_gate("LONG", "LONG") == (True, "bias=LONG matches LONG")
    assert bias_gate("SHORT", "SHORT") == (True, "bias=SHORT matches SHORT")


def test_bias_gate_blocks_neutral_and_opposite():
    ok, reason = bias_gate("LONG", "NEUTRAL")
    assert not ok and "NEUTRAL" in reason
    ok, reason = bias_gate("SHORT", "LONG")
    assert not ok and "LONG" in reason and "SHORT" in reason
    ok, reason = bias_gate("LONG", "SHORT")
    assert not ok and "SHORT" in reason


def test_bias_gate_no_side_or_unavailable_treats_as_allow():
    assert bias_gate("WAIT", "LONG")[0] is True
    assert bias_gate("LONG", None)[0] is True
    assert bias_gate("LONG", "")[0] is True
    # detector outage (garbage/unknown bias) must never block the loop
    assert bias_gate("LONG", "???")[0] is True


def test_bias_gate_case_insensitive():
    assert bias_gate("long", "long")[0] is True
    assert bias_gate("Long", "neutral")[0] is False