"""Tests for market regime classification (regime.py)."""
from trading.regime import detect_market_regime


def test_none_input_returns_unknown():
    r = detect_market_regime(None)
    assert r["name"] == "UNKNOWN"
    assert r["sizeMultiplier"] == 1.0


def test_volatile_regime():
    intel = {
        "precision": {"atrPct": 1.2, "bbBandwidth": 0.08},  # high ATR → volatile
        "momentum": {"volumeRatio": 1.2, "momentumPct": -0.05},
    }
    r = detect_market_regime(intel)
    assert r["name"] == "VOLATILE"


def test_trend_regime():
    intel = {
        "precision": {
            "atrPct": 0.2,
            "bbBandwidth": 0.03,
            "longScore": 5.0,
            "shortScore": 0.5,
            "trendUp": True,
        },
        "momentum": {"volumeRatio": 1.2, "momentumPct": 0.3},
    }
    r = detect_market_regime(intel)
    assert r["name"] == "TREND"


def test_range_regime():
    intel = {
        "precision": {
            "atrPct": 0.02,  # low ATR → range
            "bbBandwidth": 0.01,
            "longScore": 3.0,
            "shortScore": 2.5,
        },
        "momentum": {"volumeRatio": 1.0, "momentumPct": 0.01},
    }
    r = detect_market_regime(intel)
    assert r["name"] == "RANGE"


def test_loss_streak_lowers_thresholds():
    # At high loss streak, even moderate ATR should trigger VOLATILE
    intel = {
        "precision": {"atrPct": 0.6, "bbBandwidth": 0.03},
        "momentum": {"volumeRatio": 1.0, "momentumPct": 0.02},
    }
    r_high = detect_market_regime(intel, loss_streak=5)
    assert r_high["name"] == "VOLATILE"


def test_volatile_sizes_down():
    intel = {
        "precision": {"atrPct": 1.5, "bbBandwidth": 0.10},
        "momentum": {"volumeRatio": 1.3, "momentumPct": -0.1},
    }
    r = detect_market_regime(intel)
    assert r["sizeMultiplier"] < 1.0
