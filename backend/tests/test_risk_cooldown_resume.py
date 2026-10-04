"""Regression: the adaptive risk-cooldown release gate must actually run.

_risk_cooldown_resume_ok referenced `board`, which is not in scope, so every
adaptive check since 2026-08-24 raised NameError and the cooldown could only
ever expire by its timer (the market-based early release was dead code).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_resume_ok_runs_without_nameerror():
    import main

    board = main.AUTO_TRADE.get("scanBoard")
    main.AUTO_TRADE["scanBoard"] = [{"confidence": 0.7}, {"confidence": 0.8}]
    try:
        ok, reason = main._risk_cooldown_resume_ok({"minConfidence": 0.62}, None, None)
    finally:
        main.AUTO_TRADE["scanBoard"] = board
    assert ok is False
    assert reason == "no market intel"


def test_resume_ok_accepts_strong_signal():
    import main

    board = main.AUTO_TRADE.get("scanBoard")
    main.AUTO_TRADE["scanBoard"] = []
    try:
        intel = {
            "symbol": "BTCUSDT",
            "signal": "LONG",
            "confidence": 0.9,
            "precision": {"longScore": 2.0, "shortScore": 0.5},
            "execution": {"spreadBps": 5.0},
        }
        ok, reason = main._risk_cooldown_resume_ok({"minConfidence": 0.62}, "BTCUSDT", intel)
    finally:
        main.AUTO_TRADE["scanBoard"] = board
    assert ok is True
