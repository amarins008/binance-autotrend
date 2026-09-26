"""Entry-timing telemetry tests — `_entry_timing_from_1m` + snapshot propagation.

The telemetry mirrors the offline entry-forensics definition so per-trade
numbers stay comparable with the 2026-09-26 analysis (chase-exhaustion study).
Telemetry only — it must never gate entries.
"""
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
for _cand in (_HERE, _HERE.parent):
    if _cand not in sys.path:
        sys.path.insert(0, str(_cand))

import main  # noqa: E402  (ensures backend root on sys.path for state_ops)
from analysis.intel_analyze import _entry_timing_from_1m  # noqa: E402
from trading.state_ops import entry_snapshot_from_intel  # noqa: E402


def _row(i, close):
    return [i * 60000, close - 0.05, close + 0.05, close - 0.05, close, 100.0,
            i * 60000 + 59999, 10000.0, 10, 50.0, 5000.0, 0]


def _series(values):
    return [_row(i, px) for i, px in enumerate(values)]


class TestEntryTimingFrom1m:
    def test_strong_uptrend_sits_at_range_top(self):
        rows = _series([100.0 + i * 0.5 for i in range(60)])
        et = _entry_timing_from_1m(rows)
        assert et["rangePos60m"] >= 0.99
        assert et["runup60mPct"] > 20.0
        assert et["candles"] == 60

    def test_downtrend_sits_at_range_bottom(self):
        rows = _series([100.0 - i * 0.5 for i in range(60)])
        et = _entry_timing_from_1m(rows)
        assert et["rangePos60m"] <= 0.01
        assert et["breakdown60mPct"] > 20.0

    def test_flat_series_mid_range(self):
        rows = _series([100.0] * 60)
        et = _entry_timing_from_1m(rows)
        assert 0.0 <= et["rangePos60m"] <= 1.0
        assert et["range60mPct"] <= 0.2

    def test_too_few_candles_returns_empty(self):
        assert _entry_timing_from_1m(_series([100.0] * 10)) == {}
        assert _entry_timing_from_1m(None) == {}
        assert _entry_timing_from_1m([]) == {}

    def test_uses_last_60_only(self):
        rows = _series([200.0] * 200 + [100.0 + i * 0.3 for i in range(60)])
        et = _entry_timing_from_1m(rows)
        assert et["candles"] == 60
        assert et["rangePos60m"] >= 0.99  # 200-block must not pollute the window


class TestSnapshotCarriesEntryTiming:
    def test_snapshot_copies_timing_fields(self):
        intel = {
            "symbol": "TESTUSDT",
            "signal": "LONG",
            "confidence": 0.9,
            "execution": {"spreadBps": 5.0},
            "momentum": {"momentumPct": 0.3},
            "candles": {},
            "tv": {"signal": "LONG", "confidence": 0.8, "strength": 1.0},
            "entryTiming": {
                "rangePos60m": 0.91,
                "runup60mPct": 2.58,
                "breakdown60mPct": 0.41,
                "range60mPct": 3.1,
            },
        }
        snap = entry_snapshot_from_intel("TESTUSDT", "LONG", intel)
        assert snap["entryRangePos60m"] == 0.91
        assert snap["entryRunup60mPct"] == 2.58
        assert snap["entryBreakdown60mPct"] == 0.41
        assert snap["entryRange60mPct"] == 3.1

    def test_snapshot_without_timing_stays_clean(self):
        intel = {
            "symbol": "TESTUSDT",
            "signal": "WAIT",
            "confidence": 0.5,
            "execution": {"spreadBps": 5.0},
            "momentum": {"momentumPct": 0.0},
            "candles": {},
            "tv": {"signal": "WAIT", "confidence": 0.5, "strength": 0.5},
        }
        snap = entry_snapshot_from_intel("TESTUSDT", "WAIT", intel)
        assert "entryRangePos60m" not in snap
        assert "entryRunup60mPct" not in snap
