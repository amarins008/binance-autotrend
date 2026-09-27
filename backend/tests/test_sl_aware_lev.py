"""SL-aware leverage cap tests — liquidation distance >= 2x expected SL.

2026-09-26: with the vol-bracket SL range 0.9-2.0% and the historical
ceiling of 25, the cap is exactly matched at the 2.0% tail (liq distance 4%
= 2x SL). Tight-SL trades (0.9%) may extend to ~55 only when the operator
raises leverageMax — the clamp never raises leverage on its own.
"""
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
for _cand in (_HERE, _HERE.parent):
    if _cand not in sys.path:
        sys.path.insert(0, str(_cand))

import main  # noqa: E402
from trading.risk import sl_aware_leverage_cap  # noqa: E402


class TestSlAwareLeverageCap:
    def test_tail_sl_two_pct_caps_at_25(self):
        # the 2.0% tail matches the historical ceiling exactly
        assert sl_aware_leverage_cap(2.0, 50) == 25.0
        assert sl_aware_leverage_cap(2.0, 25) == 25.0

    def test_tight_sl_allows_higher_lev(self):
        # cap = 100 / (2 x expected SL); expected SL = 1.2 x swing
        assert abs(sl_aware_leverage_cap(0.9, 50) - 46.3) < 0.1
        assert abs(sl_aware_leverage_cap(1.0, 50) - 41.7) < 0.1
        assert abs(sl_aware_leverage_cap(0.75, 50) - 50.0) < 0.1  # expected SL 0.9 -> cap at lev_max

    def test_wide_sl_bounded_by_formula_not_lev_max(self):
        # swing 5% -> sl_pred 2.0 (clamped) -> cap 25 even with lev_max 50
        assert sl_aware_leverage_cap(5.0, 50) == 25.0

    def test_missing_swing_falls_back_to_tail(self):
        assert sl_aware_leverage_cap(None, 50) == 25.0
        assert sl_aware_leverage_cap(0.0, 50) == 25.0

    def test_never_raises_leverage(self):
        assert sl_aware_leverage_cap(0.9, 10) == 10.0   # cap below current -> stays

    def test_live_ceiling_25_unchanged(self):
        # with the production leverageMax=25 the clamp is a no-op for all SLs
        for swing in (0.9, 1.0, 1.2, 1.5, 2.0, 3.0, 5.0):
            assert sl_aware_leverage_cap(swing, 25) == 25.0

    def test_main_wiring_uses_swing30(self):
        src = Path(main.__file__).read_text(encoding="utf-8")
        assert "_sl_aware_leverage_cap(" in src
        assert "swing30Pct" in src
