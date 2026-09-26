"""Fee-model regression tests — the 2026-09-26 Binance income reconciliation
measured real commission at ~5.9-7bps/side (VIP0 taker 5bps), while the bot
modeled 4bps — understating cost ~40% and letting fee-unsafe micro-wins
through the fee_edge gate. Defaults are now 6bps/side, and close records
carry feeEstUsdt so the daily KPI is NET of fees."""
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
for _cand in (_HERE, _HERE.parent):
    if _cand not in sys.path:
        sys.path.insert(0, str(_cand))

import main  # noqa: E402
from trading.risk import (  # noqa: E402
    AUTOTRADE_TAKER_FEE_BPS_PER_SIDE,
    estimate_trade_edge_usdt,
    fee_edge_min_net_usdt,
)
from trading.pipeline import EntryInputs  # noqa: E402


class TestFeeModelDefaults:
    def test_module_default_is_six_bps(self):
        assert AUTOTRADE_TAKER_FEE_BPS_PER_SIDE == 6.0
        assert main.AUTOTRADE_TAKER_FEE_BPS_PER_SIDE == 6.0

    def test_estimate_default_uses_six_bps(self):
        gross, cost, net = estimate_trade_edge_usdt(usdt_amount=100.0, tp_pct=2.0, max_slippage_bps=0.0)
        # cost = 100 * (2*6 + 2) / 10000 = 0.14 USDT
        assert abs(cost - 0.14) < 1e-9
        assert abs(net - (2.0 - 0.14)) < 1e-9

    def test_fee_edge_min_net_scales_with_real_notional(self):
        val = fee_edge_min_net_usdt({"feeMinNetProfitUSDT": 0.01}, 0.0, 100.0)
        # 100 notional * 12bps RT = 0.12
        assert abs(val - 0.12) < 1e-9

    def test_entry_inputs_default_is_six_bps(self):
        import dataclasses
        fields = {f.name: f.default for f in dataclasses.fields(EntryInputs)}
        assert fields["taker_fee_bps"] == 6.0
