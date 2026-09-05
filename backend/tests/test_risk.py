"""Tests for the risk desk functions (risk.py)."""
from trading.risk import (
    estimate_trade_edge_usdt,
    effective_min_net_profit_usdt,
    passes_min_risk_reward,
    fee_edge_min_net_usdt,
    calc_tp_sl_prices,
)


def test_calc_tp_sl_prices_long():
    tp, sl = calc_tp_sl_prices("LONG", 100.0, 1.8, 0.8)
    assert abs(tp - 101.8) < 1e-9
    assert abs(sl - 99.2) < 1e-9


def test_calc_tp_sl_prices_short():
    tp, sl = calc_tp_sl_prices("SHORT", 100.0, 1.8, 0.8)
    assert abs(tp - 98.2) < 1e-9
    assert abs(sl - 100.8) < 1e-9


def test_fee_edge_min_net_uses_configured_floor():
    # est_cost 0, notional 0 → just the configured floor (clamped by multiple).
    val = fee_edge_min_net_usdt({"feeMinNetProfitUSDT": 0.05}, 0.0, 0.0)
    assert val == 0.05


def test_fee_edge_min_net_scales_with_cost():
    val = fee_edge_min_net_usdt({"feeMinNetProfitUSDT": 0.05}, est_cost_usdt=0.10)
    assert val >= 0.30  # 0.10 * 3.0


def test_fee_edge_min_net_taker_roundtrip():
    # notional 100 @ 4bps/side → roundtrip = 100 * 2*4/10000 = 0.08; *3 = 0.24
    val = fee_edge_min_net_usdt({"feeMinNetProfitUSDT": 0.01}, 0.0, 100.0)
    assert abs(val - 0.24) < 1e-6


def test_estimate_trade_edge_basic():
    gross, cost, net = estimate_trade_edge_usdt(
        usdt_amount=100.0,
        tp_pct=1.8,
        max_slippage_bps=28.0,
    )
    # gross = 100 * 0.018 = 1.8
    assert abs(gross - 1.8) < 1e-6
    assert cost > 0
    assert net == gross - cost


def test_passes_min_risk_reward_ok():
    assert passes_min_risk_reward(tp_pct=1.8, sl_pct=0.8, min_rr=1.5)


def test_passes_min_risk_reward_fail():
    assert not passes_min_risk_reward(tp_pct=0.5, sl_pct=1.0, min_rr=1.5)


def test_passes_min_risk_reward_matches_exactly():
    assert passes_min_risk_reward(tp_pct=1.5, sl_pct=1.0, min_rr=1.5)


def test_effective_min_net_zero_disables_gate():
    cfg = {"feeMinNetProfitUSDT": 0, "usdtAmount": 100, "takeProfitPct": 1.8}
    assert effective_min_net_profit_usdt(cfg) == 0.0


def test_effective_min_net_positive():
    cfg = {
        "feeMinNetProfitUSDT": 0.05,
        "feeMinEdgeVsCostMultiple": 1.2,
        "usdtAmount": 100,
        "takeProfitPct": 1.8,
        "maxSlippageBps": 28.0,
    }
    val = effective_min_net_profit_usdt(cfg)
    assert val >= 0.05


def test_fee_adaptive_scales_with_volatility():
    cfg = {
        "feeMinNetProfitUSDT": 0.05,
        "usdtAmount": 100,
        "takeProfitPct": 1.8,
        "maxSlippageBps": 28.0,
        "feeAdaptiveNetEnabled": True,
    }
    low_vol = effective_min_net_profit_usdt(cfg, realized_vol_pct=0.05)
    high_vol = effective_min_net_profit_usdt(cfg, realized_vol_pct=0.4)
    # High volatility should have a lower multiplier requirement
    assert high_vol <= low_vol + 1e-6
