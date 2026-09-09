# Task Tracker

## Active Tasks

### ✅ Completed
- [x] Fee analysis: 3d gross +$19.94, fees -$21.07, net -$1.13
- [x] Config: TP=$2-5, slToTpRatio=0.67, minRR=1.5, feeMin=$0.20
- [x] Direction bias gate: biasGateEnabled=true, minStrength=0.25
- [x] Guardian retrace_budget fix: fresh fee_min each cycle (commit 8689364)
- [x] SL ratio: 0.75→0.67 (R:R 1.33→1.49 ≈ minRR 1.50)

### 🔄 In Progress
- [ ] Monitor 1-2 days: verify net PnL improvement after config changes

### 📋 Backlog
- [ ] Commit SL ratio change
- [ ] Analyze trade frequency vs quality tradeoff
- [ ] Consider tradeNotionalCapUsdt adjustment

## Today Stats (2026-09-09)
- Trades: 19 (17W/2L = 89% WR)
- Gross: +$4.77
- Key: ETHUSDT +$3.13, ADAUSDT +$1.03
- Exit: RETRACE_BUDGET all positive ✅

## Config Summary
```
TP: $2.0-$5.0 USDT
SL:TP: 0.67
MinRR: 1.50
FeeMin: $0.20
FeeMult: 2.0x
Leverage: 15x
Margin cap: $15
Notional cap: $100
Bias gate: ON (0.25)
```
