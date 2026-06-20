# Pro Trading Engine (v2)

## Design

The bot follows a **desk-style pipeline**: data → confluence → regime → gates → risk → execution → position management.

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌────────────────┐
│ Market Data │───▶│  Confluence  │───▶│   Regime    │───▶│ Entry Pipeline │
│ klines/book │    │ L/S scoring  │    │ TREND/RANGE │    │ 9 ordered gates│
└─────────────┘    └──────────────┘    └─────────────┘    └───────┬────────┘
                                                                    │
                    ┌──────────────┐    ┌─────────────┐             ▼
                    │  Guardian    │◀───│  Execution  │◀─── approved EntryPlan
                    │ trail/lock   │    │ LIVE/PAPER  │
                    └──────────────┘    └─────────────┘
```

## Package layout

| Module | Role |
|--------|------|
| `trading/confluence.py` | Multi-TF indicator scoring → signal |
| `trading/regime.py` | Vol/trend/chop classification |
| `trading/risk.py` | R:R, ATR TP/SL, fee edge |
| `trading/position.py` | Hold winner, early cut, trail |
| `trading/pipeline.py` | Entry gates + audit trail |
| `trading/config.py` | PRO defaults merge |
| `trading/presets.py` | `autotrade.pro.json` source preset |

## Entry gates (order)

1. **signal** — LONG/SHORT only  
2. **spread** — regime-adjusted max bps  
3. **confidence** — adaptive floor (learning + loss streak)  
4. **vision** — optional consensus  
5. **htf** — 15m/1h EMA alignment; no chase at BB extremes in TREND  
6. **ema200** — no counter-macro entries  
7. **funding** — skip when rate fights position  
8. **slippage** — mark vs bid/ask  
9. **risk_reward** — min R:R (default 1.5)  
10. **fee_edge** — net profit after costs  

Each cycle stores `intel.entryPipeline` for dashboard/debug.

## Config

- **PRO preset file:** `backend/autotrade.pro.json`  
- **Start bot:** `python cmux_cli.py start --config autotrade.pro.json`  
- **Playbook API:** `GET /trading/playbook`

## Philosophy

- **Trade less** — chop filter, pair lock, max trades/hour  
- **Align more** — HTF + EMA200 + confluence edge  
- **Size smart** — vol targeting, regime multiplier  
- **Protect capital** — R:R floor, ATR stops, profit lock on LIVE  
