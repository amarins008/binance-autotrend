# Binance AI Copilot (Futures-First)

## What is production-upgraded

- Futures-only symbol guard (`USDT/BUSD` contracts)
- Exchange filter validation (`minQty`, `stepSize`, `tickSize`)
- Auto leverage + margin mode set per order
- Entry + protective TP/SL orders (`TAKE_PROFIT_MARKET`, `STOP_MARKET`)
- Reduce-only close position
- Monitor loop for trigger-based auto entry
- Risk guardrails: kill-switch, max notional, max leverage, max daily loss
- Unified market-intel endpoint (orderflow-first)
- Binance official connector integration (Python) for futures trade execution path

## Run

### Extension

```bash
npm install
npm run dev
```

### Pro Trading Engine (v2)

Layered desk-style engine in `backend/trading/`:

- **Confluence** — multi-TF L/S scoring (EMA, MACD, RSI, VWAP, CVD, EMA200)
- **Regime** — TREND / RANGE / VOLATILE sizing
- **Entry pipeline** — 9 ordered gates with audit trail (`intel.entryPipeline`)
- **Risk** — min R:R 1.5, ATR floor TP/SL, fee-positive edge

PRO config: `backend/autotrade.pro.json`  
Docs: `docs/TRADING_ARCHITECTURE.md`  
API: `GET /trading/playbook`

```bash
python codex_cli.py start --config autotrade.pro.json
```

### Standalone Bot (No Chrome Extension)

```bash
cd backend
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

Run `cmux` control-plane:

```bash
python cmux_service.py serve
```

Start bot via `Codex` CLI (calls `cmux`, which manages `Hermes`):

```bash
python codex_cli.py start --config autotrade.standalone.json
```

Stop bot:

```bash
python codex_cli.py stop --force
```

Status:

```bash
python codex_cli.py status
```

Service-only controls:

```bash
python codex_cli.py service start
python codex_cli.py service status
python codex_cli.py service stop
```

### Professional Dashboard

1. Run cmux:

```bash
python cmux_service.py serve
```

2. Serve dashboard:

```bash
npm run dashboard:serve
```

3. Open:

```text
http://127.0.0.1:8040
```

Dashboard features:
- Start/stop Hermes service
- Start/stop bot with runtime config
- Live KPI (WinRate, RealizedPnL, Trades/hour)
- Active position + recent decision log + scan board
- Learning report + Train Now (from Obsidian trade history)

### Continuous Learning (Obsidian -> Better Next Trades)

- `bot/start` now supports `autoLearn` (default: `true`) to auto-tune entry/risk thresholds from learning history.
- CLI train now:

```bash
python backend/codex_cli.py learning train-now
python backend/codex_cli.py learning report
```

- Scheduler (daily by default in `cmux`):
  - `LEARNING_SCHEDULER_ENABLED=true|false` (default `true`)
  - `LEARNING_TRAIN_INTERVAL_SEC=86400` (default daily)

### Backend

```bash
cd backend
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
uvicorn main:app --reload --port 8000
```

## Important env

- `BINANCE_API_KEY`, `BINANCE_API_SECRET`
- `BINANCE_TESTNET=true`
- `CONNECTOR_MODE=auto|official|legacy`
- `DEFAULT_LEVERAGE`, `DEFAULT_MARGIN_TYPE`
- `DEFAULT_TP_PCT`, `DEFAULT_SL_PCT`
- `KILL_SWITCH`, `MAX_NOTIONAL_USDT`, `MAX_LEVERAGE`, `MAX_DAILY_LOSS_USDT`
- Vision provider (optional):
- `AI_PROVIDER=openai|hermes|off`
- OpenAI mode: `OPENAI_API_KEY`, `OPENAI_VISION_MODEL`
- Hermes mode: `HERMES_BASE_URL`, `HERMES_MODEL`
- AutoTrade Pro:
- `maxSpreadBps` กรองช่วงสเปรดกว้าง
- `maxSlippageBps` กรอง slippage สูง
- `noTradeWindows` ช่วงเวลางดเทรด (รูปแบบ `HH:MM-HH:MM`)
- `trailingStopPct` เปิด trailing stop ปิดกำไรอัตโนมัติ
- `executionMode=PAPER|LIVE` (เริ่มด้วย `PAPER` เพื่อจำลองก่อน)

## API highlights

- `POST /trade`
- `POST /autotrade/start`
- `POST /autotrade/stop`
- `GET /autotrade/status`
- `GET /symbol-meta`
- `POST /strategy/parse`
- `POST /strategy/evaluate`
- `POST /monitor/start`
- `POST /monitor/stop/{monitor_id}`
- `GET /risk-config`
- `POST /risk-config`
- `POST /intel/analyze`

## Trade Error Mapping

`/trade` now returns friendlier `detail` payload for common failures:

- `RISK_KILL_SWITCH`
- `RISK_NOTIONAL_LIMIT`
- `RISK_LEVERAGE_LIMIT`
- `QTY_TOO_SMALL`
- `INSUFFICIENT_MARGIN`
- `ORDER_REJECTED`

## Integration test

Run:

```bash
cd backend
python integration_test.py
```

It verifies:

- `/health`
- `/trade` (WAIT path)
- `/intel/analyze`

## Notes

- Monitor storage is in-memory (reset on backend restart)
- For live mode, use Binance testnet first
- Java connector (`binance-connector-java`) is recommended if you later split execution into a Java microservice.
