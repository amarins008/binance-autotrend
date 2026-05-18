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
