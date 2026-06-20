# Recovery Notes

Captured: 2026-06-10

## What Was Recovered

- Current workspace backup was created at `E:/My Project/Binance autotrend-standalone-final-recovery-backup-20260610042135`.
- FastAPI `/openapi.json` was restored to HTTP 200 by repairing the damaged `backend/.venv` metadata and missing Pydantic/FastAPI files from the known-good Forex virtualenv.
- Deleted tracked `__pycache__` files were restored from git so `git status` no longer shows deleted tracked files.
- Runtime route map was saved to `backend/recovery_artifacts/live_openapi_paths.json`.
- Runtime status snapshots were saved to:
  - `backend/recovery_artifacts/status-lite-live.json`
  - `backend/recovery_artifacts/marketcontext-status-live.json`
- MarketContext/Supervisor reference source was recovered from `E:/My Project/Forex autotrade` into this folder:
  - `marketcontext_watcher_reference_from_forex.py`
  - `hermes_agents_reference_from_forex.py`
  - `hermes_service_reference_from_forex.py`
  - `cmux_service_reference_from_forex.py`
  - `test_supervisor_learning_reference_from_forex.py`
  - `test_live_multi_guard_reference_from_forex.py`
- Dashboard access was restored through a safe side server:
  - `backend/dashboard/index.html`
  - `backend/dashboard_server.py`
  - URL: `http://127.0.0.1:8031/`
- `backend/main.py` was partially reconstructed from the live OpenAPI/runtime state and Forex reference code:
  - `/autotrade/config`
  - `/hermes/supervisor-review`
  - `/hermes/supervisor/external-signal`
  - `/hermes/marketcontext/status`
  - `/hermes/marketcontext/refresh`
  - MarketContext MCP target selection, calls, finding generation, entry gate context, and bounded Supervisor tuning.
- `backend/schemas.py` was updated so `AutoTradeStartRequest` accepts MarketContext/Guardian/Supervisor config fields after a restart.

## Still Not Fully Recovered

- `backend/main.py` on disk is the older Binance backup baseline, while the running Hermes process still has newer MarketContext/Supervisor endpoints in memory.
- The disk `backend/main.py` now has the recovered endpoint/entry-gate layer, but it is still not guaranteed to be byte-for-byte equal to the running in-memory Hermes code.
- Do not clean restart Hermes for production trading until the reconstructed source is reviewed against the live route/status artifacts and a controlled dry-run restart is planned.

## Current MarketContext State

- Local `marketcontext-mcp.exe` exists and responds to `--help`.
- Hermes internal watcher still reports `FileNotFoundError`, even after runtime config was pointed at the executable.
- The sidecar path remains the safer working bridge until the internal watcher source is repaired.
