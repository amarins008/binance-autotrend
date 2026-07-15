"""Central runtime state for the autotrade backend."""

from __future__ import annotations

import asyncio
import os


from hermes_agents import new_agent_state as _hermes_new_agent_state


def _new_agent_state() -> dict:
    """Returns a fresh agent state dict with all 12 agents populated."""
    return _hermes_new_agent_state()


MONITORS: dict[str, dict] = {}
MAX_ACTIVE_MONITORS = 50
MONITORS_LOCK = asyncio.Lock()

DAILY_REALIZED_PNL = 0.0
_DAILY_PNL_DATE_KEY = 0
_SNAPSHOT_LAST_FLUSH = 0.0
_LEARN_PROFILES_LAST_FLUSH = 0.0
_LIVE_POSITIONS_CACHE = (0.0, [])
_SUPERVISOR_LAST_REVIEW = 0
_LEARNING_PROFILES_LAST_CLEANUP = 0
_LEARN_PROFILES_BUFFER = None

_GUARDIAN_PREV_INTEL: dict = {}

_AUTOTRADE_TASK: asyncio.Task | None = None
_AUTOTRADE_TASK_LOCK = asyncio.Lock()

AUTO_TRADE: dict = {
    "running": False,
    "manageOpenOnly": False,
    "pauseUntil": 0,
    "riskCooldownLossSignature": "",
    "riskCooldownBySymbol": {},
    "riskCooldownLastMarketCheckAt": 0,
    "perfLocks": {},
    "sessionId": None,
    "startedAt": 0,
    "config": None,
    "lastDecision": None,
    "lastSkip": None,
    "lastTradeAt": 0,
    "trades": [],
    "log": [],
    "consecutiveErrors": 0,
    "liveProfitLocks": {},
    "scanBoard": [],
    "cooldownWatchlist": {},
    "hermesAgents": _new_agent_state(),
    "hermesSupervisorReview": {},
    "paper": {
        "position": None,
        "wins": 0,
        "losses": 0,
        "realizedPnl": 0.0,
        "history": [],
    },
    # ---- Infra-health snapshot (A. shared state for all agents) ----------
    # Populated by services.infra_health.collect(). Read by supervisor,
    # market_analyst, and data quality agent. Refreshed every cycle in
    # trading.autotrade_loop. NOT persisted to snapshot (live-runtime
    # telemetry, not config) — see _save_snapshot in autotrade_loop.py.
    "infraHealth": {},
    "lastCycleAt": 0,
    "_snapshot_saved_at": None,
    "_snapshot_loaded_at": None,
    "_snapshot_recovered_log": None,
    # ---- Diagnostics: error / restart / crash tracking ---------------------
    # Populated by services.diagnostics. See docs and the module docstring
    # there for the full schema. These fields are intentionally additive —
    # older snapshots simply lack them and load() leaves them as defaults.
    "lastError": None,
    "lastErrorAt": 0,
    "lastErrorType": "",
    "lastErrorSource": "",
    "lastErrorContext": {},
    "lastErrorTraceback": "",
    "lastRestartAt": 0,
    "lastRestartReason": "",
    "lastRestartTrigger": "",
    "restartCount": 0,
    "crashCount": 0,
    "uptimeStartAt": 0,
}

RISK: dict = {
    "kill_switch": os.getenv("KILL_SWITCH", "false").lower() == "true",
    "max_notional": float(os.getenv("MAX_NOTIONAL_USDT", "200")),
    "max_leverage": float(os.getenv("MAX_LEVERAGE", "25")),
    "max_daily_loss": float(os.getenv("MAX_DAILY_LOSS_USDT", "50")),
}
