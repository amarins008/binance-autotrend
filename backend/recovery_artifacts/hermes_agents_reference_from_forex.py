from __future__ import annotations

import time
from typing import Any


HERMES_AGENT_PIPELINE = (
    ("market_analyst", "Market Core", "Python reads MT5 ticks/candles and creates XAUUSD signal context"),
    ("strategy_builder", "Strategy Core", "Python validates trend, momentum, confidence, and entry direction"),
    ("portfolio_manager", "Portfolio Core", "Python keeps diversified 3-5 concurrent positions and symbol queue"),
    ("lot_sizing_agent", "Lot Core", "Python calculates lot size and exposure budget"),
    ("risk_manager", "Risk Core", "Python blocks max orders, exposure, drawdown, and unsafe accounts"),
    ("execution_agent", "Execution Core", "Python sends MT5 demo orders and records broker responses"),
    ("position_guardian", "Position Core", "Python monitors open MT5 positions, uPnL, TP, and SL"),
    ("memory_agent", "Memory Core", "Python stores state snapshots and trade reports"),
    ("hermes_supervisor", "Supervisor Core", "Python monitors sub-agent assignments, daily win rate, and auto-tuning"),
)

HERMES_ENGINE_IDENTITY = {
    "id": "hermes_ai_decision_engine",
    "name": "Hermes",
    "title": "Python Trading Core",
    "role": "หัวหน้าระบบ Python เทรด",
    "mission": "ให้ Python core เทรดจริงแบบ deterministic และใช้ Cmux/LLM เฉพาะงาน review, debug, optimize เป็นรอบ",
    "responsibilities": [
        "analyze_data",
        "plan_trades",
        "apply_risk_guards",
        "execute_orders",
        "monitor_positions",
        "record_state",
    ],
}


def _agent_run_day(now: int | None = None) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(now or int(time.time())))


def _normalize_daily_runs(agent: dict[str, Any], now: int | None = None) -> dict[str, Any]:
    today = _agent_run_day(now)
    if str(agent.get("dailyRunDate") or "") != today:
        agent["dailyRunDate"] = today
        agent["dailyRuns"] = 0
    else:
        agent["dailyRuns"] = int(agent.get("dailyRuns", 0) or 0)
    return agent


def new_agent_state() -> dict[str, Any]:
    now = int(time.time())
    today = _agent_run_day(now)
    return {
        "version": "hermes-multi-agent-1",
        "dailyCounterDate": today,
        "engine": dict(HERMES_ENGINE_IDENTITY),
        "updatedAt": now,
        "kanban": {
            "todo": [agent_id for agent_id, _name, _role in HERMES_AGENT_PIPELINE],
            "doing": [],
            "done": [],
            "blocked": [],
        },
        "agents": {
            agent_id: {
                "id": agent_id,
                "name": name,
                "role": role,
                "state": "todo",
                "lastAction": "waiting",
                "lastReason": "",
                "updatedAt": now,
                "runs": 0,
                "dailyRuns": 0,
                "dailyRunDate": today,
            }
            for agent_id, name, role in HERMES_AGENT_PIPELINE
        },
    }


def ensure_agent_state(state: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(state, dict) or not isinstance(state.get("agents"), dict):
        return new_agent_state()
    template = new_agent_state()
    agents = state.setdefault("agents", {})
    today = _agent_run_day()
    reset_daily_counters = str(state.get("dailyCounterDate") or "") != today
    active_ids = set(template["agents"])
    for agent_id in list(agents):
        if agent_id not in active_ids:
            agents.pop(agent_id, None)
    for agent_id, agent in template["agents"].items():
        if not isinstance(agents.get(agent_id), dict):
            agents[agent_id] = agent
        else:
            agents[agent_id].setdefault("id", agent_id)
            agents[agent_id].setdefault("name", agent["name"])
            agents[agent_id].setdefault("role", agent["role"])
            agents[agent_id].setdefault("state", "todo")
            agents[agent_id].setdefault("lastAction", "waiting")
            agents[agent_id].setdefault("lastReason", "")
            agents[agent_id].setdefault("updatedAt", int(time.time()))
            agents[agent_id].setdefault("runs", 0)
            if reset_daily_counters:
                agents[agent_id]["dailyRunDate"] = today
                agents[agent_id]["dailyRuns"] = 0
            else:
                _normalize_daily_runs(agents[agent_id])
    state["dailyCounterDate"] = today
    state["version"] = state.get("version") or template["version"]
    engine = state.get("engine")
    if not isinstance(engine, dict):
        state["engine"] = dict(HERMES_ENGINE_IDENTITY)
    else:
        for key, value in HERMES_ENGINE_IDENTITY.items():
            engine.setdefault(key, value)
    rebuild_kanban(state)
    return state


def rebuild_kanban(state: dict[str, Any]) -> dict[str, Any]:
    kanban = {"todo": [], "doing": [], "done": [], "blocked": []}
    agents = state.get("agents") if isinstance(state.get("agents"), dict) else {}
    for agent_id, _name, _role in HERMES_AGENT_PIPELINE:
        agent = agents.get(agent_id) if isinstance(agents.get(agent_id), dict) else {}
        bucket = str(agent.get("state", "todo") or "todo")
        if bucket not in kanban:
            bucket = "todo"
        kanban[bucket].append(agent_id)
    state["kanban"] = kanban
    state["updatedAt"] = int(time.time())
    return state


def mark_agent(
    state: dict[str, Any] | None,
    agent_id: str,
    stage: str,
    action: str,
    reason: str = "",
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = ensure_agent_state(state)
    agents = state["agents"]
    if agent_id not in agents:
        return state
    now = int(time.time())
    bucket = stage if stage in ("todo", "doing", "done", "blocked") else "todo"
    agent = agents[agent_id]
    agent["state"] = bucket
    agent["lastAction"] = str(action)[:180]
    agent["lastReason"] = str(reason)[:240]
    agent["updatedAt"] = now
    _normalize_daily_runs(agent, now)
    if bucket == "done":
        agent["runs"] = int(agent.get("runs", 0) or 0) + 1
        agent["dailyRuns"] = int(agent.get("dailyRuns", 0) or 0) + 1
    if isinstance(data, dict):
        agent["data"] = data
    else:
        agent.pop("data", None)
    return rebuild_kanban(state)


def start_cycle(state: dict[str, Any] | None) -> dict[str, Any]:
    state = ensure_agent_state(state)
    now = int(time.time())
    for agent in state["agents"].values():
        agent["state"] = "todo"
        agent["lastAction"] = "waiting"
        agent["lastReason"] = ""
        agent.pop("data", None)
        agent["updatedAt"] = now
    return rebuild_kanban(state)
