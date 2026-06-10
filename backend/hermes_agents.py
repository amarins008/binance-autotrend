from __future__ import annotations

import time
from typing import Any


AGENT_MARK_DEDUPE_SEC = 20


HERMES_AGENT_PIPELINE = (
    ("hermes_supervisor", "Hermes Supervisor", "Control-plane governor, agent cadence, delegation policy"),
    ("market_analyst", "Market Analyst", "Market scan, momentum, symbol ranking"),
    ("data_quality_guard", "Data Quality Guard", "Data freshness, missing fields, stale/analyze-error protection"),
    ("news_sentiment_guard", "News Sentiment Guard", "News/event risk as guard-only; never opens trades"),
    ("risk_manager", "Risk Manager", "Risk gates, cooldowns, exposure limits"),
    ("portfolio_manager", "Portfolio Manager", "Portfolio capacity, position count, symbol/day caps"),
    ("position_guardian", "Position Guardian", "Open-position monitoring, profit lock, reversal protection"),
    ("strategy_builder", "Strategy Builder", "Signal shaping, entry rules, session bias"),
    ("backtest_agent", "Backtest Agent", "Learning report and walk-forward validation"),
    ("execution_agent", "Execution Agent", "Order placement and exchange responses"),
    ("reflection_agent", "Reflection Agent", "Loss-streak review and self-tuning"),
    ("memory_agent", "Memory Agent", "Snapshots, logs, learning profiles"),
)

HERMES_AGENT_PLAYBOOKS = {
    agent_id: f"docs/hermes_agents/{agent_id}.md"
    for agent_id, _name, _role in HERMES_AGENT_PIPELINE
}

HERMES_ENGINE_IDENTITY = {
    "id": "hermes_ai_decision_engine",
    "name": "Hermes",
    "title": "AI Decision Engine",
    "role": "หัวหน้าทีมเทรด",
    "mission": "วิเคราะห์ วางแผน เรียนรู้ ตั้ง hypothesis สรุปความผิดพลาด และ optimize strategy",
    "responsibilities": [
        "analyze_data",
        "plan_trades",
        "learn_from_results",
        "form_hypotheses",
        "review_failures",
        "optimize_strategy",
    ],
}


def _same_agent_data(left: Any, right: Any) -> bool:
    if not isinstance(left, dict) and not isinstance(right, dict):
        return True
    if not isinstance(left, dict) or not isinstance(right, dict):
        return False
    return left == right


def new_agent_state() -> dict[str, Any]:
    now = int(time.time())
    return {
        "version": "hermes-multi-agent-1",
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
                "playbookPath": HERMES_AGENT_PLAYBOOKS.get(agent_id, ""),
                "state": "todo",
                "lastAction": "waiting",
                "lastReason": "",
                "updatedAt": now,
                "runs": 0,
            }
            for agent_id, name, role in HERMES_AGENT_PIPELINE
        },
    }


def ensure_agent_state(state: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(state, dict) or not isinstance(state.get("agents"), dict):
        return new_agent_state()
    template = new_agent_state()
    agents = state.setdefault("agents", {})
    for agent_id, agent in template["agents"].items():
        if not isinstance(agents.get(agent_id), dict):
            agents[agent_id] = agent
        else:
            agents[agent_id].setdefault("id", agent_id)
            agents[agent_id].setdefault("name", agent["name"])
            agents[agent_id].setdefault("role", agent["role"])
            agents[agent_id].setdefault("playbookPath", agent.get("playbookPath", ""))
            agents[agent_id].setdefault("state", "todo")
            agents[agent_id].setdefault("lastAction", "waiting")
            agents[agent_id].setdefault("lastReason", "")
            agents[agent_id].setdefault("updatedAt", int(time.time()))
            agents[agent_id].setdefault("runs", 0)
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
    next_action = str(action)[:180]
    next_reason = str(reason)[:240]
    same_event = (
        str(agent.get("state", "todo") or "todo") == bucket
        and str(agent.get("lastAction", "") or "") == next_action
        and str(agent.get("lastReason", "") or "") == next_reason
        and _same_agent_data(agent.get("data"), data)
    )
    last_update = int(agent.get("updatedAt", 0) or 0)
    if same_event and last_update > 0 and now - last_update < AGENT_MARK_DEDUPE_SEC:
        return state
    agent["state"] = bucket
    agent["lastAction"] = next_action
    agent["lastReason"] = next_reason
    agent["updatedAt"] = now
    agent["runs"] = int(agent.get("runs", 0) or 0) + (1 if bucket == "done" else 0)
    if isinstance(data, dict):
        agent["data"] = data
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
