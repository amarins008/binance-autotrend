from __future__ import annotations

import time
from datetime import datetime, timezone
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


_AGENT_STATE_TEMPLATE: dict[str, Any] | None = None


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
        "cycle": 0,
        "cycleStartedAt": now,
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
                "starts": 0,
                "completions": 0,
                "startedAt": 0,
                "completedAt": 0,
                "blockedAt": 0,
                "lastCompletedAction": "",
            }
            for agent_id, name, role in HERMES_AGENT_PIPELINE
        },
    }


def _agent_state_template() -> dict[str, Any]:
    """Memoized agent-state template to avoid rebuilding on every call."""
    global _AGENT_STATE_TEMPLATE
    if _AGENT_STATE_TEMPLATE is None:
        _AGENT_STATE_TEMPLATE = new_agent_state()
    return _AGENT_STATE_TEMPLATE


def ensure_agent_state(state: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(state, dict) or not isinstance(state.get("agents"), dict):
        return new_agent_state()
    template = _agent_state_template()
    agents = state.setdefault("agents", {})

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    last_reset = state.get("_dailyResetDate")
    need_daily_reset = last_reset != today_str

    for agent_id, agent in template["agents"].items():
        if not isinstance(agents.get(agent_id), dict):
            agents[agent_id] = dict(agent)
        else:
            existing = agents[agent_id]
            existing.setdefault("id", agent_id)
            existing.setdefault("name", agent["name"])
            existing.setdefault("role", agent["role"])
            existing.setdefault("playbookPath", agent.get("playbookPath", ""))
            existing.setdefault("state", "todo")
            existing.setdefault("lastAction", "waiting")
            existing.setdefault("lastReason", "")
            existing.setdefault("updatedAt", int(time.time()))
            existing.setdefault("runs", 0)
            existing.setdefault("starts", 0)
            existing.setdefault("completions", int(existing.get("runs", 0) or 0))
            existing.setdefault("startedAt", 0)
            existing.setdefault("completedAt", 0)
            existing.setdefault("blockedAt", 0)
            existing.setdefault("lastCompletedAction", "")

            if need_daily_reset:
                existing["runs"] = 0
                existing["starts"] = 0
                existing["completions"] = 0
                existing["lastStartedCycle"] = -1
                existing["lastCompletedCycle"] = -1

    if need_daily_reset:
        state["_dailyResetDate"] = today_str

    state["version"] = state.get("version") or template["version"]
    state.setdefault("cycle", 0)
    state.setdefault("cycleStartedAt", int(time.time()))
    engine = state.get("engine")
    if not isinstance(engine, dict):
        state["engine"] = dict(HERMES_ENGINE_IDENTITY)
    else:
        for key, value in HERMES_ENGINE_IDENTITY.items():
            engine.setdefault(key, value)
    # Kanban is rebuilt by mark_agent / rebuild_kanban only when state actually changes.
    # Avoid redundant rebuild here since ensure_agent_state is called many times per cycle.
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

def move_agent_in_kanban(state: dict[str, Any], agent_id: str, from_bucket: str, to_bucket: str) -> dict[str, Any]:
    """Move agent between buckets without full rebuild (O(1) vs O(n))."""
    kanban = state.setdefault("kanban", {"todo": [], "doing": [], "done": [], "blocked": []})
    
    # Remove from old bucket
    if from_bucket in kanban and agent_id in kanban[from_bucket]:
        kanban[from_bucket].remove(agent_id)
    
    # Add to new bucket
    if to_bucket in kanban:
        kanban[to_bucket].append(agent_id)
    
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
    # Track if state actually changed for lazy rebuild
    old_state = str(agent.get("state", "todo") or "todo")
    old_action = str(agent.get("lastAction", "") or "")
    state_changed = (old_state != bucket or old_action != next_action)
    agent["state"] = bucket
    agent["lastAction"] = next_action
    agent["lastReason"] = next_reason
    agent["updatedAt"] = now
    agent["runs"] = int(agent.get("runs", 0) or 0) + (1 if bucket == "done" else 0)
    cycle = int(state.get("cycle", 0) or 0)
    if bucket == "doing":
        agent["startedAt"] = now
        if int(agent.get("lastStartedCycle", -1) or -1) != cycle:
            agent["starts"] = int(agent.get("starts", 0) or 0) + 1
            agent["lastStartedCycle"] = cycle
    elif bucket == "done":
        agent["completedAt"] = now
        agent["lastCompletedAction"] = next_action
        if int(agent.get("lastCompletedCycle", -1) or -1) != cycle:
            agent["completions"] = int(agent.get("completions", 0) or 0) + 1
            agent["lastCompletedCycle"] = cycle
    elif bucket == "blocked":
        agent["blockedAt"] = now
    if isinstance(data, dict):
        agent["data"] = data
    # Only rebuild kanban if state actually changed
    if state_changed:
        return rebuild_kanban(state)
    return state

def batch_mark_agents(state: dict[str, Any], updates: list[tuple[str, str, str, str, dict[str, Any] | None]]) -> dict[str, Any]:
    """Update multiple agents at once and rebuild kanban once (reduces rebuild frequency)."""
    state = ensure_agent_state(state)
    agents = state["agents"]
    now = int(time.time())
    cycle = int(state.get("cycle", 0) or 0)
    
    for agent_id, stage, action, reason, data in updates:
        if agent_id not in agents:
            continue
        
        bucket = stage if stage in ("todo", "doing", "done", "blocked") else "todo"
        agent = agents[agent_id]
        next_action = str(action)[:180]
        next_reason = str(reason)[:240]
        
        # Check dedupe
        same_event = (
            str(agent.get("state", "todo") or "todo") == bucket
            and str(agent.get("lastAction", "") or "") == next_action
            and str(agent.get("lastReason", "") or "") == next_reason
            and _same_agent_data(agent.get("data"), data)
        )
        last_update = int(agent.get("updatedAt", 0) or 0)
        if same_event and last_update > 0 and now - last_update < AGENT_MARK_DEDUPE_SEC:
            continue
        
        # Update agent state
        agent["state"] = bucket
        agent["lastAction"] = next_action
        agent["lastReason"] = next_reason
        agent["updatedAt"] = now
        agent["runs"] = int(agent.get("runs", 0) or 0) + (1 if bucket == "done" else 0)
        
        if bucket == "doing":
            agent["startedAt"] = now
            if int(agent.get("lastStartedCycle", -1) or -1) != cycle:
                agent["starts"] = int(agent.get("starts", 0) or 0) + 1
                agent["lastStartedCycle"] = cycle
        elif bucket == "done":
            agent["completedAt"] = now
            agent["lastCompletedAction"] = next_action
            if int(agent.get("lastCompletedCycle", -1) or -1) != cycle:
                agent["completions"] = int(agent.get("completions", 0) or 0) + 1
                agent["lastCompletedCycle"] = cycle
        elif bucket == "blocked":
            agent["blockedAt"] = now
        
        if isinstance(data, dict):
            agent["data"] = data
    
    # Rebuild kanban once after all updates
    return rebuild_kanban(state)


def start_cycle(state: dict[str, Any] | None) -> dict[str, Any]:
    state = ensure_agent_state(state)
    now = int(time.time())
    state["cycle"] = int(state.get("cycle", 0) or 0) + 1
    state["cycleStartedAt"] = now
    for agent in state["agents"].values():
        # A prior cycle cannot still be executing. Preserve its last completed
        # or blocked result so operators can see what happened between cycles.
        if str(agent.get("state", "todo") or "todo") == "doing":
            agent["state"] = "todo"
            agent["lastAction"] = "waiting"
            agent["lastReason"] = ""
            agent.pop("data", None)
            agent["updatedAt"] = now
    return rebuild_kanban(state)
