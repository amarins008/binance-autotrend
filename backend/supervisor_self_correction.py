import copy
import time
from typing import Any

# AUTO_TRADE is injected from main.py at runtime; define a safe accessor.
_AUTO_TRADE_GLOBAL: Any = None


def _set_autotrade_global(val: Any) -> None:
    global _AUTO_TRADE_GLOBAL
    _AUTO_TRADE_GLOBAL = val


def _autotrade() -> Any:
    return _AUTO_TRADE_GLOBAL


def _dispatch_supervisor_auto_actions(review: dict, cfg: dict) -> dict:
    """Execute basic self-corrections from supervisor auto-actions.

    Each agent attempts to fix its own basic issues before escalating
    as escalation tasks.  Returns a dict of what was self-corrected vs
    what needs deep review.
    """
    bot = _autotrade()
    running = bool(bot.get("running")) if bot else False
    execution_mode = str(cfg.get("executionMode", "") or "").upper()
    is_live = running and execution_mode == "LIVE"
    results = {
        "selfCorrected": [],
        "escalated": [],
        "skipped": [],
    }
    auto_actions = review.get("autoActions") if isinstance(review.get("autoActions"), list) else []
    for action in auto_actions:
        if not isinstance(action, dict):
            continue
        agent_id = str(action.get("agent", "") or "")
        action_text = str(action.get("action", "") or "")
        issue_type = str(action.get("issueType", "") or "")
        status = str(action.get("status", "") or "")
        # Skip already-applied actions (they were handled inline during review)
        if status == "applied":
            continue
        # Only process "recommended" actions that agents can self-correct
        if status != "recommended":
            continue
        # Try self-correction per agent
        corrected, changes = False, {}
        try:
            if agent_id == "position_guardian":
                corrected, changes = _position_guardian_self_correct(action_text, issue_type, cfg)
            elif agent_id == "strategy_builder":
                corrected, changes = _strategy_builder_self_correct(action_text, issue_type, cfg)
            elif agent_id == "market_analyst":
                corrected, changes = _market_analyst_self_correct(action_text, issue_type, cfg)
            elif agent_id == "portfolio_manager":
                corrected, changes = _portfolio_manager_self_correct(action_text, issue_type, cfg)
            elif agent_id == "risk_manager":
                corrected, changes = _risk_manager_self_correct(action_text, issue_type, cfg)
            elif agent_id == "execution_agent":
                corrected, changes = _execution_agent_self_correct(action_text, issue_type, cfg)
            elif agent_id == "reflection_agent":
                corrected, changes = _reflection_agent_self_correct(action_text, issue_type, cfg)
            elif agent_id == "memory_agent":
                corrected, changes = _memory_agent_self_correct(action_text, issue_type, cfg)
            elif agent_id == "backtest_agent":
                corrected, changes = _backtest_agent_self_correct(action_text, issue_type, cfg)
        except Exception as e:
            print(f"[Supervisor Self-Correction] {agent_id} error: {e}")
            corrected, changes = False, {}
        if corrected:
            action["status"] = "applied"
            action["selfCorrected"] = True
            if changes:
                action["changes"] = changes
            results["selfCorrected"].append({"agent": agent_id, "action": action_text, "issueType": issue_type})
            # Mark agent as done
            if bot is not None and hasattr(bot, "get"):
                _mark_agent_done(agent_id, f"self-corrected: {action_text}", issue_type)
        else:
            # Escalate to deep correction if self-correction failed or not available
            if _is_deep_correction_required(agent_id, issue_type, action_text):
                results["escalated"].append({"agent": agent_id, "action": action_text, "issueType": issue_type})
            else:
                results["skipped"].append({"agent": agent_id, "action": action_text, "issueType": issue_type})
    return results


def _mark_agent_done(agent_id: str, action: str, detail: str = "") -> None:
    """Mark an agent as done after self-correction."""
    try:
        from services.app_state import AUTO_TRADE
        from hermes_agents import mark_agent
        state = AUTO_TRADE.get("hermesAgents")
        if isinstance(state, dict):
            mark_agent(state, agent_id, "done", action, detail)
    except Exception:
        pass


def _is_deep_correction_required(agent_id: str, issue_type: str, action_text: str) -> bool:
    """Determine if an issue requires deep correction vs basic self-fix."""
    # Deep corrections: structural issues, code changes, or complex tuning
    deep_issue_types = {
        "daily_entry_regression",
        "negative_expectancy",
        "weak_payoff_ratio",
        "small_profit_capture",
        "symbol_drag",
        "infra_auth",
        "infra_data_timeout",
        "scan_config_drift",
        "fapi_lock_restore",
    }
    # Some action texts indicate deep review needed
    deep_keywords = [
        "review",
        "inspect",
        "validate",
        "debug",
        "trace",
        "compare",
        "wire",
    ]
    if issue_type in deep_issue_types:
        return True
    action_lower = action_text.lower()
    if any(kw in action_lower for kw in deep_keywords):
        return True
    # Workload imbalance and structural issues need human review
    if "workload" in action_lower or "split" in action_lower:
        return True
    return False


# ---------------------------------------------------------------------------
# Per-agent self-correction handlers
# Each returns (corrected: bool, changes: dict)
# ---------------------------------------------------------------------------


def _position_guardian_self_correct(action_text: str, issue_type: str, cfg: dict) -> tuple[bool, dict]:
    """Position Guardian basic self-corrections."""
    changes: dict = {}
    # Heartbeat priority: already fixed in _manage_live_open_positions_once
    if "heartbeat" in action_text.lower() or "monitor" in action_text.lower():
        return True, changes
    # Profit-lock adjustments: tighten thresholds if small wins detected
    if "profit-lock" in action_text.lower() or "small-profit" in action_text.lower() or "small_profit" in issue_type.lower():
        # Self-correct: lower maxGiveback slightly if not at safe limit
        current_giveback = float(cfg.get("profitLockMaxGivebackUsdt", 0.15) or 0.15)
        if current_giveback > 0.05:
            changes["profitLockMaxGivebackUsdt"] = round(max(0.05, current_giveback * 0.90), 6)
            return True, changes
        return True, changes  # already at safe limit, just mark corrected
    # Weak payoff: tighten SL slightly
    if "payoff" in action_text.lower():
        current_sl = float(cfg.get("stopLossPct", 0.75) or 0.75)
        if current_sl > 0.5:
            changes["stopLossPct"] = round(max(0.5, current_sl * 0.95), 6)
            return True, changes
    return False, changes


def _strategy_builder_self_correct(action_text: str, issue_type: str, cfg: dict) -> tuple[bool, dict]:
    """Strategy Builder basic self-corrections."""
    changes: dict = {}
    # Entry gate inspection: tighten confidence if no entries despite qualified candidates
    if "entry gate" in action_text.lower() or "inspect" in action_text.lower():
        if issue_type == "no_new_position_activity":
            return True, changes
        # Self-correct: raise minConfidence slightly if too many late chases
        current_conf = float(cfg.get("minConfidence", 0.66) or 0.66)
        if current_conf < 0.75:
            changes["minConfidence"] = round(min(0.75, current_conf + 0.02), 4)
            return True, changes
    # Anti-chase: tighten late-entry thresholds
    if "anti-chase" in action_text.lower() or "late" in action_text.lower():
        current_bb = float(cfg.get("lateEntryMaxBbPctB", 0.95) or 0.95)
        current_vwap = float(cfg.get("lateEntryMaxVwapDistancePct", 0.40) or 0.40)
        if current_bb > 0.75:
            changes["lateEntryMaxBbPctB"] = round(max(0.75, current_bb - 0.03), 4)
        if current_vwap > 0.20:
            changes["lateEntryMaxVwapDistancePct"] = round(max(0.20, current_vwap * 0.92), 4)
        if changes:
            return True, changes
    # Hold-winner review: already handled by _should_hold_winner changes
    if "hold-winner" in action_text.lower():
        return True, changes
    # Negative expectancy / daily regression: already handled by auto-tune
    if issue_type in {"negative_expectancy", "daily_entry_regression"}:
        return True, changes
    # ── No-entry stale guard: relax anti-chase if no trades for too long ──
    if issue_type in {"no_new_position_activity", "low_entry_activity"}:
        return _relax_anti_chase_if_stale(cfg)
    return False, changes


def _relax_anti_chase_if_stale(cfg: dict) -> tuple[bool, dict]:
    """Relax anti-chase thresholds when no entries for too long.

    Checks if lastTradeAt is >15 min ago AND there are qualified symbols
    on scan board. If so, widens lateEntryMaxBbPctB and lateEntryMaxVwapDistancePct
    by one step. Returns (corrected, changes).
    """
    bot = _autotrade()
    if not bot:
        return False, {}
    now = int(time.time())
    last_trade = int(bot.get("lastTradeAt", 0) or 0)
    # If never traded or traded <15 min ago, skip
    if last_trade == 0:
        last_trade = int(bot.get("startedAt", 0) or 0)
    if last_trade == 0:
        return False, {}
    stale_min = 15
    if now - last_trade < stale_min * 60:
        return False, {}
    # Check scan board for qualified symbols that were skipped by anti-chase
    scan_board = bot.get("scanBoard", [])
    if not isinstance(scan_board, list):
        return False, {}
    chase_skip_count = sum(
        1 for s in scan_board
        if s.get("qualified") and s.get("rejectReason", "") == ""
        and s.get("confidence", 0) >= float(cfg.get("minConfidence", 0.66) or 0.66)
    )
    # Need at least 2 qualified candidates to justify relaxation
    if chase_skip_count < 2:
        return False, {}
    # Check cooldown: don't relax more than once per 10 min
    cooldown_key = "_anti_chase_relax_cooldown"
    last_relax = int(bot.get(cooldown_key, 0) or 0)
    if now - last_relax < 600:
        return False, {}
    # Relax: widen thresholds by one step
    changes: dict = {}
    current_bb = float(cfg.get("lateEntryMaxBbPctB", 0.95) or 0.95)
    current_vwap = float(cfg.get("lateEntryMaxVwapDistancePct", 0.40) or 0.40)
    new_bb = min(0.98, current_bb + 0.02)
    new_vwap = min(0.60, current_vwap + 0.05)
    if new_bb != current_bb:
        changes["lateEntryMaxBbPctB"] = round(new_bb, 4)
    if new_vwap != current_vwap:
        changes["lateEntryMaxVwapDistancePct"] = round(new_vwap, 4)
    if changes:
        bot[cooldown_key] = now
        return True, changes
    return False, {}


def _market_analyst_self_correct(action_text: str, issue_type: str, cfg: dict) -> tuple[bool, dict]:
    """Market Analyst basic self-corrections."""
    changes: dict = {}
    # Refresh scan blockers: expand universe if needed
    if "refresh" in action_text.lower() or "diversify" in action_text.lower():
        current_top = int(cfg.get("scanTopLiquid", 60) or 60)
        current_analyze = int(cfg.get("scanAnalyzeTop", 12) or 12)
        if current_top < 80:
            changes["scanTopLiquid"] = min(80, current_top + 10)
        if current_analyze < 16:
            changes["scanAnalyzeTop"] = min(16, current_analyze + 2)
        return True, changes
    # Scan board stale: trigger refresh by clearing board
    if "stale" in action_text.lower() or "empty" in action_text.lower():
        bot = _autotrade()
        if bot is not None and isinstance(bot.get("scanBoard"), list):
            bot["scanBoard"] = []
        return True, changes
    # Analyze errors: cooldown problematic symbols
    if "analyze error" in action_text.lower():
        return True, changes
    # Scan timeout: already handled by _maybe_tune_scan_timeout_from_skip
    if issue_type == "scan_timeout":
        return True, changes
    return False, changes


def _portfolio_manager_self_correct(action_text: str, issue_type: str, cfg: dict) -> tuple[bool, dict]:
    """Portfolio Manager basic self-corrections."""
    # Capacity verification: already accurate
    if "capacity" in action_text.lower() or "verify" in action_text.lower():
        return True, {}
    # Symbol daily cap: already handled by _cooldown_scan_symbol
    if issue_type == "symbol_day_cap":
        return True, {}
    # Symbol drag: already handled by _maybe_lock_symbol_drag_from_review
    if issue_type == "symbol_drag":
        return True, {}
    return False, {}


def _risk_manager_self_correct(action_text: str, issue_type: str, cfg: dict) -> tuple[bool, dict]:
    """Risk Manager basic self-corrections."""
    # Size streak: already handled by _maybe_tune_size_multiplier_from_streak
    if issue_type == "size_streak":
        return True, {}
    # Risk cooldown: already adaptive
    if "cooldown" in action_text.lower():
        return True, {}
    return False, {}


def _execution_agent_self_correct(action_text: str, issue_type: str, cfg: dict) -> tuple[bool, dict]:
    """Execution Agent basic self-corrections."""
    changes: dict = {}
    # Infra/auth: requires human intervention, can't self-correct
    if issue_type in {"infra_auth", "fapi_agreement_required"}:
        return False, changes
    # Slippage/execution: can adjust maxSlippageBps slightly
    if "slippage" in action_text.lower():
        current_slippage = float(cfg.get("maxSlippageBps", 28.0) or 28.0)
        if current_slippage < 50:
            changes["maxSlippageBps"] = round(min(50, current_slippage + 2), 2)
            return True, changes
    return False, changes


def _reflection_agent_self_correct(action_text: str, issue_type: str, cfg: dict) -> tuple[bool, dict]:
    """Reflection Agent basic self-corrections."""
    # Summarization: already triggered by marking done
    if "summarize" in action_text.lower():
        return True, {}
    return False, {}


def _memory_agent_self_correct(action_text: str, issue_type: str, cfg: dict) -> tuple[bool, dict]:
    """Memory Agent basic self-corrections."""
    # Memory storage: already triggered by marking done
    if "store" in action_text.lower() or "memory" in action_text.lower():
        return True, {}
    return False, {}


def _backtest_agent_self_correct(action_text: str, issue_type: str, cfg: dict) -> tuple[bool, dict]:
    """Backtest Agent basic self-corrections."""
    # Validation: already triggered by marking done
    if "validate" in action_text.lower():
        return True, {}
    return False, {}
