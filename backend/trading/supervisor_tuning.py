"""Supervisor auto-tuning helpers."""

from __future__ import annotations

import math
import time

from services import app_state
from trading.supervisor_state import (
    _commit_supervisor_config_tune,
    _supervisor_delegation_cooldown,
    _tuning_rollback_last,
    _tuning_should_rollback,
    _tuning_signature,
    _tuning_mode_lock_acquire,
    _tuning_mode_lock_release,
)

AUTO_TRADE = app_state.AUTO_TRADE


def _main():
    import main as m
    return m


def _maybe_tune_external_signal_guard(payload: dict | None, cfg: dict | None = None) -> dict:
    """No-op stub: external MCP signal guard was removed.

    Kept as a thin shim so existing call sites (e.g. /hermes/supervisor/external-signal)
    do not break. Any payload is acknowledged but not acted upon.
    """
    return {"applied": False, "reason": "external_mcp_removed"}

def _maybe_tune_low_entry_activity(reason: str, cfg: dict | None = None, board: list[dict] | None = None) -> dict:
    if not isinstance(cfg, dict):
        return {}
    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("low_entry_activity", cfg, 30)

    if _tuning_should_rollback("low_entry_activity"):
        rollback = _tuning_rollback_last("low_entry_activity")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state, delegations, "low_entry_activity", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            _tuning_mode_lock_release()  # Rollback clears the lock
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    # Acquire loosening mode lock — if tightener recently fired, skip
    if not _tuning_mode_lock_acquire("loosening", f"low_entry:{reason}", cfg):
        return {"applied": False, "reason": "opposite_mode_active", "mode": "loosening", "blockedBy": "tightening"}

    changes: dict[str, dict] = {}

    def set_float(key: str, value: float, digits: int = 3):
        old = float(cfg.get(key, value) or value)
        new = round(float(value), digits)
        if abs(old - new) >= 1e-9:
            cfg[key] = new
            changes[key] = {"old": round(old, digits), "new": new}

    def set_int(key: str, value: int):
        old = int(cfg.get(key, value) or value)
        new = int(value)
        if old != new:
            cfg[key] = new
            changes[key] = {"old": old, "new": new}

    board_rows = [row for row in (board or []) if isinstance(row, dict)]
    reject_counts: dict[str, int] = {}
    momentum_values: list[float] = []
    max_spread_bps = float(cfg.get("maxSpreadBps", 16.0) or 16.0)
    recovery_relax = max(0.0, min(0.04, float(cfg.get("supervisorStuckLowConfRelax", 0.04) or 0.04)))
    near_miss_rows: list[dict] = []
    for row in board_rows:
        rr = str(row.get("rejectReason", "") or "unknown")
        reject_counts[rr] = reject_counts.get(rr, 0) + 1
        try:
            momentum_values.append(abs(float(row.get("momentumPct", 0.0) or 0.0)))
        except Exception:
            pass
        if rr == "low_conf":
            try:
                conf = float(row.get("confidence", 0.0) or 0.0)
                adaptive_min = float(row.get("adaptiveMinConf", cfg.get("minConfidence", 0.62)) or cfg.get("minConfidence", 0.62))
                spread = float(row.get("spreadBps", 999.0) or 999.0)
            except Exception:
                continue
            if conf > 0.0 and 0.0 <= (adaptive_min - conf) <= recovery_relax and spread <= max_spread_bps:
                near_miss_rows.append(row)
    stuck_no_entry = (
        bool(AUTO_TRADE.get("running"))
        and str((cfg or {}).get("executionMode", "") or "").upper() == "LIVE"
        and int(AUTO_TRADE.get("tradesLastHour", 0) or 0) == 0
        and not AUTO_TRADE.get("openLivePositions")
        and str((AUTO_TRADE.get("lastSkip") or {}).get("code", "") or reason or "") in {"scan_none", "low_entry_activity"}
        and bool(near_miss_rows)
    )
    if active and not stuck_no_entry:
        return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec}
    if active and stuck_no_entry:
        rec = delegations.get("low_entry_activity") if isinstance(delegations.get("low_entry_activity"), dict) else {}
        recovery_cooldown_sec = max(60, int(cfg.get("supervisorStuckLowEntryRecoveryCooldownSec", 300) or 300))
        if (
            str(rec.get("reason", "") or "") == "stuck_low_entry_recovery"
            and int(time.time()) - int(rec.get("at", 0) or 0) < recovery_cooldown_sec
        ):
            return {
                "applied": False,
                "alreadyTuned": True,
                "cooldownSec": recovery_cooldown_sec,
                "stuckRecovery": True,
            }
    quiet_board = bool(board_rows) and (
        (momentum_values and max(momentum_values) < 0.35)
        or all(str(row.get("rejectReason", "") or "") in {"signal_wait", "low_conf", "perf_lock", ""} for row in board_rows)
    )

    # Adaptive severity based on tradesLastHour=0 duration
    no_entry_metric = 0.0 if stuck_no_entry else (0.5 if quiet_board else 1.0)
    severity = max(0.0, min(1.0, (1.0 - no_entry_metric) / 1.0))

    target_min = max(1, int(cfg.get("supervisorTargetOpenPositionsMin", 3) or 3))
    target_max = max(target_min, int(cfg.get("supervisorTargetOpenPositionsMax", 6) or 6))
    target_min = min(target_min, 6)
    target_max = min(max(target_max, target_min), 6)
    old_max_open = max(1, int(cfg.get("maxOpenPositions", target_max) or target_max))
    if old_max_open < target_min:
        set_int("maxOpenPositions", target_min)
    elif old_max_open > target_max:
        set_int("maxOpenPositions", target_max)
    cfg["supervisorTargetOpenPositionsMin"] = target_min
    cfg["supervisorTargetOpenPositionsMax"] = target_max

    min_conf = float(cfg.get("minConfidence", 0.62) or 0.62)
    early_conf = float(cfg.get("earlyEntryMinConfidence", 0.60) or 0.60)
    if stuck_no_entry:
        min_floor = max(0.60, min(0.82, float(cfg.get("supervisorStuckLowEntryMinConfidenceFloor", 0.76) or 0.76)))
        early_floor = max(0.56, min(0.78, float(cfg.get("supervisorStuckLowEntryEarlyConfidenceFloor", 0.68) or 0.68)))
        set_float("minConfidence", max(min_floor, min_conf - 0.015 * (1.0 + severity * 2.0)), 3)
        set_float("earlyEntryMinConfidence", max(early_floor, early_conf - 0.015 * (1.0 + severity * 2.0)), 3)
        for key in ("scanFallbackNearConfRelax", "scanGuardedFallbackConfRelax"):
            old = cfg.get(key)
            old_num = float(old) if old is not None else None
            if old_num is None or abs(old_num - recovery_relax) >= 1e-9:
                cfg[key] = round(recovery_relax, 3)
                changes[key] = {"old": old, "new": round(recovery_relax, 3)}
        if not bool(cfg.get("scanFallbackNearEnabled", True)):
            changes["scanFallbackNearEnabled"] = {"old": bool(cfg.get("scanFallbackNearEnabled", False)), "new": True}
            cfg["scanFallbackNearEnabled"] = True
        if not bool(cfg.get("scanGuardedFallbackEnabled", True)):
            changes["scanGuardedFallbackEnabled"] = {"old": bool(cfg.get("scanGuardedFallbackEnabled", False)), "new": True}
            cfg["scanGuardedFallbackEnabled"] = True
    else:
        base_step_min = 0.03 if quiet_board else 0.02
        base_step_early = 0.025 if quiet_board else 0.015
        set_float("minConfidence", max(0.48 if quiet_board else 0.50, min_conf - base_step_min * (1.0 + severity * 2.0)), 3)
        set_float("earlyEntryMinConfidence", max(0.50 if quiet_board else 0.52, early_conf - base_step_early * (1.0 + severity * 2.0)), 3)
    if quiet_board and not stuck_no_entry:
        score_gap = float(cfg.get("earlyEntryScoreGapMin", 1.40) or 1.40)
        set_float("earlyEntryScoreGapMin", max(0.90, score_gap - 0.12 * (1.0 + severity * 2.0)), 3)
        hybrid_score = float(cfg.get("hybridMinScore", 0.72) or 0.72)
        set_float("hybridMinScore", max(0.62, hybrid_score - 0.025 * (1.0 + severity * 2.0)), 3)
        hybrid_edge = float(cfg.get("hybridMinEdge", 0.06) or 0.06)
        set_float("hybridMinEdge", max(0.025, hybrid_edge - 0.01 * (1.0 + severity * 2.0)), 3)
    analyze_top = max(3, int(cfg.get("scanAnalyzeTop", 8) or 8))
    # Caps mirror backend/main.py — see that file for rationale on keeping
    # these ceilings low (event-loop wedge mitigation during scan cycles).
    set_int("scanAnalyzeTop", min(8, analyze_top + int(round(2 * (1.0 + severity * 2.0)))))
    top_liquid = max(5, int(cfg.get("scanTopLiquid", 30) or 30))
    set_int("scanTopLiquid", min(40, top_liquid + int(round(10 * (1.0 + severity * 2.0)))))
    guarded_top = max(analyze_top + 2, int(cfg.get("scanGuardedFallbackAnalyzeTop", analyze_top * 2) or analyze_top * 2))
    set_int("scanGuardedFallbackAnalyzeTop", min(12, max(guarded_top, int(cfg.get("scanAnalyzeTop", analyze_top)))))
    fallback_keys = ("scanFallbackNearEnabled",) if stuck_no_entry else ("scanFallbackNearEnabled", "scanPerfSoftFallbackEnabled")
    for key in fallback_keys:
        if not bool(cfg.get(key, True)):
            changes[key] = {"old": bool(cfg.get(key, False)), "new": True}
            cfg[key] = True

    if not changes:
        return {"applied": False, "reason": "no_safe_delta"}
    signature = _tuning_signature("low_entry_activity", stuck=stuck_no_entry, quiet=quiet_board, reason=str(reason or "low_entry_activity"))
    out = _commit_supervisor_config_tune(
        state,
        delegations,
        "low_entry_activity",
        cfg,
        changes,
        "stuck_low_entry_recovery" if stuck_no_entry else str(reason or "low entry activity"),
    )
    out["rejectCounts"] = reject_counts
    out["quietMarket"] = quiet_board
    out["targetOpenPositions"] = {"min": target_min, "max": target_max}
    out["stuckRecovery"] = stuck_no_entry
    out["severity"] = severity
    out["signature"] = signature
    if stuck_no_entry:
        out["nearMissSymbols"] = [str(row.get("symbol", "") or "") for row in near_miss_rows[:5]]
    return out

def _maybe_tune_scan_timeout_from_skip(skip_msg: str, cfg: dict | None = None) -> dict:
    if not isinstance(cfg, dict):
        return {}
    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("scan_timeout", cfg, 20)
    if active:
        return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec}

    if _tuning_should_rollback("scan_timeout"):
        rollback = _tuning_rollback_last("scan_timeout")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state, delegations, "scan_timeout", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    changes: dict[str, dict] = {}

    def set_float(key: str, value: float, digits: int = 2):
        old = float(cfg.get(key, value) or value)
        new = round(float(value), digits)
        if abs(old - new) >= 1e-9:
            cfg[key] = new
            changes[key] = {"old": round(old, digits), "new": new}

    def set_int(key: str, value: int):
        old = int(cfg.get(key, value) or value)
        new = int(value)
        if old != new:
            cfg[key] = new
            changes[key] = {"old": old, "new": new}

    per_symbol_timeout = max(2.0, float(cfg.get("scanPerSymbolTimeoutSec", 12.0) or 12.0))
    # Adaptive severity based on timeout deviation from baseline
    severity = max(0.0, min(1.0, (per_symbol_timeout - 2.0) / 8.0))
    # Capped at 6.0s (was 10.0) — matches the cap in backend/main.py.
    set_float("scanPerSymbolTimeoutSec", min(6.0, per_symbol_timeout * (1.0 + 0.15 * (1.0 + severity * 2.0))), 2)
    analyze_top = max(3, int(cfg.get("scanAnalyzeTop", 8) or 8))
    set_int("scanAnalyzeTop", max(3, analyze_top - int(round(1 * (1.0 + severity * 2.0)))))
    guarded_top = max(analyze_top, int(cfg.get("scanGuardedFallbackAnalyzeTop", max(analyze_top * 2, analyze_top + 4)) or analyze_top))
    set_int("scanGuardedFallbackAnalyzeTop", max(int(cfg.get("scanAnalyzeTop", analyze_top) or analyze_top), guarded_top - int(round(2 * (1.0 + severity * 2.0)))))
    fallback_retries = max(1, int(cfg.get("scanFallbackRetrySymbols", 3) or 3))
    set_int("scanFallbackRetrySymbols", max(1, fallback_retries - int(round(1 * (1.0 + severity * 2.0)))))

    if not changes:
        return {"applied": False, "reason": "no_safe_delta"}
    signature = _tuning_signature("scan_timeout", skip_msg=str(skip_msg or "scan timeout"), per_symbol_timeout=per_symbol_timeout)
    out = _commit_supervisor_config_tune(state, delegations, "scan_timeout", cfg, changes, str(skip_msg or "scan timeout"))
    out["severity"] = severity
    out["signature"] = signature
    return out

maybe_tune_external_signal_guard = _maybe_tune_external_signal_guard
maybe_tune_low_entry_activity = _maybe_tune_low_entry_activity
maybe_tune_scan_timeout_from_skip = _maybe_tune_scan_timeout_from_skip

def _supervisor_trade_period_reviews(trades: list[dict], *, now_ts: int | None = None) -> list[dict]:
    cleaned: list[dict] = []
    for trade in trades or []:
        if not isinstance(trade, dict):
            continue
        try:
            pnl = float(trade.get("_pnl", trade.get("pnl", 0.0)) or 0.0)
        except Exception:
            continue
        if not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        try:
            ts = int(float(trade.get("_ts", trade.get("closedAt", trade.get("ts", 0))) or 0))
        except Exception:
            ts = 0
        item = dict(trade)
        item["_pnl"] = pnl
        item["_ts"] = ts
        cleaned.append(item)
    cleaned.sort(key=lambda x: int(x.get("_ts", 0) or 0))
    if not cleaned:
        return []

    def build_window(rows: list[dict], label: str, previous: list[dict] | None = None) -> dict:
        pnls = [float(t.get("_pnl", 0.0) or 0.0) for t in rows]
        wins = [p for p in pnls if p >= 0.0]
        losses = [p for p in pnls if p < 0.0]
        win_rate = (len(wins) / max(len(rows), 1)) * 100.0
        pnl_sum = sum(pnls)
        avg_win = sum(wins) / max(len(wins), 1) if wins else 0.0
        avg_loss = sum(losses) / max(len(losses), 1) if losses else 0.0
        gross_win = sum(wins)
        gross_loss = abs(sum(losses))
        profit_factor = gross_win / gross_loss if gross_loss > 0 else (999.0 if gross_win > 0 else 0.0)
        payoff_ratio = avg_win / abs(avg_loss) if avg_win > 0 and avg_loss < 0 else 0.0
        reasons: dict[str, int] = {}
        symbols: dict[str, dict] = {}
        quick_losses = 0
        small_wins = 0
        for trade in rows:
            reason = str(trade.get("reason", "UNKNOWN") or "UNKNOWN").upper()
            reasons[reason] = reasons.get(reason, 0) + 1
            sym = str(trade.get("symbol", "") or "UNKNOWN").upper()
            bucket = symbols.setdefault(sym, {"trades": 0, "pnl": 0.0})
            bucket["trades"] += 1
            bucket["pnl"] = round(float(bucket["pnl"]) + float(trade.get("_pnl", 0.0) or 0.0), 6)
            opened = int(float(trade.get("openedAt", 0) or 0))
            closed = int(float(trade.get("_ts", 0) or 0))
            minutes = (closed - opened) / 60.0 if opened > 0 and closed >= opened else None
            pnl = float(trade.get("_pnl", 0.0) or 0.0)
            if pnl < 0 and minutes is not None and minutes <= 8:
                quick_losses += 1
            if 0.0 < pnl < 0.25:
                small_wins += 1
        top_reasons = sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:3]
        worst_symbols = sorted(symbols.items(), key=lambda x: float(x[1].get("pnl", 0.0)))[:3]
        review = {
            "label": label,
            "trades": len(rows),
            "wins": len(wins),
            "losses": len(losses),
            "winRatePct": round(win_rate, 2),
            "pnl": round(pnl_sum, 6),
            "avgPnl": round(pnl_sum / max(len(rows), 1), 6),
            "profitFactor": round(min(profit_factor, 999.0), 4),
            "payoffRatio": round(payoff_ratio, 4),
            "avgWin": round(avg_win, 6),
            "avgLoss": round(avg_loss, 6),
            "quickLosses": quick_losses,
            "smallWins": small_wins,
            "topReasons": [{"reason": r, "count": c} for r, c in top_reasons],
            "worstSymbols": [
                {"symbol": sym, "trades": int(data.get("trades", 0)), "pnl": round(float(data.get("pnl", 0.0)), 6)}
                for sym, data in worst_symbols
            ],
        }
        if previous:
            prev_pnls = [float(t.get("_pnl", 0.0) or 0.0) for t in previous]
            prev_wr = (sum(1 for p in prev_pnls if p >= 0.0) / max(len(prev_pnls), 1)) * 100.0
            prev_avg = sum(prev_pnls) / max(len(prev_pnls), 1)
            review["previous"] = {"winRatePct": round(prev_wr, 2), "avgPnl": round(prev_avg, 6)}
            review["trend"] = (
                "degrading"
                if win_rate < prev_wr - 10.0 or review["avgPnl"] < prev_avg - 0.04
                else "improving"
                if win_rate > prev_wr + 10.0 and review["avgPnl"] > prev_avg + 0.03
                else "stable"
            )
        else:
            review["trend"] = "insufficient_previous"
        return review

    reviews: list[dict] = []
    for window in (8, 20, 50):
        if len(cleaned) < max(4, min(window, 8)):
            continue
        rows = cleaned[-window:]
        previous = cleaned[-(window * 2) : -window] if len(cleaned) >= window * 2 else []
        reviews.append(build_window(rows, f"last_{min(window, len(rows))}_trades", previous or None))
    return reviews

def _symbol_drag_candidate_from_review(review: dict, cfg: dict | None = None) -> dict:
    if not isinstance(review, dict):
        return {}
    cfg = cfg if isinstance(cfg, dict) else {}
    worst_symbols = review.get("worstSymbols") if isinstance(review.get("worstSymbols"), list) else []
    worst = worst_symbols[0] if worst_symbols and isinstance(worst_symbols[0], dict) else {}
    sym = str(worst.get("symbol", "") or "").upper().strip()
    if not sym or sym == "UNKNOWN":
        return {}
    try:
        trades_n = int(review.get("trades", 0) or 0)
        total_pnl = float(review.get("pnl", 0.0) or 0.0)
        worst_trades = int(worst.get("trades", 0) or 0)
        worst_pnl = float(worst.get("pnl", 0.0) or 0.0)
    except Exception:
        return {}
    min_trades = int(cfg.get("symbolDragLockMinTrades", 3) or 3)
    min_loss = abs(float(cfg.get("symbolDragLockMinLossUsdt", 0.50) or 0.50))
    dominance = abs(worst_pnl) / max(abs(total_pnl), 0.01)
    extreme_dominance = (
        worst_trades >= 2
        and worst_pnl <= -(min_loss * 1.25)
        and dominance >= float(cfg.get("symbolDragExtremeDominanceMin", 1.25) or 1.25)
    )
    dominant = (
        trades_n >= 6
        and (worst_trades >= min_trades or extreme_dominance)
        and worst_pnl <= -min_loss
        and total_pnl < 0.0
        and dominance >= float(cfg.get("symbolDragDominanceMin", 0.75) or 0.75)
    )
    if not dominant:
        return {}
    return {
        "symbol": sym,
        "trades": worst_trades,
        "pnl": round(worst_pnl, 6),
        "dominance": round(dominance, 4),
        "label": str(review.get("label", "") or ""),
    }

def _maybe_lock_symbol_drag_from_review(review: dict, cfg: dict | None = None) -> dict:
    candidate = _symbol_drag_candidate_from_review(review, cfg)
    if not candidate:
        return {}
    cfg = cfg if isinstance(cfg, dict) else {}
    now = int(time.time())
    locks = AUTO_TRADE.get("perfLocks")
    if not isinstance(locks, dict):
        locks = {}
    sym = str(candidate.get("symbol", "") or "").upper().strip()
    current = locks.get(sym) if isinstance(locks.get(sym), dict) else {}
    if int(current.get("until", 0) or 0) > now:
        candidate["locked"] = True
        candidate["alreadyLocked"] = True
        candidate["until"] = int(current.get("until", 0) or 0)
        return candidate
    lock_min = max(30, int(cfg.get("symbolDragLockMinutes", cfg.get("perfLockMinutes", 120)) or 120))
    until = now + (lock_min * 60)
    locks[sym] = {
        "until": until,
        "at": now,
        "reason": "symbol_drag",
        "perf": {
            "trades": int(candidate.get("trades", 0) or 0),
            "pnl": float(candidate.get("pnl", 0.0) or 0.0),
            "source": str(candidate.get("label", "") or ""),
            "dominance": float(candidate.get("dominance", 0.0) or 0.0),
        },
    }
    AUTO_TRADE["perfLocks"] = locks
    candidate["locked"] = True
    candidate["alreadyLocked"] = False
    candidate["until"] = until
    candidate["minutes"] = lock_min
    return candidate

def _maybe_tune_weak_payoff_from_review(review: dict, cfg: dict | None = None) -> dict:
    if not isinstance(review, dict):
        return {}
    cfg = cfg if isinstance(cfg, dict) else {}
    try:
        trades_n = int(review.get("trades", 0) or 0)
        payoff_ratio = float(review.get("payoffRatio", 0.0) or 0.0)
        avg_win = float(review.get("avgWin", 0.0) or 0.0)
        avg_loss = abs(float(review.get("avgLoss", 0.0) or 0.0))
    except Exception:
        return {}
    if trades_n < 6 or payoff_ratio <= 0.0 or payoff_ratio >= 0.75:
        return {}

    # Infra guard: don't tighten if weak payoff is plausibly caused by infra.
    try:
        from main import _infra_incident_from_messages
        last_skip = AUTO_TRADE.get("lastSkip") if isinstance(AUTO_TRADE.get("lastSkip"), dict) else {}
        recent_msgs = [
            str(row.get("msg", "") or "")
            for row in (AUTO_TRADE.get("log") or [])[:24]
            if isinstance(row, dict)
        ]
        infra = _infra_incident_from_messages(
            recent_msgs,
            skip_code=str(last_skip.get("code", "") or ""),
            skip_msg=str(last_skip.get("msg", "") or ""),
        )
        if infra.get("active"):
            return {
                "applied": False,
                "infraShortCircuited": True,
                "reason": f"infra_incident:{infra.get('category')}",
                "infraCause": infra,
            }
    except Exception:
        pass

    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("weak_payoff", cfg, 90)
    signature = _tuning_signature("weak_payoff", trades=trades_n, payoff=round(payoff_ratio, 4))
    rec = delegations.get("weak_payoff") if isinstance(delegations.get("weak_payoff"), dict) else {}
    if active:
        return {"applied": False, "alreadyTuned": True, "signature": signature, "cooldownSec": cooldown_sec}

    # Auto-rollback if previous tuning worsened performance
    if _tuning_should_rollback("weak_payoff"):
        rollback = _tuning_rollback_last("weak_payoff")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state, delegations, "weak_payoff", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    # Adaptive severity: 0.0 (mild) → 1.0 (severe)
    severity = max(0.0, min(1.0, (0.75 - payoff_ratio) / 0.75 + (avg_loss / max(avg_win, 0.01) - 1.0) / 2.0))

    changes: dict[str, dict] = {}

    def set_float(key: str, value: float, digits: int = 4):
        old = float(cfg.get(key, value) or value)
        new = round(float(value), digits)
        if abs(old - new) >= 1e-9:
            cfg[key] = new
            changes[key] = {"old": round(old, digits), "new": new}

    if not bool(cfg.get("holdWinners", True)):
        changes["holdWinners"] = {"old": bool(cfg.get("holdWinners", False)), "new": True}
        cfg["holdWinners"] = True

    hold_min = float(cfg.get("holdMinConfidence", 0.78) or 0.78)
    set_float("holdMinConfidence", max(0.68, min(0.86, hold_min - 0.03 * (1.0 + severity))), 3)

    tp_min = max(0.20, float(cfg.get("tpTargetMinUsdt", 0.55) or 0.55))
    tp_max = max(tp_min + 0.10, float(cfg.get("tpTargetMaxUsdt", 2.2) or 2.2))
    set_float("tpTargetMinUsdt", min(1.20, tp_min * (1.0 + 0.15 * severity)), 3)
    set_float("tpTargetMaxUsdt", min(3.20, max(tp_max * (1.0 + 0.12 * severity), float(cfg.get("tpTargetMinUsdt", tp_min)) * 2.4)), 3)

    be_trigger = float(cfg.get("profitLockBreakevenTriggerUsdt", 0.16) or 0.16)
    set_float("profitLockBreakevenTriggerUsdt", min(0.45, max(0.20, be_trigger * (1.0 + 0.15 * severity))), 3)

    weak_loss_pressure = avg_loss > 0 and avg_win > 0 and avg_loss > avg_win * 1.35
    if weak_loss_pressure:
        sl_pct = max(0.20, float(cfg.get("stopLossPct", 0.9) or 0.9))
        loss_pressure = avg_loss / max(avg_win, 0.01)
        sl_factor = max(0.70, 0.88 if loss_pressure >= 2.4 else 0.94 - 0.06 * severity)
        set_float("stopLossPct", max(0.48, sl_pct * sl_factor), 3)
        cap = float(cfg.get("payoffLossGuardLossToWinCap", 1.05) or 1.05)
        set_float("payoffLossGuardLossToWinCap", max(0.85, min(cap, 0.95 - 0.05 * severity)), 3)
        max_loss_usdt = float(cfg.get("payoffLossGuardMaxLossUsdt", 0.9) or 0.9)
        target_loss_cap = max(0.25, min(max_loss_usdt, avg_win * 1.25))
        set_float("payoffLossGuardMaxLossUsdt", max(0.25, min(0.75, target_loss_cap * (1.0 - 0.10 * severity))), 3)
        min_loss_usdt = float(cfg.get("payoffLossGuardMinLossUsdt", 0.22) or 0.22)
        set_float("payoffLossGuardMinLossUsdt", max(0.12, min(min_loss_usdt, avg_win * 0.90)), 3)
        size_mult = float(cfg.get("supervisorSizeMultiplier", 1.0) or 1.0)
        set_float("supervisorSizeMultiplier", max(0.70, min(size_mult, 0.85 - 0.10 * severity)), 3)

    if not changes:
        return {"applied": False, "reason": "no_safe_delta", "signature": signature}

    state.update({
        "at": int(time.time()),
        "signature": signature,
        "reason": "weak_payoff_ratio",
        "payoffRatio": round(payoff_ratio, 4),
        "avgWin": round(avg_win, 6),
        "avgLoss": round(avg_loss, 6),
        "severity": round(severity, 3),
        "changes": changes,
    })
    AUTO_TRADE["supervisorAutoTune"] = state
    out = _commit_supervisor_config_tune(state, delegations, "weak_payoff", cfg, changes, "weak_payoff_ratio")
    out["signature"] = signature
    out["severity"] = round(severity, 3)
    return out

def _daily_trade_regime_review(trades: list[dict], cfg: dict | None = None, *, now_ts: int | None = None) -> dict:
    cfg = cfg if isinstance(cfg, dict) else {}
    now_ts = int(now_ts or time.time())
    today_key = time.strftime("%Y-%m-%d", time.localtime(now_ts))

    # Cache to avoid re-processing all trades every call
    cache = AUTO_TRADE.setdefault("_dailyRegimeCache", {})
    cache_key = (today_key, len(trades or []))
    if cache_key in cache:
        return cache[cache_key]

    min_trades = max(4, int(cfg.get("supervisorDailyBaselineMinTrades", 8) or 8))
    cleaned: list[dict] = []
    for trade in trades or []:
        if not isinstance(trade, dict):
            continue
        try:
            pnl = float(trade.get("_pnl", trade.get("pnl", 0.0)) or 0.0)
            ts = int(float(trade.get("_ts", trade.get("closedAt", trade.get("ts", 0))) or 0))
        except Exception:
            continue
        if ts <= 0 or not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        item = dict(trade)
        item["_pnl"] = pnl
        item["_ts"] = ts
        item["_day"] = time.strftime("%Y-%m-%d", time.localtime(ts))
        cleaned.append(item)
    if not cleaned:
        return {}

    grouped: dict[str, list[dict]] = {}
    for trade in cleaned:
        grouped.setdefault(str(trade.get("_day")), []).append(trade)

    def summarize(day: str, rows: list[dict]) -> dict:
        pnls = [float(t.get("_pnl", 0.0) or 0.0) for t in rows]
        wins = [p for p in pnls if p >= 0.0]
        losses = [p for p in pnls if p < 0.0]
        small_wins = sum(1 for p in wins if 0.0 < p < float(cfg.get("supervisorSmallProfitWinUsdt", 0.25) or 0.25))
        avg_win = sum(wins) / max(len(wins), 1) if wins else 0.0
        avg_loss = sum(losses) / max(len(losses), 1) if losses else 0.0
        return {
            "day": day,
            "trades": len(rows),
            "wins": len(wins),
            "losses": len(losses),
            "winRatePct": round((len(wins) / max(len(rows), 1)) * 100.0, 2),
            "pnl": round(sum(pnls), 6),
            "avgPnl": round(sum(pnls) / max(len(rows), 1), 6),
            "avgWin": round(avg_win, 6),
            "avgLoss": round(avg_loss, 6),
            "smallWins": small_wins,
            "smallWinRatePct": round((small_wins / max(len(rows), 1)) * 100.0, 2),
        }

    today = summarize(today_key, grouped.get(today_key, []))
    previous_days = [
        summarize(day, rows)
        for day, rows in grouped.items()
        if day != today_key and len(rows) >= min_trades
    ]
    profitable_days = [d for d in previous_days if float(d.get("pnl", 0.0) or 0.0) > 0.0]
    profitable_days.sort(key=lambda d: (float(d.get("pnl", 0.0) or 0.0), float(d.get("winRatePct", 0.0) or 0.0)), reverse=True)
    baseline = profitable_days[0] if profitable_days else {}
    if not baseline:
        return {"today": today, "baseline": {}, "status": "insufficient_profitable_baseline"}

    wr_drop = float(baseline.get("winRatePct", 0.0) or 0.0) - float(today.get("winRatePct", 0.0) or 0.0)
    avg_drop = float(baseline.get("avgPnl", 0.0) or 0.0) - float(today.get("avgPnl", 0.0) or 0.0)
    avg_win_drop = float(baseline.get("avgWin", 0.0) or 0.0) - float(today.get("avgWin", 0.0) or 0.0)
    today_trades = int(today.get("trades", 0) or 0)
    degraded = (
        today_trades >= min_trades
        and float(today.get("pnl", 0.0) or 0.0) < 0.0
        and (wr_drop >= float(cfg.get("supervisorDailyWrDropAlertPct", 18.0) or 18.0) or avg_drop >= 0.10)
    )
    giveback_like = (
        today_trades >= min_trades
        and int(today.get("smallWins", 0) or 0) >= max(3, math.ceil(today_trades * 0.25))
        and avg_win_drop >= 0.10
    )
    result = {
        "today": today,
        "baseline": baseline,
        "status": "degrading" if degraded else "stable",
        "degraded": degraded,
        "givebackLike": giveback_like,
        "winRateDropPct": round(wr_drop, 2),
        "avgPnlDrop": round(avg_drop, 6),
        "avgWinDrop": round(avg_win_drop, 6),
        "minTrades": min_trades,
    }
    cache[cache_key] = result
    return result

def _maybe_tune_daily_entry_regression(daily_review: dict, cfg: dict | None = None) -> dict:
    if not isinstance(daily_review, dict) or not isinstance(cfg, dict) or not bool(daily_review.get("degraded")):
        return {}
    # Infra guard: if today's regression is plausibly caused by an infra
    # incident, do NOT tighten the strategy — the operator must fix infra first.
    try:
        from main import _infra_incident_from_messages
        last_skip = AUTO_TRADE.get("lastSkip") if isinstance(AUTO_TRADE.get("lastSkip"), dict) else {}
        recent_msgs = [
            str(row.get("msg", "") or "")
            for row in (AUTO_TRADE.get("log") or [])[:32]
            if isinstance(row, dict)
        ]
        infra = _infra_incident_from_messages(
            recent_msgs,
            skip_code=str(last_skip.get("code", "") or ""),
            skip_msg=str(last_skip.get("msg", "") or ""),
        )
        if infra.get("active"):
            return {
                "applied": False,
                "infraShortCircuited": True,
                "reason": f"infra_incident:{infra.get('category')}",
                "infraCause": infra,
            }
    except Exception:
        pass
    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("daily_entry_regression", cfg, 30)

    # Rollback check — must be first to avoid blocking rollback with cooldown
    if _tuning_should_rollback("daily_entry_regression"):
        rollback = _tuning_rollback_last("daily_entry_regression")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state, delegations, "daily_entry_regression", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            _tuning_mode_lock_release()  # Rollback clears the lock
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    today = daily_review.get("today") if isinstance(daily_review.get("today"), dict) else {}
    baseline = daily_review.get("baseline") if isinstance(daily_review.get("baseline"), dict) else {}

    # Adaptive severity based on winRatePct drop and avgPnl drop
    wr_drop = float(baseline.get("winRatePct", 0.0) or 0.0) - float(today.get("winRatePct", 0.0) or 0.0)
    avg_drop = float(baseline.get("avgPnl", 0.0) or 0.0) - float(today.get("avgPnl", 0.0) or 0.0)
    severity = max(0.0, min(1.0, max(wr_drop / 30.0, avg_drop / 0.20)))

    signature = _tuning_signature("daily_entry_regression", day=today.get("day"), trades=today.get("trades"), winRatePct=today.get("winRatePct"), pnl=today.get("pnl"), baseline_day=baseline.get("day"), baseline_pnl=baseline.get("pnl"))
    rec = delegations.get("daily_entry_regression") if isinstance(delegations.get("daily_entry_regression"), dict) else {}

    # Cooldown check (after rollback, before mode lock)
    if active:
        return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec, "signature": signature}

    # Acquire tightening mode lock — if loosener recently fired, skip
    if not _tuning_mode_lock_acquire("tightening", f"daily_regression:{today.get('day')}", cfg):
        return {"applied": False, "reason": "opposite_mode_active", "mode": "tightening", "blockedBy": "loosening", "signature": signature}

    changes: dict[str, dict] = {}

    def set_float(key: str, value: float, digits: int = 3):
        old = float(cfg.get(key, value) or value)
        new = round(float(value), digits)
        if abs(old - new) >= 1e-9:
            cfg[key] = new
            changes[key] = {"old": round(old, digits), "new": new}

    def set_int(key: str, value: int):
        old = int(cfg.get(key, value) or value)
        new = int(value)
        if old != new:
            cfg[key] = new
            changes[key] = {"old": old, "new": new}

    min_conf = float(cfg.get("minConfidence", 0.66) or 0.66)
    # Profitable baseline days were not purely high-confidence trades; avoid
    # choking AUTO scan by pushing the global gate toward 0.90.
    set_float("minConfidence", min(0.84, max(min_conf, min_conf + 0.015 * (1.0 + severity * 2.0))), 3)
    early_conf = float(cfg.get("earlyEntryMinConfidence", 0.60) or 0.60)
    set_float("earlyEntryMinConfidence", min(0.76, max(early_conf, early_conf + 0.015 * (1.0 + severity * 2.0))), 3)
    gap = float(cfg.get("earlyEntryScoreGapMin", 1.40) or 1.40)
    set_float("earlyEntryScoreGapMin", min(2.10, max(gap, gap + 0.12 * (1.0 + severity * 2.0))), 3)
    set_float("earlyEntryMaxBbPctB", min(float(cfg.get("earlyEntryMaxBbPctB", 0.82) or 0.82), 0.78), 3)
    set_float("earlyEntryMinBbPctBShort", max(float(cfg.get("earlyEntryMinBbPctBShort", 0.18) or 0.18), 0.22), 3)
    set_float("earlyEntryMaxVwapDistancePct", min(float(cfg.get("earlyEntryMaxVwapDistancePct", 0.24) or 0.24), 0.18), 3)
    set_float("riskCooldownResumeScoreGapMin", min(2.60, max(float(cfg.get("riskCooldownResumeScoreGapMin", 2.0) or 2.0), 2.20 + 0.40 * severity)), 3)
    max_spread = float(cfg.get("maxSpreadBps", 22.0) or 22.0)
    set_float("maxSpreadBps", max(12.0, min(max_spread, max_spread * (0.90 - 0.05 * severity))), 2)
    if not bool(cfg.get("scanFallbackNearEnabled", True)):
        changes["scanFallbackNearEnabled"] = {"old": False, "new": True}
        cfg["scanFallbackNearEnabled"] = True
    near_relax = float(cfg.get("scanFallbackNearConfRelax", 0.04) or 0.04)
    set_float("scanFallbackNearConfRelax", max(0.02, min(near_relax, 0.035 - 0.005 * severity)), 3)
    guarded_relax = float(cfg.get("scanGuardedFallbackConfRelax", 0.04) or 0.04)
    set_float("scanGuardedFallbackConfRelax", max(0.02, min(guarded_relax, 0.04 - 0.005 * severity)), 3)
    if bool(cfg.get("scanPerfSoftFallbackEnabled", True)):
        changes["scanPerfSoftFallbackEnabled"] = {"old": True, "new": False}
        cfg["scanPerfSoftFallbackEnabled"] = False
    if not bool(cfg.get("todayPerformanceGuardEnabled", True)):
        changes["todayPerformanceGuardEnabled"] = {"old": False, "new": True}
        cfg["todayPerformanceGuardEnabled"] = True
    set_int("todayPerformanceGuardMinTrades", min(6, int(cfg.get("todayPerformanceGuardMinTrades", 8) or 8)))
    set_float("todayPerformanceGuardMaxWinRatePct", min(48.0, max(float(cfg.get("todayPerformanceGuardMaxWinRatePct", 40.0) or 40.0), 45.0 + 3.0 * severity)), 2)

    if not changes:
        if active and str(rec.get("signature", "") or "") == signature:
            return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec, "signature": signature}
        return {"applied": False, "reason": "no_safe_delta", "signature": signature}
    out = _commit_supervisor_config_tune(state, delegations, "daily_entry_regression", cfg, changes, "daily_entry_regression")
    delegations = AUTO_TRADE.get("supervisorAutoTune", {}).get("delegations", {})
    if isinstance(delegations.get("daily_entry_regression"), dict):
        delegations["daily_entry_regression"]["signature"] = signature
        delegations["daily_entry_regression"]["today"] = today
        delegations["daily_entry_regression"]["baseline"] = baseline
    out["signature"] = signature
    out["severity"] = severity
    return out

def _maybe_tune_small_profit_capture_from_review(review: dict, cfg: dict | None = None) -> dict:
    if not isinstance(review, dict) or not isinstance(cfg, dict):
        return {}
    try:
        trades_n = int(review.get("trades", 0) or 0)
        small_wins = int(review.get("smallWins", 0) or 0)
    except Exception:
        return {}
    if trades_n < 6 or small_wins < max(3, math.ceil(trades_n * 0.30)):
        return {}
    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("small_profit_capture", cfg, 90)

    if _tuning_should_rollback("small_profit_capture"):
        rollback = _tuning_rollback_last("small_profit_capture")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state, delegations, "small_profit_capture", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    # Block repeat tuning during the cooldown window so we don't oscillate
    if active:
        return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec}

    # Adaptive severity based on smallWins/trades_n ratio
    ratio = small_wins / max(trades_n, 1)
    severity = max(0.0, min(1.0, (ratio - 0.30) / 0.40))

    changes: dict[str, dict] = {}

    def set_float(key: str, value: float, digits: int = 3):
        old = float(cfg.get(key, value) or value)
        new = round(float(value), digits)
        if abs(old - new) >= 1e-9:
            cfg[key] = new
            changes[key] = {"old": round(old, digits), "new": new}

    if not bool(cfg.get("holdWinners", True)):
        changes["holdWinners"] = {"old": bool(cfg.get("holdWinners", False)), "new": True}
        cfg["holdWinners"] = True
    hold_min = float(cfg.get("holdMinConfidence", 0.72) or 0.72)
    set_float("holdMinConfidence", max(0.66, min(0.84, hold_min - 0.02 * (1.0 + severity * 2.0))), 3)
    trigger = float(cfg.get("profitLockTriggerUsdt", 0.35) or 0.35)
    keep = float(cfg.get("profitLockKeepUsdt", 0.15) or 0.15)
    giveback = float(cfg.get("profitLockMaxGivebackUsdt", 0.22) or 0.22)
    set_float("profitLockTriggerUsdt", max(0.22, min(trigger, trigger * (0.85 - 0.05 * severity))), 3)
    set_float("profitLockKeepUsdt", min(0.45, max(keep, keep * (1.20 + 0.10 * severity), 0.18)), 3)
    set_float("profitLockMaxGivebackUsdt", max(0.10, min(giveback, giveback * (0.82 - 0.08 * severity), 0.18)), 3)
    floor = float(cfg.get("profitLockBreakevenFloorUsdt", 0.08) or 0.08)
    set_float("profitLockBreakevenFloorUsdt", min(0.18, max(floor, 0.10 + 0.02 * severity)), 3)
    tp_min = max(0.20, float(cfg.get("tpTargetMinUsdt", 0.55) or 0.55))
    set_float("tpTargetMinUsdt", min(1.20, max(0.45, tp_min)), 3)
    tp_max = max(tp_min + 0.10, float(cfg.get("tpTargetMaxUsdt", 2.2) or 2.2))
    set_float("tpTargetMaxUsdt", min(3.20, max(tp_max, float(cfg.get("tpTargetMinUsdt", tp_min)) * 2.0)), 3)
    breakeven_trigger = float(cfg.get("profitLockBreakevenTriggerUsdt", 0.16) or 0.16)
    set_float("profitLockBreakevenTriggerUsdt", min(float(cfg.get("profitLockTriggerUsdt", trigger) or trigger), max(0.14, breakeven_trigger * (0.95 - 0.05 * severity))), 3)

    if not changes:
        if active:
            return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec}
        return {"applied": False, "reason": "no_safe_delta"}
    signature = _tuning_signature("small_profit_capture", trades=trades_n, small_wins=small_wins, ratio=round(ratio, 3))
    out = _commit_supervisor_config_tune(state, delegations, "small_profit_capture", cfg, changes, str(review.get("label") or "small wins"))
    out["severity"] = severity
    out["signature"] = signature
    return out

def _maybe_tune_negative_expectancy_from_review(review: dict, cfg: dict | None = None) -> dict:
    if not isinstance(review, dict) or not isinstance(cfg, dict):
        return {}
    try:
        trades_n = int(review.get("trades", 0) or 0)
        win_rate = float(review.get("winRatePct", 0.0) or 0.0)
        avg_pnl = float(review.get("avgPnl", 0.0) or 0.0)
        total_pnl = float(review.get("pnl", 0.0) or 0.0)
        quick_losses = int(review.get("quickLosses", 0) or 0)
    except Exception:
        return {}
    if trades_n < 6 or total_pnl >= 0.0 or (win_rate >= 45.0 and avg_pnl >= -0.04):
        return {}

    # Infra guard: do NOT tighten strategy if recent losses are explainable
    # by an infrastructure incident (drift/network/exchange/rate-limit/auth).
    try:
        from main import _infra_incident_from_messages
        last_skip = AUTO_TRADE.get("lastSkip") if isinstance(AUTO_TRADE.get("lastSkip"), dict) else {}
        recent_msgs = [
            str(row.get("msg", "") or "")
            for row in (AUTO_TRADE.get("log") or [])[:24]
            if isinstance(row, dict)
        ]
        infra = _infra_incident_from_messages(
            recent_msgs,
            skip_code=str(last_skip.get("code", "") or ""),
            skip_msg=str(last_skip.get("msg", "") or ""),
        )
        if infra.get("active"):
            return {
                "applied": False,
                "infraShortCircuited": True,
                "reason": f"infra_incident:{infra.get('category')}",
                "infraCause": infra,
            }
    except Exception:
        pass

    label = str(review.get("label", "") or "recent_trades")
    # Adaptive severity based on winRatePct and avgPnl
    severity = max(0.0, min(1.0, max((45.0 - win_rate) / 45.0, (-0.04 - avg_pnl) / 0.20)))
    signature = _tuning_signature("negative_expectancy", label=label, trades=trades_n, win_rate=win_rate, avg_pnl=avg_pnl, total_pnl=total_pnl)
    state, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("negative_expectancy", cfg, 45)

    # IMPORTANT: Check rollback BEFORE cooldown — harmful tunes must be
    # rolled back even if the tuner is on cooldown (fixes rollback gap).
    if _tuning_should_rollback("negative_expectancy"):
        rollback = _tuning_rollback_last("negative_expectancy")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state, delegations, "negative_expectancy", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            _tuning_mode_lock_release()  # Rollback clears the lock
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    rec = delegations.get("negative_expectancy") if isinstance(delegations.get("negative_expectancy"), dict) else {}
    if active:
        return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec, "signature": signature}

    # Acquire tightening mode lock — if loosener recently fired, skip
    if not _tuning_mode_lock_acquire("tightening", f"negative_expectancy:{label}", cfg):
        return {"applied": False, "reason": "opposite_mode_active", "mode": "tightening", "blockedBy": "loosening", "signature": signature}

    changes: dict[str, dict] = {}

    def set_float(key: str, value: float, digits: int = 3):
        old = float(cfg.get(key, value) or value)
        new = round(float(value), digits)
        if abs(old - new) >= 1e-9:
            cfg[key] = new
            changes[key] = {"old": round(old, digits), "new": new}

    def set_int(key: str, value: int):
        old = int(cfg.get(key, value) or value)
        new = int(value)
        if old != new:
            cfg[key] = new
            changes[key] = {"old": old, "new": new}

    min_conf = float(cfg.get("minConfidence", 0.62) or 0.62)
    conf_step = 0.03 if avg_pnl < -0.10 or win_rate < 40.0 else 0.02
    set_float("minConfidence", min(0.84, max(min_conf, min_conf + min(conf_step, 0.015) * (1.0 + severity * 2.0))), 3)

    early_conf = float(cfg.get("earlyEntryMinConfidence", 0.60) or 0.60)
    set_float("earlyEntryMinConfidence", min(0.76, max(early_conf, early_conf + 0.015 * (1.0 + severity * 2.0))), 3)
    gap = float(cfg.get("earlyEntryScoreGapMin", 1.40) or 1.40)
    gap_step = 0.12 if quick_losses >= 2 else 0.08
    set_float("earlyEntryScoreGapMin", min(2.10, max(gap, gap + gap_step * (1.0 + severity * 2.0))), 3)
    if not bool(cfg.get("earlyEntryPullbackResetEnabled", True)):
        changes["earlyEntryPullbackResetEnabled"] = {"old": False, "new": True}
        cfg["earlyEntryPullbackResetEnabled"] = True
    early_bb = float(cfg.get("earlyEntryMaxBbPctB", 0.82) or 0.82)
    set_float("earlyEntryMaxBbPctB", max(0.74, min(early_bb, 0.82)), 3)
    early_vwap = float(cfg.get("earlyEntryMaxVwapDistancePct", 0.24) or 0.24)
    set_float("earlyEntryMaxVwapDistancePct", max(0.12, min(early_vwap, 0.24)), 3)
    hybrid_score = float(cfg.get("hybridMinScore", 0.72) or 0.72)
    set_float("hybridMinScore", min(0.86, max(hybrid_score, hybrid_score + 0.02 * (1.0 + severity * 2.0))), 3)
    hybrid_edge = float(cfg.get("hybridMinEdge", 0.06) or 0.06)
    set_float("hybridMinEdge", min(0.12, max(hybrid_edge, hybrid_edge + 0.01 * (1.0 + severity * 2.0))), 3)

    max_spread = float(cfg.get("maxSpreadBps", 22.0) or 22.0)
    set_float("maxSpreadBps", max(12.0, min(max_spread, max_spread * (0.92 - 0.05 * severity))), 2)
    if str(cfg.get("scanSidePreference", "score") or "score") != "score":
        changes["scanSidePreference"] = {"old": cfg.get("scanSidePreference"), "new": "score"}
        cfg["scanSidePreference"] = "score"
    if not bool(cfg.get("scanFallbackNearEnabled", True)):
        changes["scanFallbackNearEnabled"] = {"old": False, "new": True}
        cfg["scanFallbackNearEnabled"] = True
    near_relax = float(cfg.get("scanFallbackNearConfRelax", 0.04) or 0.04)
    set_float("scanFallbackNearConfRelax", max(0.02, min(near_relax, 0.035 - 0.005 * severity)), 3)
    guarded_relax = float(cfg.get("scanGuardedFallbackConfRelax", 0.04) or 0.04)
    set_float("scanGuardedFallbackConfRelax", max(0.02, min(guarded_relax, 0.04 - 0.005 * severity)), 3)
    if bool(cfg.get("scanPerfSoftFallbackEnabled", True)):
        changes["scanPerfSoftFallbackEnabled"] = {"old": True, "new": False}
        cfg["scanPerfSoftFallbackEnabled"] = False

    set_int("perfLockMinutes", max(120, int(cfg.get("perfLockMinutes", 90) or 90)))
    set_int("perfGateEarlyMinSamples", min(4, int(cfg.get("perfGateEarlyMinSamples", 4) or 4)))
    set_float("perfGateEarlyMinPnlUsdt", max(float(cfg.get("perfGateEarlyMinPnlUsdt", -0.35) or -0.35), -0.35), 3)
    if not bool(cfg.get("sessionBiasEnabled", True)):
        changes["sessionBiasEnabled"] = {"old": False, "new": True}
        cfg["sessionBiasEnabled"] = True
    if not bool(cfg.get("todayPerformanceGuardEnabled", True)):
        changes["todayPerformanceGuardEnabled"] = {"old": False, "new": True}
        cfg["todayPerformanceGuardEnabled"] = True
    set_int("todayPerformanceGuardMinTrades", min(8, int(cfg.get("todayPerformanceGuardMinTrades", 8) or 8)))
    guard_wr = float(cfg.get("todayPerformanceGuardMaxWinRatePct", 40.0) or 40.0)
    set_float("todayPerformanceGuardMaxWinRatePct", min(45.0, max(guard_wr, 40.0 + 3.0 * severity)), 2)
    set_int("sessionBiasMinSamples", min(8, int(cfg.get("sessionBiasMinSamples", 10) or 10)))
    bad_wr = float(cfg.get("sessionBiasBadWinRatePct", 42.0) or 42.0)
    set_float("sessionBiasBadWinRatePct", min(48.0, max(bad_wr, 45.0 + 2.0 * severity)), 2)
    max_shift = float(cfg.get("sessionBiasMaxConfShift", 0.05) or 0.05)
    set_float("sessionBiasMaxConfShift", min(0.06, max(max_shift, 0.05 + 0.01 * severity)), 3)

    if not changes:
        return {"applied": False, "reason": "no_safe_delta", "signature": signature}
    out = _commit_supervisor_config_tune(state, delegations, "negative_expectancy", cfg, changes, "negative_expectancy")
    delegations = AUTO_TRADE.get("supervisorAutoTune", {}).get("delegations", {})
    if isinstance(delegations.get("negative_expectancy"), dict):
        delegations["negative_expectancy"]["signature"] = signature
        delegations["negative_expectancy"]["winRatePct"] = round(win_rate, 2)
        delegations["negative_expectancy"]["avgPnl"] = round(avg_pnl, 6)
        delegations["negative_expectancy"]["pnl"] = round(total_pnl, 6)
    out["signature"] = signature
    out["severity"] = severity
    return out

def _maybe_tune_size_multiplier_from_streak(trades: list[dict], cfg: dict | None = None) -> dict:
    if not isinstance(cfg, dict) or not bool(cfg.get("supervisorSizeStreakEnabled", True)):
        return {}
    state = _recent_live_result_streak_state(trades, int(cfg.get("supervisorSizeLookbackTrades", 12) or 12))
    kind = str(state.get("kind", "") or "")
    streak = int(state.get("streak", 0) or 0)
    win_min = max(2, int(cfg.get("supervisorSizeWinStreakMin", 3) or 3))
    loss_min = max(1, int(cfg.get("supervisorSizeLossStreakMin", 2) or 2))
    old_mult = float(cfg.get("supervisorSizeMultiplier", 1.0) or 1.0)
    min_mult = max(0.1, min(1.0, float(cfg.get("supervisorSizeMinMultiplier", 0.50) or 0.50)))
    if bool(cfg.get("marketScan")) or str(cfg.get("symbol", "")).upper() in {"AUTO", "SCAN"}:
        diversified_floor = max(0.1, min(1.0, float(cfg.get("supervisorSizeDiversifiedMinMultiplier", 0.65) or 0.65)))
        min_mult = max(min_mult, diversified_floor)
    max_mult = max(1.0, min(3.0, float(cfg.get("supervisorSizeMaxMultiplier", 1.35) or 1.35)))
    if max_mult < min_mult:
        max_mult = min_mult

    target = 1.0
    reason = "streak_reset"
    if kind == "win" and streak >= win_min:
        step = max(0.0, float(cfg.get("supervisorSizeWinStepPct", 10.0) or 10.0)) / 100.0
        severity = max(0.0, min(1.0, (streak - win_min) / 10.0))
        scaled_step = step * (1.0 + severity * 2.0)
        target = min(max_mult, 1.0 + scaled_step * (streak - win_min + 1))
        reason = "win_streak"
    elif kind == "loss" and streak >= loss_min:
        step = max(0.0, float(cfg.get("supervisorSizeLossStepPct", 15.0) or 15.0)) / 100.0
        severity = max(0.0, min(1.0, (streak - loss_min) / 10.0))
        scaled_step = step * (1.0 + severity * 2.0)
        target = max(min_mult, 1.0 - scaled_step * (streak - loss_min + 1))
        reason = "loss_streak"
    elif abs(old_mult - 1.0) >= 0.001:
        target = 1.0
        severity = 0.0
    else:
        return {"applied": False, "reason": "no_streak", "streak": state}

    target = round(float(target), 3)
    if abs(old_mult - target) < 0.001:
        return {"applied": False, "reason": "no_safe_delta", "streak": state}

    # Guard: do NOT reduce size multiplier when session has very few trades
    # (stale loss streak from a previous session would otherwise keep hammering it down)
    recent_count = len(trades) if isinstance(trades, list) else 0
    if kind == "loss" and recent_count < max(4, int(cfg.get("supervisorSizeLossStreakMin", 2) or 2) + 2):
        return {"applied": False, "reason": "insufficient_recent_trades_to_tune", "streak": state, "recentCount": recent_count}

    # Use longer cooldown (60 min) for size streak tuning to prevent rapid self-harm
    size_streak_cooldown_min = max(10, int(cfg.get("supervisorSizeStreakCooldownMin", 60) or 60))
    signature = _tuning_signature("size_streak", reason=reason, streak=streak, target=target, kind=kind)
    state_obj, delegations, active, cooldown_sec = _supervisor_delegation_cooldown("size_streak", cfg, size_streak_cooldown_min)
    rec = delegations.get("size_streak") if isinstance(delegations.get("size_streak"), dict) else {}
    if active:
        return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec, "signature": signature, "streak": state}

    if _tuning_should_rollback("size_streak"):
        rollback = _tuning_rollback_last("size_streak")
        if rollback.get("reverted"):
            pre = rollback.get("preMetrics", {})
            for k, v in pre.items():
                if isinstance(v, (int, float)) and k in cfg:
                    cfg[k] = v
            _commit_supervisor_config_tune(state_obj, delegations, "size_streak", cfg, {k: {"reverted": v} for k, v in pre.items()}, "rollback_worsened")
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    cfg["supervisorSizeMultiplier"] = target
    changes = {
        "supervisorSizeMultiplier": {
            "old": round(old_mult, 3),
            "new": target,
            "reason": reason,
            "streak": streak,
            "pnl": state.get("pnl"),
        }
    }
    out = _commit_supervisor_config_tune(state_obj, delegations, "size_streak", cfg, changes, reason)
    delegations = AUTO_TRADE.get("supervisorAutoTune", {}).get("delegations", {})
    if isinstance(delegations.get("size_streak"), dict):
        delegations["size_streak"]["signature"] = signature
        delegations["size_streak"]["kind"] = kind
        delegations["size_streak"]["streak"] = streak
        delegations["size_streak"]["pnl"] = state.get("pnl")
    out["signature"] = signature
    out["streak"] = state
    out["severity"] = severity
    return out

def _recent_live_result_streak_state(*args, **kwargs):
    return _main()._recent_live_result_streak_state(*args, **kwargs)

