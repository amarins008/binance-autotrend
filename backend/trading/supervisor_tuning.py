"""Supervisor auto-tuning helpers."""

from __future__ import annotations

import json
import math
import time

from services import app_state
from trading.supervisor_state import (
    _apply_rollback_old_values,
    _commit_supervisor_config_tune,
    _supervisor_delegation_cooldown,
    _tuning_rollback_last,
    _tuning_should_rollback,
    _tuning_signature,
    _tuning_mode_lock_acquire,
    _tuning_mode_lock_release,
)

AUTO_TRADE = app_state.AUTO_TRADE

# ── TV alert webhook ──────────────────────────────────────────────────────────
# Sends a JSON POST to cfg["tradingviewWebhookUrl"] (Discord / Telegram bot /
# generic endpoint) when TradingView health degrades or recovers. Fire-and-forget
# with a per-level cooldown so a flapping client cannot spam the webhook.
_TV_ALERT_COOLDOWN_SEC = 600  # 10 min between alerts of the same level
_TV_ALERT_LAST: dict[str, float] = {}


def _tv_alert_send(cfg: dict | None, level: str, title: str, message: str) -> dict:
    """Send one TV health alert to the configured webhook (if any).

    level ∈ {"info", "warning", "critical"}. Rate-limited per level:
    only the first alert of a level within _TV_ALERT_COOLDOWN_SEC goes out.
    Returns {"sent": bool, "reason": str}.
    """
    global _TV_ALERT_LAST
    url = ""
    if isinstance(cfg, dict):
        url = str(cfg.get("tradingviewWebhookUrl") or "").strip()
    if not url:
        return {"sent": False, "reason": "no_webhook_configured"}

    level = str(level or "info").lower()
    now = time.time()
    if now - _TV_ALERT_LAST.get(level, 0.0) < _TV_ALERT_COOLDOWN_SEC:
        return {"sent": False, "reason": "cooldown"}
    _TV_ALERT_LAST[level] = now

    try:
        import urllib.request

        payload = {
            "source": "binance-autotrend",
            "topic": "tradingview",
            "level": level,
            "title": str(title or "TradingView alert"),
            "message": str(message or "")[:2000],
            "ts": int(now),
        }
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=8) as resp:
            status = getattr(resp, "status", 200)
        # Discord/Telegram webhooks expect a 2xx; anything else = failure.
        if not (200 <= int(status) < 300):
            return {"sent": False, "reason": f"http_{status}"}
        return {"sent": True, "reason": "ok"}
    except Exception as exc:
        return {"sent": False, "reason": f"{type(exc).__name__}: {str(exc)[:80]}"}


def _maybe_tune_tradingview_health(cfg: dict | None = None) -> dict:
    """Monitor TradingView health and attempt auto-recovery.

    Always checks health (no cooldown). Recovery actions have independent 120s
    cooldown with progressive escalation: force_enable -> reset -> disable + report.
    """
    if not isinstance(cfg, dict):
        return {}

    from trading.tradingview_mcp import get_tv_client, reset_tv_client

    tv_client = get_tv_client(cfg)
    health = tv_client.get_health_status()
    is_healthy = health.get("healthy", True)
    fail_count = health.get("fail_count", 0)
    last_error = health.get("last_error", "")
    error_type = health.get("error_type", "")

    # If healthy, no action needed (unless disabled — auto-recover).
    # NOTE: don't gate auto-recovery on fail_count < 3 — fail_count is
    # cumulative (decremented once per success, reset by force_enable), so a
    # streak that already happened keeps the counter high even after the
    # client recovered. That left TV disabled forever (2026-08-02: healthy
    # + fail_count=5 -> never re-enabled -> entries opened blind against
    # strong TV, LINK 06:26 / ENA 10:35). Healthy means the client works now.
    state: dict = AUTO_TRADE.get("supervisorAutoTune") or {}
    delegations: dict = state.get("delegations") or {}
    if is_healthy:
        if not cfg.get("tradingviewEnabled", True):
            try:
                from main import _autotrade_log as alog
            except ImportError:
                def alog(*a, **kw): pass
            tv_client.force_enable()
            cfg["tradingviewEnabled"] = True
            # Reset recovery count since TV recovered
            tv_rec = delegations.get("tradingview_health") or {}
            tv_rec["recovery_count"] = 0
            tv_rec["at"] = int(time.time())
            delegations["tradingview_health"] = tv_rec
            state["delegations"] = delegations
            AUTO_TRADE["supervisorAutoTune"] = state
            alog("[TradingView] Auto-recovered: re-enabled after health restored")
            _tv_alert_send(cfg, "info", "TradingView กลับมาแล้ว",
                           f"TV auto-recovered: re-enabled หลัง health กลับมา (fail_count={fail_count})")
            return {"applied": True, "reason": "tv_recovered_auto_enable", "health": health, "changes": {"tradingviewEnabled": {"set": True, "was": False}}}
        return {"applied": False, "reason": "tv_healthy", "health": health}

    # Classify: rate limit vs real failure
    is_rate_limited = error_type == "rate_limited"

    if is_rate_limited:
        # Warn once when the rate-limit streak climbs (not per cycle — the
        # per-level cooldown in _tv_alert_send handles that).
        if fail_count >= 8:
            _tv_alert_send(cfg, "warning", "TradingView rate-limited ต่อเนื่อง",
                           f"fail_count={fail_count} (rate_limited) — TV signal อาจขาด coverage")
        return {
            "applied": False,
            "reason": "tv_rate_limited",
            "health": health,
            "symbol_cooldowns": health.get("symbol_cooldowns", 0),
        }

    # Real failure — check recovery cooldown (bypass _supervisor_delegation_cooldown
    # because its min 300s is too slow for immediate recovery)
    tv_rec: dict = delegations.get("tradingview_health") or {}
    last_at = int(tv_rec.get("at", 0) or 0)
    _TV_RECOVERY_COOLDOWN = 120  # 2 min between recovery attempts
    in_cooldown = time.time() - last_at < _TV_RECOVERY_COOLDOWN

    recovery_count = int(tv_rec.get("recovery_count", 0))

    if in_cooldown:
        return {
            "applied": False,
            "alreadyTuned": True,
            "cooldownSec": _TV_RECOVERY_COOLDOWN,
            "health": health,
            "tv_unhealthy": True,
            "recovery_count": recovery_count,
        }

    # --- Progressive recovery ---
    changes: dict = {}
    reason = ""
    tv_disabled = False

    try:
        if recovery_count < 2:
            # Attempt 1-2: force_enable (reset client health state)
            tv_client.force_enable()
            reason = f"force_enabled_tv: {last_error}"
            changes["tradingviewEnabled"] = {"set": True, "was": cfg.get("tradingviewEnabled", False)}
            cfg["tradingviewEnabled"] = True
            # Sync config back to TV client so self.enabled matches
            tv_client.update_config(cfg)

        elif recovery_count < 4:
            # Attempt 3-4: full client reset (fresh singleton)
            reset_tv_client()
            get_tv_client(cfg)  # recreates instance
            reason = f"reset_tv_client: {last_error}"
            changes["tradingview_reset"] = {"set": True, "was": False}

        else:
            # Attempt 5+: unrecoverable, disable TV for 10 min + flag
            tv_client.force_disable(600)
            cfg["tradingviewEnabled"] = False
            tv_disabled = True
            reason = f"tv_unrecoverable_after_{recovery_count}_attempts: {last_error}"
            changes["tradingviewEnabled"] = {"set": False, "was": True}
            changes["tradingview_disabled_reason"] = {"set": reason, "was": ""}

    except Exception as e:
        reason = f"tv_recovery_error: {str(e)}"

    # Update recovery count
    tv_rec["recovery_count"] = recovery_count + 1
    tv_rec["at"] = int(time.time())
    tv_rec["last_error"] = last_error
    tv_rec["recovery_action"] = reason
    delegations["tradingview_health"] = tv_rec
    state["delegations"] = delegations
    AUTO_TRADE["supervisorAutoTune"] = state

    if not changes:
        return {"applied": False, "reason": "no_safe_delta", "health": health}

    signature = _tuning_signature(
        "tradingview_health",
        fail_count=fail_count,
        healthy=is_healthy,
        error_type=error_type,
        recovery_count=recovery_count + 1,
    )

    # Log always
    try:
        from main import _autotrade_log
        _autotrade_log(f"[TradingView] {reason} (recovery #{recovery_count + 1}, fail_count={fail_count})")
    except Exception:
        pass

    if tv_disabled:
        try:
            from main import _autotrade_log as al
            al(f"[TradingView] CRITICAL: auto-recovery failed after {recovery_count + 1} attempts. "
               f"TradingView MCP disabled for 10 min. Last error: {last_error}")
        except Exception:
            pass
        _tv_alert_send(cfg, "critical", "TradingView ถูกปิดอัตโนมัติ",
                       f"Auto-recovery ล้มเหลวหลัง {recovery_count + 1} ครั้ง — TV ถูก disable 10 นาที\n"
                       f"last_error: {last_error}\n"
                       f"fail_count: {fail_count}")

    out = _commit_supervisor_config_tune(
        state,
        delegations,
        "tradingview_health",
        cfg,
        changes,
        reason,
    )
    # Preserve recovery tracking (commit overwrites delegations[key] with 3 fields)
    tv_rec_extra = {
        "recovery_count": recovery_count + 1,
        "last_error": last_error,
        "recovery_action": reason,
    }
    state = AUTO_TRADE.setdefault("supervisorAutoTune", {})
    delegations = state.setdefault("delegations", {})
    if isinstance(delegations.get("tradingview_health"), dict):
        delegations["tradingview_health"].update(tv_rec_extra)
    else:
        delegations["tradingview_health"] = {"at": int(time.time()), **tv_rec_extra}
    AUTO_TRADE["supervisorAutoTune"] = state

    out["severity"] = min(1.0, fail_count / 10.0)
    out["signature"] = signature
    out["health"] = health
    out["recovery_count"] = recovery_count + 1
    out["tv_disabled"] = tv_disabled
    return out


maybe_tune_tradingview_health = _maybe_tune_tradingview_health


def _recent_live_result_streak_state(*args, **kwargs):
    from trading.learning import _recent_live_result_streak_state as _fn
    return _fn(*args, **kwargs)


# ── Supervisor review functions (extracted from main.py) ─────────────────


def _supervisor_trade_period_reviews(trades: list[dict], *, now_ts: int | None = None) -> list[dict]:
    """Build performance reviews over rolling trade windows (8/20/50).

    Pure function — no side effects, no global state reads.
    """
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


def _maybe_tune_size_multiplier_from_streak(trades: list[dict], cfg: dict | None = None) -> dict:
    """Auto-tune supervisorSizeMultiplier based on recent win/loss streak.

    Reads streak state, computes a target multiplier, and commits the change
    via the standard supervisor tuning pipeline (cooldown, rollback, etc).
    """
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
    if active and str(rec.get("signature", "") or "") == signature:
        return {"applied": False, "alreadyTuned": True, "cooldownSec": cooldown_sec, "signature": signature, "streak": state}

    if _tuning_should_rollback("size_streak"):
        rollback = _tuning_rollback_last("size_streak")
        if rollback.get("reverted"):
            _commit_supervisor_config_tune(state_obj, delegations, "size_streak", cfg, _apply_rollback_old_values(cfg, rollback), "rollback_worsened")
            _tuning_mode_lock_release()  # Rollback clears the lock
            return {"applied": True, "rollback": True, "reason": "previous tuning worsened metrics"}

    # Gate on the size domain: weak_payoff tightens supervisorSizeMultiplier in
    # the same direction; a recent opposite-direction tune blocks a rapid flip.
    lock_mode = "loosening" if target >= 1.0 else "tightening"
    if not _tuning_mode_lock_acquire(lock_mode, f"size_streak:{reason}", cfg, domain="size"):
        return {"applied": False, "reason": "opposite_mode_active", "mode": lock_mode, "blockedBy": "opposite", "domain": "size", "signature": signature, "streak": state}

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
