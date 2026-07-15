"""Risk cooldown helpers: symbol-level cooldown tracking and pruning.

These helpers manage the ``riskCooldownBySymbol`` state to prevent repeated
entries after loss streaks or risk events.  All functions that mutate state
receive the ``AUTO_TRADE`` dict explicitly so they can be tested in isolation.
"""

from __future__ import annotations

import time


# ---------------------------------------------------------------------------
# Internal helpers (no mutation)
# ---------------------------------------------------------------------------

def _risk_cooldown_signature_last_ts(signature: str) -> int:
    latest = 0
    for part in str(signature or "").split("|"):
        bits = part.split(":")
        if len(bits) >= 3:
            try:
                latest = max(latest, int(float(bits[2])))
            except (TypeError, ValueError):
                pass
    return latest


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------

def _risk_cooldown_map(auto_trade: dict) -> dict:
    raw = auto_trade.get("riskCooldownBySymbol")
    if not isinstance(raw, dict):
        raw = {}
        auto_trade["riskCooldownBySymbol"] = raw
    return raw


def _prune_risk_cooldowns(auto_trade: dict, now: int | None = None) -> dict:
    now_i = int(now or time.time())
    raw = _risk_cooldown_map(auto_trade)
    kept = {}
    max_age_sec = 6 * 3600
    for sym, rec in raw.items():
        if not isinstance(rec, dict) or int(rec.get("until", 0) or 0) <= now_i:
            continue
        reason = str(rec.get("reason", "") or "")
        if reason in {"loss_streak", "legacy_global_risk_cooldown"}:
            last_ts = (
                int(rec.get("lastClosedAt", 0) or 0)
                or _risk_cooldown_signature_last_ts(str(rec.get("signature", "") or ""))
            )
            if last_ts and now_i - last_ts > max_age_sec:
                continue
        kept[str(sym).upper()] = rec
    auto_trade["riskCooldownBySymbol"] = kept
    return kept


def _risk_cooldown_symbols(auto_trade: dict, now: int | None = None) -> set[str]:
    return set(_prune_risk_cooldowns(auto_trade, now).keys())


def _symbol_risk_cooldown_record(auto_trade: dict, symbol: str, now: int | None = None) -> dict | None:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return None
    return _prune_risk_cooldowns(auto_trade, now).get(sym)


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def _arm_symbol_risk_cooldown(
    auto_trade: dict,
    symbol: str,
    minutes: int,
    signature: str,
    loss_streak: int,
    reason: str,
    now: int | None = None,
    last_closed_at: int = 0,
) -> dict | None:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return None
    now_i = int(now or time.time())
    until = now_i + max(1, int(minutes or 1)) * 60
    state = _prune_risk_cooldowns(auto_trade, now_i)
    rec = {
        "symbol": sym,
        "until": until,
        "at": now_i,
        "signature": str(signature or ""),
        "lossStreak": int(loss_streak or 0),
        "reason": str(reason or "risk_cooldown"),
        "lastClosedAt": int(last_closed_at or 0),
    }
    state[sym] = rec
    auto_trade["riskCooldownBySymbol"] = state
    return rec
