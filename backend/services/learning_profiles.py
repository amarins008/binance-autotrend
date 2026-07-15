"""Learning profile I/O, scan health tracking, and cooldown management.

All functions that read/write learning profiles are centralized here to avoid
duplication and make testing easier.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from obsidian_memory import ensure_trading_vault
from services.config_paths import TRADES_LOG_PATH, VAULT_DIR


# ---------------------------------------------------------------------------
# Vault helpers
# ---------------------------------------------------------------------------

def _ensure_vault() -> None:
    try:
        VAULT_DIR.mkdir(parents=True, exist_ok=True)
        ensure_trading_vault(VAULT_DIR)
    except Exception as exc:
        print(f"[Trade Log] ERROR writing {TRADES_LOG_PATH}: {exc}")


# ---------------------------------------------------------------------------
# Per-symbol helpers
# ---------------------------------------------------------------------------

def _load_single_profile(symbol: str) -> dict:
    """Load a single symbol's profile from per-symbol storage."""
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {}
    try:
        from trading.per_symbol_storage import PerSymbolStorage
        storage = PerSymbolStorage(VAULT_DIR, sym)
        profile = storage.load_profile()
        if profile:
            return profile
    except Exception:
        pass
    return {}


def _save_single_profile(symbol: str, profile: dict) -> None:
    """Save a single symbol's profile to per-symbol storage."""
    sym = str(symbol or "").upper().strip()
    if not sym:
        return
    try:
        from trading.per_symbol_storage import PerSymbolStorage
        storage = PerSymbolStorage(VAULT_DIR, sym)
        storage.save_profile(profile)
    except Exception:
        pass


def _cleanup_stale_profiles(cutoff_days: int = 90) -> int:
    """Remove per-symbol profiles with no trades and no updates within cutoff_days.
    Returns the number of profiles removed.
    """
    from trading.per_symbol_storage import PerSymbolStorage
    symbols_dir = VAULT_DIR / "symbols"
    if not symbols_dir.exists():
        return 0
    cutoff = time.time() - (cutoff_days * 86400)
    removed = 0
    for sym_dir in symbols_dir.iterdir():
        if not sym_dir.is_dir():
            continue
        profile_file = sym_dir / "profile.json"
        if not profile_file.exists():
            continue
        try:
            pr = json.loads(profile_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(pr, dict):
            continue
        last_trade = int(pr.get("updatedAt", 0) or 0)
        trades = int(pr.get("trades", 0) or 0)
        if trades == 0 and last_trade > 0 and last_trade < cutoff:
            profile_file.unlink(missing_ok=True)
            removed += 1
    if removed > 0:
        print(f"[Learning Profiles] Cleaned {removed} stale profiles (> {cutoff_days} days, no trades)")
    return removed


def _load_learning_profiles() -> dict:
    """Legacy: load all profiles from per-symbol directories."""
    _ensure_vault()
    symbols_dir = VAULT_DIR / "symbols"
    if not symbols_dir.exists():
        return {}
    result = {}
    for sym_dir in symbols_dir.iterdir():
        if not sym_dir.is_dir():
            continue
        profile_file = sym_dir / "profile.json"
        if not profile_file.exists():
            continue
        try:
            pr = json.loads(profile_file.read_text(encoding="utf-8"))
            if isinstance(pr, dict):
                result[sym_dir.name] = pr
        except Exception:
            pass
    return result


# ---------------------------------------------------------------------------
# Scan health
# ---------------------------------------------------------------------------

def _scan_health_state(symbol: str) -> dict[str, int]:
    sym = str(symbol or "").upper().strip()
    pr = _load_single_profile(sym)
    if not isinstance(pr, dict):
        return {"streak": 0, "cooldownUntil": 0, "lastErrorAt": 0, "lastSuccessAt": 0}
    return {
        "streak": max(0, int(pr.get("scanErrorStreak", 0) or 0)),
        "cooldownUntil": max(0, int(pr.get("scanCooldownUntil", 0) or 0)),
        "lastErrorAt": max(0, int(pr.get("lastScanErrorAt", 0) or 0)),
        "lastSuccessAt": max(0, int(pr.get("lastScanSuccessAt", 0) or 0)),
    }


def _record_scan_health(symbol: str, ok: bool, reason: str | None = None) -> None:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return
    pr = _load_single_profile(sym)
    if not pr:
        pr = {"wins": 0, "losses": 0, "realizedPnl": 0.0, "trades": 0}
    now = int(time.time())
    if ok:
        pr["scanErrorStreak"] = 0
        pr["scanCooldownUntil"] = 0
        pr["lastScanSuccessAt"] = now
    else:
        streak = int(pr.get("scanErrorStreak", 0) or 0) + 1
        pr["scanErrorStreak"] = streak
        pr["lastScanErrorAt"] = now
        # Back off repeated timeouts so the same symbol does not pin the scan head.
        if streak >= 2:
            pr["scanCooldownUntil"] = now + min(900, 45 * streak)
    if reason:
        pr["lastScanErrorReason"] = str(reason)[:120]
    pr["updatedAt"] = now
    _save_single_profile(sym, pr)


def _cooldown_scan_symbol(symbol: str, seconds: int, reason: str) -> None:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return
    pr = _load_single_profile(sym)
    if not pr:
        pr = {"wins": 0, "losses": 0, "realizedPnl": 0.0, "trades": 0}
    now = int(time.time())
    pr["scanErrorStreak"] = max(2, int(pr.get("scanErrorStreak", 0) or 0))
    pr["scanCooldownUntil"] = max(int(pr.get("scanCooldownUntil", 0) or 0), now + max(1, int(seconds or 1)))
    pr["lastScanErrorAt"] = now
    pr["lastScanErrorReason"] = str(reason or "symbol cooldown")[:120]
    pr["updatedAt"] = now
    _save_single_profile(sym, pr)


def _scan_error_penalty(symbol: str) -> float:
    hs = _scan_health_state(symbol)
    now = int(time.time())
    streak = max(0, int(hs.get("streak", 0) or 0))
    cooldown_until = max(0, int(hs.get("cooldownUntil", 0) or 0))
    last_error_at = max(0, int(hs.get("lastErrorAt", 0) or 0))
    penalty = float(streak) * 50.0
    if cooldown_until > now:
        penalty += 500.0 + float(cooldown_until - now) / 60.0
    elif last_error_at > 0:
        age_min = max(0.0, (now - last_error_at) / 60.0)
        penalty += max(0.0, 15.0 - min(15.0, age_min))
    return penalty
