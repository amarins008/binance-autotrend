"""Shared read/write operations on the AUTO_TRADE runtime state.

These functions previously lived in ``main`` and were re-exposed to
``exchange.futures_orders`` / ``trading.live_guardian`` via ``_lazy_main``
delegates.  Keeping them here lets those modules depend on a real module
instead of the monolith, without changing behavior.

All functions mutate ``services.app_state.AUTO_TRADE`` in place — the same
canonical dict that ``main`` binds after startup reconciliation.
"""

from __future__ import annotations

import json
import time

from hermes_agents import ensure_agent_state, mark_agent
from services import app_state
from services.config_paths import SNAPSHOT_PATH, VAULT_DIR
from trading.risk_cooldown import _prune_risk_cooldowns


def autotrade_log(msg: str) -> None:
    """Append a timestamped entry to the top of the rotating autotrade log."""
    state = app_state.AUTO_TRADE
    state["log"] = ([{"ts": int(time.time()), "msg": msg}] + state["log"])[:100]


def agent_mark(
    agent_id: str,
    stage: str,
    action: str,
    reason: str = "",
    data: dict | None = None,
) -> None:
    """Mark an Hermes agent's stage on the shared kanban state."""
    state = app_state.AUTO_TRADE
    state["hermesAgents"] = mark_agent(
        state.get("hermesAgents"),
        agent_id,
        stage,
        action,
        reason,
        data,
    )


def last_decision_intel(
    symbol: str | None = None,
    max_age_sec: int | None = None,
) -> dict | None:
    """Return the stored intel snapshot for a symbol (or global fallback)."""
    state = app_state.AUTO_TRADE
    want = str(symbol or "").upper().strip() if symbol else ""
    if want:
        per_sym = state.get("lastDecisions")
        if isinstance(per_sym, dict):
            ld = per_sym.get(want)
            if isinstance(ld, dict):
                intel = ld.get("intel") if "intel" in ld else ld
                if isinstance(intel, dict):
                    if max_age_sec is not None:
                        try:
                            ts = int(ld.get("ts", 0) or intel.get("ts", 0) or 0)
                        except Exception:
                            ts = 0
                        if ts <= 0 or int(time.time()) - ts > int(max_age_sec):
                            return None
                    return intel
    ld = state.get("lastDecision")
    if not isinstance(ld, dict):
        return None
    intel = ld.get("intel") if "intel" in ld else ld
    if not isinstance(intel, dict):
        return None
    if want:
        got = str(intel.get("symbol") or ld.get("symbol") or "").upper().strip()
        if got and want and got != want:
            return None
    if max_age_sec is not None:
        try:
            ts = int(ld.get("ts", 0) or intel.get("ts", 0) or 0)
        except Exception:
            ts = 0
        if ts <= 0 or int(time.time()) - ts > int(max_age_sec):
            return None
    return intel


def entry_snapshot_from_intel(
    symbol: str | None,
    side: str | None,
    intel: dict | None = None,
) -> dict:
    intel = intel if isinstance(intel, dict) else last_decision_intel(symbol)
    if not isinstance(intel, dict):
        intel = {}
    candles = intel.get("candles") if isinstance(intel.get("candles"), dict) else {}
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    snap = {
        "entrySymbol": str(symbol or intel.get("symbol") or "").upper().strip(),
        "entrySide": str(side or intel.get("signal") or "").upper().strip(),
        "patternTags": candles.get("tags", []),
        "patternBias": float(candles.get("bias", 0.0) or 0.0),
        "patternScore": float(candles.get("score", 0.0) or 0.0),
        "entryConfidence": float(intel.get("confidence", 0.0) or 0.0),
        "entryScore": float(intel.get("score", intel.get("weightedScore", 0.0)) or 0.0),
        "entrySpreadBps": float(ex.get("spreadBps", 0.0) or 0.0),
        "entryMomentumPct": float(ex.get("momentumPct", 0.0) or 0.0),
        "entryDecisionAt": int(time.time()),
    }
    try:
        _tv = intel.get("tv") if isinstance(intel.get("tv"), dict) else {}
        if not _tv:
            try:
                _sym = str(symbol or intel.get("symbol") or "").upper().strip()
                _tv_path = VAULT_DIR / "symbols" / _sym / "tv_signal.json"
                if _tv_path.exists():
                    _tv_disk = json.loads(_tv_path.read_text(encoding="utf-8"))
                    if isinstance(_tv_disk, dict) and str(_tv_disk.get("signal", "") or "").strip():
                        _tv = {
                            "signal": str(_tv_disk.get("signal", "") or ""),
                            "confidence": float(_tv_disk.get("confidence", 0.0) or 0.0),
                            "strength": float(_tv_disk.get("strength", 0.0) or 0.0),
                            "age": int(max(0.0, time.time() - float(_tv_disk.get("ts", 0) or 0))),
                            "_source": "disk-fallback",
                        }
            except Exception:
                _tv = {}
        if _tv:
            snap["tvSignal"] = str(_tv.get("signal", "") or "")
            snap["tvConfidence"] = float(_tv.get("confidence", 0.0) or 0.0)
            snap["tvStrength"] = float(_tv.get("strength", 0.0) or 0.0)
            if _tv.get("age") is not None:
                snap["tvAge"] = int(_tv.get("age") or 0)
    except Exception:
        pass
    return snap


def persist_autotrade_snapshot(force: bool = False) -> None:
    """Persist the AUTO_TRADE runtime state to disk (throttled to 30 s unless forced)."""
    if not force:
        now = time.time()
        if (now - app_state._SNAPSHOT_LAST_FLUSH) < 30.0:
            return
    try:
        # Stale lock cleanup: remove locks whose updatedAt is > 2 hours old
        # (position was likely closed externally without Guardian noticing).
        _locks_data = app_state.AUTO_TRADE.get("liveProfitLocks")
        if isinstance(_locks_data, dict) and _locks_data:
            _now_ts = int(time.time())
            _stale_keys = [
                k for k, v in _locks_data.items()
                if isinstance(v, dict) and (_now_ts - int(v.get("updatedAt", 0) or 0)) > 7200
            ]
            if _stale_keys:
                for _sk in _stale_keys:
                    _locks_data.pop(_sk, None)
                autotrade_log(f"[Snapshot] Cleaned {len(_stale_keys)} stale lock(s): {', '.join(_stale_keys)}")
        payload = {
            "savedAt": int(time.time()),
            "paper": dict(app_state.AUTO_TRADE["paper"]),
            "config": app_state.AUTO_TRADE.get("config"),
            "running": bool(app_state.AUTO_TRADE.get("running")),
            "pauseUntil": int(app_state.AUTO_TRADE.get("pauseUntil", 0) or 0),
            "riskCooldownLossSignature": str(app_state.AUTO_TRADE.get("riskCooldownLossSignature", "") or ""),
            "riskCooldownBySymbol": _prune_risk_cooldowns(app_state.AUTO_TRADE),
            "riskCooldownLastMarketCheckAt": int(app_state.AUTO_TRADE.get("riskCooldownLastMarketCheckAt", 0) or 0),
            "sessionId": app_state.AUTO_TRADE.get("sessionId"),
            "startedAt": app_state.AUTO_TRADE.get("startedAt", 0),
            "lastTradeAt": app_state.AUTO_TRADE.get("lastTradeAt", 0),
            "liveProfitLocks": app_state.AUTO_TRADE.get("liveProfitLocks"),
            "scanBoard": list(app_state.AUTO_TRADE.get("scanBoard", []))[:10],
            "cooldownWatchlist": app_state.AUTO_TRADE.get("cooldownWatchlist") if isinstance(app_state.AUTO_TRADE.get("cooldownWatchlist"), dict) else {},
            "hermesAgents": ensure_agent_state(app_state.AUTO_TRADE.get("hermesAgents")),
            "hermesSupervisorReview": app_state.AUTO_TRADE.get("hermesSupervisorReview") if isinstance(app_state.AUTO_TRADE.get("hermesSupervisorReview"), dict) else {},
            "trades": list(app_state.AUTO_TRADE.get("trades", []))[-60:],
            "dailyRealizedPnlUSDT": app_state.DAILY_REALIZED_PNL,
            "dailyPnlDateKey": app_state._DAILY_PNL_DATE_KEY,
        }
        raw = json.dumps(payload, ensure_ascii=False, default=str)
        if len(raw.encode("utf-8")) > 2 * 1024 * 1024:
            print("[Snapshot] WARN: payload > 2MB, trimming trades further")
            payload["trades"] = list(app_state.AUTO_TRADE.get("trades", []))[-20:]
            raw = json.dumps(payload, ensure_ascii=False, default=str)
        tmp_path = SNAPSHOT_PATH.with_suffix(".tmp")
        tmp_path.write_text(raw, encoding="utf-8")
        tmp_path.replace(SNAPSHOT_PATH)
        app_state.AUTO_TRADE["_snapshot_saved_at"] = payload["savedAt"]
        app_state._SNAPSHOT_LAST_FLUSH = time.time()
        # Per-symbol runtime split: each symbol's cooldown state lives on its
        # own file so a single symbol cannot corrupt the shared global snapshot.
        try:
            from trading.per_symbol_context import PerSymbolContext
            from trading.shared_cache_layer import get_shared_cache
            _rc = app_state.AUTO_TRADE.get("riskCooldownBySymbol")
            if isinstance(_rc, dict) and _rc:
                _rcache = get_shared_cache(VAULT_DIR)
                for _sym, _cd in _rc.items():
                    if not isinstance(_cd, dict):
                        continue
                    _s = str(_sym).upper().strip()
                    if ":" in _s:
                        continue
                    try:
                        _ctx = PerSymbolContext(_s, _rcache, app_state.AUTO_TRADE.get("config"))
                        _rt = _ctx.get_runtime()
                        if not isinstance(_rt, dict):
                            _rt = {}
                        _rt["riskCooldown"] = _cd
                        _ctx.save_runtime(_rt)
                    except Exception:
                        continue
        except Exception:
            pass
    except Exception as exc:
        print(f"[Snapshot] ERROR writing {SNAPSHOT_PATH}: {exc}")