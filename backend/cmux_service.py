from __future__ import annotations

import argparse
import ipaddress
import json
import os
import time
import threading
import socket
from urllib.parse import parse_qs, urlparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
import urllib.request
from dotenv import load_dotenv

import hermes_service

ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=ROOT / ".env", override=False)
DEFAULT_CONFIG = ROOT / "autotrade.standalone.json"
AUTOTRADE_SNAPSHOT_PATH = ROOT / "autotrade_snapshot.json"
CMUX_PORT = int(os.getenv("CMUX_PORT", "8030"))
CMUX_HOST = os.getenv("CMUX_HOST", "0.0.0.0")
STATE_DIR = ROOT / ".standalone"
TRAIN_REPORT_PATH = STATE_DIR / "learning_report.json"
LEARNING_APPLIED_PATH = STATE_DIR / "learning_applied.json"
BOT_DESIRED_STATE_PATH = STATE_DIR / "bot_desired.json"
LEARNING_SCHEDULER_ENABLED = os.getenv("LEARNING_SCHEDULER_ENABLED", "true").lower() == "true"
LEARNING_TRAIN_INTERVAL_SEC = int(os.getenv("LEARNING_TRAIN_INTERVAL_SEC", "86400"))
LEARNING_AUTO_APPLY_PROMOTED = os.getenv("LEARNING_AUTO_APPLY_PROMOTED", "true").lower() == "true"
LEARNING_TUNABLE_KEYS = (
    "minConfidence",
    "hybridMinScore",
    "hybridMinEdge",
    "adaptiveSizeBoostMaxPct",
    "maxOpenPositions",
    "takeProfitPct",
    "stopLossPct",
)
PUBLIC_IP_TTL_SEC = int(os.getenv("PUBLIC_IP_TTL_SEC", "60"))
PUBLIC_IP_TIMEOUT_SEC = float(os.getenv("PUBLIC_IP_TIMEOUT_SEC", "0.8"))
PUBLIC_IP_SOURCES = ("https://api.ipify.org",)
HERMES_WATCHDOG_INTERVAL_SEC = max(15, int(os.getenv("HERMES_WATCHDOG_INTERVAL_SEC", "30")))
HERMES_WATCHDOG_RETRY_GAP_SEC = max(30, int(os.getenv("HERMES_WATCHDOG_RETRY_GAP_SEC", "60")))
STATUS_CACHE_TTL_SEC = max(0.2, float(os.getenv("CMUX_STATUS_CACHE_TTL_SEC", "3.0")))
STATUS_CACHE_STALE_SEC = max(10.0, float(os.getenv("CMUX_STATUS_CACHE_STALE_SEC", "90.0")))
_PUBLIC_IP_STATE: dict[str, Any] = {
    "ip": None,
    "previousIp": None,
    "lastCheckedAt": 0,
    "changedAt": 0,
    "changed": False,
    "source": None,
    "error": None,
}
_PUBLIC_IP_REFRESHING = False
_PUBLIC_IP_LOCK = threading.Lock()
_LAST_LEARNING_APPLIED: dict[str, Any] = {}
_HERMES_WATCHDOG_LOCK = threading.Lock()
_HERMES_WATCHDOG_LAST_ATTEMPT = 0.0
_HERMES_WATCHDOG_LAST_HEALTHY = 0.0
_BOT_WATCHDOG_LAST_ATTEMPT = 0.0
_STATUS_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
_STATUS_REFRESHING = False
_STATUS_LOCK = threading.Lock()
_RICH_BOT_STATUS_CACHE: dict[str, Any] = {}


def _local_publish_hint() -> str:
    candidates: list[ipaddress.IPv4Address] = []
    try:
        infos = socket.getaddrinfo(socket.gethostname(), None, family=socket.AF_INET, type=socket.SOCK_STREAM)
        for info in infos:
            raw = str(info[4][0] or "")
            try:
                ip = ipaddress.ip_address(raw)
            except ValueError:
                continue
            if isinstance(ip, ipaddress.IPv4Address) and not ip.is_loopback:
                candidates.append(ip)
    except Exception:
        pass
    if not candidates:
        try:
            local = socket.gethostbyname(socket.gethostname())
            ip = ipaddress.ip_address(local)
            if isinstance(ip, ipaddress.IPv4Address) and not ip.is_loopback:
                candidates.append(ip)
        except Exception:
            pass
    for network in (
        ipaddress.ip_network("192.168.0.0/16"),
        ipaddress.ip_network("10.0.0.0/8"),
        ipaddress.ip_network("172.16.0.0/12"),
    ):
        for ip in candidates:
            if ip in network:
                return str(ip)
    for ip in candidates:
        if ip.is_private:
            return str(ip)
    if candidates:
        return str(candidates[0])
    return "127.0.0.1"


def _fetch_public_ip() -> tuple[str | None, str | None, str | None]:
    for src in PUBLIC_IP_SOURCES:
        try:
            with urllib.request.urlopen(src, timeout=PUBLIC_IP_TIMEOUT_SEC) as r:
                txt = r.read().decode("utf-8", errors="ignore").strip()
                ip = txt.splitlines()[0].strip() if txt else ""
                if ip and len(ip) <= 64:
                    return ip, src, None
        except Exception as exc:
            last_err = str(exc)
    return None, None, locals().get("last_err", "unknown error")


def _public_ip_status(force: bool = False) -> dict[str, Any]:
    global _PUBLIC_IP_REFRESHING
    now = int(time.time())
    cache_fresh = (now - int(_PUBLIC_IP_STATE.get("lastCheckedAt", 0) or 0)) < max(5, PUBLIC_IP_TTL_SEC)
    if not force and cache_fresh:
        return {
            "ip": _PUBLIC_IP_STATE.get("ip"),
            "previousIp": _PUBLIC_IP_STATE.get("previousIp"),
            "changed": bool(_PUBLIC_IP_STATE.get("changed", False)),
            "changedAt": _PUBLIC_IP_STATE.get("changedAt"),
            "lastCheckedAt": _PUBLIC_IP_STATE.get("lastCheckedAt"),
            "source": _PUBLIC_IP_STATE.get("source"),
            "error": _PUBLIC_IP_STATE.get("error"),
            "localHint": _local_publish_hint(),
            "publishUrlHint": f"http://{_local_publish_hint()}:8040",
        }
    # Never block dashboard/status request path on external public-IP lookup.
    # Refresh in background when cache is stale.
    if not force and not cache_fresh:
        with _PUBLIC_IP_LOCK:
            if not _PUBLIC_IP_REFRESHING:
                _PUBLIC_IP_REFRESHING = True
                def _bg_refresh() -> None:
                    global _PUBLIC_IP_REFRESHING
                    try:
                        _public_ip_status(force=True)
                    finally:
                        with _PUBLIC_IP_LOCK:
                            _PUBLIC_IP_REFRESHING = False
                threading.Thread(target=_bg_refresh, daemon=True).start()
        return {
            "ip": _PUBLIC_IP_STATE.get("ip"),
            "previousIp": _PUBLIC_IP_STATE.get("previousIp"),
            "changed": bool(_PUBLIC_IP_STATE.get("changed", False)),
            "changedAt": _PUBLIC_IP_STATE.get("changedAt"),
            "lastCheckedAt": _PUBLIC_IP_STATE.get("lastCheckedAt"),
            "source": _PUBLIC_IP_STATE.get("source"),
            "error": _PUBLIC_IP_STATE.get("error"),
            "localHint": _local_publish_hint(),
            "publishUrlHint": f"http://{_local_publish_hint()}:8040",
        }
    ip, src, err = _fetch_public_ip()
    prev = _PUBLIC_IP_STATE.get("ip")
    changed = bool(ip and prev and ip != prev)
    if changed:
        _PUBLIC_IP_STATE["previousIp"] = prev
        _PUBLIC_IP_STATE["changedAt"] = now
    _PUBLIC_IP_STATE["changed"] = changed
    _PUBLIC_IP_STATE["ip"] = ip or prev
    _PUBLIC_IP_STATE["source"] = src or _PUBLIC_IP_STATE.get("source")
    _PUBLIC_IP_STATE["error"] = err
    _PUBLIC_IP_STATE["lastCheckedAt"] = now
    return {
        "ip": _PUBLIC_IP_STATE.get("ip"),
        "previousIp": _PUBLIC_IP_STATE.get("previousIp"),
        "changed": bool(_PUBLIC_IP_STATE.get("changed", False)),
        "changedAt": _PUBLIC_IP_STATE.get("changedAt"),
        "lastCheckedAt": _PUBLIC_IP_STATE.get("lastCheckedAt"),
        "source": _PUBLIC_IP_STATE.get("source"),
        "error": _PUBLIC_IP_STATE.get("error"),
        "localHint": _local_publish_hint(),
        "publishUrlHint": f"http://{_local_publish_hint()}:8040",
    }


def _normalize_symbol_for_learning(payload: dict[str, Any]) -> str | None:
    raw = str(payload.get("symbol", "") or "").upper().strip()
    if raw in ("", "AUTO", "SCAN"):
        return None
    return raw


def _apply_learning_tuning(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    tuned = dict(payload)
    sym = _normalize_symbol_for_learning(payload)
    report: dict[str, Any] = {"enabled": True, "symbol": sym, "applied": False}
    proposal = hermes_service.learning_propose_config(sym)
    if not isinstance(proposal, dict) or "proposed" not in proposal:
        report["reason"] = proposal.get("error", "proposal unavailable") if isinstance(proposal, dict) else "proposal unavailable"
        return tuned, report
    proposed = proposal.get("proposed") if isinstance(proposal.get("proposed"), dict) else {}
    # Merge only known keys from propose-config output.
    for k in LEARNING_TUNABLE_KEYS:
        if k in proposed:
            tuned[k] = proposed[k]
    report["applied"] = True
    report["winRatePct"] = proposal.get("winRatePct")
    report["trades"] = proposal.get("trades")
    report["reasons"] = proposal.get("reasons", [])
    report["proposed"] = proposed
    return tuned, report


def _learning_patch_from_promotion(promotion: dict[str, Any]) -> dict[str, Any]:
    proposed = promotion.get("proposed") if isinstance(promotion.get("proposed"), dict) else {}
    patch: dict[str, Any] = {}
    for key in LEARNING_TUNABLE_KEYS:
        if key in proposed:
            patch[key] = proposed[key]
    return patch


def _select_learning_promotion(report: dict[str, Any], current_symbol: str | None = None) -> dict[str, Any] | None:
    promoted = report.get("promoted") if isinstance(report.get("promoted"), list) else []
    usable = [p for p in promoted if isinstance(p, dict) and _learning_patch_from_promotion(p)]
    if not usable:
        return None
    cur = str(current_symbol or "").upper().strip()
    if cur and cur not in ("AUTO", "SCAN"):
        for item in usable:
            if str(item.get("symbol", "")).upper().strip() == cur:
                return item
    usable.sort(key=lambda item: float(item.get("hitRatePct", 0.0) or 0.0), reverse=True)
    return usable[0]


def _auto_apply_promoted_learning(report: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    if not LEARNING_AUTO_APPLY_PROMOTED or payload.get("autoApply") is False:
        return {"enabled": LEARNING_AUTO_APPLY_PROMOTED, "applied": False, "reason": "disabled"}
    if not isinstance(report, dict) or not report.get("ok"):
        return {"enabled": True, "applied": False, "reason": "train_report_not_ok"}
    if int(report.get("promotedCount", 0) or 0) <= 0:
        return {"enabled": True, "applied": False, "reason": "no_promoted_symbols"}

    bot_status = hermes_service.autotrade_status_quick()
    cfg = bot_status.get("config") if isinstance(bot_status.get("config"), dict) else {}
    if not bool(bot_status.get("running")):
        return {"enabled": True, "applied": False, "reason": "bot_not_running"}
    if str(cfg.get("executionMode", "")).upper() != "LIVE":
        return {"enabled": True, "applied": False, "reason": "not_live_mode", "mode": cfg.get("executionMode")}

    selected = _select_learning_promotion(report, cfg.get("symbol"))
    if not selected:
        return {"enabled": True, "applied": False, "reason": "no_usable_promotion"}
    patch = _learning_patch_from_promotion(selected)
    update_rs = hermes_service.autotrade_update_config(patch)
    applied = bool(isinstance(update_rs, dict) and update_rs.get("ok"))
    out = {
        "enabled": True,
        "applied": applied,
        "symbol": selected.get("symbol"),
        "hitRatePct": selected.get("hitRatePct"),
        "patch": patch,
        "update": update_rs,
    }
    if not applied:
        out["reason"] = "update_config_failed"
    return out


def _save_train_report(report: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    TRAIN_REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def _empty_train_report(detail: str) -> dict[str, Any]:
    return {
        "ok": True,
        "detail": detail,
        "trainedAt": 0,
        "symbolsScanned": 0,
        "symbols": [],
        "results": [],
        "promoted": [],
        "promotedCount": 0,
        "promoteHitRatePct": 0.0,
        "autoApply": {"enabled": LEARNING_AUTO_APPLY_PROMOTED, "applied": False, "reason": detail},
    }


def _load_train_report() -> dict[str, Any]:
    if not TRAIN_REPORT_PATH.exists():
        return _empty_train_report("no_learning_report_yet")
    try:
        return json.loads(TRAIN_REPORT_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        return _empty_train_report(f"invalid_report_file: {exc}")


def _save_learning_applied(payload: dict[str, Any]) -> None:
    global _LAST_LEARNING_APPLIED
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        LEARNING_APPLIED_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        _LAST_LEARNING_APPLIED = dict(payload)
    except Exception:
        pass


def _load_learning_applied() -> dict[str, Any]:
    global _LAST_LEARNING_APPLIED
    if _LAST_LEARNING_APPLIED:
        return dict(_LAST_LEARNING_APPLIED)
    if not LEARNING_APPLIED_PATH.exists():
        return {}
    try:
        _LAST_LEARNING_APPLIED = json.loads(LEARNING_APPLIED_PATH.read_text(encoding="utf-8"))
        return dict(_LAST_LEARNING_APPLIED) if isinstance(_LAST_LEARNING_APPLIED, dict) else {}
    except Exception:
        return {}


def _save_bot_desired_state(running: bool, config: dict[str, Any] | None = None, reason: str = "") -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {
            "running": bool(running),
            "updatedAt": int(time.time()),
            "reason": str(reason or ""),
        }
        if running and isinstance(config, dict):
            payload["config"] = dict(config)
        BOT_DESIRED_STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _load_bot_desired_state() -> dict[str, Any]:
    if not BOT_DESIRED_STATE_PATH.exists():
        return {"running": False}
    try:
        data = json.loads(BOT_DESIRED_STATE_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {"running": False}
    except Exception:
        return {"running": False, "error": "invalid_desired_state"}


def _bot_watchdog_maybe_resume(bot_status: dict[str, Any] | None = None, *, reason: str = "") -> dict[str, Any]:
    global _BOT_WATCHDOG_LAST_ATTEMPT
    desired = _load_bot_desired_state()
    if not bool(desired.get("running")):
        return {"attempted": False, "reason": "desired_not_running"}
    status_source = str((bot_status or {}).get("source", "") or "").lower()
    transient_status = (
        status_source in {"timeout", "cache", "stale_cache"}
        or bool((bot_status or {}).get("softStale"))
        or str(reason or "").lower() in {"timeout", "cache", "stale_cache"}
    )
    if transient_status:
        return {
            "attempted": False,
            "reason": "transient_status_timeout",
            "source": status_source or str(reason or ""),
        }
    cfg = desired.get("config")
    if not isinstance(cfg, dict) or not cfg.get("symbol"):
        return {"attempted": False, "reason": "missing_desired_config"}
    now = time.time()
    with _HERMES_WATCHDOG_LOCK:
        if (now - _BOT_WATCHDOG_LAST_ATTEMPT) < HERMES_WATCHDOG_RETRY_GAP_SEC:
            return {"attempted": False, "reason": "retry_gap"}
        _BOT_WATCHDOG_LAST_ATTEMPT = now
    start_rs = hermes_service.start(timeout_sec=min(18.0, float(HERMES_WATCHDOG_INTERVAL_SEC)))
    if not isinstance(start_rs, dict) or not start_rs.get("ok"):
        return {"attempted": True, "ok": False, "step": "hermes.start", "service": start_rs}
    bot_rs = hermes_service.autotrade_start(dict(cfg))
    ok = isinstance(bot_rs, dict) and bool(bot_rs.get("ok", bot_rs.get("running", False)))
    return {
        "attempted": True,
        "ok": ok,
        "reason": str(reason or (bot_status or {}).get("source") or "bot_not_running"),
        "bot": bot_rs,
    }


def _hermes_watchdog_loop() -> None:
    """Keep Hermes alive without blocking the dashboard request path."""
    global _HERMES_WATCHDOG_LAST_ATTEMPT, _HERMES_WATCHDOG_LAST_HEALTHY
    while True:
        try:
            st = hermes_service.status_quick()
            if bool(st.get("healthy")):
                _HERMES_WATCHDOG_LAST_HEALTHY = time.time()
                bot_status = hermes_service.autotrade_status_quick()
                if not bool(bot_status.get("running")):
                    resume_rs = _bot_watchdog_maybe_resume(bot_status, reason=str(bot_status.get("source") or bot_status.get("error") or "not_running"))
                    if resume_rs.get("attempted"):
                        print(f"[watchdog] Bot not running -> resume attempt: {resume_rs}")
                time.sleep(HERMES_WATCHDOG_INTERVAL_SEC)
                continue

            now = time.time()
            with _HERMES_WATCHDOG_LOCK:
                if (now - _HERMES_WATCHDOG_LAST_ATTEMPT) < HERMES_WATCHDOG_RETRY_GAP_SEC:
                    time.sleep(HERMES_WATCHDOG_INTERVAL_SEC)
                    continue
                _HERMES_WATCHDOG_LAST_ATTEMPT = now

            print("[watchdog] Hermes unhealthy -> restart attempt")
            start_rs = hermes_service.start(timeout_sec=min(18.0, float(HERMES_WATCHDOG_INTERVAL_SEC)))
            if isinstance(start_rs, dict) and start_rs.get("ok"):
                print("[watchdog] Hermes restart ok")
                _HERMES_WATCHDOG_LAST_HEALTHY = time.time()
                resume_rs = _bot_watchdog_maybe_resume(reason="hermes_restarted")
                if resume_rs.get("attempted"):
                    print(f"[watchdog] Bot resume after Hermes restart: {resume_rs}")
        except Exception:
            pass
        time.sleep(HERMES_WATCHDOG_INTERVAL_SEC)


def _run_learning_train(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    start_rs = hermes_service.start()
    if not start_rs.get("ok"):
        return {"ok": False, "step": "hermes.start", "service": start_rs}

    symbols = payload.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        status = hermes_service.learning_status()
        items = status.get("items", []) if isinstance(status, dict) else []
        symbols = [str(x.get("symbol", "")).upper() for x in items if isinstance(x, dict) and x.get("symbol")]
    if not isinstance(symbols, list) or not symbols:
        bot_status = hermes_service.autotrade_status_quick()
        cfg = bot_status.get("config") if isinstance(bot_status.get("config"), dict) else {}
        scan_board = bot_status.get("scanBoard") if isinstance(bot_status.get("scanBoard"), list) else []
        open_positions = bot_status.get("openLivePositions") if isinstance(bot_status.get("openLivePositions"), list) else []
        fallback_symbols: list[str] = []
        for src in (
            cfg.get("symbol"),
            cfg.get("primarySymbol"),
            *[x.get("symbol") for x in scan_board if isinstance(x, dict)],
            *[x.get("symbol") for x in open_positions if isinstance(x, dict)],
        ):
            sym = str(src or "").upper().strip()
            if sym and sym not in ("AUTO", "SCAN"):
                fallback_symbols.append(sym)
        symbols = fallback_symbols
    symbols = sorted({s for s in symbols if s and s not in ("AUTO", "SCAN")})
    max_symbols = int(payload.get("maxSymbols", 20))
    symbols = symbols[: max(1, max_symbols)]

    # Adaptive promote threshold: if previous run produced 0 promotions,
    # lower the bar a bit so the system can actually start applying learning.
    base_hit_rate = float(payload.get("promoteHitRatePct", 55.0))
    promote_hit_rate = base_hit_rate
    try:
        last = _load_train_report() if callable(_load_train_report) else None
        if isinstance(last, dict) and int(last.get("promotedCount", 0) or 0) <= 0:
            promote_hit_rate = max(45.0, base_hit_rate - 8.0)
    except Exception:
        pass

    results = []
    promoted = []
    for sym in symbols:
        proposal = hermes_service.learning_propose_config(sym)
        wf = hermes_service.learning_walk_forward(sym, mode=str(payload.get("mode", "ALL")), train_size=int(payload.get("trainSize", 30)), test_size=int(payload.get("testSize", 10)))
        hit_rate = float((wf.get("walkForwardHitRatePct", 0.0) if isinstance(wf, dict) else 0.0) or 0.0)
        proposed_cfg = proposal.get("proposed", {}) if isinstance(proposal, dict) else {}
        row = {
            "symbol": sym,
            "proposalOk": isinstance(proposal, dict) and "proposed" in proposal,
            "walkForwardOk": bool(isinstance(wf, dict) and wf.get("ok")),
            "walkForwardHitRatePct": hit_rate,
            "proposal": proposed_cfg,
            "proposalReasons": proposal.get("reasons", []) if isinstance(proposal, dict) else [],
            "walkForwardRecommendation": wf.get("recommendation", {}) if isinstance(wf, dict) else {},
        }
        if row["proposalOk"] and row["walkForwardOk"] and hit_rate >= promote_hit_rate:
            promoted.append(
                {
                    "symbol": sym,
                    "hitRatePct": hit_rate,
                    "recommended": row["walkForwardRecommendation"],
                    "proposed": proposed_cfg,
                }
            )
        results.append(row)

    report = {
        "ok": True,
        "trainedAt": int(time.time()),
        "scheduler": bool(payload.get("_scheduler", False)),
        "symbolsScanned": len(symbols),
        "symbols": symbols,
        "promoteHitRatePct": promote_hit_rate,
        "promotedCount": len(promoted),
        "promoted": promoted,
        "results": results,
    }
    report["autoApply"] = _auto_apply_promoted_learning(report, payload)
    _save_train_report(report)
    return report


def _scheduler_loop() -> None:
    while True:
        try:
            _run_learning_train({"_scheduler": True})
        except Exception:
            pass
        time.sleep(max(300, LEARNING_TRAIN_INTERVAL_SEC))


def _has_hermes_agents(bot_status: dict[str, Any] | None) -> bool:
    agents = ((bot_status or {}).get("hermesAgents") or {}).get("agents")
    return isinstance(agents, dict) and bool(agents)


def _merge_rich_bot_status(bot_status: dict[str, Any]) -> dict[str, Any]:
    """Keep dashboard-only rich fields stable across status-lite timeouts."""
    global _RICH_BOT_STATUS_CACHE
    if _has_hermes_agents(bot_status):
        _RICH_BOT_STATUS_CACHE = {
            key: bot_status.get(key)
            for key in (
                "hermesAgents",
                "hermesSupervisorReview",
                "scanBoard",
                "openLivePositions",
                "log",
                "lastDecision",
            )
            if key in bot_status
        }
        return bot_status
    if not _has_hermes_agents(_RICH_BOT_STATUS_CACHE):
        try:
            snap = json.loads(AUTOTRADE_SNAPSHOT_PATH.read_text(encoding="utf-8"))
        except Exception:
            snap = {}
        if _has_hermes_agents(snap):
            _RICH_BOT_STATUS_CACHE = {
                key: snap.get(key)
                for key in (
                    "hermesAgents",
                    "hermesSupervisorReview",
                    "scanBoard",
                    "openLivePositions",
                    "log",
                    "lastDecision",
                )
                if key in snap
            }
    if not _has_hermes_agents(_RICH_BOT_STATUS_CACHE):
        return bot_status
    out = dict(bot_status)
    for key, value in _RICH_BOT_STATUS_CACHE.items():
        if key not in out or out.get(key) in (None, [], {}):
            out[key] = value
    out["richStatusCached"] = True
    return out


def _build_status_payload_live() -> dict[str, Any]:
    """Fast status for dashboard polling — avoid blocking on slow network/hermes."""
    learning_report = _load_train_report()
    learning_applied = _load_learning_applied()
    learning_summary = {
        "lastApplied": learning_applied,
        "lastTrain": {
            "ok": bool(learning_report.get("ok")) if isinstance(learning_report, dict) else False,
            "trainedAt": learning_report.get("trainedAt") if isinstance(learning_report, dict) else None,
            "promotedCount": learning_report.get("promotedCount") if isinstance(learning_report, dict) else 0,
            "symbolsScanned": learning_report.get("symbolsScanned") if isinstance(learning_report, dict) else 0,
        },
    }
    try:
        hermes_quick = hermes_service.status_quick()
    except Exception as exc:
        hermes_quick = {
            "ok": False,
            "name": "Hermes",
            "running": False,
            "healthy": False,
            "error": f"status_quick failed: {exc}",
        }
    hermes_down = not bool(hermes_quick.get("healthy"))
    try:
        bot_status = hermes_service.autotrade_status_quick()
    except Exception as exc:
        bot_status = {
            "ok": False,
            "running": False,
            "error": f"autotrade_status_quick failed: {exc}",
            "stale": True,
        }

    if hermes_down and isinstance(bot_status, dict):
        bot_status["stale"] = True
        bot_status["staleReason"] = "hermes_unhealthy"
        bot_status["source"] = bot_status.get("source") or "stale_cache"
        bot_status["warning"] = "Hermes is down; showing cached bot state"
    desired_state = _load_bot_desired_state()
    if isinstance(bot_status, dict):
        bot_status["desiredRunning"] = bool(desired_state.get("running"))
        if bool(desired_state.get("running")) and not bool(bot_status.get("running")) and str(bot_status.get("source", "")) == "timeout":
            bot_status["ok"] = True
            bot_status["running"] = True
            bot_status["stale"] = True
            bot_status["staleReason"] = "status_timeout_desired_running"
            bot_status["statusUnknown"] = True
            bot_status["warning"] = "Bot status timed out while desired running; keeping desired-running state until live status confirms stopped"
        bot_status = _merge_rich_bot_status(bot_status)

    bot_soft_stale = isinstance(bot_status, dict) and bool(bot_status.get("softStale"))
    bot_hard_stale = isinstance(bot_status, dict) and bool(bot_status.get("stale")) and not bot_soft_stale

    # Per-symbol profiles so the dashboard "Symbol Profile (3-tier)" table has
    # something to render. Loaded lazily and quietly — never block the cycle.
    symbol_profiles_payload: dict[str, Any] = {}
    try:
        import main as _main  # local import to avoid circular dependency at module load
        _profiles = _main._load_symbol_profiles() or {}
        if isinstance(_profiles, dict):
            symbol_profiles_payload = _profiles
    except Exception:
        symbol_profiles_payload = {}

    out = {
        "ok": True,
        "cmux": {"running": True, "port": CMUX_PORT},
        "network": {"publicIp": _public_ip_status(force=False)},
        "hermes": hermes_quick,
        "bot": bot_status,
        "botDesired": {
            "running": bool(desired_state.get("running")),
            "updatedAt": desired_state.get("updatedAt", 0),
            "reason": desired_state.get("reason", ""),
        },
        "learning": learning_summary,
    "symbolProfiles": symbol_profiles_payload,
        "stale": bool(hermes_down or bot_hard_stale),
    }
    with _STATUS_LOCK:
        _STATUS_CACHE["ts"] = time.time()
        _STATUS_CACHE["data"] = dict(out)
    return out


def _refresh_status_cache_background() -> None:
    global _STATUS_REFRESHING
    try:
        _build_status_payload_live()
    except Exception:
        pass
    finally:
        with _STATUS_LOCK:
            _STATUS_REFRESHING = False


def _build_status_payload() -> dict[str, Any]:
    """Dashboard status with stale-while-refresh cache to keep UI responsive."""
    global _STATUS_REFRESHING
    now = time.time()
    with _STATUS_LOCK:
        cached = _STATUS_CACHE.get("data")
        age = now - float(_STATUS_CACHE.get("ts", 0.0) or 0.0)
        if isinstance(cached, dict) and age <= STATUS_CACHE_TTL_SEC:
            out = dict(cached)
            out["cmuxStatusCache"] = {"hit": True, "ageSec": round(age, 2), "refreshing": bool(_STATUS_REFRESHING)}
            return out
        if isinstance(cached, dict):
            if not _STATUS_REFRESHING:
                _STATUS_REFRESHING = True
                threading.Thread(target=_refresh_status_cache_background, daemon=True).start()
            out = dict(cached)
            out["cmuxStatusCache"] = {
                "hit": True,
                "stale": True,
                "tooOld": age > STATUS_CACHE_STALE_SEC,
                "ageSec": round(age, 2),
                "refreshing": True,
            }
            out["stale"] = False
            bot = out.get("bot")
            if isinstance(bot, dict):
                bot["softStale"] = True
                bot["stale"] = False
                bot["source"] = bot.get("source") or "cmux_cache"
            return out
        _STATUS_REFRESHING = True
    try:
        return _build_status_payload_live()
    finally:
        with _STATUS_LOCK:
            _STATUS_REFRESHING = False


def dispatch_get(path: str, query: dict[str, list[str]] | None = None) -> tuple[int, dict[str, Any]]:
    qs = query or {}
    if path == "/health":
        return 200, {"ok": True, "role": "cmux", "port": CMUX_PORT}
    if path in ("/status", "/status/quick"):
        return 200, _build_status_payload()
    if path == "/learning/report":
        return 200, _load_train_report()
    if path == "/bot/precheck-live":
        symbol = str((qs.get("symbol", ["BTCUSDT"])[0] or "BTCUSDT")).strip().upper()
        if symbol in ("", "AUTO", "SCAN"):
            symbol = "BTCUSDT"
        return 200, {"ok": True, "precheck": hermes_service.autotrade_precheck_live(symbol)}
    return 404, {"ok": False, "detail": "not found"}


def dispatch_post(path: str, payload: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
    data = dict(payload or {})

    if path == "/service/start":
        return 200, hermes_service.start()
    if path == "/service/stop":
        return 200, hermes_service.stop()
    if path == "/bot/start":
        start_rs = hermes_service.start()
        if not start_rs.get("ok"):
            return 500, {"ok": False, "step": "hermes.start", "service": start_rs}
        mode = str(data.get("executionMode", "PAPER") or "PAPER").upper()
        precheck_warning = None
        if mode == "LIVE":
            sym = str(data.get("symbol", "BTCUSDT") or "BTCUSDT").upper()
            if sym in ("AUTO", "SCAN", ""):
                sym = "BTCUSDT"
            pre = hermes_service.autotrade_precheck_live(sym)
            if not pre.get("ok"):
                precheck_warning = {
                    "code": "LIVE_PRECHECK_FAILED_CONTINUE",
                    "message": "LIVE pre-check failed; continue start with caution",
                    "precheck": pre,
                }
        auto_learn = bool(data.pop("autoLearn", True))
        tuned_payload = dict(data)
        tuning_report = {"enabled": False, "applied": False}
        if auto_learn:
            tuned_payload, tuning_report = _apply_learning_tuning(data)
        out = {
            "ok": True,
            "service": hermes_service.status(),
            "learning": tuning_report,
            "bot": hermes_service.autotrade_start(tuned_payload),
        }
        if isinstance(out.get("bot"), dict) and out["bot"].get("ok", out["bot"].get("running", False)):
            _save_bot_desired_state(True, tuned_payload, "bot_start")
        _save_learning_applied(
            {
                "ts": int(time.time()),
                "symbol": tuning_report.get("symbol"),
                "applied": bool(tuning_report.get("applied")),
                "proposed": tuning_report.get("proposed", {}),
                "reasons": tuning_report.get("reasons", []),
                "winRatePct": tuning_report.get("winRatePct"),
                "trades": tuning_report.get("trades"),
            }
        )
        if precheck_warning:
            out["warning"] = precheck_warning
        return 200, out
    if path == "/bot/stop":
        _save_bot_desired_state(False, None, "bot_stop")
        return 200, {"ok": True, "bot": hermes_service.autotrade_stop(data), "service": hermes_service.status()}
    if path == "/bot/config":
        bot_rs = hermes_service.autotrade_update_config(data)
        # Compatibility fallback:
        # older Hermes builds may not expose /autotrade/config (HTTP 404).
        if isinstance(bot_rs, dict) and (not bot_rs.get("ok")) and ("HTTP 404" in str(bot_rs.get("error", ""))):
            st = hermes_service.autotrade_status()
            cur = dict(st.get("config") or {})
            if cur:
                cur.update(data or {})
                bot_rs = hermes_service.autotrade_start(cur)
                if isinstance(bot_rs, dict) and bot_rs.get("ok"):
                    bot_rs = {
                        "ok": True,
                        "updated": True,
                        "fallback": "autotrade/start",
                        "config": cur,
                    }
        if isinstance(bot_rs, dict) and bot_rs.get("ok"):
            desired = _load_bot_desired_state()
            if bool(desired.get("running")):
                cfg = desired.get("config") if isinstance(desired.get("config"), dict) else {}
                cfg.update(data or {})
                _save_bot_desired_state(True, cfg, "bot_config_update")
        return 200, {"ok": True, "bot": bot_rs, "service": hermes_service.status()}
    if path == "/bot/close-orphan":
        return 200, {"ok": True, "bot": hermes_service.autotrade_close_orphan(data), "service": hermes_service.status()}
    if path == "/bot/update-sl-tp":
        out = hermes_service.autotrade_update_config(
            {
                "takeProfitPct": data.get("takeProfitPct"),
                "stopLossPct": data.get("stopLossPct"),
            }
        )
        # Call dedicated endpoint when available for immediate guardian update.
        try:
            import urllib.request, json as _json

            req2 = urllib.request.Request(
                f"{hermes_service.HERMES_BASE}/autotrade/update-sl-tp",
                data=_json.dumps(data, ensure_ascii=False).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req2, timeout=20.0) as r2:
                out = _json.loads(r2.read().decode("utf-8"))
        except Exception:
            pass
        return 200, {"ok": True, "bot": out, "service": hermes_service.status()}
    if path == "/bot/adopt-live":
        start_rs = hermes_service.start()
        if not start_rs.get("ok"):
            return 500, {"ok": False, "step": "hermes.start", "service": start_rs}
        return 200, {"ok": True, "bot": hermes_service.autotrade_adopt_live(data), "service": hermes_service.status()}
    if path == "/learning/train-now":
        report = _run_learning_train(data)
        if isinstance(report, dict) and report.get("ok"):
            auto_apply = report.get("autoApply") if isinstance(report.get("autoApply"), dict) else {}
            _save_learning_applied(
                {
                    "ts": int(time.time()),
                    "symbol": auto_apply.get("symbol"),
                    "applied": bool(auto_apply.get("applied")),
                    "note": "train_now_completed",
                    "trainedAt": report.get("trainedAt"),
                    "promotedCount": report.get("promotedCount", 0),
                    "hitRatePct": auto_apply.get("hitRatePct"),
                    "patch": auto_apply.get("patch", {}),
                    "reason": auto_apply.get("reason"),
                }
            )
        return 200, report
    return 404, {"ok": False, "detail": "not found"}


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: object) -> None:
        return

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        return json.loads(raw) if raw else {}

    def _write(self, code: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            return

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    # Paths that are cmux API — never serve as files.
    _API_PATHS = frozenset([
        "/health", "/status", "/status/quick", "/status-lite",
        "/autotrade/status-lite", "/learning/report", "/learning/train-now",
        "/bot/precheck-live", "/bot/start", "/bot/stop", "/bot/config",
        "/service/start", "/service/stop",
        "/symbol/profile", "/hermes/symbol/profile",
        "/intel/rank", "/analyze", "/intel/analyze",
    ])

    # Serve HTML/dashboard assets for any path NOT starting with these API prefixes
    _SERVE_FILE_PREFIXES = (
        "/health", "/status", "/autotrade", "/learning",
        "/bot/", "/service", "/symbol", "/hermes", "/intel",
        "/analyze", "/api/",
    )

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        qs = parse_qs(parsed.query or "")

        # If it's an API path, call dispatch. Otherwise serve a file.
        if any(path.startswith(p) for p in self._SERVE_FILE_PREFIXES):
            code, response = dispatch_get(path, qs)
            self._write(code, response)
        else:
            self._serve_file(path)

    def _serve_file(self, path: str) -> None:
        """Serve a file from the dashboard directory."""
        if path == "/" or not path:
            file_path = ROOT / "dashboard" / "index.html"
        else:
            # Prevent path traversal
            safe = path.lstrip("/").replace("..", "")
            file_path = ROOT / "dashboard" / safe

        if not file_path.is_file():
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not Found")
            return

        ext = file_path.suffix.lower()
        ctype = {
            ".html": "text/html; charset=utf-8",
            ".js": "application/javascript",
            ".css": "text/css",
            ".json": "application/json",
            ".png": "image/png",
            ".ico": "image/x-icon",
        }.get(ext, "application/octet-stream")

        content = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.end_headers()
        self.wfile.write(content)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        payload = self._read_json()
        code, response = dispatch_post(path, payload)
        self._write(code, response)

    def _write_json(self, code: int, response: dict) -> None:
        body = json.dumps(response, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run_server() -> None:
    if LEARNING_SCHEDULER_ENABLED:
        threading.Thread(target=_scheduler_loop, daemon=True).start()
    threading.Thread(target=_hermes_watchdog_loop, daemon=True).start()
    server = ThreadingHTTPServer((CMUX_HOST, CMUX_PORT), _Handler)
    print(f"cmux listening on http://{CMUX_HOST}:{CMUX_PORT}")
    server.serve_forever()


def _main() -> int:
    parser = argparse.ArgumentParser(description="cmux control-plane")
    parser.add_argument("command", choices=["serve", "status", "service-start", "service-stop", "bot-start", "bot-stop"])
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="JSON config for bot-start")
    parser.add_argument("--force", action="store_true", help="Force bot stop")
    args = parser.parse_args()

    if args.command == "serve":
        run_server()
        return 0
    if args.command == "status":
        print(json.dumps(_build_status_payload(), ensure_ascii=False, indent=2))
        return 0
    if args.command == "service-start":
        out = hermes_service.start()
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 1
    if args.command == "service-stop":
        out = hermes_service.stop()
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 1
    if args.command == "bot-start":
        cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
        out = {"ok": True, "service": hermes_service.start(), "bot": hermes_service.autotrade_start(cfg)}
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0

    out = {"ok": True, "bot": hermes_service.autotrade_stop({"force": bool(args.force)}), "service": hermes_service.status()}
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
