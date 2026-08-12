"""AI Tuner Apply — Hermes cron helper applying SAFE verdicts via the live backend.

Ground truth (verified against code):
- risk_tune.json written by hand is OVERWRITTEN on next cache refresh
  (per_symbol_context._compute_risk_tune recomputes from trades) → do NOT
  write risk_tune.json directly.
- Per-symbol cooldown / perfLock are armed by the BACKEND automatically from
  loss streaks (riskCooldownEnabled) and drag reviews (perfLocks) — no
  external endpoint; the cron agent only MONITORS them.
- The only safe external apply channel is POST /autotrade/config (merge into
  live config) for global tuning knobs.

So this script supports: --status (monitor) and --config KEY=VALUE (apply).
Hermes cron decides which config patches are safe (bounded, non-core keys).

Usage:
  python ai_tuner_apply.py --status
  python ai_tuner_apply.py --config minConfidence 0.72
  python ai_tuner_apply.py --config maxDailyTradesPerSymbol 12
"""
import argparse
import json
import os
import sys
import time
import urllib.request

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VAULT = os.path.join(BACKEND, "obsidian_vault")
API = "http://127.0.0.1:8020"
AUDIT = os.path.join(VAULT, "shared", "ai_tuner_audit.jsonl")

# Telemetry-backed safety boundary. The 0.70-0.80 entry-confidence band was
# materially loss-making; autonomous tuning may tighten this gate but cannot
# lower it without an explicit code review and out-of-sample evidence.
MIN_CONFIDENCE_FLOOR = 0.82

# Keys Hermes is allowed to auto-apply (bounded, reversible, non-core).
# Core risk keys (leverage, TP/SL, TV gates, fee floors) are EXCLUDED.
SAFE_CONFIG_KEYS = {
    "minConfidence",
    "maxDailyTradesPerSymbol",
    "riskCooldownMinutes",
    "deadZoneExitSec",
    "guardianMinHoldSec",
    "profitLockMinUsdt",
    "profitLockTriggerUsdt",
    "tryGreenExitMinProfitUsdt",
    "tryGreenExitMaxProfitUsdt",
    "holdTrailPct",
    "preemptiveLossExitMinEntryPct",
    "preemptiveLossExitMaxEntryPct",
    "maxOpenPositions",
    "diversificationFloor",
}

def _audit(entry: dict):
    os.makedirs(os.path.dirname(AUDIT), exist_ok=True)
    entry["ts"] = int(time.time())
    with open(AUDIT, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

def _api(path, payload=None):
    req = urllib.request.Request(API + path, method="POST" if payload else "GET")
    if payload:
        req.add_header("Content-Type", "application/json")
        req.data = json.dumps(payload).encode()
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode() or "{}")
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}

def _coerce(value: str):
    low = value.lower()
    if low in ("true", "1", "yes"):
        return True
    if low in ("false", "0", "no"):
        return False
    try:
        return float(value) if ("." in value or "e" in low) else int(value)
    except ValueError:
        return value

def apply_config(key: str, value: str) -> dict:
    if key not in SAFE_CONFIG_KEYS:
        return {"ok": False, "reason": f"KEY_NOT_IN_SAFE_LIST: {key} (allowed: {sorted(SAFE_CONFIG_KEYS)})"}
    val = _coerce(value)
    requested = val
    if key == "minConfidence":
        try:
            val = max(MIN_CONFIDENCE_FLOOR, float(val))
        except (TypeError, ValueError):
            return {"ok": False, "reason": f"INVALID_MIN_CONFIDENCE: {value}"}
    res = _api("/autotrade/config", {key: val})
    _audit({
        "action": "config",
        "key": key,
        "requested": requested,
        "value": val,
        "floor": MIN_CONFIDENCE_FLOOR if key == "minConfidence" else None,
        "ok": bool(res.get("ok")),
    })
    if key == "minConfidence" and requested != val:
        res["clamped"] = True
        res["requested"] = requested
        res["floor"] = MIN_CONFIDENCE_FLOOR
    return res

def status() -> dict:
    res = _api("/autotrade/status")
    cfg = res.get("config") or {}
    return {
        "running": res.get("running"),
        "riskCooldownEnabled": cfg.get("riskCooldownEnabled"),
        "minConfidence": cfg.get("minConfidence"),
        "maxDailyTradesPerSymbol": cfg.get("maxDailyTradesPerSymbol"),
        "perfLocks": res.get("perfLocks"),
        "riskCooldownBySymbol": res.get("riskCooldownBySymbol"),
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--config", nargs=2, metavar=("KEY", "VALUE"))
    args = ap.parse_args()

    if args.status:
        print(json.dumps(status(), indent=1, ensure_ascii=False))
        sys.exit(0)
    if args.config:
        key, value = args.config
        r = apply_config(key, value)
        print(json.dumps(r, ensure_ascii=False))
        sys.exit(0 if r.get("ok") else 1)
    print("usage: --status | --config KEY VALUE")
    sys.exit(1)

if __name__ == "__main__":
    main()
