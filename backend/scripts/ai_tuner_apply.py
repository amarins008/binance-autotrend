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

# OWNERSHIP SPLIT (2026-08-14) RULE #1 hard floor: minConfidence must NEVER be
# pushed below 0.82. The tuner clamps any minConfidence write to this floor so it
# cannot accidentally drop the gate (a cron verification run on 2026-08-25 did exactly
# that — pushed 0.70 -> clamped to 0.80 -> live went BELOW 0.82; fixed & restored).
# Boss's 2026-08-23 "0.80" note was superseded by the 0.82 mandate enforcement; the
# required floor here is 0.82 and must not be lowered without explicit out-of-sample
# evidence AND owner sign-off.
MIN_CONFIDENCE_FLOOR = 0.82

# Anti-thrash: do not re-apply minConfidence more often than this (hours) unless
# the requested value moves >= 0.03. Prevents the tuner from swinging the gate.
TUNER_MIN_CONF_COOLDOWN_HOURS = 6

# Keys Hermes is allowed to auto-apply (bounded, reversible, non-core).
# Core risk keys (leverage, TP/SL, TV gates, fee floors) are EXCLUDED.
# OWNERSHIP SPLIT (2026-08-14): minConfidence and maxOpenPositions are NOT
# tuner-owned. minConfidence is shared with the supervisor's hard 0.82 floor
# brake and was swinging 0.72<->0.83 when both agents wrote it; maxOpenPositions
# is owned exclusively by the in-process supervisor (loss-streak circuit breaker).
# The tuner keeps minConfidence only as a monitored/audited floor-enforced value.
SAFE_CONFIG_KEYS = {
    "minConfidence",
    "maxDailyTradesPerSymbol",   # tuner-owned (long-horizon frequency cap)
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
    # maxOpenPositions / diversificationFloor: SUPERVISOR-OWNED (removed 2026-08-14)
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


def _min_conf_recent_apply() -> tuple[bool, float | None]:
    """Return (skip_due_to_cooldown, last_applied_value) for minConfidence.

    Reads the audit log for the most recent successful minConfidence apply and
    decides whether a new apply within TUNER_MIN_CONF_COOLDOWN_HOURS should be
    skipped. OWNERSHIP SPLIT: stops the tuner swinging the gate.
    """
    now = int(time.time())
    last_ts = 0
    last_val = None
    if os.path.exists(AUDIT):
        try:
            with open(AUDIT, encoding="utf-8") as f:
                for line in f:
                    try:
                        e = json.loads(line)
                    except Exception:
                        continue
                    if e.get("key") == "minConfidence" and e.get("ok"):
                        ts = int(e.get("ts", 0) or 0)
                        if ts >= last_ts:
                            last_ts = ts
                            last_val = e.get("value")
        except Exception:
            pass
    if last_ts == 0 or last_val is None:
        return (False, None)
    age_h = (now - last_ts) / 3600.0
    return (age_h < TUNER_MIN_CONF_COOLDOWN_HOURS, float(last_val))


def apply_config(key: str, value: str) -> dict:
    if key not in SAFE_CONFIG_KEYS:
        return {"ok": False, "reason": f"KEY_NOT_IN_SAFE_LIST: {key} (allowed: {sorted(SAFE_CONFIG_KEYS)})"}
    val = _coerce(value)
    requested = val
    # Anti-thrash cooldown for minConfidence: the tuner must not re-write this
    # key more than once per TUNER_MIN_CONF_COOLDOWN_HOURS unless the requested
    # value moves materially. This stops the 0.72<->0.83 swing (OWNERSHIP SPLIT).
    if key == "minConfidence":
        _cooldown_skip, _last_val = _min_conf_recent_apply()
        try:
            if _cooldown_skip and abs(float(requested) - float(_last_val)) < 0.03:
                return {"ok": False, "reason": "COOLDOWN_SKIP", "lastApplied": _last_val,
                        "cooldownHours": TUNER_MIN_CONF_COOLDOWN_HOURS}
        except (TypeError, ValueError):
            pass
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
