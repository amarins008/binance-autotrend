"""Laya System-1 shadow observer — non-blocking, observational only.

After the bot executes a real trade, this module fires a daemon thread that asks
the local laya sidecar for its read on the same feature set the bot decided on.
The answer is appended to ``obsidian_vault/laya_shadow.jsonl`` keyed by the
entry so replay analysis can later join it against trade outcomes and decide
whether laya has predictive edge worth gating on (Phase 2). It NEVER gates a
trade and NEVER blocks the order path.
"""
import json
import threading
import time

from services.config_paths import VAULT_DIR

_URL = "http://127.0.0.1:8790/v1/predict"
_CONNECT_TIMEOUT = 0.7   # down sidecar must not hold threads for long
_TOTAL_TIMEOUT = 30.0    # background inference can take ~5-17s on CPU
_MAX_CONCURRENT = 2

# Fixed question schema (stable across calls so Phase 2 can batch by schema).
_QUESTIONS = {
    "open_trade": {
        "type": "noul",
        "instructions": "Given this trading setup, should the bot open this position right now?",
        "criteria": {
            "false": "setup is weak, misaligned, or the edge does not survive fees; do not open",
            "true": "setup is strong, aligned, and the edge survives fees; open",
        },
    },
    "edge_strength": {
        "type": "choice",
        "instructions": "How strong is the edge of this setup after accounting for fees?",
        "criteria": {
            "weak": "marginal or negative edge, likely loses to fees",
            "medium": "a genuine but moderate edge",
            "strong": "high-conviction edge that clearly survives fees",
        },
    },
    "risk": {
        "type": "score",
        "instructions": "How risky is this setup?",
        "criteria": [
            "low risk, tight and predictable move",
            "moderate risk",
            "high risk, wide volatility around the signal",
        ],
    },
}

_conn_lock = threading.Lock()
_inflight = 0
_recent_keys = set()
_recent_lock = threading.Lock()
_RECENT_MAX = 300


def _f(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _s(v, default=""):
    try:
        return str(v)
    except Exception:
        return default


def _clean_s(v, default=""):
    """str() with a default when v is None/empty (avoids 'None' leaking through)."""
    if v is None:
        return default
    s = _s(v, default)
    return s if s else default


def feature_pack(symbol: str, side: str, intel: dict | None, extra: dict | None = None) -> dict:
    """Flatten the intel the bot decided on into the state dict laya reads."""
    intel = intel if isinstance(intel, dict) else {}
    mm = intel.get("momentum") if isinstance(intel.get("momentum"), dict) else {}
    ex = intel.get("execution") if isinstance(intel.get("execution"), dict) else {}
    candle = intel.get("candles") if isinstance(intel.get("candles"), dict) else {}
    tv = intel.get("tv") if isinstance(intel.get("tv"), dict) else {}
    db = intel.get("directionBias") if isinstance(intel.get("directionBias"), dict) else {}
    oob = intel.get("orderBook") if isinstance(intel.get("orderBook"), dict) else {}
    sm = intel.get("setup")
    pack = {
        "symbol": _clean_s(symbol, "UNKNOWN"),
        "signal": _clean_s(side, "FLAT"),
        "confidence": round(_f(intel.get("confidence")), 4),
        "score": round(_f(candle.get("score"), _f(intel.get("score"), _f(intel.get("weightedScore")))), 4),
        "setup": _clean_s(sm, "unknown"),
        "momentumPct": round(_f(mm.get("momentumPct")), 4),
        "volumeRatio": round(_f(mm.get("volumeRatio")), 4),
        "divergence": _clean_s(mm.get("divergence"), "NONE").upper(),
        "spreadBps": round(_f(ex.get("spreadBps")), 4),
        "fundingRate": round(_f(ex.get("lastFundingRate"), 0.0), 6),
        "patternBias": round(_f(candle.get("bias")), 4),
        "regime": _clean_s(db.get("regime"), "UNKNOWN").upper(),
        "directionBias": _clean_s(db.get("bias"), "NEUTRAL").upper(),
        "directionBiasStrength": round(_f(db.get("strength")), 4),
        "tvSignal": _clean_s(tv.get("signal"), "WAIT"),
        "tvStrength": round(_f(tv.get("strength")), 4),
        "tvConfidence": round(_f(tv.get("confidence")), 4),
        "tvAge": int(_f(tv.get("age"), -1)),
        "bidAskImbalance": round(_f(oob.get("imbalance")), 4),
    }
    if isinstance(extra, dict):
        for k, v in extra.items():
            if v is not None:
                pack[_s(k)] = v
    return pack


def render_state_text(pack: dict) -> str:
    """Render the feature pack into the natural-language state laya actually reads.

    Probe evidence: the english checkpoint answers off the *semantics of the words*,
    not the numeric values (flipping numbers barely moves noul; flipping words does).
    A numeric JSON dict therefore reads ~like empty noise. This rendering keeps the
    same deterministic features but phrases them so the model can reason on them.
    """
    sym = _clean_s(pack.get("symbol"), "the market")
    side = _clean_s(pack.get("signal"), "a position").upper()
    moneyness = ["low", "below-average", "average", "above-average", "high"][
        min(4, max(0, int(_f(pack.get("confidence"), 0) * 5 // 1)))]
    mom = _f(pack.get("momentumPct"))
    mom_word = f"pushing firmly in {'favour' if mom >= 0 else 'against'} the trade by {abs(mom):.2f}%" if mom else "flat momentum"
    vol = _f(pack.get("volumeRatio"))
    vol_word = f"at {vol:.2f}x normal volume" if vol else "at quiet volume"
    spread = _f(pack.get("spreadBps"))
    spread_word = "a tight spread" if spread <= 1.0 else ("a wide spread" if spread >= 5.0 else "a moderate spread")
    db = _clean_s(pack.get("directionBias"), "NEUTRAL").upper()
    db_strength = _f(pack.get("directionBiasStrength"))
    align = f"aligned ({db} strength {db_strength:.2f})" if (side.startswith("LONG") and db == "LONG") or (side.startswith("SHORT") and db == "SHORT") else f"against the {side} ({db})"
    tv = _clean_s(pack.get("tvSignal"), "WAIT").upper()
    tv_conf = _f(pack.get("tvConfidence"))
    fund = _f(pack.get("fundingRate"))
    reason = (f"Setup {_clean_s(pack.get('setup'), 'no named setup')}. "
              f"Momentum {mom_word} {vol_word}, {spread_word}. "
              f"Directional bias {align}. TradingView {tv} at confidence {tv_conf:.2f}. "
              f"Funding cost {'negligible' if abs(fund) < 5e-4 else ('costly' if fund * (1 if side.startswith('LONG') else -1) > 0 else 'favourable')}.")
    verdict = (f"Confidence is {moneyness}. "
               f"The overall picture is {'constructive and aligned' if (mom > 0.5 and tv_conf >= 0.7 and (side.startswith('LONG') and db == 'LONG') or (side.startswith('SHORT') and db == 'SHORT')) else 'mixed to unfavourable'}.")
    return (f"You are evaluating opening a {side} position on {sym}. {reason} {verdict} "
            f"Do not let the {side} label bias you; assess the evidence.")


def _shadow_path():
    return VAULT_DIR / "laya_shadow.jsonl"


def _dedup(key: str) -> bool:
    """True if we already recorded this entry (skip duplicates)."""
    now = time.time()
    with _recent_lock:
        # prune old keys
        if len(_recent_keys) > _RECENT_MAX:
            _recent_keys.clear()
        if key in _recent_keys:
            return False
        _recent_keys.add(key)
        return True


def _record(symbol: str, side: str, intel: dict | None, extra: dict | None) -> None:
    global _inflight
    with _conn_lock:
        if _inflight >= _MAX_CONCURRENT:
            return
        _inflight += 1
    entry_ts = int(time.time())
    key = "|".join([_s(symbol).upper(), _s(side).upper(), str(entry_ts // 120)])
    if not _dedup(key):
        with _conn_lock:
            _inflight -= 1
        return
    try:
        import httpx

        pack = feature_pack(symbol, side, intel, extra)
        rec = {
            "ts": entry_ts,
            "symbol": _s(symbol).upper(),
            "side": _s(side).upper(),
            "state": pack,
            "state_text": render_state_text(pack),
            "answers": {},
            "routing": {},
            "elapsedMs": 0,
            "error": None,
        }
        t0 = time.perf_counter()
        try:
            with httpx.Client(timeout=httpx.Timeout(_CONNECT_TIMEOUT, read=_TOTAL_TIMEOUT)) as cli:
                r = cli.post(_URL, json={"state": rec["state_text"], "questions": _QUESTIONS})
            if r.status_code == 200:
                res = r.json()
                out = {}
                for qid, qa in (res.get("answers") or {}).items():
                    slim = {k: qa.get(k) for k in ("type", "choice", "score", "noul", "confidence", "answer_confidence", "probabilities") if k in qa}
                    out[qid] = slim
                rec["answers"] = out
                rec["routing"] = res.get("routing") or {}
            else:
                rec["error"] = "http_%d" % r.status_code
        except Exception as e:
            rec["error"] = "%s: %s" % (type(e).__name__, str(e)[:120])
        rec["elapsedMs"] = int((time.perf_counter() - t0) * 1000)
        try:
            p = _shadow_path()
            line = json.dumps(rec, ensure_ascii=False)
            with open(p, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass
    finally:
        with _conn_lock:
            _inflight -= 1


def shadow_observe(symbol: str, side: str, intel: dict | None, extra: dict | None = None) -> None:
    """Ask laya about a just-executed entry, fully in the background.

    Returns immediately. All failures are swallowed (shadow = best-effort).
    """
    try:
        # Skip when the feature pack has no signal to read (e.g. empty intel).
        sym = _s(symbol).strip()
        sd = _s(side).strip()
        if not sym or not sd:
            return
        t = threading.Thread(target=_record, args=(sym, sd, intel, extra), daemon=True)
        t.start()
    except Exception:
        pass