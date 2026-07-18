"""Per-symbol closed-loop parameter optimizer.

Philosophy:
  1. Learning phase — use standard defaults, collect data, no optimization
  2. Active phase — optimize only when performance degrades
  3. Stable phase — keep good params, don't touch
  4. Revert — if optimization worsens, go back to best-ever known params

Flow:
  1. snapshot_active_params()   — record active params when position opens
  2. record_guardian_stats()    — collect guardian stats per position
  3. record_trade_outcome()     — link trade result + trigger opt if needed
  4. evaluate_param_set()       — compute effectiveness metrics
  5. suggest_candidates()       — generate nearby param candidates
  6. _simulate_on_history()     — simulate candidates on historical trades
  7. apply_optimization()       — write validated params + update best-ever
  8. check_revert()             — revert to best-ever if post-opt worsens
"""

from __future__ import annotations

import json
import math
import time

# ── Tuning knobs ──────────────────────────────────────────────────────────────

MIN_TRADES_TO_OPTIMIZE = 10
MIN_TRADES_PER_PARAM_SET = 3
IMPROVEMENT_THRESHOLD = 0.10
MAX_PARAM_CHANGE_PER_CYCLE = 0.15
OPTIMIZATION_INTERVAL_TRADES = 15
OPTIMIZATION_INTERVAL_ACTIVE = 8
REVERT_WORSEN_THRESHOLD = 0.15
REVERT_CHECK_TRADES = 5
SL_PCT_FLOOR = 0.50
HOLD_TRAIL_MIN_TRADES_WITH_HOLDER = 20

# Phase thresholds
LEARNING_PHASE_TRADES = 10
STABLE_SCORE_THRESHOLD = 2.0
STABLE_WINRATE_THRESHOLD = 55.0
DEGRADED_AVG_PNL = -0.05
DEGRADED_WINRATE = 42.0

PARAM_GRID = {
    "holdTrailPct": {
        "range": (0.10, 0.50),
        "step": 0.02,
        "default": 0.25,
    },
    "holdMinConfidence": {
        "range": (0.60, 0.85),
        "step": 0.02,
        "default": 0.72,
    },
    "tpPct": {
        "range": (0.35, 4.0),
        "step": 0.15,
        "default": 1.8,
    },
    "slPct": {
        "range": (SL_PCT_FLOOR, 2.5),
        "step": 0.10,
        "default": 0.75,
    },
}

AUTOTUNE_LOG_PREFIX = "[Autotuner]"

def _log(msg: str) -> None:
    print(f"{AUTOTUNE_LOG_PREFIX} {msg}")

def _clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))

def _snap_key(params: dict) -> str:
    parts = []
    for k in sorted(PARAM_GRID.keys()):
        v = params.get(k)
        if v is not None:
            parts.append(f"{k}={round(float(v), 4)}")
    return "|".join(parts)

def _params_close(a: dict, b: dict, tol: float = 0.01) -> bool:
    for k in PARAM_GRID:
        va = float(a.get(k, PARAM_GRID[k]["default"]))
        vb = float(b.get(k, PARAM_GRID[k]["default"]))
        if abs(va - vb) > tol:
            return False
    return True

def _defaults() -> dict:
    return {k: PARAM_GRID[k]["default"] for k in PARAM_GRID}

# ── I/O helpers ───────────────────────────────────────────────────────────────

def _load_trades_jsonl(symbol: str, max_trades: int = 0) -> list:
    """Load trades from JSONL file. If max_trades > 0, only read the last N lines."""
    try:
        from services.config_paths import VAULT_DIR
        path = VAULT_DIR / "symbols" / symbol / "trades.jsonl"
        if not path.exists():
            return []
        if max_trades > 0:
            lines = path.read_text(encoding="utf-8").splitlines()
            lines = [l.strip() for l in lines[-max_trades:] if l.strip()]
        else:
            lines = path.read_text(encoding="utf-8").splitlines()
        trades = []
        for line in lines:
            try:
                t = json.loads(line)
                if isinstance(t, dict):
                    trades.append(t)
            except json.JSONDecodeError:
                continue
        return trades
    except Exception:
        return []

def _load_symbol_profile(symbol: str) -> dict:
    try:
        from trading.per_symbol_storage import PerSymbolStorage
        from services.config_paths import VAULT_DIR
        return PerSymbolStorage(VAULT_DIR, symbol).load_symbol_profile()
    except Exception:
        return {}

def _save_symbol_profile(symbol: str, profile: dict) -> bool:
    try:
        from trading.per_symbol_storage import PerSymbolStorage
        from services.config_paths import VAULT_DIR
        PerSymbolStorage(VAULT_DIR, symbol).save_symbol_profile(profile)
        return True
    except Exception:
        return False

# ── Snapshot & Record ─────────────────────────────────────────────────────────

def snapshot_active_params(symbol: str, eff: dict) -> dict:
    return {
        "holdTrailPct": round(float(eff.get("holdTrailPct", 0.25) or 0.25), 4),
        "holdMinConfidence": round(float(eff.get("holdMinConfidence", 0.72) or 0.72), 4),
        "tpPct": round(float(eff.get("tpPct", 1.8) or 1.8), 4),
        "slPct": round(float(eff.get("slPct", 0.75) or 0.75), 4),
        "snapshotAt": int(time.time()),
    }

def record_guardian_stats(lock: dict, stats: dict) -> None:
    if not isinstance(lock, dict):
        return
    gs = lock.get("guardianStats")
    if not isinstance(gs, dict):
        gs = {}
    gs.update(stats)
    gs["updatedAt"] = int(time.time())
    lock["guardianStats"] = gs

def record_trade_outcome(symbol: str, trade: dict) -> None:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return

    trades = _load_trades_jsonl(sym)
    total_trades = len(trades)

    sp = _load_symbol_profile(sym)
    phase = _determine_phase(sp, total_trades)

    if phase == "learning":
        _log(f"{sym}: LEARNING phase ({total_trades}/{LEARNING_PHASE_TRADES} trades), using defaults")
        return

    last_opt_at = _get_last_optimization_timestamp(sym, _sp=sp)
    trades_since = _count_trades_since(trades, last_opt_at)
    interval = _get_optimization_interval(sym, trades)

    if phase == "stable" and trades_since < interval * 2:
        return

    if trades_since < interval:
        return

    _log(f"{sym}: {phase.upper()} phase, {trades_since} trades since last opt (interval={interval})")
    result = optimize_symbol(sym)
    if result.get("applied"):
        _log(f"{sym}: APPLIED new params {json.dumps(result.get('new_params', {}))}")
    else:
        _log(f"{sym}: no change ({result.get('reason', 'unknown')})")

# ── Phase Detection ───────────────────────────────────────────────────────────

def _determine_phase(sp: dict, total_trades: int) -> str:
    has_history = bool(sp.get("autotuneHistory"))
    if not has_history and total_trades < LEARNING_PHASE_TRADES:
        return "learning"

    last_score = float(sp.get("autotuneLastScore", 0.0) or 0.0)
    last_winrate = float(sp.get("autotuneLastWinRate", 50.0) or 50.0)

    if last_score >= STABLE_SCORE_THRESHOLD and last_winrate >= STABLE_WINRATE_THRESHOLD:
        return "stable"

    return "active"

def _get_last_optimization_timestamp(symbol: str, _sp: dict | None = None) -> int:
    sp = _sp if _sp is not None else _load_symbol_profile(symbol)
    return int(sp.get("autotuneLastAt", 0) or 0)

def _count_trades_since(trades: list, since_ts: int) -> int:
    count = 0
    for t in reversed(trades):
        ts = int(t.get("closedAt", t.get("ts", 0)) or 0)
        if ts <= since_ts:
            break
        count += 1
    return count

def _count_trades_with_hold_winner(trades: list) -> int:
    count = 0
    for t in trades:
        gs = t.get("guardian_stats", {})
        if gs.get("holdWinnerActivated"):
            count += 1
    return count

def _get_optimization_interval(symbol: str, trades: list) -> int:
    hold_winner_count = _count_trades_with_hold_winner(trades)
    if hold_winner_count >= HOLD_TRAIL_MIN_TRADES_WITH_HOLDER:
        return OPTIMIZATION_INTERVAL_ACTIVE
    return OPTIMIZATION_INTERVAL_TRADES

def _group_trades_by_params(trades: list) -> dict:
    groups = {}
    for t in trades:
        params = t.get("params_at_entry")
        if not isinstance(params, dict):
            continue
        key = _snap_key(params)
        groups.setdefault(key, []).append(t)
    return groups

# ── Evaluate ──────────────────────────────────────────────────────────────────

def evaluate_param_set(trades: list) -> dict:
    if not trades:
        return {"score": -999.0, "trades": 0}

    pnls = [float(t.get("_pnl", t.get("pnl", 0.0)) or 0.0) for t in trades]
    wins = [p for p in pnls if p >= 0.0]
    losses = [p for p in pnls if p < 0.0]

    n = len(pnls)
    total_pnl = sum(pnls)
    avg_pnl = total_pnl / max(n, 1)
    win_rate = len(wins) / max(n, 1) * 100.0

    gross_win = sum(wins) if wins else 0.0
    gross_loss = abs(sum(losses)) if losses else 0.0001
    profit_factor = gross_win / gross_loss if gross_loss > 0 else (999.0 if gross_win > 0 else 0.0)

    if n >= 2:
        mean = avg_pnl
        var = sum((p - mean) ** 2 for p in pnls) / (n - 1)
        std = math.sqrt(max(var, 1e-12))
        sharpe = avg_pnl / std
    else:
        sharpe = 0.0

    cum = 0.0
    peak_cum = 0.0
    max_dd = 0.0
    for p in pnls:
        cum += p
        peak_cum = max(peak_cum, cum)
        dd = peak_cum - cum
        max_dd = max(max_dd, dd)

    hold_eff = 0.0
    win_count = len(wins)
    for t in trades:
        gs = t.get("guardian_stats", {})
        peak_pnl = float(gs.get("peakProfitUsdt", 0.0) or 0.0)
        actual_pnl = float(t.get("_pnl", t.get("pnl", 0.0)) or 0.0)
        if peak_pnl > 0.01 and actual_pnl > 0:
            hold_eff += min(1.0, actual_pnl / peak_pnl)
    hold_efficiency = hold_eff / max(win_count, 1)

    score = (
        avg_pnl * 10.0
        + (win_rate - 50.0) * 0.05
        + min(profit_factor, 5.0) * 0.3
        + sharpe * 0.5
        - max_dd * 2.0
        + hold_efficiency * 0.2
    )

    return {
        "score": round(score, 6),
        "trades": n,
        "avg_pnl": round(avg_pnl, 6),
        "win_rate": round(win_rate, 2),
        "profit_factor": round(profit_factor, 4),
        "sharpe": round(sharpe, 4),
        "max_drawdown": round(max_dd, 6),
        "hold_efficiency": round(hold_efficiency, 4),
        "total_pnl": round(total_pnl, 6),
    }

# ── Best-Ever Tracking ────────────────────────────────────────────────────────

def _update_best_ever(sp: dict, params: dict, perf: dict) -> None:
    best = sp.get("autotuneBestEver")
    if not isinstance(best, dict):
        best = {"params": _defaults(), "score": -999.0, "win_rate": 0.0}

    new_score = float(perf.get("score", -999.0) or -999.0)
    new_winrate = float(perf.get("win_rate", 0.0) or 0.0)
    new_avg_pnl = float(perf.get("avg_pnl", 0.0) or 0.0)

    old_score = float(best.get("score", -999.0) or -999.0)
    old_winrate = float(best.get("win_rate", 0.0) or 0.0)
    old_avg_pnl = float(best.get("avg_pnl", 0.0) or 0.0)

    is_better = (
        new_score > old_score
        or (new_score >= old_score and new_winrate > old_winrate)
        or (new_score >= old_score and new_winrate >= old_winrate and new_avg_pnl > old_avg_pnl)
    )

    if is_better:
        sp["autotuneBestEver"] = {
            "params": dict(params),
            "score": round(new_score, 6),
            "win_rate": round(new_winrate, 2),
            "avg_pnl": round(new_avg_pnl, 6),
            "updatedAt": int(time.time()),
        }

def _get_best_ever_params(sp: dict) -> dict:
    best = sp.get("autotuneBestEver")
    if isinstance(best, dict) and isinstance(best.get("params"), dict):
        return best["params"]
    return _defaults()

# ── Suggest Candidates ────────────────────────────────────────────────────────

def _current_params_from_profile(symbol: str, _sp: dict | None = None) -> dict:
    sp = _sp if _sp is not None else _load_symbol_profile(symbol)
    params = {}
    for k in PARAM_GRID:
        val = sp.get(k)
        if val is not None:
            params[k] = float(val)
        else:
            params[k] = PARAM_GRID[k]["default"]
    return params

def suggest_candidates(current_params: dict, current_perf: dict) -> list:
    candidates = [dict(current_params)]

    avg_pnl = float(current_perf.get("avg_pnl", 0.0) or 0.0)
    win_rate = float(current_perf.get("win_rate", 50.0) or 50.0)
    hold_eff = float(current_perf.get("hold_efficiency", 0.5) or 0.5)

    if avg_pnl < DEGRADED_AVG_PNL or win_rate < DEGRADED_WINRATE:
        for k in PARAM_GRID:
            default_val = PARAM_GRID[k]["default"]
            c = dict(current_params)
            c[k] = default_val
            candidates.append(c)
        return candidates

    if hold_eff < 0.4:
        c = dict(current_params)
        c["holdTrailPct"] = _clamp(
            c["holdTrailPct"] - PARAM_GRID["holdTrailPct"]["step"] * 2,
            *PARAM_GRID["holdTrailPct"]["range"],
        )
        c["holdMinConfidence"] = _clamp(
            c["holdMinConfidence"] - PARAM_GRID["holdMinConfidence"]["step"],
            *PARAM_GRID["holdMinConfidence"]["range"],
        )
        candidates.append(c)

    if hold_eff >= 0.6:
        c = dict(current_params)
        c["holdTrailPct"] = _clamp(
            c["holdTrailPct"] + PARAM_GRID["holdTrailPct"]["step"] * 2,
            *PARAM_GRID["holdTrailPct"]["range"],
        )
        candidates.append(c)

    if avg_pnl > 0.02 and win_rate >= 55:
        for k in ("holdTrailPct", "tpPct"):
            step = PARAM_GRID[k]["step"]
            for direction in (-1, 1):
                c = dict(current_params)
                c[k] = _clamp(c[k] + step * direction, *PARAM_GRID[k]["range"])
                candidates.append(c)
    else:
        for k in ("slPct", "holdMinConfidence"):
            step = PARAM_GRID[k]["step"]
            for direction in (-1, 1):
                c = dict(current_params)
                c[k] = _clamp(c[k] + step * direction, *PARAM_GRID[k]["range"])
                candidates.append(c)

    seen = set()
    unique = []
    # Fee-based TP% floor: filter out candidates that lose money to fees.
    _TAKER_FEE_BPS = 4.0
    _EXTRA_COST_BPS = 2.0
    _SLIPPAGE_BPS = 18.0
    _COST_BPS = (2.0 * _TAKER_FEE_BPS) + _EXTRA_COST_BPS + (_SLIPPAGE_BPS * 0.5)
    _MIN_NET_USDT = 0.05
    _USDT = 20.0
    _tp_fee_floor = max(((_COST_BPS / 10000.0 * _USDT) + _MIN_NET_USDT) / _USDT * 100.0, 0.25)
    for c in candidates:
        if float(c.get("tpPct", PARAM_GRID["tpPct"]["default"])) < _tp_fee_floor:
            continue
        key = _snap_key(c)
        if key not in seen:
            seen.add(key)
            unique.append(c)
    return unique

# ── Walk-Forward Simulation ───────────────────────────────────────────────────

def _simulate_on_history(trades: list, params: dict) -> dict:
    if not trades:
        return {"score": -999.0, "trades": 0}

    tp_pct = float(params.get("tpPct", 1.8))
    sl_pct = float(params.get("slPct", 0.75))
    hold_trail = float(params.get("holdTrailPct", 0.25))

    # Capture efficiency: guardian won't perfectly detect every peak.
    # 0.92 reflects ~8% average peak detection lag across market regimes.
    _PEAK_CAPTURE_EFF = 0.92

    # Fee model: round-trip taker fee + micro cost buffer + half slippage.
    # Matches risk.py estimate_trade_edge_usdt() defaults.
    _TAKER_FEE_BPS = 4.0
    _EXTRA_COST_BPS = 2.0
    _SLIPPAGE_BPS = 18.0
    _COST_BPS = (2.0 * _TAKER_FEE_BPS) + _EXTRA_COST_BPS + (_SLIPPAGE_BPS * 0.5)  # ~19 bps

    simulated_pnls = []
    for idx, t in enumerate(trades):
        entry = float(t.get("entry", 0) or t.get("markPrice", 0) or 0)
        peak = float(t.get("guardian_stats", {}).get("peakProfitUsdt", 0.0) or 0.0)
        actual_pnl = float(t.get("_pnl", t.get("pnl", 0.0)) or 0.0)
        notional = float(t.get("guardian_stats", {}).get("notionalUsdt", 20.0) or 20.0)
        side = str(t.get("side", "LONG")).upper()

        tp_dist_usdt = notional * tp_pct / 100.0
        sl_dist_usdt = notional * sl_pct / 100.0
        trail_usdt = notional * hold_trail / 100.0

        # Round-trip fee for this trade
        fee_usdt = notional * _COST_BPS / 10000.0

        # Apply capture efficiency to peak to reduce survivor bias
        effective_peak = peak * _PEAK_CAPTURE_EFF

        if actual_pnl >= 0:
            captured = min(effective_peak, tp_dist_usdt) if effective_peak > 0 else actual_pnl
            if captured > trail_usdt and effective_peak > tp_dist_usdt * 0.5:
                captured = max(captured * 0.95, trail_usdt)
            sim_pnl = max(0, min(captured, tp_dist_usdt))
        else:
            sim_pnl = max(-sl_dist_usdt, actual_pnl)

        # Deduct round-trip fee from simulated PnL
        sim_pnl -= fee_usdt

        simulated_pnls.append(sim_pnl)

    return evaluate_param_set([{"_pnl": p, "pnl": p} for p in simulated_pnls])

def walk_forward_validate(trades: list, candidates: list) -> dict:
    if not candidates:
        return {"ok": False, "reason": "no_candidates"}

    all_with_snapshots = [t for t in trades if isinstance(t.get("params_at_entry"), dict)]
    all_without = [t for t in trades if not isinstance(t.get("params_at_entry"), dict)]

    base_trades = all_without if all_without else trades[-MIN_TRADES_TO_OPTIMIZE:]

    best = None
    best_score = -999.0
    results = []

    for cand in candidates:
        sim = _simulate_on_history(base_trades, cand)
        results.append({"params": cand, "sim": sim})
        if sim["score"] > best_score and sim["trades"] >= MIN_TRADES_PER_PARAM_SET:
            best_score = sim["score"]
            best = cand

    if all_with_snapshots:
        current_key = None
        for t in reversed(all_with_snapshots):
            current_key = t.get("params_at_entry")
            break
        if current_key:
            current_sim = _simulate_on_history(base_trades, current_key)
            if best and best_score <= current_sim["score"]:
                return {
                    "ok": False,
                    "reason": "no_improvement_over_current",
                    "current_score": current_sim["score"],
                    "best_candidate_score": best_score,
                }

    if best is None:
        return {"ok": False, "reason": "no_valid_candidate"}

    return {
        "ok": True,
        "best_params": best,
        "best_score": best_score,
        "candidates_evaluated": len(candidates),
    }

# ── Apply ─────────────────────────────────────────────────────────────────────

def _max_change_exceeded(old_params: dict, new_params: dict) -> bool:
    for k in PARAM_GRID:
        old_val = float(old_params.get(k, PARAM_GRID[k]["default"]))
        new_val = float(new_params.get(k, PARAM_GRID[k]["default"]))
        if old_val > 0:
            change_pct = abs(new_val - old_val) / old_val
            if change_pct > MAX_PARAM_CHANGE_PER_CYCLE:
                return True
    return False

def apply_optimization(symbol: str, new_params: dict, validation: dict) -> bool:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return False

    # Fee-based TP% floor: reject if tpPct would lose money to fees.
    # Matches the fee model in risk.py: round-trip = 2*taker + extra + slippage/2.
    _TAKER_FEE_BPS = 4.0
    _EXTRA_COST_BPS = 2.0
    _SLIPPAGE_BPS = 18.0
    _COST_BPS = (2.0 * _TAKER_FEE_BPS) + _EXTRA_COST_BPS + (_SLIPPAGE_BPS * 0.5)
    _MIN_NET_USDT = 0.05
    _USDT = 20.0  # default notional; actual notional varies per trade
    _tp_fee_floor_pct = ((_COST_BPS / 10000.0 * _USDT) + _MIN_NET_USDT) / _USDT * 100.0
    _tp_fee_floor_pct = max(_tp_fee_floor_pct, 0.25)

    new_tp = float(new_params.get("tpPct", PARAM_GRID["tpPct"]["default"]))
    if new_tp < _tp_fee_floor_pct:
        _log(f"{sym}: REJECTED tpPct={new_tp:.4f}% < fee floor={_tp_fee_floor_pct:.4f}%")
        return False

    old_params = _current_params_from_profile(sym)
    if _max_change_exceeded(old_params, new_params):
        clamped = {}
        for k in PARAM_GRID:
            old_val = float(old_params.get(k, PARAM_GRID[k]["default"]))
            new_val = float(new_params.get(k, PARAM_GRID[k]["default"]))
            max_delta = old_val * MAX_PARAM_CHANGE_PER_CYCLE
            clamped_val = _clamp(new_val, old_val - max_delta, old_val + max_delta)
            clamped_val = _clamp(clamped_val, *PARAM_GRID[k]["range"])
            clamped[k] = round(clamped_val, 4)
        new_params = clamped

    sp = _load_symbol_profile(sym)
    if not isinstance(sp, dict):
        sp = {}

    changes = {}
    for k in PARAM_GRID:
        old_val = float(sp.get(k, PARAM_GRID[k]["default"]))
        new_val = float(new_params.get(k, PARAM_GRID[k]["default"]))
        if abs(new_val - old_val) > 0.005:
            sp[k] = round(new_val, 4)
            changes[k] = {"from": round(old_val, 4), "to": round(new_val, 4)}

    if not changes:
        return False

    sp["autotuneLastAt"] = int(time.time())
    sp["autotuneLastChanges"] = changes
    sp["autotuneLastScore"] = float(validation.get("best_score", 0.0))

    prev_history = sp.get("autotuneHistory", [])
    if not isinstance(prev_history, list):
        prev_history = []
    prev_history.append({
        "at": int(time.time()),
        "changes": changes,
        "score": round(float(validation.get("best_score", 0.0)), 4),
        "candidates": int(validation.get("candidates_evaluated", 0)),
    })
    sp["autotuneHistory"] = prev_history[-50:]

    _log(f"{sym}: writing optimized params {json.dumps(changes)}")
    return _save_symbol_profile(sym, sp)

# ── Revert to Best-Ever ───────────────────────────────────────────────────────

def check_revert(symbol: str, _sp: dict | None = None) -> bool:
    sym = str(symbol or "").upper().strip()
    sp = _sp if _sp is not None else _load_symbol_profile(sym)
    if not isinstance(sp, dict):
        return False

    last_at = int(sp.get("autotuneLastAt", 0) or 0)
    if last_at == 0:
        return False

    trades = _load_trades_jsonl(sym, max_trades=REVERT_CHECK_TRADES + 10)
    recent = []
    for t in reversed(trades):
        ts = int(t.get("closedAt", t.get("ts", 0)) or 0)
        if ts <= last_at:
            break
        recent.append(t)
        if len(recent) >= REVERT_CHECK_TRADES:
            break

    if len(recent) < REVERT_CHECK_TRADES:
        return False

    pnls = [float(t.get("_pnl", t.get("pnl", 0.0)) or 0.0) for t in recent]
    avg_after = sum(pnls) / len(pnls)
    total_after = sum(pnls)

    if avg_after < -REVERT_WORSEN_THRESHOLD and total_after < -0.10:
        best_ever_params = _get_best_ever_params(sp)
        current_params = {}
        for k in PARAM_GRID:
            current_params[k] = float(sp.get(k, PARAM_GRID[k]["default"]))

        if _params_close(current_params, best_ever_params, tol=0.005):
            _log(f"{sym}: already at best-ever params, cannot revert further")
            return False

        for k in PARAM_GRID:
            sp[k] = round(float(best_ever_params.get(k, PARAM_GRID[k]["default"])), 4)

        sp["autotuneRevertedAt"] = int(time.time())
        sp["autotuneRevertedReason"] = f"avg_pnl={avg_after:.4f} after optimization, reverted to best-ever"
        _save_symbol_profile(sym, sp)
        _log(f"{sym}: REVERTED to best-ever params (avg_pnl={avg_after:.4f} post-opt)")
        return True
    return False

def check_rollback_effectiveness(symbol: str, _sp: dict | None = None) -> dict:
    sym = str(symbol or "").upper().strip()
    sp = _sp if _sp is not None else _load_symbol_profile(sym)
    if not isinstance(sp, dict):
        return {"ok": False, "reason": "no_profile"}

    reverted_at = int(sp.get("autotuneRevertedAt", 0) or 0)
    if reverted_at == 0:
        return {"ok": False, "reason": "no_rollback"}

    trades = _load_trades_jsonl(sym)
    pre_rollback = []
    post_rollback = []
    for t in trades:
        ts = int(t.get("closedAt", t.get("ts", 0)) or 0)
        if ts > reverted_at:
            post_rollback.append(t)
        elif ts > reverted_at - 86400:
            pre_rollback.append(t)

    if len(pre_rollback) < 3 or len(post_rollback) < 3:
        return {"ok": False, "reason": "insufficient_data"}

    pre_pnls = [float(t.get("_pnl", t.get("pnl", 0.0)) or 0.0) for t in pre_rollback]
    post_pnls = [float(t.get("_pnl", t.get("pnl", 0.0)) or 0.0) for t in post_rollback]
    pre_avg = sum(pre_pnls) / len(pre_pnls)
    post_avg = sum(post_pnls) / len(post_pnls)

    effectiveness = post_avg - pre_avg
    sp["autotuneRollbackEffectiveness"] = round(effectiveness, 6)
    sp["autotuneRollbackPreAvg"] = round(pre_avg, 6)
    sp["autotuneRollbackPostAvg"] = round(post_avg, 6)
    _save_symbol_profile(sym, sp)

    _log(f"{sym}: rollback effectiveness={effectiveness:.4f} (pre={pre_avg:.4f}, post={post_avg:.4f})")
    return {
        "ok": True,
        "effectiveness": round(effectiveness, 6),
        "pre_avg_pnl": round(pre_avg, 6),
        "post_avg_pnl": round(post_avg, 6),
    }

# ── Main Entry Point ──────────────────────────────────────────────────────────

def optimize_symbol(symbol: str) -> dict:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {"ok": False, "reason": "no_symbol"}

    trades = _load_trades_jsonl(sym)
    if len(trades) < MIN_TRADES_TO_OPTIMIZE:
        return {"ok": False, "reason": "insufficient_trades", "trades": len(trades)}

    sp = _load_symbol_profile(sym)
    check_revert(sym, _sp=sp)
    check_rollback_effectiveness(sym, _sp=sp)
    sp = _load_symbol_profile(sym)
    phase = _determine_phase(sp, len(trades))

    current_params = _current_params_from_profile(sym, _sp=sp)

    all_with_snap = [t for t in trades if isinstance(t.get("params_at_entry"), dict)]

    if all_with_snap:
        current_perf = evaluate_param_set(all_with_snap)
    else:
        current_perf = evaluate_param_set(trades[-MIN_TRADES_TO_OPTIMIZE:])

    sp["autotuneLastScore"] = current_perf.get("score", 0.0)
    sp["autotuneLastWinRate"] = current_perf.get("win_rate", 50.0)
    sp["autotunePhase"] = phase

    _update_best_ever(sp, current_params, current_perf)
    _save_symbol_profile(sym, sp)

    if phase == "stable":
        return {"ok": True, "applied": False, "reason": "stable_no_change_needed"}

    if phase == "learning":
        return {"ok": True, "applied": False, "reason": "learning_collecting_data"}

    if current_perf.get("score", -999) > STABLE_SCORE_THRESHOLD and current_perf.get("win_rate", 0) >= STABLE_WINRATE_THRESHOLD:
        return {"ok": True, "applied": False, "reason": "performing_well_no_change_needed"}

    candidates = suggest_candidates(current_params, current_perf)
    if len(candidates) <= 1:
        return {"ok": True, "applied": False, "reason": "only_baseline_candidate"}

    validation = walk_forward_validate(trades, candidates)
    if not validation.get("ok"):
        return {"ok": True, "applied": False, "reason": validation.get("reason", "validation_failed")}

    best_params = validation.get("best_params", {})
    if not best_params:
        return {"ok": True, "applied": False, "reason": "no_best_params"}

    if _params_close(current_params, best_params, tol=0.005):
        return {"ok": True, "applied": False, "reason": "best_params_same_as_current"}

    applied = apply_optimization(sym, best_params, validation)
    return {
        "ok": True,
        "applied": applied,
        "old_params": current_params,
        "new_params": best_params,
        "improvement": validation.get("best_score", 0.0),
        "candidates_evaluated": validation.get("candidates_evaluated", 0),
    }
