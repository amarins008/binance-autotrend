"""Apply data-driven optimization to minimize losses."""
from __future__ import annotations

import json
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from trading.per_symbol_storage import PerSymbolStorage

SNAPSHOT = Path(__file__).resolve().parent / "autotrade_snapshot.json"
VAULT_DIR = Path(__file__).resolve().parent / "obsidian_vault"
API = "http://127.0.0.1:8020/autotrade/config"

_storage = PerSymbolStorage(VAULT_DIR)

# --- Analysis-driven patch (payoff 0.87, 24h WR 35%, 7d -7.38 USDT) ---
OPTIMIZATION_PATCH = {
    # Tighter stop-loss: avg loss (-0.36) > avg win (+0.31)
    "stopLossPct": 0.65,
    "slToTpRatio": 0.42,
    "takeProfitPct": 1.6,
    "minRiskRewardRatio": 1.6,
    # Lock profits earlier
    "profitLockTriggerUsdt": 0.25,
    "profitLockKeepUsdt": 0.12,
    "profitLockMaxGivebackUsdt": 0.18,
    # Higher entry quality
    "minConfidence": 0.73,
    "earlyEntryMinConfidence": 0.66,
    "earlyEntryScoreGapMin": 1.6,
    "htfMinStrength": 0.28,
    "lateEntryMaxBbPctB": 0.85,
    "lateEntryMaxVwapDistancePct": 0.26,
    # Reduce concurrent exposure
    "maxOpenPositions": 4,
    "supervisorTargetOpenPositionsMin": 2,
    "supervisorTargetOpenPositionsMax": 4,
    "maxTradesPerHour": 4,
    "tradeNotionalCapUsdt": 60.0,
    "autoScanTradeNotionalCapUsdt": 60.0,
    # Payoff guard — cut losers faster
    "payoffLossGuardEnabled": True,
    "payoffLossGuardMaxLossUsdt": 0.50,
    "payoffLossGuardLossToWinCap": 0.80,
    "payoffLossGuardMaxPayoffRatio": 0.70,
    "payoffLossGuardMinLossUsdt": 0.15,
    # Stricter performance gates
    "perfGateMinWinRatePct": 45.0,
    "perfGateMinPnlUsdt": -0.30,
    "perfGateEarlyMinWinRatePct": 40.0,
    "perfGateEarlyMinPnlUsdt": -0.25,
    "perfGateMinRewardScore": -0.80,
    "perfLockMinutes": 150,
    # Today guard — already triggered at 40.6% WR
    "todayPerformanceGuardEnabled": True,
    "todayPerformanceGuardMaxWinRatePct": 44.0,
    "todayPerformanceGuardMaxPnlUsdt": -1.0,
    # Risk cooldown tighter
    "riskCooldownLossStreak": 2,
    "pairLockMinutes": 120,
    "pairLockLossStreak": 2,
    # Session sizing — reduce bad-session exposure
    "sessionAsianMultiplier": 0.75,
    "sessionAsianEveningMultiplier": 0.70,
    "sessionBiasMaxSizeShiftPct": 15.0,
    # Block chronically losing symbols (data-driven)
    "scanDenySymbols": [
        "XAUUSDT", "XAGUSDT", "SPCXUSDT", "CLUSDT", "MRVLUSDT", "INTCUSDT",
        # Worst LIVE performers (pnl < -0.5, WR < 40%)
        "DOGEUSDT", "ASTERUSDT", "BSBUSDT", "BCHUSDT", "GUAUSDT", "SPXUSDT",
        "WLFIUSDT", "HEIUSDT", "XRPUSDT", "PLAYUSDT", "UBUSDT",
        # Recent 7D drag
        "EVAAUSDT", "PARTIUSDT", "XPINUSDT", "HMSTRUSDT",
        # Scan board underperformers
        "ETHUSDT", "LINKUSDT", "ARBUSDT", "FILUSDT",
    ],
    # Fix no-trade windows: remove good hour 17-18, block worst BKK hours
    "noTradeWindows": [
        "20:00-21:00",  # worst: -12.64 USDT
        "23:00-00:00",  # -8.12 USDT
        "04:00-05:00",  # -8.01 USDT
        "11:00-12:00",  # -7.32 USDT
        "00:00-01:00",  # -6.80 USDT
    ],
    # Adaptive loss streak — reduce size faster
    "adaptiveLossStreakThreshold": 2,
    "adaptiveLossStreakMaxReduction": 0.55,
    "supervisorSizeLossStepPct": 18.0,
    "supervisorSizeMinMultiplier": 0.60,
    # Fee edge — require better edge vs cost
    "feeMinEdgeVsCostMultiple": 1.70,
    "feeMinNetProfitUSDT": 0.12,
    # Reflection hard block — stricter
    "reflectionAvoidPatternHardBlockWrPct": 30.0,
    "reflectionAvoidPatternHardBlockMinSamples": 8,
    # Keep auto-tune off (manual optimization applied)
    "supervisorAutoTuneEnabled": False,
}

# Symbols to mark weak in learning profiles
WEAK_SYMBOLS = [
    "DOGEUSDT", "BCHUSDT", "ETHUSDT", "LINKUSDT", "FILUSDT", "ARBUSDT",
    "HBARUSDT", "HYPEUSDT", "NEARUSDT", "XRPUSDT", "PARTIUSDT", "XPINUSDT",
    "HMSTRUSDT", "EVAAUSDT", "VIRTUALUSDT", "SUIUSDT", "DOTUSDT",
]


def apply_config_via_api(patch: dict) -> dict:
    data = json.dumps(patch).encode("utf-8")
    req = urllib.request.Request(
        API,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def update_snapshot(patch: dict) -> None:
    if not SNAPSHOT.exists():
        return
    snap = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    cfg = snap.get("config") or {}
    cfg.update(patch)
    snap["config"] = cfg
    snap["frozenReason"] = (
        f"loss_minimize_tune {datetime.now().strftime('%Y-%m-%d %H:%M')} Bangkok"
    )
    SNAPSHOT.write_text(json.dumps(snap, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"Snapshot updated: {SNAPSHOT}")


def update_learning_profiles(weak_symbols: list[str]) -> int:
    updated = 0
    for sym in weak_symbols:
        pr = _storage.load_profile(sym)
        if not isinstance(pr, dict):
            pr = {"symbol": sym, "trades": 0}
        pr["priority"] = "back"
        pr["learningTier"] = "cold"
        pr["learningScore"] = min(float(pr.get("learningScore", 50) or 50), 15.0)
        if not isinstance(pr.get("latestWindow"), dict):
            pr["latestWindow"] = {}
        pr["latestWindow"]["weak"] = True
        pr["latestWindow"]["reason"] = "loss_minimize_tune"
        pr["updatedAt"] = int(datetime.now(tz=timezone.utc).timestamp())
        _storage.save_profile(sym, pr)
        updated += 1
    print(f"Learning profiles updated: {updated} symbols marked weak")
    return updated


def main() -> None:
    print("=== Applying Loss-Minimize Optimization ===")
    print(f"Patch keys: {len(OPTIMIZATION_PATCH)}")

    # 1. Apply via live API
    try:
        result = apply_config_via_api(OPTIMIZATION_PATCH)
        print(f"API result: ok={result.get('ok')} updated={result.get('updated')}")
        if result.get("config"):
            c = result["config"]
            print(f"  minConf={c.get('minConfidence')} SL={c.get('stopLossPct')} TP={c.get('takeProfitPct')}")
            print(f"  maxOpen={c.get('maxOpenPositions')} deny={len(c.get('scanDenySymbols', []))}")
            print(f"  noTradeWindows={c.get('noTradeWindows')}")
    except Exception as exc:
        print(f"API failed ({exc}), falling back to snapshot-only")
        update_snapshot(OPTIMIZATION_PATCH)

    # 2. Ensure snapshot is in sync
    update_snapshot(OPTIMIZATION_PATCH)

    # 3. Mark weak symbols in learning profiles
    update_learning_profiles(WEAK_SYMBOLS)

    print("\n=== Optimization Complete ===")


if __name__ == "__main__":
    main()
