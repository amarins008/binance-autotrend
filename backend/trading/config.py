"""Runtime config defaults and preset merge."""

import os

from trading.presets import PRO_STANDALONE_PRESET

# ── Schema version ────────────────────────────────────────────────────────
# Bump this when you change default values that should override stale
# snapshot config.  On the first restart after a bump, force-override keys
# listed in _FORCE_DEFAULTS to the new values.  Subsequent restarts
# respect the snapshot (user may have tuned).
CONFIG_VERSION = 17

# Keys that are force-overridden when _configVersion < CONFIG_VERSION.
# After the override, users can still change these via the dashboard; the
# override only fires on a version bump.
_FORCE_DEFAULTS_V1: dict = {
    # Guardian safety thresholds
    "guardianMinHoldSec": 180,
    "deadZoneExitSec": 600,
    "preemptiveLossExitMinEntryPct": 0.50,
    "preemptiveLossExitMinConfirmations": 2,
    "strongFlipMinConfidence": 0.90,
    # TV signal freshness
    "tvStaleEntrySec": 300,
    "tvExhaustionPenalty": 0.03,
    "tradingviewConfidenceBoost": 0.08,
    # Hold window
    "holdMinConfidence": 0.78,
    # Auto-enable TV after restart (user disabled → saved → re-enabled on bump)
    "tradingviewEnabled": True,
}
_FORCE_DEFAULTS_V2: dict = {
    # Enable TV-based early exit for Guardian (was disabled → Guardian never used TV)
    "tradingviewEarlyExitEnabled": True,
    "tradingviewEarlyExitMinStrength": 0.45,
}
_FORCE_DEFAULTS_V3: dict = {
    # Entry quality: higher min confidence, fewer trades per hour
    "minConfidence": 0.74,
    "maxTradesPerHour": 5,
}
_FORCE_DEFAULTS_V4: dict = {
    # Guardian: faster loss cut for 30+min bleeds
    "deadZoneExitSec": 480,
    "preemptiveLossExitMinEntryPct": 0.42,
    "preemptiveLossExitMaxEntryPct": 0.75,
    # Perf gate: lock out bad symbols sooner
    "perfGateMinWinRatePct": 38.0,
    "perfGateMinPnlUsdt": -0.40,
    "perfGateEarlyMinWinRatePct": 35.0,
    "perfGateEarlyMinPnlUsdt": -0.30,
}
_FORCE_DEFAULTS_V5: dict = {
    # Guardian: widen SL/TP + slower preemptive exit + extend dead zone
    "stopLossPct": 1.0,
    "takeProfitPct": 2.5,
    "deadZoneExitSec": 900,
    "preemptiveLossExitMinEntryPct": 0.70,
    "preemptiveLossExitMinConfirmations": 4,
}
_FORCE_DEFAULTS_V6: dict = {
    # Revert to Jun 3 baseline — current config too restrictive
    "minConfidence": 0.74,
    "stopLossPct": 1.0,
    "takeProfitPct": 2.5,
    "leverage": 15,
    "marginType": "CROSSED",
    "maxTradesPerHour": 8,
    "earlyEntryScoreGapMin": 1.7,
    "earlyEntryMinConfidence": 0.68,
    "profitLockTriggerUsdt": 0.30,
    "profitLockMaxGivebackUsdt": 0.18,
    "holdMinConfidence": 0.72,
    # TV aggressive caching to avoid 429 (lowered rate + longer cache after Aug 2026 429 storm)
    "tradingviewCacheTtl": 600,
    "tradingviewRateLimit": 2,
    "tradingviewTimeout": 10.0,
    "tradingviewMaxFailures": 10,
    "tradingviewNegativeCacheTtl": 120,  # longer backoff after a 429/empty
    "tvStaleEntrySec": 300,
    # TV entry confirmation gate (2026-08-22): reject entries whose TradingView
    # snapshot is stale (>tvEntryMaxAgeSec) or weak (<tvEntryMinConfidence).
    # Telemetry: tvAge>30s -> WR 12-17%; tvConf<0.7 -> net -4.78 USDT.
    "tvEntryMaxAgeSec": 30,
    "tvEntryMinConfidence": 0.70,
    # Guardian: hold/extend winners once upnl exceeds this floor (not only when
    # price literally taps TP). Fixes holdWinnerActivated=0/195 (TP set too far).
    "holdMinProfitUsdt": 0.03,
    # Guardian whipsaw grace period (2026-08-22): refuse EARLY_WHIPSAW_CUT during
    # the first whipGraceSec of a position's life so a normal dip can bounce
    # before the guardian cuts at the bottom.
    "whipGraceSec": 180,
    # SHORT-specific TV confidence floor (2026-08-22): SHORT only enters when TV
    # confidence >= this (telemetry: SHORT TV-conf>=0.7 was net-positive WR 62%,
    # while 0.6-0.7 SHORT lost WR 22%). Also blocks SHORT when TV signal=LONG.
    "shortTvMinConfidence": 0.70,
}

_FORCE_DEFAULTS_V7: dict = {
    # Strong-signal guardian: TV conflict must confirm internal structure
    # before closing. A pullback inside a still-valid trend is held with a
    # tightened stop instead of being closed at the bottom right before the
    # bounce. (Feature: "strong signal wrong side → close only on real
    # reversal; strong signal right side → extend TP + trail SL".)
    "tvEarlyExitStructureConfirm": True,
    "tvConflictConfirmationsRequired": 2,
    "tvPullbackTrailPct": 0.12,
}

_FORCE_DEFAULTS_V8: dict = {
    # Anti-early-exit round 2: guardian closed winners too fast (75% of green
    # exits kept running >0.3% within 20 min). Hard-override the trade rate and
    # require real swing breaks / held winners before taking profit early.
    "maxTradesPerHour": 5,
    "weakSignalMinHoldSec": 180,
    "weakSignalPeakMultiplier": 2.0,
    "swingPeakRequireMomentumAgainst": True,
}


_FORCE_DEFAULTS_V9: dict = {
    # Autotune hard ceiling: supervisor tuners (daily_entry_regression,
    # negative_expectancy, loss_streak_self_review) may tighten minConfidence
    # only up to this bound, and the effective adaptive_min_conf (learned +
    # session shift) is clamped to it as well. Prevents a bad day from
    # ratcheting the entry gate up to 0.92-0.93 where no candidate can ever
    # qualify — the bot goes quiet instead of protecting.
    "supervisorMinConfidenceCeiling": 0.72,
}


_FORCE_DEFAULTS_V10: dict = {
    # Performance audit 2026-08-01 (full-audit session):
    # 1) weak_payoff ratchet was tightening SL down to a 0.48% floor on every
    #    bad day (main.py set_float stopLossPct max(0.48,...)) → whipsaw SLs
    #    (29 SLs / 200 trades vs 2 TP hits) + TP targets drifting up → 41%
    #    DEAD_ZONE_TIMEOUT. Restore the proven baseline and cap the ratchet:
    "stopLossPct": 0.9,
    "takeProfitPct": 1.8,
    "supervisorStopLossFloor": 0.80,       # weak_payoff may not tighten below this
    "supervisorTpTargetMinCeiling": 0.85,  # tpTargetMinUsdt raise cap
    "supervisorTpTargetMaxCeiling": 2.50,  # tpTargetMaxUsdt raise cap
    # 2) confidence zones (1,385 LIVE trades): 0.7-0.8 = -29.11 (WR 49%),
    #    0.8-0.9 = +1.42 (WR 54%), >=0.9 = -5.24 (WR 44% late-chase).
    #    Raise the hard floor to cut the 0.7-0.8 hole; cap the top to stop
    #    late entries at reversal tops:
    "minConfidenceFloor": 0.72,            # entry-path adaptive floor
    "minConfidenceHardFloor": 0.72,
    "scanPerfSoftFallbackEnabled": True,     # allow high-conf (>=0.90) symbols to bypass perf_lock
        # scan-board floor (was hardcoded 0.60)
    "maxEntryConfidence": 0.95,            # late-chase cap (raised 0.90->0.95 so 0.90-0.95 zone, e.g. ETH 0.918, is not needlessly rejected; >=0.95 still gated by WR-guard)
    # 3) momentum gate 0.065 was blocking every candidate (lastSkip weak_momentum
    #    0.010-0.016 for hours) — relax to keep entries flowing:
    "minMomentumStrength": 0.03,
}


_FORCE_DEFAULTS_V11: dict = {
    # Entry-quality gates (2026-08-01 V12 audit): candlestick patterns with
    # lifetime WR 37-46% (bearish_engulfing 46.3%/-12.24, doji 46.4%/-13.23,
    # hammer 39.8%/-7.40, 5m_hammer 36.7%/-7.02 over 2,616 LIVE trades) are
    # blocked at the pipeline entry gate; TV signals opposing the entry at
    # strength >= 0.60 are blocked as well (intel layer 1 blocks >= 0.75,
    # this closes the 0.60-0.75 soft-conflict zone where entries against TV
    # kept SL'ing in seconds — GIGGLE/MMT/COTI today).
    "blockedEntryPatterns": [
        "bearish_engulfing",
        "5m_bearish_engulfing",
        "15m_bearish_engulfing",
        "doji",
        "15m_doji",
        "hammer",
        "5m_hammer",
    ],
    "tvConflictBlockStrength": 0.45,
    # TV fresh and explicitly WAIT (neutral): entries against an undecided
    # TV are the NEUTRAL bucket (WR 50%, net -0.239 over 8 trades after
    # V13.3) — require extra confidence so we don't blind-enter while TV
    # has no edge. Lower than tvUnavailableMinConf only when TV says
    # nothing at all (n/a); a fresh WAIT is a deliberate non-confirmation.
    "tvWaitMinConf": 0.82,
}

_FORCE_DEFAULTS_V12: dict = {
    # V13.6 entry-timing audit (2026-08-02): NEUTRAL bucket (entering while
    # a fresh TV signal is explicitly WAIT / undecided) was WR 50% net
    # -0.239 over 8 LIVE trades after V13.3 — blind-entries while TV has no
    # edge. Fresh WAIT now requires conf >= tvWaitMinConf (0.88) instead of
    # falling through to the generic n/a floor (0.85).
    "tvWaitMinConf": 0.82,
}


# V13 (2026-08-14): evening-volatility guard. The 7-day telemetry (Thailand
# time) showed the US-overlap window (16-23 BKK) bleeds: WR 54% but payoff 0.58
# because avgLoss (-0.30) is ~2x avgWin (+0.17). Explicit, history-independent
# size + entry-gate reduction during that window. Bangkok time, not server UTC.
_FORCE_DEFAULTS_V13: dict = {
    "eveningVolatilityGuardEnabled": True,
    "eveningSessionHourStart": 16,
    "eveningSessionHourEnd": 23,
    "eveningSessionSizeMult": 0.70,
    "eveningSessionConfShift": 0.04,
    "eveningPreemptiveTighten": True,
    "eveningPreemptiveMaxEntryPct": 0.75,
    "eveningPreemptiveMinConfidence": 0.60,
    "eveningShortSizeMult": 0.60,
    "tvStaleSec": 900,
    "regimeSizingEnabled": True,
    "regimeVolSizeMult": 0.60,
    "regimeVolCapMult": 0.60,
    "regimeVolConfFloor": 0.05,
    "regimeCalmSizeMult": 1.25,
    "regimeCalmCapMult": 1.20,
    "regimeCalmWinStreakMin": 3,
    "regimeLowEdgeConfFloor": 0.03,
    "noTradeWindowsAutoEnabled": False,
    "shortMinConfidence": 0.80,
    "shortBaseSizeMult": 0.55,
    "longTpBoostPct": 0.35,
    # 2026-08-20 capital-preservation: cap per-trade notional at 40 USDT so a
    # 1% SL on a single position can never bleed more than ~0.4 USDT (was 80).
    "tradeNotionalCapUsdt": 40.0,
    "autoScanTradeNotionalCapUsdt": 40.0,
    # 2026-08-20: low-cap / meme symbols that repeatedly SL out (7d telemetry
    # PUMPUSDT -12 trades, LINKUSDT, ALICEUSDT, BOMEUSDT, WLDUSDT, REDUSDT).
    # Denied in BOTH scan and single-symbol mode via _risk_cooldown_resume_ok.
    "denySymbols": ["PUMPUSDT", "LINKUSDT", "ALICEUSDT", "BOMEUSDT", "WLDUSDT", "REDUSDT"],
}


_FORCE_DEFAULTS_V14: dict = {
    # TV entry confirmation gate + SHORT-specific gate + Guardian hold/whipsaw/TV-exit (2026-08-22)
    # Hard override (not soft) so these keys are enforced on version bump even if already present.
    # This prevents config drift where live values (e.g. shortTvMinConfidence=0.70) differ from defaults.
    "tvEntryMaxAgeSec": 30,
    "tvEntryMinConfidence": 0.60,
    "shortTvMinConfidence": 0.60,
    "holdMinProfitUsdt": 0.03,
    "whipGraceSec": 180,
    "tradingviewEarlyExitMinStrength": 0.60,
}

_FORCE_DEFAULTS_V15: dict = {
    # Guardian hold-on-profit fix (2026-08-22): thresholds were too high, never fired.
    # - tpExtendMinConfidence was hardcoded 0.78 -> lower to 0.55 (match holdMinConfidence)
    # - tpExtendMinScoreGap was hardcoded 1.2 -> lower to 0.5 (TV score gap more achievable)
    # - holdMinMomentumPct was hardcoded 0.15% -> lower to 0.05% (momentum threshold)
    # - holdMinConfidence in code default was 0.55 -> align config 0.72 but allow lower via key
    "tpExtendMinConfidence": 0.55,
    "tpExtendMinScoreGap": 0.5,
    "holdMinMomentumPct": 0.05,
    # Guardian: ensure hold logic evaluates even if TV briefly WAIT
    "holdAllowWaitSignal": True,
    # P1: time-based fallback to prevent LIVE_CLOSE timeout (reduced from 1800->900 per backtest)
    "liveCloseFallbackMinSec": 900,
    # P2: SL floor to prevent noise-whacks (local SL hits had min 0.43%)
    "slMinPct": 0.60,
    # P4: SHORT-specific tuning (SHORT wins 48.6%, conf floor 0.80 blocked 99% - lower to 0.65)
    "shortSlMult": 0.80,
    "shortConfFloor": 0.65,
    # P3: TV batch fetch rate-limit tuning (reduce 429 frequency)
    "tvBatchRateLimitPerMin": 2,       # lowered from 4 to avoid burst 429
    "tvBatchRetryBackoff": 2.0,        # seconds to wait on 429 before next batch
    "tvStaleSec": 900,                 # re-fetch after 15min (was 300 in places)

}
_FORCE_DEFAULTS_V16: dict = {
    # V16: transitional version — no new forced overrides beyond V15.
    # (Placeholder so the migration loop in apply_autotrade_defaults can
    # iterate V10..V17 without a NameError on _FORCE_DEFAULTS_V16.)
}
_FORCE_DEFAULTS_V17: dict = {
    # V17: allow small-capital sizing (capital 50 USDT, 3 concurrent trades).
    # Lowers the hard floors so usdtAmount/tradeNotionalCapUsdt can go to 10
    # and maxOpenPositions to 3 (capital-aware, not the old 20/6 minimums).
    "usdtAmount": 16.0,
    "tradeNotionalCapUsdt": 18.0,
    "maxOpenPositions": 3,
    "leverage": 10,
    "takeProfitPct": 3.0,
    "stopLossPct": 1.2,
    "holdWinnerEnabled": True,
    "holdMinProfitUsdt": 0.10,
    "weakSignalMinHoldSec": 240,
    "usdtTooSmallMultiplierMax": 1.0,
    "usdtTooSmallAction": "multiply",
    # Margin-aware sizing: auto-scale trade size to live account balance
    "marginBasedSizing": True,
    "marginRiskFraction": 0.33,
    "marginSizingMinUsdt": 5.0,
    "marginSizingMaxUsdt": 25.0,
    # Per-symbol target-profit-aware multiplier: scale each symbol's size so a
    # typical win lands in [0.5, 1.0] USDT (Boss directive).
    "perSymbolTargetProfitMinUsdt": 0.5,
    "perSymbolTargetProfitMaxUsdt": 1.0,
    # Entry confidence floor (2026-08-23 review: Boss confirmed 0.80 to allow
    # entries in low-confidence markets — median signal conf ~0.77, >=0.82 only
    # 34% of signals. 0.80 keeps the lossy 0.80-0.82 band open intentionally
    # for small-capital bot that needs trade frequency).
    "minConfidence": 0.80,
}


def _normalize_config_symbol(symbol: str) -> str:
    return str(symbol or "").upper().replace("/", "").strip()


def merge_preset(cfg: dict | None, preset: str = "pro") -> dict:
    out = dict(cfg or {})
    if preset.lower() in ("pro", "professional", "standalone_pro"):
        for k, v in PRO_STANDALONE_PRESET.items():
            if k == "executionMode":
                continue
            out.setdefault(k, v)
    return out


# Telemetry-backed entry safety boundary. The 0.70-0.80 confidence bucket was
# materially loss-making in the August 2026 review. All config normalization
# paths must preserve this floor; raise it only with out-of-sample evidence.
# NOTE: Boss confirmed 0.80 on 2026-08-23 (low-confidence market, small capital
# needs trade frequency). This reverses the 0.82 mandate from 2026-08-14.
ENTRY_MIN_CONFIDENCE_FLOOR = 0.72


def apply_autotrade_defaults(cfg: dict | None, *, preset: str | None = "pro") -> dict:
    """Apply PRO-oriented defaults; existing keys in cfg are preserved."""
    out = merge_preset(cfg, preset or "pro")
    out.setdefault("intervalSec", 25)
    out.setdefault("cooldownSec", 20)
    out.setdefault("maxTradesPerHour", 8)
    out.setdefault("allowFlip", False)
    out.setdefault("strongFlipEnabled", True)
    out.setdefault("strongFlipMinConfidence", 0.90)
    out.setdefault("strongFlipMinScoreGap", 1.5)
    out.setdefault("strongFlipUltraScoreGap", 2.2)
    out.setdefault("strongFlipUltraConfRelax", 0.08)
    out.setdefault("minConfidence", ENTRY_MIN_CONFIDENCE_FLOOR)
    try:
        # Boss directive: enforce a hard entry-confidence floor (0.82, see
        # ENTRY_MIN_CONFIDENCE_FLOOR). Use max() so a higher tuner-validated
        # value (e.g. 0.84, justified by out-of-sample evidence) is PRESERVED
        # rather than clobbered, while never allowing anything below the floor.
        out["minConfidence"] = max(
            ENTRY_MIN_CONFIDENCE_FLOOR,
            float(out.get("minConfidence") or ENTRY_MIN_CONFIDENCE_FLOOR),
        )
    except (TypeError, ValueError):
        out["minConfidence"] = ENTRY_MIN_CONFIDENCE_FLOOR
    try:
        out["minConfidenceHardFloor"] = max(
            ENTRY_MIN_CONFIDENCE_FLOOR,
            float(out.get("minConfidenceHardFloor", 0.0) or 0.0),
        )
    except (TypeError, ValueError):
        out["minConfidenceHardFloor"] = ENTRY_MIN_CONFIDENCE_FLOOR
    out.setdefault("htfStrictEnabled", True)
    out.setdefault("htfMinStrength", 0.28)
    out.setdefault("requireVisionConsensus", False)
    # Capital-aware default: 3 concurrent trades (Boss: 50 USDT capital).
    # V17 forces this; kept as setdefault so older presets still land here,
    # but the marketScan block below hard-clamps to 3 regardless of stored value.
    out.setdefault("maxOpenPositions", 3)
    out.setdefault("maxSpreadBps", 16.0)
    out.setdefault("maxSlippageBps", 18.0)
    out.setdefault("noTradeWindows", [])
    out.setdefault("trailingStopPct", 0.0)
    out.setdefault("takeProfitPct", 2.5)
    out.setdefault("stopLossPct", 1.0)
    out.setdefault("minRiskRewardRatio", 1.5)
    out.setdefault("atrTpSlEnabled", True)
    out.setdefault("ema200StrictEnabled", True)
    out.setdefault("usdtAmount", 25.0)
    out.setdefault("leverage", 15)
    out.setdefault("leverageMin", 3)
    out.setdefault("leverageMax", 25)
    out.setdefault("leverageAutoEnabled", True)
    out.setdefault("adaptiveLeverageEnabled", True)
    # Respect an explicit operator-set adaptiveLeverageMax (e.g. 15) — do NOT
    # silently lift it up to leverageMax. Only clamp to the 25x hard ceiling.
    if "adaptiveLeverageMax" not in out or out.get("adaptiveLeverageMax") in (None, ""):
        out["adaptiveLeverageMax"] = 25
    try:
        out["adaptiveLeverageMax"] = max(1, min(25, int(out.get("adaptiveLeverageMax", 25) or 25)))
    except (TypeError, ValueError):
        out["adaptiveLeverageMax"] = 25
    out.setdefault("marginType", "CROSSED")
    out.setdefault("aggressiveScalp", False)
    out.setdefault("waitOverrideImbalance", 0.08)
    out.setdefault("earlyEntryEnabled", True)
    out.setdefault("earlyEntryScoreGapMin", 1.7)
    out.setdefault("earlyEntryMinConfidence", 0.68)
    out.setdefault("staleWaitSymbolSkipEnabled", True)
    out.setdefault("staleWaitSymbolSkipCycles", 6)
    out.setdefault("staleWaitSymbolLockMinutes", 20)
    out.setdefault("lateEntryMaxBbPctB", 0.90)
    out.setdefault("lateEntryMaxVwapDistancePct", 0.32)
    out.setdefault("skipFundingAgainst", 0.0)
    out.setdefault("preReversalScoreBlock", 0.65)
    out.setdefault("preReversalScoreSoftener", 0.20)
    out.setdefault("holdWinners", True)
    out.setdefault("holdMinConfidence", 0.72)
    out.setdefault("holdTrailPct", 0.32)
    out.setdefault("holdMinMomentumPct", 0.05)
    out.setdefault("holdAllowWaitSignal", True)
    out.setdefault("tpExtendMinConfidence", 0.55)
    out.setdefault("tpExtendMinScoreGap", 0.5)
    out.setdefault("liveCloseFallbackMinSec", 1800)
    out.setdefault("slMinPct", 0.60)
    out.setdefault("shortSlMult", 0.80)
    out.setdefault("shortConfFloor", 0.80)
    out.setdefault("tvBatchRateLimitPerMin", 4)
    out.setdefault("tvBatchRetryBackoff", 2.0)
    out.setdefault("tvStaleSec", 900)
    out.setdefault("profitLockTriggerUsdt", 0.25)
    out.setdefault("profitLockKeepUsdt", 0.10)
    out.setdefault("profitLockMaxGivebackUsdt", 0.18)
    out.setdefault("payoffLossGuardEnabled", True)
    out.setdefault("payoffLossGuardMinTrades", 6)
    out.setdefault("payoffLossGuardWindowTrades", 8)
    out.setdefault("payoffLossGuardMaxPayoffRatio", 0.75)
    out.setdefault("payoffLossGuardLossToWinCap", 0.95)
    out.setdefault("payoffLossGuardMinLossUsdt", 0.18)
    out.setdefault("payoffLossGuardMaxLossUsdt", 0.75)
    out.setdefault("preemptiveLossExitEnabled", True)
    out.setdefault("preemptiveLossExitMinConfidence", 0.55)
    out.setdefault("preemptiveLossExitMinEntryPct", 0.50)
    out.setdefault("preemptiveLossExitMaxEntryPct", 0.85)
    out.setdefault("preemptiveLossExitMinConfirmations", 2)
    out.setdefault("swingDecelerationEnabled", True)
    out.setdefault("tryGreenExitEnabled", True)
    out.setdefault("tryGreenExitMinProfitUsdt", 0.06)
    out.setdefault("tryGreenExitMaxProfitUsdt", 0.15)
    out.setdefault("tryGreenExitMinPriorLossUsdt", 0.20)
    out.setdefault("tryGreenExitMinRecoveryPct", 0.70)
    out.setdefault("guardianMinHoldSec", 180)
    out.setdefault("deadZoneExitSec", 600)
    # ── Guardian: proactive trail + TP extension (Improvement 1) ──
    out.setdefault("proactiveTrailEnabled", True)
    out.setdefault("proactiveTrailMinProfitPct", 0.25)
    out.setdefault("proactiveTrailStepPct", 0.12)
    out.setdefault("proactiveTrailMaxSlFromEntryPct", 0.40)
    out.setdefault("proactiveTrailTpExtendPct", 0.08)
    # ── Guardian: adaptive preemptive exit (Improvement 2) ──
    out.setdefault("preemptiveExitAdaptiveEnabled", True)
    out.setdefault("preemptiveExitHighConfMinEntryPct", 0.15)
    out.setdefault("preemptiveExitHighConfThreshold", 0.75)
    out.setdefault("preemptiveExitVolumeConfirmEnabled", True)
    out.setdefault("preemptiveExitRecoveryPenaltyPct", 0.12)
    # ── Guardian: swing peak detection (Improvement 3) ──
    out.setdefault("swingPeakDetectionEnabled", True)
    out.setdefault("swingPeakLookbackCycles", 8)
    out.setdefault("swingPeakRsiOverbought", 72)
    out.setdefault("swingPeakRsiOversold", 38)
    out.setdefault("swingPeakBbUpperPct", 0.78)
    out.setdefault("swingPeakBbLowerPct", 0.22)
    out.setdefault("swingPeakMinProfitUsdt", 0.08)
    out.setdefault("swingPeakMomDecelThreshold", 0.30)
    out.setdefault("slCandleAdaptiveEnabled", True)
    out.setdefault("slCandleLookback", 5)
    out.setdefault("candlePatternLookback", 5)
    out.setdefault("slToTpRatio", 0.5)
    out.setdefault("tpSlTargetUsdtEnabled", True)
    out.setdefault("tpTargetMinUsdt", 0.55)
    out.setdefault("tpTargetMaxUsdt", 2.2)
    out.setdefault("feeMinNetProfitUSDT", 0.06)
    out.setdefault("feeMinEdgeVsCostMultiple", 1.35)
    out.setdefault("feeMinOrderUsdt", 5.0)
    out.setdefault("tradeNotionalCapUsdt", 80.0)
    out.setdefault("autoScanTradeNotionalCapUsdt", 80.0)
    out.setdefault("adaptiveSizing", True)
    out.setdefault("adaptiveSizeBoostMaxPct", 18.0)
    out.setdefault("supervisorSizeStreakEnabled", True)
    out.setdefault("supervisorSizeMultiplier", 1.0)
    out.setdefault("supervisorSizeWinStreakMin", 3)
    out.setdefault("supervisorSizeLossStreakMin", 2)
    out.setdefault("supervisorSizeWinStepPct", 10.0)
    out.setdefault("supervisorSizeLossStepPct", 20.0)  # Increased from 15.0 to 20.0 for faster risk reduction
    out.setdefault("supervisorSizeMaxMultiplier", 1.35)
    out.setdefault("supervisorSizeMinMultiplier", 0.35)  # Reduced from 0.50 to 0.35 for deeper risk reduction
    out.setdefault("supervisorSizeDiversifiedMinMultiplier", 0.55)  # Reduced from 0.65 to 0.55
    out.setdefault("adaptiveLossStreakEnabled", True)  # New: Enable adaptive sizing based on loss streak
    out.setdefault("adaptiveLossStreakThreshold", 3)  # Start reducing size after 3 consecutive losses
    out.setdefault("adaptiveLossStreakMaxReduction", 0.50)  # Maximum 50% size reduction during severe loss streaks
    out.setdefault("sessionBasedAdjustments", True)  # New: Enable session-based adjustments instead of time blocks
    out.setdefault("sessionAsianMultiplier", 0.85)  # Asian session size multiplier
    out.setdefault("sessionLondonMultiplier", 1.15)  # London session size multiplier
    out.setdefault("sessionUSOverlapMultiplier", 1.2)  # US overlap size multiplier
    out.setdefault("sessionUSAfternoonMultiplier", 1.05)  # US afternoon size multiplier
    out.setdefault("sessionAsianEveningMultiplier", 0.9)  # Asian evening size multiplier
    out.setdefault("minMomentumStrength", 0.065)  # Minimum momentum strength required for entries
    out.setdefault("momentumDirectionConfirmation", True)  # Require momentum direction to match signal
    out.setdefault("divergenceFilterEnabled", True)  # Enable divergence detection filter
    out.setdefault("volumeSpikeThreshold", 3.0)  # Volume spike threshold (multiple of average)
    out.setdefault("wickRejectionThreshold", 0.4)  # Wick size threshold for rejection detection
    out.setdefault("keyLevelBuffer", 0.08)  # Buffer around key levels (BB extremes)
    out.setdefault("orderFlowConfirmation", False)  # Require order flow confirmation (disabled: too aggressive)
    out.setdefault("minOrderFlowImbalance", 0.03)  # Minimum order flow imbalance for confirmation
    out.setdefault("tradingviewEnabled", True)  # TradingView MCP integration
    # Allow enabling TradingView MCP via env var (set by Start Binance AutoTrade.bat).
    # Only applies when the user has not explicitly set tradingviewEnabled in config.
    if (
        "tradingviewEnabled" not in (cfg or {})
        and str(os.getenv("TRADINGVIEW_ENABLED", "")).lower() in ("1", "true", "yes", "on")
    ):
        out["tradingviewEnabled"] = True
    out.setdefault("tradingviewApiKey", "")  # TradingView API key
    out.setdefault("tradingviewApiSecret", "")  # TradingView API secret
    out.setdefault("tradingviewWebhookUrl", "")  # TradingView webhook URL
    out.setdefault("tradingviewCacheTtl", 600)  # Cache TTL in seconds (longer to avoid 429)
    out.setdefault("tradingviewRateLimit", 2)  # Rate limit per minute (lowered from 6)
    out.setdefault("tradingviewTimeout", 10.0)  # API timeout in seconds
    out.setdefault("tradingviewConfidenceBoost", 0.08)  # Confidence boost on confirmation
    out.setdefault("tradingviewMaxFailures", 10)
    out.setdefault("tradingviewNegativeCacheTtl", 120)  # Longer backoff after 429/empty
    out.setdefault("tvUnavailableMinConf", 0.80)  # Conservative floor when TV signal is unavailable
    out.setdefault("tvWaitMinConf", 0.88)  # Fresh explicit TV WAIT needs extra confidence (V13.6)
    out.setdefault("tvStaleEntrySec", 300)  # TV signal age limit for entry boost (seconds)
    out.setdefault("tvExhaustionPenalty", 0.03)  # Penalty per exhausted oscillator (RSI/STOCH/CCI)
    # TradingView position management (global defaults)
    out.setdefault("tradingviewTpExtensionEnabled", True)  # Enable TP extension based on TradingView
    out.setdefault("tradingviewTpExtensionMinStrength", 0.7)  # Min strength for TP extension
    out.setdefault("tradingviewTpExtensionBasePct", 0.2)  # Base TP extension percentage
    out.setdefault("tradingviewTpExtensionMaxPct", 0.5)  # Max TP extension percentage
    out.setdefault("tradingviewSlTrailingEnabled", True)  # Enable SL trailing based on TradingView
    out.setdefault("tradingviewSlTrailingMinStrength", 0.6)  # Min strength for SL trailing
    out.setdefault("tradingviewSlTrailingBasePct", 0.15)  # Base SL trailing percentage
    out.setdefault("tradingviewSlTrailingMaxPct", 0.3)  # Max SL trailing percentage
    out.setdefault("tradingviewEarlyExitEnabled", True)  # Enable early exit based on TradingView
    out.setdefault("tradingviewEarlyExitMinStrength", 0.45)  # Min strength for early exit (TV rarely >0.7)
    # Per-symbol overrides (optional - overrides global defaults)
    out.setdefault("tradingviewTpExtensionOverride", {})  # {"BTCUSDT": True, "ETHUSDT": False}
    out.setdefault("tradingviewSlTrailingOverride", {})  # {"BTCUSDT": True, "ETHUSDT": False}
    out.setdefault("tradingviewEarlyExitOverride", {})  # {"BTCUSDT": True, "ETHUSDT": False}
    out.setdefault("volTargetEnabled", True)
    out.setdefault("volTargetPct", 0.2)
    out.setdefault("volSizeMaxMult", 1.15)
    out.setdefault("marketScan", True)
    out.setdefault("scanTopLiquid", 60)
    out.setdefault("scanAnalyzeTop", 12)
    out.setdefault("scanGuardedFallbackAnalyzeTop", 18)
    out.setdefault("scanDenySymbols", ["XAUUSDT", "XAGUSDT", "SPCXUSDT", "CLUSDT", "MRVLUSDT", "INTCUSDT"])
    out.setdefault("supervisorTargetOpenPositionsMin", 3)
    out.setdefault("supervisorTargetOpenPositionsMax", 6)
    out.setdefault("scanPerSymbolTimeoutSec", 7.5)
    out.setdefault("scanFallbackNearEnabled", True)
    out.setdefault("scanFallbackNearConfRelax", 0.04)
    out.setdefault("orphanAutoAdoptEnabled", True)
    out.setdefault("orphanAutoAdoptForceSingleSymbol", False)
    out.setdefault("orphanAutoAdoptMultiEnabled", True)
    out.setdefault("pairLockEnabled", True)
    out.setdefault("pairLockLossStreak", 2)
    out.setdefault("pairLockMinutes", 60)
    out.setdefault("benchmarkFilterEnabled", True)
    out.setdefault("liveBadUtcHours", [])
    if isinstance(out.get("liveBadUtcHours"), list):
        unlocked_hours = []
        for h in out["liveBadUtcHours"]:
            try:
                hour = int(h)
            except (TypeError, ValueError):
                continue
            if hour != 7:
                unlocked_hours.append(hour)
        out["liveBadUtcHours"] = unlocked_hours
    out.setdefault("riskCooldownEnabled", True)  # V13.8: per-symbol cooldown after loss streaks (was False)
    out.setdefault("riskCooldownAdaptiveMarket", True)
    out.setdefault("riskCooldownAdaptiveCheckSec", 30)
    out.setdefault("riskCooldownLightScanEnabled", True)
    out.setdefault("riskCooldownLightScanSec", 45)
    out.setdefault("riskCooldownLightScanAnalyzeTop", 6)
    out.setdefault("riskCooldownPauseOnVolatile", True)
    out.setdefault("riskCooldownVolatileMinutes", 10)
    out.setdefault("riskCooldownResumeScoreGapMin", 2.0)
    out.setdefault("maxDailyTradesPerSymbol", 14)
    out.setdefault("perfGateMinSamples", 8)
    out.setdefault("perfGateMinWinRatePct", 40)
    out.setdefault("perfGateMinPnlUsdt", -0.5)
    out.setdefault("perfLockMinutes", 45)
    out.setdefault("learningRewardEnabled", True)
    out.setdefault("learningBehaviorRewardEnabled", True)
    out.setdefault("memoryPrimaryDays", 7)
    out.setdefault("memoryConfirmDays", 15)
    out.setdefault("memoryBaselineDays", 30)
    out.setdefault("memoryArchiveWeightPct", 0.0)
    out.setdefault("learningRewardWin", 1.0)
    out.setdefault("learningPenaltyLoss", 0.8)
    out.setdefault("learningRewardDecay", 0.985)
    out.setdefault("learningRewardCap", 50.0)
    out.setdefault("learningPnlClipAbsUsdt", 25.0)
    out.setdefault("learningRewardEntryTiming", 0.25)
    out.setdefault("learningRewardHoldWinner", 0.2)
    out.setdefault("learningRewardTpHitBase", 0.2)
    out.setdefault("learningRewardTpScalePerUsdt", 0.35)
    out.setdefault("learningRewardFlipGood", 0.1)
    out.setdefault("learningPenaltyEarlySl", 0.35)
    out.setdefault("learningPenaltyMemoryMiss", 0.15)
    out.setdefault("learningBehaviorDeltaCap", 2.5)
    out.setdefault("learningFastWinMinutes", 45)
    if bool(out.get("marketScan")) or str(out.get("symbol", "")).upper() in {"AUTO", "SCAN"}:
        try:
            cap_usdt = max(20.0, min(120.0, float(out.get("autoScanTradeNotionalCapUsdt", 80.0) or 80.0)))
        except (TypeError, ValueError):
            cap_usdt = 80.0
        out["autoScanTradeNotionalCapUsdt"] = cap_usdt
        out["tradeNotionalCapUsdt"] = min(
            cap_usdt,
            max(10.0, float(out.get("tradeNotionalCapUsdt", cap_usdt) or cap_usdt)),
        )
        try:
            out["usdtAmount"] = max(10.0, min(cap_usdt, float(out.get("usdtAmount", cap_usdt) or cap_usdt)))
        except (TypeError, ValueError):
            out["usdtAmount"] = cap_usdt
        try:
            out["adaptiveSizeBoostMaxPct"] = min(
                12.0,
                max(0.0, float(out.get("adaptiveSizeBoostMaxPct", 12.0) or 12.0)),
            )
        except (TypeError, ValueError):
            out["adaptiveSizeBoostMaxPct"] = 12.0
        try:
            out["sessionBiasMaxSizeShiftPct"] = min(
                10.0,
                max(0.0, float(out.get("sessionBiasMaxSizeShiftPct", 10.0) or 10.0)),
            )
        except (TypeError, ValueError):
            out["sessionBiasMaxSizeShiftPct"] = 10.0
        try:
            out["volSizeMaxMult"] = min(1.15, max(0.8, float(out.get("volSizeMaxMult", 1.15) or 1.15)))
        except (TypeError, ValueError):
            out["volSizeMaxMult"] = 1.15
        try:
            # Capital-aware hard clamp: never exceed 3 concurrent trades on
            # small capital (Boss directive). Overrides any stored value.
            out["maxOpenPositions"] = 3
        except (TypeError, ValueError):
            out["maxOpenPositions"] = 3
        try:
            out["scanTopLiquid"] = max(60, min(120, int(out.get("scanTopLiquid", 60) or 60)))
        except (TypeError, ValueError):
            out["scanTopLiquid"] = 60
        try:
            out["scanAnalyzeTop"] = max(12, min(30, int(out.get("scanAnalyzeTop", 12) or 12)))
        except (TypeError, ValueError):
            out["scanAnalyzeTop"] = 12
        try:
            out["scanGuardedFallbackAnalyzeTop"] = max(
                int(out.get("scanAnalyzeTop", 12) or 12),
                min(30, int(out.get("scanGuardedFallbackAnalyzeTop", 18) or 18)),
            )
        except (TypeError, ValueError):
            out["scanGuardedFallbackAnalyzeTop"] = 18
        out["scanDenySymbols"] = [
            _normalize_config_symbol(v)
            for v in out.get("scanDenySymbols", [])
            if str(v).strip()
        ]
        try:
            target_min = max(3, min(6, int(out.get("supervisorTargetOpenPositionsMin", 3) or 3)))
        except (TypeError, ValueError):
            target_min = 3
        try:
            target_max = max(6, target_min, min(6, int(out.get("supervisorTargetOpenPositionsMax", 6) or 6)))
        except (TypeError, ValueError):
            target_max = 6
        out["supervisorTargetOpenPositionsMin"] = target_min
        out["supervisorTargetOpenPositionsMax"] = target_max
        try:
            diversified_floor = max(
                0.1,
                min(1.0, float(out.get("supervisorSizeDiversifiedMinMultiplier", 0.65) or 0.65)),
            )
            out["supervisorSizeMinMultiplier"] = max(
                diversified_floor,
                min(1.0, float(out.get("supervisorSizeMinMultiplier", 0.50) or 0.50)),
            )
            out["supervisorSizeMultiplier"] = max(
                diversified_floor,
                float(out.get("supervisorSizeMultiplier", 1.0) or 1.0),
            )
        except (TypeError, ValueError):
            out["supervisorSizeMinMultiplier"] = 0.65
            out["supervisorSizeMultiplier"] = 1.0
    # Self-heal legacy strict flip config that blocks valid reversals too often.
    try:
        out["strongFlipMinScoreGap"] = min(2.0, max(0.8, float(out.get("strongFlipMinScoreGap", 1.5) or 1.5)))
    except Exception:
        out["strongFlipMinScoreGap"] = 1.5
    try:
        out["strongFlipUltraScoreGap"] = min(3.5, max(out["strongFlipMinScoreGap"] + 0.2, float(out.get("strongFlipUltraScoreGap", 2.2) or 2.2)))
    except Exception:
        out["strongFlipUltraScoreGap"] = 2.2
    out["engineVersion"] = "pro-5.1"  # Guardian V5.1: wider SL/TP, slower preemptive exit
    # ── Force-override stale snapshot config on version bump ───────────────
    # When CONFIG_VERSION increases, keys in _FORCE_DEFAULTS_VN are
    # overwritten to their new defaults — regardless of what the snapshot
    # had stored.  The snapshot is then saved with the new version so the
    # override only fires once (the first restart after a code update).
    try:
        _stored_ver = int(out.get("_configVersion", 0) or 0)
    except (TypeError, ValueError):
        _stored_ver = 0
    if _stored_ver < CONFIG_VERSION:
        for _fk, _fv in _FORCE_DEFAULTS_V1.items():
            out[_fk] = _fv
        if _stored_ver < 2:
            for _fk, _fv in _FORCE_DEFAULTS_V2.items():
                out[_fk] = _fv
        if _stored_ver < 3:
            for _fk, _fv in _FORCE_DEFAULTS_V3.items():
                out[_fk] = _fv
        if _stored_ver < 4:
            for _fk, _fv in _FORCE_DEFAULTS_V4.items():
                out[_fk] = _fv
        if _stored_ver < 5:
            for _fk, _fv in _FORCE_DEFAULTS_V5.items():
                out[_fk] = _fv
        if _stored_ver < 6:
            for _fk, _fv in _FORCE_DEFAULTS_V6.items():
                out[_fk] = _fv
        if _stored_ver < 7:
            for _fk, _fv in _FORCE_DEFAULTS_V7.items():
                if _fk not in out or out[_fk] is None:
                    out[_fk] = _fv
        if _stored_ver < 8:
            for _fk, _fv in _FORCE_DEFAULTS_V8.items():
                out[_fk] = _fv
        if _stored_ver < 9:
            for _fk, _fv in _FORCE_DEFAULTS_V9.items():
                out[_fk] = _fv
        if _stored_ver < 10:
            for _fk, _fv in _FORCE_DEFAULTS_V10.items():
                out[_fk] = _fv
        if _stored_ver < 11:
            for _fk, _fv in _FORCE_DEFAULTS_V11.items():
                out[_fk] = _fv
        if _stored_ver < 12:
            for _fk, _fv in _FORCE_DEFAULTS_V12.items():
                out[_fk] = _fv
        if _stored_ver < 13:
            for _fk, _fv in _FORCE_DEFAULTS_V13.items():
                out[_fk] = _fv
        if _stored_ver < 14:
            for _fk, _fv in _FORCE_DEFAULTS_V14.items():
                out[_fk] = _fv
        if _stored_ver < 15:
            for _fk, _fv in _FORCE_DEFAULTS_V15.items():
                out[_fk] = _fv
        if _stored_ver < 16:
            for _fk, _fv in _FORCE_DEFAULTS_V16.items():
                out[_fk] = _fv
        if _stored_ver < 17:
            for _fk, _fv in _FORCE_DEFAULTS_V17.items():
                out[_fk] = _fv
        out["_configVersion"] = CONFIG_VERSION

    # Keep this final: version migrations above may intentionally overwrite
    # legacy defaults, but they must never reopen the known-loss confidence band.
    try:
        out["minConfidence"] = max(ENTRY_MIN_CONFIDENCE_FLOOR, float(out.get("minConfidence") or 0.0))
    except (TypeError, ValueError):
        out["minConfidence"] = ENTRY_MIN_CONFIDENCE_FLOOR
    try:
        out["minConfidenceHardFloor"] = max(
            ENTRY_MIN_CONFIDENCE_FLOOR,
            float(out.get("minConfidenceHardFloor", 0.0) or 0.0),
        )
    except (TypeError, ValueError):
        out["minConfidenceHardFloor"] = ENTRY_MIN_CONFIDENCE_FLOOR
    return out
