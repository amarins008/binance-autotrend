"""Runtime config defaults and preset merge."""

from trading.presets import PRO_STANDALONE_PRESET


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


def apply_autotrade_defaults(cfg: dict | None, *, preset: str | None = "pro") -> dict:
    """Apply PRO-oriented defaults; existing keys in cfg are preserved."""
    out = merge_preset(cfg, preset or "pro")
    out.setdefault("intervalSec", 25)
    out.setdefault("cooldownSec", 20)
    out.setdefault("maxTradesPerHour", 8)
    out.setdefault("allowFlip", False)
    out.setdefault("strongFlipEnabled", True)
    out.setdefault("strongFlipMinConfidence", 0.76)
    out.setdefault("strongFlipMinScoreGap", 1.5)
    out.setdefault("strongFlipUltraScoreGap", 2.2)
    out.setdefault("strongFlipUltraConfRelax", 0.08)
    out.setdefault("minConfidence", 0.66)
    out.setdefault("htfStrictEnabled", True)
    out.setdefault("htfMinStrength", 0.28)
    out.setdefault("requireVisionConsensus", False)
    out.setdefault("maxOpenPositions", 6)
    out.setdefault("maxSpreadBps", 16.0)
    out.setdefault("maxSlippageBps", 18.0)
    out.setdefault("noTradeWindows", [])
    out.setdefault("trailingStopPct", 0.0)
    out.setdefault("takeProfitPct", 1.8)
    out.setdefault("stopLossPct", 0.9)
    out.setdefault("minRiskRewardRatio", 1.5)
    out.setdefault("atrTpSlEnabled", True)
    out.setdefault("ema200StrictEnabled", True)
    out.setdefault("usdtAmount", 10.0)
    out.setdefault("leverage", 5)
    out.setdefault("leverageMin", 3)
    out.setdefault("leverageMax", 25)
    out.setdefault("leverageAutoEnabled", True)
    out.setdefault("adaptiveLeverageEnabled", True)
    out.setdefault("adaptiveLeverageMax", 25)
    try:
        lev_max = int(out.get("leverageMax", 25) or 25)
        adaptive_max = int(out.get("adaptiveLeverageMax", lev_max) or lev_max)
        if adaptive_max < lev_max:
            out["adaptiveLeverageMax"] = min(25, lev_max)
    except (TypeError, ValueError):
        out["adaptiveLeverageMax"] = 25
    out.setdefault("marginType", "CROSSED")
    out.setdefault("aggressiveScalp", False)
    out.setdefault("waitOverrideImbalance", 0.08)
    out.setdefault("earlyEntryEnabled", True)
    out.setdefault("earlyEntryScoreGapMin", 1.4)
    out.setdefault("earlyEntryMinConfidence", 0.60)
    out.setdefault("staleWaitSymbolSkipEnabled", True)
    out.setdefault("staleWaitSymbolSkipCycles", 6)
    out.setdefault("staleWaitSymbolLockMinutes", 20)
    out.setdefault("lateEntryMaxBbPctB", 0.90)
    out.setdefault("lateEntryMaxVwapDistancePct", 0.32)
    out.setdefault("skipFundingAgainst", 0.0)
    out.setdefault("holdWinners", True)
    out.setdefault("holdMinConfidence", 0.78)
    out.setdefault("holdTrailPct", 0.32)
    out.setdefault("profitLockTriggerUsdt", 0.35)
    out.setdefault("profitLockKeepUsdt", 0.15)
    out.setdefault("profitLockMaxGivebackUsdt", 0.22)
    out.setdefault("payoffLossGuardEnabled", True)
    out.setdefault("payoffLossGuardMinTrades", 6)
    out.setdefault("payoffLossGuardWindowTrades", 8)
    out.setdefault("payoffLossGuardMaxPayoffRatio", 0.75)
    out.setdefault("payoffLossGuardLossToWinCap", 0.95)
    out.setdefault("payoffLossGuardMinLossUsdt", 0.18)
    out.setdefault("payoffLossGuardMaxLossUsdt", 0.75)
    out.setdefault("slCandleAdaptiveEnabled", True)
    out.setdefault("slCandleLookback", 5)
    out.setdefault("candlePatternLookback", 5)
    out.setdefault("slToTpRatio", 0.5)
    out.setdefault("tpSlTargetUsdtEnabled", True)
    out.setdefault("tpTargetMinUsdt", 0.55)
    out.setdefault("tpTargetMaxUsdt", 2.2)
    out.setdefault("feeMinNetProfitUSDT", 0.1)
    out.setdefault("feeMinEdgeVsCostMultiple", 1.55)
    out.setdefault("feeMinOrderUsdt", 20.0)
    out.setdefault("tradeNotionalCapUsdt", 80.0)
    out.setdefault("autoScanTradeNotionalCapUsdt", 80.0)
    out.setdefault("adaptiveSizing", True)
    out.setdefault("adaptiveSizeBoostMaxPct", 18.0)
    out.setdefault("supervisorSizeStreakEnabled", True)
    out.setdefault("supervisorSizeMultiplier", 1.0)
    out.setdefault("supervisorSizeWinStreakMin", 3)
    out.setdefault("supervisorSizeLossStreakMin", 2)
    out.setdefault("supervisorSizeWinStepPct", 10.0)
    out.setdefault("supervisorSizeLossStepPct", 15.0)
    out.setdefault("supervisorSizeMaxMultiplier", 1.35)
    out.setdefault("supervisorSizeMinMultiplier", 0.50)
    out.setdefault("supervisorSizeDiversifiedMinMultiplier", 0.65)
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
    out.setdefault("riskCooldownEnabled", False)
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
    out.setdefault("perfLockMinutes", 90)
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
            max(20.0, float(out.get("tradeNotionalCapUsdt", cap_usdt) or cap_usdt)),
        )
        try:
            out["usdtAmount"] = max(20.0, min(cap_usdt, float(out.get("usdtAmount", cap_usdt) or cap_usdt)))
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
            out["maxOpenPositions"] = max(6, min(12, int(out.get("maxOpenPositions", 6) or 6)))
        except (TypeError, ValueError):
            out["maxOpenPositions"] = 6
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
    out["engineVersion"] = "pro-2.0"
    return out
