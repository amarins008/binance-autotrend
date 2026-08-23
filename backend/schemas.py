from pydantic import BaseModel, Field
from typing import Literal


class OrderBookLevel(BaseModel):
    price: float
    qty: float


class OrderBookSummary(BaseModel):
    symbol: str
    bidNotional: float
    askNotional: float
    imbalance: float
    spoofingRisk: Literal["LOW", "MEDIUM", "HIGH"]
    buyWall: OrderBookLevel | None = None
    sellWall: OrderBookLevel | None = None


class AnalyzeRequest(BaseModel):
    symbol: str
    orderBook: OrderBookSummary


class TradeRequest(BaseModel):
    symbol: str
    side: Literal["LONG", "SHORT", "WAIT", "CLOSE"]
    qty: float = Field(gt=0)
    price: float | None = None
    takeProfitPct: float | None = None
    stopLossPct: float | None = None


class VisionAnalyzeRequest(BaseModel):
    symbol: str
    imageDataUrl: str


class IntelAnalyzeRequest(BaseModel):
    symbol: str


class CoinRankRequest(BaseModel):
    symbols: list[str] = Field(default_factory=list)
    scanMarket: bool = True
    scanTopLiquid: int = Field(default=60, ge=5, le=120)
    scanAnalyzeTop: int = Field(default=12, ge=3, le=30)
    whitelistSymbols: list[str] = Field(default_factory=list)
    topN: int = Field(default=10, ge=1, le=30)


class StrategyParseRequest(BaseModel):
    command: str
    symbol: str


class StrategyPlan(BaseModel):
    command: str
    symbol: str
    triggerType: Literal["BREAKOUT_ABOVE", "BREAKDOWN_BELOW", "MARKET_NOW"]
    triggerPrice: float | None = None
    side: Literal["LONG", "SHORT"]
    quantity: float = Field(gt=0)
    takeProfitPct: float = Field(gt=0)
    stopLossPct: float = Field(gt=0)
    trailingStopPct: float = Field(gt=0)
    active: bool = True


class MonitorStartRequest(BaseModel):
    plan: StrategyPlan
    intervalSec: int = Field(default=10, ge=3, le=60)


class RiskConfig(BaseModel):
    killSwitch: bool
    maxNotionalUSDT: float
    maxLeverage: float = Field(ge=1, le=25)
    maxDailyLossUSDT: float


class AutoTradeStartRequest(BaseModel):
    symbol: str = "AUTO"
    usdtAmount: float = Field(gt=0)
    leverage: int = Field(default=5, ge=1, le=25)
    leverageMin: int = Field(default=3, ge=1, le=25)
    leverageMax: int = Field(default=25, ge=1, le=25)
    leverageAutoEnabled: bool = True
    adaptiveLeverageEnabled: bool = True
    adaptiveLeverageMax: int = Field(default=25, ge=1, le=25)
    marginType: Literal["ISOLATED", "CROSSED"] = "ISOLATED"
    takeProfitPct: float = Field(default=1.8, gt=0)
    stopLossPct: float = Field(default=0.8, gt=0)
    intervalSec: int = Field(default=20, ge=5, le=120)
    minConfidence: float = Field(default=0.72, ge=0.0, le=1.0)
    htfStrictEnabled: bool = True
    htfMinStrength: float = Field(default=0.22, ge=0.0, le=1.0)
    requireVisionConsensus: bool = False
    cooldownSec: int = Field(default=120, ge=0, le=3600)
    maxTradesPerHour: int = Field(default=6, ge=1, le=60)
    allowFlip: bool = False
    strongFlipEnabled: bool = True
    strongFlipMinConfidence: float = Field(default=0.82, ge=0.5, le=1.0)
    strongFlipMinScoreGap: float = Field(default=1.5, ge=0.0, le=10.0)
    strongFlipUltraScoreGap: float = Field(default=2.2, ge=0.0, le=20.0)
    strongFlipUltraConfRelax: float = Field(default=0.08, ge=0.0, le=0.5)
    strongFlipStructureConfirmEnabled: bool = True
    strongFlipConfirmationsRequired: int = Field(default=2, ge=1, le=5)
    strongFlipVwapConfirmPct: float = Field(default=0.06, ge=0.0, le=5.0)
    strongFlipBbConfirmPctB: float = Field(default=0.42, ge=0.05, le=0.49)
    maxSpreadBps: float = Field(default=22.0, ge=0.0, le=200.0)
    maxSlippageBps: float = Field(default=28.0, ge=0.0, le=300.0)
    noTradeWindows: list[str] = Field(default_factory=list)
    trailingStopPct: float = Field(default=0.0, ge=0.0, le=10.0)
    executionMode: Literal["PAPER", "LIVE"] = "PAPER"
    aggressiveScalp: bool = False
    waitOverrideImbalance: float = Field(default=0.08, ge=0.0, le=1.0)
    earlyEntryEnabled: bool = True
    earlyEntryScoreGapMin: float = Field(default=1.4, ge=0.5, le=10.0)
    earlyEntryMinConfidence: float = Field(default=0.60, ge=0.3, le=1.0)
    earlyEntryPullbackResetEnabled: bool = True
    earlyEntryMaxBbPctB: float = Field(default=0.82, ge=0.5, le=1.0)
    earlyEntryMinBbPctBShort: float = Field(default=0.18, ge=0.0, le=0.5)
    earlyEntryMaxVwapDistancePct: float = Field(default=0.24, ge=0.05, le=5.0)
    lateEntryMaxBbPctB: float = Field(default=0.90, ge=0.5, le=1.0)
    lateEntryMaxVwapDistancePct: float = Field(default=0.32, ge=0.05, le=5.0)
    # lastFundingRate from premiumIndex; skip entry when rate works against position (0 = disabled).
    skipFundingAgainst: float = Field(default=0.0, ge=0.0, le=0.05)
    holdWinners: bool = True
    holdMinConfidence: float = Field(default=0.72, ge=0.0, le=1.0)
    holdTrailPct: float = Field(default=0.35, ge=0.05, le=5.0)
    aiTpSlFromLearning: bool = True
    marketScan: bool = True
    scanTopLiquid: int = Field(default=60, ge=5, le=120)
    scanAnalyzeTop: int = Field(default=12, ge=3, le=30)
    scanPerSymbolTimeoutSec: float = Field(default=7.5, ge=2.0, le=20.0)
    scanFallbackNearEnabled: bool = True
    scanFallbackNearConfRelax: float = Field(default=0.04, ge=0.0, le=0.15)
    scanSidePreference: Literal["score", "long", "short"] = "score"
    whitelistSymbols: list[str] = Field(default_factory=list)
    scanDenySymbols: list[str] = Field(
        default_factory=lambda: ["XAUUSDT", "XAGUSDT", "SPCXUSDT", "CLUSDT", "MRVLUSDT", "INTCUSDT", "HYPEUSDT", "LABUSDT", "XRPUSDT", "DOGEUSDT", "NEARUSDT", "AKEUSDT", "BANKUSDT"]
    )
    adaptiveSizing: bool = True
    adaptiveSizeBoostMaxPct: float = Field(default=18.0, ge=0.0, le=150.0)
    tradeNotionalCapUsdt: float = Field(default=80.0, ge=20.0, le=500.0)
    autoScanTradeNotionalCapUsdt: float = Field(default=80.0, ge=20.0, le=250.0)
    supervisorSizeStreakEnabled: bool = True
    supervisorSizeMultiplier: float = Field(default=1.0, ge=0.2, le=2.0)
    supervisorSizeWinStreakMin: int = Field(default=3, ge=2, le=20)
    supervisorSizeLossStreakMin: int = Field(default=2, ge=1, le=20)
    supervisorSizeWinStepPct: float = Field(default=10.0, ge=0.0, le=100.0)
    supervisorSizeLossStepPct: float = Field(default=15.0, ge=0.0, le=100.0)
    supervisorSizeMaxMultiplier: float = Field(default=1.35, ge=1.0, le=3.0)
    supervisorSizeMinMultiplier: float = Field(default=0.50, ge=0.1, le=1.0)
    supervisorSizeDiversifiedMinMultiplier: float = Field(default=0.65, ge=0.1, le=1.0)
    supervisorDailyBaselineMinTrades: int = Field(default=8, ge=4, le=200)
    supervisorDailyWrDropAlertPct: float = Field(default=18.0, ge=1.0, le=100.0)
    supervisorSmallProfitWinUsdt: float = Field(default=0.25, ge=0.01, le=10.0)
    sessionBiasEnabled: bool = True
    sessionBiasMinSamples: int = Field(default=10, ge=3, le=200)
    sessionBiasMaxConfShift: float = Field(default=0.05, ge=0.0, le=0.20)
    sessionBiasMaxSizeShiftPct: float = Field(default=25.0, ge=0.0, le=100.0)
    sessionBiasGoodWinRatePct: float = Field(default=50.0, ge=0.0, le=100.0)
    sessionBiasBadWinRatePct: float = Field(default=42.0, ge=0.0, le=100.0)
    sessionBiasLowVolMovePct: float = Field(default=0.45, ge=0.0, le=10.0)
    sessionBiasHighVolMovePct: float = Field(default=0.90, ge=0.0, le=20.0)
    todayPerformanceGuardEnabled: bool = True
    todayPerformanceGuardMinTrades: int = Field(default=8, ge=1, le=200)
    todayPerformanceGuardMaxWinRatePct: float = Field(default=40.0, ge=0.0, le=100.0)
    todayPerformanceGuardMaxPnlUsdt: float = Field(default=0.0, ge=-500.0, le=500.0)
    hybridScan: bool = False
    hybridMinScore: float = Field(default=0.72, ge=0.2, le=2.0)
    hybridMinEdge: float = Field(default=0.06, ge=0.0, le=1.0)
    maxOpenPositions: int = Field(default=4, ge=1, le=20)
    volTargetEnabled: bool = True
    volTargetPct: float = Field(default=0.22, ge=0.05, le=3.0)
    volLookback: int = Field(default=30, ge=10, le=240)
    volSizeMinMult: float = Field(default=0.6, ge=0.2, le=1.2)
    volSizeMaxMult: float = Field(default=1.4, ge=0.8, le=3.0)
    volumeConfirmEnabled: bool = True
    volumeConfirmMinRatio: float = Field(default=0.85, ge=0.05, le=10.0)
    volumeStrongRatio: float = Field(default=1.20, ge=0.1, le=20.0)
    volumeLowPenalty: float = Field(default=0.06, ge=0.0, le=0.4)
    volumeAlignedBoost: float = Field(default=0.05, ge=0.0, le=0.4)
    volumeBreakoutBoost: float = Field(default=0.04, ge=0.0, le=0.4)
    volumeRequireForLiteEntry: bool = True
    newsDailyEnabled: bool = True
    newsRefreshHours: int = Field(default=6, ge=1, le=24)
    newsMinHeadlines: int = Field(default=5, ge=1, le=50)
    newsMaxHeadlines: int = Field(default=25, ge=5, le=100)
    newsConfidenceMaxBoostPct: float = Field(default=8.0, ge=0.0, le=30.0)
    qualityLessonsLiveOnly: bool = True
    qualityLessonsUseRegime: bool = True
    qualityRecencyHalfLifeDays: float = Field(default=7.0, ge=1.0, le=60.0)
    qualityMaxSamples: int = Field(default=60, ge=10, le=500)
    qualityMinSamples: int = Field(default=8, ge=3, le=100)
    benchmarkFilterEnabled: bool = True
    benchmarkSymbol: str = "BTCUSDT"
    benchmarkRefreshSec: int = Field(default=45, ge=10, le=600)
    benchmarkConflictPenaltyPct: float = Field(default=6.0, ge=0.0, le=30.0)
    pairLockEnabled: bool = True
    pairLockLossStreak: int = Field(default=2, ge=1, le=10)
    pairLockMinutes: int = Field(default=45, ge=1, le=1440)
    liveBadUtcHours: list[int] = Field(default_factory=list)
    maxDailyTradesPerSymbol: int = Field(default=14, ge=1, le=200)
    perfGateMinSamples: int = Field(default=8, ge=3, le=200)
    perfGateMinWinRatePct: float = Field(default=40.0, ge=0.0, le=100.0)
    perfGateMinPnlUsdt: float = Field(default=-0.5, ge=-500.0, le=500.0)
    perfGateEarlyMinSamples: int = Field(default=4, ge=3, le=50)
    perfGateEarlyMinWinRatePct: float = Field(default=35.0, ge=0.0, le=100.0)
    perfGateEarlyMinPnlUsdt: float = Field(default=-0.35, ge=-500.0, le=500.0)
    perfGateMinRewardScore: float = Field(default=-1.25, ge=-100.0, le=100.0)
    perfLockMinutes: int = Field(default=90, ge=1, le=1440)
    memoryPrimaryDays: int = Field(default=7, ge=1, le=30)
    memoryConfirmDays: int = Field(default=15, ge=2, le=60)
    memoryBaselineDays: int = Field(default=30, ge=7, le=120)
    memoryArchiveWeightPct: float = Field(default=0.0, ge=0.0, le=5.0)
    riskCooldownEnabled: bool = True
    riskCooldownLookback: int = Field(default=8, ge=2, le=100)
    riskCooldownLossStreak: int = Field(default=2, ge=2, le=20)
    riskCooldownMinutes: int = Field(default=60, ge=1, le=1440)
    riskCooldownAdaptiveMarket: bool = True
    riskCooldownAdaptiveCheckSec: int = Field(default=30, ge=10, le=600)
    riskCooldownLightScanEnabled: bool = True
    riskCooldownLightScanSec: int = Field(default=45, ge=20, le=600)
    riskCooldownLightScanAnalyzeTop: int = Field(default=6, ge=3, le=12)
    riskCooldownPauseOnVolatile: bool = True
    riskCooldownVolatileMinutes: int = Field(default=10, ge=1, le=240)
    riskCooldownResumeScoreGapMin: float = Field(default=2.0, ge=0.0, le=10.0)
    selfReviewMinHourSamples: int = Field(default=8, ge=3, le=200)
    usdtTooSmallAction: Literal["multiply", "skip"] = "multiply"
    # Adaptive multiplier range when USDT is too small (5x-10x).
    usdtTooSmallMultiplierMin: float = Field(default=5.0, ge=1.0, le=100.0)
    usdtTooSmallMultiplierMax: float = Field(default=10.0, ge=1.0, le=200.0)
    feeMinNetProfitUSDT: float = Field(default=0.15, ge=0.0, le=50.0)
    feeMinEdgeVsCostMultiple: float = Field(default=3.0, ge=1.0, le=5.0)
    feeMinOrderUsdt: float = Field(default=5.0, ge=5.0, le=200.0)
    feeAdaptiveNetEnabled: bool = True
    feeAdaptiveVolLowPct: float = Field(default=0.08, ge=0.01, le=3.0)
    feeAdaptiveVolHighPct: float = Field(default=0.35, ge=0.02, le=5.0)
    feeAdaptiveMinFactor: float = Field(default=0.8, ge=0.6, le=1.0)
    feeAdaptiveMaxFactor: float = Field(default=1.15, ge=1.0, le=1.6)
    tpSlTargetUsdtEnabled: bool = True
    tpTargetMinUsdt: float = Field(default=0.5, ge=0.05, le=20.0)
    tpTargetMaxUsdt: float = Field(default=2.0, ge=0.1, le=50.0)
    profitLockTriggerUsdt: float = Field(default=0.35, ge=0.01, le=50.0)
    profitLockKeepUsdt: float = Field(default=0.15, ge=0.0, le=50.0)
    profitLockMaxGivebackUsdt: float = Field(default=0.22, ge=0.01, le=50.0)
    slToTpRatio: float = Field(default=0.55, ge=0.35, le=0.85)
    minRiskRewardRatio: float = Field(default=1.35, ge=1.0, le=4.0)
    atrTpSlEnabled: bool = True
    ema200StrictEnabled: bool = True
    learningPnlClipEnabled: bool = True
    learningPnlClipAbsUsdt: float = Field(default=25.0, ge=0.5, le=5000.0)
    learningBehaviorRewardEnabled: bool = True
    learningRewardTpHitBase: float = Field(default=0.2, ge=0.0, le=5.0)
    learningRewardTpScalePerUsdt: float = Field(default=0.35, ge=0.0, le=5.0)
    learningRewardEntryTiming: float = Field(default=0.25, ge=0.0, le=5.0)
    learningRewardHoldWinner: float = Field(default=0.2, ge=0.0, le=5.0)
    learningRewardFlipGood: float = Field(default=0.1, ge=0.0, le=3.0)
    learningPenaltyEarlySl: float = Field(default=0.35, ge=0.0, le=5.0)
    learningPenaltyMemoryMiss: float = Field(default=0.15, ge=0.0, le=3.0)
    learningBehaviorDeltaCap: float = Field(default=2.5, ge=0.1, le=20.0)
    learningFastWinMinutes: int = Field(default=45, ge=1, le=1440)
    orphanAutoAdoptEnabled: bool = True
    orphanAutoAdoptForceSingleSymbol: bool = False
    orphanAutoAdoptMultiEnabled: bool = True
    learningRewardEnabled: bool = True
    learningRewardWin: float = Field(default=1.0, ge=0.0, le=20.0)
    learningPenaltyLoss: float = Field(default=0.8, ge=0.0, le=20.0)
    learningRewardDecay: float = Field(default=0.985, ge=0.9, le=1.0)
    learningRewardCap: float = Field(default=50.0, ge=1.0, le=500.0)
    tradingviewEnabled: bool = True
    tradingviewCacheTtl: int = Field(default=60, ge=10, le=600)
    tradingviewRateLimit: int = Field(default=30, ge=5, le=120)
    tradingviewTimeout: float = Field(default=5.0, ge=1.0, le=30.0)
    tradingviewConfidenceBoost: float = Field(default=0.08, ge=0.0, le=0.3)
    tradingviewStalenessThreshold: int = Field(default=300, ge=60, le=1800)
    tradingviewMaxFailures: int = Field(default=5, ge=1, le=20)
    supervisorTradingViewHealthCooldownMinutes: int = Field(default=15, ge=5, le=120)


class AutoTradeControlRequest(BaseModel):
    sessionId: str | None = None
    force: bool = False
