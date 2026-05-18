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
    buyWall: OrderBookLevel | None = None
    sellWall: OrderBookLevel | None = None
    spoofingRisk: Literal["LOW", "MEDIUM", "HIGH"]

class AnalyzeRequest(BaseModel):
    symbol: str
    orderBook: OrderBookSummary

class TradeRequest(BaseModel):
    symbol: str
    side: Literal["LONG", "SHORT", "WAIT", "CLOSE"]
    quantity: float | None = Field(default=None, gt=0)
    usdtAmount: float | None = Field(default=None, gt=0)
    leverage: int | None = None
    marginType: Literal["ISOLATED", "CROSSED"] | None = None
    takeProfitPct: float | None = None
    stopLossPct: float | None = None

class VisionAnalyzeRequest(BaseModel):
    symbol: str
    imageDataUrl: str

class IntelAnalyzeRequest(BaseModel):
    symbol: str

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
    maxLeverage: float
    maxDailyLossUSDT: float

class AutoTradeStartRequest(BaseModel):
    symbol: str
    usdtAmount: float = Field(gt=0)
    leverage: int = Field(default=5, ge=1, le=125)
    marginType: Literal["ISOLATED", "CROSSED"] = "ISOLATED"
    takeProfitPct: float = Field(default=1.8, gt=0)
    stopLossPct: float = Field(default=0.8, gt=0)
    intervalSec: int = Field(default=20, ge=5, le=120)
    minConfidence: float = Field(default=0.65, ge=0.0, le=1.0)
    requireVisionConsensus: bool = False
    cooldownSec: int = Field(default=120, ge=0, le=3600)
    maxTradesPerHour: int = Field(default=6, ge=1, le=60)
    allowFlip: bool = False
    maxSpreadBps: float = Field(default=22.0, ge=0.0, le=200.0)
    maxSlippageBps: float = Field(default=28.0, ge=0.0, le=300.0)
    noTradeWindows: list[str] = Field(default_factory=list)
    trailingStopPct: float = Field(default=0.0, ge=0.0, le=10.0)
    executionMode: Literal["PAPER", "LIVE"] = "PAPER"
    aggressiveScalp: bool = False
    waitOverrideImbalance: float = Field(default=0.08, ge=0.0, le=1.0)
    # lastFundingRate from premiumIndex; skip entry when rate works against position (0 = disabled).
    skipFundingAgainst: float = Field(default=0.0, ge=0.0, le=0.05)
    holdWinners: bool = True
    holdMinConfidence: float = Field(default=0.72, ge=0.0, le=1.0)
    holdTrailPct: float = Field(default=0.35, ge=0.05, le=5.0)
    marketScan: bool = False
    scanTopLiquid: int = Field(default=30, ge=5, le=120)
    scanAnalyzeTop: int = Field(default=8, ge=3, le=30)
    whitelistSymbols: list[str] = Field(default_factory=list)
    adaptiveSizing: bool = True
    adaptiveSizeBoostMaxPct: float = Field(default=35.0, ge=0.0, le=150.0)
    hybridScan: bool = False
    hybridMinScore: float = Field(default=0.72, ge=0.2, le=2.0)
    hybridMinEdge: float = Field(default=0.06, ge=0.0, le=1.0)
    maxOpenPositions: int = Field(default=6, ge=1, le=20)

class AutoTradeControlRequest(BaseModel):
    sessionId: str | None = None
    force: bool = False
