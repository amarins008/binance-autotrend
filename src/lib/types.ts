export type Side = "LONG" | "SHORT" | "WAIT" | "CLOSE"

export interface BackendStatus {
  online: boolean
  latencyMs: number | null
  checkedAt: number
}

export interface OrderBookLevel { price: number; qty: number }

export interface OrderBookSummary {
  symbol: string
  bidNotional: number
  askNotional: number
  imbalance: number
  buyWall: OrderBookLevel | null
  sellWall: OrderBookLevel | null
  spoofingRisk: "LOW" | "MEDIUM" | "HIGH"
  cvdProxy?: number        // positive = buy pressure
  icebergDetected?: boolean
}

export interface AiInsight {
  trend: "Bullish" | "Bearish" | "Neutral"
  rsi: number
  volumeSignal: "Weak" | "Normal" | "Strong"
  setup: string
  recommendation: Side
  warning?: string
}

export interface VisionInsight {
  pattern: string
  confidence: number
  notes: string[]
  recommendation: Side
}

export interface StrategyPlan {
  command: string
  symbol: string
  triggerType: "BREAKOUT_ABOVE" | "BREAKDOWN_BELOW" | "MARKET_NOW"
  triggerPrice?: number
  side: "LONG" | "SHORT"
  quantity: number
  takeProfitPct: number
  stopLossPct: number
  trailingStopPct: number
  active: boolean
}

export interface MonitorState {
  id: string
  status: string
}

export interface AutoTradeConfig {
  symbol: string
  usdtAmount: number
  leverage: number
  marginType: "ISOLATED" | "CROSSED"
  takeProfitPct: number
  stopLossPct: number
  intervalSec: number
  minConfidence: number
  cooldownSec: number
  maxTradesPerHour: number
  allowFlip: boolean
  trailingStopPct: number
  executionMode: "PAPER" | "LIVE"
  aggressiveScalp: boolean
  maxSpreadBps: number
}

export interface AutoTradeStatus {
  running: boolean
  sessionId: string | null
  startedAt: number
  config: AutoTradeConfig | null
  paper: {
    position: {
      side: "LONG" | "SHORT"
      entry: number
      qty: number
      tp: number
      sl: number
      symbol: string
    } | null
    wins: number
    losses: number
    winRatePct: number
    realizedPnl: number
    lastTrades: Array<{
      side: string
      entry: number
      exit: number
      pnl: number
      reason: string
      closedAt?: number
    }>
  }
  lastDecision: {
    signal: string
    confidence: number
    setup: string
    notes?: string[]
    execution?: {
      bid?: number
      ask?: number
      mark?: number
      spreadBps?: number
      lastFundingRate?: number
    }
  } | null
  lastSkip: { ts: number; code: string; msg: string } | null
  consecutiveErrors: number
  lastTradeAt: number
  tradesLastHour: number
  log: Array<{ ts: number; msg: string }>
  liveGuardian: {
    symbol: string
    side: string
    tp: number
    sl: number
    active: boolean
  } | null
  activePosition: {
    mode: string
    paper: { side: string; qty: number; notionalUsdtApprox: number }
    live: { side: string; qty: number; notionalUsdtApprox: number }
  }
  continuity?: {
    snapshotFile?: string
    snapshotSavedAt?: number
    recoveredLog?: string
    hints?: string[]
    orphanLive?: { symbol: string; side: string; qty: number; notionalUsdtApprox: number } | null
  }
}

export interface PrecisionSignal {
  rsi14: number
  rsi14_5m: number
  stochK: number
  stochD: number
  macdLine: number
  macdSignal: number
  macdHist: number
  macdBullish: boolean
  macdCrossUp: boolean
  macdCrossDn: boolean
  bbPctB: number
  bbBandwidth: number
  bbSqueeze: boolean
  vwap: number
  priceAboveVwap: boolean
  vwapDistancePct: number
  atrPct: number
  atrTpMult: number
  atrSlMult: number
  cvd: number
  trendUp: boolean
  trendDown: boolean
  trendUpPartial: boolean
  trendDnPartial: boolean
  breakoutUp: boolean
  breakoutDown: boolean
  nearResistance: boolean
  nearSupport: boolean
  session: string
  highLiquiditySession: boolean
  longScore: number
  shortScore: number
}

export interface IntelResult {
  symbol: string
  signal: "LONG" | "SHORT" | "WAIT"
  confidence: number
  setup: string
  notes: string[]
  momentum: {
    momentumPct: number
    volumeRatio: number
    divergence: string
  }
  precision: PrecisionSignal
  execution?: {
    bid?: number
    ask?: number
    mark?: number
    spreadBps?: number
    lastFundingRate?: number
  }
}
