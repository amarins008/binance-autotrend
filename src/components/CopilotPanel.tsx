// CopilotPanel.tsx — 4-tab professional UI
import React, { memo, useCallback, useEffect, useRef, useState } from "react"
import type {
  AiInsight, AutoTradeConfig, AutoTradeStatus, BackendStatus,
  IntelResult, MonitorState, OrderBookSummary, Side, StrategyPlan, VisionInsight,
} from "~lib/types"

// ── Types ─────────────────────────────────────────────────────────────────────
type Tab = "data" | "trade" | "auto" | "settings"

interface Props {
  symbol: string
  loading: boolean
  backendStatus: BackendStatus | null
  insight: AiInsight | null
  intel: IntelResult | null
  orderBook: OrderBookSummary | null
  vision: VisionInsight | null
  strategyPlan: StrategyPlan | null
  monitor: MonitorState | null
  autoTradeStatus: AutoTradeStatus | null
  alerts: string[]
  autoRefreshSec: number
  onRefresh: () => void
  onAnalyzeVision: () => void
  onParseCommand: (cmd: string) => void
  onEvaluatePlan: () => void
  onStartMonitor: () => void
  onStopMonitor: () => void
  onAction: (side: Side, qty: number) => void
  onSetAutoRefresh: (sec: number) => void
  onStartAutoTrade: (cfg: AutoTradeConfig) => void
  onStopAutoTrade: () => void
  onResetAutoTrade: () => void
  onStartBackend: () => Promise<void>
}

// ── Palette ───────────────────────────────────────────────────────────────────
const C = {
  bg: "#080e1a",
  surface: "#0d1526",
  card: "#111d30",
  border: "#1e2d45",
  borderHover: "#2a3f5f",
  green: "#22c55e",
  greenDim: "#14532d",
  red: "#ef4444",
  redDim: "#7f1d1d",
  yellow: "#f59e0b",
  yellowDim: "#78350f",
  blue: "#3b82f6",
  blueDim: "#1e3a8a",
  text: "#e2e8f0",
  muted: "#64748b",
  subtle: "#94a3b8",
}

// ── Helpers ───────────────────────────────────────────────────────────────────
const sigColor = (s?: string | null) =>
  s === "LONG" ? C.green : s === "SHORT" ? C.red : C.yellow

const trendColor = (t?: string) =>
  t === "Bullish" ? C.green : t === "Bearish" ? C.red : C.yellow

const fmt2 = (n?: number | null) => (n == null ? "—" : n.toFixed(2))
const fmt4 = (n?: number | null) => (n == null ? "—" : n.toFixed(4))
const fmtPct = (n?: number | null) => (n == null ? "—" : `${(n * 100).toFixed(4)}%`)

// ── Micro components ──────────────────────────────────────────────────────────
const Pill = ({ label, color }: { label: string; color: string }) => (
  <span style={{
    display: "inline-block", padding: "1px 8px", borderRadius: 20,
    fontSize: 11, fontWeight: 700, letterSpacing: 0.5,
    background: color + "22", border: `1px solid ${color}`, color,
  }}>{label}</span>
)

const KV = ({ k, v, vc }: { k: string; v: React.ReactNode; vc?: string }) => (
  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "3px 0", borderBottom: `1px solid ${C.border}` }}>
    <span style={{ color: C.muted, fontSize: 11 }}>{k}</span>
    <span style={{ color: vc ?? C.text, fontSize: 12, fontWeight: 600 }}>{v}</span>
  </div>
)

const Bar = ({ value, color, label }: { value: number; color: string; label?: string }) => (
  <div style={{ marginTop: 4 }}>
    {label && (
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, marginBottom: 2 }}>
        <span style={{ color: C.muted }}>{label}</span>
        <span style={{ color, fontWeight: 700 }}>{Math.round(value * 100)}%</span>
      </div>
    )}
    <div style={{ background: C.border, borderRadius: 3, height: 5, overflow: "hidden" }}>
      <div style={{ width: `${Math.min(value * 100, 100)}%`, height: "100%", background: color, borderRadius: 3, transition: "width .4s" }} />
    </div>
  </div>
)

const ScoreBar = ({ long, short }: { long: number; short: number }) => {
  const total = Math.max(long + short, 1)
  return (
    <div style={{ marginTop: 6 }}>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, marginBottom: 2 }}>
        <span style={{ color: C.green }}>L {long}</span>
        <span style={{ color: C.muted, fontSize: 9 }}>CONFLUENCE</span>
        <span style={{ color: C.red }}>S {short}</span>
      </div>
      <div style={{ background: C.redDim, borderRadius: 3, height: 5, overflow: "hidden" }}>
        <div style={{ width: `${(long / total) * 100}%`, height: "100%", background: C.greenDim, borderRadius: 3, transition: "width .4s" }} />
      </div>
    </div>
  )
}

const Card = ({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) => (
  <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: "10px 12px", marginBottom: 8, ...style }}>
    {children}
  </div>
)

const CardTitle = ({ children }: { children: React.ReactNode }) => (
  <div style={{ fontSize: 11, fontWeight: 700, color: C.subtle, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>
    {children}
  </div>
)

const Btn = ({ children, onClick, color, disabled, style }: {
  children: React.ReactNode; onClick?: () => void
  color?: "green" | "red" | "yellow" | "blue" | "ghost"
  disabled?: boolean; style?: React.CSSProperties
}) => {
  const map = {
    green: { bg: C.greenDim, border: C.green },
    red: { bg: C.redDim, border: C.red },
    yellow: { bg: C.yellowDim, border: C.yellow },
    blue: { bg: C.blueDim, border: C.blue },
    ghost: { bg: C.surface, border: C.border },
  }
  const t = map[color ?? "ghost"]
  return (
    <button onClick={onClick} disabled={disabled} style={{
      padding: "7px 12px", borderRadius: 8, border: `1px solid ${t.border}`,
      background: t.bg, color: C.text, fontWeight: 700, fontSize: 12,
      cursor: disabled ? "not-allowed" : "pointer", opacity: disabled ? 0.5 : 1,
      transition: "opacity .2s", ...style,
    }}>{children}</button>
  )
}

//  Toast notification 
interface ToastMsg { id: number; text: string; type: "ok" | "err" | "info" }
let _toastId = 0
type ToastFn = (text: string, type?: ToastMsg["type"]) => void
const ToastContext = React.createContext<ToastFn>(() => {})

const ToastProvider = ({ children }: { children: React.ReactNode }) => {
  const [toasts, setToasts] = useState<ToastMsg[]>([])
  const push: ToastFn = useCallback((text, type = "info") => {
    const id = ++_toastId
    setToasts(p => [...p.slice(-3), { id, text, type }])
    setTimeout(() => setToasts(p => p.filter(t => t.id !== id)), 2200)
  }, [])
  const bgMap = { ok: "#14532d", err: "#7f1d1d", info: "#1e3a8a" }
  const borderMap = { ok: C.green, err: C.red, info: C.blue }
  return (
    <ToastContext.Provider value={push}>
      {children}
      <div style={{ position: "fixed", bottom: 16, left: "50%", transform: "translateX(-50%)", display: "flex", flexDirection: "column", gap: 6, zIndex: 9999, pointerEvents: "none", width: 260 }}>
        {toasts.map(t => (
          <div key={t.id} style={{ background: bgMap[t.type], border: `1px solid ${borderMap[t.type]}`, borderRadius: 8, padding: "7px 12px", fontSize: 12, color: C.text, fontWeight: 600, textAlign: "center", animation: "toastIn .18s ease" }}>
            {t.type === "ok" ? " " : t.type === "err" ? " " : "ℹ "}{t.text}
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  )
}

//  BtnFx  button with press + busy + toast feedback 
const BtnFx = ({ children, onClick, color, disabled, style, toast }: {
  children: React.ReactNode
  onClick?: () => void | Promise<void>
  color?: "green" | "red" | "yellow" | "blue" | "ghost"
  disabled?: boolean; style?: React.CSSProperties; toast?: string
}) => {
  const [pressed, setPressed] = useState(false)
  const [busy, setBusy] = useState(false)
  const pushToast = React.useContext(ToastContext)
  const map = {
    green:  { bg: C.greenDim,  border: C.green,  active: "#166534" },
    red:    { bg: C.redDim,    border: C.red,    active: "#991b1b" },
    yellow: { bg: C.yellowDim, border: C.yellow, active: "#92400e" },
    blue:   { bg: C.blueDim,   border: C.blue,   active: "#1d4ed8" },
    ghost:  { bg: C.surface,   border: C.border, active: "#374151" },
  }
  const t = map[color ?? "ghost"]
  const handleClick = async () => {
    if (disabled || busy || !onClick) return
    setPressed(true); setTimeout(() => setPressed(false), 180)
    setBusy(true)
    try { await onClick(); if (toast) pushToast(toast, "ok") }
    catch (e: any) { pushToast(e?.message?.slice(0, 60) ?? "Error", "err") }
    finally { setBusy(false) }
  }
  return (
    <button onClick={handleClick} disabled={disabled || busy} style={{
      padding: "7px 12px", borderRadius: 8,
      border: `1px solid ${pressed ? C.text : t.border}`,
      background: pressed ? t.active : t.bg,
      color: C.text, fontWeight: 700, fontSize: 12,
      cursor: (disabled || busy) ? "not-allowed" : "pointer",
      opacity: (disabled || busy) ? 0.65 : 1,
      transform: pressed ? "scale(0.95)" : "scale(1)",
      transition: "transform .1s, background .12s, border-color .12s",
      outline: "none", boxShadow: pressed ? `0 0 0 2px ${t.border}44` : "none",
      ...style,
    }}>
      {busy ? "" : children}
    </button>
  )
}

const Input = ({ label, value, onChange, type = "text", step, min, max, style }: {
  label: string; value: string | number
  onChange: (v: string) => void
  type?: string; step?: number; min?: number; max?: number
  style?: React.CSSProperties
}) => (
  <div style={{ display: "flex", flexDirection: "column", gap: 2, ...style }}>
    <label style={{ fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: 0.5 }}>{label}</label>
    <input
      type={type} value={value} step={step} min={min} max={max}
      onChange={e => onChange(e.target.value)}
      style={{
        padding: "5px 8px", borderRadius: 7, border: `1px solid ${C.border}`,
        background: C.surface, color: C.text, fontSize: 12, width: "100%", boxSizing: "border-box",
      }}
    />
  </div>
)

const Select = ({ label, value, onChange, options }: {
  label: string; value: string
  onChange: (v: string) => void
  options: { value: string; label: string }[]
}) => (
  <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
    <label style={{ fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: 0.5 }}>{label}</label>
    <select value={value} onChange={e => onChange(e.target.value)} style={{
      padding: "5px 8px", borderRadius: 7, border: `1px solid ${C.border}`,
      background: C.surface, color: C.text, fontSize: 12,
    }}>
      {options.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
    </select>
  </div>
)

// ── Backend status badge ──────────────────────────────────────────────────────
const StatusBadge = ({ status }: { status: BackendStatus | null }) => {
  if (!status) return (
    <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 10, color: C.muted }}>
      <span style={{ width: 6, height: 6, borderRadius: "50%", background: C.muted, display: "inline-block" }} />
      Connecting…
    </div>
  )
  return status.online ? (
    <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 10, color: C.green }}>
      <span style={{ width: 6, height: 6, borderRadius: "50%", background: C.green, display: "inline-block", animation: "pulse 2s infinite" }} />
      Online {status.latencyMs != null ? `${status.latencyMs}ms` : ""}
    </div>
  ) : (
    <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 10, color: C.red }}>
      <span style={{ width: 6, height: 6, borderRadius: "50%", background: C.red, display: "inline-block" }} />
      Backend offline
    </div>
  )
}

// ── Tab: Data ─────────────────────────────────────────────────────────────────
const TabData = memo(({ intel, orderBook, vision, loading, onRefresh, onAnalyzeVision, autoRefreshSec, onSetAutoRefresh }: {
  intel: IntelResult | null; orderBook: OrderBookSummary | null
  vision: VisionInsight | null; loading: boolean
  onRefresh: () => void; onAnalyzeVision: () => void
  autoRefreshSec: number; onSetAutoRefresh: (s: number) => void
}) => {
  const p = intel?.precision
  const sig = intel?.signal
  const conf = intel?.confidence ?? 0
  const confColor = conf >= 0.75 ? C.green : conf >= 0.6 ? C.yellow : C.red

  return (
    <div>
      {/* Signal hero */}
      <Card style={{ textAlign: "center", padding: "14px 12px" }}>
        <div style={{ fontSize: 11, color: C.muted, marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>Signal</div>
        <div style={{ fontSize: 36, fontWeight: 900, color: sigColor(sig), letterSpacing: 2, lineHeight: 1 }}>
          {sig ?? "—"}
        </div>
        {intel && (
          <>
            <Bar value={conf} color={confColor} label="Confidence" />
            {p && <ScoreBar long={p.longScore} short={p.shortScore} />}
            <div style={{ fontSize: 11, color: C.subtle, marginTop: 8, lineHeight: 1.5 }}>{intel.setup}</div>
            <div style={{ marginTop: 6, display: "flex", flexWrap: "wrap", gap: 4, justifyContent: "center" }}>
              {intel.notes.slice(0, 4).map((n, i) => (
                <span key={i} style={{ fontSize: 10, color: C.muted, background: C.surface, borderRadius: 4, padding: "1px 6px" }}>
                  {n}
                </span>
              ))}
            </div>
          </>
        )}
      </Card>

      {/* Refresh controls */}
      <div style={{ display: "flex", gap: 6, marginBottom: 8 }}>
        <BtnFx onClick={onRefresh} disabled={loading} color="ghost" style={{ flex: 1 }} toast="Analysis refreshed">
          {loading ? "⏳ Analyzing…" : "🔄 Refresh"}
        </BtnFx>
        <select value={autoRefreshSec} onChange={e => onSetAutoRefresh(Number(e.target.value))}
          style={{ padding: "6px 8px", borderRadius: 7, border: `1px solid ${C.border}`, background: C.surface, color: C.text, fontSize: 11 }}>
          <option value={0}>Manual</option>
          <option value={15}>15s</option>
          <option value={30}>30s</option>
          <option value={60}>60s</option>
        </select>
      </div>

      {/* Precision indicators */}
      {p && (
        <Card>
          <CardTitle>Indicators</CardTitle>
          <KV k="RSI 14 (1m)" v={p.rsi14.toFixed(1)} vc={p.rsi14 > 70 ? C.red : p.rsi14 < 30 ? C.green : C.text} />
          <KV k="RSI 14 (5m)" v={p.rsi14_5m.toFixed(1)} vc={p.rsi14_5m > 70 ? C.red : p.rsi14_5m < 30 ? C.green : C.text} />
          <KV k="StochRSI K/D" v={`${p.stochK.toFixed(0)} / ${p.stochD.toFixed(0)}`} />
          <KV k="MACD" v={
            <span style={{ color: p.macdBullish ? C.green : C.red }}>
              {p.macdHist > 0 ? "▲" : "▼"} {p.macdHist.toFixed(5)}
              {p.macdCrossUp ? " ✦↑" : p.macdCrossDn ? " ✦↓" : ""}
            </span>
          } />
          <KV k="BB %B" v={p.bbPctB.toFixed(3)} vc={p.bbPctB > 1 ? C.red : p.bbPctB < 0 ? C.green : C.text} />
          <KV k="BB Squeeze" v={p.bbSqueeze ? "🔴 YES" : "NO"} vc={p.bbSqueeze ? C.yellow : C.muted} />
          <KV k="VWAP" v={p.priceAboveVwap ? "↑ Above" : "↓ Below"} vc={p.priceAboveVwap ? C.green : C.red} />
          <KV k="VWAP Dist" v={`${p.vwapDistancePct.toFixed(3)}%`} />
          <KV k="ATR %" v={`${p.atrPct.toFixed(3)}%`} />
          <KV k="CVD" v={p.cvd.toFixed(4)} vc={p.cvd > 0 ? C.green : C.red} />
          <KV k="Trend" v={p.trendUp ? "↑ Up" : p.trendDown ? "↓ Down" : "Sideways"} vc={p.trendUp ? C.green : p.trendDown ? C.red : C.yellow} />
          <KV k="Session" v={`${p.session}${p.highLiquiditySession ? " 💧" : " 🔇"}`} vc={p.highLiquiditySession ? C.green : C.muted} />
        </Card>
      )}

      {/* Execution / microstructure */}
      {intel?.execution && (
        <Card>
          <CardTitle>Microstructure</CardTitle>
          <KV k="Bid" v={fmt2(intel.execution.bid)} />
          <KV k="Ask" v={fmt2(intel.execution.ask)} />
          <KV k="Mark" v={fmt2(intel.execution.mark)} />
          <KV k="Spread" v={intel.execution.spreadBps != null ? `${intel.execution.spreadBps.toFixed(2)} bps` : "—"}
            vc={intel.execution.spreadBps != null && intel.execution.spreadBps > 10 ? C.yellow : C.green} />
          <KV k="Funding Rate" v={
            intel.execution.lastFundingRate != null
              ? <span style={{ color: intel.execution.lastFundingRate > 0 ? C.red : C.green }}>
                  {fmtPct(intel.execution.lastFundingRate)}
                </span>
              : "—"
          } />
        </Card>
      )}

      {/* Order book */}
      {orderBook && (
        <Card>
          <CardTitle>Order Book</CardTitle>
          <KV k="Imbalance" v={`${(orderBook.imbalance * 100).toFixed(2)}%`}
            vc={orderBook.imbalance > 0.04 ? C.green : orderBook.imbalance < -0.04 ? C.red : C.yellow} />
          <KV k="Buy Wall" v={orderBook.buyWall ? `${orderBook.buyWall.price} (${orderBook.buyWall.qty.toFixed(2)})` : "—"} />
          <KV k="Sell Wall" v={orderBook.sellWall ? `${orderBook.sellWall.price} (${orderBook.sellWall.qty.toFixed(2)})` : "—"} />
          <KV k="Spoofing" v={orderBook.spoofingRisk}
            vc={orderBook.spoofingRisk === "HIGH" ? C.red : orderBook.spoofingRisk === "MEDIUM" ? C.yellow : C.green} />
          {orderBook.icebergDetected && (
            <div style={{ color: C.yellow, fontSize: 11, marginTop: 4 }}>⚠ Iceberg order detected</div>
          )}
          {orderBook.cvdProxy != null && (
            <KV k="CVD Proxy" v={orderBook.cvdProxy.toFixed(4)} vc={orderBook.cvdProxy > 0 ? C.green : C.red} />
          )}
        </Card>
      )}

      {/* Vision */}
      <Card>
        <CardTitle>Chart Vision</CardTitle>
        <BtnFx onClick={onAnalyzeVision} color="ghost" style={{ width: "100%", marginBottom: 8 }} toast="Vision analysis done">
          📸 Analyze Chart Snapshot
        </BtnFx>
        {vision ? (
          <>
            <KV k="Pattern" v={vision.pattern} />
            <Bar value={vision.confidence} color={vision.confidence >= 0.7 ? C.green : C.yellow} label="Confidence" />
            <KV k="Reco" v={<Pill label={vision.recommendation} color={sigColor(vision.recommendation)} />} />
            {vision.notes.slice(0, 3).map((n, i) => (
              <div key={i} style={{ fontSize: 10, color: C.muted, marginTop: 3 }}>• {n}</div>
            ))}
          </>
        ) : (
          <div style={{ color: C.muted, fontSize: 11 }}>No vision data — click button above</div>
        )}
      </Card>
    </div>
  )
})

// ── Tab: Manual Trade ─────────────────────────────────────────────────────────
const TabTrade = memo(({ symbol, strategyPlan, monitor, alerts, onParseCommand, onEvaluatePlan, onStartMonitor, onStopMonitor, onAction }: {
  symbol: string; strategyPlan: StrategyPlan | null; monitor: MonitorState | null
  alerts: string[]
  onParseCommand: (cmd: string) => void; onEvaluatePlan: () => void
  onStartMonitor: () => void; onStopMonitor: () => void; onAction: (side: Side, qty: number) => void
}) => {
  const [qty, setQty] = useState(0.01)
  const [usdtAmt, setUsdtAmt] = useState(20)
  const [useUsdt, setUseUsdt] = useState(true)
  const [command, setCommand] = useState(`open long ${symbol} market qty 0.01`)

  return (
    <div>
      {/* One-click trade */}
      <Card>
        <CardTitle>One-Click Trade</CardTitle>
        <div style={{ display: "flex", gap: 8, marginBottom: 10, alignItems: "center" }}>
          <button
            onClick={() => setUseUsdt(true)}
            style={{ fontSize: 11, padding: "3px 10px", borderRadius: 6, border: `1px solid ${useUsdt ? C.blue : C.border}`, background: useUsdt ? C.blueDim : C.surface, color: C.text, cursor: "pointer" }}>
            USDT
          </button>
          <button
            onClick={() => setUseUsdt(false)}
            style={{ fontSize: 11, padding: "3px 10px", borderRadius: 6, border: `1px solid ${!useUsdt ? C.blue : C.border}`, background: !useUsdt ? C.blueDim : C.surface, color: C.text, cursor: "pointer" }}>
            Qty
          </button>
          {useUsdt ? (
            <input type="number" min={1} step={1} value={usdtAmt} onChange={e => setUsdtAmt(Number(e.target.value))}
              style={{ flex: 1, padding: "5px 8px", borderRadius: 7, border: `1px solid ${C.border}`, background: C.surface, color: C.text, fontSize: 12 }} />
          ) : (
            <input type="number" min={0.001} step={0.001} value={qty} onChange={e => setQty(Number(e.target.value))}
              style={{ flex: 1, padding: "5px 8px", borderRadius: 7, border: `1px solid ${C.border}`, background: C.surface, color: C.text, fontSize: 12 }} />
          )}
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
          <BtnFx color="green" onClick={() => onAction("LONG", useUsdt ? usdtAmt : qty)} style={{ width: "100%" }} toast="Long order sent">▲ Long</BtnFx>
          <BtnFx color="red" onClick={() => onAction("SHORT", useUsdt ? usdtAmt : qty)} style={{ width: "100%" }} toast="Short order sent">▼ Short</BtnFx>
          <BtnFx color="yellow" onClick={() => onAction("WAIT", qty)} style={{ width: "100%" }} toast="Waiting…">⏸ Wait</BtnFx>
          <BtnFx color="blue" onClick={() => onAction("CLOSE", qty)} style={{ width: "100%" }} toast="Close order sent">✕ Close</BtnFx>
        </div>
      </Card>

      {/* Jarvis command */}
      <Card>
        <CardTitle>Jarvis Command</CardTitle>
        <textarea value={command} onChange={e => setCommand(e.target.value)} rows={2}
          style={{ width: "100%", padding: "6px 8px", borderRadius: 7, border: `1px solid ${C.border}`, background: C.surface, color: C.text, fontSize: 12, resize: "vertical", boxSizing: "border-box", marginBottom: 6 }} />
        <BtnFx onClick={() => onParseCommand(command)} color="ghost" style={{ width: "100%" }} toast="Command parsed">Parse Command</BtnFx>

        {strategyPlan && (
          <div style={{ marginTop: 10 }}>
            <div style={{ background: C.surface, borderRadius: 8, padding: "8px 10px", marginBottom: 8 }}>
              <KV k="Trigger" v={`${strategyPlan.triggerType} ${strategyPlan.triggerPrice ?? ""}`} />
              <KV k="Side" v={<Pill label={strategyPlan.side} color={sigColor(strategyPlan.side)} />} />
              <KV k="TP / SL / TS" v={`${strategyPlan.takeProfitPct}% / ${strategyPlan.stopLossPct}% / ${strategyPlan.trailingStopPct}%`} />
              <KV k="Qty" v={strategyPlan.quantity} />
            </div>
            <div style={{ display: "flex", gap: 6 }}>
              <BtnFx onClick={onEvaluatePlan} color="ghost" style={{ flex: 1 }} toast="Plan evaluated">Evaluate</BtnFx>
              <BtnFx onClick={onStartMonitor} color="green" style={{ flex: 1 }} toast="Monitor started">▶ Monitor</BtnFx>
            </div>
            {monitor && (
              <BtnFx onClick={onStopMonitor} color="red" style={{ width: "100%", marginTop: 6 }} toast="Monitor stopped">
                ⏹ Stop Monitor ({monitor.status})
              </BtnFx>
            )}
          </div>
        )}
      </Card>

      {/* Alerts */}
      {alerts.length > 0 && (
        <Card>
          <CardTitle>Alerts</CardTitle>
          {alerts.map((a, i) => (
            <div key={i} style={{ fontSize: 11, color: C.yellow, padding: "2px 0" }}>• {a}</div>
          ))}
        </Card>
      )}
    </div>
  )
})

// ── Tab: AutoTrade ────────────────────────────────────────────────────────────
const TabAuto = memo(({ symbol, autoTradeStatus, onStartAutoTrade, onStopAutoTrade, onResetAutoTrade }: {
  symbol: string; autoTradeStatus: AutoTradeStatus | null
  onStartAutoTrade: (cfg: AutoTradeConfig) => void
  onStopAutoTrade: () => void; onResetAutoTrade: () => void
}) => {
  const [sym, setSym] = useState(symbol)
  // Sync sym when parent symbol changes — placed after running is declared below
  const [usdt, setUsdt] = useState("20")
  const [lev, setLev] = useState("5")
  const [margin, setMargin] = useState("ISOLATED")
  const [tp, setTp] = useState("1.8")
  const [sl, setSl] = useState("0.8")
  const [interval, setInterval] = useState("20")
  const [minConf, setMinConf] = useState("0.65")
  const [cooldown, setCooldown] = useState("120")
  const [maxTrades, setMaxTrades] = useState("6")
  const [trailing, setTrailing] = useState("0")
  const [maxSpread, setMaxSpread] = useState("22")
  const [mode, setMode] = useState<"PAPER" | "LIVE">("PAPER")
  const [allowFlip, setAllowFlip] = useState(false)
  const [aggressive, setAggressive] = useState(false)

  const running = autoTradeStatus?.running ?? false
  // Sync sym when parent symbol changes and not running
  useEffect(() => { if (!running) setSym(symbol) }, [symbol, running])
  const paper = autoTradeStatus?.paper
  const cfg = autoTradeStatus?.config
  const total = (paper?.wins ?? 0) + (paper?.losses ?? 0)
  // Use backend-provided winRatePct if available, else calculate
  const winRatePct = paper?.winRatePct != null && total > 0
    ? paper.winRatePct.toFixed(0)
    : total > 0 ? ((paper!.wins / total) * 100).toFixed(0) : null
  const pnl = paper?.realizedPnl ?? 0
  const activePos = autoTradeStatus?.activePosition
  const livePos = activePos?.live
  const paperPos = paper?.position

  const handleStart = () => onStartAutoTrade({
    symbol: sym || symbol,
    usdtAmount: Number(usdt), leverage: Number(lev),
    marginType: margin as "ISOLATED" | "CROSSED",
    takeProfitPct: Number(tp), stopLossPct: Number(sl),
    intervalSec: Number(interval), minConfidence: Number(minConf),
    cooldownSec: Number(cooldown), maxTradesPerHour: Number(maxTrades),
    allowFlip, trailingStopPct: Number(trailing),
    executionMode: mode, aggressiveScalp: aggressive,
    maxSpreadBps: Number(maxSpread),
  })

  return (
    <div>
      {/* Recovery notice from snapshot */}
      {!running && autoTradeStatus?.continuity?.recoveredLog && (
        <Card style={{ borderColor: C.blue + "44" }}>
          <div style={{ fontSize: 11, color: C.blue, fontWeight: 700, marginBottom: 4 }}>ℹ️ Snapshot Restored</div>
          <div style={{ fontSize: 10, color: C.subtle, lineHeight: 1.6 }}>
            {autoTradeStatus.continuity.recoveredLog}
          </div>
          {autoTradeStatus.continuity.snapshotSavedAt && (
            <div style={{ fontSize: 10, color: C.muted, marginTop: 4 }}>
              Saved: {new Date(autoTradeStatus.continuity.snapshotSavedAt * 1000).toLocaleString()}
            </div>
          )}
        </Card>
      )}

      {/* Running status */}
      {running && cfg && (
        <Card style={{ borderColor: C.green + "55" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
            <div>
              <span style={{ color: C.green, fontWeight: 800, fontSize: 13 }}>● RUNNING</span>
              <span style={{ color: C.muted, fontSize: 11, marginLeft: 8 }}>{cfg.executionMode}</span>
            </div>
            <Pill label={cfg.symbol} color={C.blue} />
          </div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 8 }}>
            <span style={{ fontSize: 11, color: C.subtle }}>x{cfg.leverage} {cfg.marginType}</span>
            <span style={{ fontSize: 11, color: C.subtle }}>TP {cfg.takeProfitPct}% / SL {cfg.stopLossPct}%</span>
            <span style={{ fontSize: 11, color: C.subtle }}>{cfg.usdtAmount} USDT</span>
          </div>

          {/* Open position — show paper or live */}
          {(paperPos || (livePos && livePos.side !== "FLAT")) && (
            <div style={{ background: C.surface, borderRadius: 8, padding: "8px 10px", marginBottom: 8, border: `1px solid ${sigColor(paperPos?.side ?? livePos?.side)}44` }}>
              <div style={{ color: sigColor(paperPos?.side ?? livePos?.side), fontWeight: 700, fontSize: 12, marginBottom: 4 }}>
                📌 {cfg?.executionMode === "LIVE" ? "LIVE" : "PAPER"} {paperPos?.side ?? livePos?.side} Position
              </div>
              {paperPos && (
                <>
                  <KV k="Entry" v={paperPos.entry.toFixed(4)} />
                  <KV k="TP" v={paperPos.tp.toFixed(4)} vc={C.green} />
                  <KV k="SL" v={paperPos.sl.toFixed(4)} vc={C.red} />
                </>
              )}
              {livePos && livePos.side !== "FLAT" && (
                <>
                  <KV k="Live Qty" v={livePos.qty.toFixed(4)} />
                  <KV k="Notional" v={`~${livePos.notionalUsdtApprox.toFixed(2)} USDT`} />
                </>
              )}
            </div>
          )}

          {/* Stats */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 4, marginBottom: 8 }}>
            {[
              { label: "WIN",    value: String(paper?.wins ?? 0),  color: C.green },
              { label: "LOSS",   value: String(paper?.losses ?? 0), color: C.red },
              { label: "W-RATE", value: winRatePct ? `${winRatePct}%` : "—", color: winRatePct && Number(winRatePct) >= 50 ? C.green : C.red },
              { label: "PnL",    value: `${pnl >= 0 ? "+" : ""}${pnl.toFixed(3)}`, color: pnl >= 0 ? C.green : C.red },
            ].map(s => (
              <div key={s.label} style={{ background: C.surface, borderRadius: 6, padding: "6px 4px", textAlign: "center" }}>
                <div style={{ fontSize: 9, color: C.muted, marginBottom: 2 }}>{s.label}</div>
                <div style={{ fontSize: 13, fontWeight: 800, color: s.color }}>{s.value}</div>
              </div>
            ))}
          </div>
          {/* Trades/hr indicator */}
          {autoTradeStatus?.tradesLastHour != null && (
            <div style={{ fontSize: 10, color: C.muted, marginBottom: 6 }}>
              Trades this hour: <span style={{ color: C.subtle, fontWeight: 700 }}>{autoTradeStatus.tradesLastHour}</span>
              {cfg?.maxTradesPerHour && <span style={{ color: C.border }}> / {cfg.maxTradesPerHour} max</span>}
            </div>
          )}

          {/* Last decision */}
          {autoTradeStatus?.lastDecision && (
            <div style={{ background: C.surface, borderRadius: 6, padding: "6px 8px", marginBottom: 6 }}>
              <div style={{ fontSize: 10, color: C.muted, marginBottom: 2 }}>Last Decision</div>
              <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <Pill label={autoTradeStatus.lastDecision.signal} color={sigColor(autoTradeStatus.lastDecision.signal)} />
                <span style={{ fontSize: 11, color: C.subtle }}>{Math.round((autoTradeStatus.lastDecision.confidence ?? 0) * 100)}% conf</span>
              </div>
              <div style={{ fontSize: 10, color: C.muted, marginTop: 3 }}>{autoTradeStatus.lastDecision.setup?.slice(0, 60)}</div>
            </div>
          )}

          {/* Skip / errors */}
          {autoTradeStatus?.lastSkip && (
            <div style={{
              fontSize: 10,
              color: autoTradeStatus.lastSkip.code === "timeout" ? C.yellow
                   : autoTradeStatus.lastSkip.code === "balance" ? C.yellow
                   : autoTradeStatus.lastSkip.code === "exception" ? C.red
                   : C.muted,
              marginBottom: 4,
              background: autoTradeStatus.lastSkip.code === "exception" ? C.redDim + "44"
                        : autoTradeStatus.lastSkip.code === "balance" ? C.yellowDim + "44"
                        : "transparent",
              borderRadius: 4,
              padding: ["exception","balance"].includes(autoTradeStatus.lastSkip.code) ? "3px 6px" : "0",
            }}>
              {autoTradeStatus.lastSkip.code === "timeout" ? "⏱" :
               autoTradeStatus.lastSkip.code === "balance" ? "💰" :
               autoTradeStatus.lastSkip.code === "exception" ? "⚠" : "⏭"} {autoTradeStatus.lastSkip.msg}
            </div>
          )}
          {(autoTradeStatus?.consecutiveErrors ?? 0) > 0 && (
            <div style={{ fontSize: 10, color: (autoTradeStatus?.consecutiveErrors ?? 0) >= 3 ? C.red : C.yellow, marginBottom: 4 }}>
              {(autoTradeStatus?.consecutiveErrors ?? 0) >= 3 ? "🔴" : "🟡"} {autoTradeStatus!.consecutiveErrors} consecutive error{autoTradeStatus!.consecutiveErrors > 1 ? "s" : ""}
              {(autoTradeStatus?.consecutiveErrors ?? 0) >= 5 && " — consider stopping"}
            </div>
          )}

          {/* Log */}
          <div style={{ maxHeight: 80, overflowY: "auto" }}>
            {autoTradeStatus?.log?.slice(0, 5).map((l, i) => (
              <div key={i} style={{ fontSize: 10, color: C.muted, padding: "1px 0" }}>
                <span style={{ color: C.border }}>{new Date(l.ts * 1000).toLocaleTimeString()} </span>{l.msg}
              </div>
            ))}
          </div>

          {/* Controls */}
          <div style={{ display: "flex", gap: 6, marginTop: 10 }}>
            <BtnFx color="red" onClick={onStopAutoTrade} style={{ flex: 1 }} toast="AutoTrade stopped">⏹ Stop</BtnFx>
            <BtnFx color="ghost" onClick={onResetAutoTrade} style={{ flex: 1 }} toast="Session reset">🔄 Reset</BtnFx>
          </div>
        </Card>
      )}

      {/* Last session summary when stopped */}
      {!running && total > 0 && (
        <Card>
          <CardTitle>Last Session</CardTitle>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 4, marginBottom: 6 }}>
            {[
              { label: "WIN", value: String(paper?.wins ?? 0), color: C.green },
              { label: "LOSS", value: String(paper?.losses ?? 0), color: C.red },
              { label: "W-RATE", value: winRatePct ? `${winRatePct}%` : "—", color: winRatePct && Number(winRatePct) >= 50 ? C.green : C.red },
              { label: "PnL", value: `${pnl >= 0 ? "+" : ""}${pnl.toFixed(3)}`, color: pnl >= 0 ? C.green : C.red },
            ].map(s => (
              <div key={s.label} style={{ background: C.surface, borderRadius: 6, padding: "6px 4px", textAlign: "center" }}>
                <div style={{ fontSize: 9, color: C.muted, marginBottom: 2 }}>{s.label}</div>
                <div style={{ fontSize: 13, fontWeight: 800, color: s.color }}>{s.value}</div>
              </div>
            ))}
          </div>
          {paper?.lastTrades?.slice(0, 5).map((h, i) => (
            <div key={i} style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: C.muted, padding: "2px 0", borderBottom: `1px solid ${C.border}` }}>
              <span style={{ color: sigColor(h.side) }}>{h.side}</span>
              <span>{fmt4(h.entry)} → {fmt4(h.exit)}</span>
              <span style={{ color: h.pnl >= 0 ? C.green : C.red }}>{h.pnl >= 0 ? "+" : ""}{h.pnl.toFixed(4)}</span>
              <span style={{ color: C.muted }}>{h.reason}</span>
            </div>
          ))}
        </Card>
      )}

      {/* Config form */}
      {!running && (
        <Card>
          <CardTitle>Configuration</CardTitle>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 10 }}>
            <Input label="Symbol" value={sym} onChange={v => setSym(v.toUpperCase())} style={{ gridColumn: "1 / -1" }} />
            <Input label="USDT Amount" value={usdt} onChange={setUsdt} type="number" min={1} />
            <Input label="Leverage" value={lev} onChange={setLev} type="number" min={1} max={125} />
            <Select label="Margin Type" value={margin} onChange={setMargin}
              options={[{ value: "ISOLATED", label: "ISOLATED" }, { value: "CROSSED", label: "CROSSED" }]} />
            <Select label="Execution Mode" value={mode} onChange={v => setMode(v as "PAPER" | "LIVE")}
              options={[{ value: "PAPER", label: "📄 PAPER" }, { value: "LIVE", label: "⚡ LIVE" }]} />
            <Input label="TP %" value={tp} onChange={setTp} type="number" step={0.1} min={0.1} />
            <Input label="SL %" value={sl} onChange={setSl} type="number" step={0.1} min={0.1} />
            <Input label="Interval (s)" value={interval} onChange={setInterval} type="number" min={5} max={120} />
            <Input label="Min Confidence" value={minConf} onChange={setMinConf} type="number" step={0.05} min={0} max={1} />
            <Input label="Cooldown (s)" value={cooldown} onChange={setCooldown} type="number" min={0} />
            <Input label="Max Trades/hr" value={maxTrades} onChange={setMaxTrades} type="number" min={1} max={60} />
            <Input label="Trailing Stop %" value={trailing} onChange={setTrailing} type="number" step={0.1} min={0} />
            <Input label="Max Spread bps" value={maxSpread} onChange={setMaxSpread} type="number" min={0} />
          </div>
          <div style={{ display: "flex", gap: 12, marginBottom: 10 }}>
            <label style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, color: C.subtle, cursor: "pointer" }}>
              <input type="checkbox" checked={allowFlip} onChange={e => setAllowFlip(e.target.checked)} />
              Allow Flip
            </label>
            <label style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, color: C.subtle, cursor: "pointer" }}>
              <input type="checkbox" checked={aggressive} onChange={e => setAggressive(e.target.checked)} />
              Aggressive Scalp
            </label>
          </div>
          {mode === "LIVE" && (
            <div style={{ background: C.redDim, border: `1px solid ${C.red}`, borderRadius: 8, padding: "8px 10px", marginBottom: 10, fontSize: 11, color: C.red }}>
              ⚠ LIVE mode will execute real orders on Binance Futures. Ensure API keys are set and risk limits are configured.
            </div>
          )}
          <BtnFx color="green" onClick={handleStart} style={{ width: "100%", padding: "10px" }} toast="AutoTrade started">
            ▶ Start AutoTrade
          </BtnFx>
        </Card>
      )}
    </div>
  )
})

// ── Tab: Settings ─────────────────────────────────────────────────────────────
const TabSettings = memo(({ alerts, backendStatus, onStartBackend }: {
  alerts: string[]
  backendStatus: BackendStatus | null
  onStartBackend: () => Promise<void>
}) => {
  const [testnet, setTestnet] = useState(true)
  const [killSwitch, setKillSwitch] = useState(false)
  const [maxNotional, setMaxNotional] = useState("200")
  const [maxLeverage, setMaxLeverage] = useState("5")
  const [maxDailyLoss, setMaxDailyLoss] = useState("50")
  const [diagResult, setDiagResult] = useState<any>(null)
  const [diagLoading, setDiagLoading] = useState(false)
  const [envStatus, setEnvStatus] = useState<any>(null)

  const runDiag = async () => {
    setDiagLoading(true)
    setDiagResult(null)
    try {
      const [authRes, envRes] = await Promise.all([
        fetch("http://127.0.0.1:8000/debug/binance-auth-check?symbol=BTCUSDT"),
        fetch("http://127.0.0.1:8000/debug/env-status"),
      ])
      const auth = await authRes.json()
      const env = await envRes.json()
      setDiagResult(auth)
      setEnvStatus(env)
    } catch (e) {
      setDiagResult({ ok: false, stage: "network", message: "Cannot reach backend at :8000" })
    } finally {
      setDiagLoading(false)
    }
  }

  const applyRisk = async () => {
    try {
      await fetch("http://127.0.0.1:8000/risk-config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          killSwitch,
          maxNotionalUSDT: Number(maxNotional),
          maxLeverage: Number(maxLeverage),
          maxDailyLossUSDT: Number(maxDailyLoss),
        }),
      })
    } catch {}
  }

  return (
    <div>
      {/* ── Backend Control ── */}
      <Card style={{ borderColor: backendStatus?.online ? C.green + "44" : C.red + "44" }}>
        <CardTitle>Backend Server</CardTitle>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: backendStatus?.online ? 0 : 10 }}>
          <div>
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <span style={{
                width: 8, height: 8, borderRadius: "50%", display: "inline-block",
                background: backendStatus?.online ? C.green : C.red,
                animation: backendStatus?.online ? "pulse 2s infinite" : "none",
              }} />
              <span style={{ fontSize: 13, fontWeight: 700, color: backendStatus?.online ? C.green : C.red }}>
                {backendStatus?.online ? "Online" : "Offline"}
              </span>
              {backendStatus?.online && backendStatus.latencyMs != null && (
                <span style={{ fontSize: 10, color: C.muted }}>{backendStatus.latencyMs}ms</span>
              )}
            </div>
            <div style={{ fontSize: 10, color: C.muted, marginTop: 2 }}>http://127.0.0.1:8000</div>
          </div>
          {!backendStatus?.online && (
            <BtnFx color="green" onClick={onStartBackend} toast="Backend starting…" style={{ padding: "8px 16px" }}>
              ▶ Start Backend
            </BtnFx>
          )}
        </div>
        {!backendStatus?.online && (
          <div style={{ background: C.surface, borderRadius: 8, padding: "8px 10px", fontSize: 11, color: C.muted, lineHeight: 1.6, marginTop: 8 }}>
            <div style={{ color: C.yellow, fontWeight: 700, marginBottom: 4 }}>⚠ Native Host required for auto-start</div>
            <div>Run once: <code style={{ color: C.subtle, background: C.bg, padding: "1px 4px", borderRadius: 3 }}>native_host\install_host.bat</code></div>
            <div style={{ marginTop: 6, color: C.muted }}>Or start manually in terminal:</div>
            <code style={{ display: "block", background: C.bg, borderRadius: 6, padding: "6px 8px", marginTop: 4, color: C.subtle, fontSize: 10, wordBreak: "break-all" }}>
              cd backend{"\n"}.venv\Scripts\python.exe -m uvicorn main:app --port 8000
            </code>
          </div>
        )}
      </Card>

      {/* API Key diagnostic */}
      <Card>
        <CardTitle>API Key Diagnostic</CardTitle>

        {/* -2015 help box */}
        <div style={{ background: "#1a0a00", border: `1px solid ${C.yellow}44`, borderRadius: 8, padding: "10px 12px", marginBottom: 10 }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: C.yellow, marginBottom: 6 }}>
            ⚠ Error -2015 checklist
          </div>
          {[
            { step: "1", text: "IP Whitelist — เพิ่ม IP ปัจจุบันใน Binance API Management", link: "https://www.binance.com/en/my/settings/api-management", linkText: "Open API Management" },
            { step: "2", text: "Testnet/Mainnet — ถ้าใช้ Testnet key ต้องตั้ง BINANCE_TESTNET=true ใน .env", link: "https://testnet.binancefuture.com", linkText: "Testnet site" },
            { step: "3", text: "Futures Permission — เปิด 'Enable Futures' ใน API key settings" },
            { step: "4", text: "Key pair ถูกต้อง — ตรวจว่า copy ครบ ไม่มีช่องว่าง" },
          ].map(item => (
            <div key={item.step} style={{ display: "flex", gap: 8, marginBottom: 6, alignItems: "flex-start" }}>
              <span style={{ background: C.yellowDim, color: C.yellow, borderRadius: 4, padding: "0 5px", fontSize: 10, fontWeight: 700, flexShrink: 0, marginTop: 1 }}>{item.step}</span>
              <div>
                <div style={{ fontSize: 11, color: C.subtle }}>{item.text}</div>
                {item.link && (
                  <a href={item.link} target="_blank" rel="noreferrer"
                    style={{ fontSize: 10, color: C.blue, textDecoration: "none" }}>
                    → {item.linkText}
                  </a>
                )}
              </div>
            </div>
          ))}
        </div>

        <BtnFx onClick={runDiag} disabled={diagLoading} color="blue" style={{ width: "100%", marginBottom: 8 }} toast="Diagnostic complete">
          {diagLoading ? "⏳ Checking…" : "🔍 Run Auth Diagnostic"}
        </BtnFx>

        {/* Env status */}
        {envStatus && (
          <div style={{ background: C.surface, borderRadius: 8, padding: "8px 10px", marginBottom: 8 }}>
            <div style={{ fontSize: 10, color: C.muted, marginBottom: 4 }}>ENV STATUS</div>
            <KV k="API Key set" v={envStatus.binanceApiKeySet ? `✅ (${envStatus.binanceApiKeyLen} chars)` : "❌ Missing"} vc={envStatus.binanceApiKeySet ? C.green : C.red} />
            <KV k="API Secret set" v={envStatus.binanceApiSecretSet ? `✅ (${envStatus.binanceApiSecretLen} chars)` : "❌ Missing"} vc={envStatus.binanceApiSecretSet ? C.green : C.red} />
            <KV k="Testnet" v={envStatus.binanceTestnet} vc={envStatus.binanceTestnet === "true" ? C.yellow : C.green} />
            <div style={{ fontSize: 10, color: C.muted, marginTop: 4 }}>
              .env path: <code style={{ color: C.subtle, fontSize: 9 }}>{envStatus.envPath}</code>
            </div>
          </div>
        )}

        {/* Diag result */}
        {diagResult && (
          <div style={{
            background: diagResult.ok ? "#052e16" : "#1a0505",
            border: `1px solid ${diagResult.ok ? C.green : C.red}44`,
            borderRadius: 8, padding: "10px 12px",
          }}>
            <div style={{ fontWeight: 700, fontSize: 12, color: diagResult.ok ? C.green : C.red, marginBottom: 6 }}>
              {diagResult.ok ? "✅ Auth OK" : `❌ Failed at: ${diagResult.stage}`}
            </div>
            <div style={{ fontSize: 11, color: C.subtle, marginBottom: 4 }}>{diagResult.message}</div>
            {diagResult.hint && (
              <div style={{ fontSize: 11, color: C.yellow, background: C.yellowDim + "44", borderRadius: 6, padding: "6px 8px", marginBottom: 4 }}>
                💡 {diagResult.hint}
              </div>
            )}
            {diagResult.markPrice && (
              <KV k="Mark Price" v={diagResult.markPrice} />
            )}
            {diagResult.base && (
              <KV k="Endpoint" v={diagResult.base} />
            )}
          </div>
        )}
      </Card>

      {/* .env instructions */}
      <Card>
        <CardTitle>Configure .env</CardTitle>
        <div style={{ fontSize: 11, color: C.subtle, marginBottom: 8, lineHeight: 1.6 }}>
          แก้ไขไฟล์ <code style={{ color: C.blue, background: C.surface, padding: "1px 4px", borderRadius: 3 }}>backend/.env</code> แล้ว restart backend
        </div>
        <div style={{ background: C.surface, borderRadius: 8, padding: "10px 12px", fontFamily: "monospace", fontSize: 11, color: C.subtle, lineHeight: 1.8 }}>
          <div><span style={{ color: C.muted }}>BINANCE_API_KEY=</span><span style={{ color: C.green }}>your_key_here</span></div>
          <div><span style={{ color: C.muted }}>BINANCE_API_SECRET=</span><span style={{ color: C.green }}>your_secret_here</span></div>
          <div><span style={{ color: C.muted }}>BINANCE_TESTNET=</span><span style={{ color: C.yellow }}>true</span><span style={{ color: C.muted }}> # false for mainnet</span></div>
        </div>
        <div style={{ fontSize: 10, color: C.muted, marginTop: 8 }}>
          หลังแก้ไข .env ให้ restart backend:
          <code style={{ display: "block", background: C.surface, borderRadius: 6, padding: "6px 8px", marginTop: 4, color: C.subtle, fontSize: 10 }}>
            uvicorn main:app --reload --port 8000
          </code>
        </div>
      </Card>

      {/* Risk limits */}
      <Card>
        <CardTitle>Risk Limits</CardTitle>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 10 }}>
          <Input label="Max Notional USDT" value={maxNotional} onChange={setMaxNotional} type="number" />
          <Input label="Max Leverage" value={maxLeverage} onChange={setMaxLeverage} type="number" />
          <Input label="Max Daily Loss USDT" value={maxDailyLoss} onChange={setMaxDailyLoss} type="number" style={{ gridColumn: "1 / -1" }} />
        </div>
        <label style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12, color: killSwitch ? C.red : C.subtle, cursor: "pointer", marginBottom: 10 }}>
          <input type="checkbox" checked={killSwitch} onChange={e => setKillSwitch(e.target.checked)} />
          Kill Switch (block all auto-trading)
        </label>
        <BtnFx color={killSwitch ? "red" : "ghost"} onClick={applyRisk} style={{ width: "100%" }} toast="Risk config applied">Apply Risk Config</BtnFx>
      </Card>

      {/* Alerts */}
      {alerts.length > 0 && (
        <Card>
          <CardTitle>Risk Alerts</CardTitle>
          {alerts.map((a, i) => <div key={i} style={{ fontSize: 11, color: C.yellow, padding: "2px 0" }}>• {a}</div>)}
        </Card>
      )}

      <Card>
        <CardTitle>About</CardTitle>
        <KV k="Extension" v="Binance AI Copilot" />
        <KV k="Version" v="0.2.0" />
        <KV k="Backend" v="FastAPI 0.4.0" />
        <KV k="Engine" v="Multi-TF Confluence" />
        <div style={{ fontSize: 10, color: C.muted, marginTop: 8, lineHeight: 1.6 }}>
          Indicators: RSI · MACD · BB · VWAP · ATR · StochRSI · CVD · S/R · Session
        </div>
      </Card>
    </div>
  )
})

// ── Main CopilotPanel ─────────────────────────────────────────────────────────

// Width constants — exported so overlay.tsx can inject body padding
export const PANEL_WIDTH = 332   // panel open width px
export const PANEL_COLLAPSED_W = 36  // collapsed tab strip width px

export const CopilotPanel = memo(({
  symbol, loading, backendStatus, insight, intel, orderBook, vision,
  strategyPlan, monitor, autoTradeStatus, alerts, autoRefreshSec,
  onRefresh, onAnalyzeVision, onParseCommand, onEvaluatePlan,
  onStartMonitor, onStopMonitor, onAction, onSetAutoRefresh,
  onStartAutoTrade, onStopAutoTrade, onResetAutoTrade,
  onPanelToggle, onStartBackend,
}: Props & { onPanelToggle?: (open: boolean) => void }) => {
  const [tab, setTab] = useState<Tab>("data")
  const [open, setOpen] = useState(true)

  const atRunning = autoTradeStatus?.running ?? false
  const sig = intel?.signal ?? insight?.recommendation ?? null

  const tabs: { id: Tab; label: string; icon: string; badge?: string }[] = [
    { id: "data",     icon: "📊", label: "Data" },
    { id: "trade",    icon: "⚡", label: "Trade" },
    { id: "auto",     icon: "🤖", label: "Auto", badge: atRunning ? "●" : undefined },
    { id: "settings", icon: "⚙",  label: "Settings" },
  ]

  const toggle = () => {
    const next = !open
    setOpen(next)
    onPanelToggle?.(next)
  }

  // Collapsed strip — vertical tab icons + toggle button
  if (!open) {
    return (
      <ToastProvider>
      <div style={{
        position: "fixed", top: 0, right: 0, bottom: 0,
        width: PANEL_COLLAPSED_W, zIndex: 2147483647,
        background: C.surface, borderLeft: `1px solid ${C.border}`,
        display: "flex", flexDirection: "column", alignItems: "center",
        paddingTop: 12, gap: 4,
        fontFamily: "ui-sans-serif, 'Segoe UI', system-ui, sans-serif",
      }}>
        {/* Expand button */}
        <button onClick={toggle} title="Open panel" style={{
          width: 28, height: 28, borderRadius: 8, border: `1px solid ${C.border}`,
          background: C.card, color: C.text, cursor: "pointer",
          fontSize: 14, display: "flex", alignItems: "center", justifyContent: "center",
          marginBottom: 8,
        }}>◀</button>

        {/* Signal dot */}
        {sig && (
          <div style={{
            width: 28, height: 28, borderRadius: 8,
            background: sigColor(sig) + "22", border: `1px solid ${sigColor(sig)}`,
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 9, fontWeight: 900, color: sigColor(sig), marginBottom: 4,
          }}>{sig === "LONG" ? "L" : sig === "SHORT" ? "S" : "W"}</div>
        )}

        {/* Backend dot */}
        <div style={{
          width: 8, height: 8, borderRadius: "50%",
          background: backendStatus?.online ? C.green : C.red,
          marginBottom: 8,
          animation: backendStatus?.online ? "pulse 2s infinite" : "none",
        }} title={backendStatus?.online ? "Backend online" : "Backend offline"} />

        {/* Tab icons */}
        {tabs.map(t => (
          <button key={t.id} onClick={() => { setTab(t.id); toggle() }}
            title={t.label}
            style={{
              width: 28, height: 28, borderRadius: 8,
              border: `1px solid ${tab === t.id ? C.blue : C.border}`,
              background: tab === t.id ? C.blueDim : "transparent",
              color: C.text, cursor: "pointer", fontSize: 14,
              display: "flex", alignItems: "center", justifyContent: "center",
              position: "relative",
            }}>
            {t.icon}
            {t.badge && (
              <span style={{ position: "absolute", top: 2, right: 2, width: 6, height: 6, borderRadius: "50%", background: C.green }} />
            )}
          </button>
        ))}

        <style>{`@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}`}</style>
      </div>
      </ToastProvider>
    )  }

  // ── Full panel ──────────────────────────────────────────────────────────────
  return (
    <ToastProvider>
    <div style={{
      position: "fixed", top: 0, right: 0, bottom: 0,
      width: PANEL_WIDTH, zIndex: 2147483647,
      background: C.bg, color: C.text,
      borderLeft: `1px solid ${C.border}`,
      boxShadow: "-8px 0 32px rgba(0,0,0,0.5)",
      fontFamily: "ui-sans-serif, 'Segoe UI', system-ui, sans-serif",
      display: "flex", flexDirection: "column",
    }}>
      {/* ── Header (sticky) ── */}
      <div style={{
        padding: "10px 12px 0",
        background: `linear-gradient(180deg, ${C.surface} 0%, ${C.bg} 100%)`,
        borderBottom: `1px solid ${C.border}`,
        flexShrink: 0,
      }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 6 }}>
          <div>
            <div style={{ fontSize: 9, color: C.muted, letterSpacing: 1.5, textTransform: "uppercase" }}>AI Copilot</div>
            <div style={{ fontSize: 20, fontWeight: 900, letterSpacing: 0.5, lineHeight: 1.1 }}>{symbol}</div>
          </div>
          <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 4 }}>
            <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
              {sig && (
                <div style={{
                  padding: "3px 10px", borderRadius: 20,
                  background: sigColor(sig) + "22", border: `1px solid ${sigColor(sig)}`,
                  color: sigColor(sig), fontWeight: 900, fontSize: 13, letterSpacing: 1,
                }}>{sig}</div>
              )}
              {/* Collapse button */}
              <button onClick={toggle} title="Collapse panel" style={{
                width: 26, height: 26, borderRadius: 7,
                border: `1px solid ${C.border}`, background: C.card,
                color: C.muted, cursor: "pointer", fontSize: 13,
                display: "flex", alignItems: "center", justifyContent: "center",
              }}>▶</button>
            </div>
            <StatusBadge status={backendStatus} />
          </div>
        </div>

        {/* Tab bar */}
        <div style={{ display: "flex", gap: 1 }}>
          {tabs.map(t => (
            <button key={t.id} onClick={() => setTab(t.id)} style={{
              flex: 1, padding: "6px 2px", border: "none",
              borderBottom: `2px solid ${tab === t.id ? C.blue : "transparent"}`,
              background: "transparent",
              color: tab === t.id ? C.text : C.muted,
              fontWeight: tab === t.id ? 700 : 400,
              fontSize: 11, cursor: "pointer", transition: "all .15s",
              position: "relative",
            }}>
              <span style={{ marginRight: 3 }}>{t.icon}</span>{t.label}
              {t.badge && (
                <span style={{
                  position: "absolute", top: 3, right: 4,
                  width: 6, height: 6, borderRadius: "50%", background: C.green,
                }} />
              )}
            </button>
          ))}
        </div>
      </div>

      {/* ── Scrollable content ── */}
      <div style={{
        flex: 1, overflowY: "auto", padding: "10px 10px 16px",
        scrollbarWidth: "thin", scrollbarColor: `${C.border} transparent`,
      }}>
        {tab === "data" && (
          <TabData
            intel={intel} orderBook={orderBook} vision={vision}
            loading={loading} onRefresh={onRefresh} onAnalyzeVision={onAnalyzeVision}
            autoRefreshSec={autoRefreshSec} onSetAutoRefresh={onSetAutoRefresh}
          />
        )}
        {tab === "trade" && (
          <TabTrade
            symbol={symbol} strategyPlan={strategyPlan} monitor={monitor} alerts={alerts}
            onParseCommand={onParseCommand} onEvaluatePlan={onEvaluatePlan}
            onStartMonitor={onStartMonitor} onStopMonitor={onStopMonitor} onAction={onAction}
          />
        )}
        {tab === "auto" && (
          <TabAuto
            symbol={symbol} autoTradeStatus={autoTradeStatus}
            onStartAutoTrade={onStartAutoTrade}
            onStopAutoTrade={onStopAutoTrade}
            onResetAutoTrade={onResetAutoTrade}
          />
        )}
        {tab === "settings" && (
          <TabSettings alerts={alerts} backendStatus={backendStatus} onStartBackend={onStartBackend} />
        )}
      </div>

      <style>{`
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
        @keyframes toastIn { from{opacity:0;transform:translateY(8px)} to{opacity:1;transform:translateY(0)} }
        ::-webkit-scrollbar { width: 4px }
        ::-webkit-scrollbar-track { background: transparent }
        ::-webkit-scrollbar-thumb { background: ${C.border}; border-radius: 2px }
      `}</style>
    </div>
    </ToastProvider>
  )
})
