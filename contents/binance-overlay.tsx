import { useEffect, useRef, useState } from "react"
import { createRoot } from "react-dom/client"
import { CopilotPanel, PANEL_WIDTH, PANEL_COLLAPSED_W, PANEL_MOBILE_BREAKPOINT } from "~components/CopilotPanel"
import {
  analyzeChartVision, analyzeIntel, checkHealth, evaluateStrategy,
  fetchAutoTradeStatus, fetchRiskAlerts, listMonitors, parseStrategyCommand,
  resetAutoTrade, sendTradeSignal, startAutoTrade, startBackendProcess,
  startMonitor, stopAutoTrade, stopMonitor,
} from "~lib/backend"
import { fetchOrderBookSummary } from "~lib/orderbook"
import type {
  AiInsight, AutoTradeConfig, AutoTradeStatus, BackendStatus,
  IntelResult, MonitorState, OrderBookSummary, Side, StrategyPlan, VisionInsight,
} from "~lib/types"

// ── Helpers ───────────────────────────────────────────────────────────────────

const readSymbol = () => {
  const url = new URL(window.location.href)
  return (
    url.searchParams.get("symbol") ||
    document.title.match(/([A-Z]{3,8}USDT)/)?.[1] ||
    "BTCUSDT"
  ).toUpperCase()
}

const getChartSnapshot = (): string | null => {
  const canvases = Array.from(document.querySelectorAll("canvas")) as HTMLCanvasElement[]
  const target = canvases
    .filter((c) => c.width >= 300 && c.height >= 180)
    .sort((a, b) => b.width * b.height - a.width * a.height)[0]
  if (!target) return null
  try { return target.toDataURL("image/png") } catch { return null }
}

const pushAlert = (prev: string[], msg: string) => [msg, ...prev].slice(0, 6)

// ── App ───────────────────────────────────────────────────────────────────────

const App = ({ onPanelToggle }: { onPanelToggle?: (open: boolean) => void }) => {
  const [symbol, setSymbol]               = useState(readSymbol())
  const [loading, setLoading]             = useState(false)
  const [backendStatus, setBackendStatus] = useState<BackendStatus | null>(null)
  const [insight, setInsight]             = useState<AiInsight | null>(null)
  const [intel, setIntel]                 = useState<IntelResult | null>(null)
  const [orderBook, setOrderBook]         = useState<OrderBookSummary | null>(null)
  const [vision, setVision]               = useState<VisionInsight | null>(null)
  const [strategyPlan, setStrategyPlan]   = useState<StrategyPlan | null>(null)
  const [monitor, setMonitor]             = useState<MonitorState | null>(null)
  const [autoTradeStatus, setAutoTradeStatus] = useState<AutoTradeStatus | null>(null)
  const [alerts, setAlerts]               = useState<string[]>([])
  const [autoRefreshSec, setAutoRefreshSec] = useState(30)

  // ── Stable refs — avoid re-creating callbacks on every render ────────────
  const symbolRef        = useRef(readSymbol())
  const backendOnlineRef = useRef(false)
  const autoTradeRef     = useRef<AutoTradeStatus | null>(null)
  const loadingRef       = useRef(false)
  const autoRefreshRef   = useRef(autoRefreshSec)
  const tickRef          = useRef(0)  // counts 5s ticks

  useEffect(() => { symbolRef.current = symbol }, [symbol])
  useEffect(() => { autoRefreshRef.current = autoRefreshSec }, [autoRefreshSec])

  // ── Analysis ──────────────────────────────────────────────────────────────
  const runAnalysis = async (sym: string) => {
    if (loadingRef.current) return  // prevent concurrent runs
    loadingRef.current = true
    setLoading(true)
    try {
      const [ob, intelResult, risk] = await Promise.all([
        fetchOrderBookSummary(sym),
        analyzeIntel(sym),
        fetchRiskAlerts(sym),
      ])
      setOrderBook(ob)
      setIntel(intelResult)
      setAlerts(risk.alerts)
      setInsight({
        trend: intelResult.signal === "LONG" ? "Bullish" : intelResult.signal === "SHORT" ? "Bearish" : "Neutral",
        rsi: intelResult.precision?.rsi14 ?? 50,
        volumeSignal: intelResult.momentum.volumeRatio > 1.2 ? "Strong" : intelResult.momentum.volumeRatio < 0.8 ? "Weak" : "Normal",
        setup: intelResult.setup,
        recommendation: intelResult.signal,
      })
    } catch (err) {
      setAlerts((p) => pushAlert(p, `Analysis error: ${err instanceof Error ? err.message : String(err)}`))
    } finally {
      loadingRef.current = false
      setLoading(false)
    }
  }

  // ── Single unified polling tick (every 5s) ────────────────────────────────
  // Replaces 5 separate intervals with 1. Each task runs on its own cadence
  // by checking the tick counter.
  //
  //  tick % 1 (every 5s)  → autotrade status (only when running)
  //  tick % 3 (every 15s) → health check
  //  tick % 6 (every 30s) → auto-refresh analysis (if enabled & not hidden)
  //  tick % 10 (every 50s)→ monitor list (only when a monitor exists)
  //
  useEffect(() => {
    const TICK_MS = 5000

    const tick = async () => {
      if (document.hidden) {
        tickRef.current++
        return
      }
      const t = ++tickRef.current

      // ── Health check every 15s ──────────────────────────────────────────
      if (t % 3 === 0) {
        try {
          const status = await checkHealth()
          const wasOnline = backendOnlineRef.current
          backendOnlineRef.current = status.online
          setBackendStatus(status)

          // Reconnect: backend just came back
          if (status.online && !wasOnline) {
            setTimeout(() => runAnalysis(symbolRef.current), 500)
            setTimeout(async () => {
              try {
                const at = await fetchAutoTradeStatus(symbolRef.current)
                autoTradeRef.current = at
                setAutoTradeStatus(at)
                if (at.running) {
                  setAlerts((p) => pushAlert(p, `✅ Backend reconnected — AutoTrade resumed (${at.config?.symbol ?? ""})`))
                } else if (at.continuity?.recoveredLog) {
                  setAlerts((p) => pushAlert(p, `ℹ️ ${at.continuity!.recoveredLog!.slice(0, 80)}`))
                }
              } catch {}
            }, 1200)
          }
        } catch {}
      }

      // ── AutoTrade status every 5s (only when running or just stopped) ───
      if (t % 1 === 0 && backendOnlineRef.current) {
        try {
          const status = await fetchAutoTradeStatus(symbolRef.current)
          const prev = autoTradeRef.current
          // Shallow diff — only update state when something meaningful changed
          const changed = !prev
            || prev.running !== status.running
            || prev.consecutiveErrors !== status.consecutiveErrors
            || prev.lastSkip?.ts !== status.lastSkip?.ts
            || prev.lastDecision?.signal !== status.lastDecision?.signal
            || prev.paper?.wins !== status.paper?.wins
            || prev.paper?.losses !== status.paper?.losses
            || prev.paper?.realizedPnl !== status.paper?.realizedPnl
            || prev.paper?.position?.entry !== status.paper?.position?.entry
            || prev.log?.[0]?.ts !== status.log?.[0]?.ts
          if (changed) {
            autoTradeRef.current = status
            setAutoTradeStatus(status)
          }
        } catch {}
      }

      // ── Auto-refresh analysis ────────────────────────────────────────────
      const refreshTicks = Math.max(1, Math.round(autoRefreshRef.current / (TICK_MS / 1000)))
      if (autoRefreshRef.current > 0 && t % refreshTicks === 0 && backendOnlineRef.current) {
        runAnalysis(symbolRef.current)
      }

      // ── Monitor list every 50s (only if monitor state exists) ───────────
      if (t % 10 === 0 && backendOnlineRef.current) {
        try {
          const data = await listMonitors()
          const first = data.items[0]
          if (first) setMonitor({ id: first.id, status: first.status })
        } catch {}
      }
    }

    // Initial health check immediately
    checkHealth().then((s) => {
      backendOnlineRef.current = s.online
      setBackendStatus(s)
      if (s.online) runAnalysis(symbolRef.current)
    }).catch(() => {})

    const id = window.setInterval(tick, TICK_MS)
    return () => window.clearInterval(id)
  }, []) // empty deps — never recreated

  // ── Symbol tracking via MutationObserver + URL polling ───────────────────
  // MutationObserver watches title changes (cheaper than setInterval 2s)
  useEffect(() => {
    let lastSym = symbolRef.current

    const check = () => {
      const sym = readSymbol()
      if (sym !== lastSym) {
        lastSym = sym
        symbolRef.current = sym
        setSymbol(sym)
      }
    }

    // Watch document title for symbol changes
    const obs = new MutationObserver(check)
    obs.observe(document.querySelector("title") ?? document.head, {
      subtree: true, childList: true, characterData: true,
    })

    // Fallback: check URL every 3s (for SPA navigation without title change)
    const id = window.setInterval(check, 3000)
    return () => { obs.disconnect(); window.clearInterval(id) }
  }, [])

  // Trigger analysis when symbol changes
  const prevSymRef = useRef(symbol)
  useEffect(() => {
    if (symbol !== prevSymRef.current) {
      prevSymRef.current = symbol
      if (backendOnlineRef.current) runAnalysis(symbol)
    }
  }, [symbol])

  // ── Handlers (stable refs — no useCallback deps that cause re-creation) ──

  const onAnalyzeVision = async () => {
    const imageDataUrl = getChartSnapshot()
    if (!imageDataUrl) return setAlerts((p) => pushAlert(p, "Vision error: cannot capture chart"))
    try {
      const result = await analyzeChartVision({ symbol: symbolRef.current, imageDataUrl })
      setVision(result)
      setAlerts((p) => pushAlert(p, `Vision: ${result.pattern} (${Math.round(result.confidence * 100)}%)`))
    } catch (err) {
      setAlerts((p) => pushAlert(p, `Vision error: ${err instanceof Error ? err.message : String(err)}`))
    }
  }

  const onParseCommand = async (command: string) => {
    try {
      const plan = await parseStrategyCommand(command, symbolRef.current)
      setStrategyPlan(plan)
      setAlerts((p) => pushAlert(p, `Plan ready: ${plan.side} ${plan.symbol}`))
    } catch (err) {
      setAlerts((p) => pushAlert(p, `Parse error: ${err instanceof Error ? err.message : String(err)}`))
    }
  }

  const onEvaluatePlan = async () => {
    if (!strategyPlan) return
    try {
      const result = await evaluateStrategy(strategyPlan)
      setAlerts((p) => pushAlert(p, `Plan eval: ${result.status} — ${result.reason}`))
      if (result.action === "LONG" || result.action === "SHORT")
        await sendTradeSignal({ symbol: strategyPlan.symbol, side: result.action, quantity: strategyPlan.quantity })
    } catch (err) {
      setAlerts((p) => pushAlert(p, `Eval error: ${err instanceof Error ? err.message : String(err)}`))
    }
  }

  const onStartMonitor = async () => {
    if (!strategyPlan) return
    try {
      const res = await startMonitor(strategyPlan, 10)
      setMonitor({ id: res.id, status: res.status })
      setAlerts((p) => pushAlert(p, `Monitor started: ${res.id}`))
    } catch (err) {
      setAlerts((p) => pushAlert(p, `Monitor start error: ${err instanceof Error ? err.message : String(err)}`))
    }
  }

  const onStopMonitor = async () => {
    if (!monitor) return
    try {
      await stopMonitor(monitor.id)
      setMonitor({ ...monitor, status: "STOPPED" })
      setAlerts((p) => pushAlert(p, "Monitor stopped"))
    } catch (err) {
      setAlerts((p) => pushAlert(p, `Monitor stop error: ${err instanceof Error ? err.message : String(err)}`))
    }
  }

  const onAction = async (side: Side, quantity: number) => {
    try {
      await sendTradeSignal({ symbol: symbolRef.current, side, quantity })
      setAlerts((p) => pushAlert(p, `Trade sent: ${side} ${symbolRef.current} qty ${quantity}`))
    } catch (err) {
      setAlerts((p) => pushAlert(p, `Trade error: ${err instanceof Error ? err.message : String(err)}`))
    }
  }

  const onStartAutoTrade = async (cfg: AutoTradeConfig) => {
    try {
      await startAutoTrade(cfg)
      setAlerts((p) => pushAlert(p, `AutoTrade started: ${cfg.executionMode} ${cfg.symbol} x${cfg.leverage}`))
      const status = await fetchAutoTradeStatus(cfg.symbol)
      autoTradeRef.current = status
      setAutoTradeStatus(status)
    } catch (err) {
      setAlerts((p) => pushAlert(p, `AutoTrade error: ${err instanceof Error ? err.message : String(err)}`))
    }
  }

  const onStopAutoTrade = async () => {
    try {
      await stopAutoTrade(autoTradeRef.current?.sessionId ?? undefined)
      setAlerts((p) => pushAlert(p, "AutoTrade stopped"))
      const status = await fetchAutoTradeStatus(symbolRef.current)
      autoTradeRef.current = status
      setAutoTradeStatus(status)
    } catch (err) {
      setAlerts((p) => pushAlert(p, `AutoTrade stop error: ${err instanceof Error ? err.message : String(err)}`))
    }
  }

  const onResetAutoTrade = async () => {
    try {
      await resetAutoTrade()
      setAlerts((p) => pushAlert(p, "AutoTrade reset"))
      const status = await fetchAutoTradeStatus(symbolRef.current)
      autoTradeRef.current = status
      setAutoTradeStatus(status)
    } catch (err) {
      setAlerts((p) => pushAlert(p, `AutoTrade reset error: ${err instanceof Error ? err.message : String(err)}`))
    }
  }

  const onStartBackend = async () => {
    const result = await startBackendProcess()
    if (result.ok) {
      setTimeout(async () => {
        const s = await checkHealth()
        backendOnlineRef.current = s.online
        setBackendStatus(s)
        if (s.online) runAnalysis(symbolRef.current)
      }, 2000)
    } else {
      setAlerts((p) => pushAlert(p, `Backend start failed: ${result.msg ?? "Native host not installed"}`))
    }
  }

  return (
    <CopilotPanel
      symbol={symbol}
      loading={loading}
      backendStatus={backendStatus}
      insight={insight}
      intel={intel}
      orderBook={orderBook}
      vision={vision}
      strategyPlan={strategyPlan}
      monitor={monitor}
      autoTradeStatus={autoTradeStatus}
      alerts={alerts}
      autoRefreshSec={autoRefreshSec}
      onRefresh={() => runAnalysis(symbolRef.current)}
      onAnalyzeVision={onAnalyzeVision}
      onParseCommand={onParseCommand}
      onEvaluatePlan={onEvaluatePlan}
      onStartMonitor={onStartMonitor}
      onStopMonitor={onStopMonitor}
      onAction={onAction}
      onSetAutoRefresh={setAutoRefreshSec}
      onStartAutoTrade={onStartAutoTrade}
      onStopAutoTrade={onStopAutoTrade}
      onResetAutoTrade={onResetAutoTrade}
      onPanelToggle={onPanelToggle}
      onStartBackend={onStartBackend}
    />
  )
}

// ── Mount ─────────────────────────────────────────────────────────────────────

const STYLE_ID = "binance-copilot-body-style"

function injectBodyPadding(open: boolean) {
  let el = document.getElementById(STYLE_ID) as HTMLStyleElement | null
  if (!el) {
    el = document.createElement("style")
    el.id = STYLE_ID
    document.head.appendChild(el)
  }
  const pad = open ? PANEL_WIDTH : PANEL_COLLAPSED_W
  el.textContent = `
    body { padding-right: ${pad}px !important; box-sizing: border-box !important; }
    #__APP, [class*="layout"], [class*="Layout"], main { max-width: calc(100% - ${pad}px) !important; }
    @media (max-width: ${PANEL_MOBILE_BREAKPOINT}px) {
      body { padding-right: 0 !important; }
      #__APP, [class*="layout"], [class*="Layout"], main { max-width: 100% !important; }
    }
  `
}

injectBodyPadding(true)

const host = document.createElement("div")
host.id = "binance-ai-copilot-root"
host.style.cssText = "position:fixed;top:0;right:0;bottom:0;z-index:2147483647;pointer-events:none;"
document.documentElement.appendChild(host)

const shadow = host.attachShadow({ mode: "open" })
const mountPoint = document.createElement("div")
mountPoint.style.cssText = "pointer-events:auto;height:100%;"
shadow.appendChild(mountPoint)

createRoot(mountPoint).render(<App onPanelToggle={injectBodyPadding} />)
