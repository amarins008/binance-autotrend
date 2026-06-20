import type { AiInsight, BackendStatus, IntelResult, OrderBookSummary, Side, StrategyPlan, VisionInsight } from "./types"

const BASE_URL = "http://127.0.0.1:8000"

// Reuse keep-alive connections — reduces TIME_WAIT connection buildup
const HEADERS = { "Content-Type": "application/json", "Connection": "keep-alive" }
const GET_HEADERS = { "Connection": "keep-alive" }

// ── Native Messaging — start/stop backend process ─────────────────────────────
const NATIVE_HOST = "com.cmux_hermes.host"

const sendNativeMessage = (msg: object): Promise<{ ok: boolean; msg?: string; running?: boolean }> =>
  new Promise((resolve) => {
    try {
      chrome.runtime.sendNativeMessage(NATIVE_HOST, msg, (response) => {
        if (chrome.runtime.lastError) {
          resolve({ ok: false, msg: chrome.runtime.lastError.message })
        } else {
          resolve(response ?? { ok: false, msg: "No response" })
        }
      })
    } catch (e: any) {
      resolve({ ok: false, msg: e?.message ?? "Native messaging unavailable" })
    }
  })

export const startBackendProcess = () => sendNativeMessage({ action: "start" })
export const stopBackendProcess = () => sendNativeMessage({ action: "stop" })
export const checkBackendProcess = () => sendNativeMessage({ action: "status" })

// ── Health / backend status ───────────────────────────────────────────────────

export const checkHealth = async (): Promise<BackendStatus> => {
  const start = Date.now()
  try {
    const res = await fetch(`${BASE_URL}/health`, {
      signal: AbortSignal.timeout(4000),
      headers: GET_HEADERS,
    })
    const latencyMs = Date.now() - start
    if (res.ok) return { online: true, latencyMs, checkedAt: Date.now() }
    return { online: false, latencyMs, checkedAt: Date.now() }
  } catch {
    return { online: false, latencyMs: null, checkedAt: Date.now() }
  }
}

// ── Core analysis ─────────────────────────────────────────────────────────────

export const fetchAiInsight = async (symbol: string, orderBook: OrderBookSummary): Promise<AiInsight> => {
  const res = await fetch(`${BASE_URL}/analyze`, {
    method: "POST", headers: HEADERS,
    body: JSON.stringify({ symbol, orderBook }),
  })
  if (!res.ok) throw new Error(`AI analyze failed (${res.status})`)
  return (await res.json()) as AiInsight
}

export const analyzeIntel = async (symbol: string): Promise<IntelResult> => {
  const res = await fetch(`${BASE_URL}/intel/analyze`, {
    method: "POST", headers: HEADERS,
    body: JSON.stringify({ symbol }),
  })
  if (!res.ok) throw new Error(`Intel analyze failed (${res.status})`)
  return res.json() as Promise<IntelResult>
}

export const analyzeChartVision = async (payload: { symbol: string; imageDataUrl: string }): Promise<VisionInsight> => {
  const res = await fetch(`${BASE_URL}/analyze-vision`, {
    method: "POST", headers: HEADERS,
    body: JSON.stringify(payload),
  })
  if (!res.ok) throw new Error(`Vision analyze failed (${res.status})`)
  return (await res.json()) as VisionInsight
}

// ── Strategy ──────────────────────────────────────────────────────────────────

export const parseStrategyCommand = async (command: string, symbol: string): Promise<StrategyPlan> => {
  const res = await fetch(`${BASE_URL}/strategy/parse`, {
    method: "POST", headers: HEADERS,
    body: JSON.stringify({ command, symbol }),
  })
  if (!res.ok) throw new Error(`Strategy parse failed (${res.status})`)
  return (await res.json()) as StrategyPlan
}

export const evaluateStrategy = async (plan: StrategyPlan) => {
  const res = await fetch(`${BASE_URL}/strategy/evaluate`, {
    method: "POST", headers: HEADERS,
    body: JSON.stringify(plan),
  })
  if (!res.ok) throw new Error(`Strategy evaluate failed (${res.status})`)
  return res.json() as Promise<{ status: string; reason: string; action?: Side; risk?: string }>
}

export const startMonitor = async (plan: StrategyPlan, intervalSec = 10) => {
  const res = await fetch(`${BASE_URL}/monitor/start`, {
    method: "POST", headers: HEADERS,
    body: JSON.stringify({ plan, intervalSec }),
  })
  if (!res.ok) throw new Error(`Monitor start failed (${res.status})`)
  return res.json() as Promise<{ id: string; status: string }>
}

export const listMonitors = async () => {
  const res = await fetch(`${BASE_URL}/monitor/list`, { headers: GET_HEADERS })
  if (!res.ok) throw new Error(`Monitor list failed (${res.status})`)
  return res.json() as Promise<{ items: Array<{ id: string; status: string; lastResult?: { status?: string; reason?: string } }> }>
}

export const stopMonitor = async (monitorId: string) => {
  const res = await fetch(`${BASE_URL}/monitor/stop/${monitorId}`, {
    method: "POST", headers: GET_HEADERS,
  })
  if (!res.ok) throw new Error(`Monitor stop failed (${res.status})`)
  return res.json()
}

export const sendTradeSignal = async (payload: { symbol: string; side: Side; quantity: number }) => {
  const res = await fetch(`${BASE_URL}/trade`, {
    method: "POST", headers: HEADERS,
    body: JSON.stringify(payload),
  })
  if (!res.ok) throw new Error(`Trade API failed (${res.status})`)
  return res.json()
}

export const fetchRiskAlerts = async (symbol: string) => {
  const res = await fetch(`${BASE_URL}/risk-alerts?symbol=${encodeURIComponent(symbol)}`, {
    headers: GET_HEADERS,
  })
  if (!res.ok) throw new Error(`Risk alert API failed (${res.status})`)
  return (await res.json()) as { alerts: string[] }
}

export const startAutoTrade = async (cfg: import("./types").AutoTradeConfig) => {
  const res = await fetch(`${BASE_URL}/autotrade/start`, {
    method: "POST", headers: HEADERS,
    body: JSON.stringify(cfg),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }))
    throw new Error(err.detail ?? `AutoTrade start failed (${res.status})`)
  }
  return res.json() as Promise<{ ok: boolean; running: boolean; sessionId: string }>
}

export const stopAutoTrade = async (sessionId?: string) => {
  const res = await fetch(`${BASE_URL}/autotrade/stop`, {
    method: "POST", headers: HEADERS,
    body: JSON.stringify({ sessionId: sessionId ?? null, force: false }),
  })
  if (!res.ok) throw new Error(`AutoTrade stop failed (${res.status})`)
  return res.json() as Promise<{ ok: boolean; running: boolean }>
}

export const resetAutoTrade = async () => {
  const res = await fetch(`${BASE_URL}/autotrade/reset`, {
    method: "POST", headers: HEADERS,
    body: JSON.stringify({ force: true }),
  })
  if (!res.ok) throw new Error(`AutoTrade reset failed (${res.status})`)
  return res.json()
}

export const fetchAutoTradeStatus = async (symbol?: string): Promise<import("./types").AutoTradeStatus> => {
  const url = symbol
    ? `${BASE_URL}/autotrade/status?symbol=${encodeURIComponent(symbol)}`
    : `${BASE_URL}/autotrade/status`
  const res = await fetch(url, { headers: GET_HEADERS })
  if (!res.ok) throw new Error(`AutoTrade status failed (${res.status})`)
  return res.json()
}
