import type { OrderBookLevel, OrderBookSummary } from "./types"

const toLevels = (rows: string[][]): OrderBookLevel[] =>
  rows.map(([price, qty]) => ({
    price: Number(price),
    qty: Number(qty)
  }))

const findWall = (levels: OrderBookLevel[]) => {
  if (!levels.length) return null
  return levels.reduce((max, level) => (level.qty > max.qty ? level : max))
}

/** Detect iceberg: cluster of orders at same price level >> average */
const detectIceberg = (levels: OrderBookLevel[]): boolean => {
  if (levels.length < 5) return false
  const clusters: Record<number, number> = {}
  for (const l of levels) {
    const px = Math.round(l.price * 100) / 100
    clusters[px] = (clusters[px] ?? 0) + l.qty
  }
  const vals = Object.values(clusters)
  const avg = vals.reduce((a, b) => a + b, 0) / vals.length
  return Math.max(...vals) > avg * 8
}

/** Cumulative Volume Delta proxy from order book depth */
const calcCvdProxy = (bids: OrderBookLevel[], asks: OrderBookLevel[]): number => {
  const bidTop = bids.slice(0, 10).reduce((s, l) => s + l.qty, 0)
  const askTop = asks.slice(0, 10).reduce((s, l) => s + l.qty, 0)
  const total = bidTop + askTop
  return total > 0 ? (bidTop - askTop) / total : 0
}

export const fetchOrderBookSummary = async (
  symbol: string
): Promise<OrderBookSummary> => {
  const normalized = symbol.toUpperCase()
  // Use 100 levels for better wall/iceberg detection
  const url = `https://fapi.binance.com/fapi/v1/depth?symbol=${normalized}&limit=100`
  const res = await fetch(url)

  if (!res.ok) {
    throw new Error(`Order book request failed (${res.status})`)
  }

  const data = (await res.json()) as { bids: string[][]; asks: string[][] }
  const bids = toLevels(data.bids)
  const asks = toLevels(data.asks)

  const bidNotional = bids.reduce((acc, l) => acc + l.price * l.qty, 0)
  const askNotional = asks.reduce((acc, l) => acc + l.price * l.qty, 0)
  const imbalance = (bidNotional - askNotional) / Math.max(bidNotional + askNotional, 1)

  const buyWall = findWall(bids)
  const sellWall = findWall(asks)

  const wallRatio =
    buyWall && sellWall
      ? Math.max(
          buyWall.qty / Math.max(sellWall.qty, 1e-9),
          sellWall.qty / Math.max(buyWall.qty, 1e-9)
        )
      : 1

  const icebergDetected = detectIceberg(bids) || detectIceberg(asks)
  const cvdProxy = calcCvdProxy(bids, asks)

  // Spoofing: large wall ratio OR iceberg detected
  const spoofingRisk =
    wallRatio > 5 || icebergDetected ? "HIGH" : wallRatio > 3 ? "MEDIUM" : "LOW"

  return {
    symbol: normalized,
    bidNotional,
    askNotional,
    imbalance,
    buyWall,
    sellWall,
    spoofingRisk,
    cvdProxy,
    icebergDetected,
  }
}
