"""Direct TradingView scanner API client (replaces tradingview_ta library).

The legacy `tradingview_ta` v3.3.0 depends on endpoints that TradingView has
since shut down (symbol-search -> 403, technicals JS bundle -> 404), so it
returns HTTP 429 for every call. This client talks directly to the public
scanner API (https://scanner.tradingview.com) which is reachable and returns
the same recommendation data we need for the Guardian TV gate.

Interface mirrors what `tradingview_mcp.TVSignalResult` expects:
  - signal: TVSignal (LONG/SHORT/WAIT)
  - confidence: float
  - timestamp: float
  - metadata: dict with recommendation / oscillators / moving_averages / strength
"""

import time
import json
import urllib.request
import urllib.error
from typing import Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum

try:
    from trading.tradingview_mcp import TVSignal, TVSignalResult
except Exception:  # standalone import safety
    class TVSignal(Enum):
        LONG = "LONG"
        SHORT = "SHORT"
        WAIT = "WAIT"
        ERROR = "ERROR"

    @dataclass
    class TVSignalResult:
        signal: TVSignal
        confidence: float
        timestamp: float
        source: str = "tv-scanner"
        metadata: Dict[str, Any] = None
        _is_stale: bool = False


SCANNER_URL = "https://scanner.tradingview.com/crypto/scan"

# Columns we pull. Recommend.All / Recommend.MA drive the signal; the rest
# feed oscillators / moving_averages metadata for the Guardian gate.
_COLUMNS = [
    "Recommend.All",
    "Recommend.MA",
    "RSI",
    "Stoch.K",
    "Stoch.D",
    "ADX",
    "ATR",
    "close",
    "volume",
    "high",
    "low",
]

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def _recommendation_from_score(score: float) -> str:
    """Map TradingView Recommend.All (-1..1) to a label."""
    if score >= 0.5:
        return "STRONG_BUY"
    if score > 0.1:
        return "BUY"
    if score > -0.1:
        return "NEUTRAL"
    if score > -0.5:
        return "SELL"
    return "STRONG_SELL"


def _signal_from_label(label: str) -> TVSignal:
    if label in ("STRONG_BUY", "BUY"):
        return TVSignal.LONG
    if label in ("STRONG_SELL", "SELL"):
        return TVSignal.SHORT
    return TVSignal.WAIT


def _normalized_symbol(symbol: str) -> str:
    """BINANCE:BTCUSDT is what the scanner expects."""
    s = str(symbol or "").upper().strip()
    if ":" in s:
        return s
    return f"BINANCE:{s}"


def fetch_guidance(symbol: str, timeout: float = 10.0) -> Optional[TVSignalResult]:
    """Fetch TradingView guidance for one symbol via the scanner API.

    Returns TVSignalResult on success, or None on any failure (caller's
    negative-cache / rate-limit layer handles backoff).
    """
    sym = _normalized_symbol(symbol)
    payload = json.dumps({
        "symbols": {"tickers": [sym]},
        "columns": _COLUMNS,
    }).encode("utf-8")
    req = urllib.request.Request(
        SCANNER_URL,
        data=payload,
        headers={"User-Agent": _UA, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            data = json.load(resp)
    except urllib.error.HTTPError as e:
        # 429 / 403 etc — treat as unavailable, let caller back off
        return None
    except Exception:
        return None

    rows = data.get("data") or []
    if not rows or rows[0].get("s") != sym:
        return None
    d = rows[0].get("d") or []
    if len(d) < len(_COLUMNS):
        return None

    rec_all = d[0]
    rec_ma = d[1]
    rsi = d[2]
    stoch_k = d[3]
    stoch_d = d[4]
    adx = d[5]
    close = d[7]

    if rec_all is None:
        return None

    label = _recommendation_from_score(float(rec_all))
    # Strength: combine Recommend.All (trend+oscillator composite) with
    # Recommend.MA (pure moving-average bias). Recommend.All already spans
    # -1..1; map to 0..1 so a clear BUY (rec_all~0.5) reads as a strong signal
    # rather than a weak one. Recommend.MA (often ~0.8) lifts strength further
    # so the Guardian TV gate (min_strength 0.6-0.7) actually fires.
    base = (float(rec_all) + 1.0) / 2.0  # 0..1
    ma = float(rec_ma) if rec_ma is not None else base
    strength = round(max(0.0, min(1.0, 0.6 * base + 0.4 * ma)), 4)
    confidence = round(max(0.0, min(1.0, (float(rec_all) + 1.0) / 2.0)), 4)

    oscillators = {
        "rsi": rsi,
        "stoch_k": stoch_k,
        "stoch_d": stoch_d,
        # Legacy BUY/SELL/NEUTRAL counts expected by tradingview_mcp._fetch_from_tradingview
        "BUY": 0,
        "SELL": 0,
        "NEUTRAL": 0,
    }
    moving_averages = {
        "adx": adx,
        "recommend_ma": rec_ma,
        # Legacy BUY/SELL/NEUTRAL counts expected by tradingview_mcp._fetch_from_tradingview
        "BUY": 0,
        "SELL": 0,
        "NEUTRAL": 0,
    }

    # Derive legacy buy/sell indicator counts from the raw values so the
    # strength computation in tradingview_mcp (osc_buy+ma_buy vs total) works.
    if rsi is not None:
        if rsi < 30:
            oscillators["BUY"] += 1
        elif rsi > 70:
            oscillators["SELL"] += 1
        else:
            oscillators["NEUTRAL"] += 1
    if stoch_k is not None:
        if stoch_k < 20:
            oscillators["BUY"] += 1
        elif stoch_k > 80:
            oscillators["SELL"] += 1
        else:
            oscillators["NEUTRAL"] += 1
    if rec_ma is not None:
        # Recommend.MA spans -1..1; >0.1 leans bullish, <-0.1 bearish
        if float(rec_ma) > 0.1:
            moving_averages["BUY"] += 1
        elif float(rec_ma) < -0.1:
            moving_averages["SELL"] += 1
        else:
            moving_averages["NEUTRAL"] += 1

    return TVSignalResult(
        signal=_signal_from_label(label),
        confidence=confidence,
        timestamp=time.time(),
        source="tv-scanner",
        metadata={
            "recommendation": label,
            "strength": strength,
            "oscillators": oscillators,
            "moving_averages": moving_averages,
            "close": close,
            "rec_all": rec_all,
            "rec_ma": rec_ma,
        },
    )


# --- Thin compatibility shim so callers can swap TA_Handler -> ScannerClient ---

class ScannerClient:
    """Drop-in replacement for the tradingview_ta.TA_Handler usage."""

    def __init__(self, symbol: str, screener: str = "CRYPTO",
                 exchange: str = "BINANCE", interval=None, timeout: float = 10.0):
        self.symbol = symbol
        self.timeout = timeout

    def get_analysis(self):
        """Return a minimal object exposing .summary / .oscillators / .moving_averages
        so existing code paths that read analysis.summary['RECOMMENDATION'] keep working.
        """
        res = fetch_guidance(self.symbol, timeout=self.timeout)
        if not res:
            return None

        class _Analysis:
            pass

        a = _Analysis()
        md = res.metadata or {}
        a.summary = {"RECOMMENDATION": md.get("recommendation", "NEUTRAL")}
        a.oscillators = md.get("oscillators", {})
        a.moving_averages = md.get("moving_averages", {})
        a.signal = res.signal
        a.confidence = res.confidence
        a.timestamp = res.timestamp
        return a


if __name__ == "__main__":
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    r = fetch_guidance(sym)
    print(json.dumps(r.metadata if r else None, indent=2, ensure_ascii=False))
