import json
import time
import urllib.error
import urllib.request


HERMES = "http://127.0.0.1:8020"
MCP = "http://127.0.0.1:8877/mcp"
DEFAULT_DENY_SYMBOLS = {"XAUUSDT", "XAGUSDT", "SPCXUSDT", "CLUSDT", "MRVLUSDT", "INTCUSDT"}


def _http_json(method, url, payload=None, headers=None, timeout=30):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"content-type": "application/json", **(headers or {})},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        return resp.headers, raw


def _mcp_post(payload, session=None, timeout=45):
    headers = {"accept": "application/json, text/event-stream"}
    if session:
        headers["mcp-session-id"] = session
    hdrs, raw = _http_json("POST", MCP, payload, headers=headers, timeout=timeout)
    return hdrs.get("mcp-session-id"), raw


def _event_json(raw):
    for line in raw.splitlines():
        if line.startswith("data: "):
            return json.loads(line[6:])
    return json.loads(raw)


def _mcp_call(tool, arguments):
    session, raw = _mcp_post(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "hermes-tv-sidecar", "version": "1.0"},
            },
        },
        timeout=20,
    )
    if not session:
        raise RuntimeError("MCP session missing")
    _mcp_post({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}, session, timeout=10)
    _, raw = _mcp_post(
        {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": tool, "arguments": arguments}},
        session,
        timeout=60,
    )
    data = _event_json(raw)
    result = data.get("result") if isinstance(data, dict) else {}
    content = result.get("content") if isinstance(result, dict) else []
    if content and isinstance(content[0], dict):
        text = content[0].get("text", "")
        try:
            return json.loads(text)
        except Exception:
            return {"raw": text}
    return result


def _status():
    _, raw = _http_json("GET", f"{HERMES}/autotrade/status-lite", timeout=45)
    return json.loads(raw)


def _signal_from_analysis(analysis):
    price = analysis.get("price_data") if isinstance(analysis.get("price_data"), dict) else {}
    rsi = analysis.get("rsi") if isinstance(analysis.get("rsi"), dict) else {}
    macd = analysis.get("macd") if isinstance(analysis.get("macd"), dict) else {}
    ema = analysis.get("ema") if isinstance(analysis.get("ema"), dict) else {}
    tf = analysis.get("timeframe_context") if isinstance(analysis.get("timeframe_context"), dict) else {}
    signals = [str(x).lower() for x in ema.get("signals", []) if isinstance(x, str)]
    score = 0
    if str(tf.get("bias", "")).lower().startswith("bull"):
        score += 1
    if str(tf.get("bias", "")).lower().startswith("bear"):
        score -= 1
    if "bullish" in str(rsi.get("signal", "")).lower():
        score += 1
    if "bearish" in str(rsi.get("signal", "")).lower():
        score -= 1
    if "bullish" in str(macd.get("crossover", "")).lower():
        score += 1
    if "bearish" in str(macd.get("crossover", "")).lower():
        score -= 1
    if any("price below ema20" in s or "price below ema50" in s for s in signals):
        score -= 1
    if any("price above ema20" in s or "price above ema50" in s for s in signals):
        score += 1
    status = "MIXED/RANGING"
    if score >= 2:
        status = "MOSTLY BULLISH"
    elif score <= -2:
        status = "MOSTLY BEARISH"
    return {
        "status": status,
        "score": score,
        "price": price.get("current_price"),
        "bias": tf.get("bias"),
        "rsi": rsi.get("value"),
        "macd": macd.get("crossover"),
    }


def _deny_symbols(status):
    cfg = status.get("config") if isinstance(status.get("config"), dict) else {}
    rows = cfg.get("scanDenySymbols")
    if not isinstance(rows, list):
        return set(DEFAULT_DENY_SYMBOLS)
    return {str(x or "").upper().replace("/", "").strip() for x in rows if str(x or "").strip()}


def _targets_from_status(status):
    denied = _deny_symbols(status)
    targets = []

    def add(symbol, side, target):
        sym = str(symbol or "").upper().replace("/", "").strip()
        sd = str(side or "").upper().strip()
        if not sym or sd not in {"LONG", "SHORT"} or sym in denied:
            return
        item = {"symbol": sym, "side": sd, "target": target}
        if not any(x["symbol"] == sym and x["side"] == sd and x["target"] == target for x in targets):
            targets.append(item)

    open_rows = status.get("openLivePositions") if isinstance(status.get("openLivePositions"), list) else []
    for row in open_rows:
        if isinstance(row, dict):
            add(row.get("symbol"), row.get("side"), "open_position")

    last = status.get("lastDecision") if isinstance(status.get("lastDecision"), dict) else {}
    intel = last.get("intel") if isinstance(last.get("intel"), dict) else {}
    add(last.get("symbol"), intel.get("signal") or last.get("side"), "pending_entry")
    return targets


def _submit(symbol, side, target, signal):
    aligned = (side == "LONG" and signal["score"] >= 2) or (side == "SHORT" and signal["score"] <= -2)
    contradicted = (side == "LONG" and signal["score"] <= -2) or (side == "SHORT" and signal["score"] >= 2)
    condition = "direction_context"
    severity = "medium"
    action = "Use MarketContext sidecar context as confirmation only."
    if contradicted:
        condition = "strong_mtf_contradiction"
        severity = "high"
        action = "Block or close/reduce this side until alignment recovers."
    elif aligned:
        condition = "confirmed_direction"
        severity = "low"
        action = "Allow only if internal Hermes gates also pass."
    payload = {
        "source": "marketcontext_mcp_sidecar",
        "findings": [
            {
                "symbol": symbol,
                "side": side,
                "target": target,
                "condition": condition,
                "severity": severity,
                "marketContextSignal": json.dumps(signal, ensure_ascii=False),
                "alignment": signal["status"],
                "hermesState": "MarketContext MCP sidecar healthy; internal watcher bridge bypass active",
                "recommendedAction": action,
            }
        ],
    }
    _http_json("POST", f"{HERMES}/hermes/supervisor/external-signal", payload, timeout=20)


def _submit_failure(symbol, side, target, message):
    msg = str(message)
    low = msg.lower()
    unsupported = any(
        token in low
        for token in ("no data found", "not found for", "symbol not found", "invalid symbol", "no candles")
    )
    payload = {
        "source": "marketcontext_mcp_sidecar",
        "findings": [
            {
                "symbol": symbol,
                "side": side,
                "target": target,
                "condition": "mcp_unsupported_symbol" if unsupported else "mcp_data_failure",
                "severity": "medium" if unsupported else "high",
                "marketContextSignal": msg[:500],
                "alignment": "UNKNOWN",
                "hermesState": (
                    "MarketContext MCP has no data for this symbol"
                    if unsupported
                    else "MarketContext MCP sidecar failed while checking target"
                ),
                "recommendedAction": (
                    "Skip this symbol in MarketContext-gated scans."
                    if unsupported
                    else "Pause or down-rank this symbol until MarketContext context recovers."
                ),
            }
        ],
    }
    _http_json("POST", f"{HERMES}/hermes/supervisor/external-signal", payload, timeout=20)


def main():
    while True:
        try:
            status = _status()
            for item in _targets_from_status(status):
                symbol = item["symbol"]
                side = item["side"]
                target = item["target"]
                try:
                    analysis = _mcp_call(
                        "coin_analysis",
                        {"exchange": "BINANCE", "symbol": symbol, "timeframe": "1h"},
                    )
                    signal = _signal_from_analysis(analysis)
                    _submit(symbol, side, target, signal)
                except Exception as exc:
                    try:
                        _submit_failure(symbol, side, target, exc)
                    except Exception:
                        pass
        except (urllib.error.URLError, TimeoutError, Exception):
            pass
        time.sleep(45)


if __name__ == "__main__":
    main()
