import asyncio
import math

from fastapi import APIRouter

# Lazy imports to break the circular dependency with main.py.
# main.py imports this router at the bottom of its module after everything
# is defined. Importing intel_analyze, analyze etc. at the top level would
# cause a circular-import error because main.py has not finished loading yet.
def _lazy_main():
    import main as _m
    return _m


from schemas import CoinRankRequest, RiskConfig

router = APIRouter()


def _rank_row(symbol: str, intel: dict, position_order: int) -> dict:
    _m = _lazy_main()
    signal = str((intel or {}).get("signal", "WAIT")).upper()
    confidence = float((intel or {}).get("confidence", 0.0) or 0.0)
    execution = intel.get("execution") if isinstance((intel or {}).get("execution"), dict) else {}
    momentum_pct = abs(float(execution.get("momentumPct", 0.0) or 0.0))
    spread_bps = float(execution.get("spreadBps", 0.0) or 0.0)
    score = _m._intel_score(symbol, intel)
    return {
        "positionOrder": position_order,
        "symbol": symbol,
        "signal": signal,
        "entrySide": signal if signal in ("LONG", "SHORT") else "WAIT",
        "confidence": round(confidence, 4),
        "score": round(score, 6),
        "profitBias": round(max(0.0, min(1.0, score)), 4),
        "accuracyBias": round(max(0.0, min(1.0, confidence)), 4),
        "momentumPct": round(momentum_pct, 4),
        "spreadBps": round(spread_bps, 4),
    }


async def rank_coins(req: CoinRankRequest):
    _m = _lazy_main()
    if req.symbols:
        candidates: list[str] = []
        seen: set[str] = set()
        for raw_symbol in req.symbols:
            try:
                symbol = _m._normalize_symbol(str(raw_symbol).strip())
            except Exception:
                continue
            if symbol in seen:
                continue
            seen.add(symbol)
            candidates.append(symbol)
        if not candidates:
            return {
                "ok": True,
                "source": "symbols",
                "bestSymbol": None,
                "bestSignal": None,
                "ranked": [],
                "positionOrder": [],
            }

        results = await asyncio.gather(
            *[_m.intel_analyze(_m.IntelAnalyzeRequest(symbol=symbol)) for symbol in candidates],
            return_exceptions=True,
        )

        ranked: list[dict] = []
        best_symbol = None
        best_signal = None
        best_score = -999.0
        for symbol, outcome in zip(candidates, results):
            if isinstance(outcome, Exception) or not isinstance(outcome, dict):
                continue
            score = _m._intel_score(symbol, outcome)
            ranked.append(_rank_row(symbol, outcome, len(ranked) + 1))
            signal = str(outcome.get("signal", "WAIT")).upper()
            if signal in ("LONG", "SHORT") and score > best_score:
                best_score = score
                best_symbol = symbol
                best_signal = signal

        ranked.sort(key=lambda item: item["score"], reverse=True)
        for index, item in enumerate(ranked, start=1):
            item["positionOrder"] = index
        return {
            "ok": True,
            "source": "symbols",
            "bestSymbol": best_symbol,
            "bestSignal": best_signal,
            "ranked": ranked[: req.topN],
            "positionOrder": [item["symbol"] for item in ranked[: req.topN]],
        }

    cfg = {
        "scanTopLiquid": req.scanTopLiquid,
        "scanAnalyzeTop": req.scanAnalyzeTop,
        "whitelistSymbols": req.whitelistSymbols,
    }
    best_symbol, best_intel, board = await _m._pick_best_symbol_from_scan(cfg)
    ranked: list[dict] = []
    sorted_board = sorted(
        board,
        key=lambda row: float(row.get("score", 0.0) or 0.0),
        reverse=True,
    )
    for index, row in enumerate(sorted_board[: req.topN], start=1):
        item = dict(row)
        item["positionOrder"] = index
        item["entrySide"] = item["signal"] if item["signal"] in ("LONG", "SHORT") else "WAIT"
        item["profitBias"] = round(max(0.0, min(1.0, float(item.get("score", 0.0) or 0.0))), 4)
        item["accuracyBias"] = round(max(0.0, min(1.0, float(item.get("confidence", 0.0) or 0.0))), 4)
        ranked.append(item)

    best_signal = None
    if isinstance(best_intel, dict):
        best_signal = str(best_intel.get("signal", "WAIT")).upper()
    return {
        "ok": True,
        "source": "market-scan",
        "bestSymbol": best_symbol,
        "bestSignal": best_signal,
        "ranked": ranked,
        "positionOrder": [item["symbol"] for item in ranked if item["signal"] in ("LONG", "SHORT")],
    }


def _route_getter(name: str):
    """Lazily resolve a function from main.py when the route is actually called."""
    _m = _lazy_main()
    return getattr(_m, name)


def _bucket_key(trade: dict, field: str):
    """Map a trade to its accuracy-bucket key for one entry-condition field."""
    try:
        pnl = float(trade.get("pnl", 0.0) or 0.0)
    except Exception:
        return None
    if field == "side":
        return str(trade.get("side", "") or "n/a").upper()
    if field == "tvSignal":
        raw = (trade.get("tvAtEntry") or trade.get("tvSignal") or "")
        if not raw:
            raw = trade.get("tvAtEntryConfidence") is not None and trade.get("tv") or ""
        sig = str(raw or "").upper()
        return sig if sig else "n/a"
    if field == "tvStrength":
        v = trade.get("tvStrength")
        try:
            v = float(v)
        except (TypeError, ValueError):
            return "n/a"
        return ">=0.90" if v >= 0.90 else "0.45-0.89" if v >= 0.45 else "<0.45"
    if field == "tvConfidence":
        v = (trade.get("tvConfidence") if trade.get("tvConfidence") is not None
             else trade.get("tvAtEntryConfidence"))
        try:
            v = float(v)
        except (TypeError, ValueError):
            return "n/a"
        return ">=0.80" if v >= 0.80 else "0.60-0.79" if v >= 0.60 else "<0.60"
    if field == "entryConfidence":
        v = trade.get("entryConfidence")
        try:
            v = float(v)
        except (TypeError, ValueError):
            return "n/a"
        return ">=0.85" if v >= 0.85 else "0.72-0.84" if v >= 0.72 else "<0.72"
    if field == "momentum":
        v = trade.get("entryMomentumPct")
        try:
            v = float(v)
        except (TypeError, ValueError):
            return "n/a"
        return ">" if v > 0.0 else "<" if v < 0.0 else "0"
    if field == "patternBias":
        v = trade.get("patternBias")
        try:
            v = float(v)
        except (TypeError, ValueError):
            return "n/a"
        return "pos" if v > 0.0001 else "neg" if v < -0.0001 else "0"
    if field == "directionBias":
        raw = str(trade.get("entryDirectionBias", "") or "").upper()
        return raw if raw else "n/a"
    return "n/a"


def _accuracy_bucket(trades: list, field: str, side_filter: str | None = None):
    """Aggregate win-rate + pnl per bucket for one entry-condition field."""
    buckets: dict[str, dict] = {}
    for t in trades:
        if side_filter:
            side = str(t.get("side", "") or "").upper()
            if side != side_filter:
                continue
        key = _bucket_key(t, field)
        if not key:
            continue
        try:
            pnl = float(t.get("pnl", 0.0) or 0.0)
        except Exception:
            continue
        b = buckets.setdefault(key, {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0})
        b["trades"] += 1
        b["pnl"] += pnl
        if pnl >= 0:
            b["wins"] += 1
        else:
            b["losses"] += 1
    rows = []
    for key, b in buckets.items():
        rows.append({
            "key": key,
            "trades": b["trades"],
            "wins": b["wins"],
            "losses": b["losses"],
            "winRatePct": round(100 * b["wins"] / b["trades"], 1) if b["trades"] else 0.0,
            "pnl": round(b["pnl"], 4),
        })
    order = {"n/a": -9, "0": 0, "pos": 1, "neg": 2, ">": 1, "<": 2}
    rows.sort(key=lambda r: (order.get(r["key"], 99), -r["trades"]))
    return rows


def entry_accuracy():
    """Entry-condition accuracy: win-rate + pnl grouped by what was known at entry time.

    Reads the LIVE trade log (last 90 days) and buckets realized results by the
    TV signal, TV strength, TV confidence, entry confidence, momentum, pattern
    bias — so the operator can see which entry conditions actually produced
    accurate fills.
    """
    _m = _lazy_main()
    trades = list(_m._live_closed_trades_from_log())
    total = len(trades)
    wins = sum(1 for t in trades if float(t.get("pnl", 0.0) or 0.0) >= 0)
    total_pnl = round(sum(float(t.get("pnl", 0.0) or 0.0) for t in trades), 4)
    return {
        "ok": True,
        "totalTrades": total,
        "wins": wins,
        "losses": total - wins,
        "winRatePct": round(100 * wins / total, 1) if total else 0.0,
        "totalPnl": total_pnl,
        "avgPnl": round(total_pnl / total, 4) if total else 0.0,
        "bucketTvSignal": _accuracy_bucket(trades, "tvSignal"),
        "bucketTvStrength": _accuracy_bucket(trades, "tvStrength"),
        "bucketTvConfidence": _accuracy_bucket(trades, "tvConfidence"),
        "bucketEntryConfidence": _accuracy_bucket(trades, "entryConfidence"),
        "bucketMomentum": _accuracy_bucket(trades, "momentum"),
        "bucketPatternBias": _accuracy_bucket(trades, "patternBias"),
        "bucketDirectionBias": _accuracy_bucket(trades, "directionBias"),
        "bucketSideLong": _accuracy_bucket(trades, "tvSignal", "LONG"),
        "bucketSideShort": _accuracy_bucket(trades, "tvSignal", "SHORT"),
    }


router.add_api_route('/entry-accuracy', entry_accuracy, methods=['GET'])


router.add_api_route('/risk-config', lambda: _route_getter('get_risk_config')(), methods=['GET'])
router.add_api_route('/symbol-meta', lambda: _route_getter('symbol_meta')(), methods=['GET'])


def _set_risk_config(req: RiskConfig):
    return _route_getter('set_risk_config')(req)


router.add_api_route('/risk-config', _set_risk_config, methods=['POST'])
router.add_api_route('/analyze', lambda: _route_getter('analyze')(), methods=['POST'])
router.add_api_route('/analyze-vision', lambda: _route_getter('analyze_vision')(), methods=['POST'])
router.add_api_route('/intel/analyze', lambda: _route_getter('intel_analyze')(), methods=['POST'])
router.add_api_route('/intel/rank', rank_coins, methods=['POST'])
router.add_api_route('/risk-alerts', lambda: _route_getter('risk_alerts')(), methods=['GET'])
router.add_api_route('/strategy/parse', lambda: _route_getter('parse_strategy')(), methods=['POST'])
