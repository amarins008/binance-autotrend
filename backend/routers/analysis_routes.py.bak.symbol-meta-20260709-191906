import asyncio

from fastapi import APIRouter

# Lazy imports to break the circular dependency with main.py.
# main.py imports this router at the bottom of its module after everything
# is defined. Importing intel_analyze, analyze etc. at the top level would
# cause a circular-import error because main.py has not finished loading yet.
def _lazy_main():
    import main as _m
    return _m


from schemas import CoinRankRequest

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


router.add_api_route('/risk-config', lambda: _route_getter('get_risk_config')(), methods=['GET'])
router.add_api_route('/symbol-meta', lambda: _route_getter('symbol_meta')(), methods=['GET'])
router.add_api_route('/risk-config', lambda: _route_getter('set_risk_config')(), methods=['POST'])
router.add_api_route('/analyze', lambda: _route_getter('analyze')(), methods=['POST'])
router.add_api_route('/analyze-vision', lambda: _route_getter('analyze_vision')(), methods=['POST'])
router.add_api_route('/intel/analyze', lambda: _route_getter('intel_analyze')(), methods=['POST'])
router.add_api_route('/intel/rank', rank_coins, methods=['POST'])
router.add_api_route('/risk-alerts', lambda: _route_getter('risk_alerts')(), methods=['GET'])
router.add_api_route('/strategy/parse', lambda: _route_getter('parse_strategy')(), methods=['POST'])
