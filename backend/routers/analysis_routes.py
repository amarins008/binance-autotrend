import asyncio

from fastapi import APIRouter

from main import (
    AnalyzeRequest,
    IntelAnalyzeRequest,
    _intel_score,
    _normalize_symbol,
    _pick_best_symbol_from_scan,
    analyze,
    analyze_vision,
    intel_analyze,
    parse_strategy,
    risk_alerts,
    set_risk_config,
    symbol_meta,
    get_risk_config,
)
from schemas import CoinRankRequest

router = APIRouter()


def _rank_row(symbol: str, intel: dict, position_order: int) -> dict:
    signal = str((intel or {}).get("signal", "WAIT")).upper()
    confidence = float((intel or {}).get("confidence", 0.0) or 0.0)
    execution = intel.get("execution") if isinstance((intel or {}).get("execution"), dict) else {}
    momentum_pct = abs(float(execution.get("momentumPct", 0.0) or 0.0))
    spread_bps = float(execution.get("spreadBps", 0.0) or 0.0)
    score = _intel_score(symbol, intel)
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
    if req.symbols:
        candidates: list[str] = []
        seen: set[str] = set()
        for raw_symbol in req.symbols:
            try:
                symbol = _normalize_symbol(str(raw_symbol).strip())
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
            *[intel_analyze(IntelAnalyzeRequest(symbol=symbol)) for symbol in candidates],
            return_exceptions=True,
        )

        ranked: list[dict] = []
        best_symbol = None
        best_signal = None
        best_score = -999.0
        for symbol, outcome in zip(candidates, results):
            if isinstance(outcome, Exception) or not isinstance(outcome, dict):
                continue
            score = _intel_score(symbol, outcome)
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
    best_symbol, best_intel, board = await _pick_best_symbol_from_scan(cfg)
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


router.add_api_route('/risk-config', get_risk_config, methods=['GET'])
router.add_api_route('/symbol-meta', symbol_meta, methods=['GET'])
router.add_api_route('/risk-config', set_risk_config, methods=['POST'])
router.add_api_route('/analyze', analyze, methods=['POST'])
router.add_api_route('/analyze-vision', analyze_vision, methods=['POST'])
router.add_api_route('/intel/analyze', intel_analyze, methods=['POST'])
router.add_api_route('/intel/rank', rank_coins, methods=['POST'])
router.add_api_route('/risk-alerts', risk_alerts, methods=['GET'])
router.add_api_route('/strategy/parse', parse_strategy, methods=['POST'])
