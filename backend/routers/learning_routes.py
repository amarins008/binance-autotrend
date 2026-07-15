from fastapi import APIRouter

# Lazy import to break the circular dependency with main.py.
def _lazy_main():
    import main as _m
    return _m


router = APIRouter()


def _route_getter(name: str):
    _m = _lazy_main()
    return getattr(_m, name)


def learning_status_route(symbol: str | None = None):
    """Wrapper around main.learning_status with graceful degradation.

    A failure here used to surface as a 500 to the dashboard, which then
    rendered as a misleading "learning error" toast. Now we catch any
    exception (file I/O, missing profiles, aggregation bugs) and return
    a safe empty response with an `error` field so the UI can show
    "learning data temporarily unavailable" instead of crashing.
    """
    try:
        return _route_getter("learning_status")(symbol)
    except Exception as exc:
        # Keep the error short — the dashboard just shows a banner.
        err_msg = str(exc)[:200] or exc.__class__.__name__
        print(f"[learning_routes] learning_status failed: {err_msg}")
        if symbol:
            return {
                "symbol": str(symbol).upper(),
                "profile": {"wins": 0, "losses": 0, "trades": 0, "realizedPnl": 0.0},
                "winRatePct": 0.0,
                "adaptiveMinConf": 0.62,
                "source": "unavailable",
                "error": err_msg,
            }
        return {
            "items": [],
            "vaultDir": "",
            "source": "unavailable",
            "error": err_msg,
        }


def learning_propose_config_route(symbol: str | None = None):
    return _route_getter("learning_propose_config")(symbol)


def learning_walk_forward_route(
    symbol: str,
    mode: str = "ALL",
    train_size: int = 30,
    test_size: int = 10,
):
    return _route_getter("learning_walk_forward")(
        symbol=symbol,
        mode=mode,
        train_size=train_size,
        test_size=test_size,
    )


async def learning_train_now_route():
    return await _route_getter("learning_train_now")()


router.add_api_route("/learning/status", learning_status_route, methods=["GET"])
router.add_api_route("/learning/propose-config", learning_propose_config_route, methods=["GET"])
router.add_api_route("/learning/walk-forward", learning_walk_forward_route, methods=["GET"])
router.add_api_route("/learning/train-now", learning_train_now_route, methods=["POST"])
