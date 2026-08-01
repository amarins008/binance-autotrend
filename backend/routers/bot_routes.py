"""Bot control & service endpoints router.

Route registration only — handler implementations stay in main.py to avoid
touching their logic. ``register()`` is called at the bottom of main.py.
"""

from fastapi import APIRouter


def register(_m) -> APIRouter:
    router = APIRouter()
    router.add_api_route(
        "/bot/backfill-vault-trades", _m.autotrade_backfill_vault_trades, methods=["POST"]
    )
    router.add_api_route("/bot/start", _m.bot_start, methods=["POST"])
    router.add_api_route("/bot/stop", _m.bot_stop, methods=["POST"])
    router.add_api_route("/bot/config", _m.bot_config, methods=["POST"])
    router.add_api_route("/service/start", _m.service_start, methods=["POST"])
    router.add_api_route("/service/stop", _m.service_stop, methods=["POST"])
    router.add_api_route("/bot/precheck-live", _m.bot_precheck_live, methods=["GET"])
    return router
