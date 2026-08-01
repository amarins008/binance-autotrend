"""Trading & autotrade endpoints router.

Route registration only — handler implementations stay in main.py to avoid
touching their logic. ``register()`` is called at the bottom of main.py
(after every handler is defined) so the handlers can be passed by reference
with their original signatures (query params, Body, etc. keep working).
"""

from fastapi import APIRouter


def register(_m) -> APIRouter:
    router = APIRouter()
    router.add_api_route("/trade", _m.trade, methods=["POST"])
    router.add_api_route("/autotrade/config", _m.autotrade_update_config, methods=["POST"])
    router.add_api_route("/autotrade/adopt-live", _m.autotrade_adopt_live, methods=["POST"])
    router.add_api_route("/autotrade/close-orphan", _m.autotrade_close_orphan, methods=["POST"])
    router.add_api_route("/autotrade/update-sl-tp", _m.autotrade_update_sl_tp, methods=["POST"])
    router.add_api_route("/autotrade/start", _m.autotrade_start, methods=["POST"])
    router.add_api_route("/autotrade/stop", _m.autotrade_stop, methods=["POST"])
    router.add_api_route("/autotrade/reset", _m.autotrade_reset, methods=["POST"])
    router.add_api_route("/autotrade/status", _m.autotrade_status, methods=["GET"])
    router.add_api_route("/autotrade/status-lite", _m.autotrade_status_lite, methods=["GET"])
    return router
