"""Misc dashboard/status/debug endpoints router.

Route registration only — handler implementations stay in main.py to avoid
touching their logic. ``register()`` is called at the bottom of main.py.
"""

from fastapi import APIRouter


def register(_m) -> APIRouter:
    router = APIRouter()
    router.add_api_route("/debug/binance-positions", _m.debug_binance_positions, methods=["GET"])
    router.add_api_route("/debug/direction-bias", _m.debug_direction_bias, methods=["GET"])
    router.add_api_route("/status", _m.combined_status, methods=["GET"])
    router.add_api_route("/status/quick", _m.combined_status, methods=["GET"])
    router.add_api_route("/api/ip-info", _m.api_ip_info, methods=["GET"])
    return router
