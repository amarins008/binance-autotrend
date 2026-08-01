"""Hermes symbol-profile & supervisor endpoints router.

Route registration only — handler implementations stay in main.py to avoid
touching their logic. ``register()`` is called at the bottom of main.py.
"""

from fastapi import APIRouter


def register(_m) -> APIRouter:
    router = APIRouter()
    router.add_api_route("/hermes/symbol/profile", _m.hermes_get_symbol_profile, methods=["GET"])
    router.add_api_route("/hermes/symbol/profile", _m.hermes_set_symbol_profile, methods=["POST"])
    router.add_api_route("/hermes/symbol/profiles", _m.hermes_list_symbol_profiles, methods=["GET"])
    router.add_api_route("/hermes/supervisor-review", _m.hermes_supervisor_review, methods=["GET"])
    router.add_api_route(
        "/hermes/supervisor/external-signal", _m.hermes_supervisor_external_signal, methods=["POST"]
    )
    return router
