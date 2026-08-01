"""Learning endpoints router.

Route registration only — handler implementations stay in main.py to avoid
touching their logic. ``register()`` is called at the bottom of main.py
(after every handler is defined) so the handlers can be passed by reference
with their original signatures (query params, Body, etc. keep working).
"""

from fastapi import APIRouter


def register(_m) -> APIRouter:
    router = APIRouter()
    router.add_api_route("/learning/status", _m.learning_status, methods=["GET"])
    router.add_api_route(
        "/learning/propose-config", _m.learning_propose_config, methods=["GET"]
    )
    router.add_api_route(
        "/learning/walk-forward", _m.learning_walk_forward, methods=["GET"]
    )
    router.add_api_route("/learning/train-now", _m.learning_train_now, methods=["POST"])
    router.add_api_route("/learning/report", _m.learning_report, methods=["GET"])
    return router
