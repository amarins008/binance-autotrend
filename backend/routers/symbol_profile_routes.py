from fastapi import APIRouter, Body

# Lazy import to break the circular dependency with main.py.
def _lazy_main():
    import main as _m
    return _m


router = APIRouter()


def _route_getter(name: str):
    _m = _lazy_main()
    return getattr(_m, name)


def hermes_get_symbol_profile_route(symbol: str):
    return _route_getter("hermes_get_symbol_profile")(symbol)


def hermes_set_symbol_profile_route(payload: dict = Body(default_factory=dict)):
    return _route_getter("hermes_set_symbol_profile")(payload)


def hermes_list_symbol_profiles_route():
    return _route_getter("hermes_list_symbol_profiles")()


router.add_api_route("/hermes/symbol/profile", hermes_get_symbol_profile_route, methods=["GET"])
router.add_api_route("/hermes/symbol/profile", hermes_set_symbol_profile_route, methods=["POST"])
router.add_api_route("/hermes/symbol/profiles", hermes_list_symbol_profiles_route, methods=["GET"])
