
from fastapi import APIRouter

# Lazy import to break the circular dependency with main.py.
def _lazy_main():
    import main as _m
    return _m

router = APIRouter()

def _route_getter(name: str):
    _m = _lazy_main()
    return getattr(_m, name)

router.add_api_route('/autotrade/start', lambda: _route_getter('autotrade_start')(), methods=['POST'])
router.add_api_route('/autotrade/stop', lambda: _route_getter('autotrade_stop')(), methods=['POST'])
router.add_api_route('/autotrade/reset', lambda: _route_getter('autotrade_reset')(), methods=['POST'])
router.add_api_route('/autotrade/status', lambda: _route_getter('autotrade_status')(), methods=['GET'])
