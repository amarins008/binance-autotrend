
from fastapi import APIRouter

# Lazy import to break the circular dependency with main.py.
def _lazy_main():
    import main as _m
    return _m

router = APIRouter()

def _route_getter(name: str):
    _m = _lazy_main()
    return getattr(_m, name)

router.add_api_route('/health', lambda: _route_getter('health')(), methods=['GET'])
router.add_api_route('/system/restart', lambda: _route_getter('system_restart')(), methods=['POST'])
router.add_api_route('/debug/env-status', lambda: _route_getter('debug_env_status')(), methods=['GET'])
router.add_api_route('/debug/binance-auth-check', lambda: _route_getter('debug_binance_auth_check')(), methods=['GET'])
