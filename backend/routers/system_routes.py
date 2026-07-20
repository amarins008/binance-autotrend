
import asyncio
from fastapi import APIRouter

# Lazy import to break the circular dependency with main.py.
def _lazy_main():
    import main as _m
    return _m

router = APIRouter()

def _route_getter(name: str):
    _m = _lazy_main()
    return getattr(_m, name)

def _sync_handler(name: str):
    def _handler():
        return _route_getter(name)()
    return _handler

def _async_handler(name: str):
    async def _handler():
        fn = _route_getter(name)
        result = fn()
        if asyncio.iscoroutine(result):
            return await result
        return result
    return _handler

router.add_api_route('/health', _sync_handler('health'), methods=['GET'])
router.add_api_route('/system/restart', _async_handler('system_restart'), methods=['POST'])
router.add_api_route('/debug/env-status', _sync_handler('debug_env_status'), methods=['GET'])
router.add_api_route('/debug/binance-auth-check', _async_handler('debug_binance_auth_check'), methods=['GET'])
