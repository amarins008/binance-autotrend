
from fastapi import APIRouter

from main import debug_binance_auth_check, debug_env_status, health, system_restart

router = APIRouter()
router.add_api_route('/health', health, methods=['GET'])
router.add_api_route('/system/restart', system_restart, methods=['POST'])
router.add_api_route('/debug/env-status', debug_env_status, methods=['GET'])
router.add_api_route('/debug/binance-auth-check', debug_binance_auth_check, methods=['GET'])
