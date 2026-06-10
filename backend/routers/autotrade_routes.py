from fastapi import APIRouter

from main import autotrade_reset, autotrade_start, autotrade_status, autotrade_stop

router = APIRouter()
router.add_api_route('/autotrade/start', autotrade_start, methods=['POST'])
router.add_api_route('/autotrade/stop', autotrade_stop, methods=['POST'])
router.add_api_route('/autotrade/reset', autotrade_reset, methods=['POST'])
router.add_api_route('/autotrade/status', autotrade_status, methods=['GET'])
