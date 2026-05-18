
from fastapi import APIRouter

from main import analyze, analyze_vision, intel_analyze, parse_strategy, risk_alerts, set_risk_config, symbol_meta, get_risk_config

router = APIRouter()
router.add_api_route('/risk-config', get_risk_config, methods=['GET'])
router.add_api_route('/symbol-meta', symbol_meta, methods=['GET'])
router.add_api_route('/risk-config', set_risk_config, methods=['POST'])
router.add_api_route('/analyze', analyze, methods=['POST'])
router.add_api_route('/analyze-vision', analyze_vision, methods=['POST'])
router.add_api_route('/intel/analyze', intel_analyze, methods=['POST'])
router.add_api_route('/risk-alerts', risk_alerts, methods=['GET'])
router.add_api_route('/strategy/parse', parse_strategy, methods=['POST'])
