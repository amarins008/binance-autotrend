import sys
sys.path.insert(0, '.')
from main import _scan_timeout_budget_sec, SCAN_ANALYZE_CONCURRENCY

for name, cfg in [
    ('LIVE default (interval=20)', {'intervalSec':20, 'scanPerSymbolTimeoutSec':5.0, 'scanAnalyzeTop':7, 'scanGuardedFallbackAnalyzeTop':9, 'scanTopLiquid':45, 'scanFallbackRetrySymbols':3}),
    ('perf_patch.json values',     {'intervalSec':15, 'scanPerSymbolTimeoutSec':5.0, 'scanAnalyzeTop':7, 'scanGuardedFallbackAnalyzeTop':9, 'scanTopLiquid':45, 'scanFallbackRetrySymbols':3}),
    ('LIVE conservative 12s',      {'intervalSec':20, 'scanPerSymbolTimeoutSec':12.0, 'scanAnalyzeTop':7, 'scanGuardedFallbackAnalyzeTop':9, 'scanTopLiquid':45, 'scanFallbackRetrySymbols':3}),
    ('Bare bones legacy',          {'intervalSec':20, 'scanAnalyzeTop':8, 'scanTopLiquid':30, 'scanFallbackRetrySymbols':3}),
]:
    print(f'--- {name} ---')
    pst = cfg.get('scanPerSymbolTimeoutSec', 12.0)
    at = cfg.get('scanAnalyzeTop', 8)
    print(f'  scanAnalyzeTop={at}, per_symbol={pst}, concurrency={SCAN_ANALYZE_CONCURRENCY}')
    print(f'  budget={_scan_timeout_budget_sec(cfg):.1f}s')