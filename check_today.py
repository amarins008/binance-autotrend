import sys
sys.path.insert(0, 'backend')
import main
print('TRADES_LOG_PATH:', main.TRADES_LOG_PATH)
print('Cache size before:', len(main._LIVE_STATS_CACHE))
stats = main._aggregate_live_trade_stats_from_log(None)
print('Stats all-symbol:', stats)
stats_sym = main._aggregate_live_trade_stats_from_log('FILUSDT')
print('Stats FILUSDT:', stats_sym)
print('Cache size after:', len(main._LIVE_STATS_CACHE))
print('Cache keys (last 10):')
for k, v in list(main._LIVE_STATS_CACHE.items())[-10:]:
    sym, mtime, size = k
    print(f'  sym={sym!r} mtime={mtime} size={size} -> winsToday={v[1].get("winsToday")}, lossesToday={v[1].get("lossesToday")}, pnlToday={v[1].get("realizedPnlToday")}, wins={v[1].get("wins")}, pnl={v[1].get("realizedPnl")}')