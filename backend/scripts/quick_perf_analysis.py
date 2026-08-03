"""Analyze Binance autotrend trades_log.jsonl — 7d + 24h stats, per-symbol, exit reasons."""
import json, collections

rows = []
with open('obsidian_vault/trades_log.jsonl', encoding='utf-8') as f:
    for l in f:
        l = l.strip()
        if l:
            rows.append(json.loads(l))

live = [r for r in rows if r.get('mode') == 'LIVE']
now = max(r.get('closedAt') or r.get('ts') or 0 for r in live)
print(f'TOTAL {len(rows)} | LIVE {len(live)} | last ts {now}')

def ts(r):
    return r.get('closedAt') or r.get('ts') or 0

def stats(w, label):
    n = len(w)
    if n == 0:
        print(f'--- {label}: 0 ไม้'); return 0.0
    wins = [r['pnl'] for r in w if r['pnl'] > 0]
    losses = [r['pnl'] for r in w if r['pnl'] <= 0]
    net = sum(r['pnl'] for r in w)
    aw = sum(wins)/len(wins) if wins else 0
    al = sum(losses)/len(losses) if losses else 0
    payoff = (aw/abs(al)) if wins and losses else 0
    fee = sum(r.get('feePaidUsdt', 0) for r in w)
    print(f'--- {label}: {n} ไม้ | net={net:+.3f} | WR={len(wins)/n*100:.1f}% | avgWin={aw:+.3f} | avgLoss={al:+.3f} | payoff={payoff:.2f} | fee~{fee:.3f}')
    return net

w7 = [r for r in live if ts(r) >= now - 7*86400]
w24 = [r for r in live if ts(r) >= now - 86400]
stats(w24, '24h'); stats(w7, '7d')

print('\n=== exit reasons 7d ===')
ec = collections.Counter(r.get('reason', '?') for r in w7)
for k, v in ec.most_common(15):
    sub = [r['pnl'] for r in w7 if r.get('reason') == k]
    print(f'  {k:28s} {v:4d}  net={sum(sub):+7.3f}  avg={sum(sub)/len(sub):+.3f}')

print('\n=== per-symbol 7d (top by |net|) ===')
by = collections.defaultdict(list)
for r in w7:
    by[r['symbol']].append(r)
sym_rows = []
for s, arr in by.items():
    n = len(arr); wins = [r['pnl'] for r in arr if r['pnl'] > 0]
    net = sum(r['pnl'] for r in arr)
    sl = sum(1 for r in arr if 'SL' in r.get('reason', '') or 'STOP_LOSS' in r.get('reason', ''))
    sym_rows.append((s, n, net, len(wins)/n*100 if n else 0, sl))
for s, n, net, wr, sl in sorted(sym_rows, key=lambda x: x[2])[:15]:
    print(f'  {s:12s} {n:3d} ไม้  net={net:+7.3f}  WR={wr:5.1f}%  SL={sl}')

print('\n=== avg hold time & conf (7d) ===')
holds = []
confs = []
for r in w7:
    e = r.get('entryDecisionAt') or 0
    c = ts(r)
    if e and c > e:
        holds.append((c - e) / 60.0)
    if r.get('entryConfidence'):
        confs.append(r['entryConfidence'])
if holds:
    print(f'  hold avg={sum(holds)/len(holds):.1f} min (n={len(holds)})')
if confs:
    print(f'  entryConf avg={sum(confs)/len(confs):.3f} (n={len(confs)})')
