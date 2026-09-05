import time, json
from collections import defaultdict

CHANGE = 1785784440
print("CHANGE epoch UTC:", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(CHANGE)))
print("CHANGE epoch local:", time.strftime("%Y-%m-%d %H:%M:%S %z", time.localtime(CHANGE)))
print("local tz:", time.strftime("%z", time.localtime()))

# distribution of trades by UTC day
dayc = defaultdict(int)
tslist = []
with open("obsidian_vault/trades_log.jsonl", encoding="utf-8") as f:
    for ln in f:
        ln = ln.strip()
        if not ln:
            continue
        try:
            o = json.loads(ln)
        except Exception:
            continue
        ts = o.get("ts") or o.get("closedAt")
        if not ts:
            continue
        tslist.append(ts)
        dayc[time.strftime("%Y-%m-%d", time.gmtime(ts))] += 1

for d in sorted(dayc):
    print(d, dayc[d])

print("file min UTC:", time.strftime("%Y-%m-%d %H:%M", time.gmtime(min(tslist))))
print("file max UTC:", time.strftime("%Y-%m-%d %H:%M", time.gmtime(max(tslist))))

# How many trades fall in 24h BEFORE change vs AFTER using the user's epoch
before = [t for t in tslist if CHANGE - 86400 <= t < CHANGE]
after = [t for t in tslist if t >= CHANGE]
print(f"\nBy user epoch: BEFORE(24h): {len(before)}  AFTER: {len(after)}")
if before:
    print("  before span UTC:", time.strftime("%Y-%m-%d %H:%M", time.gmtime(min(before))),
          "->", time.strftime("%Y-%m-%d %H:%M", time.gmtime(max(before))))
if after:
    print("  after span UTC:", time.strftime("%Y-%m-%d %H:%M", time.gmtime(min(after))),
          "->", time.strftime("%Y-%m-%d %H:%M", time.gmtime(max(after))))
