"""Simulate the cache logic with the fix to verify today's counter is preserved across SCAN appends."""
import time
import json
import math
from pathlib import Path

# Simulate state
LIVE_STATS_CACHE = {}

def time_local(*, ts=None):
    if ts is None:
        ts = time.time()
    return time.localtime(ts)


def _apply_trade_log_delta(stats, lines, symbol):
    sym = (symbol or "").upper().strip()
    now_local = time_local()
    today_key = (now_local.tm_year, now_local.tm_mon, now_local.tm_mday)
    for line in lines:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if str(obj.get("mode", "")).upper() != "LIVE":
            continue
        if sym and str(obj.get("symbol", "")).upper() != sym:
            continue
        if "pnl" not in obj:
            continue
        try:
            pnl = float(obj.get("pnl", 0.0) or 0.0)
        except Exception:
            continue
        if not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        if pnl >= 0:
            stats["wins"] += 1
        else:
            stats["losses"] += 1
        stats["realizedPnl"] = round(float(stats["realizedPnl"]) + pnl, 6)
        ts_raw = obj.get("closedAt", obj.get("ts", 0))
        try:
            ts = int(float(ts_raw or 0))
        except Exception:
            ts = 0
        if ts > 0:
            tloc = time_local(ts=ts)
            if (tloc.tm_year, tloc.tm_mon, tloc.tm_mday) == today_key:
                if pnl >= 0:
                    stats["winsToday"] += 1
                else:
                    stats["lossesToday"] += 1
                stats["realizedPnlToday"] = round(float(stats["realizedPnlToday"]) + pnl, 6)
    return stats


def aggregate_with_fix(sym, mtime, size, all_log_lines, current_log_tail_lines):
    """Mirror backend logic with the fix."""
    now_local = time_local()
    today_key = (now_local.tm_year, now_local.tm_mon, now_local.tm_mday)

    cache_key = (sym, mtime, size)
    cached = LIVE_STATS_CACHE.get(cache_key)
    if cached:
        return dict(cached[1])

    base_stats = None
    base_size = -1
    candidates = [
        (k, v) for k, v in LIVE_STATS_CACHE.items()
        if isinstance(k, tuple) and len(k) == 3 and k[0] == sym
    ]
    candidates.sort(key=lambda kv: kv[0][2], reverse=True)
    for k, v in candidates:
        prev_mtime, prev_size = k[1], k[2]
        if prev_size <= size and abs(prev_mtime - mtime) < 1e-3:
            base_stats = dict(v[1])
            base_size = prev_size
            break
        if prev_size <= size:
            base_stats = dict(v[1])
            try:
                prev_tloc = time_local(ts=prev_mtime)
                prev_day_key = (prev_tloc.tm_year, prev_tloc.tm_mon, prev_tloc.tm_mday)
            except Exception:
                prev_day_key = None
            if prev_day_key != today_key:
                base_stats["winsToday"] = 0
                base_stats["lossesToday"] = 0
                base_stats["realizedPnlToday"] = 0.0
            base_size = prev_size
            break

    if base_stats is not None and base_size >= 0 and base_size < size:
        new_lines = current_log_tail_lines
        stats = _apply_trade_log_delta(base_stats, new_lines, sym)
        LIVE_STATS_CACHE[cache_key] = (time.time(), dict(stats))
        return stats

    # Full parse
    stats = {"wins": 0, "losses": 0, "realizedPnl": 0.0, "winsToday": 0, "lossesToday": 0, "realizedPnlToday": 0.0, "lastTrades": []}
    for line in all_log_lines:
        # simplified full parse inline
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if str(obj.get("mode", "")).upper() != "LIVE":
            continue
        if sym and str(obj.get("symbol", "")).upper() != sym:
            continue
        if "pnl" not in obj:
            continue
        try:
            pnl = float(obj.get("pnl", 0.0) or 0.0)
        except Exception:
            continue
        if not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        if pnl >= 0:
            stats["wins"] += 1
        else:
            stats["losses"] += 1
        stats["realizedPnl"] = round(float(stats["realizedPnl"]) + pnl, 6)
        ts_raw = obj.get("closedAt", obj.get("ts", 0))
        try:
            ts = int(float(ts_raw or 0))
        except Exception:
            ts = 0
        if ts > 0:
            tloc = time_local(ts=ts)
            if (tloc.tm_year, tloc.tm_mon, tloc.tm_mday) == today_key:
                if pnl >= 0:
                    stats["winsToday"] += 1
                else:
                    stats["lossesToday"] += 1
                stats["realizedPnlToday"] = round(float(stats["realizedPnlToday"]) + pnl, 6)
    LIVE_STATS_CACHE[cache_key] = (time.time(), dict(stats))
    return stats


# Simulation: build a fake log with 5 LIVE trades today, then 10 SCAN appends after
fake_log = []
now = int(time.time())
for i in range(5):
    fake_log.append(json.dumps({
        "mode": "LIVE", "symbol": "BTCUSDT", "pnl": 0.5 if i % 2 == 0 else -0.3,
        "closedAt": now - 60 + i, "ts": now - 60 + i
    }))
# Initial scan
fake_log.append(json.dumps({"mode": "SCAN", "symbol": "BTCUSDT", "ts": now}))

# Step 1: First call after trades
size1 = sum(len(l) + 1 for l in fake_log)
mtime1 = now + 0.001
stats = aggregate_with_fix("", mtime1, size1, fake_log, fake_log[-1:])
print(f"Step 1 (first call, after 5 trades + 1 SCAN): winsToday={stats['winsToday']}, lossesToday={stats['lossesToday']}, pnlToday={stats['realizedPnlToday']}")
assert stats["winsToday"] == 3, f"expected 3, got {stats['winsToday']}"
assert stats["lossesToday"] == 2, f"expected 2, got {stats['lossesToday']}"

# Step 2: Add 5 more SCAN entries (simulate appends)
for i in range(5):
    fake_log.append(json.dumps({"mode": "SCAN", "symbol": "BTCUSDT", "ts": now + i + 1}))
size2 = sum(len(l) + 1 for l in fake_log)
mtime2 = now + 0.5
# Only delta (new tail)
tail = fake_log[6:]  # last 5 SCAN entries
stats = aggregate_with_fix("", mtime2, size2, fake_log, tail)
print(f"Step 2 (5 more SCAN appends, no LIVE): winsToday={stats['winsToday']}, lossesToday={stats['lossesToday']}, pnlToday={stats['realizedPnlToday']}")
assert stats["winsToday"] == 3, f"expected 3 (preserved), got {stats['winsToday']}"
assert stats["lossesToday"] == 2, f"expected 2 (preserved), got {stats['lossesToday']}"

# Step 3: Add 1 more LIVE trade today + 1 SCAN
fake_log.append(json.dumps({"mode": "LIVE", "symbol": "BTCUSDT", "pnl": 0.8, "closedAt": now + 10, "ts": now + 10}))
fake_log.append(json.dumps({"mode": "SCAN", "symbol": "BTCUSDT", "ts": now + 11}))
size3 = sum(len(l) + 1 for l in fake_log)
mtime3 = now + 1.0
tail = fake_log[11:]
stats = aggregate_with_fix("", mtime3, size3, fake_log, tail)
print(f"Step 3 (1 LIVE + 1 SCAN): winsToday={stats['winsToday']}, lossesToday={stats['lossesToday']}, pnlToday={stats['realizedPnlToday']}")
assert stats["winsToday"] == 4, f"expected 4, got {stats['winsToday']}"
assert stats["lossesToday"] == 2, f"expected 2, got {stats['lossesToday']}"

print("\nAll assertions passed. Fix works correctly.")