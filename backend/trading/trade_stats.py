"""Trade log writing, LIVE stats aggregation, and delta updates.

Extracted from main.py to separate I/O-heavy trade logging from the FastAPI
application layer.  These functions own:
- Scan event rotation (_rotate_scan_events_if_needed)
- Append-only trade log writing (_append_trade_log)
- Incremental stats delta (_apply_trade_log_delta)
- Full + cached stats aggregation (_aggregate_live_trade_stats_from_log)
- Per-symbol stats aggregation (_aggregate_live_trade_stats_by_symbol_from_log)
- Today's performance guard (_today_entry_performance_guard)

State lives in module-level caches and is backed by the files on disk.
"""

from __future__ import annotations

import json
import math
import os
import time
from pathlib import Path

from services.config_paths import TRADES_LOG_PATH, VAULT_DIR
from services import cache_registry as _cache_registry

# Direct references to shared mutable state
_LIVE_STATS_CACHE = _cache_registry._LIVE_STATS_CACHE
_SESSION_BIAS_CACHE = _cache_registry._SESSION_BIAS_CACHE

# ---------------------------------------------------------------------------
# Scan events path (not in config_paths — derived at module level)
# ---------------------------------------------------------------------------
SCAN_EVENTS_PATH = VAULT_DIR / "scan_events.jsonl"


def _ensure_vault() -> None:
    """Ensure the vault directory exists."""
    try:
        VAULT_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Scan event rotation
# ---------------------------------------------------------------------------

def _rotate_scan_events_if_needed(max_bytes: int = 25 * 1024 * 1024) -> None:
    """Archive scan_events.jsonl once it grows past max_bytes.

    The scan-event log grows ~1.5MB/day and nothing reads it in a hot path,
    so archiving keeps disk bounded without any runtime cost. Runs only on a
    SCAN append (inside _append_trade_log), i.e. at most once per cycle, and
    only when the size threshold is actually crossed.
    """
    try:
        if not SCAN_EVENTS_PATH.exists():
            return
        size = SCAN_EVENTS_PATH.stat().st_size
        if size < max_bytes:
            return
        archive = VAULT_DIR / f"scan_events.archive.{time.strftime('%Y%m%d')}.jsonl"
        if archive.exists():
            with SCAN_EVENTS_PATH.open("r", encoding="utf-8") as src:
                tail = src.read()
            with archive.open("a", encoding="utf-8") as dst:
                dst.write(tail)
            SCAN_EVENTS_PATH.write_text("", encoding="utf-8")
        else:
            SCAN_EVENTS_PATH.rename(archive)
        print(f"[Scan Events] Rotated {size/1048576:.1f} MB -> {archive.name}")
    except Exception as exc:
        print(f"[Scan Events] Rotate skipped: {exc}")


# ---------------------------------------------------------------------------
# Trade log append
# ---------------------------------------------------------------------------

def _append_trade_log(entry: dict) -> None:
    """Append a trade/scan entry to the appropriate log file.

    LIVE trades go to trades_log.jsonl; SCAN events go to scan_events.jsonl.
    On cloud-sync lock failures, falls back to a local backup directory.
    Increments _LIVE_STATS_VERSION on LIVE trade close so the stats cache
    is invalidated.
    """
    _ensure_vault()
    is_scan = str(entry.get("mode", "")).upper() == "SCAN"
    target = SCAN_EVENTS_PATH if is_scan else TRADES_LOG_PATH
    if is_scan:
        _rotate_scan_events_if_needed()
    written = False
    last_err = None
    # Retry a few times — the E: drive is often locked by cloud-sync
    for attempt in range(4):
        try:
            with target.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            written = True
            break
        except Exception as exc:
            last_err = exc
            time.sleep(0.3 * (attempt + 1))
    if not written:
        try:
            backup_dir = Path(os.environ.get("LOCALAPPDATA", "C:/tmp")) / "hermes" / "binance_trades_backup"
            backup_dir.mkdir(parents=True, exist_ok=True)
            backup = backup_dir / target.name
            with backup.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            print(f"[Trade Log] E: write failed ({last_err}); wrote to backup {backup}")
            written = True
        except Exception as bexc:
            print(f"[Trade Log] ERROR writing {target} AND backup failed: {last_err} | backup: {bexc}")
    if written:
        print(f"[Trade Log] Written to {target.name}: mode={entry.get('mode')}, pnl={entry.get('pnl')}, symbol={entry.get('symbol')}")
        if not is_scan and "pnl" in entry:
            _cache_registry._LIVE_STATS_VERSION += 1
            print(f"[Trade Log] LIVE_STATS_VERSION incremented to {_cache_registry._LIVE_STATS_VERSION}")
            _SESSION_BIAS_CACHE["builtAt"] = 0.0


# ---------------------------------------------------------------------------
# Incremental delta update
# ---------------------------------------------------------------------------

def _apply_trade_log_delta(stats: dict, lines: list[str], symbol: str) -> dict:
    """Apply new trade-log lines (oldest → newest) on top of a previously
    computed stats dict. Updates wins/losses/pnl/today counters and
    prepends new entries to ``lastTrades`` (most recent first, capped at 10).
    """
    if not lines:
        return stats
    sym = (symbol or "").upper().strip()
    now_local = time.localtime()
    today_key = (now_local.tm_year, now_local.tm_mon, now_local.tm_mday)
    new_last_trades: list[dict] = []
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
        new_last_trades.append(obj)
        ts_raw = obj.get("closedAt", obj.get("ts", 0))
        try:
            ts = int(float(ts_raw or 0))
        except Exception:
            ts = 0
        if ts > 0:
            tloc = time.localtime(ts)
            if (tloc.tm_year, tloc.tm_mon, tloc.tm_mday) == today_key:
                if pnl >= 0:
                    stats["winsToday"] += 1
                else:
                    stats["lossesToday"] += 1
                stats["realizedPnlToday"] = round(float(stats["realizedPnlToday"]) + pnl, 6)
    if new_last_trades:
        existing = stats.get("lastTrades") if isinstance(stats.get("lastTrades"), list) else []
        merged: list[dict] = []
        for obj in reversed(new_last_trades):
            merged.append(obj)
            if len(merged) >= 10:
                break
        for obj in existing:
            if len(merged) >= 10:
                break
            merged.append(obj)
        stats["lastTrades"] = merged
    return stats


# ---------------------------------------------------------------------------
# Full + cached stats aggregation
# ---------------------------------------------------------------------------

def _empty_stats() -> dict:
    return {
        "wins": 0, "losses": 0, "realizedPnl": 0.0,
        "winsToday": 0, "lossesToday": 0, "realizedPnlToday": 0.0,
        "lastTrades": [],
    }


def _aggregate_live_trade_stats_from_log(symbol: str | None = None) -> dict:
    """Build LIVE KPI from trade log only (source-of-truth), with light anomaly filtering.

    Performance: the trade log can grow to 100k+ lines; full re-parse on every
    call would make ``/autotrade/status-lite`` block for several seconds during
    heavy trade flow.  We cache by ``(symbol, mtime, size)`` and, on miss, look
    for a previous entry with the same symbol and a smaller size to apply only
    the appended delta (file is append-only; truncation is detected via mtime
    change and falls back to a full parse).
    """
    sym = str(symbol or "").upper().strip()
    now = time.time()
    now_local = time.localtime()
    today_key = (now_local.tm_year, now_local.tm_mon, now_local.tm_mday)

    if not TRADES_LOG_PATH.exists():
        return _empty_stats()

    try:
        stat = TRADES_LOG_PATH.stat()
        mtime = float(stat.st_mtime)
        size = int(stat.st_size)
    except Exception:
        return _empty_stats()

    cache_key = (sym, mtime, size)
    try:
        cached = _LIVE_STATS_CACHE.get(cache_key)
        if cached:
            return dict(cached[1])
    except Exception:
        cached = None

    # Try incremental update from the most recent cache entry for this symbol
    base_stats: dict | None = None
    base_size = -1
    try:
        candidates = [
            (k, v) for k, v in _LIVE_STATS_CACHE.items()
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
                    prev_tloc = time.localtime(prev_mtime)
                    prev_day_key = (prev_tloc.tm_year, prev_tloc.tm_mon, prev_tloc.tm_mday)
                except Exception:
                    prev_day_key = None
                if prev_day_key != today_key:
                    base_stats["winsToday"] = 0
                    base_stats["lossesToday"] = 0
                    base_stats["realizedPnlToday"] = 0.0
                base_size = prev_size
                break
    except Exception:
        base_stats = None
        base_size = -1

    if base_stats is not None and base_size >= 0 and base_size < size:
        try:
            with TRADES_LOG_PATH.open("rb") as f:
                f.seek(base_size)
                tail = f.read(size - base_size)
            new_text = tail.decode("utf-8", errors="replace")
            new_lines = new_text.splitlines()
            stats = _apply_trade_log_delta(base_stats, new_lines, sym)
        except Exception:
            stats = None
        if stats is not None:
            _LIVE_STATS_CACHE[cache_key] = (now, dict(stats))
            _evict_old_cache()
            return stats

    # Full re-parse fallback
    stats = _empty_stats()
    try:
        rows = TRADES_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return stats
    parsed: list[dict] = []
    for line in rows:
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
        parsed.append(obj)
        ts_raw = obj.get("closedAt", obj.get("ts", 0))
        try:
            ts = int(float(ts_raw or 0))
        except Exception:
            ts = 0
        if ts > 0:
            tloc = time.localtime(ts)
            if (tloc.tm_year, tloc.tm_mon, tloc.tm_mday) == today_key:
                if pnl >= 0:
                    stats["winsToday"] += 1
                else:
                    stats["lossesToday"] += 1
                stats["realizedPnlToday"] = round(float(stats["realizedPnlToday"]) + pnl, 6)
    stats["lastTrades"] = list(reversed(parsed[-10:]))
    _LIVE_STATS_CACHE[cache_key] = (now, dict(stats))
    _evict_old_cache()
    return stats


def _evict_old_cache() -> None:
    """Keep the stats cache bounded at 32 entries."""
    if len(_LIVE_STATS_CACHE) > 32:
        oldest_key = min(
            _LIVE_STATS_CACHE,
            key=lambda k: _LIVE_STATS_CACHE[k][0] if isinstance(_LIVE_STATS_CACHE[k], tuple) and len(_LIVE_STATS_CACHE[k]) >= 2 else 0,
        )
        _LIVE_STATS_CACHE.pop(oldest_key, None)


# ---------------------------------------------------------------------------
# Today's performance guard
# ---------------------------------------------------------------------------

def _today_entry_performance_guard(cfg: dict | None = None) -> dict:
    """Check if today's performance warrants pausing entries."""
    cfg = cfg if isinstance(cfg, dict) else {}
    if not bool(cfg.get("todayPerformanceGuardEnabled", True)):
        return {"active": False, "reason": "disabled"}
    stats = _aggregate_live_trade_stats_from_log(None)
    wins = int(stats.get("winsToday", 0) or 0)
    losses = int(stats.get("lossesToday", 0) or 0)
    trades = wins + losses
    min_trades = max(1, int(cfg.get("todayPerformanceGuardMinTrades", 8) or 8))
    max_wr = float(cfg.get("todayPerformanceGuardMaxWinRatePct", 40.0) or 40.0)
    max_pnl = float(cfg.get("todayPerformanceGuardMaxPnlUsdt", 0.0) or 0.0)
    pnl = float(stats.get("realizedPnlToday", 0.0) or 0.0)
    win_rate = (wins / max(trades, 1)) * 100.0 if trades > 0 else 0.0
    active = trades >= min_trades and win_rate < max_wr and pnl < max_pnl
    return {
        "active": bool(active),
        "reason": "today_underperforming" if active else "ok",
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "winRatePct": round(win_rate, 2),
        "pnl": round(pnl, 6),
        "minTrades": min_trades,
        "maxWinRatePct": round(max_wr, 2),
    }


# ---------------------------------------------------------------------------
# Per-symbol stats aggregation (single pass)
# ---------------------------------------------------------------------------

def _aggregate_live_trade_stats_by_symbol_from_log() -> dict[str, dict]:
    """Build per-symbol LIVE KPI in one pass for learning/status.

    Avoids calling _aggregate_live_trade_stats_from_log once per symbol, which
    repeatedly rereads a large trades log and can stall the learning endpoint.
    """
    out: dict[str, dict] = {}
    if not TRADES_LOG_PATH.exists():
        return out
    try:
        rows = TRADES_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return out
    for line in rows:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if str(obj.get("mode", "")).upper() != "LIVE":
            continue
        sym = str(obj.get("symbol", "") or "").upper().strip()
        if not sym or "pnl" not in obj:
            continue
        try:
            pnl = float(obj.get("pnl", 0.0) or 0.0)
        except Exception:
            continue
        if not math.isfinite(pnl) or abs(pnl) > 5000.0:
            continue
        stats = out.setdefault(sym, {"wins": 0, "losses": 0, "realizedPnl": 0.0})
        if pnl >= 0:
            stats["wins"] += 1
        else:
            stats["losses"] += 1
        stats["realizedPnl"] = round(float(stats["realizedPnl"]) + pnl, 6)
    return out
