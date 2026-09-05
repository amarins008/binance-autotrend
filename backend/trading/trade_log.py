"""Trade log I/O, aggregation, rotation, and vault backfill."""

from __future__ import annotations

import json
import math
import os
import re
import threading
import time
from pathlib import Path

from obsidian_memory import ensure_trading_vault
from services import cache_registry as _cache_registry
from services.cache_registry import _LIVE_STATS_CACHE, _SESSION_BIAS_CACHE, _TRADE_LOG_CACHE
from services.config_paths import TRADES_LOG_PATH, VAULT_DIR
from services.file_utils import _atomic_write_text

_TRADES_LOG_ROTATE_MIN_BYTES = int(
    os.getenv(
        "TRADES_LOG_ROTATE_MIN_BYTES",
        str(int(os.getenv("TRADES_LOG_ROTATE_MIN_MB", "8")) * 1024 * 1024),
    )
)
_TRADES_LOG_KEEP_DAYS = max(1, int(os.getenv("TRADES_LOG_KEEP_DAYS", "45")))
_TRADES_LOG_ROTATION_COOLDOWN_SEC = max(60, int(os.getenv("TRADES_LOG_ROTATION_COOLDOWN_SEC", "600")))
_TRADES_LOG_ROTATION_LOCK = threading.Lock()
_TRADES_LOG_LAST_ROTATION = 0.0


def is_corrupt_trade(entry: dict) -> bool:
    """True when a closed-trade record carries corrupted phantive fields.

    Legacy records (mid-Aug 2026 refactor, e.g. ONDO/POL/STX/XLM) occasionally
    wrote an exit price several orders of magnitude off the entry (exit 900-2400
    vs entry ~0.1-0.4). Those records produce absurd pnl once multiplied out and
    must not feed telemetry/learning. NOTE: qty is deliberately NOT a corrupt
    signal — cheap coins (e.g. ARCUSDT at ~0.065) legitimately carry qty 500+
    for a $50 position.
    """
    try:
        pnl = float(entry.get("pnl") or 0.0)
    except Exception:
        return True
    if not math.isfinite(pnl):
        return True
    if abs(pnl) > 5000.0:
        return True
    try:
        entry_px = float(entry.get("entry") or 0.0)
        exit_px = float(entry.get("exit") or 0.0)
    except Exception:
        return False
    if entry_px > 0 and exit_px > 0:
        ratio = exit_px / entry_px
        # A legitimate futures swing on these symbols is < ~8x; anything above
        # means the exit looked up a different price scale (corrupted record).
        if ratio > 8.0 or ratio < 0.125:
            return True
    return False


def trade_hold_seconds(entry: dict) -> float:
    """Return position hold duration for a closed trade (seconds).

    Falls back in order of decreasing reliability: close timestamp derived from
    ``closedAt``/``ts`` minus the entry decision timestamp ``entryDecisionAt``
    (or ``ts`` itself on records that use it as the open time).
    """
    def _sec(v) -> float:
        try:
            f = float(v)
        except (TypeError, ValueError):
            return 0.0
        if not math.isfinite(f):
            return 0.0
        if f < 1e12:
            f *= 1000.0
        return f
    close_raw = entry.get("closedAt") or entry.get("ts") or 0
    open_raw = entry.get("entryDecisionAt") or entry.get("ts") or close_raw
    close_ms = _sec(close_raw)
    open_ms = _sec(open_raw)
    if close_ms <= 0 or open_ms <= 0:
        return 0.0
    return max(0.0, (close_ms - open_ms) / 1000.0)



def _ensure_vault() -> None:
    try:
        VAULT_DIR.mkdir(parents=True, exist_ok=True)
        ensure_trading_vault(VAULT_DIR)
    except Exception as exc:
        print(f"[Trade Log] ERROR ensuring vault {VAULT_DIR}: {exc}")


def _maybe_rotate_trades_log(force: bool = False) -> bool:
    global _TRADES_LOG_LAST_ROTATION
    if not _TRADES_LOG_ROTATION_LOCK.acquire(blocking=False):
        return False
    try:
        now = time.time()
        if not force and (now - _TRADES_LOG_LAST_ROTATION) < _TRADES_LOG_ROTATION_COOLDOWN_SEC:
            return False
        try:
            size = TRADES_LOG_PATH.stat().st_size
        except Exception:
            return False
        if size < _TRADES_LOG_ROTATE_MIN_BYTES:
            return False
        if not TRADES_LOG_PATH.exists():
            return False
        keep_cutoff = int(now - (_TRADES_LOG_KEEP_DAYS * 86400))
        kept: list[str] = []
        archived: list[str] = []
        try:
            with TRADES_LOG_PATH.open("r", encoding="utf-8") as f:
                for line in f:
                    if not line or not line.strip():
                        continue
                    keep = False
                    try:
                        obj = json.loads(line)
                    except Exception:
                        kept.append(line if line.endswith("\n") else line + "\n")
                        continue
                    mode = str(obj.get("mode", "")).upper()
                    if mode == "LIVE" and "pnl" in obj:
                        keep = True
                    else:
                        try:
                            ts = int(float(obj.get("closedAt", obj.get("ts", 0)) or 0))
                        except Exception:
                            ts = 0
                        keep = ts <= 0 or ts >= keep_cutoff
                    if keep:
                        kept.append(line if line.endswith("\n") else line + "\n")
                    else:
                        archived.append(line if line.endswith("\n") else line + "\n")
        except Exception as exc:
            print(f"[Trade Log] rotation read failed: {exc}")
            _TRADES_LOG_LAST_ROTATION = now
            return False
        if not archived:
            _TRADES_LOG_LAST_ROTATION = now
            return False
        archive_path = TRADES_LOG_PATH.with_name("trades_log.archive.jsonl")
        try:
            with archive_path.open("a", encoding="utf-8") as af:
                af.writelines(archived)
        except Exception as exc:
            print(f"[Trade Log] archive append failed (keeping full log): {exc}")
            _TRADES_LOG_LAST_ROTATION = now
            return False
        try:
            _atomic_write_text(TRADES_LOG_PATH, "".join(kept))
        except Exception as exc:
            print(f"[Trade Log] rotation rewrite failed: {exc}")
            _TRADES_LOG_LAST_ROTATION = now
            return False
        _TRADES_LOG_LAST_ROTATION = now
        print(
            f"[Trade Log] rotated: kept {len(kept)} lines, archived {len(archived)} lines, "
            f"new size ~{sum(len(s) for s in kept)} bytes"
        )
        return True
    finally:
        _TRADES_LOG_ROTATION_LOCK.release()


def _append_trade_log(entry: dict) -> None:
    _ensure_vault()
    try:
        with TRADES_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        print(
            f"[Trade Log] Written to {TRADES_LOG_PATH}: mode={entry.get('mode')}, "
            f"pnl={entry.get('pnl')}, symbol={entry.get('symbol')}"
        )
        if str(entry.get("mode", "")).upper() == "LIVE" and "pnl" in entry:
            _cache_registry._LIVE_STATS_VERSION += 1
            print(f"[Trade Log] LIVE_STATS_VERSION incremented to {_cache_registry._LIVE_STATS_VERSION}")
            _SESSION_BIAS_CACHE["builtAt"] = 0.0
        try:
            _maybe_rotate_trades_log()
        except Exception as rot_exc:
            print(f"[Trade Log] rotation check failed (non-fatal): {rot_exc}")
    except Exception as exc:
        print(f"[Trade Log] ERROR writing {TRADES_LOG_PATH}: {exc}")


def _apply_trade_log_delta(stats: dict, lines: list[str], symbol: str) -> dict:
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


def _aggregate_live_trade_stats_from_log(symbol: str | None = None) -> dict:
    sym = str(symbol or "").upper().strip()
    now = time.time()
    empty = {
        "wins": 0,
        "losses": 0,
        "realizedPnl": 0.0,
        "winsToday": 0,
        "lossesToday": 0,
        "realizedPnlToday": 0.0,
        "lastTrades": [],
    }
    if not TRADES_LOG_PATH.exists():
        return dict(empty)

    try:
        stat = TRADES_LOG_PATH.stat()
        mtime = float(stat.st_mtime)
        size = int(stat.st_size)
    except Exception:
        return dict(empty)

    cache_key = (sym, mtime, size)
    try:
        cached = _LIVE_STATS_CACHE.get(cache_key)
        if cached:
            return dict(cached[1])
    except Exception:
        cached = None

    now_local = time.localtime()
    today_key = (now_local.tm_year, now_local.tm_mon, now_local.tm_mday)
    base_stats: dict | None = None
    base_size = -1
    try:
        candidates = [
            (k, v)
            for k, v in _LIVE_STATS_CACHE.items()
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
            stats = _apply_trade_log_delta(base_stats, new_text.splitlines(), sym)
        except Exception:
            stats = None
        if stats is not None:
            _LIVE_STATS_CACHE[cache_key] = (now, dict(stats))
            if len(_LIVE_STATS_CACHE) > 32:
                oldest_key = min(
                    _LIVE_STATS_CACHE,
                    key=lambda k: _LIVE_STATS_CACHE[k][0]
                    if isinstance(_LIVE_STATS_CACHE[k], tuple) and len(_LIVE_STATS_CACHE[k]) >= 2
                    else 0,
                )
                _LIVE_STATS_CACHE.pop(oldest_key, None)
            return stats

    stats = dict(empty)
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
    if len(_LIVE_STATS_CACHE) > 32:
        oldest_key = min(
            _LIVE_STATS_CACHE,
            key=lambda k: _LIVE_STATS_CACHE[k][0]
            if isinstance(_LIVE_STATS_CACHE[k], tuple) and len(_LIVE_STATS_CACHE[k]) >= 2
            else 0,
        )
        _LIVE_STATS_CACHE.pop(oldest_key, None)
    return stats


def _aggregate_live_trade_stats_by_symbol_from_log() -> dict[str, dict]:
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


def _live_closed_trades_from_log(symbol: str | None = None, mode: str = "ALL") -> list[dict]:
    out: list[dict] = []
    if not TRADES_LOG_PATH.exists():
        return out
    sym = str(symbol or "").upper().strip()
    mode_up = str(mode or "ALL").upper()
    cache_key = (sym, mode_up)
    try:
        mtime = TRADES_LOG_PATH.stat().st_mtime
        size = TRADES_LOG_PATH.stat().st_size
    except Exception:
        mtime = 0.0
        size = 0
    cached = _TRADE_LOG_CACHE.get(cache_key)
    if cached:
        cached_mtime, cached_size, cached_data = cached
        if cached_mtime == mtime and cached_size == size:
            return list(cached_data)
    try:
        lines = TRADES_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return out
    seen_keys: set[str] = set()
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
        side = str(obj.get("side", "")).upper()
        if mode_up in ("LONG", "SHORT") and side != mode_up:
            continue
        try:
            pnl = float(obj.get("pnl", 0.0) or 0.0)
        except Exception:
            continue
        if not math.isfinite(pnl):
            continue
        if abs(pnl) > 5000.0:
            continue
        if is_corrupt_trade(obj):
            continue
        ts_raw = obj.get("closedAt", obj.get("ts", 0))
        try:
            ts = int(float(ts_raw or 0))
        except Exception:
            ts = 0
        item = dict(obj)
        item["_pnl"] = pnl
        item["_ts"] = ts
        dedup_key = (
            f"{item.get('symbol', '')}:{item.get('side', '')}:"
            f"{item.get('closedAt', item.get('ts', 0))}:{item.get('pnl', 0)}"
        )
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)
        out.append(item)
    out.sort(key=lambda x: int(x.get("_ts", 0)))
    cutoff_ts = int(time.time()) - (90 * 86400)
    out = [t for t in out if int(t.get("_ts", 0)) >= cutoff_ts]
    _TRADE_LOG_CACHE[cache_key] = (mtime, size, list(out))
    if len(_TRADE_LOG_CACHE) > 50:
        oldest = min(_TRADE_LOG_CACHE, key=lambda k: _TRADE_LOG_CACHE[k][0])
        del _TRADE_LOG_CACHE[oldest]
    return out


def _live_closed_trades_from_symbol(symbol: str, mode: str = "ALL", vault_dir: Path = VAULT_DIR) -> list[dict]:
    return _live_closed_trades_from_log(symbol=symbol, mode=mode)


def _backfill_vault_trades_to_log(
    vault_dir: Path = VAULT_DIR,
    log_path: Path = TRADES_LOG_PATH,
) -> dict:
    existing_keys: set[tuple] = set()
    if log_path.exists():
        for line in log_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if str(obj.get("mode", "")).upper() != "LIVE" or "pnl" not in obj:
                continue
            ts_raw = int(obj.get("closedAt") or obj.get("ts") or 0)
            existing_keys.add(
                (str(obj.get("symbol", "")).upper(), ts_raw, round(float(obj.get("pnl") or 0), 6))
            )
    appended: list[dict] = []
    for p in vault_dir.rglob("*.md"):
        fname = p.name
        m = re.search(r"(\d{4}-\d{2}-\d{2})", fname)
        if not m:
            continue
        txt = p.read_text(encoding="utf-8", errors="ignore")
        if "Mode: LIVE" not in txt or "PnL:" not in txt:
            continue
        for block in re.split(r"\n## ", txt):
            if "Mode: LIVE" not in block or "PnL:" not in block:
                continue
            sym_match = re.search(r"Symbol:\s*\[\[[^|]+\|([^\]]+)\]\]", block)
            t_match = re.search(r"Time:\s*(\d\d):(\d\d):(\d\d)", block)
            pnl_match = re.search(r"PnL:\s*([-+]?\d+(?:\.\d+)?)", block)
            if not (sym_match and t_match and pnl_match):
                continue
            y, mn, d = map(int, m.group(1).split("-"))
            hh, mm, ss = map(int, t_match.groups())
            ts = int(time.mktime((y, mn, d, hh, mm, ss, 0, 0, -1)))
            pnl_val = round(float(pnl_match.group(1)), 6)
            key = (str(sym_match.group(1)).upper(), ts, pnl_val)
            if key in existing_keys:
                continue
            side_match = re.search(r"Side:\s*([^\n]+)", block)
            side = str(side_match.group(1)).strip() if side_match else None
            reason_match = re.search(r"Reason:\s*([^\n]+)", block)
            reason = str(reason_match.group(1)).strip() if reason_match else "LIVE_CLOSE"
            entry = {
                "ts": ts,
                "mode": "LIVE",
                "symbol": str(sym_match.group(1)).upper(),
                "side": side,
                "pnl": pnl_val,
                "reason": reason,
                "closedAt": ts,
            }
            appended.append(entry)
            existing_keys.add(key)
    if not appended:
        return {"appended": 0, "symbols": [], "message": "No missing trades to backfill"}
    appended.sort(key=lambda x: (x["ts"], x["symbol"]))
    with log_path.open("a", encoding="utf-8") as f:
        for entry in appended:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    _cache_registry._LIVE_STATS_VERSION += len(appended)
    symbols = sorted({e["symbol"] for e in appended})
    return {"appended": len(appended), "symbols": symbols, "message": "Backfill complete"}


append_trade = _append_trade_log
get_live_stats = _aggregate_live_trade_stats_from_log
backfill_from_vault = _backfill_vault_trades_to_log
