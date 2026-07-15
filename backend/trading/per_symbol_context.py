"""PerSymbolContext: unified per-symbol autotune context.

Combines storage + cache + auto-tune logic for a single symbol so that
supervisor review, scan cycle, and trade-close handlers all share the
same code path.

Usage inside ``_hermes_supervisor_review()``::

    shared_cache = SharedCacheLayer(vault_dir)
    for symbol in active_symbols:
        ctx = PerSymbolContext(symbol, shared_cache, cfg)
        # ... read profile, trades, windows, risk-tune ...
        # ... record trade / observation ...
        ctx.commit()  # persists all dirty data
"""

from __future__ import annotations

import math
import time
from pathlib import Path
from typing import Any

from trading.per_symbol_storage import PerSymbolStorage
from trading.shared_cache_layer import SharedCacheLayer


class PerSymbolContext:
    """Per-symbol autotune context with lazy loading and batched writes.

    Attributes:
        symbol:       Normalised uppercase symbol string.
        profile:      Learning profile dict (loaded lazily).
        sym_profile:  3-tier symbol profile dict (loaded lazily).
    """

    def __init__(
        self,
        symbol: str,
        cache: SharedCacheLayer,
        config: dict | None = None,
    ):
        self.symbol = symbol.upper().strip()
        self._cache = cache
        self._storage = cache.get_storage(self.symbol)
        self._config = config if isinstance(config, dict) else {}

        # Lazy-loaded state
        self._profile: dict | None = None
        self._sym_profile: dict | None = None
        self._risk_tune: dict | None = None

        # Dirty tracking
        self._dirty_profile = False
        self._dirty_sym_profile = False
        self._pending_trades: list[dict] = []
        self._pending_observation: dict | None = None

    # ------------------------------------------------------------------
    # Profile accessors
    # ------------------------------------------------------------------

    @property
    def profile(self) -> dict:
        if self._profile is None:
            self._profile = self._cache.get_profile(self.symbol)
        return self._profile

    def update_profile(self, updates: dict) -> None:
        """Merge *updates* into the learning profile and mark dirty."""
        self.profile.update(updates)
        self._dirty_profile = True

    @property
    def sym_profile(self) -> dict:
        if self._sym_profile is None:
            self._sym_profile = self._storage.load_symbol_profile()
        return self._sym_profile

    def update_sym_profile(self, updates: dict) -> None:
        """Merge *updates* into the symbol profile and mark dirty."""
        self.sym_profile.update(updates)
        self._dirty_sym_profile = True

    # ------------------------------------------------------------------
    # Trade helpers
    # ------------------------------------------------------------------

    def get_trades(self, mode: str = "LIVE") -> list[dict]:
        """Return closed trades for this symbol (from disk / cache)."""
        return self._storage.load_trades(mode=mode)

    def record_trade(self, trade: dict) -> None:
        """Queue a trade for writing on commit().

        Also appends to per-symbol vault (journal / failure / improvement).
        """
        self._pending_trades.append(trade)
        self._storage.append_trade(trade)
        self._storage.append_journal(trade)
        self._storage.append_failure(trade)
        self._storage.append_improvement(trade)

    # ------------------------------------------------------------------
    # Observation helpers
    # ------------------------------------------------------------------

    def record_observation(self, intel: dict, chosen: bool, score: float) -> None:
        """Record a scan observation (increment counters, update last* fields)."""
        pr = self.profile
        pr["observations"] = int(pr.get("observations", 0) or 0) + 1
        if chosen:
            pr["pickedCount"] = int(pr.get("pickedCount", 0) or 0) + 1
            self._storage.append_scan_pick(intel, score)
        pr["lastSignal"] = intel.get("signal", "WAIT")
        pr["lastConfidence"] = intel.get("confidence", 0.0)
        pr["lastScanScore"] = score
        pr["lastSpreadBps"] = intel.get("execution", {}).get("spreadBps", 0.0)
        pr["lastMomentumPct"] = intel.get("execution", {}).get("momentumPct", 0.0)
        pr["updatedAt"] = int(time.time())
        self._dirty_profile = True
        self._pending_observation = {"intel": intel, "chosen": chosen, "score": score}

    # ------------------------------------------------------------------
    # Rolling windows
    # ------------------------------------------------------------------

    def get_rolling_windows(self) -> dict:
        """Return cached rolling windows, recomputing if stale."""
        windows = self._cache.get_windows(self.symbol)
        if windows:
            return windows
        windows = self._compute_rolling_windows()
        if windows:
            self._storage.save_windows(windows)
            self._cache.set_windows(self.symbol, windows)
        return windows

    def _compute_rolling_windows(self) -> dict:
        """Compute memory windows from per-symbol trades."""
        trades = self.get_trades("LIVE")
        cleaned: list[dict] = []
        for t in trades:
            if not isinstance(t, dict):
                continue
            try:
                pnl = float(t.get("_pnl", t.get("pnl", 0.0)) or 0.0)
            except Exception:
                continue
            if not math.isfinite(pnl) or abs(pnl) > 5000.0:
                continue
            ts = 0
            try:
                ts = int(float(t.get("_ts", t.get("closedAt", t.get("ts", 0))) or 0))
            except Exception:
                ts = 0
            item = dict(t)
            item["_pnl"] = pnl
            item["_ts"] = ts
            cleaned.append(item)
        cleaned.sort(key=lambda x: int(x.get("_ts", 0) or 0))
        if not cleaned:
            return {}

        def _window(rows: list[dict], label: str) -> dict:
            pnls = [float(r.get("_pnl", 0.0) or 0.0) for r in rows]
            wins = [p for p in pnls if p >= 0.0]
            losses = [p for p in pnls if p < 0.0]
            wr = (len(wins) / max(len(rows), 1)) * 100.0
            pnl_sum = sum(pnls)
            avg_win = sum(wins) / max(len(wins), 1) if wins else 0.0
            avg_loss = abs(sum(losses) / max(len(losses), 1)) if losses else 0.0
            gross_win = sum(wins)
            gross_loss = abs(sum(losses))
            pf = gross_win / gross_loss if gross_loss > 0 else (999.0 if gross_win > 0 else 0.0)
            return {
                "label": label,
                "trades": len(rows),
                "winRatePct": round(wr, 2),
                "pnl": round(pnl_sum, 6),
                "avgWin": round(avg_win, 6),
                "avgLoss": round(avg_loss, 6),
                "profitFactor": round(min(pf, 999.0), 4),
                "grossWin": round(gross_win, 6),
                "grossLoss": round(gross_loss, 6),
            }

        result: dict[str, dict] = {}
        for window in (8, 20, 50):
            if len(cleaned) < max(4, min(window, 8)):
                continue
            rows = cleaned[-window:]
            result[f"{min(window, len(rows))}"] = _window(rows, f"last_{min(window, len(rows))}_trades")
        result["all"] = _window(cleaned, f"all_{len(cleaned)}_trades")
        return result

    # ------------------------------------------------------------------
    # Risk tune
    # ------------------------------------------------------------------

    def get_risk_tune(self) -> dict:
        """Return cached risk-tune, recomputing if stale."""
        risk = self._cache.get_risk_tune(self.symbol)
        if risk and risk.get("active"):
            return risk
        risk = self._compute_risk_tune()
        if risk:
            self._storage.save_risk_tune(risk)
            self._cache.set_risk_tune(self.symbol, risk)
        return risk

    def _compute_risk_tune(self) -> dict:
        """Compute symbolRiskTune from recent trades."""
        try:
            from main import _symbol_risk_tune_from_recent_trades
        except ImportError:
            return {}
        trades = self.get_trades("LIVE")
        return _symbol_risk_tune_from_recent_trades(self.symbol, trades, self._config)

    # ------------------------------------------------------------------
    # Guardian lock (per-symbol liveProfitLocks entry)
    # ------------------------------------------------------------------

    def get_guardian_lock(self) -> dict:
        """Load the per-symbol guardian lock entry from disk/cache."""
        return self._storage.load_guardian_lock()

    def save_guardian_lock(self, lock: dict) -> None:
        """Persist the per-symbol guardian lock entry."""
        self._storage.save_guardian_lock(lock)
        self._cache.set_guardian_lock(self.symbol, lock)

    # ------------------------------------------------------------------
    # TradingView signal cache (per-symbol)
    # ------------------------------------------------------------------

    def get_tv_signal(self) -> dict:
        """Load cached TradingView signal for this symbol."""
        return self._storage.load_tv_signal()

    def save_tv_signal(self, signal: dict) -> None:
        """Persist cached TradingView signal for this symbol."""
        self._storage.save_tv_signal(signal)
        self._cache.set_tv_signal(self.symbol, signal)

    # ------------------------------------------------------------------
    # Runtime state (per-symbol transient state)
    # ------------------------------------------------------------------

    def get_runtime(self) -> dict:
        """Load the per-symbol runtime state dict."""
        return self._storage.load_runtime()

    def save_runtime(self, runtime: dict) -> None:
        """Persist the per-symbol runtime state dict."""
        self._storage.save_runtime(runtime)
        self._cache.set_runtime(self.symbol, runtime)

    # ------------------------------------------------------------------
    # Symbol note
    # ------------------------------------------------------------------

    def update_symbol_note(self, last_trade: dict | None = None) -> None:
        """Write/update the per-symbol MarketPatterns note."""
        self._storage.update_symbol_note(self.profile, last_trade)

    # ------------------------------------------------------------------
    # Commit
    # ------------------------------------------------------------------

    def commit(self) -> None:
        """Persist all dirty data to disk and update caches."""
        if self._dirty_profile and self._profile is not None:
            self._storage.save_profile(self._profile)
            self._cache.set_profile(self.symbol, self._profile)
            self._dirty_profile = False

        if self._dirty_sym_profile and self._sym_profile is not None:
            self._storage.save_symbol_profile(self._sym_profile)
            self._dirty_sym_profile = False

        # Write daily stats to shared storage
        for trade in self._pending_trades:
            pnl = float(trade.get("pnl", 0.0) or 0.0)
            self._cache.shared.append_global_trade(trade)
            stats = self._cache.shared.load_daily_stats()
            stats["trades"] = int(stats.get("trades", 0) or 0) + 1
            if pnl >= 0:
                stats["wins"] = int(stats.get("wins", 0) or 0) + 1
            else:
                stats["losses"] = int(stats.get("losses", 0) or 0) + 1
            stats["pnl"] = round(float(stats.get("pnl", 0.0) or 0.0) + pnl, 6)
            self._cache.shared.save_daily_stats(stats)

        self._pending_trades.clear()
        self._pending_observation = None

        # Invalidate after commit so next cycle gets fresh data
        self._cache.invalidate_symbol(self.symbol)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @property
    def storage(self) -> PerSymbolStorage:
        return self._storage

    @property
    def config(self) -> dict:
        return self._config
