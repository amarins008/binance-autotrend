"""PineForge Engine — ctypes FFI adapter for PineScript v6 backtesting.

Provides a Python wrapper around the compiled libpineforge.dll,
offering:
  - OHLCV → BarC[] conversion (zero-copy via numpy when available)
  - Strategy backtest execution with full parameter override control
  - Metrics extraction (trades, PnL, win rate, drawdown, Sharpe, etc.)
  - Parameter sweep for autotuner optimization

Architecture:
  libpineforge.dll  ←→  PineForgeEngine  ←→  autotuner/symbol_autotuner.py

The engine loads a pre-compiled strategy .so (not transpiled at runtime),
so it requires the PineForge codegen pipeline to have produced the
strategy binary beforehand.
"""
from __future__ import annotations

import ctypes
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

_PINEFORGE_ROOT = Path(__file__).resolve().parents[2] / "pineforge-engine"

# MinGW runtime — strategy .so needs libstdc++/libgcc/libwinpthread
_MINGW_BIN = Path("C:/msys64/mingw64/bin")

def _ensure_mingw_path() -> None:
    """Add MinGW bin to DLL search path on Windows."""
    if sys.platform != "win32":
        return
    try:
        os.add_dll_directory(str(_MINGW_BIN))
    except (OSError, AttributeError):
        pass
    mingw_str = str(_MINGW_BIN)
    if mingw_str not in os.environ.get("PATH", ""):
        os.environ["PATH"] = mingw_str + ";" + os.environ.get("PATH", "")

# Ensure MinGW path is available before any DLL loading
_ensure_mingw_path()


# ---------------------------------------------------------------------------
# ctypes struct mirrors  (must match pineforge.h exactly)
# ---------------------------------------------------------------------------

class BarC(ctypes.Structure):
    _fields_ = [
        ("open", ctypes.c_double),
        ("high", ctypes.c_double),
        ("low", ctypes.c_double),
        ("close", ctypes.c_double),
        ("volume", ctypes.c_double),
        ("timestamp", ctypes.c_int64),
    ]

class TradeTickC(ctypes.Structure):
    _fields_ = [
        ("timestamp", ctypes.c_int64),
        ("sequence", ctypes.c_uint64),
        ("price", ctypes.c_double),
        ("quantity", ctypes.c_double),
    ]

class TradeC(ctypes.Structure):
    _fields_ = [
        ("entry_time", ctypes.c_int64),
        ("exit_time", ctypes.c_int64),
        ("entry_price", ctypes.c_double),
        ("exit_price", ctypes.c_double),
        ("pnl", ctypes.c_double),
        ("pnl_pct", ctypes.c_double),
        ("is_long", ctypes.c_int),
        ("max_runup", ctypes.c_double),
        ("max_drawdown", ctypes.c_double),
        ("qty", ctypes.c_double),
        ("commission", ctypes.c_double),
        ("entry_bar_index", ctypes.c_int32),
        ("exit_bar_index", ctypes.c_int32),
    ]

class TradeStatsC(ctypes.Structure):
    _fields_ = [
        ("num_trades", ctypes.c_int32), ("num_wins", ctypes.c_int32),
        ("num_losses", ctypes.c_int32), ("num_even", ctypes.c_int32),
        ("percent_profitable", ctypes.c_double),
        ("net_profit", ctypes.c_double), ("net_profit_pct", ctypes.c_double),
        ("gross_profit", ctypes.c_double), ("gross_profit_pct", ctypes.c_double),
        ("gross_loss", ctypes.c_double), ("gross_loss_pct", ctypes.c_double),
        ("profit_factor", ctypes.c_double),
        ("avg_trade", ctypes.c_double), ("avg_trade_pct", ctypes.c_double),
        ("avg_win", ctypes.c_double), ("avg_win_pct", ctypes.c_double),
        ("avg_loss", ctypes.c_double), ("avg_loss_pct", ctypes.c_double),
        ("ratio_avg_win_avg_loss", ctypes.c_double),
        ("largest_win", ctypes.c_double), ("largest_win_pct", ctypes.c_double),
        ("largest_loss", ctypes.c_double), ("largest_loss_pct", ctypes.c_double),
        ("commission_paid", ctypes.c_double),
        ("expectancy", ctypes.c_double),
        ("max_consecutive_wins", ctypes.c_int32),
        ("max_consecutive_losses", ctypes.c_int32),
        ("avg_bars_in_trade", ctypes.c_double),
        ("avg_bars_in_wins", ctypes.c_double),
        ("avg_bars_in_losses", ctypes.c_double),
    ]

class EquityStatsC(ctypes.Structure):
    _fields_ = [
        ("max_equity_drawdown", ctypes.c_double),
        ("max_equity_drawdown_pct", ctypes.c_double),
        ("max_equity_runup", ctypes.c_double),
        ("max_equity_runup_pct", ctypes.c_double),
        ("buy_hold_return", ctypes.c_double),
        ("buy_hold_return_pct", ctypes.c_double),
        ("sharpe_tv", ctypes.c_double), ("sortino_tv", ctypes.c_double),
        ("sharpe_bar", ctypes.c_double), ("sortino_bar", ctypes.c_double),
        ("cagr", ctypes.c_double), ("calmar", ctypes.c_double),
        ("recovery_factor", ctypes.c_double),
        ("time_in_market_pct", ctypes.c_double),
        ("open_pl", ctypes.c_double),
    ]

class MetricsC(ctypes.Structure):
    _fields_ = [
        ("all", TradeStatsC), ("longs", TradeStatsC),
        ("shorts", TradeStatsC), ("equity", EquityStatsC),
    ]

class EquityPointC(ctypes.Structure):
    _fields_ = [
        ("time_ms", ctypes.c_int64),
        ("equity", ctypes.c_double),
        ("open_profit", ctypes.c_double),
    ]

class SecurityDiagC(ctypes.Structure):
    _fields_ = [
        ("sec_id", ctypes.c_int),
        ("feed_count", ctypes.c_int64),
        ("eval_complete_count", ctypes.c_int64),
        ("eval_partial_count", ctypes.c_int64),
    ]

class TraceEntryC(ctypes.Structure):
    _fields_ = [
        ("timestamp", ctypes.c_int64),
        ("bar_index", ctypes.c_int32),
        ("name_id", ctypes.c_int32),
        ("value", ctypes.c_double),
    ]

class ReportC(ctypes.Structure):
    _fields_ = [
        ("total_trades", ctypes.c_int),
        ("trades", ctypes.POINTER(TradeC)),
        ("trades_len", ctypes.c_int),
        ("net_profit", ctypes.c_double),
        ("input_bars_processed", ctypes.c_int64),
        ("script_bars_processed", ctypes.c_int64),
        ("security_feeds_total", ctypes.c_int64),
        ("security_complete_total", ctypes.c_int64),
        ("security_partial_total", ctypes.c_int64),
        ("magnifier_sub_bars_total", ctypes.c_int64),
        ("magnifier_sample_ticks_total", ctypes.c_int64),
        ("input_tf_seconds", ctypes.c_int),
        ("script_tf_seconds", ctypes.c_int),
        ("script_tf_ratio", ctypes.c_int),
        ("needs_aggregation", ctypes.c_int),
        ("bar_magnifier_enabled", ctypes.c_int),
        ("security_diag", ctypes.POINTER(SecurityDiagC)),
        ("security_diag_len", ctypes.c_int),
        ("trace", ctypes.POINTER(TraceEntryC)),
        ("trace_len", ctypes.c_int),
        ("trace_names", ctypes.POINTER(ctypes.c_char_p)),
        ("trace_names_len", ctypes.c_int),
        ("metrics", MetricsC),
        ("equity_curve", ctypes.POINTER(EquityPointC)),
        ("equity_curve_len", ctypes.c_int64),
    ]

class PfVersionC(ctypes.Structure):
    _fields_ = [
        ("major", ctypes.c_int), ("minor", ctypes.c_int),
        ("patch", ctypes.c_int), ("commit_sha", ctypes.c_char_p),
    ]

EXPECTED_PF_ABI = 2

_MAG_DIST_INT = {
    "UNIFORM": 0, "COSINE": 1, "TRIANGLE": 2,
    "ENDPOINTS": 3, "FRONT_LOADED": 4, "BACK_LOADED": 5,
}


# ---------------------------------------------------------------------------
# Engine wrapper
# ---------------------------------------------------------------------------

class PineForgeEngine:
    """High-level wrapper around a compiled PineForge strategy .so.

    Each strategy .so is self-contained (embeds the full runtime).
    No separate libpineforge.dll is needed.

    Usage:
        engine = PineForgeEngine()
        result = engine.backtest(
            bars_ohlcv=ohlcv_list,
            strategy_so=Path("strategy.so"),
            inputs={"Fast Length": 12, "Slow Length": 26},
            overrides={"commission_value": 0.04},
        )
    """

    def __init__(self, dll_path: Optional[Path] = None):
        # dll_path is unused for per-strategy loading — kept for API compat.
        # Version check is done via any strategy .so at backtest time.
        pass

    def _setup_runtime_signatures(self, lib: ctypes.CDLL) -> None:
        """Set up argtypes/restype for all functions in a strategy .so.

        Each strategy .so is self-contained (embeds the full runtime).
        """
        L = lib
        # Strategy lifecycle
        L.strategy_create.argtypes = [ctypes.c_char_p]
        L.strategy_create.restype = ctypes.c_void_p
        L.strategy_free.argtypes = [ctypes.c_void_p]
        L.report_free.argtypes = [ctypes.POINTER(ReportC)]

        # Backtest
        L.run_backtest_full.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(BarC), ctypes.c_int,
            ctypes.c_char_p, ctypes.c_char_p,
            ctypes.c_int, ctypes.c_int, ctypes.c_int,
            ctypes.POINTER(ReportC),
        ]
        L.run_backtest_full.restype = None

        # Strategy configuration
        for fn in ("strategy_set_input", "strategy_set_override"):
            if hasattr(L, fn):
                getattr(L, fn).argtypes = [
                    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]

        # Error reporting
        if hasattr(L, "strategy_get_last_error"):
            L.strategy_get_last_error.argtypes = [ctypes.c_void_p]
            L.strategy_get_last_error.restype = ctypes.c_char_p

        # Chart timezone
        if hasattr(L, "strategy_set_chart_timezone"):
            L.strategy_set_chart_timezone.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

        # Magnifier
        if hasattr(L, "strategy_set_magnifier_volume_weighted"):
            L.strategy_set_magnifier_volume_weighted.argtypes = [ctypes.c_void_p, ctypes.c_int]

        # Version
        if hasattr(L, "pf_abi_version"):
            L.pf_abi_version.restype = ctypes.c_int

        # Streaming
        if hasattr(L, "strategy_stream_begin"):
            L.strategy_stream_begin.argtypes = [
                ctypes.c_void_p, ctypes.POINTER(BarC), ctypes.c_int,
                ctypes.c_char_p, ctypes.c_char_p]
            L.strategy_stream_begin.restype = ctypes.c_int
        if hasattr(L, "strategy_stream_push_tick"):
            L.strategy_stream_push_tick.argtypes = [
                ctypes.c_void_p, ctypes.POINTER(TradeTickC)]
            L.strategy_stream_push_tick.restype = ctypes.c_int
        if hasattr(L, "strategy_stream_push_ticks"):
            L.strategy_stream_push_ticks.argtypes = [
                ctypes.c_void_p, ctypes.POINTER(TradeTickC), ctypes.c_int]
            L.strategy_stream_push_ticks.restype = ctypes.c_int
        if hasattr(L, "strategy_stream_advance_time"):
            L.strategy_stream_advance_time.argtypes = [
                ctypes.c_void_p, ctypes.c_int64]
            L.strategy_stream_advance_time.restype = ctypes.c_int
        if hasattr(L, "strategy_stream_end"):
            L.strategy_stream_end.argtypes = [
                ctypes.c_void_p, ctypes.c_int]
            L.strategy_stream_end.restype = ctypes.c_int
        if hasattr(L, "strategy_stream_fill_report"):
            L.strategy_stream_fill_report.argtypes = [
                ctypes.c_void_p, ctypes.POINTER(ReportC)]
            L.strategy_stream_fill_report.restype = ctypes.c_int

    def _check_abi(self, lib: ctypes.CDLL) -> None:
        try:
            abi = lib.pf_abi_version()
        except AttributeError:
            _log.warning("PineForge has no pf_abi_version — skipping ABI check")
            return
        if abi != EXPECTED_PF_ABI:
            raise RuntimeError(
                f"PineForge ABI mismatch: strategy reports {abi}, "
                f"harness expects {EXPECTED_PF_ABI}")

    @property
    def version(self) -> str:
        try:
            s = self._lib.pf_version_string()
            return s.decode("utf-8", "replace") if s else "unknown"
        except Exception:
            return "unknown"

    # --- OHLCV conversion -------------------------------------------------

    @staticmethod
    def ohlcv_to_bars(ohlcv: list[dict]) -> tuple[ctypes.Array, int]:
        """Convert a list of OHLCV dicts to (BarC[], count).

        Each dict must have: open, high, low, close, volume, timestamp (ms).
        Uses numpy for zero-copy conversion when available.
        """
        n = len(ohlcv)
        if n == 0:
            return (BarC * 0)(), 0

        try:
            import numpy as np
            dt = np.dtype([
                ("open", "<f8"), ("high", "<f8"), ("low", "<f8"),
                ("close", "<f8"), ("volume", "<f8"), ("timestamp", "<i8"),
            ])
            arr = np.empty(n, dtype=dt)
            for i, bar in enumerate(ohlcv):
                arr[i] = (bar["open"], bar["high"], bar["low"],
                          bar["close"], bar["volume"], bar["timestamp"])
            return (BarC * n).from_buffer_copy(arr.tobytes()), n
        except ImportError:
            bars = (BarC * n)()
            for i, bar in enumerate(ohlcv):
                bars[i] = BarC(
                    float(bar["open"]), float(bar["high"]),
                    float(bar["low"]), float(bar["close"]),
                    float(bar["volume"]), int(bar["timestamp"]),
                )
            return bars, n

    @staticmethod
    def bars_to_ohlcv(bars: ctypes.Array, n: int) -> list[dict]:
        """Convert BarC[] back to a list of OHLCV dicts."""
        return [
            {
                "open": bars[i].open, "high": bars[i].high,
                "low": bars[i].low, "close": bars[i].close,
                "volume": bars[i].volume, "timestamp": bars[i].timestamp,
            }
            for i in range(n)
        ]

    # --- Report extraction -------------------------------------------------

    @staticmethod
    def report_to_dict(report: ReportC) -> dict:
        """Extract a clean Python dict from a C ReportC struct."""
        trades = []
        for i in range(report.trades_len):
            t = report.trades[i]
            trades.append({
                "entry_time": int(t.entry_time),
                "exit_time": int(t.exit_time),
                "entry_price": float(t.entry_price),
                "exit_price": float(t.exit_price),
                "pnl": float(t.pnl),
                "pnl_pct": float(t.pnl_pct),
                "is_long": bool(t.is_long),
                "max_runup": float(t.max_runup),
                "max_drawdown": float(t.max_drawdown),
                "qty": float(t.qty),
                "commission": float(t.commission),
                "entry_bar_index": int(t.entry_bar_index),
                "exit_bar_index": int(t.exit_bar_index),
            })

        m = report.metrics.all
        equity = report.metrics.equity
        return {
            "total_trades": int(report.total_trades),
            "net_profit": float(report.net_profit),
            "input_bars_processed": int(report.input_bars_processed),
            "script_bars_processed": int(report.script_bars_processed),
            "bar_magnifier_enabled": bool(report.bar_magnifier_enabled),
            "trades": trades,
            # Aggregate stats
            "num_trades": int(m.num_trades),
            "num_wins": int(m.num_wins),
            "num_losses": int(m.num_losses),
            "percent_profitable": float(m.percent_profitable),
            "profit_factor": float(m.profit_factor),
            "avg_trade": float(m.avg_trade),
            "avg_trade_pct": float(m.avg_trade_pct),
            "avg_win": float(m.avg_win),
            "avg_loss": float(m.avg_loss),
            "largest_win": float(m.largest_win),
            "largest_loss": float(m.largest_loss),
            "expectancy": float(m.expectancy),
            "max_consecutive_wins": int(m.max_consecutive_wins),
            "max_consecutive_losses": int(m.max_consecutive_losses),
            # Long/short split
            "long_trades": int(report.metrics.longs.num_trades),
            "short_trades": int(report.metrics.shorts.num_trades),
            # Equity stats
            "max_equity_drawdown": float(equity.max_equity_drawdown),
            "max_equity_drawdown_pct": float(equity.max_equity_drawdown_pct),
            "sharpe": float(equity.sharpe_tv),
            "sortino": float(equity.sortino_tv),
            "calmar": float(equity.calmar),
            "recovery_factor": float(equity.recovery_factor),
            "time_in_market_pct": float(equity.time_in_market_pct),
            "buy_hold_return_pct": float(equity.buy_hold_return_pct),
            "cagr": float(equity.cagr),
        }

    # --- Backtest ----------------------------------------------------------

    def backtest(
        self,
        bars_ohlcv: list[dict],
        strategy_so: Path,
        *,
        inputs: dict[str, Any] | None = None,
        overrides: dict[str, Any] | None = None,
        input_tf: str = "",
        script_tf: str = "",
        bar_magnifier: bool = False,
        magnifier_samples: int = 4,
        magnifier_distribution: str = "ENDPOINTS",
        chart_timezone: str = "",
    ) -> dict:
        """Run a backtest on OHLCV data with a compiled strategy.

        The strategy .so contains per-strategy exports (strategy_create,
        run_backtest, report_free, etc.). The runtime libpineforge.dll
        provides shared infrastructure (streaming, version).

        Args:
            bars_ohlcv: List of OHLCV dicts with keys:
                open, high, low, close, volume, timestamp (Unix ms).
            strategy_so: Path to the compiled strategy .so/.dll.
            inputs: PineScript input.*() values (key→value).
            overrides: strategy(...) header overrides (commission_value, etc.).
            input_tf: Input timeframe (e.g. "15"). Empty = auto-detect.
            script_tf: Script timeframe. Empty = same as input_tf.
            bar_magnifier: Enable bar magnifier for lower-timeframe simulation.
            magnifier_samples: Sub-bar samples (default 4).
            magnifier_distribution: Sampling distribution name.
            chart_timezone: IANA timezone for Pine date builtins.

        Returns:
            Dict with net_profit, num_trades, percent_profitable, trades[], etc.
        """
        if not strategy_so.exists():
            raise FileNotFoundError(f"Strategy not found: {strategy_so}")

        # Load strategy .so (self-contained — embeds full runtime)
        strat_lib = ctypes.CDLL(str(strategy_so))
        self._setup_runtime_signatures(strat_lib)
        self._check_abi(strat_lib)

        # Create strategy state
        state = strat_lib.strategy_create(b"{}")
        if not state:
            raise RuntimeError("strategy_create returned NULL")

        report = ReportC()
        try:
            # Apply overrides
            if overrides:
                for k, v in overrides.items():
                    strat_lib.strategy_set_override(
                        state, str(k).encode(), str(v).encode())

            # Apply inputs
            if inputs:
                for k, v in inputs.items():
                    strat_lib.strategy_set_input(
                        state, str(k).encode(), str(v).encode())

        # Chart timezone
            if chart_timezone and hasattr(strat_lib, "strategy_set_chart_timezone"):
                strat_lib.strategy_set_chart_timezone(
                    state, chart_timezone.encode())

            # Convert bars
            bars, n = PineForgeEngine.ohlcv_to_bars(bars_ohlcv)

            # Run backtest
            mag_dist = _MAG_DIST_INT.get(magnifier_distribution.upper(), 3)
            strat_lib.run_backtest_full(
                state, bars, n,
                input_tf.encode(), script_tf.encode(),
                int(bar_magnifier), magnifier_samples, mag_dist,
                ctypes.byref(report),
            )

            # Check errors
            if hasattr(strat_lib, "strategy_get_last_error"):
                err = strat_lib.strategy_get_last_error(state)
                if err:
                    raise RuntimeError(
                        "PineForge error: " + err.decode("utf-8", "replace"))

            return PineForgeEngine.report_to_dict(report)
        finally:
            strat_lib.report_free(ctypes.byref(report))
            strat_lib.strategy_free(state)

    # --- Streaming API -----------------------------------------------------

    def stream_begin(
        self,
        strategy_so: Path,
        warmup_bars: list[dict],
        input_tf: str = "",
        script_tf: str = "",
    ) -> _StreamSession:
        """Begin a streaming session with warmup bars.

        Returns a _StreamSession that accepts push_tick() and advance_time().
        """
        if not strategy_so.exists():
            raise FileNotFoundError(f"Strategy not found: {strategy_so}")

        strat_lib = ctypes.CDLL(str(strategy_so))
        self._setup_runtime_signatures(strat_lib)

        state = strat_lib.strategy_create(b"{}")
        bars, n = self.ohlcv_to_bars(warmup_bars)

        ret = strat_lib.strategy_stream_begin(
            state, bars, n, input_tf.encode(), script_tf.encode())
        if ret != 0:
            err = strat_lib.strategy_get_last_error(state)
            msg = err.decode("utf-8", "replace") if err else "unknown"
            strat_lib.strategy_free(state)
            raise RuntimeError(f"stream_begin failed: {msg}")

        return _StreamSession(strat_lib, state)


class _StreamSession:
    """Active streaming session — push ticks and advance time."""

    def __init__(self, strat_lib, state):
        self._lib = strat_lib
        self._state = state

    def push_tick(self, timestamp_ms: int, price: float, quantity: float,
                  sequence: int = 0) -> int:
        tick = TradeTickC(timestamp_ms, sequence, price, quantity)
        return self._lib.strategy_stream_push_tick(self._state, tick)

    def push_ticks(self, ticks: list[dict]) -> int:
        n = len(ticks)
        if n == 0:
            return 0
        arr = (TradeTickC * n)()
        for i, t in enumerate(ticks):
            arr[i] = TradeTickC(
                int(t["timestamp"]), int(t.get("sequence", 0)),
                float(t["price"]), float(t["quantity"]),
            )
        return self._lib.strategy_stream_push_ticks(self._state, arr, n)

    def advance_time(self, timestamp_ms: int) -> int:
        return self._lib.strategy_stream_advance_time(
            self._state, timestamp_ms)

    def end(self, finalize_partial_bar: bool = True) -> int:
        return self._lib.strategy_stream_end(
            self._state, int(finalize_partial_bar))

    def fill_report(self) -> dict:
        report = ReportC()
        ret = self._lib.strategy_stream_fill_report(
            self._state, ctypes.byref(report))
        if ret != 0:
            raise RuntimeError("stream_fill_report failed")
        result = PineForgeEngine.report_to_dict(report)
        self._lib.report_free(ctypes.byref(report))
        return result

    def close(self):
        if self._state:
            self._lib.strategy_free(self._state)
            self._state = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# ---------------------------------------------------------------------------
# Singleton accessor (for autotuner integration)
# ---------------------------------------------------------------------------

_instance: Optional[PineForgeEngine] = None

def get_engine() -> PineForgeEngine:
    """Get or create the global PineForgeEngine singleton."""
    global _instance
    if _instance is None:
        _instance = PineForgeEngine()
    return _instance
