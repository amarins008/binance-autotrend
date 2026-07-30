"""TradingView client using tradingview-ta library for maximum stability."""

import time
import threading
import random
from typing import Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum

try:
    from tradingview_ta import TA_Handler, Interval, Exchange
    TRADINGVIEW_TA_AVAILABLE = True
except ImportError:
    TRADINGVIEW_TA_AVAILABLE = False


class TVSignal(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    WAIT = "WAIT"
    ERROR = "ERROR"


@dataclass
class TVSignalResult:
    signal: TVSignal
    confidence: float
    timestamp: float
    source: str = "tradingview-ta"
    metadata: Dict[str, Any] = None
    _is_stale: bool = False

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class TradingViewClient:
    def __init__(self, config: Dict[str, Any]):
        self._cache: Dict[str, TVSignalResult] = {}
        self._signal_history: Dict[str, Dict[str, Any]] = {}
        self._rate_limit_tracker: Dict[str, float] = {}
        self._rate_limit_lock = threading.Lock()
        self._health_status = {"healthy": True, "last_check": 0, "fail_count": 0, "last_error": ""}
        self._disabled_until = 0
        self._symbol_cooldown: Dict[str, float] = {}
        self._consecutive_fails: Dict[str, int] = {}
        self._global_consecutive_fails = 0
        self.update_config(config)

    def update_config(self, config: Dict[str, Any]) -> None:
        self.enabled = bool(config.get("tradingviewEnabled", False))
        self.cache_ttl = int(config.get("tradingviewCacheTtl", 300))
        self.rate_limit_per_minute = int(config.get("tradingviewRateLimit", 6))
        self.timeout = float(config.get("tradingviewTimeout", 10.0))
        self.confidence_boost = float(config.get("tradingviewConfidenceBoost", 0.08))
        self.staleness_threshold = int(config.get("tradingviewStalenessThreshold", 900))
        self._max_failures_before_disable = int(config.get("tradingviewMaxFailures", 10))

    def is_enabled(self) -> bool:
        if not self.enabled:
            return False
        if not TRADINGVIEW_TA_AVAILABLE:
            return False
        now = time.time()
        if now < self._disabled_until:
            return False
        if not self._health_status["healthy"] and now < self._disabled_until:
            return False
        return True

    def _symbol_on_cooldown(self, symbol: str) -> bool:
        until = self._symbol_cooldown.get(symbol, 0.0)
        if time.time() < until:
            return True
        self._symbol_cooldown.pop(symbol, None)
        return False

    def _compute_symbol_cooldown(self, symbol: str):
        fails = self._consecutive_fails.get(symbol, 0)
        base = 30.0
        backoff = base * (2 ** min(fails, 5))
        jitter = random.uniform(0.5, 1.5)
        self._symbol_cooldown[symbol] = time.time() + backoff * jitter

    def _check_rate_limit(self, symbol: str) -> bool:
        if self._symbol_on_cooldown(symbol):
            return False
        with self._rate_limit_lock:
            now = time.time()
            minute_key = f"{symbol}_{int(now // 60)}"
            global_key = f"_global_{int(now // 60)}"

            global_max = 20
            if self._rate_limit_tracker.get(global_key, 0) >= global_max:
                return False

            if self._rate_limit_tracker.get(minute_key, 0) >= self.rate_limit_per_minute:
                return False

            self._rate_limit_tracker[minute_key] = self._rate_limit_tracker.get(minute_key, 0) + 1
            self._rate_limit_tracker[global_key] = self._rate_limit_tracker.get(global_key, 0) + 1

            current_minute = str(int(now // 60))
            old_keys = [k for k in self._rate_limit_tracker if k.split("_")[-1] != current_minute]
            for k in old_keys:
                del self._rate_limit_tracker[k]

            return True

    def _tv_result_to_dict(self, result: TVSignalResult) -> dict:
        if result is None:
            return {}
        try:
            _sig = result.signal
            _meta = result.metadata if isinstance(result.metadata, dict) else {}
            return {
                "signal": getattr(_sig, "value", str(_sig)) if _sig is not None else None,
                "confidence": result.confidence,
                "strength": float(_meta.get("strength", 0.0) or 0.0),
                "timestamp": result.timestamp,
                "ts": result.timestamp,
                "source": result.source,
                "metadata": result.metadata,
            }
        except Exception:
            return {}

    def _tv_dict_to_result(self, d: dict) -> Optional[TVSignalResult]:
        try:
            _sig = d.get("signal")
            try:
                _sig_enum = TVSignal(_sig) if _sig is not None else TVSignal.WAIT
            except Exception:
                _sig_enum = TVSignal.WAIT
            return TVSignalResult(
                signal=_sig_enum,
                confidence=float(d.get("confidence", 0.0) or 0.0),
                timestamp=float(d.get("timestamp", time.time())),
                source=str(d.get("source", "disk")),
                metadata=d.get("metadata") or {},
            )
        except Exception:
            return None

    def _get_from_cache(self, symbol: str, force_refresh: bool = False, skip_stale_disk: bool = False, require_fresh: bool = False) -> Optional[TVSignalResult]:
        now = time.time()
        if not force_refresh and symbol in self._cache:
            cached = self._cache[symbol]
            age = now - cached.timestamp
            if age < self.cache_ttl:
                return cached
            elif not require_fresh and age < self.staleness_threshold:
                return cached
            else:
                del self._cache[symbol]

        if not force_refresh:
            try:
                from trading.per_symbol_context import PerSymbolContext
                from trading.shared_cache_layer import SharedCacheLayer
                from services.config_paths import VAULT_DIR
                ctx = PerSymbolContext(str(symbol).upper().strip(), SharedCacheLayer(VAULT_DIR), None)
                d = ctx.get_tv_signal()
                if isinstance(d, dict) and d.get("ts"):
                    age = now - float(d["ts"])
                    if age < self.cache_ttl:
                        res = self._tv_dict_to_result(d)
                        if res is not None:
                            self._cache[symbol] = res
                            return res
                    elif not skip_stale_disk and age < self.staleness_threshold:
                        res = self._tv_dict_to_result(d)
                        if res is not None:
                            self._cache[symbol] = res
                            return res
            except Exception:
                pass
        return None

    def _store_in_cache(self, symbol: str, result: TVSignalResult):
        self._cache[symbol] = result
        try:
            from trading.per_symbol_context import PerSymbolContext
            from trading.shared_cache_layer import SharedCacheLayer
            from services.config_paths import VAULT_DIR
            ctx = PerSymbolContext(str(symbol).upper().strip(), SharedCacheLayer(VAULT_DIR), None)
            ctx.save_tv_signal(self._tv_result_to_dict(result))
        except Exception:
            pass

        now = time.time()
        old_symbols = [k for k, v in self._cache.items() if now - v.timestamp > self.staleness_threshold]
        for s in old_symbols:
            del self._cache[s]

    def _update_health(self, success: bool):
        now = time.time()
        self._health_status["last_check"] = now

        if success:
            self._health_status["fail_count"] = max(0, self._health_status["fail_count"] - 1)
            self._health_status["last_error"] = ""
            if self._health_status["fail_count"] == 0:
                self._health_status["healthy"] = True
        else:
            self._health_status["fail_count"] += 1
            if self._health_status["fail_count"] >= self._max_failures_before_disable:
                self._health_status["healthy"] = False
                self._disabled_until = now + 300

    def get_signal(self, symbol: str, internal_signal: str, internal_confidence: float, force_refresh: bool = False) -> Optional[TVSignalResult]:
        if not self.is_enabled():
            return None

        cached = self._get_from_cache(symbol, force_refresh=force_refresh)
        if cached and not force_refresh:
            self._consecutive_fails.pop(symbol, None)
            return cached

        if not self._check_rate_limit(symbol):
            return None

        try:
            result = self._fetch_from_tradingview(symbol, internal_signal, internal_confidence)

            if result:
                self._store_in_cache(symbol, result)
                self._consecutive_fails.pop(symbol, None)
                self._global_consecutive_fails = max(0, self._global_consecutive_fails - 1)
                self._update_health(True)
                return result
            else:
                self._consecutive_fails[symbol] = self._consecutive_fails.get(symbol, 0) + 1
                self._global_consecutive_fails += 1
                self._compute_symbol_cooldown(symbol)
                self._update_health(False)
                return None

        except Exception:
            self._consecutive_fails[symbol] = self._consecutive_fails.get(symbol, 0) + 1
            self._global_consecutive_fails += 1
            self._compute_symbol_cooldown(symbol)
            self._update_health(False)
            return None

    def _fetch_from_tradingview(self, symbol: str, internal_signal: str, internal_confidence: float) -> Optional[TVSignalResult]:
        try:
            handler = TA_Handler(
                symbol=symbol,
                screener="CRYPTO",
                exchange="BINANCE",
                interval=Interval.INTERVAL_1_HOUR,
                timeout=self.timeout
            )

            analysis = handler.get_analysis()

            if not analysis:
                return None

            if not hasattr(analysis, 'summary'):
                return None

            recommend = analysis.summary.get("RECOMMENDATION", "NEUTRAL")

            if recommend in ("STRONG_BUY", "BUY"):
                tv_signal = TVSignal.LONG
                confidence = 0.8 if recommend == "STRONG_BUY" else 0.6
            elif recommend in ("STRONG_SELL", "SELL"):
                tv_signal = TVSignal.SHORT
                confidence = 0.8 if recommend == "STRONG_SELL" else 0.6
            else:
                tv_signal = TVSignal.WAIT
                confidence = 0.3

            oscillators = analysis.oscillators if hasattr(analysis, 'oscillators') else {}
            moving_averages = analysis.moving_averages if hasattr(analysis, 'moving_averages') else {}

            osc_buy = oscillators.get("BUY", 0)
            osc_sell = oscillators.get("SELL", 0)
            ma_buy = moving_averages.get("BUY", 0)
            ma_sell = moving_averages.get("SELL", 0)

            total_indicators = osc_buy + osc_sell + ma_buy + ma_sell
            if total_indicators > 0:
                buy_ratio = (osc_buy + ma_buy) / total_indicators
                sell_ratio = (osc_sell + ma_sell) / total_indicators
                strength = max(buy_ratio, sell_ratio)
            else:
                strength = 0.5

            return TVSignalResult(
                signal=tv_signal,
                confidence=confidence,
                timestamp=time.time(),
                metadata={
                    "recommendation": recommend,
                    "oscillators": oscillators,
                    "moving_averages": moving_averages,
                    "strength": strength
                }
            )

        except Exception as exc:
            self._health_status["last_error"] = f"{type(exc).__name__}: {exc}"[:240]
            return None

    def confirm_signal(self, tv_result: TVSignalResult, internal_signal: str) -> float:
        if not tv_result or tv_result.signal == TVSignal.ERROR:
            return 0.0

        internal_signal_upper = internal_signal.upper()
        boost = self.confidence_boost

        if tv_result.signal.value == internal_signal_upper:
            return boost * tv_result.confidence

        if tv_result.signal != TVSignal.WAIT and tv_result.signal.value != internal_signal_upper:
            return -0.5 * boost * tv_result.confidence

        return boost * 0.3

    def get_position_guidance(self, symbol: str, side: str, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        if not self.is_enabled():
            return None

        cached = self._get_from_cache(symbol, force_refresh=force_refresh, skip_stale_disk=True, require_fresh=True)
        if cached and not force_refresh:
            return {
                "recommendation": cached.metadata.get("recommendation"),
                "oscillators": cached.metadata.get("oscillators", {}),
                "moving_averages": cached.metadata.get("moving_averages", {}),
                "strength": float(cached.metadata.get("strength", 0.0) or 0.0),
                "confidence": cached.confidence,
                "signal": cached.signal.value,
                "timestamp": cached.timestamp
            }

        if not self._check_rate_limit(symbol):
            return None

        try:
            result = self._fetch_from_tradingview(symbol, side, 0.0)
            if not result:
                return None

            self._store_in_cache(symbol, result)

            return {
                "recommendation": result.metadata.get("recommendation"),
                "oscillators": result.metadata.get("oscillators", {}),
                "moving_averages": result.metadata.get("moving_averages", {}),
                "strength": float(result.metadata.get("strength", 0.0) or 0.0),
                "confidence": result.confidence,
                "signal": result.signal.value,
                "timestamp": result.timestamp
            }
        except Exception:
            return None

    def _track_signal_history(self, symbol: str, tv_result: TVSignalResult) -> None:
        if not tv_result or not symbol:
            return
        strength = tv_result.metadata.get("strength", 0.5)
        self._signal_history[symbol] = {
            "strength": strength,
            "signal": tv_result.signal.value,
            "ts": time.time(),
            "confidence": tv_result.confidence,
        }
        if len(self._signal_history) > 10:
            oldest = min(self._signal_history, key=lambda k: self._signal_history[k]["ts"])
            del self._signal_history[oldest]

    def get_signal_momentum(self, symbol: str, current_strength: float) -> Dict[str, Any]:
        prev = self._signal_history.get(symbol)
        if not prev:
            return {"weakening": False, "delta": 0.0, "prev_strength": 0.0, "age_sec": 0}

        age_sec = time.time() - prev["ts"]
        if age_sec > 600:
            return {"weakening": False, "delta": 0.0, "prev_strength": prev["strength"], "age_sec": age_sec}

        delta = current_strength - prev["strength"]
        weakening = delta < -0.05
        return {
            "weakening": weakening,
            "delta": delta,
            "prev_strength": prev["strength"],
            "age_sec": age_sec,
        }

    def get_health_status(self) -> Dict[str, Any]:
        # Auto-recover if disable window has expired
        if not self._health_status["healthy"] and time.time() >= self._disabled_until:
            self._health_status["healthy"] = True
            self._health_status["fail_count"] = 0
        last_error = self._health_status.get("last_error", "")
        if "429" in last_error:
            error_type = "rate_limited"
        elif last_error and ("timeout" in last_error.lower() or "connection" in last_error.lower()):
            error_type = "connection"
        elif last_error:
            error_type = "unknown"
        else:
            error_type = ""
        # Read recovery_count from supervisor state
        try:
            from services.app_state import AUTO_TRADE
            _s = (AUTO_TRADE.get("supervisorAutoTune") or {}).get("delegations") or {}
            _r = _s.get("tradingview_health") or {}
            recovery_count = int(_r.get("recovery_count", 0))
        except Exception:
            recovery_count = 0
        return {
            "enabled": self.enabled,
            "healthy": self._health_status["healthy"],
            "disabled_until": self._disabled_until,
            "fail_count": self._health_status["fail_count"],
            "cache_size": len(self._cache),
            "last_check": self._health_status["last_check"],
            "last_error": last_error,
            "error_type": error_type,
            "recovery_count": recovery_count,
            "tradingview_ta_available": TRADINGVIEW_TA_AVAILABLE,
            "symbol_cooldowns": len(self._symbol_cooldown),
        }

    def force_disable(self, duration_seconds: int = 300):
        self._disabled_until = time.time() + duration_seconds
        self._health_status["healthy"] = False

    def force_enable(self):
        self._disabled_until = 0
        self._health_status["healthy"] = True
        self._health_status["fail_count"] = 0
        self._symbol_cooldown.clear()
        self._consecutive_fails.clear()
        self._global_consecutive_fails = 0
        self.enabled = True


# Global instance
_tv_client_instance: Optional[TradingViewClient] = None


def get_tv_client(config: Dict[str, Any]) -> TradingViewClient:
    global _tv_client_instance
    if _tv_client_instance is None:
        _tv_client_instance = TradingViewClient(config)
    else:
        _tv_client_instance.update_config(config)
    return _tv_client_instance


def reset_tv_client():
    global _tv_client_instance
    _tv_client_instance = None


# Backward compatibility aliases
get_tv_mcp = get_tv_client
reset_tv_mcp = reset_tv_client


async def async_get_position_guidance(tv_client: TradingViewClient, symbol: str, side: str):
    """Async wrapper for get_position_guidance to avoid blocking the event loop."""
    import asyncio
    return await asyncio.to_thread(tv_client.get_position_guidance, symbol, side)
