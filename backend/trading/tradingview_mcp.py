"""TradingView client using tradingview-ta library for maximum stability."""

import time
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

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class TradingViewClient:
    """
    TradingView client using tradingview-ta library for maximum stability.
    Designed as secondary confirmation layer only.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.enabled = bool(config.get("tradingviewEnabled", False))
        self.cache_ttl = int(config.get("tradingviewCacheTtl", 60))  # seconds
        self.rate_limit_per_minute = int(config.get("tradingviewRateLimit", 30))
        self.timeout = float(config.get("tradingviewTimeout", 5.0))
        self.confidence_boost = float(config.get("tradingviewConfidenceBoost", 0.05))
        
        # Internal state
        self._cache: Dict[str, TVSignalResult] = {}
        self._rate_limit_tracker: Dict[str, float] = {}
        self._health_status = {"healthy": True, "last_check": 0, "fail_count": 0}
        self._max_failures_before_disable = int(config.get("tradingviewMaxFailures", 5))
        self._disabled_until = 0
        
    def is_enabled(self) -> bool:
        """Check if TradingView integration is currently enabled."""
        if not self.enabled:
            return False
        if not TRADINGVIEW_TA_AVAILABLE:
            return False
        if time.time() < self._disabled_until:
            return False
        if not self._health_status["healthy"]:
            return False
        return True
    
    def _check_rate_limit(self, symbol: str) -> bool:
        """Check if we're within rate limits."""
        now = time.time()
        minute_key = f"{symbol}_{int(now // 60)}"
        
        if minute_key in self._rate_limit_tracker:
            if self._rate_limit_tracker[minute_key] >= self.rate_limit_per_minute:
                return False
        
        self._rate_limit_tracker[minute_key] = self._rate_limit_tracker.get(minute_key, 0) + 1
        
        # Clean old entries
        old_keys = [k for k in self._rate_limit_tracker.keys() if k.split("_")[1] != str(int(now // 60))]
        for k in old_keys:
            del self._rate_limit_tracker[k]
        
        return True
    
    def _tv_result_to_dict(self, result: TVSignalResult) -> dict:
        """Serialize a TVSignalResult to a plain dict for per-symbol storage."""
        if result is None:
            return {}
        try:
            _sig = result.signal
            return {
                "signal": getattr(_sig, "value", str(_sig)) if _sig is not None else None,
                "confidence": result.confidence,
                "timestamp": result.timestamp,
                "source": result.source,
                "metadata": result.metadata,
            }
        except Exception:
            return {}

    def _tv_dict_to_result(self, d: dict) -> Optional[TVSignalResult]:
        """Rebuild a TVSignalResult from a persisted per-symbol dict."""
        try:
            _sig = d.get("signal")
            try:
                _sig_enum = TVSignal(_sig) if _sig is not None else TVSignal.NEUTRAL
            except Exception:
                _sig_enum = TVSignal.NEUTRAL
            return TVSignalResult(
                signal=_sig_enum,
                confidence=float(d.get("confidence", 0.0) or 0.0),
                timestamp=float(d.get("timestamp", time.time())),
                source=str(d.get("source", "disk")),
                metadata=d.get("metadata") or {},
            )
        except Exception:
            return None

    def _get_from_cache(self, symbol: str) -> Optional[TVSignalResult]:
        """Get signal from cache if available and not expired."""
        if symbol in self._cache:
            cached = self._cache[symbol]
            if time.time() - cached.timestamp < self.cache_ttl:
                return cached
            else:
                del self._cache[symbol]
        # Fallback: load persisted per-symbol TV signal from disk so a restart
        # or a fresh process does not immediately re-hit the TradingView API.
        try:
            from trading.per_symbol_context import PerSymbolContext
            from trading.shared_cache_layer import SharedCacheLayer
            from services.config_paths import VAULT_DIR
            ctx = PerSymbolContext(str(symbol).upper().strip(), SharedCacheLayer(VAULT_DIR), None)
            d = ctx.get_tv_signal()
            if isinstance(d, dict) and d.get("ts"):
                if time.time() - float(d["ts"]) < self.cache_ttl:
                    res = self._tv_dict_to_result(d)
                    if res is not None:
                        self._cache[symbol] = res
                        return res
        except Exception:
            pass
        return None

    def _store_in_cache(self, symbol: str, result: TVSignalResult):
        """Store signal in cache."""
        self._cache[symbol] = result
        # Persist to per-symbol storage so the signal survives a restart and is
        # shared per-symbol rather than only living in this process cache.
        try:
            from trading.per_symbol_context import PerSymbolContext
            from trading.shared_cache_layer import SharedCacheLayer
            from services.config_paths import VAULT_DIR
            ctx = PerSymbolContext(str(symbol).upper().strip(), SharedCacheLayer(VAULT_DIR), None)
            ctx.save_tv_signal(self._tv_result_to_dict(result))
        except Exception:
            pass

        # Clean old cache entries
        now = time.time()
        old_symbols = [k for k, v in self._cache.items() if now - v.timestamp > self.cache_ttl * 2]
        for s in old_symbols:
            del self._cache[s]
    
    def _update_health(self, success: bool):
        """Update health status based on API call results."""
        now = time.time()
        self._health_status["last_check"] = now
        
        if success:
            self._health_status["fail_count"] = max(0, self._health_status["fail_count"] - 1)
            if self._health_status["fail_count"] == 0:
                self._health_status["healthy"] = True
        else:
            self._health_status["fail_count"] += 1
            if self._health_status["fail_count"] >= self._max_failures_before_disable:
                self._health_status["healthy"] = False
                # Disable for 5 minutes
                self._disabled_until = now + 300
    
    def get_signal(self, symbol: str, internal_signal: str, internal_confidence: float) -> Optional[TVSignalResult]:
        """
        Get TradingView signal for a symbol using tradingview-ta library.
        Returns None if disabled, rate limited, or on error (fallback to internal only).
        """
        if not self.is_enabled():
            return None
        
        if not self._check_rate_limit(symbol):
            return None
        
        # Check cache first
        cached = self._get_from_cache(symbol)
        if cached:
            return cached
        
        try:
            result = self._fetch_from_tradingview(symbol, internal_signal, internal_confidence)
            
            if result:
                self._store_in_cache(symbol, result)
                self._update_health(True)
                return result
            else:
                self._update_health(False)
                return None
                
        except Exception:
            self._update_health(False)
            return None
    
    def _fetch_from_tradingview(self, symbol: str, internal_signal: str, internal_confidence: float) -> Optional[TVSignalResult]:
        """
        Fetch signal from TradingView using tradingview-ta library.
        This provides maximum stability by using a well-tested library.
        """
        try:
            # Use tradingview-ta library for technical analysis
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
            
            # Extract signals from TradingView analysis
            recommend = analysis.summary.get("RECOMMENDATION", "NEUTRAL")
            
            # Convert recommendation to signal
            if recommend in ("STRONG_BUY", "BUY"):
                tv_signal = TVSignal.LONG
                confidence = 0.8 if recommend == "STRONG_BUY" else 0.6
            elif recommend in ("STRONG_SELL", "SELL"):
                tv_signal = TVSignal.SHORT
                confidence = 0.8 if recommend == "STRONG_SELL" else 0.6
            else:
                tv_signal = TVSignal.WAIT
                confidence = 0.3
            
            # Get additional indicators for confidence
            oscillators = analysis.oscillators if hasattr(analysis, 'oscillators') else {}
            moving_averages = analysis.moving_averages if hasattr(analysis, 'moving_averages') else {}
            
            # Calculate overall confidence based on multiple indicators
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
            
        except Exception:
            return None
    
    def confirm_signal(self, tv_result: TVSignalResult, internal_signal: str) -> float:
        """
        Calculate confidence boost based on TradingView confirmation.
        Returns boost amount (0.0 to 1.0).
        """
        if not tv_result or tv_result.signal == TVSignal.ERROR:
            return 0.0
        
        internal_signal_upper = internal_signal.upper()
        
        # Full confirmation
        if tv_result.signal.value == internal_signal_upper:
            return self.confidence_boost * tv_result.confidence
        
        # Conflict - no boost
        if tv_result.signal != TVSignal.WAIT:
            return 0.0
        
        # Neutral signal - small boost
        return self.confidence_boost * 0.3

    def get_position_guidance(self, symbol: str, side: str) -> Optional[Dict[str, Any]]:
        """
        Get TradingView guidance for position management (TP/SL).
        Returns guidance dict with recommendation, oscillators, moving_averages, strength, signal.
        """
        if not self.is_enabled():
            return None
        
        try:
            # Fetch current TradingView analysis
            result = self._fetch_from_tradingview(symbol, side, 0.0)
            if not result:
                return None
            
            return {
                "recommendation": result.metadata.get("recommendation"),
                "oscillators": result.metadata.get("oscillators", {}),
                "moving_averages": result.metadata.get("moving_averages", {}),
                "strength": result.confidence,
                "signal": result.signal.value,
                "timestamp": result.timestamp
            }
        except Exception:
            return None
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get current health status."""
        return {
            "enabled": self.enabled,
            "healthy": self._health_status["healthy"],
            "disabled_until": self._disabled_until,
            "fail_count": self._health_status["fail_count"],
            "cache_size": len(self._cache),
            "last_check": self._health_status["last_check"],
            "tradingview_ta_available": TRADINGVIEW_TA_AVAILABLE
        }
    
    def force_disable(self, duration_seconds: int = 300):
        """Force disable TradingView integration for specified duration."""
        self._disabled_until = time.time() + duration_seconds
        self._health_status["healthy"] = False
    
    def force_enable(self):
        """Force enable TradingView integration."""
        self._disabled_until = 0
        self._health_status["healthy"] = True
        self._health_status["fail_count"] = 0


# Global instance
_tv_client_instance: Optional[TradingViewClient] = None


def get_tv_client(config: Dict[str, Any]) -> TradingViewClient:
    """Get or create TradingView client instance."""
    global _tv_client_instance
    if _tv_client_instance is None:
        _tv_client_instance = TradingViewClient(config)
    return _tv_client_instance


def reset_tv_client():
    """Reset global TradingView client instance."""
    global _tv_client_instance
    _tv_client_instance = None


# Backward compatibility aliases
get_tv_mcp = get_tv_client
reset_tv_mcp = reset_tv_client


async def async_get_position_guidance(tv_client: TradingViewClient, symbol: str, side: str):
    """Async wrapper for get_position_guidance to avoid blocking the event loop."""
    import asyncio
    return await asyncio.to_thread(tv_client.get_position_guidance, symbol, side)
