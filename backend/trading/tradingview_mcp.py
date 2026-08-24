"""TradingView client using tradingview-ta library for maximum stability."""

import time
import json
import threading
import random
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

try:
    import requests as _requests
    REQUESTS_AVAILABLE = True
except ImportError:
    _requests = None
    REQUESTS_AVAILABLE = False

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
        self._cfg: Dict[str, Any] = dict(config or {})
        self._cache: Dict[str, TVSignalResult] = {}
        self._signal_history: Dict[str, Dict[str, Any]] = {}
        self._rate_limit_tracker: Dict[str, float] = {}
        self._negative_cache: Dict[str, float] = {}  # symbol -> last-fail ts (short negative cache)
        self._rate_limit_lock = threading.Lock()
        self._health_status = {"healthy": True, "last_check": 0, "fail_count": 0, "last_error": ""}
        self._last_success_ts = 0.0
        self._disabled_until = 0
        self._symbol_cooldown: Dict[str, float] = {}
        self._consecutive_fails: Dict[str, int] = {}
        self._global_consecutive_fails = 0
        self._disable_round = 0  # consecutive disable rounds for exponential backoff
        # Symbols the scanner API does not know (stock tokens, new listings
        # absent from TV's crypto universe). Tracked so batches skip them
        # instead of repeatedly burning rate-limit and tripping the
        # failure-disable circuit (SNXX/KORU/DRAM/MUU … 0 rows every time).
        self._tv_missing: Dict[str, float] = {}
        self._tv_missing_ttl = 86400.0  # re-probe once per day
        # Proactive universe whitelist: the set of symbols TradingView's
        # crypto scanner actually knows for BINANCE, fetched once per TTL.
        # Symbols absent from it are skipped before analysis entirely, so we
        # never burn a per-symbol timeout / rate-limit slot on them.
        self._tv_universe: set[str] = set()
        self._tv_universe_ts = 0.0
        self._tv_universe_ttl = 86400.0
        self.update_config(config)
        self._load_missing_cache()
        self._load_universe_cache()

    def _missing_cache_path(self) -> str:
        try:
            from services.config_paths import VAULT_DIR
            return str(VAULT_DIR / "shared" / "tv_missing.json")
        except Exception:
            return ""

    def _universe_cache_path(self) -> str:
        try:
            from services.config_paths import VAULT_DIR
            return str(VAULT_DIR / "shared" / "tv_universe.json")
        except Exception:
            return ""

    def _save_universe_cache(self) -> None:
        path = self._universe_cache_path()
        if not path:
            return
        try:
            from pathlib import Path
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(
                json.dumps({"ts": self._tv_universe_ts, "symbols": sorted(self._tv_universe)},
                           ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _load_universe_cache(self) -> None:
        path = self._universe_cache_path()
        if not path:
            return
        try:
            from pathlib import Path
            raw = Path(path).read_text(encoding="utf-8")
            d = json.loads(raw)
            self._tv_universe = set(str(s) for s in (d.get("symbols") or []))
            self._tv_universe_ts = float(d.get("ts", 0.0) or 0.0)
        except Exception:
            self._tv_universe = set()
            self._tv_universe_ts = 0.0

    def get_tv_universe(self, force_refresh: bool = False) -> set[str]:
        """Return the set of BINANCE USDT symbols TradingView's crypto scanner
        knows (base names, '.P'/'.PERP' suffix stripped). Fetched once per TTL
        and persisted to disk; returns the cached set on failure so a network
        blip never empties the whitelist. Empty set means 'unknown yet'."""
        now = time.time()
        if not force_refresh and self._tv_universe and (now - self._tv_universe_ts) < self._tv_universe_ttl:
            return self._tv_universe
        try:
            resp = _requests.post(
                self._SCAN_URL,
                json={"filter": [{"left": "exchange", "operation": "equal", "right": "BINANCE"}],
                      "columns": ["name"], "options": {"lang": "en"}},
                timeout=min(20.0, max(5.0, self.timeout * 2)),
            )
            if resp.status_code == 200:
                rows = (resp.json() or {}).get("data", [])
                univ: set[str] = set()
                for row in rows:
                    ticker = str(row.get("s", ""))
                    base = ticker.split(":", 1)[-1] if ":" in ticker else ticker
                    for suffix in (".P", ".PERP"):
                        if base.endswith(suffix):
                            base = base[: -len(suffix)]
                            break
                    if base.endswith("USDT"):
                        univ.add(base)
                if univ:
                    self._tv_universe = univ
                    self._tv_universe_ts = now
                    self._save_universe_cache()
            return self._tv_universe
        except Exception:
            return self._tv_universe

    def is_tv_known(self, symbol: str) -> bool:
        """True if symbol is in the known TV universe. Without a loaded
        universe (first run / fetch never succeeded) this returns True so we
        fall back to the learned _tv_missing list instead of skipping
        everything."""
        if not self._tv_universe:
            return True
        return str(symbol).upper() in self._tv_universe

    def _save_missing_cache(self) -> None:
        """Persist the miss list so a restart does not re-arm the fail chain."""
        path = self._missing_cache_path()
        if not path:
            return
        try:
            from pathlib import Path
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(
                json.dumps(self._tv_missing, ensure_ascii=False), encoding="utf-8"
            )
        except Exception:
            pass

    def _load_missing_cache(self) -> None:
        path = self._missing_cache_path()
        if not path:
            return
        try:
            from pathlib import Path
            p = Path(path)
            if p.exists():
                data = json.loads(p.read_text(encoding="utf-8"))
                now = time.time()
                self._tv_missing = {
                    s: float(ts) for s, ts in data.items()
                    if (now - float(ts)) < self._tv_missing_ttl
                }
        except Exception:
            self._tv_missing = {}

    def update_config(self, config: Dict[str, Any]) -> None:
        self._cfg = dict(config or {})
        self.enabled = bool(config.get("tradingviewEnabled", False))
        self.cache_ttl = int(config.get("tradingviewCacheTtl", 300))
        self.rate_limit_per_minute = int(config.get("tradingviewRateLimit", 6))
        self.timeout = float(config.get("tradingviewTimeout", 10.0))
        self.confidence_boost = float(config.get("tradingviewConfidenceBoost", 0.08))
        self.staleness_threshold = int(config.get("tradingviewStalenessThreshold", 900))
        self._max_failures_before_disable = int(config.get("tradingviewMaxFailures", 10))
        self._negative_cache_ttl = int(config.get("tradingviewNegativeCacheTtl", 60))

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
            # 2026-08-22: after consuming a rate-limit slot, impose a tiny global
            # spacing so a tight scan loop cannot burn all 20 global slots in one
            # burst and trip the 429 circuit. Cheap sleep only when near the cap.
            if self._rate_limit_tracker.get(global_key, 0) >= self.rate_limit_per_minute:
                time.sleep(2.0)

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
            self._negative_cache.clear()  # recovered: allow retries
            self._health_status["last_error"] = ""
            self._last_success_ts = now
            if self._health_status["fail_count"] == 0:
                self._health_status["healthy"] = True
                self._disable_round = 0  # recovered -> backoff resets
        else:
            self._health_status["fail_count"] += 1
            if self._health_status["fail_count"] >= self._max_failures_before_disable:
                self._health_status["healthy"] = False
                # 2026-08-22: exponential backoff on the rate-limit circuit. A flat
                # 300s disable just retries at the same burst rate and re-trips 429
                # forever (observed: fail_count stuck at 10-11, TV never recovers).
                # Each successive disable round doubles the cooldown (5/10/20/40/80m
                # cap) so the free API quota actually refills between retries.
                self._disable_round += 1
                _backoff = 300 * (2 ** min(self._disable_round - 1, 4))
                self._disabled_until = now + _backoff

    def get_signal(self, symbol: str, internal_signal: str, internal_confidence: float, force_refresh: bool = False) -> Optional[TVSignalResult]:
        if not self.is_enabled():
            return None

        # Known-absent symbol: do not burn a per-symbol fetch on it.
        # Checked even for force_refresh: batch_fetch_signals re-probes the
        # analyzed set + open positions every scan cycle and auto-heals the
        # miss list (pop on return), so a stale miss self-recovers within TTL.
        miss_ts = self._tv_missing.get(symbol)
        if miss_ts and (time.time() - miss_ts) < self._tv_missing_ttl:
            return None

        cached = self._get_from_cache(symbol, force_refresh=force_refresh)
        if cached and not force_refresh:
            self._consecutive_fails.pop(symbol, None)
            return cached

        # 2026-08-22: NEGATIVE CACHE. If this symbol just failed (429 / network /
        # empty), do not re-hammer the API on every loop tick — return None
        # immediately (without consuming a rate-limit slot) until the short
        # negative TTL expires. This is the sustainable rate-limit defense: a
        # failed symbol stays quiet instead of burning quota every 30-60s.
        _neg = self._negative_cache.get(symbol, 0)
        if _neg and (time.time() - _neg) < self._negative_cache_ttl:
            return None

        if not self._check_rate_limit(symbol):
            return None

        try:
            result = self._fetch_from_tradingview(symbol, internal_signal, internal_confidence)

            if result:
                self._store_in_cache(symbol, result)
                self._consecutive_fails.pop(symbol, None)
                self._negative_cache.pop(symbol, None)
                self._global_consecutive_fails = max(0, self._global_consecutive_fails - 1)
                self._update_health(True)
                return result
            else:
                self._consecutive_fails[symbol] = self._consecutive_fails.get(symbol, 0) + 1
                self._global_consecutive_fails += 1
                self._compute_symbol_cooldown(symbol)
                self._negative_cache[symbol] = time.time()
                self._update_health(False)
                return None

        except Exception:
            self._consecutive_fails[symbol] = self._consecutive_fails.get(symbol, 0) + 1
            self._global_consecutive_fails += 1
            self._compute_symbol_cooldown(symbol)
            self._negative_cache[symbol] = time.time()
            self._update_health(False)
            return None

    def _fetch_from_tradingview(self, symbol: str, internal_signal: str, internal_confidence: float) -> Optional[TVSignalResult]:
        try:
            # Use the direct scanner client (tradingview_ta v3.3.0 endpoints are
            # dead: symbol-search -> 403, technicals JS -> 404). ScannerClient
            # mirrors the .summary / .oscillators interface the code below expects.
            from trading.tradingview_scanner import ScannerClient
            handler = ScannerClient(
                symbol=symbol,
                screener="CRYPTO",
                exchange="BINANCE",
                interval=Interval.INTERVAL_1_HOUR,
                timeout=self.timeout,
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

    # ── Batch fetch: N symbols in ONE request (TV scanner API) ──────────────
    # tradingview_ta's get_analysis() issues 1 HTTP request per symbol, which
    # trips the free API's rate limit (429) when scanning many symbols. The
    # underlying scanner endpoint accepts many tickers per request, so we call
    # it directly and rebuild the same Analysis-shaped dict tradingview_ta
    # would have produced. Results are cached per-symbol exactly like the
    # single-symbol path, so all downstream consumers (confluence, position
    # guidance) work unchanged.
    _SCAN_URL = "https://scanner.tradingview.com/crypto/scan"
    _SCAN_COLUMNS = [
        "name", "close",
        "Recommend.All", "Recommend.MA", "Recommend.Other",
        "RSI", "RSI|1",
        "Stoch.K", "Stoch.D", "Stoch.K|1", "Stoch.D|1",
        "CCI20", "CCI20|1",
        "MACD.macd", "MACD.signal",
        "EMA10", "SMA10", "EMA20", "SMA20", "EMA30", "SMA30",
        "EMA50", "SMA50", "EMA100", "SMA100", "EMA200", "SMA200",
    ]

    @staticmethod
    def _tv_rec_from_score(score) -> str:
        """Map scanner score (-1..1) to tradingview_ta RECOMMENDATION string.

        Mirrors Compute.Recommend() exactly (>= -1..<-0.5 STRONG_SELL,
        >= -0.5..<-0.1 SELL, >= -0.1..<=0.1 NEUTRAL, >0.1..<=0.5 BUY,
        >0.5..<=1 STRONG_BUY).
        """
        if score is None:
            return "NEUTRAL"
        try:
            score = float(score)
        except (TypeError, ValueError):
            return "NEUTRAL"
        if -1 <= score < -0.5:
            return "STRONG_SELL"
        if -0.5 <= score < -0.1:
            return "SELL"
        if -0.1 <= score <= 0.1:
            return "NEUTRAL"
        if 0.1 < score <= 0.5:
            return "BUY"
        if 0.5 < score <= 1:
            return "STRONG_BUY"
        return "NEUTRAL"

    @staticmethod
    def _tv_osc_signal(name: str, v: dict) -> str:
        """Recompute oscillator BUY/SELL/NEUTRAL using tradingview_ta thresholds."""
        if name == "RSI":
            rsi, rsi1 = v.get("RSI"), v.get("RSI|1")
            if rsi is None or rsi1 is None:
                return "NEUTRAL"
            if rsi < 30 and rsi1 < rsi:
                return "BUY"
            if rsi > 70 and rsi1 > rsi:
                return "SELL"
            return "NEUTRAL"
        if name == "STOCH.K":
            k, d, k1, d1 = v.get("Stoch.K"), v.get("Stoch.D"), v.get("Stoch.K|1"), v.get("Stoch.D|1")
            if None in (k, d, k1, d1):
                return "NEUTRAL"
            if k < 20 and d < 20 and k > d and k1 < d1:
                return "BUY"
            if k > 80 and d > 80 and k < d and k1 > d1:
                return "SELL"
            return "NEUTRAL"
        if name == "CCI":
            cci, cci1 = v.get("CCI20"), v.get("CCI20|1")
            if cci is None or cci1 is None:
                return "NEUTRAL"
            if cci < -100 and cci > cci1:
                return "BUY"
            if cci > 100 and cci < cci1:
                return "SELL"
            return "NEUTRAL"
        if name == "MACD":
            macd, sig = v.get("MACD.macd"), v.get("MACD.signal")
            if macd is None or sig is None:
                return "NEUTRAL"
            return "BUY" if macd > sig else ("SELL" if macd < sig else "NEUTRAL")
        return "NEUTRAL"

    @staticmethod
    def _tv_ma_signal(close, ma) -> str:
        if close is None or ma is None:
            return "NEUTRAL"
        return "BUY" if ma < close else ("SELL" if ma > close else "NEUTRAL")

    def _build_batch_result(self, symbol: str, v: dict) -> Optional[TVSignalResult]:
        """Rebuild a TVSignalResult from one scanner row (same shape as _fetch_from_tradingview)."""
        try:
            recommend = self._tv_rec_from_score(v.get("Recommend.All"))
            if recommend in ("STRONG_BUY", "BUY"):
                tv_signal = TVSignal.LONG
                confidence = 0.8 if recommend == "STRONG_BUY" else 0.6
            elif recommend in ("STRONG_SELL", "SELL"):
                tv_signal = TVSignal.SHORT
                confidence = 0.8 if recommend == "STRONG_SELL" else 0.6
            else:
                tv_signal = TVSignal.WAIT
                confidence = 0.3

            # Oscillators — same structure tradingview_ta emits (incl. COMPUTE sub-dict)
            osc_compute = {
                "RSI": self._tv_osc_signal("RSI", v),
                "STOCH.K": self._tv_osc_signal("STOCH.K", v),
                "CCI": self._tv_osc_signal("CCI", v),
                "MACD": self._tv_osc_signal("MACD", v),
            }
            osc_counter = {"BUY": 0, "SELL": 0, "NEUTRAL": 0}
            for s in osc_compute.values():
                osc_counter[s] = osc_counter.get(s, 0) + 1
            osc_rec = self._tv_rec_from_score(v.get("Recommend.Other"))
            oscillators = {
                "RECOMMENDATION": osc_rec,
                "BUY": osc_counter.get("BUY", 0),
                "SELL": osc_counter.get("SELL", 0),
                "NEUTRAL": osc_counter.get("NEUTRAL", 0),
                "COMPUTE": osc_compute,
            }

            # Moving averages — same structure
            close = v.get("close")
            ma_compute = {}
            ma_counter = {"BUY": 0, "SELL": 0, "NEUTRAL": 0}
            for col in ("EMA10", "SMA10", "EMA20", "SMA20", "EMA30", "SMA30",
                        "EMA50", "SMA50", "EMA100", "SMA100", "EMA200", "SMA200"):
                sig = self._tv_ma_signal(close, v.get(col))
                ma_compute[col] = sig
                ma_counter[sig] = ma_counter.get(sig, 0) + 1
            ma_rec = self._tv_rec_from_score(v.get("Recommend.MA"))
            moving_averages = {
                "RECOMMENDATION": ma_rec,
                "BUY": ma_counter.get("BUY", 0),
                "SELL": ma_counter.get("SELL", 0),
                "NEUTRAL": ma_counter.get("NEUTRAL", 0),
                "COMPUTE": ma_compute,
            }

            osc_buy = int(oscillators.get("BUY", 0))
            osc_sell = int(oscillators.get("SELL", 0))
            ma_buy = int(moving_averages.get("BUY", 0))
            ma_sell = int(moving_averages.get("SELL", 0))
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
                source="tradingview-scan-batch",
                metadata={
                    "recommendation": recommend,
                    "oscillators": oscillators,
                    "moving_averages": moving_averages,
                    "strength": strength
                }
            )
        except Exception:
            return None

    def batch_fetch_signals(self, symbols: List[str], force_refresh: bool = False) -> Dict[str, TVSignalResult]:
        """Fetch TV analysis for many symbols in a single HTTP request.

        Uses the same scanner endpoint tradingview_ta calls, but batched —
        N symbols = 1 request, which avoids the per-symbol 429 rate limit.
        Results are stored in the per-symbol cache so get_signal() /
        get_position_guidance() / confluence all see them without refetching.

        Returns {symbol: TVSignalResult} for the symbols the API returned.
        """
        if not self.is_enabled():
            return {}
        if not REQUESTS_AVAILABLE:
            return {}
        if not symbols:
            return {}

        symbols = list(dict.fromkeys(str(s).upper().strip() for s in symbols if str(s).strip()))
        if not symbols:
            return {}

        # Skip symbols already cached & fresh unless force_refresh; also skip
        # symbols known to be absent from the scanner API (miss cache).
        now = time.time()
        need = []
        for s in symbols:
            if force_refresh:
                need.append(s)
                continue
            if self._tv_missing.get(s, 0) and (now - self._tv_missing.get(s, 0)) < self._tv_missing_ttl:
                continue  # known absent — do not burn rate limit on it
            cached = self._get_from_cache(s, force_refresh=False, skip_stale_disk=True, require_fresh=True)
            if cached is None:
                need.append(s)
        if not need:
            return {}

        # Rate limit: one batch = one request (counts against global cap only)
        with self._rate_limit_lock:
            now = time.time()
            minute_key = f"_batch_{int(now // 60)}"
            _rate_limit_per_min = int(float(self._cfg.get("tvBatchRateLimitPerMin", 4) or 4))
            if self._rate_limit_tracker.get(minute_key, 0) >= _rate_limit_per_min:
                return {}
            self._rate_limit_tracker[minute_key] = self._rate_limit_tracker.get(minute_key, 0) + 1

        tickers = [f"BINANCE:{s}" for s in need]
        try:
            resp = _requests.post(
                self._SCAN_URL,
                json={"symbols": {"tickers": tickers}, "columns": self._SCAN_COLUMNS},
                timeout=self.timeout,
            )
            if resp.status_code != 200:
                # 429 handling with exponential backoff
                if resp.status_code == 429:
                    backoff = float(self._cfg.get("tvBatchRetryBackoff", 2.0) or 2.0)
                    time.sleep(backoff)
                self._health_status["last_error"] = f"HTTP {resp.status_code}: {resp.text[:200]}"
                self._update_health(False)
                return {}
            data = (resp.json() or {}).get("data", [])
            if not data:
                # 0 rows is a legitimate response when every requested symbol
                # is absent from the scanner's crypto universe (stock tokens,
                # delisted names). That is NOT an API failure — mark them
                # missing so future batches skip them, and keep the client
                # healthy. Only HTTP errors / exceptions trip the circuit.
                for s in need:
                    self._tv_missing[s] = now
                self._save_missing_cache()
                return {}

            results: Dict[str, TVSignalResult] = {}
            found = set()
            for row in data:
                ticker = str(row.get("s", ""))
                sym = ticker.split(":", 1)[-1] if ":" in ticker else ticker
                found.add(sym)
                vals = dict(zip(self._SCAN_COLUMNS, row.get("d", [])))
                res = self._build_batch_result(sym, vals)
                if res is not None:
                    results[sym] = res
                    self._store_in_cache(sym, res)
                    self._consecutive_fails.pop(sym, None)
                    # Symbol came back on TV — clear its miss entry (auto-heal).
                    if sym in self._tv_missing:
                        self._tv_missing.pop(sym, None)
            # Symbols we asked for but the API did not return are absent
            # from TV — remember them so future batches skip them.
            miss_changed = False
            for s in need:
                if s not in found and s not in self._tv_missing:
                    self._tv_missing[s] = now
                    miss_changed = True
            if miss_changed:
                self._save_missing_cache()
            # Partial success is not a failure (rate-limit/HTTP errors are).
            self._update_health(True)
            self._global_consecutive_fails = max(0, self._global_consecutive_fails - 1)
            return results
        except Exception as exc:
            self._health_status["last_error"] = f"{type(exc).__name__}: {exc}"[:240]
            self._update_health(False)
            return {}

    def confirm_signal(self, tv_result: TVSignalResult, internal_signal: str) -> float:
        if not tv_result or tv_result.signal == TVSignal.ERROR:
            return 0.0

        internal_signal_upper = internal_signal.upper()
        boost = self.confidence_boost
        strength = float(getattr(tv_result.metadata, "get", lambda *a, **k: 0.0)("strength", 0.0) or 0.0)

        if tv_result.signal.value == internal_signal_upper:
            return boost * tv_result.confidence

        if tv_result.signal != TVSignal.WAIT and tv_result.signal.value != internal_signal_upper:
            # Strong disagreement escalates the penalty. TV is a secondary
            # signal, so a hard conflict (strength >= 0.75) returns a sentinel
            # that confluence interprets as a block — entries against TV are
            # the #1 loser (DEXEUSDT/KAITOUSDT opened LONG vs TV SHORT 0.69+).
            # Directional-conflict guard (2026-08-15): if TV and the internal
            # signal are OPPOSITE sides (LONG vs SHORT), block whenever TV
            # strength clears tvConflictBlockStrength (default 0.60) — not
            # just at 0.75. The 2026-08-15 session showed SHORT entries opened
            # against TV=LONG (strength ~0.6-0.7) bled badly (SHORT WR 22%,
            # -3.27 USDT in one day) because the soft penalty (-0.072) did not
            # stop them. A genuine opposite-side TV read is a hard conflict.
            block_strength = float(getattr(self, "tv_conflict_block_strength", 0.60) or 0.60)
            try:
                _cfg = getattr(self, "_live_cfg", None) or {}
                block_strength = float(_cfg.get("tvConflictBlockStrength", block_strength) or block_strength)
            except Exception:
                pass
            if strength >= block_strength:
                return -999.0  # hard block sentinel (opposite side)
            if strength >= 0.45:
                return -1.5 * boost * tv_result.confidence  # ~-0.072 @ conf 0.6
            return -0.5 * boost * tv_result.confidence

        return boost * 0.3

    def get_position_guidance(self, symbol: str, side: str, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        if not self.is_enabled():
            return None

        # Known-absent symbol: do not burn a fetch (same guard as get_signal).
        # The guardian calls this every cycle for open positions; a symbol
        # TradingView does not know would otherwise time out per cycle
        # (10s each), trip the rate limiter, and eventually disable TV.
        miss_ts = self._tv_missing.get(symbol)
        if miss_ts and (time.time() - miss_ts) < self._tv_missing_ttl:
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
        now = time.time()
        # Auto-recover if disable window has expired
        if not self._health_status["healthy"] and now >= self._disabled_until:
            self._health_status["healthy"] = True
            self._health_status["fail_count"] = 0
        # A missing library is a hard, non-transient failure — never report
        # healthy for it. This was the silent-death case: is_enabled() returns
        # False but get_health_status() still said healthy=True with no error.
        if not TRADINGVIEW_TA_AVAILABLE:
            self._health_status["healthy"] = False
            self._health_status["last_error"] = "tradingview_ta library not installed"
            self._health_status["fail_count"] = max(self._health_status["fail_count"], 1)
        # Staleness: TV may be "healthy" by the fail-counter but have not
        # produced a successful signal in a long time (silent degradation).
        _stale_sec = float(self._cfg.get("tvStaleSec", 900) or 900)
        _stale = self._last_success_ts > 0 and (now - self._last_success_ts) > _stale_sec
        if _stale and self._health_status["healthy"]:
            self._health_status["healthy"] = False
            self._health_status["last_error"] = (
                f"TV signal stale: {(now - self._last_success_ts):.0f}s since last success (> {_stale_sec:.0f}s)"
            )
        last_error = self._health_status.get("last_error", "")
        if "429" in last_error:
            error_type = "rate_limited"
        elif last_error and ("timeout" in last_error.lower() or "connection" in last_error.lower()):
            error_type = "connection"
        elif "not installed" in last_error:
            error_type = "missing_lib"
        elif "stale" in last_error:
            error_type = "stale"
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


def attempt_tv_soft_recovery(reason: str = "") -> bool:
    """Drop the cached singleton so the next call重建s a fresh client.

    This is the cheap, in-process self-heal: it clears rate-limit trackers,
    symbol cooldowns and the stale cache that can accumulate after a TV
    outage. Returns True if a reset was performed. The caller (main loop)
    decides whether to escalate to a full backend restart.
    """
    global _tv_client_instance
    inst = _tv_client_instance
    if inst is None:
        return False
    try:
        inst._rate_limit_tracker.clear()
        inst._symbol_cooldown.clear()
        inst._consecutive_fails.clear()
        inst._health_status["fail_count"] = 0
        inst._disabled_until = 0
        inst._last_success_ts = 0.0
        inst._health_status["last_error"] = f"soft-recovered: {reason}"[:240]
        # Keep healthy False until the next successful fetch re-affirms it,
        # but allow is_enabled() to retry immediately (disabled_until=0).
        inst._health_status["healthy"] = False
    except Exception:
        pass
    _tv_client_instance = None
    return True


# Backward compatibility aliases
get_tv_mcp = get_tv_client
reset_tv_mcp = reset_tv_client


async def async_get_position_guidance(tv_client: TradingViewClient, symbol: str, side: str):
    """Async wrapper for get_position_guidance to avoid blocking the event loop."""
    import asyncio
    return await asyncio.to_thread(tv_client.get_position_guidance, symbol, side)
