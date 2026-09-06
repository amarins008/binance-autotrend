"""Binance USD-M futures HTTP client layer."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import time

import httpx
from fastapi import HTTPException

from exceptions import ExchangeError
from logger import get_logger, log_exception
from services import app_state
from services.cache_registry import (
    DATA_GET_CONNECT_TIMEOUT_SEC,
    DATA_GET_MAX_ATTEMPTS,
    DATA_GET_TIMEOUT_SEC,
    _DATA_PROVIDER_HEALTH,
    _EXCHANGE_FILTERS_CACHE,
    _EXCHANGE_FILTERS_CACHE_TTL,
    _EXCHANGE_FILTERS_LOCK,
)

BINANCE_RECV_WINDOW_MS = int(os.getenv("BINANCE_RECV_WINDOW_MS", "10000"))
CONNECTOR_MODE = os.getenv("CONNECTOR_MODE", "auto").lower()

_HTTP: httpx.AsyncClient | None = None
_DATA_HTTP: httpx.AsyncClient | None = None

_log = get_logger("exchange.binance_client")


class BinanceClient:
    def __init__(self) -> None:
        self.http: httpx.AsyncClient | None = None
        self.data_http: httpx.AsyncClient | None = None

    def configure(self, http: httpx.AsyncClient | None, data_http: httpx.AsyncClient | None) -> None:
        global _HTTP, _DATA_HTTP
        self.http = http
        self.data_http = data_http
        _HTTP = http
        _DATA_HTTP = data_http


_client = BinanceClient()


def get_client() -> BinanceClient:
    return _client


def configure_clients(http: httpx.AsyncClient | None, data_http: httpx.AsyncClient | None) -> None:
    _client.configure(http, data_http)


def _data_provider_cooldown_active() -> bool:
    until = int(_DATA_PROVIDER_HEALTH.get("cooldownUntil", 0) or 0)
    if until > int(time.time()):
        return True
    # Cooldown expired — reset streak so old errors don't carry over
    if until > 0 and _DATA_PROVIDER_HEALTH.get("streak", 0):
        _DATA_PROVIDER_HEALTH["streak"] = 0
        _DATA_PROVIDER_HEALTH["cooldownUntil"] = 0
    return False


def _record_data_provider_health(ok: bool, err: Exception | str | None = None, path: str = "") -> None:
    now = int(time.time())
    if ok:
        _DATA_PROVIDER_HEALTH["streak"] = 0
        _DATA_PROVIDER_HEALTH["cooldownUntil"] = 0
        return
    # Do NOT record health for the synthetic "cooldown active" sentinel error —
    # recording it would amplify the streak and extend cooldown indefinitely.
    err_str = str(err or "")
    if "data provider cooldown active" in err_str:
        return
    streak = int(_DATA_PROVIDER_HEALTH.get("streak", 0) or 0) + 1
    # Cap cooldown at 90s; only trigger after 3 consecutive real network failures.
    cooldown_sec = min(90, 5 * streak) if streak >= 3 else 0
    _DATA_PROVIDER_HEALTH["streak"] = streak
    _DATA_PROVIDER_HEALTH["lastErrorAt"] = now
    _DATA_PROVIDER_HEALTH["lastError"] = err_str[:160]
    if path:
        _DATA_PROVIDER_HEALTH["lastPath"] = str(path)[:120]
    if cooldown_sec:
        _DATA_PROVIDER_HEALTH["cooldownUntil"] = now + cooldown_sec
    app_state.AUTO_TRADE["lastDataProviderError"] = {
        "ts": now,
        "streak": streak,
        "cooldownUntil": int(_DATA_PROVIDER_HEALTH.get("cooldownUntil", 0) or 0),
        "error": err_str[:160],
        "path": str(path or "")[:120],
    }


def _resolve_umfutures_class():
    import services.cache_registry as cache_registry

    if cache_registry._UMFUTURES_CLASS is not None:
        return cache_registry._UMFUTURES_CLASS
    try:
        from binance.um_futures import UMFutures as resolved_umfutures
    except Exception:
        resolved_umfutures = None
    cache_registry._UMFUTURES_CLASS = resolved_umfutures
    return cache_registry._UMFUTURES_CLASS

async def _public_get(url: str) -> httpx.Response:
    """Use long-lived client from app lifespan; fallback for scripts/tests."""
    if _HTTP is not None:
        try:
            return await asyncio.wait_for(_HTTP.get(url), timeout=10.0)
        except (httpx.RemoteProtocolError, httpx.LocalProtocolError, httpx.ReadError, asyncio.TimeoutError):
            pass  # stale connection or timeout — fall through to fresh client
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0)) as c:
        return await c.get(url)

def _is_retryable_http_exc(err: Exception) -> bool:
    txt = str(err).lower()
    if isinstance(err, (asyncio.TimeoutError, httpx.RequestError, httpx.TimeoutException, httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadError, httpx.PoolTimeout)):
        return True
    return ("getaddrinfo failed" in txt) or ("name or service not known" in txt) or ("temporary failure in name resolution" in txt)

async def _data_get(path: str) -> httpx.Response:
    """
    Fast GET for public market data on fapi.binance.com mainnet.
    Uses the persistent _DATA_HTTP client (connection pooling) when available to avoid
    TLS handshake overhead on every request. Falls back to a fresh client for
    scripts/tests where the lifespan client is not initialized.
    path must start with /fapi/...
    """
    full_url = f"https://fapi.binance.com{path}"
    if _data_provider_cooldown_active():
        raise httpx.ConnectTimeout("data provider cooldown active")
    last_err: Exception | None = None
    for attempt in range(DATA_GET_MAX_ATTEMPTS):
        try:
            # Prefer the long-lived pooled client to avoid per-request TLS overhead
            if _DATA_HTTP is not None:
                try:
                    res = await asyncio.wait_for(
                        _DATA_HTTP.get(full_url, headers={"Accept-Encoding": "identity"}),
                        timeout=DATA_GET_TIMEOUT_SEC,
                    )
                    _record_data_provider_health(True)
                    return res
                except (httpx.RemoteProtocolError, httpx.LocalProtocolError, httpx.ReadError):
                    pass  # stale connection — fall through to fresh client below
            # Fallback: fresh client (scripts / tests / stale pooled connection)
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(DATA_GET_TIMEOUT_SEC, connect=DATA_GET_CONNECT_TIMEOUT_SEC),
                headers={"Accept-Encoding": "identity"},
            ) as c:
                res = await c.get(full_url)
            _record_data_provider_health(True)
            return res
        except Exception as e:
            last_err = e
            _record_data_provider_health(False, e, path)
            # Stop retrying once we are on the final attempt, or on non-transient errors.
            if attempt + 1 >= DATA_GET_MAX_ATTEMPTS or not _is_retryable_http_exc(e):
                if not isinstance(e, (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, asyncio.TimeoutError)):
                    log_exception(_log, e, {"path": path, "symbol": path.split("symbol=")[-1][:16] if "symbol=" in path else ""})
                raise ExchangeError(f"market data fetch failed: {e}") from e
            await asyncio.sleep(0.35 * (attempt + 1))

    if last_err is not None:
        raise ExchangeError(f"market data fetch failed after {DATA_GET_MAX_ATTEMPTS} attempts: {last_err}") from last_err
    raise ExchangeError("market data fetch failed")

def _binance_base():
    # Default to mainnet (false) so LIVE mode never accidentally hits testnet.
    return "https://testnet.binancefuture.com" if os.getenv("BINANCE_TESTNET", "false").lower() == "true" else "https://fapi.binance.com"


def _safe_api_key_prefix(key: str | None) -> str:
    text = str(key or "")
    return text[:6] if text else ""


def _signed_request_diagnostic(base: str, endpoint: str, key: str | None, params: dict | None = None) -> dict:
    params = params if isinstance(params, dict) else {}
    base_text = str(base or "")
    endpoint_text = str(endpoint or "")
    market_type = (
        "USDT-M"
        if "/fapi/" in endpoint_text or "fapi.binance.com" in base_text or "binancefuture.com" in base_text
        else "COIN-M"
        if "/dapi/" in endpoint_text or "dapi.binance.com" in base_text
        else "SPOT"
        if "api.binance.com" in base_text
        else "UNKNOWN"
    )
    return {
        "keyPrefix": _safe_api_key_prefix(key),
        "base": base_text,
        "endpoint": endpoint_text,
        "marketType": market_type,
        "symbol": str(params.get("symbol", "") or ""),
        "orderType": str(params.get("type", "") or ""),
        "side": str(params.get("side", "") or ""),
        "positionSide": str(params.get("positionSide", "") or ""),
        "reduceOnly": params.get("reduceOnly"),
        "binanceTestnet": os.getenv("BINANCE_TESTNET", "false").lower() == "true",
    }

_TIME_OFFSET_MS = 0
_TIME_OFFSET_LAST_SYNC = 0.0
_TIME_OFFSET_LOCK = asyncio.Lock()
# In-flight flag so the background loop and the reactive recovery path in
# autotrade_loop don't fire two /fapi/v1/time calls at the same instant.
_TIME_SYNC_INFLIGHT = False


def get_server_time_offset_ms() -> int:
    """Public accessor for the signed server/local time offset (ms).

    Returns the offset measured by the last successful `/fapi/v1/time` sync,
    or 0 if no successful sync has happened yet. Positive means local clock
    is behind the Binance server; negative means local is ahead.
    """
    try:
        return int(_TIME_OFFSET_MS)
    except Exception:
        return 0


def get_server_time_sync_age_sec() -> float:
    """How long ago the last `/fapi/v1/time` sync ran. Returns +inf if never."""
    try:
        last = float(_TIME_OFFSET_LAST_SYNC or 0.0)
        if last <= 0:
            return float("inf")
        return max(0.0, time.time() - last)
    except Exception:
        return float("inf")

async def sync_server_time() -> bool:
    """Proactive time sync — call on startup or periodically so the infra
    health gate never blocks entries just because no signed request has run yet.
    Returns True if sync succeeded.

    Safe to call concurrently: an in-flight guard short-circuits a second
    caller to avoid stampeding `/fapi/v1/time` when both the background loop
    and the autotrade recovery path try to resync at the same instant.
    """
    global _TIME_OFFSET_MS, _TIME_OFFSET_LAST_SYNC, _TIME_SYNC_INFLIGHT
    if _TIME_SYNC_INFLIGHT:
        # Another task is already syncing — let it finish and reuse the result.
        return get_server_time_sync_age_sec() < 60.0
    _TIME_SYNC_INFLIGHT = True
    try:
        local_before = int(time.time() * 1000)
        client = _HTTP
        base = _binance_base()
        if client is not None:
            st = await client.get(f"{base}/fapi/v1/time")
        else:
            async with httpx.AsyncClient(timeout=5.0) as c:
                st = await c.get(f"{base}/fapi/v1/time")
        local_after = int(time.time() * 1000)
        local_midpoint = (local_before + local_after) // 2
        server_ms = int(st.json().get("serverTime", local_midpoint))
        new_offset = server_ms - local_midpoint - 150
        old_offset = _TIME_OFFSET_MS
        _TIME_OFFSET_MS = new_offset
        _TIME_OFFSET_LAST_SYNC = time.time()
        # Surface large drift so the background loop's job is visible in logs.
        try:
            drift_ms = new_offset - int(old_offset or 0)
            if abs(drift_ms) >= 250:
                print(
                    f"[time-sync] ok offset={new_offset:+d}ms drift={drift_ms:+d}ms",
                    flush=True,
                )
        except Exception:
            pass
        return True
    except Exception:
        return False
    finally:
        _TIME_SYNC_INFLIGHT = False


async def _signed_request(method: str, base: str, endpoint: str, key: str, secret: str, params: dict):
    global _TIME_OFFSET_MS, _TIME_OFFSET_LAST_SYNC
    params = dict(params)
    diagnostic = _signed_request_diagnostic(base, endpoint, key, params)
    
    now_ts = time.time()
    if now_ts - _TIME_OFFSET_LAST_SYNC > 60.0:
        async with _TIME_OFFSET_LOCK:
            # Double check pattern
            if now_ts - _TIME_OFFSET_LAST_SYNC > 60.0:
                try:
                    local_before = int(time.time() * 1000)
                    client = _HTTP
                    if client is not None:
                        st = await client.get(f"{base}/fapi/v1/time")
                    else:
                        async with httpx.AsyncClient(timeout=5.0) as c:
                            st = await c.get(f"{base}/fapi/v1/time")
                    local_after = int(time.time() * 1000)
                    # Use the midpoint of roundtrip to calculate offset accurately
                    local_midpoint = (local_before + local_after) // 2
                    server_ms = int(st.json().get("serverTime", local_midpoint))
                    # Prevent setting offset that places us ahead of server by subtracting a small buffer
                    _TIME_OFFSET_MS = server_ms - local_midpoint - 150
                    _TIME_OFFSET_LAST_SYNC = time.time()
                except Exception:
                    pass

    # Apply offset, making sure we are slightly behind server time (safe side) rather than ahead
    server_ms = int(time.time() * 1000) + _TIME_OFFSET_MS
    params["timestamp"] = server_ms
    params["recvWindow"] = BINANCE_RECV_WINDOW_MS
    query = "&".join(f"{k}={v}" for k, v in params.items())
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    signed = f"{query}&signature={sig}"
    headers = {"X-MBX-APIKEY": key}
    url = f"{base}{endpoint}?{signed}"
    res: httpx.Response | None = None
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            client = _HTTP
            if client is not None:
                try:
                    if method == "POST":
                        res = await client.post(url, headers=headers)
                    else:
                        res = await client.get(url, headers=headers)
                except (httpx.RemoteProtocolError, httpx.LocalProtocolError, httpx.ReadError, httpx.ConnectError, httpx.PoolTimeout, httpx.ConnectTimeout, httpx.RequestError):
                    async with httpx.AsyncClient(timeout=15.0) as c2:
                        if method == "POST":
                            res = await c2.post(url, headers=headers)
                        else:
                            res = await c2.get(url, headers=headers)
            else:
                async with httpx.AsyncClient(timeout=15.0) as c2:
                    if method == "POST":
                        res = await c2.post(url, headers=headers)
                    else:
                        res = await c2.get(url, headers=headers)
            break
        except Exception as e:
            last_err = e
            if attempt >= 2 or not _is_retryable_http_exc(e):
                raise
            await asyncio.sleep(0.35 * (attempt + 1))
    if res is None:
        if last_err is not None:
            raise last_err
        raise HTTPException(status_code=503, detail="signed request failed: no response")
    if res.status_code >= 400:
        raise HTTPException(
            status_code=res.status_code,
            detail={
                "message": res.text,
                "binanceRequest": diagnostic,
                "offsetDiag": {
                    "sentTsMs": server_ms,
                    "offsetMs": _TIME_OFFSET_MS,
                    "syncAgeSec": round(get_server_time_sync_age_sec(), 2),
                    "recvWindow": BINANCE_RECV_WINDOW_MS,
                    "httpConfigured": _HTTP is not None,
                },
            },
        )
    return res.json()

async def _exchange_filters(symbol: str):
    sym_upper = str(symbol or "").upper().strip()
    now_ts = time.time()
    # Use the module-level TTL constant instead of a hardcoded 900s that drifted
    # from the declared _EXCHANGE_FILTERS_CACHE_TTL (60s). Shorter TTL keeps
    # filter changes (status flips, step-size tweaks) visible to the engine
    # sooner while the single-symbol fetch below keeps the cost negligible.
    ttl = _EXCHANGE_FILTERS_CACHE_TTL

    def _payload_for(payload: dict) -> dict:
        if payload.get("status") != "TRADING":
            raise HTTPException(status_code=400, detail=f"Symbol not tradable: {symbol}")
        return payload

    cached = _EXCHANGE_FILTERS_CACHE.get(sym_upper)
    if cached and now_ts - cached[0] < ttl:
        return _payload_for(cached[1])

    async with _EXCHANGE_FILTERS_LOCK:
        # Double-check inside the lock — another coroutine may have just refreshed it.
        cached = _EXCHANGE_FILTERS_CACHE.get(sym_upper)
        if cached and now_ts - cached[0] < ttl:
            return _payload_for(cached[1])

        # Prefer the single-symbol endpoint: its response is a few KB vs the
        # multi-MB full exchangeInfo. The full payload was causing MemoryError
        # during httpx decompression on memory-constrained runs (see api.err.log),
        # and we only ever need filters for one symbol at a time anyway.
        payload: dict | None = None
        try:
            res = await _data_get(f"/fapi/v1/exchangeInfo?symbol={sym_upper}")
            if res.status_code < 400:
                data = res.json()
                symbols = data.get("symbols", []) if isinstance(data, dict) else []
                if symbols:
                    s = symbols[0]
                    filters = {f["filterType"]: f for f in s.get("filters", [])}
                    payload = {
                        "stepSize": float(filters.get("LOT_SIZE", {}).get("stepSize", "0")),
                        "minQty": float(filters.get("LOT_SIZE", {}).get("minQty", "0")),
                        "tickSize": float(filters.get("PRICE_FILTER", {}).get("tickSize", "0")),
                        "minNotional": float(filters.get("MIN_NOTIONAL", {}).get("notional", filters.get("NOTIONAL", {}).get("minNotional", "0"))),
                        "maxLeverage": int(os.getenv("MAX_LEVERAGE_DEFAULT", "20")),
                        "maxQty": float(filters.get("LOT_SIZE", {}).get("maxQty", "0") or 0),
                        "contractSize": float(s.get("contractSize", "1") or 1),
                        "status": s.get("status", "BREAK"),
                    }
                    _EXCHANGE_FILTERS_CACHE[sym_upper] = (now_ts, payload)
            elif res.status_code == 400:
                raise HTTPException(status_code=400, detail=f"Invalid Binance symbol: {sym_upper}")
        except HTTPException:
            raise
        except (MemoryError, Exception) as e:
            # MemoryError here is exactly the production OOM we are guarding
            # against; fall through to the full-list fetch / stale cache below.
            if not isinstance(e, MemoryError) and not _is_retryable_http_exc(e):
                # Non-transient parsing/logic error — let it surface, but first
                # try to serve a stale cache entry so the cycle can continue.
                pass
            print(f"[Exchange Filters] single-symbol fetch failed for {sym_upper}: {type(e).__name__}: {e}")

        # Fallback 1: full exchangeInfo list (populates cache for many symbols at once).
        # Used when the single-symbol endpoint 400s (e.g. delisted/odd symbol) or
        # when callers historically relied on bulk cache warming.
        if payload is None:
            try:
                res = await _data_get("/fapi/v1/exchangeInfo")
                if res.status_code >= 400:
                    # Non-200: try stale cache before surfacing the HTTP error.
                    if cached:
                        return _payload_for(cached[1])
                    raise HTTPException(status_code=res.status_code, detail=res.text)
                data = res.json()
                symbols = data.get("symbols", []) if isinstance(data, dict) else []
                for s in symbols:
                    sym_name = str(s.get("symbol") or "").upper().strip() or sym_upper
                    try:
                        filters = {f["filterType"]: f for f in s.get("filters", [])}
                        entry_payload = {
                            "stepSize": float(filters.get("LOT_SIZE", {}).get("stepSize", "0")),
                            "minQty": float(filters.get("LOT_SIZE", {}).get("minQty", "0")),
                            "tickSize": float(filters.get("PRICE_FILTER", {}).get("tickSize", "0")),
                            "minNotional": float(filters.get("MIN_NOTIONAL", {}).get("notional", filters.get("NOTIONAL", {}).get("minNotional", "0"))),
                            "maxLeverage": int(os.getenv("MAX_LEVERAGE_DEFAULT", "20")),
                            "maxQty": float(filters.get("LOT_SIZE", {}).get("maxQty", "0") or 0),
                            "contractSize": float(s.get("contractSize", "1") or 1),
                            "status": s.get("status", "BREAK"),
                        }
                        _EXCHANGE_FILTERS_CACHE[sym_name] = (now_ts, entry_payload)
                    except Exception:
                        continue
                from services.cache_registry import _limit_cache_size
                _limit_cache_size(_EXCHANGE_FILTERS_CACHE, max_size=150)
                payload = _EXCHANGE_FILTERS_CACHE.get(sym_upper, (0, None))[1]
            except HTTPException:
                raise
            except (MemoryError, Exception) as e:
                print(f"[Exchange Filters] full-list fetch failed for {sym_upper}: {type(e).__name__}: {e}")
                # Fallback 2: serve stale cache if we have ANY prior entry, even an
                # expired one — better than crashing the entry cycle over a transient
                # exchange/timeout/MemoryError on exchangeInfo.
                if cached:
                    print(f"[Exchange Filters] serving stale cache for {sym_upper} after fetch failure")
                    return _payload_for(cached[1])
                raise HTTPException(status_code=503, detail=f"exchangeInfo unavailable: {type(e).__name__}")

        if payload is None:
            # Should be unreachable given the fallbacks above, but keep a safe guard.
            if cached:
                return _payload_for(cached[1])
            raise HTTPException(status_code=400, detail=f"Symbol not found on futures: {symbol}")

        return _payload_for(payload)

def _get_um_client(key: str, secret: str, base: str):
    if os.getenv("CONNECTOR_MODE", "auto").lower() == "legacy":
        return None
    connector_cls = _resolve_umfutures_class()
    if connector_cls is None:
        if os.getenv("CONNECTOR_MODE", "auto").lower() == "official":
            raise HTTPException(status_code=500, detail="Official connector mode enabled but UMFutures not available")
        return None
    return connector_cls(key=key, secret=secret, base_url=base)
