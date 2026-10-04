"""Tests for protective-order placement guards and the entry safety gates.

Covers the 2026-10-03 incident: ``POST /fapi/v1/algoOrder`` answers HTTP 200
with ``algoStatus: REJECTED``, so TP/SL placement looked successful while the
position had no exchange-side protection.
"""
import time
import asyncio
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exchange import futures_orders as fo
from trading.risk_cooldown import _recent_big_losses_by_symbol


# --------------------------------------------------------------------------
# _raise_if_algo_rejected
# --------------------------------------------------------------------------

def test_algo_new_status_is_accepted():
    fo._raise_if_algo_rejected({"algoStatus": "NEW", "algoId": 1}, "tp")


def test_algo_working_status_is_accepted():
    fo._raise_if_algo_rejected({"algoStatus": "WORKING"}, "sl")


def test_algo_rejected_raises():
    """The exact shape Binance returns for "Reduce only reject"."""
    resp = {"algoStatus": "REJECTED", "rejectReason": "Reduce only reject"}
    with pytest.raises(RuntimeError) as exc:
        fo._raise_if_algo_rejected(resp, "tp")
    assert "Reduce only reject" in str(exc.value)


def test_algo_rejected_without_reason_still_raises():
    with pytest.raises(RuntimeError):
        fo._raise_if_algo_rejected({"algoStatus": "REJECTED"}, "sl")


def test_algo_error_code_raises_even_without_status():
    with pytest.raises(RuntimeError) as exc:
        fo._raise_if_algo_rejected({"code": -2010, "msg": "ReduceOnly Order is rejected."}, "tp")
    assert "-2010" in str(exc.value) or "ReduceOnly" in str(exc.value)


def test_algo_new_with_reject_reason_raises():
    with pytest.raises(RuntimeError):
        fo._raise_if_algo_rejected({"algoStatus": "NEW", "rejectReason": "Reduce only reject"}, "tp")


def test_algo_non_dict_response_is_ignored():
    fo._raise_if_algo_rejected(None, "tp")
    fo._raise_if_algo_rejected([1, 2], "tp")


# --------------------------------------------------------------------------
# _verify_protective_orders
# --------------------------------------------------------------------------

async def _verify(rows, expected):
    async def fake_request(method, base, path, key, secret, params):
        return rows

    with mock.patch.object(fo, "_signed_request", side_effect=fake_request):
        return await fo._verify_protective_orders("QNTUSDT", "k", "s", "b", expected)


def test_verify_passes_when_both_levels_present():
    rows = [
        {"algoStatus": "NEW", "triggerPrice": "258.8"},
        {"algoStatus": "NEW", "triggerPrice": "252.1"},
    ]
    asyncio.run(_verify(rows, [("tp", 258.8), ("sl", 252.1)]))


def test_verify_raises_when_a_level_is_missing():
    """Catches the async rejection that the submit response cannot show."""
    rows = [{"algoStatus": "NEW", "triggerPrice": "252.1"}]
    with pytest.raises(RuntimeError) as exc:
        asyncio.run(_verify(rows, [("tp", 258.8), ("sl", 252.1)]))
    assert "tp@258.8" in str(exc.value)


def test_verify_ignores_terminal_orders():
    rows = [
        {"algoStatus": "FINISHED", "triggerPrice": "258.8"},
        {"algoStatus": "NEW", "triggerPrice": "252.1"},
    ]
    with pytest.raises(RuntimeError):
        asyncio.run(_verify(rows, [("tp", 258.8), ("sl", 252.1)]))


def test_verify_is_noop_when_endpoint_unavailable():
    """An endpoint outage must not block entries or fake missing protection."""
    with mock.patch.object(fo, "_signed_request", side_effect=RuntimeError("404 not available")):
        asyncio.run(fo._verify_protective_orders("QNTUSDT", "k", "s", "b", [("tp", 1.0)]))
    with mock.patch.object(fo, "_signed_request", side_effect=RuntimeError("transient 502")):
        asyncio.run(fo._verify_protective_orders("QNTUSDT", "k", "s", "b", [("tp", 1.0)]))


def test_verify_retries_once_before_giving_up():
    """A single transient blip must not be read as missing protection."""
    calls = {"n": 0}

    async def flaky(method, base, path, key, secret, params):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient 502")
        return [{"algoStatus": "NEW", "triggerPrice": "100.0"}]

    with mock.patch.object(fo, "_signed_request", side_effect=flaky):
        asyncio.run(fo._verify_protective_orders("QNTUSDT", "k", "s", "b", [("tp", 100.0)]))
    assert calls["n"] == 2


# --------------------------------------------------------------------------
# sweep_orphan_protective_orders
# --------------------------------------------------------------------------

def _run_sweep(open_algo, positions, cancel_records, after=None):
    after = after if after is not None else open_algo
    calls = {"n": 0}

    async def fake_request(method, base, path, key, secret, params):
        if path == "/fapi/v1/openAlgoOrders":
            calls["n"] += 1
            return open_algo if calls["n"] == 1 else after
        if path == "/fapi/v2/positionRisk":
            return positions
        return {}

    async def fake_cancel(symbol, key, secret, base):
        cancel_records.append(symbol)

    with mock.patch.object(fo, "_signed_request", side_effect=fake_request), \
         mock.patch.object(fo, "_cancel_all_open_orders", side_effect=fake_cancel):
        return asyncio.run(fo.sweep_orphan_protective_orders("k", "s", "b"))


def test_sweep_cancels_orders_on_flat_symbols_only():
    open_algo = [
        {"symbol": "QNTUSDT", "algoStatus": "NEW"},
        {"symbol": "ZECUSDT", "algoStatus": "NEW"},
        {"symbol": "ZECUSDT", "algoStatus": "NEW"},
    ]
    positions = [{"symbol": "ZECUSDT", "positionAmt": "1.5"}]
    cancelled = []
    res = _run_sweep(open_algo, positions, cancelled, after=[])
    assert res["ok"] is True
    assert cancelled == ["QNTUSDT"]
    assert res["cancelledSymbols"] == ["QNTUSDT"]


def test_sweep_reports_uncleared_when_exchange_keeps_orders():
    """Binance returns 200 on algo DELETE without cancelling; say so, don't claim success."""
    open_algo = [{"symbol": "QNTUSDT", "algoStatus": "NEW"}]
    cancelled = []
    res = _run_sweep(open_algo, [], cancelled, after=open_algo)
    assert res["ok"] is True
    assert res["cancelledSymbols"] == []
    assert res["unclearedSymbols"] == ["QNTUSDT"]
    assert cancelled == ["QNTUSDT"]


def test_sweep_counts_every_order_on_a_symbol():
    open_algo = [{"symbol": "ZECUSDT", "algoStatus": "NEW"} for _ in range(7)]
    cancelled = []
    res = _run_sweep(open_algo, [], cancelled, after=[])
    assert res["orphanSymbols"]["ZECUSDT"] == 7


def test_sweep_fails_closed_when_positions_unreadable():
    """Never cancel on partial information — that could drop live protection."""
    cancelled = []

    async def fake_request(method, base, path, key, secret, params):
        if path == "/fapi/v1/openAlgoOrders":
            return [{"symbol": "QNTUSDT", "algoStatus": "NEW"}]
        raise RuntimeError("positionRisk 503")

    with mock.patch.object(fo, "_signed_request", side_effect=fake_request), \
         mock.patch.object(fo, "_cancel_all_open_orders", side_effect=lambda *a: cancelled.append(a)):
        res = asyncio.run(fo.sweep_orphan_protective_orders("k", "s", "b"))
    assert res["ok"] is False
    assert cancelled == []


def test_sweep_noop_when_nothing_open():
    res = _run_sweep([], [], [])
    assert res["ok"] is True
    assert res["cancelledSymbols"] == []


# --------------------------------------------------------------------------
# _recent_big_losses_by_symbol
# --------------------------------------------------------------------------

def _t(symbol, pnl, ts):
    return {"symbol": symbol, "_pnl": pnl, "_ts": ts}


def test_big_loss_after_small_wins_arms_cooldown():
    """QNTUSDT 2026-10-03: +0.001 then -1.963, re-entered 4m45s later.

    The streak counter resets on the small win so it never arms; a single loss
    over the threshold must stand on its own.
    """
    trades = [
        _t("QNTUSDT", 0.976, 100),
        _t("QNTUSDT", -0.052, 150),
        _t("QNTUSDT", 0.001, 200),
        _t("QNTUSDT", -1.963, 250),
    ]
    out = _recent_big_losses_by_symbol(trades, 1.0, 7200, now=300)
    assert "QNTUSDT" in out
    assert abs(out["QNTUSDT"]["pnl"] + 1.963) < 1e-9
    assert out["QNTUSDT"]["lastClosedAt"] == 250


def test_big_loss_followed_by_a_win_does_not_arm():
    """A loss already recovered by a later win on the same symbol is not live risk."""
    trades = [_t("QNTUSDT", -1.963, 250), _t("QNTUSDT", 0.5, 300)]
    assert _recent_big_losses_by_symbol(trades, 1.0, 7200, now=400) == {}


def test_big_loss_uses_only_the_last_close():
    trades = [_t("QNTUSDT", -2.0, 100), _t("QNTUSDT", 0.4, 200)]
    assert _recent_big_losses_by_symbol(trades, 1.0, 7200, now=300) == {}


def test_big_loss_respects_threshold():
    trades = [_t("QNTUSDT", -0.5, 100)]
    assert _recent_big_losses_by_symbol(trades, 1.0, 7200, now=200) == {}


def test_big_loss_respects_recent_window():
    trades = [_t("QNTUSDT", -2.0, 100)]
    assert _recent_big_losses_by_symbol(trades, 1.0, 600, now=5000) == {}


def test_big_loss_disabled_when_threshold_zero():
    trades = [_t("QNTUSDT", -2.0, 100)]
    assert _recent_big_losses_by_symbol(trades, 0.0, 7200, now=200) == {}


def test_big_loss_signature_is_unique_per_close():
    trades = [_t("QNTUSDT", -2.0, 100)]
    first = _recent_big_losses_by_symbol(trades, 1.0, 7200, now=200)["QNTUSDT"]["signature"]
    later = _recent_big_losses_by_symbol([_t("QNTUSDT", -2.0, 900)], 1.0, 7200, now=1000)["QNTUSDT"]["signature"]
    assert first != later


# --------------------------------------------------------------------------
# _tv_confirmation_streak
# --------------------------------------------------------------------------

def _streak_calls(main, reads, cfg=None, start=1000, step=40):
    """Feed a sequence of (signal, conf) readings, returning each streak count."""
    out = []
    now = start
    for sig, conf in reads:
        out.append(main._tv_confirmation_streak("QNTUSDT", sig, conf, cfg or {}, now))
        now += step
    return out


def test_tv_streak_counts_consecutive_same_readings():
    import main
    main.AUTO_TRADE["tvConfirmStreaks"] = {}
    counts = _streak_calls(main, [("LONG", 0.8)] * 3)
    assert counts == [1, 2, 3]


def test_tv_streak_resets_on_signal_flip():
    """This is the flip-flop the gate exists to filter."""
    import main
    main.AUTO_TRADE["tvConfirmStreaks"] = {}
    counts = _streak_calls(main, [("LONG", 0.8), ("WAIT", 0.4), ("LONG", 0.8)])
    assert counts == [1, 1, 1]


def test_tv_streak_resets_when_reading_goes_stale():
    import main
    main.AUTO_TRADE["tvConfirmStreaks"] = {}
    counts = _streak_calls(main, [("LONG", 0.8), ("LONG", 0.8)], cfg={"tvConfirmWindowSec": 30}, step=300)
    assert counts == [1, 1]


def test_tv_streak_is_per_symbol():
    import main
    main.AUTO_TRADE["tvConfirmStreaks"] = {}
    main._tv_confirmation_streak("QNTUSDT", "LONG", 0.8, {}, 1000)
    assert main._tv_confirmation_streak("BTCUSDT", "LONG", 0.8, {}, 1000) == 1
    assert main._tv_confirmation_streak("QNTUSDT", "LONG", 0.8, {}, 1010) == 2

# --------------------------------------------------------------------------
# GTD expiry on protective placement (_place_tp_sl)
# --------------------------------------------------------------------------

def _capture_placements(gtd_sec, expect_gtd):
    """Run _place_tp_sl with mocked endpoints; return the algo POST params."""
    captured = []

    async def fake_request(method, base, path, key, secret, params):
        if method == "POST" and path == "/fapi/v1/algoOrder":
            captured.append(dict(params))
            return {"algoStatus": "NEW", "algoId": 123}
        return []

    async def fake_verify(*a, **k):
        return None

    cfg = {"protectiveOrderGtdSec": gtd_sec}
    with mock.patch.object(fo, "_signed_request", side_effect=fake_request), \
         mock.patch.object(fo, "_verify_protective_orders", fake_verify), \
         mock.patch.dict(fo.AUTO_TRADE, {"config": cfg}):
        asyncio.run(fo._place_tp_sl(
            "QNTUSDT", "LONG", 1.0, 260.0, 0.5, 0.5, "k", "s", "b",
            0.001, "0.001", True, "LONG",
        ))
    assert len(captured) == 2, captured
    return captured


def test_place_tp_sl_sets_gtd_expiry_when_configured():
    posts = _capture_placements(7200, True)
    for p in posts:
        assert p["timeInForce"] == "GTD"
        assert int(p["goodTillDate"]) > time.time() * 1000


def test_place_tp_sl_gtd_defaults_on_without_config_key():
    """Missing key must not silently degrade to an immortal resting order."""
    posts = _capture_placements(7200, True)
    for p in posts:
        assert p["timeInForce"] == "GTD"


def test_place_tp_sl_gtd_zero_means_gtc():
    posts = _capture_placements(0, False)
    for p in posts:
        assert "timeInForce" not in p or p.get("timeInForce") != "GTD"
        assert "goodTillDate" not in p
