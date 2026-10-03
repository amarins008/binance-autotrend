"""Tests for exchange-truth LIVE close reconciliation.

Covers the ghost class observed on 2026-10-03: a close that never reached the
trade log because the exchange closed the position first, and partial fills
recorded with the full pre-close quantity.
"""

from __future__ import annotations

import asyncio

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trading import close_reconciler as cr  # noqa: E402


def _fill(fid: int, t_ms: int, side: str, qty: float, price: float, pnl: float, commission: float = 0.0, pos_side: str = "BOTH"):
    return {
        "id": fid,
        "timeMs": t_ms,
        "side": side,
        "positionSide": pos_side,
        "qty": qty,
        "price": price,
        "realizedPnl": pnl,
        "commission": commission,
        "commissionAsset": "USDT",
        "reduces": (side == "SELL" and pos_side in ("BOTH", "LONG")) or (side == "BUY" and pos_side in ("BOTH", "SHORT")),
        "closesSide": "LONG" if side == "SELL" else "SHORT",
    }


class TestFillClassification:
    def test_sell_reduces_long_buy_opening_does_not(self):
        assert _fill(1, 0, "SELL", 1, 100, -1)["reduces"] is True
        assert _fill(2, 0, "BUY", 1, 100, 0, pos_side="LONG")["reduces"] is False

    def test_buy_reduces_short_in_one_way_mode(self):
        # positionSide=BOTH: a BUY here flattens a short, it is not an entry.
        assert _fill(6, 0, "BUY", 1, 100, -1)["reduces"] is True

    def test_buy_reduces_short(self):
        assert _fill(3, 0, "BUY", 1, 100, -1, pos_side="SHORT")["reduces"] is True
        assert _fill(4, 0, "BUY", 1, 100, 0, pos_side="LONG")["reduces"] is False

    def test_opening_fill_is_not_a_close(self):
        f = _fill(5, 0, "BUY", 1, 100, 0, pos_side="LONG")
        assert cr._close_events([f], 0) == []


class TestEventGrouping:
    def test_contiguous_partials_form_one_event(self):
        fills = [
            _fill(1, 1_000_000, "SELL", 0.3, 260.0, -0.09),
            _fill(2, 1_005_000, "SELL", 0.2, 259.8, -0.06),
        ]
        events = cr._close_events(fills, 0)
        assert len(events) == 1
        assert [f["id"] for f in events[0]] == [1, 2]

    def test_distant_fills_split(self):
        fills = [
            _fill(1, 1_000_000, "SELL", 0.3, 260.0, -0.09),
            _fill(2, 1_000_000 + cr._EVENT_GAP_MS + 1, "SELL", 0.2, 259.8, -0.06),
        ]
        assert len(cr._close_events(fills, 0)) == 2

    def test_side_change_splits(self):
        fills = [
            _fill(1, 1_000_000, "SELL", 0.3, 260.0, -0.09),
            _fill(2, 1_001_000, "BUY", 0.3, 259.0, 0.03, pos_side="SHORT"),
        ]
        assert len(cr._close_events(fills, 0)) == 2

    def test_since_ms_filters_older_fills(self):
        fills = [_fill(1, 1_000_000, "SELL", 1, 260.0, -1.0)]
        assert cr._close_events(fills, 2_000_000) == []


class TestEventTradeMath:
    def test_long_entry_is_derived_from_fills(self):
        # 0.5 @ 260 sold for -0.50 total => entry = 261.0
        ev = [_fill(1, 1_000_000, "SELL", 0.5, 260.0, -0.50)]
        trade = cr._event_trade("AAAUSDT", ev, "LOCAL_SL_HIT")
        assert trade["side"] == "LONG"
        assert trade["entry"] == pytest.approx(261.0)
        assert trade["exit"] == pytest.approx(260.0)
        assert trade["pnl"] == pytest.approx(-0.50)
        assert trade["reason"] == "LOCAL_SL_HIT"
        assert trade["fills"] == [1]

    def test_short_entry_is_derived_from_fills(self):
        # short 0.5 bought back at 260 for +0.50 => entry = 260 + 0.50/0.5
        ev = [_fill(1, 1_000_000, "BUY", 0.5, 260.0, 0.50, pos_side="SHORT")]
        trade = cr._event_trade("AAAUSDT", ev, "EXCHANGE_CLOSE")
        assert trade["side"] == "SHORT"
        assert trade["entry"] == pytest.approx(261.0)
        assert trade["pnl"] == pytest.approx(0.50)

    def test_partial_fills_sum_to_realized_only(self):
        # full close of 0.5, but only 0.3 filled -> pnl must reflect 0.3
        ev = [_fill(1, 1_000_000, "SELL", 0.3, 260.0, -0.30)]
        trade = cr._event_trade("AAAUSDT", ev, "LOCAL_SL_HIT")
        assert trade["qty"] == pytest.approx(0.3)
        assert trade["pnl"] == pytest.approx(-0.30)

    def test_commission_is_split_out(self):
        ev = [_fill(1, 1_000_000, "SELL", 0.5, 260.0, -0.20, commission=0.13)]
        trade = cr._event_trade("AAAUSDT", ev, "LOCAL_SL_HIT")
        assert trade["commission"] == pytest.approx(0.13)
        assert trade["netPnl"] == pytest.approx(-0.33)
        assert trade["pnl"] == pytest.approx(-0.20)


class TestReconcileSymbolCloses:
    @pytest.fixture(autouse=True)
    def _clean(self):
        cr._TRADES_CACHE.clear()
        cr.pop_close_intent("ZZZPROBEUSDT")
        cr._intents().pop("ZZZPROBEUSDT", None)
        yield
        cr._TRADES_CACHE.clear()
        cr.pop_close_intent("ZZZPROBEUSDT")

    def _patch(self, rows, recorded):
        import trading.learning as learning

        learning._record_learning_trade = lambda sym, trade, mode: recorded.append((sym, trade, mode))
        cr._signed_request = _async_rows(rows)

    def test_ghost_close_is_recorded_from_fills_alone(self):
        """positionRisk already flat, but userTrades still holds the close."""
        sym = "ZZZPROBEUSDT"
        recorded = []
        rows = [
            _fill(101, 1_000_000, "SELL", 0.25, 260.0, -0.28, commission=0.033),
            _fill(102, 1_004_000, "SELL", 0.25, 259.9, -0.30, commission=0.032),
        ]
        self._patch(rows, recorded)
        try:
            out = asyncio.run(
                cr.reconcile_symbol_closes(
                    sym, "k", "s", "https://fapi.binance.com", reason="LOCAL_SL_HIT", since_ms=0
                )
            )
        finally:
            cr._signed_request = _ORIG_SIGNED
            import trading.learning as learning

            learning._record_learning_trade = _ORIG_RECORD

        assert len(recorded) == 1
        assert recorded[0][0] == sym
        assert recorded[0][2] == "LIVE"
        trade = recorded[0][1]
        assert trade["fills"] == [101, 102]
        assert trade["pnl"] == pytest.approx(-0.58)
        assert trade["commission"] == pytest.approx(0.065)
        assert trade["closeSource"] == "EXCHANGE_FILLS"
        assert out and out[0]["fills"] == [101, 102]

    def test_reconcile_is_idempotent(self):
        sym = "ZZZPROBEUSDT"
        recorded = []
        rows = [_fill(201, 1_000_000, "SELL", 0.5, 260.0, -0.5)]
        self._patch(rows, recorded)
        try:
            first = asyncio.run(
                cr.reconcile_symbol_closes(sym, "k", "s", "b", reason="LOCAL_SL_HIT", since_ms=0)
            )
            second = asyncio.run(
                cr.reconcile_symbol_closes(sym, "k", "s", "b", reason="LOCAL_SL_HIT", since_ms=0)
            )
        finally:
            cr._signed_request = _ORIG_SIGNED
            import trading.learning as learning

            learning._record_learning_trade = _ORIG_RECORD
        assert len(recorded) == 1, "second pass must not double-record"
        assert first and first[0]["fills"] == [201]
        assert second == []

    def test_exchange_unreachable_returns_none_and_keeps_intent(self):
        sym = "ZZZPROBEUSDT"
        cr.mark_close_intent(sym, "LOCAL_SL_HIT", "LONG")

        async def boom(*a, **k):
            raise RuntimeError("binance down")

        cr._signed_request = boom
        try:
            out = asyncio.run(cr.reconcile_symbol_closes(sym, "k", "s", "b"))
        finally:
            cr._signed_request = _ORIG_SIGNED
        assert out is None
        assert cr.peek_close_intent(sym)["reason"] == "LOCAL_SL_HIT", "intent must survive an unreadable exchange"


class TestReconcileCycle:
    """The cycle reports untracked positions; it must not manage them itself.

    live_guardian Phase 1 owns adoption because it derives TP/SL through
    _effective_tpsl_pct_for_trade (the entry's USDT-target path). Seeding a
    lock from here with the legacy _effective_tp_sl would pin the wrong SL.
    """

    def test_untracked_position_is_reported_but_not_seeded(self):
        import trading.live_guardian as lg

        cr._TRADES_CACHE.clear()
        sym = "ZZZORPHANUSDT"
        rows = [
            {
                "symbol": sym,
                "side": "LONG",
                "qty": 1.0,
                "entryMark": 10.0,
                "markPrice": 10.0,
                "notionalUsdtApprox": 10.0,
                "unRealizedProfit": 0.0,
            }
        ]
        # Scoped dict object: other suites leave locks on the shared map, and
        # asserting against the restored map would read their state, not ours.
        saved = cr.AUTO_TRADE.get("liveProfitLocks")
        scoped_locks: dict = {}
        cr.AUTO_TRADE["liveProfitLocks"] = scoped_locks

        async def fake_positions(*a, **k):
            return rows

        orig_pick = lg._pick_live_orphan_positions
        lg._pick_live_orphan_positions = fake_positions
        import os as _os

        env = {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"}
        prev = {k: _os.environ.get(k) for k in env}
        _os.environ.update(env)
        try:
            out = asyncio.run(cr.reconcile_cycle())
        finally:
            lg._pick_live_orphan_positions = orig_pick
            for k, v in prev.items():
                if v is None:
                    _os.environ.pop(k, None)
                else:
                    _os.environ[k] = v
            cr.AUTO_TRADE["liveProfitLocks"] = saved if saved is not None else {}
            cr._TRADES_CACHE.clear()

        assert out["ok"] is True
        assert [u["symbol"] for u in out["untracked"]] == [sym]
        assert not scoped_locks, "Phase 1 owns adoption, not this module"
        assert "adopted" not in out


# ── Regression cover for the 2026-10-03 ledger hole ──────────────────────
# QNTUSDT lost -1.9631 / -1.1120 / -1.0840 across three closes that never
# reached trades.jsonl. Cause: an empty reconcile result was read as "there is
# nothing to record" and popped the close intent, so the background cycle had
# nothing left to reconcile. `[]` actually means "userTrades has not surfaced
# the fill yet" for the milliseconds after a MARKET reduce is accepted.

_SYM = "ZZZHOLEUSDT"
# The reconcile paths filter fills against a since_ms derived from the wall
# clock, so fixtures must sit inside that window rather than at a fixed
# epoch: _T_NOW for intent scans (20s back), _T0 for the wider
# discovery/repair windows (1h back).
_T_NOW = int(time.time() * 1000) - 20_000
_T0 = int(time.time() * 1000) - 3_600_000


def _fill_oid(fid: int, oid: int, t_ms: int, side: str, qty: float, price: float, pnl: float, pos_side: str = "BOTH"):
    f = _fill(fid, t_ms, side, qty, price, pnl, pos_side=pos_side)
    f["orderId"] = oid
    return f


def _raw(fid: int, oid: int, t_ms: int, side: str, qty: float, price: float, pnl: float, pos_side: str = "BOTH"):
    """A raw /fapi/v1/userTrades row — what fetch_recent_fills actually consumes.

    The tests above stub _signed_request with already-normalised rows and pin
    since_ms=0. Anything exercising the real fetch path has to hand back
    exchange-shaped rows instead, or ``time`` reads as 0 and every fill falls
    outside the since window.
    """
    return {
        "id": fid,
        "orderId": oid,
        "time": t_ms,
        "side": side,
        "positionSide": pos_side,
        "qty": str(qty),
        "price": str(price),
        "realizedPnl": str(pnl),
        "commission": "0.01",
        "commissionAsset": "USDT",
    }


class TestIntentRetention:
    """`[]` must keep the intent alive; only a real record pops it."""

    @pytest.fixture(autouse=True)
    def _env(self, tmp_path, monkeypatch):
        import os as _os

        for k in ("BINANCE_API_KEY", "BINANCE_API_SECRET"):
            monkeypatch.setenv(k, "k" if k.endswith("KEY") else "s")
        monkeypatch.setattr(cr, "_trades_log_path", lambda s: tmp_path / "trades.jsonl")
        cr._TRADES_CACHE.clear()
        # closeIntents lives on the shared AUTO_TRADE dict; other test files
        # leave intents behind and this stub answers every symbol with the
        # same fills, so the whole map has to be scoped to this test.
        saved_intents = dict(cr._intents())
        cr._intents().clear()
        yield
        cr._TRADES_CACHE.clear()
        cr._intents().clear()
        cr._intents().update(saved_intents)

    def _router(self, income_rows, fills_rows):
        async def _call(method, base, endpoint, key, secret, params):
            if endpoint.endswith("/income"):
                return list(income_rows)
            return list(fills_rows)

        return _call

    def test_empty_result_keeps_intent_for_the_next_cycle(self):
        """The regression itself: scan sees no fill, intent must survive."""
        cr.mark_close_intent(_SYM, "RETRACE_BUDGET", "LONG")
        cr._signed_request = self._router([], [])
        try:
            asyncio.run(cr.reconcile_pending_closes())
        finally:
            cr._signed_request = _ORIG_SIGNED
        assert _SYM in cr._intents(), "empty scan must not drop the close intent"

    def test_unreadable_exchange_keeps_intent(self):
        cr.mark_close_intent(_SYM, "RETRACE_BUDGET", "LONG")

        async def boom(*a, **k):
            raise RuntimeError("binance down")

        cr._signed_request = boom
        try:
            asyncio.run(cr.reconcile_pending_closes())
        finally:
            cr._signed_request = _ORIG_SIGNED
        assert _SYM in cr._intents()

    def test_recorded_fill_pops_intent(self):
        recorded = []
        import trading.learning as learning

        learning._record_learning_trade = lambda sym, trade, mode: recorded.append(trade)
        cr.mark_close_intent(_SYM, "RETRACE_BUDGET", "LONG")
        fills = [_raw(501, 9001, _T_NOW, "SELL", 0.5, 260.0, -0.5)]
        cr._signed_request = self._router([], fills)
        try:
            asyncio.run(cr.reconcile_pending_closes())
        finally:
            cr._signed_request = _ORIG_SIGNED
            learning._record_learning_trade = _ORIG_RECORD
        assert len(recorded) == 1
        assert _SYM not in cr._intents()

    def test_intent_expires_after_ttl(self):
        """A close that never fills must not keep its intent forever."""
        cr.mark_close_intent(_SYM, "RETRACE_BUDGET", "LONG")
        cr._intents()[_SYM]["sinceMs"] = int((time.time() - cr.INTENT_TTL_SEC - 60) * 1000)
        cr._signed_request = self._router([], [])
        try:
            asyncio.run(cr.reconcile_pending_closes())
        finally:
            cr._signed_request = _ORIG_SIGNED
        assert _SYM not in cr._intents()


class TestOrderIdAnchor:
    """The arithmetic fallback records before userTrades catches up.

    Its row has no fill ids, so without an order anchor the very next scan sees
    the same close as unrecorded and counts it twice.
    """

    @pytest.fixture(autouse=True)
    def _clean(self, tmp_path, monkeypatch):
        path = tmp_path / "trades.jsonl"
        monkeypatch.setattr(cr, "_trades_log_path", lambda s: path)
        cr._TRADES_CACHE.clear()
        self.path = path
        yield
        cr._TRADES_CACHE.clear()

    def _write_row(self, **row):
        import json

        row.setdefault("symbol", _SYM)
        row.setdefault("mode", "LIVE")
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")

    def test_anchors_are_read_from_both_keys(self):
        self._write_row(fills=[11, 12], orderIds=[77])
        self._write_row(fills=None, orderIds=[88])
        fills, orders = cr._recorded_anchors(_SYM)
        assert fills == {11, 12}
        assert orders == {77, 88}

    def test_order_anchor_suppresses_a_later_fill_scan(self):
        recorded = []
        import trading.learning as learning

        learning._record_learning_trade = lambda sym, trade, mode: recorded.append(trade)
        self._write_row(side="LONG", qty=0.5, pnl=-0.51, orderIds=[9001], fills=None)
        cr._signed_request = _async_rows([_raw(601, 9001, _T0, "SELL", 0.5, 260.0, -0.5)])
        try:
            out = asyncio.run(cr.reconcile_symbol_closes(_SYM, "k", "s", "b", since_ms=0))
        finally:
            cr._signed_request = _ORIG_SIGNED
            learning._record_learning_trade = _ORIG_RECORD
        assert out == []
        assert recorded == [], "an already-recorded close must not be counted twice"


class TestAnchorRepair:
    """Rows written before anchors existed get their ids attached, not duplicated."""

    @pytest.fixture(autouse=True)
    def _clean(self, tmp_path, monkeypatch):
        path = tmp_path / "trades.jsonl"
        monkeypatch.setattr(cr, "_trades_log_path", lambda s: path)
        cr._TRADES_CACHE.clear()
        self.path = path
        yield
        cr._TRADES_CACHE.clear()

    def _rows(self):
        import json

        return [json.loads(x) for x in self.path.read_text(encoding="utf-8").splitlines() if x.strip()]

    def _write_unanchored(self, **row):
        import json

        row.setdefault("mode", "LIVE")
        row.setdefault("symbol", _SYM)
        row.setdefault("fills", None)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _event(self, t_ms=None, qty=0.5, side="SELL", pnl=-0.5, pos_side="BOTH"):
        return [_raw(701, 9101, t_ms or _T0, side, qty, 260.0, pnl, pos_side=pos_side)]

    def test_repair_attaches_fills_without_recording_a_second_trade(self):
        recorded = []
        import trading.learning as learning

        learning._record_learning_trade = lambda sym, trade, mode: recorded.append(trade)
        self._write_unanchored(side="LONG", qty=0.5, pnl=-0.51, closedAt=_T0 // 1000)
        cr._signed_request = _async_rows(self._event())
        try:
            detail = asyncio.run(cr.reconcile_symbol_closes_detailed(_SYM, "k", "s", "b", since_ms=0))
        finally:
            cr._signed_request = _ORIG_SIGNED
            learning._record_learning_trade = _ORIG_RECORD

        assert recorded == [], "repair must not write a duplicate trade"
        assert len(detail["repaired"]) == 1
        rows = self._rows()
        assert len(rows) == 1
        assert rows[0]["fills"] == [701]
        assert rows[0]["orderIds"] == [9101]
        assert rows[0]["closeSource"] == "EXCHANGE_FILLS"

    def test_repair_never_rewrites_recorded_pnl(self):
        """The recorded number came from an estimate; correcting history would
        invalidate every profile aggregate derived from it. Anchor only."""
        self._write_unanchored(side="LONG", qty=0.5, pnl=-0.51, closedAt=_T0 // 1000)
        cr._signed_request = _async_rows(self._event())
        try:
            asyncio.run(cr.reconcile_symbol_closes_detailed(_SYM, "k", "s", "b", since_ms=0))
        finally:
            cr._signed_request = _ORIG_SIGNED
        assert self._rows()[0]["pnl"] == -0.51

    def test_row_outside_the_tolerance_is_a_real_ghost(self):
        recorded = []
        import trading.learning as learning

        learning._record_learning_trade = lambda sym, trade, mode: recorded.append(trade)
        # Same symbol, but a different close an hour earlier with another size.
        self._write_unanchored(side="LONG", qty=0.5, pnl=-0.51, closedAt=(_T0 - 3_600_000) // 1000)
        cr._signed_request = _async_rows(self._event())
        try:
            detail = asyncio.run(cr.reconcile_symbol_closes_detailed(_SYM, "k", "s", "b", since_ms=0))
        finally:
            cr._signed_request = _ORIG_SIGNED
            learning._record_learning_trade = _ORIG_RECORD
        assert len(detail["recorded"]) == 1
        assert detail["repaired"] == []

    def test_matching_is_one_to_one(self):
        recorded = []
        import trading.learning as learning

        learning._record_learning_trade = lambda sym, trade, mode: recorded.append(trade)
        self._write_unanchored(side="LONG", qty=0.5, pnl=-0.51, closedAt=_T0 // 1000)
        first = [_raw(801, 9201, _T0, "SELL", 0.5, 260.0, -0.5)]
        cr._signed_request = _async_rows(first)
        try:
            repaired = asyncio.run(cr.reconcile_symbol_closes_detailed(_SYM, "k", "s", "b", since_ms=0))
            # A second close of the same shape six hours later must not reuse
            # the row that is already anchored.
            second = [_raw(802, 9202, _T0 + 21_600_000, "SELL", 0.5, 260.0, -0.5)]
            cr._signed_request = _async_rows(second)
            detail = asyncio.run(cr.reconcile_symbol_closes_detailed(_SYM, "k", "s", "b", since_ms=0))
        finally:
            cr._signed_request = _ORIG_SIGNED
            learning._record_learning_trade = _ORIG_RECORD

        assert len(repaired["repaired"]) == 1 and repaired["recorded"] == []
        assert len(detail["recorded"]) == 1
        assert detail["repaired"] == [], "an anchored row must not absorb a later close"
        assert len(self._rows()) == 1, "only the repaired row is on disk; the new trade is the caller's write"


class TestDiscovery:
    """Closes whose intent is gone are found from the income ledger instead."""

    @pytest.fixture(autouse=True)
    def _clean(self, tmp_path, monkeypatch):
        path = tmp_path / "trades.jsonl"
        monkeypatch.setattr(cr, "_trades_log_path", lambda s: path)
        for k in ("BINANCE_API_KEY", "BINANCE_API_SECRET"):
            monkeypatch.setenv(k, "k" if k.endswith("KEY") else "s")
        cr._TRADES_CACHE.clear()
        # closeIntents is shared AUTO_TRADE state; scope the whole map.
        saved_intents = dict(cr._intents())
        cr._intents().clear()
        yield
        cr._TRADES_CACHE.clear()
        cr._intents().clear()
        cr._intents().update(saved_intents)

    def test_ghost_with_no_intent_is_backfilled(self):
        recorded = []
        import trading.learning as learning

        learning._record_learning_trade = lambda sym, trade, mode: recorded.append(trade)

        async def _call(method, base, endpoint, key, secret, params):
            if endpoint.endswith("/income"):
                return [{"symbol": _SYM, "incomeType": "REALIZED_PNL", "income": "-0.5"}]
            return [_raw(901, 9301, _T0, "SELL", 0.5, 260.0, -0.5)]

        cr._signed_request = _call
        try:
            out = asyncio.run(cr.discover_unrecorded_closes("k", "s", "b", lookback_sec=86400))
        finally:
            cr._signed_request = _ORIG_SIGNED
            learning._record_learning_trade = _ORIG_RECORD

        assert out["ok"] is True
        assert _SYM not in cr._intents(), "discovery must not depend on an intent"
        assert len(out["recorded"]) == 1
        assert out["recorded"][0]["pnl"] == pytest.approx(-0.5)
        assert out["recorded"][0]["fills"] == [901]

    def test_second_discovery_run_finds_nothing(self):
        recorded = []
        import trading.learning as learning

        learning._record_learning_trade = lambda sym, trade, mode: recorded.append(trade)

        async def _call(method, base, endpoint, key, secret, params):
            if endpoint.endswith("/income"):
                return [{"symbol": _SYM, "incomeType": "REALIZED_PNL", "income": "-0.5"}]
            return [_raw(902, 9302, _T0, "SELL", 0.5, 260.0, -0.5)]

        cr._signed_request = _call
        try:
            first = asyncio.run(cr.discover_unrecorded_closes("k", "s", "b", lookback_sec=86400))
            second = asyncio.run(cr.discover_unrecorded_closes("k", "s", "b", lookback_sec=86400))
        finally:
            cr._signed_request = _ORIG_SIGNED
            learning._record_learning_trade = _ORIG_RECORD

        assert len(first["recorded"]) == 1
        assert second["recorded"] == [], "the recorded close must be anchored and skipped"


def _async_rows(rows):
    async def _call(method, base, endpoint, key, secret, params):
        return list(rows)

    return _call


_ORIG_SIGNED = cr._signed_request
import trading.learning as _learning  # noqa: E402

_ORIG_RECORD = _learning._record_learning_trade

