"""Net-PnL accounting: gross price-delta minus recorded fee/funding estimates.

2026-09-29: stats and learning must judge performance NET of the costs the
close paths stamp on each record (feeEstUsdt, fundingEstUsdt). Records
without cost fields (pre-telemetry, paper, backfill) stay gross.
"""

import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import trading.trade_log as trade_log
import trading.trade_stats as trade_stats
from trading.trade_log import net_pnl_of, _apply_trade_log_delta
from trading.learning import _net_pnl_after_costs
from exchange.futures_orders import _funding_usdt_from_rate


def _stats() -> dict:
    return {
        "wins": 0,
        "losses": 0,
        "realizedPnl": 0.0,
        "winsToday": 0,
        "lossesToday": 0,
        "realizedPnlToday": 0.0,
        "lastTrades": [],
    }


class TestNetPnlOf(unittest.TestCase):
    def test_gross_only_record_stays_gross(self):
        self.assertEqual(net_pnl_of({"pnl": 1.25}), 1.25)

    def test_fee_subtracted(self):
        self.assertAlmostEqual(net_pnl_of({"pnl": 1.0, "feeEstUsdt": 0.3}), 0.7)

    def test_fee_and_funding_subtracted(self):
        self.assertAlmostEqual(
            net_pnl_of({"pnl": 1.0, "feeEstUsdt": 0.3, "fundingEstUsdt": 0.05}),
            0.65,
        )

    def test_alias_keys_accepted(self):
        self.assertAlmostEqual(net_pnl_of({"pnl": 2.0, "feeUsdt": 0.5, "fundingUsdt": 0.1}), 1.4)

    def test_invalid_values_are_ignored(self):
        self.assertEqual(net_pnl_of({"pnl": "abc"}), 0.0)
        self.assertEqual(net_pnl_of({}), 0.0)
        self.assertAlmostEqual(net_pnl_of({"pnl": 1.0, "feeEstUsdt": None}), 1.0)


class TestFundingUsdtFromRate(unittest.TestCase):
    def test_zero_inputs(self):
        self.assertEqual(_funding_usdt_from_rate(0.0, 0.0001, 3600.0), 0.0)
        self.assertEqual(_funding_usdt_from_rate(500.0, 0.0, 3600.0), 0.0)
        self.assertEqual(_funding_usdt_from_rate(500.0, 0.0001, 0.0), 0.0)

    def test_proportional_to_hold(self):
        # 500 USDT notional, 0.01% per 8h, held 4h -> half an expected event
        self.assertAlmostEqual(_funding_usdt_from_rate(500.0, 0.0001, 14400.0), 0.025)

    def test_capped_at_two_events(self):
        # held 5 days -> would be 15 events uncapped; must cap at 2
        self.assertAlmostEqual(_funding_usdt_from_rate(500.0, 0.0001, 5 * 86400.0), 0.1)


class TestApplyTradeLogDelta(unittest.TestCase):
    def test_fee_record_counts_net_and_classifies_by_net_sign(self):
        # gross +0.20 minus fee 0.30 -> NET loss: must count as a loss
        line = json.dumps(
            {
                "mode": "LIVE",
                "symbol": "BTCUSDT",
                "side": "LONG",
                "pnl": 0.20,
                "feeEstUsdt": 0.30,
                "closedAt": int(time.time()),
            }
        )
        stats = _apply_trade_log_delta(_stats(), [line], None)
        self.assertEqual(stats["wins"], 0)
        self.assertEqual(stats["losses"], 1)
        self.assertAlmostEqual(stats["realizedPnl"], -0.10)
        self.assertAlmostEqual(stats["realizedPnlToday"], -0.10)

    def test_record_without_costs_stays_gross(self):
        line = json.dumps(
            {
                "mode": "LIVE",
                "symbol": "BTCUSDT",
                "side": "LONG",
                "pnl": 0.40,
                "closedAt": int(time.time()),
            }
        )
        stats = _apply_trade_log_delta(_stats(), [line], None)
        self.assertEqual(stats["wins"], 1)
        self.assertAlmostEqual(stats["realizedPnl"], 0.40)

    def test_funding_only_record_counts_net(self):
        line = json.dumps(
            {
                "mode": "LIVE",
                "symbol": "BTCUSDT",
                "side": "LONG",
                "pnl": 0.50,
                "fundingEstUsdt": 0.10,
                "closedAt": int(time.time()),
            }
        )
        stats = _apply_trade_log_delta(_stats(), [line], None)
        self.assertAlmostEqual(stats["realizedPnl"], 0.40)


class TestLiveClosedTradesFromLog(unittest.TestCase):
    def test_pnl_field_is_net_and_gross_is_kept(self):
        now = int(time.time())
        lines = "\n".join(
            [
                json.dumps(
                    {
                        "mode": "LIVE",
                        "symbol": "BTCUSDT",
                        "side": "LONG",
                        "pnl": 0.60,
                        "feeEstUsdt": 0.10,
                        "closedAt": now,
                    }
                ),
                # pre-telemetry record: no cost fields -> gross
                json.dumps(
                    {
                        "mode": "LIVE",
                        "symbol": "ETHUSDT",
                        "side": "LONG",
                        "pnl": -0.25,
                        "closedAt": now,
                    }
                ),
            ]
        )
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "trades_log.jsonl"
            log_path.write_text(lines + "\n", encoding="utf-8")
            with mock.patch.object(trade_log, "TRADES_LOG_PATH", log_path), \
                 mock.patch.object(trade_log, "_TRADE_LOG_CACHE", {}):
                rows = trade_log._live_closed_trades_from_log(symbol=None, mode="ALL")
        by_sym = {r["symbol"]: r for r in rows}
        self.assertAlmostEqual(by_sym["BTCUSDT"]["_pnl"], 0.50)
        self.assertAlmostEqual(by_sym["BTCUSDT"]["_grossPnl"], 0.60)
        self.assertAlmostEqual(by_sym["ETHUSDT"]["_pnl"], -0.25)
        self.assertAlmostEqual(by_sym["ETHUSDT"]["_grossPnl"], -0.25)


class TestNetPnlAfterCosts(unittest.TestCase):
    def test_live_with_recorded_fee_and_funding(self):
        t = {"pnl": 1.0, "feeEstUsdt": 0.30, "fundingEstUsdt": 0.05, "qty": 1.0, "exit": 100.0}
        self.assertAlmostEqual(_net_pnl_after_costs(t, "LIVE"), 0.65)

    def test_live_without_fee_falls_back_to_roundtrip_estimate(self):
        # qty 1 @ exit 100 -> notional 100 -> 2 x 6bps = 0.12
        t = {"pnl": 1.0, "qty": 1.0, "exit": 100.0}
        self.assertAlmostEqual(_net_pnl_after_costs(t, "LIVE"), 0.88)

    def test_paper_stays_gross(self):
        t = {"pnl": 1.0, "feeEstUsdt": 0.30, "qty": 1.0, "exit": 100.0}
        self.assertAlmostEqual(_net_pnl_after_costs(t, "PAPER"), 1.0)


class TestTradeStatsAggregateLivePath(unittest.TestCase):
    """trade_stats is the module main.py aliases for /status — must be net."""

    def test_aggregate_is_net_of_costs(self):
        now = int(time.time())
        lines = "\n".join(
            [
                json.dumps(
                    {
                        "mode": "LIVE",
                        "symbol": "BTCUSDT",
                        "side": "LONG",
                        "pnl": 0.60,
                        "feeEstUsdt": 0.10,
                        "fundingEstUsdt": 0.05,
                        "closedAt": now,
                    }
                ),
                json.dumps(
                    {
                        "mode": "LIVE",
                        "symbol": "ETHUSDT",
                        "side": "LONG",
                        "pnl": -0.25,
                        "closedAt": now,
                    }
                ),
            ]
        )
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "trades_log.jsonl"
            log_path.write_text(lines + "\n", encoding="utf-8")
            with mock.patch.object(trade_stats, "TRADES_LOG_PATH", log_path), \
                 mock.patch.object(trade_stats, "_LIVE_STATS_CACHE", {}):
                stats = trade_stats._aggregate_live_trade_stats_from_log(None)
        self.assertEqual(stats["wins"], 1)   # BTC net +0.45 win
        self.assertEqual(stats["losses"], 1)  # ETH gross loss
        self.assertAlmostEqual(stats["realizedPnl"], 0.20)

    def test_by_symbol_aggregate_is_net(self):
        now = int(time.time())
        line = json.dumps(
            {
                "mode": "LIVE",
                "symbol": "BTCUSDT",
                "side": "LONG",
                "pnl": 0.60,
                "feeEstUsdt": 0.10,
                "closedAt": now,
            }
        )
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "trades_log.jsonl"
            log_path.write_text(line + "\n", encoding="utf-8")
            with mock.patch.object(trade_stats, "TRADES_LOG_PATH", log_path):
                out = trade_stats._aggregate_live_trade_stats_by_symbol_from_log()
        self.assertAlmostEqual(out["BTCUSDT"]["realizedPnl"], 0.50)


if __name__ == "__main__":
    unittest.main()
