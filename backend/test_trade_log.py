import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import trading.trade_log as trade_log


class TestApplyTradeLogDelta(unittest.TestCase):
    def test_applies_live_win_and_loss(self):
        stats = {
            "wins": 0,
            "losses": 0,
            "realizedPnl": 0.0,
            "winsToday": 0,
            "lossesToday": 0,
            "realizedPnlToday": 0.0,
            "lastTrades": [],
        }
        lines = [
            json.dumps({"mode": "LIVE", "symbol": "BTCUSDT", "pnl": 1.5, "closedAt": 1_700_000_000}),
            json.dumps({"mode": "LIVE", "symbol": "BTCUSDT", "pnl": -0.5, "closedAt": 1_700_000_100}),
            json.dumps({"mode": "PAPER", "symbol": "BTCUSDT", "pnl": 9.0, "closedAt": 1_700_000_200}),
        ]
        out = trade_log._apply_trade_log_delta(stats, lines, "BTCUSDT")
        self.assertEqual(out["wins"], 1)
        self.assertEqual(out["losses"], 1)
        self.assertAlmostEqual(out["realizedPnl"], 1.0)
        self.assertEqual(len(out["lastTrades"]), 2)


class TestRotateTradesLog(unittest.TestCase):
    def test_archives_old_non_live_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "trades_log.jsonl"
            old_ts = 1
            recent_ts = int(__import__("time").time())
            log_path.write_text(
                json.dumps({"mode": "SCAN", "ts": old_ts, "symbol": "BTCUSDT"}) + "\n"
                + json.dumps({"mode": "LIVE", "symbol": "BTCUSDT", "pnl": 0.5, "closedAt": recent_ts}) + "\n",
                encoding="utf-8",
            )
            with mock.patch.object(trade_log, "TRADES_LOG_PATH", log_path), mock.patch.object(
                trade_log, "_TRADES_LOG_ROTATE_MIN_BYTES", 1
            ), mock.patch.object(trade_log, "_TRADES_LOG_ROTATION_COOLDOWN_SEC", 0), mock.patch.object(
                trade_log, "_TRADES_LOG_KEEP_DAYS", 45
            ):
                trade_log._TRADES_LOG_LAST_ROTATION = 0.0
                rotated = trade_log._maybe_rotate_trades_log(force=True)
            self.assertTrue(rotated)
            kept = log_path.read_text(encoding="utf-8")
            self.assertIn("LIVE", kept)
            self.assertNotIn("SCAN", kept)


if __name__ == "__main__":
    unittest.main()
