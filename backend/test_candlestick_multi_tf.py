import unittest
import asyncio
from unittest.mock import patch, AsyncMock
import time

# Import helper functions
from main import (
    _detect_timeframe_patterns,
    _candlestick_pattern_context,
    _decision_data_layers,
    _intel_data_quality_guard,
    _last_decision_pattern_metrics,
    intel_analyze,
    IntelAnalyzeRequest
)
from hermes_agents import new_agent_state

class TestCandlestickMultiTimeframe(unittest.IsolatedAsyncioTestCase):
    
    def setUp(self):
        # Sample klines: [open_time, open, high, low, close, volume, close_time, ...]
        # For a Doji (very small body): close close to open
        self.kline_doji = [
            [1700000000000, "100.0", "101.0", "99.0", "100.0", "10.0", 1700000000000 + 59999, "1000.0", 100, "5.0", "5.0", "0"], # prev prev
            [1700000060000, "100.0", "101.0", "99.0", "100.05", "10.0", 1700000060000 + 59999, "1000.0", 100, "5.0", "5.0", "0"], # prev (completed)
            [1700000120000, "100.0", "101.0", "99.0", "100.02", "10.0", 1700000120000 + 59999, "1000.0", 100, "5.0", "5.0", "0"]  # curr (forming - not used in detection)
        ]
        
        # For a Bullish Engulfing: prev is red, curr engulfs prev body
        self.kline_bullish_engulfing = [
            [1700000000000, "100.0", "101.0", "99.0", "100.0", "10.0", 1700000000000 + 59999, "1000.0", 100, "5.0", "5.0", "0"], # prev prev
            [1700000060000, "100.0", "101.0", "98.0", "97.0", "10.0", 1700000060000 + 59999, "1000.0", 100, "5.0", "5.0", "0"],  # prev completed (red, body size = 3.0)
            [1700000120000, "96.5", "102.0", "96.0", "101.5", "10.0", 1700000120000 + 59999, "1000.0", 100, "5.0", "5.0", "0"]   # curr completed (green, body size = 5.0, engulfs 97-100)
        ]

    def test_detect_timeframe_patterns_doji(self):
        tags, bias = _detect_timeframe_patterns(self.kline_doji)
        self.assertIn("doji", tags)
        self.assertAlmostEqual(bias, 0.0)

    def test_detect_timeframe_patterns_bullish_engulfing(self):
        # We need at least 3 klines, let's append completed candle at the end
        klines = [
            [0, "100.0", "100.0", "100.0", "100.0", "10", 0, "10", 0, "5", "5", "0"], # dummy prev prev
            [0, "100.0", "101.0", "99.0", "97.0", "10", 0, "10", 0, "5", "5", "0"], # completed prev (red, open=100, close=97)
            [0, "96.0", "102.0", "95.0", "101.0", "10", 0, "10", 0, "5", "5", "0"], # completed curr (green, open=96, close=101)
            [0, "101.0", "101.0", "101.0", "101.0", "10", 0, "10", 0, "5", "5", "0"] # active forming candle (forming)
        ]
        tags, bias = _detect_timeframe_patterns(klines)
        self.assertIn("bullish_engulfing", tags)
        self.assertGreater(bias, 0.0)

    @patch("main._cached_klines")
    async def test_candlestick_pattern_context_confluence(self, mock_cached_klines):
        # Mock 5m to return a Doji
        # Mock 15m to return a Bullish Engulfing
        klines_5m = [
            [0, "100.0", "100.0", "100.0", "100.0", "10", 0, "10", 0, "5", "5", "0"],
            [0, "100.0", "101.0", "99.0", "100.02", "10", 0, "10", 0, "5", "5", "0"], # completed doji
            [0, "100.0", "100.0", "100.0", "100.0", "10", 0, "10", 0, "5", "5", "0"]  # active forming
        ]
        
        klines_15m = [
            [0, "100.0", "100.0", "100.0", "100.0", "10", 0, "10", 0, "5", "5", "0"],
            [0, "100.0", "101.0", "99.0", "97.0", "10", 0, "10", 0, "5", "5", "0"],  # completed red
            [0, "96.0", "102.0", "95.0", "101.0", "10", 0, "10", 0, "5", "5", "0"], # completed green engulfing
            [0, "101.0", "101.0", "101.0", "101.0", "10", 0, "10", 0, "5", "5", "0"]  # active forming
        ]
        
        async def mock_get(symbol, interval, limit):
            if interval == "5m":
                return klines_5m
            return klines_15m
            
        mock_cached_klines.side_effect = mock_get
        
        res = await _candlestick_pattern_context("BTCUSDT")
        
        self.assertTrue(res["ok"])
        self.assertIn("5m_doji", res["tags"])
        self.assertIn("doji", res["tags"])
        self.assertIn("15m_bullish_engulfing", res["tags"])
        self.assertIn("bullish_engulfing", res["tags"])
        
        # Combined bias: (bias_5m * 0.6) + (bias_15m * 0.4)
        # bias_5m = 0.0 (Doji)
        # bias_15m = 0.04 (Bullish Engulfing)
        # expected_bias = 0.0 * 0.6 + 0.04 * 0.4 = 0.016
        self.assertAlmostEqual(res["bias"], 0.016, places=5)
        self.assertAlmostEqual(res["score"], 0.016 * 60.0, places=4)

    @patch("main._cached_klines")
    async def test_candlestick_pattern_context_graceful_fallback(self, mock_cached_klines):
        # Test network failure / exception fallback
        mock_cached_klines.side_effect = Exception("Binance API error")
        
        res = await _candlestick_pattern_context("BTCUSDT")
        
        self.assertFalse(res["ok"])
        self.assertEqual(res["tags"], [])
        self.assertEqual(res["bias"], 0.0)
        self.assertEqual(res["score"], 0.0)

    def test_decision_data_layers_keep_news_as_guard_only(self):
        layers = _decision_data_layers(
            symbol="BTCUSDT",
            signal="LONG",
            confidence=0.78,
            setup="test setup",
            long_score=8,
            short_score=3,
            momentum={"momentumPct": 0.2, "volumeRatio": 1.4},
            precision={"trendUp": True, "trendDown": False, "atrPct": 0.9},
            execution={"spreadBps": 4.2, "lastFundingRate": 0.004},
            order_book={"imbalance": 0.08},
            candle_ctx={"ok": True, "bias": 0.016, "tags": ["15m_bullish_engulfing"]},
            notes=["core signal"],
        )

        self.assertEqual(layers["schema"], "hermes-decision-data-v1")
        self.assertEqual(layers["marketCore"]["signal"], "LONG")
        self.assertEqual(layers["newsSentimentGuard"]["decisionImpact"], "none_until_news_agent_is_connected")
        self.assertIn("riskGuards", layers["policy"])
        self.assertTrue(any(x["name"] == "volatility" and x["state"] == "block_bias" for x in layers["riskGuards"]["guards"]))

    def test_data_quality_guard_blocks_invalid_core_fields_only(self):
        ok = _intel_data_quality_guard({"symbol": "BTCUSDT", "signal": "WAIT", "confidence": 0.5, "execution": {}})
        bad = _intel_data_quality_guard({"symbol": "BTCUSDT", "signal": "BUY", "confidence": 1.2, "execution": {}})

        self.assertTrue(ok["ok"])
        self.assertIn("spread_missing", ok["issues"])
        self.assertFalse(bad["ok"])
        self.assertIn("signal_invalid", bad["issues"])

    def test_new_guard_agents_are_registered(self):
        agents = new_agent_state()["agents"]

        self.assertIn("data_quality_guard", agents)
        self.assertIn("news_sentiment_guard", agents)

if __name__ == "__main__":
    unittest.main()
