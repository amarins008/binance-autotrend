import unittest


class TestRefactorSmoke(unittest.TestCase):
    def test_schemas_import_and_build(self):
        from schemas import AnalyzeRequest, OrderBookLevel, OrderBookSummary

        summary = OrderBookSummary(
            symbol="BTCUSDT",
            bidNotional=120.5,
            askNotional=98.25,
            imbalance=0.101,
            buyWall=OrderBookLevel(price=65000.0, qty=1.5),
            sellWall=None,
            spoofingRisk="LOW",
        )
        request = AnalyzeRequest(symbol="BTCUSDT", orderBook=summary)

        self.assertEqual(summary.symbol, "BTCUSDT")
        self.assertEqual(request.symbol, "BTCUSDT")
        self.assertEqual(request.orderBook.symbol, "BTCUSDT")
        self.assertAlmostEqual(summary.buyWall.price, 65000.0)

    def test_indicators_helpers_are_available(self):
        from indicators import _ema, _rsi

        self.assertAlmostEqual(_ema([10.0, 10.0, 10.0], 5), 10.0)
        self.assertGreater(_rsi([1.0, 2.0, 3.0, 4.0, 5.0], 2), 0.0)


if __name__ == "__main__":
    unittest.main()
