import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import main


class TestCoinRankRoute(unittest.TestCase):
    def test_intel_rank_route_registered(self):
        routes = {getattr(route, "path", None) for route in main.app.routes}
        self.assertIn("/intel/rank", routes)

    def test_intel_rank_route_orders_by_score(self):
        client = TestClient(main.app)

        async def fake_pick(_cfg):
            return (
                "BTCUSDT",
                {"signal": "SHORT", "confidence": 0.91, "execution": {"spreadBps": 2.0, "momentumPct": -1.4}},
                [
                    {
                        "symbol": "ETHUSDT",
                        "signal": "LONG",
                        "confidence": 0.81,
                        "score": 0.93,
                        "momentumPct": 1.2,
                        "spreadBps": 4.2,
                    },
                    {
                        "symbol": "BTCUSDT",
                        "signal": "SHORT",
                        "confidence": 0.91,
                        "score": 1.12,
                        "momentumPct": 0.8,
                        "spreadBps": 2.1,
                    },
                ],
            )

        with patch("main._pick_best_symbol_from_scan", side_effect=fake_pick):
            response = client.post(
                "/intel/rank",
                json={"symbols": [], "scanMarket": True, "topN": 2},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["bestSymbol"], "BTCUSDT")
        self.assertEqual(payload["bestSignal"], "SHORT")
        self.assertEqual(payload["positionOrder"], ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(payload["ranked"][0]["positionOrder"], 1)
        self.assertEqual(payload["ranked"][0]["symbol"], "BTCUSDT")


if __name__ == "__main__":
    unittest.main()
