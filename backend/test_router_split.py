
import unittest

import main


class TestRouterSplit(unittest.TestCase):
    def test_router_modules_register_core_routes(self):
        import routers.analysis_routes  # noqa: F401
        import routers.autotrade_routes  # noqa: F401
        import routers.system_routes  # noqa: F401

        paths = {getattr(route, "path", None) for route in main.app.routes}
        expected = {
            "/health",
            "/system/restart",
            "/debug/env-status",
            "/debug/binance-auth-check",
            "/risk-config",
            "/symbol-meta",
            "/analyze",
            "/analyze-vision",
            "/intel/analyze",
            "/intel/rank",
            "/risk-alerts",
            "/strategy/parse",
            "/autotrade/start",
            "/autotrade/stop",
            "/autotrade/reset",
            "/autotrade/status",
        }
        self.assertTrue(expected.issubset(paths))


class TestExchangeFiltersCache(unittest.IsolatedAsyncioTestCase):
    async def test_exchange_filters_uses_cache(self):
        calls = []

        class FakeResponse:
            status_code = 200

            def json(self):
                return {
                    "symbols": [
                        {
                            "status": "TRADING",
                            "filters": [
                                {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                                {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                                {"filterType": "MIN_NOTIONAL", "notional": "5"},
                            ],
                        }
                    ]
                }

        async def fake_data_get(path: str):
            calls.append(path)
            return FakeResponse()

        original = main._data_get
        try:
            main._data_get = fake_data_get
            first = await main._exchange_filters("BTCUSDT")
            second = await main._exchange_filters("BTCUSDT")
        finally:
            main._data_get = original

        self.assertEqual(first["stepSize"], 0.001)
        self.assertEqual(second["tickSize"], 0.1)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
