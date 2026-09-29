"""-1021 auto-recovery: a stale time offset must trigger a re-sync and one
retry instead of surfacing the raw Binance timestamp error (2026-09-29)."""

import asyncio
import json
import time
import unittest
from unittest import mock

from exchange import binance_client as bc


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class _FakeClient:
    """Signed endpoint returns -1021 once, then succeeds. /fapi/v1/time works."""

    def __init__(self):
        self.signed_calls = 0
        self.time_calls = 0

    async def get(self, url, headers=None, **kw):
        if "/fapi/v1/time" in url:
            self.time_calls += 1
            return _FakeResponse(200, {"serverTime": int(time.time() * 1000) + 4500})
        self.signed_calls += 1
        if self.signed_calls == 1:
            return _FakeResponse(400, {"code": -1021, "msg": "Timestamp for this request was 1000ms ahead of the server's time."})
        return _FakeResponse(200, {"ok": True})


class TestSignedRequest1021Retry(unittest.TestCase):
    def test_1021_triggers_resync_and_retry(self):
        fake = _FakeClient()
        async def run():
            with mock.patch.object(bc, "_HTTP", fake), \
                 mock.patch.object(bc, "_TIME_OFFSET_LAST_SYNC", time.time()), \
                 mock.patch.object(bc, "_TIME_OFFSET_MS", -4500), \
                 mock.patch.object(bc, "_TIME_SYNC_INFLIGHT", False):
                res = await bc._signed_request(
                    "GET", "https://fapi.binance.com", "/fapi/v2/balance",
                    "k", "s", {},
                )
                return res
        res = asyncio.run(run())
        self.assertEqual(res, {"ok": True})
        # 1 failed signed call + 1 successful retry + time syncs
        self.assertEqual(fake.signed_calls, 2)
        self.assertGreaterEqual(fake.time_calls, 1)

    def test_1021_twice_still_raises(self):
        class _Always1021(_FakeClient):
            async def get(self, url, headers=None, **kw):
                if "/fapi/v1/time" in url:
                    self.time_calls += 1
                    return _FakeResponse(200, {"serverTime": int(time.time() * 1000) + 4500})
                self.signed_calls += 1
                return _FakeResponse(400, {"code": -1021, "msg": "ahead"})
        fake = _Always1021()
        async def run():
            with mock.patch.object(bc, "_HTTP", fake), \
                 mock.patch.object(bc, "_TIME_OFFSET_LAST_SYNC", time.time()), \
                 mock.patch.object(bc, "_TIME_OFFSET_MS", -4500), \
                 mock.patch.object(bc, "_TIME_SYNC_INFLIGHT", False):
                await bc._signed_request(
                    "GET", "https://fapi.binance.com", "/fapi/v2/balance",
                    "k", "s", {},
                )
        with self.assertRaises(Exception):
            asyncio.run(run())
        self.assertEqual(fake.signed_calls, 2)  # exactly one retry, no storm


if __name__ == "__main__":
    unittest.main()
