"""
Emergency close script for open ADAUSDT position.
"""
import asyncio
import os
import time
import hmac
import hashlib
import httpx
from dotenv import load_dotenv

load_dotenv()

async def close_adausdt():
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    base = "https://fapi.binance.com"

    # First: check current position
    ts = int(time.time() * 1000)
    qs = f"symbol=ADAUSDT&timestamp={ts}"
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{base}/fapi/v2/positionRisk?{qs}&signature={sig}",
                              headers={"X-MBX-APIKEY": api_key})
        positions = r.json()
        print("Current positions:", positions)

        long_pos = None
        short_pos = None
        for p in positions:
            amt = float(p.get("positionAmt", 0))
            ps = p.get("positionSide", "BOTH")
            if ps == "LONG" and amt > 0:
                long_pos = p
            elif ps == "SHORT" and amt < 0:
                short_pos = p
            elif ps == "BOTH" and amt != 0:
                long_pos = p  # one-way mode

        if not long_pos and not short_pos:
            print("No open positions found. Already closed!")
            return

        # Close LONG position
        if long_pos:
            amt = abs(float(long_pos["positionAmt"]))
            ps = long_pos.get("positionSide", "BOTH")
            print(f"\nClosing LONG position: {amt} ADA (positionSide={ps})")
            ts2 = int(time.time() * 1000)
            params = {
                "symbol": "ADAUSDT",
                "side": "SELL",
                "type": "MARKET",
                "quantity": amt,
                "reduceOnly": "true" if ps == "BOTH" else "false",
                "timestamp": ts2,
            }
            if ps == "LONG":
                params.pop("reduceOnly", None)
                params["positionSide"] = "LONG"

            qs2 = "&".join(f"{k}={v}" for k, v in params.items())
            sig2 = hmac.new(api_secret.encode(), qs2.encode(), hashlib.sha256).hexdigest()
            r2 = await client.post(
                f"{base}/fapi/v1/order?{qs2}&signature={sig2}",
                headers={"X-MBX-APIKEY": api_key},
            )
            print("Close LONG result:", r2.status_code, r2.text)

        # Close SHORT position
        if short_pos:
            amt = abs(float(short_pos["positionAmt"]))
            ps = short_pos.get("positionSide", "BOTH")
            print(f"\nClosing SHORT position: {amt} ADA (positionSide={ps})")
            ts3 = int(time.time() * 1000)
            params3 = {
                "symbol": "ADAUSDT",
                "side": "BUY",
                "type": "MARKET",
                "quantity": amt,
                "reduceOnly": "true" if ps == "BOTH" else "false",
                "timestamp": ts3,
            }
            if ps == "SHORT":
                params3.pop("reduceOnly", None)
                params3["positionSide"] = "SHORT"

            qs3 = "&".join(f"{k}={v}" for k, v in params3.items())
            sig3 = hmac.new(api_secret.encode(), qs3.encode(), hashlib.sha256).hexdigest()
            r3 = await client.post(
                f"{base}/fapi/v1/order?{qs3}&signature={sig3}",
                headers={"X-MBX-APIKEY": api_key},
            )
            print("Close SHORT result:", r3.status_code, r3.text)

if __name__ == "__main__":
    asyncio.run(close_adausdt())
