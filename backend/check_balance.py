import asyncio
import os
import time
import hmac
import hashlib
import httpx
from dotenv import load_dotenv

load_dotenv()

async def main():
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    timestamp = int(time.time() * 1000)
    query_string = f"recvWindow=10000&timestamp={timestamp}"
    signature = hmac.new(
        api_secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    
    url = f"https://fapi.binance.com/fapi/v2/account?{query_string}&signature={signature}"
    headers = {
        "X-MBX-APIKEY": api_key
    }
    
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(url, headers=headers)
            if res.status_code == 200:
                data = res.json()
                print("=== Binance Futures Balance ===")
                print(f"Total Wallet Balance: {data.get('totalWalletBalance')} USDT")
                print(f"Total Unearned PNL: {data.get('totalUnrealizedProfit')} USDT")
                print(f"Available Balance: {data.get('availableBalance')} USDT")
                
                # Print assets with positive balance
                print("\n=== Assets ===")
                assets = data.get("assets", [])
                for asset in assets:
                    wallet_balance = float(asset.get("walletBalance", 0) or 0)
                    if wallet_balance > 0:
                        print(f"{asset.get('asset')}: Wallet={wallet_balance}, Available={asset.get('availableBalance')}")
            else:
                print(f"Failed to fetch account info: {res.status_code} - {res.text}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
