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
    
    print("=== Outgoing IP Check ===")
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get("https://api.ipify.org?format=json")
            ip = res.json().get("ip")
            print(f"Your Public IP (IPv4): {ip}")
    except Exception as e:
        print(f"Failed to get public IP: {e}")
        
    print("\n=== Binance API Check ===")
    if not api_key or not api_secret:
        print("Error: BINANCE_API_KEY or BINANCE_API_SECRET is missing in .env")
        return
        
    print(f"API Key Prefix: {api_key[:8]}...")
    print(f"API Secret Prefix: {api_secret[:8]}...")
    
    # Test public endpoint
    print("Testing public endpoint (/fapi/v1/ping)...")
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get("https://fapi.binance.com/fapi/v1/ping")
            print(f"Public Ping Status: {res.status_code}")
    except Exception as e:
        print(f"Public Ping Failed: {e}")
        
    # Test signed endpoint (account info)
    print("Testing signed endpoint (/fapi/v2/account)...")
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
            print(f"Binance HTTP Status: {res.status_code}")
            print(f"Response: {res.text}")
    except Exception as e:
        print(f"Request Failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
