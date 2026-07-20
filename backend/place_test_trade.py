import asyncio
import os
from dotenv import load_dotenv

# Import main to ensure all configurations, env variables, and modules are loaded
import main
from exchange.futures_orders import place_futures_order

load_dotenv()

async def run_test():
    symbol = "ADAUSDT"
    print(f"=== Starting Order Placement Test on {symbol} ===")
    
    # Place a small LONG order
    try:
        print(f"Placing a small LONG order on {symbol} (55 USDT, leverage=5)...")
        order_res = await place_futures_order(
            symbol=symbol,
            side="LONG",
            usdt_amount=55.0,
            leverage=5,
            margin_type="ISOLATED",
            tp_pct=2.0,
            sl_pct=1.0
        )
        print("\n=== Order Placement Result ===")
        # Show key info
        if isinstance(order_res, dict):
            print(f"Mode: {order_res.get('mode')}")
            entry = order_res.get('entry', {})
            if isinstance(entry, dict):
                print(f"Order ID: {entry.get('orderId')}")
                print(f"Symbol: {entry.get('symbol')}")
                print(f"Side: {entry.get('side')}")
                print(f"Qty: {entry.get('origQty')}")
                print(f"Status: {entry.get('status')}")
            protective = order_res.get('protective', {})
            if isinstance(protective, dict):
                if protective.get('warning'):
                    print(f"\n*** TP/SL WARNING: {protective['warning'][:200]}")
                else:
                    print(f"\nTP: {protective.get('tp')}")
                    print(f"SL: {protective.get('sl')}")
            guardian = order_res.get('localGuardian')
            if guardian:
                print(f"\nLocal Guardian: {guardian.get('active')} TP={guardian.get('tp')} SL={guardian.get('sl')}")
        else:
            print(order_res)
        
    except Exception as e:
        print(f"Order placement failed: {e}")
        return

    # Wait for a few seconds
    print("\n--- Waiting 8 seconds before closing the position... ---")
    await asyncio.sleep(8)

    # Close the position
    try:
        print(f"\n=== Closing position for {symbol} ===")
        close_res = await place_futures_order(
            symbol=symbol,
            side="CLOSE"
        )
        print("Close Result:")
        print(close_res)
        print("\n=== TEST PASSED: Open + Close successful! ===")
        
    except Exception as e:
        print(f"Close position failed: {e}")

if __name__ == "__main__":
    asyncio.run(run_test())
