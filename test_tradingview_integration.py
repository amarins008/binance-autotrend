"""Test TradingView integration with tradingview-ta library."""

import sys
sys.path.insert(0, 'backend')

from trading.tradingview_mcp import get_tv_client, reset_tv_client

def test_tradingview_client():
    """Test TradingView client with real data."""
    
    # Test configuration
    config = {
        "tradingviewEnabled": True,
        "tradingviewCacheTtl": 60,
        "tradingviewRateLimit": 30,
        "tradingviewTimeout": 5.0,
        "tradingviewConfidenceBoost": 0.05,
        "tradingviewMaxFailures": 5
    }
    
    # Reset any existing instance
    reset_tv_client()
    
    # Get client instance
    tv_client = get_tv_client(config)
    
    print("=== TradingView Client Test ===")
    print(f"Enabled: {tv_client.is_enabled()}")
    print(f"Health status: {tv_client.get_health_status()}")
    
    # Test with a popular crypto pair
    symbol = "BTCUSDT"
    internal_signal = "LONG"
    internal_confidence = 0.75
    
    print(f"\nTesting with {symbol}...")
    print(f"Internal signal: {internal_signal}, confidence: {internal_confidence}")
    
    try:
        result = tv_client.get_signal(symbol, internal_signal, internal_confidence)
        
        if result:
            print(f"✅ TradingView signal: {result.signal.value}")
            print(f"   Confidence: {result.confidence:.3f}")
            print(f"   Metadata: {result.metadata}")
            
            # Test confirmation
            boost = tv_client.confirm_signal(result, internal_signal)
            print(f"   Confidence boost: +{boost:.3f}")
        else:
            print("❌ No signal returned (fallback to internal only)")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Test health status after call
    print(f"\nHealth status after call: {tv_client.get_health_status()}")
    
    # Test with cache
    print(f"\nTesting cache (second call should be cached)...")
    result2 = tv_client.get_signal(symbol, internal_signal, internal_confidence)
    if result2:
        print(f"✅ Cached signal: {result2.signal.value}")
    
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    test_tradingview_client()
