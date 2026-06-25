"""Test TradingView MCP integration with live data."""

import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from trading.tradingview_mcp import get_tv_mcp, TRADINGVIEW_TA_AVAILABLE

def test_tradingview_availability():
    """Test if TradingView library is available."""
    print("=" * 70)
    print("TRADINGVIEW MCP AVAILABILITY TEST")
    print("=" * 70)
    
    print(f"\nTradingView TA Library Available: {TRADINGVIEW_TA_AVAILABLE}")
    
    if not TRADINGVIEW_TA_AVAILABLE:
        print("✗ tradingview-ta library is NOT installed or not importable")
        print("  Install with: pip install tradingview-ta")
        return False
    
    print("✓ tradingview-ta library is available")
    return True

def test_tradingview_client():
    """Test TradingView client creation and basic functionality."""
    print("\n" + "=" * 70)
    print("TRADINGVIEW CLIENT TEST")
    print("=" * 70)
    
    config = {
        "tradingviewEnabled": True,
        "tradingviewCacheTtl": 60,
        "tradingviewRateLimit": 30,
        "tradingviewTimeout": 5.0,
        "tradingviewConfidenceBoost": 0.05,
        "tradingviewMaxFailures": 5
    }
    
    try:
        tv_client = get_tv_mcp(config)
        print(f"✓ TradingView client created successfully")
        print(f"  - Enabled: {tv_client.is_enabled()}")
        print(f"  - Cache TTL: {tv_client.cache_ttl}s")
        print(f"  - Rate Limit: {tv_client.rate_limit_per_minute}/min")
        print(f"  - Timeout: {tv_client.timeout}s")
        return tv_client
    except Exception as e:
        print(f"✗ Error creating TradingView client: {e}")
        return None

def test_tradingview_signal(tv_client):
    """Test getting TradingView signal."""
    print("\n" + "=" * 70)
    print("TRADINGVIEW SIGNAL TEST")
    print("=" * 70)
    
    if not tv_client:
        print("✗ No TradingView client available")
        return None
    
    symbol = "BTCUSDT"
    internal_signal = "LONG"
    internal_confidence = 0.75
    
    print(f"Testing signal for {symbol} with internal signal {internal_signal}")
    print(f"Internal confidence: {internal_confidence}")
    
    try:
        result = tv_client.get_signal(symbol, internal_signal, internal_confidence)
        
        if result:
            print(f"✓ TradingView signal retrieved successfully")
            print(f"  - Signal: {result.signal}")
            print(f"  - Confidence: {result.confidence:.3f}")
            print(f"  - Source: {result.source}")
            if result.metadata:
                print(f"  - Metadata: {result.metadata}")
            return result
        else:
            print(f"✗ No TradingView signal returned")
            print(f"  - Client enabled: {tv_client.is_enabled()}")
            print(f"  - Health status: {tv_client.get_health_status()}")
            return None
            
    except Exception as e:
        print(f"✗ Error getting TradingView signal: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_tradingview_position_guidance(tv_client):
    """Test getting TradingView position guidance."""
    print("\n" + "=" * 70)
    print("TRADINGVIEW POSITION GUIDANCE TEST")
    print("=" * 70)
    
    if not tv_client:
        print("✗ No TradingView client available")
        return None
    
    symbol = "BTCUSDT"
    side = "LONG"
    
    print(f"Testing position guidance for {symbol} {side}")
    
    try:
        guidance = tv_client.get_position_guidance(symbol, side)
        
        if guidance:
            print(f"✓ Position guidance retrieved successfully")
            print(f"  - Recommendation: {guidance.get('recommendation')}")
            print(f"  - Signal: {guidance.get('signal')}")
            print(f"  - Strength: {guidance.get('strength'):.3f}")
            print(f"  - Oscillators: {guidance.get('oscillators')}")
            print(f"  - Moving Averages: {guidance.get('moving_averages')}")
            return guidance
        else:
            print(f"✗ No position guidance returned")
            print(f"  - Client enabled: {tv_client.is_enabled()}")
            print(f"  - Health status: {tv_client.get_health_status()}")
            return None
            
    except Exception as e:
        print(f"✗ Error getting position guidance: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_position_logic():
    """Test position management logic with TradingView."""
    print("\n" + "=" * 70)
    print("POSITION MANAGEMENT LOGIC TEST")
    print("=" * 70)
    
    from trading.position import (
        should_extend_tp_with_tradingview,
        should_trail_sl_with_tradingview,
        should_exit_early_with_tradingview,
        get_tradingview_tp_extension_pct,
        get_tradingview_sl_trailing_pct
    )
    
    config = {
        "tradingviewTpExtensionEnabled": True,
        "tradingviewTpExtensionMinStrength": 0.7,
        "tradingviewTpExtensionBasePct": 0.2,
        "tradingviewTpExtensionMaxPct": 0.5,
        "tradingviewSlTrailingEnabled": True,
        "tradingviewSlTrailingMinStrength": 0.6,
        "tradingviewSlTrailingBasePct": 0.15,
        "tradingviewSlTrailingMaxPct": 0.3,
        "tradingviewEarlyExitEnabled": True,
        "tradingviewEarlyExitMinStrength": 0.7
    }
    
    # Test with strong BUY signal
    mock_guidance = {
        "recommendation": "STRONG_BUY",
        "strength": 0.85,
        "oscillators": {"BUY": 8, "SELL": 2},
        "moving_averages": {"BUY": 7, "SELL": 3}
    }
    
    side = "LONG"
    symbol = "BTCUSDT"
    
    should_extend = should_extend_tp_with_tradingview(side, mock_guidance, config, symbol)
    should_trail = should_trail_sl_with_tradingview(side, mock_guidance, config, symbol)
    should_exit = should_exit_early_with_tradingview(side, mock_guidance, config, symbol)
    
    print(f"Mock guidance: STRONG_BUY (strength: 0.85)")
    print(f"  - Should extend TP: {should_extend}")
    print(f"  - Should trail SL: {should_trail}")
    print(f"  - Should exit early: {should_exit}")
    
    if should_extend:
        tp_pct = get_tradingview_tp_extension_pct(side, mock_guidance, config)
        print(f"  - TP extension %: {tp_pct:.3f}")
    
    if should_trail:
        sl_pct = get_tradingview_sl_trailing_pct(side, mock_guidance, config)
        print(f"  - SL trailing %: {sl_pct:.3f}")
    
    # Test with reversal signal
    mock_reversal = {
        "recommendation": "STRONG_SELL",
        "strength": 0.85,
        "oscillators": {"BUY": 1, "SELL": 9},
        "moving_averages": {"BUY": 2, "SELL": 8}
    }
    
    should_exit_rev = should_exit_early_with_tradingview(side, mock_reversal, config, symbol)
    print(f"\nMock reversal: STRONG_SELL (strength: 0.85)")
    print(f"  - Should exit early: {should_exit_rev}")

def main():
    """Run all tests."""
    print("\n" + "=" * 70)
    print("TRADINGVIEW MCP LIVE INTEGRATION TEST")
    print("=" * 70)
    
    # Test 1: Library availability
    if not test_tradingview_availability():
        print("\n" + "=" * 70)
        print("CONCLUSION")
        print("=" * 70)
        print("✗ TradingView MCP cannot work - library not available")
        print("=" * 70)
        return
    
    # Test 2: Client creation
    tv_client = test_tradingview_client()
    if not tv_client:
        print("\n" + "=" * 70)
        print("CONCLUSION")
        print("=" * 70)
        print("✗ TradingView MCP cannot work - client creation failed")
        print("=" * 70)
        return
    
    # Test 3: Signal retrieval
    signal_result = test_tradingview_signal(tv_client)
    
    # Test 4: Position guidance
    guidance = test_tradingview_position_guidance(tv_client)
    
    # Test 5: Position logic
    test_position_logic()
    
    # Conclusion
    print("\n" + "=" * 70)
    print("CONCLUSION")
    print("=" * 70)
    
    if signal_result and guidance:
        print("✓ TradingView MCP is WORKING correctly")
        print("  - Can retrieve signals from TradingView")
        print("  - Can provide position guidance")
        print("  - Position management logic is functional")
        print("\nIf it's not being used in trades, check:")
        print("  1. Config: tradingviewEnabled = true")
        print("  2. Config: tradingviewTpExtensionEnabled = true")
        print("  3. Config: tradingviewSlTrailingEnabled = true")
        print("  4. Config: tradingviewEarlyExitEnabled = true")
    else:
        print("✗ TradingView MCP is NOT working correctly")
        print("  - Cannot retrieve signals or guidance")
        print("  - This could be due to:")
        print("    - Network issues")
        print("    - TradingView API rate limits")
        print("    - Invalid symbol")
        print("    - Library compatibility issues")
    
    print("=" * 70)

if __name__ == "__main__":
    main()
