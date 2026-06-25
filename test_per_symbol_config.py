"""Test per-symbol TradingView configuration."""

import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from trading.position import (
    should_extend_tp_with_tradingview,
    should_trail_sl_with_tradingview,
    should_exit_early_with_tradingview
)

def test_per_symbol_override():
    """Test per-symbol override configuration."""
    print("=" * 60)
    print("TEST: Per-Symbol Configuration Override")
    print("=" * 60)
    
    # Config with global defaults and per-symbol overrides
    config = {
        "tradingviewTpExtensionEnabled": False,  # Global: disabled
        "tradingviewSlTrailingEnabled": False,  # Global: disabled
        "tradingviewEarlyExitEnabled": False,  # Global: disabled
        # Per-symbol overrides
        "tradingviewTpExtensionOverride": {
            "BTCUSDT": True,  # Enable for BTC only
            "ETHUSDT": False  # Explicit disable for ETH
        },
        "tradingviewSlTrailingOverride": {
            "BTCUSDT": True,  # Enable for BTC only
            "ETHUSDT": True   # Enable for ETH only
        },
        "tradingviewEarlyExitOverride": {
            "BTCUSDT": True,  # Enable for BTC only
            "SOLUSDT": False  # Explicit disable for SOL
        }
    }
    
    # Mock guidance
    mock_guidance = {
        "recommendation": "STRONG_BUY",
        "strength": 0.9,
        "oscillators": {"BUY": 10, "SELL": 0},
        "moving_averages": {"BUY": 10, "SELL": 0}
    }
    
    side = "LONG"
    
    # Test BTCUSDT (all overrides enabled)
    print("\n--- BTCUSDT (All overrides enabled) ---")
    should_extend_btc = should_extend_tp_with_tradingview(side, mock_guidance, config, "BTCUSDT")
    should_trail_btc = should_trail_sl_with_tradingview(side, mock_guidance, config, "BTCUSDT")
    should_exit_btc = should_exit_early_with_tradingview(side, mock_guidance, config, "BTCUSDT")
    
    print(f"TP Extension: {should_extend_btc} (Expected: True)")
    print(f"SL Trailing: {should_trail_btc} (Expected: True)")
    print(f"Early Exit: {should_exit_btc} (Expected: True)")
    
    assert should_extend_btc == True, "BTCUSDT TP extension should be True"
    assert should_trail_btc == True, "BTCUSDT SL trailing should be True"
    assert should_exit_btc == True, "BTCUSDT early exit should be True"
    print("✓ BTCUSDT overrides working correctly")
    
    # Test ETHUSDT (TP disabled, SL enabled, no early exit override)
    print("\n--- ETHUSDT (TP disabled, SL enabled, no early exit override) ---")
    should_extend_eth = should_extend_tp_with_tradingview(side, mock_guidance, config, "ETHUSDT")
    should_trail_eth = should_trail_sl_with_tradingview(side, mock_guidance, config, "ETHUSDT")
    should_exit_eth = should_exit_early_with_tradingview(side, mock_guidance, config, "ETHUSDT")
    
    print(f"TP Extension: {should_extend_eth} (Expected: False)")
    print(f"SL Trailing: {should_trail_eth} (Expected: True)")
    print(f"Early Exit: {should_exit_eth} (Expected: False - uses global default)")
    
    assert should_extend_eth == False, "ETHUSDT TP extension should be False"
    assert should_trail_eth == True, "ETHUSDT SL trailing should be True"
    assert should_exit_eth == False, "ETHUSDT early exit should be False (global default)"
    print("✓ ETHUSDT overrides working correctly")
    
    # Test SOLUSDT (no overrides, uses global defaults)
    print("\n--- SOLUSDT (No overrides, uses global defaults) ---")
    should_extend_sol = should_extend_tp_with_tradingview(side, mock_guidance, config, "SOLUSDT")
    should_trail_sol = should_trail_sl_with_tradingview(side, mock_guidance, config, "SOLUSDT")
    should_exit_sol = should_exit_early_with_tradingview(side, mock_guidance, config, "SOLUSDT")
    
    print(f"TP Extension: {should_extend_sol} (Expected: False - global default)")
    print(f"SL Trailing: {should_trail_sol} (Expected: False - global default)")
    print(f"Early Exit: {should_exit_sol} (Expected: False - global default)")
    
    assert should_extend_sol == False, "SOLUSDT TP extension should be False (global default)"
    assert should_trail_sol == False, "SOLUSDT SL trailing should be False (global default)"
    assert should_exit_sol == False, "SOLUSDT early exit should be False (global default)"
    print("✓ SOLUSDT using global defaults correctly")
    
    # Test with global enabled and no overrides
    print("\n--- Global enabled, no overrides ---")
    config_global_enabled = {
        "tradingviewTpExtensionEnabled": True,
        "tradingviewSlTrailingEnabled": True,
        "tradingviewEarlyExitEnabled": True,
        "tradingviewTpExtensionOverride": {},
        "tradingviewSlTrailingOverride": {},
        "tradingviewEarlyExitOverride": {}
    }
    
    should_extend_global = should_extend_tp_with_tradingview(side, mock_guidance, config_global_enabled, "ADAUSDT")
    should_trail_global = should_trail_sl_with_tradingview(side, mock_guidance, config_global_enabled, "ADAUSDT")
    
    # For early exit, need reversal signal
    mock_guidance_reversal = {
        "recommendation": "STRONG_SELL",
        "strength": 0.9,
        "oscillators": {"BUY": 0, "SELL": 10, "RSI": 75},
        "moving_averages": {"BUY": 0, "SELL": 10}
    }
    should_exit_global = should_exit_early_with_tradingview(side, mock_guidance_reversal, config_global_enabled, "ADAUSDT")
    
    print(f"TP Extension: {should_extend_global} (Expected: True)")
    print(f"SL Trailing: {should_trail_global} (Expected: True)")
    print(f"Early Exit: {should_exit_global} (Expected: True - with reversal signal + RSI)")
    
    assert should_extend_global == True, "ADAUSDT TP extension should be True (global enabled)"
    assert should_trail_global == True, "ADAUSDT SL trailing should be True (global enabled)"
    assert should_exit_global == True, "ADAUSDT early exit should be True (global enabled with reversal)"
    print("✓ Global enabled working correctly")
    
    # Test override that disables globally enabled feature
    print("\n--- Override disables globally enabled feature ---")
    config_override_disable = {
        "tradingviewTpExtensionEnabled": True,  # Global: enabled
        "tradingviewTpExtensionOverride": {
            "DOGEUSDT": False  # Override: disable for DOGE
        }
    }
    
    should_extend_doge = should_extend_tp_with_tradingview(side, mock_guidance, config_override_disable, "DOGEUSDT")
    should_extend_other = should_extend_tp_with_tradingview(side, mock_guidance, config_override_disable, "PEPEUSDT")
    
    print(f"DOGEUSDT TP Extension: {should_extend_doge} (Expected: False - override)")
    print(f"PEPEUSDT TP Extension: {should_extend_other} (Expected: True - global)")
    
    assert should_extend_doge == False, "DOGEUSDT TP extension should be False (override)"
    assert should_extend_other == True, "PEPEUSDT TP extension should be True (global)"
    print("✓ Override disable working correctly")
    
    print("\n" + "=" * 60)
    print("ALL PER-SYMBOL CONFIGURATION TESTS PASSED")
    print("=" * 60)
    print("\nSummary:")
    print("- Per-symbol overrides take precedence over global defaults")
    print("- Symbols not in override dict use global defaults")
    print("- Can enable/disable features per symbol")
    print("- Can override globally enabled features")
    print("- Can override globally disabled features")
    print("\nConfiguration priority:")
    print("1. Per-symbol override (if exists)")
    print("2. Global default (fallback)")

def main():
    """Run per-symbol configuration tests."""
    print("\n" + "=" * 60)
    print("PER-SYMBOL TRADINGVIEW CONFIGURATION TEST SUITE")
    print("=" * 60)
    
    test_per_symbol_override()
    
    print("\n" + "=" * 60)
    print("TEST SUITE COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()
