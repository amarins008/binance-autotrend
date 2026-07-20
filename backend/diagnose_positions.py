"""Diagnostic script to check why only one position is opened at a time."""

import json
import os
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services import app_state

def diagnose():
    AUTO_TRADE = app_state.AUTO_TRADE
    cfg = AUTO_TRADE.get("config") or {}
    
    print("=== Hermes Position Diagnostic ===\n")
    
    # 1. Check maxOpenPositions
    max_open = int(cfg.get("maxOpenPositions", 4) or 4)
    print(f"1. maxOpenPositions: {max_open}")
    if max_open <= 1:
        print("   ⚠️  maxOpenPositions is 1 or less! This limits to ONE position.")
        print("   Fix: Send POST /autotrade/config with {\"maxOpenPositions\": 4}")
    else:
        print(f"   ✓ maxOpenPositions allows up to {max_open} positions")
    
    # 2. Check if market scan is enabled
    scan_mode = bool(cfg.get("marketScan")) or str(cfg.get("symbol", "")).upper() in ("AUTO", "SCAN")
    print(f"\n2. Market Scan Mode: {scan_mode}")
    if not scan_mode:
        print(f"   ⚠️  Fixed symbol mode: symbol = {cfg.get('symbol')}")
        print("   In fixed mode, only ONE symbol is traded. Switch to scan mode for multiple positions.")
    else:
        print("   ✓ Scan mode is enabled")
    
    # 3. Check risk cooldowns
    risk_cooldowns = AUTO_TRADE.get("riskCooldownBySymbol", {})
    print(f"\n3. Risk Cooldowns: {len(risk_cooldowns)} symbols")
    if risk_cooldowns:
        for sym, rec in risk_cooldowns.items():
            print(f"   - {sym}: reason={rec.get('reason')}, until={rec.get('until')}")
    else:
        print("   ✓ No active risk cooldowns")
    
    # 4. Check perfLocks
    perf_locks = AUTO_TRADE.get("perfLocks", {})
    print(f"\n4. Performance Locks: {len(perf_locks)} symbols")
    if perf_locks:
        for sym, rec in perf_locks.items():
            print(f"   - {sym}: reason={rec.get('reason')}, until={rec.get('until')}")
    
    # 5. Check pauseUntil
    pause_until = int(AUTO_TRADE.get("pauseUntil", 0) or 0)
    import time
    now = int(time.time())
    if pause_until > now:
        print(f"\n5. Global Pause: active until {pause_until} ({pause_until - now}s remaining)")
    else:
        print("\n5. Global Pause: not active")
    
    # 6. Check last skip
    last_skip = AUTO_TRADE.get("lastSkip")
    if isinstance(last_skip, dict):
        print(f"\n6. Last Skip: {last_skip.get('code')} - {last_skip.get('msg')}")
    else:
        print("\n6. Last Skip: none")
    
    # 7. Check running state
    print(f"\n7. Running: {AUTO_TRADE.get('running')}")
    print(f"   manageOpenOnly: {AUTO_TRADE.get('manageOpenOnly')}")
    
    # 8. Check log for recent skips
    print("\n8. Recent log entries (last 10):")
    log = AUTO_TRADE.get("log", [])[:10]
    for entry in log:
        if isinstance(entry, dict):
            print(f"   [{entry.get('ts')}] {entry.get('msg')}")
    
    print("\n=== End Diagnostic ===")
    
    # Recommendations
    print("\n=== Recommendations ===")
    if max_open <= 1:
        print("• Fix maxOpenPositions to 4 via POST /autotrade/config")
    if not scan_mode:
        print("• Switch to scan mode: set symbol='AUTO' and marketScan=true")
    if risk_cooldowns:
        print("• Risk cooldowns are active. Wait for them to expire or disable riskCooldownEnabled")
    if pause_until > now:
        print("• Global pause is active. Wait for it to expire.")
    
    return {
        "maxOpenPositions": max_open,
        "scanMode": scan_mode,
        "riskCooldowns": len(risk_cooldowns),
        "pauseActive": pause_until > now,
    }

if __name__ == "__main__":
    result = diagnose()
    print(f"\nSummary: {json.dumps(result, indent=2)}")
