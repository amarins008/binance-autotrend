"""Analyze TradingView MCP effectiveness from trade logs."""

import sys
import os
import json
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

def load_trade_logs(log_path: str) -> List[Dict]:
    """Load trade logs from JSONL file."""
    trades = []
    try:
        with open(log_path, 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        trade = json.loads(line)
                        trades.append(trade)
                    except json.JSONDecodeError:
                        continue
    except FileNotFoundError:
        print(f"Trade log file not found: {log_path}")
    except Exception as e:
        print(f"Error loading trade logs: {e}")
    return trades

def analyze_tradingview_effectiveness(trades: List[Dict]) -> Dict[str, Any]:
    """Analyze TradingView MCP effectiveness from trade data."""
    
    metrics = {
        "total_trades": len(trades),
        "trades_with_tv_guidance": 0,
        "tp_extended_trades": 0,
        "tp_extended_winners": 0,
        "tp_extended_losers": 0,
        "sl_trailed_trades": 0,
        "sl_trailed_winners": 0,
        "sl_trailed_losers": 0,
        "early_exit_trades": 0,
        "early_exit_winners": 0,
        "early_exit_losers": 0,
        "tv_guidance_trades_pnl": 0.0,
        "tv_guidance_trades_win_rate": 0.0,
        "non_tv_trades_pnl": 0.0,
        "non_tv_trades_win_rate": 0.0,
        "by_symbol": defaultdict(lambda: {
            "total": 0,
            "tv_guidance": 0,
            "tp_extended": 0,
            "sl_trailed": 0,
            "early_exit": 0,
            "pnl": 0.0,
            "wins": 0,
            "losses": 0
        })
    }
    
    for trade in trades:
        symbol = trade.get("symbol", "UNKNOWN")
        pnl = float(trade.get("pnl", 0.0) or 0.0)
        is_win = pnl > 0
        
        # Check if trade used TradingView guidance
        used_tv = trade.get("tradingview_guidance", False) or trade.get("tv_guidance", False)
        tp_extended = trade.get("tp_extended", False) or trade.get("tradingview_tp_extended", False)
        sl_trailed = trade.get("sl_trailed", False) or trade.get("tradingview_sl_trailed", False)
        early_exit = trade.get("early_exit", False) or trade.get("tradingview_early_exit", False)
        
        # Update symbol metrics
        metrics["by_symbol"][symbol]["total"] += 1
        metrics["by_symbol"][symbol]["pnl"] += pnl
        if is_win:
            metrics["by_symbol"][symbol]["wins"] += 1
        else:
            metrics["by_symbol"][symbol]["losses"] += 1
        
        if used_tv:
            metrics["trades_with_tv_guidance"] += 1
            metrics["tv_guidance_trades_pnl"] += pnl
            metrics["by_symbol"][symbol]["tv_guidance"] += 1
            
            if is_win:
                metrics["tv_guidance_trades_win_rate"] += 1
        else:
            metrics["non_tv_trades_pnl"] += pnl
            
            if is_win:
                metrics["non_tv_trades_win_rate"] += 1
        
        if tp_extended:
            metrics["tp_extended_trades"] += 1
            metrics["by_symbol"][symbol]["tp_extended"] += 1
            if is_win:
                metrics["tp_extended_winners"] += 1
            else:
                metrics["tp_extended_losers"] += 1
        
        if sl_trailed:
            metrics["sl_trailed_trades"] += 1
            metrics["by_symbol"][symbol]["sl_trailed"] += 1
            if is_win:
                metrics["sl_trailed_winners"] += 1
            else:
                metrics["sl_trailed_losers"] += 1
        
        if early_exit:
            metrics["early_exit_trades"] += 1
            metrics["by_symbol"][symbol]["early_exit"] += 1
            if is_win:
                metrics["early_exit_winners"] += 1
            else:
                metrics["early_exit_losers"] += 1
    
    # Calculate win rates
    if metrics["trades_with_tv_guidance"] > 0:
        metrics["tv_guidance_trades_win_rate"] /= metrics["trades_with_tv_guidance"]
    
    if metrics["total_trades"] - metrics["trades_with_tv_guidance"] > 0:
        metrics["non_tv_trades_win_rate"] /= (metrics["total_trades"] - metrics["trades_with_tv_guidance"])
    
    return metrics

def print_report(metrics: Dict[str, Any]):
    """Print TradingView effectiveness report."""
    print("\n" + "=" * 70)
    print("TRADINGVIEW MCP EFFECTIVENESS REPORT")
    print("=" * 70)
    
    print(f"\nTotal Trades Analyzed: {metrics['total_trades']}")
    print(f"Trades with TradingView Guidance: {metrics['trades_with_tv_guidance']}")
    print(f"Coverage: {(metrics['trades_with_tv_guidance'] / max(metrics['total_trades'], 1) * 100):.1f}%")
    
    print("\n" + "-" * 70)
    print("TP EXTENSION PERFORMANCE")
    print("-" * 70)
    print(f"Trades with TP Extended: {metrics['tp_extended_trades']}")
    print(f"  - Winners: {metrics['tp_extended_winners']}")
    print(f"  - Losers: {metrics['tp_extended_losers']}")
    if metrics['tp_extended_trades'] > 0:
        win_rate = (metrics['tp_extended_winners'] / metrics['tp_extended_trades']) * 100
        print(f"  - Win Rate: {win_rate:.1f}%")
    
    print("\n" + "-" * 70)
    print("SL TRAILING PERFORMANCE")
    print("-" * 70)
    print(f"Trades with SL Trailed: {metrics['sl_trailed_trades']}")
    print(f"  - Winners: {metrics['sl_trailed_winners']}")
    print(f"  - Losers: {metrics['sl_trailed_losers']}")
    if metrics['sl_trailed_trades'] > 0:
        win_rate = (metrics['sl_trailed_winners'] / metrics['sl_trailed_trades']) * 100
        print(f"  - Win Rate: {win_rate:.1f}%")
    
    print("\n" + "-" * 70)
    print("EARLY EXIT PERFORMANCE")
    print("-" * 70)
    print(f"Trades with Early Exit: {metrics['early_exit_trades']}")
    print(f"  - Winners: {metrics['early_exit_winners']}")
    print(f"  - Losers: {metrics['early_exit_losers']}")
    if metrics['early_exit_trades'] > 0:
        win_rate = (metrics['early_exit_winners'] / metrics['early_exit_trades']) * 100
        print(f"  - Win Rate: {win_rate:.1f}%")
    
    print("\n" + "-" * 70)
    print("TRADINGVIEW GUIDANCE VS NO GUIDANCE")
    print("-" * 70)
    print(f"With TV Guidance:")
    print(f"  - Total PnL: {metrics['tv_guidance_trades_pnl']:.2f} USDT")
    print(f"  - Win Rate: {metrics['tv_guidance_trades_win_rate'] * 100:.1f}%")
    
    print(f"\nWithout TV Guidance:")
    print(f"  - Total PnL: {metrics['non_tv_trades_pnl']:.2f} USDT")
    print(f"  - Win Rate: {metrics['non_tv_trades_win_rate'] * 100:.1f}%")
    
    print("\n" + "-" * 70)
    print("BY SYMBOL")
    print("-" * 70)
    for symbol, data in sorted(metrics["by_symbol"].items(), key=lambda x: x[1]["pnl"], reverse=True):
        if data["total"] > 0:
            win_rate = (data["wins"] / data["total"]) * 100
            print(f"\n{symbol}:")
            print(f"  Total: {data['total']} | PnL: {data['pnl']:.2f} | Win Rate: {win_rate:.1f}%")
            print(f"  TV Guidance: {data['tv_guidance']} | TP Extended: {data['tp_extended']} | SL Trailed: {data['sl_trailed']} | Early Exit: {data['early_exit']}")
    
    print("\n" + "=" * 70)
    print("RECOMMENDATIONS")
    print("=" * 70)
    
    if metrics['tp_extended_trades'] > 0:
        tp_win_rate = (metrics['tp_extended_winners'] / metrics['tp_extended_trades']) * 100
        if tp_win_rate > 60:
            print("✓ TP Extension is performing well (win rate > 60%)")
        elif tp_win_rate > 50:
            print("⚠ TP Extension performance is moderate (win rate > 50%)")
        else:
            print("✗ TP Extension may need adjustment (win rate < 50%)")
    
    if metrics['sl_trailed_trades'] > 0:
        sl_win_rate = (metrics['sl_trailed_winners'] / metrics['sl_trailed_trades']) * 100
        if sl_win_rate > 60:
            print("✓ SL Trailing is performing well (win rate > 60%)")
        elif sl_win_rate > 50:
            print("⚠ SL Trailing performance is moderate (win rate > 50%)")
        else:
            print("✗ SL Trailing may need adjustment (win rate < 50%)")
    
    if metrics['early_exit_trades'] > 0:
        early_win_rate = (metrics['early_exit_winners'] / metrics['early_exit_trades']) * 100
        if early_win_rate > 60:
            print("✓ Early Exit is performing well (win rate > 60%)")
        elif early_win_rate > 50:
            print("⚠ Early Exit performance is moderate (win rate > 50%)")
        else:
            print("✗ Early Exit may need adjustment (win rate < 50%)")
    
    tv_win_rate = metrics['tv_guidance_trades_win_rate'] * 100
    non_tv_win_rate = metrics['non_tv_trades_win_rate'] * 100
    if tv_win_rate > non_tv_win_rate + 5:
        print(f"✓ TV Guidance improves win rate by {tv_win_rate - non_tv_win_rate:.1f}%")
    elif tv_win_rate > non_tv_win_rate:
        print(f"⚠ TV Guidance slightly improves win rate by {tv_win_rate - non_tv_win_rate:.1f}%")
    else:
        print(f"✗ TV Guidance does not improve win rate (difference: {tv_win_rate - non_tv_win_rate:.1f}%)")
    
    print("=" * 70)

def main():
    """Main function."""
    print("TradingView MCP Effectiveness Analyzer")
    print("=" * 70)
    
    # Try to find trade log file
    possible_paths = [
        "backend/obsidian_vault/trades_log.jsonl",
        "obsidian_vault/trades_log.jsonl",
        "trades_log.jsonl"
    ]
    
    trade_log_path = None
    for path in possible_paths:
        if os.path.exists(path):
            trade_log_path = path
            break
    
    if not trade_log_path:
        print("Error: Trade log file not found.")
        print("Searched paths:")
        for path in possible_paths:
            print(f"  - {path}")
        print("\nNote: Trade logs may be in .gitignore and not accessible.")
        print("Please provide the correct path to trades_log.jsonl")
        return
    
    print(f"Loading trade logs from: {trade_log_path}")
    trades = load_trade_logs(trade_log_path)
    
    if not trades:
        print("No trades found in log file.")
        return
    
    print(f"Loaded {len(trades)} trades")
    
    # Check if trades have TradingView data
    sample_trade = trades[0]
    has_tv_data = any(key for key in sample_trade.keys() if 'tv' in key.lower() or 'tradingview' in key.lower())
    
    if not has_tv_data:
        print("\n⚠ Warning: Trade logs do not contain TradingView guidance data.")
        print("This could mean:")
        print("  - TradingView MCP was not enabled during these trades")
        print("  - Trade logging does not include TradingView metadata")
        print("  - Trades are from before TradingView MCP was implemented")
        print("\nAnalysis will show 0 for all TradingView metrics.")
    
    metrics = analyze_tradingview_effectiveness(trades)
    print_report(metrics)

if __name__ == "__main__":
    main()
