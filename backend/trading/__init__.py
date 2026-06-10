"""
Professional trading engine — layered design.

Layers:
  confluence  → signal + confidence from multi-TF indicators
  regime      → market state (TREND / RANGE / VOLATILE)
  risk        → R:R, ATR stops, fee edge, position sizing math
  position    → hold winner, cut loser, trail levels
  pipeline    → ordered entry gates with audit trail
  config      → defaults + PRO preset merge
"""

from trading.config import apply_autotrade_defaults, merge_preset
from trading.confluence import evaluate_confluence
from trading.pipeline import EntryInputs, EntryPlan, evaluate_entry_plan
from trading.position import (
    intel_momentum_pct,
    max_winner_tp_expands,
    should_cut_loser_early,
    should_hold_winner,
    trail_winner_levels,
)
from trading.presets import PRO_STANDALONE_PRESET, PLAYBOOK
from trading.regime import detect_market_regime
from trading.risk import (
    blend_tpsl_with_atr,
    effective_min_net_profit_usdt,
    effective_tpsl_pct_for_trade,
    estimate_trade_edge_usdt,
    passes_min_risk_reward,
)

__all__ = [
    "PRO_STANDALONE_PRESET",
    "PLAYBOOK",
    "EntryInputs",
    "EntryPlan",
    "apply_autotrade_defaults",
    "merge_preset",
    "evaluate_confluence",
    "evaluate_entry_plan",
    "detect_market_regime",
    "intel_momentum_pct",
    "should_hold_winner",
    "should_cut_loser_early",
    "trail_winner_levels",
    "max_winner_tp_expands",
    "blend_tpsl_with_atr",
    "effective_tpsl_pct_for_trade",
    "effective_min_net_profit_usdt",
    "estimate_trade_edge_usdt",
    "passes_min_risk_reward",
]
