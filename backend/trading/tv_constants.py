"""TradingView integration constants.

Named constants replace magic numbers scattered across confluence.py,
tradingview_mcp.py, and main.py so every TV gate uses the same sentinel
and threshold vocabulary.
"""

# confirm_signal() returns this when TV strongly disagrees with the
# internal signal (strength >= tvConflictBlockStrength).
# confluence.py and main.py intel_analyze both check for this value.
TV_HARD_CONFLICT_SENTINEL = -999.0

# Threshold for checking the sentinel — any value <= this is treated as
# a hard-block request by downstream consumers.
TV_SENTINEL_CHECK_THRESHOLD = -900.0

# Maximum allowed TV confidence boost on alignment (confirm_signal returns
# boost * tv_confidence, where boost = 0.08 by default).
# This cap prevents TV from inflating confidence beyond a defined bound.
TV_MAX_BOOST_CAP = 0.10

# TV signal age thresholds (seconds) — centralize so intel_analyze,
# pipeline.py, and confluence.py stay in sync.
TV_STALE_ENTRY_SEC_DEFAULT = 300
TV_ENTRY_MAX_AGE_SEC_DEFAULT = 30

# TV conflict block strength thresholds — used in confirm_signal(),
# intel_analyze(), and pipeline.py to decide when to block an entry.
TV_CONFLICT_BLOCK_STRENGTH_DEFAULT = 0.60
TV_WAIT_MIN_CONF_DEFAULT = 0.82
TV_UNAVAILABLE_MIN_CONF_DEFAULT = 0.80
