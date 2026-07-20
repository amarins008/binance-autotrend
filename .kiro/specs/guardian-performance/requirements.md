# Requirements Document

## Introduction

The Position Guardian system monitors open Binance Futures positions and enforces TP/SL/profit-lock decisions every loop cycle (~20 s). It currently consists of two parallel subsystems — a single-position guardian (`_live_guardian_maybe_close`, System A) and a multi-position profit-lock manager (`_live_multi_profit_lock_manage`, System B) — coordinated by a shared orchestrator (`_manage_live_open_positions_once`).

Several architectural and correctness issues degrade reliability and performance under multiple simultaneous open positions: sequential (non-parallel) `intel_analyze` calls that chain 6 s timeouts, divergent TP/SL state between System A and System B for the same position, dead code left from the MarketContext MCP removal, a structure-confirmation bypass that allows strong-reversal exits with zero confirmation, a position-cache TTL that expires before System A can consume it, a supervisor staleness threshold that fires false HIGH-severity alarms on normal slow cycles, stale entry-price state after restart, and a profit-lock floor that may prevent timely exits on low-notional trades.

This specification defines requirements for correcting all eight issues while preserving the existing profit-lock semantics and adding no new external dependencies.

---

## Glossary

- **Guardian**: The combined Position Guardian system (`_manage_live_open_positions_once` and its two subsystems).
- **System_A**: `_live_guardian_maybe_close` — manages the bot's own opened position tracked in `AUTO_TRADE["liveGuardian"]`.
- **System_B**: `_live_multi_profit_lock_manage` — manages all open positions fetched from Binance, tracked in `AUTO_TRADE["liveProfitLocks"]`.
- **Orchestrator**: `_manage_live_open_positions_once` — calls System_B then System_A each cycle.
- **Intel_Analyze**: The `intel_analyze()` coroutine that performs market analysis for a given symbol.
- **Position_Cache**: `app_state._LIVE_POSITIONS_CACHE` — the module-level tuple `(cached_at: float, rows: list[dict])` populated by `_pick_live_orphan_positions`.
- **Lock_State**: An entry in `AUTO_TRADE["liveProfitLocks"]` keyed by `SYMBOL:SIDE`.
- **Guardian_State**: The `AUTO_TRADE["liveGuardian"]` dict managed by System_A.
- **Entry_Price**: The price at which a position was opened, used as the reference for TP/SL calculations.
- **Profit_Lock**: The mechanism that raises the effective stop-loss to lock in a minimum profit once peak unrealized PnL exceeds a threshold.
- **Breakeven_Guard**: A sub-mechanism of Profit_Lock that exits a position when profit has retreated to near zero after previously being meaningful.
- **Strong_Reversal_Exit**: The logic that exits a position early when `intel_analyze` returns a high-confidence signal in the opposite direction.
- **Structure_Confirmation**: The check inside `_strong_reversal_structure_confirmed()` that validates a reversal signal against technical indicators (trend, MACD, VWAP, Bollinger Bands, RSI).
- **Supervisor**: The Hermes supervisor agent (`_hermes_supervisor_review`) that runs every 90 s and raises alerts when cycle wall-clock time exceeds a staleness threshold.
- **Cycle**: One iteration of `_autotrade_loop`, nominally ~20 s end-to-end.
- **Notional**: Position size in USDT, calculated as `qty × mark_price`.
- **Fee_Edge_Min**: The minimum net profit in USDT required to cover fees, returned by `_fee_edge_min_net_usdt`.
- **EARS**: Easy Approach to Requirements Syntax — the pattern language used for all acceptance criteria below.

---

## Requirements

### Requirement 1: Parallel Intel Analysis in System B

**User Story:** As a bot operator running multiple simultaneous open positions, I want `intel_analyze` to be fetched for all positions concurrently, so that the guardian cycle wall-clock time does not grow linearly with the number of open positions.

#### Acceptance Criteria

1. WHEN System_B processes N ≥ 2 open positions in a single cycle, THE Guardian SHALL dispatch all N `intel_analyze` calls concurrently using `asyncio.gather`, wrapping each individual call with a per-symbol timeout of 6 seconds via `asyncio.wait_for`.
2. WHEN the concurrent intel batch is dispatched, THE Guardian SHALL also enforce a total batch timeout of `min(30, max(6, N × 2))` seconds applied to the `asyncio.gather` call as a whole, so the batch cannot block the cycle indefinitely.
3. IF an individual `intel_analyze` call raises an exception or its 6-second timeout fires, THEN THE Guardian SHALL record that symbol's intel result as `None` for the current cycle; IF all N calls time out or raise exceptions, THE Guardian SHALL proceed with all intel results as `None` rather than aborting the cycle.
4. WHEN intel results are collected, THE Guardian SHALL process them in the same iteration order as the `rows` list passed to System_B (i.e., sorted by notional descending), applying TP/SL and profit-lock decisions with identical logic to the current sequential implementation.
5. THE Guardian SHALL NOT introduce any library outside the Python standard library to implement parallelism.
6. WHEN N equals 1, THE Guardian SHALL produce identical close/hold decisions as the current sequential implementation for that single position.
7. WHEN N equals 0, THE Guardian SHALL skip the intel dispatch phase entirely and return `False` (no positions closed).

---

### Requirement 2: Unified TP/SL State — Eliminate System A / System B Divergence

**User Story:** As a bot operator, I want a single authoritative TP/SL record per open position, so that System A and System B never issue conflicting close orders for the same position.

#### Acceptance Criteria

1. THE Guardian SHALL designate `AUTO_TRADE["liveProfitLocks"]` as the single authoritative TP/SL store for every open position, including positions opened by the current bot session.
2. WHEN System_A starts a guardian cycle, THE Guardian SHALL read its effective TP and SL from the corresponding `Lock_State` entry in `liveProfitLocks` rather than exclusively from `liveGuardian`.
3. WHEN System_A updates a TP or SL value (profit-lock raise, trail, extend), THE Guardian SHALL write the updated value back to the corresponding `Lock_State` entry in `liveProfitLocks` as well as to `liveGuardian`.
4. WHEN System_B creates a new `Lock_State` entry for a symbol that also has an active `Guardian_State`, THE Guardian SHALL initialise `Lock_State.tp` and `Lock_State.sl` from `Guardian_State` values if those values are already set.
5. IF both System_A and System_B evaluate the same position in the same cycle and both determine a close is required, THEN THE Guardian SHALL issue exactly one close order for that position.
6. THE Guardian SHALL NOT allow `liveGuardian.sl` to be lower than `liveProfitLocks[key].sl` for a LONG position, nor higher for a SHORT position, at the end of any cycle.

---

### Requirement 3: Remove Dead `if False:` Block

**User Story:** As a developer maintaining the guardian codebase, I want the `tv_blocks_tp_extension` dead-code block removed, so that the close-logic execution path is unambiguous and static analysis tools do not flag unreachable code.

#### Acceptance Criteria

1. THE Guardian SHALL NOT contain any `if False:` conditional block in `_live_guardian_maybe_close`.
2. THE Guardian SHALL NOT reference `tv_blocks_tp_extension` or `tv_guard` anywhere in the guardian execution path after the cleanup.
3. WHEN the dead block is removed, THE Guardian SHALL preserve all other logic in `_live_guardian_maybe_close` without behavioural change.
4. THE Guardian SHALL NOT gate TP extension on `tv_blocks_tp_extension`; TP extension SHALL be determined solely by `_strong_follow_tp_extension` and `_should_hold_winner`.

---

### Requirement 4: Fix Structure Confirmation Bypass on Missing Precision Data

**User Story:** As a bot operator, I want strong-reversal exits to require genuine structural confirmation before firing, so that a transient intel response that omits `precision` data cannot trigger an early close based solely on signal and confidence.

#### Acceptance Criteria

1. IF `_strong_reversal_structure_confirmed` is called with a `precision` argument that is `None`, not a `dict`, or an empty `dict`, THEN THE Guardian SHALL return `(False, "structure=unavailable")`.
2. IF `precision` is a non-empty `dict` but none of the five indicator keys (`trendUp`/`trendDown`/`trendUpPartial`/`trendDnPartial`, `macdBullish`/`macdBearish`/`macdCrossUp`/`macdCrossDn`, `vwapDistancePct`, `bbPctB`, `rsi14`/`rsi14_5m`) are present, THEN THE Guardian SHALL return `(False, "structure=no-indicators")`.
3. IF `precision` contains at least one observable indicator key (`observed >= 1`) and `len(confirms) >= min(required, observed)`, THEN THE Guardian SHALL return `(True, f"structure={','.join(confirms[:4])}")`.
4. IF `precision` contains at least one observable indicator key (`observed >= 1`) and `len(confirms) < min(required, observed)`, THEN THE Guardian SHALL return `(False, f"structure={len(confirms)}/{min(required, observed)}")`.
5. IF `strongFlipStructureConfirmEnabled` is `False` in config, THEN THE Guardian SHALL return `(True, "structure=disabled")` before evaluating any precision keys, preserving the existing opt-out path.
6. WHEN `_strong_reversal_structure_confirmed` returns `(False, ...)` and thereby prevents a strong-reversal exit, THE Guardian SHALL pass the reason string to `_autotrade_log` at the call site in `_strong_reversal_exit` so it is visible in `AUTO_TRADE["log"]`.

---

### Requirement 5: Extend Position Cache TTL to Cover Full Cycle Duration

**User Story:** As a bot operator, I want the `Position_Cache` to remain valid for the entire ~20 s cycle, so that System A does not make a redundant Binance API call for position data that was already fetched by System B earlier in the same cycle.

#### Acceptance Criteria

1. THE Guardian SHALL set the `Position_Cache` TTL to 25 seconds (raised from 5 seconds).
2. WHEN System_A calls `_current_position_amount` for a symbol that is also tracked in `Position_Cache`, THE Guardian SHALL serve the quantity from `Position_Cache` if the cached entry is no older than 25 seconds, avoiding a separate Binance API call.
3. WHEN System_B populates `Position_Cache` at the start of a cycle, the same cache entry SHALL be valid for System_A later in the same cycle under normal ~20 s loop timing.
4. WHEN the cache is older than 25 seconds, THE Guardian SHALL re-fetch from Binance and update `Position_Cache` as it does today.
5. IF `_current_position_amount` is called with a symbol not present in `Position_Cache`, THE Guardian SHALL still issue a direct Binance API call for that symbol.
6. THE Guardian SHALL NOT use stale cache entries from a prior cycle when the loop is delayed beyond 25 seconds; a cache miss MUST trigger a fresh fetch.

---

### Requirement 6: Increase Supervisor Staleness Threshold to Reduce False Alarms

**User Story:** As a bot operator, I want the supervisor staleness threshold to reflect realistic multi-position cycle durations, so that normal slow cycles with multiple sequential intel calls do not generate spurious HIGH severity alerts.

#### Acceptance Criteria

1. THE Supervisor SHALL raise a staleness alert only when the cycle wall-clock duration exceeds 120 seconds (raised from 90 seconds).
2. WHEN the cycle duration is between 90 and 120 seconds, THE Supervisor SHALL log a MEDIUM severity advisory rather than a HIGH severity alert.
3. WHEN the cycle duration exceeds 120 seconds, THE Supervisor SHALL raise a HIGH severity alert as before.
4. THE Supervisor SHALL include the number of open positions processed in the current cycle in the staleness alert payload, so operators can distinguish normal slow cycles (N positions) from true hangs.
5. WHEN the parallelism improvement from Requirement 1 is active, THE Supervisor SHALL recalculate the expected budget as `base_budget + N × 2` seconds, where `base_budget` is 30 seconds and N is the number of positions evaluated this cycle.
6. IF the calculated expected budget exceeds 120 seconds (e.g., more than 45 simultaneous positions), THE Supervisor SHALL cap the alert threshold at 120 seconds.

---

### Requirement 7: Resolve Stale Entry Price After Bot Restart in System A

**User Story:** As a bot operator, I want System A to verify and refresh its entry price from Binance after a bot restart, so that TP/SL calculations are not based on a stale snapshot value.

#### Acceptance Criteria

1. WHEN `_live_guardian_maybe_close` is invoked AND `AUTO_TRADE["_snapshot_loaded_at"]` is set AND `liveGuardian` does not contain a `"session_validated": True` flag, THE Guardian SHALL fetch the current live position data from Binance for the guardian symbol and compare the live `entryPrice` to `liveGuardian["entryMark"]`.
2. IF the absolute difference between the live `entryPrice` and `liveGuardian["entryMark"]` exceeds `0.0001 × liveGuardian["entryMark"]`, THEN THE Guardian SHALL set `liveGuardian["entryMark"]` to the live `entryPrice` and recompute `liveGuardian["tp"]` and `liveGuardian["sl"]` using the new `entryMark` and the stored percentage fields `liveGuardian["entryTPPct"]` and `liveGuardian["entrySLPct"]`.
3. WHEN System_A updates `entryMark` under criterion 2, THE Guardian SHALL call `_autotrade_log` with a message that includes both the old and new `entryMark` values.
4. IF the live `entryPrice` is within `0.0001 × liveGuardian["entryMark"]` of the stored value, THEN THE Guardian SHALL set `liveGuardian["session_validated"] = True` without modifying `tp`, `sl`, or `entryMark`.
5. WHEN System_A completes the validation check (criterion 2 or criterion 4), THE Guardian SHALL set `liveGuardian["session_validated"] = True` and call `_persist_autotrade_snapshot()` before the next `_live_guardian_maybe_close` invocation.
6. WHEN the live Binance fetch returns no open position for the guardian symbol during the first-cycle validation, THE Guardian SHALL set `liveGuardian["active"] = False` and `liveGuardian["closedBy"] = "NO_POSITION_AT_RESTART"` and SHALL NOT compute TP/SL.
7. WHILE `liveGuardian["session_validated"]` is `True`, THE Guardian SHALL skip the Binance entry-price fetch and comparison on every subsequent `_live_guardian_maybe_close` call within the same process lifetime.
8. IF the Binance fetch fails during first-cycle validation (network error, timeout, or exception), THEN THE Guardian SHALL log the error, retain the existing snapshot `entryMark`, `tp`, and `sl` values unchanged, and defer validation to the next cycle by leaving `session_validated` unset.

---

### Requirement 8: Tune `min_profit_lock` to Respect Position Notional

**User Story:** As a bot operator, I want the minimum profit threshold for the `WEAK_SIGNAL` close path to scale with position notional value, so that low-notional trades are not held open unnecessarily while high-notional trades have an appropriate minimum.

#### Acceptance Criteria

1. WHEN System_B evaluates whether to close on `WEAK_SIGNAL`, THE Guardian SHALL compute `min_profit_lock` as `max(fee_edge_min × 2.0, cfg.profitLockMinUsdt, notional × profitLockWeakSignalRatePct / 100)`.
2. THE Guardian SHALL use a default value of `0.04` for `profitLockWeakSignalRatePct` unless overridden in config, yielding a rate-based floor of `0.04%` of notional.
3. WHEN the notional-based floor (`notional × 0.0004`) is lower than the hard `0.08 USDT` floor, THE Guardian SHALL use `fee_edge_min × 2.0` as the effective minimum, allowing the threshold to drop below `0.08` for trades where fees justify it.
4. THE Guardian SHALL NOT close on `WEAK_SIGNAL` when `upnl < min_profit_lock`, regardless of whether the peak has crossed `lock_trigger`.
5. WHERE `profitLockMinUsdt` is explicitly set in config to a non-zero value, THE Guardian SHALL treat it as a hard lower bound that cannot be undercut by the rate-based calculation.
6. WHEN `min_profit_lock` is computed differently from the current hard floor for a given position, THE Guardian SHALL include the computed value and its components in the `WEAK_SIGNAL` close log message for traceability.

---

### Requirement 9: Guardian Cycle Observability

**User Story:** As a bot operator, I want the guardian to emit a structured timing summary after each cycle, so that I can detect regressions in cycle duration and audit which close decisions were made.

#### Acceptance Criteria

1. WHEN the Orchestrator completes a cycle, THE Guardian SHALL log a single structured summary line containing: cycle wall-clock duration in milliseconds, number of positions evaluated, number of intel calls that succeeded, number of intel calls that timed out or failed, and any close decisions taken (symbol, side, reason).
2. WHEN no positions are open, THE Guardian SHALL emit a reduced heartbeat log entry with cycle duration and a position count of 0, rather than a full summary.
3. THE Guardian SHALL include the cycle summary log entry within the existing `_autotrade_log` infrastructure so it appears in `AUTO_TRADE["log"]` and is visible in `/autotrade/status`.
4. WHEN a close decision is taken by System_A, the summary SHALL attribute the decision to `system=A`; when taken by System_B, the summary SHALL attribute it to `system=B`.
5. THE Guardian SHALL NOT emit the cycle summary more than once per Orchestrator invocation.

---

### Requirement 10: Regression Safety — Existing Profit-Lock Semantics Preserved

**User Story:** As a bot operator, I want all changes to the guardian to preserve existing profit-lock and breakeven-guard semantics, so that the improvements do not accidentally regress trade exit quality.

#### Acceptance Criteria

1. WHEN System_B arms a profit lock (`peak >= lock_trigger`), THE Guardian SHALL raise the effective SL to at least `entry + lock_usdt / qty` for LONG (or `entry - lock_usdt / qty` for SHORT), consistent with the current implementation.
2. WHEN System_B evaluates `RETRACE_BUDGET`, the retrace threshold calculation (`max(fee_min, lock_usdt × 0.55, peak × 0.55, peak − 0.14)`) SHALL remain unchanged.
3. WHEN System_A evaluates `BREAKEVEN_GUARD`, the condition (`breakevenGuardArmed AND 0 < upnl <= breakevenFloor`) SHALL remain unchanged.
4. WHEN System_B evaluates `TARGET_MAX` (`upnl >= tp_max`), the threshold and close behaviour SHALL remain unchanged.
5. THE Guardian SHALL NOT modify the `strongFlipMinConfidence`, `strongFlipMinScoreGap`, or `strongFlipUltraScoreGap` default thresholds as part of any of the above changes.
6. FOR ALL existing close reason codes (`LOCAL_TP_HIT`, `LOCAL_SL_HIT`, `BREAKEVEN_GUARD`, `STRONG_REVERSAL_EXIT`, `PAYOFF_LOSS_GUARD`, `RETRACE_BUDGET`, `TARGET_MAX`, `WEAK_SIGNAL`), THE Guardian SHALL continue to emit the same reason string in log messages and in the closed state record after this change.
