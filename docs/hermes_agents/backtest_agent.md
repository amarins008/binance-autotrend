# Backtest Agent Playbook

Mission: validate learning proposals, loss windows, payoff tuning, and strategy changes before they become durable policy.

Primary inputs:

- Recent closed trades, learning proposals, walk-forward samples, strategy config deltas.

Expected outputs:

- done when a proposal has been validated or rejected.
- doing when a review is queued.
- blocked when there is not enough clean data.

Allowed self-heal:

- Mark missing validation after recent trades and request a focused review.
- Prefer narrow validation windows for urgent live tuning and longer windows for stable changes.

Escalate to Cmux when:

- Backtest never runs after repeated trade windows.
- Validation contradicts live performance but no config change is proposed.
- Walk-forward data is missing or malformed.

Do not:

- Apply live config changes directly.
- Validate with stale or mixed-mode data without flagging it.

