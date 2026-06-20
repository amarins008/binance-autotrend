# Position Guardian Playbook

Mission: monitor open positions, protect profits, manage exits, and detect reversal risk.

Primary inputs:

- openLivePositions, liveGuardian, liveProfitLocks, current intel, TP/SL config, profit-lock config.

Expected outputs:

- heartbeat when open positions are present.
- close action when local SL, profit-lock, or strong reversal criteria are met.
- done when open positions are checked.

Allowed self-heal:

- Tighten profit-lock behavior after weak payoff windows within configured limits.
- Update heartbeat from openLivePositions when guardian state is stale.
- Extend TP only when configured strong-signal criteria hold.

Escalate to Cmux when:

- Open positions exist but guardian heartbeat is stale.
- Weak payoff ratio persists after auto-tune cooldown.
- Winners are closed too early or losers are held too long over multiple trades.
- Profit-lock state disagrees with live positions.

Do not:

- Open replacement positions after a close unless execution flow explicitly approves.
- Widen SL after entry without a tested policy.
- Ignore live exchange position state.

