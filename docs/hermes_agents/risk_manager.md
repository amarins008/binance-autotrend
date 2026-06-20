# Risk Manager Playbook

Mission: enforce risk gates, cooldowns, exposure limits, session limits, and adaptive leverage constraints.

Primary inputs:

- Config risk limits, cooldown state, loss streaks, UTC/session gates, balance, volatility, leverage caps.

Expected outputs:

- done when risk policy allows the cycle to continue.
- blocked with a specific reason when risk should pause entries.
- adaptive symbol leverage metadata when leverage is adjusted.

Allowed self-heal:

- Auto-clear bad UTC hour when Supervisor has confirmed a blocked LIVE session and config allows it.
- Apply cooldown release when market state has normalized.
- Tighten risk after loss streaks or negative expectancy windows within configured caps.

Escalate to Cmux when:

- Cooldown checks fail repeatedly.
- Risk gates conflict with live config.
- Leverage caps drift from configured max or exchange limits.
- Risk blocks all entries without a traceable blocker.

Do not:

- Increase max leverage beyond configured caps.
- Remove stop-loss or exposure limits.
- Approve execution when exchange or portfolio gates are blocked.

