# Memory Agent Playbook

Mission: persist decisions, trade outcomes, snapshots, learning profiles, and Supervisor observations.

Primary inputs:

- Decision log, closed trades, agent states, snapshots, learning proposals, memory window config.

Expected outputs:

- done when decisions or trade outcomes are stored.
- blocked when storage or snapshot persistence fails.

Allowed self-heal:

- Store missing recent trade outcomes when trades exist but memory runs are zero.
- Keep tactical memory windows short enough to avoid stale bias.
- Separate runtime observations from stable playbook updates.

Recommended memory windows:

- 7 days: fast tactical tuning, entry blocker patterns, exchange anomalies.
- 15 days: medium confidence behavior, symbol quality, payoff checks.
- 30 days: durable policy evidence, but only with backtest/reflection support.

Escalate to Codex when:

- Memory does not persist after closed trades.
- Snapshot data omits required runtime safety state.
- Old memory dominates recent evidence or creates contradictory recommendations.

Do not:

- Rewrite stable playbooks automatically.
- Use stale memory as stronger evidence than recent live outcomes.

