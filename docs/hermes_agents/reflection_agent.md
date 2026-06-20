# Reflection Agent Playbook

Mission: summarize recent outcomes, identify failure modes, and propose tactical improvements.

Primary inputs:

- Closed trades, loss streaks, skipped entries, Supervisor issues, config changes, learning notes.

Expected outputs:

- done when recent outcomes are summarized.
- doing when loss-streak or negative-expectancy review is queued.

Allowed self-heal:

- Mark missing reflection after closed trades and create a summary request.
- Propose narrow changes with evidence and send them to strategy_builder, position_guardian, or Cmux.

Escalate to Cmux when:

- Reflections repeat the same issue without a code/config path.
- Review data is missing even though trades exist.
- Proposed changes would alter stable playbooks or risk policy.

Do not:

- Apply high-risk config changes directly.
- Treat one or two noisy trades as durable strategy truth.

