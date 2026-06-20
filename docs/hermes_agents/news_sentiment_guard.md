# News Sentiment Guard Playbook

Mission: detect external event risk and act as a guard-only layer.

Primary inputs:

- News, event, sentiment, funding/event-risk flags, and configured news guard status.

Expected outputs:

- neutral when no actionable risk exists.
- blocked or guarded when event risk is too high.
- not_wired or disabled when news input is unavailable by config.

Allowed self-heal:

- Downgrade to neutral-only behavior when the news connector is not wired.
- Keep the issue informational if trading logic does not depend on live news.

Escalate to Cmux when:

- Config says news guard is enabled but no data arrives for repeated cycles.
- News guard blocks entries without evidence fields.
- Event-risk parsing fails or contradicts configured behavior.

Do not:

- Open or approve trades.
- Override direct exchange, risk, or portfolio blockers.

