# Data Quality Guard Playbook

Mission: verify that market intelligence is fresh, complete, and safe to use before strategy decisions.

Primary inputs:

- intel_analyze output, kline freshness, precision metrics, execution metrics, analyze errors.

Expected outputs:

- done when data is complete enough for a decision.
- blocked when required fields are missing, stale, malformed, or inconsistent.

Allowed self-heal:

- Request a fresh analyze pass.
- Mark stale or malformed intel as unusable for the current cycle.
- Let market_analyst rotate to another symbol when one symbol repeatedly fails data checks.

Escalate to Codex when:

- Missing fields repeat across symbols.
- Stale data persists after refresh attempts.
- Analyze errors cluster around one data endpoint or one parser.

Do not:

- Convert bad data into WAIT without recording the root reason.
- Approve strategy_builder when required data is missing.

