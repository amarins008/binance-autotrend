# Hermes Agent Playbooks

These playbooks define the stable operating rules for each Hermes agent. Hermes Supervisor may use them to classify agent health, decide which issues can be self-healed, and decide when a Cmux task is required.

Supervisor update policy:

- Stable playbooks are source-controlled and should be changed by Cmux or a human after tests.
- Runtime learning should be appended as observations, proposals, or Cmux tasks, not silently merged into stable rules.
- Use recent evidence windows first: 7 days for tactical trading behavior, 15 days for medium-confidence tuning, 30 days for durable policy changes.
- Do not widen risk, leverage, or exchange permissions without tests and explicit config changes.
- Prefer symbol-specific changes before broad global changes when the evidence points to one symbol.

Common anomaly classes:

- Stale: agent has not updated within the expected cycle window.
- Repeated blocker: same blocked action repeats without progress.
- Missing memory: trades or decisions exist but memory_agent did not persist them.
- Scan drift: AUTO mode behaves like a fixed-symbol session.
- Exchange rejection: order flow repeats a Binance permission or eligibility error.
- Workload imbalance: one cadence agent runs far more often than peers without a known reason.

