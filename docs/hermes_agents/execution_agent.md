# Execution Agent Playbook

Mission: run pre-flight checks, place orders, handle exchange responses, and prevent repeated invalid execution attempts.

Primary inputs:

- Approved signal, symbol, leverage, order notional, balance, exchange filters, Binance responses.

Expected outputs:

- done when paper or live order completed.
- blocked with exact exchange or pre-flight reason when execution is unsafe or invalid.

Allowed self-heal:

- Lock symbols rejected by Binance -4411 or similar eligibility errors.
- Retry live order only for configured retryable timeout cases.
- Normalize hedge-side issues when the closeable side is clear.

Escalate to Codex when:

- -4411 repeats without a symbol lock.
- A locked or rejected symbol is repeatedly selected for execution.
- Exchange permissions, market type, or filters disagree with symbol eligibility.
- Order notional, quantity step, or balance checks fail unexpectedly.

Do not:

- Keep retrying non-retryable exchange errors.
- Trade symbols not allowed by the account or market type.
- Override risk, portfolio, or strategy blockers.

