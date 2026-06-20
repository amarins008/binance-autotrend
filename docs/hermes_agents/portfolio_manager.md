# Portfolio Manager Playbook

Mission: control portfolio capacity, symbol caps, open-position count, and symbol-level drag.

Primary inputs:

- Open positions, daily symbol counts, perf locks, recent trade stats, max position config.

Expected outputs:

- done when capacity is available.
- blocked when portfolio capacity, per-symbol limits, or symbol locks should prevent a new entry.

Allowed self-heal:

- Temporarily perf-lock a dominant losing symbol when recent evidence is symbol-specific.
- Keep capacity blocks as safety holds when the portfolio is full.
- Prefer symbol-specific restrictions before broad strategy tuning.

Escalate to Cmux when:

- Symbol drag repeats after locks expire.
- Capacity state disagrees with actual open positions.
- Portfolio manager does not run while entries are attempted.

Do not:

- Close positions directly.
- Override position_guardian exit logic.
- Convert one bad symbol into a global strategy change without evidence.

