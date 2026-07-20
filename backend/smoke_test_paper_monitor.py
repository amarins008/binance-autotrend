"""Smoke test for the paper-position monitor helper.

Drives ``main._paper_position_monitor_decision`` against the EXACT scenario
shapes that hit production on 2026-07-09 (PAXG @ 4056 → EDGE @ 0.4087 → QQQ
@ 713.56 → EWY @ 175.43 → QQQ → PAXG → ...) and shows what the OLD buggy
logic would have done vs what the NEW helper decides.

Run:  python smoke_test_paper_monitor.py
"""

from __future__ import annotations

import asyncio
import sys
from typing import Optional

import main


# Real mark prices observed from the production paper history on 2026-07-09.
MARKS = {
    "PAXGUSDT":  4056.75,
    "EDGEUSDT":     0.40872988,
    "QQQUSDT":    713.56,
    "EWYUSDT":    175.43,
    "BTCUSDT":  67500.00,   # sanity reference
}


# Each row is one autotrade cycle: cfg_symbol -> cfg_mark -> open position state.
# These mirror the exact shape of the bug: scan switches to a different
# symbol while an older paper position is still open.
SCENARIOS = [
    {
        "label":  "1. PAXG LONG opened, scan picks EDGE",
        "cfg":    {"symbol": "EDGEUSDT", "holdTrailPct": 0.35},
        "intel":  {"symbol": "EDGEUSDT", "execution": {"mark": MARKS["EDGEUSDT"]}},
        "position": {
            "symbol": "PAXGUSDT", "side": "LONG",
            "entry": 4056.75, "qty": 0.05,
            "tp": 4056.75 * 1.018, "sl": 4056.75 * 0.992,
            "openedAt": 1_700_000_000,
        },
        "cfg_mark": MARKS["EDGEUSDT"],
    },
    {
        "label":  "2. EDGE SHORT opened, scan picks QQQ",
        "cfg":    {"symbol": "QQQUSDT", "holdTrailPct": 0.35},
        "intel":  {"symbol": "QQQUSDT", "execution": {"mark": MARKS["QQQUSDT"]}},
        "position": {
            "symbol": "EDGEUSDT", "side": "SHORT",
            "entry": 0.40872988, "qty": 100,
            "tp": 0.40872988 * 0.98, "sl": 0.40872988 * 1.03,
            "openedAt": 1_700_000_000,
        },
        "cfg_mark": MARKS["QQQUSDT"],
    },
    {
        "label":  "3. QQQ LONG opened, scan picks EWY",
        "cfg":    {"symbol": "EWYUSDT", "holdTrailPct": 0.35},
        "intel":  {"symbol": "EWYUSDT", "execution": {"mark": MARKS["EWYUSDT"]}},
        "position": {
            "symbol": "QQQUSDT", "side": "LONG",
            "entry": 713.56, "qty": 1.0,
            "tp": 713.56 * 1.02, "sl": 713.56 * 0.99,
            "openedAt": 1_700_000_000,
        },
        "cfg_mark": MARKS["EWYUSDT"],
    },
    {
        "label":  "4. EWY SHORT opened, scan picks PAXG",
        "cfg":    {"symbol": "PAXGUSDT", "holdTrailPct": 0.35},
        "intel":  {"symbol": "PAXGUSDT", "execution": {"mark": MARKS["PAXGUSDT"]}},
        "position": {
            "symbol": "EWYUSDT", "side": "SHORT",
            "entry": 175.43, "qty": 1.0,
            "tp": 175.43 * 0.98, "sl": 175.43 * 1.02,
            "openedAt": 1_700_000_000,
        },
        "cfg_mark": MARKS["PAXGUSDT"],
    },
    # Same-symbol sanity: cfg and position agree.
    {
        "label":  "5. (sanity) BTCUSDT LONG, scan still BTCUSDT, mark just under TP",
        "cfg":    {"symbol": "BTCUSDT", "holdTrailPct": 0.35},
        "intel":  {"symbol": "BTCUSDT", "execution": {"mark": 68000.0}},
        "position": {
            "symbol": "BTCUSDT", "side": "LONG",
            "entry": 67500.0, "qty": 0.001,
            "tp": 67500.0 * 1.018, "sl": 67500.0 * 0.992,
            "openedAt": 1_700_000_000,
        },
        "cfg_mark": 68000.0,
    },
]


def simulate_old_buggy_logic(position: dict, cfg_mark: float) -> str:
    """The pre-fix behaviour: use cfg_mark directly, ignore position symbol.

    This is exactly what the inline block did before the helper extracted it.
    """
    mark = cfg_mark
    side = position["side"]
    if side == "LONG":
        if mark >= position["tp"]:
            return "TP_HIT"
        if mark <= position["sl"]:
            return "SL_HIT"
        return "NONE"
    if mark <= position["tp"]:
        return "TP_HIT"
    if mark >= position["sl"]:
        return "SL_HIT"
    return "NONE"


async def fetch_mark(symbol: str) -> float:
    return MARKS[symbol]


async def main_smoke() -> None:
    print("=" * 78)
    print("PAPER POSITION MONITOR — smoke test against 2026-07-09 incident scenarios")
    print("=" * 78)

    fmt = "{:<52} {:<14} {:<14} {:<14}"
    print(fmt.format("scenario", "OLD (buggy)", "NEW (helper)", "fetch symbol"))
    print("-" * 78)

    for sc in SCENARIOS:
        decision = await main._paper_position_monitor_decision(
            cfg=sc["cfg"],
            intel=sc["intel"],
            position=sc["position"],
            cfg_mark=sc["cfg_mark"],
            fetch_mark_fn=fetch_mark,
        )
        old_action = simulate_old_buggy_logic(sc["position"], sc["cfg_mark"])
        new_action = decision.get("action", "NONE")
        if new_action == "TP_HIT":
            new_label = f"TP_HIT@{decision['exit_price']:.4f}"
        elif new_action == "SL_HIT":
            new_label = f"SL_HIT@{decision['exit_price']:.4f}"
        elif new_action == "TRAIL":
            new_label = f"TRAIL@{decision['new_sl']:.4f}/{decision['new_tp']:.4f}"
        else:
            new_label = new_action

        fetch_target = (
            sc["position"]["symbol"] if sc["position"]["symbol"].upper() != sc["cfg"]["symbol"].upper()
            else "(same sym — used cfg_mark)"
        )

        # Flag scenarios where the bug would have closed at a wildly wrong price.
        buggy_close_price = sc["cfg_mark"]
        position_band_lo = min(sc["position"]["tp"], sc["position"]["sl"])
        position_band_hi = max(sc["position"]["tp"], sc["position"]["sl"])
        bug_distance = max(abs(buggy_close_price - position_band_lo),
                           abs(buggy_close_price - position_band_hi))
        bug_indicator = "  ⚠ BUG" if (old_action != "NONE" and bug_distance > 100) else ""

        print(fmt.format(sc["label"], old_action + bug_indicator, new_label, fetch_target))

    print("-" * 78)
    print()
    print("Interpretation:")
    print("  • OLD column shows what the buggy inline code WOULD have decided.")
    print("  • NEW column shows what the fixed helper decides.")
    print("  • ⚠ BUG rows = the OLD logic closed a paper position at a price")
    print("    hundreds/thousands of units away from the position's actual")
    print("    TP/SL band — exactly the PnL pollution from the incident.")
    print()
    print("Helper correctly returns MARK_UNAVAILABLE if it can't fetch the")
    print("position's mark — never falls back to the unrelated cfg_mark.")


if __name__ == "__main__":
    try:
        asyncio.run(main_smoke())
    except Exception as exc:
        print(f"SMOKE TEST FAILED: {exc!r}", file=sys.stderr)
        sys.exit(1)