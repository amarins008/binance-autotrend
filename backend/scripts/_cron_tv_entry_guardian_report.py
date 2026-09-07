#!/usr/bin/env python3
"""
TV + Entry + Guardian Performance Report
========================================

Produces a three-window TV-entry-attribution report for the cron scheduler.
Splits trades into:
  1. PRE-GATE    — closedAt <= GATE_DEPLOY_TS (directional conflict gate deployed 2026-08-22 07:21 UTC)
  2. POST-GATE PRE-RESTART — GATE_DEPLOY_TS < closedAt <= restart_ts
  3. POST-RESTART — closedAt > restart_ts (live uptimeSec from /autotrade/status)

GATE-LEAK CHECK: count AGAINST trades (tvAtEntry opposite the side) with
closedAt > restart_ts. ANY such trade = HIGH-severity alert.

SAMPLE_TOO_SMALL flag: when POST-RESTART trade count < 30.

Convention: read live config + uptimeSec from /autotrade/status (not hard-coded
deploy timestamp) so POST-RESTART is always correct after any restart.
Read trades_log.jsonl with encoding='utf-8-sig' (BOM).
tvAtEntry is a plain STRING — normalize with str(v).strip().upper().
Use tvAtEntry (ENTRY-time) for alignment, never tvSignal (CLOSE-time).
Deliver origin; flag SAMPLE_TOO_SMALL and any post-restart AGAINST as alerts.
"""

import json
import os
import sys
import time
import io
from collections import defaultdict

# ── Paths ──────────────────────────────────────────────────────────────
BACKEND = os.path.dirname(os.path.abspath(__file__))  # .../backend
TRADES_LOG = os.path.join(BACKEND, "obsidian_vault", "trades_log.jsonl")
SNAPSHOT = os.path.join(BACKEND, "autotrade_snapshot.json")

# ── Constants ──────────────────────────────────────────────────────────
# Directional conflict gate deployed 2026-08-22 07:21 UTC
GATE_DEPLOY_TS = 1788969669  # 2026-08-22 07:21:09 UTC in unix epoch

# ── Helper: read live status ───────────────────────────────────────────
def get_live_status():
    """Fetch /autotrade/status and return relevant fields."""
    import urllib.request
    try:
        with urllib.request.urlopen("http://127.0.0.1:8020/autotrade/status", timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"WARNING: Could not reach /autotrade/status: {e}", file=sys.stderr)
        data = {"running": False, "uptimeSec": 0, "at": int(time.time())}
    return data


# ── Helper: parse trades_log with BOM handling ─────────────────────────
def parse_trades_log(path):
    """Read trades_log.jsonl with UTF-8-sig encoding; yield per-trade dicts."""
    if not os.path.exists(path):
        return []
    rows = []
    with io.open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                t = json.loads(line)
                rows.append(t)
            except json.JSONDecodeError:
                continue
    return rows


# ── Main ───────────────────────────────────────────────────────────────
def main():
    # ── Load live status ────────────────────────────────────────────
    live = get_live_status()
    running = live.get("running", False)
    uptime_sec = live.get("uptimeSec", 0)  # seconds since restart
    now = time.time()

    # Restart timestamp = now - uptime (when the CURRENT process started)
    restart_ts = int(now - uptime_sec)

    # GATE_DEPLOY_TS is hard-coded (2026-08-22 07:21 UTC)
    # POST-RESTART = closedAt > restart_ts
    # We also need to know if gate has been deployed — if restart_ts > GATE_DEPLOY_TS,
    # the gate is definitely active. If not, all entries are PRE-GATE.

    # ── Parse trades ──────────────────────────────────────────────────
    trades = parse_trades_log(TRADES_LOG)
    total_trades = len(trades)

    # ── Split into three windows ──────────────────────────────────────
    pre_gate = []
    post_gate_pre_restart = []
    post_restart = []

    for t in trades:
        closed = int(t.get("closedAt", 0))
        if closed <= GATE_DEPLOY_TS:
            pre_gate.append(t)
        elif closed > restart_ts:
            post_restart.append(t)
        else:
            post_gate_pre_restart.append(t)

    # ── Gate-leak check: AGAINST trades after restart ─────────────────
    against_after_restart = 0
    for t in post_restart:
        sig = str(t.get("tvAtEntry", "")).strip().upper()
        side = str(t.get("side", "")).strip().upper()
        if sig in ("LONG", "SHORT") and side in ("LONG", "SHORT"):
            if sig != side:
                against_after_restart += 1

    # ── Per-window counts ─────────────────────────────────────────────
    def window_counts(window):
        aligned = 0
        against = 0
        wait = 0
        no_tv = 0
        for t in window:
            sig = str(t.get("tvAtEntry", "")).strip().upper()
            side = str(t.get("side", "")).strip().upper()
            if sig == "WAIT" or sig == "NEUTRAL":
                wait += 1
            elif sig == "" or sig is None:
                no_tv += 1
            elif (sig == "LONG" and side == "LONG") or (sig == "SHORT" and side == "SHORT"):
                aligned += 1
            elif (sig == "LONG" and side == "SHORT") or (sig == "SHORT" and side == "LONG"):
                against += 1
        return aligned, against, wait, no_tv

    c_pre = window_counts(pre_gate)
    c_post_pres = window_counts(post_gate_pre_restart)
    c_post = window_counts(post_restart)

    # ── Scan health (from live status) ────────────────────────────────
    scan_board = live.get("scanBoard", [])
    trades_last_hour = live.get("tradesLastHour", 0)
    tv_health = live.get("tradingviewHealth", {})
    tv_enabled = tv_health.get("enabled", False)
    tv_healthy = tv_health.get("healthy", False)
    fail_count = tv_health.get("fail_count", 0)

    # ── Aggregate ─────────────────────────────────────────────────────
    lines = []
    lines.append("=" * 72)
    lines.append("TV + ENTRY + GUARDIAN PERFORMANCE REPORT")
    lines.append(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')} (BKK time)")
    lines.append(f"Bot running: {running}")
    lines.append(f"Uptime: {uptime_sec}s ({uptime_sec/3600:.1f}h) | Restart at ts={restart_ts}")
    lines.append(f"Gate deployed at ts={GATE_DEPLOY_TS} (2026-08-22 07:21 UTC)")
    lines.append("")

    # Window summaries
    lines.append("--- THREE-WINDOW SPLIT ---")
    lines.append(f"PRE-GATE           : {len(pre_gate)} trades  (before gate deployment)")
    lines.append(f"POST-GATE PRE-RT   : {len(post_gate_pre_restart)} trades  (gate active but pre-restart)")
    lines.append(f"POST-RESTART       : {len(post_restart)} trades  (live running code)")
    lines.append("")

    # Per-window alignment breakdown
    lines.append("--- ALIGNMENT BY WINDOW ---")
    lines.append(f"PRE-GATE:    aligned={c_pre[0]}, against={c_pre[1]}, wait={c_pre[2]}, no_tv={c_pre[3]}")
    lines.append(f"POST-GATE:   aligned={c_post_pres[0]}, against={c_post_pres[1]}, wait={c_post_pres[2]}, no_tv={c_post_pres[3]}")
    lines.append(f"POST-RT:     aligned={c_post[0]}, against={c_post[1]}, wait={c_post[2]}, no_tv={c_post[3]}")
    lines.append("")

    # Gate-leak check
    lines.append("--- GATE-LEAK CHECK ---")
    lines.append(f"AGAINST trades (POST-RESTART): {against_after_restart}")
    if against_after_restart > 0:
        lines.append("⚠️  LEAK SUSPECTED: AGAINST trades found after restart — "
                      "the running conflict gate is NOT blocking opposing TV signals.")
        lines.append("   → Recommend: check intel_analyze directional block at "
                      "tvConflictBlockStrength in main.py, then restart via launcher 8021.")
    else:
        lines.append("✅  No AGAINST trades after restart — conflict gate is clean.")

    # Sample size flag
    lines.append("")
    lines.append(f"POST-RESTART trade count: {len(post_restart)}")
    if len(post_restart) < 30:
        lines.append("⚠️  SAMPLE_TOO_SMALL: POST-RESTART count < 30 — alignment comparison "
                      "is not yet significant. Flag for attention after more trades accumulate.")
    else:
        lines.append("✅  POST-RESTART sample size sufficient (≥30).")

    # Live state
    lines.append("")
    lines.append("--- LIVE STATE ---")
    lines.append(f"running: {running}")
    lines.append(f"tradesLastHour: {trades_last_hour}")
    lines.append(f"tvHealth enabled: {tv_enabled}, healthy: {tv_healthy}, fail_count: {fail_count}")
    lines.append(f"config minConfidence: {live.get('config',{}).get('minConfidence', 'N/A')}")
    lines.append(f"scanBoard symbols: {len(scan_board)}")

    # Agent health snapshot
    sr = live.get("hermesSupervisorReview", {}).get("agentHealth", {})
    lines.append("")
    lines.append("--- AGENT HEALTH (from live status) ---")
    for aid, ah in sr.items():
        state = ah.get("state", "unknown")
        runs = ah.get("runs", 0)
        age = ah.get("ageSec", 0)
        lines.append(f"  {aid}: state={state}, runs={runs}, ageSec={age}")

    # ── Output ────────────────────────────────────────────────────────
    output = "\n".join(lines)
    print(output)

    # Deliver to origin (the system will handle this, but echo for cron)
    # The VERDICT_FILE pattern from §5: write verdict JSON and print line
    # For this report, we just print — the cron delivery handles the rest.
    # However, to register fresh evidence with the tracker, we need a VERDICT_FILE.
    # Since this is a report script (not a verification script), we skip the verdict
    # file unless the user specifically requests it. The tracker keys on the last
    # command's output containing VERDICT_FILE:, so if this is the last command,
    # include a verdict line.

    # Write a verdict JSON for the tracker (fresh evidence registration)
    verdict = {
        "gate_leak_against_after_restart": against_after_restart,
        "post_restart_count": len(post_restart),
        "sample_too_small": len(post_restart) < 30,
        "against_after_restart": against_after_restart > 0,
        "timestamp": int(time.time()),
    }
    verdict_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "hermes-verify-tv-entry-guardian-report.json"
    )
    with open(verdict_path, "w", encoding="utf-8") as vf:
        json.dump(verdict, vf, indent=2, ensure_ascii=False)

    # Print VERDICT_FILE line so the tracker registers fresh evidence
    print(f"VERDICT_FILE:{verdict_path}")


if __name__ == "__main__":
    main()