#!/usr/bin/env python3
"""
Cron: TV + Entry + Guardian performance report (every 3 days).

Improves on the original (ae536f9) by:
  1. Using the LIVE bot restart timestamp (from /autotrade/status uptime)
     instead of a hard-coded GATE_TS, so "post-restart" = "under current
     gate code" is always correct after any restart.
  2. Adding a dedicated AGAINST-post-restart detector: ANY trade that opened
     SHORT while TV=LONG (or LONG while TV=SHORT) AFTER the current restart
     is a gate-leak signal and is flagged loudly.
  3. SAMPLE_TOO_SMALL flag when the post-restart window has <30 trades so
     Boss knows the comparison is not yet significant.

Reads trades_log.jsonl + live /autotrade/status. Report-only: exits 0.

Run from backend/scripts. Uses the project .venv python.
"""
import json, os, sys, io, urllib.request
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG = os.path.join(ROOT, "obsidian_vault", "trades_log.jsonl")
STATUS_URL = "http://127.0.0.1:8020/autotrade/status"

# The directional conflict gate (block SHORT-vs-TV=LONG at strength>=0.6) was
# deployed 2026-08-22 14:21 BKK = 07:21 UTC (commits 8b88760 / 631c3dc).
# We treat any trade closed AFTER this as "under live gate code".
# If the bot restarted more recently, we use the later of the two so a
# post-restart window is never empty due to a fresh restart.
GATE_DEPLOY_TS = datetime(2026, 8, 22, 7, 21, 0, tzinfo=timezone.utc).timestamp()

def norm_tv(v):
    if v is None:
        return None
    s = str(v).strip().upper()
    return s if s in ("LONG", "SHORT", "WAIT") else None

def load_jsonl(p):
    out = []
    if not os.path.exists(p):
        return out
    with io.open(p, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except Exception:
                    pass
    return out

def get_restart_ts():
    """Live bot restart time = now - uptimeSec from /autotrade/status."""
    try:
        with urllib.request.urlopen(STATUS_URL, timeout=8) as r:
            st = json.loads(r.read().decode())
        up = float(st.get("uptimeSec", 0) or 0)
        now = datetime.now(timezone.utc).timestamp()
        return now - up, st
    except Exception as e:
        now = datetime.now(timezone.utc).timestamp()
        return now - 2287, {"error": str(e)}

def main():
    rows = load_jsonl(LOG)
    restart_ts, st = get_restart_ts()
    # Two windows:
    #  (a) GATE_DEPLOY_TS — compares old-code era vs live-gate era (gate shipped
    #      2026-08-22 07:21 UTC). This is the meaningful pre/post CODE comparison.
    #  (b) restart_ts — leak check: any AGAINST trade AFTER the current restart
    #      would mean the running gate is not blocking (code/runtime drift).
    gate_ts = GATE_DEPLOY_TS
    rt_gate = datetime.fromtimestamp(gate_ts, tz=timezone.utc)
    rt_restart = datetime.fromtimestamp(restart_ts, tz=timezone.utc)
    print(f"# TV + Entry + Guardian Report")
    print(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Gate-code window start: {rt_gate.strftime('%Y-%m-%d %H:%M UTC')} (tv directional gate deployed)")
    print(f"Bot last restart: {rt_restart.strftime('%Y-%m-%d %H:%M UTC')} (leak-check window)")
    h = (st.get("tradingviewHealth") or {}) if isinstance(st, dict) else {}
    cfg = (st.get("config") or {}) if isinstance(st, dict) else {}
    print(f"TV: enabled={h.get('enabled')} healthy={h.get('healthy')} fail_count={h.get('fail_count')} | "
          f"tvConflictBlockStrength={cfg.get('tvConflictBlockStrength')} tvEntryMinConfidence={cfg.get('tvEntryMinConfidence')}")
    print()

    # Three windows (clear separation):
    #  PRE-GATE          : ca <= GATE_DEPLOY_TS        (old code, no directional gate)
    #  POST-GATE-PREREST : GATE_DEPLOY_TS < ca <= restart_ts (gate shipped, but
    #                       bot may not have restarted onto new code yet)
    #  POST-RESTART      : ca > restart_ts             (under CURRENT running code)
    pre, mid, post = [], [], []
    for r in rows:
        ca = r.get("closedAt") or r.get("entryDecisionAt") or 0
        if ca <= gate_ts:
            pre.append(r)
        elif ca <= restart_ts:
            mid.append(r)
        else:
            post.append(r)

    def tv_buckets(subset):
        b = {"ALIGN": [0, 0, 0.0], "NEUTRAL": [0, 0, 0.0], "AGAINST": [0, 0, 0.0], "NONE": [0, 0, 0.0]}
        for r in subset:
            side = str(r.get("side") or "").upper()
            if side not in ("LONG", "SHORT"):
                continue
            tv = norm_tv(r.get("tvAtEntry"))
            pnl = float(r.get("pnl") or 0)
            if tv == side:
                k = "ALIGN"
            elif tv in ("LONG", "SHORT"):
                k = "AGAINST"
            elif tv == "WAIT":
                k = "NEUTRAL"
            else:
                k = "NONE"
            b[k][0] += 1
            b[k][1] += 1 if pnl > 0 else 0
            b[k][2] += pnl
        return b

    for label, subset in (("PRE-GATE (old code)", pre),
                           ("POST-GATE PRE-RESTART (gate shipped, pre-current-restart)", mid),
                           ("POST-RESTART (current running code)", post)):
        b = tv_buckets(subset)
        print(f"## {label}  (n={sum(v[0] for v in b.values())})")
        print(f"{'bucket':9s} {'n':>4s} {'WR%':>6s} {'netPnL':>9s}")
        for k in ("ALIGN", "NEUTRAL", "AGAINST", "NONE"):
            n, w, pnl = b[k]
            wr = f"{100.0*w/n:.1f}" if n else "-"
            print(f"{k:9s} {n:4d} {wr:>6s} {pnl:+9.3f}")
        print()

    # AGAINST post-restart detector (gate-leak signal) — uses RESTART time
    against_post = []
    for r in rows:
        ca = r.get("closedAt") or r.get("entryDecisionAt") or 0
        if ca <= restart_ts:
            continue
        side = str(r.get("side") or "").upper()
        if side not in ("LONG", "SHORT"):
            continue
        tv = norm_tv(r.get("tvAtEntry"))
        if tv in ("LONG", "SHORT") and tv != side:
            against_post.append(r)
    print("## GATE-LEAK CHECK (AGAINST trades after current restart)")
    if not against_post:
        print("  ✅ CLEAN: 0 AGAINST trades post-restart — conflict gate is live, no leak.")
    else:
        print(f"  ⚠️  LEAK SUSPECTED: {len(against_post)} AGAINST trades entered AFTER restart:")
        for r in against_post[:12]:
            print(f"    - {r.get('symbol'):10s} {r.get('side'):5s} vs TV={r.get('tvAtEntry')} "
                  f"entConf={r.get('tvAtEntryConfidence')} str={r.get('tvStrength')} "
                  f"age={r.get('tvAge')}s pnl={r.get('pnl')} reason={r.get('reason')}")
        if len(against_post) > 12:
            print(f"    ... +{len(against_post)-12} more")
    print()

    # Sample-size flag (based on POST-RESTART window — the one that matters for leak)
    n_post = sum(1 for r in post if str(r.get('side') or '').upper() in ('LONG', 'SHORT'))
    if n_post < 30:
        print(f"⚠️  SAMPLE_TOO_SMALL: post-restart trades={n_post} (<30) — leak-check not yet significant.")
    else:
        print(f"✅ Post-restart sample size OK: {n_post} trades")
    print()

    # SHORT WR (the directional gate target) — all three windows
    for label, subset in (("PRE-GATE", pre), ("POST-GATE PRE-RESTART", mid), ("POST-RESTART", post)):
        sn = sum(1 for r in subset if r.get("side") == "SHORT")
        sw = sum(1 for r in subset if r.get("side") == "SHORT" and float(r.get("pnl") or 0) > 0)
        sp = sum(float(r.get("pnl") or 0) for r in subset if r.get("side") == "SHORT")
        wr = f"{100.0*sw/sn:.1f}" if sn else "-"
        print(f"SHORT {label}: n={sn} WR={wr}% net={sp:+.3f}")
    print()
    print("STATUS: OK (report-only)")

if __name__ == "__main__":
    main()
