#!/usr/bin/env python3
"""
Cron: TV + Entry + Guardian performance report (every 3 days).
Compares post-gate (2026-08-22 07:21 UTC) vs pre-gate TV-alignment buckets,
entry quality (conf>=0.85 rate), SHORT WR, guardian fast-exit / leak metrics.
Reads trades_log.jsonl + scan_events.jsonl + live /autotrade/status.
Output: prints a compact markdown-ish report; exits 0 always (report-only).
"""
import json, os, sys, io
from datetime import datetime, timezone
from collections import defaultdict, Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG = os.path.join(ROOT, "obsidian_vault", "trades_log.jsonl")
SCAN = os.path.join(ROOT, "obsidian_vault", "scan_events.jsonl")
# Gate deploy 2026-08-22 14:21 BKK = 07:21 UTC
GATE_TS = datetime(2026, 8, 22, 7, 21, 0).timestamp()

def norm(s):
    if s is None: return None
    s = str(s).strip().upper()
    return s if s in ("LONG", "SHORT", "WAIT") else None

def load_jsonl(p):
    out = []
    if not os.path.exists(p):
        return out
    with io.open(p, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try: out.append(json.loads(line))
                except: pass
    return out

def phase_of(r):
    ca = r.get("closedAt") or r.get("entryDecisionAt") or 0
    return "BEFORE" if ca < GATE_TS else "AFTER"

def main():
    rows = load_jsonl(LOG)
    se = load_jsonl(SCAN)

    # ---- TV alignment buckets ----
    tv_rows = [r for r in rows if r.get("tvAtEntry") is not None]
    buckets = defaultdict(lambda: defaultdict(lambda: {"n": 0, "w": 0, "pnl": 0.0}))
    for r in tv_rows:
        tv = norm(r.get("tvAtEntry")); side = r.get("side")
        pnl = float(r.get("pnl") or 0)
        if tv == side: b = "ALIGN"
        elif tv in ("LONG", "SHORT"): b = "AGAINST"
        else: b = "NEUTRAL"
        ph = phase_of(r)
        d = buckets[ph][b]
        d["n"] += 1; d["w"] += 1 if pnl > 0 else 0; d["pnl"] += pnl

    # ---- Entry quality + SHORT WR ----
    eq = defaultdict(lambda: {"n": 0, "w": 0, "pnl": 0.0, "hi": 0, "short_n": 0, "short_w": 0, "short_pnl": 0.0})
    for r in rows:
        ph = phase_of(r); d = eq[ph]
        d["n"] += 1
        pnl = float(r.get("pnl") or 0)
        d["w"] += 1 if pnl > 0 else 0; d["pnl"] += pnl
        if float(r.get("entryConfidence", 0) or 0) >= 0.85: d["hi"] += 1
        if r.get("side") == "SHORT":
            d["short_n"] += 1; d["short_w"] += 1 if pnl > 0 else 0; d["short_pnl"] += pnl

    # ---- Guardian leak (winners peak vs actual) ----
    leak = defaultdict(lambda: {"n": 0, "pnl": 0.0, "peak": 0.0, "leak": 0.0, "fast": 0})
    FAST = {"EARLY_WHIPSAW_CUT", "WEAK_SIGNAL", "TRADINGVIEW_EARLY_EXIT", "SWING_PEAK_CLOSE", "RETRACE_BUDGET"}
    for r in rows:
        pnl = float(r.get("pnl") or 0)
        if pnl <= 0: continue
        gs = r.get("guardian_stats") or {}
        peak = gs.get("peakProfitUsdt")
        tip = gs.get("timeInPositionSec")
        if peak is None: continue
        ph = phase_of(r); d = leak[ph]
        d["n"] += 1; d["pnl"] += pnl; d["peak"] += peak; d["leak"] += (peak - pnl)
        if (tip or 0) < 180: d["fast"] += 1

    # ---- scan_events picked rate (last 3 days) ----
    se_by_day = defaultdict(lambda: {"picked": 0, "total": 0})
    for r in se:
        ts = r.get("ts") or r.get("timestamp")
        if not ts: continue
        d = datetime.utcfromtimestamp(ts).date()
        se_by_day[d]["total"] += 1
        if r.get("picked"): se_by_day[d]["picked"] += 1

    # ---- Live gate evidence ----
    live = {}
    try:
        import urllib.request
        st = json.loads(urllib.request.urlopen("http://127.0.0.1:8020/autotrade/status", timeout=10).read())
        cfg = st.get("config", {}) or {}
        live = {
            "running": st.get("running"),
            "tv_enabled": cfg.get("tradingviewEnabled"),
            "tv_healthy": (st.get("tradingviewHealth") or {}).get("healthy"),
            "shortTvMinConfidence": cfg.get("shortTvMinConfidence"),
            "tvEntryMinConfidence": cfg.get("tvEntryMinConfidence"),
            "whipGraceSec": cfg.get("whipGraceSec"),
            "tradingviewEarlyExitMinStrength": cfg.get("tradingviewEarlyExitMinStrength"),
            "_configVersion": cfg.get("_configVersion"),
        }
    except Exception as e:
        live = {"_err": str(e)[:80]}

    # ================= REPORT =================
    L = []
    L.append("=== TV+ENTRY+GUARIAN REPORT (cron 3d) ===")
    L.append(f"generated: {datetime.utcnow().isoformat()}Z  gate_deploy=2026-08-22T07:21Z")
    L.append("")
    L.append("--- TV ALIGNMENT BUCKETS (tvAtEntry) ---")
    for ph in ("BEFORE", "AFTER"):
        L.append(f"  [{ph}]")
        for b in ("ALIGN", "NEUTRAL", "AGAINST"):
            d = buckets[ph].get(b)
            if not d or d["n"] == 0:
                L.append(f"    {b:8s}: n=0")
                continue
            L.append(f"    {b:8s}: n={d['n']:4d} WR={100*d['w']/d['n']:5.1f}% net={d['pnl']:+.3f}")
    L.append("")
    L.append("--- ENTRY QUALITY + SHORT ---")
    for ph in ("BEFORE", "AFTER"):
        d = eq[ph]
        if d["n"] == 0:
            L.append(f"  [{ph}] n=0"); continue
        L.append(f"  [{ph}] n={d['n']} WR={100*d['w']/d['n']:.1f}% net={d['pnl']:+.2f} "
                 f"conf>=0.85={100*d['hi']/d['n']:.0f}% "
                 f"| SHORT n={d['short_n']} WR={100*d['short_w']/max(1,d['short_n']):.1f}% net={d['short_pnl']:+.2f}")
    L.append("")
    L.append("--- GUARDIAN LEAK (winners peak vs actual) ---")
    for ph in ("BEFORE", "AFTER"):
        d = leak[ph]
        if d["n"] == 0:
            L.append(f"  [{ph}] winners=0"); continue
        leak_pct = 100*d["leak"]/d["peak"] if d["peak"] else 0
        L.append(f"  [{ph}] winners={d['n']} leak={leak_pct:.1f}% of peak (<180s:{d['fast']})")
    L.append("")
    L.append("--- SCAN PICKED RATE (last 3 days) ---")
    for d in sorted(se_by_day)[-3:]:
        v = se_by_day[d]
        L.append(f"  {d}: {v['picked']}/{v['total']} ({100*v['picked']/max(1,v['total']):.2f}%)")
    L.append("")
    L.append("--- LIVE GATE STATE ---")
    for k, v in live.items():
        L.append(f"  {k}: {v}")

    report = "\n".join(L)
    print(report)

    # verdict-ish summary line
    after_align = buckets["AFTER"].get("ALIGN")
    after_against = buckets["AFTER"].get("AGAINST")
    sig = "SAMPLE_TOO_SMALL" if (not after_align or after_align["n"] < 30) else "OK"
    print(f"\nSUMMARY: post_gate_align_n={after_align['n'] if after_align else 0} "
          f"against_n={after_against['n'] if after_against else 0} -> {sig}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
