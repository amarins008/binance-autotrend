"""One-off cleanup: remove the 3 fabricated trades created by the buggy
reconcile fallback (old-income matching) and restore profile/snapshot state.

Fabricated entries to remove (symbol, reason, closedAt):
  AVAXUSDT EXCHANGE_TP_HIT 1790434620  (old 09-26 income)
  TAOUSDT  EXCHANGE_TP_HIT 1790397331  (old 09-26 income)
  UNIUSDT  EXCHANGE_SL_HIT 1790306168  (old 09-25 income)

For each symbol this script:
  1. Removes the fabricated line from trades_log.jsonl and symbols/<SYM>/trades.jsonl
  2. Inverts the profile counters (wins/losses/trades/realizedPnl/sumPnl/avg/max*)
  3. Inverts the multiplicative tp/sl/lock/trail/conf nudges
  4. Inverts rewardScore via the stored rewardDelta/rewardBehaviorDelta
  5. Restores streaks/tpslFeedback from the last remaining real trade
  6. Recomputes memoryWindows/weightedRecentScore/symbolRiskTune from remaining rows
Finally fixes snapshot dailyRealizedPnlUSDT/dailyPnlDateKey to today's true sum.

The REAL missing trade (AVAXUSDT SHORT closed 10:17:25 +0.512 gross) is NOT
recorded here — it is recorded after the bot restarts (separate step).
"""
import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

FABRICATED = [
    ("AVAXUSDT", "EXCHANGE_TP_HIT", 1790434620, 0.091975),   # net (win_like)
    ("TAOUSDT", "EXCHANGE_TP_HIT", 1790397331, 0.667777),    # net (win_like)
    ("UNIUSDT", "EXCHANGE_SL_HIT", 1790306168, -0.035504),   # net (loss_like)
]

BACKEND = Path(__file__).parent.parent
VAULT = BACKEND / "obsidian_vault"
TRADES_LOG = VAULT / "trades_log.jsonl"
SNAPSHOT = BACKEND / "autotrade_snapshot.json"


def is_fabricated(t) -> bool:
    for sym, reason, closed_at, _ in FABRICATED:
        if (
            t.get("symbol") == sym
            and t.get("reason") == reason
            and t.get("closedAt") == closed_at
        ):
            return True
    return False


def main():
    # ---------- Phase A: clean global trades_log.jsonl ----------
    shutil.copy2(TRADES_LOG, str(TRADES_LOG) + ".pre_cleanup.bak")
    with open(TRADES_LOG, "r", encoding="utf-8") as f:
        rows = [json.loads(l) for l in f if l.strip()]
    kept = [t for t in rows if not is_fabricated(t)]
    removed_global = len(rows) - len(kept)
    with open(TRADES_LOG, "w", encoding="utf-8") as f:
        for t in kept:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")
    print(f"[A] trades_log.jsonl: removed {removed_global}, kept {len(kept)}")

    # ---------- Phase B: per-symbol cleanup + profile revert ----------
    from trading.learning import (
        _live_closed_trades_from_symbol,
        _memory_windows_from_trades,
        _weighted_recent_memory_score,
        _symbol_risk_tune_from_recent_trades,
        _trade_reward_components,
    )
    from services import app_state

    cfg = {}
    try:
        with open(SNAPSHOT, "r", encoding="utf-8") as f:
            cfg = (json.load(f) or {}).get("config") or {}
    except Exception:
        pass

    for sym, reason, closed_at, net in FABRICATED:
        sym_dir = VAULT / "symbols" / sym

        # --- B1: per-symbol trades.jsonl ---
        tpath = sym_dir / "trades.jsonl"
        shutil.copy2(tpath, str(tpath) + ".pre_cleanup.bak")
        with open(tpath, "r", encoding="utf-8") as f:
            rows = [json.loads(l) for l in f if l.strip()]
        kept_rows = [t for t in rows if not is_fabricated(t)]
        with open(tpath, "w", encoding="utf-8") as f:
            for t in kept_rows:
                f.write(json.dumps(t, ensure_ascii=False) + "\n")
        print(f"[B] {sym}: trades.jsonl removed {len(rows) - len(kept_rows)}, kept {len(kept_rows)}")

        # --- B2: profile revert ---
        ppath = sym_dir / "profile.json"
        shutil.copy2(ppath, str(ppath) + ".pre_cleanup.bak")
        with open(ppath, "r", encoding="utf-8") as f:
            pr = json.load(f)

        win_like = net >= 0
        pr["wins"] = int(pr.get("wins", 0)) - (1 if win_like else 0)
        pr["losses"] = int(pr.get("losses", 0)) - (0 if win_like else 1)
        pr["trades"] = int(pr.get("trades", 0)) - 1
        pr["realizedPnl"] = round(float(pr.get("realizedPnl", 0.0)) - net, 6)
        pr["sumPnl"] = round(float(pr.get("sumPnl", 0.0)) - net, 6)
        pr["avgPnlPerTrade"] = round(float(pr["sumPnl"]) / max(int(pr["trades"]), 1), 6)
        # maxWin/maxLoss verified not equal to fabricated values — leave as-is.

        # multiplicative guard nudges (exact inverse of the fabricated trade's factor)
        if win_like:
            pr["tpPct"] = round(float(pr.get("tpPct", 1.8)) / 1.012, 4)
            pr["slPct"] = round(float(pr.get("slPct", 0.9)) / 1.006, 4)
            pr["profitLockTriggerUsdt"] = round(float(pr.get("profitLockTriggerUsdt", 0.35)) / 1.010, 4)
            pr["holdTrailPct"] = round(float(pr.get("holdTrailPct", 0.25)) / 1.01, 4)
            pr["holdMinConfidence"] = round(float(pr.get("holdMinConfidence", 0.72)) / 0.99, 4)
        else:
            pr["tpPct"] = round(float(pr.get("tpPct", 1.8)) / 0.988, 4)
            pr["slPct"] = round(float(pr.get("slPct", 0.9)) / 0.986, 4)
            pr["profitLockTriggerUsdt"] = round(float(pr.get("profitLockTriggerUsdt", 0.35)) / 0.965, 4)
            pr["holdTrailPct"] = round(float(pr.get("holdTrailPct", 0.25)) / 0.98, 4)
            pr["holdMinConfidence"] = round(float(pr.get("holdMinConfidence", 0.72)) / 1.01, 4)

        # rewardScore exact inverse: cur = prev*0.985 + base + behavior
        rd = float(pr.get("rewardDelta", 0.0) or 0.0)
        rbd = float(pr.get("rewardBehaviorDelta", 0.0) or 0.0)
        pr["rewardScore"] = round((float(pr.get("rewardScore", 0.0)) - rd - rbd) / 0.985, 6)

        # find the last remaining real trade (for streaks/tpslFeedback/last-*)
        prev_trade = kept_rows[-1] if kept_rows else None
        fab_trade = next(
            (t for t in rows if is_fabricated(t)),
            {"reason": reason, "pnl": net, "closedAt": closed_at},
        )
        # rebuild the fabricated trade dict to recompute behavior delta for reporting
        prev_net = None
        if prev_trade is not None:
            from trading.trade_log import net_pnl_of
            try:
                prev_net = net_pnl_of(prev_trade)
            except Exception:
                prev_net = None

        if prev_trade is not None and prev_net is not None:
            if prev_net >= 0:
                # deterministic: prev was a win -> winStreak >=1, lossStreak 0
                pr["rewardLossStreak"] = 0
                if int(pr.get("rewardWinStreak", 0)) < 1:
                    pr["rewardWinStreak"] = 1
            else:
                pr["rewardLossStreak"] = max(1, int(pr.get("rewardLossStreak", 0)))
                pr["rewardWinStreak"] = 0
            pr["lastTradeSide"] = str(prev_trade.get("side", ""))
            pr["lastTradeReason"] = str(prev_trade.get("reason", ""))
            pr["tpslFeedback"] = {
                "updatedAt": int(time.time()),
                "mode": "LIVE",
                "reason": str(prev_trade.get("reason", "")),
                "pnl": round(float(prev_net), 6),
            }
            # informational last-delta fields: recompute for the prev real trade
            try:
                prev_components = _trade_reward_components(prev_trade, cfg)
                pnl_clip = max(1.0, float(cfg.get("learningPnlClipAbsUsdt", 25.0) or 25.0))
                pnl_scale = min(1.0, abs(float(prev_net)) / pnl_clip)
                base = (1.0 * (0.75 + 0.25 * pnl_scale)) if prev_net >= 0 else (-0.8 * (0.75 + 0.25 * pnl_scale))
                pr["rewardDelta"] = round(base, 6)
                pr["rewardBehaviorDelta"] = round(float(prev_components.get("total", 0.0) or 0.0), 6)
                pr["rewardComponents"] = prev_components
            except Exception:
                pass

        # recompute windows/risk-tune from remaining rows (exact same functions the recorder uses)
        try:
            recent = _live_closed_trades_from_symbol(sym, mode="ALL", vault_dir=VAULT)
            windows = _memory_windows_from_trades(recent)
            pr["memoryWindows"] = windows
            pr["weightedRecentScore"] = _weighted_recent_memory_score(windows)
            pr["symbolRiskTune"] = _symbol_risk_tune_from_recent_trades(sym, recent, cfg)
        except Exception as e:
            print(f"    WARN window recompute failed for {sym}: {e}")

        pr["updatedAt"] = int(time.time())
        with open(ppath, "w", encoding="utf-8") as f:
            json.dump(pr, f, ensure_ascii=False, indent=1)
        print(f"    profile reverted: wins={pr['wins']} losses={pr['losses']} trades={pr['trades']} realizedPnl={pr['realizedPnl']}")

    # ---------- Phase E: snapshot daily PnL fix ----------
    with open(SNAPSHOT, "r", encoding="utf-8") as f:
        snap = json.load(f)
    tloc = time.localtime()
    today_key = (tloc.tm_year, tloc.tm_mon, tloc.tm_mday)
    daily_sum = 0.0
    with open(TRADES_LOG, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            t = json.loads(line)
            ts = int(t.get("closedAt", t.get("ts", 0)) or 0)
            if ts <= 0:
                continue
            tl = time.localtime(ts)
            if (tl.tm_year, tl.tm_mon, tl.tm_mday) == today_key:
                from trading.trade_log import net_pnl_of
                daily_sum += net_pnl_of(t)
    # add the real AVAX trade that will be recorded after restart (+0.421568 net)
    REAL_AVAX_NET = 0.421568
    daily_sum = round(daily_sum + REAL_AVAX_NET, 6)
    snap["dailyRealizedPnlUSDT"] = daily_sum
    snap["dailyPnlDateKey"] = [today_key[0], today_key[1], today_key[2]]
    with open(SNAPSHOT, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False)
    print(f"[E] snapshot dailyRealizedPnlUSDT={daily_sum} dailyPnlDateKey={snap['dailyPnlDateKey']}")
    print("DONE")


if __name__ == "__main__":
    main()
