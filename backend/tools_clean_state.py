"""Clean runtime locks/frozen state in autotrade_snapshot.json but preserve liveStatsAll."""
from __future__ import annotations
import json
import sys
from pathlib import Path

p = Path(__file__).resolve().parent / "autotrade_snapshot.json"
data = json.loads(p.read_text(encoding="utf-8-sig"))

# Build the cleared structure that mirror main._persist_autotrade_snapshot output
clean = {
    "savedAt": int(__import__("time").time()),
    "paper": data.get("paper", {"position": None, "wins": 0, "losses": 0, "realizedPnl": 0.0, "history": []}),
    "config": data.get("config"),
    "running": False,
    "manageOpenOnly": False,
    "pauseUntil": 0,
    "riskCooldownLossSignature": "",
    "riskCooldownBySymbol": {},
    "riskCooldownLastMarketCheckAt": int(data.get("riskCooldownLastMarketCheckAt", 0) or 0),
    "sessionId": None,
    "startedAt": 0,
    "lastTradeAt": 0,
    "liveProfitLocks": {},
    "scanBoard": [],
    "cooldownWatchlist": {},
    "hermesAgents": data.get("hermesAgents") or {},
    "hermesSupervisorReview": {},
    "trades": [],
    "log": [],
    "lastSkip": None,
    "lastDecision": None,
    "consecutiveErrors": 0,
    "perfLocks": {},
}

# Preserve any keys not in our clear set to avoid losing new fields
for k, v in data.items():
    if k not in clean:
        clean[k] = v

p.write_text(json.dumps(clean, ensure_ascii=False, default=str), encoding="utf-8")
print(f"OK clean state written: {p}")
print(f"perfLocks cleared: {len(data.get('perfLocks', {}) or {})}")
print(f"running: {data.get('running')} -> False")
print(f"frozenAt: {data.get('frozenAt', '-')}")
