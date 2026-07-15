"""Per-symbol storage: isolated file-per-symbol for profiles, trades, and vault.

Instead of reading/writing a single global ``learning_profiles.json`` and
``symbol_profiles.json`` for every symbol update (which scales as O(symbols)
on each write), each symbol gets its own directory under
``obsidian_vault/symbols/{SYMBOL}/`` containing:

- profile.json            – learning profile (was inside learning_profiles.json)
- symbol_profile.json     – 3-tier symbol profile (was inside symbol_profiles.json)
- trades.jsonl            – closed trades for this symbol only
- windows.json            – rolling window cache (TTL-based)
- risk_tune.json          – last risk-tune snapshot
- vault/                  – per-symbol Obsidian vault folder

Shared/global data stays in ``obsidian_vault/shared/``.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

from services.config_paths import VAULT_DIR
from services.file_utils import _atomic_write_text


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_name(value: str) -> str:
    """Sanitise a symbol string for use as a directory/file name."""
    text = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip())
    return text.strip("-") or "UNKNOWN"


def _today_key(ts: int | None = None) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(int(ts or time.time())))


def _append_section(path: Path, heading: str, lines: list[str]) -> None:
    """Append a markdown section to *path*, creating it if necessary."""
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join([f"## {heading}", *lines, ""])
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        sep = "" if existing.endswith("\n") else "\n"
        path.write_text(existing + sep + body, encoding="utf-8")
    else:
        path.write_text(f"# {path.stem}\n\n{body}", encoding="utf-8")


# ---------------------------------------------------------------------------
# PerSymbolStorage
# ---------------------------------------------------------------------------

class PerSymbolStorage:
    """File-per-symbol storage for profiles, trades, and vault notes.

    Directory layout::

        obsidian_vault/symbols/{SYMBOL}/
        ├── profile.json
        ├── symbol_profile.json
        ├── trades.jsonl
        ├── windows.json
        ├── risk_tune.json
        └── vault/
            ├── Journal/
            ├── MarketPatterns/
            ├── Failures/
            └── Improvements/
    """

    VAULT_FOLDERS = ("Journal", "MarketPatterns", "Failures", "Improvements")

    def __init__(self, vault_dir: Path | None = None, symbol: str = ""):
        self._vault_dir = Path(vault_dir) if vault_dir else VAULT_DIR
        self.symbol = _safe_name(symbol).upper()
        self._dir = self._vault_dir / "symbols" / self.symbol
        self._vault = self._dir / "vault"
        self._ensure_dirs()

    # ------------------------------------------------------------------
    # Directory bootstrap
    # ------------------------------------------------------------------

    def _ensure_dirs(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        for folder in self.VAULT_FOLDERS:
            (self._vault / folder).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Profile (learning_profiles.json per-symbol entry)
    # ------------------------------------------------------------------

    def load_profile(self) -> dict:
        """Load the learning profile for this symbol."""
        path = self._dir / "profile.json"
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
        return {}

    def save_profile(self, profile: dict) -> None:
        """Atomically save the learning profile for this symbol."""
        _atomic_write_text(
            self._dir / "profile.json",
            json.dumps(profile, ensure_ascii=False, indent=2),
        )

    # ------------------------------------------------------------------
    # Symbol profile (symbol_profiles.json per-symbol entry)
    # ------------------------------------------------------------------

    def load_symbol_profile(self) -> dict:
        """Load the 3-tier symbol profile for this symbol."""
        path = self._dir / "symbol_profile.json"
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
        return {}

    def save_symbol_profile(self, profile: dict) -> None:
        """Atomically save the 3-tier symbol profile for this symbol."""
        _atomic_write_text(
            self._dir / "symbol_profile.json",
            json.dumps(profile, ensure_ascii=False, indent=2),
        )

    # ------------------------------------------------------------------
    # Trades (per-symbol trades.jsonl)
    # ------------------------------------------------------------------

    def load_trades(self, mode: str = "LIVE") -> list[dict]:
        """Load closed trades for this symbol, optionally filtered by *mode*."""
        path = self._dir / "trades.jsonl"
        if not path.exists():
            return []
        trades: list[dict] = []
        mode_up = str(mode or "").upper()
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if mode_up and mode_up != "ALL":
                    if str(obj.get("mode", "")).upper() != mode_up:
                        continue
                trades.append(obj)
        except Exception:
            pass
        return trades

    def append_trade(self, trade: dict) -> None:
        """Append a single trade entry to the per-symbol log."""
        path = self._dir / "trades.jsonl"
        line = json.dumps(trade, ensure_ascii=False, default=str) + "\n"
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Windows cache (rolling window / memory window snapshot)
    # ------------------------------------------------------------------

    def load_windows(self, max_age_sec: int = 300) -> dict:
        """Load cached rolling windows if fresh enough."""
        path = self._dir / "windows.json"
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                ts = float(data.get("ts", 0) or 0)
                if time.time() - ts < max_age_sec:
                    return data.get("windows", {})
        except Exception:
            pass
        return {}

    def save_windows(self, windows: dict) -> None:
        """Cache rolling windows with a timestamp."""
        _atomic_write_text(
            self._dir / "windows.json",
            json.dumps({"windows": windows, "ts": time.time()}, ensure_ascii=False, indent=2),
        )

    # ------------------------------------------------------------------
    # Risk tune cache
    # ------------------------------------------------------------------

    def load_risk_tune(self, max_age_sec: int = 300) -> dict:
        """Load cached risk-tune if fresh enough."""
        path = self._dir / "risk_tune.json"
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                ts = float(data.get("ts", 0) or 0)
                if time.time() - ts < max_age_sec:
                    return data
        except Exception:
            pass
        return {}

    def save_risk_tune(self, risk_tune: dict) -> None:
        """Save risk-tune snapshot with a timestamp."""
        out = dict(risk_tune)
        out["ts"] = time.time()
        _atomic_write_text(
            self._dir / "risk_tune.json",
            json.dumps(out, ensure_ascii=False, indent=2),
        )

    # ------------------------------------------------------------------
    # Guardian lock (per-symbol liveProfitLocks entry)
    # ------------------------------------------------------------------

    def load_guardian_lock(self) -> dict:
        """Load the per-symbol guardian (liveProfitLocks) entry."""
        path = self._dir / "guardian_lock.json"
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
        return {}

    def save_guardian_lock(self, lock: dict) -> None:
        """Atomically save the per-symbol guardian lock entry."""
        out = dict(lock)
        out["ts"] = time.time()
        _atomic_write_text(
            self._dir / "guardian_lock.json",
            json.dumps(out, ensure_ascii=False, indent=2, default=str),
        )

    # ------------------------------------------------------------------
    # TradingView signal cache (per-symbol)
    # ------------------------------------------------------------------

    def load_tv_signal(self) -> dict:
        """Load cached TradingView signal for this symbol."""
        path = self._dir / "tv_signal.json"
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
        return {}

    def save_tv_signal(self, signal: dict) -> None:
        """Atomically save the cached TradingView signal for this symbol."""
        out = dict(signal)
        out["ts"] = time.time()
        _atomic_write_text(
            self._dir / "tv_signal.json",
            json.dumps(out, ensure_ascii=False, indent=2, default=str),
        )

    # ------------------------------------------------------------------
    # Runtime state (per-symbol transient state: cooldowns, scan board, etc.)
    # ------------------------------------------------------------------

    def load_runtime(self) -> dict:
        """Load the per-symbol runtime state dict."""
        path = self._dir / "runtime.json"
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
        return {}

    def save_runtime(self, runtime: dict) -> None:
        """Atomically save the per-symbol runtime state dict."""
        _atomic_write_text(
            self._dir / "runtime.json",
            json.dumps(runtime, ensure_ascii=False, indent=2, default=str),
        )

    # ------------------------------------------------------------------
    # Vault – per-symbol journal / failure / improvement / note
    # ------------------------------------------------------------------

    def append_journal(self, trade: dict) -> None:
        """Append a trade outcome to the per-symbol Journal."""
        now = int(trade.get("closedAt") or trade.get("ts") or time.time())
        day = _today_key(now)
        pnl = float(trade.get("pnl", 0.0) or 0.0)
        outcome = "WIN" if pnl >= 0 else "LOSS"
        lines = [
            f"- Time: {time.strftime('%H:%M:%S', time.localtime(now))}",
            f"- Symbol: {self.symbol}",
            f"- Mode: {str(trade.get('mode', 'LIVE')).upper()}",
            f"- Side: {trade.get('side', '-')}",
            f"- Outcome: {outcome}",
            f"- PnL: {round(pnl, 6)}",
            f"- Reason: {trade.get('reason', '-')}",
        ]
        _append_section(self._vault / "Journal" / f"{day}.md", f"{self.symbol} {outcome}", lines)

    def append_failure(self, trade: dict) -> None:
        """Append a losing trade to the per-symbol Failures folder."""
        pnl = float(trade.get("pnl", 0.0) or 0.0)
        if pnl >= 0:
            return
        now = int(trade.get("closedAt") or trade.get("ts") or time.time())
        day = _today_key(now)
        lines = [
            f"- Time: {time.strftime('%H:%M:%S', time.localtime(now))}",
            f"- Mode: {str(trade.get('mode', 'LIVE')).upper()}",
            f"- Side: {trade.get('side', '-')}",
            f"- PnL: {round(pnl, 6)}",
            f"- Reason: {trade.get('reason', '-')}",
            "- Reflection: Compare with recent scan context before next entry.",
            "- Next Check: raise confidence, reduce size, or pause if pattern repeats.",
        ]
        _append_section(self._vault / "Failures" / f"{day}.md", f"{self.symbol} Failure Review", lines)

    def append_improvement(self, trade: dict) -> None:
        """Append a winning trade to the per-symbol Improvements folder."""
        pnl = float(trade.get("pnl", 0.0) or 0.0)
        if pnl < 0:
            return
        now = int(trade.get("closedAt") or trade.get("ts") or time.time())
        day = _today_key(now)
        lines = [
            f"- Time: {time.strftime('%H:%M:%S', time.localtime(now))}",
            f"- Mode: {str(trade.get('mode', 'LIVE')).upper()}",
            f"- Side: {trade.get('side', '-')}",
            f"- PnL: {round(pnl, 6)}",
            f"- Reason: {trade.get('reason', '-')}",
            "- Learning: reinforce setup only if follow-up trades keep positive expectancy.",
        ]
        _append_section(self._vault / "Improvements" / f"{day}.md", f"{self.symbol} Positive Reinforcement", lines)

    def append_scan_pick(self, intel: dict, score: float) -> None:
        """Record a scan-pick event in the per-symbol Journal."""
        day = _today_key()
        lines = [
            f"- Symbol: {self.symbol}",
            f"- Signal: {intel.get('signal', 'WAIT')}",
            f"- Confidence: {intel.get('confidence', 0.0)}",
            f"- Score: {round(float(score or 0.0), 6)}",
            "- Memory Intent: candidate selected for future outcome comparison.",
        ]
        _append_section(self._vault / "Journal" / f"{day}.md", f"{self.symbol} scan picked", lines)

    def update_symbol_note(self, profile: dict, last_trade: dict | None = None) -> None:
        """Write/update the MarketPatterns/{SYMBOL}.md summary note."""
        wins = int(profile.get("wins", 0) or 0)
        losses = int(profile.get("losses", 0) or 0)
        total = wins + losses
        wr = round((wins / total) * 100, 2) if total > 0 else 0.0
        pnl = round(float(profile.get("realizedPnl", 0.0) or 0.0), 6)
        observations = int(profile.get("observations", 0) or 0)
        picked = int(profile.get("pickedCount", 0) or 0)
        pick_rate = round((picked / observations) * 100, 2) if observations > 0 else 0.0
        lines = [
            f"# {self.symbol}",
            "",
            "## Performance",
            f"- Wins: {wins}",
            f"- Losses: {losses}",
            f"- Trades: {total}",
            f"- WinRate: {wr}%",
            f"- RealizedPnL: {pnl}",
            f"- RewardScore: {round(float(profile.get('rewardScore', 0.0) or 0.0), 6)}",
            "",
            "## Scan Behavior",
            f"- Observations: {observations}",
            f"- PickedCount: {picked}",
            f"- PickRate: {pick_rate}%",
            f"- LastSignal: {profile.get('lastSignal', 'WAIT')}",
            f"- LastConfidence: {profile.get('lastConfidence', 0.0)}",
            f"- LastScanScore: {profile.get('lastScanScore', 0.0)}",
            f"- UpdatedAt: {int(time.time())}",
            "",
        ]
        if last_trade:
            lines += [
                "## Last Trade",
                f"- Side: {last_trade.get('side')}",
                f"- Entry: {last_trade.get('entry')}",
                f"- Exit: {last_trade.get('exit')}",
                f"- PnL: {last_trade.get('pnl')}",
                f"- Reason: {last_trade.get('reason')}",
                f"- ClosedAt: {last_trade.get('closedAt')}",
                f"- PatternTags: {last_trade.get('patternTags', [])}",
                f"- PatternBias: {last_trade.get('patternBias', 0.0)}",
                f"- PatternScore: {last_trade.get('patternScore', 0.0)}",
                "",
            ]
        path = self._vault / "MarketPatterns" / f"{self.symbol}.md"
        path.write_text("\n".join(lines), encoding="utf-8")

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def symbol_dir(self) -> Path:
        """Return the root directory for this symbol."""
        return self._dir

    def vault_dir(self) -> Path:
        """Return the per-symbol vault directory."""
        return self._vault
