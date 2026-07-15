"""Shared storage: global data that all symbols read but rarely mutates.

Stores config snapshots, risk limits, and daily aggregates under
``obsidian_vault/shared/`` so per-symbol storage never needs to touch
global files.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from services.config_paths import VAULT_DIR
from services.file_utils import _atomic_write_text


class SharedStorage:
    """File-per-concern shared storage.

    Directory layout::

        obsidian_vault/shared/
        ├── config.json       – global config snapshot
        ├── risk.json         – global risk limits
        ├── daily_stats.json  – daily PnL aggregate
        └── all_trades.jsonl  – append-only global trade log
    """

    def __init__(self, vault_dir: Path | None = None):
        self._dir = Path(vault_dir) if vault_dir else VAULT_DIR
        self._shared = self._dir / "shared"
        self._shared.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------

    def load_config(self) -> dict:
        path = self._shared / "config.json"
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
        return {}

    def save_config(self, config: dict) -> None:
        _atomic_write_text(
            self._shared / "config.json",
            json.dumps(config, ensure_ascii=False, indent=2, default=str),
        )

    # ------------------------------------------------------------------
    # Risk limits
    # ------------------------------------------------------------------

    def load_risk(self) -> dict:
        path = self._shared / "risk.json"
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
        return {}

    def save_risk(self, risk: dict) -> None:
        _atomic_write_text(
            self._shared / "risk.json",
            json.dumps(risk, ensure_ascii=False, indent=2, default=str),
        )

    # ------------------------------------------------------------------
    # Daily stats
    # ------------------------------------------------------------------

    def load_daily_stats(self) -> dict:
        path = self._shared / "daily_stats.json"
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict) and data.get("date") == time.strftime("%Y-%m-%d"):
                    return data
            except Exception:
                pass
        return {"date": time.strftime("%Y-%m-%d"), "pnl": 0.0, "trades": 0, "wins": 0, "losses": 0}

    def save_daily_stats(self, stats: dict) -> None:
        _atomic_write_text(
            self._shared / "daily_stats.json",
            json.dumps(stats, ensure_ascii=False, indent=2, default=str),
        )

    # ------------------------------------------------------------------
    # Global trade log (append-only)
    # ------------------------------------------------------------------

    def append_global_trade(self, trade: dict) -> None:
        path = self._shared / "all_trades.jsonl"
        line = json.dumps(trade, ensure_ascii=False, default=str) + "\n"
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass

    def load_global_trades(self, limit: int = 200) -> list[dict]:
        path = self._shared / "all_trades.jsonl"
        if not path.exists():
            return []
        trades: list[dict] = []
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
            for line in lines[-limit:]:
                line = line.strip()
                if not line:
                    continue
                try:
                    trades.append(json.loads(line))
                except Exception:
                    continue
        except Exception:
            pass
        return trades
