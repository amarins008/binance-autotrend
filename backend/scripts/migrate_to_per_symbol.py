"""Migrate existing global learning/symbol profiles to per-symbol vault layout.

Run this once to bootstrap the new per-symbol storage without losing data.

Usage:
    cd backend
    python scripts/migrate_to_per_symbol.py [--dry-run]
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
VAULT = BACKEND / "obsidian_vault"
LEARN_PATH = VAULT / "learning_profiles.json"
SYM_PROFILES_PATH = VAULT / "symbol_profiles.json"
TRADES_LOG_PATH = VAULT / "trades_log.jsonl"
SHARED_DIR = VAULT / "shared"
SYMBOLS_DIR = VAULT / "symbols"


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def migrate(dry_run: bool = False) -> dict:
    """Migrate global profiles into per-symbol directories.

    Returns a summary dict with counts.
    """
    learn = _load_json(LEARN_PATH)
    sym_profiles = _load_json(SYM_PROFILES_PATH)
    trades = _load_jsonl(TRADES_LOG_PATH)

    # Group trades by symbol
    trades_by_symbol: dict[str, list[dict]] = {}
    for t in trades:
        sym = str(t.get("symbol", "") or "").upper().strip()
        if sym:
            trades_by_symbol.setdefault(sym, []).append(t)

    all_symbols = set(learn.keys()) | set(sym_profiles.keys()) | set(trades_by_symbol.keys())
    migrated = 0
    skipped = 0

    if not dry_run:
        SHARED_DIR.mkdir(parents=True, exist_ok=True)
        SYMBOLS_DIR.mkdir(parents=True, exist_ok=True)

    # Save shared config snapshot (empty for now; will be populated on first save)
    if not dry_run:
        (SHARED_DIR / "config.json").write_text("{}", encoding="utf-8")
        (SHARED_DIR / "risk.json").write_text("{}", encoding="utf-8")
        (SHARED_DIR / "daily_stats.json").write_text(
            json.dumps({"date": "", "pnl": 0.0, "trades": 0, "wins": 0, "losses": 0}, indent=2),
            encoding="utf-8",
        )

    for sym in sorted(all_symbols):
        sym_dir = SYMBOLS_DIR / sym
        if sym_dir.exists() and (sym_dir / "profile.json").exists():
            skipped += 1
            continue

        if dry_run:
            migrated += 1
            continue

        sym_dir.mkdir(parents=True, exist_ok=True)
        vault_dir = sym_dir / "vault"
        vault_dir.mkdir(exist_ok=True)

        # profile.json
        profile = learn.get(sym, {})
        (sym_dir / "profile.json").write_text(
            json.dumps(profile, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

        # symbol_profile.json
        sp = sym_profiles.get(sym, {})
        (sym_dir / "symbol_profile.json").write_text(
            json.dumps(sp, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

        # trades.jsonl
        sym_trades = trades_by_symbol.get(sym, [])
        if sym_trades:
            lines = "\n".join(json.dumps(t, ensure_ascii=False, default=str) for t in sym_trades) + "\n"
            (sym_dir / "trades.jsonl").write_text(lines, encoding="utf-8")

        migrated += 1

    # Keep backward-compat: copy trades_log.jsonl into shared/all_trades.jsonl
    if not dry_run and TRADES_LOG_PATH.exists():
        shutil.copy2(TRADES_LOG_PATH, SHARED_DIR / "all_trades.jsonl")

    return {
        "totalSymbols": len(all_symbols),
        "migrated": migrated,
        "skipped": skipped,
        "dryRun": dry_run,
    }


def main():
    dry_run = "--dry-run" in sys.argv
    result = migrate(dry_run)
    print(f"Migration {'(dry run) ' if dry_run else ''}complete:")
    print(f"  Total symbols: {result['totalSymbols']}")
    print(f"  Migrated:      {result['migrated']}")
    print(f"  Skipped:       {result['skipped']}")


if __name__ == "__main__":
    main()
