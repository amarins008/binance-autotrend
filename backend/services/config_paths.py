"""Central path constants for the Hermes autotrade backend.

All file-system paths are defined here to avoid duplication and drift
across modules.
"""

from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parent.parent

ENV_PATH = BACKEND_ROOT / ".env"
SNAPSHOT_PATH = BACKEND_ROOT / "autotrade_snapshot.json"
VAULT_DIR = BACKEND_ROOT / "obsidian_vault"
TRADES_LOG_PATH = VAULT_DIR / "trades_log.jsonl"
