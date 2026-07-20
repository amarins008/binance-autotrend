"""Central path constants for the Hermes autotrade backend.

All file-system paths are defined here to avoid duplication and drift
across modules.
"""

import os
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = Path(os.environ.get("HERMES_DATA_DIR", BACKEND_ROOT)).resolve()

ENV_PATH = Path(os.environ.get("HERMES_ENV_PATH", BACKEND_ROOT / ".env")).resolve()
SNAPSHOT_PATH = DATA_ROOT / "autotrade_snapshot.json"
VAULT_DIR = DATA_ROOT / "obsidian_vault"
TRADES_LOG_PATH = VAULT_DIR / "trades_log.jsonl"
