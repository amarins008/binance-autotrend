import tempfile
import unittest
import importlib.util
import json
import time
from pathlib import Path

from obsidian_memory import (
    TRADING_VAULT_FOLDERS,
    append_scan_memory,
    append_trade_memory,
    ensure_trading_vault,
    trading_vault_dir,
    write_self_review_memory,
    write_symbol_memory,
)


def _load_clean_vault_module():
    path = Path(__file__).parent / "obsidian_vault" / "clean_vault.py"
    spec = importlib.util.spec_from_file_location("clean_vault_under_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class TestObsidianLongTermMemory(unittest.TestCase):
    def test_creates_trading_vault_structure(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)

            root = ensure_trading_vault(vault)

            self.assertEqual(root, trading_vault_dir(vault))
            for folder in TRADING_VAULT_FOLDERS:
                self.assertTrue((root / folder).is_dir(), folder)
            self.assertTrue((root / "README.md").exists())

    def test_writes_symbol_trade_scan_and_self_review_memory(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)
            profile = {
                "wins": 1,
                "losses": 1,
                "trades": 2,
                "realizedPnl": -0.25,
                "observations": 4,
                "pickedCount": 2,
                "lastSignal": "SHORT",
                "lastConfidence": 0.81,
                "lastScanScore": 1.2,
            }
            trade = {"symbol": "BTCUSDT", "side": "SHORT", "entry": 100.0, "exit": 101.0, "pnl": -0.5, "reason": "SL"}

            write_symbol_memory(vault, "BTCUSDT", profile, trade)
            append_trade_memory(vault, trade, "LIVE")
            append_scan_memory(vault, "BTCUSDT", {"signal": "SHORT", "confidence": 0.81}, True, 1.2)
            write_self_review_memory(
                vault,
                {
                    "lossStreak": 3,
                    "actions": ["minConfidence 0.72->0.74"],
                    "minConfidence": 0.74,
                    "maxOpenPositions": 2,
                    "noTradeWindows": ["19:00-20:00"],
                },
            )

            root = trading_vault_dir(vault)
            self.assertTrue((root / "MarketPatterns" / "BTCUSDT.md").exists())
            self.assertTrue(any((root / "Journal").glob("*.md")))
            self.assertTrue(any((root / "Failures").glob("*BTCUSDT.md")))
            self.assertTrue(any((root / "AI-Thoughts").glob("*.md")))
            self.assertTrue((root / "RiskRules" / "ActiveRiskRules.md").exists())

    def test_self_review_memory_separates_infra_auth_incident(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)

            write_self_review_memory(
                vault,
                {
                    "lossStreak": 4,
                    "causeCategory": "infra_auth",
                    "causeTitle": "Binance API/IP permission incident",
                    "causeDetail": "Invalid API-key, IP, or permissions for action (-2015)",
                    "operatorAction": "Fix Binance whitelist IP",
                    "actions": ["operator_action_required: fix Binance API/IP permission"],
                    "minConfidence": 0.62,
                    "maxOpenPositions": 5,
                    "noTradeWindows": [],
                },
            )

            root = trading_vault_dir(vault)
            thought_files = list((root / "AI-Thoughts").glob("*.md"))
            self.assertTrue(thought_files)
            text = thought_files[0].read_text(encoding="utf-8")
            self.assertIn("CauseCategory: infra_auth", text)
            self.assertIn("Infrastructure/auth control issue", text)
            self.assertIn("OperatorAction: Fix Binance whitelist IP", text)
            self.assertNotIn("entry filter may be misaligned", text)

    def test_clean_vault_archives_stale_trade_rows_without_deleting_history(self):
        clean_vault = _load_clean_vault_module()
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)
            clean_vault.VAULT_DIR = vault
            clean_vault.LOG_PATH = vault / "trades_log.jsonl"
            clean_vault.ARCHIVE_DIR = vault / "archive"
            clean_vault.ARCHIVE_LOG_PATH = clean_vault.ARCHIVE_DIR / "trades_log.archive.jsonl"
            clean_vault.ANOMALY_LOG_PATH = clean_vault.ARCHIVE_DIR / "trades_log.anomalies.jsonl"
            now = int(time.time())
            old_live = {"ts": now - 40 * 86400, "mode": "LIVE", "symbol": "OLDUSDT", "pnl": 1.0}
            fresh_live = {"ts": now - 2 * 86400, "mode": "LIVE", "symbol": "NEWUSDT", "pnl": 0.5}
            old_scan = {"ts": now - 10 * 86400, "mode": "SCAN", "symbol": "SCANUSDT", "score": 0.8}
            clean_vault.LOG_PATH.write_text(
                "\n".join(json.dumps(x) for x in (old_live, fresh_live, old_scan)) + "\n",
                encoding="utf-8",
            )

            out = clean_vault.clean_trades_log()

            active = clean_vault.LOG_PATH.read_text(encoding="utf-8")
            archived = clean_vault.ARCHIVE_LOG_PATH.read_text(encoding="utf-8")
            self.assertEqual(out["archived"], 2)
            self.assertIn("NEWUSDT", active)
            self.assertNotIn("OLDUSDT", active)
            self.assertIn("OLDUSDT", archived)
            self.assertIn("SCANUSDT", archived)

    def test_clean_vault_rebuilds_profiles_from_active_live_log_only(self):
        clean_vault = _load_clean_vault_module()
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)
            clean_vault.VAULT_DIR = vault
            clean_vault.LOG_PATH = vault / "trades_log.jsonl"
            clean_vault.PROFILES_PATH = vault / "learning_profiles.json"
            now = int(time.time())
            clean_vault.LOG_PATH.write_text(
                "\n".join(
                    json.dumps(x)
                    for x in (
                        {"ts": now, "mode": "LIVE", "symbol": "BTCUSDT", "pnl": 0.4},
                        {"ts": now, "mode": "LIVE", "symbol": "BTCUSDT", "pnl": -0.1},
                        {"ts": now, "mode": "SCAN", "symbol": "BTCUSDT", "score": 0.9},
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            clean_vault.PROFILES_PATH.write_text(
                json.dumps(
                    {
                        "BTCUSDT": {"wins": 99, "losses": 99, "realizedPnl": -99.0, "observations": 5},
                        "OLDUSDT": {"wins": 3, "losses": 1, "realizedPnl": 8.0, "observations": 2},
                    }
                ),
                encoding="utf-8",
            )

            profiles = clean_vault.rebuild_learning_profiles_from_active_log()

            self.assertEqual(profiles["BTCUSDT"]["wins"], 1)
            self.assertEqual(profiles["BTCUSDT"]["losses"], 1)
            self.assertEqual(profiles["BTCUSDT"]["realizedPnl"], 0.3)
            self.assertEqual(profiles["BTCUSDT"]["observations"], 5)
            self.assertEqual(profiles["OLDUSDT"]["wins"], 0)
            self.assertEqual(profiles["OLDUSDT"]["losses"], 0)
            self.assertEqual(profiles["OLDUSDT"]["rewardScore"], 0.0)


if __name__ == "__main__":
    unittest.main()
