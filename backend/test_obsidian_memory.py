import tempfile
import unittest
import json
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
        self.skipTest(
            "clean_vault.py was removed by the per-symbol storage migration; "
            "global trades_log.jsonl archive no longer exists."
        )

    def test_clean_vault_rebuilds_profiles_from_active_live_log_only(self):
        self.skipTest(
            "clean_vault.py was removed by the per-symbol storage migration; "
            "global learning_profiles.json rebuild no longer exists."
        )


if __name__ == "__main__":
    unittest.main()
