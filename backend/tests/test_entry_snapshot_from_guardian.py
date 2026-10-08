import sys, os, pathlib
# Add project root to sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
from trading import learning as learning_mod
from backend import main
import unittest

class DummyCtx:
    def __init__(self, *args, **kwargs):
        self.profile = {}
        self._sym_profile = {}
        self._dirty_sym_profile = False
    def record_trade(self, entry):
        pass
    def commit(self):
        pass
    def update_symbol_note(self, trade):
        pass

class TestEntrySnapshotFromGuardian(unittest.TestCase):
    def test_fields_from_guardian_lock(self):
        # set config (not used for new fields)
        main.AUTO_TRADE["config"] = {"minConfidence": 0.8}
        # dummy snapshot with required keys
        dummy_snapshot = {
            "entryConfidence": 0.71,
            "entryScore": 0.65,
            "patternBias": 0.02,
            "patternScore": 1.1,
            "tvConfidence": 0.9,
            "entryNotional": 55.0,
            "tvConfirmHits": 3,
            "tvWaitMinConfUsed": 0.85,
            "perfLockedAtEntry": 123456,
            "leverageAtEntry": 4,
            "feesPaidOnEntry": 0.04,
            "adaptiveMinConf": 0.78,
        }
        # mock per_symbol_storage module
        import types, tempfile, shutil
        temp_dir = tempfile.mkdtemp()
        # Ensure learning_mod uses temp VAULT_DIR to avoid side effects
        learning_mod.VAULT_DIR = temp_dir
        class MockPerSymbolStorage:
            def __init__(self, *a, **kw):
                pass
            def load_guardian_lock(self):
                return {"entrySnapshot": dummy_snapshot}
        sys.modules['trading.per_symbol_storage'] = types.SimpleNamespace(PerSymbolStorage=MockPerSymbolStorage)
        # capture output
        captured = {}
        learning_mod._append_trade_log = lambda entry: captured.update(entry)
        trade = {"pnl": 1.2, "closedAt": 1700000000, "reason": "LOCAL_TP_HIT"}
        learning_mod._record_learning_trade("TESTUSDT", trade, "LIVE")
        # cleanup temp dir
        shutil.rmtree(temp_dir)

        # assertions
        self.assertEqual(captured["entryConfidence"], dummy_snapshot["entryConfidence"])
        self.assertEqual(captured["entryScore"], dummy_snapshot["entryScore"])
        self.assertEqual(captured["biasValue"], dummy_snapshot["patternBias"])
        self.assertEqual(captured["patternScoreAtEntry"], dummy_snapshot["patternScore"])
        self.assertEqual(captured["tvWaitMinConfUsed"], dummy_snapshot["tvWaitMinConfUsed"])
        self.assertEqual(captured["entryNotional"], dummy_snapshot["entryNotional"])
        self.assertEqual(captured["tvConfirmHits"], dummy_snapshot["tvConfirmHits"])
        self.assertEqual(captured["perfLockedAtEntry"], dummy_snapshot["perfLockedAtEntry"])
        self.assertEqual(captured["leverageAtEntry"], dummy_snapshot["leverageAtEntry"])
        self.assertEqual(captured["feesPaidOnEntry"] , dummy_snapshot["feesPaidOnEntry"])
        self.assertEqual(captured["adaptiveMinConf"], dummy_snapshot["adaptiveMinConf"])

if __name__ == "__main__":
    unittest.main()
