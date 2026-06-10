import unittest

from trading.confluence import evaluate_confluence
from trading.config import apply_autotrade_defaults
from trading.pipeline import EntryInputs, evaluate_entry_plan
from trading.presets import PRO_STANDALONE_PRESET


class TradingEngineTests(unittest.TestCase):
    def test_pro_preset_has_rr(self):
        self.assertGreaterEqual(float(PRO_STANDALONE_PRESET["minRiskRewardRatio"]), 1.35)

    def test_apply_defaults_engine_version(self):
        cfg = apply_autotrade_defaults({})
        self.assertEqual(cfg.get("engineVersion"), "pro-2.0")
        self.assertTrue(cfg.get("marketScan"))
        self.assertFalse(cfg.get("orphanAutoAdoptForceSingleSymbol"))
        self.assertTrue(cfg.get("orphanAutoAdoptMultiEnabled"))
        self.assertLessEqual(float(cfg.get("minConfidence")), 0.66)
        self.assertLessEqual(float(cfg.get("earlyEntryScoreGapMin")), 1.4)
        self.assertLessEqual(float(cfg.get("earlyEntryMinConfidence")), 0.60)
        self.assertLessEqual(float(cfg.get("lateEntryMaxBbPctB")), 0.90)
        self.assertEqual(cfg.get("liveBadUtcHours"), [])
        self.assertEqual(cfg.get("leverageMax"), 25)
        self.assertTrue(cfg.get("adaptiveLeverageEnabled"))
        self.assertEqual(cfg.get("memoryPrimaryDays"), 7)
        self.assertEqual(cfg.get("memoryConfirmDays"), 15)
        self.assertEqual(cfg.get("memoryBaselineDays"), 30)

    def test_apply_defaults_expands_auto_scan_capacity_for_diversification(self):
        cfg = apply_autotrade_defaults({
            "symbol": "AUTO",
            "marketScan": True,
            "maxOpenPositions": 3,
            "scanTopLiquid": 25,
            "scanAnalyzeTop": 6,
            "supervisorTargetOpenPositionsMin": 1,
            "supervisorTargetOpenPositionsMax": 3,
            "supervisorSizeMultiplier": 0.55,
            "usdtAmount": 200,
            "adaptiveSizeBoostMaxPct": 35.0,
            "sessionBiasMaxSizeShiftPct": 25.0,
            "volSizeMaxMult": 1.4,
        })

        self.assertEqual(cfg.get("maxOpenPositions"), 6)
        self.assertEqual(cfg.get("scanTopLiquid"), 60)
        self.assertEqual(cfg.get("scanAnalyzeTop"), 12)
        self.assertEqual(cfg.get("supervisorTargetOpenPositionsMin"), 3)
        self.assertEqual(cfg.get("supervisorTargetOpenPositionsMax"), 6)
        self.assertEqual(cfg.get("supervisorSizeMinMultiplier"), 0.65)
        self.assertEqual(cfg.get("supervisorSizeMultiplier"), 0.65)
        self.assertTrue(cfg.get("riskCooldownLightScanEnabled"))
        self.assertEqual(cfg.get("riskCooldownLightScanAnalyzeTop"), 6)
        self.assertEqual(cfg.get("usdtAmount"), 80.0)
        self.assertEqual(cfg.get("tradeNotionalCapUsdt"), 80.0)
        self.assertEqual(cfg.get("autoScanTradeNotionalCapUsdt"), 80.0)
        self.assertEqual(cfg.get("adaptiveSizeBoostMaxPct"), 12.0)
        self.assertEqual(cfg.get("sessionBiasMaxSizeShiftPct"), 10.0)
        self.assertEqual(cfg.get("volSizeMaxMult"), 1.15)

    def test_apply_defaults_does_not_force_execution_mode(self):
        self.assertNotIn("executionMode", apply_autotrade_defaults({}))
        self.assertEqual(apply_autotrade_defaults({"executionMode": "LIVE"}).get("executionMode"), "LIVE")

    def test_apply_defaults_unlocks_utc_07_only(self):
        cfg = apply_autotrade_defaults({"liveBadUtcHours": [7, 15, 16, 17, 19, 21]})
        self.assertEqual(cfg.get("liveBadUtcHours"), [15, 16, 17, 19, 21])

    def test_confluence_chop_blocks(self):
        pk = {
            "trendUp": True,
            "trendDown": True,
            "trendUpPartial": True,
            "trendDnPartial": True,
            "macdBullish": True,
            "macdBearish": True,
            "macdBullish5m": True,
            "macdCrossUp": False,
            "macdCrossDn": False,
            "rsi14": 50,
            "rsi14_5m": 50,
            "stochK": 50,
            "stochD": 50,
            "priceNearBbLower": False,
            "priceNearBbUpper": False,
            "priceAboveBbMid": True,
            "bbSqueeze": False,
            "priceAboveVwap": True,
            "breakoutUp": False,
            "breakoutDown": False,
            "volumeRatio": 1.0,
            "cvd": 0,
            "nearResistance": False,
            "nearSupport": False,
            "highLiquiditySession": True,
            "ema200Ready": False,
            "atrPct": 0.1,
            "bbPctB": 0.5,
            "macdBullish": True,
        }
        mm = {"momentumPct": 0.05, "volumeRatio": 1.1}
        r = evaluate_confluence(pk, mm, pre_signal="LONG", pre_confidence=0.7)
        self.assertIn(r.long_score, range(0, 25))
        self.assertIn(r.short_score, range(0, 25))

    def test_confluence_soft_confirms_earlier_bias(self):
        pk = {
            "trendUp": False,
            "trendDown": False,
            "trendUpPartial": True,
            "trendDnPartial": False,
            "macdBullish": True,
            "macdBearish": False,
            "macdBullish5m": False,
            "macdCrossUp": False,
            "macdCrossDn": False,
            "rsi14": 56,
            "rsi14_5m": 52,
            "stochK": 60,
            "stochD": 55,
            "priceNearBbLower": False,
            "priceNearBbUpper": False,
            "priceAboveBbMid": False,
            "bbSqueeze": False,
            "priceAboveVwap": False,
            "breakoutUp": False,
            "breakoutDown": False,
            "volumeRatio": 1.0,
            "cvd": 0,
            "nearResistance": False,
            "nearSupport": False,
            "highLiquiditySession": False,
            "ema200Ready": False,
            "atrPct": 0.1,
            "bbPctB": 0.55,
        }
        mm = {"momentumPct": 0.05, "volumeRatio": 1.0}
        r = evaluate_confluence(pk, mm, pre_signal="LONG", pre_confidence=0.58)
        self.assertEqual(r.signal, "LONG")
        self.assertGreaterEqual(r.confidence, 0.66)

    def test_pipeline_rejects_low_rr(self):
        cfg = apply_autotrade_defaults({"minRiskRewardRatio": 2.5, "takeProfitPct": 0.5, "stopLossPct": 0.8})
        intel = {"precision": {"atrPct": 0.05}, "momentum": {"momentumPct": 0.1}}
        plan = evaluate_entry_plan(
            EntryInputs(
                cfg=cfg,
                intel=intel,
                regime={"name": "NORMAL"},
                signal="LONG",
                confidence=0.8,
                spread_bps=5,
                slippage_bps=3,
                mark=100,
                ex={},
                htf={"dir": "LONG", "strength": 0.5},
                candle_ctx={},
                adaptive_min_conf=0.7,
                trade_usdt=50,
                eff_leverage=5,
            )
        )
        self.assertFalse(plan.approved)
        self.assertEqual(plan.skip_code, "risk_reward")


if __name__ == "__main__":
    unittest.main()
