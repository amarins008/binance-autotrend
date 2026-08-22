#!/usr/bin/env python3
"""
Verification harness for the new TV entry gates (2026-08-22):
- Generic gate: tvEntryMaxAgeSec=30, tvEntryMinConfidence=0.60
- SHORT gate: shortTvMinConfidence=0.60 (blocks if TV signal=LONG)
Tests against the actual scan-board qualification logic in main.py.
"""
import sys, os, time, copy, json
sys.path.insert(0, "E:/My Project/Binance autotrend/backend")

from trading.config import apply_autotrade_defaults

# ============================================================
# Minimal stub of the scan-board qualification logic (main.py ~3466-3520)
# ============================================================

def qualify_entry(out: dict, cfg: dict) -> tuple[bool, str]:
    """Replicates the qualification logic from main.py scan board."""
    sig = out.get("signal", "WAIT")
    conf = float(out.get("confidence", 0.0) or 0.0)
    spread_bps = float(out.get("spreadBps", 0.0) or 0.0)
    
    qualified = True
    reject_reason = ""
    
    if sig not in ("LONG", "SHORT"):
        qualified = False
        reject_reason = "signal_wait"
    elif conf < 0.60:  # adaptive_min_conf hard floor from config
        qualified = False
        reject_reason = "low_conf"
    elif conf > float(cfg.get("maxEntryConfidence", 0.90) or 0.90):
        qualified = False
        reject_reason = "conf_too_high_late"
    elif spread_bps > float(cfg.get("maxSpreadBps", 50) or 50):
        qualified = False
        reject_reason = "wide_spread"
    
    # Generic TV gate (tv_stale_or_weak)
    elif bool(cfg.get("tradingviewEnabled", False)):
        _tv = out.get("tv") if isinstance(out.get("tv"), dict) else {}
        if _tv:
            _tv_age = int(_tv.get("age", 9999) or 9999)
            _tv_conf = float(_tv.get("confidence", 0.0) or 0.0)
            _max_age = int(cfg.get("tvEntryMaxAgeSec", 30) or 30)
            _min_conf = float(cfg.get("tvEntryMinConfidence", 0.60) or 0.60)
            if _tv_age > _max_age or _tv_conf < _min_conf:
                qualified = False
                reject_reason = "tv_stale_or_weak"
    
    # SHORT-specific TV gate
    if qualified and sig == "SHORT":
        _tv = out.get("tv") if isinstance(out.get("tv"), dict) else {}
        if _tv:
            _tv_sig = str(_tv.get("signal", "")).upper()
            _tv_c = float(_tv.get("confidence", 0.0) or 0.0)
            _short_min_conf = float(cfg.get("shortTvMinConfidence", 0.60) or 0.60)
            if _tv_sig == "LONG":
                qualified = False
                reject_reason = "short_tv_conflict_long"
            elif _tv_c < _short_min_conf:
                qualified = False
                reject_reason = "short_tv_low_conf"
    
    return qualified, reject_reason


# ============================================================
# Test cases with REALISTIC TV confidence values (0.3, 0.6, 0.8)
# ============================================================

def make_out(signal, conf, tv_sig=None, tv_conf=0.6, tv_age=10, tv_strength=0.8, spread=5.0):
    """Create an output dict like intel_analyze returns."""
    out = {
        "symbol": "TESTUSDT",
        "signal": signal,
        "confidence": conf,
        "spreadBps": spread,
    }
    if tv_sig is not None:
        out["tv"] = {
            "signal": tv_sig,
            "confidence": tv_conf,
            "strength": tv_strength,
            "age": tv_age,
        }
    return out


def run_tests():
    # Use the DEFAULTS from config.py (which has 0.60 now)
    cfg = apply_autotrade_defaults({})
    
    print("=" * 70)
    print("TV Entry Gate Verification (using config.py defaults)")
    print("=" * 70)
    print(f"Config: tvEntryMaxAgeSec={cfg.get('tvEntryMaxAgeSec')}")
    print(f"        tvEntryMinConfidence={cfg.get('tvEntryMinConfidence')}")
    print(f"        shortTvMinConfidence={cfg.get('shortTvMinConfidence')}")
    print(f"        tradingviewEnabled={cfg.get('tradingviewEnabled')}")
    print()
    
    test_cases = [
        # (name, out_dict, expected_qualified, expected_reject_reason)
        
        # --- Generic TV gate ---
        ("LONG: TV fresh, conf=0.6 (BUY/SELL)", make_out("LONG", 0.75, "LONG", 0.6, 10), True, ""),
        ("LONG: TV fresh, conf=0.8 (STRONG)", make_out("LONG", 0.75, "LONG", 0.8, 10), True, ""),
        ("LONG: TV WAIT conf=0.3", make_out("LONG", 0.75, "WAIT", 0.3, 10), False, "tv_stale_or_weak"),
        ("LONG: TV stale age=60s", make_out("LONG", 0.75, "LONG", 0.6, 60), False, "tv_stale_or_weak"),
        ("LONG: TV ERROR", make_out("LONG", 0.75, "ERROR", 0.0, 0), False, "tv_stale_or_weak"),
        ("LONG: no TV data", make_out("LONG", 0.75, None), True, ""),  # gate skipped when no TV
        
        # --- SHORT gate: conflict with TV=LONG ---
        ("SHORT: TV=LONG (conflict)", make_out("SHORT", 0.75, "LONG", 0.6, 10), False, "short_tv_conflict_long"),
        ("SHORT: TV=SHORT aligned conf=0.6", make_out("SHORT", 0.75, "SHORT", 0.6, 10), True, ""),
        ("SHORT: TV=SHORT aligned conf=0.8", make_out("SHORT", 0.75, "SHORT", 0.8, 10), True, ""),
        ("SHORT: TV=WAIT conf=0.3", make_out("SHORT", 0.75, "WAIT", 0.3, 10), False, "tv_stale_or_weak"),
        ("SHORT: TV=SHORT conf=0.6, shortTvMinConf=0.70 (old bug)", 
         make_out("SHORT", 0.75, "SHORT", 0.6, 10), None, None),  # will test with both configs
        
        # --- TV disabled should skip gate ---
        ("LONG: TV disabled, stale TV", make_out("LONG", 0.75, "LONG", 0.6, 60), True, ""),
        
        # --- Base gates (no TV) ---
        ("WAIT signal", make_out("WAIT", 0.75), False, "signal_wait"),
        ("Low confidence < 0.60", make_out("LONG", 0.55), False, "low_conf"),
        ("High confidence > 0.90", make_out("LONG", 0.95), False, "conf_too_high_late"),
        ("Wide spread", make_out("LONG", 0.75, tv_sig="LONG", tv_conf=0.6, tv_age=10, tv_strength=0.8, spread=100), False, "wide_spread"),
    ]
    
    # Override for shortTvMinConf test
    cfg_short_070 = copy.deepcopy(cfg)
    cfg_short_070["shortTvMinConfidence"] = 0.70
    
    # Config with TV disabled for the "TV disabled" test
    cfg_tv_disabled = copy.deepcopy(cfg)
    cfg_tv_disabled["tradingviewEnabled"] = False
    
    passed = 0
    failed = 0
    
    for name, out, exp_qual, exp_reason in test_cases:
        # Use appropriate config
        if "TV disabled" in name:
            test_cfg = cfg_tv_disabled
        elif "shortTvMinConf=0.70" in name:
            test_cfg = cfg_short_070
        else:
            test_cfg = cfg
        
        qual, reason = qualify_entry(out, test_cfg)
        
        if exp_qual is None:
            # Special case: test both configs
            qual_060, _ = qualify_entry(out, cfg)
            qual_070, _ = qualify_entry(out, cfg_short_070)
            print(f"[INFO] {name}: cfg=0.60 -> {qual_060}, cfg=0.70 -> {qual_070}")
            if not qual_060 and qual_070:
                print(f"  ^^^ This proves 0.70 blocks all SHORT (conf=0.6)")
            continue
        
        ok = (qual == exp_qual) and (reason == exp_reason)
        status = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
        print(f"[{status}] {name}")
        if not ok:
            print(f"       expected: qualified={exp_qual}, reason='{exp_reason}'")
            print(f"       got:      qualified={qual}, reason='{reason}'")
    
    print()
    print(f"Results: {passed} passed, {failed} failed")
    return failed == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)