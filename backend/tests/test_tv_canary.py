"""TV silent-death canary tests — the active heartbeat that catches
'health says healthy but every real fetch crashes' (the 09-28
_next_global_slot_ts AttributeError ran entries TV-less for days)."""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from trading import tradingview_mcp as tvm


@pytest.fixture(autouse=True)
def _reset_canary_state(monkeypatch):
    tvm._CANARY_STATE.update({
        "fails": 0, "ok": 0, "down": False,
        "last_ok_ts": 0.0, "last_fail_ts": 0.0, "last_error": "",
        "symbol": "", "just_went_down": False, "just_recovered": False,
    })
    from services import app_state
    app_state.AUTO_TRADE.pop("tvCanaryDown", None)
    app_state.AUTO_TRADE.pop("tvCanary", None)
    app_state.AUTO_TRADE.pop("tvNoDataEntryStreak", None)
    app_state.AUTO_TRADE.pop("tvNoDataAlerted", None)
    yield
    app_state.AUTO_TRADE.pop("tvCanaryDown", None)
    app_state.AUTO_TRADE.pop("tvCanary", None)
    app_state.AUTO_TRADE.pop("tvNoDataEntryStreak", None)
    app_state.AUTO_TRADE.pop("tvNoDataAlerted", None)


def _fake_client(behavior: str):
    client = MagicMock()
    if behavior == "ok":
        res = MagicMock()
        res.signal = MagicMock()
        res.signal.value = "BUY"
        client.get_signal.return_value = res
    elif behavior == "error_signal":
        res = MagicMock()
        res.signal = MagicMock()
        res.signal.value = "ERROR"
        client.get_signal.return_value = res
    elif behavior == "none":
        client.get_signal.return_value = None
    elif behavior == "raise":
        client.get_signal.side_effect = AttributeError("'_next_global_slot_ts' style crash")
    return client


def _patch_client(monkeypatch, behavior: str):
    client = _fake_client(behavior)
    monkeypatch.setattr(tvm, "get_tv_client", lambda cfg: client)
    return client


def test_canary_ok_resets_fails(monkeypatch):
    _patch_client(monkeypatch, "ok")
    tvm._CANARY_STATE["fails"] = 2
    state = tvm.run_tv_canary({})
    assert state["fails"] == 0
    assert state["down"] is False
    assert state["ok"] == 1
    assert state["last_ok_ts"] > 0


def test_canary_counts_failures_until_down(monkeypatch):
    _patch_client(monkeypatch, "raise")
    cfg = {"tvCanaryMaxFails": 3}
    s1 = tvm.run_tv_canary(cfg)
    assert s1["fails"] == 1 and s1["down"] is False and s1["just_went_down"] is False
    s2 = tvm.run_tv_canary(cfg)
    assert s2["fails"] == 2 and s2["down"] is False
    s3 = tvm.run_tv_canary(cfg)
    assert s3["fails"] == 3 and s3["down"] is True and s3["just_went_down"] is True


def _make_inputs(cfg_overrides=None, tv=None, conf=0.92):
    """Full EntryInputs that reaches the TV gate (passes all earlier gates)."""
    from trading.pipeline import EntryInputs
    cfg = {
        "maxSpreadBps": 18.0,
        "maxEntryConfidence": 0.96,
        "tvUnavailableMinConf": 0.88,
        "tvWaitMinConf": 0.99,
        "tvShortWaitMinConf": 0.99,
        "tvCanaryBlockEntries": True,
    }
    cfg.update(cfg_overrides or {})
    return EntryInputs(
        cfg=cfg,
        intel={"tv": tv if tv is not None else {"signal": "", "status": "unavailable", "age": 9999, "blocked": False, "strength": 0.0}, "precision": {}, "candles": {}},
        regime={},
        signal="LONG",
        confidence=conf,
        spread_bps=5.0,
        slippage_bps=1.0,
        mark=100.0,
        ex={},
        htf={},
        candle_ctx={},
        adaptive_min_conf=0.72,
        trade_usdt=50.0,
        _applied_regime_sizing=True,
        _applied_session_sizing=True,
    )


def test_canary_down_blocks_pipeline_tvless_entry(monkeypatch):
    """When the canary says down, a TV-less entry must be refused."""
    _patch_client(monkeypatch, "raise")
    for _ in range(3):
        tvm.run_tv_canary({"tvCanaryMaxFails": 3})
    assert tvm.tv_canary_down() is True

    from trading.pipeline import evaluate_entry_plan
    plan = evaluate_entry_plan(_make_inputs())
    assert plan.approved is False
    assert plan.skip_code == "tv_canary_down"


def test_canary_recovered_resets_down(monkeypatch):
    _patch_client(monkeypatch, "raise")
    for _ in range(3):
        tvm.run_tv_canary({"tvCanaryMaxFails": 3})
    assert tvm.tv_canary_down() is True

    _patch_client(monkeypatch, "ok")
    state = tvm.run_tv_canary({})
    assert state["down"] is False
    assert state["just_recovered"] is True
    assert tvm.tv_canary_down() is False


def test_canary_error_signal_counts_as_fail(monkeypatch):
    _patch_client(monkeypatch, "error_signal")
    state = tvm.run_tv_canary({"tvCanaryMaxFails": 1})
    assert state["fails"] == 1
    assert state["down"] is True


def test_canary_none_result_counts_as_fail(monkeypatch):
    _patch_client(monkeypatch, "none")
    state = tvm.run_tv_canary({"tvCanaryMaxFails": 2})
    assert state["fails"] == 1 and state["down"] is False


def test_canary_mirrors_to_app_state(monkeypatch):
    from services import app_state
    _patch_client(monkeypatch, "raise")
    for _ in range(3):
        tvm.run_tv_canary({"tvCanaryMaxFails": 3})
    assert app_state.AUTO_TRADE.get("tvCanaryDown") is True
    mirrored = app_state.AUTO_TRADE.get("tvCanary")
    assert mirrored["down"] is True
    assert "crash" in mirrored["lastError"]


def test_pipeline_tvless_streak_increments_and_resets(monkeypatch):
    from services import app_state
    from trading.pipeline import evaluate_entry_plan

    # TV unavailable but canary fine + high conf → passes, streak grows
    for _ in range(2):
        evaluate_entry_plan(_make_inputs())
    # (later gates may still reject the synthetic entry — the streak is what
    # this test observes, and the TV gate already ran)
    assert app_state.AUTO_TRADE.get("tvNoDataEntryStreak") == 2

    # A TV-aligned entry resets the streak
    evaluate_entry_plan(_make_inputs(tv={"signal": "LONG", "status": "ok", "age": 10, "blocked": False, "strength": 0.8}))
    assert app_state.AUTO_TRADE.get("tvNoDataEntryStreak") == 0


def test_pipeline_tv_conflict_counts_as_alive(monkeypatch):
    """TV answered (conflict) = TV alive → streak must not grow."""
    from services import app_state
    from trading.pipeline import evaluate_entry_plan

    app_state.AUTO_TRADE["tvNoDataEntryStreak"] = 2
    evaluate_entry_plan(_make_inputs(tv={"signal": "SHORT", "status": "ok", "age": 10, "blocked": False, "strength": 0.3}))
    assert app_state.AUTO_TRADE.get("tvNoDataEntryStreak") == 0


def test_pipeline_tvless_low_conf_still_blocked_normally(monkeypatch):
    """Canary up + TV-less + conf below unavailable floor → normal block."""
    from trading.pipeline import evaluate_entry_plan
    plan = evaluate_entry_plan(_make_inputs(conf=0.85))
    assert plan.approved is False
    assert plan.skip_code == "tv_unavailable_low_conf"
