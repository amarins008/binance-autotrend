"""Shared market-context object — single source of truth for regime,
vol targeting, dominant pattern, and effective late-entry bound.

Previously these values were scattered across consumers:
  - ``regime`` recomputed at every read site via ``detect_market_regime``
  - ``volTargetPct`` only in cfg (config.py) — ratio never derived
  - ``lateEntryMaxBbPctB`` volatility-aware knobs in cfg but not sourced
    from realized vol until now
  - ``patternTags`` survived only in ``intel["candles"]["tags"]`` and was
    re-extracted at downstream sites (confluence, learning)

This module derives all of them once from the populated intel result and
embeds the snapshot into ``intel["marketContext"]``. Agents consume via
``get(intel)`` or the convenience accessors (``regime()``, ``vol_ratio()``,
``effective_bb()``).
"""

from __future__ import annotations

import time
from typing import Any


def _safe_float(d: dict | None, key: str, default: float) -> float:
    try:
        v = (d or {}).get(key, default)
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _coerce_tags(raw: Any) -> list[str]:
    """Normalize the candle-tags field. Schema drift across versions:
    sometimes list[str], sometimes dict[str, score], sometimes missing."""
    if isinstance(raw, dict):
        return [str(k) for k in raw.keys()][:8]
    if isinstance(raw, (list, tuple)):
        out: list[str] = []
        for t in raw:
            if isinstance(t, str):
                if t:
                    out.append(t)
            elif isinstance(t, dict):
                # Score-tagged form: pick the key with highest weight
                if t:
                    k = max(t.items(), key=lambda kv: kv[1] if isinstance(kv[1], (int, float)) else 0)
                    out.append(str(k[0]))
            if len(out) >= 8:
                break
        return out
    return []


def build(intel: dict, cfg: dict | None = None) -> dict:
    """Build ``marketContext`` from a fully-populated intel result.

    Reads:
      - ``intel["precision"]["atrPct"]``            → realized vol
      - ``intel["precision"]["bbPctB"]``             → current BB position
      - ``intel["candles"]["tags"]``                → pattern tags
      - ``cfg["volTargetPct"]``                     → target volatility
      - ``cfg["lateEntryMaxBbPctB*"]`` (Base/Min/Max/Multiplier) →
        vol-scaled effective bound
      - ``detect_market_regime(intel)``             → regime classification
    """
    cfg = cfg or {}
    precision = intel.get("precision") if isinstance(intel.get("precision"), dict) else {}
    candles = intel.get("candles") if isinstance(intel.get("candles"), dict) else {}

    atr_pct = _safe_float(precision, "atrPct", 0.0)
    bb_pct_b = _safe_float(precision, "bbPctB", 0.5)

    vol_target_pct = _safe_float(cfg, "volTargetPct", 0.22)
    if vol_target_pct <= 0:
        vol_target_pct = 0.22
    vol_target_ratio = atr_pct / vol_target_pct

    # Volatility-aware late-entry bound. The cfg knobs were vestigial
    # (defined but never sourced from realized vol) — this is where they
    # actually fire.
    base_bb = _safe_float(cfg, "lateEntryMaxBbPctB", 0.90)
    bb_min = _safe_float(cfg, "lateEntryMaxBbPctBVolMin", 0.85)
    bb_max = _safe_float(cfg, "lateEntryMaxBbPctBVolMax", 0.98)
    bb_mult = _safe_float(cfg, "lateEntryMaxBbPctBVolMultiplier", 0.04)
    if vol_target_ratio >= 1.0:
        # Realized vol ≥ target → widen late-entry band (toward bb_max)
        delta = bb_mult * (vol_target_ratio - 1.0) * 10.0
    else:
        # Realized vol < target → tighten band (toward bb_min)
        delta = -bb_mult * (1.0 - vol_target_ratio) * 10.0
    effective_bb = max(bb_min, min(bb_max, base_bb + delta))

    # Pattern detection — most-recent-first wins
    raw_tags = candles.get("tags") if isinstance(candles, dict) else None
    tag_list = _coerce_tags(raw_tags)
    dominant_pattern = tag_list[0] if tag_list else ""

    # Regime classification (existing helper — was previously recomputed
    # at every consumer site; now derived once and embedded).
    try:
        from trading.regime import detect_market_regime
        regime_detail = detect_market_regime(intel)
    except Exception:
        regime_detail = {
            "name": "UNKNOWN",
            "confidenceBoost": 0.0,
            "sizeMultiplier": 1.0,
            "strictness": "normal",
        }
    if not isinstance(regime_detail, dict):
        regime_detail = {"name": "UNKNOWN", "sizeMultiplier": 1.0}

    return {
        "asOf": int(time.time()),
        "regime": str(regime_detail.get("name", "UNKNOWN") or "UNKNOWN"),
        "regimeDetail": regime_detail,
        "realizedVol": round(atr_pct, 4),
        "volTargetPct": round(vol_target_pct, 4),
        "volTargetRatio": round(vol_target_ratio, 3),
        "dominantPattern": dominant_pattern,
        "patternTags": tag_list,
        "baseLateEntryMaxBbPctB": round(base_bb, 4),
        "effectiveLateEntryMaxBbPctB": round(effective_bb, 4),
        "bbPctB": round(bb_pct_b, 3),
    }


def get(intel: dict | None, cfg: dict | None = None) -> dict:
    """Safe accessor. Returns ``intel["marketContext"]`` if present,
    builds on-demand for partial / stale intel, else returns a conservative
    fallback. Never raises — all paths return a dict.
    """
    if isinstance(intel, dict):
        mc = intel.get("marketContext")
        if isinstance(mc, dict) and mc:
            return mc
        return build(intel, cfg)
    return {
        "asOf": int(time.time()),
        "regime": "UNKNOWN",
        "regimeDetail": {"name": "UNKNOWN", "sizeMultiplier": 1.0, "strictness": "normal"},
        "realizedVol": 0.0,
        "volTargetPct": _safe_float(cfg, "volTargetPct", 0.22),
        "volTargetRatio": 1.0,
        "dominantPattern": "",
        "patternTags": [],
        "baseLateEntryMaxBbPctB": _safe_float(cfg, "lateEntryMaxBbPctB", 0.90),
        "effectiveLateEntryMaxBbPctB": _safe_float(cfg, "lateEntryMaxBbPctB", 0.90),
        "bbPctB": 0.5,
    }


# --- Convenience accessors (read in consumer code) ----------------------

def regime_name(mc: dict | None) -> str:
    if not isinstance(mc, dict):
        return "UNKNOWN"
    return str(mc.get("regime", "UNKNOWN") or "UNKNOWN")


def vol_target_ratio(mc: dict | None) -> float:
    if not isinstance(mc, dict):
        return 1.0
    try:
        return float(mc.get("volTargetRatio", 1.0) or 1.0)
    except Exception:
        return 1.0


def effective_bb(mc: dict | None, default: float = 0.90) -> float:
    if not isinstance(mc, dict):
        return default
    try:
        v = mc.get("effectiveLateEntryMaxBbPctB", default)
        return float(v) if v is not None else default
    except Exception:
        return default


def dominant_pattern(mc: dict | None) -> str:
    if not isinstance(mc, dict):
        return ""
    return str(mc.get("dominantPattern", "") or "")


def pattern_tags(mc: dict | None) -> list[str]:
    if not isinstance(mc, dict):
        return []
    tags = mc.get("patternTags", [])
    return list(tags) if isinstance(tags, list) else []
