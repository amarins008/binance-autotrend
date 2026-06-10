from dataclasses import dataclass, field
from typing import Any, Literal

Side = Literal["LONG", "SHORT", "WAIT"]


@dataclass
class GateStep:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class ConfluenceResult:
    signal: str
    confidence: float
    long_score: int
    short_score: int
    notes: list[str] = field(default_factory=list)


@dataclass
class EntryPlan:
    approved: bool
    skip_code: str = ""
    skip_message: str = ""
    signal: str = "WAIT"
    confidence: float = 0.0
    trade_usdt: float = 0.0
    eff_tp_pct: float = 0.0
    eff_sl_pct: float = 0.0
    eff_leverage: int = 5
    tpsl_meta: dict = field(default_factory=dict)
    pipeline: list[dict] = field(default_factory=dict)
    extra: dict = field(default_factory=dict)
