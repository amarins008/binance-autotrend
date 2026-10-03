"""Shared TradingView signal types.

`tradingview_mcp` and `tradingview_scanner` both produce these objects. Keeping
them in a leaf module breaks the import cycle between those two files, which
otherwise forces one of them to re-declare the types behind a try/except and
silently end up with two distinct `TVSignal` classes.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict


class TVSignal(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    WAIT = "WAIT"
    ERROR = "ERROR"


@dataclass
class TVSignalResult:
    signal: TVSignal
    confidence: float
    timestamp: float
    source: str = "tradingview-ta"
    metadata: Dict[str, Any] = None
    _is_stale: bool = False

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}