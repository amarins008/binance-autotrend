"""Structured JSON logging for the trading system.

Provides a single configured logger that emits machine-parseable JSON
lines (timestamp / level / module / function / message + optional data)
instead of free-form text.  This makes log scraping, alerting and
debugging deterministic.

Usage::

    from logger import get_logger

    log = get_logger(__name__)
    log.info("trade placed", extra={"data": {"symbol": "BTCUSDT", "side": "LONG"}})

The ``extra["data"]`` key is special-cased and serialised into the
``data`` field of the JSON line.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

_LOGGERS: dict[str, logging.Logger] = {}
_CONFIGURED = False

_LOG_LEVEL_NAMES = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


class StructuredFormatter(logging.Formatter):
    """Format a LogRecord as a single JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "func": record.funcName or "",
            "line": record.lineno,
            "msg": record.getMessage(),
        }
        data = getattr(record, "data", None)
        if data is not None:
            if isinstance(data, dict):
                log_data["data"] = data
            else:
                log_data["data"] = {"value": data}
        if record.exc_info and record.exc_info[0] is not None:
            log_data["exc"] = self.formatException(record.exc_info)
        if hasattr(record, "symbol"):
            log_data["symbol"] = record.symbol
        return json.dumps(log_data, ensure_ascii=False, default=str)


def configure_logging(
    level: str | int | None = None,
    *,
    stream=None,
) -> None:
    """Idempotently configure the root trading logger.

    Args:
        level: log level name or int.  Defaults to env ``LOG_LEVEL`` then INFO.
        stream: optional output stream (defaults to stderr).
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    if level is None:
        raw = os.getenv("LOG_LEVEL", "INFO").upper()
        level = raw if raw in _LOG_LEVEL_NAMES else logging.INFO

    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(StructuredFormatter())

    root = logging.getLogger("trading")
    root.setLevel(level)
    root.handlers = []
    root.addHandler(handler)
    root.propagate = False

    _CONFIGURED = True


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a structured logger.

    Args:
        name: usually ``__name__`` of the caller module.  All loggers are
            children of the ``trading`` root logger and share its config.
    """
    configure_logging()
    if not name or name == "__main__":
        name = __name__
    logger = _LOGGERS.get(name)
    if logger is None:
        logger = logging.getLogger(f"trading.{name}")
        _LOGGERS[name] = logger
    return logger


def log_exception(logger: logging.Logger, exc: BaseException, context: dict | None = None) -> None:
    """Log an exception with optional context data at ERROR level."""
    try:
        logger.error(
            str(exc) or type(exc).__name__,
            extra={"data": context or {"type": type(exc).__name__}},
            exc_info=exc,
        )
    except Exception:
        # Never let logging itself crash the caller.
        pass
