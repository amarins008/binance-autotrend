"""
Hermes backend entrypoint — forces SelectorEventLoop BEFORE uvicorn creates
its event loop, so we avoid the ProactorEventLoop OSError(64) crash that
hammers the /health endpoint on Windows.

Use this instead of `python -m uvicorn main:app`:
    python run_backend.py
"""
from __future__ import annotations

import asyncio
import os
import sys

import uvicorn

# Apply SelectorEventLoop on Windows.
# WindowsSelectorEventLoopPolicy is deprecated in Python 3.14+ and will be
# removed in 3.16.  The recommended replacement is to create and set a
# SelectorEventLoop directly, which works on all supported Python versions.
if sys.platform == "win32":
    try:
        # Python 3.14+: set_event_loop_policy is deprecated; create loop directly.
        _selector_loop = asyncio.SelectorEventLoop()
        asyncio.set_event_loop(_selector_loop)
    except Exception as exc:  # pragma: no cover - defensive
        print(f"[run_backend] WARN: could not set SelectorEventLoop: {exc}", file=sys.stderr)


# Default to 0.0.0.0 for LAN / Tailscale access. The Start bat ensures a
# firewall rule exists before launching. Override with BACKEND_HOST env var.
HOST = os.getenv("BACKEND_HOST", "0.0.0.0")
PORT = int(os.getenv("BACKEND_PORT", "8020"))


def main() -> None:
    import time
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"[{ts}] [run_backend] Starting backend pid={os.getpid()} host={HOST} port={PORT}", flush=True)
    uvicorn.run(
        "main:app",
        host=HOST,
        port=PORT,
        log_level=os.getenv("BACKEND_LOG_LEVEL", "info"),
        # Force selector loop even if main:app was already imported elsewhere
        loop="asyncio",
        # Limit to a single worker on Windows.  Without this, uvicorn defaults
        # to cpu_count() workers — each forked worker inherits the ProactorEventLoop
        # (the system default) instead of the SelectorPolicy set above, and
        # crashes with WinError 64 ("network name no longer available") the moment
        # /health is hammered by the dashboard or launcher watchdog.
        workers=1,
    )


if __name__ == "__main__":
    main()
