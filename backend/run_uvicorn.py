import os
import uvicorn
import asyncio
import sys

# Run uvicorn directly. Honors BACKEND_HOST / BACKEND_PORT for external access
# (e.g. Tailscale). Defaults keep the original local-only behavior.
HOST = os.getenv("BACKEND_HOST", "127.0.0.1")
PORT = int(os.getenv("BACKEND_PORT", "8020"))

if __name__ == "__main__":
    uvicorn.run("main:app", host=HOST, port=PORT, log_level="info")
