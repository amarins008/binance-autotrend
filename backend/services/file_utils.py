"""Low-level file I/O utilities used across the backend."""

from __future__ import annotations

import os
from pathlib import Path


def _atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    """Write text to ``path`` atomically so a crash mid-write never leaves a
    truncated/corrupt file.

    Writes to a sibling temp file then ``os.replace`` (atomic on Windows and
    POSIX), guaranteeing readers either see the old or the new full file.
    Falls back to a direct write only if the temp path itself cannot be created.
    """
    parent = path.parent
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        # Parent may already exist or be unwritable; let the write below fail loudly.
        pass
    tmp = path.with_name(path.name + ".tmp")
    try:
        tmp.write_text(text, encoding=encoding)
        os.replace(tmp, path)
    except Exception:
        # Last-resort cleanup so we don't leave a stale .tmp behind.
        try:
            tmp.unlink()
        except Exception:
            pass
        raise
