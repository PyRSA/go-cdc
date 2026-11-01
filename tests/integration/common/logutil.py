"""Timestamped progress logging for integration harness stdout/stderr (CI-friendly)."""

from __future__ import annotations

import sys
from datetime import datetime
from typing import Any, TextIO


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(*args: Any, file: TextIO | None = None, **kwargs: Any) -> None:
    """Print with ``[YYYY-MM-DD HH:MM:SS]`` prefix; always flush."""
    kwargs.setdefault("flush", True)
    out = file if file is not None else sys.stdout
    print(f"[{_ts()}]", *args, file=out, **kwargs)
