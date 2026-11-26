"""Content-free JSONL diagnostics.

Records what was emitted and how it was classified. Never records endpoints,
credentials, or message content (protocol §9.7 / §10), so a trace file can be
attached to a bug report.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any


class Trace:
    def __init__(self, enabled: bool = False, path: str | None = None) -> None:
        self._enabled = enabled
        self._path = Path(path) if path else None
        self._lock = threading.Lock()
        if self._enabled and self._path is not None:
            try:
                self._path.parent.mkdir(parents=True, exist_ok=True)
            except OSError:
                self._enabled = False

    @property
    def enabled(self) -> bool:
        return self._enabled and self._path is not None

    def emit(self, event: dict[str, Any]) -> None:
        if not self.enabled:
            return
        record = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()), **event}
        line = json.dumps(record, ensure_ascii=False, default=str)
        try:
            with self._lock:
                assert self._path is not None
                with self._path.open("a", encoding="utf-8") as handle:
                    handle.write(line + "\n")
        except OSError:
            pass
