"""Bounded in-memory feed of emitted hints and manage outcomes.

The live page (``GET /api/extension/agent-hints/live``) polls it, so an operator
can watch the actual fields during an interactive session without attaching a
proxy. Content-free by construction: the events are exactly what the plugin
also writes to its JSONL trace.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any

DEFAULT_MAXLEN = 500


class EventFeed:
    def __init__(self, maxlen: int = DEFAULT_MAXLEN) -> None:
        self._events: deque[dict[str, Any]] = deque(maxlen=maxlen)
        self._seq = 0
        self._lock = threading.Lock()

    def append(self, event: dict[str, Any]) -> int:
        with self._lock:
            self._seq += 1
            record = {"seq": self._seq, "at_ms": int(time.time() * 1000), **event}
            self._events.append(record)
            return self._seq

    def since(self, seq: int = 0, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock:
            items = [item for item in self._events if item["seq"] > seq]
        return items[-limit:]

    @property
    def last_seq(self) -> int:
        with self._lock:
            return self._seq
