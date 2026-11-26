"""Read-only access to DeerFlow's ``threads_meta`` rows.

There is no sanctioned extension API for host tables; ``session_factory`` is the
documented "legacy" escape hatch. All coupling lives here, and every failure
degrades to "unknown" (no ``stop``) instead of breaking a run.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import bindparam, text

logger = logging.getLogger(__name__)

_ARCHIVED_KEY = "deerflow_archived"
_BRANCH_PARENT_KEY = "branch_parent_thread_id"

_QUERY = text("SELECT thread_id, user_id, incarnation, status, metadata_json FROM threads_meta WHERE thread_id IN :ids").bindparams(
    bindparam("ids", expanding=True)
)


@dataclass(frozen=True)
class ThreadMetaView:
    thread_id: str
    user_id: str | None = None
    incarnation: str | None = None
    status: str | None = None
    archived: bool = False
    branch_parent_thread_id: str | None = None


class ThreadMetaReader:
    def __init__(self, session_factory: Any | None) -> None:
        self._session_factory = session_factory
        self._warned = False

    def bind(self, session_factory: Any | None) -> None:
        if session_factory is not None and self._session_factory is None:
            self._session_factory = session_factory

    @property
    def available(self) -> bool:
        return self._session_factory is not None

    async def read(self, thread_ids: list[str]) -> dict[str, ThreadMetaView]:
        if self._session_factory is None or not thread_ids:
            return {}
        views: dict[str, ThreadMetaView] = {}
        for start in range(0, len(thread_ids), 200):
            chunk = thread_ids[start : start + 200]
            try:
                async with self._session_factory() as session:  # type: ignore[misc]
                    rows = (await session.execute(_QUERY, {"ids": chunk})).all()
            except Exception:
                if not self._warned:
                    self._warned = True
                    logger.warning(
                        "agent-hints: threads_meta is not readable through session_factory; archive/delete stop detection is disabled",
                        exc_info=True,
                    )
                return views
            for row in rows:
                mapping = row._mapping
                metadata = mapping["metadata_json"]
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata) if metadata else {}
                    except ValueError:
                        metadata = {}
                if not isinstance(metadata, dict):
                    metadata = {}
                views[str(mapping["thread_id"])] = ThreadMetaView(
                    thread_id=str(mapping["thread_id"]),
                    user_id=mapping["user_id"],
                    incarnation=mapping["incarnation"],
                    status=mapping["status"],
                    archived=metadata.get(_ARCHIVED_KEY) is True,
                    branch_parent_thread_id=metadata.get(_BRANCH_PARENT_KEY),
                )
        return views
