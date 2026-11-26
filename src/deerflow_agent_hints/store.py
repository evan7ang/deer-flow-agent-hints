"""Per-session facts — the persisted state behind every hint.

Lead sessions live in the host database (the extension owns its own table, per
the DeerFlow extension convention) with a SQLite-file fallback for deployments
whose database backend is ``memory``. Subagent sessions never leave the process:
DeerFlow rebuilds a subagent's context for every execution, so those facts die
with it.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import BigInteger, Boolean, Column, Integer, MetaData, String, Table, Text, delete, select, update
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)

metadata = MetaData()

SESSIONS = Table(
    "extension_agent_hints_sessions",
    metadata,
    Column("session_id", String(256), primary_key=True),
    Column("kind", String(16), nullable=False),
    Column("thread_id", String(64)),
    Column("user_id", String(64)),
    Column("incarnation", String(32)),
    Column("parent_session_id", String(256)),
    Column("call_index", Integer, nullable=False, default=0),
    Column("epoch", Integer, nullable=False, default=0),
    Column("seeded_known", Boolean, nullable=False, default=False),
    Column("seeded", Boolean, nullable=False, default=False),
    Column("started", Boolean, nullable=False, default=False),
    Column("summary_available", Boolean, nullable=False, default=False),
    Column("summary_fp", String(64)),
    Column("head_ids", Text),
    Column("last_signature", Text),
    Column("last_response_end_ms", BigInteger),
    Column("last_accepted_at_ms", BigInteger),
    Column("active_run_id", String(64)),
    Column("last_task_id", String(64)),
    Column("paused", Boolean, nullable=False, default=False),
    Column("pending_rewrites", Integer, nullable=False, default=0),
    Column("quarantined_reason", Text),
    Column("stopped", Boolean, nullable=False, default=False),
    Column("model_name", String(256)),
    Column("base_url", Text),
    Column("family", String(16)),
    Column("updated_at_ms", BigInteger, nullable=False, default=0),
)


def now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass
class SessionFacts:
    """Everything one hint needs to know about a session."""

    session_id: str
    kind: str = "lead"  # lead | subagent
    thread_id: str = ""
    user_id: str = ""
    incarnation: str | None = None
    parent_session_id: str | None = None
    call_index: int = 0
    epoch: int = 0
    seeded_known: bool = False
    seeded: bool = False
    started: bool = False
    summary_available: bool = False
    summary_fp: str | None = None
    head_ids: list[str] | None = None
    last_signature: str | None = None
    last_response_end_ms: int | None = None
    last_accepted_at_ms: int | None = None
    active_run_id: str | None = None
    last_task_id: str | None = None
    paused: bool = False
    pending_rewrites: int = 0
    quarantined_reason: str | None = None
    stopped: bool = False
    model_name: str | None = None
    base_url: str | None = None
    family: str | None = None
    updated_at_ms: int = 0
    #: Process-local only: whether the session was seen by this process.
    local: bool = field(default=False, compare=False)

    @property
    def quarantined(self) -> bool:
        return bool(self.quarantined_reason)

    def errors(self, reason: str) -> None:
        self.quarantined_reason = reason
        self.updated_at_ms = now_ms()


def _row(facts: SessionFacts) -> dict[str, Any]:
    return {
        "session_id": facts.session_id,
        "kind": facts.kind,
        "thread_id": facts.thread_id,
        "user_id": facts.user_id,
        "incarnation": facts.incarnation,
        "parent_session_id": facts.parent_session_id,
        "call_index": int(facts.call_index),
        "epoch": int(facts.epoch),
        "seeded_known": bool(facts.seeded_known),
        "seeded": bool(facts.seeded),
        "started": bool(facts.started),
        "summary_available": bool(facts.summary_available),
        "summary_fp": facts.summary_fp,
        "head_ids": json.dumps(facts.head_ids) if facts.head_ids is not None else None,
        "last_signature": facts.last_signature,
        "last_response_end_ms": facts.last_response_end_ms,
        "last_accepted_at_ms": facts.last_accepted_at_ms,
        "active_run_id": facts.active_run_id,
        "last_task_id": facts.last_task_id,
        "paused": bool(facts.paused),
        "pending_rewrites": int(facts.pending_rewrites),
        "quarantined_reason": facts.quarantined_reason,
        "stopped": bool(facts.stopped),
        "model_name": facts.model_name,
        "base_url": facts.base_url,
        "family": facts.family,
        "updated_at_ms": int(facts.updated_at_ms or now_ms()),
    }


def _facts(row: Any) -> SessionFacts:
    data = dict(row._mapping)
    head = data.get("head_ids")
    try:
        head_ids = json.loads(head) if isinstance(head, str) and head else None
    except (TypeError, ValueError):
        head_ids = None
    return SessionFacts(
        session_id=data["session_id"],
        kind=data.get("kind") or "lead",
        thread_id=data.get("thread_id") or "",
        user_id=data.get("user_id") or "",
        incarnation=data.get("incarnation"),
        parent_session_id=data.get("parent_session_id"),
        call_index=int(data.get("call_index") or 0),
        epoch=int(data.get("epoch") or 0),
        seeded_known=bool(data.get("seeded_known")),
        seeded=bool(data.get("seeded")),
        started=bool(data.get("started")),
        summary_available=bool(data.get("summary_available")),
        summary_fp=data.get("summary_fp"),
        head_ids=head_ids,
        last_signature=data.get("last_signature"),
        last_response_end_ms=data.get("last_response_end_ms"),
        last_accepted_at_ms=data.get("last_accepted_at_ms"),
        active_run_id=data.get("active_run_id"),
        last_task_id=data.get("last_task_id"),
        paused=bool(data.get("paused")),
        pending_rewrites=int(data.get("pending_rewrites") or 0),
        quarantined_reason=data.get("quarantined_reason"),
        stopped=bool(data.get("stopped")),
        model_name=data.get("model_name"),
        base_url=data.get("base_url"),
        family=data.get("family"),
        updated_at_ms=int(data.get("updated_at_ms") or 0),
        local=True,
    )


class SessionStore:
    """Durable session facts (host database, or an owned SQLite file)."""

    def __init__(self, session_factory: Any | None, fallback_path: str) -> None:
        self._session_factory = session_factory
        self._fallback_path = fallback_path
        self._engine: Any | None = None
        self._owns_engine = False
        self._ready = False

    def bind(self, session_factory: Any | None) -> None:
        """Attach the host session factory (called once the Gateway is ready)."""
        if session_factory is not None and self._session_factory is None:
            self._session_factory = session_factory

    async def start(self) -> None:
        if self._session_factory is None:
            from pathlib import Path

            from sqlalchemy.ext.asyncio import create_async_engine

            path = Path(self._fallback_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            self._engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
            from sqlalchemy.ext.asyncio import async_sessionmaker

            self._session_factory = async_sessionmaker(self._engine, expire_on_commit=False)
            self._owns_engine = True
        await self._create_tables()
        self._ready = True

    async def _create_tables(self) -> None:
        assert self._session_factory is not None
        async with self._session_factory() as session:
            await session.run_sync(lambda sync: metadata.create_all(sync.connection()))
            await session.commit()

    async def stop(self) -> None:
        self._ready = False
        if self._owns_engine and self._engine is not None:
            await self._engine.dispose()
            self._engine = None

    @property
    def ready(self) -> bool:
        return self._ready and self._session_factory is not None

    async def fetch(self, session_id: str) -> SessionFacts | None:
        if not self.ready:
            return None
        async with self._session_factory() as session:  # type: ignore[misc]
            row = (await session.execute(select(SESSIONS).where(SESSIONS.c.session_id == session_id))).first()
        return _facts(row) if row is not None else None

    async def save(self, facts: SessionFacts) -> None:
        """Upsert a full row (owner path)."""
        if not self.ready:
            return
        facts.updated_at_ms = now_ms()
        values = _row(facts)
        async with self._session_factory() as session, session.begin():  # type: ignore[misc]
            result = await session.execute(update(SESSIONS).where(SESSIONS.c.session_id == facts.session_id).values(**values))
            if result.rowcount == 0:
                try:
                    await session.execute(SESSIONS.insert().values(**values))
                except IntegrityError:
                    await session.execute(update(SESSIONS).where(SESSIONS.c.session_id == facts.session_id).values(**values))

    async def set_flags(self, session_id: str, **flags: Any) -> None:
        """Update lifecycle flags without touching the owner's progress columns."""
        if not self.ready or not flags:
            return
        flags["updated_at_ms"] = now_ms()
        values = {key: (json.dumps(value) if key == "head_ids" and isinstance(value, list) else value) for key, value in flags.items()}
        async with self._session_factory() as session, session.begin():  # type: ignore[misc]
            await session.execute(update(SESSIONS).where(SESSIONS.c.session_id == session_id).values(**values))

    async def claim_stop(self, session_id: str) -> bool:
        """Flip ``stopped`` from false to true exactly once."""
        if not self.ready:
            return False
        async with self._session_factory() as session, session.begin():  # type: ignore[misc]
            result = await session.execute(
                update(SESSIONS)
                .where(SESSIONS.c.session_id == session_id, SESSIONS.c.stopped.is_(False))
                .values(stopped=True, updated_at_ms=now_ms())
            )
        return result.rowcount == 1

    async def reset_session(self, session_id: str, *, incarnation: str | None) -> None:
        """Forget progress after a thread id was deleted and recreated."""
        await self.set_flags(
            session_id,
            incarnation=incarnation,
            call_index=0,
            epoch=0,
            seeded=False,
            seeded_known=False,
            started=False,
            summary_fp=None,
            head_ids=None,
            last_signature=None,
            last_response_end_ms=None,
            last_accepted_at_ms=None,
            active_run_id=None,
            last_task_id=None,
            paused=False,
            pending_rewrites=0,
            quarantined_reason=None,
            stopped=False,
        )

    async def latest_accepted_other(self, *, user_id: str, exclude: str) -> int | None:
        if not self.ready:
            return None
        async with self._session_factory() as session:  # type: ignore[misc]
            rows = (
                (
                    await session.execute(
                        select(SESSIONS.c.last_accepted_at_ms).where(
                            SESSIONS.c.kind == "lead",
                            SESSIONS.c.user_id == user_id,
                            SESSIONS.c.session_id != exclude,
                        )
                    )
                )
                .scalars()
                .all()
            )
        values = [int(value) for value in rows if isinstance(value, int)]
        return max(values) if values else None

    async def idle_candidates(self, *, user_id: str, before_ms: int, exclude: str, limit: int = 20) -> list[SessionFacts]:
        if not self.ready:
            return []
        async with self._session_factory() as session:  # type: ignore[misc]
            rows = (
                await session.execute(
                    select(SESSIONS)
                    .where(
                        SESSIONS.c.kind == "lead",
                        SESSIONS.c.user_id == user_id,
                        SESSIONS.c.session_id != exclude,
                        SESSIONS.c.paused.is_(False),
                        SESSIONS.c.stopped.is_(False),
                        SESSIONS.c.active_run_id.is_(None),
                        SESSIONS.c.quarantined_reason.is_(None),
                        SESSIONS.c.last_accepted_at_ms.is_not(None),
                        SESSIONS.c.last_accepted_at_ms < before_ms,
                    )
                    .limit(limit)
                )
            ).all()
        return [_facts(row) for row in rows]

    async def tracked_lead(self, limit: int = 200) -> list[SessionFacts]:
        if not self.ready:
            return []
        async with self._session_factory() as session:  # type: ignore[misc]
            rows = (
                await session.execute(
                    select(SESSIONS)
                    .where(
                        SESSIONS.c.kind == "lead",
                        SESSIONS.c.stopped.is_(False),
                        SESSIONS.c.incarnation.is_not(None),
                    )
                    .limit(limit)
                )
            ).all()
        return [_facts(row) for row in rows]

    async def forget(self, session_id: str) -> None:
        if not self.ready:
            return
        async with self._session_factory() as session, session.begin():  # type: ignore[misc]
            await session.execute(delete(SESSIONS).where(SESSIONS.c.session_id == session_id))
