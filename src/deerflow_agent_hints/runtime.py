"""The session fact machine: identity, call ordinals, rewrites, priorities.

Two middleware hooks feed it:

* :class:`LogicalHintMiddleware` (``MODEL_LOGICAL``) opens one logical call per
  decision, outside the host's retry loop.
* :class:`PhysicalHintMiddleware` (``MODEL_PHYSICAL``) sees the final request and
  freezes the derived values into the ``X-Agent-Hint`` header.

Lifecycle contributors feed it task boundaries and compaction events. Every
failure here is fail-open: the model request itself must never break because of
a hint (protocol §0, §9.1).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from deerflow_agent_hints.config import PluginConfig, model_allowed
from deerflow_agent_hints.control import ControlService, endpoint_from_model, model_family
from deerflow_agent_hints.policy import (
    MIN_CACHE_TOKENS,
    derive_ttl_s,
    estimate_tokens,
    head_ids,
    head_rewritten,
    logical_message_count,
    model_identifier,
    prefix_signature,
    priority_for,
    session_id_for,
    signature_stable,
)
from deerflow_agent_hints.store import SessionFacts, SessionStore, now_ms
from deerflow_agent_hints.threads import ThreadMetaReader
from deerflow_agent_hints.trace import Trace
from deerflow_agent_hints.wire import ordinary_hint

logger = logging.getLogger(__name__)

PRE_EXISTING_IDS_KEY = "__deerflow_pre_run_message_ids"
THREAD_INCARNATION_KEY = "thread_incarnation"

_USER_LIKE_ORIGINS = frozenset({"im_channel", "github"})


@dataclass(frozen=True)
class Identity:
    session_id: str
    kind: str
    thread_id: str
    run_id: str
    task_id: str
    agent_name: str | None
    user_id: str
    incarnation: str | None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class LogicalCall:
    """Frozen values for one logical model call (stable across retries)."""

    session_id: str
    kind: str
    state: str
    call_index: int
    epoch: int
    ttl_s: int
    priority_pair: tuple[int, str] | None
    after_rewrite: bool
    head_changed: bool
    prev_signature: str | None
    parent_session_id: str | None
    head_ids: list[str] | None
    suppress: bool = False
    signature: str | None = None
    msg_offset: int | None = None
    cache_declared: bool = False
    traced: bool = False


def _fingerprint(value: Any) -> str:
    text = "" if value is None else str(value)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def _pre_existing_ids(request: Any) -> frozenset[str]:
    runtime = getattr(request, "runtime", None)
    context = getattr(runtime, "context", None)
    if not isinstance(context, Mapping):
        return frozenset()
    raw = context.get(PRE_EXISTING_IDS_KEY)
    if not isinstance(raw, (frozenset, set, list, tuple)):
        return frozenset()
    return frozenset(str(item) for item in raw if item)


def _count_ai_messages(messages: Any) -> int:
    count = 0
    for message in messages or []:
        if str(getattr(message, "type", "") or "") == "ai":
            count += 1
    return count


def _error_fallback(response: Any) -> bool:
    """Whether a handler result is the host's error fallback, not a real answer."""
    messages = response if isinstance(response, list) else getattr(response, "result", None)
    if not isinstance(messages, list):
        messages = [response]
    for message in messages:
        kwargs = getattr(message, "additional_kwargs", None)
        if isinstance(kwargs, dict) and kwargs.get("deerflow_error_fallback"):
            return True
    return False


def _effective_user_id() -> str:
    try:
        from deerflow.runtime.user_context import get_effective_user_id

        value = get_effective_user_id()
        return str(value) if value else ""
    except Exception:  # pragma: no cover - host import or context missing
        return ""


def resolve_identity(request: Any) -> Identity | None:
    """Resolve the serving session from the task store the host seeds."""
    try:
        from deerflow_extension_api import TaskInfo, task_store_from_runtime
    except Exception:  # pragma: no cover - contract package missing
        return None
    runtime = getattr(request, "runtime", None)
    store = task_store_from_runtime(runtime)
    info = store.get(TaskInfo) if store is not None else None
    if info is None:
        # Only Gateway runs are attributed: no task identity, no hint.
        return None
    context = getattr(runtime, "context", None)
    context = context if isinstance(context, Mapping) else {}
    thread_id = str(info.thread_id or context.get("thread_id") or "")
    task_id = str(info.task_id or "")
    run_id = str(info.run_id or "")
    session_id = session_id_for(thread_id, run_id, task_id) if info.kind == "subagent" else thread_id
    if not session_id:
        return None
    user_id = _effective_user_id() or str(context.get("user_id") or "")
    incarnation = context.get(THREAD_INCARNATION_KEY)
    return Identity(
        session_id=session_id,
        kind=str(info.kind),
        thread_id=thread_id,
        run_id=run_id,
        task_id=task_id,
        agent_name=info.agent_name,
        user_id=user_id,
        incarnation=str(incarnation) if isinstance(incarnation, str) and incarnation else None,
        metadata=dict(info.metadata or {}),
    )


def _origin_user_like(identity: Identity) -> bool:
    """Whether a run was opened by a human waiting on the answer.

    ``deerflow_origin`` is absent for web/API/PAT runs ("interactive or
    legacy"), which count as user-facing. Scheduled runs only count when the
    operator triggered them manually; extension, batch-goal and MCP runs are
    self-driven.
    """
    origin = identity.metadata.get("deerflow_origin")
    kind = origin.get("kind") if isinstance(origin, Mapping) else None
    if kind is None:
        return True
    if kind in _USER_LIKE_ORIGINS:
        return True
    if kind == "schedule":
        return identity.metadata.get("scheduled_trigger") == "manual"
    return False


class PluginRuntime:
    """State machine shared by the hooks, the lifecycle contributors and the service."""

    def __init__(
        self,
        config: PluginConfig,
        store: SessionStore,
        control: ControlService,
        meta: ThreadMetaReader,
        trace: Trace,
    ) -> None:
        self.config = config
        self._store = store
        self._control = control
        self._meta = meta
        self._trace = trace
        self._locks: dict[str, asyncio.Lock] = {}
        self._cache: dict[str, SessionFacts] = {}
        self._subagents: dict[str, SessionFacts] = {}
        self._sub_lock = threading.Lock()
        self._tasks: set[asyncio.Task] = set()
        self._counters: dict[str, int] = {}
        self._counter_lock = threading.Lock()
        self._injection: str = "unknown"
        self._disabled_reason: str | None = None

    # -- counters / diagnostics ---------------------------------------------

    def count(self, key: str) -> None:
        with self._counter_lock:
            self._counters[key] = self._counters.get(key, 0) + 1

    def counters(self) -> dict[str, int]:
        with self._counter_lock:
            return dict(self._counters)

    @property
    def injection_status(self) -> str:
        return self._injection

    @property
    def disabled_reason(self) -> str | None:
        return self._disabled_reason

    @property
    def emitting(self) -> bool:
        return self.config.enabled and self._disabled_reason is None

    def set_injection_status(self, status: str) -> None:
        self._injection = status

    def suspend(self, reason: str) -> None:
        """Stop emitting everything (hints and manage traffic) with a visible reason."""
        self._disabled_reason = reason
        logger.error("agent-hints: emission suspended (%s)", reason)

    def remember_endpoint(self, session_id: str, request: Any) -> None:
        endpoint = endpoint_from_model(request.model)
        self._control.remember(session_id, endpoint)

    def _lock(self, session_id: str) -> asyncio.Lock:
        lock = self._locks.get(session_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[session_id] = lock
        return lock

    def schedule(self, coro: Any) -> None:
        try:
            task = asyncio.get_running_loop().create_task(coro)
        except RuntimeError:  # pragma: no cover - no loop (tests/tools)
            return
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # -- hook: MODEL_LOGICAL ------------------------------------------------

    async def begin_logical(self, request: Any) -> LogicalCall | None:
        if not self.emitting:
            return None
        try:
            identity = resolve_identity(request)
            if identity is None:
                return None
            family = model_family(request.model)
            if family is None:
                return None
            model_name = model_identifier(request.model)
            if not model_allowed(self.config, model_name):
                return None
            self.remember_endpoint(identity.session_id, request)
            if identity.kind == "subagent":
                return self._begin_subagent(request, identity, model_name, family)
            return await self._begin_lead(request, identity, model_name, family)
        except Exception:
            logger.exception("agent-hints: could not open a logical call; the request continues without a hint")
            return None

    async def _begin_lead(self, request: Any, identity: Identity, model_name: str, family: str) -> LogicalCall:
        async with self._lock(identity.session_id):
            facts = await self._load_lead(identity)
            if self.config.session_control.enabled:
                await self._control.wait_idle(identity.session_id, self.config.session_control.pending_op_await_ms)
            now = now_ms()
            self._learn_history(request, facts)
            old_epoch = facts.epoch
            after_rewrite = self._detect_rewrite(request, facts)
            current_head = head_ids(getattr(request, "messages", None))
            head_changed = head_rewritten(facts.head_ids, current_head)

            call_index = facts.call_index + 1
            state = "append" if (facts.started or facts.seeded) else "start"
            turn_opening = self._is_turn_opening(facts, identity)
            interval = None if facts.last_response_end_ms is None else now - facts.last_response_end_ms
            ttl_s = derive_ttl_s(interval)
            prev_signature = facts.last_signature

            if after_rewrite and self.config.session_control.enabled:
                await self._control.send("compact", facts, call_index=max(1, facts.call_index), epoch=old_epoch)
            if self.config.session_control.enabled and self._needs_resume(facts):
                await self._control.send("resume", facts, call_index=call_index, epoch=facts.epoch)
                facts.paused = False
                self._store_flag_soon(identity.session_id, paused=False)

            facts.call_index = call_index
            facts.last_task_id = identity.task_id
            facts.model_name = model_name
            facts.family = family
            if identity.incarnation:
                facts.incarnation = identity.incarnation
            if current_head is not None:
                facts.head_ids = current_head
            await self._store.save(facts)
            self._cache[identity.session_id] = facts

            call = LogicalCall(
                session_id=identity.session_id,
                kind="lead",
                state=state,
                call_index=call_index,
                epoch=facts.epoch,
                ttl_s=ttl_s,
                priority_pair=priority_for(turn_opening),
                after_rewrite=after_rewrite,
                head_changed=head_changed,
                prev_signature=prev_signature,
                parent_session_id=facts.parent_session_id,
                head_ids=current_head,
                suppress=facts.quarantined,
            )
            self._trace.emit(
                {
                    "event": "logical",
                    "session_id": call.session_id,
                    "kind": call.kind,
                    "call_index": call.call_index,
                    "epoch": call.epoch,
                    "state": call.state,
                    "after_rewrite": after_rewrite,
                    "head_changed": head_changed,
                    "quarantined": facts.quarantined,
                    "at_ms": now,
                }
            )
            return call

    def _begin_subagent(self, request: Any, identity: Identity, model_name: str, family: str) -> LogicalCall:
        with self._sub_lock:
            facts = self._subagents.get(identity.session_id)
            if facts is None:
                facts = SessionFacts(
                    session_id=identity.session_id,
                    kind="subagent",
                    thread_id=identity.thread_id,
                    user_id=identity.user_id,
                )
                self._subagents[identity.session_id] = facts
            self._learn_history(request, facts)
            after_rewrite = self._detect_rewrite(request, facts)
            current_head = head_ids(getattr(request, "messages", None))
            head_changed = head_rewritten(facts.head_ids, current_head)
            now = now_ms()
            interval = None if facts.last_response_end_ms is None else now - facts.last_response_end_ms
            call = LogicalCall(
                session_id=identity.session_id,
                kind="subagent",
                state="append" if facts.started else "start",
                call_index=facts.call_index + 1,
                epoch=facts.epoch,
                ttl_s=derive_ttl_s(interval),
                priority_pair=(1, "critical_path"),
                after_rewrite=after_rewrite,
                head_changed=head_changed,
                prev_signature=facts.last_signature,
                parent_session_id=None,
                head_ids=current_head,
            )
            facts.call_index = call.call_index
            facts.last_task_id = identity.task_id
            facts.model_name = model_name
            facts.family = family
            if current_head is not None:
                facts.head_ids = current_head
        if after_rewrite and self.config.session_control.enabled:
            self._control.fire_and_forget("compact", facts, call_index=max(1, call.call_index - 1), epoch=max(0, call.epoch - 1))
        return call

    async def _load_lead(self, identity: Identity) -> SessionFacts:
        facts = await self._store.fetch(identity.session_id)
        if facts is None:
            facts = SessionFacts(
                session_id=identity.session_id,
                kind="lead",
                thread_id=identity.thread_id,
                user_id=identity.user_id,
                incarnation=identity.incarnation,
            )
            if self._meta is not None and identity.thread_id:
                view = (await self._meta.read([identity.thread_id])).get(identity.thread_id)
                if view is not None:
                    facts.user_id = facts.user_id or (view.user_id or "")
                    facts.incarnation = facts.incarnation or view.incarnation
                    facts.parent_session_id = view.branch_parent_thread_id
        else:
            if not facts.user_id:
                facts.user_id = identity.user_id
            if not facts.incarnation:
                facts.incarnation = identity.incarnation
        facts.local = True
        return facts

    def _learn_history(self, request: Any, facts: SessionFacts) -> None:
        """Decide ``seeded`` on first sight and seed the call ordinal."""
        if facts.seeded_known:
            return
        facts.seeded = bool(_pre_existing_ids(request))
        facts.seeded_known = True
        if facts.seeded and facts.call_index == 0:
            facts.call_index = _count_ai_messages(getattr(request, "messages", None))

    def _detect_rewrite(self, request: Any, facts: SessionFacts) -> bool:
        """Detect a committed history rewrite (§6.3).

        The authoritative signal is DeerFlow's ``summary_text`` channel: it only
        changes when a compaction (automatic or manual) rewrote the history.
        Older hosts without that channel fall back to the compaction observer's
        pending counter.
        """
        state = getattr(request, "state", None)
        if isinstance(state, Mapping) and "summary_text" in state:
            facts.summary_available = True
        if facts.summary_available:
            fingerprint = _fingerprint(state.get("summary_text") if isinstance(state, Mapping) else None)
            if facts.summary_fp is None:
                facts.summary_fp = fingerprint
                # A non-empty summary appearing after calls we already served is
                # a rewrite we simply had not observed before (e.g. the plugin
                # started mid-life, or the summary channel materialized late).
                if facts.call_index > 0 and fingerprint != _fingerprint(None):
                    facts.epoch += 1
                    return True
                return False
            if fingerprint != facts.summary_fp:
                facts.summary_fp = fingerprint
                facts.epoch += 1
                return True
            return False
        if facts.pending_rewrites:
            facts.epoch += facts.pending_rewrites
            facts.pending_rewrites = 0
            return True
        return False

    def _is_turn_opening(self, facts: SessionFacts, identity: Identity) -> bool:
        if identity.kind != "lead":
            return False
        if facts.last_task_id == identity.task_id:
            return False
        return _origin_user_like(identity)

    @staticmethod
    def _needs_resume(facts: SessionFacts) -> bool:
        if facts.paused:
            return True
        # Adopted/restored/forked context: resume before its first request (§7.4).
        return facts.seeded and facts.last_accepted_at_ms is None

    def _store_flag_soon(self, session_id: str, **flags: Any) -> None:
        self.schedule(self._store.set_flags(session_id, **flags))

    # -- hook: MODEL_PHYSICAL ----------------------------------------------

    def hint_for(self, request: Any, call: LogicalCall) -> dict[str, Any] | None:
        """Freeze the derived values into one wire hint (retry-stable)."""
        try:
            signature = prefix_signature(request.model, request.tools)
            call.signature = signature
            offset = logical_message_count(
                getattr(request, "messages", None),
                system_present=getattr(request, "system_message", None) is not None,
            )
            call.msg_offset = offset
            if call.suppress:
                self.count("hint_suppressed")
                return None

            priority_pair = call.priority_pair
            if self.config.priority == "omit":
                priority_pair = None
            elif isinstance(self.config.priority, tuple):
                priority_pair = self.config.priority

            cache: tuple[int, int] | None = None
            if self.config.cache == "dynamic":
                if (
                    not call.after_rewrite
                    and not call.head_changed
                    and offset >= 1
                    and signature_stable(call.prev_signature, signature, call.call_index)
                ):
                    tokens = estimate_tokens([getattr(request, "system_message", None), *list(getattr(request, "messages", None) or [])])
                    if tokens >= MIN_CACHE_TOKENS:
                        cache = (call.ttl_s, offset)
            elif isinstance(self.config.cache, tuple):
                ttl_s, fixed_offset = self.config.cache
                cache = (ttl_s, fixed_offset or offset)

            hint = ordinary_hint(
                session_id=call.session_id,
                call_index=call.call_index,
                epoch=call.epoch,
                state=call.state,
                parent_session_id=call.parent_session_id,
                priority_pair=priority_pair,
                cache=cache,
            )
            call.cache_declared = cache is not None
            if not call.traced:
                call.traced = True
                self.count("hint_emitted")
                if call.state == "start":
                    self.count("state_start")
                else:
                    self.count("state_append")
                if call.cache_declared:
                    self.count("cache_declared")
                self._trace.emit(
                    {
                        "event": "hint",
                        "session_id": call.session_id,
                        "kind": call.kind,
                        "call_index": call.call_index,
                        "epoch": call.epoch,
                        "state": call.state,
                        "priority": priority_pair,
                        "ttl_s": cache[0] if cache else None,
                        "msg_offset": cache[1] if cache else None,
                        "after_rewrite": call.after_rewrite,
                    }
                )
            return hint
        except Exception:
            logger.exception("agent-hints: could not build a hint; the request continues without one")
            call.suppress = True
            return None

    # -- hook: end of a logical call ---------------------------------------

    async def finish_logical(self, call: LogicalCall, response: Any, error: BaseException | None) -> None:
        try:
            accepted = error is None and response is not None and not _error_fallback(response)
            now = now_ms()
            if call.kind == "subagent":
                with self._sub_lock:
                    facts = self._subagents.get(call.session_id)
                    if facts is None:
                        return
                    if call.signature:
                        facts.last_signature = call.signature
                    if accepted:
                        facts.started = True
                        facts.last_response_end_ms = now
                        facts.last_accepted_at_ms = now
                        if call.head_ids is not None:
                            facts.head_ids = call.head_ids
                return
            async with self._lock(call.session_id):
                facts = self._cache.get(call.session_id) or await self._store.fetch(call.session_id)
                if facts is None:
                    return
                if call.signature:
                    facts.last_signature = call.signature
                if accepted:
                    facts.started = True
                    facts.last_response_end_ms = now
                    facts.last_accepted_at_ms = now
                    if call.head_ids is not None:
                        facts.head_ids = call.head_ids
                    facts.local = True
                await self._store.save(facts)
                self._cache[call.session_id] = facts
            if accepted and self.config.session_control.enabled:
                self.schedule(self._pause_others(facts))
        except Exception:
            logger.exception("agent-hints: failed to finish a logical call")

    async def _pause_others(self, facts: SessionFacts) -> None:
        """Protocol V5: a session that just became active pauses idle siblings."""
        if not facts.user_id or facts.last_accepted_at_ms is None:
            return
        candidates = await self._store.idle_candidates(
            user_id=facts.user_id,
            before_ms=facts.last_accepted_at_ms,
            exclude=facts.session_id,
        )
        for candidate in candidates:
            candidate.local = True
            await self._pause_session(candidate)

    async def _maybe_pause_idle(self, facts: SessionFacts) -> None:
        """A session that went idle after a sibling became active is paused."""
        if not facts.user_id or facts.active_run_id:
            return
        latest = await self._store.latest_accepted_other(user_id=facts.user_id, exclude=facts.session_id)
        if latest is None:
            return
        if (facts.last_accepted_at_ms or 0) < latest:
            await self._pause_session(facts)

    async def _pause_session(self, facts: SessionFacts) -> None:
        if facts.paused or facts.stopped or facts.quarantined:
            return
        if facts.active_run_id:
            return
        await self._store.set_flags(facts.session_id, paused=True)
        outcome = await self._control.send("pause", facts, call_index=max(1, facts.call_index), epoch=facts.epoch)
        if outcome == "accepted":
            facts.paused = True

    # -- lifecycle contributors --------------------------------------------

    async def on_task_start(self, info: Any) -> None:
        if not self.emitting:
            return
        try:
            if info.kind == "subagent":
                self._register_subagent(info)
                return
            async with self._lock(info.thread_id):
                facts = await self._load_lead(
                    Identity(
                        session_id=info.thread_id,
                        kind="lead",
                        thread_id=info.thread_id,
                        run_id=info.run_id,
                        task_id=info.task_id,
                        agent_name=info.agent_name,
                        user_id=_effective_user_id(),
                        incarnation=None,
                        metadata=dict(info.metadata or {}),
                    )
                )
                facts.active_run_id = info.run_id
                await self._store.save(facts)
                self._cache[info.thread_id] = facts
            if facts.paused and self.config.session_control.enabled:
                await self._store.set_flags(facts.session_id, paused=False)
                facts.paused = False
                self.schedule(self._control.send("resume", facts, call_index=facts.call_index + 1, epoch=facts.epoch))
        except Exception:
            logger.exception("agent-hints: on_task_start failed")

    async def on_task_stop(self, info: Any) -> None:
        if not self.emitting:
            return
        try:
            if info.kind == "subagent":
                session_id = session_id_for(info.thread_id, info.run_id, info.task_id)
                with self._sub_lock:
                    facts = self._subagents.pop(session_id, None)
                if facts is not None and self.config.session_control.enabled:
                    self._control.fire_and_forget("stop", facts, call_index=max(1, facts.call_index), epoch=facts.epoch)
                self._control.forget(session_id)
                return
            async with self._lock(info.thread_id):
                facts = await self._store.fetch(info.thread_id)
                if facts is None:
                    return
                facts.active_run_id = None
                await self._store.save(facts)
                self._cache[info.thread_id] = facts
            if self.config.session_control.enabled:
                await self._maybe_pause_idle(facts)
        except Exception:
            logger.exception("agent-hints: on_task_stop failed")

    async def on_context_compacted(self, event: Any, task_store: Any) -> None:
        """Latch a rewrite when the host has no ``summary_text`` channel."""
        if not self.emitting:
            return
        try:
            thread_id = getattr(event, "thread_id", None)
            if not thread_id:
                self._trace.emit({"event": "compaction", "session_id": None, "note": "anonymous (manual compaction)"})
                return
            facts = await self._store.fetch(thread_id)
            if facts is None or facts.summary_available:
                return
            facts.pending_rewrites += 1
            await self._store.set_flags(thread_id, pending_rewrites=facts.pending_rewrites)
            self._cache[thread_id] = facts
            self._trace.emit(
                {
                    "event": "compaction",
                    "session_id": thread_id,
                    "kept": getattr(event, "kept_message_count", None),
                    "compacted": getattr(event, "compacted_message_count", None),
                }
            )
        except Exception:
            logger.exception("agent-hints: on_context_compacted failed")

    async def clear_isolation(self, session_id: str) -> bool:
        """Operator escape hatch: forget an unknown-mutation quarantine."""
        facts = await self._store.fetch(session_id)
        if facts is None or not facts.quarantined:
            return False
        await self._store.set_flags(session_id, quarantined_reason=None)
        self._cache.pop(session_id, None)
        logger.info("agent-hints: isolation cleared for session %s", session_id)
        return True

    def _register_subagent(self, info: Any) -> None:
        session_id = session_id_for(info.thread_id, info.run_id, info.task_id)
        with self._sub_lock:
            self._subagents.setdefault(
                session_id,
                SessionFacts(
                    session_id=session_id,
                    kind="subagent",
                    thread_id=info.thread_id or "",
                ),
            )

    # -- status --------------------------------------------------------------

    async def status(self) -> dict[str, Any]:
        sessions: list[dict[str, Any]] = []
        quarantined: list[dict[str, Any]] = []
        try:
            tracked = await self._store.tracked_lead(limit=100)
        except Exception:
            tracked = []
        for facts in tracked:
            row = {
                "session_id": facts.session_id,
                "thread_id": facts.thread_id,
                "call_index": facts.call_index,
                "epoch": facts.epoch,
                "seeded": facts.seeded,
                "started": facts.started,
                "paused": facts.paused,
                "active_run": bool(facts.active_run_id),
                "quarantined": facts.quarantined,
                "last_accepted_at_ms": facts.last_accepted_at_ms,
            }
            sessions.append(row)
            if facts.quarantined:
                quarantined.append({"session_id": facts.session_id, "reason": facts.quarantined_reason})
        with self._sub_lock:
            subagents = len(self._subagents)
        return {
            "enabled": self.emitting,
            "disabled_reason": self._disabled_reason,
            "level": "L1+L2+L3-lite",
            "injection": self._injection,
            "counters": self.counters(),
            "manage": self._control.counts(),
            "tracked_sessions": len(sessions),
            "subagent_sessions": subagents,
            "quarantined": quarantined,
            "sessions": sessions,
        }
