"""Gateway service: start/stop the store and client, self-check, poll threads."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from deerflow_extension_api import ExtensionRuntimeDeps

from deerflow_agent_hints.config import PluginConfig
from deerflow_agent_hints.control import ControlService
from deerflow_agent_hints.feed import EventFeed
from deerflow_agent_hints.runtime import PluginRuntime
from deerflow_agent_hints.store import SessionStore
from deerflow_agent_hints.threads import ThreadMetaReader
from deerflow_agent_hints.trace import Trace

logger = logging.getLogger(__name__)


async def check_injection() -> str:
    """Prove that an in-place ``model_settings`` change reaches the provider.

    DeerFlow wraps extension middlewares so that a *replacement* request is
    dropped; this hint depends on mutating the request in place. The check runs
    the real wrapper against a recording model, so a host upgrade that closes
    the pass-through is loud at startup instead of silently hint-less.
    """
    try:
        from deerflow.extensions.isolation import IsolatedMiddleware
        from langchain.agents import create_agent
        from langchain.agents.middleware import AgentMiddleware
        from langchain_core.language_models.chat_models import BaseChatModel
        from langchain_core.messages import AIMessage
        from langchain_core.outputs import ChatGeneration, ChatResult
    except Exception:
        return "unavailable"

    seen: list[dict[str, Any]] = []

    class ProbeModel(BaseChatModel):
        @property
        def _llm_type(self) -> str:
            return "agent-hints-probe"

        def _generate(self, messages, stop=None, run_manager=None, **kwargs):  # type: ignore[override]
            seen.append(dict(kwargs))
            return ChatResult(generations=[ChatGeneration(message=AIMessage(content="ok"))])

    class ProbeMiddleware(AgentMiddleware):
        name = "agent_hints_selfcheck"

        async def awrap_model_call(self, request, handler):
            settings = request.model_settings
            existing = settings.get("extra_headers") or {}
            settings["extra_headers"] = {**existing, "X-Agent-Hint": "selfcheck"}
            return await handler(request)

    try:
        agent = create_agent(
            ProbeModel(),
            tools=[],
            middleware=[
                IsolatedMiddleware(
                    ProbeMiddleware(),
                    "agent-hints-selfcheck",
                    lambda diagnostic: None,
                    name="agent_hints_selfcheck_isolated",
                )
            ],
        )
        await agent.ainvoke(
            {"messages": [("user", "ping")]},
            config={"configurable": {"thread_id": "agent-hints-selfcheck"}},
        )
    except Exception:
        logger.exception("agent-hints: injection self-check raised")
        return "error"

    ok = any((entry.get("extra_headers") or {}).get("X-Agent-Hint") == "selfcheck" for entry in seen if isinstance(entry, dict))
    if not ok:
        logger.error(
            "agent-hints: the host no longer forwards an in-place model_settings change "
            "through the isolated extension wrapper; ordinary requests will carry no hint"
        )
    return "ok" if ok else "broken"


class AgentHintsService:
    """Owns runtime resources; the Gateway starts and stops it."""

    def __init__(
        self,
        config: PluginConfig,
        runtime: PluginRuntime,
        store: SessionStore,
        control: ControlService,
        meta: ThreadMetaReader,
        trace: Trace,
        feed: EventFeed,
    ) -> None:
        self._config = config
        self._feed = feed
        self._runtime = runtime
        self._store = store
        self._control = control
        self._meta = meta
        self._trace = trace
        self._poll_task: asyncio.Task | None = None

    async def start(self, deps: ExtensionRuntimeDeps) -> None:
        self._store.bind(deps.session_factory)
        self._meta.bind(deps.session_factory)
        await self._store.start()
        await self._control.start()
        status = await check_injection()
        self._runtime.set_injection_status(status)
        if status == "broken":
            # The header channel is dead on this host version; emitting manage
            # traffic for hints nobody sees would only mislead operators.
            self._runtime.suspend("injection-broken")
        if (
            self._config.session_control.enabled
            and self._config.session_control.thread_poll_seconds > 0
            and self._meta.available
            and self._store.ready
        ):
            self._poll_task = asyncio.create_task(self._poll_loop())
        self._trace.emit(
            {
                "event": "service",
                "action": "start",
                "injection": self._runtime.injection_status,
                "store": "host" if deps.session_factory is not None else "file",
            }
        )

    async def stop(self) -> None:
        if self._poll_task is not None:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._poll_task
            self._poll_task = None
        # Protocol §8.4: no new manage requests after unload. In-flight ones are
        # awaited by the client close below, bounded by the control timeout.
        await self._control.stop()
        await self._store.stop()
        self._trace.emit({"event": "service", "action": "stop"})

    def events(self, since: int = 0, limit: int = 200) -> dict[str, Any]:
        return {"seq": self._feed.last_seq, "events": self._feed.since(since, limit)}

    async def status(self) -> dict[str, Any]:
        payload = await self._runtime.status()
        payload["events"] = {"seq": self._feed.last_seq, "limit": 500}
        payload["live"] = "/api/extension/agent-hints/live"
        payload["store"] = "host" if self._store.ready else "unavailable"
        payload["thread_poller"] = self._poll_task is not None
        return payload

    # -- thread metadata polling (archive / delete -> stop) -----------------

    async def _poll_loop(self) -> None:
        interval = self._config.session_control.thread_poll_seconds
        while True:
            try:
                await asyncio.sleep(interval)
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("agent-hints: thread metadata poll failed")

    async def _poll_once(self) -> None:
        if not self._store.ready:
            return
        rows = await self._store.tracked_lead(limit=200)
        if not rows:
            return
        views = await self._meta.read([row.thread_id for row in rows if row.thread_id])
        for facts in rows:
            if not facts.thread_id:
                continue
            view = views.get(facts.thread_id)
            if view is None:
                if facts.incarnation:
                    await self._stop_session(facts, "deleted")
                continue
            if view.archived:
                await self._stop_session(facts, "archived")
                continue
            if facts.incarnation and view.incarnation and facts.incarnation != view.incarnation:
                await self._stop_session(facts, "recreated")
                await self._store.reset_session(facts.session_id, incarnation=view.incarnation)

    async def clear_isolation(self, session_id: str) -> bool:
        return await self._runtime.clear_isolation(session_id)

    async def _stop_session(self, facts: Any, reason: str) -> None:
        if not self._runtime.emitting:
            return
        if not await self._store.claim_stop(facts.session_id):
            return
        facts.stopped = True
        logger.info("agent-hints: stopping session %s (%s)", facts.session_id, reason)
        await self._control.send("stop", facts, call_index=max(1, facts.call_index), epoch=facts.epoch)
