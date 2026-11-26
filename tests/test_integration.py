"""In-process integration against the real DeerFlow extension host.

Loads the extension through ``load_extensions``, composes the real anchor stack
(``compose_with_extensions``), wraps the contribution in the host's
``IsolatedMiddleware``, and asserts on the bytes a recording chat model receives.
Requires the DeerFlow backend environment (skipped elsewhere).
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

pytest.importorskip("deerflow.extensions.loader")

from deerflow_extension_api import (  # noqa: E402
    EXTENSION_TASK_STORE_KEY,
    AgentBuildContext,
    AgentScope,
    ExtensionData,
    TaskInfo,
)
from langchain.agents import create_agent  # noqa: E402
from langchain_openai import ChatOpenAI  # noqa: E402

from deerflow_agent_hints.wire import decode, validate_ordinary  # noqa: E402
from support import CaptureServer  # noqa: E402

LONG_SYSTEM = "You are a careful assistant. " * 60  # > 128 tokens
_COMPLETION = {
    "id": "chatcmpl-1",
    "object": "chat.completion",
    "created": 1,
    "model": "gpt-4o",
    "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
    "usage": {"prompt_tokens": 10, "completion_tokens": 1, "total_tokens": 11},
}


class Wire:
    """A chat model whose transport records the outgoing request."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(
            {
                "headers": {key.lower(): value for key, value in request.headers.items()},
                "body": json.loads(request.content),
            }
        )
        return httpx.Response(200, json=_COMPLETION)

    def model(self) -> ChatOpenAI:
        return ChatOpenAI(
            model="gpt-4o",
            api_key="test-key",
            max_retries=0,
            http_async_client=httpx.AsyncClient(transport=httpx.MockTransport(self._handle)),
        )

    def last_hint(self) -> dict[str, Any]:
        header = self.requests[-1]["headers"].get("x-agent-hint")
        assert header, "the request carried no X-Agent-Hint header"
        return decode(header)


def run_context(
    thread_id: str,
    run_id: str,
    *,
    pre_existing: tuple[str, ...] = (),
    kind: str = "lead",
    task_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    store = ExtensionData(task_id or run_id)
    store.set(
        TaskInfo(
            task_id=task_id or run_id,
            run_id=run_id,
            thread_id=thread_id,
            kind=kind,  # type: ignore[arg-type]
            agent_name="lead_agent" if kind == "lead" else "researcher",
            metadata=metadata or {},
        )
    )
    return {
        EXTENSION_TASK_STORE_KEY: store,
        "thread_id": thread_id,
        "run_id": run_id,
        "user_id": "user-1",
        "__deerflow_pre_run_message_ids": frozenset(pre_existing),
    }


@pytest.fixture
async def host(tmp_path):
    from deerflow.config.app_config import AppConfig
    from deerflow.extensions.gateway import start_services, stop_services
    from deerflow.extensions.loader import ExtensionSpec, load_extensions

    server = CaptureServer()
    config = AppConfig.model_validate(
        {
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
            "database": {"backend": "memory"},
            "memory": {"enabled": False},
            "skills": {"path": str(tmp_path / "skills")},
        }
    )
    loaded, diagnostics = load_extensions(
        [
            ExtensionSpec(
                name="agent-hints",
                use="deerflow_agent_hints:install",
                table_prefix="extension_agent_hints_",
                config={
                    "sessionControl": {"threadPollSeconds": 0, "pendingOpAwaitMs": 500},
                    "controlEndpoint": {"url": server.manage_url, "model": "manage-model"},
                    "state": {"path": str(tmp_path / "state.db")},
                    "trace": {"enabled": True, "file": str(tmp_path / "trace.jsonl")},
                },
            )
        ]
    )
    assert not [item for item in diagnostics if getattr(item, "level", "") == "error"], diagnostics
    await start_services(loaded, config, None)
    try:
        yield loaded, config, server
    finally:
        await stop_services(loaded)
        server.close()


def _stack(loaded: Any, config: Any, scope: AgentScope = AgentScope.LEAD) -> list[Any]:
    from deerflow.agents.middlewares.clarification_middleware import ClarificationMiddleware
    from deerflow.agents.middlewares.llm_error_handling_middleware import LLMErrorHandlingMiddleware
    from deerflow.agents.middlewares.safety_finish_reason_middleware import SafetyFinishReasonMiddleware
    from deerflow.agents.middlewares.terminal_response_middleware import TerminalResponseMiddleware
    from deerflow.extensions.stack import compose_with_extensions

    base = [
        LLMErrorHandlingMiddleware(app_config=config),
        TerminalResponseMiddleware(),
        SafetyFinishReasonMiddleware(),
        ClarificationMiddleware(),
    ]
    return compose_with_extensions(base, scope, AgentBuildContext(scope=scope), loaded)


def _agent(
    host: Any,
    wire: Wire,
    *,
    scope: AgentScope = AgentScope.LEAD,
    state_schema: Any = None,
    checkpointer: Any = None,
) -> Any:
    loaded, config, _ = host
    if checkpointer is None:
        from langgraph.checkpoint.memory import InMemorySaver

        checkpointer = InMemorySaver()
    return create_agent(
        wire.model(),
        tools=[],
        system_prompt=LONG_SYSTEM,
        middleware=_stack(loaded, config, scope),
        state_schema=state_schema,
        checkpointer=checkpointer,
    )


async def test_header_reaches_the_wire_and_body_is_untouched(host):
    wire = Wire()
    agent = _agent(host, wire)
    await agent.ainvoke(
        {"messages": [("user", "hello")]},
        config={"configurable": {"thread_id": "thread-1"}},
        context=run_context("thread-1", "run-1"),
    )
    hint = wire.last_hint()
    assert validate_ordinary(hint) == []
    assert hint["session"] == {
        "session_id": "thread-1",
        "call_index": 1,
        "epoch": 0,
        "state": "start",
    }
    assert hint["priority_control"] == {"priority": {"level": 3, "reason": "user_waiting"}}
    cache = hint["cache_control"]
    assert cache["type"] == "ephemeral"
    assert 60 <= cache["ttl_s"] <= 3600
    body = wire.requests[-1]["body"]
    assert cache["msg_offset"] == len(body["messages"])
    assert not any("hint" in key.lower() or "agent_hint" in key.lower() for key in body)
    assert set(body) == {"messages", "model", "stream"}


async def test_append_keeps_the_ordinal_monotonic_across_runs(host):
    wire = Wire()
    agent = _agent(host, wire)
    config = {"configurable": {"thread_id": "thread-2"}}
    await agent.ainvoke({"messages": [("user", "first")]}, config=config, context=run_context("thread-2", "run-a"))
    first = wire.last_hint()
    await agent.ainvoke(
        {"messages": [("user", "second")]},
        config=config,
        context=run_context("thread-2", "run-b"),
    )
    second = wire.last_hint()
    assert first["session"]["state"] == "start" and first["session"]["call_index"] == 1
    assert second["session"]["state"] == "append"
    assert second["session"]["call_index"] == 2
    assert second["session"]["epoch"] == 0
    assert second["priority_control"]["priority"] == {"level": 3, "reason": "user_waiting"}
    assert "cache_control" in second  # signature stable, no rewrite


async def test_continuation_inside_one_run_is_critical_path(host):
    wire = Wire()
    agent = _agent(host, wire)
    config = {"configurable": {"thread_id": "thread-3"}}
    context = run_context("thread-3", "run-c")
    await agent.ainvoke({"messages": [("user", "go")]}, config=config, context=context)
    await agent.ainvoke({"messages": [("user", "again")]}, config=config, context=context)
    hint = wire.last_hint()
    assert hint["session"]["call_index"] == 2
    assert hint["session"]["state"] == "append"
    assert hint["priority_control"] == {"priority": {"level": 1, "reason": "critical_path"}}


async def test_subagent_is_its_own_session_without_a_parent(host):
    wire = Wire()
    agent = _agent(host, wire, scope=AgentScope.SUBAGENT)
    await agent.ainvoke(
        {"messages": [("user", "research this")]},
        config={"configurable": {"thread_id": "thread-4"}},
        context=run_context("thread-4", "run-d", kind="subagent", task_id="task-A"),
    )
    hint = wire.last_hint()
    assert validate_ordinary(hint) == []
    assert hint["session"]["session_id"] == "thread-4:run-d:task-A"
    assert hint["session"]["state"] == "start"
    assert hint["session"]["call_index"] == 1
    assert "parent_session_id" not in hint["session"]
    assert hint["priority_control"] == {"priority": {"level": 1, "reason": "critical_path"}}


async def test_seeded_session_resumes_before_its_first_request(host):
    wire = Wire()
    agent = _agent(host, wire)
    await agent.ainvoke(
        {"messages": [("user", "continue")]},
        config={"configurable": {"thread_id": "thread-5"}},
        context=run_context("thread-5", "run-e", pre_existing=("old-1", "old-2")),
    )
    hint = wire.last_hint()
    assert hint["session"]["state"] == "append"
    manages = [decode(request["headers"]["x-agent-hint"]) for request in host[2].requests]
    resumes = [item for item in manages if item["session"]["state"] == "resume"]
    assert resumes, "a restored session must resume before its first request"
    assert resumes[0]["session"]["call_index"] == hint["session"]["call_index"]
    assert resumes[0]["context_management"]["edits"][0]["type"] == "prefetch"


async def test_compaction_bumps_epoch_suppresses_cache_and_sends_compact(host):
    from deerflow.agents.thread_state import get_thread_state_schema

    wire = Wire()
    agent = _agent(host, wire, state_schema=get_thread_state_schema("full"))
    config = {"configurable": {"thread_id": "thread-6"}}
    await agent.ainvoke({"messages": [("user", "one")]}, config=config, context=run_context("thread-6", "run-f"))
    assert "cache_control" in wire.last_hint()
    await agent.ainvoke(
        {"messages": [("user", "two")], "summary_text": "a compacted summary"},
        config=config,
        context=run_context("thread-6", "run-g"),
    )
    hint = wire.last_hint()
    assert hint["session"]["epoch"] == 1
    assert hint["session"]["state"] == "append"
    assert "cache_control" not in hint
    manages = [decode(request["headers"]["x-agent-hint"]) for request in host[2].requests]
    compacts = [item for item in manages if item["session"]["state"] == "compact"]
    assert compacts, "a committed rewrite must emit a compact manage request"
    assert compacts[0]["session"]["epoch"] == 0  # the old epoch
    assert compacts[0]["context_management"]["edits"][0]["type"] == "evict"


async def test_a_second_active_session_pauses_the_idle_one(host):
    wire_a, wire_b = Wire(), Wire()
    agent = _agent(host, wire_a)
    agent_b = _agent(host, wire_b)
    await agent.ainvoke(
        {"messages": [("user", "a")]},
        config={"configurable": {"thread_id": "thread-a"}},
        context=run_context("thread-a", "run-h"),
    )
    await agent_b.ainvoke(
        {"messages": [("user", "b")]},
        config={"configurable": {"thread_id": "thread-b"}},
        context=run_context("thread-b", "run-i"),
    )
    await _eventually(
        lambda: any(decode(request["headers"]["x-agent-hint"])["session"]["state"] == "pause" for request in host[2].requests)
    )
    pauses = [decode(request["headers"]["x-agent-hint"]) for request in host[2].requests]
    paused = [item for item in pauses if item["session"]["state"] == "pause"]
    assert paused[0]["session"]["session_id"] == "thread-a"
    assert paused[0]["context_management"]["edits"][0]["type"] == "offload"
    # Returning to the paused session resumes it before the request.
    await agent.ainvoke(
        {"messages": [("user", "a again")]},
        config={"configurable": {"thread_id": "thread-a"}},
        context=run_context("thread-a", "run-j"),
    )
    states = [decode(request["headers"]["x-agent-hint"])["session"]["state"] for request in host[2].requests]
    assert states.count("resume") >= 1


async def _eventually(predicate, timeout: float = 3.0) -> None:
    import asyncio

    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    assert predicate(), "condition was not met before the timeout"
