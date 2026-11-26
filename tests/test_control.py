"""Manage transport: placeholder body, classification, retries, quarantine."""

from __future__ import annotations

import pytest

from deerflow_agent_hints.config import parse_config
from deerflow_agent_hints.control import ControlService, endpoint_from_model
from deerflow_agent_hints.store import SessionFacts, SessionStore
from deerflow_agent_hints.trace import Trace
from deerflow_agent_hints.wire import decode, validate_manage
from support import CaptureServer


async def _service(tmp_path, url: str, *, retries: int = 1) -> tuple[ControlService, SessionStore]:
    config = parse_config(
        {
            "sessionControl": {"controlTimeoutMs": 1000, "retries": retries},
            "controlEndpoint": {"url": url, "model": "stub-model", "apiKey": "k"},
        }
    )
    store = SessionStore(None, str(tmp_path / "state.db"))
    await store.start()
    control = ControlService(config, store, Trace())
    await control.start()
    return control, store


def _facts(session_id: str = "s-1") -> SessionFacts:
    return SessionFacts(session_id=session_id, kind="lead", thread_id=session_id, call_index=3, epoch=1)


async def test_manage_wire_shape_and_placeholder_body(tmp_path):
    server = CaptureServer()
    control, store = await _service(tmp_path, server.manage_url)
    try:
        outcome = await control.send("pause", _facts(), call_index=3, epoch=1)
        assert outcome == "accepted"
        request = server.requests[0]
        hint = decode(request["headers"]["x-agent-hint"])
        assert validate_manage(hint) == []
        assert hint["session"]["state"] == "pause"
        assert hint["session"]["call_index"] == 3
        assert hint["session"]["epoch"] == 1
        assert hint["context_management"] == {
            "manage_request": True,
            "edits": [{"type": "offload", "target": "session"}],
        }
        assert request["body"] == {"model": "stub-model", "messages": [], "stream": False}
        assert "agent_hint" not in {key.lower() for key in request["body"]}
    finally:
        await control.stop()
        await store.stop()
        server.close()


async def test_client_error_is_failed_without_quarantine(tmp_path):
    server = CaptureServer(status=400)
    control, store = await _service(tmp_path, server.manage_url)
    facts = _facts()
    try:
        assert await control.send("stop", facts, call_index=3, epoch=1) == "failed"
        assert facts.quarantined_reason is None
        assert len(server.requests) == 1  # mutations never retry
    finally:
        await control.stop()
        await store.stop()
        server.close()


async def test_server_error_on_mutation_quarantines(tmp_path):
    server = CaptureServer(status=503)
    control, store = await _service(tmp_path, server.manage_url)
    facts = _facts()
    await store.save(facts)
    try:
        assert await control.send("compact", facts, call_index=2, epoch=0) == "unknown"
        assert facts.quarantined_reason is not None
        stored = await store.fetch("s-1")
        assert stored is not None and stored.quarantined_reason
        assert len(server.requests) == 1  # never retried
    finally:
        await control.stop()
        await store.stop()
        server.close()


async def test_resume_retries_transient_then_succeeds(tmp_path):
    server = CaptureServer(status=lambda count: 503 if count == 1 else 200)
    control, store = await _service(tmp_path, server.manage_url, retries=1)
    facts = _facts()
    try:
        assert await control.send("resume", facts, call_index=1, epoch=0) == "accepted"
        assert len(server.requests) == 2
        assert facts.quarantined_reason is None
        assert decode(server.requests[0]["headers"]["x-agent-hint"])["session"]["state"] == "resume"
    finally:
        await control.stop()
        await store.stop()
        server.close()


async def test_resume_exhaustion_is_failed_not_quarantined(tmp_path):
    server = CaptureServer(status=429)
    control, store = await _service(tmp_path, server.manage_url, retries=2)
    facts = _facts()
    try:
        assert await control.send("resume", facts, call_index=1, epoch=0) == "failed"
        assert len(server.requests) == 3
        assert facts.quarantined_reason is None
    finally:
        await control.stop()
        await store.stop()
        server.close()


async def test_wait_idle_is_bounded(tmp_path):
    control, store = await _service(tmp_path, "http://127.0.0.1:9/never")
    try:
        assert await control.wait_idle("s-1", 10) is True
        lock = control._lock("s-1")  # noqa: SLF001 - the barrier is the unit under test
        async with lock:
            assert await control.wait_idle("s-1", 10) is False
    finally:
        await control.stop()
        await store.stop()


def test_endpoint_from_openai_and_anthropic_models():
    from langchain_anthropic import ChatAnthropic
    from langchain_openai import ChatOpenAI

    openai_endpoint = endpoint_from_model(ChatOpenAI(model="gpt-4o", api_key="sk-test", base_url="http://gw.local/v1"))
    assert openai_endpoint is not None
    assert openai_endpoint.url == "http://gw.local/v1/chat/completions"
    assert openai_endpoint.headers["Authorization"] == "Bearer sk-test"
    assert openai_endpoint.model == "gpt-4o"

    anthropic_endpoint = endpoint_from_model(ChatAnthropic(model="claude-test", api_key="ant-key", base_url="http://gw.local"))
    assert anthropic_endpoint is not None
    assert anthropic_endpoint.url == "http://gw.local/v1/messages"
    assert anthropic_endpoint.headers["x-api-key"] == "ant-key"
    assert anthropic_endpoint.headers["anthropic-version"]


def test_endpoint_rejects_unsupported_families():
    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel

    assert endpoint_from_model(GenericFakeChatModel(messages=iter([]))) is None


@pytest.mark.parametrize("verb", ["pause", "resume", "stop", "compact"])
async def test_every_verb_maps_to_its_edit(tmp_path, verb):
    server = CaptureServer()
    control, store = await _service(tmp_path, server.manage_url)
    try:
        assert await control.send(verb, _facts(), call_index=1, epoch=0) == "accepted"  # type: ignore[arg-type]
        hint = decode(server.requests[0]["headers"]["x-agent-hint"])
        assert validate_manage(hint) == []
        assert hint["session"]["state"] == verb
    finally:
        await control.stop()
        await store.stop()
        server.close()


async def test_clear_isolation_lifts_the_quarantine(tmp_path):
    from deerflow_extension_api import ExtensionRuntimeDeps

    from deerflow_agent_hints.config import parse_config
    from deerflow_agent_hints.feed import EventFeed
    from deerflow_agent_hints.router import build_router
    from deerflow_agent_hints.runtime import PluginRuntime
    from deerflow_agent_hints.service import AgentHintsService
    from deerflow_agent_hints.threads import ThreadMetaReader
    from deerflow_agent_hints.trace import Trace

    server = CaptureServer(status=503)
    config = parse_config(
        {
            "sessionControl": {"threadPollSeconds": 0},
            "controlEndpoint": {"url": server.manage_url, "model": "m"},
            "state": {"path": str(tmp_path / "state.db")},
        }
    )
    trace = Trace()
    store = SessionStore(None, config.state_path)
    control = ControlService(config, store, trace)
    meta = ThreadMetaReader(None)
    runtime = PluginRuntime(config, store, control, meta, trace)
    service = AgentHintsService(config, runtime, store, control, meta, trace, EventFeed())
    await service.start(ExtensionRuntimeDeps(session_factory=None))
    try:
        await store.save(
            SessionFacts(
                session_id="s-iso",
                kind="lead",
                thread_id="s-iso",
                quarantined_reason="compact HTTP 503",
            )
        )
        assert await service.clear_isolation("s-iso") is True
        stored = await store.fetch("s-iso")
        assert stored is not None and stored.quarantined_reason is None
        assert await service.clear_isolation("s-iso") is False  # already clean
        assert await service.clear_isolation("missing") is False
        paths = {route.path for route in build_router(service).routes}
        assert "/api/extension/agent-hints/sessions/{session_id}/clear-isolation" in paths
    finally:
        await service.stop()
        server.close()
