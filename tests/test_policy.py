"""Pure derivation tests: TTL, msg_offset, signatures, rewrite detection, ids."""

from __future__ import annotations

from types import SimpleNamespace

from deerflow_agent_hints.policy import (
    derive_ttl_s,
    estimate_tokens,
    head_ids,
    head_rewritten,
    logical_message_count,
    message_ids,
    model_identifier,
    prefix_signature,
    priority_for,
    session_id_for,
    signature_stable,
    tool_names,
)


class Msg:
    def __init__(self, role: str, content: str = "", *, id: str | None = None, tool_calls=None):
        self.type = role
        self.content = content
        self.id = id
        self.tool_calls = tool_calls


def test_ttl_derivation_matches_protocol():
    assert derive_ttl_s(None) == 600  # 300 s fallback gap
    assert derive_ttl_s(0) == 600
    assert derive_ttl_s(1000) == 60  # clamped up
    assert derive_ttl_s(30000) == 60
    assert derive_ttl_s(90000) == 180
    assert derive_ttl_s(10_000_000) == 3600


def test_token_estimate_cjk_and_latin():
    assert estimate_tokens("") == 0
    assert estimate_tokens("abcd") == 1
    assert estimate_tokens("abcde") == 2
    assert estimate_tokens("你好") == 2
    assert estimate_tokens([{"content": "你好abcd"}]) == 3


def test_logical_message_count_counts_system_and_tools():
    messages = [
        Msg("human", "hi", id="1"),
        Msg("ai", "", id="2", tool_calls=[{"name": "t"}]),
        Msg("tool", "result", id="3"),
    ]
    assert logical_message_count(messages, system_present=False) == 3
    assert logical_message_count(messages, system_present=True) == 4


def test_logical_message_count_drops_trailing_empty_ai_placeholder():
    messages = [Msg("human", "hi", id="1"), Msg("ai", "", id="2")]
    assert logical_message_count(messages, system_present=False) == 1
    messages = [Msg("human", "hi", id="1"), Msg("ai", "answer", id="2")]
    assert logical_message_count(messages, system_present=False) == 2


def test_priority_rule():
    assert priority_for(True) == (3, "user_waiting")
    assert priority_for(False) == (1, "critical_path")


def test_head_rewrite_detection():
    previous = ["a", "b", "c"]
    assert not head_rewritten(None, ["x"])
    assert not head_rewritten(previous, ["a", "b", "c", "d"])
    assert head_rewritten(previous, ["summary", "b", "c"])
    assert head_rewritten(previous, ["a", "b"])  # head truncated
    assert head_rewritten(previous, None or [])  # missing ids are unknown, not a rewrite
    ids = [Msg("human", id=str(i)) for i in range(20)]
    assert head_ids(ids) == [str(i) for i in range(8)]
    assert head_ids([Msg("human")]) is None
    assert message_ids(ids)[0] == "0"


def test_head_window_covers_the_front_and_ignores_tail_churn():
    previous = [f"m{i}" for i in range(12)]
    # Churn beyond the front window (tool reminders near the tail) is invisible.
    assert not head_rewritten(previous[:8], [*previous[:8], "new-tool", "new-ai"])
    # Compaction replaces the front with a summary message.
    assert head_rewritten(previous[:8], ["summary", *previous[8:12]])
    # Churn inside the short-conversation window is visible: the runtime only
    # uses this as a conservative cache-suppression signal, never as an epoch.
    assert head_rewritten(["u1", "a1", "todo-1", "u2"], ["u1", "a1", "todo-2", "u2", "a2"])


def test_signature_uses_model_and_sorted_tools():
    tools = [SimpleNamespace(name="b"), SimpleNamespace(name="a")]
    model = SimpleNamespace(model_name="m-1")
    assert tool_names(tools) == ["a", "b"]
    assert prefix_signature(model, tools) == "m-1\x1fa\x1fb"
    assert model_identifier(model) == "m-1"
    assert model_identifier(SimpleNamespace(model="m-2")) == "m-2"
    assert model_identifier(SimpleNamespace()) == "SimpleNamespace"


def test_signature_stability_first_sight_only_allows_call_one():
    assert signature_stable(None, "sig", 1)
    assert not signature_stable(None, "sig", 2)
    assert signature_stable("sig", "sig", 5)
    assert not signature_stable("sig", "other", 5)


def test_session_id_for_subagents_never_collides_with_the_thread():
    assert session_id_for("thread-1", "run-1", None) == "thread-1"
    assert session_id_for("thread-1", "run-1", "task-1") == "thread-1:run-1:task-1"
    assert session_id_for("", "run-1", "task-1") == "run-1:task-1"
    assert len(session_id_for("t" * 200, "r" * 100, "x" * 100)) == 256
