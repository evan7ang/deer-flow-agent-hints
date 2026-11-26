"""Configuration parsing: defaults, overrides, and load-time failures."""

from __future__ import annotations

import pytest

from deerflow_agent_hints.config import model_allowed, parse_config


def test_defaults():
    config = parse_config(None)
    assert config.enabled is True
    assert config.priority == "dynamic"
    assert config.cache == "dynamic"
    assert config.session_control.enabled is True
    assert config.session_control.pending_op_await_ms == 3000
    assert config.session_control.control_timeout_ms == 2000
    assert config.session_control.retries == 1
    assert config.session_control.thread_poll_seconds == 3.0
    assert config.models == ()
    assert model_allowed(config, "anything")


def test_priority_and_cache_forms():
    assert parse_config({"priority": "omit"}).priority == "omit"
    assert parse_config({"priority": None}).priority is None
    assert parse_config({"priority": {"level": 1, "reason": "critical_path"}}).priority == (1, "critical_path")
    assert parse_config({"cacheControl": "omit"}).cache is None
    assert parse_config({"cacheControl": {"ttl_s": 240}}).cache == (240, 0)
    assert parse_config({"cacheControl": {"ttl_s": 240, "msg_offset": 12}}).cache == (240, 12)


def test_models_globs():
    config = parse_config({"models": ["doubao-*", "gpt-4o"]})
    assert model_allowed(config, "doubao-seed")
    assert model_allowed(config, "gpt-4o")
    assert not model_allowed(config, "claude-3-5")


def test_session_control_and_paths():
    config = parse_config(
        {
            "sessionControl": {"pendingOpAwaitMs": 0, "threadPollSeconds": 0, "retries": 0},
            "state": {"path": "/tmp/x.db"},
            "trace": {"enabled": True, "file": "/tmp/trace.jsonl"},
        }
    )
    assert config.session_control.pending_op_await_ms == 0
    assert config.session_control.thread_poll_seconds == 0
    assert config.session_control.retries == 0
    assert config.state_path == "/tmp/x.db"
    assert config.trace_enabled and config.trace_file == "/tmp/trace.jsonl"


def test_endpoint_override():
    config = parse_config({"controlEndpoint": {"url": "http://x/chat/completions", "apiKey": "k", "headers": {"a": "b"}}})
    assert config.control_endpoint == {"url": "http://x/chat/completions", "apiKey": "k", "headers": {"a": "b"}}


@pytest.mark.parametrize(
    "raw",
    [
        {"unknown": 1},
        {"priority": "sometimes"},
        {"priority": {"level": 2, "reason": "user_waiting"}},
        {"priority": {"level": 3, "reason": "critical_path"}},
        {"cacheControl": {"ttl_s": 10}},
        {"cacheControl": {"ttl_s": 600, "msg_offset": 0}},
        {"models": "doubao"},
        {"sessionControl": {"enabled": "yes"}},
        {"sessionControl": {"pendingOpAwaitMs": -1}},
        {"controlEndpoint": {"url": ""}},
        {"trace": {"enabled": 1}},
    ],
)
def test_invalid_configuration_fails_loudly(raw):
    with pytest.raises(ValueError):
        parse_config(raw)
