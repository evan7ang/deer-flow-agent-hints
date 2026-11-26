"""Wire-shape tests: the four-block contract, encoding, and validators."""

from __future__ import annotations

import base64
import json

import pytest

from deerflow_agent_hints.wire import (
    HINT_HEADER,
    MANAGE_EDITS,
    decode,
    encode,
    manage_body,
    manage_hint,
    ordinary_hint,
    priority,
    validate_manage,
    validate_ordinary,
)


def test_header_name():
    assert HINT_HEADER == "X-Agent-Hint"


def test_ordinary_start_shape_is_strict():
    hint = ordinary_hint(
        session_id="s-1",
        call_index=1,
        epoch=0,
        state="start",
        priority_pair=(3, "user_waiting"),
        cache=(600, 10),
    )
    assert validate_ordinary(hint) == []
    assert hint == {
        "session": {"session_id": "s-1", "call_index": 1, "epoch": 0, "state": "start"},
        "priority_control": {"priority": {"level": 3, "reason": "user_waiting"}},
        "cache_control": {"type": "ephemeral", "ttl_s": 600, "msg_offset": 10},
    }


def test_ordinary_root_omits_parent_and_blocks_are_optional():
    hint = ordinary_hint(session_id="s-1", call_index=2, epoch=1, state="append")
    assert validate_ordinary(hint) == []
    assert set(hint) == {"session"}
    assert "parent_session_id" not in hint["session"]


def test_ordinary_child_carries_parent():
    hint = ordinary_hint(session_id="s-1:r:t", call_index=1, epoch=0, state="append", parent_session_id="parent")
    assert hint["session"]["parent_session_id"] == "parent"


def test_manage_shape_is_strict_and_maps_edits():
    for verb in ("pause", "resume", "stop", "compact"):
        hint = manage_hint(session_id="s-1", verb=verb, call_index=5, epoch=1)
        assert validate_manage(hint) == [], (verb, validate_manage(hint))
        assert hint["session"]["state"] == verb
        assert hint["context_management"] == {
            "manage_request": True,
            "edits": [{"type": MANAGE_EDITS[verb], "target": "session"}],
        }
        assert "priority_control" not in hint and "cache_control" not in hint


def test_manage_body_is_placeholder():
    assert manage_body("m") == {"model": "m", "messages": [], "stream": False}


def test_encoding_is_unpadded_base64url_json():
    hint = ordinary_hint(session_id="s", call_index=1, epoch=0, state="start", priority_pair=(3, "user_waiting"))
    encoded = encode(hint)
    assert "=" not in encoded and "+" not in encoded and "/" not in encoded
    raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
    assert json.loads(raw) == hint
    assert decode(encoded) == hint


def test_encoding_is_compact_utf8():
    hint = ordinary_hint(session_id="会话-1", call_index=1, epoch=0, state="start")
    encoded = encode(hint)
    raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)).decode("utf-8")
    assert "会话-1" in raw
    assert ", " not in raw and ": " not in raw


def test_priority_pair_validation():
    assert priority(3, "user_waiting") == (3, "user_waiting")
    assert priority(1, "critical_path") == (1, "critical_path")
    for level, reason in ((3, "critical_path"), (1, "user_waiting"), (2, "user_waiting"), (3, "background"), (3, "nope")):
        with pytest.raises(ValueError):
            priority(level, reason)


def test_builder_rejects_out_of_range_values():
    with pytest.raises(ValueError):
        ordinary_hint(session_id="", call_index=1, epoch=0, state="start")
    with pytest.raises(ValueError):
        ordinary_hint(session_id="s", call_index=0, epoch=0, state="start")
    with pytest.raises(ValueError):
        ordinary_hint(session_id="s", call_index=1, epoch=-1, state="start")
    with pytest.raises(ValueError):
        ordinary_hint(session_id="s", call_index=1, epoch=0, state="pause")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        ordinary_hint(session_id="s", call_index=1, epoch=0, state="start", cache=(59, 1))
    with pytest.raises(ValueError):
        ordinary_hint(session_id="s", call_index=1, epoch=0, state="start", cache=(600, 0))


def test_validators_flag_legacy_fields():
    hint = ordinary_hint(session_id="s", call_index=1, epoch=0, state="start")
    hint["agent_status"] = {"state": "running", "state_since_ms": 1}
    assert any("extra top-level" in problem for problem in validate_ordinary(hint))
    hint = ordinary_hint(session_id="s", call_index=1, epoch=0, state="start")
    hint["session"]["agent_type"] = "x"
    assert any("extra session keys" in problem for problem in validate_ordinary(hint))
    hint = ordinary_hint(session_id="s", call_index=1, epoch=0, state="start", cache=(600, 1))
    hint["cache_control"]["range"] = {"end": 1}
    assert any("cache_control must be" in problem for problem in validate_ordinary(hint))
    hint = ordinary_hint(session_id="s", call_index=1, epoch=0, state="start")
    hint["context_management"] = {"manage_request": True, "edits": []}
    assert any("context_management" in problem for problem in validate_ordinary(hint))


def test_validate_manage_flags_wrong_edit():
    hint = manage_hint(session_id="s", verb="pause", call_index=1, epoch=0)
    hint["context_management"]["edits"][0]["type"] = "prefetch"
    assert any("does not match the verb" in problem for problem in validate_manage(hint))
    hint = manage_hint(session_id="s", verb="pause", call_index=1, epoch=0)
    hint["context_management"]["manage_request"] = "true"
    assert any("JSON boolean true" in problem for problem in validate_manage(hint))
