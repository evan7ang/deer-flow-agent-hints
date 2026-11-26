"""Agent Hint v1.0 wire objects — strict four-block contract.

Protocol: ``agent-hint-protocol.md`` v1.0 (§3 transport, §5 fields, §8.2 verbs).

Ordinary requests carry ``session`` + ``priority_control`` (+ ``cache_control``
when it qualifies). Manage requests carry ``session`` + ``context_management``.
No other top-level key is ever emitted: the protocol requires exactly these four
blocks and forbids the legacy ``agent_status`` / ``prediction`` /
``strict_priority`` vocabulary.

Pure stdlib: no harness imports, so the wire shape is unit-testable on its own.
"""

from __future__ import annotations

import base64
import json
from typing import Any, Literal

HINT_HEADER = "X-Agent-Hint"

OrdinaryState = Literal["start", "append"]
ManageVerb = Literal["pause", "resume", "stop", "compact"]
SessionState = Literal["start", "append", "pause", "resume", "stop", "compact"]

#: Fixed verb -> edit mapping (protocol §8.2).
MANAGE_EDITS: dict[str, Literal["offload", "prefetch", "evict"]] = {
    "pause": "offload",
    "resume": "prefetch",
    "stop": "evict",
    "compact": "evict",
}

#: The only four top-level keys v1 allows (protocol §1.3 rule 3).
TOP_LEVEL_KEYS = frozenset({"session", "context_management", "priority_control", "cache_control"})

#: Legal reasons and their fixed levels (protocol §5.4).
PRIORITY_REASONS: dict[str, int] = {"user_waiting": 3, "critical_path": 1}


def encode(hint: dict[str, Any]) -> str:
    """Serialize one hint: compact UTF-8 JSON -> unpadded base64url (§3.2)."""
    raw = json.dumps(hint, separators=(",", ":"), ensure_ascii=False)
    return base64.urlsafe_b64encode(raw.encode("utf-8")).rstrip(b"=").decode("ascii")


def decode(encoded: str) -> dict[str, Any]:
    """Inverse of :func:`encode` (test/verification helper; tolerates padding)."""
    padded = encoded + "=" * (-len(encoded) % 4)
    return json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))


def priority(level: int, reason: str) -> tuple[int, str]:
    """Validate a ``(level, reason)`` pair against the protocol's fixed pairing."""
    if level not in (1, 3):
        raise ValueError(f"priority level must be 1 or 3, got {level!r}")
    expected = PRIORITY_REASONS.get(reason)
    if expected is None:
        raise ValueError(f"priority reason must be user_waiting or critical_path, got {reason!r}")
    if expected != level:
        raise ValueError(f"priority pair is inconsistent: level {level} with reason {reason!r}")
    return level, reason


def ordinary_hint(
    *,
    session_id: str,
    call_index: int,
    epoch: int,
    state: OrdinaryState,
    parent_session_id: str | None = None,
    priority_pair: tuple[int, str] | None = None,
    cache: tuple[int, int] | None = None,
) -> dict[str, Any]:
    """Build one ordinary-request hint (``session`` + optional blocks)."""
    if not session_id:
        raise ValueError("session_id is required")
    if call_index < 1:
        raise ValueError(f"call_index must be >= 1, got {call_index!r}")
    if epoch < 0:
        raise ValueError(f"epoch must be >= 0, got {epoch!r}")
    if state not in ("start", "append"):
        raise ValueError(f"ordinary request state must be start or append, got {state!r}")

    session: dict[str, Any] = {
        "session_id": session_id,
        "call_index": int(call_index),
        "epoch": int(epoch),
        "state": state,
    }
    # Root sessions omit the key (protocol §4.2 recommends omission; the
    # appendix A schema does not accept an explicit null).
    if parent_session_id:
        session["parent_session_id"] = parent_session_id

    hint: dict[str, Any] = {"session": session}
    if priority_pair is not None:
        level, reason = priority(priority_pair[0], priority_pair[1])
        hint["priority_control"] = {"priority": {"level": level, "reason": reason}}
    if cache is not None:
        ttl_s, msg_offset = cache
        if not 60 <= int(ttl_s) <= 3600:
            raise ValueError(f"ttl_s must be within [60, 3600], got {ttl_s!r}")
        if int(msg_offset) < 1:
            raise ValueError(f"msg_offset must be >= 1, got {msg_offset!r}")
        hint["cache_control"] = {
            "type": "ephemeral",
            "ttl_s": int(ttl_s),
            "msg_offset": int(msg_offset),
        }
    return hint


def manage_hint(
    *,
    session_id: str,
    verb: ManageVerb,
    call_index: int,
    epoch: int,
    parent_session_id: str | None = None,
) -> dict[str, Any]:
    """Build one manage-request hint (``session`` + ``context_management``)."""
    if not session_id:
        raise ValueError("session_id is required")
    if verb not in MANAGE_EDITS:
        raise ValueError(f"unknown manage verb {verb!r}")
    if call_index < 1:
        raise ValueError(f"call_index must be >= 1, got {call_index!r}")
    if epoch < 0:
        raise ValueError(f"epoch must be >= 0, got {epoch!r}")

    session: dict[str, Any] = {
        "session_id": session_id,
        "call_index": int(call_index),
        "epoch": int(epoch),
        "state": verb,
    }
    if parent_session_id:
        session["parent_session_id"] = parent_session_id
    return {
        "session": session,
        "context_management": {
            "manage_request": True,
            "edits": [{"type": MANAGE_EDITS[verb], "target": "session"}],
        },
    }


def manage_body(model: str) -> dict[str, Any]:
    """The placeholder body a manage request must carry (protocol §3.4)."""
    return {"model": model, "messages": [], "stream": False}


def validate_ordinary(hint: dict[str, Any]) -> list[str]:
    """Return protocol violations for one ordinary hint (empty means conformant)."""
    problems: list[str] = []
    extra = set(hint) - TOP_LEVEL_KEYS
    if extra:
        problems.append(f"extra top-level keys: {sorted(extra)}")
    if "context_management" in hint:
        problems.append("context_management must not appear on an ordinary request")
    session = hint.get("session")
    if not isinstance(session, dict):
        return [*problems, "session block missing or not an object"]
    allowed = {"session_id", "parent_session_id", "call_index", "epoch", "state"}
    extra_session = set(session) - allowed
    if extra_session:
        problems.append(f"extra session keys: {sorted(extra_session)}")
    for key in ("session_id", "call_index", "epoch", "state"):
        if key not in session:
            problems.append(f"session.{key} is required")
    if session.get("state") not in ("start", "append"):
        problems.append(f"session.state must be start|append, got {session.get('state')!r}")
    if not isinstance(session.get("call_index"), int) or session.get("call_index", 0) < 1:
        problems.append("session.call_index must be an integer >= 1")
    if not isinstance(session.get("epoch"), int) or session.get("epoch", -1) < 0:
        problems.append("session.epoch must be an integer >= 0")
    if "parent_session_id" in session and not isinstance(session["parent_session_id"], str):
        problems.append("session.parent_session_id must be a string when present")
    pc = hint.get("priority_control")
    if pc is not None:
        if not isinstance(pc, dict) or set(pc) != {"priority"}:
            problems.append("priority_control must be {priority: {...}}")
        else:
            p = pc.get("priority")
            if not isinstance(p, dict) or set(p) != {"level", "reason"}:
                problems.append("priority must be {level, reason}")
            else:
                try:
                    priority(int(p["level"]), str(p["reason"]))
                except (ValueError, TypeError) as exc:
                    problems.append(f"invalid priority pair: {exc}")
    cc = hint.get("cache_control")
    if cc is not None:
        if not isinstance(cc, dict) or set(cc) != {"type", "ttl_s", "msg_offset"}:
            problems.append("cache_control must be {type, ttl_s, msg_offset}")
        else:
            if cc.get("type") != "ephemeral":
                problems.append("cache_control.type must be ephemeral")
            if not isinstance(cc.get("ttl_s"), int) or not 60 <= cc.get("ttl_s", 0) <= 3600:
                problems.append("cache_control.ttl_s must be an integer within [60, 3600]")
            if not isinstance(cc.get("msg_offset"), int) or cc.get("msg_offset", 0) < 1:
                problems.append("cache_control.msg_offset must be an integer >= 1")
    return problems


def validate_manage(hint: dict[str, Any]) -> list[str]:
    """Return protocol violations for one manage hint (empty means conformant)."""
    problems: list[str] = []
    extra = set(hint) - TOP_LEVEL_KEYS
    if extra:
        problems.append(f"extra top-level keys: {sorted(extra)}")
    if "priority_control" in hint or "cache_control" in hint:
        problems.append("manage requests must not carry priority_control or cache_control")
    session = hint.get("session")
    if not isinstance(session, dict):
        return [*problems, "session block missing or not an object"]
    if set(session) - {"session_id", "parent_session_id", "call_index", "epoch", "state"}:
        problems.append(f"extra session keys: {sorted(set(session) - {'session_id', 'parent_session_id', 'call_index', 'epoch', 'state'})}")
    if session.get("state") not in ("pause", "resume", "stop", "compact"):
        problems.append(f"manage state must be a lifecycle verb, got {session.get('state')!r}")
    cm = hint.get("context_management")
    if not isinstance(cm, dict):
        return [*problems, "context_management block missing"]
    if cm.get("manage_request") is not True:
        problems.append("context_management.manage_request must be the JSON boolean true")
    edits = cm.get("edits")
    if not isinstance(edits, list) or len(edits) != 1:
        problems.append("context_management.edits must carry exactly one edit")
    else:
        edit = edits[0]
        if not isinstance(edit, dict) or set(edit) != {"type", "target"}:
            problems.append("each edit must be {type, target}")
        else:
            if edit.get("type") not in ("offload", "prefetch", "evict"):
                problems.append(f"invalid edit type {edit.get('type')!r}")
            if edit.get("target") != "session":
                problems.append("edit target must be session")
            if edit.get("type") != MANAGE_EDITS.get(str(session.get("state"))):
                problems.append("edit type does not match the verb")
    return problems
