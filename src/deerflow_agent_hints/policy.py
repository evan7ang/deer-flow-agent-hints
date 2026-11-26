"""Value derivation — the pure half of protocol §6.

Every function here is deterministic over request facts, so the same logical
call always produces the same hint (retries included). No harness imports.
"""

from __future__ import annotations

from typing import Any

#: One engine cache block is 128 tokens; smaller prefixes are not declared.
MIN_CACHE_TOKENS = 128
TTL_MIN_S = 60
TTL_MAX_S = 3600
#: Gap fallback when there is no observed interval (§6.8).
FALLBACK_GAP_S = 300.0


def estimate_tokens(value: Any) -> int:
    """Tokenizer-free size estimate (protocol §6.8 note).

    CJK characters count as one token each, everything else as one token per
    four characters. Walks strings, message objects (``.content``), lists and
    dicts so the same helper serves every provider shape.
    """
    cjk = 0
    other = 0

    def walk(item: Any) -> None:
        nonlocal cjk, other
        if item is None:
            return
        if isinstance(item, str):
            for char in item:
                code = ord(char)
                if 0x2E80 <= code <= 0x9FFF or 0xF900 <= code <= 0xFAFF or 0xFF00 <= code <= 0xFFEF:
                    cjk += 1
                else:
                    other += 1
        elif isinstance(item, (list, tuple)):
            for child in item:
                walk(child)
        elif isinstance(item, dict):
            for child in item.values():
                walk(child)
        elif hasattr(item, "content"):
            walk(item.content)
        elif hasattr(item, "text"):
            walk(item.text)

    walk(value)
    return cjk + (other + 3) // 4


def logical_message_count(messages: Any, *, system_present: bool) -> int:
    """Logical input count for ``msg_offset``: ``[system?] + messages``.

    A trailing empty assistant placeholder (no content, no tool calls) is not
    part of the logical input and is not counted. Tool results count as one
    entry each, which is what the consumer validates against.
    """
    kept = list(messages or [])
    if kept:
        last = kept[-1]
        content = getattr(last, "content", "")
        tool_calls = getattr(last, "tool_calls", None)
        role = str(getattr(last, "type", "") or getattr(last, "role", ""))
        if "ai" in role and not tool_calls and not str(content or "").strip():
            kept = kept[:-1]
    return len(kept) + (1 if system_present else 0)


def derive_ttl_s(interval_ms: int | float | None) -> int:
    """TTL from the observed prefix interval (§6.8): ``clamp(round(gap*2), 60, 3600)``.

    ``interval_ms`` is "previous main-line response end -> this call start".
    No sample means the 300 s fallback gap.
    """
    gap_s = float(interval_ms) / 1000.0 if interval_ms is not None and interval_ms > 0 else FALLBACK_GAP_S
    return max(TTL_MIN_S, min(TTL_MAX_S, int(round(gap_s * 2.0))))


def priority_for(turn_opening: bool) -> tuple[int, str]:
    """The protocol's dynamic priority rule (§6.7)."""
    return (3, "user_waiting") if turn_opening else (1, "critical_path")


def message_ids(messages: Any) -> list[str] | None:
    """Stable message ids in order, or ``None`` when any id is missing."""
    ids: list[str] = []
    for message in messages or []:
        identifier = getattr(message, "id", None)
        if identifier is None and isinstance(message, dict):
            identifier = message.get("id")
        if not identifier:
            return None
        ids.append(str(identifier))
    return ids


def head_ids(messages: Any, *, limit: int = 8) -> list[str] | None:
    """Head fingerprint used for rewrite detection.

    Only the durable state head is compared: compaction replaces the front of
    the history (summary message first), while ordinary tail churn (tool
    reminders, dynamic context) does not move it.
    """
    ids = message_ids(messages)
    return None if ids is None else ids[:limit]


def head_rewritten(previous: list[str] | None, current: list[str] | None) -> bool:
    """Whether the head of the history changed since the previous call."""
    if previous is None or current is None:
        return False
    return current[: len(previous)] != list(previous)


def model_identifier(model: Any) -> str:
    """Model id for the prefix signature (``model_name`` / ``model`` / class)."""
    for attribute in ("model_name", "model", "model_id"):
        value = getattr(model, attribute, None)
        if isinstance(value, str) and value:
            return value
    return type(model).__name__


def tool_names(tools: Any) -> list[str]:
    """Sorted tool names visible to the final request."""
    names: list[str] = []
    for tool in tools or []:
        name = getattr(tool, "name", None)
        if name is None and isinstance(tool, dict):
            name = tool.get("name") or (tool.get("function") or {}).get("name")
        names.append(str(name or type(tool).__name__))
    return sorted(names)


def prefix_signature(model: Any, tools: Any) -> str:
    """``model id + sorted tool names`` (§6.8)."""
    return "\x1f".join([model_identifier(model), *tool_names(tools)])


def signature_stable(previous: str | None, current: str, call_index: int) -> bool:
    """Whether the prefix may be reused (§6.8, C-07 for the restart case)."""
    if previous is None:
        return call_index <= 1
    return previous == current


def session_id_for(thread_id: str, run_id: str | None, task_id: str | None) -> str:
    """Session identity: lead runs use the thread id; subagents get their own.

    A subagent owns a fresh context per execution (DeerFlow never resumes it),
    so it is a separate session (protocol §4.2, S-08) — never a child of the
    thread whose task spawned it.
    """
    if not task_id:
        return thread_id
    prefix = thread_id or ""
    parts = [part for part in (prefix, run_id, task_id) if part]
    return ":".join(parts)[:256]
