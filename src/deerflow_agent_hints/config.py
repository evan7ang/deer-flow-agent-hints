"""Plugin configuration — parsed and validated once, at install time.

Invalid configuration raises instead of degrading silently (protocol §12.3).
The extension is registered as optional by the operator, so a raise here fails
open at Gateway startup with a diagnostic naming this extension.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

_TOP_LEVEL_KEYS = {
    "enabled",
    "priority",
    "cacheControl",
    "models",
    "sessionControl",
    "controlEndpoint",
    "state",
    "trace",
}
_SESSION_KEYS = {"enabled", "pendingOpAwaitMs", "controlTimeoutMs", "retries", "threadPollSeconds"}
_ENDPOINT_KEYS = {"url", "apiKey", "model", "headers"}
_STATE_KEYS = {"path"}
_TRACE_KEYS = {"enabled", "file"}


@dataclass(frozen=True)
class SessionControl:
    """Manage-request behaviour."""

    enabled: bool = True
    pending_op_await_ms: int = 3000
    control_timeout_ms: int = 2000
    retries: int = 1
    thread_poll_seconds: float = 3.0


@dataclass(frozen=True)
class PluginConfig:
    enabled: bool = True
    #: ``"dynamic"``, ``"omit"`` (no block), or a fixed ``(level, reason)``.
    priority: str | tuple[int, str] | None = "dynamic"
    #: ``"dynamic"``, ``"omit"``/``None``, or a fixed ``(ttl_s, msg_offset)``.
    cache: str | tuple[int, int] | None = "dynamic"
    #: Model-name globs; empty means every supported model family.
    models: tuple[str, ...] = ()
    session_control: SessionControl = field(default_factory=SessionControl)
    #: Explicit manage target overriding per-model derivation.
    control_endpoint: Mapping[str, Any] | None = None
    #: Where the session table lives when the host database is unavailable.
    state_path: str = ".deer-flow/agent-hints.db"
    trace_enabled: bool = False
    trace_file: str | None = None


def _bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean, got {type(value).__name__}")
    return value


def _positive_int(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer, got {value!r}")
    return value


def _non_negative_int(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer, got {value!r}")
    return value


def _reject_unknown(mapping: Mapping[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(mapping) - allowed)
    if unknown:
        raise ValueError(f"unknown {where} option(s): {', '.join(unknown)}")


def _parse_priority(value: Any) -> str | tuple[int, str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        if value not in ("dynamic", "omit"):
            raise ValueError(f"priority must be dynamic, omit, or {{level, reason}}; got {value!r}")
        return value
    if isinstance(value, Mapping):
        _reject_unknown(value, {"level", "reason"}, "priority")
        level = value.get("level")
        reason = value.get("reason")
        if level not in (1, 3):
            raise ValueError(f"priority.level must be 1 or 3, got {level!r}")
        expected = {"user_waiting": 3, "critical_path": 1}.get(str(reason))
        if expected is None or expected != level:
            raise ValueError(f"priority.reason must pair with the level (user_waiting=3, critical_path=1), got {reason!r}")
        return (int(level), str(reason))
    raise ValueError(f"priority must be a string or a mapping, got {type(value).__name__}")


def _parse_cache(value: Any) -> str | tuple[int, int] | None:
    if value is None:
        return None
    if isinstance(value, str):
        if value not in ("dynamic", "omit"):
            raise ValueError(f"cacheControl must be dynamic, omit, or {{ttl_s, msg_offset}}; got {value!r}")
        return None if value == "omit" else value
    if isinstance(value, Mapping):
        _reject_unknown(value, {"ttl_s", "msg_offset"}, "cacheControl")
        ttl = value.get("ttl_s")
        if not isinstance(ttl, int) or isinstance(ttl, bool) or not 60 <= ttl <= 3600:
            raise ValueError(f"cacheControl.ttl_s must be an integer within [60, 3600], got {ttl!r}")
        offset = value.get("msg_offset")
        if offset is not None and (not isinstance(offset, int) or isinstance(offset, bool) or offset < 1):
            raise ValueError(f"cacheControl.msg_offset must be an integer >= 1, got {offset!r}")
        return (int(ttl), int(offset)) if offset is not None else (int(ttl), 0)
    raise ValueError(f"cacheControl must be a string or a mapping, got {type(value).__name__}")


def parse_config(raw: Mapping[str, Any] | None) -> PluginConfig:
    """Parse the ``plugins[].config`` mapping; raise ``ValueError`` when invalid."""
    mapping: Mapping[str, Any] = raw or {}
    if not isinstance(mapping, Mapping):
        raise ValueError("extension config must be a mapping")
    _reject_unknown(mapping, _TOP_LEVEL_KEYS, "config")

    session_raw = mapping.get("sessionControl") or {}
    if not isinstance(session_raw, Mapping):
        raise ValueError("sessionControl must be a mapping")
    _reject_unknown(session_raw, _SESSION_KEYS, "sessionControl")
    session = SessionControl(
        enabled=_bool(session_raw.get("enabled", True), "sessionControl.enabled"),
        pending_op_await_ms=_non_negative_int(session_raw.get("pendingOpAwaitMs", 3000), "sessionControl.pendingOpAwaitMs"),
        control_timeout_ms=_positive_int(session_raw.get("controlTimeoutMs", 2000), "sessionControl.controlTimeoutMs"),
        retries=_non_negative_int(session_raw.get("retries", 1), "sessionControl.retries"),
        thread_poll_seconds=float(session_raw.get("threadPollSeconds", 3.0)),
    )
    if session.thread_poll_seconds < 0:
        raise ValueError("sessionControl.threadPollSeconds must be >= 0")

    endpoint = mapping.get("controlEndpoint")
    if endpoint is not None:
        if not isinstance(endpoint, Mapping):
            raise ValueError("controlEndpoint must be a mapping")
        _reject_unknown(endpoint, _ENDPOINT_KEYS, "controlEndpoint")
        if not isinstance(endpoint.get("url"), str) or not endpoint["url"]:
            raise ValueError("controlEndpoint.url is required")

    state_raw = mapping.get("state") or {}
    if not isinstance(state_raw, Mapping):
        raise ValueError("state must be a mapping")
    _reject_unknown(state_raw, _STATE_KEYS, "state")

    trace_raw = mapping.get("trace") or {}
    if not isinstance(trace_raw, Mapping):
        raise ValueError("trace must be a mapping")
    _reject_unknown(trace_raw, _TRACE_KEYS, "trace")

    models = mapping.get("models") or []
    if not isinstance(models, list) or not all(isinstance(item, str) for item in models):
        raise ValueError("models must be a list of model-name globs")

    return PluginConfig(
        enabled=_bool(mapping.get("enabled", True), "enabled"),
        priority=_parse_priority(mapping.get("priority", "dynamic")),
        cache=_parse_cache(mapping.get("cacheControl", "dynamic")),
        models=tuple(models),
        session_control=session,
        control_endpoint=dict(endpoint) if endpoint is not None else None,
        state_path=str(state_raw.get("path", ".deer-flow/agent-hints.db")),
        trace_enabled=_bool(trace_raw.get("enabled", False), "trace.enabled"),
        trace_file=str(trace_raw["file"]) if trace_raw.get("file") else None,
    )


def model_allowed(config: PluginConfig, model_name: str) -> bool:
    """Whether a model name passes the configured glob filter."""
    if not config.models:
        return True
    from fnmatch import fnmatchcase

    return any(fnmatchcase(model_name, pattern) for pattern in config.models)
