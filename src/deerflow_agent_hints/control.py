"""Manage-request transport (protocol §7.3, §8.3, §8.4).

Lifecycle verbs travel as their own HTTP request with a placeholder body; the
only payload is the ``X-Agent-Hint`` header. The client is async (DeerFlow runs
on an event loop), serializes per session, retries only ``resume`` on explicit
transient rejections, and quarantines a session whose mutation outcome is
unknown. HTTP 2xx means "accepted", never "KV action completed".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import httpx

from deerflow_agent_hints.config import PluginConfig
from deerflow_agent_hints.policy import model_identifier
from deerflow_agent_hints.store import SessionFacts, SessionStore, now_ms
from deerflow_agent_hints.trace import Trace
from deerflow_agent_hints.wire import HINT_HEADER, ManageVerb, encode, manage_body, manage_hint

logger = logging.getLogger(__name__)

_RETRYABLE = frozenset({408, 429})


@dataclass(frozen=True)
class Endpoint:
    """One manage target plus the credentials to reach it (never persisted)."""

    url: str
    headers: dict[str, str] = field(default_factory=dict)
    model: str = ""
    family: str = "openai"
    base_url: str = ""


def _secret(value: Any) -> str | None:
    if value is None:
        return None
    getter = getattr(value, "get_secret_value", None)
    if callable(getter):
        value = getter()
    text = str(value)
    return text or None


def _chat_url(base: str) -> str:
    base = base.rstrip("/")
    if base.endswith("/chat/completions"):
        return base
    return f"{base}/chat/completions"


def _messages_url(base: str) -> str:
    base = base.rstrip("/")
    if base.endswith("/messages"):
        return base
    if base.endswith("/v1"):
        return f"{base}/messages"
    return f"{base}/v1/messages"


def _base_of(model: Any) -> str | None:
    for attribute in ("openai_api_base", "base_url", "api_base"):
        value = getattr(model, attribute, None)
        if isinstance(value, str) and value:
            return value
    client = getattr(model, "root_async_client", None) or getattr(model, "root_client", None)
    base = getattr(client, "base_url", None) if client is not None else None
    return str(base) if base else None


def model_family(model: Any) -> str | None:
    """``openai`` / ``anthropic`` for models that accept per-request headers."""
    try:
        from langchain_openai.chat_models.base import BaseChatOpenAI

        if isinstance(model, BaseChatOpenAI):
            return "openai"
    except Exception:  # pragma: no cover - optional dependency
        pass
    try:
        from langchain_anthropic import ChatAnthropic

        if isinstance(model, ChatAnthropic):
            return "anthropic"
    except Exception:  # pragma: no cover - optional dependency
        pass
    return None


def endpoint_from_model(model: Any) -> Endpoint | None:
    """Derive the manage target from a live model instance (secrets stay in memory)."""
    family = model_family(model)
    if family is None:
        return None
    headers = {str(key): str(value) for key, value in (getattr(model, "default_headers", None) or {}).items()}
    model_name = model_identifier(model)
    if family == "openai":
        base = _base_of(model) or "https://api.openai.com/v1"
        key = _secret(getattr(model, "openai_api_key", None))
        if key and not any(name.lower() == "authorization" for name in headers):
            headers["Authorization"] = f"Bearer {key}"
        return Endpoint(url=_chat_url(base), headers=headers, model=model_name, family=family, base_url=base)
    base = getattr(model, "anthropic_api_url", None) or getattr(model, "base_url", None) or "https://api.anthropic.com"
    key = _secret(getattr(model, "anthropic_api_key", None) or getattr(model, "api_key", None))
    headers.setdefault("anthropic-version", "2023-06-01")
    if key and not any(name.lower() in ("x-api-key", "authorization") for name in headers):
        headers["x-api-key"] = key
    return Endpoint(url=_messages_url(str(base)), headers=headers, model=model_name, family=family, base_url=str(base))


def endpoint_from_config(facts: SessionFacts) -> Endpoint | None:
    """Rebuild an endpoint for a session without a live model instance."""
    if not facts.model_name:
        return None
    try:
        from deerflow.config.app_config import get_app_config
    except Exception:  # pragma: no cover - standalone import
        return None
    try:
        config = get_app_config()
        model_config = config.get_model_config(facts.model_name)
    except Exception:
        logger.debug("agent-hints: cannot resolve model %r for manage", facts.model_name, exc_info=True)
        return None
    if model_config is None:
        return None
    family = facts.family or "openai"
    base = (
        getattr(model_config, "api_base", None)
        or getattr(model_config, "base_url", None)
        or facts.base_url
        or ("https://api.openai.com/v1" if family == "openai" else "https://api.anthropic.com")
    )
    key = _secret(getattr(model_config, "api_key", None))
    headers: dict[str, str] = {str(k): str(v) for k, v in (getattr(model_config, "default_headers", None) or {}).items()}
    if family == "openai":
        if key:
            headers.setdefault("Authorization", f"Bearer {key}")
        return Endpoint(url=_chat_url(str(base)), headers=headers, model=facts.model_name, family=family, base_url=str(base))
    headers.setdefault("anthropic-version", "2023-06-01")
    if key:
        headers.setdefault("x-api-key", key)
    return Endpoint(url=_messages_url(str(base)), headers=headers, model=facts.model_name, family=family, base_url=str(base))


class ControlService:
    """Send manage requests and remember what can only be known in-process."""

    def __init__(self, config: PluginConfig, store: SessionStore, trace: Trace) -> None:
        self._config = config
        self._store = store
        self._trace = trace
        self._client: httpx.AsyncClient | None = None
        self._endpoints: dict[str, Endpoint] = {}
        self._locks: dict[str, Any] = {}
        self._counters: dict[str, int] = {}
        self._loop: Any | None = None

    async def start(self) -> None:
        import asyncio

        self._loop = asyncio.get_running_loop()
        timeout = max(0.1, self._config.session_control.control_timeout_ms / 1000.0)
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(timeout))

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # -- endpoint + lock plumbing -------------------------------------------

    def remember(self, session_id: str, endpoint: Endpoint | None) -> None:
        if endpoint is not None:
            self._endpoints[session_id] = endpoint

    def forget(self, session_id: str) -> None:
        self._endpoints.pop(session_id, None)

    def _lock(self, session_id: str) -> Any:
        import asyncio

        lock = self._locks.get(session_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[session_id] = lock
        return lock

    def _endpoint(self, facts: SessionFacts) -> Endpoint | None:
        override = self._config.control_endpoint
        if override:
            headers = {str(k): str(v) for k, v in (override.get("headers") or {}).items()}
            key = override.get("apiKey")
            if key:
                headers.setdefault("Authorization", f"Bearer {key}")
            return Endpoint(
                url=str(override["url"]),
                headers=headers,
                model=str(override.get("model") or facts.model_name or "manage"),
                family="control",
                base_url=str(override["url"]),
            )
        remembered = self._endpoints.get(facts.session_id)
        if remembered is not None:
            return remembered
        return endpoint_from_config(facts)

    async def wait_idle(self, session_id: str, timeout_ms: int) -> bool:
        """Request-side barrier: wait for an in-flight mutation, bounded."""
        import asyncio

        lock = self._lock(session_id)
        if not lock.locked():
            return True
        if timeout_ms <= 0:
            return False
        try:
            await asyncio.wait_for(lock.acquire(), timeout_ms / 1000.0)
        except TimeoutError:
            logger.warning("agent-hints: session %s manage barrier timed out after %sms", session_id, timeout_ms)
            return False
        lock.release()
        return True

    def fire_and_forget(self, verb: ManageVerb, facts: SessionFacts, *, call_index: int, epoch: int) -> None:
        """Send without awaiting, from any loop/thread (subagent hooks, sweeps)."""
        import asyncio

        loop = self._loop
        if loop is None or loop.is_closed():
            return
        coro = self.send(verb, facts, call_index=call_index, epoch=epoch)
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        try:
            if running is loop:
                loop.create_task(coro)
            else:
                asyncio.run_coroutine_threadsafe(coro, loop)
        except Exception:  # pragma: no cover - loop shutting down
            coro.close()
            logger.debug("agent-hints: could not dispatch manage %s", verb, exc_info=True)

    def counts(self) -> dict[str, int]:
        return dict(self._counters)

    def _count(self, key: str) -> None:
        self._counters[key] = self._counters.get(key, 0) + 1

    # -- send ----------------------------------------------------------------

    async def send(
        self,
        verb: ManageVerb,
        facts: SessionFacts,
        *,
        call_index: int,
        epoch: int,
    ) -> str:
        """Send one manage request. Returns ``accepted`` / ``failed`` / ``unknown``."""
        if self._client is None:
            return "failed"
        endpoint = self._endpoint(facts)
        if endpoint is None:
            logger.info("agent-hints: no manage endpoint for session %s (%s)", facts.session_id, verb)
            self._count(f"{verb}:skipped")
            return "failed"
        hint = manage_hint(
            session_id=facts.session_id,
            verb=verb,
            call_index=max(1, int(call_index)),
            epoch=max(0, int(epoch)),
            parent_session_id=facts.parent_session_id,
        )
        payload = encode(hint)
        headers = {**endpoint.headers, "Content-Type": "application/json", HINT_HEADER: payload}
        body = manage_body(endpoint.model or facts.model_name or "manage")
        attempts = 1 + (self._config.session_control.retries if verb == "resume" else 0)
        lock = self._lock(facts.session_id)
        async with lock:
            for attempt in range(attempts):
                try:
                    response = await self._client.post(endpoint.url, json=body, headers=headers)
                except (httpx.HTTPError, OSError) as exc:
                    transient = verb == "resume"
                    if transient and attempt + 1 < attempts:
                        continue
                    return await self._finish(
                        verb,
                        facts,
                        "unknown" if not transient else "failed",
                        detail=type(exc).__name__,
                        payload=payload,
                        call_index=call_index,
                        epoch=epoch,
                    )
                code = response.status_code
                if 200 <= code < 300:
                    self._record(verb, facts, call_index, epoch, "accepted", code, hint)
                    return "accepted"
                if verb == "resume" and (code in _RETRYABLE or 500 <= code < 600):
                    if attempt + 1 < attempts:
                        continue
                    self._record(verb, facts, call_index, epoch, "failed", code, hint)
                    return "failed"
                if 400 <= code < 500:
                    self._record(verb, facts, call_index, epoch, "failed", code, hint)
                    return "failed"
                return await self._finish(
                    verb,
                    facts,
                    "unknown",
                    detail=f"HTTP {code}",
                    payload=payload,
                    status=code,
                    hint=hint,
                    call_index=call_index,
                    epoch=epoch,
                )
        return "failed"

    def _record(
        self,
        verb: ManageVerb,
        facts: SessionFacts,
        call_index: int,
        epoch: int,
        outcome: str,
        status: int | None,
        hint: dict[str, Any] | None = None,
    ) -> None:
        self._trace.emit(
            {
                "event": "manage",
                "hint": hint,
                "verb": verb,
                "session_id": facts.session_id,
                "call_index": call_index,
                "epoch": epoch,
                "outcome": outcome,
                "status": status,
            }
        )
        self._count(f"{verb}:{outcome}")

    async def _finish(
        self,
        verb: ManageVerb,
        facts: SessionFacts,
        outcome: str,
        *,
        detail: str,
        payload: str,
        status: int | None = None,
        call_index: int = 1,
        epoch: int = 0,
        hint: dict[str, Any] | None = None,
    ) -> str:
        if outcome == "unknown":
            reason = f"{verb} {detail}"
            facts.quarantined_reason = reason
            await self._store.set_flags(facts.session_id, quarantined_reason=reason)
            logger.warning(
                "agent-hints: manage %s for session %s has an unknown outcome (%s); session quarantined",
                verb,
                facts.session_id,
                detail,
            )
        self._trace.emit(
            {
                "event": "manage",
                "hint": hint,
                "verb": verb,
                "session_id": facts.session_id,
                "call_index": call_index,
                "epoch": epoch,
                "outcome": outcome,
                "status": status,
                "detail": detail,
                "hint_bytes": len(payload),
                "at_ms": now_ms(),
            }
        )
        self._count(f"{verb}:{outcome}")
        return outcome
