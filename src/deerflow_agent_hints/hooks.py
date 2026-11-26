"""The two middleware contributions that put the hint on the wire.

``MODEL_LOGICAL`` opens one logical call per decision (outside the host retry
loop). ``MODEL_PHYSICAL`` sees the final request and writes the header. The
header is written **in place** into ``request.model_settings``: DeerFlow's
isolated extension wrapper forwards the same request object downstream, so any
replacement returned by the hook would be discarded (see
``docs/implementation-profile.md``, injection self-check).
"""

from __future__ import annotations

import contextvars
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from deerflow_extension_api import (
    AgentBuildContext,
    AgentScope,
    ExtensionData,
    MiddlewarePlacement,
    Placement,
)
from langchain.agents.middleware import AgentMiddleware

from deerflow_agent_hints.runtime import LogicalCall, PluginRuntime
from deerflow_agent_hints.wire import HINT_HEADER, encode

logger = logging.getLogger(__name__)

_CURRENT_CALL: contextvars.ContextVar[LogicalCall | None] = contextvars.ContextVar("deerflow_agent_hints_logical_call", default=None)


class LogicalHintMiddleware(AgentMiddleware):
    """Open the logical call, then account for it after the response."""

    name = "agent_hints_logical"

    def __init__(self, runtime: PluginRuntime) -> None:
        super().__init__()
        self._runtime = runtime

    async def awrap_model_call(self, request: Any, handler: Callable[[Any], Awaitable[Any]]) -> Any:
        call = await self._runtime.begin_logical(request)
        token = _CURRENT_CALL.set(call)
        try:
            try:
                response = await handler(request)
            except BaseException as exc:
                if call is not None:
                    await self._runtime.finish_logical(call, None, exc)
                raise
            if call is not None:
                await self._runtime.finish_logical(call, response, None)
            return response
        finally:
            _CURRENT_CALL.reset(token)


class PhysicalHintMiddleware(AgentMiddleware):
    """Write the frozen hint into the final request's ``extra_headers``."""

    name = "agent_hints_physical"

    def __init__(self, runtime: PluginRuntime) -> None:
        super().__init__()
        self._runtime = runtime

    async def awrap_model_call(self, request: Any, handler: Callable[[Any], Awaitable[Any]]) -> Any:
        call = _CURRENT_CALL.get()
        if call is not None:
            hint = self._runtime.hint_for(request, call)
            if hint is not None:
                settings = request.model_settings
                existing = settings.get("extra_headers") or {}
                if not isinstance(existing, dict):
                    existing = {}
                # In-place: the host forwards this same request object; a
                # returned replacement would be dropped by the isolation layer.
                settings["extra_headers"] = {**existing, HINT_HEADER: encode(hint)}
        return await handler(request)


class HintContributor:
    """Contribute both middlewares to lead and subagent chains."""

    def __init__(self, runtime: PluginRuntime) -> None:
        self._runtime = runtime

    def contribute_middlewares(self, app_store: ExtensionData, ctx: AgentBuildContext) -> Sequence[MiddlewarePlacement]:
        if not self._runtime.config.enabled:
            return ()
        return (
            MiddlewarePlacement(
                LogicalHintMiddleware(self._runtime),
                Placement.MODEL_LOGICAL,
                scope=AgentScope.BOTH,
                order=0,
            ),
            MiddlewarePlacement(
                PhysicalHintMiddleware(self._runtime),
                Placement.MODEL_PHYSICAL,
                scope=AgentScope.BOTH,
                # Innermost at this anchor: another contributor's transform must
                # not run after the final request was measured.
                order=100,
            ),
        )
