"""Lifecycle contributors: task boundaries and compaction events."""

from __future__ import annotations

from typing import Any

from deerflow_extension_api import ExtensionData, TaskInfo, TaskOutcome

from deerflow_agent_hints.runtime import PluginRuntime


class HintTaskLifecycle:
    """Track run activity and subagent session lifetimes."""

    def __init__(self, runtime: PluginRuntime) -> None:
        self._runtime = runtime

    async def on_task_start(self, app_store: ExtensionData, task_store: ExtensionData, info: TaskInfo) -> None:
        await self._runtime.on_task_start(info)

    async def on_task_stop(
        self,
        app_store: ExtensionData,
        task_store: ExtensionData,
        info: TaskInfo,
        outcome: TaskOutcome,
    ) -> None:
        await self._runtime.on_task_stop(info)


class HintCompactionObserver:
    """Receive committed compactions (the rewrite signal for older hosts)."""

    def __init__(self, runtime: PluginRuntime) -> None:
        self._runtime = runtime

    async def on_context_compacted(self, app_store: ExtensionData, task_store: ExtensionData, event: Any) -> None:
        await self._runtime.on_context_compacted(event, task_store)
