"""deerflow-agent-hints — Agent Hint protocol v1.0 sender for DeerFlow.

One ``X-Agent-Hint`` header per main-line model request (session identity,
lifecycle state, priority, cache intent) plus dedicated manage requests for
``pause`` / ``resume`` / ``stop`` / ``compact``. The request body is never
touched.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from deerflow_extension_api import ExtensionInstall, ExtensionRegistry, extension

from deerflow_agent_hints.config import PluginConfig, parse_config
from deerflow_agent_hints.control import ControlService
from deerflow_agent_hints.feed import EventFeed
from deerflow_agent_hints.hooks import HintContributor
from deerflow_agent_hints.lifecycle import HintCompactionObserver, HintTaskLifecycle
from deerflow_agent_hints.router import build_router
from deerflow_agent_hints.runtime import PluginRuntime
from deerflow_agent_hints.service import AgentHintsService
from deerflow_agent_hints.store import SessionStore
from deerflow_agent_hints.threads import ThreadMetaReader
from deerflow_agent_hints.trace import Trace

logger = logging.getLogger(__name__)

__all__ = ["install", "__version__"]

__version__ = "0.2.0"


@extension(api="0.2.13", name="agent-hints")
def install(registry: ExtensionRegistry, config: Mapping[str, Any]) -> None:
    """Register the hint middlewares, lifecycle observers, service and status route."""
    plugin_config: PluginConfig = parse_config(config)
    if not plugin_config.enabled:
        logger.info("agent-hints: disabled by configuration")
        return

    feed = EventFeed()
    trace = Trace(plugin_config.trace_enabled, plugin_config.trace_file, feed)
    store = SessionStore(None, plugin_config.state_path)
    meta = ThreadMetaReader(None)
    control = ControlService(plugin_config, store, trace)
    runtime = PluginRuntime(plugin_config, store, control, meta, trace)
    service = AgentHintsService(plugin_config, runtime, store, control, meta, trace, feed)

    registry.middlewares(HintContributor(runtime))
    registry.task_lifecycle(HintTaskLifecycle(runtime))
    registry.context_compaction_observer(HintCompactionObserver(runtime))
    registry.service(service)
    registry.routers((build_router(service),))


_entry_point: ExtensionInstall = install
