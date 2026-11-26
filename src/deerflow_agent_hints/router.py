"""Operator status route (read-only; the host applies its own auth)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from deerflow_agent_hints.service import AgentHintsService


def build_router(service: AgentHintsService) -> APIRouter:
    router = APIRouter(prefix="/api/extension/agent-hints", tags=["agent-hints"])

    @router.get("/status")
    async def read_status() -> dict[str, Any]:
        return await service.status()

    @router.post("/sessions/{session_id}/clear-isolation")
    async def clear_isolation(session_id: str) -> dict[str, Any]:
        """Forget an isolation record after an operator confirmed the KV state."""
        cleared = await service.clear_isolation(session_id)
        return {"session_id": session_id, "cleared": cleared}

    return router
