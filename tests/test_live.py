"""Live feed, live page route, and the demo upstream's echo mode."""

from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "e2e"))

from deerflow_agent_hints.feed import EventFeed  # noqa: E402
from deerflow_agent_hints.router import LIVE_HTML, build_router  # noqa: E402


def test_feed_is_bounded_but_seq_keeps_counting():
    feed = EventFeed(maxlen=3)
    for index in range(5):
        feed.append({"event": "hint", "i": index})
    assert feed.last_seq == 5
    assert [item["i"] for item in feed.since(3)] == [3, 4]
    assert [item["i"] for item in feed.since(0)] == [2, 3, 4]  # oldest dropped
    assert feed.since(5) == []
    assert feed.since(4, limit=1)[0]["i"] == 4
    assert feed.since(0, limit=2)[-1]["i"] == 4


def test_event_records_carry_seq_and_time():
    feed = EventFeed()
    feed.append({"event": "manage", "verb": "pause"})
    record = feed.since(0)[0]
    assert record["seq"] == 1
    assert isinstance(record["at_ms"], int)
    assert record["verb"] == "pause"


class _FakeService:
    def __init__(self) -> None:
        self.feed = EventFeed()

    def events(self, since: int = 0, limit: int = 200):
        return {"seq": self.feed.last_seq, "events": self.feed.since(since, limit)}

    async def status(self):
        return {"injection": "ok", "level": "L1+L2+L3-lite", "events": {"seq": self.feed.last_seq}}

    async def clear_isolation(self, session_id: str) -> bool:
        return session_id == "iso"


def test_live_routes():
    service = _FakeService()
    service.feed.append({"event": "hint", "hint": {"session": {"session_id": "s-1", "state": "start"}}})
    app = FastAPI()
    app.include_router(build_router(service))
    client = TestClient(app)

    page = client.get("/api/extension/agent-hints/live")
    assert page.status_code == 200
    assert "X-Agent-Hint" in page.text
    assert "events?since=" in page.text
    assert "show internal events" in page.text
    assert "hint" in LIVE_HTML

    events = client.get("/api/extension/agent-hints/events").json()
    assert events["seq"] == 1
    assert events["events"][0]["hint"]["session"]["state"] == "start"
    assert client.get("/api/extension/agent-hints/events", params={"since": 1}).json()["events"] == []

    status = client.get("/api/extension/agent-hints/status").json()
    assert status["injection"] == "ok"

    assert client.post("/api/extension/agent-hints/sessions/iso/clear-isolation").json()["cleared"] is True
    assert client.post("/api/extension/agent-hints/sessions/nope/clear-isolation").json()["cleared"] is False


def test_demo_upstream_echo_mode():
    from stub_upstream import reply_text

    body = {"tools": [{"type": "function"}], "messages": [{"role": "user", "content": "hi there"}]}
    assert reply_text(body, echo=True) == "Echo: hi there"
    assert reply_text({"messages": [{"role": "user", "content": "summarize this"}]}, echo=True) == "Noted."
    assert reply_text(body, echo=False) == "stub answer"
    assert reply_text({"tools": [{}], "messages": []}, echo=True) == "Echo."
