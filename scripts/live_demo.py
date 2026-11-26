#!/usr/bin/env python3
"""Live demo: a real DeerFlow Gateway emitting Agent Hints, observed in real time.

Boots ``app.gateway.app:create_app`` in a subprocess with an isolated
``DEER_FLOW_HOME`` and a scripted OpenAI-compatible upstream (no credentials
needed, main-line replies echo your message), then lets you chat from the
terminal. Every emitted hint / manage outcome streams to this terminal and to a
browser page:

    http://127.0.0.1:<port>/api/extension/agent-hints/live

With a real deployment (``make dev`` + a configured model) the same page is at
``http://localhost:2026/api/extension/agent-hints/live``.

Usage:
    bash scripts/live.sh              # interactive chat
    bash scripts/live.sh --smoke      # scripted end-to-end check (exit 0/1)
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "e2e"))

from stub_upstream import StubUpstream  # noqa: E402

BACKEND = Path(os.environ.get("DEERFLOW_BACKEND") or (Path.home() / "deer-flow" / "backend"))
STATE_COLOR = {
    "start": "\033[32m",
    "append": "\033[34m",
    "pause": "\033[33m",
    "resume": "\033[36m",
    "stop": "\033[31m",
    "compact": "\033[35m",
}
RESET = "\033[0m"
DIM = "\033[2m"


# --------------------------------------------------------------------------- #
# Gateway lifecycle
# --------------------------------------------------------------------------- #


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def build_config(stub_base_url: str, home: Path) -> dict[str, Any]:
    return {
        "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
        "database": {"backend": "sqlite", "sqlite_dir": str(home / "data")},
        "memory": {"enabled": False},
        "skills": {"path": str(home / "skills")},
        "run_events": {"backend": "db"},
        "summarization": {
            "enabled": True,
            "model_name": None,
            "trigger": [{"type": "messages", "value": 6}],
            "keep": {"type": "messages", "value": 2},
        },
        "models": [
            {
                "name": "stub-model",
                "display_name": "Stub",
                "use": "langchain_openai:ChatOpenAI",
                "model": "stub-model",
                "api_base": stub_base_url,
                "api_key": "demo-key",
                "max_retries": 0,
                "stream_chunk_timeout": 60,
                "context_window": 128000,
            }
        ],
        "plugins": [
            {
                "name": "agent-hints",
                "use": "deerflow_agent_hints:install",
                "table_prefix": "extension_agent_hints_",
                "config": {
                    "sessionControl": {"threadPollSeconds": 1, "pendingOpAwaitMs": 1000},
                    "state": {"path": str(home / "agent-hints.db")},
                    "trace": {"enabled": True, "file": str(home / "trace.jsonl")},
                },
            }
        ],
    }


class Gateway:
    def __init__(self, home: Path, stub_base_url: str, port: int | None = None) -> None:
        self.home = home
        self.port = port or _free_port()
        self.log_path = home / "gateway.log"
        self._process: subprocess.Popen | None = None
        home.mkdir(parents=True, exist_ok=True)
        (home / "config.yaml").write_text(json.dumps(build_config(stub_base_url, home), indent=2), encoding="utf-8")

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def live_url(self) -> str:
        return f"{self.base_url}/api/extension/agent-hints/live"

    def start(self) -> None:
        env = dict(os.environ)
        env.update(
            {
                "DEER_FLOW_CONFIG_PATH": str(self.home / "config.yaml"),
                "DEER_FLOW_HOME": str(self.home),
                "DEER_FLOW_AUTH_DISABLED": "1",
                "PYTHONPATH": os.pathsep.join([str(ROOT / "src"), str(BACKEND), env.get("PYTHONPATH", "")]).rstrip(os.pathsep),
            }
        )
        env.pop("DEER_FLOW_ENV", None)
        env.pop("ENVIRONMENT", None)
        log = self.log_path.open("w", encoding="utf-8")
        self._process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.gateway.app:create_app",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--log-level",
                "warning",
            ],
            cwd=str(BACKEND),
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )

    def wait_ready(self, timeout: float = 120.0) -> bool:
        deadline = time.time() + timeout
        with httpx.Client(base_url=self.base_url, timeout=5.0) as client:
            while time.time() < deadline:
                if self._process is not None and self._process.poll() is not None:
                    return False
                try:
                    response = client.get("/api/extension/agent-hints/status")
                    if response.status_code == 200:
                        return True
                except httpx.HTTPError:
                    pass
                time.sleep(0.5)
        return False

    def log_tail(self, lines: int = 60) -> str:
        try:
            text = self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            return ""
        return "\n".join(text[-lines:])

    def stop(self) -> None:
        if self._process is None:
            return
        self._process.terminate()
        try:
            self._process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=10)
        self._process = None


# --------------------------------------------------------------------------- #
# HTTP API + event tail
# --------------------------------------------------------------------------- #


class Api:
    def __init__(self, base_url: str) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=httpx.Timeout(300.0))

    def close(self) -> None:
        self._client.close()

    def create_thread(self) -> str:
        response = self._client.post("/api/threads", json={})
        response.raise_for_status()
        return response.json()["thread_id"]

    def run(self, thread: str, text: str) -> dict[str, Any]:
        response = self._client.post(
            f"/api/threads/{thread}/runs/wait",
            json={
                "assistant_id": "lead_agent",
                "input": {"messages": [{"role": "user", "content": text}]},
                "config": {"recursion_limit": 80},
            },
        )
        response.raise_for_status()
        payload = response.json()
        status = payload.get("status") if isinstance(payload, dict) else None
        if status is not None and status != "success":
            raise RuntimeError(f"run on {thread[:8]} ended as {status}: {payload.get('error') or payload}")
        return payload

    def compact(self, thread: str) -> dict[str, Any]:
        response = self._client.post(
            f"/api/threads/{thread}/compact",
            json={"force": True, "keep": {"type": "messages", "value": 1}},
        )
        response.raise_for_status()
        return response.json()

    def archive(self, thread: str) -> dict[str, Any]:
        response = self._client.patch(f"/api/threads/{thread}", json={"metadata": {"deerflow_archived": True}})
        response.raise_for_status()
        return response.json()

    def events(self, since: int = 0, limit: int = 500) -> dict[str, Any]:
        response = self._client.get("/api/extension/agent-hints/events", params={"since": since, "limit": limit})
        response.raise_for_status()
        return response.json()

    def status(self) -> dict[str, Any]:
        response = self._client.get("/api/extension/agent-hints/status")
        response.raise_for_status()
        return response.json()

    def live_page(self) -> tuple[int, str]:
        response = self._client.get("/api/extension/agent-hints/live")
        return response.status_code, response.text


def assistant_text(payload: dict[str, Any]) -> str | None:
    messages = payload.get("messages")
    if not isinstance(messages, list):
        return None
    for message in reversed(messages):
        if not isinstance(message, dict):
            continue
        role = str(message.get("type") or message.get("role") or "").lower()
        if role not in ("ai", "assistant"):
            continue
        content = message.get("content")
        if isinstance(content, str) and content.strip():
            return content
        if isinstance(content, list):
            parts = [block.get("text") for block in content if isinstance(block, dict) and isinstance(block.get("text"), str)]
            text = "".join(parts).strip()
            if text:
                return text
    return None


def format_event(event: dict[str, Any], *, color: bool = True) -> str:
    stamp = time.strftime("%H:%M:%S", time.localtime(event.get("at_ms", 0) / 1000))
    hint = event.get("hint") or {}
    session = hint.get("session") or {}
    sid = str(session.get("session_id") or event.get("session_id") or "")[:8]
    state = str(session.get("state") or event.get("state") or event.get("verb") or "")
    call = session.get("call_index", event.get("call_index", ""))
    epoch = session.get("epoch", event.get("epoch", ""))
    paint = STATE_COLOR.get(state, "") if color else ""
    reset = RESET if color else ""
    if event.get("event") == "hint":
        priority = hint.get("priority_control") or {}
        pair = priority.get("priority") or {}
        prio = f"{pair.get('level')}/{pair.get('reason')}" if pair else "-"
        cache = hint.get("cache_control") or {}
        ttl = cache.get("ttl_s", "-")
        offset = cache.get("msg_offset", "-")
        return f"{stamp} HINT   {paint}{state:<8}{reset} ses={sid} call={call} epoch={epoch} prio={prio} ttl={ttl} off={offset}"
    outcome = event.get("outcome", "")
    return f"{stamp} MANAGE {paint}{state:<8}{reset} ses={sid} call={call} epoch={epoch} -> {outcome}"


class Tail(threading.Thread):
    """Poll the extension feed and mirror hint/manage events into the terminal."""

    def __init__(self, api: Api, *, color: bool, verbose: bool = False) -> None:
        super().__init__(daemon=True)
        self._api = api
        self._color = color
        self._verbose = verbose
        self._halt = threading.Event()
        self._lock = threading.Lock()
        self.events: list[dict[str, Any]] = []
        self.since = 0

    def run(self) -> None:
        while not self._halt.is_set():
            try:
                payload = self._api.events(self.since)
            except httpx.HTTPError:
                self._halt.wait(1.0)
                continue
            for event in payload.get("events") or []:
                with self._lock:
                    self.events.append(event)
                self.since = max(self.since, int(event.get("seq", 0)))
                if event.get("event") in ("hint", "manage") or self._verbose:
                    print(format_event(event, color=self._color), flush=True)
            self._halt.wait(0.4)

    def stop(self) -> None:
        self._halt.set()
        self.join(timeout=3)

    def wait(self, predicate: Callable[[dict], bool], *, timeout: float = 25.0, label: str = "") -> dict | None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                for event in self.events:
                    if predicate(event):
                        return event
            time.sleep(0.1)
        if label:
            print(f"  (timed out waiting for {label})", flush=True)
        return None

    def matching(self, predicate: Callable[[dict], bool]) -> list[dict]:
        with self._lock:
            return [event for event in self.events if predicate(event)]


# --------------------------------------------------------------------------- #
# Chat / smoke
# --------------------------------------------------------------------------- #


@dataclass
class Threads:
    ids: list[str] = field(default_factory=list)
    current: int = 0

    def add(self, thread_id: str) -> int:
        self.ids.append(thread_id)
        self.current = len(self.ids) - 1
        return self.current

    def use(self, index: int) -> bool:
        if 0 <= index < len(self.ids):
            self.current = index
            return True
        return False

    @property
    def current_id(self) -> str:
        return self.ids[self.current]


def print_threads(threads: Threads) -> None:
    for index, thread_id in enumerate(threads.ids):
        marker = "*" if index == threads.current else " "
        print(f" {marker} [{index}] {thread_id[:8]}")


def smoke(api: Api, tail: Tail, threads: Threads) -> int:
    failures: list[str] = []

    def expect(ok: bool, message: str) -> None:
        print(("  ok   " if ok else "  FAIL ") + message, flush=True)
        if not ok:
            failures.append(message)

    def hint_for(session: str, **fields: Any) -> Callable[[dict], bool]:
        def predicate(event: dict) -> bool:
            if event.get("event") != "hint":
                return False
            block = (event.get("hint") or {}).get("session") or {}
            if block.get("session_id") != session:
                return False
            return all(block.get(key) == value for key, value in fields.items())

        return predicate

    def manage_for(session: str, verb: str) -> Callable[[dict], bool]:
        return lambda event: (
            event.get("event") == "manage"
            and (event.get("hint") or {}).get("session", {}).get("session_id") == session
            and event.get("verb") == verb
        )

    print("\n[1] first turn -> start", flush=True)
    t1 = api.create_thread()
    threads.add(t1)
    api.run(t1, "hello there")
    first = tail.wait(hint_for(t1, state="start"), label="start hint")
    expect(first is not None, f"start hint for {t1[:8]}")
    if first:
        hint = first["hint"]
        expect(hint["session"]["call_index"] == 1, f"call_index == 1 ({hint['session']['call_index']})")
        expect(hint["session"]["epoch"] == 0, f"epoch == 0 ({hint['session']['epoch']})")
        expect(
            hint["priority_control"]["priority"] == {"level": 3, "reason": "user_waiting"},
            "priority == 3/user_waiting",
        )
        expect("cache_control" in hint, "cache declared on the first call")

    print("\n[2] second turn -> append", flush=True)
    api.run(t1, "second turn")
    second = tail.wait(hint_for(t1, call_index=2), label="second hint")
    expect(second is not None, "append hint with call_index 2")
    if second:
        expect(second["hint"]["session"]["state"] == "append", "state == append")
        expect("cache_control" in second["hint"], "cache declared again while the prefix is stable")

    print("\n[3] a second thread pauses the idle first one", flush=True)
    t2 = api.create_thread()
    threads.add(t2)
    api.run(t2, "another thread")
    paused = tail.wait(manage_for(t1, "pause"), label="pause for thread 1")
    expect(paused is not None, "pause manage for the idle thread")

    print("\n[4] returning to the first thread resumes it", flush=True)
    threads.use(0)
    api.run(t1, "back again")
    resumed = tail.wait(manage_for(t1, "resume"), label="resume for thread 1")
    expect(resumed is not None, "resume manage before the request")

    print("\n[5] automatic compaction on a fresh thread", flush=True)
    t4 = api.create_thread()
    threads.add(t4)
    for turn in range(1, 6):
        api.run(t4, f"turn {turn}")
        time.sleep(0.25)

    def hints_for(session: str) -> list[dict]:
        return [event for event in tail.events if event.get("event") == "hint" and event.get("session_id") == session]

    rewrites = [event for event in hints_for(t4) if event.get("after_rewrite") is True]
    for _ in range(2):  # one more attempt on the rare admission race
        if rewrites:
            break
        api.run(t4, "more history")
        time.sleep(0.4)
        rewrites = [event for event in hints_for(t4) if event.get("after_rewrite") is True]
    expect(bool(rewrites), "automatic compaction observed")
    rewrite = rewrites[0] if rewrites else None
    compacts = [
        event
        for event in tail.events
        if event.get("event") == "manage"
        and event.get("verb") == "compact"
        and (event.get("hint") or {}).get("session", {}).get("session_id") == t4
    ]
    expect(bool(compacts), "compact manage after the rewrite")
    compact = compacts[0] if compacts else None
    if rewrite is not None:
        expect("cache_control" not in rewrite["hint"], "cache suppressed on the rewrite round")
        if compact is not None:
            expect(
                rewrite["hint"]["session"]["epoch"] == compact["hint"]["session"]["epoch"] + 1,
                "request carries epoch + 1 while compact carries the old epoch",
            )
    print("\n[5b] the next turn re-declares cache", flush=True)
    rewrite_call = int((rewrite or {}).get("call_index") or 0)
    recovery = None
    for _ in range(3):
        candidates = [
            event for event in hints_for(t4) if int(event.get("call_index") or 0) > rewrite_call and event.get("after_rewrite") is False
        ]
        if candidates:
            recovery = candidates[0]
            break
        api.run(t4, "recovery turn")
        time.sleep(0.4)
    expect(recovery is not None, "the turn after the rewrite produced a hint")
    if recovery is not None:
        expect("cache_control" in recovery["hint"], "cache re-declared once the prefix is stable")

    print("\n[6] manual /compact on a second thread", flush=True)
    t3 = api.create_thread()
    threads.add(t3)
    api.run(t3, "alpha")
    api.run(t3, "beta")
    result = api.compact(t3)
    expect(bool(result.get("compacted")), f"manual compaction happened ({result.get('reason')})")
    api.run(t3, "gamma")
    manual = tail.wait(manage_for(t3, "compact"), label="compact manage (manual)")
    expect(manual is not None, "manual compaction emits a compact manage request")
    manual_hint = tail.wait(
        lambda event: (
            event.get("event") == "hint"
            and (event.get("hint") or {}).get("session", {}).get("session_id") == t3
            and (event.get("hint") or {}).get("session", {}).get("epoch", 0) >= 1
        ),
        label="hint with epoch 1",
    )
    expect(manual_hint is not None, "the next request carries the bumped epoch")
    if manual_hint is not None:
        expect("cache_control" not in manual_hint["hint"], "cache suppressed on the manual rewrite round")

    print("\n[7] archive -> stop (once)", flush=True)
    api.archive(t2)
    stopped = tail.wait(manage_for(t2, "stop"), timeout=30, label="stop for thread 2")
    expect(stopped is not None, "stop manage for the archived thread")

    print("\n[8] panel data", flush=True)
    status = api.status()
    expect(status.get("injection") == "ok", f"injection self-check ({status.get('injection')})")
    page_status, page = api.live_page()
    expect(page_status == 200 and "X-Agent-Hint" in page and "events?since=" in page, f"live page served (HTTP {page_status})")
    expect(status.get("level") == "L1+L2+L3-lite", f"level ({status.get('level')})")
    states = {
        (event.get("hint") or {}).get("session", {}).get("state") for event in tail.events if event.get("event") in ("hint", "manage")
    }
    for state in ("start", "append", "pause", "resume", "compact", "stop"):
        expect(state in states, f"observed state {state}")

    print()
    if failures:
        print(f"FAILED ({len(failures)}):")
        for item in failures:
            print(f"  - {item}")
        return 1
    print(f"PASS — {len(tail.events)} events observed; live page: {status.get('live')}")
    return 0


def repl(api: Api, tail: Tail, threads: Threads, live_url: str) -> int:
    print()
    print(f"Live view: {live_url}")
    print("Commands: /new  /use N  /threads  /compact  /archive  /status  /events [n]  /help  /quit")
    print("Anything else is sent as a chat message to the current thread.\n")
    while True:
        try:
            line = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not line:
            continue
        if line.startswith("/"):
            command, _, argument = line[1:].partition(" ")
            command = command.lower()
            if command in ("quit", "exit", "q"):
                return 0
            if command == "help":
                print("Commands: /new  /use N  /threads  /compact  /archive  /status  /events [n]  /help  /quit")
            elif command == "new":
                index = threads.add(api.create_thread())
                print(f"  switched to [{index}] {threads.current_id[:8]}")
            elif command == "use":
                try:
                    index = int(argument)
                except ValueError:
                    print("  usage: /use N")
                    continue
                if threads.use(index):
                    print(f"  switched to [{index}] {threads.current_id[:8]}")
                else:
                    print("  no such thread")
            elif command == "threads":
                print_threads(threads)
            elif command == "compact":
                try:
                    result = api.compact(threads.current_id)
                except httpx.HTTPStatusError as exc:
                    print(f"  compact failed: HTTP {exc.response.status_code} {exc.response.text[:200]}")
                    continue
                print(f"  compacted={result.get('compacted')} removed={result.get('removed_message_count')} ({result.get('reason')})")
                print("  the next message on this thread shows epoch + 1 and no cache_control")
            elif command == "archive":
                api.archive(threads.current_id)
                print("  archived; the poller sends stop within a few seconds")
            elif command == "status":
                payload = api.status()
                print(
                    json.dumps(
                        {
                            "injection": payload.get("injection"),
                            "enabled": payload.get("enabled"),
                            "counters": payload.get("counters"),
                            "manage": payload.get("manage"),
                            "quarantined": payload.get("quarantined"),
                        },
                        ensure_ascii=False,
                    )
                )
            elif command == "events":
                count = int(argument) if argument.isdigit() else 10
                for event in tail.events[-count:]:
                    print(format_event(event, color=sys.stdout.isatty()))
                    print(DIM + json.dumps(event.get("hint") or {}, ensure_ascii=False) + RESET)
            else:
                print(f"  unknown command /{command}")
            continue
        try:
            payload = api.run(threads.current_id, line)
        except httpx.HTTPStatusError as exc:
            print(f"bot> (HTTP {exc.response.status_code}) {exc.response.text[:300]}")
            continue
        text = assistant_text(payload)
        if text:
            print(f"bot> {text}")
        else:
            print(f"bot> (no text; {json.dumps(payload)[:200]})")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--smoke", action="store_true", help="run the scripted end-to-end sequence instead of a chat")
    parser.add_argument("--port", type=int, default=0, help="Gateway port (default: a free port)")
    parser.add_argument("--home", type=Path, default=None, help="working directory for state (default: e2e/.tmp/live-<ts>)")
    parser.add_argument("--verbose", action="store_true", help="also print internal events (logical/compaction/service)")
    args = parser.parse_args()

    if not (BACKEND / "app" / "gateway" / "app.py").exists():
        print(f"✗ DeerFlow backend not found at {BACKEND} (set DEERFLOW_BACKEND)", file=sys.stderr)
        return 1

    home = args.home or (ROOT / "e2e" / ".tmp" / f"live-{int(time.time())}")
    color = sys.stdout.isatty()
    stub = StubUpstream(home / "captures.jsonl", echo=True)
    gateway = Gateway(home, stub.base_url, args.port or None)
    api: Api | None = None
    tail: Tail | None = None
    try:
        print(f"· upstream stub  {stub.base_url}")
        print(f"· state          {home}")
        gateway.start()
        if not gateway.wait_ready():
            print("✗ the Gateway did not become ready. Log tail:", file=sys.stderr)
            print(gateway.log_tail(), file=sys.stderr)
            return 1
        api = Api(gateway.base_url)
        tail = Tail(api, color=color, verbose=args.verbose)
        tail.start()
        status = api.status()
        if status.get("injection") != "ok":
            print(f"⚠ injection self-check is {status.get('injection')} — hints may not reach the wire")
        print(f"· Gateway        {gateway.base_url}")
        print(f"· live view      {gateway.live_url}")
        print(f"· conformance    {status.get('level')}")

        threads = Threads()
        if args.smoke:
            code = smoke(api, tail, threads)
        else:
            threads.add(api.create_thread())
            code = repl(api, tail, threads, gateway.live_url)
        return code
    finally:
        for step in (
            lambda: tail.stop() if tail is not None else None,
            lambda: api.close() if api is not None else None,
            gateway.stop,
            stub.close,
        ):
            with contextlib.suppress(Exception):  # cleanup must not mask the run result
                step()
        print(f"· captures       {home / 'captures.jsonl'}")


if __name__ == "__main__":
    raise SystemExit(main())
