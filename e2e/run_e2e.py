"""End-to-end verification against the real DeerFlow Gateway.

Boots ``app.gateway.app.create_app`` in-process with an isolated
``DEER_FLOW_HOME`` and a scripted upstream, then drives real runs through the
REST API and asserts on the captured HTTP bytes.

Usage: ``python e2e/run_e2e.py`` (see ``e2e/run.sh`` for the environment).
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from stub_upstream import StubUpstream  # noqa: E402

from deerflow_agent_hints.wire import decode, validate_manage, validate_ordinary  # noqa: E402

FAILURES: list[str] = []


def check(condition: bool, message: str) -> None:
    if condition:
        print(f"  ok   {message}")
    else:
        print(f"  FAIL {message}")
        FAILURES.append(message)


def hint_of(record: dict) -> dict | None:
    header = record["headers"].get("x-agent-hint")
    return decode(header) if header else None


def main() -> int:
    home = Path(os.environ.get("AGENT_HINTS_E2E_HOME") or (ROOT / "e2e" / ".tmp" / f"home-{int(time.time())}"))
    home.mkdir(parents=True, exist_ok=True)
    capture = home / "captures.jsonl"
    stub = StubUpstream(capture)

    os.environ["DEER_FLOW_HOME"] = str(home)
    os.environ["DEER_FLOW_AUTH_DISABLED"] = "1"
    os.environ.pop("DEER_FLOW_ENV", None)
    os.environ.pop("ENVIRONMENT", None)
    os.environ.pop("DEER_FLOW_CONFIG_PATH", None)

    from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config

    config = AppConfig.model_validate(
        {
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
            "database": {"backend": "sqlite", "sqlite_dir": str(home / "data")},
            "memory": {"enabled": False},
            "skills": {"path": str(home / "skills")},
            "run_events": {"backend": "db"},
            "summarization": {
                "enabled": True,
                "model_name": None,
                "trigger": [{"type": "messages", "value": 3}],
                "keep": {"type": "messages", "value": 1},
            },
            "models": [
                {
                    "name": "stub-model",
                    "display_name": "Stub",
                    "use": "langchain_openai:ChatOpenAI",
                    "model": "stub-model",
                    "api_base": stub.base_url,
                    "api_key": "e2e-key",
                    "max_retries": 0,
                    "stream_chunk_timeout": 30,
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
    )
    set_app_config(config)

    from app.gateway.app import create_app
    from fastapi.testclient import TestClient

    def run(client: TestClient, thread: str, text: str) -> None:
        response = client.post(
            f"/api/threads/{thread}/runs/wait",
            json={
                "assistant_id": "lead_agent",
                "input": {"messages": [{"role": "user", "content": text}]},
                "config": {"recursion_limit": 100},
            },
        )
        check(response.status_code == 200, f"run on {thread} -> HTTP {response.status_code}")

    def mainline(records: list[dict]) -> list[dict]:
        return [record for record in records if record["body"].get("tools")]

    def manages(records: list[dict], state: str | None = None) -> list[dict]:
        found = []
        for record in records:
            if record["body"].get("messages") != []:
                continue
            hint = hint_of(record)
            if hint and hint.get("context_management") and (state is None or hint["session"]["state"] == state):
                found.append(record)
        return found

    def latest_mainline(records: list[dict]) -> dict:
        candidates = mainline(records)
        check(bool(candidates), "captured at least one main-line request")
        return candidates[-1]

    def wait_for(predicate, timeout: float, label: str) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.1)
        check(False, label)
        return False

    app = create_app()
    try:
        with TestClient(app) as client:
            status = client.get("/api/extension/agent-hints/status")
            check(status.status_code == 200, f"status route -> HTTP {status.status_code}")
            payload = status.json()
            check(payload.get("injection") == "ok", f"injection self-check => {payload.get('injection')}")
            check(payload.get("level") == "L1+L2+L3-lite", f"conformance level => {payload.get('level')}")

            print("\n[1] first run: start + header shape + msg_offset")
            thread1 = client.post("/api/threads", json={}).json()["thread_id"]
            run(client, thread1, "hello there")
            record = latest_mainline(stub.records())
            hint = hint_of(record)
            check(hint is not None, "main-line request carries X-Agent-Hint")
            if hint:
                check(validate_ordinary(hint) == [], f"hint conforms to v1.0: {validate_ordinary(hint)}")
                check(hint["session"]["state"] == "start", f"session.state == start ({hint['session']['state']})")
                check(hint["session"]["call_index"] == 1, f"call_index == 1 ({hint['session']['call_index']})")
                check(hint["session"]["epoch"] == 0, f"epoch == 0 ({hint['session']['epoch']})")
                check(
                    hint["priority_control"]["priority"] == {"level": 3, "reason": "user_waiting"},
                    f"priority == 3/user_waiting ({hint['priority_control']})",
                )
                cache = hint.get("cache_control") or {}
                check(cache.get("type") == "ephemeral", "cache_control is one ephemeral object")
                check(
                    cache.get("msg_offset") == len(record["body"]["messages"]),
                    f"msg_offset {cache.get('msg_offset')} == wire messages {len(record['body']['messages'])}",
                )
            body_keys = {key.lower() for key in record["body"]}
            check(not any("hint" in key for key in body_keys), f"body carries no hint field ({sorted(body_keys)})")

            print("\n[2] auxiliary requests stay unhinted")
            aux = [
                record for record in stub.records() if not record["body"].get("tools") and record["body"].get("messages") not in ([], None)
            ]
            if aux:
                check(all(hint_of(record) is None for record in aux), f"{len(aux)} auxiliary request(s) carry no hint")
            else:
                print("  note no auxiliary (title/summary) request observed yet")

            print("\n[3] second run: append + monotonic call_index")
            run(client, thread1, "second turn")
            hint = hint_of(latest_mainline(stub.records()))
            check(hint is not None and hint["session"]["state"] == "append", "second run is append")
            check(hint is not None and hint["session"]["call_index"] == 2, f"call_index == 2 ({hint and hint['session']['call_index']})")

            print("\n[4] second thread pauses the idle first one")
            thread2 = client.post("/api/threads", json={}).json()["thread_id"]
            run(client, thread2, "other thread")
            wait_for(
                lambda: any(
                    (hint_of(record) or {}).get("session", {}).get("session_id") == thread1
                    and (hint_of(record) or {})["session"]["state"] == "pause"
                    for record in manages(stub.records())
                ),
                timeout=6,
                label="pause manage for thread1",
            )
            pauses = [hint_of(record) for record in manages(stub.records(), "pause") if hint_of(record)["session"]["session_id"] == thread1]
            if pauses:
                check(pauses[0]["context_management"]["edits"][0]["type"] == "offload", "pause maps to offload")

            print("\n[5] returning to thread1 resumes before the request")
            before = len(stub.records())
            run(client, thread1, "back again")
            resumes = [hint_of(record) for record in manages(stub.records()[before:], "resume")]
            check(bool(resumes), "resume manage captured for the returning session")
            hint = hint_of(latest_mainline(stub.records()))
            if resumes and hint:
                check(
                    resumes[0]["session"]["call_index"] == hint["session"]["call_index"],
                    f"resume call_index {resumes[0]['session']['call_index']} == upcoming {hint['session']['call_index']}",
                )
                resume_at = manages(stub.records()[before:], "resume")[0]["at"]
                check(resume_at <= latest_mainline(stub.records())["at"], "resume is accepted before the request")

            print("\n[6] compaction: auxiliary summary, compact manage, epoch bump, cache suppressed")
            thread3 = client.post("/api/threads", json={}).json()["thread_id"]
            run(client, thread3, "one")
            first_hint = hint_of(latest_mainline(stub.records()))
            check(first_hint is not None and "cache_control" in first_hint, "first call declares cache")
            before = len(stub.records())
            run(client, thread3, "two")
            added = stub.records()[before:]
            summaries = [
                record for record in added if not record["body"].get("tools") and hint_of(record) is None and record["body"].get("messages")
            ]
            check(bool(summaries), "compaction summary request carries no hint")
            compacts = [hint_of(record) for record in manages(added, "compact")]
            check(bool(compacts), "compact manage captured")
            hint = hint_of(latest_mainline(stub.records()))
            if compacts and hint:
                check(
                    compacts[0]["session"]["epoch"] + 1 == hint["session"]["epoch"],
                    f"compact carries old epoch {compacts[0]['session']['epoch']}, request carries {hint['session']['epoch']}",
                )
                check("cache_control" not in hint, "cache declaration suppressed on the rewrite round")

            print("\n[7] archive -> stop (exactly once)")
            response = client.patch(f"/api/threads/{thread2}", json={"metadata": {"deerflow_archived": True}})
            check(response.status_code == 200, f"archive -> HTTP {response.status_code}")
            wait_for(
                lambda: any(hint_of(record)["session"]["session_id"] == thread2 for record in manages(stub.records(), "stop")),
                timeout=8,
                label="stop manage for the archived thread",
            )
            stops_before = len(
                [record for record in manages(stub.records(), "stop") if hint_of(record)["session"]["session_id"] == thread2]
            )
            time.sleep(2.5)
            stops_after = len([record for record in manages(stub.records(), "stop") if hint_of(record)["session"]["session_id"] == thread2])
            check(stops_before == 1 and stops_after == 1, f"exactly one stop for the archived thread ({stops_after})")

            print("\n[8] final status")
            payload = client.get("/api/extension/agent-hints/status").json()
            print("  counters:", json.dumps(payload.get("counters"), sort_keys=True))
            print("  manage:  ", json.dumps(payload.get("manage"), sort_keys=True))
            tracked = {row["session_id"] for row in payload.get("sessions", [])}
            check({thread1, thread3} <= tracked, f"status reports the live sessions ({sorted(tracked)})")
            check(payload.get("quarantined") == [], f"no quarantined sessions ({payload.get('quarantined')})")

            print("\n[9] whole-capture invariants")
            records = stub.records()
            hints = [(record, hint_of(record)) for record in records]
            ordinary = [hint for _, hint in hints if hint and "context_management" not in hint]
            manage_hints = [hint for _, hint in hints if hint and hint.get("context_management")]
            check(
                bool(ordinary) and all(validate_ordinary(item) == [] for item in ordinary),
                f"all {len(ordinary)} ordinary hints conform",
            )
            check(
                bool(manage_hints) and all(validate_manage(item) == [] for item in manage_hints),
                f"all {len(manage_hints)} manage hints conform",
            )
            hinted_requests = [record for record, hint in hints if hint is not None]
            untools = [record for record, hint in hints if hint and "context_management" not in hint and not record["body"].get("tools")]
            check(not untools, f"no auxiliary request carries a hint ({len(untools)})")
            bodies = [record["body"] for record in records]
            check(all(not any("hint" in key.lower() for key in body) for body in bodies), "no request body carries hint fields")
            check(all("x-agent-hint" in record["headers"] for record in hinted_requests), "hint only travels in the header")
    finally:
        stub.close()
        reset_app_config()

    print()
    if FAILURES:
        print(f"FAILED ({len(FAILURES)}):")
        for item in FAILURES:
            print(f"  - {item}")
        return 1
    print(f"PASS — captures: {capture}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
