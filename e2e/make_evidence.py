"""Sanitize an e2e capture into evidence: hints and shape only, no content."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from deerflow_agent_hints.wire import decode  # noqa: E402


def sanitize(record: dict) -> dict:
    header = record["headers"].get("x-agent-hint")
    hint = decode(header) if header else None
    body = record.get("body") or {}
    messages = body.get("messages") or []
    tools = body.get("tools") or []
    return {
        "path": record["path"],
        "has_hint": hint is not None,
        "hint": hint,
        "hint_class": ("manage" if hint and "context_management" in hint else "ordinary") if hint else None,
        "message_count": len(messages) if isinstance(messages, list) else 0,
        "roles": [str((message or {}).get("role")) for message in messages if isinstance(message, dict)][:24],
        "tools": [str(((tool or {}).get("function") or {}).get("name")) for tool in tools if isinstance(tool, dict)][:24],
        "stream": body.get("stream"),
        "at": record.get("at"),
    }


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: make_evidence.py <capture.jsonl> <out.jsonl>", file=sys.stderr)
        return 2
    source, target = Path(argv[1]), Path(argv[2])
    target.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.loads(line) for line in source.read_text(encoding="utf-8").splitlines() if line.strip()]
    with target.open("w", encoding="utf-8") as handle:
        for record in lines:
            handle.write(json.dumps(sanitize(record), ensure_ascii=False) + "\n")
    print(f"wrote {len(lines)} sanitized records to {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
