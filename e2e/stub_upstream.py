"""Scripted OpenAI-compatible upstream for the Agent Hint e2e run.

Records every request (path, headers, body, arrival time) as one JSONL line and
answers with a fixed completion (JSON, or SSE when the request streams). It is
the wire observer the assertions run against; nothing here is part of the plugin.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def _completion(model: str) -> dict:
    return {
        "id": "chatcmpl-stub",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "stub answer"},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 11, "completion_tokens": 2, "total_tokens": 13},
    }


class StubUpstream:
    def __init__(self, capture_path: Path) -> None:
        self.capture_path = Path(capture_path)
        self.capture_path.parent.mkdir(parents=True, exist_ok=True)
        self.capture_path.write_text("", encoding="utf-8")
        self._lock = threading.Lock()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: object) -> None:  # noqa: ANN002
                return

            def do_GET(self) -> None:  # noqa: N802
                if self.path.rstrip("/").endswith("/models"):
                    payload = {"object": "list", "data": [{"id": "stub-model", "object": "model"}]}
                    self._send_json(200, payload)
                else:
                    self._send_json(404, {"error": "not found"})

            def _send_json(self, status: int, payload: dict) -> None:
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                try:
                    body = json.loads(raw.decode("utf-8")) if raw else {}
                except ValueError:
                    body = {"raw": raw.decode("utf-8", "replace")}
                record = {
                    "path": self.path,
                    "headers": {key.lower(): value for key, value in self.headers.items()},
                    "body": body,
                    "at": time.time(),
                }
                with outer._lock, outer.capture_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")

                model = str(body.get("model") or "stub-model")
                if body.get("stream"):
                    self._send_sse(model)
                else:
                    self._send_json(200, _completion(model))

            def _send_sse(self, model: str) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()

                def event(payload: dict) -> None:
                    data = f"data: {json.dumps(payload)}\n\n".encode()
                    self.wfile.write(f"{len(data):X}\r\n".encode() + data + b"\r\n")
                    self.wfile.flush()

                base = {
                    "id": "chatcmpl-stub",
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": model,
                }
                event(
                    {
                        **base,
                        "choices": [{"index": 0, "delta": {"role": "assistant", "content": "stub answer"}, "finish_reason": None}],
                    }
                )
                event(
                    {
                        **base,
                        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                        "usage": {"prompt_tokens": 11, "completion_tokens": 2, "total_tokens": 13},
                    }
                )
                done = b"data: [DONE]\n\n"
                self.wfile.write(f"{len(done):X}\r\n".encode() + done + b"\r\n")
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{int(self._httpd.server_address[1])}/v1"

    def records(self) -> list[dict]:
        with self._lock:
            text = self.capture_path.read_text(encoding="utf-8")
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    def close(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
