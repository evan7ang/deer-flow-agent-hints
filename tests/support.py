"""Shared test helpers: a threaded capture server for manage/wire assertions."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class CaptureServer:
    """Records every POST (path, headers, body) and answers a scripted status."""

    def __init__(self, status: int | Callable[[int], int] = 200) -> None:
        self.requests: list[dict[str, Any]] = []
        self._status = status
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:  # noqa: ANN002
                return

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                try:
                    body = json.loads(raw.decode("utf-8")) if raw else {}
                except ValueError:
                    body = {"raw": raw.decode("utf-8", "replace")}
                outer.requests.append(
                    {
                        "path": self.path,
                        "headers": {key.lower(): value for key, value in self.headers.items()},
                        "body": body,
                        "at": time.monotonic(),
                    }
                )
                count = len(outer.requests)
                status = outer._status(count) if callable(outer._status) else outer._status
                payload = b"{}"
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    @property
    def port(self) -> int:
        return int(self._httpd.server_address[1])

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/v1"

    @property
    def manage_url(self) -> str:
        return f"{self.base_url}/chat/completions"

    def close(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
