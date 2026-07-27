"""LogstashSink — JSON lines to Logstash over HTTP or a raw TCP/UDP socket (arch §8, SPEC-009).

Two mutually-exclusive modes chosen at construction:

* ``LogstashSink(url=...)`` — send the batch as JSON lines over HTTP (reusing the ``HTTPSink`` core).
* ``LogstashSink(host=..., port=...)`` — send one ``json.dumps(event) + "\\n"`` per event over a raw
  TCP or UDP socket (reusing :class:`~log_foundry.sinks._socket.SocketTransport`).

Either backend handles its own bounded retry; ``close()`` releases whichever it holds.
"""

from __future__ import annotations

import json

from log_foundry.sinks._socket import SocketTransport
from log_foundry.sinks.http import HTTPSink

__all__ = ["LogstashSink"]


class LogstashSink:
    """A :class:`~log_foundry.sinks.base.Sink` that ships JSON lines to Logstash (FR-005)."""

    def __init__(
        self,
        url: str | None = None,
        *,
        host: str | None = None,
        port: int | None = None,
        transport: str = "tcp",
        timeout: float = 5.0,
        max_retries: int = 3,
        **http_kwargs: object,
    ) -> None:
        if url is not None:
            self._http: HTTPSink | None = HTTPSink(
                url, body_format="ndjson", timeout=timeout, max_retries=max_retries,
                **http_kwargs,  # type: ignore[arg-type]
            )
            self._socket: SocketTransport | None = None
        elif host is not None and port is not None:
            self._http = None
            self._socket = SocketTransport(
                host, port, transport=transport, timeout=timeout, max_retries=max_retries
            )
        else:
            raise ValueError("LogstashSink requires either url= (HTTP) or host= + port= (socket)")

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Send the batch over the configured backend (FR-005)."""
        if not batch:
            return
        if self._http is not None:
            self._http.emit(batch)
        else:
            # Narrowing for mypy, not a runtime check: __init__ guarantees exactly one of
            # _http/_socket is set, and the branch above covers the other.
            assert self._socket is not None  # noqa: S101
            frames = [(json.dumps(event) + "\n").encode("utf-8") for event in batch]
            self._socket.send_all(frames)

    def close(self) -> None:
        """Close whichever backend is held (FR-005)."""
        if self._http is not None:
            self._http.close()
        elif self._socket is not None:
            self._socket.close()

    @property
    def failed(self) -> int:
        """Requests/messages abandoned past the retry bound, from the active backend."""
        return self._http.failed if self._http is not None else (
            self._socket.failed if self._socket is not None else 0
        )
