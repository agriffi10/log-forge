"""SplunkHECSink — ship to Splunk's HTTP Event Collector (arch §8, SPEC-009).

An :class:`~log_forge.sinks.http.HTTPSink` specialization: each event is wrapped in a HEC envelope
(``{"event": ..., "time": <epoch>, "host": ..., "source": ...}``) and the batch is sent as HEC's
concatenated-JSON-objects body with an ``Authorization: Splunk <token>`` header. ``time`` (epoch
seconds) is parsed from the event's ISO-8601 ``timestamp`` (falling back to emit-time now).
"""

from __future__ import annotations

import json

from log_forge.sinks._time import epoch_seconds
from log_forge.sinks.http import HTTPSink, merge_headers

__all__ = ["SplunkHECSink"]


class SplunkHECSink(HTTPSink):
    """POST HEC envelopes to a Splunk HTTP Event Collector endpoint (FR-008)."""

    def __init__(
        self,
        url: str,
        token: str,
        *,
        host: str | None = None,
        source: str = "log-forge",
        **http_kwargs: object,
    ) -> None:
        self._host = host
        self._source = source
        headers = merge_headers({"Authorization": f"Splunk {token}"}, http_kwargs)
        super().__init__(url, headers=headers, **http_kwargs)  # type: ignore[arg-type]

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Send the batch as HEC's concatenated JSON objects (FR-008)."""
        if not batch:
            return
        body = "".join(json.dumps(self._envelope(event)) for event in batch).encode("utf-8")
        self._send(body, content_type="application/json")

    def _envelope(self, event: dict[str, object]) -> dict[str, object]:
        envelope: dict[str, object] = {
            "event": event,
            "time": epoch_seconds(event.get("timestamp")),
            "source": self._source,
        }
        if self._host is not None:
            envelope["host"] = self._host
        return envelope
