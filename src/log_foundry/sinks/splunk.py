"""SplunkHECSink — ship to Splunk's HTTP Event Collector (arch §8, SPEC-009)."""

from __future__ import annotations

import json

from log_foundry.sinks._time import epoch_seconds
from log_foundry.sinks.http import HTTPSink, merge_headers

__all__ = ["SplunkHECSink"]


class SplunkHECSink(HTTPSink):
    """POSTs HEC envelopes to a Splunk HTTP Event Collector endpoint (FR-008).

    Each event is wrapped in a HEC envelope and the batch is sent as HEC's
    concatenated-JSON-objects body with an ``Authorization: Splunk <token>`` header. The
    envelope's epoch-seconds ``time`` is parsed from the event's ISO-8601 ``timestamp``, falling
    back to emit-time now.
    """

    def __init__(
        self,
        url: str,
        token: str,
        *,
        host: str | None = None,
        source: str = "log-foundry",
        **http_kwargs: object,
    ) -> None:
        """Points the sink at a collector endpoint.

        Args:
          url: The collector endpoint.
          token: The HEC token sent in the ``Authorization`` header.
          host: The ``host`` stamped on each envelope, or ``None`` to omit it.
          source: The ``source`` stamped on each envelope.
          **http_kwargs: Forwarded to :class:`~log_foundry.sinks.http.HTTPSink`.

        Returns:
          None.

        Raises:
          None.
        """
        self._host = host
        self._source = source
        headers = merge_headers({"Authorization": f"Splunk {token}"}, http_kwargs)
        super().__init__(url, headers=headers, **http_kwargs)  # type: ignore[arg-type]

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Sends the batch as HEC's concatenated JSON objects (FR-008).

        Args:
          batch: The events to ship. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: If the request was abandoned past the retry bound.
        """
        if not batch:
            return
        body = "".join(json.dumps(self._envelope(event)) for event in batch).encode("utf-8")
        self._send(body, content_type="application/json")

    def _envelope(self, event: dict[str, object]) -> dict[str, object]:
        """Wraps one event in a HEC envelope.

        Args:
          event: The event to wrap, which is not mutated.

        Returns:
          The envelope.

        Raises:
          None.
        """
        envelope: dict[str, object] = {
            "event": event,
            "time": epoch_seconds(event.get("timestamp")),
            "source": self._source,
        }
        if self._host is not None:
            envelope["host"] = self._host
        return envelope
