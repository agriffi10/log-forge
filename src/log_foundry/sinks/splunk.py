"""SplunkHECSink — ship to Splunk's HTTP Event Collector (arch §8, SPEC-009)."""

from __future__ import annotations

import json

from log_foundry.sinks._time import epoch_seconds
from log_foundry.sinks.http import HTTPSink, _Item, merge_headers

__all__ = ["SplunkHECSink"]


class SplunkHECSink(HTTPSink):
    """POSTs HEC envelopes to a Splunk HTTP Event Collector endpoint (FR-008).

    Each event is wrapped in a HEC envelope and the batch is sent as HEC's
    concatenated-JSON-objects body with an ``Authorization: Splunk <token>`` header. The
    envelope's epoch-seconds ``time`` is parsed from the event's ISO-8601 ``timestamp``, falling
    back to emit-time now.

    It takes **no** transport lock (SPEC-028 FR-002) and **adds no post-close guard**
    (SPEC-032 FR-003), for the reasons :class:`~log_foundry.sinks.http.HTTPSink` records: there
    is no transport held and ``close()`` releases nothing.

    Attributes:
      MAX_BATCH_COUNT: 1,000 — this library's conservative default.
      MAX_BATCH_BYTES: 1,000,000 — likewise. Splunk publishes **no fixed** HEC payload limit:
        it is ``max_content_length`` on the receiving instance, so there is no vendor figure to
        cite and the default is chosen rather than documented. Raise it with
        ``max_batch_bytes=`` to match your deployment.


    It keeps **no** client buffer (SPEC-036 FR-002): each ``emit`` is a request that has
    completed by the time it returns, and no client object outlives it holding data.
    """

    MAX_BATCH_COUNT = 1000
    MAX_BATCH_BYTES = 1_000_000

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

    def _render(self, event: dict[str, object]) -> str:
        """Serializes one event's HEC envelope (FR-008).

        Args:
          event: The event to wrap and serialize.

        Returns:
          The serialized envelope.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``sanitize`` prevents.
        """
        return json.dumps(self._envelope(event))

    def _body(self, items: list[_Item]) -> tuple[bytes, str]:
        """Concatenates the envelopes with no separator, as HEC's body format requires.

        The inherited NDJSON body would also be accepted by HEC, but this keeps the bytes on the
        wire exactly what they have always been, and concatenated objects are what Splunk
        documents for the endpoint.

        Args:
          items: The chunk's envelopes, known non-empty.

        Returns:
          The body bytes and the content type.

        Raises:
          None.
        """
        return "".join(item.rendered for item in items).encode("utf-8"), "application/json"

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
