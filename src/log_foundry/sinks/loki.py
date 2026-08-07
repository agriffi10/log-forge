"""LokiSink — push events to Grafana Loki (arch §8, SPEC-009)."""

from __future__ import annotations

import json

from log_foundry.sinks._time import epoch_nanos
from log_foundry.sinks.http import HTTPSink

__all__ = ["LokiSink"]

_PUSH_PATH = "/loki/api/v1/push"


class LokiSink(HTTPSink):
    """POSTs a Loki push payload of streams, labels and nanosecond values (FR-004).

    Events are grouped into streams by a configurable set of label keys, each stream carrying
    values of ``[<nanosecond_timestamp_str>, <log_line>]``. Timestamps are derived by parsing the
    event's ISO-8601 ``timestamp``, falling back to emit-time now.
    """

    def __init__(self, url: str, *, labels: tuple[str, ...] = ("service", "env", "level"),
                 **http_kwargs: object) -> None:
        """Points the sink at a Loki push endpoint.

        Args:
          url: The Loki base URL, to which the push path is appended.
          labels: The event keys promoted to stream labels.
          **http_kwargs: Forwarded to :class:`~log_foundry.sinks.http.HTTPSink`.

        Returns:
          None.

        Raises:
          None.
        """
        self._labels = labels
        super().__init__(
            url.rstrip("/") + _PUSH_PATH, body_format="json_array", **http_kwargs  # type: ignore[arg-type]
        )

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Groups events into labelled streams and POSTs the push payload (FR-004).

        Args:
          batch: The events to push. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: If the request was abandoned past the retry bound.
        """
        if not batch:
            return
        streams: dict[tuple[tuple[str, str], ...], tuple[dict[str, str], list[list[str]]]] = {}
        for event in batch:
            labels = {key: str(event[key]) for key in self._labels if key in event}
            stream_key = tuple(sorted(labels.items()))
            value = [str(epoch_nanos(event.get("timestamp"))), json.dumps(event)]
            streams.setdefault(stream_key, (labels, []))[1].append(value)
        payload = {
            "streams": [
                {"stream": labels, "values": values} for labels, values in streams.values()
            ]
        }
        body = json.dumps(payload).encode("utf-8")
        self._send(body, content_type="application/json")
