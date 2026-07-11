"""LokiSink — push events to Grafana Loki (arch §8, SPEC-009).

A specialization of :class:`~log_forge.sinks.http.HTTPSink` that builds Loki's push payload: events
are grouped into ``streams`` by a configurable set of label keys (e.g. ``service``/``env``/
``level``), each carrying ``values`` of ``[<nanosecond_timestamp_str>, <log_line>]``. Timestamps are
derived by parsing the event's ISO-8601 ``timestamp`` (falling back to emit-time now).
"""

from __future__ import annotations

import json

from log_forge.sinks._time import epoch_nanos
from log_forge.sinks.http import HTTPSink

__all__ = ["LokiSink"]

_PUSH_PATH = "/loki/api/v1/push"


class LokiSink(HTTPSink):
    """POST a Loki push payload (streams + labels + nanosecond values) (FR-004)."""

    def __init__(self, url: str, *, labels: tuple[str, ...] = ("service", "env", "level"),
                 **http_kwargs: object) -> None:
        self._labels = labels
        super().__init__(
            url.rstrip("/") + _PUSH_PATH, body_format="json_array", **http_kwargs  # type: ignore[arg-type]
        )

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Group events into labelled streams and POST the push payload (FR-004)."""
        if not batch:
            return
        # key = sorted label tuple -> (label dict, list of [ns_ts, line])
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
