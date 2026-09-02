"""LokiSink — push events to Grafana Loki (arch §8, SPEC-009)."""

from __future__ import annotations

import json
from typing import Unpack

from log_foundry.sinks._time import epoch_nanos
from log_foundry.sinks.http import HTTPPlatformKwargs, HTTPSink, _Item

__all__ = ["LokiSink"]

_PUSH_PATH = "/loki/api/v1/push"

_STREAM_FRAMING = 24
"""Bytes of per-stream and per-value framing charged to each item (SPEC-038 FR-001).

``{"stream":,"values":[]}`` is 23 characters around one label map, plus one for the comma that
joins the value to its neighbour. Charged per item rather than per stream because the chunker
measures items and cannot know how they will group.
"""


class LokiSink(HTTPSink):
    """POSTs a Loki push payload of streams, labels and nanosecond values (FR-004).

    Events are grouped into streams by a configurable set of label keys, each stream carrying
    values of ``[<nanosecond_timestamp_str>, <log_line>]``. Timestamps are derived by parsing the
    event's ISO-8601 ``timestamp``, falling back to emit-time now.

    It takes **no** transport lock (SPEC-028 FR-002) and **adds no post-close guard**
    (SPEC-032 FR-003), for the reasons :class:`~log_foundry.sinks.http.HTTPSink` records: there
    is no transport held and ``close()`` releases nothing.

    Attributes:
      MAX_BATCH_COUNT: 1,000 — this library's conservative default; Loki bounds a push by size,
        not by entry count.
      MAX_BATCH_BYTES: 4,000,000 — chosen, not vendor-fixed. Loki's own default is far larger
        (``distributor.max_recv_msg_size``, 100 MB for the compressed body), but that is an
        operator-tunable server setting and a hosted Loki is routinely configured well below it,
        so the default here is the conservative one. Raise it with ``max_batch_bytes=``.


    It keeps **no** client buffer (SPEC-036 FR-002): each ``emit`` is a request that has
    completed by the time it returns, and no client object outlives it holding data.
    """

    MAX_BATCH_COUNT = 1000
    MAX_BATCH_BYTES = 4_000_000

    def __init__(self, url: str, *, labels: tuple[str, ...] = ("service", "env", "level"),
                 **http_kwargs: Unpack[HTTPPlatformKwargs]) -> None:
        """Points the sink at a Loki push endpoint.

        Args:
          url: The Loki base URL, to which the push path is appended.
          labels: The event keys promoted to stream labels.
          **http_kwargs: Forwarded to :class:`~log_foundry.sinks.http.HTTPSink`, typed as
            ``HTTPPlatformKwargs`` (SPEC-051 FR-005) — every keyword it takes except
            ``body_format``, which this sink pins to Loki's JSON array.

        Returns:
          None.

        Raises:
          None.
        """
        self._labels = labels
        super().__init__(
            url.rstrip("/") + _PUSH_PATH, body_format="json_array", **http_kwargs
        )

    def _render(self, event: dict[str, object]) -> str:
        """Serializes one event as the ``[timestamp, line]`` value Loki's push payload carries.

        This is the exact text that reaches the wire, so the chunker's byte budget measures what
        is actually sent rather than the event alone.

        Args:
          event: The event to serialize.

        Returns:
          The serialized push value.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``sanitize`` prevents.
        """
        return json.dumps([str(epoch_nanos(event.get("timestamp"))), json.dumps(event)])

    def _item_size(self, rendered: str, event: dict[str, object]) -> int:
        """Charges the event its value bytes *and* the stream framing its labels may cost.

        The inherited accounting charges one separator per item, which is right for a body that
        concatenates. Loki's does not: it wraps each label set in ``{"stream":…,"values":[…]}``,
        so a chunk's body grows with the number of distinct label sets in it, not just with the
        values. Measured before this, 4,000 events carrying a per-pod ``service`` label produced
        a body 61% past the budget.

        Charging every item for a whole stream header over-counts whenever events share a label
        set, which is the safe direction: it yields more requests than strictly needed rather
        than one the destination rejects.

        Args:
          rendered: The event's serialized push value.
          event: The source event, whose labels set the framing cost.

        Returns:
          The bytes to charge the chunker.

        Raises:
          None.
        """
        labels = json.dumps(self._labels_of(event))
        return len(rendered.encode("utf-8")) + len(labels.encode("utf-8")) + _STREAM_FRAMING

    def _body_reserve(self) -> int:
        """Withholds the ``{"streams":[…]}`` wrapper, which no item is charged for.

        Fourteen bytes, and not the inherited JSON-array reserve of one: this body is an object
        wrapping an array, not the bare array ``body_format="json_array"`` otherwise implies.

        Args:
          None.

        Returns:
          The bytes to withhold from the chunker's budget.

        Raises:
          None.
        """
        return 14

    def _labels_of(self, event: dict[str, object]) -> dict[str, str]:
        """Promotes the configured event keys to stream labels.

        Args:
          event: The event to read.

        Returns:
          The labels, which are absent rather than empty when the event lacks the key.

        Raises:
          None.
        """
        return {key: str(event[key]) for key in self._labels if key in event}

    def _body(self, items: list[_Item]) -> tuple[bytes, str]:
        """Groups one chunk's values into labelled streams and builds the push payload (FR-004).

        Grouping happens per chunk rather than per batch, which is what makes chunking safe
        here: streams are re-formed inside each request, so two chunks may each carry a stream
        with the same labels and Loki accepts both. The payload is assembled from the
        already-rendered values rather than re-serialized, so each event is encoded once.

        Args:
          items: The chunk's rendered values, known non-empty.

        Returns:
          The body bytes and the content type.

        Raises:
          None.
        """
        streams: dict[tuple[tuple[str, str], ...], tuple[dict[str, str], list[str]]] = {}
        for item in items:
            labels = self._labels_of(item.event)
            streams.setdefault(tuple(sorted(labels.items())), (labels, []))[1].append(
                item.rendered
            )
        parts = [
            f'{{"stream":{json.dumps(labels)},"values":[{",".join(values)}]}}'
            for labels, values in streams.values()
        ]
        return f'{{"streams":[{",".join(parts)}]}}'.encode(), "application/json"
