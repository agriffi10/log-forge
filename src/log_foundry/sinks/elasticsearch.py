"""ElasticsearchSink / OpenSearchSink — index events via the ``_bulk`` API (arch §8, SPEC-009)."""

from __future__ import annotations

import json
from typing import Unpack

from log_foundry import _diag
from log_foundry.sinks._batch import usable_results
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.http import HTTPRetryKwargs, HTTPSink, _Item

__all__ = ["ElasticsearchSink", "OpenSearchSink"]


class ElasticsearchSink(HTTPSink):
    """POSTs events to an Elasticsearch ``_bulk`` endpoint, parsing per-item errors (FR-003).

    Each event becomes an action line followed by its source line, POSTed as newline-delimited
    JSON. The bulk response is inspected per item, so a partial failure is counted and logged
    without discarding the successfully-indexed items.

    Attributes:
      item_errors: Count of bulk items the server reported as failed, distinct from ``failed``,
        which counts whole requests abandoned past the retry bound.
      dropped_unadjudicated: Events whose outcome a ``_bulk`` response did not describe, because
        its ``items`` array did not line up with the batch sent. Abandoned rather than retried,
        for SPEC-018's reason: the request succeeded, so re-sending would duplicate what landed.

    It takes **no** transport lock (SPEC-028 FR-002) and **adds no post-close guard**
    (SPEC-032 FR-003), for the reasons :class:`~log_foundry.sinks.http.HTTPSink` records: there
    is no transport held and ``close()`` releases nothing.

    Attributes:
      MAX_BATCH_COUNT: 1,000 — this library's conservative default; Elasticsearch bounds a bulk
        request by size, not by document count.
      MAX_BATCH_BYTES: 10,000,000 — chosen *below* the documented hard limit rather than at it.
        Elasticsearch "limits the maximum size of a HTTP request to 100mb by default", but its
        bulk guidance is to find a working size by experiment rather than to send the largest
        request the server will accept, and a 100 MB bulk is a poor default for a log shipper.
        Raise it with ``max_batch_bytes=``.


    It keeps **no** client buffer (SPEC-036 FR-002): each ``emit`` is a request that has
    completed by the time it returns, and no client object outlives it holding data.
    """

    MAX_BATCH_COUNT = 1000
    MAX_BATCH_BYTES = 10_000_000

    def __init__(self, url: str, *, index: str, auth: str | tuple[str, str] | None = None,
                 **http_kwargs: Unpack[HTTPRetryKwargs]) -> None:
        """Points the sink at a cluster's ``_bulk`` endpoint.

        Args:
          url: The cluster base URL, to which ``/_bulk`` is appended.
          index: The target index named in every action line. Whitespace and the empty string
            are refused (SPEC-049 FR-004). The mechanism is **not** frame corruption — the
            audit recorded it as such and that does not reproduce, because ``json.dumps``
            escapes the ``_bulk`` action line correctly and a name the cluster rejects already
            arrives as a counted per-item error. It is refused because an index name the
            cluster can never accept makes every batch fail for the life of the process, and a
            startup error is the honest place to say so.
          auth: A bearer token, or a ``(user, password)`` pair for basic auth.
          **http_kwargs: Forwarded to :class:`~log_foundry.sinks.http.HTTPSink`, typed as
            ``HTTPRetryKwargs`` (SPEC-051 FR-005) — every keyword it takes except
            ``body_format``, pinned to the bulk API's NDJSON, and ``auth``, which is a
            parameter of this sink's own.

        Returns:
          None.

        Raises:
          ValueError: If the index is empty or contains whitespace.
        """
        if not index or any(char.isspace() for char in index):
            raise ValueError(
                f"ElasticsearchSink index must be non-empty and contain no whitespace, "
                f"not {index!r}"
            )
        self._index = index
        super().__init__(
            url.rstrip("/") + "/_bulk", auth=auth, body_format="ndjson", **http_kwargs
        )
        self.item_errors = 0
        self.dropped_unadjudicated = 0

    def _render(self, event: dict[str, object]) -> str:
        """Serializes one event as its ``_bulk`` action line and source line.

        Both lines are one item so the pair can never be split across two requests, and so the
        chunker's byte budget charges the action line the wire actually carries.

        Args:
          event: The event to index.

        Returns:
          The two newline-joined lines, without the trailing newline :meth:`_body` adds.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``sanitize`` prevents.
        """
        action = json.dumps({"index": {"_index": self._index}})
        return f"{action}\n{json.dumps(event)}"

    def _handle_response(self, payload: bytes, items: list[_Item]) -> bool:
        """Parses the chunk's ``_bulk`` items and reports whether anything was indexed.

        Returning ``False`` rather than raising is what lets a sibling chunk that succeeded
        stand: :meth:`~log_foundry.sinks.http.HTTPSink.emit` raises only when *every* chunk came
        back empty-handed, which is the total failure the worker's retry exists for, while a
        batch that indexed some of its chunks must not be re-sent whole (SPEC-026 FR-001).

        Args:
          payload: The response body.
          items: The chunk the response describes.

        Returns:
          False when every item in the chunk carried an error, True otherwise — including for a
          body this sink cannot read, since the request itself succeeded.

        Raises:
          None.
        """
        return not self._parse_bulk_response(payload, len(items))

    def losses(self) -> SinkLosses:
        """Reports abandoned requests plus server-rejected bulk items (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The counters. All three sources are summed into ``failed`` because each is an event
          the server did not confirm; they stay apart on the instance for anyone who needs to
          tell "the request never landed" from "the request landed and these items bounced" from
          "the response did not say".

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(
                dropped=self.dropped_oversized,
                failed=self.failed + self.item_errors + self.dropped_unadjudicated,
            )

    def _parse_bulk_response(self, payload: bytes, sent: int) -> bool:
        """Counts rejected items and reports whether the response proves nothing was indexed.

        The ``items`` array is positional, so the rule ``sinks/_batch.py`` states applies here
        too (SPEC-018): a length disagreement is evidence the arrays do not describe each other,
        not an invitation to read the overlap. Counting errors does not depend on position, but
        concluding "all of them failed" does, so that conclusion is gated on the length — a
        longer array could otherwise make a partial success raise and have the worker's retry
        duplicate what landed, and a shorter one could report a total failure as success.

        Shape is adjudicated as well as length, and a body saying ``errors: true`` while no
        entry yields a readable one contradicts itself, which is the same "cannot tell" as a
        short array; reading it as "no items failed" is how a total failure came back as
        success. An array that cannot be adjudicated is counted and left alone, because the
        request itself succeeded and re-sending would duplicate whatever did land.

        Args:
          payload: The response body.
          sent: How many events were in the request.

        Returns:
          True when every item failed. An unparseable or errors-free body returns False: the
          request succeeded, and a body this sink cannot read is not evidence against that.

        Raises:
          None.
        """
        try:
            data = json.loads(payload)
        except (ValueError, TypeError):
            return False
        if not isinstance(data, dict) or not data.get("errors"):
            return False
        items = usable_results(data.get("items"))
        errors = sum(1 for item in items if _has_error(item))
        if len(items) != sent or not errors:
            with self._counter_lock:
                self.dropped_unadjudicated += sent
            _diag.lost(
                "event",
                sent,
                f"{type(self).__name__} could not adjudicate a _bulk response "
                f"({sent} event(s) sent, {len(items)} readable item(s), {errors} error(s)); "
                f"not retried",
            )
            return False
        with self._counter_lock:
            self.item_errors += errors
        _diag.lost("bulk item", errors, f"{type(self).__name__}, rejected by the server")
        return errors == sent


def _has_error(item: dict[str, object]) -> bool:
    """Reports whether a ``_bulk`` item failed under any of its action keys.

    Every value is examined rather than just the first: a real response carries one key per
    item, but reading only the first would score an item whose error sits under a second key as
    a success, and a miscounted success is what turns a total failure into a partial one.

    Args:
      item: One entry of the response's ``items`` array.

    Returns:
      True when any action key reports an error.

    Raises:
      None.
    """
    return any(isinstance(result, dict) and result.get("error") for result in item.values())


class OpenSearchSink(ElasticsearchSink):
    """OpenSearch reuses the Elasticsearch ``_bulk`` protocol verbatim (FR-003).

    Endpoint and auth differ only by configuration, so this is a straight reuse.


    It keeps **no** client buffer (SPEC-036 FR-002): each ``emit`` is a request that has
    completed by the time it returns, and no client object outlives it holding data.
    """
