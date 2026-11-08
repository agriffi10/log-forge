"""ElasticsearchSink / OpenSearchSink — index events via the ``_bulk`` API (arch §8, SPEC-009)."""

from __future__ import annotations

import json
from typing import Unpack

from log_foundry import _diag
from log_foundry.sinks._batch import usable_results
from log_foundry.sinks._engine import When, run_context
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.http import (
    HTTPRetryKwargs,
    HTTPSink,
    _Item,
    _read_as_1x,
    _read_failed,
    _valid_base_url,
)
from log_foundry.sinks.verdict import Outcome

__all__ = ["ElasticsearchSink", "OpenSearchSink"]


class ElasticsearchSink(HTTPSink):
    """POSTs events to an Elasticsearch ``_bulk`` endpoint, parsing per-item errors (FR-003).

    Each event becomes an action line followed by its source line, POSTed as newline-delimited
    JSON. The bulk response is inspected per item, so a partial failure is counted and logged
    without discarding the successfully-indexed items.

    Attributes:
      item_errors: Count of bulk items the server reported as failed, distinct from ``failed``,
        which counts whole requests abandoned past the retry bound.
      dropped_unadjudicated: Events whose outcome a ``_bulk`` response did not settle, which is
        every route to ``HTTPSink._unadjudicated``: ``errors`` was truthy while the ``items``
        array did not line up with the batch sent or no readable item carried an error, or the
        body could not be decoded or was not an object — an empty body, and one that could not
        be read off the connection at all, among them. Abandoned rather than retried, for
        SPEC-018's reason: the request succeeded, so re-sending would duplicate what landed.

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
          TypeError: If a forwarded ``max_retries`` or ``max_batch_count`` is not an ``int``, or
            a forwarded ``max_retry_after`` is not an ``int`` or ``float``, refused by
            :class:`~log_foundry.sinks.http.HTTPSink` naming this class (SPEC-066).
          ValueError: If the index is empty or contains whitespace, or ``url`` carries a user or
            password, or a query or fragment, which the appended ``/_bulk`` would land inside
            (:func:`~log_foundry.sinks.http._valid_base_url`) — a ``_bulk`` parameter such as
            ``?pipeline=`` never reached ``_bulk``, so it was never applied — or holds an ``@``
            anywhere else, refused naming ``url`` once ``/_bulk`` is appended; or if ``auth``
            accompanies an ``Authorization`` header in a forwarded ``headers``, in any case,
            refused by :class:`~log_foundry.sinks.http.HTTPSink` naming this class.
        """
        _valid_base_url(url, type(self).__name__, "/_bulk", credentials=self._CREDENTIALS)
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

    def _render(self, event: dict[str, object]) -> str:
        """Serializes one event as its ``_bulk`` action line and source line.

        Both lines are one item so the pair can never be split across two requests, and so the
        chunker's byte budget charges the action line the wire actually carries.

        Args:
          event: The event to index.

        Returns:
          The two newline-joined lines, without the trailing newline :meth:`_body` adds.

        Raises:
          TypeError: If the event is not JSON-serializable. Neither source of events should
            produce one: assembly coerces every value (``sanitize``), and a ``TransformSink``
            checks and coerces what its function returns (SPEC-058 FR-001). If one does,
            ``HTTPSink._items`` drops that event alone and counts it (SPEC-058 FR-005).
        """
        action = json.dumps({"index": {"_index": self._index}})
        return f"{action}\n{json.dumps(event)}"

    def _handle_response(self, payload: bytes, items: list[_Item]) -> bool:
        """Reads the chunk's ``_bulk`` response as a 1.x caller did: ``False`` when nothing indexed.

        Kept for a direct 1.x caller (SPEC-073 FR-003). It runs :meth:`_outcomes` in a context of
        its own, committed at once, so the counters move as they did, and returns ``False``
        exactly when every outcome is ``REJECTED`` — every item carried an error. A body this
        sink cannot read returns ``True``: the request succeeded, and its events are counted on
        ``dropped_unadjudicated``.

        Args:
          payload: The response body; an ``_UnreadBody``, equal to ``b""``, when it could not be
            read.
          items: The chunk the response describes.

        Returns:
          False when every item in a non-empty chunk carried an error, True otherwise.

        Raises:
          None.
        """
        return _read_as_1x(lambda: self._outcomes(payload, items))

    def _outcomes(self, payload: bytes, items: list[_Item]) -> tuple[Outcome, ...]:
        """Reads each item's fate from the chunk's ``_bulk`` response (SPEC-073 FR-003).

        The ``items`` array is positional, so the rule ``sinks/_batch.py`` states applies here
        too (SPEC-018): a length disagreement is evidence the arrays do not describe each other,
        not an invitation to read the overlap. An aligned array marks each item with an error
        ``REJECTED`` — counted on ``item_errors`` and announced, whatever status it carries,
        never resent — and every other item ``ACCEPTED``. A body with no truthy ``errors`` is
        all ``ACCEPTED``, unverified: this sink does not check it is ``_bulk``-shaped, so ``{}``
        and an error document pass, since charging an absent key would charge every success
        from an endpoint that omits it (``docs/architecture.md`` §12).

        Every other body is given up on through ``HTTPSink._unadjudicated``, its events
        ``UNKNOWN``: a body that could not be read off the connection (``_read_failed``, asked
        first, SPEC-070 FR-004), one that cannot be decoded — an empty one included, since
        ``_bulk``'s only verdict is in the body — one that is not an object, and an ``errors:
        true`` whose array is the wrong length or holds no readable error, which contradicts
        itself. The request succeeded, so re-sending would duplicate whatever did land. The
        decode is guarded by ``Exception`` rather than the decoding errors: ``RecursionError``
        and ``MemoryError`` are neither, and the round's own guard would otherwise take the
        raise with a less exact line.

        Args:
          payload: The response body; an ``_UnreadBody`` when it could not be read.
          items: The chunk the response describes.

        Returns:
          One outcome per item.

        Raises:
          None.
        """
        sent = len(items)
        if _read_failed(payload):
            return self._unadjudicated(items, _detail(sent))
        try:
            data = json.loads(payload)
        except Exception:
            return self._unadjudicated(items, _detail(sent))
        if not isinstance(data, dict):
            return self._unadjudicated(items, _detail(sent))
        if not data.get("errors"):
            return (Outcome.ACCEPTED,) * sent
        entries = usable_results(data.get("items"))
        errors = sum(1 for entry in entries if _has_error(entry))
        if len(entries) != sent or not errors:
            return self._unadjudicated(items, _detail(sent, len(entries), errors))
        context = run_context()
        name = type(self).__name__
        context.count(self, "item_errors", errors, when=When.ALWAYS)
        context.announce(
            lambda: _diag.lost("bulk item", errors, f"{name}, rejected by the server"),
            when=When.ALWAYS,
        )
        return tuple(
            Outcome.REJECTED if _has_error(entry) else Outcome.ACCEPTED for entry in entries
        )

    def losses(self) -> SinkLosses:
        """Reports abandoned requests plus server-rejected bulk items (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The counters. All three sources are summed into ``failed`` because each is an event
          the server did not confirm; they stay apart on the instance for anyone who needs to
          tell "the request never landed" from "the request landed and these items bounced" from
          "the response did not say". That last one covers a body that could not be read as well
          as one that could not be adjudicated. On every route those events may have landed, so
          it is an upper bound on loss. ``dropped`` is the oversized events plus those whose
          render raised, as in the base class this overrides (SPEC-058 FR-005).

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(
                dropped=self.dropped_oversized + self._unrenderable,
                failed=self.failed + self.item_errors + self.dropped_unadjudicated,
            )


def _detail(sent: int, readable: int = 0, errors: int = 0) -> str:
    """The unadjudicated line's detail for a ``_bulk`` response, counts only, never the body.

    Args:
      sent: How many events were in the request.
      readable: How many readable items the response carried.
      errors: How many of those items reported an error.

    Returns:
      The detail ``HTTPSink._unadjudicated`` writes after the class name.

    Raises:
      None.
    """
    return (
        f"could not adjudicate a _bulk response ({sent} event(s) sent, {readable} readable "
        f"item(s), {errors} error(s))"
    )


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
