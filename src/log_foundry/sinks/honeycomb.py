"""HoneycombSink — ship to Honeycomb's batch events API (arch §8, SPEC-009)."""

from __future__ import annotations

import http.client
import json
import urllib.request
from typing import Any, Unpack

from log_foundry import _diag
from log_foundry.sinks._batch import usable_results
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.http import (
    HTTPPlatformKwargs,
    HTTPSink,
    _holds_at,
    _holds_delimiter,
    _Item,
    _started_request,
    _valid_base_url,
    _valid_url,
    merge_headers,
)

__all__ = ["HoneycombSink"]

_SEGMENT_BREAKERS = "/?#\\"
"""What ends a path segment or starts another URL part, or what a WHATWG parser reads as ``/``."""

_DOT_SEGMENTS = (".", "..")
"""The two segments a normalizing hop removes from a path (RFC 3986 §5.2.4)."""

_DATASET_BASE = "http://localhost/1/batch/"
"""A fixed base the dataset is checked under, so a refusal is about the dataset and nothing else."""


def _shown(dataset: str) -> str:
    """Returns how a refusal ends: quoting the dataset, or saying why it does not.

    A ``/``, ``?`` or ``#`` would land outside the dataset's segment — ``d?api_key=…`` is a query,
    which is where a credential sits — and what precedes an ``@`` may be a password. Each is read
    in every form :func:`~log_foundry.sinks.http._holds_at` and
    :func:`~log_foundry.sinks.http._holds_delimiter` read, so a fullwidth ``＃`` (``d＃SECRET``)
    or a ``%3F`` hides what follows it too.

    Args:
      dataset: The dataset as the caller gave it.

    Returns:
      ``: '<dataset>'``, or fixed text when it holds an ``@``, ``/``, ``?`` or ``#`` in any of
      those forms.

    Raises:
      None.
    """
    if _holds_at(dataset) or _holds_delimiter(dataset):
        return "; the dataset is not quoted, since it holds an '@', '/', '?' or '#'"
    return f": {dataset!r}"


def _send_failure(url: str) -> Exception | None:
    """Returns the error ``http.client`` raises building the request line for a URL, or ``None``.

    Only the exception's type is used, for the refusal's text; the exception itself is never
    raised or chained, since its message or ``object`` holds the whole request line.

    Args:
      url: A URL :func:`~log_foundry.sinks.http._valid_url` has refused.

    Returns:
      The error, or ``None`` if the request line builds, when the refusal was for another reason.

    Raises:
      None.
    """
    request = urllib.request.Request(url)  # noqa: S310
    try:
        connection = _started_request(request.host, "POST", request.selector)
        connection.putheader("Host", request.host)
    except (ValueError, http.client.InvalidURL) as err:
        return err
    return None


def _carried_in_a_path(dataset: str, owner: str) -> None:
    """Refuses a dataset a request path cannot carry, naming the dataset rather than a URL.

    A literal ``@`` delivered as written, but every HTTP sink URL holding one outside its host is
    refused (:func:`~log_foundry.sinks.http._valid_url`), and ``%40`` is how a dataset with an ``@``
    in its name reaches it, so this says that rather than the shared reason, which is about a
    password ending a host. Anything else is held to ``_valid_url`` itself, on a fixed base, so the
    rule is the one every HTTP sink URL meets; its refusal would quote ``'http://localhost'``, the
    base this class chose, which tells the caller nothing, so it is replaced by one that names the
    ``http.client`` error by its type and quotes the dataset by :func:`_shown`'s rule. The
    replacement is raised outside the ``except``, so neither ``__cause__`` nor ``__context__`` is
    set.

    Args:
      dataset: The dataset as the caller gave it, already one path segment
        (:func:`_one_segment`).
      owner: The class refusing it, for the message.

    Returns:
      None.

    Raises:
      ValueError: If ``dataset`` holds an ``@``, or ``http.client`` cannot carry it in a request
        path — a space, a control character, a lone surrogate, or a character outside Latin-1.
    """
    if "@" in dataset:
        raise ValueError(
            f"{owner} dataset must not hold an '@'; write an '@' in a dataset as '%40'"
        )
    url = f"{_DATASET_BASE}{dataset}"
    try:
        _valid_url(url, owner, name="dataset")
    except ValueError:
        refused = True
    else:
        refused = False
    if refused:
        failure = _send_failure(url)
        kind = f" ({type(failure).__name__})" if failure is not None else ""
        raise ValueError(
            f"{owner} dataset cannot be carried in a request path{kind}{_shown(dataset)}"
        )


def _one_segment(dataset: str, owner: str) -> None:
    """Refuses a dataset that would not reach Honeycomb as the one path segment it names.

    The dataset is placed in ``/1/batch/<dataset>`` as written, so a character with meaning in a
    URL moved the events somewhere the caller never named: ``urllib`` drops ``#x`` as a fragment
    and ``?x`` starts a query, so ``d#x`` and ``d?x`` both landed in ``d`` with nothing counted,
    and ``/`` starts another segment, so ``a/b`` went to a different path. A backslash is not a
    character RFC 3986 admits in a path, so what a hop makes of it is not this sink's to promise,
    and the WHATWG parser reads it as ``/``. A dataset that is ``.`` or ``..`` is a dot-segment,
    which a normalizing hop removes, and so is its percent-encoded form (``%2e``, ``.%2E``), which
    RFC 3986 §6.2.2.2 and the WHATWG parser decode before removing it. An empty dataset, or one
    that is only whitespace, names no dataset at all. None of these could be relied on to reach
    the dataset named, so each is refused rather than encoded, and none of them goes anywhere new.
    The refusal quotes the dataset unless it holds an ``@``, ``/``, ``?`` or ``#`` in any form
    (:func:`_shown`).

    What stays is what delivered as written: ``%`` escapes, which are how a caller names a
    dataset ``http.client`` cannot carry raw; the sub-delimiters and ``:``, which ``urllib`` and
    ``http.client`` carry unchanged; and trailing whitespace, which ``urllib.parse.unwrap`` floors
    on every URL, so the dot and empty checks read the dataset after that same strip. A literal
    ``@`` is not checked here but by :func:`_carried_in_a_path`, which runs next.

    Args:
      dataset: The dataset as the caller gave it.
      owner: The class refusing it, for the message.

    Returns:
      None.

    Raises:
      ValueError: If ``dataset`` holds ``/``, ``?``, ``#`` or a backslash, is ``.`` or ``..``
        once stripped and its ``%2e`` decoded, or is empty once stripped. The message echoes
        the dataset and nothing else, and not the dataset either when it holds an ``@``, ``/``,
        ``?`` or ``#``.
    """
    segment = dataset.strip()
    dots = segment.lower().replace("%2e", ".")
    if not segment or dots in _DOT_SEGMENTS or any(c in dataset for c in _SEGMENT_BREAKERS):
        raise ValueError(
            f"{owner} dataset must be one path segment: not empty, '.' or '..', and without "
            f"'/', '?', '#' or a backslash{_shown(dataset)}"
        )


class HoneycombSink(HTTPSink):
    """POSTs events to Honeycomb's batch API for a dataset (FR-010).

    The request goes to ``/1/batch/<dataset>`` with an ``X-Honeycomb-Team`` header, in
    Honeycomb's ``[{"data": <event>}, ...]`` batch shape.

    It takes **no** transport lock (SPEC-028 FR-002) and **adds no post-close guard**
    (SPEC-032 FR-003), for the reasons :class:`~log_foundry.sinks.http.HTTPSink` records: there
    is no transport held and ``close()`` releases nothing.

    The ``200`` it answers is not the verdict: the body is an array of one ``{"status": ...}``
    object per event, ``202`` for an event accepted and a ``400`` with an ``error`` for one
    rejected, so :meth:`_handle_response` reads it, positionally and all of the chunk or none of
    it (SPEC-018), and counts what the server refused.

    Attributes:
      MAX_BATCH_COUNT: 1,000 — this library's conservative default, since Honeycomb documents no
        maximum event count for the batch endpoint.
      MAX_BATCH_BYTES: 1,000,000 — Honeycomb's documented 1 MB of uncompressed JSON for the
        Create Events endpoint.
      rejected_events: Events a ``2xx`` batch response reported with a status outside ``2xx``.
        Never retried by the sink: the chunk's other events landed, and a re-send would
        duplicate them. A chunk whose every event was rejected is undelivered, so an ``emit``
        with no delivered chunk raises and the worker's retry counts it again on each attempt —
        an upper bound, like ``failed``.
      dropped_unadjudicated: Events whose outcome a ``2xx`` batch response did not settle — a
        body that is empty, could not be read or decoded, is not an array of objects, has a
        length other than the chunk's, or holds an entry whose ``status`` is not an integer.
        Counted as an upper bound and never retried, for SPEC-018's reason: the request
        succeeded, so they may have landed.


    It keeps **no** client buffer (SPEC-036 FR-002): each ``emit`` is a request that has
    completed by the time it returns, and no client object outlives it holding data.
    """

    MAX_BATCH_COUNT = 1000
    MAX_BATCH_BYTES = 1_000_000
    _CREDENTIALS = "this sink's api_key argument"

    def __init__(
        self,
        api_key: str,
        dataset: str,
        *,
        url: str = "https://api.honeycomb.io",
        **http_kwargs: Unpack[HTTPPlatformKwargs],
    ) -> None:
        """Points the sink at a dataset's batch endpoint.

        Args:
          api_key: The key sent as ``X-Honeycomb-Team``.
          dataset: The target dataset, taken as one already percent-encoded path segment and
            placed in the path as written: ``my%20data`` reaches the dataset ``my data``, and a
            literal ``%`` is written ``%25``. Trailing whitespace is floored, as ``urllib`` floors
            it on every URL.
          url: The API base URL, to which ``/1/batch/<dataset>`` is appended.
          **http_kwargs: Forwarded to :class:`~log_foundry.sinks.http.HTTPSink`, typed as
            ``HTTPPlatformKwargs`` (SPEC-051 FR-005) — every keyword it takes except
            ``body_format``, which this sink pins to Honeycomb's JSON array.

        Returns:
          None.

        Raises:
          TypeError: If a forwarded ``max_retries`` or ``max_batch_count`` is not an ``int``, or
            a forwarded ``max_retry_after`` is not an ``int`` or ``float``, refused by
            :class:`~log_foundry.sinks.http.HTTPSink` naming this class (SPEC-066).
          ValueError: If ``dataset`` is not one path segment (:func:`_one_segment`), holds a
            literal ``@`` — written ``%40`` instead — or cannot be carried in a request path,
            when the message names the ``http.client`` error's type (:func:`_carried_in_a_path`).
            Each message names ``dataset`` rather than the ``url`` this class builds from both,
            whose own check names ``url`` (SPEC-049 FR-001), and quotes the dataset unless it
            holds an ``@``, ``/``, ``?`` or ``#``; or if ``url`` carries a user or password, or a
            query or fragment, which the appended path would land inside
            (:func:`_valid_base_url`), or an ``@`` anywhere else, refused naming ``url`` once the
            path is appended.
        """
        _one_segment(dataset, type(self).__name__)
        _carried_in_a_path(dataset, type(self).__name__)
        _valid_base_url(
            url, type(self).__name__, f"/1/batch/{dataset}", credentials=self._CREDENTIALS
        )
        headers = merge_headers({"X-Honeycomb-Team": api_key}, http_kwargs)
        super().__init__(
            f"{url.rstrip('/')}/1/batch/{dataset}",
            headers=headers,
            body_format="json_array",
            **http_kwargs,  # type: ignore[misc]
        )
        self.rejected_events = 0
        self.dropped_unadjudicated = 0

    def _render(self, event: dict[str, object]) -> str:
        """Serializes one event in Honeycomb's ``{"data": event}`` batch shape (FR-010).

        Args:
          event: The event to wrap and serialize.

        Returns:
          The serialized entry.

        Raises:
          TypeError: If the event is not JSON-serializable. Neither source of events should
            produce one: assembly coerces every value (``sanitize``), and a ``TransformSink``
            checks and coerces what its function returns (SPEC-058 FR-001). If one does,
            ``HTTPSink._items`` drops that event alone and counts it (SPEC-058 FR-005).
        """
        return json.dumps({"data": event})

    def _handle_response(self, payload: bytes, items: list[_Item]) -> bool:
        """Counts the events a ``2xx`` batch response rejected and reports whether any landed.

        Honeycomb's only per-event verdict is the body, so a body that does not carry one is no
        verdict, and the chunk is counted on ``dropped_unadjudicated`` rather than read as
        delivered. That includes ``b""``: an empty body and one that could not be read after the
        status line both arrive as ``b""`` (``HTTPSink._attempt``), and reading it as success would
        settle silently an all-rejected response whose body was cut short. It is the ``_bulk``
        sinks' reading, argued here for this destination's own reason, and the opposite of
        ``OTLPSink``'s, whose ``2xx`` needs no body to succeed.

        The array is positional — Honeycomb names no event in it — so it must prove it describes
        the chunk before entry *i* is read as event *i*: the same length, and every entry an
        object whose ``status`` is an ``int`` (a ``bool`` is not one). One entry that fails that
        leaves the chunk unadjudicated whole, because the success signal here is the status
        itself, and a missing one is neither an acceptance nor a rejection. A status in
        ``200``–``299`` is an acceptance and any other integer a rejection. The server's
        ``error`` text is never written anywhere (invariant 11).

        Never raises, on any bytes, and catches ``Exception`` rather than the decoding errors,
        for the reason ``ElasticsearchSink._parse_bulk_response`` gives: an exception escaping
        here escapes ``emit``, and the worker re-sends a batch the server may have accepted.

        Args:
          payload: The response body, or ``b""`` when it could not be read.
          items: The chunk the response describes.

        Returns:
          False only when every event of the chunk was rejected, so an ``emit`` whose every
          chunk came back that way raises (invariant 7); True otherwise, the unadjudicated case
          included, since the request succeeded and a re-send could duplicate what landed.

        Raises:
          None.
        """
        sent = len(items)
        try:
            data = json.loads(payload)
        except Exception:
            self._abandon_unadjudicated(sent)
            return True
        entries = usable_results(data)
        statuses = [status for status in map(_status, entries) if status is not None]
        if len(statuses) != sent or len(entries) != sent:
            self._abandon_unadjudicated(sent, readable=len(statuses))
            return True
        rejected = sum(1 for status in statuses if not 200 <= status < 300)
        if rejected:
            with self._counter_lock:
                self.rejected_events += rejected
            _diag.lost("event", rejected, f"{type(self).__name__}, rejected by the server")
        return rejected < sent

    def losses(self) -> SinkLosses:
        """Reports abandoned requests plus the events a batch response rejected or left unsettled.

        Args:
          None.

        Returns:
          The counters. ``failed`` sums the requests abandoned past the retry bound,
          ``rejected_events`` and ``dropped_unadjudicated``, because each is an event the server
          did not confirm; they stay apart on the instance for anyone who needs to tell them
          apart. ``dropped`` is the oversized events plus those whose render raised, as in the
          base class this overrides (SPEC-058 FR-005).

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(
                dropped=self.dropped_oversized + self._unrenderable,
                failed=self.failed + self.rejected_events + self.dropped_unadjudicated,
            )

    def _abandon_unadjudicated(self, sent: int, *, readable: int = 0) -> None:
        """Counts and announces a chunk whose ``2xx`` response did not settle its events' outcome.

        The whole chunk is charged because the response cannot be relied on to say which events
        landed, which makes the count an upper bound, like ``failed``, and the line says the
        events may have landed. It carries only counts and never the body, which is the
        server's text (invariant 11).

        Args:
          sent: How many events were in the request.
          readable: How many entries carried a status this sink could read — none when any
            element of the array is not an object, since such an array is not read at all.

        Returns:
          None.

        Raises:
          None.
        """
        with self._counter_lock:
            self.dropped_unadjudicated += sent
        _diag.lost(
            "event",
            sent,
            f"{type(self).__name__} could not adjudicate a batch response "
            f"({sent} event(s) sent, {readable} readable status(es)); "
            f"they may have landed; not retried",
        )


def _status(entry: dict[str, Any]) -> int | None:
    """Returns one batch-response entry's per-event status, or ``None`` when it carries none.

    ``bool`` is refused although it is an ``int`` subclass, because ``true`` is not a status and
    reading it as ``1`` would score it a rejection; a float such as ``202.0`` is refused too,
    since Honeycomb writes the status as an integer and a server writing anything else is not
    describing the event in a form this sink can trust.

    Args:
      entry: One object of the response's array.

    Returns:
      The status, or ``None`` when it is absent or not an integer.

    Raises:
      None.
    """
    status = entry.get("status")
    if isinstance(status, int) and not isinstance(status, bool):
        return status
    return None
