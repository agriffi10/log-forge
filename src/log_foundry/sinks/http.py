"""HTTPSink — POST batches over the stdlib ``urllib`` (arch §8, §9.1, SPEC-009)."""

from __future__ import annotations

import gzip as _gzip
import json
import threading
import urllib.error
import urllib.request
from base64 import b64encode
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn

from log_foundry import _diag
from log_foundry.sinks._chunk import chunk_items
from log_foundry.sinks._retry import clamp_server_delay, wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["HTTPSink", "merge_headers"]

_BACKOFF_BASE = 0.1

DEFAULT_MAX_BATCH_COUNT = 1000
"""Entries one request may carry, unless a subclass documents its destination's own (FR-001).

A thousand is Datadog's published array limit and a conservative default everywhere else: no
destination in this family documents a *smaller* count, and a count bound is the cheap half of
the guard, since it holds whatever the per-event size turns out to be.
"""

DEFAULT_MAX_BATCH_BYTES = 5_000_000
"""Bytes one request's body may reach, unless a subclass documents its destination's own.

Five million is the largest limit published by any sink in this family (Datadog's uncompressed
payload ceiling), so it is the right *base-class* default for an arbitrary endpoint the library
knows nothing about, while every subclass that has a documented figure sets its own below it.
"""

MAX_SPLIT_DEPTH = 4
"""Halvings a ``413`` may provoke before the chunk is abandoned instead (FR-001 AC-4).

Splitting is a correction for a budget somewhat larger than the destination's real limit, and
four halvings cover a limit down to a sixteenth of the configured one. Past that the
configuration is wrong rather than slightly off, and the right answer is to stop and say so: the
recursion is otherwise ``2N-1`` requests for ``N`` events, measured at 11,954 requests for one
5,980-event exit backlog against an endpoint that answers ``413`` unconditionally. A cap turns
that into at most ``2**(MAX_SPLIT_DEPTH+1) - 1`` per originally-refused chunk.
"""


@dataclass(frozen=True)
class _Item:
    """One event rendered into this sink's per-event wire form, with its measured size.

    Rendering once and chunking over the result is the ``KinesisSink``/``FirehoseSink`` idiom:
    it costs one ``json.dumps`` per event rather than one to measure and another to build the
    body, and it makes the byte budget exact rather than estimated.

    Attributes:
      event: The source event, kept so a :meth:`HTTPSink._body` that groups rather than
        concatenates — Loki's, which re-forms streams per chunk — can read the fields it groups
        on without re-parsing what :meth:`HTTPSink._render` just serialized.
      rendered: The event's serialized form, without the separator that joins it to its
        neighbours.
      size: ``rendered``'s UTF-8 length plus one byte for that separator. The two bytes of a
        JSON array's brackets are not modelled, which is why the budget is a bound on the body
        rather than its exact length.
    """

    event: dict[str, object]
    rendered: str
    size: int


class _PayloadTooLarge(SinkDeliveryError):
    """A ``413`` for a chunk the caller may be able to split and re-send (FR-001 AC-4).

    It subclasses ``SinkDeliveryError`` so that every pre-existing caller of :meth:`HTTPSink._send`
    — ``SentrySink`` above all — keeps catching it exactly as before. Only the chunk loop, which
    passes ``splittable=True``, ever sees it as its own type.
    """


DEFAULT_MAX_RETRY_AFTER = 30.0
"""Ceiling on a server-supplied ``Retry-After``, in seconds (SPEC-027 FR-001).

Thirty rather than something smaller because a rate-limited platform asking for half a minute is
making a reasonable request, and rather than something larger because the total is what a caller
with an execution deadline pays: at the default retries the worst case is 90 s, which stays
inside a typical serverless timeout while leaving room for the request itself.
"""


def merge_headers(base: dict[str, str], http_kwargs: dict[str, object]) -> dict[str, str]:
    """Merges a platform sink's own headers with any caller-supplied ones, caller winning.

    This is shared by the SaaS sinks that set a fixed auth header — Datadog, Splunk, New Relic,
    Honeycomb.

    Args:
      base: The sink's own headers, updated in place.
      http_kwargs: The caller's keyword arguments, mutated by popping ``headers`` out so the
        rest can be forwarded to :class:`HTTPSink` without a duplicate argument.

    Returns:
      The merged headers.

    Raises:
      None.
    """
    caller = http_kwargs.pop("headers", None)
    if isinstance(caller, dict):
        base.update(caller)
    return base


class HTTPSink:
    """A :class:`~log_foundry.sinks.base.Sink` that POSTs each batch to an HTTP endpoint.

    This is the dependency-free base every other HTTP platform sink builds on: it re-chunks a
    batch to the destination's limits, serializes each chunk, applies headers, auth and optional
    gzip, POSTs it, and retries ``429`` and ``5xx`` with bounded exponential backoff. These are
    terminal, direct-ship sinks — per arch §9.1 they couple application delivery to the
    destination's availability, so for durability put a queue in front.

    **Subclasses extend it by overriding hooks, never** :meth:`emit` (FR-001). The chunk loop
    lives here, so a subclass that overrode ``emit`` would deliver whatever batch it was handed —
    which is what three shipped subclasses did, and why ``Worker._final_drain`` could hand this
    family an entire exit backlog (5,980 events measured) in one request that the destination
    then rejected whole. The hooks are :meth:`_render`, :meth:`_body` and
    :meth:`_handle_response`; a test asserts no subclass defines ``emit``.

    Credentials passed as ``auth`` are sent on every request to the URL as given, and nothing
    here requires ``https://``: over a plaintext endpoint a bearer token, and a basic-auth pair,
    travel in the clear.

    **The worst case is now per chunk, not per batch** (FR-001). One chunk costs ``max_retries``
    waits, each at most ``max_retry_after`` when the server sends a ``Retry-After`` and
    exponential otherwise — 90 s at the defaults, plus the request timeouts — and an ``emit``
    pays that for *each* chunk it produces. All of it runs on the single drain thread, so it is a
    pause on all log delivery, and a large enough backlog of failing chunks can outlast
    ``shutdown()``'s own timeout, which then expires and leaves the sink open by SPEC-027
    FR-004's rule. Two things bound it: once the worker's stop event is set the retries are
    dropped to a single attempt per chunk, and the ``413`` split stops halving. Sizing
    ``max_batch_bytes`` to the destination is what keeps the chunk count low in the first place.

    Attributes:
      MAX_BATCH_COUNT: Entries one request may carry. A subclass overrides it with its
        destination's documented figure.
      MAX_BATCH_BYTES: Bytes one request's body may reach, likewise.
      MAX_EVENT_BYTES: A destination's published limit on a *single* event, or ``None`` when it
        publishes none. Distinct from the request budget: Datadog accepts a 5 MB payload but
        caps one log at 1 MB, so an event between the two is rejected by a limit the request
        budget cannot see.
      failed: Requests abandoned past the retry bound, counted per chunk.
      dropped_oversized: Events dropped for exceeding the per-event ceiling, plus those a
        destination answered ``413`` to and no split could rescue.

    The two lifecycle decisions this class records, inherited by every platform subclass. It
    takes **no** transport lock (SPEC-028 FR-002): there is no transport to guard, since
    ``urllib`` opens a fresh connection per request and nothing is rebound after construction.
    And it **adds no post-close guard** (SPEC-032 FR-003), because ``close()`` releases nothing —
    a batch emitted afterwards still reaches the endpoint, and refusing it would be loss the
    library invented rather than loss it reported.
    """

    MAX_BATCH_COUNT = DEFAULT_MAX_BATCH_COUNT
    MAX_BATCH_BYTES = DEFAULT_MAX_BATCH_BYTES
    MAX_EVENT_BYTES: int | None = None

    def __init__(
        self,
        url: str,
        *,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        auth: str | tuple[str, str] | None = None,
        body_format: str = "ndjson",
        timeout: float = 5.0,
        gzip: bool = False,
        max_retries: int = 3,
        max_retry_after: float = DEFAULT_MAX_RETRY_AFTER,
        max_batch_count: int | None = None,
        max_batch_bytes: int | None = None,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        """Configures the endpoint, encoding, credentials and retry bounds.

        Args:
          url: The endpoint to POST to.
          method: The HTTP method.
          headers: Headers that override the sink's own defaults.
          auth: A bearer token, or a ``(user, password)`` pair for basic auth.
          body_format: ``"ndjson"`` or ``"json_array"``.
          timeout: Seconds allowed per request.
          gzip: Whether to compress the body.
          max_retries: Retries after a failing request, floored at zero as ``Worker._emit``
            floors its own (SPEC-021) — a negative value otherwise skipped the loop entirely, so
            the request was abandoned with no attempt made and no counter moved.
          max_retry_after: Ceiling on a server-supplied ``Retry-After``. It is neither floored
            nor rejected here, because ``clamp_server_delay`` refuses an unusable ceiling and
            falls back to exponential backoff, keeping the validation in one place; it is stored
            as given so a caller can read back what they passed.
          max_batch_count: Entries one request may carry, or ``None`` for this class's
            :attr:`MAX_BATCH_COUNT`. Floored at one, since a zero or negative bound would make
            every event oversized and discard the batch.
          max_batch_bytes: Bytes one request's body may reach, or ``None`` for this class's
            :attr:`MAX_BATCH_BYTES`. Floored at one for the same reason.
          opener: A ``urlopen``-shaped callable, which a test can inject to assert on the
            request without any network access.

        Returns:
          None.

        Raises:
          None.
        """
        self.url = url
        self.method = method
        self._headers = dict(headers) if headers else {}
        self._auth = auth
        self.body_format = body_format
        self.timeout = timeout
        self.gzip = gzip
        self.max_retries = max(max_retries, 0)
        self.max_retry_after = max_retry_after
        self.max_batch_count = max(
            max_batch_count if max_batch_count is not None else self.MAX_BATCH_COUNT, 1
        )
        self.max_batch_bytes = max(
            max_batch_bytes if max_batch_bytes is not None else self.MAX_BATCH_BYTES, 1
        )
        self.log_foundry_stop_signal: threading.Event | None = None
        self._opener = opener if opener is not None else urllib.request.urlopen
        self.failed = 0
        self.dropped_oversized = 0
        self._learned_max_bytes = self.max_batch_bytes
        self._counter_lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunks the batch to this destination's limits and POSTs each chunk (FR-001).

        Subclasses must not override this. They shape the request through :meth:`_render`,
        :meth:`_body` and :meth:`_handle_response`, which is what keeps the chunk loop on every
        path to the wire.

        Args:
          batch: The events to ship. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When no chunk was **settled**, which is the total failure the
            worker's retry exists for (SPEC-026 FR-001). A chunk is settled when it was
            delivered *or* when it was permanently discarded, because neither is helped by a
            retry — a ``413`` is a verdict on the bytes, not a transient fault, and re-sending
            identical bytes can only earn the same answer (the rule SPEC-016 and SPEC-018 both
            already apply). A batch that settled *some* chunks does not raise either: the worker
            retries whole batches, so raising would re-send what landed.
        """
        if not batch:
            return
        items = self._items(batch)
        if not items:
            return
        chunks = settled = 0
        for chunk in chunk_items(
            items,
            max_count=self.max_batch_count,
            max_bytes=self._chunk_budget(),
            size_of=lambda item: item.size,
        ):
            chunks += 1
            if self._deliver(chunk):
                settled += 1
        if chunks and not settled:
            raise SinkDeliveryError(f"{type(self).__name__} delivered none of {chunks} chunk(s)")

    def _items(self, batch: list[dict[str, object]]) -> list[_Item]:
        """Renders each event and discards any that cannot fit a request on its own.

        ``chunk_items`` documents oversized items as the caller's to filter, because a group it
        cannot bound is a group it would yield anyway; dropping here is what the AWS sinks
        already do in ``_records``. The drops are announced as **one** line carrying the count
        rather than one line per event, because a batch of oversized events is a configuration
        fault and 5,980 identical stderr lines describe it no better than one does.

        Args:
          batch: The events to render.

        Returns:
          The items that can be sent, in order.

        Raises:
          None.
        """
        ceiling = self._event_ceiling()
        items: list[_Item] = []
        dropped = 0
        largest = 0
        for event in batch:
            rendered = self._render(event)
            size = self._item_size(rendered, event)
            if size > ceiling:
                dropped += 1
                largest = max(largest, size)
                continue
            items.append(_Item(event=event, rendered=rendered, size=size))
        if dropped:
            with self._counter_lock:
                self.dropped_oversized += dropped
            _diag.lost(
                "event",
                dropped,
                f"{type(self).__name__}, up to {largest} bytes exceeds the "
                f"{ceiling}-byte per-event ceiling",
            )
        return items

    def _event_ceiling(self) -> int:
        """The most one rendered event may measure and still be sendable.

        Args:
          None.

        Returns:
          The smaller of the request budget and any per-event limit the destination publishes.

        Raises:
          None.
        """
        if self.MAX_EVENT_BYTES is None:
            return self.max_batch_bytes
        return min(self.max_batch_bytes, self.MAX_EVENT_BYTES)

    def _item_size(self, rendered: str, event: dict[str, object]) -> int:
        """Charges one rendered event its share of the request body.

        The default is its own bytes plus one for the separator that joins it to its neighbours.
        A subclass whose body adds framing *per group* rather than per item overrides this to
        charge that framing too — ``LokiSink`` does, because its streams carry a label map each,
        and without it a body ran 61% past the budget at high label cardinality.

        Args:
          rendered: The event's serialized form.
          event: The source event, for a subclass that sizes framing from its fields.

        Returns:
          The bytes to charge the chunker.

        Raises:
          None.
        """
        return len(rendered.encode("utf-8")) + 1

    def _body_reserve(self) -> int:
        """Bytes of whole-body framing not charged to any single item.

        A JSON array adds two brackets while the last item's separator goes unused, so its body
        is one byte longer than the item sizes sum to; NDJSON is exact, and Splunk's bare
        concatenation is shorter. Reserving keeps the budget an actual bound rather than a value
        the body is permitted to sit one byte above.

        Args:
          None.

        Returns:
          The bytes to withhold from the chunker's budget.

        Raises:
          None.
        """
        return 1 if self.body_format == "json_array" else 0

    def _chunk_budget(self) -> int:
        """The byte budget to chunk against right now, floored at one.

        This is the configured budget less :meth:`_body_reserve`, and less anything a ``413``
        has since taught this sink about the destination's real limit. That ceiling only ever
        falls: a destination that refuses a size once will refuse it again, and rediscovering it
        by halving on every batch cost a measured 75% of all bytes uploaded.

        Args:
          None.

        Returns:
          The budget in bytes.

        Raises:
          None.
        """
        with self._counter_lock:
            learned = self._learned_max_bytes
        budget = min(self.max_batch_bytes, learned) - self._body_reserve()
        return max(budget, 1)

    def _render(self, event: dict[str, object]) -> str:
        """Serializes one event into this sink's per-event wire form.

        The hook a subclass overrides to wrap or enrich an event — Datadog's ``ddsource`` keys,
        Splunk's HEC envelope, Elasticsearch's action-plus-source pair. It must not include the
        separator that joins it to its neighbours; :meth:`_body` adds that.

        Args:
          event: The event to serialize.

        Returns:
          The serialized form.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``sanitize`` prevents.
        """
        return json.dumps(event)

    def _body(self, items: list[_Item]) -> tuple[bytes, str]:
        """Joins one chunk's rendered items into a request body.

        Args:
          items: The chunk's items, known non-empty.

        Returns:
          The body bytes and the default content type for the configured format.

        Raises:
          None.
        """
        if self.body_format == "json_array":
            return (
                ("[" + ",".join(item.rendered for item in items) + "]").encode("utf-8"),
                "application/json",
            )
        return (
            "".join(item.rendered + "\n" for item in items).encode("utf-8"),
            "application/x-ndjson",
        )

    def _handle_response(self, payload: bytes, items: list[_Item]) -> bool:
        """Reports whether a ``2xx`` response means the chunk was actually accepted.

        The hook for a destination whose ``2xx`` can still describe per-record failures —
        Elasticsearch's ``_bulk`` items. A status in the success range is the whole answer
        everywhere else, so the default ignores the body.

        Args:
          payload: The response body.
          items: The chunk the response describes.

        Returns:
          True, meaning delivered.

        Raises:
          None.
        """
        return True

    def _deliver(self, items: list[_Item], depth: int = 0) -> bool:
        """Sends one chunk, splitting it if the destination calls the payload too large.

        A ``413`` is permanent for these exact bytes, so re-sending them cannot help and the
        answer is a smaller request (AC-4). Splitting is safe to do here and only here: ``413``
        is a rejection before ingestion, so nothing landed and the halves cannot duplicate
        anything — the SPEC-018 rule that only provable non-delivery may be re-sent.

        The split is refused once ``MAX_SPLIT_DEPTH`` halvings have not found a size the
        destination accepts, and refused outright while a shutdown is in progress: the halving
        runs on the one drain thread, and ``shutdown()`` is already joining it.

        Args:
          items: The chunk to send, known non-empty.
          depth: How many halvings led here, which bounds the fan-out.

        Returns:
          True when the chunk was **settled** — delivered, or permanently discarded so that no
          retry could help. False only for a failure a retry might yet fix.

        Raises:
          None.
        """
        body, content_type = self._body(items)
        try:
            payload = self._send(body, content_type=content_type, splittable=True)
        except _PayloadTooLarge:
            self._learn_ceiling(len(body))
            return self._split(items, depth)
        except SinkDeliveryError:
            return False
        return self._handle_response(payload, items)

    def _split(self, items: list[_Item], depth: int) -> bool:
        """Halves a chunk the destination refused as too large and sends each half.

        Halving terminates three ways, and all three are settled rather than retried, because a
        ``413`` is a verdict on the bytes: a single item the destination still refuses is
        permanently too large, a chunk still refused after ``MAX_SPLIT_DEPTH`` halvings is
        against a limit far below the configured one, and a shutdown ends the search. Returning
        "settled" is what stops the worker re-running the whole cascade three more times — the
        same events were otherwise dropped and counted once per attempt, which made
        ``losses().dropped`` four times the number of events actually lost.

        Args:
          items: The refused chunk, known non-empty.
          depth: How many halvings led here.

        Returns:
          True — settled either way. Whichever branch runs, a retry cannot improve on it.

        Raises:
          None.
        """
        if len(items) == 1:
            with self._counter_lock:
                self.dropped_oversized += 1
            _diag.lost("event", 1, f"{type(self).__name__}, HTTP 413 for a single event")
            return True
        if depth >= MAX_SPLIT_DEPTH or self._stopping():
            reason = "a shutdown is in progress" if self._stopping() else "still 413 when split"
            with self._counter_lock:
                self.dropped_oversized += len(items)
            _diag.lost(
                "event",
                len(items),
                f"{type(self).__name__}, abandoned after {depth} split(s), {reason}",
            )
            return True
        middle = len(items) // 2
        first = self._deliver(items[:middle], depth + 1)
        second = self._deliver(items[middle:], depth + 1)
        return first or second

    def _learn_ceiling(self, refused_bytes: int) -> None:
        """Records that the destination refused a body of this size, so later chunks stay under.

        It only ever lowers the ceiling. A destination that answered ``413`` once will answer it
        again for the same size, and without this the halving cascade is repaid on every batch
        forever — measured at 75% of all bytes uploaded being body that is uploaded and then
        rejected. Half the refused size is the next value tried, which is where the split was
        going anyway.

        Args:
          refused_bytes: The size of the body the destination refused.

        Returns:
          None.

        Raises:
          None.
        """
        with self._counter_lock:
            self._learned_max_bytes = max(min(self._learned_max_bytes, refused_bytes // 2), 1)

    def _stopping(self) -> bool:
        """Reports whether a shutdown is in progress (SPEC-027 FR-002).

        Args:
          None.

        Returns:
          True when the worker's stop event has been set.

        Raises:
          None.
        """
        signal = self.log_foundry_stop_signal
        return signal is not None and signal.is_set()

    def losses(self) -> SinkLosses:
        """Reports oversized drops and abandoned requests (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The counters. ``failed`` counts abandoned requests rather than events, since this
          class has no per-event outcome to report and an abandoned request took its whole body
          with it; subclasses that do learn per-record outcomes add them.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=self.dropped_oversized, failed=self.failed)

    def close(self) -> None:
        """Does nothing, since ``urllib`` opens a fresh connection per request (FR-012).

        Idempotent.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def _send(
        self,
        body: bytes,
        *,
        content_type: str,
        extra_headers: dict[str, str] | None = None,
        splittable: bool = False,
    ) -> bytes:
        """POSTs a body with bounded retry and returns the response bytes.

        Raising from here rather than from each ``emit`` is deliberate: every platform subclass
        builds its own body and calls this, so the rule reaches them without a line of their
        own. The one caller that must not propagate is ``SentrySink``, which sends one envelope
        per event and therefore catches it.

        Args:
          body: The serialized batch.
          content_type: The default content type, overridable by caller headers.
          extra_headers: Per-request headers beneath the caller's own.
          splittable: Whether the caller can respond to a ``413`` by sending smaller requests.
            Only :meth:`_deliver` can, so it alone passes ``True`` and alone sees
            ``_PayloadTooLarge``. Every other caller — ``SentrySink``, which sends one envelope
            per event and has nothing left to split — keeps the counted, announced abandonment
            it has always had, because a ``413`` raised past it uncounted would be exactly the
            silent loss this file's counters exist to prevent.

        Returns:
          The response bytes, which subclasses that must inspect the response — such as
          Elasticsearch's ``_bulk`` items — read.

        Raises:
          _PayloadTooLarge: On a ``413`` when ``splittable``. It moves no counter and writes no
            line, because the caller is about to re-send the same events in smaller requests and
            only what finally fails is a loss.
          SinkDeliveryError: If the request was retried to exhaustion. It used to return
            ``None``, which every caller spelled as "nothing to parse" and the worker read as a
            successful emit; an abandoned request delivered nothing, which is the total failure
            the worker's retry exists for (SPEC-026 FR-001). Since FR-001 a request carries one
            *chunk* rather than the whole batch, so it is :meth:`emit` that decides whether the
            batch as a whole failed — one abandoned chunk beside a delivered one is partial
            failure, and partial failure must not raise.
        """
        headers, data = self._prepare(body, content_type, extra_headers)
        retries = 0 if self._stopping() else self.max_retries
        for attempt in range(retries + 1):
            request = urllib.request.Request(  # noqa: S310
                self.url, data=data, method=self.method, headers=headers
            )
            try:
                status, payload, retry_after = self._attempt(request)
            except (urllib.error.URLError, OSError) as err:
                if attempt < retries:
                    self._sleep_backoff(attempt, None)
                    continue
                self._abandon(
                    f"connection error, {type(err).__name__} {_diag.errno_of(err)}".rstrip(),
                    retries + 1,
                )
            if 200 <= status < 300:
                return payload
            if (status == 429 or 500 <= status < 600) and attempt < retries:
                self._sleep_backoff(attempt, retry_after)
                continue
            if status == 413 and splittable:
                raise _PayloadTooLarge(f"{type(self).__name__}, HTTP 413")
            self._abandon(f"HTTP {status}", retries + 1)
        raise SinkDeliveryError(f"{type(self).__name__} made no attempt")

    def _prepare(
        self,
        body: bytes,
        content_type: str,
        extra_headers: dict[str, str] | None,
    ) -> tuple[dict[str, str], bytes]:
        """Builds the final header map and the optionally gzipped body.

        Caller-provided headers override the sink's defaults (FR-002).

        Args:
          body: The serialized batch.
          content_type: The default content type.
          extra_headers: Per-request headers beneath the caller's own.

        Returns:
          The headers and the body to send.

        Raises:
          None.
        """
        headers: dict[str, str] = {"Content-Type": content_type}
        if extra_headers:
            headers.update(extra_headers)
        headers.update(self._headers)
        data = body
        if self.gzip:
            data = _gzip.compress(body)
            headers["Content-Encoding"] = "gzip"
        self._apply_auth(headers)
        return headers, data

    def _apply_auth(self, headers: dict[str, str]) -> None:
        """Applies bearer-token or basic-auth credentials, if not already set.

        The URL's scheme is deliberately not checked: an ``http://`` endpoint sends these
        credentials in cleartext, and basic auth is base64 rather than encryption, but a
        plaintext endpoint is a legitimate configuration — a sidecar collector on loopback, an
        in-cluster aggregator — and this library does not overrule the application's own
        deployment.

        Args:
          headers: The header map, updated in place.

        Returns:
          None.

        Raises:
          None.
        """
        if self._auth is None or "Authorization" in headers:
            return
        if isinstance(self._auth, str):
            headers["Authorization"] = f"Bearer {self._auth}"
        else:
            user, password = self._auth
            token = b64encode(f"{user}:{password}".encode()).decode("ascii")
            headers["Authorization"] = f"Basic {token}"

    def _attempt(self, request: urllib.request.Request) -> tuple[int, bytes, float | None]:
        """Performs one request.

        A ``4xx`` or ``5xx`` arrives from ``urlopen`` as an ``HTTPError``, which is itself a
        readable response object, so it is unified here rather than raised.

        Args:
          request: The prepared request.

        Returns:
          The status, the response payload, and any ``Retry-After`` delay.

        Raises:
          urllib.error.URLError: On a connection-level failure, for the retry loop to handle.
          OSError: Likewise.
        """
        try:
            response = self._opener(request, timeout=self.timeout)
        except urllib.error.HTTPError as err:
            response = err
        status = getattr(response, "status", None)
        if status is None:
            status = response.getcode()
        payload = response.read()
        return int(status), payload, _parse_retry_after(getattr(response, "headers", None))

    def _sleep_backoff(self, attempt: int, retry_after: float | None) -> None:
        """Waits before the next attempt, honouring a usable ``Retry-After``.

        The server's value is clamped and sign-checked first (SPEC-027 FR-001): it is advice
        from the destination, not an instruction the application must obey. Unbounded, a
        measured ``Retry-After: 8`` held ``shutdown()`` for 22 s and a header of ``86400`` would
        have held it for a day, while a negative one made ``time.sleep`` raise, which the orphan
        path handed to the caller.

        Args:
          attempt: The zero-based attempt just made, which sets the exponential delay.
          retry_after: The server's requested delay, or ``None``. Anything rejected falls back
            to this sink's own backoff, which is what the destination would have got had it sent
            no header at all.

        Returns:
          None.

        Raises:
          None.
        """
        server = clamp_server_delay(retry_after, self.max_retry_after)
        delay = server if server is not None else _BACKOFF_BASE * (2**attempt)
        wait(delay, self.log_foundry_stop_signal)

    def _abandon(self, reason: str, attempts: int) -> NoReturn:
        """Counts and logs a request abandoned past the retry bound, then raises (FR-012).

        The counter moves before the raise: ``failed`` is this sink's own record of what it
        could not put on the wire, and it must not depend on who catches what. It therefore
        counts every worker retry attempt too, and now every *chunk* of a batch, making it an
        upper bound on loss rather than a count of it — see
        :class:`~log_foundry.sinks.base.SinkLosses`.

        Args:
          reason: Why the request was abandoned. It carries only library-controlled values — an
            HTTP status, an exception type, an ``errno`` — never a server-supplied body or an
            exception's text (SPEC-029 FR-002), and it is also the raised error's message for
            the same reason.
          attempts: How many attempts were actually made, which is one when a shutdown suppressed
            the retries and is therefore passed in rather than derived from ``max_retries``.

        Returns:
          Never returns.

        Raises:
          SinkDeliveryError: Always.
        """
        with self._counter_lock:
            self.failed += 1
        detail = f"{type(self).__name__}, {attempts} attempt(s), {reason}"
        _diag.lost("request", 1, detail)
        raise SinkDeliveryError(detail)


def _parse_retry_after(headers: Any) -> float | None:
    """Parses a ``Retry-After`` delay in seconds.

    Args:
      headers: The response headers, or ``None``.

    Returns:
      The delay, or ``None`` if the header is absent or in the HTTP-date form this does not
      parse, in which case the caller falls back to exponential backoff.

    Raises:
      None.
    """
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
