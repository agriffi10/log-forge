"""OTLPSink — ship events as OTLP/HTTP JSON logs over the private mapping (SPEC-056)."""

from __future__ import annotations

import json
from typing import Unpack

from log_foundry import __version__, _diag
from log_foundry._otel import framing, log_record, request_body, resource_key
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.http import HTTPPlatformKwargs, HTTPSink, _Item, _valid_url

__all__ = ["OTLPSink"]

DEFAULT_ENDPOINT = "http://localhost:4318/v1/logs"
"""OTLP/HTTP's default port and logs path, used exactly as written like any other endpoint."""

_RETRYABLE = frozenset({429, 502, 503, 504})
"""OTLP's exact retryable set; a ``500`` is abandoned on the first request (FR-003)."""

_INT64_MAX = 2**63 - 1
_MAX_COUNT_DIGITS = 19
_NOUN = "log record"


class OTLPSink(HTTPSink):
    """POSTs each chunk as an OTLP/HTTP JSON ``ExportLogsServiceRequest`` (SPEC-056 FR-003).

    Each event becomes one ``LogRecord`` through ``_otel.log_record``, grouped into one
    ``resourceLogs`` per distinct ``(service, version, env)`` by ``_otel.request_body``, so an OTLP
    backend receives the OpenTelemetry names it already indexes — severity, trace and span ids,
    ``service.name``, ``exception.type`` — with the library's own fields alongside under the
    ``log_foundry.`` prefix. JSON over HTTP only; no OpenTelemetry SDK enters the runtime.

    **Retry set.** OTLP names ``429``, ``502``, ``503`` and ``504`` retryable, and nothing else, so
    :meth:`_retryable_status` narrows the family's ``429``/``5xx`` to exactly those: a ``500`` is
    abandoned on the first request, counted, announced and raised like any other non-retryable
    status. ``Retry-After`` is honoured as for every sink. One inherited behaviour is a deliberate
    exception to OTLP's rule: on a ``413`` the base class halves the byte budget and re-sends the
    same records in smaller requests (SPEC-038). Nothing landed — a ``413`` precedes ingestion —
    and OTLP's own rule for an oversized request is to answer ``413``, which is exactly the answer
    that search exists to act on. The worker's batch-level retry is outside this sink's reach: it
    re-sends a batch whenever ``emit`` raises, whatever the status (Out of Scope).

    **The response is read under a bound.** OTLP says a client must limit the response it parses
    (4 MiB recommended), so :meth:`_response_limit` returns :attr:`MAX_RESPONSE_BYTES`: a
    non-``2xx`` over it is abandoned as non-retryable in ``HTTPSink._send``, and a ``2xx`` over it
    is counted unadjudicated by :meth:`_handle_response` (FR-004). ``urllib`` asks for
    ``Accept-Encoding: identity``, so there is no decompression to bound.

    **The budget includes the envelope.** Each distinct resource adds a ``resource``, a
    ``scopeLogs`` wrapper and two schema URLs, and a ``configure()`` mid-run can put several in
    one chunk, so :meth:`_item_size` charges every record its own group's framing and
    :meth:`_body_reserve` withholds the ``{"resourceLogs":[…]}`` wrapper — ``LokiSink``'s method,
    over-counting when events share a resource, which is the safe direction. ``max_batch_bytes``
    is measured on the uncompressed body.

    **Drops are counted through ``emit``'s ledger, and this sink adds none of its own.** It
    inherits ``emit``, ``_items`` and ``_drop_refused``, so the oversize drop, the ``413`` drop
    and an unrenderable event are retry-safe without a line here (SPEC-058, SPEC-059); its
    mapping is total, so an unrenderable event is a `json.dumps` fault and nothing else. The two
    counters below are immediate, like ``failed``, and feed ``failed`` only.

    It takes **no** transport lock (SPEC-028 FR-002) and **adds no post-close guard**
    (SPEC-032 FR-003), for the reasons :class:`~log_foundry.sinks.http.HTTPSink` records: there
    is no transport held and ``close()`` releases nothing.

    Attributes:
      MAX_BATCH_COUNT: 1,000 — the inherited generic default; OTLP publishes no record count.
      MAX_BATCH_BYTES: 5,000,000 — the inherited generic default, well under the 64 MiB a server
        is recommended to accept.
      MAX_RESPONSE_BYTES: 4 MiB, what :meth:`_response_limit` returns; OTLP's recommended bound.
      rejected_records: Records the server reported rejected in a ``2xx`` partial success,
        clamped to each chunk's size so a confused server cannot inflate ``failed`` past what
        was sent. Never retried: a retry would re-send what landed.
      dropped_unadjudicated: Records in a ``2xx`` response this sink could not read — a
        ``partialSuccess`` that is not an object, a count that is not a usable integer, a body
        over the response bound. Counted as an upper bound, like ``failed``, and never retried,
        for SPEC-018's reason: the request succeeded, so they may have landed.


    It keeps **no** client buffer (SPEC-036 FR-002): each ``emit`` is a request that has
    completed by the time it returns, and no client object outlives it holding data.
    """

    MAX_RESPONSE_BYTES = 4 * 1024 * 1024

    def __init__(
        self, endpoint: str = DEFAULT_ENDPOINT, **http_kwargs: Unpack[HTTPPlatformKwargs]
    ) -> None:
        """Points the sink at an OTLP/HTTP logs endpoint.

        Args:
          endpoint: The URL to POST to, used exactly as given — OTLP's own word for it, and the
            per-signal endpoint the exporter specification describes; no path is appended.
          **http_kwargs: Forwarded to :class:`~log_foundry.sinks.http.HTTPSink`, typed as
            ``HTTPPlatformKwargs`` (SPEC-051 FR-005) — every keyword it takes except
            ``body_format``, which this sink pins. ``method`` is accepted only as exactly
            ``"POST"`` and is forwarded through the dict rather than set here, so the typed
            shape ``tests/test_http_kwargs_roster.py`` derives still offers it.

        Returns:
          None.

        Raises:
          TypeError: If ``endpoint`` is not a ``str``.
          ValueError: If ``endpoint`` has no ``http``/``https`` scheme or names no host, or if
            ``method`` is anything but ``"POST"`` — OTLP/HTTP is POST only, and accepting another
            would send every batch to a server that refuses it for the life of the process
            (invariant 13). Every other refusal is inherited.
        """
        if not isinstance(endpoint, str):
            raise TypeError(
                f"OTLPSink endpoint must be a str, not {type(endpoint).__name__}"
            )
        _valid_url(endpoint, "OTLPSink", name="endpoint")
        method = http_kwargs.get("method", "POST")
        if method != "POST":
            raise ValueError(
                f"OTLPSink method must be exactly 'POST', not {method!r}: OTLP/HTTP is POST only"
            )
        super().__init__(endpoint, body_format="json_array", **http_kwargs)
        self._scope_version = __version__
        self.rejected_records = 0
        self.dropped_unadjudicated = 0

    def _retryable_status(self, status: int) -> bool:
        """OTLP's exact retryable set: ``429``, ``502``, ``503`` and ``504``.

        Args:
          status: The HTTP status of the response.

        Returns:
          True for exactly those four.

        Raises:
          None.
        """
        return status in _RETRYABLE

    def _response_limit(self) -> int | None:
        """OTLP's recommended bound on the response a client parses.

        Args:
          None.

        Returns:
          :attr:`MAX_RESPONSE_BYTES`.

        Raises:
          None.
        """
        return self.MAX_RESPONSE_BYTES

    def _render(self, event: dict[str, object]) -> str:
        """Serializes one event as one OTLP/JSON ``LogRecord``, compactly (FR-001).

        Compact separators are what ``_otel.framing``'s arithmetic and :meth:`_item_size`
        count, and ``allow_nan=False`` is a free guard: the mapping spells a non-finite float
        as a string, so a bare ``NaN`` here could only be a defect.

        Args:
          event: The event to map.

        Returns:
          The record's JSON text.

        Raises:
          None.
        """
        return json.dumps(
            log_record(event), separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )

    def _item_size(self, rendered: str, event: dict[str, object]) -> int:
        """Charges a record its bytes, its separator and its resource group's framing (FR-003).

        Args:
          rendered: The record's JSON text.
          event: The source event, whose stamps decide the group.

        Returns:
          The bytes to charge the chunker.

        Raises:
          None.
        """
        return (
            len(rendered.encode("utf-8"))
            + 1
            + framing(resource_key(event), self._scope_version)
        )

    def _body_reserve(self) -> int:
        """Withholds the ``{"resourceLogs":[]}`` wrapper no record is charged for.

        Args:
          None.

        Returns:
          The wrapper's length in bytes.

        Raises:
          None.
        """
        return len(request_body([], self._scope_version).encode("utf-8"))

    def _body(self, items: list[_Item]) -> tuple[bytes, str]:
        """Builds the ``ExportLogsServiceRequest`` for one chunk (FR-002).

        Args:
          items: The chunk's items, known non-empty.

        Returns:
          The body bytes and ``application/json``.

        Raises:
          None.
        """
        pairs = [(resource_key(item.event), item.rendered) for item in items]
        return request_body(pairs, self._scope_version).encode("utf-8"), "application/json"

    def _handle_response(self, payload: bytes, items: list[_Item]) -> bool:
        """Sorts a ``2xx`` body into full success, rejected records or unadjudicated (FR-004).

        Never raises, on any bytes: an exception escaping here escapes ``emit``, and the worker
        then re-sends chunks that already landed. A body that is empty, not JSON, not an object
        or without a ``partialSuccess`` is full success — what a server must send on success, so
        a proxy's page or a protobuf body is not this sink's to adjudicate, unlike a ``_bulk``
        response, whose only verdict is in the body. The body is parsed with ``parse_int=str``
        so a bare over-long ``rejectedLogRecords`` takes the digit rule rather than reading as
        not-JSON and settling as success; ``except Exception`` because a ``RecursionError`` from a
        deeply nested body is not a ``ValueError``.

        Args:
          payload: The response body, read under :meth:`_response_limit`.
          items: The chunk the response describes.

        Returns:
          False only when every record of the chunk was rejected, so an ``emit`` whose every
          chunk came back that way raises (invariant 7); True otherwise, the unadjudicated case
          included, since the request succeeded and a re-send could duplicate what landed.

        Raises:
          None.
        """
        sent = len(items)
        limit = self._response_limit()
        if limit is not None and len(payload) > limit:
            self._abandon_unadjudicated(sent)
            return True
        try:
            data = json.loads(payload, parse_int=str)
        except Exception:
            return True
        if not isinstance(data, dict):
            return True
        partial = data.get("partialSuccess")
        if partial is None:
            return True
        if not isinstance(partial, dict):
            self._abandon_unadjudicated(sent)
            return True
        count = partial.get("rejectedLogRecords")
        if count is None:
            return True
        rejected = _rejected_count(count)
        if rejected is None:
            self._abandon_unadjudicated(sent)
            return True
        if rejected == 0:
            return True
        rejected = min(rejected, sent)
        with self._counter_lock:
            self.rejected_records += rejected
        _diag.lost(_NOUN, rejected, f"{type(self).__name__}, rejected by the server")
        return rejected < sent

    def _abandon_unadjudicated(self, sent: int) -> None:
        """Counts and announces a ``2xx`` whose partial-success verdict could not be read.

        The whole chunk is charged because the response cannot say which records landed, which
        makes the count an upper bound, like ``failed``. The line carries the class and the
        count and never the body, which is the server's text (invariant 11).

        Args:
          sent: How many records were in the request.

        Returns:
          None.

        Raises:
          None.
        """
        with self._counter_lock:
            self.dropped_unadjudicated += sent
        _diag.lost(
            _NOUN,
            sent,
            f"{type(self).__name__} could not read the partial-success response; "
            f"they may have landed; not retried",
        )

    def losses(self) -> SinkLosses:
        """Reports abandoned requests plus rejected and unadjudicated records (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          ``dropped`` is the oversized events plus those whose render raised, as in the base
          class; ``failed`` sums the abandoned requests with the two counters above, each an
          event the server did not confirm, an upper bound on loss.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(
                dropped=self.dropped_oversized + self._unrenderable,
                failed=self.failed + self.rejected_records + self.dropped_unadjudicated,
            )


def _rejected_count(count: object) -> int | None:
    """Reads ``rejectedLogRecords`` as OTLP/JSON writes it: an int64 as digits, or a number.

    With ``parse_int=str`` every bare integer arrives as its digit string, so the one rule
    serves both spellings: ASCII decimal digits, at most nineteen of them, within int64. A
    ``bool``, a float, a list, a negative, a non-ASCII digit, an over-long or out-of-range
    value is unreadable.

    Args:
      count: The value under ``partialSuccess.rejectedLogRecords``.

    Returns:
      The count, or ``None`` when it cannot be read.

    Raises:
      None.
    """
    if isinstance(count, bool) or not isinstance(count, str):
        return None
    if not count.isascii() or not count.isdigit() or len(count) > _MAX_COUNT_DIGITS:
        return None
    value = int(count)
    return value if value <= _INT64_MAX else None
