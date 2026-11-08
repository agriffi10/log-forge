"""OTLPSink — ship events as OTLP/HTTP JSON logs over the private mapping (SPEC-056)."""

from __future__ import annotations

import json
from typing import Unpack

from log_foundry import __version__, _diag
from log_foundry._otel import framing, log_record, request_body, resource_key
from log_foundry.sinks._engine import When, run_context
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.http import (
    HTTPPlatformKwargs,
    HTTPSink,
    _Item,
    _read_as_1x,
    _read_failed,
    _valid_url,
)
from log_foundry.sinks.verdict import Outcome

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
    is no transport held and ``close()`` releases nothing. It keeps **no** client buffer
    (SPEC-036 FR-002): each ``emit`` is a request that has completed by the time it returns, and
    no client object outlives it holding data.

    Attributes:
      MAX_BATCH_COUNT: 1,000 — the inherited generic default; OTLP publishes no record count.
      MAX_BATCH_BYTES: 5,000,000 — the inherited generic default, well under the 64 MiB a server
        is recommended to accept.
      MAX_RESPONSE_BYTES: 4 MiB, what :meth:`_response_limit` returns; OTLP's recommended bound.
      rejected_records: Records the server reported rejected in a ``2xx`` partial success,
        clamped to each chunk's size so a confused server cannot inflate ``failed`` past what
        was sent. Never resent: a retry would re-send what landed. The response gives a count,
        not an identity, so the chunk's first *r* events are ``REJECTED`` and the rest
        ``ACCEPTED``; those positions carry no claim about which records the server refused.
      dropped_unadjudicated: Records in a ``2xx`` response this sink could not read — a
        ``partialSuccess`` that is not an object, a count that is not a usable integer, a body
        over the response bound. Counted as an upper bound, like ``failed``, and never retried,
        for SPEC-018's reason: the request succeeded, so they may have landed.
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
          TypeError: If ``endpoint`` is not a ``str``, or a forwarded ``max_retries`` or
            ``max_batch_count`` is not an ``int`` or ``max_retry_after`` not an ``int`` or
            ``float``, refused by :class:`~log_foundry.sinks.http.HTTPSink` naming this class
            (SPEC-066).
          ValueError: If ``endpoint`` carries a user or password before its host or an ``@``
            anywhere else, has no ``http``/``https`` scheme or names no host, or if ``method`` is
            anything but ``"POST"`` — OTLP/HTTP is POST only, and accepting another would send
            every batch to a server that refuses it for the life of the process (invariant 13).
            Every other refusal is inherited, a forwarded ``auth`` beside an ``Authorization``
            header in a forwarded ``headers`` among them.
        """
        owner = type(self).__name__
        if not issubclass(type(endpoint), str):
            raise TypeError(
                f"{owner} endpoint must be a str, "
                f"not {_diag._STR_STR(_diag._STORED_NAME(type(endpoint)))}"
            )
        _valid_url(endpoint, owner, name="endpoint")
        method = http_kwargs.get("method", "POST")
        if method != "POST":
            raise ValueError(
                f"{owner} method must be exactly 'POST', not {method!r}: OTLP/HTTP is POST only"
            )
        super().__init__(endpoint, body_format="json_array", **http_kwargs)
        self._scope_version = __version__
        self.rejected_records = 0

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
        return len(rendered.encode("utf-8")) + 1 + framing(resource_key(event), self._scope_version)

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
        """Reads a ``2xx`` as a 1.x caller did: ``False`` when every record of the chunk bounced.

        Kept for a direct 1.x caller (SPEC-073 FR-003): it runs :meth:`_outcomes` in a context of
        its own, committed at once, and returns ``False`` exactly when the chunk is non-empty and
        every outcome is ``REJECTED``. It never raises, on any bytes.

        Args:
          payload: The response body, read under :meth:`_response_limit`; an ``_UnreadBody``
            when it could not be read.
          items: The chunk the response describes.

        Returns:
          False only when every record of a non-empty chunk was rejected; True otherwise.

        Raises:
          None.
        """
        return _read_as_1x(lambda: self._outcomes(payload, items))

    def _outcomes(self, payload: bytes, items: list[_Item]) -> tuple[Outcome, ...]:
        """Sorts a ``2xx`` body into full success, rejected records or unadjudicated (FR-004).

        Never raises, on any bytes. A body that is empty, not JSON, not an object or without a
        ``partialSuccess`` is full success — what a server must send on success, so a proxy's
        page or a protobuf body is not this sink's to adjudicate, unlike a ``_bulk`` response,
        whose only verdict is in the body.

        **A body that could not be read is not one of those** (SPEC-070 FR-004). A ``2xx`` whose
        body was cut short, stalled or reset after the status line arrives as an
        ``_UnreadBody``, equal to ``b""``; it is asked about first, before the response bound,
        and given up on through ``HTTPSink._unadjudicated``, its records ``UNKNOWN``, never
        resent. So is a body over the response bound, a ``partialSuccess`` that is not an object
        and a count that is not a usable integer. An honestly empty body is still success. The
        body is parsed with ``parse_int=str`` so a bare over-long ``rejectedLogRecords`` takes the
        digit rule rather than reading as not-JSON and settling as success; ``except Exception``
        because a ``RecursionError`` from a deeply nested body is not a ``ValueError``.

        A ``partialSuccess`` rejecting *r* of *n* records, *r* clamped to the chunk, is counted on
        ``rejected_records``. **The response gives a count, not an identity**, so the chunk's first
        *r* events are ``REJECTED`` and the rest ``ACCEPTED``: the positions carry no claim about
        which record the server refused, only how many.

        Args:
          payload: The response body, read under :meth:`_response_limit`; an ``_UnreadBody``
            when it could not be read.
          items: The chunk the response describes.

        Returns:
          One outcome per record.

        Raises:
          None.
        """
        sent = len(items)
        unreadable = "could not read the partial-success response"
        if _read_failed(payload):
            return self._unadjudicated(items, unreadable, _NOUN)
        limit = self._response_limit()
        if limit is not None and len(payload) > limit:
            return self._unadjudicated(items, unreadable, _NOUN)
        accepted = (Outcome.ACCEPTED,) * sent
        try:
            data = json.loads(payload, parse_int=str)
        except Exception:
            return accepted
        if not isinstance(data, dict):
            return accepted
        partial = data.get("partialSuccess")
        if partial is None:
            return accepted
        if not isinstance(partial, dict):
            return self._unadjudicated(items, unreadable, _NOUN)
        count = partial.get("rejectedLogRecords")
        if count is None:
            return accepted
        rejected = _rejected_count(count)
        if rejected is None:
            return self._unadjudicated(items, unreadable, _NOUN)
        if rejected == 0:
            return accepted
        rejected = min(rejected, sent)
        context = run_context()
        name = type(self).__name__
        context.count(self, "rejected_records", rejected, when=When.ALWAYS)
        context.announce(
            lambda: _diag.lost(_NOUN, rejected, f"{name}, rejected by the server"),
            when=When.ALWAYS,
        )
        return (Outcome.REJECTED,) * rejected + (Outcome.ACCEPTED,) * (sent - rejected)

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
