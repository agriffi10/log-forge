"""SNSSink — publish events to an SNS topic (arch §8, SPEC-010)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Final

from log_foundry import _diag
from log_foundry.sanitize import _int_text
from log_foundry.sinks import _engine
from log_foundry.sinks._chunk import chunk_items
from log_foundry.sinks._classify import aws_retryable, is_retryable, sender_fault
from log_foundry.sinks._encode import encode_round
from log_foundry.sinks._engine import emit_via_deliver, run_context
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._retry import exact_int, floor_count
from log_foundry.sinks._rounds import (
    Called,
    KeyedReply,
    Lost,
    entry_ids,
    mark_sent,
    read_keyed,
    settle_keyed,
)
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    import threading
    from collections.abc import Mapping

    from log_foundry.sinks._deferral import Deferral
    from log_foundry.sinks._engine import _Drops
    from log_foundry.sinks.base import SinkLosses

__all__ = ["SNSSink"]

_SERVICE_MAX_BYTES: Final = 1_048_576
"""The most bytes any topic takes in one message and in one request's messages (SPEC-096).

``PublishBatch``: a topic's ``MaximumMessageSize`` "supports values up to 1 MiB (1,048,576
bytes)", and "The combined size of all messages in the batch, including each message's body and
attributes, must not exceed the topic's MaximumMessageSize". Every limit the sink applies is
capped here, so a ``MAX_BYTES`` raised past it costs the messages over 1 MiB one at a time
rather than a request every topic refuses whole.
"""

_LIMIT_FLOOR: Final = 1_024
"""The smallest ``max_message_bytes`` the sink accepts, a topic's least ``MaximumMessageSize``.

``SetTopicAttributes``: "Valid values are 1024 to 1048576 (1 MiB). The default is 262144 (256
KiB)" (SPEC-096 FR-001).
"""


class SNSSink:
    """A :class:`~log_foundry.sinks.base.Sink` that publishes events to an SNS topic.

    This mirrors the SPEC-005 ``SQSSink`` partial-failure policy on SNS's ``publish_batch``:
    the response's ``Failed`` list is resent within a bounded count, except a sender fault, which
    is counted and never resent (SPEC-070 FR-003), and entries still failing are counted and
    logged. ``boto3`` is the optional ``aws`` extra, imported lazily.

    **It delivers through SPEC-072's engine** (SPEC-074 FR-002): :meth:`deliver` is one round,
    every chunk published once and its reply read through the keyed reader SQS shares, and the
    engine resends only the entries that may land. The worst-case delay (SPEC-027 FR-005) is
    ``max_retries`` waits per run, 0.7 s at the defaults, each interruptible by ``shutdown()``.

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport
    lock. ``boto3`` clients are documented thread-safe, this one is built once in
    ``__init__``, and the sink rebinds nothing after construction.

    It also **adds no post-close guard** (SPEC-032 FR-003): ``close()`` is a documented no-op,
    because the client is the caller's to release or the SDK's to reap, so a batch emitted
    afterwards still reaches the topic.


    It keeps **no** client buffer (SPEC-036 FR-002): the driver call returns only once the
    destination has the batch, so nothing is queued locally between emits.

    **Its message limit is the caller's to set** (SPEC-096). Each round reads one limit, used for
    every decision the round makes: ``max_message_bytes`` when it is not ``None``, otherwise
    :attr:`MAX_BYTES`, and then the smaller of that and the 1,048,576 bytes any topic allows. A
    message whose UTF-8 body costs more is dropped, counted in ``dropped_oversized`` and announced
    naming the limit, and a request's bodies together cost at most the same limit, in UTF-8
    bytes, as the topic itself counts them.

    Attributes:
      MAX_BYTES: The message limit when ``max_message_bytes`` is ``None``, 262,144 bytes, 1.x's
        256 KiB and every topic's default. It is read through ``self`` once per round, so an
        assignment on the instance, a subclass's override and the class's own value all set the
        limit as they did in 1.x; a ``max_message_bytes`` the caller passed wins over it,
        whatever it holds.
      max_message_bytes: The message limit the caller passed, an exact ``int`` from 1,024 to
        1,048,576, or ``None`` for :attr:`MAX_BYTES`. It should be the topic's own
        ``MaximumMessageSize``: a larger value sends requests the topic refuses whole.
      log_foundry_counters: The engine's event totals, which :meth:`losses` reports.
      dropped_oversized: Events dropped for exceeding the message limit, counted only by a run
        that settled.
    """

    MAX_BATCH = 10
    MAX_BYTES = 256 * 1024

    def __init__(
        self,
        topic_arn: str,
        *,
        client: Any = None,
        max_retries: int = 3,
        max_message_bytes: int | None = None,
    ) -> None:
        """Binds the sink to a topic.

        Args:
          topic_arn: The topic to publish to.
          client: A boto3-shaped SNS client, or ``None`` to build one lazily.
          max_retries: Further rounds the engine gives the entries still failing, floored at
            zero as ``Worker._emit`` floors its own (SPEC-021). Anything but an ``int`` is refused
            with ``TypeError`` (SPEC-066 FR-001), before any client is built; a ``bool`` or
            ``int`` subclass is read as the ``int`` it holds.
          max_message_bytes: The most bytes one message may cost, and the most a request's
            messages may cost together, or ``None`` for :attr:`MAX_BYTES` (SPEC-096 FR-001). Pass
            the topic's ``MaximumMessageSize``, in bytes, and nothing larger: a topic is 262,144
            by default, and one that fans out to HTTP, email or SMS subscribers cannot be raised
            past it. It is judged after ``max_retries`` and before any client is built, read
            through ``exact_int``, so a ``bool`` or ``int`` subclass is judged and stored as the
            exact ``int`` it holds, and refused outside 1,024 to 1,048,576, the range a topic's
            own limit takes, rather than floored, since a new argument has no working
            configuration to keep.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066), or ``max_message_bytes`` is
            neither ``None`` nor an ``int`` (SPEC-096).
          ValueError: If ``max_message_bytes`` holds a number below 1,024 or above 1,048,576; the
            message names the exact number, an integer too long to print as
            ``<int: ~N digits>``.
          ImportError: If ``boto3`` is needed and the ``aws`` extra is not installed.
        """
        self.max_retries = floor_count(max_retries, 0, "max_retries", "SNSSink")
        if max_message_bytes is not None:
            max_message_bytes = exact_int(max_message_bytes, "max_message_bytes", "SNSSink")
            if max_message_bytes < _LIMIT_FLOOR or max_message_bytes > _SERVICE_MAX_BYTES:
                raise ValueError(
                    f"SNSSink max_message_bytes must be from {_LIMIT_FLOOR} to "
                    f"{_SERVICE_MAX_BYTES} bytes, got {_int_text(max_message_bytes)}"
                )
        if client is None:
            client = import_extra("boto3", "aws").client("sns")
        self.topic_arn = topic_arn
        self.client = client
        self.max_message_bytes = max_message_bytes
        self.log_foundry_stop_signal: threading.Event | None = None
        self.log_foundry_counters = SinkCounters()
        self.dropped_oversized = 0

    @property
    def _counter_lock(self) -> threading.Lock:
        """The counters' lock, which every commit and ``losses()`` read takes.

        Assigning it replaces the counters' lock, so a test installing a counting lock here
        counts every acquisition the engine makes (SPEC-074 FR-001).

        Args:
          None.

        Returns:
          The lock.

        Raises:
          None.
        """
        return self.log_foundry_counters._counter_lock

    @_counter_lock.setter
    def _counter_lock(self, lock: threading.Lock) -> None:
        """Replaces the counters' lock.

        Args:
          lock: The new lock.

        Returns:
          None.

        Raises:
          None.
        """
        self.log_foundry_counters._counter_lock = lock

    @property
    def failed(self) -> int:
        """Entries not confirmed: sender faults, entries still failing and chunks lost to a raise.

        The engine's ``failed_events``, each lost entry once per run (SPEC-074 FR-001).

        Args:
          None.

        Returns:
          The count.

        Raises:
          None.
        """
        return self.log_foundry_counters.failed_events

    @failed.setter
    def failed(self, value: int) -> None:
        """Sets the count, under the counter lock, as assigning the 1.x attribute did.

        Args:
          value: The new count.

        Returns:
          None.

        Raises:
          None.
        """
        with self._counter_lock:
            self.log_foundry_counters.failed_events = value

    def losses(self) -> SinkLosses:
        """Reports the engine's totals: drops of a settled run, and entries never confirmed.

        ``dropped`` folds in the events the encode step could not encode (SPEC-069 FR-001);
        ``dropped_oversized`` still means oversize only.

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        return self.log_foundry_counters.losses()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers the batch through the engine inline, keeping the 1.x ``emit`` contract.

        Args:
          batch: The events to publish.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the run failed: nothing landed and an entry was still failing
            retryably or refused for a reason other than a sender fault (SPEC-026 FR-001). A
            batch SNS rejected only as the caller's fault returns with the entries counted, as
            ``SQSSink``'s does, because a resend can only fail the same way (SPEC-070 FR-003),
            and so does a batch of nothing but oversized or unencodable events.
        """
        emit_via_deliver(self, batch)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-074 FR-002).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``BaseException`` from the client propagates.
        """
        return _engine.one_round(self._round, batch)

    def _limit(self) -> int:
        """Returns the effective message limit: the caller's, else ``MAX_BYTES``, at most 1 MiB.

        ``max_message_bytes`` when it is not ``None``, otherwise :attr:`MAX_BYTES` read through
        ``self``, so an instance assignment and a subclass override both reach it as in 1.x, and
        then the smaller of that and :data:`_SERVICE_MAX_BYTES` (SPEC-096 FR-003). The test is
        ``is None``, never truthiness. A judged ``max_message_bytes`` is never above the cap, so
        the cap binds only a ``MAX_BYTES`` raised past 1 MiB, which would otherwise publish
        requests every topic refuses whole. :meth:`_round` calls this once per round.

        Args:
          None.

        Returns:
          The limit in bytes.

        Raises:
          None.
        """
        limit = self.MAX_BYTES if self.max_message_bytes is None else self.max_message_bytes
        return min(limit, _SERVICE_MAX_BYTES)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Publishes every remaining chunk once and classifies every event of the round.

        The round reads one effective message limit (:meth:`_limit`, SPEC-096 FR-003), by which
        :meth:`_bodies` drops an oversized message and which bounds each chunk's summed UTF-8
        bytes, so every decision and line of a round names one number.

        Every event is encoded once per run (:func:`~log_foundry.sinks._encode.encode_round`),
        an unencodable one ``DROPPED`` with SPEC-069's line; :meth:`_bodies` drops an oversized
        one; the rest are chunked with ``chunk_items`` and each chunk published once through
        :meth:`_send`, its reply settled by the keyed rule SQS shares — a sender fault
        ``REJECTED`` and a final rejection, any other failed entry ``RETRYABLE``, an unreadable
        reply ``UNKNOWN``, a raise ``RETRYABLE`` or ``REJECTED`` as ``_retryable_error`` answers.

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``BaseException`` from the client propagates.
        """
        context = run_context()
        limit = self._limit()
        fates = [Outcome.DROPPED] * len(batch)
        pairs = encode_round(self, batch, self._encode, sink=_diag.type_name(self))
        bodies = self._bodies(pairs, context.drops(self), limit)
        lost = Lost(self)
        final: list[int] = []
        for chunk in chunk_items(
            bodies,
            max_count=self.MAX_BATCH,
            max_bytes=limit,
            size_of=lambda p: len(p[1].encode("utf-8")),
        ):
            positions = [position for position, _ in chunk]
            carried = [batch[position] for position in positions]
            mark_sent(self, carried)
            sent = self._send([body for _, body in chunk], entry_ids(self, carried))
            settle_keyed(
                self,
                positions=positions,
                carried=carried,
                error=sent.error,
                retryable=sent.retryable,
                reply=sent.result,
                call="publish_batch",
                name="SNSSink",
                fates=fates,
                final=final,
                lost=lost,
            )
        lost.register()
        context.final_rejections(final)
        return Verdict(outcomes=tuple(fates))

    def close(self) -> None:
        """Does nothing, since the sink buffers nothing internally (FR-001).

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def _encode(self, event: dict[str, object]) -> str:
        """Encodes one event as its message body.

        Args:
          event: The event to encode.

        Returns:
          The JSON text.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_round`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return json.dumps(event)

    def _bodies(
        self, encoded: list[tuple[int, str]], pending: Deferral | _Drops, limit: int
    ) -> list[tuple[int, str]]:
        """Keeps each encoded body, dropping one larger than the round's message limit (FR-011).

        It takes what :meth:`_encode` built (SPEC-069 FR-001), so the size check runs over
        encoded forms and nothing here can fail on an event. A body's UTF-8 length is its cost,
        as ``chunk_items`` reads it in :meth:`_round` (SPEC-096 FR-003), the unit the topic
        counts; characters agreed with it only while ``json.dumps`` escaped every non-ASCII
        character. A body costing more than ``limit``, the round's effective message limit, is
        dropped, and its line names that limit, bound here rather than read when the line is
        written.

        A drop goes to ``pending`` rather than to the counter, so it is counted and announced
        only when ``emit`` returns (SPEC-059 FR-001) — on the engine, ``pending`` is the run
        context's ``SETTLED`` view, so only when the run settles (SPEC-074). The line names no
        per-event value, so each drop registers its own count and its own line, one per event
        as before.

        Args:
          encoded: Each encodable event's position in the round's batch with its JSON body.
          pending: The run's ``SETTLED`` view, which holds every drop decided here until the run
            is known to settle.
          limit: The round's effective message limit, in bytes (:meth:`_limit`).

        Returns:
          The position and body of each message that can be sent.

        Raises:
          None.
        """
        bodies: list[tuple[int, str]] = []
        for position, body in encoded:
            if len(body.encode("utf-8")) > limit:
                pending.count("dropped_oversized")
                pending.announce(
                    lambda: _diag.lost(
                        "event", 1, f"SNSSink, exceeds the {limit}-byte message limit"
                    )
                )
                continue
            bodies.append((position, body))
        return bodies

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether a failed call is worth another attempt (SPEC-070 FR-002).

        botocore's transient and throttling failures, through the predicate the four AWS sinks
        share (:func:`~log_foundry.sinks._classify.aws_retryable`); anything else — a
        ``ParamValidationError``, an ``AccessDenied``, a ``TypeError`` from the caller's own
        code — fails at once, counted and announced on its first attempt with no backoff wait.

        Args:
          err: The exception the attempt raised.

        Returns:
          True when another attempt may succeed.

        Raises:
          None. :func:`~log_foundry.sinks._classify.is_retryable` reads a raise here as False.
        """
        return aws_retryable(err)

    def _retryable_entry(self, entry: Mapping[str, object]) -> bool:
        """Reports whether a resend of one rejected entry may succeed (SPEC-070 FR-003).

        A sender fault is the caller's: a byte-identical resend fails again, so it is counted and
        never retried. Read through :func:`~log_foundry.sinks._classify.sender_fault`, which SQS
        and SNS share, so the two services answer the same required field the same way.

        Args:
          entry: One item of the reply's ``Failed`` list.

        Returns:
          False for a sender fault, True otherwise.

        Raises:
          AttributeError: When the entry is not a mapping; the keyed reader refuses such a reply
            before this is asked (SPEC-074 FR-002).
        """
        return not sender_fault(entry)

    def _send(self, bodies: list[str], ids: list[str]) -> Called[KeyedReply]:
        """Publishes one chunk once and reads the reply per entry (SPEC-074 FR-002).

        Nothing here waits or retries: the engine resends a ``RETRYABLE`` entry in a later round.
        The reply's ``Failed`` list is read through
        :func:`~log_foundry.sinks._batch.adjudicate_keyed` against each entry's ``Id``, so a
        malformed list adjudicates none of the chunk rather than escaping ``emit`` raw.

        **A client exception costs this chunk, never the batch** (SPEC-048 FR-002), and it is
        treated as **provable non-delivery** for the chunk: a read timeout means the request went
        out and the reply was lost, so a resend may duplicate — taken because an unreachable
        endpoint is the common case. **An entry SNS marks ``SenderFault`` is not resent**
        (SPEC-070 FR-003): SNS's ``BatchResultErrorEntry`` carries the same required field as
        SQS's, and a byte-identical resend of the caller's fault fails again.

        Args:
          bodies: One chunk's serialized events.
          ids: Each entry's ``Id``, kept across the run's rounds.

        Returns:
          The call's reply.

        Raises:
          None. ``Exception`` is caught rather than ``BaseException``, so a ``KeyboardInterrupt``
            or ``SystemExit`` still reaches the caller (SPEC-025 FR-004).
        """
        entries = [
            {"Id": entry_id, "Message": body} for entry_id, body in zip(ids, bodies, strict=True)
        ]
        try:
            response = self.client.publish_batch(
                TopicArn=self.topic_arn, PublishBatchRequestEntries=entries
            )
        except Exception as err:
            return Called(None, error=err, retryable=is_retryable(self._retryable_error, err))
        return Called(read_keyed(self._retryable_entry, ids, response))
