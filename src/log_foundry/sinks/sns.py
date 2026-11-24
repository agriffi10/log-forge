"""SNSSink — publish events to an SNS topic (arch §8, SPEC-010)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Final, cast

from log_foundry import _diag
from log_foundry.sanitize import _int_text
from log_foundry.sinks import _engine, _fifo
from log_foundry.sinks._classify import aws_retryable, is_retryable, sender_fault
from log_foundry.sinks._encode import encode_round
from log_foundry.sinks._engine import When, emit_via_deliver, run_context
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._fifo import DedupIdSource, GroupIdSource, judge, plan_round
from log_foundry.sinks._retry import exact_int, floor_count
from log_foundry.sinks._rounds import (
    Called,
    KeyedReply,
    read_keyed,
)
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    import threading
    from collections.abc import Mapping

    from log_foundry.sinks.base import SinkLosses

__all__ = ["DedupIdSource", "GroupIdSource", "SNSSink"]

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

    **Its message limit is the caller's to set** (SPEC-096). Each run of ``emit`` reads one limit,
    used for every decision of every round of that run: ``max_message_bytes`` when it is not
    ``None``, otherwise :attr:`MAX_BYTES`, and then the smaller of that and the 1,048,576 bytes
    any topic allows. A message whose cost — its UTF-8 body and, on a FIFO topic, both ids — is
    more is dropped, counted in ``dropped_oversized`` and announced naming the limit, and a
    request's messages together cost at most the same limit, in UTF-8 bytes.

    **Both topic types are supported** (SPEC-100), through ``sinks/_fifo.py``, the one mechanism
    ``SQSSink`` uses for a FIFO queue, so the two FIFO sinks cannot diverge (invariant 6). A
    ``.fifo`` topic ARN selects FIFO behaviour, which ``fifo=`` overrides. On a FIFO topic every
    entry carries a ``MessageGroupId``, the event's ``trace_id`` by default, so a trace keeps its
    order while traces travel in parallel, and a ``MessageDeduplicationId``, the event's
    ``log_id`` by default, SPEC-095's idempotency key. Both are resolved once per run and every
    resend carries them unchanged, so the topic can absorb a copy a lost reply caused for as long
    as its de-duplication interval lasts; a de-duplication id too long to send goes out as its
    SHA-256 digest (SPEC-100 FR-003). A later message of a group is held behind an earlier one as
    ``SQSSink`` holds it (:meth:`_round`). A standard topic's entries carry ``Id`` and
    ``Message`` and nothing else, as in 1.x. The sink still declares no idempotency: a resend
    outside the topic's interval, or across a caller's own whole-batch resend with an unstable
    id, is not absorbed.

    Attributes:
      MAX_BYTES: The message limit when ``max_message_bytes`` is ``None``, 262,144 bytes, 1.x's
        256 KiB and every topic's default. It is read through ``self`` once per run, so an
        assignment on the instance, a subclass's override and the class's own value all set the
        limit as they did in 1.x; a ``max_message_bytes`` the caller passed wins over it,
        whatever it holds.
      max_message_bytes: The message limit the caller passed, an exact ``int`` from 1,024 to
        1,048,576, or ``None`` for :attr:`MAX_BYTES`. It should be the topic's own
        ``MaximumMessageSize``: a larger value sends requests the topic refuses whole.
      fifo: Whether the topic is FIFO, decided once at construction (SPEC-100 FR-001).
      message_group_id: The group source the caller passed: a constant, a callable, or ``None``
        for each event's ``trace_id``.
      message_deduplication_id: The de-duplication source the caller passed: a callable, or
        ``None`` for each event's ``log_id``.
      log_foundry_counters: The engine's event totals, which :meth:`losses` reports.
      dropped_oversized: Events dropped for exceeding the message limit, counted only by a run
        that settled.
      dropped_unadjudicated: Entries whose reply named them in neither ``Successful`` nor
        ``Failed``, or whose reply could not be read, counted ``UNKNOWN`` and never resent
        (SPEC-093 FR-003). A sub-count of ``failed``, which already holds every ``UNKNOWN``
        entry, so it adds nothing to ``losses()``.
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
        fifo: bool | None = None,
        message_group_id: GroupIdSource = None,
        message_deduplication_id: DedupIdSource = None,
    ) -> None:
        """Binds the sink to a topic and decides its type once, not per emit.

        The three FIFO arguments are judged by :func:`~log_foundry.sinks._fifo.judge`, which
        ``SQSSink`` shares, after ``max_message_bytes`` and before any client is built, so every
        call refused before SPEC-100 keeps its exact error.

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
          fifo: ``True``, ``False`` or ``None`` for the topic ARN's ``.fifo`` suffix (SPEC-100
            FR-001), as ``SQSSink`` reads its queue URL. AWS requires every FIFO topic name to end
            in ``.fifo``, so ``fifo=True`` on an ARN without it is refused, and ``fifo=False``
            overrides a ``.fifo`` name, whose every publish to a real FIFO topic then fails,
            counted and announced.
          message_group_id: The group itself as a ``str``, a callable deriving it from the event,
            or ``None`` to group by ``trace_id``, so a trace keeps its order while traces travel in
            parallel; on a FIFO topic only.
          message_deduplication_id: A callable deriving the de-duplication id from the event, or
            ``None`` for its ``log_id``, the library's idempotency key (SPEC-095); on a FIFO topic
            only.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066), or ``max_message_bytes`` is
            neither ``None`` nor an ``int`` (SPEC-096); or, naming the class, if
            ``message_group_id`` is not a ``str``, a callable or ``None``,
            ``message_deduplication_id`` is neither callable nor ``None``, or ``fifo`` is neither
            a ``bool`` nor ``None`` (SPEC-100 FR-001).
          ValueError: If ``max_message_bytes`` holds a number below 1,024 or above 1,048,576; the
            message names the exact number, an integer too long to print as
            ``<int: ~N digits>``. Also if a constant group id is blank, ``fifo=True`` names an ARN
            not ending in ``.fifo``, or an id argument is passed to a topic that is not FIFO
            (SPEC-100 FR-001).
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
        is_fifo = judge(
            _diag.type_name(self),
            "topic",
            "topic_arn",
            topic_arn,
            fifo,
            message_group_id,
            message_deduplication_id,
        )
        if client is None:
            client = import_extra("boto3", "aws").client("sns")
        self.topic_arn = topic_arn
        self.client = client
        self.max_message_bytes = max_message_bytes
        self.fifo = is_fifo
        self.message_group_id = message_group_id
        self.message_deduplication_id = message_deduplication_id
        self.log_foundry_stop_signal: threading.Event | None = None
        self.log_foundry_counters = SinkCounters()
        self.dropped_oversized = 0
        self.dropped_unadjudicated = 0

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
        requests every topic refuses whole. :meth:`_round` calls this once per run of ``emit`` and
        keeps the value in the run's notes, so every round of the run reads one limit.

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

        **The limit is read once per run** of ``emit`` (:meth:`_limit`, SPEC-096 FR-003): the first
        round reads it and keeps it in the run's notes, and every later round of the run takes
        that value, so a limit changed mid-run reaches no round of it. By that one value an
        oversized message is dropped and each chunk's summed cost is bounded, so every decision
        and line of the run names one number.

        **The plan is the one ``SQSSink`` uses** (SPEC-100 FR-002). Every event is encoded once
        per run (:func:`~log_foundry.sinks._encode.encode_round`), an unencodable one ``DROPPED``
        with SPEC-069's line, and :func:`~log_foundry.sinks._fifo.plan_round` prepares each event
        occurrence once per run through :meth:`_prepare` — on a FIFO topic resolving its
        ``MessageGroupId`` and ``MessageDeduplicationId`` and costing them with the body — keeps
        the entry in this sink's run notes, so every resend carries the first send's ids, and
        chunks the rest within the count and byte limits. When every event's id callable raised,
        the round is a refusal carrying the first exception. The drops are registered by
        :meth:`_register_drops`.

        **The round loop is the one ``SQSSink`` runs** (SPEC-100 FR-004):
        :func:`~log_foundry.sinks._fifo.send_round` publishes each chunk once through
        :meth:`_send`, settles its reply by the keyed rule SQS shares — a sender fault
        ``REJECTED`` and a final rejection, any other failed entry ``RETRYABLE``, an unreadable
        reply ``UNKNOWN``, a raise ``RETRYABLE`` or ``REJECTED`` as ``_retryable_error`` answers
        — marks each sent occurrence whose fate is final done in the run's cache, and on a FIFO
        topic holds a group. In every round but the run's last a message waits, ``RETRYABLE``
        and unsent, while an earlier message of its group sent in an earlier call has neither
        landed nor used its ``max_retries + 1`` sends; in the last round nothing is held, so a
        held message follows its predecessor's final send. The hold keeps a later *call* from
        overtaking an earlier message of its group; what the topic does with a call's later
        entries when an earlier one fails is the topic's. A message a run leaves held is counted
        ``failed`` with the line ``SNSSink, <n> attempt(s), held behind an earlier message of its
        FIFO group``. A standard topic holds nothing. This method passes only what is this sink's
        own: the client call, its API's name and the class name its lines carry.

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``BaseException`` from the client or a caller's callable propagates.
        """
        context = run_context()
        limit = cast("int", context.notes(self).setdefault("limit", self._limit()))
        bodies = dict(encode_round(self, batch, self._encode, sink=_diag.type_name(self)))
        plan = plan_round(self, batch, bodies, self._prepare, max_count=self.MAX_BATCH, limit=limit)
        if plan.refused is not None:
            context.refuse(plan.refused)
            return Verdict.uniform(Outcome.REJECTED, len(batch))
        fates = [Outcome.RETRYABLE] * len(batch)
        self._register_drops(batch, plan)
        for position in plan.dropped:
            fates[position] = Outcome.DROPPED
        _fifo.send_round(
            self,
            batch,
            plan,
            fates,
            fifo=self.fifo,
            max_retries=self.max_retries,
            send=self._send,
            call="publish_batch",
            name="SNSSink",
        )
        return Verdict(outcomes=tuple(fates))

    def _prepare(self, event: dict[str, object], body: str | None, *, limit: int) -> _fifo.Entry:
        """Prepares one event occurrence's entry through ``_fifo.prepare``, or decides its drop.

        The ids, the costing and the drop reasons are :func:`~log_foundry.sinks._fifo.prepare`'s,
        which ``SQSSink`` shares (SPEC-100 FR-002). A message costs its body's UTF-8 length, the
        unit the topic counts, plus, on a FIFO topic, both ids' UTF-8 lengths, as ``SQSSink``
        costs its own: SNS documents only the body and attributes as counting, so the ids
        over-count by at most 256 bytes, which drops an event at the edge, counted and announced,
        rather than sending one the topic may refuse with its chunk. It is dropped as oversize
        when that cost exceeds ``limit``.

        Args:
          event: The event.
          body: Its encoded body, or ``None`` when the encode step could not encode it.
          limit: The run's effective message limit, in bytes (:meth:`_limit`).

        Returns:
          The entry.

        Raises:
          None. A ``BaseException`` from a caller's callable propagates.
        """
        return _fifo.prepare(
            body,
            event,
            fifo=self.fifo,
            group=self.message_group_id,
            dedup=self.message_deduplication_id,
            limit=limit,
        )

    def _register_drops(self, batch: list[dict[str, object]], plan: _fifo.Plan) -> None:
        """Counts and announces the round's oversize and id-failure drops, ``SETTLED``.

        It replaces SPEC-096's ``_bodies``, whose oversize check moved into the plan
        :func:`~log_foundry.sinks._fifo.plan_round` builds for both FIFO sinks (SPEC-100 FR-002),
        and mirrors ``SQSSink._register_drops``. The unencodable drop's line is the encode step's
        own. An oversize drop keeps 1.x's shape, its own count and its own line,
        ``SNSSink, exceeds the <limit>-byte message limit``, naming the run's limit the plan
        carries rather than one read when the line is written. The id failures, on a FIFO topic
        only, are announced once per round, ``SNSSink, a FIFO id callable raised <Type>``, naming
        the first exception's type (SPEC-058 FR-004), through the one line both FIFO sinks
        share, :func:`~log_foundry.sinks._fifo.announce_id_failures`. Every
        drop is registered ``SETTLED``, so it is counted and announced only when ``emit`` returns
        — on the engine, only when the run settles (SPEC-059 FR-001, SPEC-074).

        Args:
          batch: The round's events.
          plan: The round's plan.

        Returns:
          None.

        Raises:
          None.
        """
        context = run_context()
        oversize = sum(1 for reason in plan.dropped.values() if reason == "oversize")
        limit = plan.limit
        for _ in range(oversize):
            context.count(self, "dropped_oversized", 1, when=When.SETTLED)
            context.announce(
                lambda: _diag.lost("event", 1, f"SNSSink, exceeds the {limit}-byte message limit"),
                when=When.SETTLED,
            )
        _fifo.announce_id_failures(self, plan, "SNSSink")

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

    def _send(self, prepared: list[_fifo.Prepared], ids: list[str]) -> Called[KeyedReply]:
        """Publishes one chunk once and reads the reply per entry (SPEC-074 FR-002).

        Nothing here waits or retries: the engine resends a ``RETRYABLE`` entry in a later round.
        The reply's ``Failed`` list is read through
        :func:`~log_foundry.sinks._batch.adjudicate_keyed` against each entry's ``Id``, so a
        malformed list adjudicates none of the chunk rather than escaping ``emit`` raw. Each
        entry carries ``Id`` and ``Message``, and ``MessageGroupId`` and
        ``MessageDeduplicationId`` exactly when :func:`~log_foundry.sinks._fifo.prepare` resolved
        them, which is on a FIFO topic and never one without the other (SPEC-100 FR-002), as
        ``SQSSink._send`` attaches its own; a standard topic's entry is byte for byte 1.x's.

        **A client exception costs this chunk, never the batch** (SPEC-048 FR-002), and it is
        treated as **provable non-delivery** for the chunk: a read timeout means the request went
        out and the reply was lost, so a resend may duplicate — taken because an unreachable
        endpoint is the common case. **An entry SNS marks ``SenderFault`` is not resent**
        (SPEC-070 FR-003): SNS's ``BatchResultErrorEntry`` carries the same required field as
        SQS's, and a byte-identical resend of the caller's fault fails again.

        Args:
          prepared: One chunk's prepared entries, each with its body and its ids or ``None``.
          ids: Each entry's ``Id``, kept across the run's rounds.

        Returns:
          The call's reply.

        Raises:
          None. ``Exception`` is caught rather than ``BaseException``, so a ``KeyboardInterrupt``
            or ``SystemExit`` still reaches the caller (SPEC-025 FR-004).
        """
        entries: list[dict[str, str]] = []
        for entry_id, item in zip(ids, prepared, strict=True):
            entry = {"Id": entry_id, "Message": item.body}
            if item.group_id is not None:
                entry["MessageGroupId"] = item.group_id
            if item.dedup_id is not None:
                entry["MessageDeduplicationId"] = item.dedup_id
            entries.append(entry)
        try:
            response = self.client.publish_batch(
                TopicArn=self.topic_arn, PublishBatchRequestEntries=entries
            )
        except Exception as err:
            return Called(None, error=err, retryable=is_retryable(self._retryable_error, err))
        return Called(read_keyed(self._retryable_entry, ids, response))
