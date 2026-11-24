"""SQSSink — ship batches to an SQS queue (arch §8, §9.1, guide Phase 10).

Ordering on a FIFO queue is kept within a message group for the sink's own retry budget and is
best-effort past it (SPEC-074 FR-002): a message is held back while an earlier message of its
group is still being retried within ``max_retries + 1`` sends, as 1.x's per-chunk loop held it,
and once that message has used its sends a later one may land first, because holding a group for
the worker's whole budget would trade log delivery for ordering the consumer can rebuild from
``timestamp``. In a run's last round nothing is held, each held message sent after its
predecessor's final send, as 1.x sent it after giving up on that predecessor (SPEC-074 Q4).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Final, cast

from log_foundry import _diag
from log_foundry.sanitize import _int_text
from log_foundry.sinks import _engine, _fifo
from log_foundry.sinks._args import require_callable
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

__all__ = ["DedupIdSource", "GroupIdSource", "SQSSink"]

DEFAULT_GROUP_ID = _fifo.DEFAULT_GROUP_ID
"""Fallback group for an event carrying no usable ``trace_id``, the object ``_fifo`` holds."""

MAX_ID_LEN = _fifo.MAX_ID_LEN
"""SQS maximum length for ``MessageGroupId`` and ``MessageDeduplicationId``, ``_fifo``'s object."""

_SERVICE_MAX_BYTES: Final = 1_048_576
"""The most bytes SQS takes in one message and in one request's messages together (SPEC-096).

``SendMessageBatch``: "The maximum allowed individual message size and the maximum total payload
size (the sum of the individual lengths of all of the batched messages) are both 1 MiB 1,048,576
bytes". Every limit the sink applies is capped here, so a ``MAX_BYTES`` raised past it costs the
events over 1 MiB one at a time rather than a request the service refuses whole.
"""

_LIMIT_FLOOR: Final = 1_024
"""The smallest ``max_message_bytes`` the sink accepts, a queue's least ``MaximumMessageSize``.

``CreateQueue`` and ``SetQueueAttributes``: "Valid values: An integer from 1,024 bytes (1 KiB) to
1,048,576 bytes (1 MiB)" (SPEC-096 FR-001).
"""

_Prepared = _fifo.Prepared
_Plan = _fifo.Plan
_Entry = _fifo.Entry
_bounded = _fifo.bounded
_hold = _fifo.hold
_block = _fifo.block
"""The six names SPEC-100 moved to ``_fifo``, each still bound here to the one shared object."""


class SQSSink:
    """A :class:`~log_foundry.sinks.base.Sink` that sends events to an SQS queue.

    SQS is the headline production path: a durable buffer that decouples the app from ELK
    availability, so events accumulate safely in the queue during downstream outages instead of
    being lost or back-pressuring the app. ``boto3`` is an optional dependency, imported lazily
    inside the sink, so the library stays dependency-free unless a sink is actually instantiated
    without an injected client.

    Both queue types are supported (SPEC-016). A ``.fifo`` queue URL selects FIFO behaviour,
    where every entry additionally carries a ``MessageGroupId``: SQS orders messages within a
    group, and the default group is the event's own ``trace_id``, which is exactly the unit
    whose events should stay ordered while keeping traces independent. Standard queues are
    untouched, their entries carrying ``Id`` and ``MessageBody`` and nothing else.

    **It delivers through SPEC-072's engine** (SPEC-074 FR-002). :meth:`deliver` is one round:
    every chunk is sent once, its reply read per entry through the keyed reader, and the engine
    resends only the entries that may land, waiting the 1.x backoff between rounds. The
    worst-case delay (SPEC-027 FR-005) is therefore ``max_retries`` waits per run, 0.7 s at the
    defaults, every one interruptible by ``shutdown()``.

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport
    lock. ``boto3`` documents that clients — unlike Sessions and Resources — are
    thread-safe, and this sink builds its client once in ``__init__`` rather than inside
    ``emit`` — which is what keeps it clear of the documented caveat that calling
    ``boto3.client()`` concurrently can produce response-ordering faults. Nothing else here is
    rebound after construction.

    It also **adds no post-close guard** (SPEC-032 FR-003): ``close()`` is a documented no-op,
    because the client is the caller's to release or the SDK's to reap, so a batch emitted
    afterwards still reaches the queue.

    It keeps **no** client buffer (SPEC-036 FR-002): the driver call returns only once the
    destination has the batch, so nothing is queued locally between emits.

    **Its message limit is the caller's to set** (SPEC-096). Each run of ``emit`` reads one limit,
    used for every decision of every round of that run: ``max_message_bytes`` when it is not
    ``None``, otherwise :attr:`MAX_BYTES`, and then the smaller of that and SQS's own 1,048,576
    bytes. An entry costing more is dropped, counted in ``dropped_oversized`` and announced naming
    the limit, and a request's entries together cost at most the same limit.

    Attributes:
      MAX_BYTES: The message limit when ``max_message_bytes`` is ``None``, 262,144 bytes, 1.x's
        256 KiB. It is read through ``self`` once per run, so an assignment on the instance, a
        subclass's override and the class's own value all set the limit as they did in 1.x; a
        ``max_message_bytes`` the caller passed wins over it, whatever it holds.
      max_message_bytes: The message limit the caller passed, an exact ``int`` from 1,024 to
        1,048,576, or ``None`` for :attr:`MAX_BYTES`. It should be the queue's own
        ``MaximumMessageSize``: a larger value sends what the queue then refuses.
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
        queue_url: str,
        *,
        client: Any = None,
        max_retries: int = 3,
        fifo: bool | None = None,
        message_group_id: GroupIdSource = None,
        message_deduplication_id: DedupIdSource = None,
        max_message_bytes: int | None = None,
    ) -> None:
        """Binds the sink to a queue and decides its type once, not per emit.

        Args:
          queue_url: The queue to send to.
          client: A boto3-shaped SQS client, or ``None`` to build one lazily.
          max_retries: Further rounds the engine gives the entries still failing, floored at
            zero as ``Worker._emit`` floors its own (SPEC-021); a FIFO group keeps its order for
            ``max_retries + 1`` sends of a message. Anything but an ``int`` is refused
            with ``TypeError`` (SPEC-066 FR-001), before any client is built; a ``bool`` or
            ``int`` subclass is read as the ``int`` it holds.
          fifo: ``True``, ``False`` or ``None`` for the queue URL's ``.fifo`` suffix. AWS requires
            every FIFO queue name to end in ``.fifo``, so ``fifo=True`` on a URL without it is
            refused, and ``fifo=False`` overrides a ``.fifo`` name (SPEC-100 FR-001).
          message_group_id: The group itself as a ``str``, a callable deriving it, or ``None`` to
            group by ``trace_id``; on a FIFO queue only.
          message_deduplication_id: A callable deriving the dedup id, or ``None`` to use the
            event's ``log_id``; on a FIFO queue only.
          max_message_bytes: The most bytes one message may cost, and the most a request's
            messages may cost together, or ``None`` for :attr:`MAX_BYTES` (SPEC-096 FR-001). Pass
            the queue's ``MaximumMessageSize``, in bytes: ``1048576`` for a queue created at the
            service default. It is judged after every argument a 1.x call could pass, before the
            FIFO arguments' :func:`~log_foundry.sinks._fifo.judge` and any client is built, read
            through ``exact_int``, so a ``bool`` or ``int`` subclass is judged and
            stored as the exact ``int`` it holds, and refused outside 1,024 to 1,048,576, the
            range a queue's own limit takes, rather than floored, since a new argument has no
            working configuration to keep.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066), or if ``message_group_id``
            is not a ``str``, a callable or ``None``, or ``message_deduplication_id`` is neither
            callable nor ``None`` (SPEC-071 FR-005). Both are judged on a standard queue too,
            which never reads either: the value is outside the declared type. Also if
            ``max_message_bytes`` is neither ``None`` nor an ``int`` (SPEC-096), or ``fifo`` is
            neither a ``bool`` nor ``None`` (SPEC-100 FR-001), where 1.x read it by truthiness.
          ValueError: If a constant group id is blank, or if ``max_message_bytes`` holds a number
            below 1,024 or above 1,048,576; the message names the exact number, an integer too
            long to print as ``<int: ~N digits>``. Also, through ``_fifo.judge`` (SPEC-100
            FR-001), if ``fifo=True`` names a URL not ending in ``.fifo``, or an id argument is
            passed to a queue that is not FIFO, which 1.x ignored.
          ImportError: If ``boto3`` is needed and the ``aws`` extra is not installed.
        """
        owner = _diag.type_name(self)
        require_callable(message_group_id, "message_group_id", owner, text=True)
        require_callable(message_deduplication_id, "message_deduplication_id", owner)
        self.max_retries = floor_count(max_retries, 0, "max_retries", "SQSSink")
        if (
            issubclass(type(message_group_id), str)
            and not _diag._STR_STR(cast("str", message_group_id)).strip()
        ):
            raise ValueError(
                "message_group_id must be a non-empty string; pass None to group by trace_id"
            )
        if max_message_bytes is not None:
            max_message_bytes = exact_int(max_message_bytes, "max_message_bytes", "SQSSink")
            if max_message_bytes < _LIMIT_FLOOR or max_message_bytes > _SERVICE_MAX_BYTES:
                raise ValueError(
                    f"SQSSink max_message_bytes must be from {_LIMIT_FLOOR} to "
                    f"{_SERVICE_MAX_BYTES} bytes, got {_int_text(max_message_bytes)}"
                )
        is_fifo = judge(
            owner, "queue", "queue_url", queue_url, fifo, message_group_id, message_deduplication_id
        )
        if client is None:
            client = import_extra("boto3", "aws").client("sqs")
        self.queue_url = queue_url
        self.client = client
        self.log_foundry_stop_signal: threading.Event | None = None
        self.fifo = is_fifo
        self.message_group_id = message_group_id
        self.message_deduplication_id = message_deduplication_id
        self.max_message_bytes = max_message_bytes
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

        Args:
          None.

        Returns:
          The counters. Sender faults land in ``failed`` alongside the retry-exhausted entries:
          SQS rejected the request itself, so the entry is as lost as one that timed out, and
          SPEC-016 settled that it must not be re-sent byte-identical. An event dropped because
          a FIFO id callable raised for it is in ``dropped`` beside the oversized ones, and so is
          one the encode step could not encode (SPEC-069 FR-001), while ``dropped_oversized``
          keeps meaning oversize only (SPEC-058 FR-004).

        Raises:
          None.
        """
        return self.log_foundry_counters.losses()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers the batch through the engine inline, keeping the 1.x ``emit`` contract.

        Args:
          batch: The events to ship.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the run failed: nothing landed and an entry was still failing
            retryably or refused for a reason other than a sender fault. A batch lost only to
            sender faults returns, the entries counted, because a byte-identical resend can only
            fail the same way (SPEC-016 FR-006); so does one of nothing but drops.
          Exception: The first id callable's own exception, by identity, when every event's id
            callable failed (SPEC-058 FR-004): the batch is refused, costs one attempt, and
            nothing is counted.
        """
        emit_via_deliver(self, batch)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-074 FR-002).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``BaseException`` from the client or a caller's callable propagates.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Sends every remaining chunk once and classifies every event of the round.

        The drops :meth:`_chunks` decides are ``DROPPED`` and their lines registered ``SETTLED``,
        so they are counted and announced only when ``emit`` returns — on the engine, only when
        the run settles. When every event's id callable raised, the round is a refusal carrying
        the first exception. Otherwise :func:`~log_foundry.sinks._fifo.send_round`, the one
        round loop ``SNSSink`` runs too (SPEC-100 FR-004), sends each chunk once through
        :meth:`_send`, its FIFO holds decided at send time in every round but the run's last
        (:attr:`RunContext.last`), where every send is final and so nothing is held: a held
        message then follows its predecessor's final send in the same round, chunks going out in
        batch order, as 1.x sent it after giving up on that predecessor (SPEC-074 Q4). Each
        entry's outcome is set from the reply: a sender fault ``REJECTED`` and marked a final
        rejection with 1.x's line, any other failed entry ``RETRYABLE``, an unreadable reply
        ``UNKNOWN`` with one line, and a raise ``RETRYABLE`` or ``REJECTED`` as
        ``_retryable_error`` answers. Each landed occurrence is marked in the run's cache, so a
        later round never resends a landed occurrence's ids.

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``BaseException`` from the client or a caller's callable propagates.
        """
        context = run_context()
        plan = self._chunks(batch)
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
            call="send_message_batch",
            name="SQSSink",
        )
        return Verdict(outcomes=tuple(fates))

    def _register_drops(self, batch: list[dict[str, object]], plan: _fifo.Plan) -> None:
        """Counts and announces the round's oversize and id-failure drops, ``SETTLED``.

        The unencodable drop's line is the encode step's own (:func:`encode_round`). The oversize
        lines keep their one-per-event shape and the id failures are announced once per round,
        naming the first exception's type (SPEC-058 FR-004), all counted and announced only when
        ``emit`` returns — on the engine, only when the run settles.

        Args:
          batch: The round's events.
          plan: The round's plan.

        Returns:
          None.

        Raises:
          None.
        """
        context = run_context()
        entries = cast("list[_fifo.Entry | None]", context.notes(self)["prepared_round"])
        sizes = [
            cached.size
            for position, reason in sorted(plan.dropped.items())
            if reason == "oversize" and (cached := entries[position]) is not None
        ]
        failures = [
            cached.error
            for position, reason in sorted(plan.dropped.items())
            if reason == "id failure" and (cached := entries[position]) is not None
        ]
        if sizes:
            limit = plan.limit
            context.count(self, "dropped_oversized", len(sizes), when=When.SETTLED)
            context.announce(lambda: self._announce_oversized(sizes, limit), when=When.SETTLED)
        if failures:
            kind = _diag.type_name(failures[0])
            count = len(failures)
            context.announce(
                lambda: _diag.lost("event", count, f"SQSSink, a FIFO id callable raised {kind}"),
                when=When.SETTLED,
            )

    def close(self) -> None:
        """Does nothing, since the sink buffers nothing internally (FR-005).

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def _group_id(self, event: dict[str, object]) -> str:
        """Resolves one event's ``MessageGroupId`` through ``_fifo.group_id``.

        **Nothing in the sink calls this any more** (SPEC-100 FR-002): :meth:`_prepare` hands
        ``message_group_id`` itself to :func:`~log_foundry.sinks._fifo.prepare`, which resolves
        the id for ``SQSSink`` and ``SNSSink`` alike. A subclass that overrides this method
        therefore changes nothing the sink sends, and as a private method it carries no
        compatibility promise; it stays only so the name still resolves.

        Args:
          event: The event being sent.

        Returns:
          The group id, as the sink would send it.

        Raises:
          Exception: A caller-supplied callable's own exception.
        """
        return _fifo.group_id(self.message_group_id, event)

    def _dedup_id(self, event: dict[str, object]) -> str:
        """Resolves one event's ``MessageDeduplicationId`` through ``_fifo.dedup_id``.

        **Nothing in the sink calls this any more** (SPEC-100 FR-002): :meth:`_prepare` hands
        ``message_deduplication_id`` itself to :func:`~log_foundry.sinks._fifo.prepare`, which
        resolves the id for ``SQSSink`` and ``SNSSink`` alike. A subclass that overrides this
        method therefore changes nothing the sink sends, and as a private method it carries no
        compatibility promise; it stays only so the name still resolves.

        Args:
          event: The event being sent.

        Returns:
          The deduplication id, as the sink would send it.

        Raises:
          Exception: A caller-supplied callable's own exception.
        """
        return _fifo.dedup_id(self.message_deduplication_id, event)

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

    def _limit(self) -> int:
        """Returns the effective message limit: the caller's, else ``MAX_BYTES``, at most 1 MiB.

        ``max_message_bytes`` when it is not ``None``, otherwise :attr:`MAX_BYTES` read through
        ``self``, so an instance assignment and a subclass override both reach it as in 1.x, and
        then the smaller of that and :data:`_SERVICE_MAX_BYTES` (SPEC-096 FR-002). The test is
        ``is None``, never truthiness. A judged ``max_message_bytes`` is never above the cap, so
        the cap binds only a ``MAX_BYTES`` raised past 1 MiB, which would otherwise send requests
        SQS refuses whole. :meth:`_chunks` calls this once per run of ``emit``, keeps the value in
        the run's notes and carries it on each round's plan, so every decision of every round of
        the run reads one limit.

        Args:
          None.

        Returns:
          The limit in bytes.

        Raises:
          None.
        """
        limit = self.MAX_BYTES if self.max_message_bytes is None else self.max_message_bytes
        return min(limit, _SERVICE_MAX_BYTES)

    def _chunks(self, batch: list[dict[str, object]]) -> _fifo.Plan:
        """Encodes the round's batch and plans its sends through ``_fifo.plan_round``.

        The plan — every drop decided before the first send, each entry prepared once per run and
        kept in this sink's run notes by event identity, and the chunks bounded by both the
        entry-count and byte limits — is :func:`~log_foundry.sinks._fifo.plan_round`'s, which
        ``SNSSink`` shares (SPEC-100 FR-002). What stays here is what is this sink's own: the
        encode step (:func:`~log_foundry.sinks._encode.encode_round`), kept in this module so
        ``tests/test_encode.py``'s roster still derives the sink, and the limit.

        **The limit is read once per run** of ``emit`` (:meth:`_limit`, SPEC-096 FR-002): the first
        round reads it and keeps it in the run's notes under ``"limit"``, and every later round of
        the run takes that value, so a limit changed mid-run reaches no round of it. The one value
        bounds each entry :meth:`_prepare` costs and each chunk's summed cost, and rides on the
        plan so the round's oversize lines name the same number; an entry prepared in an earlier
        round of the run therefore keeps a verdict the later rounds' chunking agrees with.

        Args:
          batch: The events the round was handed.

        Returns:
          The round's plan.

        Raises:
          None. A ``BaseException`` from a caller's callable propagates.
        """
        notes = run_context().notes(self)
        limit = cast("int", notes.setdefault("limit", self._limit()))
        bodies = dict(encode_round(self, batch, self._encode, sink=_diag.type_name(self)))
        return plan_round(self, batch, bodies, self._prepare, max_count=self.MAX_BATCH, limit=limit)

    def _prepare(self, event: dict[str, object], body: str | None, *, limit: int) -> _fifo.Entry:
        """Prepares one event occurrence's entry through ``_fifo.prepare``, or decides its drop.

        The ids, the costing and the drop reasons are :func:`~log_foundry.sinks._fifo.prepare`'s,
        which ``SNSSink`` shares (SPEC-100 FR-002): an entry is costed as its body's UTF-8 length
        plus, on a FIFO queue, both ids' UTF-8 lengths (SPEC-016 FR-005), and dropped as oversize
        when that cost exceeds ``limit``, the run's effective message limit (SPEC-096 FR-002).

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

    def _announce_oversized(self, sizes: list[int], limit: int) -> None:
        """Writes one line per event dropped for exceeding the message limit (FR-004).

        One line per event is today's shape, kept on purpose; what SPEC-058 FR-004 changed is
        only *when* the lines are written — after the count, and only by a run that settled
        (SPEC-074). The limit each line names is the run's, the one the round judged against,
        passed in rather than read here, since the lines are written when the run settles and
        ``MAX_BYTES`` could have been reassigned by then (SPEC-096 FR-002).

        Args:
          sizes: The costed size of each dropped event, in the order they were met.
          limit: The effective message limit of the run that dropped them.

        Returns:
          None.

        Raises:
          None.
        """
        for size in sizes:
            _diag.lost(
                "event",
                1,
                f"SQSSink, {size} bytes exceeds the {limit}-byte message limit",
            )

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
        """Sends one chunk once and reads the reply per entry (SPEC-074 FR-002).

        Nothing here waits or retries: the engine resends a ``RETRYABLE`` entry in a later round.
        The reply's ``Failed`` list is read through
        :func:`~log_foundry.sinks._batch.adjudicate_keyed` against the ``Id`` each entry was
        given, so a malformed list — not a list, an item that is not a mapping, an unknown or
        repeated ``Id`` — adjudicates none of the chunk rather than escaping ``emit`` raw, and a
        missing ``Failed`` pairs nothing, as 1.x read it. Entries SQS marks ``SenderFault`` are
        ``REJECTED`` (SPEC-016 FR-006), since a retry re-sends them byte-identical; a missing flag
        is treated as retryable, so an unfamiliar response shape degrades to a resend rather than
        a drop. The FIFO parameters are attached only on a FIFO queue (SPEC-016 FR-004).

        **A client exception costs this chunk, never the batch** (SPEC-048 FR-002), and it is
        treated as **provable non-delivery** for the chunk: a retryable one leaves the chunk
        ``RETRYABLE``, which the engine resends, and one :meth:`_retryable_error` refuses is
        ``REJECTED`` at its first attempt. A read timeout means the request went out and the
        *response* was lost, so entries may have landed and a resend duplicates them, which is
        SPEC-018's "cannot prove nothing landed"; it is taken on expected cost, as before.

        Args:
          prepared: One chunk's serialized, costed entries.
          ids: Each entry's ``Id``, kept across the run's rounds (:func:`entry_ids`).

        Returns:
          The reply, per entry.

        Raises:
          None. ``Exception`` is caught rather than ``BaseException``, so a ``KeyboardInterrupt``
            or ``SystemExit`` still reaches the caller (SPEC-025 FR-004).
        """
        entries: list[dict[str, str]] = []
        for entry_id, item in zip(ids, prepared, strict=True):
            entry = {"Id": entry_id, "MessageBody": item.body}
            if item.group_id is not None:
                entry["MessageGroupId"] = item.group_id
            if item.dedup_id is not None:
                entry["MessageDeduplicationId"] = item.dedup_id
            entries.append(entry)
        try:
            response = self.client.send_message_batch(QueueUrl=self.queue_url, Entries=entries)
        except Exception as err:
            return Called(None, error=err, retryable=is_retryable(self._retryable_error, err))
        return Called(read_keyed(self._retryable_entry, ids, response))
