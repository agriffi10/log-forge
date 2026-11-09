"""SQSSink — ship batches to an SQS queue (arch §8, §9.1, guide Phase 10).

Ordering on a FIFO queue is kept within a message group for the sink's own retry budget and is
best-effort past it (SPEC-074 FR-002): a message is held back while an earlier message of its
group is still being retried within ``max_retries + 1`` sends, as 1.x's per-chunk loop held it,
and once that message has used its sends a later one may land first, because holding a group for
the worker's whole budget would trade log delivery for ordering the consumer can rebuild from
``timestamp``.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from log_foundry import _diag
from log_foundry.sinks import _engine
from log_foundry.sinks._args import require_callable
from log_foundry.sinks._chunk import chunk_items
from log_foundry.sinks._classify import aws_retryable, is_retryable, sender_fault
from log_foundry.sinks._encode import encode_round
from log_foundry.sinks._engine import When, emit_via_deliver, run_context
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._retry import floor_count
from log_foundry.sinks._rounds import (
    Called,
    KeyedReply,
    Lost,
    entry_ids,
    mark_sent,
    read_keyed,
    sends_of,
    settle_keyed,
)
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    import threading

    from log_foundry.sinks.base import SinkLosses

__all__ = ["DedupIdSource", "GroupIdSource", "SQSSink"]

DEFAULT_GROUP_ID = "log-foundry"
"""Fallback group for an event carrying no usable ``trace_id`` — never send an empty group id."""

MAX_ID_LEN = 128
"""SQS maximum length for ``MessageGroupId`` and ``MessageDeduplicationId``."""

GroupIdSource = str | Callable[[dict[str, object]], str] | None
"""What ``message_group_id=`` accepts: an event key to read, a callable, or ``None``."""

DedupIdSource = Callable[[dict[str, object]], str] | None
"""What ``message_deduplication_id=`` accepts: a callable, or ``None`` for a minted UUID."""


class _Prepared(NamedTuple):
    """One event serialized and costed, with its FIFO ids resolved, at its place in the round.

    The ids are ``None`` on a standard queue, and are derived once per run rather than again at
    send time or in a later round: the deduplication fallback mints a fresh UUID, so deriving
    twice would bill the byte budget for one value and put a different one on the wire, and a
    resend with a new id would lose SQS's five-minute de-duplication of it (SPEC-074 FR-002).

    Attributes:
      position: The event's position in the batch the round was handed, so each chunk's outcomes
        map back to the verdict.
      body: The serialized event.
      group_id: The resolved ``MessageGroupId``, or ``None``.
      dedup_id: The resolved ``MessageDeduplicationId``, or ``None``.
      size: The costed bytes: the body's UTF-8 length plus, on a FIFO queue, both ids'. Carried
        so :func:`~log_foundry.sinks._chunk.chunk_items` reads the cost rather than recomputing
        it (SPEC-071 FR-004).
    """

    position: int
    body: str
    group_id: str | None
    dedup_id: str | None
    size: int


class _Plan(NamedTuple):
    """One round's sends and drops, every drop decided before the first send (SPEC-074 FR-002).

    Attributes:
      chunks: The round's chunks, each valid for one request, built with ``chunk_items``.
      dropped: Each dropped position of the round's batch and its reason: ``"oversize"``,
        ``"id failure"`` or ``"unencodable"``.
      refused: The first id callable's exception, set only when every event's id callable
        raised; the round is then a refusal.
    """

    chunks: list[list[_Prepared]]
    dropped: dict[int, str]
    refused: Exception | None


@dataclass(slots=True)
class _Entry:
    """One event occurrence's prepared entry or drop reason, kept for its delivery run.

    Attributes:
      body: The serialized event, or ``""`` for a drop.
      group_id: The resolved ``MessageGroupId``, or ``None``.
      dedup_id: The resolved ``MessageDeduplicationId``, or ``None``.
      size: The costed bytes.
      drop: The drop reason, or ``None`` for an entry that can be sent.
      error: The id callable's exception, for an id failure.
      landed: Whether SQS has accepted this occurrence.
    """

    body: str = ""
    group_id: str | None = None
    dedup_id: str | None = None
    size: int = 0
    drop: str | None = None
    error: Exception | None = None
    landed: bool = False


def _bounded(raw: str, fallback: str) -> str:
    """Normalizes a derived id, falling back when blank and truncating when over-long.

    Args:
      raw: The derived id.
      fallback: What a blank id becomes.

    Returns:
      The id, at most the SQS maximum length.

    Raises:
      None.
    """
    cleaned = raw.strip()
    if not cleaned:
        return fallback
    return cleaned[:MAX_ID_LEN]


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

    Attributes:
      log_foundry_counters: The engine's event totals, which :meth:`losses` reports.
      dropped_oversized: Events dropped for exceeding the message limit, counted only by a run
        that settled.
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
          fifo: Overrides the queue type. AWS requires every FIFO queue name to end in
            ``.fifo``, so the suffix is a contract rather than a guess, but an explicit flag
            still wins.
          message_group_id: A constant or a callable deriving the group, or ``None`` to group by
            ``trace_id``.
          message_deduplication_id: A callable deriving the dedup id, or ``None`` to use the
            event's ``log_id``.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066), or if ``message_group_id``
            is not a ``str``, a callable or ``None``, or ``message_deduplication_id`` is neither
            callable nor ``None`` (SPEC-071 FR-005). Both are judged on a standard queue too,
            which never reads either: the value is outside the declared type.
          ValueError: If a constant group id is blank.
          ImportError: If ``boto3`` is needed and the ``aws`` extra is not installed.
        """
        owner = type(self).__name__
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
        if client is None:
            client = import_extra("boto3", "aws").client("sqs")
        self.queue_url = queue_url
        self.client = client
        self.log_foundry_stop_signal: threading.Event | None = None
        self.fifo = queue_url.endswith(".fifo") if fifo is None else fifo
        self.message_group_id = message_group_id
        self.message_deduplication_id = message_deduplication_id
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
        the first exception. Otherwise each chunk is sent once through :meth:`_send`, its FIFO
        holds decided at send time (:func:`_hold`), and each entry's outcome set from the reply:
        a sender fault ``REJECTED`` and marked a final rejection with 1.x's line, any other failed
        entry ``RETRYABLE``, an unreadable reply ``UNKNOWN`` with one line, and a raise
        ``RETRYABLE`` or ``REJECTED`` as ``_retryable_error`` answers.

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
        entries = cast("list[_Entry | None]", context.notes(self)["prepared_round"])
        lost = Lost(self)
        final: list[int] = []
        blocked: set[str] = set()
        limit = self.max_retries + 1
        name = "SQSSink"
        for chunk in plan.chunks:
            held = (
                _hold(
                    chunk, blocked, [sends_of(self, batch[item.position]) for item in chunk], limit
                )
                if self.fifo
                else [False] * len(chunk)
            )
            sending = [item for item, hold in zip(chunk, held, strict=True) if not hold]
            waiting = [batch[item.position] for item, hold in zip(chunk, held, strict=True) if hold]
            lost.add(
                waiting,
                when=When.LAST,
                key="held",
                line=lambda n, count: _diag.lost(
                    "message",
                    count,
                    f"{name}, {n} attempt(s), held behind an earlier message of its FIFO group",
                ),
            )
            if not sending:
                continue
            carried = [batch[item.position] for item in sending]
            mark_sent(self, carried)
            sent = self._send(sending, entry_ids(self, carried))
            landed = settle_keyed(
                self,
                positions=[item.position for item in sending],
                carried=carried,
                error=sent.error,
                retryable=sent.retryable,
                reply=sent.result,
                call="send_message_batch",
                name="SQSSink",
                fates=fates,
                final=final,
                lost=lost,
            )
            for position in landed:
                cached = entries[position]
                if cached is not None:
                    cached.landed = True
            if self.fifo:
                _block(sending, fates, [sends_of(self, event) for event in carried], blocked, limit)
        lost.register()
        context.final_rejections(final)
        return Verdict(outcomes=tuple(fates))

    def _register_drops(self, batch: list[dict[str, object]], plan: _Plan) -> None:
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
        entries = cast("list[_Entry | None]", context.notes(self)["prepared_round"])
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
            context.count(self, "dropped_oversized", len(sizes), when=When.SETTLED)
            context.announce(lambda: self._announce_oversized(sizes), when=When.SETTLED)
        if failures:
            kind = type(failures[0]).__name__
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
        """Resolves one event's ``MessageGroupId`` (SPEC-016 FR-002).

        A constant is used as given, a callable is asked, and otherwise the event's own
        ``trace_id`` groups it. The constant is told apart by its real type and read through
        ``str.__str__`` (SPEC-071 FR-005), so a callable whose ``__class__`` claims ``str`` is
        still asked, and a ``str`` subclass's own ``strip`` never runs per event. Every route is
        bounded, so a caller's callable returning an empty string cannot put an empty parameter on
        the wire.

        Args:
          event: The event being sent.

        Returns:
          The group id.

        Raises:
          Exception: A caller-supplied callable's own exception, which :meth:`_chunks` turns
            into a drop of this one event (SPEC-058 FR-004).
        """
        source = self.message_group_id
        if issubclass(type(source), str):
            raw = _diag._STR_STR(cast("str", source))
        elif source is not None:
            raw = cast("Callable[[dict[str, object]], str]", source)(event)
        else:
            raw = str(event.get("trace_id", ""))
        return _bounded(raw, DEFAULT_GROUP_ID)

    def _dedup_id(self, event: dict[str, object]) -> str:
        """Resolves one event's ``MessageDeduplicationId`` (SPEC-016 FR-003).

        This defaults to the event's ``log_id``, already a per-event UUID, so SQS's five-minute
        deduplication window can never collapse two genuinely distinct records. The fallback
        mints a fresh id for the same reason: a shared constant would make unrelated events
        deduplicate each other.

        Args:
          event: The event being sent.

        Returns:
          The deduplication id.

        Raises:
          Exception: A caller-supplied callable's own exception, which :meth:`_chunks` turns
            into a drop of this one event (SPEC-058 FR-004).
        """
        source = self.message_deduplication_id
        raw = source(event) if source is not None else str(event.get("log_id", ""))
        if raw.strip():
            return _bounded(raw, "")
        from log_foundry.ids import new_log_id

        return new_log_id()

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

    def _chunks(self, batch: list[dict[str, object]]) -> _Plan:
        """Splits the round's batch into sends within both the entry-count and byte limits.

        The boundaries are :func:`~log_foundry.sinks._chunk.chunk_items`' (SPEC-071 FR-004): every
        per-event drop is decided first, so an event that can never fit is filtered out before any
        boundary is drawn, and the chunks are materialised as a list, so all of them are known
        before the first send.

        **Each entry is prepared once per run** (SPEC-074 FR-002). Its body comes from the encode
        step (:func:`~log_foundry.sinks._encode.encode_round`), and on a FIFO queue its ids are
        resolved once (SPEC-016 FR-005), because they travel in the same request; the prepared
        entry — body, ``MessageGroupId``, ``MessageDeduplicationId`` and costed size — or its drop
        reason is kept in the run's notes keyed by event identity, so the id callables and the
        random fallback run **once per event per run** and every round resends the identical
        entry. Only the grouping and each entry's index are per round. An identity that appears
        twice keeps one entry per occurrence, and a round hands an occurrence the first entry of
        its identity that has not landed. An entry whose costed size exceeds the limit can never
        fit a message, so it is dropped with a counted warning (FR-004) — and the check is made
        on everything that travels, not the body alone.

        **A FIFO id callable that raises for one event costs only that event** (SPEC-058 FR-004),
        under the wrappers' ledger rule: when every event of the round failed in its id callable,
        ``refused`` carries the first exception; otherwise the failed events are dropped. This
        counts and announces nothing: :meth:`_round` turns the drops into ``DROPPED`` outcomes
        and their lines, counted and announced only when ``emit`` returns — on the engine, only
        when the run settles.

        Args:
          batch: The events the round was handed.

        Returns:
          The round's plan.

        Raises:
          None. A ``BaseException`` from a caller's callable propagates.
        """
        notes = run_context().notes(self)
        cache = cast(
            "dict[int, tuple[dict[str, object], list[_Entry]]]", notes.setdefault("prepared", {})
        )
        round_entries: list[_Entry | None] = [None] * len(batch)
        notes["prepared_round"] = round_entries
        bodies = dict(encode_round(self, batch, self._encode, sink=type(self).__name__))
        taken: dict[int, int] = {}
        prepared: list[_Prepared] = []
        dropped: dict[int, str] = {}
        failures = 0
        first: Exception | None = None
        for position, event in enumerate(batch):
            held = cache.get(id(event))
            if held is None or held[0] is not event:
                held = (event, [])
                cache[id(event)] = held
            live = [cached for cached in held[1] if not cached.landed]
            skip = taken.get(id(event), 0)
            taken[id(event)] = skip + 1
            if skip < len(live):
                entry = live[skip]
            else:
                entry = self._prepare(event, bodies.get(position))
                held[1].append(entry)
            round_entries[position] = entry
            if entry.drop is not None:
                dropped[position] = entry.drop
                if entry.drop == "id failure":
                    failures += 1
                    first = first if first is not None else entry.error
                continue
            prepared.append(
                _Prepared(position, entry.body, entry.group_id, entry.dedup_id, entry.size)
            )
        chunks = list(
            chunk_items(
                prepared,
                max_count=self.MAX_BATCH,
                max_bytes=self.MAX_BYTES,
                size_of=lambda item: item.size,
            )
        )
        refused = first if batch and failures == len(batch) else None
        return _Plan(chunks, dropped, refused)

    def _prepare(self, event: dict[str, object], body: str | None) -> _Entry:
        """Prepares one event occurrence's entry, or decides its drop.

        Args:
          event: The event.
          body: Its encoded body, or ``None`` when the encode step could not encode it.

        Returns:
          The entry.

        Raises:
          None. A ``BaseException`` from a caller's callable propagates.
        """
        if body is None:
            return _Entry(drop="unencodable")
        size = len(body.encode("utf-8"))
        group_id: str | None = None
        dedup_id: str | None = None
        if self.fifo:
            try:
                group_id = self._group_id(event)
                dedup_id = self._dedup_id(event)
            except Exception as err:
                return _Entry(drop="id failure", error=err)
            size += len(group_id.encode("utf-8")) + len(dedup_id.encode("utf-8"))
        if size > self.MAX_BYTES:
            return _Entry(size=size, drop="oversize")
        return _Entry(body=body, group_id=group_id, dedup_id=dedup_id, size=size)

    def _announce_oversized(self, sizes: list[int]) -> None:
        """Writes one line per event dropped for exceeding the message limit (FR-004).

        One line per event is today's shape, kept on purpose; what SPEC-058 FR-004 changed is
        only *when* the lines are written — after the count, and only by a run that settled
        (SPEC-074).

        Args:
          sizes: The costed size of each dropped event, in the order they were met.

        Returns:
          None.

        Raises:
          None.
        """
        for size in sizes:
            _diag.lost(
                "event",
                1,
                f"SQSSink, {size} bytes exceeds the {self.MAX_BYTES}-byte message limit",
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

    def _send(self, prepared: list[_Prepared], ids: list[str]) -> Called[KeyedReply]:
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


def _hold(chunk: list[_Prepared], blocked: set[str], sends: list[int], limit: int) -> list[bool]:
    """Decides, at send time, which entries of a FIFO chunk wait behind their group.

    SQS documents ordering within a message group as a FIFO guarantee, and 1.x kept it for as
    long as its loop retried a chunk, ``max_retries + 1`` sends. So an entry is held when its
    group is ``blocked`` — an earlier message of it, sent in an earlier chunk of this round or
    held itself, has neither landed nor used its sends — or when an earlier entry of its group in
    this chunk was already sent in an earlier round and this send will not be its last. Entries
    of a group never sent before travel together in one chunk, as 1.x sent them, so a healthy
    one-group batch is one call. A held entry blocks its group for the rest of the round. This is
    the one function deciding a hold, so another reading of it is a change here alone.

    Args:
      chunk: The chunk's entries, in batch order.
      blocked: Groups blocked so far this round, updated in place.
      sends: Each entry's sends so far this run.
      limit: ``max_retries + 1``.

    Returns:
      Per entry, whether it is held.

    Raises:
      None.
    """
    held: list[bool] = []
    for item, count in zip(chunk, sends, strict=True):
        group = item.group_id or ""
        if group in blocked:
            held.append(True)
            continue
        held.append(False)
        if count and count + 1 < limit:
            blocked.add(group)
    return held


def _block(
    sent: list[_Prepared], fates: list[Outcome], sends: list[int], blocked: set[str], limit: int
) -> None:
    """Blocks the group of every sent entry that neither landed nor used its sends.

    Args:
      sent: The entries a call carried.
      fates: The round's outcomes, by position.
      sends: Each sent entry's sends so far this run, this call's included.
      blocked: Groups blocked so far this round, updated in place.
      limit: ``max_retries + 1``.

    Returns:
      None.

    Raises:
      None.
    """
    for item, count in zip(sent, sends, strict=True):
        if fates[item.position] is not Outcome.ACCEPTED and count < limit:
            blocked.add(item.group_id or "")
