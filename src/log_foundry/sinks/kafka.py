"""KafkaSink — produce events to a Kafka topic (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._args import refuse_with_injected
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks._encode import encode_events
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._guard import ClosedGuard, GuardMode
from log_foundry.sinks._retry import usable_timeout
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["KafkaSink"]


def _usable_timeout(value: float) -> float:
    """Returns a flush timeout that is actually a bound, or the default.

    ``0`` is the value that switched this sink's exit delivery off, and ``inf`` is the unbounded
    wait FR-006 exists to remove, so neither is accepted from a caller. The rule itself lives in
    :func:`~log_foundry.sinks._retry.usable_timeout`, shared with ``NATSSink``'s publish budget
    since SPEC-047; this wrapper binds it to :data:`DEFAULT_FLUSH_TIMEOUT`.

    Args:
      value: The caller's timeout.

    Returns:
      The value if it bounds anything, else :data:`DEFAULT_FLUSH_TIMEOUT`.

    Raises:
      None.
    """
    return usable_timeout(value, DEFAULT_FLUSH_TIMEOUT)

DEFAULT_FLUSH_TIMEOUT = 10.0
"""Seconds :meth:`KafkaSink.close` waits for the producer to drain (SPEC-038 FR-006).

Ten rather than ``message.timeout.ms``'s five minutes.

It is spent **beside** ``shutdown()``'s budget, not from it: ``Worker.shutdown`` joins the drain
thread against its deadline and *then* closes the sink inline, with no remaining-budget argument
(arch §13, since ``Sink.close`` takes no timeout). Measured, ``shutdown(timeout=2.0)`` against a
dead broker takes 10.01 s. Ten seconds is chosen to keep that overrun small rather than to fit
inside a budget it cannot see — five minutes is what it replaces.
"""


class KafkaSink:
    """A :class:`~log_foundry.sinks.base.Sink` that produces events to a Kafka topic.

    This is a durable-buffer sink built on ``confluent-kafka``, the optional ``kafka`` extra,
    imported lazily. ``produce()`` enqueues locally into the producer's internal batch and
    returns without blocking, while the delivery result arrives asynchronously on a callback
    serviced by ``poll()`` and ``flush()``.

    **Retry (SPEC-041 FR-004).** This sink adds none and needs none: ``produce()`` is a local
    hand-off that returns without waiting — measured at 0.0001 s for three messages against a
    dead broker — so it never holds the worker's single drain thread, and librdkafka's own retry
    is bounded by ``message.timeout.ms`` (five minutes by default). Measured: with that set to
    1500 ms the delivery callback fired at 2.01 s, and at 4000 ms it fired at 4.01 s. SPEC-027's
    "cut short by a shutdown" governs a wait *between attempts*, and this sink has none to cut —
    which is also why it deliberately exposes no ``log_foundry_stop_signal`` attribute, since
    ``_lifecycle.offer_stop_signal`` probes by ``hasattr`` and that absence is the opt-out.

    **The bound is ``librdkafka``'s, and since SPEC-047 FR-003 it is reachable.** ``message.
    timeout.ms`` is what decides how long an accepted message is retried, five minutes by
    default — measured, the delivery callback fires at 300.18 s — and before ``producer_config``
    nothing could change it without abandoning ``bootstrap_servers=`` and injecting a whole
    producer. No log-foundry retry was added instead: ``produce()`` having returned means the
    producer owns delivery, so re-sending duplicates whatever it eventually lands (SPEC-018), and
    a second bound over a five-minute one multiplies the worst case rather than shortening it.

    The worst case (SPEC-027 FR-005) is not a retry loop — this sink has none — but its
    ``close()``: one ``flush_timeout`` wait, 10 s at the default, spent **beside**
    ``shutdown()``'s budget rather than from it. ``Worker.shutdown`` joins the drain thread
    against its deadline and *then* closes the sink inline with no remaining-budget argument
    (arch §13), so the two add up: measured, ``shutdown(timeout=2.0)`` against a dead broker
    takes 10.01 s.

    Attributes:
      flush_timeout: Seconds :meth:`close` waits for the producer to drain.
      failed: Messages whose delivery callback reported an error, plus any still queued when
        that close flush expired — the count ``flush()`` returns — plus those an ``emit`` handed
        to the producer while the close's release overtook it (SPEC-076 FR-003).
      rejected: Messages ``produce()`` itself refused — a full local queue, a serialization fault
        — which never reached the producer's batch at all. Counted only when :meth:`emit`
        returns, so only when the rest of the batch was handed off: a batch refused whole raises
        and is counted where the raise is caught (SPEC-059 FR-003).

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport
    lock. ``confluent-kafka`` documents its ``Producer`` as thread-safe, and this sink adds no state
    of its own to guard — ``produce()`` is a local hand-off into the client's internal queue.

    It refuses an emit after :meth:`close` (SPEC-032 FR-001), which is not the housekeeping it
    looks like: ``flush()`` is the only thing that drains the producer's local batch and services
    its delivery callbacks, so a message produced after it would die with the process —
    accepted, uncounted, and never the subject of a callback. ``emit`` returning normally is
    what made that silent, since the worker then never retried and ``flush()`` reported
    success. The close itself is guarded the same way (SPEC-076 FR-003): an ``emit`` still
    producing when a close begins is waited for, for at most ``CLOSE_OVERLAP_WAIT``, and one
    the release overtook anyway counts what it produced in ``failed``, with a line.
    """

    def __init__(
        self,
        topic: str,
        *,
        producer: Any = None,
        bootstrap_servers: str | None = None,
        key_field: str = "trace_id",
        flush_timeout: float = DEFAULT_FLUSH_TIMEOUT,
        producer_config: dict[str, object] | None = None,
    ) -> None:
        """Binds the sink to a topic and a producer.

        Args:
          topic: The topic to produce to.
          producer: A ``confluent-kafka``-shaped producer, or ``None`` to build one.
          bootstrap_servers: The broker list, required when no producer is injected.
          key_field: The event key used as the message key, or empty for no key.
          flush_timeout: Seconds :meth:`close` waits for the producer to drain before counting
            what is left as lost (SPEC-038 FR-006). Unbounded, an unreachable broker held
            process exit for ``message.timeout.ms`` — five minutes by default — despite
            ``shutdown(timeout=30)``. Non-positive or non-finite values fall back to the
            default, as ``Worker._emit`` floors its own retries (SPEC-021): ``0`` reinstates
            exactly the ``flush(0)`` that switched this sink's exit delivery off, and ``inf``
            reinstates the unbounded wait.
          producer_config: ``librdkafka`` configuration merged **beneath** this sink's own keys
            when it builds the producer (SPEC-047 FR-003). It is the route to the bound that
            actually governs delivery here: this sink adds no retry — ``produce()`` is a local
            hand-off, measured at 0.0001 s for three messages against a dead broker — and
            ``librdkafka`` retries on its own thread within ``message.timeout.ms``, whose
            five-minute default was measured at a 300.18 s delivery callback. Before this
            argument nothing reached it, so a caller working to a shorter deadline could not fit
            the sink inside one. The default is deliberately left alone: lowering it would drop
            messages that a process surviving a two-minute broker outage delivers today, which
            is the durable-buffer role ``architecture.md`` section 8 gives this sink.

        Returns:
          None.

        Raises:
          ValueError: If no producer is injected and no broker list was given, or if
            ``producer_config`` or ``bootstrap_servers`` is passed alongside ``producer=`` —
            either can only be applied to a producer this sink builds, and ignoring it would
            silently discard what the caller configured (SPEC-043's rule;
            ``bootstrap_servers`` since SPEC-071 FR-003).
          ImportError: If the ``kafka`` extra is not installed.
        """
        refuse_with_injected(
            "KafkaSink",
            "producer",
            producer,
            {"bootstrap_servers": bootstrap_servers, "producer_config": producer_config},
        )
        if producer is None:
            if bootstrap_servers is None:
                raise ValueError(
                    "KafkaSink requires bootstrap_servers when no producer is injected"
                )
            producer = import_extra("confluent_kafka", "kafka").Producer(
                {**(producer_config or {}), "bootstrap.servers": bootstrap_servers}
            )
        self.topic = topic
        self.producer = producer
        self.key_field = key_field
        self.flush_timeout = _usable_timeout(flush_timeout)
        self.failed = 0
        self.rejected = 0
        self._counter_lock = threading.Lock()
        self._guard = ClosedGuard(GuardMode.CLOSE_ONLY)

    @property
    def _closed(self) -> bool:
        """Whether a close has begun: the 1.x name, read-only, answered by the guard (SPEC-076).

        A 1.x subclass reading ``self._closed`` keeps working. It reads true from the moment a
        close begins, while that close still waits, which is the guard's ``closed``; assigning it
        raises ``AttributeError``, since the guard owns the state.

        Args:
          None.

        Returns:
          The guard's ``closed``.

        Raises:
          None.
        """
        return self._guard.closed

    @property
    def _close_lock(self) -> threading.Lock:
        """The guard's lock under its 1.x name, read-only (SPEC-076 FR-005).

        In ``CLOSE_ONLY`` mode it is held by a close for its whole run and by no section, so a
        1.x subclass taking it serialises with the close only, as it did with the 1.x lock;
        assigning it raises ``AttributeError``.

        Args:
          None.

        Returns:
          The lock the close holds.

        Raises:
          None.
        """
        return self._guard._lock

    def losses(self) -> SinkLosses:
        """Reports refused and undelivered messages (SPEC-026 FR-002).

        A callback failure never makes :meth:`emit` raise, and the reason is ownership rather
        than timing — ``poll(0)`` runs inside ``emit``, so a callback for a message produced
        earlier in the same batch can and does fire before it returns. Once the producer has
        accepted a message it owns delivery, including its own retries, and re-producing it from
        here would duplicate whatever the producer eventually lands.

        Args:
          None.

        Returns:
          The counters. Refusals are reported as ``dropped``, since nothing ever left the
          process, while ``failed`` is the delivery callback's verdict on a message the producer
          did accept. A refusal is counted and announced only when ``emit`` returns (SPEC-059
          FR-003), so ``dropped`` holds the refusals of batches whose other messages were handed
          off. A batch refused whole raised instead and is counted where the raise was caught —
          the worker's ``failed_batches`` once its retries are spent, ``orphan_lost`` outside a
          span, or a ``MultiSink``'s own ``failed`` when a sibling delivered — and a retry that
          hands it off counts nothing. An event :meth:`emit` could not encode is counted in the
          same ``rejected`` (SPEC-069 FR-001): it never left the process either.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=self.rejected, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Produces one message per event and serves delivery callbacks without blocking.

        ``produce()`` is a local hand-off, so what it refuses is the only failure this call can
        observe (FR-002). A refusal is counted and announced only when ``emit`` returns, through
        the deferral helper whose ``with`` encloses the produce loop and the total-failure raise
        (SPEC-059 FR-003). A partial refusal returns, so its count lands then, with one line per
        refused message naming that message's own exception type, bound as the refusal was
        decided; the messages that were accepted are already on their way, so it is not retried.
        When every message was refused the call raises and counts nothing — whoever catches the
        raise owns the batch, so a batch refused on one attempt and accepted on the next records
        no loss that never happened. A raise mid-loop (``poll``) discards the refusals too. A
        delivery callback ``poll(0)`` serves counts ``failed`` immediately, which stays an upper
        bound this does not defer.

        Every event's body and key are encoded before the first produce (SPEC-069 FR-001). One
        that cannot be encoded is dropped into ``rejected``, counted and announced only when this
        call returns, with one line per call; before, the messages ahead of it were already
        produced when its encode raised, and the worker's retry produced them again. A batch of
        nothing encodable produces nothing and returns (FR-002), and the total-refusal raise
        counts the encodable messages only.

        A closed or closing sink refuses the batch before touching the producer (SPEC-032
        FR-001). Refusing is not a loss this sink absorbed, so it moves no counter here: it
        is a failure reported to the worker, which records it in
        ``health().failed_batches``. The produce loop is a ``CLOSE_ONLY`` section of the
        guard rather than a lock held across it, which would serialize a client
        ``confluent-kafka`` documents as thread-safe (SPEC-076 FR-003). A close that lands
        while a call is producing waits for it, for at most ``CLOSE_OVERLAP_WAIT``, so the
        close's flush walks what it produced. A call the close's release overtook anyway
        is told so, and the messages it handed to the producer are counted in ``failed``
        at once, beside :meth:`_on_delivery`'s count, with one line naming the count: the
        release's flush may have delivered some of them, so it is the upper bound
        invariant 2 records for ``failed``, and nothing this call accepted goes uncounted.
        That count is made only when this call returns normally; one that raises leaves
        its whole batch to its catcher.

        Args:
          batch: The events to produce.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the sink is closed or closing, or when every message was
            refused so the batch reached nothing — the total failure the worker's retry exists
            for (SPEC-026 FR-001). Either leaves no refusal counted.
        """
        if not batch:
            return
        accepted = 0
        with self._guard.enter() as section:
            if not section:
                raise SinkDeliveryError(
                    f"KafkaSink produced none of {len(batch)} message(s): the sink is closed"
                )
            with Deferral(self) as pending:
                refused: list[str] = []
                messages = encode_events(
                    batch, self._encode, pending, sink=type(self).__name__, counter="rejected"
                )
                if not messages:
                    return
                for _, (body, key) in messages:
                    try:
                        self.producer.produce(
                            self.topic, value=body, key=key, callback=self._on_delivery
                        )
                    except Exception as err:
                        refused.append(type(err).__name__)
                        continue
                    accepted += 1
                    self.producer.poll(0)
                if refused:
                    pending.count("rejected", len(refused))
                    pending.announce(lambda: self._announce_refused(refused))
                if not accepted:
                    raise SinkDeliveryError(
                        f"KafkaSink produced none of {len(messages)} message(s)"
                    )
        if section.released_during and accepted:
            with self._counter_lock:
                self.failed += accepted
            _diag.lost("message", accepted, "KafkaSink, produced while the sink was closing")

    def _encode(self, event: dict[str, object]) -> tuple[bytes, bytes | None]:
        """Encodes one event as its message body and partition key.

        The key is encoded here with the body because ``_key``'s ``.encode("utf-8")`` can raise
        too, and an event whose key cannot be encoded is as unencodable as one whose body cannot.

        Args:
          event: The event to encode.

        Returns:
          The UTF-8 JSON body and the encoded key, or ``None`` for no key.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_events`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
          UnicodeEncodeError: If the key field holds a lone surrogate, which assembly replaces;
            the same drop.
        """
        return json.dumps(event).encode("utf-8"), self._key(event)

    def _announce_refused(self, kinds: list[str]) -> None:
        """Writes one line per message ``produce()`` refused, naming its exception type (FR-002).

        Called by the calling ``emit``'s deferral after the count is committed, outside the
        counter lock, and only when that ``emit`` returned normally (SPEC-059 FR-003).

        Args:
          kinds: Each refused message's exception type name, in the order they were refused.

        Returns:
          None.

        Raises:
          None.
        """
        for kind in kinds:
            _diag.lost("message", 1, f"KafkaSink produce, {kind}")

    def close(self) -> None:
        """Flushes the producer so buffered messages are delivered before exit (FR-002).

        Idempotent through the guard, whose lock is held *across* the flush rather than only
        around the state, as every other guarded sink holds its own (SPEC-032 FR-001).
        Releasing it early would let a second concurrent ``close()`` return while the
        first is still draining, so a caller would believe the producer was flushed when
        it was not — and ``atexit`` racing user cleanup is the documented case for a second
        call. The cost is that a second close waits on an unreachable broker, which is the
        ``architecture.md`` §13 constraint that already applies to every sink here.

        The guard is ``CLOSED`` *before* the flush rather than after, so an emit arriving
        during a flush is refused rather than producing into a batch the flush has already
        walked past; and a call still producing when the close began is waited for, for at
        most ``CLOSE_OVERLAP_WAIT``, before the flush (SPEC-076 FR-003).

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the producer raises on flush.
        """
        self._guard.close(self._flush_bounded)

    def flush(self) -> None:
        """Drains the producer's local buffer without closing the sink (SPEC-036 FR-002).

        ``emit`` hands to librdkafka and returns; only ``flush()`` drains it. Before this hook
        existed the buffer was unreachable through ``log_foundry.flush()`` — the whole point of
        that call in a process about to be frozen — and went out only at ``close()``.

        It **raises** when the producer still holds messages after ``flush_timeout``, which is what
        makes ``log_foundry.flush()`` report ``reason="sink-flush"`` rather than success. Nothing
        is counted as lost: unlike the close path those messages are still queued and the next
        flush or the close may yet deliver them, so booking them against ``failed`` would report a
        loss that has not happened. The remainder is named in the error instead.

        Refuses after ``close()`` on SPEC-032's rule — the producer has been flushed and
        released — and while one is in flight. The flush is a ``CLOSE_ONLY`` section of
        the guard, so a close that begins during it waits for it, for at most
        ``CLOSE_OVERLAP_WAIT`` (SPEC-076 FR-003).

        Args:
          None.

        Returns:
          None.

        Raises:
          SinkDeliveryError: The sink is closed, or the producer could not be drained inside
            ``flush_timeout``.
          Exception: Whatever the producer raises.
        """
        with self._guard.enter() as section:
            if not section:
                raise SinkDeliveryError("KafkaSink cannot flush: the sink is closed")
            remaining = self.producer.flush(self.flush_timeout)
        if type(remaining) is int and remaining > 0:
            raise SinkDeliveryError(
                f"KafkaSink flushed within {self.flush_timeout}s with {remaining} "
                "message(s) still queued"
            )

    def _flush_bounded(self) -> None:
        """Flushes the producer within a bound, counting whatever it could not deliver (FR-006).

        ``Producer.flush()`` with no timeout waits for ``message.timeout.ms`` — five minutes by
        default — and ``Worker.shutdown`` closes the live sink **inline and unbounded** (arch
        §13), so an unreachable broker held process exit for five minutes despite
        ``shutdown(timeout=30)``. The wait is now capped by ``flush_timeout``.

        **The stop signal is deliberately not consulted here.** A revision cut the timeout to
        zero while a shutdown was in progress, reasoning that the events were lost either way.
        They were not: ``produce()`` is a local hand-off and ``flush()`` is the only thing that
        drains the producer's batch, so with the stop event set — which it always is by the time
        ``close()`` runs, since ``Worker.shutdown`` sets it *before* the join — Kafka's exit
        delivery was switched off entirely. Measured through a real ``shutdown()``: eleven buffered
        messages — nine ``info()`` calls plus the span's start and end — ``flush(0)``, **zero
        delivered**, all eleven booked as ``failed``. That is worse
        than the hang this bound exists to fix, and it is the same mistake FR-001 AC-4a records
        for ``HTTPSink`` — skipping *work* during the exit drain, rather than skipping a *wait*.

        ``flush()`` *returns* the number of messages still queued, which is exactly the count
        lost at exit — the old call discarded it, so the loss was silent. It is counted into
        ``failed`` and announced once, with a count rather than one line per message.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the producer raises, which ``close``'s caller already handles.
        """
        remaining = self.producer.flush(self.flush_timeout)
        if type(remaining) is not int or remaining <= 0:
            return
        with self._counter_lock:
            self.failed += remaining
        _diag.lost(
            "message",
            remaining,
            f"KafkaSink, still queued when the {self.flush_timeout}s close flush expired",
        )

    def _key(self, event: dict[str, object]) -> bytes | None:
        """Derives one message's partition key from the configured field.

        Args:
          event: The event being produced.

        Returns:
          The encoded key, or ``None`` when there is no key field or no value.

        Raises:
          UnicodeEncodeError: If the value's text holds a lone surrogate, which only a direct
            ``emit`` can hand this sink; :meth:`_encode` drops that one event.
        """
        if not self.key_field:
            return None
        value = event.get(self.key_field)
        return str(value).encode("utf-8") if value is not None else None

    def _on_delivery(self, err: object, msg: object) -> None:
        """Counts and logs a delivery failure, as ``confluent-kafka``'s callback (FR-002).

        The error is a ``KafkaError`` rather than an exception, so it goes to
        :func:`~log_foundry._diag.lost` rather than ``absorbed``. Its ``str`` is a human message
        that can quote the record, so only the type and the numeric code are written — the code
        is librdkafka's own enumeration and is what makes a delivery failure diagnosable
        (SPEC-029 FR-002).

        Args:
          err: The delivery error, or ``None`` on success.
          msg: The message the callback describes, unused.

        Returns:
          None.

        Raises:
          None.
        """
        if err is not None:
            with self._counter_lock:
                self.failed += 1
            _diag.lost("message", 1, f"KafkaSink delivery, {type(err).__name__}{_code(err)}")


def _code(err: object) -> str:
    """Renders a ``KafkaError``'s numeric code for a diagnostic.

    ``code()`` returns one of librdkafka's integer constants and carries no caller data. This is
    deliberately narrow: only an ``int`` is written, so a stub or a future version returning
    something else contributes nothing rather than smuggling text past the type-name rule. The
    ``int()`` sits inside the guard for the reason ``_diag.errno_of`` puts it there —
    ``isinstance(code, int)`` admits a subclass whose ``__int__`` is Python that can raise.

    Args:
      err: The delivery error.

    Returns:
      The rendered code, or an empty string when there is none.

    Raises:
      None. This runs in a delivery callback, so an escaping exception would surface from
        ``emit`` and cost the whole batch; a diagnostic can never be the failure (FR-003).
    """
    try:
        code = getattr(err, "code", None)
        value = code() if callable(code) else None
        return f" code={int(value)}" if isinstance(value, int) else ""
    except Exception:
        return ""
