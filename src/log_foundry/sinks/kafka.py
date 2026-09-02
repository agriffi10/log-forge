"""KafkaSink — produce events to a Kafka topic (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
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
        that close flush expired — the count ``flush()`` returns.
      rejected: Messages ``produce()`` itself refused — a full local queue, a serialization fault
        — which never reached the producer's batch at all.

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport
    lock. ``confluent-kafka`` documents its ``Producer`` as thread-safe, and this sink adds no state
    of its own to guard — ``produce()`` is a local hand-off into the client's internal queue.

    It refuses an emit after :meth:`close` (SPEC-032 FR-001), which is not the housekeeping it
    looks like: ``flush()`` is the only thing that drains the producer's local batch and services
    its delivery callbacks, so a message produced after it dies with the process — accepted,
    uncounted, and never the subject of a callback. ``emit`` returning normally is what made that
    silent, since the worker then never retried and ``flush()`` reported success.
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
            ``producer_config`` is passed alongside ``producer=`` — it can only be applied to a
            producer this sink builds, and ignoring it would silently discard the caller's bound
            (SPEC-043's rule).
          ImportError: If the ``kafka`` extra is not installed.
        """
        if producer is not None:
            if producer_config is not None:
                raise ValueError(
                    "KafkaSink cannot apply producer_config to an injected producer, which is "
                    "already built; pass the settings where the producer is constructed"
                )
        else:
            if bootstrap_servers is None:
                raise ValueError(
                    "KafkaSink requires bootstrap_servers when no producer is injected"
                )
            from confluent_kafka import Producer  # type: ignore[import-not-found]

            producer = Producer(
                {**(producer_config or {}), "bootstrap.servers": bootstrap_servers}
            )
        self.topic = topic
        self.producer = producer
        self.key_field = key_field
        self.flush_timeout = _usable_timeout(flush_timeout)
        self.failed = 0
        self.rejected = 0
        self._counter_lock = threading.Lock()
        self._closed = False
        self._close_lock = threading.Lock()

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
          did accept.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=self.rejected, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Produces one message per event and serves delivery callbacks without blocking.

        ``produce()`` is a local hand-off, so what it refuses is the only failure this call can
        observe (FR-002). A partial refusal is counted and left alone, since the messages that
        were accepted are already on their way.

        A closed sink refuses the batch before touching the producer (SPEC-032 FR-001). Refusing
        is not a loss this sink absorbed, so it moves no counter here: it is a failure reported
        to the worker, which records it in ``health().failed_batches``. The check is an unlocked
        read of a write-once flag rather than a lock held across the produce loop, which would
        serialize a client ``confluent-kafka`` documents as thread-safe. A close landing between
        the check and the produce therefore still loses that message, exactly as ``MongoDBSink``
        documents for its own check; what the flag ends is the far larger case of a sink closed
        long before, which is every log written after ``shutdown()``.

        Args:
          batch: The events to produce.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the sink is closed, or when every message was refused so the
            batch reached nothing — the total failure the worker's retry exists for
            (SPEC-026 FR-001).
        """
        if not batch:
            return
        if self._closed:
            raise SinkDeliveryError(
                f"KafkaSink produced none of {len(batch)} message(s): the sink is closed"
            )
        accepted = 0
        for event in batch:
            body = json.dumps(event).encode("utf-8")
            key = self._key(event)
            try:
                self.producer.produce(self.topic, value=body, key=key, callback=self._on_delivery)
            except Exception as err:
                with self._counter_lock:
                    self.rejected += 1
                _diag.lost("message", 1, f"KafkaSink produce, {type(err).__name__}")
                continue
            accepted += 1
            self.producer.poll(0)
        if batch and not accepted:
            raise SinkDeliveryError(f"KafkaSink produced none of {len(batch)} message(s)")

    def close(self) -> None:
        """Flushes the producer so buffered messages are delivered before exit (FR-002).

        Idempotent, and the lock is held *across* the flush rather than only around the flag, as
        every other guarded sink holds its own (SPEC-032 FR-001). Releasing it early would let a
        second concurrent ``close()`` return while the first is still draining, so a caller would
        believe the producer was flushed when it was not — and ``atexit`` racing user cleanup is
        the documented case for a second call. The cost is that a second close waits on an
        unreachable broker, which is the ``architecture.md`` §13 constraint that already applies
        to every sink here.

        The flag is set *before* the flush rather than after, so an emit arriving during a flush
        is refused rather than producing into a batch the flush has already walked past.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the producer raises on flush.
        """
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
            self._flush_bounded()

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

        Refuses after ``close()`` on SPEC-032's rule — the producer has been flushed and released.

        Args:
          None.

        Returns:
          None.

        Raises:
          SinkDeliveryError: The sink is closed, or the producer could not be drained inside
            ``flush_timeout``.
          Exception: Whatever the producer raises.
        """
        if self._closed:
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
          None.
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
