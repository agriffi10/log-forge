"""KafkaSink — produce events to a Kafka topic (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from log_foundry import _diag
from log_foundry.sinks import _engine
from log_foundry.sinks._args import refuse_with_injected
from log_foundry.sinks._encode import encode_round
from log_foundry.sinks._engine import When, emit_via_deliver, run_context
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._guard import ClosedGuard, GuardMode
from log_foundry.sinks._refusal import final_refusal
from log_foundry.sinks._retry import pause_on, usable_timeout, wait
from log_foundry.sinks._rounds import Lost, mark_sent, raised_line
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    import threading

    from log_foundry.sinks.base import SinkLosses

__all__ = ["KafkaSink"]

DEFINITIVE_CODES = frozenset({21, 87})
"""Kafka protocol codes no attempt of the refused message could have escaped (SPEC-094 FR-001).

A client retries a message on its own, and a later attempt's refusal says nothing about an
earlier one that timed out ``POSSIBLY_PERSISTED``, so a report is ``REJECTED`` only when the
refusal judges what every attempt carries unchanged. 21, ``INVALID_REQUIRED_ACKS``, judges the
producer's ``acks``, which is its for life; 87, ``INVALID_RECORD``, judges the record's own key,
value and headers. Left out: 10 and 18, which judge the batch librdkafka builds again for each
attempt; 29 and 31, authorization, which can change between attempts; 2, a corruption that can be
one attempt's; and 17, which could not be shown never to follow a kept attempt. Every other error
is ``UNKNOWN``. Read from the Kafka protocol and librdkafka v2.16.0's source, not measured.
"""

_REPORT_SLICE = 0.05
"""Seconds one pass of the round's report wait flushes for, and the first slice's length."""


def _announce_each(prefix: str, kinds: list[str]) -> None:
    """Writes one ``lost 1 message(s)`` line per entry, ``<prefix>, <kind>``.

    Args:
      prefix: The line's sink and step, such as ``KafkaSink produce``.
      kinds: Each message's type name, and code where it has one.

    Returns:
      None.

    Raises:
      None.
    """
    for kind in kinds:
        _diag.lost("message", 1, f"{prefix}, {kind}")


class _Reports:
    """One round's delivery reports by position, closed to late reports once the round settles.

    A round's ``flush`` or ``poll`` can serve another round's callbacks, on another thread, so
    every write and the round's reads and close take one lock (SPEC-094 FR-001): the sink's
    counters lock, so the table adds no primitive of its own. It is **read from the sink's
    ``SinkCounters`` at each use, never kept** (invariant 12). A settled table outlives its round
    inside the callback closure librdkafka holds, which no fork repair walk reaches; the walk
    rebinds ``SinkCounters._counter_lock`` in a child, so a table that had kept the parent's lock
    object would take it for a late report the child's close flush or ``flush()`` hook serves,
    and deadlock there if a parent thread held it at the fork. No section under the lock calls
    the producer, so a callback served while it is held cannot be one this thread is waiting in.
    """

    __slots__ = ("_counters", "_reports", "_settled")

    def __init__(self, counters: SinkCounters) -> None:
        """Starts with no report.

        Args:
          counters: The sink's counters, whose lock each use reads afresh.

        Returns:
          None.

        Raises:
          None.
        """
        self._counters = counters
        self._reports: dict[int, object] = {}
        self._settled = False

    def callback(self, position: int) -> Any:
        """Returns the delivery callback for the message at ``position``.

        Args:
          position: The message's position in the round's batch.

        Returns:
          The callback.

        Raises:
          None.
        """

        def report(err: object, msg: object) -> None:
            """Records one report; one arriving after the round settled changes nothing.

            Args:
              err: The delivery error, or ``None``.
              msg: The message, unused.

            Returns:
              None.

            Raises:
              None.
            """
            with self._counters._counter_lock:
                if not self._settled:
                    self._reports[position] = err

        return report

    def count(self) -> int:
        """How many reports have arrived.

        Args:
          None.

        Returns:
          The count.

        Raises:
          None.
        """
        with self._counters._counter_lock:
            return len(self._reports)

    def has_all(self, positions: list[int]) -> bool:
        """Whether every one of ``positions`` has a report.

        Args:
          positions: The positions produced this round.

        Returns:
          The answer.

        Raises:
          None.
        """
        with self._counters._counter_lock:
            return all(position in self._reports for position in positions)

    def settle(self) -> dict[int, object]:
        """Closes the table to late reports and returns what it holds.

        Args:
          None.

        Returns:
          Each reported position's error, ``None`` for success.

        Raises:
          None.
        """
        with self._counters._counter_lock:
            self._settled = True
            return dict(self._reports)


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
"""Seconds a round waits for its delivery reports, and :meth:`KafkaSink.close` for the producer.

Ten rather than ``message.timeout.ms``'s five minutes (SPEC-038 FR-006); since SPEC-094 FR-001 it
also bounds each round's wait for its own reports.

It is spent **beside** ``shutdown()``'s budget, not from it: ``Worker.shutdown`` joins the drain
thread against its deadline and *then* closes the sink inline, with no remaining-budget argument
(arch §13, since ``Sink.close`` takes no timeout). Measured, ``shutdown(timeout=2.0)`` against a
dead broker takes 10.01 s. Ten seconds is chosen to keep that overrun small rather than to fit
inside a budget it cannot see — five minutes is what it replaces.
"""


class KafkaSink:
    """A :class:`~log_foundry.sinks.base.Sink` that produces events to a Kafka topic.

    This is a durable-buffer sink built on ``confluent-kafka``, the optional ``kafka`` extra,
    imported lazily. ``produce()`` enqueues locally into the producer's internal batch, and the
    delivery result arrives on a callback served by ``poll()`` and ``flush()``.

    **It waits for the report (SPEC-094 FR-001; Andrew's decision B, 2026-11-15, and "Truthy,
    counted", 2026-11-16).** It is a delivery-engine sink: each round hands every remaining
    message to the producer and then waits, within ``flush_timeout`` and until the stop signal,
    for the reports on that round's own messages, asking ``flush(slice)`` in slices of
    :data:`_REPORT_SLICE` so one message stuck on a leaderless partition costs a later round one
    slice rather than the whole timeout. ``flush`` holds librdkafka's flushing mark while it
    runs, so ``linger.ms`` does not hold back a round. A success is ``ACCEPTED``; an error in
    :data:`DEFINITIVE_CODES` is ``REJECTED``, so a batch of nothing else fails and ``flush()``
    reads false; any other error, and no report by the deadline or the stop, is ``UNKNOWN``,
    counted in ``failed`` with a may-have-landed line and never sent again, with ``flush()``
    still truthy. A message without a report is ``ACCEPTED`` when ``flush`` read the ``int`` 0,
    meaning nothing waits, which is how ``delivery.report.only.error`` reads. Nothing is counted
    after the round's verdict: a report served later changes nothing. It exposes
    ``log_foundry_stop_signal``, which the engine's wait between rounds and the report wait
    both read; once the wait's first slice has passed, a set signal ends it. A pause between two
    asks is never cut short by a signal already set when it began (``_retry.pause_on``), so a
    round under a set stop does not ask in a tight loop. The sink has no ``max_retries``: through
    the worker a run has the worker's rounds, the 1.x total of attempts.

    **The bound is ``librdkafka``'s, and since SPEC-047 FR-003 it is reachable.** ``message.
    timeout.ms`` is what decides how long an accepted message is retried, five minutes by
    default — measured, the delivery callback fires at 300.18 s — and before ``producer_config``
    nothing could change it without abandoning ``bootstrap_servers=`` and injecting a whole
    producer. No log-foundry retry was added instead: ``produce()`` having returned means the
    producer owns delivery, so re-sending duplicates whatever it eventually lands (SPEC-018), and
    a second bound over a five-minute one multiplies the worst case rather than shortening it.

    The worst case (SPEC-027 FR-005) is a round against a broker that never answers, one
    ``flush_timeout`` wait, unless a shutdown cuts it, and an orphan log outside a span holds the
    caller's thread for the same wait; and its ``close()``: one ``flush_timeout`` wait, 10 s at
    the default, spent **beside** ``shutdown()``'s budget rather than from it. ``Worker.shutdown`` joins the drain thread
    against its deadline and *then* closes the sink inline with no remaining-budget argument
    (arch §13), so the two add up: measured, ``shutdown(timeout=2.0)`` against a dead broker
    takes 10.01 s.

    Attributes:
      flush_timeout: Seconds a round waits for its reports, and :meth:`close` for the producer.
      log_foundry_counters: The engine's event totals, which :meth:`losses` reports.
      log_foundry_stop_signal: The sink's own stop signal, set by every stop and cleared by a
        ``configure()`` that installs the sink again (SPEC-098), or ``None``.
      failed: Messages a definitive report refused or no definitive report settled, once per
        run, the engine's ``failed_events``; assignable.
      rejected: Messages ``produce()`` itself refused — a full local queue, a serialization
        fault — beside a hand-off that worked, and events that could not be encoded, the
        engine's ``dropped_events``, counted only when the run settles; assignable. A round where
        nothing was produced is resent by the engine instead.

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport
    lock. ``confluent-kafka`` documents its ``Producer`` as thread-safe, and this sink adds no state
    of its own to guard — ``produce()`` is a local hand-off into the client's internal queue.

    It refuses an emit after :meth:`close` (SPEC-032 FR-001), which is not the housekeeping it
    looks like: ``flush()`` is the only thing that drains the producer's local batch and services
    its delivery callbacks, so a message produced after it would die with the process —
    accepted, uncounted, and never the subject of a callback. ``emit`` returning normally is
    what made that silent, since the worker then never retried and ``flush()`` reported
    success. The close itself is guarded the same way (SPEC-076 FR-003): a round still producing
    when a close begins is waited for, for at most ``CLOSE_OVERLAP_WAIT``. Every message a round
    produced was settled by that round, so the close counts nothing.
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
          flush_timeout: Seconds a round waits for its own delivery reports (SPEC-094 FR-001),
            and :meth:`close` for the producer to drain (SPEC-038 FR-006). Unbounded, an unreachable broker held
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
        self.log_foundry_counters = SinkCounters()
        self.log_foundry_stop_signal: threading.Event | None = None
        self._guard = ClosedGuard(GuardMode.CLOSE_ONLY)

    @property
    def _counter_lock(self) -> threading.Lock:
        """The counters' lock.

        Args:
          None.

        Returns:
          The lock.

        Raises:
          None.
        """
        return self.log_foundry_counters._counter_lock

    @property
    def failed(self) -> int:
        """Messages refused or unconfirmed, the engine's ``failed_events``.

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
        """Sets the count.

        Args:
          value: The new count.

        Returns:
          None.

        Raises:
          None.
        """
        with self._counter_lock:
            self.log_foundry_counters.failed_events = value

    @property
    def rejected(self) -> int:
        """Messages that never left the process, the engine's ``dropped_events``.

        Args:
          None.

        Returns:
          The count.

        Raises:
          None.
        """
        return self.log_foundry_counters.dropped_events

    @rejected.setter
    def rejected(self, value: int) -> None:
        """Sets the count.

        Args:
          value: The new count.

        Returns:
          None.

        Raises:
          None.
        """
        with self._counter_lock:
            self.log_foundry_counters.dropped_events = value

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
        """Reports the engine's totals (SPEC-094 FR-001).

        Args:
          None.

        Returns:
          The counters: ``rejected`` as ``dropped``, since nothing left the process, a refusal
          counted and announced only when ``emit`` returns — on the engine, only when the run
          settles — and
          ``failed``, each message a definitive report refused or no definitive report settled,
          counted once per run when its round settles, never later.

        Raises:
          None.
        """
        return self.log_foundry_counters.losses()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers the batch through the engine inline, the 1.x entry point (SPEC-094 FR-001).

        A failed run's events are counted in ``failed`` before it raises, as every engine
        sink's adapter does: a run fails when nothing was ``ACCEPTED``, something was
        ``REJECTED`` and nothing is ``UNKNOWN``.

        Args:
          batch: The events to produce.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the run failed, the sink is closed among them.
        """
        emit_via_deliver(self, batch)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-094 FR-001).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          Exception: Within a run, whatever the round's ``poll`` or ``flush`` raises, which the
            run absorbs as the round's failure; a round run on its own absorbs it here.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Produces every remaining message, then waits for the round's delivery reports.

        A closed or closing sink refuses with 1.x's error. Every event is encoded once per run,
        an unencodable one ``DROPPED`` with SPEC-069's line. Each encodable event is produced with
        a callback bound to this round's report table, ``poll(0)`` after each. A ``produce()``
        that raises beside one that worked is ``DROPPED``, counted in ``rejected`` and named,
        ``KafkaSink produce, <Type>``, only when the run settles; in a round where nothing was
        produced, every refused message is ``RETRYABLE`` and the engine resends it after its
        wait. A refusal or an unencodable event is so counted and announced only when ``emit``
        returns — on the engine, only when the run settles. Then :meth:`_await_reports` waits, and each produced message takes its outcome:
        a report with no error ``ACCEPTED``; one in :data:`DEFINITIVE_CODES` ``REJECTED``, one
        line each, ``KafkaSink delivery, <Type> code=N``; any other ``UNKNOWN``; no report
        ``ACCEPTED`` when the queue read empty and ``UNKNOWN`` otherwise. The ``UNKNOWN``
        messages share one ``ALWAYS`` line. The table is then closed, under the lock every write
        takes, so a report a later ``flush``, ``poll`` or ``close()`` serves, on any thread,
        changes nothing.

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          Exception: Whatever the producer's ``poll`` or ``flush`` raises, which the engine
            absorbs as the round's failure; a ``produce`` that raises is read as a refusal.
        """
        with self._guard.enter() as section:
            if not section:
                run_context().refuse(
                    final_refusal(
                        self,
                        f"KafkaSink produced none of {len(batch)} message(s): the sink is closed",
                    )
                )
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            context = run_context()
            fates = [Outcome.DROPPED] * len(batch)
            name = _diag.type_name(self)
            reports = _Reports(self.log_foundry_counters)
            produced: list[int] = []
            refused: list[tuple[int, str]] = []
            for position, (body, key) in encode_round(self, batch, self._encode, sink=name):
                mark_sent(self, [batch[position]])
                try:
                    self.producer.produce(
                        self.topic, value=body, key=key, callback=reports.callback(position)
                    )
                except Exception as err:
                    refused.append((position, _diag.type_name(err)))
                    continue
                produced.append(position)
                self.producer.poll(0)
            lost = Lost(self)
            if refused and not produced:
                for position, kind in refused:
                    fates[position] = Outcome.RETRYABLE
                    lost.add(
                        [batch[position]],
                        when=When.LAST,
                        key=kind,
                        line=raised_line("message", "KafkaSink produce", kind),
                    )
                lost.register()
                return Verdict(outcomes=tuple(fates))
            kinds = [kind for _, kind in refused]
            if kinds:
                context.announce(
                    lambda: _announce_each("KafkaSink produce", kinds), when=When.SETTLED
                )
            if not produced:
                return Verdict(outcomes=tuple(fates))
            drained = self._await_reports(reports, produced)
            seen = reports.settle()
            definitive: list[str] = []
            unknown = 0
            for position in produced:
                if position not in seen:
                    fates[position] = Outcome.ACCEPTED if drained else Outcome.UNKNOWN
                    unknown += 0 if drained else 1
                    continue
                report = seen[position]
                if report is None:
                    fates[position] = Outcome.ACCEPTED
                elif _is_definitive(report):
                    fates[position] = Outcome.REJECTED
                    definitive.append(f"{_diag.type_name(report)}{_code(report)}")
                else:
                    fates[position] = Outcome.UNKNOWN
                    unknown += 1
            if definitive:
                context.announce(
                    lambda: _announce_each("KafkaSink delivery", definitive), when=When.ALWAYS
                )
            if unknown:
                timeout = self.flush_timeout
                context.announce(
                    lambda: _diag.lost(
                        "message",
                        unknown,
                        f"KafkaSink, no definitive delivery report within {timeout}s; may have "
                        "landed, not retried",
                    ),
                    when=When.ALWAYS,
                )
        return Verdict(outcomes=tuple(fates))

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

    def close(self) -> None:
        """Flushes the producer so buffered messages are delivered before exit (FR-002).

        Every message a round produced was settled by that round (SPEC-094 FR-001), so the close
        counts nothing; its bounded flush still gives a message a round left ``UNKNOWN`` its
        chance to land.

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
        """Serves any pending delivery report and returns, without closing the sink (SPEC-036).

        It calls ``producer.flush(0)``, which serves pending reports and returns at once, and it
        does not raise for a remainder (SPEC-094 FR-001): every message a remainder holds was
        handed over by a round that waited for its report and already counted it. 1.x waited
        ``flush_timeout`` and raised for a remainder, which made ``log_foundry.flush()`` falsy
        (``reason="sink-flush"``) over an indefinite report and, at the defaults, overran its 5 s
        budget; Andrew's "Truthy, counted" answer of 2026-11-16 rules that out.

        Refuses after ``close()`` on SPEC-032's rule — the producer has been flushed and
        released — and while one is in flight. The flush is a ``CLOSE_ONLY`` section of
        the guard, so a close that begins during it waits for it, for at most
        ``CLOSE_OVERLAP_WAIT`` (SPEC-076 FR-003).

        Args:
          None.

        Returns:
          None.

        Raises:
          SinkDeliveryError: The sink is closed.
          Exception: Whatever the producer raises.
        """
        with self._guard.enter() as section:
            if not section:
                raise final_refusal(self, "KafkaSink cannot flush: the sink is closed")
            self.producer.flush(0)

    def _await_reports(self, reports: _Reports, produced: list[int]) -> bool:
        """Waits until every produced position has a report, the queue drains, or time is up.

        Each pass asks ``flush(slice)``: it serves reports for at most one slice and, while it
        runs, holds the producer's flushing mark, so ``linger.ms`` does not hold back what is
        queued, and it returns with the producer's queue length. The wait ends when every
        produced position has a report, when ``flush`` returned the ``int`` 0 (a ``bool`` is not
        one, so a stand-in answering ``False`` gives no empty reading), when
        ``flush_timeout`` has passed, or when the stop signal is set and the first slice, a
        deadline at the wait's start plus :data:`_REPORT_SLICE`, has passed. A pass that served
        nothing waits out the rest of its slice through ``_retry.wait``, so a ``flush`` that
        returns at once does not spin; the pause is cut short by the stop signal unless it was
        already set when the pause began (``_retry.pause_on``). The signal's attribute is read
        once per wait, and ``is_set()`` on every pass. ``kafka`` is in the only-the-engine-waits
        gate's exclusions for this wait, which is not a retry backoff.

        Args:
          reports: The round's report table.
          produced: The positions produced this round.

        Returns:
          Whether the producer's queue read empty, so an unreported message was delivered.

        Raises:
          Exception: Whatever the producer's ``flush`` raises.
        """
        began = time.monotonic()
        deadline = began + self.flush_timeout
        first_slice_end = began + _REPORT_SLICE
        stop = self.log_foundry_stop_signal
        while True:
            seen = reports.count()
            now = time.monotonic()
            slice_start = now
            slice_ = max(0.0, min(deadline - now, _REPORT_SLICE))
            remaining = self.producer.flush(slice_)
            if reports.has_all(produced):
                return False
            if _diag.is_a(remaining, int) and not _diag.is_a(remaining, bool) and remaining == 0:
                return True
            now = time.monotonic()
            if now >= deadline or (now >= first_slice_end and stop is not None and stop.is_set()):
                return False
            if reports.count() == seen:
                wait(slice_ - (now - slice_start), pause_on(stop))

    def _flush_bounded(self) -> None:
        """Flushes the producer within a bound before the close (FR-006); it counts nothing.

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

        It counts nothing (SPEC-094 FR-001): every message was settled by the round that
        produced it, so a count here would land after its batch's verdict. 1.x's "still queued
        when the close flush expired" count went for that reason.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the producer raises, which ``close``'s caller already handles.
        """
        self.producer.flush(self.flush_timeout)

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


def _is_definitive(err: object) -> bool:
    """Reports whether a delivery error says no attempt of the message could have been written.

    The code is read as :func:`_code` reads it: ``code()`` must return a value
    ``_diag.is_a(value, int)`` admits, an ``int`` subclass included, compared as the ``int`` it
    holds; anything else, or a ``code`` that raises, is not definitive.

    Args:
      err: The delivery error.

    Returns:
      True when its code is in :data:`DEFINITIVE_CODES`.

    Raises:
      None.
    """
    try:
        code = getattr(err, "code", None)
        value = code() if callable(code) else None
        return _diag.is_a(value, int) and int(value) in DEFINITIVE_CODES
    except Exception:
        return False


def _code(err: object) -> str:
    """Renders a ``KafkaError``'s numeric code for a diagnostic.

    ``code()`` returns one of librdkafka's integer constants and carries no caller data. This is
    deliberately narrow: only an ``int`` is written, so a stub or a future version returning
    something else contributes nothing rather than smuggling text past the type-name rule. The
    ``int()`` sits inside the guard for the reason ``_diag.errno_of`` puts it there —
    ``_diag.is_a(value, int)`` admits a subclass whose ``__int__`` is Python that can raise.

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
        return f" code={int(value)}" if _diag.is_a(value, int) else ""
    except Exception:
        return ""
