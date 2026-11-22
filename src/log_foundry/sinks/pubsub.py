"""GooglePubSubSink — publish events to a Google Cloud Pub/Sub topic (arch §8, SPEC-010)."""

from __future__ import annotations

import contextlib
import json
import os
import threading
import time
from typing import TYPE_CHECKING, Any

from log_foundry import _diag
from log_foundry.sinks import _engine
from log_foundry.sinks._classify import driver_classes
from log_foundry.sinks._encode import encode_round
from log_foundry.sinks._engine import When, emit_via_deliver, run_context
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._refusal import final_refusal
from log_foundry.sinks._retry import floor_count, pause_on, usable_timeout, wait
from log_foundry.sinks._rounds import Lost, mark_sent, raised_line
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    from log_foundry.sinks.base import SinkLosses

__all__ = ["GooglePubSubSink"]


def _announce_each(prefix: str, kinds: list[str]) -> None:
    """Writes one ``lost 1 event(s)`` line per entry, ``<prefix>, <kind>``.

    Args:
      prefix: The line's sink and step.
      kinds: Each event's type name.

    Returns:
      None.

    Raises:
      None.
    """
    for kind in kinds:
        _diag.lost("event", 1, f"{prefix}, {kind}")


DEFAULT_OVERFLOW_TIMEOUT = 30.0
"""Seconds one round waits on its publish futures, under one deadline (SPEC-094 FR-002).

Thirty matches ``DEFAULT_MAX_RETRY_AFTER``: long enough that an ordinary publish settles inside
it. It bounds the **whole** wait of one round, not each future in it, which would be thirty
seconds times the batch. It equals ``DEFAULT_SHUTDOWN_TIMEOUT``, and the stop signal is what keeps
a shutdown from waiting it out: once the round's first slice has passed, a set stop ends the wait.
"""

_POLL_INTERVAL = 0.05
"""Longest single wait on one future, and the round's first slice (SPEC-027, SPEC-094).

The deadline bounds the *total*, but a client honouring a 30-second timeout on one future
blocks for all thirty regardless — measured, a shutdown 0.05 s into an emit went unnoticed for
10 s. Waiting in slices makes the stop signal effective within one slice, which is the
"interruptible" half of SPEC-027's rule; the deadline is the "bounded" half.
"""

_TAKES_NO_TIMEOUT = "a future that takes no timeout"
"""The reading of a future whose ``result`` refuses ``timeout=``, which ``close()`` never waits on."""

DEFAULT_MAX_PENDING = 1000
"""``max_pending``'s default, kept as 1.x surface; since SPEC-094 it bounds nothing.

Each round gives every future its verdict before it returns, so none awaits one between emits
for a bound to hold; the ones it left ``UNKNOWN`` are kept only for ``close()`` to wait on.
"""


class GooglePubSubSink:
    """A :class:`~log_foundry.sinks.base.Sink` that publishes events to a Pub/Sub topic.

    This is a durable-buffer sink on ``google-cloud-pubsub``, the optional ``gcp-pubsub`` extra,
    imported lazily. ``publish()`` returns a future that resolves asynchronously.

    **It waits for the report (SPEC-094 FR-002; Andrew's decision B, 2026-11-15, and "Truthy,
    counted", 2026-11-16).** It is a delivery-engine sink: each round publishes every remaining
    event, then waits on that round's futures under one deadline, ``overflow_timeout``, in
    slices of :data:`_POLL_INTERVAL`, until the stop signal once the first slice has passed. A
    result is ``ACCEPTED``. ``InvalidArgument``, a request the server refuses whole and the
    publish retry does not retry, and ``FlowControlLimitError``, set on a future the client never
    sent, are ``REJECTED``, so a batch of nothing else fails and ``flush()`` reads false. Any other
    exception, ``PermissionDenied``, ``NotFound`` and ``DeadlineExceeded`` among them, since the
    publish retry can end in one after an attempt the server kept, and no result by the deadline
    or the stop, is ``UNKNOWN``, counted in ``failed`` with one may-have-landed line and never
    sent again. Nothing is counted after a batch's verdict, and ``flush()`` has nothing to
    resolve. **``close()`` waits for the unconfirmed** (Andrew's "Wait at close (Recommended)",
    2026-11-22): the futures a round left ``UNKNOWN`` are kept, and ``close()`` waits on them up to
    ``overflow_timeout``, not cut short by the stop signal, so a publish the client is still
    sending when the process exits normally lands rather than dying with its daemon thread. That
    wait counts nothing and writes no line.

    **Retry (SPEC-041 FR-004).** The client's own retry is bounded and runs on the client's
    threads: the generated ``publish`` carries ``Retry(initial=0.1, maximum=60.0, multiplier=4,
    deadline=600.0)``. This sink's wait for a round's futures is bounded by ``overflow_timeout``
    and cut short by a shutdown, per SPEC-027; a publish that raises is read as a refused hand-off,
    resent by the engine only in a round where nothing was published. An orphan log outside a
    span holds the caller's thread for the same wait.

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport lock —
    the publisher client owns its own batching and threading.

    **This sink keeps its own closed state** rather than the closed guard the other transport
    sinks share (SPEC-076 FR-005), the sink-delivery decision's declared exception row 3. A
    publish whose hand-off finds the sink closed, read under ``_futures_lock``, is ``UNKNOWN``
    with one line and is not waited on; ``close()`` sets the flag, and takes the futures kept
    for it, in the same lock.

    Attributes:
      max_pending: Accepted and validated as 1.x surface; it bounds nothing since SPEC-094.
      overflow_timeout: Seconds one round waits on its futures, under one deadline.
      log_foundry_counters: The engine's event totals, which :meth:`losses` reports.
      log_foundry_stop_signal: The worker's shutdown event, or ``None``.
      failed: Publishes refused definitively or left unconfirmed, once per run, the engine's
        ``failed_events``; assignable.
      rejected: Publishes the client refused before any future existed, beside one that was
        published, and events that could not be encoded, the engine's ``dropped_events``, counted
        only when the run settles; assignable.
    """

    def __init__(
        self,
        topic: str,
        *,
        client: Any = None,
        max_pending: int | None = None,
        overflow_timeout: float = DEFAULT_OVERFLOW_TIMEOUT,
    ) -> None:
        """Binds the sink to a topic.

        Args:
          topic: The topic to publish to.
          client: A publisher client to borrow, or ``None`` to build one.
          max_pending: Accepted as 1.x surface, or ``None`` for :data:`DEFAULT_MAX_PENDING`. It
            has no effect since SPEC-094 FR-002, because each round gives every future its verdict
            before it returns, so none awaits one between emits; it is still validated and floored
            at one. A supplied
            value that is not an ``int`` is refused with ``TypeError`` (SPEC-066 FR-002), before
            any client is built: ``nan`` raised at every reap and ``2.5`` from the third
            outstanding future on. ``inf``, which used to mean no bound, is refused with them —
            pass a large ``int`` such as ``sys.maxsize`` for that. A ``bool`` or ``int`` subclass
            is read as the ``int`` it holds.
          overflow_timeout: Seconds one round waits on its publish futures, under one deadline
            (SPEC-094 FR-002). The wait runs on the worker's single drain thread, so it is bounded
            by SPEC-027's rule that no sink wait may be unbounded or uninterruptible. **Floored** to :data:`DEFAULT_OVERFLOW_TIMEOUT` when it cannot
            bound anything (SPEC-049 FR-002) — ``nan`` made every deadline comparison ``False``,
            so the loop that bounded the waits had no bound at all, and ``inf`` removed it
            outright. Floored rather than refused because it is an
            *existing* argument some of whose degenerate values deliver against a healthy client,
            exactly ``KafkaSink(flush_timeout=)``'s shape, and SPEC-047 floored every one of those
            through :func:`~log_foundry.sinks._retry.usable_timeout`; refusing ``inf`` here while
            flooring it there would be two rules for one value.

        Returns:
          None.

        Raises:
          TypeError: If a supplied ``max_pending`` is not an ``int`` (SPEC-066).
          ImportError: If the ``gcp-pubsub`` extra is not installed.
        """
        self.max_pending = (
            floor_count(max_pending, 1, "max_pending", "GooglePubSubSink")
            if max_pending is not None
            else DEFAULT_MAX_PENDING
        )
        if client is None:
            client = import_extra("google.cloud.pubsub_v1", "gcp-pubsub").PublisherClient()
        self.topic = topic
        self.client = client
        self.overflow_timeout = usable_timeout(overflow_timeout, DEFAULT_OVERFLOW_TIMEOUT)
        self.log_foundry_stop_signal: threading.Event | None = None
        self.log_foundry_counters = SinkCounters()
        self._futures_lock = threading.Lock()
        self._closed = False
        self._unconfirmed: list[Any] = []
        self._kept_by = os.getpid()

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
        """Publishes refused or unconfirmed, the engine's ``failed_events``.

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
        """Publishes that never left the process, the engine's ``dropped_events``.

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

    def losses(self) -> SinkLosses:
        """Reports the engine's totals (SPEC-094 FR-002).

        Args:
          None.

        Returns:
          The counters: ``rejected`` as ``dropped``, a refusal counted and announced only when
          ``emit`` returns — on the engine, only when the run settles — and ``failed``, each
          publish refused definitively or left unconfirmed, counted once per run when its round
          settles, never later.

        Raises:
          None.
        """
        return self.log_foundry_counters.losses()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers the batch through the engine inline, the 1.x entry point (SPEC-094 FR-002).

        A failed run's events are counted in ``failed`` before it raises, as every engine
        sink's adapter does.

        Args:
          batch: The events to publish.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the run failed, the sink is closed on entry among them.
        """
        emit_via_deliver(self, batch)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-094 FR-002).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``publish`` that raises is a refusal and a ``result`` that raises an
            ``Exception`` is that future's outcome; a ``BaseException`` from the client
            propagates.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Publishes every remaining event, then waits for the round's futures.

        A sink closed on entry refuses with 1.x's error. Events are encoded once per run. A
        publish that raises beside one that was published is ``DROPPED``, counted in ``rejected``
        and named, ``GooglePubSubSink refused the publish, <Type>``, counted and announced only
        when ``emit`` returns — on the engine, only when the run settles; in a round where
        nothing was published, every refused event is ``RETRYABLE`` and the engine resends it. A
        publish whose hand-off then finds the sink closed is ``UNKNOWN``, ``GooglePubSubSink
        publish unconfirmed, the sink closed mid-batch``, and is not waited on. Then
        :meth:`_await_round` reads each future: a definitive refusal ``REJECTED``, one line each,
        ``GooglePubSubSink publish refused, <Type>``; anything else unsettled ``UNKNOWN``, one
        line for the round, ``GooglePubSubSink publish unconfirmed (<first Type>); may have
        landed, not retried``.

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``publish`` that raises is a refusal and a ``result`` that raises an
            ``Exception`` is that future's outcome; a ``BaseException`` from the client
            propagates.
        """
        context = run_context()
        if self._closed:
            context.refuse(
                final_refusal(
                    self,
                    f"GooglePubSubSink published none of {len(batch)} event(s): the sink is closed",
                )
            )
            return Verdict.uniform(Outcome.REJECTED, len(batch))
        fates = [Outcome.DROPPED] * len(batch)
        name = _diag.type_name(self)
        futures: list[tuple[int, Any]] = []
        refused: list[tuple[int, str]] = []
        orphaned = 0
        for position, data in encode_round(self, batch, self._encode, sink=name):
            mark_sent(self, [batch[position]])
            try:
                future = self.client.publish(self.topic, data=data)
            except Exception as err:
                refused.append((position, _diag.type_name(err)))
                continue
            with self._futures_lock:
                closed = self._closed
            if closed:
                fates[position] = Outcome.UNKNOWN
                orphaned += 1
                continue
            futures.append((position, future))
        if orphaned:
            context.announce(
                lambda: _diag.lost(
                    "event",
                    orphaned,
                    "GooglePubSubSink publish unconfirmed, the sink closed mid-batch",
                ),
                when=When.ALWAYS,
            )
        lost = Lost(self)
        if refused and not futures and not orphaned:
            for position, kind in refused:
                fates[position] = Outcome.RETRYABLE
                lost.add(
                    [batch[position]],
                    when=When.LAST,
                    key=kind,
                    line=raised_line("event", "GooglePubSubSink publish", kind),
                )
            lost.register()
            return Verdict(outcomes=tuple(fates))
        kinds = [kind for _, kind in refused]
        if kinds:
            context.announce(
                lambda: _announce_each("GooglePubSubSink refused the publish", kinds),
                when=When.SETTLED,
            )
        definitive: list[str] = []
        unknown: list[str] = []
        published = dict(futures)
        waitable: list[Any] = []
        for position, state, kind in self._await_round(futures):
            if state == "ok":
                fates[position] = Outcome.ACCEPTED
            elif state == "definitive":
                fates[position] = Outcome.REJECTED
                definitive.append(kind)
            else:
                fates[position] = Outcome.UNKNOWN
                unknown.append(kind)
                if kind != _TAKES_NO_TIMEOUT:
                    waitable.append(published[position])
        if waitable:
            self._keep_for_close(waitable)
        if definitive:
            context.announce(
                lambda: _announce_each("GooglePubSubSink publish refused", definitive),
                when=When.ALWAYS,
            )
        if unknown:
            count = len(unknown)
            first = unknown[0]
            context.announce(
                lambda: _diag.lost(
                    "event",
                    count,
                    f"GooglePubSubSink publish unconfirmed ({first}); may have landed, not "
                    "retried",
                ),
                when=When.ALWAYS,
            )
        return Verdict(outcomes=tuple(fates))

    def _await_round(self, futures: list[tuple[int, Any]]) -> list[tuple[int, str, str]]:
        """Waits for one round's futures under one deadline, cut short by the stop signal.

        Every future is asked at least once, through ``result(timeout=slice)``. The first slice
        is a deadline, the wait's start plus :data:`_POLL_INTERVAL`: once it has passed and the
        stop signal is set, each later ask waits nothing, and a future still unresolved reads as
        unknown. A ``result`` that raises ``TimeoutError`` before its slice is over is followed by
        a pause for the rest of the slice through ``_retry.wait``, cut short by the stop signal
        unless it was already set when the pause began (``_retry.pause_on``). ``result()`` raises
        the builtin ``TimeoutError`` both for a pending future and for one whose outcome is a
        ``TimeoutError``; :func:`_resolved` tells them apart by ``done()``, so the second is
        ``UNKNOWN`` at once rather than after the deadline. A ``result`` that takes no ``timeout``
        (``TypeError``) is ``UNKNOWN`` at once. The signal's attribute is read once per wait, and
        ``is_set()`` on every ask. ``pubsub`` is in the only-the-engine-waits gate's exclusions for
        this wait, which is not a retry backoff.

        Args:
          futures: Each published event's position and future.

        Returns:
          Per future: its position, ``ok``, ``definitive`` or ``unknown``, and a type name.

        Raises:
          None. A ``publish`` that raises is a refusal and a ``result`` that raises an
            ``Exception`` is that future's outcome; a ``BaseException`` from the client
            propagates.
        """
        began = time.monotonic()
        deadline = began + self.overflow_timeout
        first_slice_end = began + _POLL_INTERVAL
        stop = self.log_foundry_stop_signal
        settled: list[tuple[int, str, str]] = []
        for position, future in futures:
            outcome: tuple[str, str] | None = None
            while outcome is None:
                now = time.monotonic()
                left = deadline - now
                stopping = now >= first_slice_end and stop is not None and stop.is_set()
                slice_ = 0.0 if stopping or left <= 0 else min(left, _POLL_INTERVAL)
                try:
                    future.result(timeout=slice_)
                except TimeoutError as err:
                    if _resolved(future):
                        outcome = ("unknown", _diag.type_name(err))
                        break
                    if stopping or left <= 0:
                        outcome = ("unknown", "no result within the bound")
                        break
                    wait(slice_ - (time.monotonic() - now), pause_on(stop))
                    continue
                except TypeError:
                    outcome = ("unknown", _TAKES_NO_TIMEOUT)
                except Exception as err:
                    kind = _diag.type_name(err)
                    outcome = ("definitive", kind) if _is_definitive(err) else ("unknown", kind)
                else:
                    outcome = ("ok", "")
            settled.append((position, *outcome))
        return settled

    def _encode(self, event: dict[str, object]) -> bytes:
        """Encodes one event as the UTF-8 JSON data of its message.

        Args:
          event: The event to encode.

        Returns:
          The message data.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_events`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return json.dumps(event).encode("utf-8")

    def flush(self) -> None:
        """Refuses when closed, and otherwise returns: nothing is in flight (SPEC-094 FR-002).

        Each round gave every future its verdict before it returned, so no publish awaits one
        between emits for this hook to resolve; the futures left ``UNKNOWN`` are waited on by
        ``close()`` alone, which counts nothing. 1.x resolved a pending list here and raised for a
        remainder, which made ``log_foundry.flush()`` falsy over an indefinite report; Andrew's
        "Truthy, counted" answer of 2026-11-16 rules that out.

        Args:
          None.

        Returns:
          None.

        Raises:
          SinkDeliveryError: The sink is closed.
        """
        if self._closed:
            raise final_refusal(self, "GooglePubSubSink cannot flush: the sink is closed")

    def _keep_for_close(self, futures: list[Any]) -> None:
        """Keeps a round's unconfirmed futures for ``close()`` to wait on (SPEC-094 FR-002).

        Under ``_futures_lock``, and only while the sink is open: a round settling after the close
        took the list keeps nothing, since no later ``close()`` waits. Each keep first drops the
        futures already kept that :func:`_waitable` says are resolved or cannot be asked, so the
        list holds only publishes still in flight and does not grow with every round a slow
        client left unconfirmed. A list kept under another pid, a parent's inherited through
        ``os.fork()``, is dropped first (invariant 12): its futures are resolved by the parent's
        client threads, which do not exist in this process.

        Args:
          futures: The round's ``UNKNOWN`` futures whose ``result`` takes a ``timeout``.

        Returns:
          None.

        Raises:
          None.
        """
        with self._futures_lock:
            if not self._closed:
                self._unconfirmed = [f for f in self._kept_here() if _waitable(f)] + futures

    def _kept_here(self) -> list[Any]:
        """The kept futures, or none when the list was kept under another pid.

        Called under ``_futures_lock``. A list inherited through ``os.fork()`` holds the
        parent's futures, which only the parent's client threads resolve, so a child reads it
        as empty and records itself as the keeper from then on (invariant 12).

        Args:
          None.

        Returns:
          The futures this process kept.

        Raises:
          None.
        """
        pid = os.getpid()
        if self._kept_by != pid:
            self._unconfirmed = []
            self._kept_by = pid
        return self._unconfirmed

    def close(self) -> None:
        """Marks the sink closed and waits for the publishes its rounds left unconfirmed.

        SPEC-094 FR-002, Andrew's "Wait at close (Recommended)", 2026-11-22. A round under a set
        stop signal reads a future still unresolved after its first slice as ``UNKNOWN``, but the
        client's own thread, a daemon in google-cloud-pubsub, is still sending it; a process
        that exits on that verdict ends the send. So the flag is set and the kept futures taken
        under ``_futures_lock``, the lock a round's hand-off reads the flag under, and each is then
        waited on outside the lock, under one deadline of ``overflow_timeout`` for them all,
        **not** cut short by the stop signal, which is already set at every exit. The wait
        counts nothing and writes no line: each future's verdict was given by its round and
        counted once there (SPEC-088's one-term ledger), and a future the wait sees resolve, or
        not, changes none of it. A future whose ``done()`` raises, or whose ``result`` refuses
        ``timeout=``, is not waited on, as its round reads it. A second ``close()`` finds the list
        empty and waits on nothing; a publish whose hand-off lands after the close is read as
        unconfirmed by its round and never kept. A forked child's ``close()``, a caller's own
        included, waits on nothing its parent kept (invariant 12): no thread in the child will
        ever resolve those futures, so waiting on them would only cost ``overflow_timeout``.

        Args:
          None.

        Returns:
          None.

        Raises:
          None. An ``Exception`` from a kept future is its outcome, already given; a
            ``BaseException`` propagates.
        """
        with self._futures_lock:
            self._closed = True
            kept, self._unconfirmed = self._kept_here(), []
        deadline = time.monotonic() + self.overflow_timeout
        for future in kept:
            if not _waitable(future):
                continue
            with contextlib.suppress(Exception):
                future.result(timeout=max(0.0, deadline - time.monotonic()))


def _resolved(future: Any) -> bool:
    """Whether a future has resolved, so a ``TimeoutError`` from it is its own outcome.

    Args:
      future: The publish future.

    Returns:
      True when its ``done()`` answers true; False when it has none or the call fails.

    Raises:
      None.
    """
    done = getattr(future, "done", None)
    try:
        return callable(done) and bool(done())
    except Exception:
        return False


def _waitable(future: Any) -> bool:
    """Whether ``close()`` should wait on a kept future: unresolved, and its ``done()`` answers.

    A future with no callable ``done`` is waitable, since its round asked it through ``result``
    alone. One whose ``done()`` raises is not, so a future that cannot say whether it is
    resolved is neither kept past the next round nor waited on at the close.

    Args:
      future: The publish future.

    Returns:
      False when its ``done()`` answers true or raises; True otherwise.

    Raises:
      None.
    """
    done = getattr(future, "done", None)
    if not callable(done):
        return True
    try:
        return not bool(done())
    except Exception:
        return False


def _is_definitive(err: Exception) -> bool:
    """Reports whether a future's exception says no attempt of the publish could have been kept.

    ``google.api_core.exceptions.InvalidArgument``: the server refuses a publish request whole and
    the publish retry does not retry it, so no attempt it follows was kept; it lands on every
    message of the client's batch, not only the one at fault. ``FlowControlLimitError``: set on a
    future the client never sent. Both are resolved through ``driver_classes``, importing nothing.
    That a refused request is applied to none of its messages is an inference from the API's
    meaning, not a measurement.

    Args:
      err: The exception ``result()`` raised.

    Returns:
      True for ``InvalidArgument`` and the publisher's ``FlowControlLimitError``.

    Raises:
      None.
    """
    definitive = (
        *driver_classes("google.api_core.exceptions", "InvalidArgument"),
        *driver_classes("google.cloud.pubsub_v1.publisher.exceptions", "FlowControlLimitError"),
    )
    return _diag.is_a(err, definitive)
