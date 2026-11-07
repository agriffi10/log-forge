"""NATSSink — publish events to a NATS subject, optionally via JetStream (arch §8, SPEC-010)."""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from typing import TYPE_CHECKING, Any, TypeVar

from log_foundry import _diag
from log_foundry.sanitize import _int_text
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._retry import exact_int, usable_timeout
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

if TYPE_CHECKING:
    import concurrent.futures
    from collections.abc import Coroutine

__all__ = ["NATSSink"]

T = TypeVar("T")

DEFAULT_PUBLISH_TIMEOUT = 10.0
"""Seconds one whole :meth:`NATSSink.emit` may spend publishing (SPEC-047 FR-001).

A budget for the **batch**, not for an event. Under JetStream each publish awaits an ack bounded by
the driver's own timeout, and applying that per item is ``n x timeout``, which is not a bound --
measured, five events against a stalled server cost 25.01 s, and ``Worker._final_drain`` hands the
process's exit backlog over as a single batch (SPEC-038 measured 5,980 events).

It is spent inside ``_lifecycle.DEFAULT_SHUTDOWN_TIMEOUT`` (30.0): ``Worker.shutdown`` joins the
drain thread against that deadline and ``_final_drain``'s single emit runs on that thread, and an
expired join leaves the sink **open** by SPEC-027 FR-004, so ``close()`` never drains the client's
outbound buffer.

**The exit drain fits the join, and the arithmetic is stated rather than assumed.**
``Worker._emit`` retries a failing batch up to ``max_retries + 1`` times -- four at the default --
and its inter-attempt wait returns immediately once the stop event is set, which it is for the whole
of the exit drain. This deadline deliberately does not consult that event (a shutdown shortens a
wait and never skips work), so on its own the sequence would cost ``(max_retries + 1) x
publish_timeout``, 40 s against a 30 s join. Since SPEC-062 the worker declines a retry predicted to
end after the ``shutdown()`` deadline, so the exit drain's worst case is bounded by the
``shutdown()`` budget plus up to two ``publish_timeout``s -- the emit in flight when the budget
runs out, and the first attempt of a tail batch, which is never declined -- and at these defaults
the third attempt is declined at 20 s and ``close()`` runs. That rests on one emit costing at most
``publish_timeout``, which SPEC-061 keeps: its liveness and confirmation round trips and each
publish are waited under the emit's one deadline. The only overrun is :data:`CALL_SLACK`, the
backstop for a loop a driver has wedged, and it fires once: the sink then closes itself, so every
later attempt is refused at once. Ten is chosen on
the size of the improvement: the same path was *unbounded* before this bound, 8.3 hours for a
5,980-event backlog at 5 s an event.
"""

DEFAULT_ACK_TIMEOUT = 5.0
"""Ceiling on any one JetStream publish's ack wait, in seconds (SPEC-047 FR-001).

A constant of this module's own rather than a value read from the driver: the sink builds its
context with a bare ``self._client.jetstream()``, so ``JetStreamContext``'s ``timeout`` is its
constructor default and is not readable back off the object. It mirrors that default, and every
publish is given ~~``min(DEFAULT_ACK_TIMEOUT, remaining budget)``~~ the lesser of this and the
remaining budget less :data:`DRIVER_DEADLINE_MARGIN` (:func:`_driver_timeout`, diff review frame
3) -- so a divergence can only ever make the per-publish timeout smaller than the driver would
have used, never larger.
"""

DEFAULT_CLOSE_TIMEOUT = 1.0
"""Seconds :meth:`NATSSink.close` gives its own sequence when no ``drain_timeout`` was passed.

Chosen so that this plus :data:`LOOP_JOIN_TIMEOUT` equals ``_lifecycle.DEFAULT_CLOSER_GRACE``
(2.0): a close that fits the grace is not cut by the exit, and a longer one would be. Nothing
uncounted rides on the round trip this deadline bounds, because SPEC-061 FR-003 leaves every core
batch either confirmed or counted before ``emit`` returns, so an expired close loses no event that
``losses()`` does not already report. A caller who passes a larger ``drain_timeout`` accepts a
close the exit may cut (SPEC-061 FR-004).
"""

PROBE_FLUSH_TIMEOUT = 600.0
"""Seconds the driver's own ``wait_for`` inside a probe may run before it cancels the PONG future.

The sink never cancels a PONG wait the server may still answer (SPEC-061 M3): a late PONG landing
on a cancelled future raises inside the driver's reader and ends it while the status stays
CONNECTED, after which every ``flush()`` returns without a round trip. The probe runs the driver's
public ``flush(timeout=...)``, whose only cancellation is its own inner ``wait_for``, so this value
must **exceed** ``ping_interval x (max_outstanding_pings + 1)``: the loop time by which the
driver's ping task has declared a silent connection stale and ended it, 120 s x (2 + 1) = 360 s
at the driver's defaults (M1). A PONG can only arrive on the connection its PING left on, and that
connection ends no later than 360 s into a stall, so by the time this inner bound could cancel the
future there is no PONG left that could arrive for it. An injected client built with a longer
``ping_interval`` falls outside the argument, and an injected client is never probed.
"""

CALL_SLACK = 1.0
"""Seconds the backstop adds to a bound the coroutine already enforces on itself (SPEC-061 FR-001).

A caller never waits on the loop without a limit. ``emit`` and ``flush`` wait ``publish_timeout``
plus this, ``close`` waits its deadline plus this, and each coroutine honours its own deadline well
inside that, so the margin only has to cover loop scheduling. When it expires the loop is treated
as wedged by a driver and is not recovered: the sink marks itself closed, stops the loop and joins
the thread under :data:`LOOP_JOIN_TIMEOUT`, because a hung application thread is the one outcome
this library must never produce and a duplicate publish is the safer error beside it.
"""

LOOP_JOIN_TIMEOUT = 1.0
"""Seconds :meth:`NATSSink.close` and the backstop wait for the loop thread to end (SPEC-061).

Enough for the cancellation callbacks the close scheduled to run and the loop to stop, and no
more: the close coroutine has already waited for the client under its own deadline, so a join
that waited longer would defeat FR-004's bound. A thread still alive when the join expires is
abandoned as a daemon and its loop is left unclosed, since a running loop cannot be closed. The
stated worst case for a close is its deadline plus :data:`CALL_SLACK` plus this.
"""

PENDING_POLL = 0.005
"""Seconds between reads of the driver's ``pending_data_size`` while a round trip waits for zero.

The driver writes a PING straight to the transport while publishes wait in its pending buffer
for the flusher, so a PING sent while that buffer is non-empty overtakes the publishes and its
PONG confirms nothing about them (SPEC-061, review finding 1: reproduced on CPython 3.12.12, the
server had parsed 133 of 400 publishes when it answered). Before a confirming round trip the sink
therefore waits, inside its deadline, until the counter reads zero, polling at this step on a
timed yield rather than a busy spin. Small enough that the wait adds at most this to a healthy
round trip; large enough that a 10 s budget is a few thousand reads, not a spin.
"""

DRIVER_DEADLINE_MARGIN = 0.05
"""Seconds by which a timeout the sink hands the driver ends before the sink's own wait on it.

**The one timing rule every wait in this module follows, stated here and applied through
:func:`_driver_timeout`.** When the sink waits on a driver call that carries a timeout of its own,
and the driver's expiry is meant to be *reported* -- a JetStream ack wait raises, the publish is
counted as raised, and a batch in which nothing landed goes back to the worker for its retry --
the driver's timeout must end strictly inside the sink's wait. Two timers due at the same instant
are decided by the order they were armed, and the sink arms its ``asyncio.wait`` before the
driver's task has even started, so a driver timeout *equal* to the sink's remaining budget loses
the tie every time: the driver's raise is never seen, and the publish reads as one that never
returned (diff review frame 3, finding 1: against a real server at ``publish_timeout=1.0``, 0 of 4
ack-starved emits raised, where before SPEC-061 all did). The margin only has to cover the loop
scheduling between the two timers. Its mirror is :data:`PROBE_FLUSH_TIMEOUT`, a driver timeout
that deliberately *outlives* every sink wait, because a PONG wait must never be cancelled.

The second half of the rule is for latches: each ages from the start of the call whose budget
the stall spent, not from when the sink noticed it (:meth:`NATSSink._confirm`), so the latch holds
for the very next caller and a stall costs one caller one budget.
"""

_LOOP_THREAD_NAME = "log-foundry-nats-loop"
"""The name of the daemon thread that runs a sink's event loop for the sink's whole life."""


def _driver_timeout(remaining: float, ceiling: float) -> float:
    """Returns the timeout to give the driver for a wait the sink bounds by ``remaining``.

    The lesser of ``ceiling`` and ``remaining`` less :data:`DRIVER_DEADLINE_MARGIN`, so the
    driver's expiry lands, and is raised, before the sink's own wait gives up on the call. Below
    twice the margin the margin shrinks to half of what remains, which keeps the result positive
    and below ``remaining`` at every value: a fixed floor could not, since a floor at or above a
    tiny remainder hands the driver the tie the margin exists to avoid. A budget that small no
    longer holds a scheduling gap either, so there the ordering is likely rather than assured.

    Args:
      remaining: Seconds the sink will wait on the call.
      ceiling: The most the driver may be given, such as :data:`DEFAULT_ACK_TIMEOUT`.

    Returns:
      A timeout strictly between zero and ``remaining``, at most ``ceiling``.

    Raises:
      None.
    """
    return min(ceiling, remaining - min(DRIVER_DEADLINE_MARGIN, remaining / 2))


def _retrieve(task: asyncio.Task[Any]) -> None:
    """Retrieves a finished task's outcome so asyncio never logs it as unretrieved.

    A done callback for every task the sink creates and may not wait for: an abandoned publish
    and a probe discarded on a reconnect both end on their own, and an exception nobody read
    would otherwise be logged as "Task exception was never retrieved" at collection.

    Args:
      task: The task that finished.

    Returns:
      None.

    Raises:
      None.
    """
    if not task.cancelled():
        task.exception()


class _LoopWedgedError(TimeoutError):
    """The backstop bound expired on a coroutine that had not finished (SPEC-061 FR-001).

    A private subclass so ``emit``, ``flush`` and ``close`` can tell the backstop's expiry from a
    ``TimeoutError`` a driver raised inside the coroutine, which the future carries and which is
    not evidence of a wedged loop.
    """


class NATSSink:
    """A :class:`~log_foundry.sinks.base.Sink` that publishes events to a NATS subject.

    This is a durable-buffer sink on ``nats-py``, the optional ``nats`` extra, imported lazily.
    The driver is async, so the sink owns a private event loop ~~and drives each publish to
    completion from the synchronous ``emit``, which therefore returns only after the batch is
    handed off~~ **superseded by SPEC-061 FR-001**: the loop runs on one daemon thread the sink
    owns from construction to :meth:`close`, so the driver's reader, ping and reconnect tasks
    receive CPU between calls, and ``emit``, ``flush`` and ``close`` each submit one coroutine to
    it and wait under a bound. With JetStream enabled it publishes through JetStream for durable
    acknowledgement.

    **Retry and the worst-case delay** (SPEC-041 FR-004, SPEC-047 FR-001). This sink adds no retry
    loop and needs none: ~~a core ``publish()`` writes into the client's outbound buffer and
    returns without waiting — measured at 0.00 s for fifty publishes — so it never holds the
    worker's single drain thread and there is no backoff for a shutdown to cut short.~~
    **Superseded by SPEC-061 FR-002 and FR-003**: a core ``emit`` now waits for the server twice,
    for a round trip before its first publish and for a second one after its last, both inside
    the one ``publish_timeout``, so the hand-off is no longer free and the wait is bounded rather
    than absent. SPEC-027's guarantee is met because the wait is bounded.

    > ~~Under JetStream ``publish()`` awaits an ack bounded by the driver's own timeout (5 s by
    > default) and does not retry.~~ **Superseded by SPEC-047 FR-001.** True per *event* and false
    > per *batch*: the awaits are sequential and nothing bounded how many there were, so the cost
    > was ``n x 5 s`` on the drain thread — measured at 25.01 s for five events against a stalled
    > server, and ``Worker._final_drain`` hands the exit backlog over as one batch. One
    > :data:`DEFAULT_PUBLISH_TIMEOUT` now bounds the whole :meth:`emit`.

    The worst case is therefore ``publish_timeout`` per emit — SPEC-061's two round trips and every
    publish, a JetStream ack wait included, spend that one deadline — and across the exit drain
    the ``shutdown()`` budget plus up to two ``publish_timeout``s — the emit in flight when the
    budget runs out and the first attempt of a tail batch — since ``Worker._emit`` declines a
    retry predicted to end after that deadline (SPEC-062) and a batch's first attempt is always
    made. Before that it was ``(max_retries + 1) x publish_timeout``, which exceeded the join at
    the defaults; ``architecture.md`` section 12 records the closure.

    **A disconnected client is reported, not absorbed** (FR-004 AC-5). That non-blocking publish
    is exactly what made this sink report success for events that had not left the process: with
    the server stopped, five successive emits each returned in 0.00 s with ``losses()`` reading
    all zeros, and when the client's reconnect budget (60 attempts × 2 s by default) ran out
    first, **one of six events reached the destination with every counter still at zero**. That
    is SPEC-026 FR-001's shape — a sink the worker believes, so its retry never engages and
    ``failed_batches`` never moves. ``emit`` now refuses a batch while the client reports itself
    disconnected.

    ~~The limit is stated rather than overclaimed: ``is_connected`` does not flip the instant the
    server dies, so a batch in the window before the client notices is still accepted and still
    buffered. Measured, one of five emits landed in that window. **Corrected by SPEC-047
    FR-004:** the window is wider than that reading suggests — the flag was still ``True``
    **40 s after the server was stopped**, which is a lower bound on the window rather than the
    window, because ``nats-py`` notices through a 120 s ping interval or a failed write.~~
    **Superseded by SPEC-061 FR-002:** the flag is no longer what guards a publish. For a client
    the sink connected itself, ``emit`` sends a PING and publishes only after the server's PONG
    arrives, so a stalled server is refused within ``publish_timeout`` rather than after the
    driver's ping task notices it. When that happens is derived, not measured: the driver's own
    ping arithmetic puts it at between 240 and 360 s of loop time, and never while the loop ran
    only inside a call. What SPEC-061 measured was 75 of 75 emits accepted over 148 s of such a
    stall. The flag stays in front of the round trip as the cheap refusal. Refusing moves no
    ``losses()`` counter, per SPEC-032: it is a failure *reported* to the worker rather than one
    absorbed, and counting both would report one loss twice.

    **A publish the driver never returns from is abandoned at the deadline, counted, and latches
    the sink** (SPEC-061 FR-001). It is counted where no confirmation will judge it -- the
    JetStream path, an injected client -- and every later ``emit`` and ``flush`` is refused at
    once while it is still pending, since each would only queue behind the same blocked
    force-flush.

    **A JetStream event with no ack is retried or not depending on how the wait ended**, and the
    asymmetry is recorded rather than removed (``docs/invariants.md``, invariant 2). An ack wait
    that expires -- and every ack wait the budget caps does, inside the sink's own wait, by
    :data:`DRIVER_DEADLINE_MARGIN` -- raises in the driver, so the event is counted as a raised
    publish and a batch in which nothing landed goes back to the worker, which retries it
    (SPEC-047's behaviour); a publish the driver never returns from (a force-flush into a peer
    that stopped reading, or a driver ignoring its own timeout) is abandoned, counted as
    unconfirmed and never retried. Both mean the same thing -- no ack, and the event may have been
    persisted -- so the first can duplicate and the second can lose. What closes it is the
    ``Nats-Msg-Id`` deduplication item in ``architecture.md`` section 12, which makes a retry safe.
    """

    def __init__(
        self,
        subject: str,
        *,
        client: Any = None,
        jetstream: bool = False,
        servers: str | None = None,
        publish_timeout: float = DEFAULT_PUBLISH_TIMEOUT,
        connect_timeout: float | None = None,
        max_reconnect_attempts: int | None = None,
        reconnect_time_wait: float | None = None,
        drain_timeout: float | None = None,
    ) -> None:
        """Binds the sink to a subject, starts its loop thread, and connects if no client was given.

        The loop thread is started before the connect, so that ``nats.connect`` runs as a coroutine
        on the loop that will drive the client for its whole life (SPEC-061 M10: a client connected
        on one loop cannot be flushed from another). Construction waits for the connect with no
        bound of the sink's own: ``connect_timeout`` and ``max_reconnect_attempts`` govern it, as
        they did before SPEC-061. A connect that raises stops the thread and closes the loop before
        the driver's exception is re-raised unchanged, and so does a missing ``nats`` extra.

        Args:
          subject: The subject to publish to.
          client: A ``nats-py``-shaped client to borrow, or ``None`` to connect one. A borrowed
            client is never probed, confirmed or drained, because the sink cannot drive a loop it
            did not connect it on (SPEC-061, M10).
          jetstream: Whether to publish through JetStream.
          servers: The server URL used when connecting.
          publish_timeout: Seconds one whole :meth:`emit` may spend, floored by
            :func:`~log_foundry.sinks._retry.usable_timeout` as ``KafkaSink`` floors its flush
            (SPEC-047 FR-001). It applies to an injected ``client=`` too: it is this sink's own
            bound over its own loop, not a request made of the driver at connect time.
          connect_timeout: Seconds the driver may spend on one connection attempt, or ``None``
            to pass nothing and leave the driver's own default (SPEC-047 FR-002).
          max_reconnect_attempts: Attempts the driver makes before giving up, or ``None`` to
            pass nothing. This is what governs the *initial* connect too, which is why the
            constructor blocked for a measured 120.17 s against a dead server at the driver's
            defaults (60 attempts x 2 s) before this argument existed. **``0`` and negative
            values do not mean "give up immediately" — they block forever.** ``nats-py`` retires
            a server from its pool only under ``if max_reconnect_attempts > 0``, so a
            non-positive value never retires one and the connect loop does not terminate;
            measured, both ``0`` and ``-1`` were still blocking at 30 s and one probe of ``0``
            ran past 400 s. The smallest value that bounds anything is ``1`` (measured 2.02 s
            against a dead server).

            ~~Passed through rather than corrected here, because overriding a driver's documented
            behaviour would surprise a caller who knows it.~~ **Superseded by SPEC-049 FR-004**,
            which ``architecture.md`` §12 had already named as this item's closure: a non-positive
            value is now **refused**. Refusing is not the overriding that sentence rejected — the
            caller is told rather than quietly given different behaviour — and 1.0 is the major
            version §12 said the change was waiting for. ``reconnect_time_wait=0`` is untouched:
            it works, and is on the floor side of SPEC-049 FR-001's rule.

            A value that is not an ``int`` is refused with ``TypeError`` (SPEC-066 FR-003), before
            the positivity check, which then runs on the exact ``int``: ``2.5``, ``nan`` and
            ``inf`` passed ``<= 0`` and were forwarded to ``nats-py`` as given. A ``bool`` or
            ``int`` subclass is read as the ``int`` it holds, and that exact ``int`` is what the
            driver receives.
          reconnect_time_wait: Seconds between reconnect attempts, or ``None`` to pass nothing.
          drain_timeout: ~~Seconds ``Client.drain`` may spend, or ``None`` to pass nothing. It is
            forwarded because it does bound the driver's subscription drain, but it is **not**
            what bounds :meth:`close`: ``drain()`` ends with a ``flush()`` carrying the driver's
            own 10 s default, and that is what fired in both measured cases (10.00 s against a
            stalled server, 0.00 s against a stopped one). Recorded in ``architecture.md``
            section 12.~~ **Superseded by SPEC-061 FR-004:** this is now the deadline of the
            sink's own :meth:`close` sequence, :data:`DEFAULT_CLOSE_TIMEOUT` when ``None``, and a
            value that bounds nothing (zero, negative, ``inf``, ``NaN``) falls back to that default
            through :func:`~log_foundry.sinks._retry.usable_timeout`. The raw value is still
            forwarded to ``nats.connect`` exactly as SPEC-047 FR-002 requires, and the driver
            option it sets no longer bounds anything the sink calls, because the sink no longer
            calls ``drain()``.

        Returns:
          None.

        Raises:
          ValueError: If a connect-time argument is passed alongside ``client=``, which cannot
            consume it — SPEC-043's rule that an argument no backend can use is an error rather
            than a silent ignore. ``servers`` joins that set in SPEC-049 FR-004, closing the last
            of the §12 items; it is checked separately from ``supplied`` rather than added to it,
            because ``nats.connect(servers or …, **supplied)`` would then pass it twice.
            ``publish_timeout`` is deliberately outside the set: it is this sink's own bound and
            applies to any client. Also if ``max_reconnect_attempts`` is non-positive.
          TypeError: If ``max_reconnect_attempts`` is given and is not an ``int`` (SPEC-066),
            judged first, before the conflict with ``client=`` and before any loop is created.
          ImportError: If the ``nats`` extra is not installed.
          Exception: Whatever the driver raises when connecting.
        """
        if max_reconnect_attempts is not None:
            max_reconnect_attempts = exact_int(
                max_reconnect_attempts, "max_reconnect_attempts", "NATSSink"
            )
        if max_reconnect_attempts is not None and max_reconnect_attempts <= 0:
            raise ValueError(
                f"NATSSink max_reconnect_attempts must be positive, not "
                f"{_int_text(max_reconnect_attempts)}: nats-py retires a server from its pool "
                f"only under `max_reconnect_attempts > 0`, so a non-positive value never "
                f"terminates the connect loop (measured: 0 and -1 both still blocking at 30 s, "
                f"one probe past 400 s). 1 is the smallest value that bounds anything."
            )
        options: dict[str, object] = {
            "connect_timeout": connect_timeout,
            "max_reconnect_attempts": max_reconnect_attempts,
            "reconnect_time_wait": reconnect_time_wait,
            "drain_timeout": drain_timeout,
        }
        supplied = {name: value for name, value in options.items() if value is not None}
        conflicting = sorted([*supplied, *(["servers"] if servers is not None else [])])
        if client is not None and conflicting:
            raise ValueError(
                "NATSSink cannot apply "
                + ", ".join(conflicting)
                + " to an injected client, which is already connected; "
                "pass them where the client is built, or drop client="
            )
        self._subject = subject
        self._jetstream = jetstream
        self.publish_timeout = usable_timeout(publish_timeout, DEFAULT_PUBLISH_TIMEOUT)
        self._drain_timeout = (
            DEFAULT_CLOSE_TIMEOUT
            if drain_timeout is None
            else usable_timeout(drain_timeout, DEFAULT_CLOSE_TIMEOUT)
        )
        self.failed = 0
        self._counter_lock = threading.Lock()
        self._lock = threading.Lock()
        self._pid = os.getpid()
        self._closing = False
        self._closed = False
        self._owns_client = client is None
        self._abandoned: set[asyncio.Task[Any]] = set()
        self._stuck: tuple[asyncio.Task[Any], ...] = ()
        self._probe: tuple[asyncio.Task[Any], float, int] | None = None
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._serve, name=_LOOP_THREAD_NAME, daemon=True)
        self._thread.start()
        try:
            if client is None:
                client = self._submit(
                    import_extra("nats", "nats").connect(
                        servers or "nats://localhost:4222", **supplied
                    )
                ).result()
        except BaseException:
            self._closed = True
            self._stop_loop()
            raise
        self._client = client

    def _serve(self) -> None:
        """Runs the sink's event loop on the loop thread until :meth:`_stop_loop` stops it.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _submit(self, coro: Coroutine[Any, Any, T]) -> concurrent.futures.Future[T]:
        """Schedules one coroutine on the loop thread and returns the future to wait on.

        The single site that hands work to the loop, so ``emit``, ``flush``, ``close`` and the
        connect all reach it the same way (SPEC-061 FR-001 AC-13).

        Args:
          coro: The coroutine to run on the loop.

        Returns:
          The ``concurrent.futures.Future`` that completes with the coroutine's outcome.

        Raises:
          None.
        """
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def _run(self, coro: Coroutine[Any, Any, T], bound: float) -> T:
        """Runs one coroutine on the loop thread and waits for it under a bound (SPEC-061 FR-001).

        Called with ``self._lock`` held, so calls stay serialized across the submit and the wait
        exactly as SPEC-028 requires. The bound is a backstop over a deadline the coroutine already
        enforces on itself; **on expiry the loop is treated as wedged and is not recovered**. The
        future is deliberately not cancelled, because cancelling it would cancel the coroutine on
        the loop, and a coroutine may be inside a PONG wait the server can still answer (SPEC-061
        M3). The loop is abandoned instead: the sink marks itself closed, schedules a cancel of
        every task and a stop, and joins the thread under :data:`LOOP_JOIN_TIMEOUT`.

        A future that completed at the moment the bound expired is not a wedged loop: its result
        is returned, or its exception raised, exactly as if the wait had ended a moment sooner, so
        a ``close()`` that finished on the last tick still marks the sink closed and stops the loop.

        Args:
          coro: The coroutine to run.
          bound: Seconds to wait for it, the coroutine's own deadline plus :data:`CALL_SLACK`.

        Returns:
          Whatever the coroutine returned.

        Raises:
          _LoopWedgedError: The bound expired; the sink is now closed.
          Exception: Whatever the coroutine raised, a driver's ``TimeoutError`` included.
        """
        future = self._submit(coro)
        try:
            return future.result(bound)
        except TimeoutError as err:
            if future.done():
                return future.result()
            self._abandon_loop()
            raise _LoopWedgedError(
                f"NATSSink: no answer from the loop within {bound}s; the sink is now closed"
            ) from err

    def _abandon_loop(self) -> None:
        """Gives up on a loop the backstop found wedged (SPEC-061 FR-001).

        Called with ``self._lock`` held. Marks the sink closed, schedules a cancel of every task
        and then a stop on the loop, and joins the thread under :data:`LOOP_JOIN_TIMEOUT`. A
        wedged loop never runs the callbacks, so the thread is normally still alive when the join
        expires and is abandoned as a daemon with its loop unclosed; if the blocking call ends
        later, the scheduled callbacks stop the loop and the thread exits on its own.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._closed = True
        self._stop_loop()

    def _stop_loop(self) -> None:
        """Stops the loop, joins its thread under a bound, and closes the loop if the thread ended.

        Scheduled through ``call_soon_threadsafe``, the one way a thread may touch a loop it does
        not run: a callback on the loop thread cancels every task still pending, which lets each
        one take one cancellation step, then stops the loop. A thread still alive when the join
        expires is abandoned as a daemon, and its loop is left unclosed because a running loop
        cannot be closed. Safe to call on a loop already stopped and closed.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        if self._loop.is_closed():
            return
        self._loop.call_soon_threadsafe(self._cancel_then_stop)
        self._thread.join(LOOP_JOIN_TIMEOUT)
        if not self._thread.is_alive():
            self._loop.close()

    def _cancel_then_stop(self) -> None:
        """Cancels every task on the loop and stops it after their cancellations have run once.

        Runs on the loop thread. ``cancel()`` schedules each task's next step, and scheduling the
        stop after them rather than calling it here is what gives those steps one iteration to
        run before the loop exits, so nothing is left "destroyed but pending" on a loop that is
        closed next.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        for task in asyncio.all_tasks(self._loop):
            task.cancel()
        self._loop.call_soon(self._loop.stop)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Publishes a batch on the loop thread and waits for it under a bound (FR-007, SPEC-061).

        ~~The driver requirement satisfied (SPEC-028 FR-002): an ``asyncio`` event loop is
        single-entry. A second thread calling ``run_until_complete`` on a loop that is already
        running raises ``RuntimeError``, and can leave the loop's task machinery in a state where
        a thread never returns from ``emit`` at all.~~ **Superseded by SPEC-061 FR-001:** nothing
        enters the loop any more; each call submits one coroutine and waits on its future. The
        lock stays, now held across the submit and the wait, so the loop still runs one caller's
        coroutine at a time and the driver sees publishes in the order callers made them.

        The guards run in a fixed order, and the order is the point. The pid is compared before
        the lock, because a lock a caller thread held at fork time stays held in the child and the
        child acquired no thread, loop or connection to run anything on (invariant 12). The
        closing flag is read before the lock and again after acquiring it, so a ``close()`` queued
        behind a stream of emits is not starved by a lock that is not FIFO: every call that has
        not yet taken the lock when the close begins is refused rather than published.

        Args:
          batch: The events to publish. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When none of the events landed, when the sink is closed or closing,
            in a forked child, while either latch holds (an unanswered probe, or a publish
            abandoned at an earlier deadline and still pending), or when the backstop bound
            expired and the sink closed itself.
        """
        if not batch:
            return
        self._refuse_outside_the_lock(f"NATSSink published none of {len(batch)} event(s)")
        with self._lock:
            self._refuse_under_the_lock(f"NATSSink published none of {len(batch)} event(s)")
            try:
                self._run(self._emit_on_loop(batch), self.publish_timeout + CALL_SLACK)
            except _LoopWedgedError as err:
                raise SinkDeliveryError(
                    f"NATSSink published none of {len(batch)} event(s) within "
                    f"{self.publish_timeout + CALL_SLACK}s: the loop is wedged and the sink is "
                    "now closed"
                ) from err

    def _refuse_outside_the_lock(self, prefix: str) -> None:
        """Raises for a call that must not even take the lock (SPEC-061 FR-001).

        Args:
          prefix: The start of the message, naming what the call would have done.

        Returns:
          None.

        Raises:
          SinkDeliveryError: In a forked child, while the sink is closing or closed, while the
            outstanding probe is known to be unanswered (the latch, SPEC-061 FR-002), or while a
            publish abandoned at an earlier deadline is still blocked in the driver.
        """
        if os.getpid() != self._pid:
            raise SinkDeliveryError(f"{prefix}: a forked child acquired no connection")
        if self._closed:
            raise SinkDeliveryError(f"{prefix}: the sink is closed")
        if self._closing:
            raise SinkDeliveryError(f"{prefix}: the sink is closing")
        if self._latched():
            raise SinkDeliveryError(
                f"{prefix}: the server has not answered a PING, and the stall it measures has "
                f"lasted more than {self.publish_timeout}s"
            )
        self._refuse_while_stuck(prefix)

    def _refuse_while_stuck(self, prefix: str) -> None:
        """Raises while a publish abandoned at an earlier deadline is still pending (SPEC-061).

        The second latch, for every client, the sink's own and an injected one alike. A publish
        is abandoned only when the driver did not return from it within the budget, which for a
        core publish means its force-flush into a peer that stopped reading, and for a JetStream
        one the same force-flush or an ack wait that outlived the budget. The next publish would
        only queue behind the same blocked flush, so it is refused at once instead of spending a
        budget of its own; a stall then costs one caller one budget, as the probe's latch makes it
        for a silent server. Read before the lock and again after it, from one tuple the loop
        thread replaces whole, so a caller never waits for it; it clears when the abandoned task
        finishes, which its done callback records.

        Args:
          prefix: The start of the message, naming what the call would have done.

        Returns:
          None.

        Raises:
          SinkDeliveryError: While an abandoned publish has not finished.
        """
        if self._publish_stuck():
            raise SinkDeliveryError(
                f"{prefix}: a publish abandoned at an earlier {self.publish_timeout}s deadline "
                "is still blocked in the driver"
            )

    def _publish_stuck(self) -> bool:
        """Reports whether a publish abandoned at an earlier deadline is still pending.

        One read of ``self._stuck``, a tuple the loop thread replaces whole, so any thread may
        call it without a lock.

        Args:
          None.

        Returns:
          True while any abandoned publish has not finished.

        Raises:
          None.
        """
        return any(not task.done() for task in self._stuck)

    def _refuse_under_the_lock(self, prefix: str) -> None:
        """Re-reads the closing flags once the lock is held (SPEC-061 FR-001).

        A ``close()`` can set its flag while this call was waiting for the lock, and a call that
        then published would land after the closer believed the sink quiet. The abandoned-publish
        latch is read again for the same reason: callers that queued on the lock before the
        publish ahead of them was abandoned would otherwise each spend a budget behind it.

        Args:
          prefix: The start of the message, naming what the call would have done.

        Returns:
          None.

        Raises:
          SinkDeliveryError: The sink closed or began closing while the call waited, or a
            publish abandoned at an earlier deadline is still blocked in the driver.
        """
        if self._closed:
            raise SinkDeliveryError(f"{prefix}: the sink is closed")
        if self._closing:
            raise SinkDeliveryError(f"{prefix}: the sink is closing")
        self._refuse_while_stuck(prefix)

    async def _emit_on_loop(self, batch: list[dict[str, object]]) -> None:
        """The coroutine behind :meth:`emit`, run on the loop thread (SPEC-061 FR-002).

        For a client the sink connected itself, a publish is made only after the server answers a
        round trip inside the emit's one ``publish_timeout`` deadline, in both core and JetStream
        modes. The driver's flag stays in front of the round trip as the cheap refusal and is read
        again once the round trip is answered, immediately before the first publish. No answer
        means a refusal with nothing published and no counter moved (SPEC-032's rule: a failure
        reported to the worker is not also counted).

        **A core batch is then confirmed, or counted** (SPEC-061 FR-003). A core ``publish``
        returns once its bytes are in the driver's pending buffer, which the driver's flusher
        task writes to the transport, while a PING is written to the transport directly. So the
        second round trip is sent only once the driver's public ``pending_data_size`` reads zero,
        waited for inside what remains of the deadline (:meth:`_drained`): then every publish is
        in the transport ahead of the PING, a single connection is processed in order, and an
        answered PONG means the server processed every publish before it. A PING sent while the
        buffer is non-empty overtakes the publishes and confirms nothing about them (reproduced
        on CPython 3.12.12, where the transport pauses the flusher past its high-water mark: 133
        of 400 publishes parsed at the PONG). No answer, including when the buffer has not
        drained by the deadline, the deadline is already spent, or the client has become
        disconnected, adds every event published and not otherwise counted to ``failed`` and
        announces it once, and **this returns rather than raises**: the bytes are in flight to a
        server that may yet process them (M6: five publishes written to a paused server all
        landed after it resumed), so a worker retry could deliver them twice, which is SPEC-018's
        rule for an unadjudicable batch. The stall is still reported, because the next call is
        refused by the probe or its latch; only the one batch that straddles the onset of a stall
        is counted rather than raised. JetStream mode has no such step, since each publish already
        awaited its ack, and an injected client has none either. The second round trip is sent
        only while the client still reports itself connected, because the driver's ``flush``
        returns without a PONG otherwise and the next call would wait on nothing. When the buffer
        does not drain, a probe is left outstanding (:meth:`_confirm`), aged from the start of
        this call, so the latch already holds when this call returns and the callers after it are
        refused at once rather than each spending a budget.

        Args:
          batch: The events to publish.

        Returns:
          None.

        Raises:
          SinkDeliveryError: The client is disconnected, the round trip went unanswered, or none
            of the events landed.
        """
        deadline = time.monotonic() + self.publish_timeout
        refusal = f"NATSSink published none of {len(batch)} event(s)"
        if not self._is_connected():
            raise SinkDeliveryError(f"{refusal}: the client is disconnected")
        probing = self._probing()
        if probing:
            if not await self._await_probe(deadline):
                raise SinkDeliveryError(
                    f"{refusal}: no PONG from the server within {self.publish_timeout}s"
                )
            if not self._is_connected():
                raise SinkDeliveryError(f"{refusal}: the client is disconnected")
        published = await self._publish_all(batch, deadline)
        if probing and not self._jetstream and published:
            confirmed = self._is_connected() and await self._confirm(deadline, arm=True)
            if not confirmed:
                with self._counter_lock:
                    self.failed += published
                _diag.lost("event", published, "NATSSink unconfirmed, no PONG after the batch")

    async def _drained(self, deadline: float) -> bool:
        """Waits until the driver's pending buffer is empty, so a PING cannot overtake it.

        The driver's ``pending_data_size`` counts bytes queued for its flusher task, and a PING
        bypasses that queue (the driver writes a PING to the transport directly and only bumps
        the counter, which the flusher's next run resets with the rest). A counter left non-zero by
        an outstanding probe's PING therefore clears as soon as the flusher runs, which on a
        connection that is draining at all it does; the only way it stays non-zero to the deadline
        is a peer that has stopped reading, and that batch is exactly the one that must be counted
        rather than confirmed. Polled on a timed yield at :data:`PENDING_POLL` rather than awaited
        on a driver future, because every driver future here is one the sink must not cancel. A
        client that does not publish the counter, or whose attribute raises, is treated as
        drained: the sink does not invent a loss for a client that never claimed the capability.

        Args:
          deadline: The ``time.monotonic()`` instant this call's own budget ends.

        Returns:
          True once the counter read zero, False when the deadline passed first.

        Raises:
          None.
        """
        while True:
            if self._pending_bytes() == 0:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            await self._pause(min(PENDING_POLL, remaining))

    def _pending_bytes(self) -> int:
        """Reads the driver's public ``pending_data_size``, or zero when it cannot be read.

        Args:
          None.

        Returns:
          The bytes still queued for the driver's flusher, zero for a client without the counter.

        Raises:
          None.
        """
        try:
            return int(getattr(self._client, "pending_data_size", 0))
        except Exception:
            return 0

    async def _pause(self, seconds: float) -> None:
        """Yields the loop for a bounded time without a driver future and without cancelling one.

        A fresh never-resolved future waited with ``asyncio.wait`` and a timeout: the wait's own
        timer ends it, and a cancellation of the surrounding task is delivered cleanly, with no
        callback left to fire on a finished future.

        Args:
          seconds: How long to yield.

        Returns:
          None.

        Raises:
          None.
        """
        await asyncio.wait({self._loop.create_future()}, timeout=seconds)

    def _probing(self) -> bool:
        """Reports whether this client is one the sink asks the server about (SPEC-061 FR-002).

        Only a client the sink connected itself, because the sink cannot drive a client bound to
        another loop (M10), and only one that offers ``flush``: the sink does not invent a
        refusal for a client that never claimed the capability, the reason :meth:`_is_connected`
        gives for its own default.

        Args:
          None.

        Returns:
          True when the probe, the confirmation and the close's round trip apply.

        Raises:
          None.
        """
        return self._owns_client and getattr(self._client, "flush", None) is not None

    def _reconnects(self) -> int:
        """Reads the driver's reconnect counter, the public sign that its PONG queue was reset.

        Args:
          None.

        Returns:
          ``stats["reconnects"]``, or 0 when it cannot be read: an unreadable counter is a
          constant, so it can never look like a reconnect.

        Raises:
          None.
        """
        try:
            return int(self._client.stats["reconnects"])
        except Exception:
            return 0

    def _latched(self) -> bool:
        """Reports whether the outstanding probe is known to be unanswered (SPEC-061 FR-002).

        Read by caller threads **before** they take the lock and without one, so a stall costs
        one caller one ``publish_timeout`` and every later caller is refused at once instead of
        queueing on the lock for its own ``publish_timeout`` — which on the orphan path is the
        application's thread (invariant 4). It is a pure function of one reference load:
        ``self._probe`` is replaced only by whole-tuple assignment on the loop thread, so a load
        sees the old tuple or the new one and never a mix, and nothing here mutates. A stale load
        can only refuse a caller whose probe was in fact still pending at the load; it can never
        let one through, because a caller that passes still runs the full answer check on the
        loop. The latch clears when the task finishes for any reason, when a reconnect moves the
        counter, or when the next call discards and re-sends.

        The probe it ages is whichever PING is outstanding: one a caller waited on, or one
        :meth:`_confirm` left behind when the driver's pending buffer did not drain by the
        deadline, which is how a stall that shows as a buffer that is not moving, rather than as
        an unanswered PING, arms the latch too. A publish the driver never returned from is the
        other latch's (:meth:`_refuse_while_stuck`).

        Args:
          None.

        Returns:
          True while the outstanding probe has gone unanswered for at least ``publish_timeout``
          since the instant it is aged from (when it was sent, or for a probe
          :meth:`_confirm` left behind, the start of the call whose budget it spent) and the
          driver has not reconnected since.

        Raises:
          None.
        """
        probe = self._probe
        if probe is None:
            return False
        task, silent_since, reconnects = probe
        return (
            not task.done()
            and time.monotonic() - silent_since >= self.publish_timeout
            and self._reconnects() == reconnects
        )

    async def _await_probe(self, deadline: float) -> bool:
        """Sends or reuses the one outstanding round trip and waits for it (SPEC-061 FR-002).

        The probe is a task running the driver's public ``flush(timeout=PROBE_FLUSH_TIMEOUT)``,
        and the sink waits on it with ``asyncio.wait`` and **never cancels it**, because a late
        PONG landing on a cancelled future kills the driver's reader while the status stays
        CONNECTED (M3). The rules, in the order the code applies them:

        A probe that finished before this call began is not evidence about now: its outcome was
        retrieved by its done callback, it is discarded, and a fresh PING is sent. A probe still
        pending is discarded only when the driver has reset its PONG queue, which its reconnect
        counter shows; the orphaned task is left to end on its own inner timeout, harmlessly,
        since its future is no longer in the driver's queue. Otherwise the pending probe is
        **reused**: this call waits on the same task, for at most the lesser of its own remaining
        deadline and ``publish_timeout`` less the probe's age, and a second PING is never stacked
        behind it, so at most one probe is outstanding.

        A probe is answered only if all three hold: the task completed without raising, the
        client reports itself connected when checked, and the reconnect counter still equals the
        value recorded when the PING was sent. The second and third exist because the driver's
        ``flush`` returns without a PONG when its reader or flusher task is done, which is the
        case while it is reconnecting (M9).

        Args:
          deadline: The ``time.monotonic()`` instant this call's own budget ends.

        Returns:
          True when the server answered during this call's wait, else False.

        Raises:
          None.
        """
        probe = self._outstanding_probe()
        if probe is None:
            probe = self._send_probe()
        task, silent_since, reconnects = probe
        now = time.monotonic()
        wait = min(deadline - now, self.publish_timeout - (now - silent_since))
        await asyncio.wait({task}, timeout=max(0.0, wait))
        if not task.done() or task.cancelled() or task.exception() is not None:
            return False
        return self._is_connected() and self._reconnects() == reconnects

    def _outstanding_probe(self) -> tuple[asyncio.Task[Any], float, int] | None:
        """Returns the probe still pending on the current connection, discarding any other.

        A probe that has finished is discarded, because its answer says nothing about now, and so
        is one still pending after the driver's reconnect counter moved, because the driver reset
        the PONG queue its future was in (SPEC-061 FR-002, rules 3 and 4). Runs on the loop thread,
        the only writer of ``self._probe``.

        Args:
          None.

        Returns:
          The pending probe's ``(task, silent_since, reconnects)``, or None when there is none.

        Raises:
          None.
        """
        probe = self._probe
        if probe is not None and (probe[0].done() or probe[2] != self._reconnects()):
            self._probe = probe = None
        return probe

    def _send_probe(
        self, silent_since: float | None = None
    ) -> tuple[asyncio.Task[Any], float, int]:
        """Sends one PING through the driver's public ``flush`` and records it as the probe.

        The caller has established that no probe is outstanding, so at most one ever is. The tuple
        is assigned whole, which is what lets :meth:`_latched` read it without a lock. Its second
        field is the instant the latch ages it from, which is when the PING is sent unless the
        caller dates it earlier (:meth:`_confirm` dates it from the start of the call whose budget
        the stall spent; :data:`DRIVER_DEADLINE_MARGIN` states the rule).

        Args:
          silent_since: The ``time.monotonic()`` instant to age the probe from, or None for now.

        Returns:
          The new probe's ``(task, silent_since, reconnects)``.

        Raises:
          None.
        """
        task = self._loop.create_task(self._client.flush(timeout=PROBE_FLUSH_TIMEOUT))
        task.add_done_callback(_retrieve)
        since = time.monotonic() if silent_since is None else silent_since
        probe = (task, since, self._reconnects())
        self._probe = probe
        return probe

    async def _confirm(self, deadline: float, *, arm: bool) -> bool:
        """Confirms the outbound buffer with a PING sent after it drained (SPEC-061 FR-003).

        Waits for the driver's pending buffer to read zero (:meth:`_drained`), then asks the
        server. **A PING sent before the buffer drained is liveness evidence only, never a
        confirmation**: it may have overtaken the publishes it would confirm. So a probe still
        outstanding at that point, one this call did not send, is waited out first as a liveness
        answer, and only then is a fresh PING sent, which is the one that confirms; at most one
        probe is ever outstanding, because the fresh one goes out only once the old one finished.

        When the buffer does not drain by the deadline nothing can confirm it, and with ``arm`` a
        probe is left outstanding without being waited for, so :meth:`_latched` has a PING to age
        and the callers after this one are refused at once rather than each spending a budget
        behind a buffer that is not moving. That probe is aged from the start of the call whose
        budget it spent, ``deadline - publish_timeout``, not from when it was sent: the drained
        wait fails only once the deadline has passed, so the probe is already ``publish_timeout``
        old when it is sent and the latch holds for the very next caller, deterministically. Dated
        when the wait failed, the next caller waited a whole budget more (diff review frame 3,
        finding 2: 1.002 s at ``publish_timeout=1.0`` on the real driver); dated when the drained
        wait began, it still waited for whatever the probe and the publishes had taken, which made
        the latch test flake when a slow pre-publish PONG ate the budget. Only ``emit`` and
        ``flush`` arm, and both pass a ``publish_timeout`` deadline. That PING is sent over the
        undrained buffer,
        and the rule above is what keeps it from ever confirming anything. ``close()`` passes
        ``arm=False``, since no call follows it.

        Args:
          deadline: The ``time.monotonic()`` instant this call's own budget ends.
          arm: Whether to leave a probe outstanding when the buffer does not drain.

        Returns:
          True when a PING sent after the buffer drained was answered within the deadline.

        Raises:
          None.
        """
        if not await self._drained(deadline):
            if arm and self._outstanding_probe() is None:
                self._send_probe(silent_since=deadline - self.publish_timeout)
            return False
        if self._outstanding_probe() is not None and not await self._await_probe(deadline):
            return False
        return await self._await_probe(deadline)

    def _is_connected(self) -> bool:
        """Reports whether the client can currently put anything on the wire (FR-004 AC-5).

        Probed by name, as ``flush`` is, because this sink is written against a driver it does not
        own and accepts an injected ``client=``. A client that does not publish the attribute is
        assumed connected: the guard exists to convert a *known* non-delivery into a reported one,
        and inventing a refusal for a client that never claimed to be disconnected would fail
        batches that were going to succeed. Since SPEC-061 FR-002 it is the cheap refusal in front
        of the server round trip rather than the only guard, and it is read again after the round
        trip is answered, immediately before the first publish.

        Args:
          None.

        Returns:
          True when the client reports itself connected, or says nothing about it.

        Raises:
          None. A driver whose attribute access raises is treated as connected, so a probe can
            never be the reason a batch fails.
        """
        try:
            connected = getattr(self._client, "is_connected", True)
        except Exception:
            return True
        return bool(connected)

    def losses(self) -> SinkLosses:
        """Reports events whose publish raised or went unconfirmed (SPEC-026 FR-002).

        Read without the counter lock in a forked child, where a lock a parent thread held at
        fork time may stay held; the child's counter can only be the inherited value, since the
        child publishes nothing.

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        if os.getpid() != self._pid:
            return SinkLosses(dropped=0, failed=self.failed)
        with self._counter_lock:
            return SinkLosses(dropped=0, failed=self.failed)

    def close(self) -> None:
        """Closes the client under the sink's own deadline, then stops the loop thread (SPEC-061).

        Sets the closing flag **before** taking the lock, so emits queued on the lock are refused
        rather than served first (``threading.Lock`` is not FIFO, and a closer behind a stream of
        emits could otherwise wait indefinitely). A call already inside its coroutine finishes
        under its own bound. In a forked child this returns without touching the client, the loop
        or the thread, because the child acquired none of them (invariant 12); a second call, or
        one after the backstop closed the sink, returns without touching anything.

        **The sequence is the sink's own, not the driver's ``drain()``** (SPEC-061 FR-004).
        ``drain()`` cannot be bounded from outside: it swallows a cancellation and its ``finally``
        flush runs to the driver's own 10 s, measured at 12.00 s under a 2 s ``wait_for`` once a
        JetStream client holds its response-inbox subscription (M7). So under one deadline, the
        ``drain_timeout`` the caller passed or :data:`DEFAULT_CLOSE_TIMEOUT`, the loop thread runs
        a round trip through the FR-002 probe, sent once the driver's pending buffer has drained
        (:meth:`_drained`) so it confirms the outbound buffer rather than overtaking it, then
        ``client.close()`` as a task cancelled on expiry, then stops the loop. The round trip is
        skipped when the client is injected, has no ``flush``, reports itself disconnected, has a
        probe latched as unanswered, or has a publish abandoned and still pending (diff review
        frame 3: the drained wait then spent the whole deadline behind the blocked flush and the
        close raised about a PING it never sent), and a skipped round trip raises nothing: emits
        were refused throughout, so nothing uncounted rides on it. One that **ran** and went unanswered raises
        ``TimeoutError`` after the loop is stopped, keeping the contract that the loop is closed
        regardless of what the flush raised; the probe's own exception is re-raised the same way.
        A failure of ``client.close()``, including the ``TypeError`` M8 measured while
        reconnecting, is absorbed rather than raised.

        An injected client is not drained: its subscriptions are closed rather than drained, and
        whatever its own ``close()`` flushes is all that is flushed (M10).

        Args:
          None.

        Returns:
          None.

        Raises:
          TimeoutError: The round trip ran and went unanswered within the deadline, or the
            backstop bound expired on a wedged loop; the sink is closed either way.
          Exception: The round trip's own exception, when the driver's ``flush`` raised.
        """
        if os.getpid() != self._pid:
            return
        self._closing = True
        with self._lock:
            if self._closed:
                return
            try:
                answered, failure = self._run(
                    self._close_on_loop(), self._drain_timeout + CALL_SLACK
                )
            except _LoopWedgedError as err:
                raise TimeoutError(
                    f"NATSSink close: the loop did not answer within "
                    f"{self._drain_timeout + CALL_SLACK}s and was abandoned"
                ) from err
            self._closed = True
            self._stop_loop()
        if failure is not None:
            raise failure
        if answered is False:
            raise TimeoutError(
                f"NATSSink close: the server did not answer the closing PING within "
                f"{self._drain_timeout}s; the client is closed and the loop stopped"
            )

    async def _close_on_loop(self) -> tuple[bool | None, BaseException | None]:
        """The coroutine behind :meth:`close`, run on the loop thread (SPEC-061 FR-004).

        Step one is the round trip, when it applies (see :meth:`close`), waited for at most the
        whole deadline. Step two runs ``client.close()`` as a task under what remains of the
        deadline, cancelling it on expiry and absorbing any failure from it; a client without a
        ``close`` attribute skips it. The task's first step runs even when the deadline is already
        spent, because ``create_task`` schedules it before ``asyncio.wait`` arms its timer and
        both land in the same loop iteration. Step three cancels every task still pending on the
        loop, the probe among them now that no PONG can arrive, and gives the cancellations half
        the join budget to run, so the loop is stopped with nothing "destroyed but pending". The
        driver's own ``_close`` swallows one cancellation of its own while it waits on its
        reconnection task, which is why step three cancels again rather than trusting step two's.
        Step two yields one loop iteration after its cancel, because two ``cancel()`` calls made
        before the task next runs coalesce into a single delivered cancellation: without the
        yield, step three's cancel would merge into step two's, the driver would swallow the one
        it received, and the close task would park through step three's wait to be ended only by
        the loop's stop.

        Neither an expired nor a failed step two skips step three, and the outcome is returned
        rather than raised so :meth:`close` decides the raise after the loop is stopped.

        Args:
          None.

        Returns:
          A pair: whether the round trip was answered (``None`` when it was skipped), and the
          round trip's own exception if it raised.

        Raises:
          None.
        """
        end = time.monotonic() + self._drain_timeout
        answered: bool | None = None
        failure: BaseException | None = None
        if (
            self._probing()
            and not self._latched()
            and not self._publish_stuck()
            and self._is_connected()
        ):
            try:
                answered = await self._confirm(end, arm=False)
            except Exception as err:
                failure = err
            probe = self._probe
            if probe is not None and probe[0].done() and not probe[0].cancelled():
                failure = probe[0].exception() or failure
        close = getattr(self._client, "close", None)
        if close is not None:
            task = self._loop.create_task(close())
            done, _ = await asyncio.wait({task}, timeout=max(0.0, end - time.monotonic()))
            if not done:
                task.cancel()
                await self._pause(0)
            elif not task.cancelled():
                close_failure = task.exception()
                if close_failure is not None:
                    _diag.absorbed("closing the NATS client", close_failure)
        await self._settle_pending_tasks()
        return answered, failure

    async def _settle_pending_tasks(self) -> None:
        """Cancels every other task on the loop and lets the cancellations run (SPEC-061 FR-001).

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        current = asyncio.current_task()
        pending = {task for task in asyncio.all_tasks(self._loop) if task is not current}
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait(pending, timeout=LOOP_JOIN_TIMEOUT / 2)

    async def _publish_all(self, batch: list[dict[str, object]], deadline: float) -> int:
        """Publishes each event under one deadline for the whole batch (SPEC-047 FR-001).

        Per-event isolation stays because a partial batch must not be retried wholesale: the
        events that published would be delivered twice. What changes is that the *batch* is
        bounded rather than each event in it — applying the driver's ack timeout per item is
        ``n x timeout``, and ``Worker._final_drain`` hands the exit backlog over as one batch.

        The two paths take the bound differently because the driver offers different handles. A
        JetStream publish accepts a ``timeout`` and is given ~~``min(DEFAULT_ACK_TIMEOUT,
        remaining)``~~ :func:`_driver_timeout` of the remainder (diff review frame 3: the bare
        remainder tied with this sink's own wait, which won, so every ack wait the budget capped
        read as abandoned instead of raised) — never the bare remainder, which would also hand
        the first event of a fresh budget a longer ack wait than the driver's own default. ~~A core publish accepts none and
        writes into the client's outbound buffer without waiting, so the deadline is checked
        between events, which is all there is to check.~~ **Corrected by SPEC-061 FR-001:** a
        publish can block without limit, because the driver force-flushes once its pending buffer
        passes 2 MiB and its flush timeout defaults to ``None``, so against a stalled peer the
        await never returns. The force-flush fires on the **cumulative** pending size, so several
        JetStream publishes into a peer that stopped reading reach it too, not only a single
        oversized payload. Every publish on either path therefore runs as a task waited under what
        remains of the deadline and **never cancelled** (the driver swallows the cancellation, and
        bytes already in its buffer may yet land, SPEC-061 M3 and M6). One that outlives the
        deadline is abandoned with a reference kept until close, counted as published so the batch
        is not sent back for a retry that could duplicate it, and the events after it as
        unattempted. For a core client the sink probes, FR-003's confirmation then decides whether
        the batch is confirmed or counted: the abandoned publish's bytes are already queued ahead of
        any PING it sends, so its answer judges them too. Wherever no confirmation will run --
        the JetStream path, an injected client, a client without ``flush`` -- the abandoned
        publish is counted as unconfirmed here and announced; before frame-2 finding 1 an injected
        core client's was counted nowhere, so a batch of one returned with ``failed`` at zero. An
        abandoned publish also arms :meth:`_refuse_while_stuck` until it finishes.

        Two populations are counted by different rules. An event whose publish **raised** is a
        driver failure and is counted here exactly as before this spec. An event **never
        attempted** because the budget expired is counted only when this returns: a raise sends
        the whole batch back to ``Worker._emit``, which retries it, so booking the remainder there
        would report a loss that has not happened — the rule
        :meth:`~log_foundry.sinks.kafka.KafkaSink.flush` already applies to its own queued
        remainder.

        Args:
          batch: The events to publish.
          deadline: The ``time.monotonic()`` instant the whole emit must finish by.

        Returns:
          How many events were published, the abandoned one included.

        Raises:
          SinkDeliveryError: When none of them landed (SPEC-026 FR-001).
        """
        target = self._client.jetstream() if self._jetstream else self._client
        published = 0
        attempted = 0
        abandoned = 0
        for event in batch:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            attempted += 1
            try:
                payload = json.dumps(event).encode("utf-8")
                if not await self._publish_bounded(target, payload, remaining):
                    published += 1
                    abandoned += 1
                    break
            except Exception as err:
                with self._counter_lock:
                    self.failed += 1
                _diag.lost("event", 1, f"NATSSink publish, {type(err).__name__}")
            else:
                published += 1
        unattempted = len(batch) - attempted
        if batch and not published:
            raise SinkDeliveryError(
                f"NATSSink published none of {len(batch)} event(s)"
                + (
                    f", {unattempted} not attempted within {self.publish_timeout}s"
                    if unattempted
                    else ""
                )
            )
        if unattempted:
            with self._counter_lock:
                self.failed += unattempted
            _diag.lost("event", unattempted, f"NATSSink publish_timeout {self.publish_timeout}s")
        if abandoned and (self._jetstream or not self._probing()):
            with self._counter_lock:
                self.failed += abandoned
            kind = "JetStream publish" if self._jetstream else "publish"
            _diag.lost("event", abandoned, f"NATSSink unconfirmed, {kind} abandoned")
        return published

    async def _publish_bounded(self, target: Any, payload: bytes, remaining: float) -> bool:
        """Runs one publish as a task and waits for it under the remaining budget.

        A JetStream publish is also given the driver's ``timeout`` for its ack wait, through
        :func:`_driver_timeout`: the lesser of :data:`DEFAULT_ACK_TIMEOUT` and the remainder less
        :data:`DRIVER_DEADLINE_MARGIN`, so an ack wait the budget caps expires in the driver, which
        raises it, rather than tying with this wait and reading as abandoned. A JetStream publish
        abandoned here therefore means the driver ignored its own timeout. A core publish takes
        no timeout. The task is
        never cancelled (see :meth:`_publish_all`); one that outlives the budget is kept in
        ``self._abandoned`` so it is neither collected mid-flight nor left with an unretrieved
        exception, and is cancelled at close with every other pending task. It is also recorded
        in ``self._stuck``, the abandoned-publish latch, until its done callback clears it.

        Args:
          target: The client or JetStream context to publish on.
          payload: The encoded event.
          remaining: Seconds left in the batch's deadline.

        Returns:
          True when the publish completed within the budget, False when it was abandoned.

        Raises:
          Exception: Whatever the completed publish raised.
        """
        if self._jetstream:
            publish = target.publish(
                self._subject, payload, timeout=_driver_timeout(remaining, DEFAULT_ACK_TIMEOUT)
            )
        else:
            publish = target.publish(self._subject, payload)
        task = self._loop.create_task(publish)
        task.add_done_callback(_retrieve)
        done, _ = await asyncio.wait({task}, timeout=remaining)
        if not done:
            self._abandoned.add(task)
            task.add_done_callback(self._abandoned.discard)
            self._stuck = (*self._stuck, task)
            task.add_done_callback(self._unstick)
            return False
        task.result()
        return True

    def _unstick(self, task: asyncio.Task[Any]) -> None:
        """Clears a finished abandoned publish from the latch; a done callback on the loop thread.

        Replaces the tuple whole rather than mutating it, so a caller thread reading it without a
        lock sees the old tuple or the new one and never a set changing size under its iteration.

        Args:
          task: The abandoned publish that finished.

        Returns:
          None.

        Raises:
          None.
        """
        self._stuck = tuple(other for other in self._stuck if other is not task)

    def flush(self) -> None:
        """Pushes the client's outbound buffer onto the wire without closing (SPEC-036 FR-002).

        Core ``publish()`` writes into the client's own outbound buffer and returns; the network
        write happens on the driver's flusher task. That is why ``log_foundry.flush()`` could not
        reach a published-but-unwritten event before this hook. Under JetStream ``publish()``
        awaits an ack, so there is nothing pending and this costs a round trip at worst.

        For a client the sink connected itself this is the FR-002 probe, bounded by
        ``publish_timeout`` and by the latch, and it raises ``SinkDeliveryError`` when the round
        trip goes unanswered or the client reports itself disconnected (SPEC-061 FR-004). The
        driver's 10 s ``FlushTimeoutError`` can no longer escape from it, and this never cancels a
        PONG wait; the driver still cancels futures of its own, so this is narrower than the claim
        that the M3 hazard is gone. It counts nothing. An injected client, or one with no
        ``flush``, keeps the bare driver call and whatever it raises, run as a task waited under
        ``publish_timeout`` and never cancelled (M3 again, on the injected client): a flush that
        outlives the budget is abandoned and reported as ``SinkDeliveryError``, and the sink stays
        usable, where before this bound a slow injected flush reached the backstop and closed the
        sink for good.

        Takes the same lock :meth:`emit` does and runs the same guards in the same order, for the
        reasons recorded there.

        Args:
          None.

        Returns:
          None.

        Raises:
          SinkDeliveryError: The sink is closed or closing, this is a forked child, either latch
            holds, the round trip went unanswered or the client is disconnected, or the backstop
            bound expired.
          Exception: Whatever an injected client's driver raises while flushing.
        """
        self._refuse_outside_the_lock("NATSSink cannot flush")
        with self._lock:
            self._refuse_under_the_lock("NATSSink cannot flush")
            try:
                self._run(self._flush_on_loop(), self.publish_timeout + CALL_SLACK)
            except _LoopWedgedError as err:
                raise SinkDeliveryError(
                    f"NATSSink cannot flush: no answer within {self.publish_timeout + CALL_SLACK}s,"
                    " the loop is wedged and the sink is now closed"
                ) from err

    async def _flush_on_loop(self) -> None:
        """The coroutine behind :meth:`flush`, run on the loop thread.

        Args:
          None.

        Returns:
          None.

        Raises:
          SinkDeliveryError: A probed client is disconnected or did not answer the round trip.
          Exception: Whatever an injected client's driver raises while flushing.
        """
        deadline = time.monotonic() + self.publish_timeout
        if not self._probing():
            await self._flush_client(deadline)
            return
        if not self._is_connected():
            raise SinkDeliveryError("NATSSink cannot flush: the client is disconnected")
        if not await self._confirm(deadline, arm=True):
            raise SinkDeliveryError(
                f"NATSSink cannot flush: no PONG confirmed the outbound buffer within "
                f"{self.publish_timeout}s"
            )

    async def _flush_client(self, deadline: float) -> None:
        """Flushes an injected client if it offers a flush, probed by name, under the deadline.

        Probed by name because the sink is written against a driver it does not own, and a client
        without the method has nothing buffered to push. The call runs as a task waited with
        ``asyncio.wait`` and never cancelled, because an injected ``nats-py`` client's flush is a
        PONG wait too (M3); one that outlives the deadline is kept in ``self._abandoned`` and
        reported, and the sink stays open.

        Args:
          deadline: The ``time.monotonic()`` instant this call's own budget ends.

        Returns:
          None.

        Raises:
          SinkDeliveryError: The flush did not return within the deadline.
          Exception: Whatever the driver raises while flushing.
        """
        flush = getattr(self._client, "flush", None)
        if flush is None:
            return
        task = self._loop.create_task(flush())
        task.add_done_callback(_retrieve)
        done, _ = await asyncio.wait({task}, timeout=max(0.0, deadline - time.monotonic()))
        if not done:
            self._abandoned.add(task)
            task.add_done_callback(self._abandoned.discard)
            raise SinkDeliveryError(
                f"NATSSink cannot flush: the client's flush did not return within "
                f"{self.publish_timeout}s"
            )
        task.result()
