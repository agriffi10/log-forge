"""SPEC-098: every sink holds one stop signal of its own, handed once per installation.

A sink the library delivers to holds one ``threading.Event`` of its own, assigned through
``log_foundry_stop_signal`` at its first delivery and again only when a ``configure()`` installs it
anew (FR-001). Every stop sets every such event; a ``configure()`` that installs a sink again with
no stop running clears it, and a delivery after a stop with no ``configure()`` finds it set
(FR-002). A forked child keeps every event object, re-initialised in place, and runs no setter
(FR-003). The README's protocol keeps its name, type and promise (FR-004).

The rows the spec's reviewer measured with probes that were never committed (``twins.py``,
``orphanwrap.py``, ``readme_sinks.py``, ``heldcond.py``) are ported here, each asserting what a
wait returned, the reason, rather than a time. Every test that forks, or parks a thread in a
setter, runs in a bounded child through ``test_fork_lifecycle.run_in_child``, which leaves
through ``os._exit``.

Serves invariants 1, 3, 4, 5, 6, 10, 11 and 12.
"""

from __future__ import annotations

import ast
import gc
import inspect
import os
import sys
import textwrap
import threading
import time

import pytest

import log_foundry
from log_foundry import _lifecycle
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.multi import MultiSink
from test_fork_lifecycle import run_in_child

GATE = 5.0
"""Seconds a parked thread or a joined call is given before a test gives up on it."""

BACKOFF = 4.0
"""A backoff a stop must cut short; a wait that runs it out returns ``False``."""


class Holder:
    """A sink that records every assignment of ``log_foundry_stop_signal`` and what it emitted."""

    def __init__(self) -> None:
        self._signal: threading.Event | None = None
        self.assigned: list[threading.Event | None] = []
        self.events: list[dict[str, object]] = []
        self.closes = 0

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return self._signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
        self.assigned.append(event)
        self._signal = event

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.events.extend(batch)

    def close(self) -> None:
        self.closes += 1


def _own(sink: object) -> threading.Event | None:
    """The event the library made for ``sink``, or ``None`` if it has no entry."""
    entry = _lifecycle._state._signals.get(id(sink))
    return None if entry is None else entry[1]


@log_foundry.trace
def _traced() -> None:
    log_foundry.info("in a span")


def _wait_returned(event: threading.Event | None, seconds: float) -> list[bool]:
    """Starts a thread waiting ``seconds`` on ``event``; the list gets what the wait returned."""
    out: list[bool] = []
    if event is not None:
        threading.Thread(target=lambda: out.append(event.wait(seconds)), daemon=True).start()
    return out


def _joined(out: list[bool], seconds: float = BACKOFF + 2.0) -> list[bool]:
    """Waits for a :func:`_wait_returned` thread to report, then returns what it reported."""
    deadline = time.monotonic() + seconds
    while not out and time.monotonic() < deadline:
        time.sleep(0.01)
    return out


# -- FR-001: one event per sink, handed at its first delivery ------------------------------------


def test_one_hand_per_installation_across_the_sequence() -> None:
    """FR-001 AC-1: the ``sequence`` row reads two assignments of one event.

    ``configure(S)``, an orphan log, a traced call, ``flush()``, ``configure(T)``,
    ``configure(S)``, an orphan log, ``shutdown()`` and an orphan log after it: S is handed at its
    first delivery and again when ``configure(S)`` installs it anew, the same object both times.
    At ``ead40798`` it read four assignments of two events.
    """
    s, t = Holder(), Holder()
    log_foundry.configure(service="seq", sink=s)
    log_foundry.info("a")
    _traced()
    log_foundry.flush(timeout=GATE)
    log_foundry.configure(sink=t)
    log_foundry.configure(sink=s)
    log_foundry.info("b")
    log_foundry.shutdown(timeout=GATE)
    log_foundry.info("after")
    assert len(s.assigned) == 2, s.assigned
    assert len({id(event) for event in s.assigned}) == 1, s.assigned
    assert s.assigned[0] is _own(s)


@pytest.mark.parametrize("route", ["orphan", "worker", "multi"])
def test_every_sink_a_swap_installs_holds_its_event_when_configure_returns(route: str) -> None:
    """FR-001 AC-2: a swap's sink holds an event before anything is delivered to it.

    ``orphan`` and ``multi`` swap with no worker, ``worker`` retargets a live one; in ``multi`` the
    ``MultiSink`` installed forwards its own event to both members.
    """
    s, t = Holder(), Holder()
    log_foundry.configure(service="swap", sink=s)
    if route == "worker":
        _traced()
        log_foundry.flush(timeout=GATE)
    else:
        log_foundry.info("a")
    if route == "multi":
        t2 = Holder()
        log_foundry.configure(sink=MultiSink(t, t2))
        held = [t.log_foundry_stop_signal, t2.log_foundry_stop_signal]
    else:
        log_foundry.configure(sink=t)
        held = [t.log_foundry_stop_signal]
    assert [event is not None for event in held] == [True] * len(held), held


def test_a_setter_that_raises_is_offered_once_and_an_interrupted_one_again(capsys) -> None:
    """FR-001 AC-4: an absorbed ``Exception`` marks the sink handed; a ``KeyboardInterrupt`` not.

    The mark is made after the setter's ``try``, so an ``Exception`` the offer absorbs, with one
    line, still counts as a hand, while a ``KeyboardInterrupt`` leaves the sink unhanded and the
    next delivery offers it again.
    """

    class Raising(Holder):
        def __init__(self, error: BaseException, *, once: bool) -> None:
            super().__init__()
            self.error, self.once, self.calls = error, once, 0

        @property
        def log_foundry_stop_signal(self) -> threading.Event | None:
            return self._signal

        @log_foundry_stop_signal.setter
        def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
            self.calls += 1
            if not self.once or self.calls == 1:
                raise self.error
            self._signal = event

    failing = Raising(RuntimeError("the setter fails"), once=False)
    log_foundry.configure(service="raises", sink=failing)
    capsys.readouterr()
    log_foundry.info("one")
    log_foundry.info("two")
    err = capsys.readouterr().err
    assert failing.calls == 1, failing.calls
    assert err.count("handing the sink its stop signal") == 1, err

    log_foundry.shutdown(timeout=GATE)
    interrupted = Raising(KeyboardInterrupt(), once=True)
    log_foundry.configure(sink=interrupted)
    assert interrupted.calls == 0, "with nothing owed, configure() offers nothing"
    with pytest.raises(KeyboardInterrupt):
        log_foundry.info("its first delivery")
    assert interrupted.calls == 1
    log_foundry.info("its next delivery, with no configure() between")
    assert interrupted.calls == 2, "an interrupted setter is offered again at its next delivery"
    log_foundry.info("and then not again")
    assert interrupted.calls == 2
    assert interrupted.log_foundry_stop_signal is _own(interrupted)


def test_the_table_holds_at_most_one_dead_entry_after_three_thousand_sinks() -> None:
    """FR-001 AC-5: dead entries are pruned whenever a sink gets its first entry.

    3,000 sinks are each configured, logged to and released, then one more is configured and
    logged to, and the collector runs. The reviewer's ``table-growth`` row read 122 dead entries
    without the prune and one with it, the last sink released before the final first entry.
    """
    for index in range(3000):
        log_foundry.configure(service="growth", sink=Holder())
        log_foundry.info(f"to sink {index}")
    last = Holder()
    log_foundry.configure(sink=last)
    log_foundry.info("the last sink")
    gc.collect()
    dead = [
        key
        for key, (ref, _event, _handed) in list(_lifecycle._state._signals.items())
        if _lifecycle._referent(ref) is None
    ]
    assert len(dead) <= 1, len(dead)
    assert _own(last) is not None


class _CountingLock:
    """Wraps a real lock and counts every acquisition, through ``with`` or ``acquire``."""

    def __init__(self, real: object) -> None:
        self.real = real
        self.count = 0

    def acquire(self, *args: object, **kwargs: object) -> bool:
        self.count += 1
        return bool(self.real.acquire(*args, **kwargs))  # type: ignore[attr-defined]

    def release(self) -> None:
        self.real.release()  # type: ignore[attr-defined]

    def __enter__(self) -> bool:
        self.count += 1
        return bool(self.real.__enter__())  # type: ignore[attr-defined]

    def __exit__(self, *exc: object) -> None:
        self.real.__exit__(*exc)  # type: ignore[attr-defined]


def test_an_orphan_log_on_an_armed_handed_sink_takes_no_lock(monkeypatch) -> None:
    """FR-001 AC-6: 1,000 notes of an armed, handed sink acquire neither lock once.

    ``_note_orphan_emit`` reads the owed record and the signal table with no lock; only a sink the
    record does not owe takes ``_state._lock``, and only a first hand ``_owned_lock``.
    """
    sink = Holder()
    log_foundry.configure(service="nolock", sink=sink)
    log_foundry.info("arms and hands it")
    state = _lifecycle._state
    lock, owned = _CountingLock(state._lock), _CountingLock(state._owned_lock)
    monkeypatch.setattr(state, "_lock", lock)
    monkeypatch.setattr(state, "_owned_lock", owned)
    for _ in range(1000):
        _lifecycle._note_orphan_emit(sink)  # type: ignore[arg-type]
    counts = (lock.count, owned.count)
    monkeypatch.undo()
    assert counts == (0, 0), counts


def test_a_sink_that_cannot_be_weakly_referenced_is_never_called() -> None:
    """The table stores such a sink itself, and reads which form it holds by type (plan Minor 2).

    A callable sink with no ``__weakref__`` slot is stored strongly; a lock-free read that called
    the first element to resolve it would run the sink's ``__call__`` on every orphan log.
    """

    class Callable_:
        __slots__ = ("_signal", "called", "events")

        def __init__(self) -> None:
            self._signal: threading.Event | None = None
            self.called = 0
            self.events = 0

        def __call__(self) -> object:
            self.called += 1
            return self

        @property
        def log_foundry_stop_signal(self) -> threading.Event | None:
            return self._signal

        @log_foundry_stop_signal.setter
        def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
            self._signal = event

        def emit(self, batch: list[dict[str, object]]) -> None:
            self.events += len(batch)

        def close(self) -> None:
            pass

    sink = Callable_()
    log_foundry.configure(service="strong", sink=sink)  # type: ignore[arg-type]
    for _ in range(3):
        log_foundry.info("x")
    assert sink.events == 3
    assert sink.called == 0, "the sink's __call__ ran"
    assert _lifecycle._state._signals[id(sink)][0] is sink


def test_the_drain_thread_emits_nothing_before_the_sink_holds_its_event() -> None:
    """FR-001: a batch on the drain thread offers the sink first (plan review, M-1).

    Thread A's traced call builds the worker on S, and S's setter parks before it assigns. Thread
    B's traced call is delivered by the drain thread meanwhile. At ``463894aa`` that emit reached S
    holding no event (the plan reviewer's ``probe_drain_before_hand.py`` read ``['none']``); the
    drain thread now offers S its own event before the batch, so S holds it at every emit.
    """

    def work() -> str:
        parked, release = threading.Event(), threading.Event()

        class Parks(Holder):
            first = True

            @property
            def log_foundry_stop_signal(self) -> threading.Event | None:
                return self._signal

            @log_foundry_stop_signal.setter
            def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
                if self.first:
                    self.first = False
                    parked.set()
                    release.wait(GATE)
                self._signal = event

            def emit(self, batch: list[dict[str, object]]) -> None:
                self.events.append({"held": self._signal is not None})

        sink = Parks()
        log_foundry.configure(service="drain", sink=sink)
        a = threading.Thread(target=_traced, daemon=True)
        a.start()
        if not parked.wait(GATE):
            return "the build's setter never parked"
        b = threading.Thread(target=_traced, daemon=True)
        b.start()
        b.join(GATE)
        deadline = time.monotonic() + GATE
        while not sink.events and time.monotonic() < deadline:
            time.sleep(0.01)
        seen = [event["held"] for event in sink.events]
        release.set()
        a.join(GATE)
        return f"emitted={bool(seen)} held={sorted(set(seen))}"

    child = run_in_child(work, timeout=20)
    assert child.output == "emitted=True held=[True]", child.output


# -- FR-002: every stop sets every event; a reinstall clears it ----------------------------------


def test_a_shutdown_cuts_a_wait_on_the_sinks_event() -> None:
    """FR-002 AC-1, the ``cut`` row: a wait on S's own event ends when ``shutdown()`` sets it."""
    sink = Holder()
    log_foundry.configure(service="cut", sink=sink)
    log_foundry.info("a")
    out = _wait_returned(sink.log_foundry_stop_signal, BACKOFF)
    log_foundry.shutdown(timeout=GATE)
    assert _joined(out) == [True], out


class _ParkingForwarder:
    """A caller's wrapper that forwards the signal, leaves its member open, and can park the drain.

    ``park``, when set, holds the drain thread inside ``emit``, before the member, until the test
    sets it; ``parked`` says it is there.
    """

    def __init__(self, inner: object) -> None:
        self._inner = inner
        self._sig: threading.Event | None = None
        self.park: threading.Event | None = None
        self.parked = threading.Event()

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        """The signal the library handed, also handed to the member."""
        return self._sig

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, sig: threading.Event | None) -> None:
        self._sig = sig
        self._inner.log_foundry_stop_signal = sig  # type: ignore[attr-defined]

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards the batch, parking the drain thread first when asked."""
        park = self.park
        if park is not None and threading.current_thread().name == "log-foundry-worker":
            self.parked.set()
            park.wait(GATE)
        self._inner.emit(batch)  # type: ignore[attr-defined]

    def close(self) -> None:
        """Closes nothing."""


def _expire_inside(wrapper: _ParkingForwarder, member: object) -> str:
    """Expires a ``shutdown()`` with the drain parked in *wrapper*, then lets the drain finish.

    Returns whether *member*'s own signal is set and whether it was closed, read after the drain
    thread has ended, so the next ``configure()`` finds nothing held.
    """
    wrapper.park = threading.Event()
    _traced()
    if not wrapper.parked.wait(GATE):
        return "the drain thread never parked"
    worker = _lifecycle._state.worker_exists()
    log_foundry.shutdown(timeout=0.3)
    wrapper.park.set()
    if worker is not None:
        worker._thread.join(GATE)
    signal = getattr(member, "log_foundry_stop_signal", None)
    closed = bool(getattr(member, "_closed", False))
    return f"set={signal is not None and signal.is_set()} closed={closed}"


def test_a_pubsub_sink_configured_again_after_a_shutdown_confirms_what_it_published() -> None:
    """FR-002 AC-2's discriminating point: no publish is counted lost after a reinstall.

    ``spec097_pubsub_after_shutdown.py`` scaled down: P is handed, held by a caller's wrapper that
    forwards the signal and leaves P open, ``shutdown()`` sets P's signal, then P is configured
    again, five orphan logs publish, ``flush()`` runs and a final ``shutdown()`` closes P.

    **The first ``shutdown()`` expires with the drain thread parked inside the wrapper** (SPEC-101).
    Before SPEC-101 a completed ``shutdown()`` closed the wrapper and left P open, since the
    wrapper's ``close()`` leaves its member alone; since SPEC-101 the library closes P after that
    wrapper, so a completed ``shutdown()`` no longer reaches "signal set, P still open". An
    expired one does: it sets every signal and closes nothing the drain thread is inside. The
    drain is then let go and its thread joined before P is configured again, so the swap releases
    the wrapper and spares P, which the graph it installs keeps. ``failed`` is counted from that
    configure, and only the five orphan logs' confirmations are counted, so the drained span's
    own rounds, which ran on the set signal, count in neither.

    Since SPEC-094 each orphan log's round waits for its own future, up to ``overflow_timeout``,
    and a set signal ends that wait after its first 50 ms slice, reading the publish ``UNKNOWN``
    and counting it in ``failed``. So futures confirm after 0.3 s, inside the 1.0 s
    ``overflow_timeout`` and past the first slice: with the reinstall's clear each round waits
    and confirms, ``failed=0``, and without it each of the five is counted, ``failed=5``
    (measured with the clear removed). The two logs before the first ``shutdown()`` confirm in
    their own rounds, before ``confirmed`` is emptied, so five are confirmed after it. Before
    SPEC-094 the band was a 0.6 s confirm against a 0.4 s ``overflow_timeout``, which SPEC-094
    reads as unconfirmed whatever the signal says. Only the counts are asserted.
    """

    def work() -> str:
        from log_foundry.sinks.pubsub import GooglePubSubSink

        confirmed: list[bytes] = []
        confirm_after = 0.3

        class Future:
            def __init__(self, data: bytes) -> None:
                self.ready_at = time.monotonic() + confirm_after
                self.data = data

            def result(self, timeout: float | None = None) -> str:
                left = self.ready_at - time.monotonic()
                if left > 0:
                    if timeout is not None and timeout < left:
                        time.sleep(max(timeout, 0))
                        raise TimeoutError("not yet")
                    time.sleep(left)
                confirmed.append(self.data)
                return "id"

        class Publisher:
            def publish(self, topic: str, data: bytes = b"", **attrs: object) -> Future:
                return Future(data)

        p = GooglePubSubSink("projects/p/topics/t", client=Publisher(), overflow_timeout=1.0)
        log_foundry.configure(service="pubsub", sink=p)
        log_foundry.info("before")
        wrapper = _ParkingForwarder(p)
        log_foundry.configure(sink=wrapper)  # type: ignore[arg-type]
        log_foundry.info("via the wrapper")
        premise = _expire_inside(wrapper, p)
        if premise != "set=True closed=False":
            return f"the premise: {premise}"
        confirmed.clear()
        before = p.failed
        log_foundry.configure(sink=p)
        for index in range(5):
            log_foundry.info(f"after {index}")
        log_foundry.flush(timeout=GATE)
        log_foundry.shutdown(timeout=GATE)
        after = sum(1 for data in confirmed if b'"after ' in data)
        return f"confirmed={after} failed={p.failed - before}"

    child = run_in_child(work, timeout=30)
    assert child.output == "confirmed=5 failed=0", child.output


def test_a_kafka_sink_configured_again_after_a_shutdown_confirms_what_it_produced() -> None:
    """The Kafka twin of the Pub/Sub test above: no message is counted lost after a reinstall.

    K is handed, held by a caller's wrapper that forwards the signal and leaves K open, and
    ``shutdown()`` sets K's signal. A round delivered to K then, before any ``configure()``, finds
    the signal set, so its wait ends after the first 50 ms slice and a report due 0.3 s after the
    produce reads ``UNKNOWN``: the window the release notes' known gap names. K is then configured
    again, five orphan logs produce, ``flush()`` runs and a final ``shutdown()`` closes K. Reports
    arrive 0.3 s after each produce, inside the 1.0 s ``flush_timeout``: with the reinstall's clear
    each round waits and confirms, ``failed=0`` for the five, and without it each is counted,
    ``failed=5`` (measured with the clear removed).

    The first ``shutdown()`` expires with the drain thread parked inside the wrapper, for the
    Pub/Sub test's reason (SPEC-101): a completed one now closes K after the wrapper.
    """

    def work() -> str:
        from log_foundry.sinks.kafka import KafkaSink
        from log_foundry.sinks.verdict import Outcome

        delay = 0.3

        class Producer:
            def __init__(self) -> None:
                self.held: list[tuple[float, object]] = []
                self.lock = threading.Lock()

            def produce(
                self, topic: str, value: object = None, key: object = None, callback: object = None
            ) -> None:
                with self.lock:
                    self.held.append((time.monotonic() + delay, callback))

            def _serve(self) -> int:
                now = time.monotonic()
                with self.lock:
                    due = [cb for at, cb in self.held if at <= now]
                    self.held = [(at, cb) for at, cb in self.held if at > now]
                    left = len(self.held)
                for cb in due:
                    cb(None, None)  # type: ignore[operator]
                return left

            def poll(self, timeout: float) -> int:
                self._serve()
                return 0

            def flush(self, timeout: float | None = None) -> int:
                end = time.monotonic() + (timeout if timeout is not None else 1e9)
                while True:
                    left = self._serve()
                    if left == 0 or time.monotonic() >= end:
                        return left
                    time.sleep(min(0.005, max(0.0, end - time.monotonic())))

        k = KafkaSink("t", producer=Producer(), flush_timeout=1.0)
        log_foundry.configure(service="kafka", sink=k)
        log_foundry.info("before")
        wrapper = _ParkingForwarder(k)
        log_foundry.configure(sink=wrapper)  # type: ignore[arg-type]
        log_foundry.info("via the wrapper")
        premise = _expire_inside(wrapper, k)
        if premise != "set=True closed=False":
            return f"the premise: {premise}"
        (late,) = k.deliver([{"message": "late"}]).outcomes
        log_foundry.configure(sink=k)
        before = k.failed
        for index in range(5):
            log_foundry.info(f"after {index}")
        log_foundry.flush(timeout=GATE)
        log_foundry.shutdown(timeout=GATE)
        late_reads = "unknown" if late is Outcome.UNKNOWN else str(late)
        return f"late={late_reads} failed={k.failed - before}"

    child = run_in_child(work, timeout=30)
    assert child.output == "late=unknown failed=0", child.output


@pytest.mark.parametrize(
    "reconfigure", [False, True], ids=["shutdown-then-fork", "and-reconfigure"]
)
def test_a_child_forked_after_shutdown_holds_the_set_event_until_it_configures(
    reconfigure: bool,
) -> None:
    """FR-002 AC-3, ``twins.py``'s two rows: a child inherits the set event; its configure clears.

    ``shutdown-then-fork``: the child's wait on S's event returns ``True`` at once, since the
    parent's ``shutdown()`` set it. ``and-reconfigure``: the child's ``configure(S)`` runs with no
    stop running in the child, clears the child's copy, and the wait runs out; the parent's copy
    stays set.
    """
    sink = Holder()
    log_foundry.configure(service="fork-after", sink=sink)
    log_foundry.info("a")
    log_foundry.shutdown(timeout=GATE)
    parent = sink.log_foundry_stop_signal
    assert parent is not None and parent.is_set()

    def child() -> str:
        if reconfigure:
            log_foundry.configure(sink=sink)
        held = sink.log_foundry_stop_signal
        return f"same={held is parent} returned={held is not None and held.wait(0.3)}"

    run = run_in_child(child)
    expected = "same=True returned=False" if reconfigure else "same=True returned=True"
    assert run.output == expected, run.output
    assert parent.is_set(), "the child's configure() touched the parent's copy"


def test_a_stop_entering_between_a_clears_check_and_its_clear_still_sets_the_event(
    monkeypatch,
) -> None:
    """FR-002 AC-4: the check and the clear are one ``_owned_lock`` section.

    A ``configure(S)`` that installs S again parks inside ``_no_stop_running``, after the real
    check found no stop, with the lock held. A ``shutdown()`` on another thread enters, adds its
    token, and blocks on that lock before it can set S's event: S's event stays unset while the
    clear is parked. Released, the clear runs and then the stop's set, so S's event is set when the
    stop returns. With the check moved out of the section, the stop sets the event while the clear
    is parked and the clear then undoes it, so the event is unset when both return: the first
    assertion. The stop's entry is observed at ``_set_every_signal``, wrapped to record that the
    stop reached its set before calling the real one, so a stop that runs to completion while the
    clear is parked, as it does under that mutant, is still seen to have entered.
    """
    sink = Holder()
    log_foundry.configure(service="race", sink=sink)
    log_foundry.info("a")
    event = _own(sink)
    assert event is not None
    real = _lifecycle._no_stop_running
    parked, go = threading.Event(), threading.Event()

    def park() -> bool:
        answer = real()
        if threading.current_thread().name == "configurer":
            parked.set()
            go.wait(GATE)
        return answer

    real_set = _lifecycle._set_every_signal
    reached_set = threading.Event()

    def set_every_signal() -> None:
        reached_set.set()
        real_set()

    monkeypatch.setattr(_lifecycle, "_no_stop_running", park)
    monkeypatch.setattr(_lifecycle, "_set_every_signal", set_every_signal)
    configurer = threading.Thread(
        target=lambda: log_foundry.configure(sink=sink), name="configurer", daemon=True
    )
    configurer.start()
    assert parked.wait(GATE), "the clear never parked"
    stopper = threading.Thread(target=lambda: log_foundry.shutdown(timeout=GATE), daemon=True)
    stopper.start()
    entered = reached_set.wait(GATE)
    set_while_parked = event.wait(0.5)
    go.set()
    configurer.join(GATE * 2)
    stopper.join(GATE * 2)
    assert event.is_set(), "the clear ran after the stop's set and left the event unset"
    assert entered, "the stop never reached its set"
    assert set_while_parked is False, "the stop set the event while the clear was parked"


@pytest.mark.parametrize("wrapper", ["multi", "factory"])
def test_configuring_a_wrapper_of_sinks_a_shutdown_set_clears_every_event(wrapper: str) -> None:
    """FR-002 AC-5: ``configure(MultiSink(P, Q))`` after a ``shutdown()`` leaves nothing set.

    ``multi``: P and Q are each installed and logged to, so each has its own event, and the
    ``shutdown()`` sets both. Installing ``MultiSink(P, Q)`` walks its graph and clears P's and Q's
    events; its first delivery hands it its own event, which it forwards onto them. ``factory``: a
    ``_FactorySink`` that has built P is installed and logged to, so both hold their own events,
    and the ``shutdown()`` sets both; installing that wrapper again reaches P through its graph and
    clears both, and the wrapper's hand offers P through the library, which hands P its own cleared
    event. That is where the graph walk is observable: without it, P keeps its set event. Before
    the next stop, no table event and no attribute is set.
    """
    from log_foundry.sinks._factory import _FactorySink

    p, q = Holder(), Holder()
    if wrapper == "multi":
        log_foundry.configure(service="graph", sink=p)
        log_foundry.info("p")
        log_foundry.configure(sink=q)
        log_foundry.info("q")
        log_foundry.shutdown(timeout=GATE)
        top: object = MultiSink(p, q)
        members = [p, q]
    else:
        top = _FactorySink(lambda: p)  # type: ignore[arg-type, return-value]
        log_foundry.configure(service="graph", sink=top)  # type: ignore[arg-type]
        log_foundry.info("builds P")
        log_foundry.shutdown(timeout=GATE)
        members = [p]
    assert all(_own(m) is not None and _own(m).is_set() for m in members)  # type: ignore[union-attr]
    log_foundry.configure(sink=top)  # type: ignore[arg-type]
    log_foundry.info("the next delivery, before the next stop")
    events = [_own(top), *(_own(member) for member in members)]
    attributes = [member.log_foundry_stop_signal for member in members]
    assert all(event is not None for event in events), events
    assert not any(event.is_set() for event in events if event is not None), events
    assert not any(a is None or a.is_set() for a in attributes), attributes


@pytest.mark.parametrize("where", ["inside-the-lock", "after-the-lock", "during-the-stop"])
def test_a_first_delivery_racing_a_stop_ends_with_its_event_set(where: str) -> None:
    """FR-002 AC-7 and the plan review's Minor 6: an event made around a stop's entry is set.

    ``inside-the-lock``: the first offer parks in ``_new_signal`` after creating the event, with
    ``_owned_lock`` held, and a ``shutdown()`` enters, which blocks on that lock in its set. The
    event is added to the set before the lock is released, so the stop's set reaches it.
    ``after-the-lock``: the offer parks after ``_stable_signal`` returns, before the setter; the
    stop's set reaches the event already in the set. ``during-the-stop``: the stop's entry has
    already set every event and the stop is still running, parked in a slow close; a first
    delivery then makes its event set, since a stop is running. In each, the event is set when
    the stop returns.
    """

    def work() -> str:
        sink, slow = Holder(), Holder()
        parked, go = threading.Event(), threading.Event()
        if where == "during-the-stop":
            in_close = threading.Event()

            def slow_close() -> None:
                in_close.set()
                go.wait(GATE)

            slow.close = slow_close  # type: ignore[method-assign]
            log_foundry.configure(service="race", sink=slow)
            log_foundry.info("arms the slow sink")
            stopper = threading.Thread(
                target=lambda: log_foundry.shutdown(timeout=GATE), daemon=True
            )
            stopper.start()
            if not in_close.wait(GATE):
                return "the stop never reached its close"
            _lifecycle.offer_stop_signal(sink, threading.Event())  # type: ignore[arg-type]
            made_set = _own(sink).is_set()  # type: ignore[union-attr]
            go.set()
            stopper.join(GATE * 2)
            return f"made_set={made_set} set_after={_own(sink).is_set()}"  # type: ignore[union-attr]
        name = "_new_signal" if where == "inside-the-lock" else "_stable_signal"
        real = getattr(_lifecycle, name)

        def parking(*args: object) -> object:
            result = real(*args)
            if threading.current_thread().name == "first":
                parked.set()
                go.wait(GATE)
            return result

        setattr(_lifecycle, name, parking)
        log_foundry.configure(service="race", sink=slow)
        log_foundry.info("so the stop has work")
        first = threading.Thread(
            target=lambda: _lifecycle.offer_stop_signal(sink, threading.Event()),  # type: ignore[arg-type]
            name="first",
            daemon=True,
        )
        first.start()
        if not parked.wait(GATE):
            return "the first offer never parked"
        stopper = threading.Thread(target=lambda: log_foundry.shutdown(timeout=GATE), daemon=True)
        stopper.start()
        deadline = time.monotonic() + GATE
        while not _lifecycle._state._stops_running and time.monotonic() < deadline:
            time.sleep(0.005)
        time.sleep(0.1)
        go.set()
        first.join(GATE)
        stopper.join(GATE * 2)
        setattr(_lifecycle, name, real)
        event = _own(sink)
        return f"set_after={event is not None and event.is_set()}"

    child = run_in_child(work, timeout=30)
    expected = "made_set=True set_after=True" if where == "during-the-stop" else "set_after=True"
    assert child.output == expected, child.output


# -- FR-003: a forked child keeps every event, re-initialised in place, and runs no setter --------


@pytest.mark.parametrize("path", ["orphan", "worker"])
def test_a_child_keeps_the_sinks_event_and_runs_no_setter(path: str) -> None:
    """FR-003 AC-1, the ``child-orphan`` and ``child-worker`` rows.

    In the child the setter runs 0 times, the sink holds the parent's event object itself, and the
    child's ``shutdown()`` ends a wait on it. At ``ead40798`` the setter ran once and twice and the
    object differed. That the parent's event stays unset is a sanity line, not proof (plan review,
    Minor 7).
    """
    sink = Holder()
    log_foundry.configure(service="child", sink=sink)
    if path == "orphan":
        log_foundry.info("a")
    else:
        _traced()
        log_foundry.flush(timeout=GATE)
    parent = sink.log_foundry_stop_signal
    before = len(sink.assigned)

    def child() -> str:
        log_foundry.info("child")
        held = sink.log_foundry_stop_signal
        out = _wait_returned(held, BACKOFF)
        time.sleep(0.05)
        log_foundry.shutdown(timeout=3)
        returned = _joined(out)
        return (
            f"setter_calls={len(sink.assigned) - before} same={held is parent} "
            f"wait_returned={returned}"
        )

    run = run_in_child(child, timeout=20)
    assert run.output == "setter_calls=0 same=True wait_returned=[True]", run.output
    assert parent is not None and not parent.is_set()


@pytest.mark.parametrize("where", ["parent", "child"])
@pytest.mark.parametrize("wrapper", ["multi", "filter"])
def test_a_member_holding_a_released_wrappers_event_is_cut_short(wrapper: str, where: str) -> None:
    """FR-003 AC-2, ``orphanwrap.py``'s four rows: the seed walks the events, not the table.

    ``configure(W(A))`` and a log forward W's own event onto A; ``configure(B)`` releases W, which
    is collected while A is kept, so W's table entry is pruned while A still holds W's event. A
    thread waits on A's attribute and ``shutdown()`` is called, in the parent or in a child. Seeded
    by the table, the child's copy of W's event was never re-initialised or set and the wait ran
    4.7 s; seeded by the event set, it returns ``True``.
    """

    def scenario() -> str:
        a, b = Holder(), Holder()
        w: object = MultiSink(a) if wrapper == "multi" else FilteringSink(a)
        log_foundry.configure(service="orphanwrap", sink=w)  # type: ignore[arg-type]
        log_foundry.info("forwards W's event onto A")
        log_foundry.configure(sink=b)
        log_foundry.info("to b")
        del w
        gc.collect()

        def cut() -> str:
            out = _wait_returned(a.log_foundry_stop_signal, BACKOFF)
            time.sleep(0.05)
            log_foundry.shutdown(timeout=3)
            return f"wait_returned={_joined(out)}"

        if where == "parent":
            return cut()
        return run_in_child(cut, timeout=20).output

    run = run_in_child(scenario, timeout=40)
    assert run.output == "wait_returned=[True]", run.output


@pytest.mark.parametrize("row", ["one-stop", "two-stops-second-none", "two-stops-second-finite"])
def test_a_setter_that_shuts_down_never_runs_in_the_fork_repair(row: str) -> None:
    """FR-003 AC-3, ``p9_second_stop_in_fork_repair.py``'s three child rows.

    The setter would call ``shutdown()`` (once, or twice with a second unbounded or bounded) when
    run in a child. At ``ead40798`` the child's fork repair ran it, and the second-unbounded row
    never left the repair. The repair now runs no setter, so the child leaves it with the setter's
    calls empty and returns.
    """
    took: list[float] = []
    home = os.getpid()

    class Shuts(Holder):
        @property
        def log_foundry_stop_signal(self) -> threading.Event | None:
            return self._signal

        @log_foundry_stop_signal.setter
        def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
            self._signal = event
            if os.getpid() != home:
                began = time.monotonic()
                if row == "one-stop":
                    log_foundry.shutdown(timeout=None)
                elif row == "two-stops-second-finite":
                    log_foundry.shutdown(timeout=3.0)
                    log_foundry.shutdown(timeout=3.0)
                else:
                    log_foundry.shutdown(timeout=3.0)
                    log_foundry.shutdown(timeout=None)
                took.append(time.monotonic() - began)

    sink = Shuts()
    log_foundry.configure(service="p9", sink=sink)
    _traced()
    log_foundry.flush(timeout=GATE)
    run = run_in_child(lambda: f"repair returned; setter calls took {took}", timeout=10)
    assert run.output == "repair returned; setter calls took []", run.output


# -- FR-004: the protocol keeps its name, type and promise ---------------------------------------


class ReadmeSink:
    """The README's leaf sink, reduced to its stop-signal contract: one backoff on the signal."""

    def __init__(self) -> None:
        self.log_foundry_stop_signal: threading.Event | None = None
        self.waits: list[bool] = []
        self.entered = threading.Event()

    def emit(self, batch: list[dict[str, object]]) -> None:
        if self.waits:
            return
        delay = BACKOFF
        self.entered.set()
        if self.log_foundry_stop_signal is not None:
            self.waits.append(self.log_foundry_stop_signal.wait(delay))
        else:
            time.sleep(delay)
            self.waits.append(False)

    def close(self) -> None:
        pass


class MyWrapper:
    """The README's wrapper example, verbatim."""

    def __init__(self, inner) -> None:  # type: ignore[no-untyped-def]
        self._inner = inner
        self._stop_signal: threading.Event | None = None

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return self._stop_signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, signal: threading.Event | None) -> None:
        self._stop_signal = signal
        self._inner.log_foundry_stop_signal = signal  # the one line that matters

    def emit(self, batch: list[dict[str, object]]) -> None:
        self._inner.emit(batch)

    def close(self) -> None:
        self._inner.close()


@pytest.mark.parametrize("path", ["orphan", "worker"])
@pytest.mark.parametrize("shape", ["leaf", "wrapper"])
def test_the_readme_sinks_are_cut_short_by_shutdown(shape: str, path: str) -> None:
    """FR-004 AC-2, ``readme_sinks.py``'s four rows: a 1.x sink works unchanged.

    The README's leaf sink backs off ``BACKOFF`` seconds on ``log_foundry_stop_signal`` and its
    wrapper forwards the attribute by a property, configured directly or wrapped, on the orphan
    path and the worker path. ``shutdown()`` ends the backoff: the wait returns ``True``.
    """

    def work() -> str:
        leaf = ReadmeSink()
        sink = leaf if shape == "leaf" else MyWrapper(leaf)
        log_foundry.configure(service="readme", sink=sink)  # type: ignore[arg-type]
        if path == "orphan":
            emitter = threading.Thread(target=lambda: log_foundry.info("x"), daemon=True)
        else:
            emitter = threading.Thread(target=_traced, daemon=True)
        emitter.start()
        if not leaf.entered.wait(GATE):
            return "the sink never backed off"
        log_foundry.shutdown(timeout=BACKOFF * 2)
        emitter.join(GATE)
        return f"waits={leaf.waits}"

    run = run_in_child(work, timeout=30)
    assert run.output == "waits=[True]", run.output


def test_a_member_configured_directly_after_a_multisink_holds_its_own_event() -> None:
    """``twins.py``'s ``multi-member-then-direct`` row, ported from the spec's FR-002 description.

    A, held through ``MultiSink(A, B)``, holds the wrapper's forwarded event; ``configure(A)`` then
    installs A itself, which is handed its own event, and a ``shutdown()`` sets the event A holds.
    """
    a, b = Holder(), Holder()
    multi = MultiSink(a, b)
    log_foundry.configure(service="twins", sink=multi)
    log_foundry.info("through the wrapper")
    assert a.log_foundry_stop_signal is _own(multi)
    log_foundry.configure(sink=a)
    assert a.log_foundry_stop_signal is _own(a), "A holds its own event once installed itself"
    out = _wait_returned(a.log_foundry_stop_signal, BACKOFF)
    log_foundry.shutdown(timeout=GATE)
    assert _joined(out) == [True], out


@pytest.mark.parametrize("path", ["orphan", "worker"])
def test_a_sink_that_reset_its_signal_to_none_is_handed_again_by_a_configure(path: str) -> None:
    """``twins.py``'s ``reset-to-none-then-shutdown`` row, ported from FR-004's description.

    A sink that sets its own attribute to ``None`` is handed again only when a ``configure()``
    installs it again: it then holds its event at its backoff, and ``shutdown()`` cuts it. On the
    worker path that ``configure()`` names the worker's own sink, so ``Worker.retarget`` returns
    ``fenced`` at once, and it offers the sink before it returns (SPEC-098's plan, D-3b).
    """
    sink = Holder()
    log_foundry.configure(service="reset", sink=sink)
    if path == "worker":
        _traced()
        log_foundry.flush(timeout=GATE)
    else:
        log_foundry.info("a")
    sink.log_foundry_stop_signal = None
    log_foundry.info("a late log alone hands nothing")
    assert sink.log_foundry_stop_signal is None
    log_foundry.configure(sink=sink)
    held = sink.log_foundry_stop_signal
    assert held is not None and held is _own(sink)
    out = _wait_returned(held, BACKOFF)
    log_foundry.shutdown(timeout=GATE)
    assert _joined(out) == [True], out


@pytest.mark.parametrize("path", ["orphan", "worker"])
def test_a_sink_armed_inside_held_does_not_break_a_configure(path: str, monkeypatch) -> None:
    """A section that calls ``held()`` over the owed record reads a snapshot of it.

    ``held()`` enters the close registry's lock section, where a signal handler can land and log on
    the same thread, re-entering ``_state._lock`` and arming a sink. SPEC-097's re-entry sweep hit
    it once SPEC-098 deleted the earlier first entry of that section: ``_swap_sink``'s comprehension
    over the live dict raised ``dictionary changed size during iteration`` out of ``configure()``.
    The handler is stood in for by a ``held`` that arms a fresh sink the first time it is asked.
    """
    first, other, installed = Holder(), Holder(), Holder()
    log_foundry.configure(service="snapshot", sink=first)
    if path == "worker":
        _traced()
        log_foundry.flush(timeout=GATE)
    else:
        log_foundry.info("arms the first sink")
    real = _lifecycle._state.held
    armed: list[bool] = []

    def held(sink: object) -> bool:
        if not armed:
            armed.append(True)
            _lifecycle._state._closes.arm(other)  # type: ignore[arg-type]
        return real(sink)  # type: ignore[arg-type]

    monkeypatch.setattr(_lifecycle._state, "held", held)
    log_foundry.configure(sink=installed)
    monkeypatch.undo()
    assert armed == [True], "held() was never asked"
    log_foundry.shutdown(timeout=GATE)


@pytest.mark.parametrize("act", ["flush", "shutdown", "info", "configure", "interrupt"])
def test_a_setter_the_drain_thread_runs_returns_when_it_calls_back(act: str) -> None:
    """A setter run on the drain thread that calls back into the library returns (diff review m-2).

    Since SPEC-098 the drain thread offers the worker's sink before each batch, so a setter can run
    there: thread A's traced call builds the worker on S and S's first setter call parks; thread B's
    traced call and ``flush()`` make the drain thread deliver B's span, and the drain thread's
    offer runs S's setter, which makes the call ``act`` names. Each returns, the call made from a
    ``log-foundry-`` thread being merged, deferred or answered as such a call always is, and a
    final ``shutdown()`` returns. ``interrupt``: the setter raises ``KeyboardInterrupt`` on the
    drain thread, which ends the worker with the stop reason that names it, as an ``emit`` raising
    one there does.
    """

    def work() -> str:
        parked, release = threading.Event(), threading.Event()
        outcome: list[str] = []

        class Calls(Holder):
            @property
            def log_foundry_stop_signal(self) -> threading.Event | None:
                return self._signal

            @log_foundry_stop_signal.setter
            def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
                name = threading.current_thread().name
                self.assigned.append(event)
                if len(self.assigned) == 1:
                    parked.set()
                    release.wait(GATE)
                elif name == "log-foundry-worker" and not outcome:
                    outcome.append("entered")
                    if act == "flush":
                        log_foundry.flush(timeout=GATE)
                    elif act == "shutdown":
                        log_foundry.shutdown(timeout=GATE)
                    elif act == "info":
                        log_foundry.info("from the setter")
                    elif act == "configure":
                        log_foundry.configure(sink=Holder())
                    else:
                        raise KeyboardInterrupt
                    outcome.append("returned")
                self._signal = event

        sink = Calls()
        log_foundry.configure(service="drain-setter", sink=sink)
        a = threading.Thread(target=_traced, daemon=True)
        a.start()
        if not parked.wait(GATE):
            return "the build's setter never parked"

        def second() -> None:
            _traced()
            log_foundry.flush(timeout=GATE)

        b = threading.Thread(target=second, daemon=True)
        b.start()
        b.join(GATE * 2)
        release.set()
        a.join(GATE)
        log_foundry.shutdown(timeout=GATE)
        reason = log_foundry.health().stopped_reason
        return f"outcome={outcome} b_returned={not b.is_alive()} stopped_reason={reason}"

    run = run_in_child(work, timeout=40)
    if act == "interrupt":
        expected = "outcome=['entered'] b_returned=True stopped_reason=KeyboardInterrupt"
    else:
        expected = "outcome=['entered', 'returned'] b_returned=True stopped_reason=None"
    assert run.output == expected, f"{run.ending}: {run.output!r}"


def test_a_sink_at_a_collected_sinks_address_is_handed_its_own_event() -> None:
    """``offer_once`` reads a handed entry as this sink's only when its referent is this sink.

    Diff review frame 2, MAJOR-1, ported from ``p_idreuse.py``. 400 sinks are each configured,
    logged to and so handed, then released and collected; their entries stay in the table until
    the next first entry prunes them. A new sink allocated at one of their addresses must still be
    offered: a lock-free check that took the dead entry's handed mark for it left it holding no
    event (372 of the probe's sinks with the identity check replaced by ``return False``). Each
    reused-address sink is asserted for itself, the reason named; a run in which the allocator
    reuses no address proves nothing, so it skips rather than passes.
    """
    count = 400
    first = [Holder() for _ in range(count)]
    for sink in first:
        log_foundry.configure(service="idreuse", sink=sink)
        log_foundry.info("hands it")
    addresses = {id(sink) for sink in first}
    log_foundry.configure(sink=Holder())
    del first, sink
    gc.collect()
    dead_handed = [
        key
        for key in addresses
        if key in _lifecycle._state._signals
        and _lifecycle._state._signals[key][2]
        and _lifecycle._referent(_lifecycle._state._signals[key][0]) is None
    ]
    second = [Holder() for _ in range(count)]
    reused = [sink for sink in second if id(sink) in dead_handed]
    if not reused:
        pytest.skip("no new sink was allocated at a collected, handed sink's address")
    for sink in reused:
        log_foundry.configure(sink=sink)
        log_foundry.info("its first delivery")
        assert sink.log_foundry_stop_signal is not None, (
            "a sink at a reused address was taken for the dead sink's handed entry and holds no event"
        )
        assert sink.log_foundry_stop_signal is _own(sink)


def test_a_first_offer_interrupted_between_its_two_writes_still_leaves_a_settable_event() -> None:
    """``_stable_signal`` adds the event to the set before the entry names it (frame 2, MINOR-2).

    Ported from ``p_window.py``'s interrupt row. A ``KeyboardInterrupt`` is raised, through
    ``sys.settrace`` scoped to ``_stable_signal``, at the second of its two writes, the entry and
    the set add, whose lines are read from its source, so the interrupt lands with exactly one of
    them made. The sink's next delivery hands it an event, and the next stop must set that event.
    With the entry written first, the interrupt left an entry whose event was in no set, the next
    delivery handed that event, and no stop ever set it.
    """
    source = inspect.getsource(_lifecycle._stable_signal)
    first_line = _lifecycle._stable_signal.__code__.co_firstlineno
    tree = ast.parse(textwrap.dedent(source))
    writes = []
    for node in ast.walk(tree):
        text = ast.unparse(node) if isinstance(node, ast.stmt) else ""
        if isinstance(node, ast.Assign) and text.startswith("_state._signals[key] ="):
            writes.append(node.lineno)
        if isinstance(node, ast.Expr) and text.startswith("_state._signal_events.add("):
            writes.append(node.lineno)
    assert len(writes) == 2, writes
    target = first_line + max(writes) - 1
    code = _lifecycle._stable_signal.__code__

    def tracer(frame: object, event: str, arg: object) -> object:
        if getattr(frame, "f_code", None) is not code:
            return None

        def local(frame: object, event: str, arg: object) -> object:
            if event == "line" and getattr(frame, "f_lineno", None) == target:
                raise KeyboardInterrupt
            return local

        return local

    sink = Holder()
    log_foundry.configure(service="window", sink=sink)
    previous = sys.gettrace()
    sys.settrace(tracer)  # type: ignore[arg-type]
    try:
        with pytest.raises(KeyboardInterrupt):
            _lifecycle.offer_once(sink)
    finally:
        sys.settrace(previous)
    log_foundry.info("the next delivery hands it an event")
    held = sink.log_foundry_stop_signal
    assert held is not None
    log_foundry.shutdown(timeout=GATE)
    assert held.is_set(), "the next stop left the event an interrupted first offer made unset"
