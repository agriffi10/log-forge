"""Shared fixtures for the log-foundry test suite.

Imports here are plain: a `log_foundry` module that will not import is an error, not a
skipped file. See `tests/README.md` for the two things that may still legitimately skip.
"""

import asyncio
import contextlib
import gc
import sys
import threading
import time
from typing import ClassVar

import pytest


def run_concurrently(work, threads: int, *, per_thread: int = 1) -> list[BaseException]:
    """Run ``work`` on N threads that start together, and return whatever it raised.

    The shared concurrent-emitter helper (SPEC-028 FR-004). Threads rendezvous on a
    ``threading.Barrier`` so they enter ``work`` at the same instant — a barrier is a real
    synchronization primitive, unlike the ``sleep`` FR-004 forbids, so the overlap does not
    depend on how fast the machine is. The switch interval is tightened for the duration to
    widen the window in which CPython preempts a read-modify-write; without it a lost increment
    is real but rare enough that a race test would pass against the bug it exists to catch.

    Args:
      work: Called as ``work(thread_index, iteration)``. Exceptions are captured, not raised in
        the worker thread where they would be discarded.
      threads: How many threads to run.
      per_thread: How many times each thread calls ``work``.

    Returns:
      Every exception raised across all threads, in completion order. Empty means all calls
      returned normally. Assert on this rather than trusting the threads were silent.

    Raises:
      None.
    """
    barrier = threading.Barrier(threads)
    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def runner(index: int) -> None:
        barrier.wait()
        for iteration in range(per_thread):
            try:
                work(index, iteration)
            except BaseException as exc:
                with errors_lock:
                    errors.append(exc)

    pool = [threading.Thread(target=runner, args=(i,)) for i in range(threads)]
    original_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for thread in pool:
            thread.start()
        for thread in pool:
            thread.join(timeout=30)
    finally:
        sys.setswitchinterval(original_interval)
    assert not any(thread.is_alive() for thread in pool), "a worker thread did not finish"
    return errors


class Stopwatch:
    """A ``time.monotonic()`` clock read when it is built, for a lower bound on elapsed time.

    It exists so a timing floor and the delay behind it share one start: a delay armed after the
    stopwatch is built cannot end before ``began + delay``, whatever the machine does between
    the two. :func:`release_after_clock` builds one and only then arms its timer.

    Attributes:
      began: The ``time.monotonic()`` instant the stopwatch was built.
      ended: The instant :meth:`stop` was called, or None while it runs.
    """

    def __init__(self) -> None:
        """Reads the clock.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.began = time.monotonic()
        self.ended: float | None = None

    def stop(self) -> float:
        """Stops the stopwatch, once, and returns the elapsed seconds.

        Args:
          None.

        Returns:
          The seconds from ``began`` to the first ``stop``.

        Raises:
          None.
        """
        if self.ended is None:
            self.ended = time.monotonic()
        return self.elapsed

    @property
    def elapsed(self) -> float:
        """Returns the seconds since ``began``, up to ``ended`` once it has stopped.

        Args:
          None.

        Returns:
          The elapsed seconds.

        Raises:
          None.
        """
        end = time.monotonic() if self.ended is None else self.ended
        return end - self.began


@contextlib.contextmanager
def release_after_clock(seconds: float, release):
    """Starts a stopwatch, then arms a timer that calls ``release`` ``seconds`` later.

    The shared helper for a test that bounds elapsed time from below. A floor ``X <= elapsed``
    holds only if the delay it measures opens **after** the test's clock does: a fake's timer
    armed in its constructor, a close that sleeps from the moment a test sees it enter, or a
    timer started on the line before the clock all open first, and any preemption between the
    two is subtracted from ``elapsed``. That class of flake recurred six times here (the orphan
    handoff, the NATS ``SlowFlusherNATS`` fake and the four sites PR 314 fixed). Gate the slow
    thing on an Event the fake waits on, and pass that Event's ``set`` here; the floor is then
    exact by construction, because the clock is read before the timer exists.

    On exit the stopwatch stops first, and then ``release`` is called if the timer has not
    called it yet, so a measured call that returned early (the bug a floor catches) never leaves
    the fake parked for a teardown to wait on. ``release`` runs exactly once either way.
    ``tests/test_timing_floors.py`` refuses the unsafe shapes this replaces.

    Args:
      seconds: The delay, from the stopwatch's start, before ``release`` is called.
      release: Called with no arguments, once, on the timer's thread or at exit.

    Returns:
      A context manager yielding the running :class:`Stopwatch`; read its ``elapsed`` after the
      block, where it is the measured call's duration.

    Raises:
      None.
    """
    watch = Stopwatch()
    once = threading.Lock()

    def fire() -> None:
        if once.acquire(blocking=False):
            release()

    timer = threading.Timer(seconds, fire)
    timer.daemon = True
    timer.start()
    try:
        yield watch
    finally:
        watch.stop()
        timer.cancel()
        fire()


class DelayArmedOnUse:
    """A loop delay a fake arms from inside the call a test times, never from its constructor.

    The asyncio twin of :func:`release_after_clock`, for a fake whose delay is its own (a
    ``call_later`` that unparks a flusher, say) rather than one the test releases. A sink that
    connects in its constructor builds its fake there too, so a ``call_later`` in the fake's
    ``__init__`` is already running before the test's clock starts. ``arm`` is called from the
    fake's first operation inside the measured call, and only the first call arms.

    Attributes:
      seconds: The delay from the first :meth:`arm`.
      armed: Whether :meth:`arm` has scheduled the delay.
    """

    def __init__(self, seconds: float, fire) -> None:
        """Records the delay without scheduling it.

        Args:
          seconds: The delay from the first :meth:`arm`.
          fire: Called with no arguments on the loop when the delay ends.

        Returns:
          None.

        Raises:
          None.
        """
        self.seconds = seconds
        self.armed = False
        self._fire = fire

    def arm(self) -> None:
        """Schedules the delay on the running loop, the first time only.

        Args:
          None.

        Returns:
          None.

        Raises:
          RuntimeError: When called with no running loop.
        """
        if not self.armed:
            self.armed = True
            asyncio.get_running_loop().call_later(self.seconds, self._fire)


CPU_SCOPES = {"thread": time.thread_time, "process": time.process_time}
"""The CPU clocks :func:`cpu_window` reads, by the scope a test claims."""

_collection_pause = threading.Lock()
_collection_holders = [0, False]
"""How many windows are open, and whether automatic collection was on when the first opened."""


class CpuWindow:
    """The CPU seconds one :func:`cpu_window` measured, readable once its block exits.

    Attributes:
      scope: ``"thread"`` or ``"process"``, the clock the window read.
    """

    def __init__(self, scope: str) -> None:
        """Records the scope; the window sets the reading on exit.

        Args:
          scope: The key into :data:`CPU_SCOPES`.

        Returns:
          None.

        Raises:
          None.
        """
        self.scope = scope
        self._spent: float | None = None

    @property
    def spent(self) -> float:
        """Returns the CPU seconds between the window's entry and exit.

        Read inside the block it would be no reading at all, so it raises rather than returning
        a zero that every bound passes.

        Args:
          None.

        Returns:
          The seconds.

        Raises:
          RuntimeError: When the window is still open.
        """
        if self._spent is None:
            raise RuntimeError("cpu_window: spent is read before the window closed")
        return self._spent


def _pause_collection() -> None:
    """Turns automatic garbage collection off for the first open window, counting the rest.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    with _collection_pause:
        if _collection_holders[0] == 0:
            _collection_holders[1] = gc.isenabled()
            gc.disable()
        _collection_holders[0] += 1


def _resume_collection() -> None:
    """Restores automatic garbage collection as the last open window found it.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    with _collection_pause:
        _collection_holders[0] -= 1
        if _collection_holders[0] == 0 and _collection_holders[1]:
            gc.enable()


@contextlib.contextmanager
def cpu_window(scope: str = "thread"):
    """Measures the CPU time a block spends, on the scope it names, with collection paused.

    The shared helper for an upper bound on CPU time, the assertion invariant 3 asks for because
    a busy-spin satisfies a wall-clock bound. Two things outside a test's claim used to count
    against such a bound, and this removes both.

    **Garbage collection.** A full collection in a pytest-xdist worker cost about 0.03 to 0.16 s
    of CPU on average, and up to 0.7 s, in two ``pytest tests -n 12`` runs at ``c810333d`` with a
    ``gc.callbacks`` probe, in which each worker ran 10 to 93 of them. CPython charges it to
    whichever thread allocated past the threshold, and a 0.05 s bound read process-wide flaked
    at 0.104 s on CI. Automatic collection is off for the window, counted across windows open
    at once on several threads, and restored as the first window found it when the last one
    closes. Nothing the library does depends on a collection running. A thread that never leaves
    its window leaves collection off for the rest of that worker's run, which costs memory, not
    a result. Two edges are outside the count: another thread calling ``gc.enable()`` or
    ``gc.disable()`` while a window is open (nothing under ``src/`` or ``tests/`` does), and a fork
    while a window is open on another thread, whose child inherits collection off.

    **Other threads.** ``"thread"`` reads ``time.thread_time()``, so only the calling thread's CPU
    counts: a library thread leaked by an earlier test, alive at the start of many tests in an
    xdist worker, or pytest's own threads cannot reach it. Use it when every thread that could
    spin in the window is this one: the caller, with the drain thread wedged inside a fake or not
    involved. ``"process"`` reads ``time.process_time()``, for a window where the thread that
    could spin is one the test cannot run the window on, such as a drain thread's retry loop. It
    still counts every other thread in the process, so prefer ``"thread"`` wherever the claim
    allows; in a forked child the two differ only by the threads the child started.

    ``tests/test_timing_floors.py`` refuses a CPU reading anywhere else under ``tests/``.

    Args:
      scope: ``"thread"`` (the default) or ``"process"``.

    Returns:
      A context manager yielding a :class:`CpuWindow`, whose ``spent`` reads once the block exits.

    Raises:
      KeyError: When ``scope`` is not a key of :data:`CPU_SCOPES`.
    """
    clock = CPU_SCOPES[scope]
    window = CpuWindow(scope)
    _pause_collection()
    try:
        began = clock()
        try:
            yield window
        finally:
            window._spent = clock() - began
    finally:
        _resume_collection()


def install_worker(worker) -> None:
    """Installs ``worker`` as the process worker **and arms its sink**, as ``_get_worker`` does.

    A test that assigns ``_lifecycle._state._worker`` directly — which is how it gets a worker
    with batching settings the library would never choose — used to be a complete install. Since
    SPEC-054 FR-002 a worker's build also **arms** its sink in the owed-close record, in the same
    critical section, and that is what `shutdown()` and `flush()` both read. Assigning the slot
    alone leaves a worker whose sink nothing owes a close and nothing will flush.

    The three lines below are `_get_worker`'s — the slot, the arm and, since SPEC-078, the move
    of the stored state that ends that critical section — which is a fixture reproducing production setup
    rather than a production expectation: what a test then asserts still comes from the library.
    A test that wants the *unarmed* state asserts on the record itself and does not use this.

    Args:
      worker: The worker to install.

    Returns:
      None.

    Raises:
      None.
    """
    from log_foundry import _lifecycle

    with _lifecycle._state._lock:
        _lifecycle._state._worker = worker
        _lifecycle._state._closes.arm(worker.sink)
        _lifecycle._state._move()


def retire_process(worker=None, timeout: float | None = 30.0) -> None:
    """Runs the library's real shutdown, optionally installing ``worker`` as the process worker.

    SPEC-054 FR-001/FR-003 moved retirement and every close off ``Worker``: its ``stop()`` ends
    the drain thread and nothing more, while the retirement count and the owed-close record are
    the lifecycle owner's. So a test about anything a *process* shutdown does — a sink being
    closed, ``submitted_after_shutdown`` moving, the closer grace — drives
    ``_lifecycle._shutdown_worker``, which is the function ``log_foundry.shutdown()`` and the
    ``atexit`` handler both bind.

    Installing the worker through :func:`install_worker` is what makes a hand-built one visible
    to that call, sink included — a shutdown closes what is **owed**, and a worker assigned into
    the slot without arming owes nothing. It installs **once**: a worker already in the slot is
    left alone, because arming is what a worker's *build* does and re-arming on every call would
    hand a second close to a sink the previous call already closed, making every idempotence
    assertion here read 2 where it should read 1. A helper that moved the count and closed the sinks
    itself would reproduce the production expression and pass against a broken one, which is the
    whole hazard; this runs the shipped path instead.

    Args:
      worker: A worker to install as the process worker first, or ``None`` to shut down whatever
        the process already has.
      timeout: Seconds for the drain and the closer grace, or ``None``.

    Returns:
      None.

    Raises:
      None.
    """
    from log_foundry import _lifecycle

    if worker is not None and _lifecycle._state.worker_exists() is not worker:
        install_worker(worker)
    _lifecycle._shutdown_worker(timeout)


@pytest.fixture(autouse=True)
def _reset_config():
    """Restore a fresh global ``Config`` before each test.

    ``log_foundry.config._config`` is a process-wide singleton mutated by ``configure()``;
    without this, tests would leak identity/sink/defaults into one another and only pass by
    ordering. No-ops until the module exists.

    It also clears the ``contextvars`` state, which is a *different* singleton and was leaking
    the same way. Baggage set with no span open is a process-level default and nothing releases
    it (SPEC-024, arch §5.1) — so one test's ``set_baggage`` reached later tests' events, and
    two `test_trace_continuation` cases failed only when run after a new file that set baggage,
    passing in isolation. That is SPEC-024's own finding, reproduced between two tests.
    """
    try:
        from log_foundry import config, context
    except ImportError:
        yield
        return
    config._config = config.Config()
    context.reset_context()
    _reset_worker()
    yield
    # Tear down any process worker a test created so its daemon thread / sink don't leak.
    _reset_worker()
    context.reset_context()


def _reset_worker() -> None:
    """Drain and clear the lazily-created process worker (SPEC-004), if present.

    Without this the first test's sink/thread would leak into later tests through the
    module-global ``_lifecycle._state._worker``. No-ops until the worker module exists.

    The names moved to ``_lifecycle._state`` in SPEC-040, and the reset is now **unconditional**
    where it used to be guarded by ``hasattr``. A guard that stops matching stops resetting, and
    the failure is diffuse: breaking one guard fails 2 tests, breaking every guard fails 74, all
    of them somewhere other than here. An attribute that has moved should raise on this line,
    naming itself.

    SPEC-031 FR-006's three flags are cleared alongside it, for the same reason and one more:
    ``retirements`` is what ``health()`` reads for ``retired``, so a test
    that calls ``shutdown()`` would otherwise make every later test read ``retired=True``, and
    the owed record would make "nothing was ever logged, so nothing is closed"
    untestable in-process. ``_atexit_registered`` is deliberately **not** cleared — the
    handler really is registered for the life of the interpreter, and re-arming the flag would
    have the next worker register a second one.

    SPEC-033 adds three more. ``_stop`` is replaced rather than cleared, since an
    ``Event`` cannot be un-set and a test that called ``shutdown()`` would otherwise leave every
    later test's sink backing off not at all. SPEC-054 made it and ``retirements`` the process's
    only pair, so resetting the count to zero is what makes ``retired`` false again and what puts
    a worker built by the next test back in step with it — a count left raised makes every later
    worker read as retired at its own build, which is a suite that delivers nothing. The close
    registry's in-flight half (``_closers`` before SPEC-078) is process-global, so a hung closer
    from one test — the capped-grace tests create them deliberately — would
    otherwise leak a non-zero ``closing_sinks`` into the next.

    SPEC-044 adds three more, and each would leak in a different direction. A
    ``_shutdown_running`` left raised by a test that injected a preemption point makes the
    **next** test's first ``@trace`` register a late worker for a shutdown that ended long ago,
    and hand it a discharged close. A ``_late_worker`` left set pins a worker object and hands
    the next ``shutdown()`` something to drain that it does not own. And an in-flight entry left
    holding an id makes the next test's sink miss its stop-signal refresh — silently, since the
    symptom is a backoff that does not happen rather than an error.

    SPEC-036 FR-003 adds the two loss counters. They are cumulative for the life of the process
    and ``health()`` synthesizes them whether or not a worker exists, so a single test that loses
    an event would otherwise leave every later test reading a non-zero ``orphan_lost`` — the exact
    failure this fixture's first paragraph describes, on a field whose whole purpose is to be
    believed.

    SPEC-050 FR-002 added the orphan close's in-flight count and its idle gate, and that pair
    leaked the most quietly of any here: three tests leave a daemon thread mid-``close()``, so the
    count stayed raised and the gate stayed clear, and every later caller that found nothing owed
    paid the whole closer grace. Nothing errored — the suite was green only because those three
    happen to sit *after* the test that measures the no-wait path. SPEC-054 FR-003 replaced both
    with a per-sink registration (the registry's in-flight half since SPEC-078), which was already
    cleared here, so the pair
    is gone rather than merely reset: a caller now waits only on closes actually in flight against
    a sink, and a leaked entry shows up as that sink never being closed rather than as a
    process-wide charge.

    SPEC-078 stores the settled state, so the reset ends by re-settling it through ``_move``
    over the fields it has just cleared, which is ``UNCONFIGURED``: a test that left a worker or
    a retirement behind would otherwise leave the next one reading ``RUNNING`` or ``CLOSED``.

    SPEC-077 adds the two token sets ``state`` reads: a test that injects an exception past a
    ``finally`` it then bypasses, or sets a token directly, would otherwise leave every later test
    reading ``SWAPPING`` or ``DRAINING``.

    SPEC-054 also merged four records into one owed record (``_state._closes.owed`` since
    SPEC-078), so one ``clear()`` replaces the four
    lines that reset them, and ``retirements`` replaces the retirement flag: it is a **count**, so
    resetting it to zero is what puts a worker built by the next test back in step with it — one
    left raised makes every later worker read as retired at its own build, which is a suite that
    delivers nothing.

    SPEC-079 adds the ownership record and its failure flag, which only
    ``tests/test_sink_ownership.py`` cleared until then. The record is process-global and, before
    SPEC-081, held a strong reference to every sink any earlier test stamped, and a forked child
    walks every live entry as a record seed of its marking, bounded by ``_MARKING_CEILING``. Left
    uncleared it grows for the whole session in each xdist worker: measured at ``44db79a6``, one
    worker ended a full run at 92,940 marking visits, and a test that configured a ``MemorySink``
    holding 60,000 events made the next test's child trip the ceiling and announce it, which is
    how ``test_a_child_of_a_process_that_built_no_worker_is_silent`` failed on ``44db79a6``.
    SPEC-081 bounds the record to what is alive, and the clear stays as isolation: a sink an
    earlier test still holds would otherwise keep its entry. Its references are dropped after the
    record lock is released, so a finalizer they run meets no lock, and the three once-only
    announcement flags SPEC-081 adds are reset with it.
    """
    try:
        from log_foundry import _lifecycle, decorator
    except ImportError:
        return
    worker = _lifecycle._state.worker_exists()
    if worker is not None:
        worker.stop()
        _lifecycle._state._worker = None
    _lifecycle._state._closes.owed.clear()
    _lifecycle._state.retirements = 0
    _lifecycle._state._stop = threading.Event()
    _lifecycle._state._shutdown_running = 0
    _lifecycle._state._late_worker = None
    _lifecycle._state._signal_owed = False
    _lifecycle._state._swaps_running.clear()
    _lifecycle._state._stops_running.clear()
    _lifecycle._state._hands_running.clear()
    _lifecycle._state._handing_here = threading.local()
    _lifecycle._state._building_here = threading.local()
    decorator._orphan_lost = 0
    decorator._in_span_lost = 0
    with _lifecycle._state._closes._lock:
        _lifecycle._state._closes._in_flight.clear()
    with _lifecycle._state._owned_lock:
        dropped = dict(_lifecycle._state._owned)
        _lifecycle._state._owned.clear()
    del dropped
    _lifecycle._state._marking_failed = False
    _lifecycle._state._capture_announced = False
    _lifecycle._state._stamp_refusal_announced = False
    _lifecycle._state._repin_announced = False
    with _lifecycle._state._lock:
        _lifecycle._state._move()


class FakeSink:
    """A Sink that records emitted batches so tests can assert on the event dicts.

    This is the test double from the guide: it exercises the part you wrote (span
    lifecycle, IDs, schema, context) without the part you didn't (the network).
    """

    def __init__(self) -> None:
        self.batches: list[list[dict]] = []

    def emit(self, batch: list[dict]) -> None:
        self.batches.append(list(batch))

    def close(self) -> None:
        pass

    @property
    def events(self) -> list[dict]:
        """All emitted events, flattened across batches."""
        return [event for batch in self.batches for event in batch]


@pytest.fixture
def fake_sink() -> FakeSink:
    return FakeSink()


@pytest.fixture
def lf(fake_sink: FakeSink, monkeypatch):
    """`log_foundry` configured with a FakeSink, flushing synchronously.

    SPEC-004 rewired the decorator to hand finished spans to a background worker, so
    `fake_sink.events` would no longer be populated by the time a pipeline test asserts.
    These tests care about span/event/context semantics, not delivery timing, so we keep
    flushing inline here (the synchronous-flush test mode this fixture always anticipated);
    the worker's own batching/retry/backpressure/shutdown behavior is covered directly in
    `test_worker.py`.
    """
    import log_foundry

    log_foundry.configure(service="test", version="0.0.0", env="test", sink=fake_sink)

    from log_foundry import decorator
    from log_foundry.config import _ensure_sink

    def _sync_flush(span) -> None:
        _ensure_sink().emit(span.events)

    monkeypatch.setattr(decorator, "_flush", _sync_flush)
    return log_foundry


# -- hostile inbound values (SPEC-067) ------------------------------------------------------------
#
# Shared by `test_diag.py`, `test_ids.py` and `test_inbound_totality.py`, so "a spy on every
# method" has one definition rather than three that drift. Every hook records its name and, when
# asked to, raises — but only while `armed()` is open. Outside it each hook behaves as the base
# type's own method, because pytest itself reads these values when it reports a failure: its
# `saferepr` reads `cls.__name__`, and an always-raising metaclass turns any failing test into an
# INTERNALERROR (the reason `_NamelessMeta` in `test_baggage_header.py` is armed the same way).


class Hostility:
    """The switch and the record every probe shares: armed or not, and which hooks ran."""

    armed: ClassVar[bool] = False
    calls: ClassVar[list[str]] = []


@contextlib.contextmanager
def armed():
    """Arms every probe for the body, and always disarms it before any assertion runs."""
    Hostility.calls.clear()
    Hostility.armed = True
    try:
        yield Hostility.calls
    finally:
        Hostility.armed = False


def _hook(name: str, raises: bool, fallback):
    """One recording hook: records ``name`` and maybe raises while armed, else defers."""

    def hook(self, *args, **kwargs):
        if Hostility.armed:
            Hostility.calls.append(name)
            if raises:
                raise RuntimeError(f"{name}-secret")
        return fallback(self, *args, **kwargs)

    hook.__name__ = name
    return hook


_NOT_HOOKED = frozenset(
    {
        "__class__",
        "__delattr__",
        "__dir__",
        "__doc__",
        "__getnewargs__",
        "__getstate__",
        "__init__",
        "__init_subclass__",
        "__new__",
        "__reduce__",
        "__reduce_ex__",
        "__setattr__",
        "__sizeof__",
        "__subclasshook__",
    }
)


def str_probe(text: str, raising=frozenset(), name: str = "_S") -> str:
    """A ``str`` subclass instance holding ``text``, with a spy on every method ``str`` has.

    Every callable in ``dir(str)`` but the construction and pickling hooks is overridden,
    ``__getattribute__`` included, so an attribute read is recorded as well as a call; those
    named in ``raising`` also raise while armed, and ``raising="all"`` makes every one raise.
    ``__hash__`` is overridden beside ``__eq__``, so the instance stays hashable.
    """
    hooked = [a for a in dir(str) if a not in _NOT_HOOKED and callable(getattr(str, a))]
    if raising == "all":
        raising = frozenset(hooked)
    namespace = {attr: _hook(attr, attr in raising, getattr(str, attr)) for attr in hooked}
    unknown = set(raising) - set(namespace)
    assert not unknown, f"not a hookable str method: {unknown}"
    return type(name, (str,), namespace)(text)


def _absent(self, *args, **kwargs):
    """What an object without the protocol does when it is asked for it."""
    raise TypeError("not supported")


_OBJECT_HOOKS = {
    "__repr__": object.__repr__,
    "__str__": object.__str__,
    "__format__": object.__format__,
    "__eq__": object.__eq__,
    "__ne__": object.__ne__,
    "__hash__": object.__hash__,
    "__getattribute__": object.__getattribute__,
    "__bool__": lambda self: True,
    "__len__": _absent,
    "__iter__": _absent,
    "__getitem__": _absent,
    "__index__": _absent,
    "__contains__": _absent,
}


def object_probe(name: str, raising=frozenset(), class_property=None, meta: bool = False):
    """An instance of a fresh class named ``name`` whose every protocol hook is a spy.

    ``class_property="raise"`` adds a ``__class__`` property that raises while armed, and
    ``"str"`` one that answers ``str`` while armed: the two shapes that make
    ``isinstance(v, str)`` raise, or lie (Probe F). ``meta=True`` gives the class a metaclass
    whose ``__getattribute__``, ``__name__`` property, ``__repr__``, ``__instancecheck__`` and
    ``__subclasscheck__`` record and raise while armed, and otherwise answer as ``type`` does.
    """
    namespace = {
        attr: _hook(attr, attr in raising, fallback) for attr, fallback in _OBJECT_HOOKS.items()
    }
    if class_property is not None:

        def spoof(self):
            if Hostility.armed:
                Hostility.calls.append("__class__")
                if class_property == "raise":
                    raise RuntimeError("__class__-secret")
                return str
            return type(self)

        namespace["__class__"] = property(spoof)
    unknown = set(raising) - set(namespace)
    assert not unknown, f"not a hookable object method: {unknown}"
    if not meta:
        return type(name, (), namespace)()

    def meta_name(cls):
        if Hostility.armed:
            Hostility.calls.append("meta.__name__")
            raise RuntimeError("meta-name-secret")
        return type.__dict__["__name__"].__get__(cls)

    metaclass = type(
        "_SpyMeta",
        (type,),
        {
            "__getattribute__": _hook("meta.__getattribute__", True, type.__getattribute__),
            "__repr__": _hook("meta.__repr__", True, type.__repr__),
            "__instancecheck__": _hook("meta.__instancecheck__", True, type.__instancecheck__),
            "__subclasscheck__": _hook("meta.__subclasscheck__", True, type.__subclasscheck__),
            "__name__": property(meta_name),
        },
    )
    return metaclass(name, (), namespace)()


def fan_out() -> object:
    """`y = 0`, then `y = [y] * 16` seven times: 16^7 paths to one leaf (SPEC-064 case (a))."""
    shape: object = 0
    for _ in range(7):
        shape = [shape] * 16
    return shape


def join_graced(seconds: float | None = 5.0) -> None:
    """Gives every detached close in flight the capped grace a shutdown would, then returns.

    The drop-in for the tests' ``_lifecycle.join_closers(time.monotonic() + seconds)``, which
    SPEC-078 replaced with the close registry's graced wait. It builds an operation of its own, so
    the wait is bounded by the smaller of ``seconds`` and ``DEFAULT_CLOSER_GRACE``, exactly as the
    old call was.

    Args:
      seconds: The budget the grace is carved from, or ``None`` for the cap alone.

    Returns:
      None.

    Raises:
      None.
    """
    from log_foundry import _lifecycle

    _lifecycle._state._closes.wait_graced(_lifecycle._Operation(_lifecycle._Op.STOP, seconds), None)


@pytest.fixture
def no_close_left_in_flight():
    """Asserts, after the test, that no close it started is left for nobody (SPEC-078 FR-001).

    A close is discharged by its own thread, which may still be finishing when the test body
    returns, so this polls for up to five seconds. An entry still there after that is a leak
    unless a live thread is still performing it: a test may abandon a stuck detached close on
    purpose (the grace's whole point), and that close discharges its entry when it ends. An entry
    with no thread, or whose thread has ended, is one nothing will ever discharge — the sink is
    then skipped by the signal refresh forever and every later bystander waits out the grace.

    Yields:
      None.
    """
    from log_foundry import _lifecycle

    def orphaned() -> dict[int, object]:
        with _lifecycle._state._closes._lock:
            return {
                key: close
                for key, close in _lifecycle._state._closes._in_flight.items()
                if not any(thread.is_alive() for thread in close.threads)
            }

    yield
    deadline = time.monotonic() + 5.0
    while orphaned() and time.monotonic() < deadline:
        time.sleep(0.01)
    left = orphaned()
    assert not left, f"a close was left in flight, and nothing will discharge it: {left}"


def count_grace_writes(monkeypatch) -> dict[int, int]:
    """Counts, per ``shutdown()`` operation, how many times its ``grace_end`` was written.

    SPEC-078 FR-002's counting hook. It wraps ``_CloseRegistry._open_grace`` and compares the
    operation's ``grace_end`` before and after each call, so it counts **writes**, not calls: an
    ``_open_grace`` that reset the instant on every call — a grace computed per wait — shows as
    two writes, where a call that found the instant already fixed shows as none.

    Args:
      monkeypatch: The test's ``monkeypatch`` fixture.

    Returns:
      A live mapping from ``id(operation)`` to the number of writes it saw.

    Raises:
      None.
    """
    from log_foundry import _lifecycle

    writes: dict[int, int] = {}
    real = _lifecycle._CloseRegistry._open_grace

    def counting(self, op):
        before = op.grace_end
        end = real(self, op)
        writes.setdefault(id(op), 0)
        if op.grace_end is not before:
            writes[id(op)] += 1
        return end

    monkeypatch.setattr(_lifecycle._CloseRegistry, "_open_grace", counting)
    return writes
