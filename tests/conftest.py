"""Shared fixtures for the log-foundry test suite.

Imports here are plain: a `log_foundry` module that will not import is an error, not a
skipped file. See `tests/README.md` for the two things that may still legitimately skip.
"""

import sys
import threading

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


def install_worker(worker) -> None:
    """Installs ``worker`` as the process worker **and arms its sink**, as ``_get_worker`` does.

    A test that assigns ``_lifecycle._state._worker`` directly — which is how it gets a worker
    with batching settings the library would never choose — used to be a complete install. Since
    SPEC-054 FR-002 a worker's build also **arms** its sink in the owed-close record, in the same
    critical section, and that is what `shutdown()` and `flush()` both read. Assigning the slot
    alone leaves a worker whose sink nothing owes a close and nothing will flush.

    The two lines below are `_get_worker`'s, which is a fixture reproducing production setup
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
        _lifecycle._state._owed[id(worker.sink)] = worker.sink


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
    ``_owed`` would make "nothing was ever logged, so nothing is closed"
    untestable in-process. ``_atexit_registered`` is deliberately **not** cleared — the
    handler really is registered for the life of the interpreter, and re-arming the flag would
    have the next worker register a second one.

    SPEC-033 adds three more. ``_stop`` is replaced rather than cleared, since an
    ``Event`` cannot be un-set and a test that called ``shutdown()`` would otherwise leave every
    later test's sink backing off not at all. SPEC-054 made it and ``retirements`` the process's
    only pair, so resetting the count to zero is what makes ``retired`` false again and what puts
    a worker built by the next test back in step with it — a count left raised makes every later
    worker read as retired at its own build, which is a suite that delivers nothing. ``_lifecycle._closers`` is now process-global, so a
    hung closer from one test — the capped-grace tests create them deliberately — would
    otherwise leak a non-zero ``closing_sinks`` into the next.

    SPEC-044 adds three more, and each would leak in a different direction. A
    ``_shutdown_running`` left raised by a test that injected a preemption point makes the
    **next** test's first ``@trace`` register a late worker for a shutdown that ended long ago,
    and hand it a discharged close. A ``_late_worker`` left set pins a worker object and hands
    the next ``shutdown()`` something to drain that it does not own. And ``_closing_now`` left
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
    with a per-sink registration in ``_closing_now``, which was already cleared here, so the pair
    is gone rather than merely reset: a caller now waits only on closes actually in flight against
    a sink, and a leaked entry shows up as that sink never being closed rather than as a
    process-wide charge.

    SPEC-054 also merged four records into ``_state._owed``, so one ``clear()`` replaces the four
    lines that reset them, and ``retirements`` replaces the retirement flag: it is a **count**, so
    resetting it to zero is what puts a worker built by the next test back in step with it — one
    left raised makes every later worker read as retired at its own build, which is a suite that
    delivers nothing.
    """
    try:
        from log_foundry import _lifecycle, decorator
    except ImportError:
        return
    worker = _lifecycle._state.worker_exists()
    if worker is not None:
        worker.stop()
        _lifecycle._state._worker = None
    _lifecycle._state._owed.clear()
    _lifecycle._state.retirements = 0
    _lifecycle._state._stop = threading.Event()
    _lifecycle._state._shutdown_running = 0
    _lifecycle._state._late_worker = None
    decorator._orphan_lost = 0
    decorator._in_span_lost = 0
    with _lifecycle._closers_lock:
        _lifecycle._closers.clear()
    with _lifecycle._closing_now_lock:
        _lifecycle._closing_now.clear()


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


